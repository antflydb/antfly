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

//! Inverted text index section for full-text search.
//!
//! Builds and queries an inverted index using:
//!   - Blocked term dictionary indexed by an FST (block ceiling → block offset)
//!   - Roaring bitmaps for posting lists (document ID sets)
//!   - Chunked int encoder for term frequencies and field norms
//!   - BM25 scoring

const std = @import("std");
const Allocator = std.mem.Allocator;
const roaring = @import("../encoding/roaring.zig");
const chunked = @import("../encoding/chunked_coder.zig");
const simd_bitpack = @import("../encoding/simd_bitpack.zig");
const fst = @import("antfly_fst");
const bloom = @import("bloom");
const platform_time = @import("antfly_platform").time;

// =====================================================================}

// Wire format versions
// =====================================================================}

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
//   v39: append-once merged blocks, trailing fixed-width range navigation;
//        the existing v38 posting layout remains valid for ordinary terms.
//   v38: v35 query structures + a compact postings header that derives block
//        count, compact-metadata length, and skip length; single-block terms
//        also omit redundant impact count and range-ID length fields
//
// Writers emit v39. The production reader accepts v23, the exact format shipped
// by origin/main when this migration began, v38, and v39. Versions v24-v37 are
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
const wire_version_streaming_blocks: u8 = 39;
const wire_version_current: u8 = wire_version_streaming_blocks;
const streamed_record_size: usize = 32;
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
    fst_len: u32,
    bloom_len: u32,
    norms_len: u32,
) void {
    std.debug.assert(dst.len >= v7_header_size);
    @memcpy(dst[0..4], "INVT");
    dst[4] = version;
    dst[5..9].* = @bitCast(@as(u32, doc_count));
    dst[9..17].* = @bitCast(@as(u64, total_field_len));
    dst[17..21].* = @bitCast(@as(u32, chunk_size));
    dst[21..25].* = @bitCast(@as(u32, fst_len));
    dst[25..29].* = @bitCast(@as(u32, bloom_len));
    dst[29..33].* = @bitCast(@as(u32, norms_len));
}

// =====================================================================}

// Varint helpers (LEB128)
// =====================================================================}

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
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, value))));
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
    var block_fst_builder = try fst.Builder.init(alloc, .{
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
    block_fst_builder: fst.Builder,
    finalized_blocks: bool = false,

    fn init(alloc: Allocator) !StreamingTermDictionaryBuilder {
        return initWithHint(alloc, 65_536);
    }

    fn initWithHint(alloc: Allocator, terms: usize) !StreamingTermDictionaryBuilder {
        return .{
            .alloc = alloc,
            // Initial runs know their vocabulary. Small fields must not pay
            // for the maximum registry used by unknown-size large merges.
            .block_fst_builder = try fst.Builder.init(alloc, .{
                .registry_table_size = std.math.clamp(terms, 64, 65_536),
            }),
        };
    }

    pub fn deinit(self: *StreamingTermDictionaryBuilder) void {
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

// =====================================================================}

// Index builder (write path)
// =====================================================================}

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
    postings_capacity_bytes: u64 = 0,
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
        return self.postings_capacity_bytes +|
            @as(u64, self.terms.capacity()) *| (@sizeOf(PostingAccumulator) + @sizeOf([]const u8) + 24) +|
            @as(u64, @intCast(self.doc_norms.capacity)) *| @sizeOf(u32) +|
            @as(u64, @intCast(self.term_arena.queryCapacity()));
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
                gop.value_ptr.* = PostingAccumulator.init();
                gop.key_ptr.* = try self.term_arena.allocator().dupe(u8, hit.term);
            }
            const before = gop.value_ptr.estimatedMemoryBytes();
            try gop.value_ptr.add(self.alloc, doc_num, hit.freq, hit.norm, hit.positions);
            self.postings_capacity_bytes +|= gop.value_ptr.estimatedMemoryBytes() - before;
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
            gop.value_ptr.* = PostingAccumulator.init();
            gop.key_ptr.* = try self.term_arena.allocator().dupe(u8, term);
        }
        const before = gop.value_ptr.estimatedMemoryBytes();
        try gop.value_ptr.add(self.alloc, doc_num, freq, norm_val, &.{});
        self.postings_capacity_bytes +|= gop.value_ptr.estimatedMemoryBytes() - before;
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
    ///   [FST data]
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

    /// Emit postings directly into a private publication sink. Large terms
    /// retain one output block; the dictionary retains compact navigation.
    pub fn writeToSink(self: *InvertedIndexBuilder, alloc: Allocator, sink: anytype) !void {
        return self.writeToSinkProfile(alloc, sink, null);
    }

    pub fn writeToSinkProfile(self: *InvertedIndexBuilder, alloc: Allocator, sink: anytype, profile: ?*InvertedIndexBuildProfile) !void {
        const sort_start = if (profile != null) platform_time.monotonicNs() else 0;
        const term_count = self.terms.count();
        if (term_count == 0) return;
        const sorted = try alloc.alloc([]const u8, term_count);
        defer alloc.free(sorted);
        var keys = self.terms.keyIterator();
        var n: usize = 0;
        while (keys.next()) |key| : (n += 1) sorted[n] = key.*;
        std.mem.sort([]const u8, sorted, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);
        if (profile) |p| p.sort_ns +|= platform_time.monotonicNs() - sort_start;
        const start = sink.len();
        const empty_header: [v7_header_size]u8 = @splat(0);
        try sink.appendSlice(&empty_header);
        var dictionary = try StreamingTermDictionaryBuilder.initWithHint(alloc, term_count);
        defer dictionary.deinit();
        var scratch = PostingSerializeScratch{};
        defer scratch.deinit(alloc);
        var encoded = std.ArrayListUnmanaged(u8).empty;
        defer encoded.deinit(alloc);
        const postings_start = if (profile != null) platform_time.monotonicNs() else 0;
        for (sorted) |term| {
            const acc = self.terms.getPtr(term).?;
            const value = if (self.config.wireVersion() == wire_version_current and
                self.config.postings_layout == .posting_count_v35 and acc.doc_ids.items.len > self.config.chunk_size)
            blk: {
                var stream = AccumulatorStream{ .source = acc, .alloc = alloc };
                var ignored_total: u64 = 0;
                break :blk (try appendPostingStreamToSink(alloc, sink, start, &stream, acc.all_positions.items.len > 0, self.doc_norms.items, &ignored_total, self.config)).?;
            } else try appendMergedTermToSink(alloc, sink, start, &encoded, &scratch, acc, self.config, self.doc_count);
            try dictionary.add(term, value);
        }
        if (profile) |p| p.postings_serialize_ns +|= platform_time.monotonicNs() - postings_start;
        const norms_start = if (profile != null) platform_time.monotonicNs() else 0;
        const norms = try encodeNormTable(alloc, self.doc_norms.items);
        defer alloc.free(norms);
        if (profile) |p| p.norms_ns +|= platform_time.monotonicNs() - norms_start;
        try finishStreamingSectionProfile(alloc, sink, start, self.doc_count, self.total_field_len, norms, &dictionary, self.config, profile);
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

    pub fn deinit(self: *PostingSerializeScratch, alloc: Allocator) void {
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
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, boundary.max_doc))));
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, @as(u32, @intCast(chunk_index))))));
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, boundary.chunk_id))));
        const payload_end = boundary.doc_ctrl_off + boundary.doc_ctrl_len;
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, payload_end))));
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
    return decodePackedU32BitRange(data, std.math.mul(usize, start_index, bits) catch return error.InvalidData, values, bits);
}
fn decodePackedU32BitRange(data: []const u8, start_bit: usize, values: []u32, bits: u8) !void {
    if (bits > 32) return error.InvalidData;
    if (bits == 0) {
        @memset(values, 0);
        return;
    }
    if (values.len == 0) return;
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

    pub fn deinit(self: *PostingAccumulator, alloc: Allocator) void {
        self.doc_ids.deinit(alloc);
        self.metas.deinit(alloc);
        self.all_positions.deinit(alloc);
    }

    pub fn estimatedMemoryBytes(self: *const PostingAccumulator) u64 {
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

    const EncodedPostingChunk = struct { metadata: V7ChunkMeta, max_freq: u16, min_norm: u16 };

    fn appendEncodedChunk(self: *const PostingAccumulator, alloc: Allocator, scratch: *PostingSerializeScratch, config: IndexConfig, doc_start: usize, doc_end: usize, chunk_id: u32, pos_offset: *usize, store_positions: bool) !EncodedPostingChunk {
        var chunk_max_freq: u16 = 0;
        var chunk_min_norm: u16 = std.math.maxInt(u16);
        scratch.doc_deltas.clearRetainingCapacity();
        scratch.freq_values.clearRetainingCapacity();
        scratch.position_chunk.clearRetainingCapacity();
        try scratch.doc_deltas.ensureTotalCapacity(alloc, doc_end - doc_start);
        try scratch.freq_values.ensureTotalCapacity(alloc, doc_end - doc_start);
        const chunk_positions_start = pos_offset.*;

        var prev_doc: u32 = 0;
        var i = doc_start;
        while (i < doc_end) : (i += 1) {
            const doc_id = self.doc_ids.items[i];
            const meta = self.metas.items[i];
            scratch.doc_deltas.appendAssumeCapacity(if (i == doc_start)
                (if (config.postings_layout == .posting_count_v35) doc_id else doc_id - chunk_id * config.chunk_size)
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
                    self.all_positions.items[pos_offset.*..][0..count],
                );
            }
            pos_offset.* += count;
        }

        if (store_positions and usesGroupedPositions(config.wireVersion())) {
            try appendGroupedPositionsForChunk(
                alloc,
                &scratch.position_chunk,
                self.metas.items[doc_start..doc_end],
                self.all_positions.items[chunk_positions_start..pos_offset.*],
                &scratch.position_group_deltas,
            );
        }

        if (store_positions) {
            try writeVarintU32(alloc, &scratch.positions, @intCast(scratch.position_chunk.items.len));
            try scratch.positions.appendSlice(alloc, scratch.position_chunk.items);
        }

        const packed_chunk = if (config.postings_layout == .posting_count_v35)
            try appendPackedPostingBlock(alloc, &scratch.payload, scratch.doc_deltas.items, scratch.freq_values.items, config.wireVersion())
        else
            try appendPackedPostingChunk(alloc, &scratch.payload, scratch.doc_deltas.items, scratch.freq_values.items);
        const metadata = V7ChunkMeta{
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
        };

        return .{ .metadata = metadata, .max_freq = chunk_max_freq, .min_norm = chunk_min_norm };
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

            const encoded = try self.appendEncodedChunk(alloc, scratch, config, doc_start, doc_end, chunk_id, &pos_offset, store_positions);
            const chunk_max_freq = encoded.max_freq;
            const chunk_min_norm = encoded.min_norm;
            try scratch.chunks.append(alloc, encoded.metadata);
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

// =====================================================================}

// Index reader (query path)
// =====================================================================}

/// Reads the origin/main v23 format and the current production format.
pub const PostingsLoader = struct {
    ptr: *anyopaque,
    context: ?*anyopaque = null,
    base: usize = 0,
    ensure: *const fn (*anyopaque, ?*anyopaque, usize) anyerror!void,
};
pub const InvertedIndexReader = struct {
    postings_loader: ?PostingsLoader = null,
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
    dict_fst: fst.FST,
    /// Optional per-segment term bloom filter. When present, callers can
    /// reject absent terms before walking the FST. Borrows into `data`.
    term_bloom: ?bloom.BorrowedFilter,

    pub fn init(alloc: Allocator, data: []const u8) !InvertedIndexReader {
        if (data.len < v7_header_size) return error.InvalidData;
        if (!std.mem.eql(u8, data[0..4], "INVT")) return error.InvalidMagic;
        const version = data[4];
        if (version != wire_version_legacy and version != wire_version_compact_postings_header and version != wire_version_current) return error.UnsupportedVersion;

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
        const dict_fst = try fst.FST.load(block_fst_data);

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
        var it = try self.termIterator();
        defer it.deinit();
        return accumulateLayoutStats(self.layoutStats(), &it);
    }

    /// Look up a term using the blocked term dictionary. Returns posting data, or null.
    /// For 1-hit terms, returns a synthetic TermPostings with the single doc.
    /// Consults the per-segment term bloom filter before walking the FST
    /// when present, so absent-term lookups skip the FST traversal entirely.
    pub fn lookup(self: *const InvertedIndexReader, term: []const u8) ?LookupResult {
        std.debug.assert(self.postings_loader == null);
        return self.lookupChecked(term) catch unreachable;
    }
    /// Remote readers propagate I/O and request-authority failures rather
    /// than turning them into an absent term.
    pub fn lookupChecked(self: *const InvertedIndexReader, term: []const u8) !?LookupResult {
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

        return .{ .postings = try self.readPostingsChecked(dict_value) };
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

    /// Iterate terms matching an automaton. Blocks are enumerated by the
    /// block-ceiling FST; blocks whose shared prefix already leaves the
    /// automaton dead are skipped by seeking past the dead prefix, and the
    /// remaining terms are checked incrementally from their front-coded
    /// shared prefix.
    pub fn fstSearchIterator(self: *const InvertedIndexReader, aut: fst.Automaton) !TermIterator {
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
        return lookupBlockedTerm(self.alloc, self.dict_blocks, block_offset, term);
    }

    fn readPostingsChecked(self: *const InvertedIndexReader, offset: u64) !TermPostings {
        if (self.postings_loader) |loader| try loader.ensure(loader.ptr, loader.context, loader.base + self.postings_offset + @as(usize, @intCast(offset)));
        return self.readPostings(offset);
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
            if (self.version >= wire_version_streaming_blocks and doc_id == std.math.maxInt(u32)) {
                const descriptor = StreamedDescriptor.decode(self.data[cursor..][0..StreamedDescriptor.size]);
                descriptor.validate(self.data.len, self.doc_count, self.chunk_size, base) catch unreachable;
                const span = self.data[@intCast(descriptor.span_start)..][0..@intCast(descriptor.span_length)];
                const table = self.data[@intCast(descriptor.table_start)..][0 .. @as(usize, descriptor.chunks) * streamed_record_size];
                const impact_len = packedU32ByteLen(descriptor.impacts, 5) + descriptor.impacts;
                const impacts = self.data[@intCast(descriptor.impact_start)..][0..impact_len];
                const ids = self.data[@intCast(descriptor.impact_start + impact_len)..][0..descriptor.ids_length];
                return .{ .doc_freq = descriptor.doc_frequency, .serialized_data = self.data[@intCast(descriptor.span_start) .. cursor + StreamedDescriptor.size], .header_len = cursor + StreamedDescriptor.size - base, .chunk_size = self.chunk_size, .version = self.version, .doc_range_aligned = descriptor.ids_length > 0, .block_max = if (descriptor.impacts == 0) null else .{ .meta = impacts, .chunk_size = impact_range_doc_count, .chunk_meta_data = ids, .chunk_meta_count = descriptor.impacts, .version = self.version, .range_ids = true, .packed_impact_frequency = true }, .chunk_meta_data = &.{}, .chunk_meta_count = descriptor.chunks, .streamed_records = table, .payload_data = span, .norms_data = self.norms_data, .positions_data = if (descriptor.positions_length == 0) null else span, .logical_payload_length = @intCast(descriptor.payload_length), .logical_positions_length = @intCast(descriptor.positions_length), .impact_chunk_ids_data = if (descriptor.ids_length == 0) null else ids, .impact_chunk_count = descriptor.impacts };
            }
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
    block_iter: fst.FSTIterator,
    current_block_prefix: []const u8 = &.{},
    current_block_cursor: usize = 0,
    current_block_remaining: u32 = 0,
    current_block_last_postings_offset: u64 = 0,
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
    automaton: ?fst.Automaton = null,
    /// Automaton states aligned with `current_key`: entry `i` is the state
    /// after consuming `current_key[0..i]`. Front-coded terms reuse the
    /// states of their shared prefix, so each decoded term only feeds its
    /// leaf bytes through the automaton.
    automaton_states: std.ArrayListUnmanaged(usize) = .empty,
    /// Scratch key used to seek the block iterator past a dead prefix.
    seek_scratch: std.ArrayListUnmanaged(u8) = .empty,
    /// Diagnostic: dictionary blocks skipped without decoding because their
    /// shared prefix cannot lead to an automaton match.
    blocks_pruned: u64 = 0,
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
            if (self.current_block_prefix.len + shared_len > self.current_key.items.len) return error.InvalidData;
            if (self.current_block_cursor + leaf_len > self.reader.dict_blocks.len) return error.InvalidData;
            const leaf = self.reader.dict_blocks[self.current_block_cursor..][0..leaf_len];
            self.current_block_cursor += leaf_len;
            const value = decodeTermDictBlockValueDelta(readVarintU64(self.reader.dict_blocks, &self.current_block_cursor) catch return error.InvalidData, &self.current_block_last_postings_offset);
            self.current_block_remaining -= 1;
            if (comptime track_decoded) decoded_count.?.* += 1;

            self.current_key.shrinkRetainingCapacity(self.current_block_prefix.len + shared_len);
            try self.current_key.appendSlice(self.alloc, leaf);

            // The automaton state stack must track every decoded key, so it
            // advances before any range check can skip the term.
            if (self.automaton) |aut| {
                if (!try self.advanceAutomatonStates(aut, self.current_block_prefix.len + shared_len)) continue;
            }
            if (self.start) |start| {
                if (std.mem.order(u8, self.current_key.items, start) == .lt) continue;
            }
            if (self.end) |end| {
                if (std.mem.order(u8, self.current_key.items, end) != .lt) return null;
            }

            const result: LookupResult = if (fstValIs1Hit(value))
                .{ .one_hit = .{
                    .doc_num = @intCast(fstValDecode1Hit(value).doc_num),
                    .norm_bits = self.reader.normForDoc(@intCast(fstValDecode1Hit(value).doc_num)),
                } }
            else
                .{ .postings = try self.reader.readPostingsChecked(value) };

            return .{ .term = self.current_key.items, .result = result };
        }
    }

    fn loadNextBlock(self: *TermIterator) !bool {
        while (true) {
            const current = self.block_iter.current() orelse return false;
            const block_offset: usize = @intCast(current.val);
            if (block_offset >= self.reader.dict_blocks.len) return error.InvalidData;
            var cursor = block_offset;
            const prefix_len = readVarintU32(self.reader.dict_blocks, &cursor) catch return error.InvalidData;
            const block_remaining = readVarintU32(self.reader.dict_blocks, &cursor) catch return error.InvalidData;
            if (cursor + prefix_len > self.reader.dict_blocks.len) return error.InvalidData;
            const block_prefix = self.reader.dict_blocks[cursor..][0..prefix_len];
            cursor += prefix_len;

            if (self.automaton) |aut| {
                if (try self.primeAutomatonStates(aut, block_prefix)) |dead_len| {
                    // Every term in this block starts with `block_prefix`, and
                    // the automaton is already dead after `dead_len` of its
                    // bytes, so no term in this block (or in any later block
                    // sharing that dead prefix) can match. Seek the block
                    // index straight past the dead range instead of decoding
                    // each block's terms one by one.
                    self.blocks_pruned += 1;
                    if (!try self.seekPastDeadPrefix(block_prefix[0..dead_len])) return false;
                    continue;
                }
            }

            _ = try self.block_iter.nextEntry();
            self.current_block_remaining = block_remaining;
            self.current_block_prefix = block_prefix;
            self.current_block_cursor = cursor;
            self.current_block_last_postings_offset = 0;
            self.current_key.clearRetainingCapacity();
            try self.current_key.appendSlice(self.alloc, self.current_block_prefix);
            return true;
        }
    }

    /// Feed a block's shared prefix through the automaton from its start
    /// state, recording the state after every byte. Returns the number of
    /// prefix bytes after which the automaton became dead, or null when the
    /// whole prefix can still lead to a match.
    fn primeAutomatonStates(self: *TermIterator, aut: fst.Automaton, block_prefix: []const u8) !?usize {
        self.automaton_states.clearRetainingCapacity();
        try self.automaton_states.ensureTotalCapacity(self.alloc, block_prefix.len + 1);
        var state = aut.start();
        self.automaton_states.appendAssumeCapacity(state);
        if (!aut.canMatch(state)) return 0;
        for (block_prefix, 0..) |byte, index| {
            state = aut.accept(state, byte);
            self.automaton_states.appendAssumeCapacity(state);
            if (!aut.canMatch(state)) return index + 1;
        }
        return null;
    }

    /// Extend the automaton state stack from the retained key prefix to the
    /// end of `current_key`. Returns whether the full term is accepted.
    fn advanceAutomatonStates(self: *TermIterator, aut: fst.Automaton, retained_len: usize) !bool {
        if (self.automaton_states.items.len <= retained_len) return error.InvalidData;
        self.automaton_states.shrinkRetainingCapacity(retained_len + 1);
        const leaf = self.current_key.items[retained_len..];
        try self.automaton_states.ensureUnusedCapacity(self.alloc, leaf.len);
        var state = self.automaton_states.items[retained_len];
        for (leaf) |byte| {
            // Dead states stay dead; keep pushing so the stack stays aligned
            // with `current_key` for the next front-coded term.
            if (aut.canMatch(state)) state = aut.accept(state, byte);
            self.automaton_states.appendAssumeCapacity(state);
        }
        return aut.canMatch(state) and aut.isMatch(state);
    }

    /// Reposition the block iterator at the first block whose ceiling sorts
    /// after every key starting with `dead_prefix`. Returns false when no such
    /// key exists (the prefix is all 0xFF bytes), which ends iteration.
    fn seekPastDeadPrefix(self: *TermIterator, dead_prefix: []const u8) !bool {
        self.seek_scratch.clearRetainingCapacity();
        try self.seek_scratch.appendSlice(self.alloc, dead_prefix);
        while (self.seek_scratch.items.len > 0 and self.seek_scratch.items[self.seek_scratch.items.len - 1] == 0xFF) {
            self.seek_scratch.items.len -= 1;
        }
        if (self.seek_scratch.items.len == 0) return false;
        self.seek_scratch.items[self.seek_scratch.items.len - 1] += 1;
        try self.block_iter.seek(self.seek_scratch.items);
        return true;
    }

    pub fn deinit(self: *TermIterator) void {
        self.current_key.deinit(self.alloc);
        self.automaton_states.deinit(self.alloc);
        self.seek_scratch.deinit(self.alloc);
        self.block_iter.deinit();
    }
};

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

const StreamedDescriptor = struct {
    doc_frequency: u32,
    chunks: u32,
    span_start: u64,
    span_length: u64,
    table_start: u64,
    impact_start: u64,
    impacts: u32,
    ids_length: u32,
    payload_length: u64,
    positions_length: u64,
    const size: usize = 64;
    fn decode(bytes: *const [size]u8) StreamedDescriptor {
        return .{
            .doc_frequency = std.mem.readInt(u32, bytes[0..][0..4], .little),
            .chunks = std.mem.readInt(u32, bytes[4..][0..4], .little),
            .span_start = std.mem.readInt(u64, bytes[8..][0..8], .little),
            .span_length = std.mem.readInt(u64, bytes[16..][0..8], .little),
            .table_start = std.mem.readInt(u64, bytes[24..][0..8], .little),
            .impact_start = std.mem.readInt(u64, bytes[32..][0..8], .little),
            .impacts = std.mem.readInt(u32, bytes[40..][0..4], .little),
            .ids_length = std.mem.readInt(u32, bytes[44..][0..4], .little),
            .payload_length = std.mem.readInt(u64, bytes[48..][0..8], .little),
            .positions_length = std.mem.readInt(u64, bytes[56..][0..8], .little),
        };
    }
    fn encode(self: StreamedDescriptor) [size]u8 {
        var bytes: [size]u8 = undefined;
        std.mem.writeInt(u32, bytes[0..][0..4], self.doc_frequency, .little);
        std.mem.writeInt(u32, bytes[4..][0..4], self.chunks, .little);
        std.mem.writeInt(u64, bytes[8..][0..8], self.span_start, .little);
        std.mem.writeInt(u64, bytes[16..][0..8], self.span_length, .little);
        std.mem.writeInt(u64, bytes[24..][0..8], self.table_start, .little);
        std.mem.writeInt(u64, bytes[32..][0..8], self.impact_start, .little);
        std.mem.writeInt(u32, bytes[40..][0..4], self.impacts, .little);
        std.mem.writeInt(u32, bytes[44..][0..4], self.ids_length, .little);
        std.mem.writeInt(u64, bytes[48..][0..8], self.payload_length, .little);
        std.mem.writeInt(u64, bytes[56..][0..8], self.positions_length, .little);
        return bytes;
    }
    fn validate(self: StreamedDescriptor, section_length: u64, doc_count: u32, chunk_size: u32, header_start: u64) !void {
        if (chunk_size == 0 or self.doc_frequency == 0 or self.doc_frequency > doc_count or self.chunks != 1 + (self.doc_frequency - 1) / chunk_size) return error.InvalidData;
        if (self.span_start < v7_header_size or self.span_start > header_start or self.span_length > header_start - self.span_start) return error.InvalidData;
        if (self.table_start != self.span_start + self.span_length or self.table_start > header_start or @as(u64, self.chunks) * streamed_record_size > header_start - self.table_start) return error.InvalidData;
        if (self.impact_start != self.table_start + @as(u64, self.chunks) * streamed_record_size or self.impact_start > header_start) return error.InvalidData;
        const max_ranges = (@as(u64, std.math.maxInt(u32)) + 1) / impact_range_doc_count;
        if (self.impacts > self.doc_frequency or self.impacts > max_ranges or (self.impacts == 0) != (self.ids_length == 0)) return error.InvalidData;
        const impact_bytes = @as(u64, packedU32ByteLen(self.impacts, 5)) + self.impacts;
        if (impact_bytes + self.ids_length != header_start - self.impact_start or header_start > section_length) return error.InvalidData;
        if (self.payload_length > self.span_length or self.positions_length != self.span_length - self.payload_length) return error.InvalidData;
    }
};

/// Parsed posting data for a single term (zero-copy view into section data).
pub const TermPostings = struct {
    doc_freq: u32,
    metadata_owner: ?@import("../segment_source.zig").SharedOwner = null,
    serialized_data: []const u8,
    serialized_range: ?@import("../segment_source.zig").View = null,
    header_len: usize,
    chunk_size: u32,
    version: u8,
    doc_range_aligned: bool,
    block_max: ?BlockMaxInfo = null,
    chunk_meta_data: []const u8,
    chunk_meta_count: u32,
    streamed_records: []const u8 = &.{},
    streamed_records_range: ?@import("../segment_source.zig").View = null,
    logical_payload_length: ?usize = null,
    logical_positions_length: ?usize = null,
    payload_data: []const u8,
    payload_range: ?@import("../segment_source.zig").View = null,
    max_payload_chunk_bytes: usize = 64 * 1024,
    norms_data: []const u8,
    norms_reader: ?RangeInvertedIndexReader = null,
    positions_data: ?[]const u8 = null,
    positions_range: ?@import("../segment_source.zig").View = null,
    max_position_record_bytes: usize = 1024 * 1024,
    skip_data: ?[]const u8 = null,
    impact_chunk_ids_data: ?[]const u8 = null,
    impact_chunk_count: u32 = 0,
    inline_single_doc: bool = false,
    inline_doc_id: u32 = 0,
    inline_freq: u32 = 0,
    inline_has_locs: bool = false,
    inline_position_bits: u8 = 0,
    inline_positions_data: []const u8 = &.{},

    pub fn payloadLength(self: TermPostings) usize {
        return self.logical_payload_length orelse if (self.payload_range) |view| @intCast(view.length) else self.payload_data.len;
    }
    pub fn positionsLength(self: TermPostings) usize {
        return self.logical_positions_length orelse if (self.positions_range) |view| @intCast(view.length) else if (self.positions_data) |data| data.len else 0;
    }

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
        if (self.inline_single_doc) {
            var inline_iterator = try PostingsIterator.initInlineSingleDoc(self, alloc);
            if (self.metadata_owner) |owner| {
                owner.retain(owner.ptr);
                inline_iterator.metadata_owner = owner;
            }
            return inline_iterator;
        }
        var iter = PostingsIterator{
            .alloc = alloc,
            .metadata_owner = self.metadata_owner,
            .doc_freq = self.doc_freq,
            .chunk_size = self.chunk_size,
            .chunk_meta_data = self.chunk_meta_data,
            .chunk_meta_count = self.chunk_meta_count,
            .streamed_records = self.streamed_records,
            .streamed_records_range = self.streamed_records_range,
            .payload_data = self.payload_data,
            .payload_range = self.payload_range,
            .max_payload_chunk_bytes = self.max_payload_chunk_bytes,
            .norms_data = self.norms_data,
            .norms_reader = self.norms_reader,
            .version = self.version,
            .doc_range_aligned = self.doc_range_aligned,
            .positions_data = self.positions_data,
            .positions_range = self.positions_range,
            .max_position_record_bytes = self.max_position_record_bytes,
            .skip_data = self.skip_data,
            .impact_chunk_ids_data = self.impact_chunk_ids_data,
            .impact_chunk_count = self.impact_chunk_count,
        };
        if (self.metadata_owner) |owner| owner.retain(owner.ptr);
        errdefer iter.deinit();
        try iter.decodeImpactChunkIds();
        return iter;
    }
};

pub const PackedPositionView = struct {
    data: []const u8 = &.{},
    range: ?@import("../segment_source.zig").View = null,
    read_cache: ?*PackedReadCache = null,
    unpacked: ?[]const u32 = null,
    start_index: usize = 0,
    encoded_width: ?u8 = null,
    count: usize,
    bits: u8,

    fn decodeInto(self: PackedPositionView, allocator: Allocator, values: []u32, scratch: *std.ArrayListUnmanaged(u8)) !void {
        if (values.len != self.count) return error.InvalidData;
        if (self.unpacked) |positions| {
            @memcpy(values, positions);
            return;
        }
        const start_bit = try std.math.mul(usize, self.start_index, self.bits);
        const skip = start_bit % 8;
        const value_bits = try std.math.mul(usize, values.len, self.bits);
        const length = (try std.math.add(usize, value_bits, skip + 7)) / 8;
        const offset = start_bit / 8;
        const data = if (self.range) |range| blk: {
            if (offset > range.length or length > range.length - offset) return error.InvalidData;
            if (range.source == .contiguous) break :blk range.source.contiguous[@intCast(range.offset + offset)..][0..length];
            try scratch.ensureTotalCapacityPrecise(allocator, length);
            scratch.items.len = length;
            if (self.read_cache) |cache| try cache.read(range, offset, scratch.items) else try range.readInto(offset, scratch.items);
            break :blk scratch.items;
        } else blk: {
            if (offset > self.data.len or length > self.data.len - offset) return error.InvalidData;
            break :blk self.data[offset..][0..length];
        };
        try decodePackedU32BitRange(data, skip, values, self.bits);
        var previous: u32 = 0;
        for (values) |*value| {
            previous +%= value.*;
            value.* = previous;
        }
    }
    fn packedBytes(self: PackedPositionView) !PackedByteCursor {
        if (self.unpacked != null) return error.InvalidData;
        return .{ .reader = try self.cursor(), .position = try std.math.mul(usize, self.start_index, self.bits), .remaining = try std.math.mul(usize, self.count, self.bits) };
    }
    pub fn cursorAlloc(self: PackedPositionView, allocator: Allocator) !PackedPositionCursor {
        _ = allocator;
        return self.cursor();
    }
    pub fn cursor(self: PackedPositionView) !PackedPositionCursor {
        if (self.bits > 32) return error.InvalidData;
        if (self.unpacked) |values| return .{ .data = &.{}, .remaining = self.count, .bits = self.bits, .byte_index = 0, .unpacked = values };
        const start_bit = std.math.mul(usize, self.start_index, self.bits) catch return error.InvalidData;
        const value_bits = std.math.mul(usize, self.count, self.bits) catch return error.InvalidData;
        const end_bit = std.math.add(usize, start_bit, value_bits) catch return error.InvalidData;
        if ((std.math.add(usize, end_bit, 7) catch return error.InvalidData) / 8 > if (self.range) |range| range.length else self.data.len) return error.InvalidData;

        var result_cursor = PackedPositionCursor{
            .data = self.data,
            .range = self.range,
            .read_cache = self.read_cache,
            .remaining = self.count,
            .bits = self.bits,
            .byte_index = start_bit / 8,
        };
        const initial_skip: u3 = @intCast(start_bit % 8);
        if (self.bits != 0 and initial_skip != 0) {
            result_cursor.reservoir = @as(u64, try result_cursor.readByte(result_cursor.byte_index)) >> initial_skip;
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
    range: ?@import("../segment_source.zig").View = null,
    read_cache: ?*PackedReadCache = null,
    unpacked: ?[]const u32 = null,
    unpacked_index: usize = 0,
    read_buffer: [256]u8 = undefined,
    read_start: usize = 0,
    read_length: usize = 0,
    pub fn deinit(self: *PackedPositionCursor) void {
        _ = self;
    }
    fn readByte(self: *PackedPositionCursor, offset: usize) !u8 {
        if (self.range) |range| {
            if (offset >= range.length) return error.InvalidData;
            if (range.source == .contiguous) return range.source.contiguous[@intCast(range.offset + offset)];
            if (self.read_cache) |cache| return cache.byte(range, offset);
            if (self.read_length == 0 or offset < self.read_start or offset - self.read_start >= self.read_length) {
                self.read_start = offset;
                self.read_length = @intCast(@min(self.read_buffer.len, range.length - offset));
                try range.readInto(offset, self.read_buffer[0..self.read_length]);
            }
            return self.read_buffer[offset - self.read_start];
        }
        if (offset >= self.data.len) return error.InvalidData;
        return self.data[offset];
    }

    pub inline fn next(self: *PackedPositionCursor) !?u32 {
        if (self.remaining == 0) return null;
        if (self.unpacked) |values| {
            if (self.unpacked_index >= values.len) return error.InvalidData;
            const position = values[self.unpacked_index];
            self.unpacked_index += 1;
            self.remaining -= 1;
            return position;
        }
        var delta: u32 = 0;
        if (self.bits != 0) {
            while (self.reservoir_bits < self.bits) {
                self.reservoir |= @as(u64, try self.readByte(self.byte_index)) << @intCast(self.reservoir_bits);
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

const PackedReadCache = struct {
    const Source = @import("../segment_source.zig").Source;
    const Page = struct { source: ?Source = null, start: u64 = 0, length: usize = 0, bytes: [4096]u8 = undefined };
    pages: [4]Page = @splat(.{}),
    next: usize = 0,
    fn same(a: Source, b: Source) bool {
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            .contiguous => |bytes| bytes.ptr == b.contiguous.ptr and bytes.len == b.contiguous.len,
            .ranges => |range| range.ptr == b.ranges.ptr and range.read_into == b.ranges.read_into and range.length == b.ranges.length,
        };
    }
    fn pageFor(self: *@This(), view: @import("../segment_source.zig").View, relative: u64) !*Page {
        if (relative >= view.length) return error.InvalidData;
        const source = view.source;
        const offset = view.offset + relative;
        for (&self.pages) |*page| if (page.source) |current| {
            if (same(current, source) and offset >= page.start and offset - page.start < page.length) return page;
        };
        const page = &self.pages[self.next];
        page.source = null;
        page.length = 0;
        const start = @max(view.offset, offset / 4096 * 4096);
        const length: usize = @intCast(@min(4096, view.offset + view.length - start));
        try source.readInto(start, page.bytes[0..length]);
        page.start = start;
        page.length = length;
        page.source = source;
        self.next = (self.next + 1) % self.pages.len;
        return page;
    }
    fn byte(self: *@This(), view: @import("../segment_source.zig").View, relative: u64) !u8 {
        const page = try self.pageFor(view, relative);
        return page.bytes[@intCast(view.offset + relative - page.start)];
    }
    fn read(self: *@This(), view: @import("../segment_source.zig").View, relative: u64, out: []u8) !void {
        if (relative > view.length or out.len > view.length - relative) return error.InvalidData;
        var copied: usize = 0;
        while (copied < out.len) {
            const page = try self.pageFor(view, relative + copied);
            const within: usize = @intCast(view.offset + relative + copied - page.start);
            const take = @min(out.len - copied, page.length - within);
            @memcpy(out[copied..][0..take], page.bytes[within..][0..take]);
            copied += take;
        }
    }
};

fn exactPositionWidth(view: PackedPositionView, allocator: Allocator) !u8 {
    var maximum: u32 = 0;
    if (view.bits <= 1 and view.unpacked == null) {
        var bytes = try view.packedBytes();
        while (try bytes.next()) |part| maximum |= part.value;
        return if (maximum == 0) 0 else 1;
    }
    var cursor = try view.cursorAlloc(allocator);
    defer cursor.deinit();
    var previous: u32 = 0;
    while (try cursor.next()) |position| {
        maximum |= position -% previous;
        previous = position;
    }
    return @intCast(32 - @clz(maximum));
}

const PackedByteCursor = struct {
    reader: PackedPositionCursor,
    position: usize,
    remaining: usize,
    fn next(self: *@This()) !?struct { value: u8, width: u8 } {
        if (self.remaining == 0) return null;
        const index = self.position / 8;
        const shift: u3 = @intCast(self.position % 8);
        const take: u8 = @intCast(@min(8, self.remaining));
        var value: u16 = @as(u16, try self.reader.readByte(index)) >> shift;
        if (@as(u8, shift) + take > 8) value |= @as(u16, try self.reader.readByte(index + 1)) << @intCast(8 - @as(u8, shift));
        const mask: u16 = (@as(u16, 1) << @intCast(take)) - 1;
        self.position += take;
        self.remaining -= take;
        return .{ .value = @intCast(value & mask), .width = take };
    }
};

const PackedHit = struct { hit: PostingsIterator.Hit, positions: PackedPositionView };
fn positionBits(positions: []const u32) u8 {
    var maximum: u32 = 0;
    var previous: u32 = 0;
    for (positions) |position| {
        maximum |= if (position >= previous) position - previous else 0;
        previous = position;
    }
    return @intCast(32 - @clz(maximum));
}
// Views borrow immutable source bytes, never the iterator's reusable decode
// buffer. Legacy records can therefore survive advancement without seeking
// from the start or materializing an entire position list.
fn nextPackedHit(iterator: *PostingsIterator, cache: *PackedReadCache) !?PackedHit {
    if (iterator.is_one_hit) {
        const hit = (try iterator.takeOneHit(false)) orelse return null;
        const view = PackedPositionView{
            .data = iterator.one_hit_positions_data,
            .count = if (iterator.one_hit_has_locs) iterator.one_hit_freq else 0,
            .bits = iterator.one_hit_position_bits,
        };
        return try packedHitWithSmallDecode(iterator, hit, view);
    }
    if (iterator.positionLength() == null) {
        const hit = (try iterator.next()) orelse return null;
        return .{ .hit = hit, .positions = .{ .count = 0, .bits = 0 } };
    }
    if (iterator.current_chunk_index == std.math.maxInt(usize) or iterator.chunk_doc_pos >= iterator.doc_values.items.len) {
        if (iterator.next_chunk_index >= iterator.chunkCount()) return null;
        const index = iterator.next_chunk_index;
        try iterator.loadChunk(index);
        try iterator.enterPositionChunk(index);
    }
    const doc_pos = iterator.chunk_doc_pos;
    const doc_id = iterator.doc_values.items[doc_pos];
    const decoded = decodeFreqHasLocs(iterator.freq_values.items[doc_pos]);
    var count: usize = if (decoded.has_locs) @intCast(decoded.freq) else 0;
    const contiguous = usesContiguousPositionGroups(iterator.version);
    var bits: u8 = 0;
    if (usesGroupedPositions(iterator.version)) {
        bits = try iterator.ensurePositionGroup(doc_pos);
    } else {
        if (iterator.version < wire_version_chunk_framed_positions) count = try iterator.positionVarint(&iterator.positions_cursor);
        if (count != 0) {
            bits = try iterator.positionByte(iterator.positions_cursor);
            iterator.positions_cursor += 1;
        }
    }
    if (bits > 32) return error.InvalidData;
    const end = if (iterator.version >= wire_version_chunk_framed_positions) iterator.positions_chunk_end else iterator.positionLength().?;
    const start = if (contiguous) iterator.positions_group_data_start else iterator.positions_cursor;
    if (start > end) return error.InvalidData;
    const length = if (contiguous) end - start else (try std.math.add(usize, try std.math.mul(usize, count, bits), 7)) / 8;
    if (start > end or length > end - start) return error.InvalidData;
    const view = PackedPositionView{
        .data = if (iterator.positions_data) |data| data[start..][0..length] else &.{},
        .range = if (iterator.positions_range) |range| try @import("../segment_source.zig").View.init(range.source, range.offset + start, length) else null,
        .start_index = if (contiguous) iterator.positions_group_value_offset else 0,
        .count = count,
        .bits = bits,
        .read_cache = cache,
    };
    if (contiguous and count <= 32) return .{ .hit = try iterator.takeCurrentWithPositions(), .positions = view };
    const norm = try iterator.readNorm(doc_id);
    if (contiguous) try iterator.advanceContiguousPositionRecord(doc_pos, count) else iterator.positions_cursor += length;
    iterator.chunk_doc_pos += 1;
    iterator.position_records_decoded +|= 1;
    iterator.noteReturnedDoc(doc_id);
    return try packedHitWithSmallDecode(iterator, .{ .doc_id = doc_id, .freq = @intCast(decoded.freq), .norm = norm }, view);
}

fn packedHitWithSmallDecode(iterator: *PostingsIterator, hit: PostingsIterator.Hit, view: PackedPositionView) !PackedHit {
    if (view.count > 32) return .{ .hit = hit, .positions = view };
    if (iterator.positions_range != null and view.count > iterator.max_position_record_bytes / 4) return error.SegmentReadBudgetExceeded;
    try iterator.positions_buf.resize(iterator.alloc, view.count);
    // A cursor needs no allocator or source-sized temporary buffer.
    var cursor = try view.cursor();
    for (iterator.positions_buf.items) |*position| position.* = (try cursor.next()) orelse return error.InvalidData;
    var decoded = hit;
    decoded.positions = iterator.positions_buf.items;
    return .{ .hit = decoded, .positions = view };
}

fn PositionByteWriter(comptime Sink: type) type {
    return struct {
        sink: Sink,
        buffer: [4096]u8 = undefined,
        used: usize = 0,
        reservoir: u64 = 0,
        bits: u8 = 0,
        fn byte(self: *@This(), byte_value: u8) !void {
            self.buffer[self.used] = byte_value;
            self.used += 1;
            if (self.used == self.buffer.len) try self.flush();
        }
        fn flush(self: *@This()) !void {
            if (self.used > 0) try self.sink.appendSlice(self.buffer[0..self.used]);
            self.used = 0;
        }
        fn value(self: *@This(), value_: u32, width: u8) !void {
            self.reservoir |= @as(u64, value_) << @intCast(self.bits);
            self.bits += width;
            if (self.bits >= 32) {
                if (self.buffer.len - self.used < 4) try self.flush();
                std.mem.writeInt(u32, self.buffer[self.used..][0..4], @truncate(self.reservoir), .little);
                self.used += 4;
                self.reservoir >>= 32;
                self.bits -= 32;
                if (self.used == self.buffer.len) try self.flush();
            }
            while (self.bits >= 8) {
                try self.byte(@truncate(self.reservoir));
                self.reservoir >>= 8;
                self.bits -= 8;
            }
        }
        fn bytes(self: *@This(), data: []const u8) !void {
            std.debug.assert(self.bits == 0);
            var offset: usize = 0;
            while (offset < data.len) {
                const take = @min(data.len - offset, self.buffer.len - self.used);
                @memcpy(self.buffer[self.used..][0..take], data[offset..][0..take]);
                self.used += take;
                offset += take;
                if (self.used == self.buffer.len) try self.flush();
            }
        }
        fn appendPacked(self: *@This(), view: PackedPositionView) !void {
            // Validate even empty windows before reading their packed bytes.
            _ = try view.cursor();
            var position = try std.math.mul(usize, view.start_index, view.bits);
            var remaining = try std.math.mul(usize, view.count, view.bits);
            var input: [4096]u8 = undefined;
            while (remaining != 0) {
                const skip: usize = position % 8;
                const take = @min(remaining, input.len * 8 - skip);
                const length = (skip + take + 7) / 8;
                const offset = position / 8;
                if (view.range) |range| {
                    if (view.read_cache) |cache| try cache.read(range, offset, input[0..length]) else try range.readInto(offset, input[0..length]);
                } else @memcpy(input[0..length], view.data[offset..][0..length]);
                if (self.bits == 0 and skip == 0) {
                    const whole = take / 8;
                    try self.bytes(input[0..whole]);
                    const tail: u8 = @intCast(take % 8);
                    if (tail != 0) try self.value(input[whole] & ((@as(u8, 1) << @intCast(tail)) - 1), tail);
                } else {
                    // Repackage up to 32 bits at once, including misaligned
                    // starts and output tails, rather than per-byte cursors.
                    var bit = skip;
                    const end = skip + take;
                    while (bit < end) {
                        const width: u8 = @intCast(@min(32, end - bit));
                        const byte_offset = bit / 8;
                        const shift: u3 = @intCast(bit % 8);
                        const needed = (@as(usize, shift) + width + 7) / 8;
                        var word: u64 = 0;
                        if (needed >= 4) {
                            word = std.mem.readInt(u32, input[byte_offset..][0..4], .little);
                            if (needed == 5) word |= @as(u64, input[byte_offset + 4]) << 32;
                        } else {
                            for (input[byte_offset..][0..needed], 0..) |part, i| word |= @as(u64, part) << @intCast(i * 8);
                        }
                        word >>= shift;
                        const mask: u64 = if (width == 32) std.math.maxInt(u32) else (@as(u64, 1) << @intCast(width)) - 1;
                        try self.value(@intCast(word & mask), width);
                        bit += width;
                    }
                }
                position += take;
                remaining -= take;
            }
        }
        fn alignByte(self: *@This()) !void {
            if (self.bits > 0) try self.byte(@truncate(self.reservoir));
            self.reservoir = 0;
            self.bits = 0;
        }
    };
}
fn appendPackedPositionsToSink(allocator: Allocator, sink: anytype, views: []const PackedPositionView) !usize {
    var length: usize = 0;
    var start: usize = 0;
    while (start < views.len) : (start += position_doc_group_size) {
        const group = views[start..@min(views.len, start + position_doc_group_size)];
        var bits: u8 = 0;
        var count: usize = 0;
        for (group) |view| {
            bits = @max(bits, view.encoded_width orelse view.bits);
            count = try std.math.add(usize, count, view.count);
        }
        const packed_bits = try std.math.mul(usize, count, bits);
        const packed_bytes = (try std.math.add(usize, packed_bits, 7)) / 8;
        length = try std.math.add(usize, length, try std.math.add(usize, 1, packed_bytes));
    }
    var frame: [5]u8 = undefined;
    var value: u32 = std.math.cast(u32, length) orelse return error.Overflow;
    var used: usize = 0;
    while (value >= 128) {
        frame[used] = @as(u8, @truncate(value)) | 128;
        used += 1;
        value >>= 7;
    }
    frame[used] = @intCast(value);
    used += 1;
    try sink.appendSlice(frame[0..used]);
    var output = PositionByteWriter(@TypeOf(sink)){ .sink = sink };
    start = 0;
    while (start < views.len) : (start += position_doc_group_size) {
        const group = views[start..@min(views.len, start + position_doc_group_size)];
        var bits: u8 = 0;
        for (group) |view| bits = @max(bits, view.encoded_width orelse view.bits);
        try output.byte(bits);
        for (group) |view| {
            if (bits == 0) continue;
            if (view.bits == bits and view.unpacked == null) {
                try output.appendPacked(view);
            } else {
                var cursor = try view.cursorAlloc(allocator);
                defer cursor.deinit();
                var previous: u32 = 0;
                while (try cursor.next()) |position| {
                    try output.value(if (position >= previous) position - previous else 0, bits);
                    previous = position;
                }
            }
        }
        try output.alignByte();
    }
    try output.flush();
    return used + length;
}

/// Iterates over (doc_id, freq, norm, positions) for a term's posting list.
pub const PostingsIterator = struct {
    alloc: Allocator,
    metadata_owner: ?@import("../segment_source.zig").SharedOwner = null,
    doc_freq: u32 = 0,
    chunk_size: u32 = 0,
    chunk_meta_data: []const u8 = &.{},
    chunk_meta_count: u32 = 0,
    streamed_records: []const u8 = &.{},
    streamed_records_range: ?@import("../segment_source.zig").View = null,
    payload_data: []const u8 = &.{},
    payload_range: ?@import("../segment_source.zig").View = null,
    payload_buffer: std.ArrayListUnmanaged(u8) = .empty,
    max_payload_chunk_bytes: usize = 64 * 1024,
    norms_data: []const u8 = &.{},
    norms_reader: ?RangeInvertedIndexReader = null,
    current_chunk_index: usize = std.math.maxInt(usize),
    current_chunk_meta: ?V7ChunkMeta = null,
    current_chunk_min_doc: u32 = 0,
    next_chunk_index: usize = 0,
    chunk_doc_pos: usize = 0,
    version: u8 = wire_version_current,
    doc_range_aligned: bool = false,
    positions_data: ?[]const u8 = null,
    positions_range: ?@import("../segment_source.zig").View = null,
    position_read_buffer: std.ArrayListUnmanaged(u8) = .empty,
    max_position_record_bytes: usize = 1024 * 1024,
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

    fn initInlineSingleDoc(postings: *const TermPostings, alloc: Allocator) !PostingsIterator {
        return .{
            .alloc = alloc,
            .norms_data = postings.norms_data,
            .version = postings.version,
            .is_one_hit = true,
            .one_hit_doc = postings.inline_doc_id,
            .one_hit_norm = if (postings.norms_reader) |reader| try reader.docLength(postings.inline_doc_id) else decodeNormValue(postings.norms_data, postings.inline_doc_id),
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
            if (self.positions_range != null) try self.positions_buf.ensureTotalCapacityPrecise(self.alloc, count) else try self.positions_buf.ensureTotalCapacity(self.alloc, count);
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

    fn hasStreamedRecords(self: *const PostingsIterator) bool {
        return self.streamed_records_range != null or self.streamed_records.len != 0;
    }
    fn streamedRecord(self: *const PostingsIterator, index: usize) ![streamed_record_size]u8 {
        if (index >= self.chunk_meta_count) return error.InvalidData;
        var bytes: [streamed_record_size]u8 = undefined;
        if (self.streamed_records_range) |view| try view.readInto(index * streamed_record_size, &bytes) else @memcpy(&bytes, self.streamed_records[index * streamed_record_size ..][0..streamed_record_size]);
        return bytes;
    }

    fn chunkMeta(self: *PostingsIterator, index: usize) !V7ChunkMeta {
        if (self.hasStreamedRecords()) {
            const bytes = try self.streamedRecord(index);
            const count = std.mem.readInt(u32, bytes[4..8], .little);
            const expected = if (index + 1 < self.chunk_meta_count) self.chunk_size else self.doc_freq - @as(u32, @intCast(index)) * self.chunk_size;
            if (count != expected) return error.InvalidData;
            return .{ .chunk_id = @intCast(index), .max_doc = std.mem.readInt(u32, bytes[0..4], .little), .doc_count = count, .doc_ctrl_off = 0, .doc_ctrl_len = std.mem.readInt(u32, bytes[16..20], .little), .doc_data_off = 0, .doc_data_len = 0, .freq_ctrl_off = 0, .freq_ctrl_len = 0, .freq_data_off = 0, .freq_data_len = 0 };
        }
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
        const meta = if (!self.hasStreamedRecords() and self.version >= wire_version_checkpoints and
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

        const payload_length = if (self.payload_range) |view| view.length else self.payload_data.len;
        const payload_offset: u64 = if (self.hasStreamedRecords()) blk: {
            const record = try self.streamedRecord(index);
            break :blk std.mem.readInt(u64, record[8..16], .little);
        } else meta.doc_ctrl_off;
        if (payload_offset > payload_length or meta.doc_ctrl_len > payload_length - payload_offset) return error.InvalidData;
        const chunk_data: []const u8 = if (self.payload_range) |view| blk: {
            if (meta.doc_ctrl_len > self.max_payload_chunk_bytes) return error.SegmentReadBudgetExceeded;
            try self.payload_buffer.ensureTotalCapacityPrecise(self.alloc, meta.doc_ctrl_len);
            self.payload_buffer.items.len = meta.doc_ctrl_len;
            try view.readInto(payload_offset, self.payload_buffer.items);
            // Warm only the next document chunk. Streamed layouts interleave
            // positions; never assume the bytes after this chunk are documents.
            if (view.source == .ranges and view.source.ranges.prefetch != null and index + 1 < self.chunk_meta_count) lookahead: {
                const next_meta = self.chunkMeta(index + 1) catch break :lookahead;
                const next_offset: u64 = if (self.hasStreamedRecords()) blk2: {
                    const record = self.streamedRecord(index + 1) catch break :lookahead;
                    break :blk2 std.mem.readInt(u64, record[8..16], .little);
                } else next_meta.doc_ctrl_off;
                if (next_offset <= view.length and next_meta.doc_ctrl_len <= view.length - next_offset)
                    view.source.prefetch(view.offset + next_offset, next_meta.doc_ctrl_len);
            }

            break :blk self.payload_buffer.items;
        } else self.payload_data[@intCast(payload_offset)..][0..meta.doc_ctrl_len];
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

    fn positionLength(self: *const PostingsIterator) ?usize {
        if (self.positions_range) |view| return std.math.cast(usize, view.length);
        return if (self.positions_data) |data| data.len else null;
    }

    fn positionByte(self: *PostingsIterator, offset: usize) !u8 {
        const length = self.positionLength() orelse return error.InvalidData;
        if (offset >= length) return error.InvalidData;
        if (self.positions_range) |view| {
            var byte: [1]u8 = undefined;
            try view.readInto(offset, &byte);
            return byte[0];
        }
        return self.positions_data.?[offset];
    }

    fn positionVarint(self: *PostingsIterator, offset: *usize) !u32 {
        if (self.positions_range == null) return readVarintU32(self.positions_data.?, offset) catch return error.InvalidData;
        var value: u32 = 0;
        for (0..5) |i| {
            const byte = try self.positionByte(offset.*);
            offset.* += 1;
            if (i == 4 and byte > 15) return error.InvalidData;
            value |= @as(u32, byte & 127) << @as(u5, @intCast(i * 7));
            if (byte & 128 == 0) return value;
        }
        return error.InvalidData;
    }

    fn positionSlice(self: *PostingsIterator, offset: usize, length: usize) ![]const u8 {
        const total = self.positionLength() orelse return error.InvalidData;
        if (offset > total or length > total - offset) return error.InvalidData;
        if (self.positions_range) |view| {
            if (length > self.max_position_record_bytes) return error.SegmentReadBudgetExceeded;
            try self.position_read_buffer.ensureTotalCapacityPrecise(self.alloc, length);
            self.position_read_buffer.items.len = length;
            try view.readInto(offset, self.position_read_buffer.items);
            return self.position_read_buffer.items;
        }
        return self.positions_data.?[offset..][0..length];
    }

    fn decodePositionPackedRange(self: *PostingsIterator, offset: usize, start: usize, values: []u32, bits: u8) !void {
        if (self.positions_range == null) return decodePackedU32Range(self.positions_data.?[offset..self.positions_chunk_end], start, values, bits);
        const start_bit = std.math.mul(usize, start, bits) catch return error.InvalidData;
        const length_bits = std.math.mul(usize, values.len, bits) catch return error.InvalidData;
        const skip: u3 = @intCast(start_bit % 8);
        const length = (std.math.add(usize, length_bits, @as(usize, skip) + 7) catch return error.InvalidData) / 8;
        const byte_offset = std.math.add(usize, offset, start_bit / 8) catch return error.InvalidData;
        if (byte_offset > self.positions_chunk_end or length > self.positions_chunk_end - byte_offset) return error.InvalidData;
        const bytes = try self.positionSlice(byte_offset, length);
        var cursor = PackedPositionCursor{ .data = bytes, .remaining = values.len, .bits = bits, .byte_index = 0 };
        if (bits != 0 and skip != 0) {
            cursor.reservoir = @as(u64, bytes[0]) >> skip;
            cursor.reservoir_bits = 8 - @as(u8, skip);
            cursor.byte_index = 1;
        }
        for (values) |*value| {
            // This helper matches decodePackedU32Range's raw delta contract;
            // decodePositionRecord applies the document-local prefix sum.
            cursor.previous = 0;
            value.* = (try cursor.next()) orelse return error.InvalidData;
        }
    }

    fn enterPositionChunk(self: *PostingsIterator, chunk_index: usize) !void {
        if (self.version < wire_version_chunk_framed_positions or self.positionLength() == null) return;
        if (self.positions_chunk_index == chunk_index) return;
        const pd_len = self.positionLength().?;
        if (self.hasStreamedRecords()) {
            const record = try self.streamedRecord(chunk_index);
            self.positions_cursor = std.math.cast(usize, std.mem.readInt(u64, record[20..28], .little)) orelse return error.SegmentReadBudgetExceeded;
            const length = std.mem.readInt(u32, record[28..32], .little);
            if (self.positions_cursor > pd_len or length > pd_len - self.positions_cursor) return error.InvalidData;
        }
        if (self.positions_cursor >= pd_len) return error.InvalidData;
        const chunk_len = try self.positionVarint(&self.positions_cursor);
        const chunk_end = self.positions_cursor + @as(usize, chunk_len);
        if (chunk_end > pd_len) return error.InvalidData;
        if (self.hasStreamedRecords()) {
            const record = try self.streamedRecord(chunk_index);
            const start = std.mem.readInt(u64, record[20..28], .little);
            const length = std.mem.readInt(u32, record[28..32], .little);
            if (@as(u64, start) + length != chunk_end) return error.InvalidData;
        }
        self.positions_chunk_end = chunk_end;
        self.positions_chunk_index = chunk_index;
        self.positions_group_doc_end = 0;
        self.positions_group_bits = 0;
        self.positions_group_data_start = 0;
        self.positions_group_value_offset = 0;
        self.positions_group_value_count = 0;
    }

    fn skipPositionChunk(self: *PostingsIterator) !void {
        const pd_len = self.positionLength() orelse return;
        if (self.version < wire_version_chunk_framed_positions) return error.InvalidData;
        if (self.positions_cursor >= pd_len) return error.InvalidData;
        const chunk_len = try self.positionVarint(&self.positions_cursor);
        if (self.positions_cursor + @as(usize, chunk_len) > pd_len) return error.InvalidData;
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
        const bits = try self.positionByte(self.positions_cursor);
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
        const pd_len = self.positionLength() orelse return;
        if (self.version >= wire_version_chunk_framed_positions) {
            const decoded = decodeFreqHasLocs(self.freq_values.items[self.chunk_doc_pos]);
            const bits = if (usesGroupedPositions(self.version))
                try self.ensurePositionGroup(self.chunk_doc_pos)
            else blk: {
                if (!decoded.has_locs) return;
                if (self.positions_cursor >= self.positions_chunk_end) return error.InvalidData;
                const value = try self.positionByte(self.positions_cursor);
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
        if (self.positions_cursor >= pd_len) return error.InvalidData;
        const num_pos = try self.positionVarint(&self.positions_cursor);
        if (num_pos == 0) return;
        if (self.positions_cursor >= pd_len) return error.InvalidData;
        const bits = try self.positionByte(self.positions_cursor);
        self.positions_cursor += 1;
        if (bits > 32) return error.InvalidData;
        const packed_len = packedU32ByteLen(@intCast(num_pos), bits);
        if (self.positions_cursor + packed_len > pd_len) return error.InvalidData;
        self.positions_cursor += packed_len;
    }

    fn decodePositionRecord(self: *PostingsIterator, doc_pos: usize, expected_count: u32, has_locs: bool) !void {
        self.positions_buf.clearRetainingCapacity();
        const pd_len = self.positionLength() orelse return;
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
            if (self.positions_cursor >= pd_len) return error.InvalidData;
            break :blk try self.positionVarint(&self.positions_cursor);
        };
        const positions_end = if (self.version >= wire_version_chunk_framed_positions) self.positions_chunk_end else pd_len;
        const bits = if (usesGroupedPositions(self.version))
            try self.ensurePositionGroup(doc_pos)
        else blk: {
            if (num_pos == 0) return;
            if (self.positions_cursor >= positions_end) return error.InvalidData;
            const value = try self.positionByte(self.positions_cursor);
            self.positions_cursor += 1;
            break :blk value;
        };
        if (num_pos == 0) return;
        if (bits > 32) return error.InvalidData;
        const count: usize = @intCast(num_pos);
        if (usesContiguousPositionGroups(self.version)) {
            if (self.positions_range != null and count > self.max_position_record_bytes / 4) return error.SegmentReadBudgetExceeded;
            if (self.positions_range != null) try self.positions_buf.ensureTotalCapacityPrecise(self.alloc, count) else try self.positions_buf.ensureTotalCapacity(self.alloc, count);
            self.positions_buf.items.len = count;
            try self.decodePositionPackedRange(self.positions_group_data_start, self.positions_group_value_offset, self.positions_buf.items, bits);
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
        if (self.positions_range != null and count > self.max_position_record_bytes / 4) return error.SegmentReadBudgetExceeded;
        if (self.positions_range != null) try self.positions_buf.ensureTotalCapacityPrecise(self.alloc, count) else try self.positions_buf.ensureTotalCapacity(self.alloc, count);
        self.positions_buf.items.len = count;
        try decodePackedU32Into(try self.positionSlice(self.positions_cursor, packed_len), self.positions_buf.items, bits);
        self.positions_cursor += packed_len;
        var prev: u32 = 0;
        for (self.positions_buf.items) |*delta| {
            const position = prev +% delta.*;
            delta.* = position;
            prev = position;
        }
    }

    fn readNorm(self: *const PostingsIterator, doc: u32) !u32 {
        if (self.norms_reader) |reader| return reader.docLength(doc);
        return decodeNormValue(self.norms_data, doc);
    }

    fn takeCurrentWithPositions(self: *PostingsIterator) !Hit {
        const doc_pos = self.chunk_doc_pos;
        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const freq_has_locs_val = self.freq_values.items[self.chunk_doc_pos];
        const norm_val = try self.readNorm(doc_id);
        self.chunk_doc_pos += 1;
        const decoded = decodeFreqHasLocs(freq_has_locs_val);
        try self.decodePositionRecord(doc_pos, @intCast(decoded.freq), decoded.has_locs);
        self.noteReturnedDoc(doc_id);
        return .{ .doc_id = doc_id, .freq = @intCast(decoded.freq), .norm = norm_val, .positions = self.positions_buf.items };
    }

    inline fn takeCurrentScoring(self: *PostingsIterator) !Hit {
        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const freq_has_locs_val = self.freq_values.items[self.chunk_doc_pos];
        const norm_val = try self.readNorm(doc_id);
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
        return try self.takeCurrentScoring();
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
        while (!self.hasStreamedRecords() and skipped_chunk < target_chunk_index) : (skipped_chunk += 1) {
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
        while (!self.hasStreamedRecords() and skipped_chunk < target_chunk_index) : (skipped_chunk += 1) try self.skipPositionChunk();
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
            .norm = try self.readNorm(doc_id),
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
        return !self.is_one_hit and (self.positions_range != null or self.positions_data != null) and usesContiguousPositionGroups(self.version);
    }

    /// Consume a deferred v30+ positional record as a zero-copy packed view.
    /// The caller may stream its delta values after this iterator advances;
    /// the view references immutable segment bytes rather than iterator scratch.
    fn peekDeferredPackedPositions(self: *PostingsIterator) !PackedPositionView {
        if (!self.deferred_position_pending or !self.canTakeDeferredPackedPositions()) return error.InvalidData;
        if (self.chunk_doc_pos >= self.freq_values.items.len) return error.InvalidData;
        const decoded = decodeFreqHasLocs(self.freq_values.items[self.chunk_doc_pos]);
        const bits = try self.ensurePositionGroup(self.chunk_doc_pos);
        return .{
            .data = if (self.positions_data) |data| data[self.positions_group_data_start..self.positions_chunk_end] else &.{},
            .range = if (self.positions_range) |range| try @import("../segment_source.zig").View.init(range.source, range.offset + self.positions_group_data_start, self.positions_chunk_end - self.positions_group_data_start) else null,
            .start_index = self.positions_group_value_offset,
            .count = if (decoded.has_locs) @intCast(decoded.freq) else 0,
            .bits = bits,
        };
    }
    pub fn takeDeferredPackedPositions(self: *PostingsIterator) !PackedPositionView {
        const view = try self.peekDeferredPackedPositions();
        self.deferred_position_pending = false;
        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        try self.advanceContiguousPositionRecord(self.chunk_doc_pos, view.count);
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
            if (self.chunk_doc_pos < self.doc_values.items.len) return try self.takeCurrentScoring();
        }

        const target_chunk_index = try self.nextChunkIndexForTarget(target);
        if (target_chunk_index >= self.chunkCount()) return null;
        try self.loadChunk(target_chunk_index);
        if (self.current_chunk_index == std.math.maxInt(usize) or self.current_chunk_index >= self.chunkCount()) return null;

        while (self.chunk_doc_pos < self.doc_values.items.len and self.doc_values.items[self.chunk_doc_pos] < target) {
            self.chunk_doc_pos += 1;
        }
        if (self.chunk_doc_pos >= self.doc_values.items.len) return try self.advanceTo(target);

        return try self.takeCurrentScoring();
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
        defer if (self.metadata_owner) |owner| owner.release(owner.ptr);
        if (self.is_one_hit) {
            if (self.one_hit_owns_scratch) self.positions_buf.deinit(self.alloc);
        } else {
            self.position_read_buffer.deinit(self.alloc);
            self.payload_buffer.deinit(self.alloc);
            self.doc_values.deinit(self.alloc);
            self.freq_values.deinit(self.alloc);
            self.chunk_metas.deinit(self.alloc);
            self.chunk_meta_values.deinit(self.alloc);
            self.impact_chunk_ids.deinit(self.alloc);
            self.positions_buf.deinit(self.alloc);
        }
    }
};

// =====================================================================}

// BM25 Scoring
// =====================================================================}

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

    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, postings.doc_freq))));
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, @as(u32, @intCast(bitmap_bytes.len))))));
    try out.appendSlice(alloc, bitmap_bytes);
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, @as(u32, @intCast(freq_norm_bytes.len))))));
    try out.appendSlice(alloc, freq_norm_bytes);
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, num_chunks))));
    try out.appendSlice(alloc, block_max_meta);

    const positions_len: u32 = if (postings.positions_data) |pd| @intCast(pd.len) else 0;
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, positions_len))));
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
        out[base..][0..2].* = @bitCast(@as(u16, 0));
        out[base + 2 ..][0..2].* = @bitCast(@as(u16, std.math.maxInt(u16)));
        out[base + 4 ..][0..2].* = @bitCast(@as(u16, 0));
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
        out[base..][0..2].* = @bitCast(@as(u16, chunk_max_freq[chunk_idx]));
        out[base + 2 ..][0..2].* = @bitCast(@as(u16, chunk_min_norm[chunk_idx]));
        out[base + 4 ..][0..2].* = @bitCast(@as(u16, chunk_max_norm[chunk_idx]));
    }

    return out;
}

// =====================================================================}

// 1-Hit Encoding
// =====================================================================}

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

// =====================================================================}

// freqHasLocs Encoding
// =====================================================================}

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

// =====================================================================}

// Configuration
// =====================================================================}

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

// =====================================================================}

// Segment merger
// =====================================================================}

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
    write_calls: usize = 0,
    largest_write: usize = 0,
    output: std.ArrayListUnmanaged(u8) = .empty,

    pub fn deinit(self: *MergeMemorySink) void {
        self.output.deinit(self.alloc);
    }

    fn len(self: *const MergeMemorySink) usize {
        return self.output.items.len;
    }

    fn appendSlice(self: *MergeMemorySink, bytes: []const u8) !void {
        try self.output.appendSlice(self.alloc, bytes);
    }

    fn writeAt(self: *MergeMemorySink, offset: usize, bytes: []const u8) !void {
        self.write_calls += 1;
        self.largest_write = @max(self.largest_write, bytes.len);
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
    sections: anytype,
    doc_counts: []const u32,
    deleted_docs: ?[]const ?roaring.RoaringBitmap,
    config: IndexConfig,
) !void {
    if (sections.len != doc_counts.len) return error.InvalidData;
    if (deleted_docs) |deleted| if (deleted.len != sections.len) return error.InvalidData;
    const maps = try prepareRankDocMaps(alloc, doc_counts, deleted_docs);
    defer deinitRankDocMaps(alloc, maps);
    const last = if (maps.len > 0) maps[maps.len - 1] else RankDocMap{ .len = 0, .offset = 0 };
    const count = last.offset + (last.len - @as(u32, @intCast(if (last.deleted) |d| d.cardinality() else 0)));
    try writeMergedInvertedSectionSlotsWithRankMaps(alloc, sink, sections, doc_counts, maps, count, config);
}

pub fn deinitRankDocMaps(alloc: Allocator, maps: []RankDocMap) void {
    for (maps) |*map| if (map.rank_index) |*rank| rank.deinit();
    alloc.free(maps);
}

pub fn prepareRankDocMaps(alloc: Allocator, doc_counts: []const u32, deleted_docs: ?[]const ?roaring.RoaringBitmap) ![]RankDocMap {
    if (deleted_docs) |deleted| if (deleted.len != doc_counts.len) return error.InvalidData;
    const maps = try alloc.alloc(RankDocMap, doc_counts.len);
    for (maps) |*map| map.* = .{ .len = 0, .offset = 0 };
    errdefer deinitRankDocMaps(alloc, maps);
    var offset: u32 = 0;
    for (doc_counts, maps, 0..) |count, *map, i| {
        const deleted = if (deleted_docs) |d| d[i] else null;
        const cardinality = if (deleted) |d| d.cardinality() else 0;
        if (deleted) |d| {
            const valid = d.rank(count);
            if (valid != cardinality) return error.InvalidData;
        }
        map.* = .{ .len = count, .offset = offset, .deleted = deleted };
        if (deleted) |d| map.rank_index = try roaring.FrozenRankIndex.init(alloc, d);
        offset = try std.math.add(u32, offset, count - @as(u32, @intCast(cardinality)));
    }
    return maps;
}

pub fn writeMergedInvertedSectionSlotsWithRankMaps(alloc: Allocator, sink: anytype, sections: anytype, doc_counts: []const u32, maps: []const RankDocMap, doc_count: u32, config: IndexConfig) !void {
    try writeMappedInvertedSection(alloc, sink, sections, doc_counts, maps, doc_count, config);
}

const StreamedNorms = struct { len: usize };
fn updateMergedNorm(norms: anytype, doc: u32, value: u32) !void {
    if (doc >= norms.len) return error.InvalidData;
    if (@TypeOf(norms) != StreamedNorms) norms[doc] = @max(norms[doc], value);
}
fn MappedNormStream(comptime Map: type) type {
    return struct {
        len: usize,
        readers: []const ScopedInvertedIndexReader,
        present: []const bool,
        maps: []const Map,
        fn write(self: @This(), sink: anytype) !void {
            const count: u32 = @intCast(self.len - 5);
            var header: [5]u8 = undefined;
            std.mem.writeInt(u32, header[0..4], count, .little);
            header[4] = 0xff;
            try sink.appendSlice(&header);
            var bytes: [16 * 1024]u8 = @splat(0);
            var position: u32 = 0;
            var next_doc: u32 = 0;
            for (self.readers, self.present, self.maps) |*reader, present, map| {
                if (!present) continue;
                for (0..map.len) |doc| {
                    const id = try mapDocument(map, @intCast(doc));
                    if (id == std.math.maxInt(u32)) continue;
                    if (id < next_doc or id >= count) return error.InvalidData;
                    while (id - position >= bytes.len) {
                        try sink.appendSlice(&bytes);
                        position += bytes.len;
                        @memset(&bytes, 0);
                    }
                    bytes[id - position] = fieldNormToId(try reader.docLength(@intCast(doc)));
                    next_doc = id + 1;
                }
            }
            while (position < count) {
                const take = @min(bytes.len, count - position);
                try sink.appendSlice(bytes[0..take]);
                position += @intCast(take);
                @memset(&bytes, 0);
            }
        }
    };
}
const InitialNormStream = MappedNormStream(AffineDocMap);
const AppendNormStream = MappedNormStream(RankDocMap);

pub const RankDocMap = struct {
    len: u32,
    offset: u32,
    deleted: ?roaring.RoaringBitmap = null,
    rank_index: ?roaring.FrozenRankIndex = null,
};

/// Private sorted-plan mapping. Live source IDs are monotonic within each
/// physically sorted input; the output-reference stream supplies norm order.
pub const FileDocMap = struct {
    len: u32,
    ids: @import("../segment_source.zig").View,
    records: @import("../segment_source.zig").View,
    monotonic: bool = true,
    scratch: ?@import("../spill_sort.zig").Options = null,
};
const FileNormStream = struct {
    len: usize,
    readers: []const ScopedInvertedIndexReader,
    present: []const bool,
    records: @import("../segment_source.zig").View,
    maps: []const FileDocMap,
    fn write(self: @This(), sink: anytype) !void {
        const count = self.len - 5;
        if (self.records.length != count * 8) return error.InvalidData;
        var header: [5]u8 = undefined;
        std.mem.writeInt(u32, header[0..4], @intCast(count), .little);
        header[4] = 0xff;
        try sink.appendSlice(&header);
        var output: [16 * 1024]u8 = undefined;
        var records: [64 * 8]u8 = undefined;
        var emitted: usize = 0;
        var used: usize = 0;
        while (emitted < count) {
            const take: usize = @min(64, count - emitted);
            try self.records.readInto(emitted * 8, records[0 .. take * 8]);
            for (0..take) |i| {
                const input = std.mem.readInt(u32, records[i * 8 ..][0..4], .little);
                const doc = std.mem.readInt(u32, records[i * 8 + 4 ..][0..4], .little);
                if (input >= self.readers.len or doc >= self.maps[input].len) return error.InvalidData;
                output[used] = if (self.present[input]) fieldNormToId(try self.readers[input].docLength(doc)) else 0;
                used += 1;
                if (used == output.len) {
                    try sink.appendSlice(&output);
                    used = 0;
                }
            }
            emitted += take;
        }
        if (used > 0) try sink.appendSlice(output[0..used]);
    }
};

/// Run-local coordinates use affine maps: no identity array or monotonicity
/// scan grows with the segment's document space.
pub const AffineDocMap = struct {
    len: u32,
    offset: u32,
    ids: ?@import("../segment_source.zig").View = null,
};

fn mapLength(map: anytype) usize {
    return map.len;
}
fn mapDocument(map: anytype, doc: u32) !u32 {
    if (@TypeOf(map) == AffineDocMap) {
        if (map.ids) |ids| {
            var bytes: [4]u8 = undefined;
            try ids.readInto(@as(u64, doc) * 4, &bytes);
            return try std.math.add(u32, map.offset, std.mem.readInt(u32, &bytes, .little));
        }
        return try std.math.add(u32, map.offset, doc);
    }
    if (@TypeOf(map) == RankDocMap) {
        if (map.deleted) |deleted| {
            if (deleted.contains(doc)) return std.math.maxInt(u32);
            return try std.math.add(u32, map.offset, doc - @as(u32, @intCast(if (map.rank_index) |rank| rank.rank(doc) else deleted.rank(doc))));
        }
        return try std.math.add(u32, map.offset, doc);
    }
    if (@TypeOf(map) == FileDocMap) {
        var bytes: [4]u8 = undefined;
        try map.ids.readInto(@as(u64, doc) * 4, &bytes);
        return std.mem.readInt(u32, &bytes, .little);
    }
    return map[doc];
}

pub fn writeMergedInitialRunsToSink(
    alloc: Allocator,
    sink: anytype,
    sections: []const ?@import("../segment_source.zig").View,
    maps: []const AffineDocMap,
    document_space: u32,
    field_doc_count: u32,
    config: IndexConfig,
) !void {
    if (field_doc_count > document_space or maps.len != sections.len) return error.InvalidData;
    const counts = try alloc.alloc(u32, sections.len);
    defer alloc.free(counts);
    for (maps, counts) |map, *count| {
        if (map.ids) |ids| {
            if (ids.length != @as(u64, map.len) * 4) return error.InvalidData;
        } else if (map.offset > document_space or map.len > document_space - map.offset) return error.InvalidData;
        count.* = map.len;
    }
    const start = sink.len();
    try writeMappedInvertedSection(alloc, sink, sections, counts, maps, document_space, config);
    var encoded: [4]u8 = undefined;
    std.mem.writeInt(u32, &encoded, field_doc_count, .little);
    try sink.writeAt(start + 5, &encoded);
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
    sections: anytype,
    doc_counts: []const u32,
    doc_maps: []const []const u32,
    merged_doc_count: u32,
    config: IndexConfig,
) !void {
    try writeMappedInvertedSection(alloc, sink, sections, doc_counts, doc_maps, merged_doc_count, config);
}

pub fn writeMergedInvertedSectionSlotsWithFileMaps(alloc: Allocator, sink: anytype, sections: anytype, doc_counts: []const u32, maps: []const FileDocMap, doc_count: u32, config: IndexConfig) !void {
    if (maps.len == 0) return error.InvalidData;
    for (maps) |map| if (map.ids.length != @as(u64, map.len) * 4 or map.records.length != @as(u64, doc_count) * 8) return error.InvalidData;
    try writeMappedInvertedSection(alloc, sink, sections, doc_counts, maps, doc_count, config);
}

fn writeMappedInvertedSection(
    alloc: Allocator,
    sink: anytype,
    sections: anytype,
    doc_counts: []const u32,
    doc_maps: anytype,
    merged_doc_count: u32,
    config: IndexConfig,
) !void {
    if (sections.len != doc_counts.len or sections.len != doc_maps.len) return error.InvalidData;

    const initial_runs = std.meta.Elem(@TypeOf(doc_maps)) == AffineDocMap;
    const append_maps = std.meta.Elem(@TypeOf(doc_maps)) == RankDocMap;
    const file_maps = std.meta.Elem(@TypeOf(doc_maps)) == FileDocMap;
    const stream_norms = initial_runs or append_maps or file_maps;
    const effective_maps = if (initial_runs) try alloc.dupe(AffineDocMap, doc_maps) else doc_maps;
    defer if (initial_runs) alloc.free(effective_maps);
    var readers = try alloc.alloc(ScopedInvertedIndexReader, sections.len);
    defer alloc.free(readers);
    var reader_present = try alloc.alloc(bool, sections.len);
    @memset(reader_present, false);
    defer {
        for (readers, reader_present) |*reader, present| if (present) reader.deinit();
        alloc.free(reader_present);
    }
    for (sections, 0..) |section_opt, i| {
        if (mapLength(doc_maps[i]) != doc_counts[i]) return error.InvalidData;
        reader_present[i] = false;
        const section = if (std.meta.Elem(@TypeOf(sections)) == ?@import("../segment_source.zig").View)
            section_opt orelse continue
        else blk: {
            const bytes = section_opt orelse continue;
            break :blk try @import("../segment_source.zig").View.init(.{ .contiguous = bytes }, 0, bytes.len);
        };
        var backing = section;
        if (initial_runs) {
            if (doc_maps[i].ids) |ids| {
                const same_source = switch (section.source) {
                    .contiguous => false,
                    .ranges => |ranges| switch (ids.source) {
                        .contiguous => false,
                        .ranges => |other| ranges.ptr == other.ptr and ranges.read_into == other.read_into,
                    },
                };
                if (same_source and ids.offset == section.offset + section.length)
                    backing = try @import("../segment_source.zig").View.init(section.source, section.offset, section.length + ids.length);
            }
        }
        var reader = switch (section.source) {
            .contiguous => |bytes| try ScopedInvertedIndexReader.initContiguous(alloc, bytes[@intCast(section.offset)..][0..@intCast(section.length)]),
            .ranges => try ScopedInvertedIndexReader.initRangesWithBacking(alloc, section, backing, .{}),
        };
        errdefer reader.deinit();
        if (initial_runs and backing.length != section.length) effective_maps[i].ids = try reader.backingView(section.length, backing.length - section.length);
        if (reader.doc_count > doc_counts[i]) return error.InvalidData;
        readers[i] = reader;
        reader_present[i] = true;
    }

    var term_iters = try alloc.alloc(ScopedInvertedIndexReader.Iterator, sections.len);
    const term_iter_present = alloc.alloc(bool, sections.len) catch |err| {
        alloc.free(term_iters);
        return err;
    };
    @memset(term_iter_present, false);
    defer {
        for (term_iters, 0..) |*iter, i| {
            if (term_iter_present[i]) iter.deinit();
        }
        alloc.free(term_iter_present);
        alloc.free(term_iters);
    }

    var current_entries = try alloc.alloc(?TermIterator.Entry, sections.len);
    defer alloc.free(current_entries);
    @memset(current_entries, null);

    for (readers, 0..) |*reader, seg_idx| {
        if (!reader_present[seg_idx]) continue;
        term_iters[seg_idx] = try reader.termIterator();
        term_iter_present[seg_idx] = true;
        current_entries[seg_idx] = try term_iters[seg_idx].next();
    }

    const section_start = sink.len();
    var header_placeholder: [v7_header_size]u8 = undefined;
    @memset(&header_placeholder, 0);
    try sink.appendSlice(&header_placeholder);
    var term_postings = std.ArrayListUnmanaged(u8).empty;
    defer term_postings.deinit(alloc);

    var total_field_len: u64 = 0;
    const merged_norms = if (stream_norms) StreamedNorms{ .len = merged_doc_count } else try alloc.alloc(u32, merged_doc_count);
    defer if (!stream_norms) alloc.free(merged_norms);
    if (!stream_norms) @memset(merged_norms, 0);
    var serialize_scratch = PostingSerializeScratch{};
    defer serialize_scratch.deinit(alloc);
    var dict_builder = try StreamingTermDictionaryBuilder.init(alloc);
    defer dict_builder.deinit();
    var merged_term = std.ArrayListUnmanaged(u8).empty;
    defer merged_term.deinit(alloc);

    const stream_maps = effective_maps;
    const stream_postings = config.wireVersion() == wire_version_current and config.postings_layout == .posting_count_v35 and monotonicDocMaps(stream_maps);
    while (true) {
        const min_term = findMinCurrentTerm(current_entries) orelse break;
        merged_term.clearRetainingCapacity();
        try merged_term.appendSlice(alloc, min_term);

        var candidate_frequency: u64 = 0;
        for (current_entries) |entry_opt| {
            const entry = entry_opt orelse continue;
            if (std.mem.eql(u8, entry.term, merged_term.items)) candidate_frequency += entry.result.docFreq();
        }
        if (stream_postings and candidate_frequency >= 4096) {
            const value = try appendStreamedMergedTermToSink(alloc, sink, section_start, current_entries, merged_term.items, stream_maps, merged_norms, &total_field_len, config);
            for (current_entries, 0..) |entry_opt, i| {
                const entry = entry_opt orelse continue;
                if (std.mem.eql(u8, entry.term, merged_term.items)) current_entries[i] = try term_iters[i].next();
            }
            if (value) |dict_value| try dict_builder.add(merged_term.items, dict_value);
            continue;
        }

        if (file_maps and !stream_postings and candidate_frequency >= 4096 and config.wireVersion() == wire_version_current and config.postings_layout == .posting_count_v35) {
            var stream = try ExternalPostingStream.init(alloc, current_entries, merged_term.items, effective_maps);
            defer stream.deinit();
            const value = try appendPostingStreamToSink(alloc, sink, section_start, &stream, stream.has_positions, merged_norms, &total_field_len, config);
            for (current_entries, 0..) |entry_opt, i| {
                const entry = entry_opt orelse continue;
                if (std.mem.eql(u8, entry.term, merged_term.items)) current_entries[i] = try term_iters[i].next();
            }
            if (value) |dict_value| try dict_builder.add(merged_term.items, dict_value);
            continue;
        }

        {
            var acc = PostingAccumulator.init();
            defer acc.deinit(alloc);

            // Compact terms stay in memory. Cross over before appending a hit
            // that would exceed the byte limit, including position payloads.
            // Restarting reads only a bounded prefix and releases it before
            // opening the external sorter. Norm maxima are idempotent; restore
            // the frequency sum so the restarted stream counts each hit once.
            const initial_field_len = total_field_len;
            var spill = false;
            const bounded = config.wireVersion() == wire_version_current and config.postings_layout == .posting_count_v35 and (stream_postings or file_maps);
            for (current_entries, 0..) |entry_opt, seg_idx| {
                const entry = entry_opt orelse continue;
                if (!std.mem.eql(u8, entry.term, merged_term.items)) continue;
                if (!try appendLookupResultToAccumulatorLimited(alloc, &acc, entry.result, effective_maps[seg_idx], merged_norms, &total_field_len, if (bounded) 256 * 1024 else null)) {
                    spill = true;
                    break;
                }
            }
            if (spill) {
                acc.deinit(alloc);
                acc = PostingAccumulator.init();
                total_field_len = initial_field_len;
                const value = if (stream_postings)
                    try appendStreamedMergedTermToSink(alloc, sink, section_start, current_entries, merged_term.items, stream_maps, merged_norms, &total_field_len, config)
                else if (file_maps) blk: {
                    var stream = try ExternalPostingStream.init(alloc, current_entries, merged_term.items, effective_maps);
                    defer stream.deinit();
                    break :blk try appendPostingStreamToSink(alloc, sink, section_start, &stream, stream.has_positions, merged_norms, &total_field_len, config);
                } else return error.InvalidData;
                for (current_entries, 0..) |entry_opt, i| {
                    const entry = entry_opt orelse continue;
                    if (std.mem.eql(u8, entry.term, merged_term.items)) current_entries[i] = try term_iters[i].next();
                }
                if (value) |dict_value| try dict_builder.add(merged_term.items, dict_value);
                continue;
            }
            for (current_entries, 0..) |entry_opt, i| {
                const entry = entry_opt orelse continue;
                if (std.mem.eql(u8, entry.term, merged_term.items)) current_entries[i] = try term_iters[i].next();
            }

            if (acc.doc_ids.items.len == 0) continue;
            try sortPostingAccumulatorByDocId(alloc, &acc);
            const dict_value = try appendMergedTermToSink(alloc, sink, section_start, &term_postings, &serialize_scratch, &acc, config, merged_doc_count);
            try dict_builder.add(merged_term.items, dict_value);
        }
    }

    if (file_maps) {
        const norms_data = FileNormStream{ .len = @as(usize, merged_doc_count) + 5, .readers = readers, .present = reader_present, .records = effective_maps[0].records, .maps = effective_maps };
        try finishStreamingMergedSectionToSink(alloc, sink, section_start, merged_doc_count, total_field_len, norms_data, &dict_builder, config);
    } else if (stream_norms) {
        const norms_data = MappedNormStream(std.meta.Elem(@TypeOf(doc_maps))){ .len = @as(usize, merged_doc_count) + 5, .readers = readers, .present = reader_present, .maps = effective_maps };
        try finishStreamingMergedSectionToSink(alloc, sink, section_start, merged_doc_count, total_field_len, norms_data, &dict_builder, config);
    } else {
        const norms_data = try encodeNormTable(alloc, merged_norms);
        defer alloc.free(norms_data);
        try finishStreamingMergedSectionToSink(alloc, sink, section_start, merged_doc_count, total_field_len, norms_data, &dict_builder, config);
    }
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
    var ordered = true;
    for (acc.doc_ids.items[1..], acc.doc_ids.items[0 .. acc.doc_ids.items.len - 1]) |next, previous| {
        if (next < previous) {
            ordered = false;
            break;
        }
    }
    if (ordered) return;

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
    fst_builder: *fst.Builder,
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
    rmap: anytype,
    doc_norms: anytype,
    total_field_len: *u64,
) !void {
    _ = try appendLookupResultToAccumulatorLimited(alloc, acc, result, rmap, doc_norms, total_field_len, null);
}

fn appendLookupResultToAccumulatorLimited(
    alloc: Allocator,
    acc: *PostingAccumulator,
    result: LookupResult,
    rmap: anytype,
    doc_norms: anytype,
    total_field_len: *u64,
    byte_limit: ?usize,
) !bool {
    switch (result) {
        .one_hit => |hit| {
            if (hit.doc_num >= mapLength(rmap)) return true;
            const remapped_doc = try mapDocument(rmap, hit.doc_num);
            if (remapped_doc == std.math.maxInt(u32)) return true;
            if (postingAccumulatorWouldSpill(acc, 0, byte_limit)) return false;
            try updateMergedNorm(doc_norms, remapped_doc, hit.norm_bits);
            try acc.add(alloc, remapped_doc, 1, hit.norm_bits, &.{});
            total_field_len.* += 1;
        },
        .postings => {
            var result_copy = result;
            var post_iter = try result_copy.iterator(alloc);
            defer post_iter.deinit();

            while (try post_iter.next()) |hit| {
                if (hit.doc_id >= mapLength(rmap)) continue;
                const remapped_doc = try mapDocument(rmap, hit.doc_id);
                if (remapped_doc == std.math.maxInt(u32)) continue;
                if (postingAccumulatorWouldSpill(acc, hit.positions.len, byte_limit)) return false;
                try updateMergedNorm(doc_norms, remapped_doc, hit.norm);
                try acc.add(alloc, remapped_doc, hit.freq, hit.norm, hit.positions);
                total_field_len.* += hit.freq;
            }
        },
    }
    return true;
}

fn postingAccumulatorWouldSpill(acc: *const PostingAccumulator, positions: usize, byte_limit: ?usize) bool {
    const limit = byte_limit orelse return false;
    // Include the reorder copy and descriptors, not just compressed wire bytes.
    const bytes = (acc.doc_ids.items.len +| 1) *| (@sizeOf(u32) + @sizeOf(PostingMeta) + @sizeOf(PostingSortEntry)) +|
        (acc.all_positions.items.len +| positions) *| (2 * @sizeOf(u32));
    return bytes > limit;
}

/// Historical nonmonotonic maps reorder bounded posting records on disk.
/// Position lists are copied once into private runs, never accumulated for an
/// entire high-frequency term or reread from its source during sort carries.
const ExternalPostingStream = struct {
    const Spill = @import("../spill_sort.zig");
    allocator: Allocator,
    sorter: Spill.Sorter,
    cursor: ?Spill.Cursor = null,
    positions: std.ArrayListUnmanaged(u32) = .empty,
    read_cache: ?*PackedReadCache = null,
    has_positions: bool = false,
    previous: ?u32 = null,
    fn init(alloc: Allocator, entries: []const ?TermIterator.Entry, term: []const u8, maps: []const FileDocMap) !@This() {
        if (maps.len == 0) return error.InvalidData;
        var self = @This(){ .allocator = alloc, .sorter = try Spill.Sorter.init(alloc, maps[0].scratch orelse return error.InvalidData) };
        errdefer self.deinit();
        self.read_cache = try alloc.create(PackedReadCache);
        self.read_cache.?.* = .{};
        var encoded = std.ArrayListUnmanaged(u8).empty;
        defer encoded.deinit(alloc);
        for (entries, maps) |entry_opt, map| {
            const entry = entry_opt orelse continue;
            if (!std.mem.eql(u8, entry.term, term)) continue;
            var result = entry.result;
            var iterator = try result.iterator(alloc);
            defer iterator.deinit();
            while (try nextPackedHit(&iterator, self.read_cache.?)) |packed_hit| {
                const hit = packed_hit.hit;
                if (hit.doc_id >= map.len) return error.InvalidData;
                const doc = try mapDocument(map, hit.doc_id);
                if (doc == std.math.maxInt(u32)) continue;
                encoded.clearRetainingCapacity();
                var header: [13]u8 = undefined;
                std.mem.writeInt(u32, header[0..4], hit.freq, .little);
                std.mem.writeInt(u32, header[4..8], hit.norm, .little);
                std.mem.writeInt(u32, header[8..12], @intCast(packed_hit.positions.count), .little);
                const encoded_width = try exactPositionWidth(packed_hit.positions, alloc);
                header[12] = encoded_width;
                try encoded.appendSlice(alloc, &header);
                const BufferSink = struct {
                    allocator: Allocator,
                    buffer: *std.ArrayListUnmanaged(u8),
                    fn appendSlice(buffer_sink: *@This(), bytes: []const u8) !void {
                        try buffer_sink.buffer.appendSlice(buffer_sink.allocator, bytes);
                    }
                };
                var buffer_sink = BufferSink{ .allocator = alloc, .buffer = &encoded };
                var output = PositionByteWriter(*BufferSink){ .sink = &buffer_sink };
                if (encoded_width != 0) {
                    if (packed_hit.positions.bits == encoded_width and packed_hit.positions.unpacked == null) {
                        try output.appendPacked(packed_hit.positions);
                    } else {
                        var cursor = try packed_hit.positions.cursorAlloc(alloc);
                        defer cursor.deinit();
                        var previous: u32 = 0;
                        while (try cursor.next()) |position| {
                            try output.value(if (position >= previous) position - previous else 0, encoded_width);
                            previous = position;
                        }
                    }
                    try output.alignByte();
                }
                try output.flush();
                self.has_positions = self.has_positions or packed_hit.positions.count != 0;
                try self.sorter.add(doc, encoded.items);
            }
        }
        if (try self.sorter.finish()) |range| self.cursor = Spill.Cursor.init(alloc, self.sorter.run, range);
        return self;
    }
    fn deinit(self: *@This()) void {
        if (self.cursor) |*cursor| cursor.deinit();
        self.positions.deinit(self.allocator);
        if (self.read_cache) |cache| self.allocator.destroy(cache);
        self.sorter.deinit();
    }
    pub fn packedBlock(self: *@This(), acc: *PostingAccumulator, views: *std.ArrayListUnmanaged(PackedPositionView), count: u32) !bool {
        acc.doc_ids.clearRetainingCapacity();
        acc.metas.clearRetainingCapacity();
        acc.all_positions.clearRetainingCapacity();
        views.clearRetainingCapacity();
        if (self.cursor == null) return false;
        while (acc.doc_ids.items.len < count) {
            const record = (try self.cursor.?.nextView()) orelse break;
            if (record.key >= std.math.maxInt(u32) or record.payload.length < 13) return error.InvalidData;
            const doc: u32 = @intCast(record.key);
            if (self.previous) |previous| if (previous >= doc) return error.InvalidData;
            self.previous = doc;
            var header: [13]u8 = undefined;
            try record.payload.readInto(0, &header);
            const frequency = std.mem.readInt(u32, header[0..4], .little);
            const norm = std.mem.readInt(u32, header[4..8], .little);
            const length = std.mem.readInt(u32, header[8..12], .little);
            if (header[12] > 32 or packedU32ByteLen(length, header[12]) != record.payload.length - 13) return error.InvalidData;
            try acc.doc_ids.append(self.allocator, doc);
            try acc.metas.append(self.allocator, .{ .freq = frequency, .norm = norm, .position_count = length });
            try views.append(self.allocator, .{ .range = try @import("../segment_source.zig").View.init(record.payload.source, record.payload.offset + 13, record.payload.length - 13), .count = length, .bits = header[12], .encoded_width = header[12], .read_cache = self.read_cache });
        }
        return acc.doc_ids.items.len != 0;
    }
    fn block(self: *@This(), acc: *PostingAccumulator, count: u32) !bool {
        var views = std.ArrayListUnmanaged(PackedPositionView).empty;
        defer views.deinit(self.allocator);
        if (!try self.packedBlock(acc, &views, count)) return false;
        for (views.items) |view| {
            var cursor = try view.cursorAlloc(self.allocator);
            defer cursor.deinit();
            while (try cursor.next()) |position| try acc.all_positions.append(self.allocator, position);
        }
        return true;
    }
};

/// A heap contains one borrowed hit per source iterator. Each hit is copied
/// into the output block before advancing its source, so positions never escape
/// the iterator's bounded working buffer.
fn MergedPostingStream(comptime Maps: type) type {
    return struct {
        const Head = struct { source: usize, doc_id: u32, frequency: u32, norm: u32 };
        const Heap = std.PriorityQueue(Head, void, struct {
            fn compare(_: void, a: Head, b: Head) std.math.Order {
                return std.math.order(a.doc_id, b.doc_id);
            }
        }.compare);
        allocator: Allocator,
        iterators: []?PostingsIterator,
        maps: Maps,
        head_views: []PackedPositionView,
        decoded_heads: [][]const u32,
        dense_ready: bool = false,
        read_cache: *PackedReadCache,
        heap: Heap = .empty,

        fn init(alloc: Allocator, entries: []const ?TermIterator.Entry, term: []const u8, maps: Maps) !@This() {
            const iterators = try alloc.alloc(?PostingsIterator, entries.len);
            errdefer alloc.free(iterators);
            const head_views = try alloc.alloc(PackedPositionView, entries.len);
            errdefer alloc.free(head_views);
            const decoded_heads = try alloc.alloc([]const u32, entries.len);
            errdefer alloc.free(decoded_heads);
            const cache = try alloc.create(PackedReadCache);
            errdefer alloc.destroy(cache);
            cache.* = .{};
            @memset(iterators, null);
            var self = @This(){ .allocator = alloc, .iterators = iterators, .maps = maps, .read_cache = cache, .head_views = head_views, .decoded_heads = decoded_heads };
            errdefer {
                for (self.iterators) |*iterator| if (iterator.*) |*present| present.deinit();
                self.heap.deinit(alloc);
            }
            for (entries, 0..) |entry_opt, i| {
                const entry = entry_opt orelse continue;
                if (!std.mem.eql(u8, entry.term, term)) continue;
                var result = entry.result;
                self.iterators[i] = try result.iterator(alloc);
                try self.advance(i);
            }
            return self;
        }
        fn deinit(self: *@This()) void {
            for (self.iterators) |*iterator| if (iterator.*) |*present| present.deinit();
            self.allocator.free(self.iterators);
            self.allocator.free(self.head_views);
            self.allocator.free(self.decoded_heads);
            self.allocator.destroy(self.read_cache);
            self.heap.deinit(self.allocator);
        }
        fn nextSourceHit(self: *@This(), source: usize) !?PostingsIterator.Hit {
            const iterator = &self.iterators[source].?;
            if (!iterator.is_one_hit and usesContiguousPositionGroups(iterator.version) and (iterator.positions_range != null or iterator.positions_data != null)) {
                if (iterator.current_chunk_index == std.math.maxInt(usize) or iterator.chunk_doc_pos >= iterator.doc_values.items.len) {
                    if (iterator.next_chunk_index >= iterator.chunkCount()) return null;
                    const index = iterator.next_chunk_index;
                    try iterator.loadChunk(index);
                    try iterator.enterPositionChunk(index);
                }
                const decoded = decodeFreqHasLocs(iterator.freq_values.items[iterator.chunk_doc_pos]);
                if (!decoded.has_locs or decoded.freq <= 32) {
                    const hit = try iterator.takeCurrentWithPositions();
                    // No packed descriptor copy on the decoded fast path.
                    // The iterator retains group coordinates until this head
                    // is emitted, so a crossover can materialize its view.
                    self.head_views[source].count = hit.positions.len;
                    self.head_views[source].unpacked = hit.positions;
                    self.decoded_heads[source] = hit.positions;
                    return hit;
                }
            }
            const packed_hit = (try nextPackedHit(iterator, self.read_cache)) orelse return null;
            self.head_views[source] = packed_hit.positions;
            self.decoded_heads[source] = packed_hit.hit.positions;
            return packed_hit.hit;
        }
        fn advance(self: *@This(), source: usize) !void {
            while (try self.nextSourceHit(source)) |hit| {
                if (hit.doc_id >= mapLength(self.maps[source])) return error.InvalidData;
                const mapped = try mapDocument(self.maps[source], hit.doc_id);
                if (mapped == std.math.maxInt(u32)) continue;
                try self.heap.push(self.allocator, .{ .source = source, .doc_id = mapped, .frequency = hit.freq, .norm = hit.norm });
                return;
            }
        }
        fn packedHeadView(self: *@This(), source: usize) !PackedPositionView {
            const head = self.head_views[source];
            if (head.unpacked == null) return head;
            const iterator = &self.iterators[source].?;
            if (head.count > iterator.positions_group_value_offset) return error.InvalidData;
            return .{
                .data = if (iterator.positions_data) |data| data[iterator.positions_group_data_start..iterator.positions_chunk_end] else &.{},
                .range = if (iterator.positions_range) |range| try @import("../segment_source.zig").View.init(range.source, range.offset + iterator.positions_group_data_start, iterator.positions_chunk_end - iterator.positions_group_data_start) else null,
                .start_index = iterator.positions_group_value_offset - head.count,
                .count = head.count,
                .bits = iterator.positions_group_bits,
                .read_cache = self.read_cache,
            };
        }
        pub fn packedBlock(self: *@This(), acc: *PostingAccumulator, views: *std.ArrayListUnmanaged(PackedPositionView), count: u32) !bool {
            acc.doc_ids.clearRetainingCapacity();
            acc.metas.clearRetainingCapacity();
            acc.all_positions.clearRetainingCapacity();
            views.clearRetainingCapacity();
            self.dense_ready = true;
            while (acc.doc_ids.items.len < count) {
                const head = self.heap.pop() orelse break;
                if (acc.doc_ids.items.len > 0 and acc.doc_ids.items[acc.doc_ids.items.len - 1] >= head.doc_id) return error.InvalidData;
                try acc.doc_ids.append(self.allocator, head.doc_id);
                try acc.metas.append(self.allocator, .{ .freq = head.frequency, .norm = head.norm, .position_count = @intCast(self.head_views[head.source].count) });
                const positions = self.decoded_heads[head.source];
                if (self.dense_ready and positions.len == self.head_views[head.source].count and acc.all_positions.items.len + positions.len <= 32 * 1024) {
                    try acc.all_positions.appendSlice(self.allocator, positions);
                } else {
                    if (self.dense_ready) {
                        // The bounded decoded prefix stays immutable after the
                        // crossover. Materialize its references only now.
                        var offset: usize = 0;
                        for (acc.metas.items[0 .. acc.metas.items.len - 1]) |meta| {
                            const prefix = acc.all_positions.items[offset..][0..meta.position_count];
                            try views.append(self.allocator, .{ .unpacked = prefix, .count = prefix.len, .bits = positionBits(prefix) });
                            offset += prefix.len;
                        }
                        self.dense_ready = false;
                    }
                    try views.append(self.allocator, try self.packedHeadView(head.source));
                }
                try self.advance(head.source);
            }
            return acc.doc_ids.items.len > 0;
        }
        pub fn densePositionsReady(self: *@This()) bool {
            return self.dense_ready;
        }
    };
}

fn monotonicDocMaps(maps: anytype) bool {
    if (std.meta.Elem(@TypeOf(maps)) == FileDocMap) {
        for (maps) |map| if (!map.monotonic) return false;
        return true;
    }
    if (std.meta.Elem(@TypeOf(maps)) == AffineDocMap or std.meta.Elem(@TypeOf(maps)) == RankDocMap) return true;
    for (maps) |map| {
        var previous: ?u32 = null;
        for (map) |id| {
            if (id == std.math.maxInt(u32)) continue;
            if (previous) |last| if (id <= last) return false;
            previous = id;
        }
    }
    return true;
}

/// Coalesce random writes to the pre-sized term ranges. Native publication
/// must issue a handful of writes per range, not one syscall per posting block.
/// Emit v39 blocks once, followed by a paged block directory. The dictionary
/// points to a trailer marked (0, maxInt(u32)), followed by a 64-byte descriptor.
/// Each 32-byte directory record holds max doc/count (u32), payload offset/length
/// (u64/u32), and position offset/length (u64/u32), all LE. Offsets in records are
/// relative to the interleaved block span; descriptor offsets are section-local.
/// Only directory/impact navigation scales with term frequency; document and
/// position scratch streams large blocks and caps dense blocks at 128 KiB. Arbitrary non-monotonic
/// remappings continue through the sorting fallback.
fn appendStreamedMergedTermToSink(
    alloc: Allocator,
    sink: anytype,
    section_start: usize,
    entries: []const ?TermIterator.Entry,
    term: []const u8,
    maps: anytype,
    norms: anytype,
    total_field_len: *u64,
    config: IndexConfig,
) !?u64 {
    var has_positions = false;
    for (entries) |maybe| if (maybe) |entry| {
        if (!std.mem.eql(u8, entry.term, term)) continue;
        switch (entry.result) {
            .one_hit => {},
            .postings => |postings| {
                has_positions = has_positions or postings.positionsLength() > 0 or postings.inline_has_locs;
            },
        }
    };
    const T = @TypeOf(maps);
    const normalized_maps: if (T == []const AffineDocMap or T == []AffineDocMap) []const AffineDocMap else if (T == []const RankDocMap or T == []RankDocMap) []const RankDocMap else if (T == []const FileDocMap or T == []FileDocMap) []const FileDocMap else []const []const u32 = maps;
    var stream = try MergedPostingStream(@TypeOf(normalized_maps)).init(alloc, entries, term, normalized_maps);
    defer stream.deinit();
    return appendPostingStreamToSink(alloc, sink, section_start, &stream, has_positions, norms, total_field_len, config);
}

/// Common block encoder for in-memory initial runs and merged source cursors.
const AccumulatorStream = struct {
    source: *const PostingAccumulator,
    alloc: Allocator,
    doc: usize = 0,
    position: usize = 0,

    pub fn packedBlock(self: *@This(), out: *PostingAccumulator, views: *std.ArrayListUnmanaged(PackedPositionView), count: u32) !bool {
        out.doc_ids.clearRetainingCapacity();
        out.metas.clearRetainingCapacity();
        out.all_positions.clearRetainingCapacity();
        views.clearRetainingCapacity();
        const end = @min(self.source.doc_ids.items.len, self.doc + count);
        if (self.doc == end) return false;
        try out.doc_ids.appendSlice(self.alloc, self.source.doc_ids.items[self.doc..end]);
        try out.metas.appendSlice(self.alloc, self.source.metas.items[self.doc..end]);
        for (self.source.metas.items[self.doc..end]) |meta| {
            const positions = self.source.all_positions.items[self.position..][0..meta.position_count];
            try views.append(self.alloc, .{ .unpacked = positions, .count = positions.len, .bits = positionBits(positions) });
            self.position += positions.len;
        }
        self.doc = end;
        return true;
    }
    fn block(self: *@This(), out: *PostingAccumulator, count: u32) !bool {
        if (self.doc == self.source.doc_ids.items.len) return false;
        out.doc_ids.clearRetainingCapacity();
        out.metas.clearRetainingCapacity();
        out.all_positions.clearRetainingCapacity();
        const end = @min(self.source.doc_ids.items.len, self.doc + count);
        try out.doc_ids.appendSlice(self.alloc, self.source.doc_ids.items[self.doc..end]);
        try out.metas.appendSlice(self.alloc, self.source.metas.items[self.doc..end]);
        var positions: usize = 0;
        for (self.source.metas.items[self.doc..end]) |meta| positions += meta.position_count;
        try out.all_positions.appendSlice(self.alloc, self.source.all_positions.items[self.position..][0..positions]);
        self.doc = end;
        self.position += positions;
        return true;
    }
};

fn appendPostingStreamToSink(
    alloc: Allocator,
    sink: anytype,
    section_start: usize,
    stream: anytype,
    has_positions: bool,
    norms: anytype,
    total_field_len: *u64,
    config: IndexConfig,
) !?u64 {
    var acc = PostingAccumulator.init();
    defer acc.deinit(alloc);
    var position_bytes = std.ArrayListUnmanaged(u8).empty;
    defer position_bytes.deinit(alloc);
    var crossover_positions = std.ArrayListUnmanaged(u32).empty;
    defer crossover_positions.deinit(alloc);
    var position_views = std.ArrayListUnmanaged(PackedPositionView).empty;
    defer position_views.deinit(alloc);
    const packed_stream = @hasDecl(@TypeOf(stream.*), "packedBlock");
    var block_scratch = PostingSerializeScratch{};
    defer block_scratch.deinit(alloc);
    var navigation = PostingSerializeScratch{};
    defer navigation.deinit(alloc);
    var payload_length: u64 = 0;
    var positions_length: u64 = 0;
    const span_start = sink.len();
    var records = std.ArrayListUnmanaged(u8).empty;
    defer records.deinit(alloc);
    var doc_freq: u32 = 0;
    var previous_doc: ?u32 = null;
    var current_impact: ?u32 = null;
    var impact_max: u16 = 0;
    var impact_min: u16 = std.math.maxInt(u16);
    if (config.chunk_size == 0) return error.InvalidData;
    while (if (comptime packed_stream) try stream.packedBlock(&acc, &position_views, config.chunk_size) else try stream.block(&acc, config.chunk_size)) {
        if (previous_doc) |last| if (last >= acc.doc_ids.items[0]) return error.InvalidData;
        previous_doc = acc.doc_ids.items[acc.doc_ids.items.len - 1];
        for (acc.doc_ids.items, acc.metas.items) |id, meta| {
            try updateMergedNorm(norms, id, meta.norm);
            total_field_len.* = try std.math.add(u64, total_field_len.*, meta.freq);
            const range = id / impact_range_doc_count;
            if (current_impact != null and current_impact.? != range) {
                try navigation.impact_chunk_ids.append(alloc, current_impact.?);
                try appendImpactRecord(alloc, &navigation.impact_block_max, impact_max, impact_min);
                impact_max = 0;
                impact_min = std.math.maxInt(u16);
            }
            current_impact = range;
            const freq: u16 = @intCast(@min(meta.freq, std.math.maxInt(u16)));
            const norm: u16 = @intCast(@min(meta.norm, std.math.maxInt(u16)));
            impact_max = @max(impact_max, freq);
            impact_min = @min(impact_min, norm);
        }
        doc_freq = try std.math.add(u32, doc_freq, @intCast(acc.doc_ids.items.len));
        block_scratch.reset();
        var small_position_block = false;
        if (packed_stream and has_positions) {
            const dense_ready = if (comptime @hasDecl(@TypeOf(stream.*), "densePositionsReady")) stream.densePositionsReady() else false;
            var count: u64 = acc.all_positions.items.len;
            if (!dense_ready) {
                count = 0;
                for (position_views.items) |view| count += view.count;
            }
            small_position_block = count <= 32 * 1024;
            if (small_position_block and !dense_ready) {
                if (acc.all_positions.items.len == 0) {
                    try acc.all_positions.ensureTotalCapacityPrecise(alloc, @intCast(count));
                    for (position_views.items) |view| {
                        const begin = acc.all_positions.items.len;
                        acc.all_positions.items.len += view.count;
                        try view.decodeInto(alloc, acc.all_positions.items[begin..], &position_bytes);
                    }
                } else {
                    // A crossover prefix borrows acc.all_positions. Decode
                    // into bounded storage before replacing that array.
                    crossover_positions.clearRetainingCapacity();
                    try crossover_positions.ensureTotalCapacityPrecise(alloc, @intCast(count));
                    for (position_views.items) |view| {
                        const begin = crossover_positions.items.len;
                        crossover_positions.items.len += view.count;
                        try view.decodeInto(alloc, crossover_positions.items[begin..], &position_bytes);
                    }
                    std.mem.swap(std.ArrayListUnmanaged(u32), &acc.all_positions, &crossover_positions);
                }
            } else if (!small_position_block) {
                for (position_views.items) |*view| if (view.encoded_width == null) {
                    view.encoded_width = try exactPositionWidth(view.*, alloc);
                };
            }
        }
        var position_offset: usize = 0;
        // Size all position frames, including empty-location groups. If the
        // entire term has no positions, the positions stream is omitted.
        const encoded = try acc.appendEncodedChunk(alloc, &block_scratch, config, 0, acc.doc_ids.items.len, 0, &position_offset, has_positions and (!packed_stream or small_position_block));
        var record: [streamed_record_size]u8 = undefined;
        std.mem.writeInt(u32, record[0..4], encoded.metadata.max_doc, .little);
        std.mem.writeInt(u32, record[4..8], encoded.metadata.doc_count, .little);
        std.mem.writeInt(u64, record[8..16], sink.len() - span_start, .little);
        std.mem.writeInt(u32, record[16..20], @intCast(block_scratch.payload.items.len), .little);
        try sink.appendSlice(block_scratch.payload.items);
        std.mem.writeInt(u64, record[20..28], sink.len() - span_start, .little);
        const position_length = if (packed_stream and has_positions and !small_position_block) try appendPackedPositionsToSink(alloc, sink, position_views.items) else blk: {
            try sink.appendSlice(block_scratch.positions.items);
            break :blk block_scratch.positions.items.len;
        };
        std.mem.writeInt(u32, record[28..32], @intCast(position_length), .little);
        try records.appendSlice(alloc, &record);
        payload_length = try std.math.add(u64, payload_length, @intCast(block_scratch.payload.items.len));
        // A positions-bearing term also frames blocks with no locations.
        positions_length = try std.math.add(u64, positions_length, @intCast(position_length));
    }
    if (doc_freq == 0) return null;
    if (doc_freq > config.chunk_size) {
        try navigation.impact_chunk_ids.append(alloc, current_impact.?);
        try appendImpactRecord(alloc, &navigation.impact_block_max, impact_max, impact_min);
    } else {
        navigation.impact_chunk_ids.clearRetainingCapacity();
        navigation.impact_block_max.clearRetainingCapacity();
    }
    const span_length: u64 = sink.len() - span_start;
    const table_start = sink.len();
    try sink.appendSlice(records.items);
    const impacts: u32 = @intCast(navigation.impact_block_max.items.len / blockMaxRecordSize(config.wireVersion()));
    try encodeImpactMetadata(alloc, &navigation, impacts, config.wireVersion());
    try encodeImpactChunkIds(alloc, &navigation.impact_ids, navigation.impact_chunk_ids.items, &navigation.doc_deltas);
    const impact_start = sink.len();
    try sink.appendSlice(navigation.impact_encoded.items);
    try sink.appendSlice(navigation.impact_ids.items);
    const header_start = sink.len();
    var marker = std.ArrayListUnmanaged(u8).empty;
    defer marker.deinit(alloc);
    try writeVarintU32(alloc, &marker, 0);
    try writeVarintU32(alloc, &marker, std.math.maxInt(u32));
    try sink.appendSlice(marker.items);
    const descriptor = StreamedDescriptor{ .doc_frequency = doc_freq, .chunks = @intCast(records.items.len / streamed_record_size), .span_start = span_start - section_start, .span_length = span_length, .table_start = table_start - section_start, .impact_start = impact_start - section_start, .impacts = impacts, .ids_length = @intCast(navigation.impact_ids.items.len), .payload_length = payload_length, .positions_length = positions_length };
    try sink.appendSlice(&descriptor.encode());
    return @intCast(header_start - section_start - v7_header_size);
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
    norms_data: anytype,
    dict_builder: *StreamingTermDictionaryBuilder,
    config: IndexConfig,
) !void {
    return finishStreamingSectionProfile(alloc, sink, section_start, doc_count, total_field_len, norms_data, dict_builder, config, null);
}

fn finishStreamingSectionProfile(
    alloc: Allocator,
    sink: anytype,
    section_start: usize,
    doc_count: u32,
    total_field_len: u64,
    norms_data: anytype,
    dict_builder: *StreamingTermDictionaryBuilder,
    config: IndexConfig,
    profile: ?*InvertedIndexBuildProfile,
) !void {
    const bloom_start = if (profile != null) platform_time.monotonicNs() else 0;
    const bloom_bytes = try dict_builder.encodeBloomAlloc(config);
    defer alloc.free(bloom_bytes);
    if (profile) |p| p.bloom_finish_ns +|= platform_time.monotonicNs() - bloom_start;

    const assembly_start = if (profile != null) platform_time.monotonicNs() else 0;
    if (@TypeOf(norms_data) == InitialNormStream or @TypeOf(norms_data) == AppendNormStream or @TypeOf(norms_data) == FileNormStream) try norms_data.write(sink) else try sink.appendSlice(norms_data);
    if (bloom_bytes.len > 0) try sink.appendSlice(bloom_bytes);
    const dict_start = if (profile != null) platform_time.monotonicNs() else 0;
    const term_dict_len = try dict_builder.finishIntoSink(sink);
    if (profile) |p| p.term_dict_ns +|= platform_time.monotonicNs() - dict_start;

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
    if (profile) |p| p.final_assembly_ns +|= platform_time.monotonicNs() - assembly_start;
}

// =====================================================================}

// Tests
// =====================================================================}

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

test "merged inverted section cleans up partially initialized term iterators" {
    const alloc = std.testing.allocator;

    var first_builder = InvertedIndexBuilder.init(alloc, .{});
    defer first_builder.deinit();
    try first_builder.addDocument(0, &.{.{ .term = "alpha", .freq = 1, .norm = 1 }});
    const first = try first_builder.build();
    defer alloc.free(first);

    var second_builder = InvertedIndexBuilder.init(alloc, .{});
    defer second_builder.deinit();
    try second_builder.addDocument(0, &.{.{ .term = "beta", .freq = 1, .norm = 1 }});
    const second = try second_builder.build();
    defer alloc.free(second);

    const Runner = struct {
        fn run(failing_alloc: Allocator, first_section: []const u8, second_section: []const u8) !void {
            const merged = try mergeInvertedSectionSlotsWithDeletes(
                failing_alloc,
                &.{ first_section, second_section },
                &.{ 1, 1 },
                null,
                .{},
            );
            defer failing_alloc.free(merged);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{ first, second });
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
    while (version < wire_version_compact_postings_header) : (version += 1) {
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

    var fst_builder = try fst.Builder.init(alloc, .{});
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
    section[5..9].* = @bitCast(@as(u32, @as(u32, 2)));
    section[9..17].* = @bitCast(@as(u64, @as(u64, 3)));
    section[17..21].* = @bitCast(@as(u32, @as(u32, 1024)));
    section[21..25].* = @bitCast(@as(u32, @as(u32, @intCast(fst_bytes.len))));
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
        expanded[dst + 2 ..][0..2].* = @bitCast(@as(u16, norm));
        expanded[dst + 4 ..][0..2].* = @bitCast(@as(u16, norm));
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
    const ranged = try RangeInvertedIndexReader.init(alloc, try @import("../segment_source.zig").View.init(.{ .contiguous = legacy }, 0, legacy.len), 4096);
    var scratch = @import("../segment_source.zig").Scratch.init(alloc, 4096);
    defer scratch.deinit();
    const address = (try ranged.lookupAddress("compat", &scratch)).?;
    var owned = try ranged.openPostings(address.postings_offset, 4096, 4096, 4096);
    defer owned.deinit();
    var range_iter = try owned.value.iterator(alloc);
    defer range_iter.deinit();
    try std.testing.expectEqualDeep(first, (try range_iter.next()).?);
    try std.testing.expectEqualDeep(second, (try range_iter.next()).?);
    try std.testing.expect((try range_iter.next()) == null);
}

fn lookupBlockedTerm(allocator: Allocator, blocks: []const u8, block_offset: u32, term: []const u8) !u64 {
    if (block_offset >= blocks.len) return error.InvalidData;
    var cursor: usize = block_offset;
    const prefix_len = try readVarintU32(blocks, &cursor);
    const entry_count = try readVarintU32(blocks, &cursor);
    if (cursor + prefix_len > blocks.len) return error.Truncated;
    const prefix = blocks[cursor..][0..prefix_len];
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
        const owned = try allocator.alloc(u8, wanted_suffix.len);
        heap_suffix = owned;
        break :blk owned;
    };
    defer if (heap_suffix) |owned| allocator.free(owned);

    var suffix_prefix_len: usize = 0;
    var previous_suffix_len: usize = 0;
    var last_postings_offset: u64 = 0;
    var remaining = entry_count;
    while (remaining > 0) : (remaining -= 1) {
        const shared_len: usize = try readVarintU32(blocks, &cursor);
        const leaf_len: usize = try readVarintU32(blocks, &cursor);
        if (shared_len > previous_suffix_len) return error.InvalidData;
        if (cursor + leaf_len > blocks.len) return error.Truncated;

        const retained_len = @min(shared_len, suffix_buf.len);
        if (retained_len > suffix_prefix_len) {
            // We retain min(actual length, requested length) bytes from the
            // previous suffix. Therefore a shared prefix can exceed the
            // retained bytes only after both have reached the requested
            // length, in which case retained_len == suffix_prefix_len.
            return error.InvalidData;
        }
        const copied_leaf_len = @min(leaf_len, suffix_buf.len - retained_len);
        @memcpy(suffix_buf[retained_len..][0..copied_leaf_len], blocks[cursor..][0..copied_leaf_len]);
        suffix_prefix_len = retained_len + copied_leaf_len;
        previous_suffix_len = shared_len +| leaf_len;
        cursor += leaf_len;
        const value = decodeTermDictBlockValueDelta(try readVarintU64(blocks, &cursor), &last_postings_offset);

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

/// Native section navigation. Header and dictionary descriptors are retained;
/// term ceilings and the selected dictionary block are read on demand. The
/// section view is borrowed, normally from a query-owned bounded block cache.
pub const RangeInvertedIndexReader = struct {
    allocator: Allocator,
    view: @import("../segment_source.zig").View,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    version: u8,
    block_count: u32,
    blocks_offset: u64,
    blocks_length: u32,
    index_offset: u64,
    index_length: u32,
    norms_offset: u64,
    norms_length: u32,
    norm_count: u32,
    norm_bits: u8,
    max_dictionary_block_bytes: usize,
    term_bloom: ?struct { bytes_offset: u64, bit_count: u32, hash_count: u8 } = null,

    pub fn init(allocator: Allocator, view: @import("../segment_source.zig").View, max_dictionary_block_bytes: usize) !RangeInvertedIndexReader {
        if (view.length < v7_header_size) return error.InvalidData;
        var header: [v7_header_size]u8 = undefined;
        try view.readInto(0, &header);
        if (!std.mem.eql(u8, header[0..4], "INVT")) return error.InvalidMagic;
        const version = header[4];
        if (version != wire_version_legacy and version != wire_version_compact_postings_header and version != wire_version_current) return error.UnsupportedVersion;
        const dict_length = std.mem.readInt(u32, header[21..25], .little);
        const bloom_length = std.mem.readInt(u32, header[25..29], .little);
        const norms_length = std.mem.readInt(u32, header[29..33], .little);
        if (dict_length < term_dict_header_size or dict_length > view.length - v7_header_size) return error.InvalidData;
        const dict_offset = view.length - dict_length;
        if (@as(u64, bloom_length) + norms_length > dict_offset - v7_header_size) return error.InvalidData;
        var dict: [term_dict_header_size]u8 = undefined;
        try view.readInto(dict_offset, &dict);
        if (!std.mem.eql(u8, dict[0..4], term_dict_magic)) return error.InvalidData;
        const count = std.mem.readInt(u32, dict[4..8], .little);
        const blocks_length = std.mem.readInt(u32, dict[8..12], .little);
        const index_length = std.mem.readInt(u32, dict[12..16], .little);
        const fst_length = std.mem.readInt(u32, dict[16..20], .little);
        if (@as(u64, count) * term_dict_index_record_size > index_length or
            term_dict_header_size + @as(u64, blocks_length) + index_length + fst_length != dict_length) return error.InvalidData;
        const chunk_size = std.mem.readInt(u32, header[17..21], .little);
        if (chunk_size == 0) return error.InvalidData;
        var term_bloom: ?struct { bytes_offset: u64, bit_count: u32, hash_count: u8 } = null;
        const bloom_header_length = bloom.magic.len + 13;
        if (bloom_length >= bloom_header_length) {
            var bloom_header: [bloom_header_length]u8 = undefined;
            try view.readInto(dict_offset - bloom_length, &bloom_header);
            const bit_count = std.mem.readInt(u32, bloom_header[bloom.magic.len + 4 ..][0..4], .little);
            const bytes_length = std.mem.readInt(u32, bloom_header[bloom.magic.len + 9 ..][0..4], .little);
            if (std.mem.eql(u8, bloom_header[0..bloom.magic.len], bloom.magic) and
                std.mem.readInt(u32, bloom_header[bloom.magic.len..][0..4], .little) == bloom.version and
                bytes_length == bloom_length - bloom_header_length and bytes_length == (@as(u64, bit_count) + 7) / 8)
            {
                term_bloom = .{ .bytes_offset = dict_offset - bloom_length + bloom_header_length, .bit_count = bit_count, .hash_count = bloom_header[bloom.magic.len + 8] };
            }
        }
        const norms_offset = dict_offset - bloom_length - norms_length;
        var norm_count = std.mem.readInt(u32, header[5..9], .little);
        var norm_bits: u8 = 0;
        if (norms_length != 0) {
            if (norms_length < 5) return error.InvalidData;
            var norm_header: [5]u8 = undefined;
            try view.readInto(norms_offset, &norm_header);
            norm_count = std.mem.readInt(u32, norm_header[0..4], .little);
            norm_bits = norm_header[4];
            const bytes = if (norm_bits == 255) @as(u64, norm_count) else if (norm_bits <= 32) (@as(u64, norm_count) * norm_bits + 7) / 8 else return error.InvalidData;
            if (bytes > norms_length - 5) return error.InvalidData;
        }
        return .{ .allocator = allocator, .view = view, .doc_count = std.mem.readInt(u32, header[5..9], .little), .total_field_len = std.mem.readInt(u64, header[9..17], .little), .chunk_size = chunk_size, .version = version, .block_count = count, .blocks_offset = dict_offset + term_dict_header_size, .blocks_length = blocks_length, .index_offset = dict_offset + term_dict_header_size + blocks_length, .index_length = index_length, .norms_offset = norms_offset, .norms_length = norms_length, .norm_count = norm_count, .norm_bits = norm_bits, .max_dictionary_block_bytes = max_dictionary_block_bytes, .term_bloom = if (term_bloom) |filter| .{ .bytes_offset = filter.bytes_offset, .bit_count = filter.bit_count, .hash_count = filter.hash_count } else null };
    }

    pub fn layoutStats(self: *const RangeInvertedIndexReader) !InvertedIndexReader.LayoutStats {
        var header: [v7_header_size]u8 = undefined;
        try self.view.readInto(0, &header);
        const dictionary_length = std.mem.readInt(u32, header[21..25], .little);
        const bloom_length = std.mem.readInt(u32, header[25..29], .little);
        var stats = InvertedIndexReader.LayoutStats{
            .header_bytes = v7_header_size,
            .term_dict_bytes = dictionary_length,
            .norm_bytes = self.norms_length,
            .bloom_bytes = bloom_length,
            .postings_bytes = self.norms_offset - v7_header_size,
            .term_block_bytes = self.blocks_length,
            .term_index_bytes = self.index_length,
            .fst_bytes = dictionary_length - term_dict_header_size - @as(u64, self.blocks_length) - self.index_length,
        };
        for (0..self.block_count) |block| {
            const offset = try self.blockOffset(@intCast(block));
            if (offset >= self.blocks_length) return error.InvalidData;
            var bytes: [10]u8 = undefined;
            const take = @min(bytes.len, self.blocks_length - offset);
            try self.view.readInto(self.blocks_offset + offset, bytes[0..take]);
            var cursor: usize = 0;
            _ = try readVarintU32(bytes[0..take], &cursor);
            stats.term_count += try readVarintU32(bytes[0..take], &cursor);
        }
        return stats;
    }

    pub fn docLength(self: *const RangeInvertedIndexReader, doc: u32) !u32 {
        if (doc >= self.norm_count or self.norms_length < 5) return 0;
        const count = self.norm_count;
        const bits = self.norm_bits;
        if (bits == 255) {
            if (@as(u64, count) + 5 > self.norms_length) return error.InvalidData;
            var norm: [1]u8 = undefined;
            try self.view.readInto(self.norms_offset + 5 + doc, &norm);
            return fieldNormFromId(norm[0]);
        }
        if (bits > 32 or (@as(u64, count) * bits + 7) / 8 > self.norms_length - 5) return error.InvalidData;
        if (bits == 0) return 0;
        const start_bit = @as(u64, doc) * bits;
        const first_byte = start_bit / 8;
        const skip: u6 = @intCast(start_bit % 8);
        const byte_count: usize = @intCast((skip + @as(u64, bits) + 7) / 8);
        var norm_bytes: [8]u8 = @splat(0);
        try self.view.readInto(self.norms_offset + 5 + first_byte, norm_bytes[0..byte_count]);
        const mask: u64 = (@as(u64, 1) << @as(u6, @intCast(bits))) - 1;
        return @intCast((std.mem.readInt(u64, &norm_bytes, .little) >> skip) & mask);
    }

    pub const TermAddress = union(enum) { one_hit: LookupResult.OneHit, postings_offset: u64 };

    pub fn lookupAddress(self: *const RangeInvertedIndexReader, term: []const u8, scratch: *@import("../segment_source.zig").Scratch) !?TermAddress {
        const value = (try self.lookupValue(term, scratch)) orelse return null;
        if (fstValIs1Hit(value)) {
            const decoded = fstValDecode1Hit(value);
            if (decoded.doc_num >= self.norm_count) return error.InvalidData;
            return .{ .one_hit = .{ .doc_num = @intCast(decoded.doc_num), .norm_bits = try self.docLength(@intCast(decoded.doc_num)) } };
        }
        return .{ .postings_offset = value };
    }

    /// Metadata owns its buffers until deinit; iterators borrow them. Payload
    /// and positions stay in native ranges and are read by bounded iterators.
    pub fn openPostings(self: *const RangeInvertedIndexReader, offset: u64, max_metadata_bytes: usize, max_payload_chunk_bytes: usize, max_position_record_bytes: usize) !OwnedRangePostings {
        return self.openPostingsWithNorms(offset, max_metadata_bytes, max_payload_chunk_bytes, max_position_record_bytes, null);
    }

    fn openPostingsWithNorms(self: *const RangeInvertedIndexReader, offset: u64, max_metadata_bytes: usize, max_payload_chunk_bytes: usize, max_position_record_bytes: usize, cached_norms: ?[]const u8) !OwnedRangePostings {
        const base = std.math.add(u64, v7_header_size, offset) catch return error.InvalidData;
        if (base >= self.norms_offset) return error.InvalidData;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        var input = RangePostingInput{ .view = self.view, .position = base, .end = self.norms_offset, .allocator = arena.allocator(), .budget = max_metadata_bytes };
        const norms = cached_norms orelse &.{};
        const doc_frequency = try input.varint();
        if (doc_frequency == 0) {
            if (!usesInlineSingleDocPostings(self.version)) return error.InvalidData;
            const doc = try input.varint();
            if (self.version >= wire_version_streaming_blocks and doc == std.math.maxInt(u32)) {
                var descriptor_bytes: [StreamedDescriptor.size]u8 = undefined;
                if (input.position > input.end or descriptor_bytes.len > input.end - input.position) return error.InvalidData;
                try self.view.readInto(input.position, &descriptor_bytes);
                const descriptor = StreamedDescriptor.decode(&descriptor_bytes);
                try descriptor.validate(self.view.length, self.doc_count, self.chunk_size, base);
                const View = @import("../segment_source.zig").View;
                const span = try View.init(self.view.source, self.view.offset + descriptor.span_start, descriptor.span_length);
                const table = try View.init(self.view.source, self.view.offset + descriptor.table_start, @as(u64, descriptor.chunks) * streamed_record_size);
                const impact_len = packedU32ByteLen(descriptor.impacts, 5) + descriptor.impacts;
                const impacts = try input.bytesAt(descriptor.impact_start, impact_len);
                const ids = try input.bytesAt(descriptor.impact_start + impact_len, descriptor.ids_length);
                const header_len: usize = @intCast(input.position + StreamedDescriptor.size - base);
                const serialized = try View.init(self.view.source, self.view.offset + descriptor.span_start, input.position + StreamedDescriptor.size - descriptor.span_start);
                return .{ .arena = arena, .value = .{ .doc_freq = descriptor.doc_frequency, .serialized_data = &.{}, .serialized_range = serialized, .header_len = header_len, .chunk_size = self.chunk_size, .version = self.version, .doc_range_aligned = descriptor.ids_length > 0, .block_max = if (descriptor.impacts == 0) null else .{ .meta = impacts, .chunk_size = impact_range_doc_count, .chunk_meta_data = ids, .chunk_meta_count = descriptor.impacts, .version = self.version, .range_ids = true, .packed_impact_frequency = true }, .chunk_meta_data = &.{}, .chunk_meta_count = descriptor.chunks, .streamed_records_range = table, .payload_data = &.{}, .payload_range = span, .max_payload_chunk_bytes = max_payload_chunk_bytes, .norms_data = norms, .norms_reader = if (cached_norms == null) self.* else null, .positions_range = if (descriptor.positions_length == 0) null else span, .max_position_record_bytes = max_position_record_bytes, .logical_payload_length = @intCast(descriptor.payload_length), .logical_positions_length = @intCast(descriptor.positions_length), .impact_chunk_ids_data = if (descriptor.ids_length == 0) null else ids, .impact_chunk_count = descriptor.impacts } };
            }
            if (doc >= self.norm_count) return error.InvalidData;
            const encoded_frequency = try input.varint();
            const decoded = decodeFreqHasLocs(encoded_frequency);
            const header_length: usize = @intCast(input.position - base);
            var bits: u8 = 0;
            var positions: []const u8 = &.{};
            if (decoded.has_locs) {
                bits = try input.byte();
                if (bits > 32) return error.InvalidData;
                positions = try input.bytes(packedU32ByteLen(@intCast(decoded.freq), bits));
            }
            const serialized = try @import("../segment_source.zig").View.init(self.view.source, self.view.offset + base, input.position - base);
            return .{ .arena = arena, .value = .{ .doc_freq = 1, .serialized_data = &.{}, .serialized_range = serialized, .header_len = header_length, .chunk_size = self.chunk_size, .version = self.version, .doc_range_aligned = false, .chunk_meta_data = &.{}, .chunk_meta_count = 0, .payload_data = &.{}, .norms_data = norms, .norms_reader = if (cached_norms == null) self.* else null, .inline_single_doc = true, .inline_doc_id = doc, .inline_freq = @intCast(decoded.freq), .inline_has_locs = decoded.has_locs, .inline_position_bits = bits, .inline_positions_data = positions } };
        }
        if (doc_frequency > self.doc_count) return error.InvalidData;
        const compact_header = usesCompactPostingsHeader(self.version);
        const chunks = if (compact_header) 1 + (doc_frequency - 1) / self.chunk_size else try input.varint();
        if (chunks == 0 or chunks > doc_frequency) return error.InvalidData;
        const stored_meta_length: ?u32 = if (compact_header) null else try input.varint();
        const stored_payload_length: ?u32 = if (self.version >= wire_version_checkpoints) try input.varint() else null;
        const positions_length = try input.varint();
        const skip_length: u64 = if (compact_header) if (chunks < postings_skip_min_chunks) 0 else @as(u64, (chunks - 1) / postings_skip_stride_chunks) * postings_skip_record_size_v24 else try input.varint();
        const explicit_impacts = usesSeparateImpactRanges(self.version) and (!compact_header or doc_frequency > self.chunk_size);
        const impacts: u32 = if (explicit_impacts) try input.varint() else if (usesSeparateImpactRanges(self.version)) 1 else 0;
        const impact_ids_length: u32 = if (explicit_impacts) try input.varint() else 0;
        const header_length: usize = @intCast(input.position - base);
        const old_block_max = try input.bytes(if (usesSeparateImpactRanges(self.version)) 0 else @as(u64, chunks) * blockMaxRecordSize(self.version));
        const metadata_length: u64 = if (stored_meta_length) |length| length else blk: {
            const header_size: usize = if (usesCompactPostingCountMeta(self.version)) 2 else postings_chunk_meta_header_size;
            var header: [postings_chunk_meta_header_size]u8 = undefined;
            if (input.position > input.end or header_size > input.end - input.position) return error.InvalidData;
            try self.view.readInto(input.position, header[0..header_size]);
            var total: u64 = header_size;
            for (header[0..header_size]) |bits| {
                if (bits > 32) return error.InvalidData;
                total += packedU32ByteLen(chunks, bits);
            }
            break :blk total;
        };
        const metadata = try input.bytes(metadata_length);
        _ = try compactChunkMetaLayout(metadata, chunks, self.version);
        const payload_length: u64 = if (stored_payload_length) |length| length else blk: {
            const last = try readCompactChunkMetaAt(metadata, chunks, self.version, self.chunk_size, doc_frequency, chunks - 1);
            break :blk @as(u64, last.doc_ctrl_off) + last.doc_ctrl_len;
        };
        const payload = try input.skipView(payload_length);
        const positions = try input.skipView(positions_length);
        const skips = try input.bytes(skip_length);
        const impact_metadata = try input.bytes(if (usesPackedImpactFrequency(self.version)) @as(u64, packedU32ByteLen(impacts, 5)) + impacts else @as(u64, impacts) * blockMaxRecordSize(self.version));
        const ids = try input.bytes(impact_ids_length);
        const block_max: ?BlockMaxInfo = if (usesSeparateImpactRanges(self.version)) if (impacts > 0) .{ .meta = impact_metadata, .chunk_size = if (impact_ids_length > 0) impact_range_doc_count else self.chunk_size, .chunk_meta_data = if (impact_ids_length > 0) ids else metadata, .chunk_meta_count = impacts, .version = self.version, .range_ids = impact_ids_length > 0, .packed_impact_frequency = usesPackedImpactFrequency(self.version) } else null else .{ .meta = old_block_max, .chunk_size = self.chunk_size, .chunk_meta_data = metadata, .chunk_meta_count = chunks, .version = self.version };
        const serialized = try @import("../segment_source.zig").View.init(self.view.source, self.view.offset + base, input.position - base);
        return .{ .arena = arena, .value = .{ .doc_freq = doc_frequency, .serialized_data = &.{}, .serialized_range = serialized, .header_len = header_length, .chunk_size = self.chunk_size, .version = self.version, .doc_range_aligned = !usesPostingCountBlocks(self.version) or (usesSeparateImpactRanges(self.version) and impact_ids_length > 0), .block_max = block_max, .chunk_meta_data = metadata, .chunk_meta_count = chunks, .payload_data = &.{}, .payload_range = payload, .max_payload_chunk_bytes = max_payload_chunk_bytes, .norms_data = norms, .norms_reader = if (cached_norms == null) self.* else null, .positions_range = if (positions_length == 0) null else positions, .max_position_record_bytes = max_position_record_bytes, .skip_data = if (skip_length == 0) null else skips, .impact_chunk_ids_data = if (impact_ids_length == 0) null else ids, .impact_chunk_count = if (impact_ids_length == 0) 0 else impacts } };
    }

    pub fn lookupValue(self: *const RangeInvertedIndexReader, term: []const u8, scratch: *@import("../segment_source.zig").Scratch) !?u64 {
        scratch.reset();
        if (self.term_bloom) |filter| {
            if (filter.bit_count == 0 or filter.hash_count == 0) return null;
            const hashes = termBloomHashes(term);
            const h2 = if (hashes.h2 == 0) 0x9e3779b97f4a7c15 else hashes.h2;
            for (0..filter.hash_count) |i| {
                const bit = (hashes.h1 +% (@as(u64, i) *% h2)) % filter.bit_count;
                var byte: [1]u8 = undefined;
                try self.view.readInto(filter.bytes_offset + bit / 8, &byte);
                if (byte[0] & (@as(u8, 1) << @as(u3, @intCast(bit % 8))) == 0) return null;
            }
        }

        const lo = (try self.ceilingBlock(term)) orelse return null;
        const start = try self.blockOffset(lo);
        const end = if (lo + 1 < self.block_count) try self.blockOffset(lo + 1) else self.blocks_length;
        if (start > end or end > self.blocks_length) return error.InvalidData;
        if (end - start > self.max_dictionary_block_bytes) return error.SegmentReadBudgetExceeded;
        const block = try scratch.allocator().alloc(u8, end - start);
        try self.view.readInto(self.blocks_offset + start, block);
        return lookupBlockedTerm(self.allocator, block, 0, term) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }

    fn ceilingBlock(self: *const RangeInvertedIndexReader, term: []const u8) !?u32 {
        var lo: u32 = 0;
        var hi = self.block_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (try self.compareCeiling(mid, term) == .lt) lo = mid + 1 else hi = mid;
        }
        return if (lo < self.block_count) lo else null;
    }

    fn blockOffset(self: *const RangeInvertedIndexReader, block: u32) !u32 {
        var bytes: [4]u8 = undefined;
        try self.view.readInto(self.index_offset + @as(u64, block) * term_dict_index_record_size, &bytes);
        return std.mem.readInt(u32, &bytes, .little);
    }

    fn compareCeiling(self: *const RangeInvertedIndexReader, block: u32, term: []const u8) !std.math.Order {
        var bytes: [4]u8 = undefined;
        try self.view.readInto(self.index_offset + @as(u64, block) * term_dict_index_record_size + 4, &bytes);
        const records_length = @as(u64, self.block_count) * term_dict_index_record_size;
        const term_offset = std.mem.readInt(u32, &bytes, .little);
        if (term_offset >= self.index_length - records_length) return error.InvalidData;
        var cursor = self.index_offset + records_length + term_offset;
        const end = self.index_offset + self.index_length;
        var length: u32 = 0;
        var finished = false;
        for (0..5) |i| {
            if (cursor >= end) return error.InvalidData;
            var byte: [1]u8 = undefined;
            try self.view.readInto(cursor, &byte);
            cursor += 1;
            if (i == 4 and byte[0] > 15) return error.InvalidData;
            length |= @as(u32, byte[0] & 127) << @as(u5, @intCast(i * 7));
            if (byte[0] & 128 == 0) {
                finished = true;
                break;
            }
        }
        if (!finished or length > end - cursor) return error.InvalidData;
        const wanted = @min(length, term.len);
        var stack: [256]u8 = undefined;
        const prefix = if (wanted <= stack.len) stack[0..wanted] else try self.allocator.alloc(u8, wanted);
        defer if (wanted > stack.len) self.allocator.free(prefix);
        try self.view.readInto(cursor, prefix);
        const order = std.mem.order(u8, prefix, term[0..wanted]);
        return if (order == .eq) std.math.order(@as(usize, length), term.len) else order;
    }
};

pub const OwnedRangePostings = struct {
    arena: std.heap.ArenaAllocator,
    value: TermPostings,
    pub fn deinit(self: *OwnedRangePostings) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const RangePostingInput = struct {
    view: @import("../segment_source.zig").View,
    position: u64,
    end: u64,
    allocator: Allocator,
    budget: usize,
    claimed: usize = 0,

    fn byte(self: *RangePostingInput) !u8 {
        if (self.position >= self.end) return error.InvalidData;
        var result: [1]u8 = undefined;
        try self.view.readInto(self.position, &result);
        self.position += 1;
        return result[0];
    }

    fn varint(self: *RangePostingInput) !u32 {
        var result: u32 = 0;
        for (0..5) |i| {
            const value = try self.byte();
            if (i == 4 and value > 15) return error.InvalidData;
            result |= @as(u32, value & 127) << @as(u5, @intCast(i * 7));
            if (value & 128 == 0) return result;
        }
        return error.InvalidData;
    }

    fn bytesAt(self: *RangePostingInput, offset: u64, length: u64) ![]const u8 {
        if (offset > self.view.length or length > self.view.length - offset) return error.InvalidData;
        if (length > self.budget - self.claimed) return error.SegmentMetadataTooLarge;
        self.claimed += @intCast(length);
        const output = try self.allocator.alloc(u8, @intCast(length));
        try self.view.readInto(offset, output);
        return output;
    }

    fn bytes(self: *RangePostingInput, length: u64) ![]const u8 {
        if (self.position > self.end or length > self.end - self.position) return error.InvalidData;
        const output = try self.bytesAt(self.position, length);
        self.position += length;
        return output;
    }

    fn skipView(self: *RangePostingInput, length: u64) !@import("../segment_source.zig").View {
        if (self.position > self.end or length > self.end - self.position) return error.InvalidData;
        const result = try @import("../segment_source.zig").View.init(self.view.source, self.view.offset + self.position, length);
        self.position += length;
        return result;
    }
};

/// One worker's field scope. Native metadata and read buffers survive until
/// the scope AND every escaping postings iterator have closed. A contiguous
/// scope allocates nothing and borrows its existing immutable section.
pub const ScopedInvertedIndexReader = struct {
    contiguous: ?InvertedIndexReader = null,
    context: ?*Context = null,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    version: u8,

    pub const Options = struct {
        cache_bytes: usize = 256 * 1024,
        navigation_bytes: usize = std.math.maxInt(usize),
        dictionary_block_bytes: usize = std.math.maxInt(usize),
        dictionary_retained_bytes: usize = 1024 * 1024,
        payload_chunk_bytes: usize = std.math.maxInt(usize),
        position_record_bytes: usize = std.math.maxInt(usize),
    };

    pub fn initContiguous(allocator: Allocator, bytes: []const u8) !ScopedInvertedIndexReader {
        const reader = try InvertedIndexReader.init(allocator, bytes);
        return .{ .contiguous = reader, .doc_count = reader.doc_count, .total_field_len = reader.total_field_len, .chunk_size = reader.chunk_size, .version = reader.version };
    }

    pub fn initRanges(allocator: Allocator, view: @import("../segment_source.zig").View, options: Options) !ScopedInvertedIndexReader {
        return initRangesWithBacking(allocator, view, view, options);
    }
    fn initRangesWithBacking(allocator: Allocator, view: @import("../segment_source.zig").View, backing: @import("../segment_source.zig").View, options: Options) !ScopedInvertedIndexReader {
        const context = try allocator.create(Context);
        context.* = .{ .allocator = allocator, .view = backing, .options = options, .navigation = std.heap.ArenaAllocator.init(allocator), .dictionary = @import("../segment_source.zig").Scratch.init(allocator, options.dictionary_retained_bytes) };
        errdefer context.destroy();
        const source = @import("../segment_source.zig").Source{ .ranges = .{ .ptr = context, .length = backing.length, .read_into = Context.read, .close = Context.closeBorrow, .prefetch = if (backing.source == .ranges and backing.source.ranges.prefetch != null) Context.prefetch else null } };
        context.cache = try @import("../segment_source.zig").BlockCache.init(allocator, source, options.cache_bytes);
        context.native = try RangeInvertedIndexReader.init(allocator, try @import("../segment_source.zig").View.init(context.cache.?.borrowedSource(), 0, view.length), options.dictionary_block_bytes);
        return .{ .context = context, .doc_count = context.native.doc_count, .total_field_len = context.native.total_field_len, .chunk_size = context.native.chunk_size, .version = context.native.version };
    }

    fn backingView(self: *const ScopedInvertedIndexReader, offset: u64, length: u64) !@import("../segment_source.zig").View {
        return @import("../segment_source.zig").View.init(self.context.?.cache.?.borrowedSource(), offset, length);
    }

    pub fn deinit(self: *ScopedInvertedIndexReader) void {
        if (self.context) |context| Context.release(context);
        self.* = undefined;
    }

    pub fn layoutStats(self: *const ScopedInvertedIndexReader) !InvertedIndexReader.LayoutStats {
        if (self.contiguous) |reader| return reader.layoutStats();
        return self.context.?.native.layoutStats();
    }
    pub fn detailedLayoutStats(self: *const ScopedInvertedIndexReader) !InvertedIndexReader.LayoutStats {
        var iterator = try self.termIterator();
        defer iterator.deinit();
        return accumulateLayoutStats(try self.layoutStats(), &iterator);
    }

    pub fn avgDocLen(self: *const ScopedInvertedIndexReader) f32 {
        return if (self.doc_count == 0) 0 else @as(f32, @floatFromInt(self.total_field_len)) / @as(f32, @floatFromInt(self.doc_count));
    }

    pub fn docLength(self: *const ScopedInvertedIndexReader, doc: u32) !u32 {
        if (self.contiguous) |reader| return reader.docLength(doc);
        const context = self.context.?;
        return context.native.docLength(doc);
    }

    pub fn lookup(self: *const ScopedInvertedIndexReader, term: []const u8) !?LookupResult {
        if (self.contiguous) |reader| return reader.lookupChecked(term);
        const context = self.context.?;
        if (context.over_budget) return error.SegmentReadBudgetExceeded;
        const value = (try context.native.lookupValue(term, &context.dictionary)) orelse return null;
        return try context.lookupValue(value);
    }

    /// Frequency queries need only the dictionary address and first posting
    /// varint. Do not allocate norms, impact tables or posting navigation.
    pub fn docFrequency(self: *const ScopedInvertedIndexReader, term: []const u8) !?u32 {
        if (self.contiguous) |reader| return if (try reader.lookupChecked(term)) |value| value.docFreq() else null;
        const context = self.context.?;
        const value = (try context.native.lookupValue(term, &context.dictionary)) orelse return null;
        if (fstValIs1Hit(value)) {
            if (fstValDecode1Hit(value).doc_num >= context.native.norm_count) return error.InvalidData;
            return 1;
        }
        const base = std.math.add(u64, v7_header_size, value) catch return error.InvalidData;
        if (base >= context.native.norms_offset) return error.InvalidData;
        var input = RangePostingInput{ .view = context.native.view, .position = base, .end = context.native.norms_offset, .allocator = context.allocator, .budget = 0 };
        const frequency = try input.varint();
        if (frequency == 0) {
            if (!usesInlineSingleDocPostings(self.version)) return error.InvalidData;
            const doc = try input.varint();
            if (self.version >= wire_version_streaming_blocks and doc == std.math.maxInt(u32)) {
                var count: [4]u8 = undefined;
                if (input.position > input.end or count.len > input.end - input.position) return error.InvalidData;
                try input.view.readInto(input.position, &count);
                const result = std.mem.readInt(u32, &count, .little);
                if (result == 0 or result > self.doc_count) return error.InvalidData;
                return result;
            }
            if (doc >= context.native.norm_count) return error.InvalidData;
            return 1;
        }
        if (frequency > self.doc_count) return error.InvalidData;
        return frequency;
    }

    pub fn termIterator(self: *const ScopedInvertedIndexReader) !Iterator {
        return self.rangeTermIterator(null, null);
    }

    pub fn rangeTermIterator(self: *const ScopedInvertedIndexReader, start: ?[]const u8, end: ?[]const u8) !Iterator {
        if (self.contiguous) |*reader| return .{ .contiguous = try reader.rangeTermIterator(start, end) };
        const context = self.context.?;
        const block = if (start) |key| (try context.native.ceilingBlock(key)) orelse context.native.block_count else 0;
        Context.retain(context);
        return .{ .ranges = .{ .context = context, .block = block, .scratch = @import("../segment_source.zig").Scratch.init(context.allocator, context.options.dictionary_retained_bytes), .start = start, .end = end } };
    }

    pub fn fstSearchIterator(self: *const ScopedInvertedIndexReader, automaton: fst.Automaton) !Iterator {
        if (self.contiguous) |*reader| return .{ .contiguous = try reader.fstSearchIterator(automaton) };
        var iterator = try self.termIterator();
        iterator.ranges.automaton = automaton;
        return iterator;
    }

    pub const Iterator = union(enum) {
        contiguous: TermIterator,
        ranges: RangeIterator,

        pub fn blocksPruned(self: *const Iterator) u64 {
            return switch (self.*) {
                .contiguous => |*iterator| iterator.blocks_pruned,
                .ranges => |*iterator| iterator.blocks_pruned,
            };
        }

        pub fn next(self: *Iterator) !?TermIterator.Entry {
            return switch (self.*) {
                .contiguous => |*iterator| iterator.next(),
                .ranges => |*iterator| iterator.next(),
            };
        }

        pub fn nextWithDecodedCount(self: *Iterator, decoded: *u64) !?TermIterator.Entry {
            return switch (self.*) {
                .contiguous => |*iterator| iterator.nextWithDecodedCount(decoded),
                .ranges => |*iterator| iterator.nextTracked(decoded),
            };
        }

        pub fn deinit(self: *Iterator) void {
            switch (self.*) {
                .contiguous => |*iterator| iterator.deinit(),
                .ranges => |*iterator| iterator.deinit(),
            }
            self.* = undefined;
        }
    };

    const RangeIterator = struct {
        context: *Context,
        scratch: @import("../segment_source.zig").Scratch,
        block: u32 = 0,
        bytes: []const u8 = &.{},
        cursor: usize = 0,
        remaining: u32 = 0,
        prefix_length: usize = 0,
        last_postings_offset: u64 = 0,
        key: std.ArrayListUnmanaged(u8) = .empty,
        start: ?[]const u8,
        end: ?[]const u8,
        automaton: ?fst.Automaton = null,
        automaton_states: std.ArrayListUnmanaged(usize) = .empty,
        seek_scratch: std.ArrayListUnmanaged(u8) = .empty,
        blocks_pruned: u64 = 0,
        done: bool = false,
        current_metadata: ?*TermMetadata = null,

        fn next(self: *RangeIterator) !?TermIterator.Entry {
            return self.nextInternal(false, null);
        }

        fn nextTracked(self: *RangeIterator, decoded: *u64) !?TermIterator.Entry {
            return self.nextInternal(true, decoded);
        }

        fn nextInternal(self: *RangeIterator, comptime tracked: bool, decoded: ?*u64) !?TermIterator.Entry {
            if (self.current_metadata) |metadata| {
                if (metadata.references.load(.acquire) == 1) {
                    // Keep small navigation capacity; release exceptional terms.
                    _ = metadata.arena.reset(if (metadata.arena.queryCapacity() <= 256 * 1024) .retain_capacity else .free_all);
                } else {
                    TermMetadata.release(metadata);
                    self.current_metadata = null;
                }
            }
            const a = self.context.allocator;
            while (!self.done) {
                if (self.remaining == 0 and !try self.loadBlock()) return null;
                const shared = try readVarintU32(self.bytes, &self.cursor);
                const leaf_length = try readVarintU32(self.bytes, &self.cursor);
                if (self.prefix_length + shared > self.key.items.len or leaf_length > self.bytes.len - self.cursor) return error.InvalidData;
                const leaf = self.bytes[self.cursor..][0..leaf_length];
                self.cursor += leaf_length;
                const value = decodeTermDictBlockValueDelta(try readVarintU64(self.bytes, &self.cursor), &self.last_postings_offset);
                self.remaining -= 1;
                if (tracked) decoded.?.* += 1;
                self.key.shrinkRetainingCapacity(self.prefix_length + shared);
                try self.key.appendSlice(a, leaf);
                if (self.start) |start| if (std.mem.order(u8, self.key.items, start) == .lt) continue;
                if (self.end) |end| if (std.mem.order(u8, self.key.items, end) != .lt) {
                    self.done = true;
                    return null;
                };
                if (self.automaton) |automaton| if (!try self.advanceAutomaton(automaton, self.prefix_length + shared)) continue;
                const result = if (fstValIs1Hit(value)) try self.context.lookupValue(value) else blk: {
                    const metadata = self.current_metadata orelse try TermMetadata.create(self.context);
                    self.current_metadata = metadata;

                    var native = self.context.native;
                    native.allocator = metadata.arena.allocator();
                    var owned = try native.openPostingsWithNorms(value, self.context.options.navigation_bytes, self.context.options.payload_chunk_bytes, self.context.options.position_record_bytes, null);
                    if (metadata.arena.queryCapacity() > self.context.options.navigation_bytes) return error.SegmentReadBudgetExceeded;
                    owned.value.metadata_owner = .{ .ptr = metadata, .retain = TermMetadata.retain, .release = TermMetadata.release };
                    break :blk LookupResult{ .postings = owned.value };
                };
                return .{ .term = self.key.items, .result = result };
            }
            return null;
        }

        fn loadBlock(self: *RangeIterator) !bool {
            while (true) {
                const reader = &self.context.native;
                if (self.block >= reader.block_count) {
                    self.done = true;
                    return false;
                }
                const start = try reader.blockOffset(self.block);
                const end = if (self.block + 1 < reader.block_count) try reader.blockOffset(self.block + 1) else reader.blocks_length;
                if (start > end or end > reader.blocks_length) return error.InvalidData;
                if (end - start > reader.max_dictionary_block_bytes) return error.SegmentReadBudgetExceeded;
                self.scratch.reset();
                const bytes = try self.scratch.allocator().alloc(u8, end - start);
                try reader.view.readInto(reader.blocks_offset + start, bytes);
                self.bytes = bytes;
                self.cursor = 0;
                self.prefix_length = try readVarintU32(bytes, &self.cursor);
                self.remaining = try readVarintU32(bytes, &self.cursor);
                if (self.remaining == 0 or self.prefix_length > bytes.len - self.cursor) return error.InvalidData;
                self.key.clearRetainingCapacity();
                try self.key.appendSlice(self.context.allocator, bytes[self.cursor..][0..self.prefix_length]);
                self.cursor += self.prefix_length;
                self.last_postings_offset = 0;
                self.block += 1;
                if (self.automaton) |automaton| {
                    if (try self.primeAutomaton(automaton)) |dead_length| {
                        self.blocks_pruned += 1;
                        self.seek_scratch.clearRetainingCapacity();
                        try self.seek_scratch.appendSlice(self.context.allocator, self.key.items[0..dead_length]);
                        while (self.seek_scratch.items.len != 0 and self.seek_scratch.items[self.seek_scratch.items.len - 1] == 255) self.seek_scratch.items.len -= 1;
                        if (self.seek_scratch.items.len == 0) {
                            self.done = true;
                            return false;
                        }
                        self.seek_scratch.items[self.seek_scratch.items.len - 1] += 1;
                        self.block = @max(self.block, (try reader.ceilingBlock(self.seek_scratch.items)) orelse reader.block_count);
                        self.remaining = 0;
                        continue;
                    }
                }
                return true;
            }
        }

        fn primeAutomaton(self: *RangeIterator, automaton: fst.Automaton) !?usize {
            self.automaton_states.clearRetainingCapacity();
            try self.automaton_states.ensureTotalCapacity(self.context.allocator, self.prefix_length + 1);
            var state = automaton.start();
            self.automaton_states.appendAssumeCapacity(state);
            if (!automaton.canMatch(state)) return 0;
            for (self.key.items, 0..) |byte, i| {
                state = automaton.accept(state, byte);
                self.automaton_states.appendAssumeCapacity(state);
                if (!automaton.canMatch(state)) return i + 1;
            }
            return null;
        }

        fn advanceAutomaton(self: *RangeIterator, automaton: fst.Automaton, retained: usize) !bool {
            if (self.automaton_states.items.len <= retained) return error.InvalidData;
            self.automaton_states.shrinkRetainingCapacity(retained + 1);
            try self.automaton_states.ensureUnusedCapacity(self.context.allocator, self.key.items.len - retained);
            var state = self.automaton_states.items[retained];
            for (self.key.items[retained..]) |byte| {
                if (automaton.canMatch(state)) state = automaton.accept(state, byte);
                self.automaton_states.appendAssumeCapacity(state);
            }
            return automaton.canMatch(state) and automaton.isMatch(state);
        }

        fn deinit(self: *RangeIterator) void {
            if (self.current_metadata) |metadata| TermMetadata.release(metadata);
            self.key.deinit(self.context.allocator);
            self.automaton_states.deinit(self.context.allocator);
            self.seek_scratch.deinit(self.context.allocator);
            self.scratch.deinit();
            Context.release(self.context);
        }
    };

    /// An enumeration entry borrows navigation until next(). Postings iterators
    /// retain it independently; a cursor reuses scratch only while unshared.
    const TermMetadata = struct {
        context: *Context,
        arena: std.heap.ArenaAllocator,
        references: std.atomic.Value(usize) = .init(1),

        fn create(context: *Context) !*TermMetadata {
            const owner = try context.allocator.create(TermMetadata);
            owner.* = .{ .context = context, .arena = std.heap.ArenaAllocator.init(context.allocator) };
            Context.retain(context);
            return owner;
        }
        fn retain(ptr: *anyopaque) void {
            const owner: *TermMetadata = @ptrCast(@alignCast(ptr));
            _ = owner.references.fetchAdd(1, .monotonic);
        }
        fn release(ptr: *anyopaque) void {
            const owner: *TermMetadata = @ptrCast(@alignCast(ptr));
            if (owner.references.fetchSub(1, .acq_rel) != 1) return;
            const context = owner.context;
            owner.arena.deinit();
            context.allocator.destroy(owner);
            Context.release(context);
        }
    };

    const Context = struct {
        allocator: Allocator,
        references: std.atomic.Value(usize) = .init(1),
        view: @import("../segment_source.zig").View,
        options: Options,
        navigation: std.heap.ArenaAllocator,
        dictionary: @import("../segment_source.zig").Scratch,
        cache: ?@import("../segment_source.zig").BlockCache = null,
        native: RangeInvertedIndexReader = undefined,
        over_budget: bool = false,

        fn destroy(self: *Context) void {
            self.navigation.deinit();
            self.dictionary.deinit();
            if (self.cache) |*cache| cache.deinit();
            self.allocator.destroy(self);
        }

        fn retain(ptr: *anyopaque) void {
            const self: *Context = @ptrCast(@alignCast(ptr));
            _ = self.references.fetchAdd(1, .monotonic);
        }

        fn release(ptr: *anyopaque) void {
            const self: *Context = @ptrCast(@alignCast(ptr));
            if (self.references.fetchSub(1, .acq_rel) == 1) self.destroy();
        }

        fn read(ptr: *anyopaque, offset: u64, output: []u8) !void {
            const self: *Context = @ptrCast(@alignCast(ptr));
            try self.view.readInto(offset, output);
        }

        fn prefetch(ptr: *anyopaque, offset: u64, length: u64) void {
            const self: *Context = @ptrCast(@alignCast(ptr));
            self.view.source.prefetch(self.view.offset + offset, length);
        }

        fn closeBorrow(_: *anyopaque) void {}

        fn lookupValue(self: *Context, value: u64) !LookupResult {
            if (fstValIs1Hit(value)) {
                const decoded = fstValDecode1Hit(value);
                if (decoded.doc_num >= self.native.norm_count) return error.InvalidData;
                return .{ .one_hit = .{ .doc_num = @intCast(decoded.doc_num), .norm_bits = try self.native.docLength(@intCast(decoded.doc_num)) } };
            }
            var native = self.native;
            native.allocator = self.navigation.allocator();
            const retained = self.navigation.queryCapacity();
            if (retained > self.options.navigation_bytes) return error.SegmentReadBudgetExceeded;
            var owned = try native.openPostingsWithNorms(value, self.options.navigation_bytes - retained, self.options.payload_chunk_bytes, self.options.position_record_bytes, null);
            // Child arenas allocate through navigation, so the context owns
            // every buffer. Its lease also retains the cache used by ranges.
            if (self.navigation.queryCapacity() > self.options.navigation_bytes) {
                self.over_budget = true;
                return error.SegmentReadBudgetExceeded;
            }
            owned.value.metadata_owner = .{ .ptr = self, .retain = retain, .release = release };
            return .{ .postings = owned.value };
        }
    };
};

fn accumulateLayoutStats(initial: InvertedIndexReader.LayoutStats, it: anytype) !InvertedIndexReader.LayoutStats {
    var stats = initial;
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
                    const positions_len = postings.positionsLength();
                    var projected_header = varintU32Size(postings.doc_freq) +
                        varintU32Size(@intCast(postings.payloadLength())) +
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
                stats.chunk_meta_bytes +|= @intCast(postings.chunk_meta_data.len + postings.streamed_records.len);
                if (postings.streamed_records_range) |records| stats.chunk_meta_bytes +|= records.length;
                stats.postings_payload_bytes +|= @intCast(postings.payloadLength());
                stats.positions_bytes +|= postings.positionsLength();
                if (postings.skip_data) |skip_data| {
                    stats.skip_bytes +|= @intCast(skip_data.len);
                }
            },
        }
    }
    return stats;
}

test "native term frequency reads do not load norms or posting navigation" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    try builder.addDocument(0, &.{
        .{ .term = "one", .freq = 1 },
        .{ .term = "inline", .freq = 2, .positions = &.{ 0, 3 } },
        .{ .term = "many", .freq = 1 },
    });
    for (1..1024) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "many", .freq = 1 }});
    const bytes = try builder.build();
    defer a.free(bytes);
    const View = @import("../segment_source.zig").View;
    var native = try ScopedInvertedIndexReader.initRanges(a, try View.init(.{ .contiguous = bytes }, 0, bytes.len), .{});
    defer native.deinit();
    try std.testing.expectEqual(@as(?u32, 1), try native.docFrequency("one"));
    try std.testing.expectEqual(@as(?u32, 1), try native.docFrequency("inline"));
    try std.testing.expectEqual(@as(?u32, 1024), try native.docFrequency("many"));
    try std.testing.expectEqual(@as(?u32, null), try native.docFrequency("missing"));
    try std.testing.expectEqual(@as(usize, 0), native.context.?.navigation.queryCapacity());
}

test "streamed common postings merge matches sorting fallback with bounded scratch" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..25_000) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 3, .positions = &.{ 0, 2, 5 } }});
    const bytes = try builder.build();
    defer a.free(bytes);
    const first = try a.alloc(u32, 25_000);
    defer a.free(first);
    const second = try a.alloc(u32, 25_000);
    defer a.free(second);
    for (first, second, 0..) |*one, *two, i| {
        one.* = @intCast(i);
        two.* = @intCast(i + 25_000);
    }
    const Budget = @import("../storage/lite/test_allocator.zig").BudgetAllocator;
    var streamed_budget = Budget{ .backing = a, .limit = 1024 * 1024 };
    var streamed_sink = MergeMemorySink{ .alloc = a };
    defer streamed_sink.deinit();
    const Source = @import("../segment_source.zig");
    const State = struct {
        bytes: []const u8,
        reads: usize = 0,
        largest: usize = 0,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            self.largest = @max(self.largest, out.len);
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .bytes = bytes };
    const view = try Source.View.init(.{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } }, 0, bytes.len);
    const stream_start = platform_time.monotonicNs();
    try writeMergedInvertedSectionSlotsWithDocMaps(streamed_budget.allocator(), &streamed_sink, @as([]const ?Source.View, &.{ view, view }), &.{ 25_000, 25_000 }, &.{ first, second }, 50_000, .{});
    const stream_ns = platform_time.monotonicNs() - stream_start;
    try std.testing.expectEqual(@as(usize, 0), streamed_budget.live);
    try std.testing.expect(state.largest <= 64 * 1024);
    // Same logical output, but non-monotonic maps force the reference sorter.
    std.mem.reverse(u32, first);
    std.mem.reverse(u32, second);
    var fallback_budget = Budget{ .backing = a };
    var fallback_sink = MergeMemorySink{ .alloc = a };
    defer fallback_sink.deinit();
    const fallback_start = platform_time.monotonicNs();
    try writeMergedInvertedSectionSlotsWithDocMaps(fallback_budget.allocator(), &fallback_sink, @as([]const ?[]const u8, &.{ bytes, bytes }), &.{ 25_000, 25_000 }, &.{ first, second }, 50_000, .{});
    const fallback_ns = platform_time.monotonicNs() - fallback_start;
    const fallback_reader = try InvertedIndexReader.init(a, fallback_sink.output.items);
    const fallback_result = fallback_reader.lookup("common").?;
    var fallback_postings = try fallback_result.iterator(a);
    defer fallback_postings.deinit();
    try std.testing.expect(streamed_budget.peak < fallback_budget.peak / 2);
    try std.testing.expect(streamed_sink.write_calls <= 8);
    try std.testing.expect(streamed_sink.largest_write <= 64 * 1024);
    var reader = try InvertedIndexReader.init(a, streamed_sink.output.items);
    var result = reader.lookup("common").?;
    var postings = try result.iterator(a);
    defer postings.deinit();
    const native_view = try Source.View.init(.{ .contiguous = streamed_sink.output.items }, 0, streamed_sink.output.items.len);
    var native = try ScopedInvertedIndexReader.initRanges(a, native_view, .{ .navigation_bytes = 4096 });
    defer native.deinit();
    try std.testing.expectEqual(@as(?u32, 50_000), try native.docFrequency("common"));
    const native_result = (try native.lookup("common")).?;
    try std.testing.expect(native_result.postings.streamed_records_range != null);
    var native_postings = try native_result.iterator(a);
    defer native_postings.deinit();
    const seek = (try native_postings.advanceToWithPositions(10_000)).?;
    try std.testing.expectEqual(@as(u32, 10_000), seek.doc_id);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2, 5 }, seek.positions);
    const deferred = (try native_postings.advanceToDeferredPositions(20_000)).?;
    try std.testing.expectEqual(@as(u32, 20_000), deferred.doc_id);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2, 5 }, (try native_postings.decodeDeferredPositions()).positions);
    var count: u32 = 0;
    while (try postings.next()) |hit| {
        const reference = (try fallback_postings.next()).?;
        try std.testing.expectEqual(reference.doc_id, hit.doc_id);
        try std.testing.expectEqual(reference.freq, hit.freq);
        try std.testing.expectEqual(reference.norm, hit.norm);
        try std.testing.expectEqualSlices(u32, reference.positions, hit.positions);
        try std.testing.expectEqual(count, hit.doc_id);
        try std.testing.expectEqualSlices(u32, &.{ 0, 2, 5 }, hit.positions);
        count += 1;
    }
    try std.testing.expectEqual(@as(u32, 50_000), count);
    std.debug.print("LITE_POSTINGS_MERGE documents=50000 streamed_peak={d} fallback_peak={d} streamed_ns={d} fallback_ns={d} writes={d}\n", .{ streamed_budget.peak, fallback_budget.peak, stream_ns, fallback_ns, streamed_sink.write_calls });
}

test "streamed postings preserve empty location blocks deletions and allocation failure ownership" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..4096) |doc| {
        const located = doc >= 1024 and doc < 2048;
        try builder.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = if (located) 2 else 1, .positions = if (located) &.{ 1, 4 } else &.{} }});
    }
    const bytes = try builder.build();
    defer a.free(bytes);
    var reader = try InvertedIndexReader.init(a, bytes);
    const entries = [_]?TermIterator.Entry{.{ .term = "common", .result = reader.lookup("common").? }};
    const map = try a.alloc(u32, 4096);
    defer a.free(map);
    for (map, 0..) |*id, i| id.* = if (i % 127 == 0) std.math.maxInt(u32) else @intCast(i);
    const maps = [_][]const u32{map};
    const norms = try a.alloc(u32, 4096);
    defer a.free(norms);
    @memset(norms, 0);
    var total: u64 = 0;
    var expected = PostingAccumulator.init();
    defer expected.deinit(a);
    try appendLookupResultToAccumulator(a, &expected, entries[0].?.result, map, norms, &total);
    var scratch = PostingSerializeScratch{};
    defer scratch.deinit(a);
    var expected_bytes = std.ArrayListUnmanaged(u8).empty;
    defer expected_bytes.deinit(a);
    try expected.serializeV9(a, &expected_bytes, &scratch, .{});
    @memset(norms, 0);
    var streamed_total: u64 = 0;
    var sink = MergeMemorySink{ .alloc = a };
    defer sink.deinit();
    const placeholder: [v7_header_size]u8 = @splat(0);
    try sink.appendSlice(&placeholder);
    const offset = (try appendStreamedMergedTermToSink(a, &sink, 0, &entries, "common", &maps, norms, &streamed_total, .{})).?;
    try std.testing.expectEqual(total, streamed_total);
    var stream_reader = reader;
    stream_reader.data = sink.output.items;
    const stream_result = stream_reader.readPostings(offset);
    var stream_iterator = try stream_result.iterator(a);
    defer stream_iterator.deinit();
    var reference_reader = reader;
    var reference_data = std.ArrayListUnmanaged(u8).empty;
    defer reference_data.deinit(a);
    try reference_data.appendNTimes(a, 0, v7_header_size);
    try reference_data.appendSlice(a, expected_bytes.items);
    reference_reader.data = reference_data.items;
    const reference_result = reference_reader.readPostings(0);
    var reference_iterator = try reference_result.iterator(a);
    defer reference_iterator.deinit();
    while (try reference_iterator.next()) |expected_hit| {
        const actual = (try stream_iterator.next()).?;
        try std.testing.expectEqual(expected_hit.doc_id, actual.doc_id);
        try std.testing.expectEqual(expected_hit.freq, actual.freq);
        try std.testing.expectEqualSlices(u32, expected_hit.positions, actual.positions);
    }
    try std.testing.expect((try stream_iterator.next()) == null);
    const Harness = struct {
        fn run(backing: Allocator, input_entries: []const ?TermIterator.Entry, input_maps: []const []const u32) !void {
            var stable = @import("../storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            const alloc = stable.allocator();
            const lengths = try alloc.alloc(u32, 4096);
            defer alloc.free(lengths);
            @memset(lengths, 0);
            var length: u64 = 0;
            var output = MergeMemorySink{ .alloc = alloc };
            defer output.deinit();
            const header: [v7_header_size]u8 = @splat(0);
            try output.appendSlice(&header);
            _ = try appendStreamedMergedTermToSink(alloc, &output, 0, input_entries, "common", input_maps, lengths, &length, .{});
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ @as([]const ?TermIterator.Entry, &entries), @as([]const []const u32, &maps) });
    @memset(map, std.math.maxInt(u32));
    @memset(norms, 0);
    streamed_total = 0;
    const before = sink.len();
    try std.testing.expect((try appendStreamedMergedTermToSink(a, &sink, 0, &entries, "common", &maps, norms, &streamed_total, .{})) == null);
    try std.testing.expectEqual(before, sink.len());
    try std.testing.expectEqual(@as(u64, 0), streamed_total);
}

test "native postings read norms larger than the navigation budget by range" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    const last: u32 = 4 * 1024 * 1024 + 1;
    try builder.addDocument(0, &.{.{ .term = "many", .freq = 3, .norm = 9 }});
    try builder.addDocument(last, &.{
        .{ .term = "one", .freq = 1, .norm = 7 },
        .{ .term = "inline", .freq = 2, .norm = 7, .positions = &.{ 1, 4 } },
        .{ .term = "many", .freq = 2, .norm = 7 },
    });
    // Sparse fixture represents a field on a larger document population.
    builder.doc_count = last + 1;
    const bytes = try builder.build();
    defer a.free(bytes);
    const View = @import("../segment_source.zig").View;
    var native = try ScopedInvertedIndexReader.initRanges(a, try View.init(.{ .contiguous = bytes }, 0, bytes.len), .{ .navigation_bytes = 4 * 1024 * 1024 });
    defer native.deinit();
    const contiguous = try InvertedIndexReader.init(a, bytes);
    try std.testing.expect(native.context.?.native.norms_length > native.context.?.options.navigation_bytes);
    for ([_][]const u8{ "one", "inline", "many" }) |term| {
        const result = (try native.lookup(term)).?;
        var iter = try result.iterator(a);
        defer iter.deinit();
        while (try iter.nextScoring()) |hit| try std.testing.expectEqual(contiguous.docLength(hit.doc_id), hit.norm);
    }
    try std.testing.expectEqual(contiguous.docLength(last), try native.docLength(last));
    try std.testing.expect(native.context.?.navigation.queryCapacity() < 64 * 1024);
}

test "v38 contiguous and native readers remain compatible with legacy posting layout" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..512) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "legacy", .freq = 2, .norm = 7, .positions = &.{ 1, 4 } }});
    const bytes = try builder.build();
    defer a.free(bytes);
    bytes[4] = wire_version_compact_postings_header;
    const contiguous = try InvertedIndexReader.init(a, bytes);
    const View = @import("../segment_source.zig").View;
    var native = try ScopedInvertedIndexReader.initRanges(a, try View.init(.{ .contiguous = bytes }, 0, bytes.len), .{});
    defer native.deinit();
    const reference = contiguous.lookup("legacy").?;
    const result = (try native.lookup("legacy")).?;
    var expected = try reference.iterator(a);
    defer expected.deinit();
    var actual = try result.iterator(a);
    defer actual.deinit();
    while (try expected.next()) |hit| {
        const fresh = (try actual.next()).?;
        try std.testing.expectEqual(hit.doc_id, fresh.doc_id);
        try std.testing.expectEqual(hit.norm, fresh.norm);
        try std.testing.expectEqualSlices(u32, hit.positions, fresh.positions);
    }
    try std.testing.expect((try actual.next()) == null);
}

test "range norms preserve sparse field document IDs above field population" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    try builder.addDocument(127, &.{.{ .term = "common", .freq = 2, .norm = 7 }});
    try builder.addDocument(128, &.{
        .{ .term = "common", .freq = 2, .norm = 9 },
        .{ .term = "one", .freq = 1, .norm = 9 },
        .{ .term = "inline", .freq = 2, .norm = 9, .positions = &.{ 1, 4 } },
    });
    const bytes = try builder.build();
    defer a.free(bytes);
    const contiguous = try InvertedIndexReader.init(a, bytes);
    const View = @import("../segment_source.zig").View;
    var native = try ScopedInvertedIndexReader.initRanges(a, try View.init(.{ .contiguous = bytes }, 0, bytes.len), .{});
    defer native.deinit();
    try std.testing.expectEqual(@as(u32, 2), native.doc_count);
    for ([_][]const u8{ "common", "one", "inline" }) |term| {
        const expected = contiguous.lookup(term).?;
        const result = (try native.lookup(term)).?;
        try std.testing.expectEqual(@as(?u32, expected.docFreq()), try native.docFrequency(term));
        var reference = try expected.iterator(a);
        defer reference.deinit();
        var actual = try result.iterator(a);
        defer actual.deinit();
        while (try reference.next()) |hit| {
            const fresh = (try actual.next()).?;
            try std.testing.expectEqual(hit.doc_id, fresh.doc_id);
            try std.testing.expectEqual(hit.norm, fresh.norm);
            try std.testing.expectEqualSlices(u32, hit.positions, fresh.positions);
            try std.testing.expectEqual(hit.norm, try native.docLength(hit.doc_id));
        }
    }
}

test "unrelated merge heads never enable streamed positions" {
    const a = std.testing.allocator;
    var common = InvertedIndexBuilder.init(a, .{});
    defer common.deinit();
    for (0..4096) |id| try common.addDocument(@intCast(id), &.{.{ .term = "a", .freq = 2 }});
    var other = InvertedIndexBuilder.init(a, .{});
    defer other.deinit();
    try other.addDocument(0, &.{.{ .term = "z", .freq = 2, .positions = &.{ 1, 2 } }});
    const one = try common.build();
    defer a.free(one);
    const two = try other.build();
    defer a.free(two);
    const one_reader = try InvertedIndexReader.init(a, one);
    const two_reader = try InvertedIndexReader.init(a, two);
    const maps = try a.alloc(u32, 4096);
    defer a.free(maps);
    for (maps, 0..) |*id, i| id.* = @intCast(i);
    const norms = try a.alloc(u32, 4097);
    defer a.free(norms);
    @memset(norms, 0);
    var sink = MergeMemorySink{ .alloc = a };
    defer sink.deinit();
    try sink.output.appendNTimes(a, 0, v7_header_size);
    var total: u64 = 0;
    const entries = [_]?TermIterator.Entry{
        .{ .term = "a", .result = one_reader.lookup("a").? },
        .{ .term = "z", .result = two_reader.lookup("z").? },
    };
    const value = (try appendStreamedMergedTermToSink(a, &sink, 0, &entries, "a", &.{ maps, &.{4096} }, norms, &total, .{})).?;
    const at = v7_header_size + @as(usize, @intCast(value));
    var cursor = at;
    _ = try readVarintU32(sink.output.items, &cursor);
    _ = try readVarintU32(sink.output.items, &cursor);
    const descriptor = StreamedDescriptor.decode(sink.output.items[cursor..][0..StreamedDescriptor.size]);
    std.debug.print("LITE_UNRELATED_POSITIONS documents=4096 positions_bytes={d}\n", .{descriptor.positions_length});
    try std.testing.expectEqual(@as(u64, 0), descriptor.positions_length);
}

test "append merge uses fixed scratch for sparse document spaces and deletion rank mappings" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, productionIndexConfig());
    defer builder.deinit();
    for (0..1024) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 1, .norm = 3 }});
    const bytes = try builder.build();
    defer a.free(bytes);
    var deleted = roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    try deleted.add(7);
    var peaks: [2]usize = undefined;
    for ([_]u32{ 1024, 100_000 }, 0..) |space, case| {
        var output = @import("../segment.zig").MemorySegmentSink.init(a);
        defer output.deinit();
        var sink = output.sink();
        var budget = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 256 * 1024 };
        try writeMergedInvertedSectionSlotsWithDeletes(budget.allocator(), &sink, @as([]const ?[]const u8, &.{ bytes, null, bytes }), &.{ space, 7, space }, &.{ deleted, null, deleted }, productionIndexConfig());
        peaks[case] = budget.peak;
        var reader = try ScopedInvertedIndexReader.initContiguous(a, output.out.items);
        defer reader.deinit();
        try std.testing.expectEqual(@as(u32, 3), try reader.docLength(7));
        try std.testing.expectEqual(@as(u32, 0), try reader.docLength(space - 1));
        try std.testing.expectEqual(@as(u32, 3), try reader.docLength(space + 6));
        var result = (try reader.lookup("common")).?;
        try std.testing.expectEqual(@as(u32, 2046), result.docFreq());
    }
    try std.testing.expectEqual(peaks[0], peaks[1]);
    std.debug.print("APPEND_MERGE sparse_spaces=2053,200005 scratch_peaks={d},{d}\n", .{ peaks[0], peaks[1] });
}

test "packed spill preserves legacy and current positional sources with bounded records" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    for ([_]bool{ true, false }) |legacy_header| {
        var builder = InvertedIndexBuilder.init(a, .{});
        defer builder.deinit();
        var positions: [256]u32 = undefined;
        for (&positions, 0..) |*position, i| position.* = @intCast(i);
        for (0..64) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 256, .norm = 256, .positions = &positions }});
        const bytes = try builder.build();
        defer a.free(bytes);
        if (legacy_header) bytes[4] = wire_version_compact_postings_header;
        const reader = try InvertedIndexReader.init(a, bytes);
        var mappings: [64 * 4]u8 = undefined;
        for (0..64) |doc| std.mem.writeInt(u32, mappings[doc * 4 ..][0..4], @intCast(63 - doc), .little);
        const View = @import("../segment_source.zig").View;
        const maps = [_]FileDocMap{.{ .len = 64, .ids = try View.init(.{ .contiguous = &mappings }, 0, mappings.len), .records = try View.init(.{ .contiguous = &.{} }, 0, 0), .monotonic = false, .scratch = .{ .io = std.testing.io, .directory = directory, .chunk_records = 4 } }};
        const entries = [_]?TermIterator.Entry{.{ .term = "common", .result = reader.lookup("common").? }};
        var stream = try ExternalPostingStream.init(a, &entries, "common", &maps);
        defer stream.deinit();
        try std.testing.expect(stream.sorter.input_bytes < 64 * 256 * 4 / 8);
        var acc = PostingAccumulator.init();
        defer acc.deinit(a);
        var docs: u32 = 0;
        while (try stream.block(&acc, 7)) {
            var offset: usize = 0;
            for (acc.doc_ids.items, acc.metas.items) |doc, meta| {
                try std.testing.expectEqual(docs, doc);
                docs += 1;
                try std.testing.expectEqual(@as(u32, 256), meta.position_count);
                try std.testing.expectEqualSlices(u32, &positions, acc.all_positions.items[offset..][0..256]);
                offset += 256;
            }
        }
        try std.testing.expectEqual(@as(u32, 64), docs);
        std.debug.print("LITE_PACKED_SPILL layout={s} docs=64 positions=16384 raw_position_bytes=65536 packed_run_bytes={d}\n", .{ if (legacy_header) "v38" else "current", stream.sorter.input_bytes });
        const Harness = struct {
            fn run(allocator: Allocator, input_entries: []const ?TermIterator.Entry, input_maps: []const FileDocMap) !void {
                var current = try ExternalPostingStream.init(allocator, input_entries, "common", input_maps);
                defer current.deinit();
                var values = PostingAccumulator.init();
                defer values.deinit(allocator);
                var refs = std.ArrayListUnmanaged(PackedPositionView).empty;
                defer refs.deinit(allocator);
                while (try current.packedBlock(&values, &refs, 7)) for (refs.items) |view| {
                    var cursor = try view.cursorAlloc(allocator);
                    defer cursor.deinit();
                    while (try cursor.next()) |_| {}
                };
            }
        };
        var no_resize = @import("../storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = a };
        try std.testing.checkAllAllocationFailures(no_resize.allocator(), Harness.run, .{ @as([]const ?TermIterator.Entry, &entries), @as([]const FileDocMap, &maps) });
    }
}

test "streamed inline position views unwind every allocation failure" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    try builder.addDocument(0, &.{.{ .term = "inline", .freq = 3, .positions = &.{ 0, 2, 5 } }});
    const bytes = try builder.build();
    defer a.free(bytes);
    const reader = try InvertedIndexReader.init(a, bytes);
    const result = reader.lookup("inline").?;
    const Harness = struct {
        fn run(allocator: Allocator, source: LookupResult) !void {
            var sink = MergeMemorySink{ .alloc = allocator };
            defer sink.deinit();
            const placeholder: [v7_header_size]u8 = @splat(0);
            try sink.appendSlice(&placeholder);
            const entries = [_]?TermIterator.Entry{.{ .term = "inline", .result = source }};
            const ids = [_]u32{0};
            const maps = [_][]const u32{&ids};
            var norms = [_]u32{0};
            var total: u64 = 0;
            _ = (try appendStreamedMergedTermToSink(allocator, &sink, 0, &entries, "inline", &maps, &norms, &total, .{})) orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u64, 3), total);
        }
    };
    try Harness.run(a, result);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{result});
}

test "packed span writer preserves every width alignment and bounded range reads" {
    const a = std.testing.allocator;
    const RangeReader = struct {
        data: []const u8,
        calls: usize = 0,
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset > self.data.len or output.len > self.data.len - offset) return error.EndOfStream;
            self.calls += 1;
            @memcpy(output, self.data[@intCast(offset)..][0..output.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var input: [8256]u8 = undefined;
    for (&input, 0..) |*byte_, i| byte_.* = @truncate(i * 73 + 19);
    var cases: usize = 0;
    var range_calls: usize = 0;
    for (0..33) |width| for ([_]usize{ 0, 1, 7 }) |start| for (0..8) |prefix| for ([_]usize{ 7, 2053 }) |count| {
        var expected = MergeMemorySink{ .alloc = a };
        defer expected.deinit();
        var reference = PositionByteWriter(*MergeMemorySink){ .sink = &expected };
        try reference.value(0x55 & ((@as(u32, 1) << @intCast(prefix)) - 1), @intCast(prefix));
        for (start * width..(start + count) * width) |bit| try reference.value((input[bit / 8] >> @as(u3, @intCast(bit % 8))) & 1, 1);
        try reference.alignByte();
        try reference.flush();
        for ([_]bool{ false, true }) |ranged| {
            var reader = RangeReader{ .data = &input };
            var actual = MergeMemorySink{ .alloc = a };
            defer actual.deinit();
            var writer = PositionByteWriter(*MergeMemorySink){ .sink = &actual };
            try writer.value(0x55 & ((@as(u32, 1) << @intCast(prefix)) - 1), @intCast(prefix));
            const view = PackedPositionView{
                .data = if (ranged) &.{} else &input,
                .range = if (ranged) try @import("../segment_source.zig").View.init(.{ .ranges = .{ .ptr = &reader, .length = input.len, .read_into = RangeReader.read, .close = RangeReader.close } }, 0, input.len) else null,
                .start_index = start,
                .count = count,
                .bits = @intCast(width),
            };
            try writer.appendPacked(view);
            try writer.alignByte();
            try writer.flush();
            try std.testing.expectEqualSlices(u8, expected.output.items, actual.output.items);
            try std.testing.expect(reader.calls <= 4);
            range_calls += reader.calls;
            cases += 1;
        }
    };
    std.debug.print("LITE_PACKED_SPANS cases={d} range_calls={d}\n", .{ cases, range_calls });
}

test "streamed positions preserve decoded prefix across packed crossover" {
    const a = std.testing.allocator;
    var positions: [16384]u32 = undefined;
    for (&positions, 0..) |*position, i| position.* = @intCast(i * 3);
    for ([_]usize{ 40, 16384 }) |large| {
        var input = InvertedIndexBuilder.init(a, .{});
        defer input.deinit();
        for (0..256) |doc| {
            const count: usize = if (doc % 3 == 1) large else 3;
            try input.addDocument(@intCast(doc), &.{.{ .term = "mixed", .freq = @intCast(count), .norm = 7, .positions = positions[0..count] }});
        }
        const bytes_ = try input.build();
        defer a.free(bytes_);
        const reader = try InvertedIndexReader.init(a, bytes_);
        var ids: [256]u32 = undefined;
        for (&ids, 0..) |*id, i| id.* = @intCast(i);
        var output = MergeMemorySink{ .alloc = a };
        defer output.deinit();
        try output.output.appendNTimes(a, 0, v7_header_size);
        var norms: [256]u32 = @splat(0);
        var total: u64 = 0;
        const entries = [_]?TermIterator.Entry{.{ .term = "mixed", .result = reader.lookup("mixed").? }};
        const maps = [_][]const u32{&ids};
        const value = (try appendStreamedMergedTermToSink(a, &output, 0, &entries, "mixed", &maps, &norms, &total, .{})).?;
        var output_reader = reader;
        output_reader.data = output.output.items;
        const term = output_reader.readPostings(value);
        var iterator = try term.iterator(a);
        defer iterator.deinit();
        var expected_total: u64 = 0;
        for (0..256) |doc| {
            const hit = (try iterator.next()).?;
            const count: usize = if (doc % 3 == 1) large else 3;
            try std.testing.expectEqual(@as(u32, @intCast(doc)), hit.doc_id);
            try std.testing.expectEqualSlices(u32, positions[0..count], hit.positions);
            expected_total += count;
        }
        try std.testing.expectEqual(expected_total, total);
        try std.testing.expect((try iterator.next()) == null);
        if (large == 40) {
            const Harness = struct {
                fn run(allocator: Allocator, input_entries: []const ?TermIterator.Entry, input_maps: []const []const u32) !void {
                    var sink = MergeMemorySink{ .alloc = allocator };
                    defer sink.deinit();
                    try sink.output.appendNTimes(allocator, 0, v7_header_size);
                    var output_norms: [256]u32 = @splat(0);
                    var field_length: u64 = 0;
                    _ = (try appendStreamedMergedTermToSink(allocator, &sink, 0, input_entries, "mixed", input_maps, &output_norms, &field_length, .{})) orelse return error.TestExpectedEqual;
                }
            };
            try std.testing.checkAllAllocationFailures(a, Harness.run, .{ @as([]const ?TermIterator.Entry, &entries), @as([]const []const u32, &maps) });
        }
    }
}

test "native scoped postings preserve prefetch through caches and translate section offsets" {
    const a = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..2048) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 1 }});
    const bytes = try builder.build();
    defer a.free(bytes);
    const prefix = 137;
    const backing = try a.alloc(u8, prefix + bytes.len);
    defer a.free(backing);
    @memset(backing[0..prefix], 0);
    @memcpy(backing[prefix..], bytes);
    const State = struct {
        bytes: []const u8,
        hints: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn prefetch(raw: *anyopaque, offset: u64, length: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(offset >= prefix and offset + length <= self.bytes.len);
            self.hints += 1;
        }
        fn close(_: *anyopaque) void {}
    };
    var state: State = .{ .bytes = backing };
    const sources = @import("../segment_source.zig");
    const source: sources.Source = .{ .ranges = .{ .ptr = &state, .length = backing.len, .read_into = State.read, .close = State.close, .prefetch = State.prefetch } };
    var concurrent = try sources.ConcurrentBlockCache.init(a, source, 64 * 1024);
    defer concurrent.deinit();
    var scoped = try ScopedInvertedIndexReader.initRanges(a, try sources.View.init(concurrent.borrowedSource(), prefix, bytes.len), .{});
    defer scoped.deinit();
    const lookup = (try scoped.lookup("common")).?;
    var iterator = try lookup.iterator(a);
    defer iterator.deinit();
    _ = try iterator.next();
    try std.testing.expect(state.hints != 0);
}

test "v23 positional merges and historical spills consume immutable packed records" {
    const a = std.testing.allocator;
    const documents = 256;
    const positions_per_doc = 4096;
    var positions: [positions_per_doc]u32 = undefined;
    for (&positions, 0..) |*position, i| position.* = @intCast(i);
    var builder = InvertedIndexBuilder.init(a, .{ .chunk_size = 16, .postings_layout = .legacy_fixture_v27 });
    defer builder.deinit();
    for (0..documents) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "legacy", .freq = positions.len, .norm = positions.len, .positions = &positions }});
    const bytes = try builder.build();
    defer a.free(bytes);
    // Test-only v27 supplies the same v23 document-range payload layout.
    // Its positions are replaced with genuine unframed v23 records below.
    bytes[4] = wire_version_compact_postings_header;
    var reader = try InvertedIndexReader.init(a, bytes);
    reader.version = wire_version_chunk_framed_positions;
    var result = reader.lookup("legacy").?;
    var legacy = std.ArrayListUnmanaged(u8).empty;
    defer legacy.deinit(a);
    for (0..documents) |_| try appendPackedPositionsForDoc(a, &legacy, &positions);
    result.postings.version = wire_version_legacy;
    result.postings.positions_data = legacy.items;
    result.postings.skip_data = null;
    var ids: [documents]u32 = undefined;
    for (&ids, 0..) |*id, doc| id.* = @intCast(doc);
    const State = struct {
        bytes: []const u8,
        reads: usize = 0,
        read_bytes: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            self.read_bytes += out.len;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    for ([_]bool{ false, true }) |ranged| {
        var state = State{ .bytes = legacy.items };
        const View = @import("../segment_source.zig").View;
        if (ranged) {
            result.postings.positions_data = null;
            result.postings.positions_range = try View.init(.{ .ranges = .{ .ptr = &state, .length = legacy.items.len, .read_into = State.read, .close = State.close } }, 0, legacy.items.len);
            // The immutable range can exceed the decode cap: only small
            // bounded windows are read, never a document-sized decode.
            result.postings.max_position_record_bytes = 128;
        }
        const entries = [_]?TermIterator.Entry{.{ .term = "legacy", .result = result }};
        var output = MergeMemorySink{ .alloc = a };
        defer output.deinit();
        try output.output.appendNTimes(a, 0, v7_header_size);
        var norms: [documents]u32 = @splat(0);
        var total: u64 = 0;
        const maps = [_][]const u32{&ids};
        const Budget = @import("../storage/lite/test_allocator.zig").BudgetAllocator;
        var budget = Budget{ .backing = a, .limit = 512 * 1024 };
        const start = @import("antfly_platform").time.monotonicNs();
        const value = (try appendStreamedMergedTermToSink(budget.allocator(), &output, 0, &entries, "legacy", &maps, &norms, &total, .{})).?;
        const elapsed = @import("antfly_platform").time.monotonicNs() - start;
        var output_reader = reader;
        output_reader.version = wire_version_current;
        output_reader.chunk_size = productionIndexConfig().chunk_size;
        output_reader.data = output.output.items;
        const merged = output_reader.readPostings(value);
        var iterator = try merged.iterator(a);
        defer iterator.deinit();
        for (0..documents) |doc| {
            const hit = (try iterator.next()).?;
            try std.testing.expectEqual(@as(u32, @intCast(doc)), hit.doc_id);
            try std.testing.expectEqualSlices(u32, &positions, hit.positions);
        }
        try std.testing.expect((try iterator.next()) == null);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        if (ranged) try std.testing.expect(state.read_bytes < legacy.items.len * 4);
        std.debug.print("LITE_LEGACY_PACKED ranged={any} docs=256 positions=4096 peak={d} elapsed_ns={d} source_reads={d} source_bytes={d}\n", .{ ranged, budget.peak, elapsed, state.reads, state.read_bytes });
        var mappings: [documents * 4]u8 = undefined;
        for (0..documents) |doc| std.mem.writeInt(u32, mappings[doc * 4 ..][0..4], @intCast(documents - 1 - doc), .little);
        const spill_maps = [_]FileDocMap{.{ .len = documents, .ids = try View.init(.{ .contiguous = &mappings }, 0, mappings.len), .records = try View.init(.{ .contiguous = &.{} }, 0, 0), .monotonic = false, .scratch = .{ .io = std.testing.io, .directory = directory, .chunk_records = 8 } }};
        var spill_budget = Budget{ .backing = a, .limit = 512 * 1024 };
        const spill_start = @import("antfly_platform").time.monotonicNs();
        var stream = try ExternalPostingStream.init(spill_budget.allocator(), &entries, "legacy", &spill_maps);
        const spill_elapsed = @import("antfly_platform").time.monotonicNs() - spill_start;
        defer stream.deinit();
        var acc = PostingAccumulator.init();
        defer acc.deinit(a);
        var doc: u32 = 0;
        while (try stream.block(&acc, 8)) {
            var offset: usize = 0;
            for (acc.doc_ids.items, acc.metas.items) |id, meta| {
                try std.testing.expectEqual(doc, id);
                doc += 1;
                try std.testing.expectEqual(@as(u32, positions_per_doc), meta.position_count);
                try std.testing.expectEqualSlices(u32, &positions, acc.all_positions.items[offset..][0..positions_per_doc]);
                offset += positions_per_doc;
            }
        }
        try std.testing.expectEqual(@as(u32, documents), doc);
        try std.testing.expect(stream.sorter.input_bytes < documents * positions_per_doc * 4 / 8);
        std.debug.print("LITE_LEGACY_SPILL ranged={any} elapsed_ns={d} peak={d} input_bytes={d}\n", .{ ranged, spill_elapsed, spill_budget.peak, stream.sorter.input_bytes });
    }
}

test "legacy packed position views survive iterator advancement and validate record bounds" {
    const a = std.testing.allocator;
    const expected = [_]u32{ 0, 3, 7, 9 };
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(a);
    try appendPackedPositionsForDoc(a, &data, &expected);
    try appendPackedPositionsForDoc(a, &data, &.{ 1, 1, 1 });
    try appendPackedPositionsForDoc(a, &data, &.{});
    var iterator = PostingsIterator{ .alloc = a, .version = wire_version_legacy, .doc_freq = 3, .positions_data = data.items, .current_chunk_index = 0 };
    defer iterator.deinit();
    try iterator.doc_values.appendSlice(a, &.{ 0, 1, 2 });
    try iterator.freq_values.appendSlice(a, &.{ @as(u32, @intCast(encodeFreqHasLocs(4, true))), @as(u32, @intCast(encodeFreqHasLocs(3, true))), @as(u32, @intCast(encodeFreqHasLocs(1, false))) });
    var cache: PackedReadCache = .{};
    const first = (try nextPackedHit(&iterator, &cache)).?;
    _ = (try nextPackedHit(&iterator, &cache)).?;
    try std.testing.expectEqual(@as(usize, 0), (try nextPackedHit(&iterator, &cache)).?.positions.count);
    var cursor = try first.positions.cursor();
    for (expected) |position| try std.testing.expectEqual(@as(?u32, position), try cursor.next());
    try std.testing.expectEqual(@as(?u32, null), try cursor.next());
    try std.testing.expectEqual(data.items.len, iterator.positions_cursor);
    iterator.positions_cursor = 0;
    iterator.chunk_doc_pos = 0;
    iterator.positions_data = data.items[0..3];
    try std.testing.expectError(error.InvalidData, nextPackedHit(&iterator, &cache));
}
