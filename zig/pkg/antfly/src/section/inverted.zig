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
//!   - Blocked term dictionary indexed by a Vellum FST (prefix → block offset)
//!   - Roaring bitmaps for posting lists (document ID sets)
//!   - Chunked int encoder for term frequencies and field norms
//!   - BM25 scoring
//!
//! Wire-compatible with zapx SectionInvertedTextIndex.

const std = @import("std");
const Allocator = std.mem.Allocator;
const roaring = @import("../encoding/roaring.zig");
const chunked = @import("../encoding/chunked_coder.zig");
const vellum = @import("antfly_vellum");
const bloom = @import("bloom");

// ============================================================================
// Wire format versions
// ============================================================================
//
//   v11: v10 postings + BlockTree-style blocked term dictionary
//
// This project is pre-release; writers emit the current v11 format.

const wire_version_current: u8 = 11;
const v7_header_size: usize = 4 + 1 + 4 + 8 + 4 + 4 + 4; // 29 bytes
const v7_chunk_meta_size: usize = 44;
const postings_header_size: usize = 24;
const postings_skip_record_size: usize = 8;
const postings_skip_stride_chunks: usize = 16;
const postings_skip_min_chunks: usize = postings_skip_stride_chunks * 2;
const term_block_prefix_len: usize = 2;
const term_dict_magic = "BTD1";
const term_dict_header_size: usize = 16;

/// Skip building a per-segment bloom filter when there are fewer terms than this.
/// FST traversal is already cheap for tiny term sets, and the filter would
/// dominate the section size.
const bloom_min_terms: usize = 64;

/// Write the 29-byte current section header into `dst[0..v7_header_size]`. Shared
/// between `InvertedIndexBuilder.build` and the merger's
/// `assembleMergedSection` so the wire layout lives in exactly one place.
fn writeCurrentHeader(
    dst: []u8,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    vellum_len: u32,
    bloom_len: u32,
) void {
    std.debug.assert(dst.len >= v7_header_size);
    @memcpy(dst[0..4], "INVT");
    dst[4] = wire_version_current;
    dst[5..9].* = @bitCast(std.mem.nativeToLittle(u32, doc_count));
    dst[9..17].* = @bitCast(std.mem.nativeToLittle(u64, total_field_len));
    dst[17..21].* = @bitCast(std.mem.nativeToLittle(u32, chunk_size));
    dst[21..25].* = @bitCast(std.mem.nativeToLittle(u32, vellum_len));
    dst[25..29].* = @bitCast(std.mem.nativeToLittle(u32, bloom_len));
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

const TermBloomHash = struct { h1: u64, h2: u64 };

const TermDictEntry = struct {
    term: []const u8,
    value: u64,
};

fn termBlockKey(term: []const u8) []const u8 {
    return term[0..@min(term.len, term_block_prefix_len)];
}

fn appendLeU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, value))));
}

fn encodeBlockedTermDictionary(alloc: Allocator, entries: []const TermDictEntry) ![]u8 {
    var block_data = std.ArrayListUnmanaged(u8).empty;
    defer block_data.deinit(alloc);

    const fst_registry_size: usize = std.math.clamp(entries.len, 64, 65_536);
    var block_fst_builder = try vellum.Builder.init(alloc, .{
        .registry_table_size = fst_registry_size,
    });
    defer block_fst_builder.deinit();

    var i: usize = 0;
    var block_count: u32 = 0;
    while (i < entries.len) {
        const prefix = termBlockKey(entries[i].term);
        const block_offset: u64 = @intCast(block_data.items.len);
        try block_fst_builder.insert(prefix, block_offset);

        var end = i + 1;
        while (end < entries.len and std.mem.eql(u8, termBlockKey(entries[end].term), prefix)) : (end += 1) {}

        try block_data.append(alloc, @intCast(prefix.len));
        try appendLeU32(alloc, &block_data, @intCast(end - i));
        try block_data.appendSlice(alloc, prefix);

        for (entries[i..end]) |entry| {
            const suffix = entry.term[prefix.len..];
            try writeVarintU32(alloc, &block_data, @intCast(suffix.len));
            try block_data.appendSlice(alloc, suffix);
            try writeVarintU64(alloc, &block_data, entry.value);
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
    try appendLeU32(alloc, &out, @intCast(block_fst.len));
    try out.appendSlice(alloc, block_data.items);
    try out.appendSlice(alloc, block_fst);
    return try out.toOwnedSlice(alloc);
}

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
    doc_count: u32 = 0,
    /// Total tokens across all documents (for avgdl)
    total_field_len: u64 = 0,

    pub fn init(alloc: Allocator, config: IndexConfig) InvertedIndexBuilder {
        return .{
            .alloc = alloc,
            .config = config,
            .terms = .empty,
            .term_arena = std.heap.ArenaAllocator.init(alloc),
        };
    }

    pub fn deinit(self: *InvertedIndexBuilder) void {
        var it = self.terms.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.alloc);
        }
        self.terms.deinit(self.alloc);
        self.term_arena.deinit();
    }

    pub fn estimatedMemoryBytes(self: *const InvertedIndexBuilder) u64 {
        var total: u64 = @as(u64, @intCast(self.terms.capacity())) * @sizeOf(std.StringHashMapUnmanaged(PostingAccumulator).Entry);
        var it = self.terms.iterator();
        while (it.next()) |entry| {
            total +|= @intCast(entry.key_ptr.*.len);
            total +|= entry.value_ptr.estimatedMemoryBytes();
        }
        return total;
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
        for (hits) |hit| {
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
        self.total_field_len += freq;
    }

    /// Build the serialized inverted index section.
    /// Caller owns returned bytes.
    ///
    /// Layout (v4, with Vellum FST + 1-hit optimization + block-max):
    ///   [header: 25 bytes]
    ///   [postings_data]
    ///   [vellum FST data]
    ///
    /// Header:
    ///   magic: "INVT" (4 bytes)
    ///   version: u8 = 4
    ///   doc_count: u32 LE
    ///   total_field_len: u64 LE
    ///   chunk_size: u32 LE
    ///   vellum_len: u32 LE  — length of Vellum FST data (at end of section)
    ///
    /// FST values:
    ///   - General: postings offset within postings_data
    ///   - 1-hit: packed docNum + normBits (for single-doc, freq=1 terms)
    ///
    /// Postings per term, v10:
    ///   [doc_freq: u32 LE]
    ///   [stored_chunks: u32 LE]
    ///   [block_max_base_chunk: u32 LE]
    ///   [block_max_chunks: u32 LE]
    ///   [positions_section_len: u32 LE]
    ///   [block_max_chunks × 6-byte block-max records]
    ///   [stored_chunks × chunk metadata]
    ///   [packed per-chunk doc-delta/freqHasLocs/norm payloads]
    ///   [positions: varint count + varint deltas per doc]
    ///
    /// v10 stores postings first, then an optional per-segment term bloom filter,
    /// then the FST. `bloom_len` in the header tells the reader how many bytes
    /// to skip past postings before the FST starts.
    pub fn build(self: *InvertedIndexBuilder) ![]u8 {
        return self.buildAlloc(self.alloc);
    }

    pub fn buildAlloc(self: *InvertedIndexBuilder, output_alloc: Allocator) ![]u8 {
        const scratch_alloc = output_alloc;
        const term_count = self.terms.count();
        if (term_count == 0) return try output_alloc.dupe(u8, &.{});

        // Step 1: Sort terms
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
                const norm_bits: u64 = acc.metas.items[0].norm;
                dict_value = fstValEncode1Hit(doc_num, norm_bits);
            } else {
                const postings_offset: u64 = @intCast(output.items.len - v7_header_size);
                try acc.serializeV9(output_alloc, &output, &serialize_scratch, self.config.chunk_size);
                dict_value = postings_offset;
            }
            dict_entries[term_idx] = .{
                .term = term,
                .value = dict_value,
            };
        }

        const term_dict_data = try encodeBlockedTermDictionary(scratch_alloc, dict_entries);
        defer scratch_alloc.free(term_dict_data);

        // Encode bloom (if any) into a single buffer that we'll inline into
        // the section. The on-disk payload is the standard `lib/bloom` magic +
        // version + bit_count + hash_count + bytes envelope.
        var bloom_bytes: []const u8 = &.{};
        defer if (bloom_bytes.len > 0) scratch_alloc.free(@constCast(bloom_bytes));
        if (bloom_builder) |*b| {
            var filter = b.finish();
            // `finish` consumes the builder (sets it to undefined); null out the
            // option so the errdefer above is a no-op.
            bloom_builder = null;
            defer filter.deinit(scratch_alloc);
            bloom_bytes = try filter.encodeAlloc(scratch_alloc);
        }

        // Step 3: Finish final section.
        if (bloom_bytes.len > 0) {
            try output.appendSlice(output_alloc, bloom_bytes);
        }
        try output.appendSlice(output_alloc, term_dict_data);

        writeCurrentHeader(
            output.items[0..v7_header_size],
            self.doc_count,
            self.total_field_len,
            self.config.chunk_size,
            @intCast(term_dict_data.len),
            @intCast(bloom_bytes.len),
        );

        return try output.toOwnedSlice(output_alloc);
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

const PostingSerializeScratch = struct {
    chunks: std.ArrayListUnmanaged(V7ChunkMeta) = .empty,
    doc_deltas: std.ArrayListUnmanaged(u32) = .empty,
    freq_values: std.ArrayListUnmanaged(u32) = .empty,
    norm_values: std.ArrayListUnmanaged(u32) = .empty,
    block_max: std.ArrayListUnmanaged(u8) = .empty,
    payload: std.ArrayListUnmanaged(u8) = .empty,
    positions: std.ArrayListUnmanaged(u8) = .empty,
    skip: std.ArrayListUnmanaged(u8) = .empty,
    svb_control: std.ArrayListUnmanaged(u8) = .empty,
    svb_data: std.ArrayListUnmanaged(u8) = .empty,

    fn reset(self: *PostingSerializeScratch) void {
        self.chunks.clearRetainingCapacity();
        self.doc_deltas.clearRetainingCapacity();
        self.freq_values.clearRetainingCapacity();
        self.norm_values.clearRetainingCapacity();
        self.block_max.clearRetainingCapacity();
        self.payload.clearRetainingCapacity();
        self.positions.clearRetainingCapacity();
        self.skip.clearRetainingCapacity();
        self.svb_control.clearRetainingCapacity();
        self.svb_data.clearRetainingCapacity();
    }

    fn deinit(self: *PostingSerializeScratch, alloc: Allocator) void {
        self.chunks.deinit(alloc);
        self.doc_deltas.deinit(alloc);
        self.freq_values.deinit(alloc);
        self.norm_values.deinit(alloc);
        self.block_max.deinit(alloc);
        self.payload.deinit(alloc);
        self.positions.deinit(alloc);
        self.skip.deinit(alloc);
        self.svb_control.deinit(alloc);
        self.svb_data.deinit(alloc);
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
    }
}

fn writeChunkMetaV7(dst: []u8, meta: V7ChunkMeta) void {
    std.debug.assert(dst.len >= v7_chunk_meta_size);
    dst[0..4].* = @bitCast(std.mem.nativeToLittle(u32, meta.chunk_id));
    dst[4..8].* = @bitCast(std.mem.nativeToLittle(u32, meta.max_doc));
    dst[8..12].* = @bitCast(std.mem.nativeToLittle(u32, meta.doc_count));
    dst[12..16].* = @bitCast(std.mem.nativeToLittle(u32, meta.doc_ctrl_off));
    dst[16..20].* = @bitCast(std.mem.nativeToLittle(u32, meta.doc_ctrl_len));
    dst[20..24].* = @bitCast(std.mem.nativeToLittle(u32, meta.doc_data_off));
    dst[24..28].* = @bitCast(std.mem.nativeToLittle(u32, meta.doc_data_len));
    dst[28..32].* = @bitCast(std.mem.nativeToLittle(u32, meta.freq_ctrl_off));
    dst[32..36].* = @bitCast(std.mem.nativeToLittle(u32, meta.freq_ctrl_len));
    dst[36..40].* = @bitCast(std.mem.nativeToLittle(u32, meta.freq_data_off));
    dst[40..44].* = @bitCast(std.mem.nativeToLittle(u32, meta.freq_data_len));
}

fn readChunkMetaV7(src: []const u8) V7ChunkMeta {
    std.debug.assert(src.len >= v7_chunk_meta_size);
    return .{
        .chunk_id = std.mem.readInt(u32, src[0..4], .little),
        .max_doc = std.mem.readInt(u32, src[4..8], .little),
        .doc_count = std.mem.readInt(u32, src[8..12], .little),
        .doc_ctrl_off = std.mem.readInt(u32, src[12..16], .little),
        .doc_ctrl_len = std.mem.readInt(u32, src[16..20], .little),
        .doc_data_off = std.mem.readInt(u32, src[20..24], .little),
        .doc_data_len = std.mem.readInt(u32, src[24..28], .little),
        .freq_ctrl_off = std.mem.readInt(u32, src[28..32], .little),
        .freq_ctrl_len = std.mem.readInt(u32, src[32..36], .little),
        .freq_data_off = std.mem.readInt(u32, src[36..40], .little),
        .freq_data_len = std.mem.readInt(u32, src[40..44], .little),
    };
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
    if (bits == 0) {
        @memset(values, 0);
        return;
    }
    const needed = packedU32ByteLen(values.len, bits);
    if (data.len < needed) return error.InvalidData;

    var bit_pos: usize = 0;
    for (values) |*value| {
        var out: u32 = 0;
        var shift: u5 = 0;
        var remaining = bits;
        while (remaining > 0) {
            const byte_index = bit_pos / 8;
            const bit_in_byte: u3 = @intCast(bit_pos % 8);
            const avail: u8 = 8 - @as(u8, bit_in_byte);
            const take: u8 = @min(remaining, avail);
            const mask: u8 = if (take == 8) 0xff else @as(u8, @truncate((@as(u16, 1) << @intCast(take)) - 1));
            const part: u32 = (data[byte_index] >> bit_in_byte) & mask;
            out |= part << shift;
            shift += @intCast(take);
            remaining -= take;
            bit_pos += take;
        }
        value.* = out;
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
        var shift: u5 = 0;
        var remaining = bits;
        while (remaining > 0) {
            const byte_index = bit_pos / 8;
            const bit_in_byte: u3 = @intCast(bit_pos % 8);
            const avail: u8 = 8 - @as(u8, bit_in_byte);
            const take: u8 = @min(remaining, avail);
            const mask: u8 = if (take == 8) 0xff else @as(u8, @truncate((@as(u16, 1) << @intCast(take)) - 1));
            const part: u32 = (data[byte_index] >> bit_in_byte) & mask;
            value |= part << shift;
            shift += @intCast(take);
            remaining -= take;
            bit_pos += take;
        }
        out[start + i * stride] = value;
    }
}

fn appendPackedPostingChunk(
    alloc: Allocator,
    payload: *std.ArrayListUnmanaged(u8),
    doc_values: []const u32,
    freq_values: []const u32,
    norm_values: []const u32,
) !struct { off: u32, len: u32 } {
    std.debug.assert(doc_values.len == freq_values.len);
    std.debug.assert(doc_values.len == norm_values.len);

    const off: u32 = @intCast(payload.items.len);
    const doc_bits = maxBitWidth(doc_values);
    const freq_bits = maxBitWidth(freq_values);
    const norm_bits = maxBitWidth(norm_values);
    try payload.appendSlice(alloc, &.{ doc_bits, freq_bits, norm_bits, 0 });
    _ = try appendPackedU32(alloc, payload, doc_values, doc_bits);
    _ = try appendPackedU32(alloc, payload, freq_values, freq_bits);
    _ = try appendPackedU32(alloc, payload, norm_values, norm_bits);
    return .{ .off = off, .len = @intCast(payload.items.len - off) };
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
        chunk_size: u32,
    ) !void {
        scratch.reset();
        const doc_freq: u32 = @intCast(self.doc_ids.items.len);
        if (doc_freq == 0) return error.InvalidData;

        const first_chunk_id = self.doc_ids.items[0] / chunk_size;
        const last_doc_id = self.doc_ids.items[self.doc_ids.items.len - 1];
        const last_chunk_id = last_doc_id / chunk_size;
        const num_doc_chunks = std.math.add(u32, last_chunk_id - first_chunk_id, 1) catch return error.DocumentOutOfRange;
        try scratch.block_max.resize(alloc, @as(usize, num_doc_chunks) * 6);
        var chunk_idx: usize = 0;
        while (chunk_idx < num_doc_chunks) : (chunk_idx += 1) {
            const off = chunk_idx * 6;
            scratch.block_max.items[off..][0..2].* = .{ 0, 0 };
            scratch.block_max.items[off + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, std.math.maxInt(u16)));
            scratch.block_max.items[off + 4 ..][0..2].* = .{ 0, 0 };
        }

        var pos_offset: usize = 0;
        var doc_start: usize = 0;
        while (doc_start < self.doc_ids.items.len) {
            const chunk_id = self.doc_ids.items[doc_start] / chunk_size;
            var doc_end = doc_start + 1;
            while (doc_end < self.doc_ids.items.len and self.doc_ids.items[doc_end] / chunk_size == chunk_id) : (doc_end += 1) {}

            scratch.doc_deltas.clearRetainingCapacity();
            scratch.freq_values.clearRetainingCapacity();
            scratch.norm_values.clearRetainingCapacity();
            try scratch.doc_deltas.ensureTotalCapacity(alloc, doc_end - doc_start);
            try scratch.freq_values.ensureTotalCapacity(alloc, doc_end - doc_start);
            try scratch.norm_values.ensureTotalCapacity(alloc, doc_end - doc_start);

            var prev_doc: u32 = 0;
            var i = doc_start;
            while (i < doc_end) : (i += 1) {
                const doc_id = self.doc_ids.items[i];
                const meta = self.metas.items[i];
                scratch.doc_deltas.appendAssumeCapacity(if (i == doc_start) doc_id - chunk_id * chunk_size else doc_id - prev_doc);
                const encoded_freq_has_locs: u32 = @intCast(encodeFreqHasLocs(meta.freq, false));
                scratch.freq_values.appendAssumeCapacity(encoded_freq_has_locs);
                scratch.norm_values.appendAssumeCapacity(meta.norm);
                prev_doc = doc_id;

                const bm_off = @as(usize, chunk_id - first_chunk_id) * 6;
                const freq_u16: u16 = if (meta.freq > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.freq);
                const norm_u16: u16 = if (meta.norm > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.norm);
                const cur_max_freq = std.mem.readInt(u16, scratch.block_max.items[bm_off..][0..2], .little);
                const cur_min_norm = std.mem.readInt(u16, scratch.block_max.items[bm_off + 2 ..][0..2], .little);
                const cur_max_norm = std.mem.readInt(u16, scratch.block_max.items[bm_off + 4 ..][0..2], .little);
                if (freq_u16 > cur_max_freq) scratch.block_max.items[bm_off..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, freq_u16));
                if (norm_u16 < cur_min_norm) scratch.block_max.items[bm_off + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, norm_u16));
                if (norm_u16 > cur_max_norm) scratch.block_max.items[bm_off + 4 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, norm_u16));

                const count = meta.position_count;
                try writeVarintU32(alloc, &scratch.positions, count);
                if (count > 0) {
                    var prev_pos: u32 = 0;
                    var p_i: usize = 0;
                    while (p_i < count) : (p_i += 1) {
                        const p = self.all_positions.items[pos_offset + p_i];
                        try writeVarintU32(alloc, &scratch.positions, if (p >= prev_pos) p - prev_pos else 0);
                        prev_pos = p;
                    }
                }
                pos_offset += count;
            }

            const packed_chunk = try appendPackedPostingChunk(
                alloc,
                &scratch.payload,
                scratch.doc_deltas.items,
                scratch.freq_values.items,
                scratch.norm_values.items,
            );
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

            doc_start = doc_end;
        }

        try appendPostingSkipData(alloc, &scratch.skip, scratch.chunks.items);

        const term_start = out.items.len;
        const total_len = postings_header_size + scratch.block_max.items.len + scratch.chunks.items.len * v7_chunk_meta_size + scratch.payload.items.len + scratch.positions.items.len + scratch.skip.items.len;
        try out.ensureUnusedCapacity(alloc, total_len);
        out.items.len += total_len;
        const dst = out.items[term_start..][0..total_len];
        dst[0..4].* = @bitCast(std.mem.nativeToLittle(u32, doc_freq));
        dst[4..8].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(scratch.chunks.items.len))));
        dst[8..12].* = @bitCast(std.mem.nativeToLittle(u32, first_chunk_id));
        dst[12..16].* = @bitCast(std.mem.nativeToLittle(u32, num_doc_chunks));
        dst[16..20].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(scratch.positions.items.len))));
        dst[20..24].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(scratch.skip.items.len))));
        var dst_pos: usize = postings_header_size;
        @memcpy(dst[dst_pos..][0..scratch.block_max.items.len], scratch.block_max.items);
        dst_pos += scratch.block_max.items.len;
        for (scratch.chunks.items) |chunk| {
            writeChunkMetaV7(dst[dst_pos..][0..v7_chunk_meta_size], chunk);
            dst_pos += v7_chunk_meta_size;
        }
        @memcpy(dst[dst_pos..][0..scratch.payload.items.len], scratch.payload.items);
        dst_pos += scratch.payload.items.len;
        @memcpy(dst[dst_pos..][0..scratch.positions.items.len], scratch.positions.items);
        dst_pos += scratch.positions.items.len;
        @memcpy(dst[dst_pos..][0..scratch.skip.items.len], scratch.skip.items);
    }
};

// ============================================================================
// Index reader (query path)
// ============================================================================

/// Reads a serialized inverted index section (v11, with blocked term dictionary).
pub const InvertedIndexReader = struct {
    alloc: Allocator,
    data: []const u8,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    postings_offset: usize,
    version: u8,
    dict_blocks: []const u8,
    dict_fst: vellum.FST,
    /// Optional per-segment term bloom filter. When present, callers can
    /// reject absent terms before walking the FST. Borrows into `data`.
    term_bloom: ?bloom.BorrowedFilter,

    pub fn init(alloc: Allocator, data: []const u8) !InvertedIndexReader {
        if (data.len < v7_header_size) return error.InvalidData;
        if (!std.mem.eql(u8, data[0..4], "INVT")) return error.InvalidMagic;
        const version = data[4];
        if (version != wire_version_current) return error.UnsupportedVersion;

        const doc_count = std.mem.readInt(u32, data[5..9], .little);
        const total_field_len = std.mem.readInt(u64, data[9..17], .little);
        const chunk_size = std.mem.readInt(u32, data[17..21], .little);
        const dict_len = std.mem.readInt(u32, data[21..25], .little);
        const bloom_len = std.mem.readInt(u32, data[25..29], .little);
        const postings_offset: usize = v7_header_size;

        if (dict_len < term_dict_header_size or dict_len > data.len) return error.InvalidData;
        const dict_offset = data.len - dict_len;
        const dict_data = data[dict_offset..];
        if (!std.mem.eql(u8, dict_data[0..4], term_dict_magic)) return error.InvalidData;
        const block_data_len = std.mem.readInt(u32, dict_data[8..12], .little);
        const block_fst_len = std.mem.readInt(u32, dict_data[12..16], .little);
        if (term_dict_header_size + @as(usize, block_data_len) + @as(usize, block_fst_len) != dict_data.len) return error.InvalidData;
        const block_data = dict_data[term_dict_header_size..][0..block_data_len];
        const block_fst_data = dict_data[term_dict_header_size + @as(usize, block_data_len) ..];
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
            .version = version,
            .dict_blocks = block_data,
            .dict_fst = dict_fst,
            .term_bloom = term_bloom,
        };
    }

    /// Average document length for BM25.
    pub fn avgDocLen(self: *const InvertedIndexReader) f32 {
        if (self.doc_count == 0) return 0;
        return @as(f32, @floatFromInt(self.total_field_len)) / @as(f32, @floatFromInt(self.doc_count));
    }

    pub const LayoutStats = struct {
        header_bytes: u64 = 0,
        fst_bytes: u64 = 0,
        bloom_bytes: u64 = 0,
        postings_header_bytes: u64 = 0,
        block_max_bytes: u64 = 0,
        chunk_meta_bytes: u64 = 0,
        postings_payload_bytes: u64 = 0,
        positions_bytes: u64 = 0,
        skip_bytes: u64 = 0,
        one_hit_terms: u64 = 0,
        postings_terms: u64 = 0,
    };

    pub fn layoutStats(self: *const InvertedIndexReader) LayoutStats {
        const dict_len = std.mem.readInt(u32, self.data[21..25], .little);
        const bloom_len = std.mem.readInt(u32, self.data[25..29], .little);
        return .{
            .header_bytes = v7_header_size,
            .fst_bytes = dict_len,
            .bloom_bytes = bloom_len,
        };
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
                    stats.postings_header_bytes +|= postings_header_size;
                    if (postings.block_max) |block_max_info| {
                        stats.block_max_bytes +|= @intCast(block_max_info.meta.len);
                    }
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
        const block_key = termBlockKey(term);
        const result = self.dict_fst.get(block_key) catch return null;
        if (!result.found) return null;
        const dict_value = self.lookupInTermBlock(@intCast(result.val), term) catch return null;

        if (fstValIs1Hit(dict_value)) {
            const decoded = fstValDecode1Hit(dict_value);
            return .{ .one_hit = .{
                .doc_num = @intCast(decoded.doc_num),
                .norm_bits = @intCast(decoded.norm_bits),
            } };
        }

        return .{ .postings = self.readPostings(@intCast(dict_value)) };
    }

    /// Iterate all terms in the dictionary using the block-prefix FST iterator.
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
            .block_iter = try self.dict_fst.iterator(self.alloc, if (start) |s| termBlockKey(s) else null, null),
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

    fn lookupInTermBlock(self: *const InvertedIndexReader, block_offset: u32, term: []const u8) !u64 {
        if (block_offset >= self.dict_blocks.len) return error.InvalidData;
        var cursor: usize = block_offset;
        const prefix_len = self.dict_blocks[cursor];
        cursor += 1;
        if (cursor + 4 > self.dict_blocks.len) return error.Truncated;
        const entry_count = std.mem.readInt(u32, self.dict_blocks[cursor..][0..4], .little);
        cursor += 4;
        if (cursor + prefix_len > self.dict_blocks.len) return error.Truncated;
        const prefix = self.dict_blocks[cursor..][0..prefix_len];
        cursor += prefix_len;
        if (!std.mem.startsWith(u8, term, prefix)) return error.NotFound;
        const wanted_suffix = term[prefix.len..];

        var i: u32 = 0;
        while (i < entry_count) : (i += 1) {
            const suffix_len = try readVarintU32(self.dict_blocks, &cursor);
            if (cursor + suffix_len > self.dict_blocks.len) return error.Truncated;
            const suffix = self.dict_blocks[cursor..][0..suffix_len];
            cursor += suffix_len;
            const value = try readVarintU64(self.dict_blocks, &cursor);
            if (std.mem.eql(u8, suffix, wanted_suffix)) return value;
        }
        return error.NotFound;
    }

    fn readPostings(self: *const InvertedIndexReader, offset: u32) TermPostings {
        const base = self.postings_offset + offset;
        const doc_freq = std.mem.readInt(u32, self.data[base..][0..4], .little);
        const stored_chunks = std.mem.readInt(u32, self.data[base + 4 ..][0..4], .little);
        const block_max_base_chunk = std.mem.readInt(u32, self.data[base + 8 ..][0..4], .little);
        const doc_chunks = std.mem.readInt(u32, self.data[base + 12 ..][0..4], .little);
        const positions_len = std.mem.readInt(u32, self.data[base + 16 ..][0..4], .little);
        const skip_len = std.mem.readInt(u32, self.data[base + 20 ..][0..4], .little);
        const block_max_start = base + postings_header_size;
        const block_max_len = @as(usize, doc_chunks) * 6;
        const chunk_meta_start = block_max_start + block_max_len;
        const chunk_meta_len = @as(usize, stored_chunks) * v7_chunk_meta_size;
        const payload_start = chunk_meta_start + chunk_meta_len;
        var payload_len: usize = 0;
        var chunk_idx: usize = 0;
        while (chunk_idx < stored_chunks) : (chunk_idx += 1) {
            const meta = readChunkMetaV7(self.data[chunk_meta_start + chunk_idx * v7_chunk_meta_size ..][0..v7_chunk_meta_size]);
            payload_len = @max(payload_len, @as(usize, meta.doc_ctrl_off) + meta.doc_ctrl_len);
            payload_len = @max(payload_len, @as(usize, meta.doc_data_off) + meta.doc_data_len);
            payload_len = @max(payload_len, @as(usize, meta.freq_ctrl_off) + meta.freq_ctrl_len);
            payload_len = @max(payload_len, @as(usize, meta.freq_data_off) + meta.freq_data_len);
        }
        const positions_start = payload_start + payload_len;
        const skip_start = positions_start + positions_len;
        const after_postings = skip_start + skip_len;

        return .{
            .doc_freq = doc_freq,
            .serialized_data = self.data[base..after_postings],
            .chunk_size = self.chunk_size,
            .version = self.version,
            .block_max = .{
                .base_chunk = block_max_base_chunk,
                .num_chunks = doc_chunks,
                .meta = self.data[block_max_start..][0..block_max_len],
            },
            .chunk_meta_data = self.data[chunk_meta_start..][0..chunk_meta_len],
            .payload_data = self.data[payload_start..][0..payload_len],
            .positions_data = if (positions_len > 0) self.data[positions_start..][0..positions_len] else null,
            .skip_data = if (skip_len > 0) self.data[skip_start..][0..skip_len] else null,
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
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
    automaton: ?vellum.Automaton = null,
    // We must copy the key before advancing, because block parsing reuses section slices.
    current_key: std.ArrayListUnmanaged(u8) = .empty,

    pub const Entry = struct { term: []const u8, result: LookupResult };

    pub fn next(self: *TermIterator) !?Entry {
        while (true) {
            if (self.current_block_remaining == 0) {
                if (!try self.loadNextBlock()) return null;
            }

            const suffix_len = readVarintU32(self.reader.dict_blocks, &self.current_block_cursor) catch return error.InvalidData;
            if (self.current_block_cursor + suffix_len > self.reader.dict_blocks.len) return error.InvalidData;
            const suffix = self.reader.dict_blocks[self.current_block_cursor..][0..suffix_len];
            self.current_block_cursor += suffix_len;
            const value = readVarintU64(self.reader.dict_blocks, &self.current_block_cursor) catch return error.InvalidData;
            self.current_block_remaining -= 1;

            self.current_key.clearRetainingCapacity();
            try self.current_key.appendSlice(self.alloc, self.current_block_prefix);
            try self.current_key.appendSlice(self.alloc, suffix);

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
                    .norm_bits = @intCast(fstValDecode1Hit(value).norm_bits),
                } }
            else
                .{ .postings = self.reader.readPostings(@intCast(value)) };

            return .{ .term = self.current_key.items, .result = result };
        }
    }

    fn loadNextBlock(self: *TermIterator) !bool {
        const current = self.block_iter.current() orelse return false;
        _ = try self.block_iter.nextEntry();
        const block_offset: usize = @intCast(current.val);
        if (block_offset >= self.reader.dict_blocks.len) return error.InvalidData;
        var cursor = block_offset;
        const prefix_len = self.reader.dict_blocks[cursor];
        cursor += 1;
        if (cursor + 4 > self.reader.dict_blocks.len) return error.InvalidData;
        self.current_block_remaining = std.mem.readInt(u32, self.reader.dict_blocks[cursor..][0..4], .little);
        cursor += 4;
        if (cursor + prefix_len > self.reader.dict_blocks.len) return error.InvalidData;
        self.current_block_prefix = self.reader.dict_blocks[cursor..][0..prefix_len];
        cursor += prefix_len;
        self.current_block_cursor = cursor;
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
/// Each chunk stores (max_freq, min_norm, max_norm) packed as 6 bytes.
pub const BlockMaxInfo = struct {
    /// First global doc chunk covered by `meta`. v8 stores sparse block-max
    /// windows per term instead of padding every term to segment max_doc.
    base_chunk: u32 = 0,
    num_chunks: u32,
    /// Packed [max_freq:u16 LE][min_norm:u16 LE][max_norm:u16 LE] per chunk.
    meta: []const u8,

    /// Compute the maximum possible BM25 impact for a chunk.
    /// Uses the most favorable values in the chunk: max_freq and min_norm (shortest doc).
    pub fn maxImpact(self: BlockMaxInfo, chunk_idx: u32, doc_count: u32, doc_freq: u32, avg_dl: f32, config: BM25Config) f32 {
        if (chunk_idx < self.base_chunk) return 0;
        const local_chunk = chunk_idx - self.base_chunk;
        if (local_chunk >= self.num_chunks) return 0;
        const offset = @as(usize, local_chunk) * 6;
        const max_freq = std.mem.readInt(u16, self.meta[offset..][0..2], .little);
        const min_norm = std.mem.readInt(u16, self.meta[offset + 2 ..][0..2], .little);
        if (max_freq == 0) return 0;
        // Use min_norm as doc_len (shortest doc → highest TF component)
        return bm25Score(max_freq, min_norm, doc_count, doc_freq, avg_dl, config);
    }
};

/// Parsed posting data for a single term (zero-copy view into section data).
pub const TermPostings = struct {
    doc_freq: u32,
    serialized_data: []const u8,
    chunk_size: u32,
    version: u8,
    block_max: ?BlockMaxInfo = null,
    chunk_meta_data: []const u8,
    payload_data: []const u8,
    positions_data: ?[]const u8 = null,
    skip_data: ?[]const u8 = null,

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
        return .{
            .alloc = alloc,
            .chunk_size = self.chunk_size,
            .chunk_meta_data = self.chunk_meta_data,
            .payload_data = self.payload_data,
            .version = self.version,
            .positions_data = self.positions_data,
            .skip_data = self.skip_data,
        };
    }
};

/// Iterates over (doc_id, freq, norm, positions) for a term's posting list.
pub const PostingsIterator = struct {
    alloc: Allocator,
    chunk_size: u32 = 0,
    chunk_meta_data: []const u8 = &.{},
    payload_data: []const u8 = &.{},
    current_chunk_index: usize = std.math.maxInt(usize),
    next_chunk_index: usize = 0,
    chunk_doc_pos: usize = 0,
    version: u8 = wire_version_current,
    positions_data: ?[]const u8 = null,
    skip_data: ?[]const u8 = null,
    positions_cursor: usize = 0,
    doc_values: std.ArrayListUnmanaged(u32) = .empty,
    freq_norm_values: std.ArrayListUnmanaged(u32) = .empty,
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

    fn chunkCount(self: *const PostingsIterator) usize {
        return self.chunk_meta_data.len / v7_chunk_meta_size;
    }

    fn chunkMeta(self: *const PostingsIterator, index: usize) V7ChunkMeta {
        return readChunkMetaV7(self.chunk_meta_data[index * v7_chunk_meta_size ..][0..v7_chunk_meta_size]);
    }

    fn skipRecordCount(self: *const PostingsIterator) usize {
        const data = self.skip_data orelse return 0;
        return data.len / postings_skip_record_size;
    }

    fn skipRecord(self: *const PostingsIterator, index: usize) struct { max_doc: u32, chunk_index: usize } {
        const data = self.skip_data.?;
        const base = index * postings_skip_record_size;
        return .{
            .max_doc = std.mem.readInt(u32, data[base..][0..4], .little),
            .chunk_index = @intCast(std.mem.readInt(u32, data[base + 4 ..][0..4], .little)),
        };
    }

    fn skipWindowForTarget(self: *const PostingsIterator, target: u32) struct { lo: usize, hi: usize } {
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

    fn nextChunkIndexForTarget(self: *const PostingsIterator, target: u32) usize {
        const window = self.skipWindowForTarget(target);
        var lo = window.lo;
        var hi = window.hi;
        if (lo >= hi) return lo;
        if (self.skipRecordCount() > 0) {
            while (lo < hi) : (lo += 1) {
                const meta = self.chunkMeta(lo);
                if (meta.max_doc >= target) return lo;
            }
            return hi;
        }
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const meta = self.chunkMeta(mid);
            if (meta.max_doc < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    fn loadChunk(self: *PostingsIterator, index: usize) !void {
        const meta = self.chunkMeta(index);
        self.doc_values.clearRetainingCapacity();
        try self.doc_values.ensureTotalCapacity(self.alloc, meta.doc_count);
        self.doc_values.items.len = meta.doc_count;

        self.freq_norm_values.clearRetainingCapacity();
        try self.freq_norm_values.ensureTotalCapacity(self.alloc, meta.doc_count * 2);
        self.freq_norm_values.items.len = meta.doc_count * 2;

        const chunk_data = self.payload_data[meta.doc_ctrl_off..][0..meta.doc_ctrl_len];
        if (chunk_data.len < 4) return error.InvalidData;
        const doc_bits = chunk_data[0];
        const freq_bits = chunk_data[1];
        const norm_bits = chunk_data[2];
        if (doc_bits > 32 or freq_bits > 32 or norm_bits > 32) return error.InvalidData;

        const count: usize = meta.doc_count;
        const doc_len = packedU32ByteLen(count, doc_bits);
        const freq_len = packedU32ByteLen(count, freq_bits);
        const norm_len = packedU32ByteLen(count, norm_bits);
        const expected_len = 4 + doc_len + freq_len + norm_len;
        if (chunk_data.len < expected_len) return error.InvalidData;

        var pos: usize = 4;
        try decodePackedU32Into(chunk_data[pos..][0..doc_len], self.doc_values.items, doc_bits);
        pos += doc_len;
        try decodePackedU32IntoStrided(chunk_data[pos..][0..freq_len], self.freq_norm_values.items, count, freq_bits, 0, 2);
        pos += freq_len;
        try decodePackedU32IntoStrided(chunk_data[pos..][0..norm_len], self.freq_norm_values.items, count, norm_bits, 1, 2);

        if (self.doc_values.items.len > 0) {
            self.doc_values.items[0] +%= meta.chunk_id * self.chunk_size;
            for (1..self.doc_values.items.len) |i| {
                self.doc_values.items[i] +%= self.doc_values.items[i - 1];
            }
        }

        self.current_chunk_index = index;
        self.next_chunk_index = index + 1;
        self.chunk_doc_pos = 0;
    }

    pub fn next(self: *PostingsIterator) !?Hit {
        if (self.is_one_hit) {
            if (self.one_hit_consumed) return null;
            self.one_hit_consumed = true;
            return .{ .doc_id = self.one_hit_doc, .freq = 1, .norm = self.one_hit_norm };
        }

        if (self.current_chunk_index == std.math.maxInt(usize) or self.chunk_doc_pos >= self.doc_values.items.len) {
            if (self.next_chunk_index >= self.chunkCount()) return null;
            try self.loadChunk(self.next_chunk_index);
        }

        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const freq_has_locs_val = self.freq_norm_values.items[self.chunk_doc_pos * 2];
        const norm_val = self.freq_norm_values.items[self.chunk_doc_pos * 2 + 1];
        self.chunk_doc_pos += 1;
        const decoded = decodeFreqHasLocs(freq_has_locs_val);

        self.positions_buf.clearRetainingCapacity();
        if (self.decode_positions) {
            if (self.positions_data) |pd| {
                if (self.positions_cursor < pd.len) {
                    const num_pos = readVarintU32(pd, &self.positions_cursor) catch 0;
                    if (num_pos > 0) {
                        try self.positions_buf.ensureTotalCapacity(self.alloc, num_pos);
                        var prev: u32 = 0;
                        var i: u32 = 0;
                        while (i < num_pos) : (i += 1) {
                            const delta = readVarintU32(pd, &self.positions_cursor) catch 0;
                            const p = prev +% delta;
                            self.positions_buf.appendAssumeCapacity(p);
                            prev = p;
                        }
                    }
                }
            }
        }

        return .{ .doc_id = doc_id, .freq = @intCast(decoded.freq), .norm = norm_val, .positions = self.positions_buf.items };
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
            self.one_hit_consumed = true;
            return .{ .doc_id = self.one_hit_doc, .freq = 1, .norm = self.one_hit_norm };
        }

        if (self.current_chunk_index != std.math.maxInt(usize)) {
            while (true) {
                if (self.chunk_doc_pos >= self.doc_values.items.len) break;
                const hit = try self.next() orelse return null;
                if (hit.doc_id >= target) return hit;
            }
        }

        const target_chunk_index = self.nextChunkIndexForTarget(target);
        if (target_chunk_index >= self.chunkCount()) return null;
        try self.loadChunk(target_chunk_index);
        if (self.current_chunk_index == std.math.maxInt(usize) or self.current_chunk_index >= self.chunkCount()) return null;

        while (self.chunk_doc_pos < self.doc_values.items.len and self.doc_values.items[self.chunk_doc_pos] < target) {
            self.chunk_doc_pos += 1;
        }
        if (self.chunk_doc_pos >= self.doc_values.items.len) return try self.advanceTo(target);

        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const freq_has_locs_val = self.freq_norm_values.items[self.chunk_doc_pos * 2];
        const norm_val = self.freq_norm_values.items[self.chunk_doc_pos * 2 + 1];
        self.chunk_doc_pos += 1;
        const decoded = decodeFreqHasLocs(freq_has_locs_val);

        self.positions_buf.clearRetainingCapacity();
        return .{ .doc_id = doc_id, .freq = @intCast(decoded.freq), .norm = norm_val };
    }

    pub fn deinit(self: *PostingsIterator) void {
        if (!self.is_one_hit) {
            self.doc_values.deinit(self.alloc);
            self.freq_norm_values.deinit(self.alloc);
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

/// Compute BM25 score for a single term-document pair.
pub fn bm25Score(
    freq: u32,
    doc_len: u32,
    doc_count: u32,
    doc_freq: u32,
    avg_doc_len: f32,
    config: BM25Config,
) f32 {
    // IDF = ln(1 + (N - n + 0.5) / (n + 0.5))
    const n: f32 = @floatFromInt(doc_count);
    const df: f32 = @floatFromInt(doc_freq);
    const idf = @log(1.0 + (n - df + 0.5) / (df + 0.5));

    // TF = (f * (k1 + 1)) / (f + k1 * (1 - b + b * dl / avgdl))
    const f: f32 = @floatFromInt(freq);
    const dl: f32 = @floatFromInt(doc_len);
    const tf = (f * (config.k1 + 1.0)) / (f + config.k1 * (1.0 - config.b + config.b * dl / avg_doc_len));

    return idf * tf;
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

pub const IndexConfig = struct {
    /// Documents per chunk for freq/norm encoding.
    chunk_size: u32 = 1024,
    /// Build a per-segment term bloom filter that lets readers reject absent
    /// terms before walking the FST. Defaults on for current segments and is
    /// auto-skipped when the term count falls below `bloom_min_terms`.
    enable_bloom: bool = true,
    /// Bloom filter sizing. 10 bits/key with 4 hashes → ~1% false-positive rate
    /// on the typical posting-list term distribution.
    bloom_bits_per_key: usize = 10,
};

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

pub fn mergeInvertedSectionSlotsWithDeletes(
    alloc: Allocator,
    sections: []const ?[]const u8,
    doc_counts: []const u32,
    deleted_docs: ?[]const ?roaring.RoaringBitmap,
    config: IndexConfig,
) ![]u8 {
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

    var postings_data = std.ArrayListUnmanaged(u8).empty;
    defer postings_data.deinit(alloc);

    var total_field_len: u64 = 0;
    var serialize_scratch = PostingSerializeScratch{};
    defer serialize_scratch.deinit(alloc);
    var bloom_hashes = std.ArrayListUnmanaged(TermBloomHash).empty;
    defer bloom_hashes.deinit(alloc);
    const collect_bloom_hashes = config.enable_bloom;
    var dict_entries = std.ArrayListUnmanaged(TermDictEntry).empty;
    defer {
        for (dict_entries.items) |entry| alloc.free(entry.term);
        dict_entries.deinit(alloc);
    }

    while (true) {
        const min_term = findMinCurrentTerm(current_entries) orelse break;

        {
            const merged_term = try alloc.dupe(u8, min_term);
            errdefer alloc.free(merged_term);

            var acc = PostingAccumulator.init();
            defer acc.deinit(alloc);

            for (current_entries, 0..) |entry_opt, seg_idx| {
                const entry = entry_opt orelse continue;
                if (!std.mem.eql(u8, entry.term, merged_term)) continue;

                const rmap = renumber_maps[seg_idx].?;
                try appendLookupResultToAccumulator(alloc, &acc, entry.result, rmap, &total_field_len);
                current_entries[seg_idx] = try nextTermIteratorEntry(term_iters, seg_idx);
            }

            if (acc.doc_ids.items.len == 0) continue;
            if (collect_bloom_hashes) {
                const h = termBloomHashes(merged_term);
                try bloom_hashes.append(alloc, .{ .h1 = h.h1, .h2 = h.h2 });
            }
            const dict_value = try appendMergedTerm(alloc, &postings_data, &serialize_scratch, &acc, config, running_offset);
            try dict_entries.append(alloc, .{
                .term = merged_term,
                .value = dict_value,
            });
        }
    }

    const term_dict_data = try encodeBlockedTermDictionary(alloc, dict_entries.items);
    defer alloc.free(term_dict_data);
    return assembleMergedSection(alloc, running_offset, total_field_len, config.chunk_size, postings_data.items, term_dict_data, config, bloom_hashes.items);
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
    total_field_len: *u64,
) !void {
    switch (result) {
        .one_hit => |hit| {
            if (hit.doc_num >= rmap.len) return;
            const remapped_doc = rmap[hit.doc_num];
            if (remapped_doc == std.math.maxInt(u32)) return;
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
                try acc.add(alloc, remapped_doc, hit.freq, hit.norm, hit.positions);
                total_field_len.* += hit.freq;
            }
        },
    }
}

fn appendMergedTerm(
    alloc: Allocator,
    postings_data: *std.ArrayListUnmanaged(u8),
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
        const norm_bits: u64 = acc.metas.items[0].norm;
        return fstValEncode1Hit(doc_num, norm_bits);
    }

    const postings_offset: u64 = @intCast(postings_data.items.len);
    _ = merged_doc_count;
    try acc.serializeV9(alloc, postings_data, serialize_scratch, config.chunk_size);
    return postings_offset;
}

fn assembleMergedSection(
    alloc: Allocator,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    postings_data: []const u8,
    term_dict_data: []const u8,
    config: IndexConfig,
    term_hashes: []const TermBloomHash,
) ![]u8 {
    var bloom_bytes: []const u8 = &.{};
    defer if (bloom_bytes.len > 0) alloc.free(@constCast(bloom_bytes));
    if (config.enable_bloom and term_hashes.len >= bloom_min_terms) {
        var builder = try bloom.Builder.init(alloc, term_hashes.len, .{
            .bits_per_key = config.bloom_bits_per_key,
        });
        errdefer builder.deinit();

        for (term_hashes) |h| builder.addHashes(h.h1, h.h2);

        var filter = builder.finish();
        defer filter.deinit(alloc);
        bloom_bytes = try filter.encodeAlloc(alloc);
    }

    const total = v7_header_size + postings_data.len + bloom_bytes.len + term_dict_data.len;
    var output = try alloc.alloc(u8, total);
    writeCurrentHeader(
        output,
        doc_count,
        total_field_len,
        chunk_size,
        @intCast(term_dict_data.len),
        @intCast(bloom_bytes.len),
    );
    var pos: usize = v7_header_size;

    @memcpy(output[pos..][0..postings_data.len], postings_data);
    pos += postings_data.len;
    if (bloom_bytes.len > 0) {
        @memcpy(output[pos..][0..bloom_bytes.len], bloom_bytes);
        pos += bloom_bytes.len;
    }
    @memcpy(output[pos..][0..term_dict_data.len], term_dict_data);

    return output;
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

test "v11 term dictionary stores prefix blocks indexed by fst" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    try builder.addDocument(0, &.{
        .{ .term = "aa0", .freq = 1 },
        .{ .term = "aa1", .freq = 1 },
        .{ .term = "ab0", .freq = 1 },
    });

    const section = try builder.build();
    defer alloc.free(section);

    try std.testing.expectEqual(@as(u8, wire_version_current), section[4]);
    const dict_len = std.mem.readInt(u32, section[21..25], .little);
    const dict = section[section.len - dict_len ..];
    try std.testing.expectEqualStrings(term_dict_magic, dict[0..4]);
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, dict[4..8], .little));

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expect(reader.lookup("aa0") != null);
    try std.testing.expect(reader.lookup("aa1") != null);
    try std.testing.expect(reader.lookup("ab0") != null);
    try std.testing.expect(reader.lookup("aa2") == null);
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

test "v4 block-max metadata round-trip" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    // 4 docs, chunk_size=2 → 2 chunks
    // Chunk 0 (docs 0,1): "term" freq 3,1 norm 10,20
    // Chunk 1 (docs 2,3): "term" freq 5,2 norm 30,15
    try builder.addDocument(0, &.{.{ .term = "term", .freq = 3, .norm = 10 }});
    try builder.addDocument(1, &.{.{ .term = "term", .freq = 1, .norm = 20 }});
    try builder.addDocument(2, &.{.{ .term = "term", .freq = 5, .norm = 30 }});
    try builder.addDocument(3, &.{.{ .term = "term", .freq = 2, .norm = 15 }});

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expectEqual(@as(u8, wire_version_current), reader.version);

    const result = reader.lookup("term") orelse return error.TestExpectedEqual;
    switch (result) {
        .postings => |p| {
            const bm = p.block_max orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u32, 2), bm.num_chunks);

            // Chunk 0: max_freq=3, min_norm=10, max_norm=20
            try std.testing.expectEqual(@as(u16, 3), std.mem.readInt(u16, bm.meta[0..2], .little));
            try std.testing.expectEqual(@as(u16, 10), std.mem.readInt(u16, bm.meta[2..4], .little));
            try std.testing.expectEqual(@as(u16, 20), std.mem.readInt(u16, bm.meta[4..6], .little));

            // Chunk 1: max_freq=5, min_norm=15, max_norm=30
            try std.testing.expectEqual(@as(u16, 5), std.mem.readInt(u16, bm.meta[6..8], .little));
            try std.testing.expectEqual(@as(u16, 15), std.mem.readInt(u16, bm.meta[8..10], .little));
            try std.testing.expectEqual(@as(u16, 30), std.mem.readInt(u16, bm.meta[10..12], .little));

            // maxImpact should return a positive score
            const impact = bm.maxImpact(0, 4, 4, 2.75, .{});
            try std.testing.expect(impact > 0);
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "v10 block-max metadata stores only term chunk window" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    try builder.addDocument(0, &.{.{ .term = "pad0", .freq = 1, .norm = 10 }});
    try builder.addDocument(1, &.{.{ .term = "pad1", .freq = 1, .norm = 10 }});
    try builder.addDocument(2, &.{.{ .term = "late", .freq = 3, .norm = 9 }});
    try builder.addDocument(3, &.{.{ .term = "late", .freq = 2, .norm = 11 }});
    try builder.addDocument(4, &.{.{ .term = "pad4", .freq = 1, .norm = 10 }});
    try builder.addDocument(5, &.{.{ .term = "pad5", .freq = 1, .norm = 10 }});

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("late") orelse return error.TestExpectedEqual;
    switch (result) {
        .postings => |p| {
            const bm = p.block_max orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u32, 1), bm.base_chunk);
            try std.testing.expectEqual(@as(u32, 1), bm.num_chunks);
            try std.testing.expectEqual(@as(usize, 6), bm.meta.len);
            try std.testing.expectEqual(@as(f32, 0), bm.maxImpact(0, 6, 2, reader.avgDocLen(), .{}));
            try std.testing.expect(bm.maxImpact(1, 6, 2, reader.avgDocLen(), .{}) > 0);
            try std.testing.expectEqual(@as(f32, 0), bm.maxImpact(2, 6, 2, reader.avgDocLen(), .{}));
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "positions round-trip v6 delta+varint" {
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
            try std.testing.expectEqual(@as(usize, 2 * postings_skip_record_size), postings.skip_data.?.len);
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

test "v6 positions are smaller than v5 raw u32" {
    // Smoke-test the shrinkage claim: dense, monotonic positions like a
    // tokenized document produces should pack much smaller in delta+varint
    // than 4 bytes per position.
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

    // v5 would have written: 4 (section_len) + 2 (num_pos) + 256 * 4 = 1030 bytes
    // for the positions section alone. v6 emits 256 deltas of value 0 or 1, all
    // single-byte varints, plus a 2-byte num_positions varint and the 4-byte
    // section_len → ~262 bytes. Verify the assembled section is well below v5.
    try std.testing.expect(section.len < 800);
}

test "v6 reads back positions across the varint multi-byte boundary" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    // Mix of single-byte (delta < 128) and multi-byte (delta ≥ 128) cases.
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
