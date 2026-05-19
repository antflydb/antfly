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
//!   - Vellum FST term dictionary (term → postings offset)
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
//   v2: baseline (FST + roaring bitmap + chunked freq/norm)
//   v3: + 1-hit FST encoding for unique terms (freqHasLocs encoding)
//   v4: + per-chunk block-max (max_freq, min_norm, max_norm) for WAND
//   v5: + per-doc positions (raw u32 LE) for phrase/proximity
//   v6: + delta-varint positions (compact) + per-segment bloom filter on terms
//
// Readers accept v2-v6. Builders emit v6 by default.

const wire_version_v6: u8 = 6;
const v6_header_size: usize = 4 + 1 + 4 + 8 + 4 + 4 + 4; // 29 bytes
const v3_v5_header_size: usize = 4 + 1 + 4 + 8 + 4 + 4; // 25 bytes

/// Skip building a per-segment bloom filter when there are fewer terms than this.
/// FST traversal is already cheap for tiny term sets, and the filter would
/// dominate the section size.
const bloom_min_terms: usize = 64;

/// Write the 29-byte v6 section header into `dst[0..v6_header_size]`. Shared
/// between `InvertedIndexBuilder.build` and the merger's
/// `assembleMergedSection` so the wire layout lives in exactly one place.
fn writeV6Header(
    dst: []u8,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    vellum_len: u32,
    bloom_len: u32,
) void {
    std.debug.assert(dst.len >= v6_header_size);
    @memcpy(dst[0..4], "INVT");
    dst[4] = wire_version_v6;
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
    /// Postings per term:
    ///   [doc_freq: u32 LE]
    ///   [bitmap_len: u32 LE][bitmap_data]
    ///   [freq_norm_len: u32 LE][freq_norm_data]  — chunked freqHasLocs+norm pairs
    ///   [num_chunks: u32 LE]                      — block-max chunk count (v4+)
    ///   per chunk: [max_freq: u16 LE][min_norm: u16 LE][max_norm: u16 LE]
    ///   [positions_section_len: u32 LE]            — length of positions data (v5+)
    ///   per doc (in posting order):
    ///     v5: [num_positions: u16 LE][positions: num_positions * u32 LE]
    ///     v6: [num_positions: varint][delta_0: varint][delta_1: varint]...
    ///         (delta_0 is the absolute first position; delta_i = pos_i - pos_{i-1})
    ///
    /// v6 also adds a per-segment bloom filter on terms between postings_data
    /// and the FST. `bloom_len` in the header tells the reader how many bytes
    /// to skip past postings before the FST starts.
    pub fn build(self: *InvertedIndexBuilder) ![]u8 {
        const term_count = self.terms.count();
        if (term_count == 0) return try self.alloc.dupe(u8, &.{});

        // Step 1: Sort terms
        const sorted_terms = try self.alloc.alloc([]const u8, term_count);
        defer self.alloc.free(sorted_terms);
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

        // Step 2: Serialize postings data and build FST (term → postings offset or 1-hit)
        var postings_data = std.ArrayListUnmanaged(u8).empty;
        defer postings_data.deinit(self.alloc);

        // Size the FST registry to roughly the term count: a 3-term segment
        // has no use for the default 10K-cell dedup table, and an enormous
        // segment shouldn't be capped to 10K either. The clamp keeps the
        // allocation between one page and a few MB.
        const fst_registry_size: usize = std.math.clamp(term_count, 64, 65_536);
        var fst_builder = try vellum.Builder.init(self.alloc, .{
            .registry_table_size = fst_registry_size,
        });
        defer fst_builder.deinit();

        // Optional per-segment term bloom filter. `null` when disabled (small
        // term sets) or when the caller opted out via `IndexConfig.enable_bloom`.
        const want_bloom = self.config.enable_bloom and term_count >= bloom_min_terms;
        var bloom_builder: ?bloom.Builder = if (want_bloom)
            try bloom.Builder.init(self.alloc, term_count, .{
                .bits_per_key = self.config.bloom_bits_per_key,
            })
        else
            null;
        // Cleared to null below after `finish()`; the conditional makes the
        // errdefer safe whether finish() has run or not.
        errdefer if (bloom_builder) |*b| b.deinit();

        for (sorted_terms) |term| {
            const acc = self.terms.getPtr(term).?;

            if (bloom_builder) |*b| {
                // Single Wyhash + splitmix64 derivation; the read path mirrors
                // it via `termBloomHashes` to keep the bit-set pattern the
                // same across writers and readers.
                const h = termBloomHashes(term);
                b.addHashes(h.h1, h.h2);
            }

            // 1-hit optimization: single doc, freq=1, no locs, no positions, docNum fits in 31 bits
            if (acc.doc_ids.items.len == 1 and
                acc.freqs.items[0] == 1 and
                acc.position_counts.items[0] == 0 and
                acc.doc_ids.items[0] <= mask_31_bits)
            {
                const doc_num: u64 = acc.doc_ids.items[0];
                const norm_bits: u64 = acc.norms.items[0];
                try fst_builder.insert(term, fstValEncode1Hit(doc_num, norm_bits));
            } else {
                const postings_offset: u64 = @intCast(postings_data.items.len);
                try acc.serialize(self.alloc, &postings_data, self.config.chunk_size, self.doc_count);
                try fst_builder.insert(term, postings_offset);
            }
        }

        const fst_data = try fst_builder.finish();
        defer self.alloc.free(fst_data);

        // Encode bloom (if any) into a single buffer that we'll inline into
        // the section. The on-disk payload is the standard `lib/bloom` magic +
        // version + bit_count + hash_count + bytes envelope.
        var bloom_bytes: []const u8 = &.{};
        defer if (bloom_bytes.len > 0) self.alloc.free(@constCast(bloom_bytes));
        if (bloom_builder) |*b| {
            var filter = b.finish();
            // `finish` consumes the builder (sets it to undefined); null out the
            // option so the errdefer above is a no-op.
            bloom_builder = null;
            defer filter.deinit(self.alloc);
            bloom_bytes = try filter.encodeAlloc(self.alloc);
        }

        // Step 3: Assemble final v6 section
        const total = v6_header_size + postings_data.items.len + bloom_bytes.len + fst_data.len;
        var output = try self.alloc.alloc(u8, total);
        writeV6Header(
            output,
            self.doc_count,
            self.total_field_len,
            self.config.chunk_size,
            @intCast(fst_data.len),
            @intCast(bloom_bytes.len),
        );
        var pos: usize = v6_header_size;

        // Postings data
        @memcpy(output[pos..][0..postings_data.items.len], postings_data.items);
        pos += postings_data.items.len;

        // Bloom filter (may be empty)
        if (bloom_bytes.len > 0) {
            @memcpy(output[pos..][0..bloom_bytes.len], bloom_bytes);
            pos += bloom_bytes.len;
        }

        // Vellum FST data (at end, so vellum_len in header tells us where it starts)
        @memcpy(output[pos..][0..fst_data.len], fst_data);

        return output;
    }
};

/// Accumulates postings for a single term during index building.
const PostingAccumulator = struct {
    doc_ids: std.ArrayListUnmanaged(u32) = .empty,
    freqs: std.ArrayListUnmanaged(u32) = .empty,
    norms: std.ArrayListUnmanaged(u32) = .empty,
    /// Flat concatenation of all position lists.
    all_positions: std.ArrayListUnmanaged(u32) = .empty,
    /// Number of positions stored for each doc (parallel to doc_ids).
    position_counts: std.ArrayListUnmanaged(u32) = .empty,

    fn init() PostingAccumulator {
        return .{};
    }

    fn deinit(self: *PostingAccumulator, alloc: Allocator) void {
        self.doc_ids.deinit(alloc);
        self.freqs.deinit(alloc);
        self.norms.deinit(alloc);
        self.all_positions.deinit(alloc);
        self.position_counts.deinit(alloc);
    }

    fn add(self: *PostingAccumulator, alloc: Allocator, doc_num: u32, freq: u32, norm_val: u32, positions: []const u32) !void {
        try self.doc_ids.append(alloc, doc_num);
        try self.freqs.append(alloc, freq);
        try self.norms.append(alloc, norm_val);
        try self.all_positions.appendSlice(alloc, positions);
        try self.position_counts.append(alloc, @intCast(positions.len));
    }

    /// Serialize this term's posting data (v4 format with freqHasLocs + block-max).
    /// Format:
    ///   [doc_freq: u32 LE]              — number of documents containing this term
    ///   [bitmap_len: u32 LE]            — length of roaring bitmap bytes
    ///   [bitmap_data: ...]               — serialized roaring bitmap
    ///   [freq_norm_data_len: u32 LE]    — length of chunked freqHasLocs+norm data
    ///   [freq_norm_data: ...]            — interleaved (freqHasLocs, norm) pairs per doc
    ///   [num_chunks: u32 LE]            — number of block-max chunks (v4 only)
    ///   per chunk: [max_freq: u16 LE][min_norm: u16 LE][max_norm: u16 LE]
    fn serialize(
        self: *const PostingAccumulator,
        alloc: Allocator,
        out: *std.ArrayListUnmanaged(u8),
        chunk_size: u32,
        max_doc: u32,
    ) !void {
        const doc_freq: u32 = @intCast(self.doc_ids.items.len);
        const encoded_doc_limit = try self.encodedDocLimit(max_doc);

        // 1. Doc frequency
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_freq))));

        // 2. Roaring bitmap of doc IDs. doc_ids are appended in document order
        // during build, so they're already sorted ascending and unique here.
        var bitmap = roaring.RoaringBitmap.init(alloc);
        defer bitmap.deinit();
        try bitmap.addSortedAscending(self.doc_ids.items);
        const bitmap_bytes = try bitmap.toBytes(alloc);
        defer alloc.free(bitmap_bytes);

        try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(bitmap_bytes.len))))));
        try out.appendSlice(alloc, bitmap_bytes);

        // 3. Chunked freqHasLocs + norm (interleaved: 2 values per doc)
        {
            var encoder = try chunked.ChunkedIntEncoder.initWithMode(alloc, chunk_size, encoded_doc_limit, .stream_vbyte);
            defer encoder.deinit();
            for (self.doc_ids.items, self.freqs.items, self.norms.items) |doc_id, freq, norm_val| {
                const freq_has_locs: u32 = @intCast(encodeFreqHasLocs(freq, false));
                try encoder.add(doc_id, &.{ freq_has_locs, norm_val });
            }
            try encoder.close();
            const freq_norm_bytes = try encoder.toBytes();
            defer alloc.free(freq_norm_bytes);
            try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(freq_norm_bytes.len))))));
            try out.appendSlice(alloc, freq_norm_bytes);
        }

        // 4. Block-max metadata (v4): per-chunk max_freq, min_norm, max_norm
        {
            const num_chunks: u32 = if (encoded_doc_limit == 0) 0 else (encoded_doc_limit - 1) / chunk_size + 1;
            try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, num_chunks))));

            // Single allocation for [max_freq, min_norm, max_norm] triples — interleaved
            // so the final emit is one bulk appendSlice over the packed wire bytes.
            const meta_bytes = @as(usize, num_chunks) * 6;
            const meta_start = out.items.len;
            try out.ensureUnusedCapacity(alloc, meta_bytes);
            out.items.len += meta_bytes;
            const meta = out.items[meta_start..][0..meta_bytes];

            // Initialize: max_freq=0, min_norm=maxInt(u16), max_norm=0.
            var ci: usize = 0;
            while (ci < num_chunks) : (ci += 1) {
                const off = ci * 6;
                meta[off..][0..2].* = .{ 0, 0 };
                meta[off + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, std.math.maxInt(u16)));
                meta[off + 4 ..][0..2].* = .{ 0, 0 };
            }

            for (self.doc_ids.items, self.freqs.items, self.norms.items) |doc_id, freq, norm_val| {
                const chunk_idx = doc_id / chunk_size;
                const off = @as(usize, chunk_idx) * 6;
                const freq_u16: u16 = if (freq > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(freq);
                const norm_u16: u16 = if (norm_val > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(norm_val);
                const cur_max_freq = std.mem.readInt(u16, meta[off..][0..2], .little);
                const cur_min_norm = std.mem.readInt(u16, meta[off + 2 ..][0..2], .little);
                const cur_max_norm = std.mem.readInt(u16, meta[off + 4 ..][0..2], .little);
                if (freq_u16 > cur_max_freq) meta[off..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, freq_u16));
                if (norm_u16 < cur_min_norm) meta[off + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, norm_u16));
                if (norm_u16 > cur_max_norm) meta[off + 4 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, norm_u16));
            }
        }

        // 5. Positions (v6): per doc, [num_positions: varint][delta_0: varint][delta_1: varint]...
        // Deltas are at the u32 level (delta_0 = first absolute position,
        // delta_i = pos_i - pos_{i-1}). On token streams positions are
        // monotonically increasing, so deltas pack into 1-2 bytes per varint
        // versus 4 bytes raw u32 in v5.
        {
            const pos_start = out.items.len;

            // Pre-size the section. Budget pessimistically: at most 5 bytes per
            // varint (LEB128 cap for u32). Most positions encode in 1-2 bytes
            // so this slightly over-reserves, which is fine for ArrayList.
            var pre_size: usize = 4; // positions_section_len header
            for (self.position_counts.items) |count| {
                pre_size += 5; // num_positions varint
                pre_size += @as(usize, count) * 5; // worst-case delta varints
            }
            try out.ensureUnusedCapacity(alloc, pre_size);
            // Reserve space for positions_section_len
            out.items.len += 4;

            var pos_offset: usize = 0;
            for (self.position_counts.items) |count| {
                try writeVarintU32(alloc, out, count);
                if (count > 0) {
                    var prev: u32 = 0;
                    var i: usize = 0;
                    while (i < count) : (i += 1) {
                        const p = self.all_positions.items[pos_offset + i];
                        // Positions within a doc are emitted in order; deltas
                        // are non-negative. We don't validate here — the caller
                        // is expected to provide sorted positions.
                        const delta: u32 = if (p >= prev) p - prev else 0;
                        try writeVarintU32(alloc, out, delta);
                        prev = p;
                    }
                }
                pos_offset += count;
            }

            // Patch positions_section_len
            const pos_data_len: u32 = @intCast(out.items.len - pos_start - 4);
            out.items[pos_start..][0..4].* = @bitCast(std.mem.nativeToLittle(u32, pos_data_len));
        }
    }

    fn encodedDocLimit(self: *const PostingAccumulator, max_doc: u32) !u32 {
        var limit = max_doc;
        for (self.doc_ids.items) |doc_id| {
            const doc_limit = std.math.add(u32, doc_id, 1) catch return error.DocumentOutOfRange;
            limit = @max(limit, doc_limit);
        }
        return limit;
    }
};

// ============================================================================
// Index reader (query path)
// ============================================================================

/// Reads a serialized inverted index section (v2 through v6, with Vellum FST).
pub const InvertedIndexReader = struct {
    alloc: Allocator,
    data: []const u8,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    postings_offset: usize,
    version: u8,
    fst: vellum.FST,
    /// Optional per-segment term bloom filter (v6+). When present, callers can
    /// reject absent terms before walking the FST. Borrows into `data`.
    term_bloom: ?bloom.BorrowedFilter,

    pub fn init(alloc: Allocator, data: []const u8) !InvertedIndexReader {
        if (data.len < v3_v5_header_size) return error.InvalidData;
        if (!std.mem.eql(u8, data[0..4], "INVT")) return error.InvalidMagic;
        const version = data[4];
        if (version < 2 or version > wire_version_v6) return error.UnsupportedVersion;

        const doc_count = std.mem.readInt(u32, data[5..9], .little);
        const total_field_len = std.mem.readInt(u64, data[9..17], .little);
        const chunk_size = std.mem.readInt(u32, data[17..21], .little);
        const vellum_len = std.mem.readInt(u32, data[21..25], .little);

        var bloom_len: u32 = 0;
        var postings_offset: usize = v3_v5_header_size;
        if (version >= wire_version_v6) {
            if (data.len < v6_header_size) return error.InvalidData;
            bloom_len = std.mem.readInt(u32, data[25..29], .little);
            postings_offset = v6_header_size;
        }

        const fst_offset = data.len - vellum_len;
        const fst_data = data[fst_offset..];
        const fst = try vellum.FST.load(fst_data);

        var term_bloom: ?bloom.BorrowedFilter = null;
        if (bloom_len > 0) {
            const bloom_offset = fst_offset - bloom_len;
            term_bloom = bloom.BorrowedFilter.decode(data[bloom_offset..fst_offset]) catch null;
        }

        return .{
            .alloc = alloc,
            .data = data,
            .doc_count = doc_count,
            .total_field_len = total_field_len,
            .chunk_size = chunk_size,
            .postings_offset = postings_offset,
            .version = version,
            .fst = fst,
            .term_bloom = term_bloom,
        };
    }

    /// Average document length for BM25.
    pub fn avgDocLen(self: *const InvertedIndexReader) f32 {
        if (self.doc_count == 0) return 0;
        return @as(f32, @floatFromInt(self.total_field_len)) / @as(f32, @floatFromInt(self.doc_count));
    }

    /// Look up a term using the Vellum FST. Returns posting data, or null.
    /// For 1-hit terms (v3), returns a synthetic TermPostings with the single doc.
    /// Consults the per-segment term bloom filter (v6+) before walking the FST
    /// when present, so absent-term lookups skip the FST traversal entirely.
    pub fn lookup(self: *const InvertedIndexReader, term: []const u8) ?LookupResult {
        if (self.term_bloom) |filter| {
            const h = termBloomHashes(term);
            if (!filter.maybeContainsHashes(h.h1, h.h2)) return null;
        }
        const result = self.fst.get(term) catch return null;
        if (!result.found) return null;

        if (self.version >= 3 and fstValIs1Hit(result.val)) {
            const decoded = fstValDecode1Hit(result.val);
            return .{ .one_hit = .{
                .doc_num = @intCast(decoded.doc_num),
                .norm_bits = @intCast(decoded.norm_bits),
            } };
        }

        return .{ .postings = self.readPostings(@intCast(result.val)) };
    }

    /// Iterate all terms in the dictionary using the FST iterator.
    pub fn termIterator(self: *const InvertedIndexReader) !TermIterator {
        return .{
            .alloc = self.alloc,
            .reader = self,
            .fst_iter = try self.fst.iterator(self.alloc, null, null),
        };
    }

    /// Iterate terms in a lexicographic range [start, end).
    pub fn rangeTermIterator(self: *const InvertedIndexReader, start: ?[]const u8, end: ?[]const u8) !TermIterator {
        return .{
            .alloc = self.alloc,
            .reader = self,
            .fst_iter = try self.fst.iterator(self.alloc, start, end),
        };
    }

    /// Iterate terms matching an automaton using FST.search (prunes non-matching prefixes).
    pub fn fstSearchIterator(self: *const InvertedIndexReader, aut: vellum.Automaton) !TermIterator {
        return .{
            .alloc = self.alloc,
            .reader = self,
            .fst_iter = try self.fst.search(self.alloc, aut, null, null),
        };
    }

    fn readPostings(self: *const InvertedIndexReader, offset: u32) TermPostings {
        const base = self.postings_offset + offset;
        const doc_freq = std.mem.readInt(u32, self.data[base..][0..4], .little);
        const bitmap_len = std.mem.readInt(u32, self.data[base + 4 ..][0..4], .little);
        const bitmap_end = base + 8 + bitmap_len;
        const freq_norm_len = std.mem.readInt(u32, self.data[bitmap_end..][0..4], .little);
        const freq_norm_end = bitmap_end + 4 + freq_norm_len;

        var block_max: ?BlockMaxInfo = null;
        var after_block_max = freq_norm_end;
        if (self.version >= 4) {
            const num_chunks = std.mem.readInt(u32, self.data[freq_norm_end..][0..4], .little);
            const meta_start = freq_norm_end + 4;
            const meta_len = @as(usize, num_chunks) * 6;
            block_max = .{
                .num_chunks = num_chunks,
                .meta = self.data[meta_start..][0..meta_len],
            };
            after_block_max = meta_start + meta_len;
        }

        var positions_data: ?[]const u8 = null;
        if (self.version >= 5) {
            const pos_len = std.mem.readInt(u32, self.data[after_block_max..][0..4], .little);
            if (pos_len > 0) {
                positions_data = self.data[after_block_max + 4 ..][0..pos_len];
            }
            after_block_max += 4 + pos_len;
        }

        return .{
            .doc_freq = doc_freq,
            .bitmap_data = self.data[base + 8 ..][0..bitmap_len],
            .freq_norm_data = self.data[bitmap_end + 4 ..][0..freq_norm_len],
            .serialized_data = self.data[base..after_block_max],
            .chunk_size = self.chunk_size,
            .version = self.version,
            .block_max = block_max,
            .positions_data = positions_data,
        };
    }
};

pub const TermIterator = struct {
    alloc: Allocator,
    reader: *const InvertedIndexReader,
    fst_iter: vellum.FSTIterator,
    // We must copy the key before advancing, because nextEntry() invalidates it
    current_key: std.ArrayListUnmanaged(u8) = .empty,
    started: bool = false,

    pub const Entry = struct { term: []const u8, result: LookupResult };

    pub fn next(self: *TermIterator) !?Entry {
        // Advance past previous entry (except on first call)
        if (self.started) {
            _ = try self.fst_iter.nextEntry();
        }
        self.started = true;

        const current = self.fst_iter.current() orelse return null;

        const result: LookupResult = if (self.reader.version >= 3 and fstValIs1Hit(current.val))
            .{ .one_hit = .{
                .doc_num = @intCast(fstValDecode1Hit(current.val).doc_num),
                .norm_bits = @intCast(fstValDecode1Hit(current.val).norm_bits),
            } }
        else
            .{ .postings = self.reader.readPostings(@intCast(current.val)) };

        // Copy key so it survives across calls
        self.current_key.clearRetainingCapacity();
        try self.current_key.appendSlice(self.alloc, current.key);

        return .{ .term = self.current_key.items, .result = result };
    }

    pub fn deinit(self: *TermIterator) void {
        self.current_key.deinit(self.alloc);
        self.fst_iter.deinit();
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
/// Each chunk stores (max_freq, min_norm, max_norm) packed as 6 bytes.
pub const BlockMaxInfo = struct {
    num_chunks: u32,
    /// Packed [max_freq:u16 LE][min_norm:u16 LE][max_norm:u16 LE] per chunk.
    meta: []const u8,

    /// Compute the maximum possible BM25 impact for a chunk.
    /// Uses the most favorable values in the chunk: max_freq and min_norm (shortest doc).
    pub fn maxImpact(self: BlockMaxInfo, chunk_idx: u32, doc_count: u32, doc_freq: u32, avg_dl: f32, config: BM25Config) f32 {
        if (chunk_idx >= self.num_chunks) return 0;
        const offset = @as(usize, chunk_idx) * 6;
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
    bitmap_data: []const u8,
    freq_norm_data: []const u8,
    serialized_data: []const u8,
    chunk_size: u32,
    version: u8,
    block_max: ?BlockMaxInfo = null,
    positions_data: ?[]const u8 = null,

    /// Decode the roaring bitmap of document IDs.
    pub fn docBitmap(self: *const TermPostings, alloc: Allocator) !roaring.RoaringBitmap {
        return roaring.RoaringBitmap.fromBytes(alloc, self.bitmap_data);
    }

    /// Create a postings iterator that yields (doc_id, freq, norm, positions) tuples.
    pub fn iterator(self: *const TermPostings, alloc: Allocator) !PostingsIterator {
        var bitmap = try self.docBitmap(alloc);
        errdefer bitmap.deinit();

        var freq_norm_decoder = try chunked.ChunkedIntDecoder.init(alloc, self.freq_norm_data, 0);
        errdefer freq_norm_decoder.deinit();

        return .{
            .alloc = alloc,
            .bitmap = bitmap,
            .freq_norm_decoder = freq_norm_decoder,
            .chunk_size = self.chunk_size,
            .current_chunk = std.math.maxInt(usize),
            .version = self.version,
            .positions_data = self.positions_data,
        };
    }
};

/// Iterates over (doc_id, freq, norm, positions) for a term's posting list.
pub const PostingsIterator = struct {
    alloc: Allocator,
    bitmap_iter: ?roaring.Iterator = null,
    bitmap: roaring.RoaringBitmap = undefined,
    freq_norm_decoder: chunked.ChunkedIntDecoder = undefined,
    chunk_size: u32 = 0,
    current_chunk: usize = std.math.maxInt(usize),
    /// `bitmap.rank(current_chunk * chunk_size)` — count of docs in all
    /// chunks that come before `current_chunk`. Cached so `advanceTo` only
    /// pays for one full bitmap rank per chunk transition instead of two
    /// per call (chunk_min + doc_id). Recomputed when `current_chunk`
    /// changes.
    chunk_base_rank: usize = 0,
    /// How many decoder values we've already consumed within the current
    /// chunk (i.e., 2 × number of docs already returned from this chunk).
    /// Used by `advanceTo` to skip-forward without reloading.
    chunk_decoder_pos: usize = 0,
    version: u8 = 3,
    positions_data: ?[]const u8 = null,
    positions_cursor: usize = 0,
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

    pub fn next(self: *PostingsIterator) !?Hit {
        if (self.is_one_hit) {
            if (self.one_hit_consumed) return null;
            self.one_hit_consumed = true;
            return .{ .doc_id = self.one_hit_doc, .freq = 1, .norm = self.one_hit_norm };
        }

        // Lazy init: create iterator after struct is at final address
        if (self.bitmap_iter == null) {
            self.bitmap_iter = self.bitmap.iterator();
        }
        const doc_id = self.bitmap_iter.?.next() orelse return null;

        // Load the right chunk for this doc
        const target_chunk = doc_id / self.chunk_size;
        if (target_chunk != self.current_chunk) {
            try self.freq_norm_decoder.loadChunk(target_chunk);
            self.current_chunk = target_chunk;
            self.chunk_base_rank = self.bitmap.rank(@intCast(target_chunk * self.chunk_size));
            self.chunk_decoder_pos = 0;
        }

        // v3+: interleaved (freqHasLocs, norm) pairs
        const freq_has_locs_val = self.freq_norm_decoder.readValue() orelse 0;
        const norm_val = self.freq_norm_decoder.readValue() orelse 0;
        self.chunk_decoder_pos += 2;
        const decoded = decodeFreqHasLocs(freq_has_locs_val);

        // Positions: v5 stores raw u32 LE; v6 stores delta+varint (LEB128).
        // Both formats are sequential in bitmap order with the same per-doc
        // length-prefix scheme, so the decode logic switches on `version`.
        // Skipped entirely when `decode_positions` is false (BM25-only
        // callers like the WAND scorer). The cursor isn't advanced in that
        // case — those iterators must not later flip the flag and try to
        // read positions back.
        self.positions_buf.clearRetainingCapacity();
        if (self.decode_positions) {
            if (self.positions_data) |pd| {
                if (self.version >= wire_version_v6) {
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
                } else if (self.positions_cursor + 2 <= pd.len) {
                    // v5: [num_positions: u16 LE][positions: num_positions × u32 LE]
                    const num_pos = std.mem.readInt(u16, pd[self.positions_cursor..][0..2], .little);
                    self.positions_cursor += 2;
                    const byte_len = @as(usize, num_pos) * 4;
                    if (self.positions_cursor + byte_len <= pd.len) {
                        try self.positions_buf.ensureTotalCapacity(self.alloc, num_pos);
                        for (0..num_pos) |i| {
                            const off = self.positions_cursor + i * 4;
                            self.positions_buf.appendAssumeCapacity(std.mem.readInt(u32, pd[off..][0..4], .little));
                        }
                        self.positions_cursor += byte_len;
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
    ///   * **Cross-chunk**: bitmap `Iterator.seekTo` jumps containers in
    ///     O(log) time, we load the destination chunk once, then use
    ///     `RoaringBitmap.rank` to anchor the chunked decoder at the right
    ///     offset within that chunk. This is where we actually skip work —
    ///     potentially thousands of docs per call.
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

        if (self.bitmap_iter == null) {
            self.bitmap_iter = self.bitmap.iterator();
        }

        // Same-chunk fast path: the bitmap target fits inside the chunk we've
        // already materialized, so spinning `next()` is cheaper than the
        // bitmap-seek + rank dance. This catches the vast majority of WAND
        // pivot-advance steps in balanced-selectivity queries. `next()`
        // already skips position decoding when `decode_positions` is false
        // (which the WAND scorer flips on every iterator it owns), so this
        // path doesn't pay the per-doc varint walk on positions-bearing
        // sections.
        const target_chunk: usize = target / self.chunk_size;
        if (self.current_chunk != std.math.maxInt(usize) and target_chunk == self.current_chunk) {
            while (true) {
                const hit = try self.next() orelse return null;
                if (hit.doc_id >= target) return hit;
                // If next() crossed into a new chunk while looking for target,
                // fall through to the cross-chunk seek logic below.
                if (self.current_chunk != target_chunk) break;
            }
        }

        // Cross-chunk seek path.
        const doc_id = self.bitmap_iter.?.seekTo(target) orelse return null;
        const new_chunk: usize = doc_id / self.chunk_size;
        if (new_chunk != self.current_chunk) {
            try self.freq_norm_decoder.loadChunk(new_chunk);
            self.current_chunk = new_chunk;
            self.chunk_base_rank = self.bitmap.rank(@intCast(new_chunk * self.chunk_size));
            self.chunk_decoder_pos = 0;
        }

        // Place the decoder at this doc's (freq, norm) pair.
        const target_pos = (self.bitmap.rank(doc_id) - self.chunk_base_rank) * 2;
        if (target_pos < self.chunk_decoder_pos) {
            // Backward seek inside the current chunk — rare. Reload to reset.
            try self.freq_norm_decoder.loadChunk(self.current_chunk);
            self.chunk_decoder_pos = 0;
        }
        self.freq_norm_decoder.skip(target_pos - self.chunk_decoder_pos);
        self.chunk_decoder_pos = target_pos;

        const freq_has_locs_val = self.freq_norm_decoder.readValue() orelse 0;
        const norm_val = self.freq_norm_decoder.readValue() orelse 0;
        self.chunk_decoder_pos += 2;
        const decoded = decodeFreqHasLocs(freq_has_locs_val);

        self.positions_buf.clearRetainingCapacity();
        return .{ .doc_id = doc_id, .freq = @intCast(decoded.freq), .norm = norm_val };
    }

    pub fn deinit(self: *PostingsIterator) void {
        if (!self.is_one_hit) {
            self.freq_norm_decoder.deinit();
            self.bitmap.deinit();
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
    /// terms before walking the FST. Defaults on for new (v6) segments and is
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

    var fst_builder = try vellum.Builder.init(alloc, .{});
    defer fst_builder.deinit();

    var total_field_len: u64 = 0;

    while (true) {
        const min_term = findMinCurrentTerm(current_entries) orelse break;
        if (singleContributorIndex(current_entries, min_term)) |seg_idx| {
            const entry = current_entries[seg_idx].?;
            if (try appendSingleContributorTerm(
                alloc,
                &fst_builder,
                &postings_data,
                entry,
                deleted_docs,
                seg_idx,
                doc_offsets[seg_idx],
                running_offset,
                &total_field_len,
            )) {
                current_entries[seg_idx] = try nextTermIteratorEntry(term_iters, seg_idx);
                continue;
            }
        }

        const merged_term = try alloc.dupe(u8, min_term);
        defer alloc.free(merged_term);

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
        try appendMergedTerm(alloc, &fst_builder, &postings_data, merged_term, &acc, config, running_offset);
    }

    return assembleMergedSection(alloc, running_offset, total_field_len, config.chunk_size, postings_data.items, try fst_builder.finish(), config);
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
    const has_deletions = if (deleted_docs) |dels|
        seg_idx < dels.len and dels[seg_idx] != null and !dels[seg_idx].?.isEmpty()
    else
        false;
    if (has_deletions) return false;

    switch (entry.result) {
        .one_hit => |hit| {
            const remapped_doc = hit.doc_num + doc_offset;
            if (remapped_doc > mask_31_bits) return false;
            total_field_len.* += 1;
            try fst_builder.insert(entry.term, fstValEncode1Hit(remapped_doc, hit.norm_bits));
            return true;
        },
        .postings => |postings| {
            // The direct-copy and partial-rewrite fast paths splice raw posting
            // bytes from the source segment into our v6 output. They're only
            // safe when the source segment is also v6, because the per-term
            // positions sub-format differs between v5 (raw u32 LE) and v6
            // (delta + varint). Older sources fall through to the accumulator
            // merge path, which decodes via PostingsIterator (version-aware)
            // and re-emits positions as v6 deltas.
            if (postings.version != wire_version_v6) return false;

            const postings_offset: u64 = @intCast(postings_data.items.len);
            if (doc_offset == 0) {
                total_field_len.* += try sumTermFrequenciesSimd(alloc, postings.freq_norm_data);
                try postings_data.appendSlice(alloc, postings.serialized_data);
            } else {
                const remapped = try remapSingleContributorPostings(alloc, postings, doc_offset, merged_doc_count, total_field_len);
                defer alloc.free(remapped);
                try postings_data.appendSlice(alloc, remapped);
            }
            try fst_builder.insert(entry.term, postings_offset);
            return true;
        },
    }
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
    fst_builder: *vellum.Builder,
    postings_data: *std.ArrayListUnmanaged(u8),
    term: []const u8,
    acc: *const PostingAccumulator,
    config: IndexConfig,
    merged_doc_count: u32,
) !void {
    if (acc.doc_ids.items.len == 1 and
        acc.freqs.items[0] == 1 and
        acc.position_counts.items[0] == 0 and
        acc.doc_ids.items[0] <= mask_31_bits)
    {
        const doc_num: u64 = acc.doc_ids.items[0];
        const norm_bits: u64 = acc.norms.items[0];
        try fst_builder.insert(term, fstValEncode1Hit(doc_num, norm_bits));
        return;
    }

    const postings_offset: u64 = @intCast(postings_data.items.len);
    try acc.serialize(alloc, postings_data, config.chunk_size, merged_doc_count);
    try fst_builder.insert(term, postings_offset);
}

fn assembleMergedSection(
    alloc: Allocator,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    postings_data: []const u8,
    fst_data: []u8,
    config: IndexConfig,
) ![]u8 {
    defer alloc.free(fst_data);

    // Re-derive the term set from the just-built FST so we can populate a
    // bloom filter without threading hash collection through every merge
    // helper. Single-pass: collect (h1, h2) pairs as we walk, then size and
    // populate the bloom from the collected hashes. The transient hash
    // buffer costs ~16 bytes/term — small even for million-term segments —
    // and replaces the previous count + repeat-walk pattern.
    var bloom_bytes: []const u8 = &.{};
    defer if (bloom_bytes.len > 0) alloc.free(@constCast(bloom_bytes));
    if (config.enable_bloom) {
        var fst = try vellum.FST.load(fst_data);
        var iter = try fst.iterator(alloc, null, null);
        defer iter.deinit();

        const HashPair = struct { h1: u64, h2: u64 };
        var hashes = std.ArrayListUnmanaged(HashPair).empty;
        defer hashes.deinit(alloc);

        while (try iter.nextEntry()) |entry| {
            const h = termBloomHashes(entry.key);
            try hashes.append(alloc, .{ .h1 = h.h1, .h2 = h.h2 });
        }

        if (hashes.items.len >= bloom_min_terms) {
            var builder = try bloom.Builder.init(alloc, hashes.items.len, .{
                .bits_per_key = config.bloom_bits_per_key,
            });
            errdefer builder.deinit();

            for (hashes.items) |h| builder.addHashes(h.h1, h.h2);

            var filter = builder.finish();
            defer filter.deinit(alloc);
            bloom_bytes = try filter.encodeAlloc(alloc);
        }
    }

    const total = v6_header_size + postings_data.len + bloom_bytes.len + fst_data.len;
    var output = try alloc.alloc(u8, total);
    writeV6Header(
        output,
        doc_count,
        total_field_len,
        chunk_size,
        @intCast(fst_data.len),
        @intCast(bloom_bytes.len),
    );
    var pos: usize = v6_header_size;

    @memcpy(output[pos..][0..postings_data.len], postings_data);
    pos += postings_data.len;
    if (bloom_bytes.len > 0) {
        @memcpy(output[pos..][0..bloom_bytes.len], bloom_bytes);
        pos += bloom_bytes.len;
    }
    @memcpy(output[pos..][0..fst_data.len], fst_data);

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

    // Builders emit v6 by default; the FST version-encoding (1-hit packing) is
    // unchanged from v3+, so the "unique" term still lands on the 1-hit path.
    try std.testing.expectEqual(@as(u8, 6), section[4]);

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
    try std.testing.expectEqual(@as(u8, 6), reader.version);

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
    try std.testing.expectEqual(@as(u8, 6), reader.version);

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
    try std.testing.expectEqual(@as(u8, 6), reader.version);

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

test "v5 hand-crafted section still reads correctly under v6 reader" {
    // Backward-compatibility check: existing on-disk segments are v5. The
    // reader must keep accepting their raw-u32 positions. We can't easily
    // round-trip a v5 segment via the builder (which now emits v6), so we
    // hand-assemble one with two positions per doc and verify decode.
    const alloc = std.testing.allocator;

    // Build the FST + postings via internal helpers, then stamp a v5 header
    // in front of them with the v3-v5 layout (no bloom_len).
    var fst_builder = try vellum.Builder.init(alloc, .{});
    defer fst_builder.deinit();

    var postings = std.ArrayListUnmanaged(u8).empty;
    defer postings.deinit(alloc);

    var acc = PostingAccumulator.init();
    defer acc.deinit(alloc);
    try acc.add(alloc, 0, 2, 10, &.{ 0, 5 });
    try acc.add(alloc, 1, 1, 8, &.{3});

    // Reach into the serialize() machinery — but it now emits v6 positions.
    // For a hand-crafted v5 section we need raw-u32 positions. Encode the
    // posting body manually (everything except positions) using the
    // accumulator helpers, then append v5-style positions.
    {
        // Doc freq.
        const doc_freq: u32 = @intCast(acc.doc_ids.items.len);
        try postings.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_freq))));

        // Roaring bitmap.
        var bitmap = roaring.RoaringBitmap.init(alloc);
        defer bitmap.deinit();
        try bitmap.addSortedAscending(acc.doc_ids.items);
        const bytes = try bitmap.toBytes(alloc);
        defer alloc.free(bytes);
        try postings.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(bytes.len))))));
        try postings.appendSlice(alloc, bytes);

        // Chunked freq/norm.
        var enc = try chunked.ChunkedIntEncoder.initWithMode(alloc, 1024, 2, .stream_vbyte);
        defer enc.deinit();
        for (acc.doc_ids.items, acc.freqs.items, acc.norms.items) |d, f, n| {
            const flv: u32 = @intCast(encodeFreqHasLocs(f, false));
            try enc.add(d, &.{ flv, n });
        }
        try enc.close();
        const fn_bytes = try enc.toBytes();
        defer alloc.free(fn_bytes);
        try postings.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(fn_bytes.len))))));
        try postings.appendSlice(alloc, fn_bytes);

        // Block-max: 1 chunk, single record (max_freq, min_norm, max_norm).
        try postings.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, 1)))));
        try postings.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, 2))));
        try postings.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, 8))));
        try postings.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, 10))));

        // v5 positions: per doc [num_positions: u16 LE][positions: u32 LE].
        var pos_section = std.ArrayListUnmanaged(u8).empty;
        defer pos_section.deinit(alloc);
        try pos_section.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, 2))));
        try pos_section.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 0))));
        try pos_section.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 5))));
        try pos_section.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, 1))));
        try pos_section.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 3))));

        try postings.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(pos_section.items.len))))));
        try postings.appendSlice(alloc, pos_section.items);
    }
    try fst_builder.insert("hello", 0);

    const fst_bytes = try fst_builder.finish();
    defer alloc.free(fst_bytes);

    // Stamp the 25-byte v5 header.
    const inv_header_size: usize = 25;
    const total = inv_header_size + postings.items.len + fst_bytes.len;
    var section = try alloc.alloc(u8, total);
    defer alloc.free(section);
    @memcpy(section[0..4], "INVT");
    section[4] = 5;
    section[5..9].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, 2)));
    section[9..17].* = @bitCast(std.mem.nativeToLittle(u64, @as(u64, 3)));
    section[17..21].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, 1024)));
    section[21..25].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(fst_bytes.len))));
    @memcpy(section[25..][0..postings.items.len], postings.items);
    @memcpy(section[25 + postings.items.len ..][0..fst_bytes.len], fst_bytes);

    // Read it back through the v6-aware reader.
    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expectEqual(@as(u8, 5), reader.version);
    try std.testing.expect(reader.term_bloom == null);

    const hello = reader.lookup("hello") orelse return error.TestExpectedEqual;
    var iter = try hello.iterator(alloc);
    defer iter.deinit();
    const h0 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 0), h0.doc_id);
    try std.testing.expectEqual(@as(u32, 2), h0.freq);
    try std.testing.expectEqual(@as(usize, 2), h0.positions.len);
    try std.testing.expectEqual(@as(u32, 0), h0.positions[0]);
    try std.testing.expectEqual(@as(u32, 5), h0.positions[1]);
    const h1 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), h1.doc_id);
    try std.testing.expectEqual(@as(usize, 1), h1.positions.len);
    try std.testing.expectEqual(@as(u32, 3), h1.positions[0]);
}
