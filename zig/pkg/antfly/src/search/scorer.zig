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

//! Block-Max WAND scorer for top-k multi-term BM25 queries.
//!
//! Accepts multiple term postings iterators and computes globally-consistent
//! BM25 scores using global document statistics (not per-segment).
//! When block-max metadata is available (v4 inverted index), uses WAND
//! to skip low-scoring blocks. Falls back to brute-force when absent.

const std = @import("std");
const Allocator = std.mem.Allocator;
const inverted = @import("../section/inverted.zig");

pub const ScoredHit = struct {
    doc_id: u32,
    score: f32,
};

/// Whether total_count is exact or a lower bound.
///
/// Block-max WAND can skip non-competitive documents once the top-k threshold
/// rises, so top-k searches should expose Lucene/Tantivy-style lower-bound
/// semantics instead of claiming an exact total hit count.
pub const TotalHitsRelation = enum {
    exact,
    gte,
};

pub const SearchResults = struct {
    hits: []ScoredHit,
    total_count: u32,
    total_relation: TotalHitsRelation = .exact,
};

/// Optional per-query work accounting. Production requests pass no diagnostics
/// pointer; benchmark/profile calls opt in and aggregate existing scorer-local
/// counters without global atomics.
pub const SearchDiagnostics = struct {
    segments_considered: u64 = 0,
    segments_searched: u64 = 0,
    segments_pruned: u64 = 0,
    postings_iterators_opened: u64 = 0,
    wand_next_in_score: u64 = 0,
    wand_next_in_advance: u64 = 0,
    wand_pivots_scored: u64 = 0,
    wand_pivots_advanced: u64 = 0,
    wand_chunks_skipped: u64 = 0,
    boolean_candidates_scored: u64 = 0,
    boolean_chunks_skipped: u64 = 0,
    phrase_candidates_verified: u64 = 0,
    phrase_position_records_decoded: u64 = 0,
    phrase_matches_scored: u64 = 0,

    pub fn addWand(self: *SearchDiagnostics, wand: *const WANDScorer) void {
        self.postings_iterators_opened +|= @intCast(wand.terms.items.len);
        self.wand_next_in_score +|= wand.next_in_score;
        self.wand_next_in_advance +|= wand.next_in_advance;
        self.wand_pivots_scored +|= wand.pivots_scored;
        self.wand_pivots_advanced +|= wand.pivots_advanced;
        self.wand_chunks_skipped +|= wand.chunks_skipped;
    }
};

pub fn scoredHitLessThan(_: void, a: ScoredHit, b: ScoredHit) bool {
    if (a.score == b.score) return a.doc_id < b.doc_id;
    return a.score > b.score;
}

pub fn sortScoredHits(hits: []ScoredHit) void {
    std.mem.sort(ScoredHit, hits, {}, scoredHitLessThan);
}

fn scoredHitWorseThan(a: ScoredHit, b: ScoredHit) bool {
    if (a.score == b.score) return a.doc_id > b.doc_id;
    return a.score < b.score;
}

fn scoredHitBetterThan(a: ScoredHit, b: ScoredHit) bool {
    if (a.score == b.score) return a.doc_id < b.doc_id;
    return a.score > b.score;
}

pub fn minTopKScore(hits: []const ScoredHit, k: u32) f32 {
    if (k == 0 or hits.len < k) return 0;
    var min_score = hits[0].score;
    for (hits[1..]) |hit| {
        if (hit.score < min_score) min_score = hit.score;
    }
    return min_score;
}

pub fn insertTopK(
    alloc: Allocator,
    top: *std.ArrayListUnmanaged(ScoredHit),
    k: u32,
    hit: ScoredHit,
) !void {
    if (k == 0) return;

    if (top.items.len < k) {
        try top.append(alloc, hit);
        return;
    }

    var min_idx: usize = 0;
    for (top.items[1..], 1..) |candidate, i| {
        if (scoredHitWorseThan(candidate, top.items[min_idx])) min_idx = i;
    }

    if (scoredHitBetterThan(hit, top.items[min_idx])) {
        top.items[min_idx] = hit;
    }
}

pub const TopKCollector = struct {
    alloc: Allocator,
    k: u32,
    hits: std.ArrayListUnmanaged(ScoredHit) = .empty,
    total_count: u32 = 0,
    total_relation: TotalHitsRelation = .exact,
    worst_index: usize = 0,

    pub fn init(alloc: Allocator, k: u32) TopKCollector {
        return .{ .alloc = alloc, .k = k };
    }

    pub fn deinit(self: *TopKCollector) void {
        self.hits.deinit(self.alloc);
    }

    pub fn topKLimit(self: *const TopKCollector) u32 {
        return self.k;
    }

    pub fn minCompetitiveScore(self: *const TopKCollector) f32 {
        if (self.k == 0 or self.hits.items.len < self.k) return 0;
        return self.hits.items[self.worst_index].score;
    }

    pub fn worstCompetitiveDocId(self: *const TopKCollector) ?u32 {
        if (self.k == 0 or self.hits.items.len < self.k) return null;
        return self.hits.items[self.worst_index].doc_id;
    }

    pub fn markLowerBound(self: *TopKCollector) void {
        self.total_relation = .gte;
    }

    pub fn markSkippedLowerBound(self: *TopKCollector) void {
        self.total_relation = .gte;
        self.total_count +|= 1;
    }

    pub fn collect(self: *TopKCollector, hit: ScoredHit) !void {
        self.total_count += 1;
        if (self.k == 0) return;
        if (self.hits.items.len < self.k) {
            try self.hits.append(self.alloc, hit);
            if (self.hits.items.len == self.k) self.refreshWorst();
            return;
        }
        if (scoredHitBetterThan(hit, self.hits.items[self.worst_index])) {
            self.hits.items[self.worst_index] = hit;
            self.refreshWorst();
        }
    }

    fn refreshWorst(self: *TopKCollector) void {
        self.worst_index = 0;
        for (self.hits.items[1..], 1..) |candidate, i| {
            if (scoredHitWorseThan(candidate, self.hits.items[self.worst_index])) self.worst_index = i;
        }
    }

    pub fn finishOwned(self: *TopKCollector) !SearchResults {
        sortScoredHits(self.hits.items);
        const hits = try self.hits.toOwnedSlice(self.alloc);
        return .{
            .hits = hits,
            .total_count = self.total_count,
            .total_relation = self.total_relation,
        };
    }
};

/// State for a single query term during WAND execution.
const TermState = struct {
    iter: inverted.PostingsIterator,
    current: ?inverted.PostingsIterator.Hit,
    idf: f32,
    bm25_scorer: inverted.BM25TermScorer,
    doc_freq: u32,
    block_max: ?inverted.BlockMaxInfo,
    chunk_size: u32,
    doc_offset: u32,
    exhausted: bool,
    /// Most recent chunk index this term was advanced into where the chunk's
    /// own max-impact alone met or exceeded the WAND threshold. As long as
    /// the term stays in this chunk and the threshold hasn't risen above the
    /// recorded value, the Block-Max chunk-skip check can short-circuit
    /// without re-reading block-max metadata. We only cache the solo-pass
    /// case; the combined `my_max + others_max` verdict depends on other
    /// terms' positions which can change between calls.
    skip_check_chunk: u32 = std.math.maxInt(u32),
    skip_check_threshold: f32 = 0,
    block_cursor: ?inverted.PostingsIterator.BlockCursor = null,
};

fn termIndexBefore(terms: []const TermState, left: usize, right: usize) bool {
    if (terms[left].exhausted) return false;
    if (terms[right].exhausted) return true;
    const left_doc = terms[left].current.?.doc_id;
    const right_doc = terms[right].current.?.doc_id;
    if (left_doc != right_doc) return left_doc < right_doc;
    return left < right;
}

/// Block-Max WAND scorer for top-k BM25 queries.
///
/// Usage:
///   var scorer = WANDScorer.init(alloc, 10, 1000, 50.0, .{});
///   defer scorer.deinit();
///   try scorer.addTerm(&postings_iter, 5, block_max, 1024);
///   const results = try scorer.execute();
pub const WANDScorer = struct {
    alloc: Allocator,
    terms: std.ArrayListUnmanaged(TermState),
    k: u32,
    global_doc_count: u32,
    global_avg_dl: f32,
    bm25_config: inverted.BM25Config,
    bound_table: ?*const inverted.BM25BoundTable = null,
    /// Diagnostics — populated during `execute*`. Useful for benchmarking
    /// where the WAND inner loop spends time. `next_in_score` is the count
    /// of `iter.next()` calls made when fully scoring a pivot doc;
    /// `next_in_advance` is the count made while skipping past the pivot.
    /// `chunks_skipped` counts how many advance calls were redirected past
    /// a low-impact chunk by the Block-Max chunk-skip optimization.
    next_in_score: u64 = 0,
    next_in_advance: u64 = 0,
    pivots_scored: u64 = 0,
    pivots_advanced: u64 = 0,
    chunks_skipped: u64 = 0,

    pub fn init(alloc: Allocator, k: u32, global_doc_count: u32, global_avg_dl: f32, bm25_config: inverted.BM25Config) WANDScorer {
        return .{
            .alloc = alloc,
            .terms = .empty,
            .k = k,
            .global_doc_count = global_doc_count,
            .global_avg_dl = global_avg_dl,
            .bm25_config = bm25_config,
        };
    }

    pub fn deinit(self: *WANDScorer) void {
        for (self.terms.items) |*t| t.iter.deinit();
        self.terms.deinit(self.alloc);
    }

    pub fn setBoundTable(self: *WANDScorer, table: *const inverted.BM25BoundTable) void {
        self.bound_table = table;
    }

    pub fn decodedChunkMetadataHeapBytes(self: *const WANDScorer) usize {
        var total: usize = 0;
        for (self.terms.items) |*term| total +|= term.iter.decodedChunkMetadataHeapBytes();
        return total;
    }

    /// Add a term's postings to the scorer. Takes ownership of the iterator.
    /// doc_offset is added to all doc IDs from this iterator (for multi-segment).
    pub fn addTerm(
        self: *WANDScorer,
        iter: inverted.PostingsIterator,
        doc_freq: u32,
        block_max: ?inverted.BlockMaxInfo,
        chunk_size: u32,
        doc_offset: u32,
    ) !void {
        const effective_doc_freq = @min(doc_freq, self.global_doc_count);

        // Precompute IDF: ln(1 + (N - n + 0.5) / (n + 0.5))
        const n: f32 = @floatFromInt(self.global_doc_count);
        const df: f32 = @floatFromInt(effective_doc_freq);
        const idf = @log(1.0 + (n - df + 0.5) / (df + 0.5));

        var iter_owned = iter;
        // BM25 scoring doesn't read positions; flip the iterator into the
        // fast path so `next()` skips the per-doc varint walk over positions.
        // Saves real wall time on phrase-aware indexes when the query is
        // ranking-only (the common case).
        iter_owned.decode_positions = false;
        try self.terms.append(self.alloc, .{
            .iter = iter_owned,
            .current = null,
            .idf = idf,
            .bm25_scorer = inverted.BM25TermScorer.init(self.global_avg_dl, idf, self.bm25_config),
            .doc_freq = effective_doc_freq,
            .block_max = block_max,
            .chunk_size = chunk_size,
            .doc_offset = doc_offset,
            .exhausted = false,
        });
    }

    /// Execute the query, returning up to k results sorted by score descending.
    pub fn execute(self: *WANDScorer) !SearchResults {
        var collector = TopKCollector.init(self.alloc, self.k);
        errdefer collector.deinit();
        try self.executeInto(&collector);
        return collector.finishOwned();
    }

    /// Execute the query into an external collector.
    ///
    /// The collector owns top-k state and exposes its current competitive score
    /// to the WAND loop. This mirrors Lucene's scorer/collector feedback model:
    /// as the global collector fills, subsequent pivot selection can skip blocks
    /// that cannot beat the collector's current threshold.
    pub fn executeInto(self: *WANDScorer, collector: anytype) !void {
        // Advance all iterators to their first hit, applying doc offset
        for (self.terms.items) |*t| {
            t.current = try t.iter.nextScoring();
            if (t.current) |*hit| {
                hit.doc_id += t.doc_offset;
            } else {
                t.exhausted = true;
            }
        }

        // Check if any term has block-max info
        var has_block_max = false;
        for (self.terms.items) |t| {
            if (t.block_max != null and !t.exhausted) {
                has_block_max = true;
                break;
            }
        }

        if (has_block_max) {
            if (collector.topKLimit() > 0) collector.markLowerBound();
            if (self.terms.items.len == 1) {
                try self.executeSingleTermWAND(collector);
            } else {
                try self.executeWAND(collector);
            }
        } else {
            try self.executeBruteForce(collector);
        }
    }

    /// Single-term Block-Max top-k does not need pivot ordering, cumulative
    /// bounds, or the generic multi-term front-interval sweep. Score the
    /// current posting directly and feed the raised threshold into the same
    /// conservative competitive-block advance used by WAND. Besides removing
    /// control flow, this avoids decoding the current impact bound once for
    /// pivot selection and again for post-collection skipping on every hit.
    fn executeSingleTermWAND(self: *WANDScorer, collector: anytype) !void {
        const term = &self.terms.items[0];
        const block_max = term.block_max.?;

        while (!term.exhausted) {
            const hit = term.current orelse break;
            self.pivots_scored += 1;
            self.next_in_score += 1;
            try collector.collect(.{
                .doc_id = hit.doc_id,
                .score = term.bm25_scorer.score(hit.freq, hit.norm),
            });

            if (hit.doc_id == std.math.maxInt(u32)) {
                term.current = null;
                term.exhausted = true;
                break;
            }

            const threshold = collector.minCompetitiveScore();
            const current_bound = term.iter.currentBlockMaxImpactWithScorer(
                block_max,
                term.bm25_scorer,
                term.idf,
                self.bound_table,
            );
            const worst_doc_id = collector.worstCompetitiveDocId();
            const ordered_tie_is_worse = current_bound == threshold and
                worst_doc_id != null and hit.doc_id >= worst_doc_id.?;
            if (current_bound < threshold or ordered_tie_is_worse) {
                const advanced = try term.iter.advanceToCompetitiveBlockWithScorer(
                    block_max,
                    threshold,
                    term.bm25_scorer,
                    term.idf,
                    self.bound_table,
                    true,
                );
                self.next_in_advance += 1;
                self.chunks_skipped +|= advanced.chunks_skipped;
                term.current = advanced.hit;
            } else {
                term.current = try term.iter.nextScoring();
            }
            if (term.current) |*next_hit| {
                next_hit.doc_id += term.doc_offset;
            } else {
                term.exhausted = true;
            }
        }
    }

    /// Brute-force: score every document that appears in any term's postings.
    fn executeBruteForce(self: *WANDScorer, collector: anytype) !void {
        while (true) {
            // Find the minimum doc_id across all active terms
            var min_doc: ?u32 = null;
            for (self.terms.items) |t| {
                if (t.exhausted) continue;
                const doc_id = t.current.?.doc_id;
                if (min_doc == null or doc_id < min_doc.?) {
                    min_doc = doc_id;
                }
            }
            if (min_doc == null) break;

            // Score this document across all terms that contain it
            var score: f32 = 0;
            for (self.terms.items) |*t| {
                if (t.exhausted) continue;
                if (t.current.?.doc_id == min_doc.?) {
                    const hit = t.current.?;
                    score += t.bm25_scorer.score(hit.freq, hit.norm);
                    t.current = try t.iter.nextScoring();
                    if (t.current) |*h| {
                        h.doc_id += t.doc_offset;
                    } else {
                        t.exhausted = true;
                    }
                }
            }

            try collector.collect(.{ .doc_id = min_doc.?, .score = score });
        }
    }

    /// Block-Max WAND: skip blocks whose max impact can't beat the threshold.
    /// Uses index-based sorting to avoid moving self-referential TermState structs.
    fn executeWAND(self: *WANDScorer, collector: anytype) !void {
        // Most search queries contain only a handful of terms. Keep their
        // ordering workspace on the stack so a multi-segment query does not
        // allocate and free one tiny slice per segment.
        var sorted_stack: [16]usize = undefined;
        const sorted = if (self.terms.items.len <= sorted_stack.len)
            sorted_stack[0..self.terms.items.len]
        else
            try self.alloc.alloc(usize, self.terms.items.len);
        defer if (self.terms.items.len > sorted_stack.len) self.alloc.free(sorted);
        for (sorted, 0..) |*s, i| s.* = i;

        var counted_skipped_lower_bound = false;
        while (true) {
            // Count active terms and sort indices by current doc_id
            var active: usize = 0;
            for (sorted) |idx| {
                if (!self.terms.items[idx].exhausted) active += 1;
            }
            if (active == 0) break;

            // Pivot advances usually move one iterator forward, so the prior
            // order is almost sorted. Insertion repair avoids a general sort
            // and its comparator setup on every WAND iteration.
            const terms = self.terms.items;
            var sort_pos: usize = 1;
            while (sort_pos < sorted.len) : (sort_pos += 1) {
                const moving = sorted[sort_pos];
                var insert_pos = sort_pos;
                while (insert_pos > 0 and termIndexBefore(terms, moving, sorted[insert_pos - 1])) : (insert_pos -= 1) {
                    sorted[insert_pos] = sorted[insert_pos - 1];
                }
                sorted[insert_pos] = moving;
            }

            if (try self.skipNonCompetitiveFrontBlock(collector, sorted)) continue;

            // Find pivot: smallest position where sum of max-impacts reaches
            // the collector's current competitive score.
            const threshold = collector.minCompetitiveScore();
            var cumulative: f32 = 0;
            var pivot_pos: ?usize = null;
            for (sorted, 0..) |idx, pos| {
                const t = &self.terms.items[idx];
                if (t.exhausted) break;
                cumulative += self.termMaxImpact(t);
                if (cumulative >= threshold) {
                    pivot_pos = pos;
                    break;
                }
            }

            if (pivot_pos == null) {
                if (!counted_skipped_lower_bound and collector.topKLimit() > 0) {
                    collector.markSkippedLowerBound();
                    counted_skipped_lower_bound = true;
                }
                break;
            }

            const pivot_doc = self.terms.items[sorted[pivot_pos.?]].current.?.doc_id;

            // Check if all terms up to pivot are at pivot_doc
            var all_at_pivot = true;
            for (sorted[0 .. pivot_pos.? + 1]) |idx| {
                const t = &self.terms.items[idx];
                if (t.exhausted or t.current.?.doc_id != pivot_doc) {
                    all_at_pivot = false;
                    break;
                }
            }

            if (all_at_pivot) {
                // Fully score this document
                self.pivots_scored += 1;
                var score: f32 = 0;
                for (self.terms.items) |*t| {
                    if (t.exhausted) continue;
                    if (t.current.?.doc_id == pivot_doc) {
                        const hit = t.current.?;
                        score += t.bm25_scorer.score(hit.freq, hit.norm);
                        self.next_in_score += 1;
                    }
                }
                try collector.collect(.{ .doc_id = pivot_doc, .score = score });

                // Multi-term WAND already reaches the chunk-skip path on
                // pivot advances. Calling advanceTo for every scored term adds
                // measurable overhead to balanced disjunctions where the
                // combined global bound prevents chunk pruning.
                for (self.terms.items) |*t| {
                    if (t.exhausted or t.current.?.doc_id != pivot_doc) continue;
                    t.current = try t.iter.nextScoring();
                    if (t.current) |*hit| {
                        hit.doc_id += t.doc_offset;
                    } else {
                        t.exhausted = true;
                    }
                }
            } else {
                // Advance the first term that's not at pivot_doc past pivot_doc.
                // Then apply Block-Max chunk skipping: if the chunk this term
                // landed in can't contribute enough (combined with the other
                // terms' max-impacts) to beat the current top-k threshold, hop
                // to the start of the next chunk and try again. This is the
                // core Block-Max WAND skip on top of WAND's pivot-level pruning.
                self.pivots_advanced += 1;
                for (sorted[0 .. pivot_pos.? + 1]) |idx| {
                    const t = &self.terms.items[idx];
                    if (!t.exhausted and t.current.?.doc_id < pivot_doc) {
                        try self.advancePastWithChunkSkip(t, pivot_doc, threshold, false);
                        break;
                    }
                }
            }
        }
        if (!counted_skipped_lower_bound and self.chunks_skipped > 0 and collector.topKLimit() > 0) {
            collector.markSkippedLowerBound();
        }
    }

    /// Apply an aligned Block-Max test to the lowest logical document block.
    /// Every active iterator is in the same segment for a WAND instance. If
    /// the sum of ceilings for terms that can occur in this block cannot beat
    /// top-k, advance all of them to the next block boundary at once.
    fn skipNonCompetitiveFrontBlock(self: *WANDScorer, collector: anytype, sorted: []const usize) !bool {
        const threshold = collector.minCompetitiveScore();
        const worst_doc_id = collector.worstCompetitiveDocId() orelse return false;
        if (threshold <= 0 or sorted.len == 0) return false;

        const front = &self.terms.items[sorted[0]];
        if (front.exhausted or front.current == null or front.chunk_size == 0) return false;
        if (!front.iter.doc_range_aligned) return self.skipNonCompetitivePostingBlockInterval(collector);
        for (self.terms.items) |*term| {
            if (term.exhausted or term.current == null) continue;
            if (term.block_max == null) return false;
            if (!term.iter.doc_range_aligned) return false;
            if (term.doc_offset != front.doc_offset or term.chunk_size != front.chunk_size) return false;
            term.block_cursor = term.iter.currentBlockCursor() orelse return false;
        }

        var skipped_blocks: u64 = 0;
        while (true) {
            var candidate_chunk: ?u32 = null;
            for (self.terms.items) |*term| {
                if (term.block_cursor) |cursor| candidate_chunk = if (candidate_chunk) |current| @min(current, cursor.chunk_id) else cursor.chunk_id;
            }
            const chunk_id = candidate_chunk orelse {
                for (self.terms.items) |*term| {
                    term.current = null;
                    term.exhausted = true;
                }
                self.chunks_skipped +|= skipped_blocks;
                return skipped_blocks > 0;
            };

            var bound: f32 = 0;
            for (self.terms.items) |*term| {
                const cursor = term.block_cursor orelse continue;
                if (cursor.chunk_id != chunk_id) continue;
                bound += term.iter.blockCursorImpactWithScorer(
                    term.block_max.?,
                    cursor,
                    term.bm25_scorer,
                    term.idf,
                    self.bound_table,
                );
            }
            const chunk_start_u64 = @as(u64, front.doc_offset) + @as(u64, chunk_id) * @as(u64, front.chunk_size);
            const tie_can_beat = chunk_start_u64 < worst_doc_id;
            if (bound > threshold or (bound == threshold and tie_can_beat)) {
                if (skipped_blocks == 0) return false;
                for (self.terms.items) |*term| {
                    const cursor = term.block_cursor orelse {
                        term.current = null;
                        term.exhausted = true;
                        continue;
                    };
                    self.next_in_advance += 1;
                    term.current = try term.iter.loadBlockCursor(cursor);
                    if (term.current) |*hit| {
                        hit.doc_id += term.doc_offset;
                    } else {
                        term.exhausted = true;
                    }
                }
                self.chunks_skipped +|= skipped_blocks;
                return true;
            }

            skipped_blocks +|= 1;
            for (self.terms.items) |*term| {
                if (term.block_cursor) |*cursor| {
                    if (cursor.chunk_id == chunk_id and !try term.iter.advanceBlockCursor(cursor)) term.block_cursor = null;
                }
            }
        }
    }

    /// Posting-count blocks do not share logical document-range IDs across
    /// terms. Sweep conservative block intervals instead: for each interval
    /// ending at the earliest active max-doc, sum every term block that could
    /// overlap it. A rejected interval can be skipped without decoding any of
    /// its postings; blocks spanning the boundary remain active in the next
    /// interval. This deliberately overestimates mixed-block scores.
    fn skipNonCompetitivePostingBlockInterval(self: *WANDScorer, collector: anytype) !bool {
        const threshold = collector.minCompetitiveScore();
        const worst_doc_id = collector.worstCompetitiveDocId() orelse return false;
        if (threshold <= 0) return false;

        var doc_offset: ?u32 = null;
        var scan_floor: u32 = std.math.maxInt(u32);
        for (self.terms.items) |*term| {
            if (term.exhausted or term.current == null) {
                term.block_cursor = null;
                continue;
            }
            if (term.block_max == null or term.iter.doc_range_aligned) return false;
            if (doc_offset) |offset| {
                if (term.doc_offset != offset) return false;
            } else {
                doc_offset = term.doc_offset;
            }
            var cursor = term.iter.currentBlockCursor() orelse return false;
            const local_doc = term.current.?.doc_id - term.doc_offset;
            cursor.min_doc = @max(cursor.min_doc, local_doc);
            scan_floor = @min(scan_floor, cursor.min_doc);
            term.block_cursor = cursor;
        }
        const offset = doc_offset orelse return false;

        var skipped_intervals: u64 = 0;
        var skipped_blocks: u64 = 0;
        while (true) {
            var interval_end: ?u32 = null;
            for (self.terms.items) |term| {
                const cursor = term.block_cursor orelse continue;
                interval_end = if (interval_end) |end| @min(end, cursor.max_doc) else cursor.max_doc;
            }
            const end = interval_end orelse {
                for (self.terms.items) |*term| {
                    term.current = null;
                    term.exhausted = true;
                }
                self.chunks_skipped +|= skipped_blocks;
                return skipped_intervals > 0;
            };

            var bound: f32 = 0;
            for (self.terms.items) |*term| {
                const cursor = term.block_cursor orelse continue;
                if (cursor.min_doc > end) continue;
                bound += term.iter.blockCursorImpactWithScorer(
                    term.block_max.?,
                    cursor,
                    term.bm25_scorer,
                    term.idf,
                    self.bound_table,
                );
            }
            const earliest_global = @as(u64, offset) + scan_floor;
            const competitive = bound > threshold or
                (bound == threshold and earliest_global < worst_doc_id);
            if (competitive) {
                if (skipped_intervals == 0) return false;
                for (self.terms.items) |*term| {
                    const cursor = term.block_cursor orelse {
                        term.current = null;
                        term.exhausted = true;
                        continue;
                    };
                    self.next_in_advance += 1;
                    term.current = try term.iter.loadBlockCursor(cursor);
                    if (term.current) |hit| {
                        if (hit.doc_id < scan_floor) term.current = try term.iter.advanceTo(scan_floor);
                    }
                    if (term.current) |*hit| {
                        hit.doc_id += term.doc_offset;
                    } else {
                        term.exhausted = true;
                    }
                }
                self.chunks_skipped +|= skipped_blocks;
                return true;
            }

            skipped_intervals +|= 1;
            if (end == std.math.maxInt(u32)) {
                for (self.terms.items) |*term| {
                    term.current = null;
                    term.exhausted = true;
                }
                self.chunks_skipped +|= skipped_blocks;
                return true;
            }
            scan_floor = end + 1;
            for (self.terms.items) |*term| {
                if (term.block_cursor) |*cursor| {
                    if (cursor.max_doc == end) {
                        skipped_blocks +|= 1;
                        if (!try term.iter.advanceBlockCursor(cursor)) term.block_cursor = null;
                    } else {
                        cursor.min_doc = @max(cursor.min_doc, scan_floor);
                    }
                }
            }
        }
    }

    /// Global maximum possible impact for a term. Pivot selection must not use
    /// only the current block's maximum: a later block can have a higher
    /// impact, so doing so could terminate before a competitive later hit.
    fn termMaxImpact(_: *const WANDScorer, t: *const TermState) f32 {
        return t.bm25_scorer.maxScore();
    }

    /// Advance a term iterator to the first doc whose ID (with offset
    /// applied) is >= `target`. Uses PostingsIterator.advanceTo, which seeks
    /// the underlying roaring bitmap and skip-positions the chunked freq/norm
    /// decoder in O(log container) instead of walking every doc.
    ///
    /// `next_in_advance` continues to count one tick per advance call so the
    /// `wand-skip-bench` ratios stay comparable across implementations.
    fn advancePast(self: *WANDScorer, t: *TermState, target: u32) !void {
        if (t.exhausted or t.current == null) {
            t.exhausted = true;
            return;
        }
        // Translate global target back to the iterator's local doc-id space.
        const local_target: u32 = if (target > t.doc_offset) target - t.doc_offset else 0;
        self.next_in_advance += 1;
        t.current = try t.iter.advanceTo(local_target);
        if (t.current) |*h| {
            h.doc_id += t.doc_offset;
        } else {
            t.exhausted = true;
        }
    }

    /// Advance a term past `target`, then keep skipping while it lands in a
    /// chunk whose `block_max` impact can't combine with the other terms'
    /// max-impacts to beat `threshold`. This is the Block-Max WAND advance
    /// optimization: pivot selection already skips low-impact chunks at the
    /// outer-loop level, but advances within the loop still walk into them
    /// once before re-pivoting kicks them out. The hop straight to the next
    /// chunk start saves the per-doc work for those throwaway chunks.
    fn advancePastWithChunkSkip(self: *WANDScorer, t: *TermState, target: u32, threshold: f32, allow_equal_prune: bool) !void {
        try self.advancePast(t, target);
        if (t.block_max == null) return;
        // No threshold yet (collector still filling): can't compute a useful
        // skip predicate, save the chunk-impact reads.
        if (threshold <= 0) return;

        // `others_max` is the sum of the other terms' max impacts at their
        // current positions. They don't move while we're hopping this term's
        // chunks, so the sum is constant across the inner loop. We compute
        // it lazily only on the first iteration that needs it — a chunk
        // whose `my_max` alone already beats `threshold` doesn't need to
        // know about the others, and uniform-distribution workloads hit
        // that case for nearly every advance.
        var others_max: ?f32 = null;

        while (!t.exhausted) {
            const current_cursor = t.iter.currentBlockCursor() orelse return;
            const chunk_idx: u32 = @intCast(current_cursor.ordinal);

            // Per-term cache: if this same chunk already passed the solo
            // check at a threshold ≥ the current one, the verdict still
            // holds (a smaller threshold can only widen the pass set).
            // Avoids re-reading block-max metadata + recomputing BM25 on
            // every same-chunk re-advance.
            if (!allow_equal_prune and chunk_idx == t.skip_check_chunk and threshold <= t.skip_check_threshold) return;

            const my_max = t.iter.currentBlockMaxImpactWithScorer(
                t.block_max.?,
                t.bm25_scorer,
                t.idf,
                self.bound_table,
            );
            if (my_max > threshold or (!allow_equal_prune and my_max == threshold)) {
                // Chunk passes solo. Cache the verdict — only the solo case
                // is safe to memoize because the combined check below depends
                // on other terms' positions, which can move between calls.
                t.skip_check_chunk = chunk_idx;
                t.skip_check_threshold = threshold;
                return;
            }

            if (others_max == null) {
                var sum: f32 = 0;
                for (self.terms.items) |*other| {
                    if (other == t) continue;
                    if (other.exhausted) continue;
                    // Other iterators can move into later chunks while this
                    // term remains in its current chunk. Their global maxima
                    // are the conservative bound for the combined check.
                    sum += self.termMaxImpact(other);
                }
                others_max = sum;
            }
            const combined_bound = my_max + others_max.?;
            if (combined_bound > threshold or (!allow_equal_prune and combined_bound == threshold)) return; // combined: don't cache

            self.chunks_skipped += 1;
            self.next_in_advance += 1;
            t.current = try t.iter.advanceToNextStoredChunk();
            if (t.current) |*hit| {
                hit.doc_id += t.doc_offset;
            } else {
                t.exhausted = true;
            }
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "brute-force scorer ranks by BM25" {
    const alloc = std.testing.allocator;

    // Build an index with 3 docs, 2 terms
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    try builder.addDocument(0, &.{
        .{ .term = "hello", .freq = 1, .norm = 10 },
        .{ .term = "world", .freq = 1, .norm = 10 },
    });
    try builder.addDocument(1, &.{
        .{ .term = "hello", .freq = 3, .norm = 15 },
    });
    try builder.addDocument(2, &.{
        .{ .term = "world", .freq = 2, .norm = 8 },
    });

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const avg_dl = reader.avgDocLen();

    // Search for "hello"
    {
        const lookup = reader.lookup("hello") orelse return error.TestExpectedEqual;
        const iter = try lookup.iterator(alloc);

        var scorer = WANDScorer.init(alloc, 10, reader.doc_count, avg_dl, .{});
        defer scorer.deinit();
        const bm = switch (lookup) {
            .postings => |p| p.block_max,
            .one_hit => null,
        };
        const cs = switch (lookup) {
            .postings => |p| p.scoringChunkSize(),
            .one_hit => 1024,
        };
        try scorer.addTerm(iter, lookup.docFreq(), bm, cs, 0);

        const results = try scorer.execute();
        defer alloc.free(results.hits);
        try std.testing.expectEqual(@as(usize, 2), results.hits.len);
        // Doc 1 (freq=3) should score higher than doc 0 (freq=1)
        try std.testing.expectEqual(@as(u32, 1), results.hits[0].doc_id);
        try std.testing.expectEqual(@as(u32, 0), results.hits[1].doc_id);
        try std.testing.expect(results.hits[0].score > results.hits[1].score);
    }
}

test "multi-term scorer combines scores" {
    const alloc = std.testing.allocator;

    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    // Doc 0 has both terms with high freq → should score highest
    try builder.addDocument(0, &.{
        .{ .term = "alpha", .freq = 3, .norm = 10 },
        .{ .term = "beta", .freq = 3, .norm = 10 },
    });
    try builder.addDocument(1, &.{
        .{ .term = "alpha", .freq = 1, .norm = 10 },
    });
    try builder.addDocument(2, &.{
        .{ .term = "beta", .freq = 1, .norm = 10 },
    });

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const avg_dl = reader.avgDocLen();

    // Search for "alpha" AND "beta" (both terms)
    const alpha_lookup = reader.lookup("alpha") orelse return error.TestExpectedEqual;
    const beta_lookup = reader.lookup("beta") orelse return error.TestExpectedEqual;
    const alpha_iter = try alpha_lookup.iterator(alloc);
    const beta_iter = try beta_lookup.iterator(alloc);

    var scorer = WANDScorer.init(alloc, 10, reader.doc_count, avg_dl, .{});
    defer scorer.deinit();

    const alpha_bm = switch (alpha_lookup) {
        .postings => |p| p.block_max,
        .one_hit => null,
    };
    const alpha_cs = switch (alpha_lookup) {
        .postings => |p| p.scoringChunkSize(),
        .one_hit => 1024,
    };
    const beta_bm = switch (beta_lookup) {
        .postings => |p| p.block_max,
        .one_hit => null,
    };
    const beta_cs = switch (beta_lookup) {
        .postings => |p| p.scoringChunkSize(),
        .one_hit => 1024,
    };
    try scorer.addTerm(alpha_iter, alpha_lookup.docFreq(), alpha_bm, alpha_cs, 0);
    try scorer.addTerm(beta_iter, beta_lookup.docFreq(), beta_bm, beta_cs, 0);

    const results = try scorer.execute();
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 3), results.hits.len);
    // Doc 0 has both terms → should score highest
    try std.testing.expectEqual(@as(u32, 0), results.hits[0].doc_id);
}

test "top-k limits results" {
    const alloc = std.testing.allocator;

    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    for (0..20) |i| {
        try builder.addDocument(@intCast(i), &.{
            .{ .term = "common", .freq = @as(u32, @intCast(i)) + 1, .norm = 10 },
        });
    }

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const avg_dl = reader.avgDocLen();
    const lookup = reader.lookup("common") orelse return error.TestExpectedEqual;
    const iter = try lookup.iterator(alloc);

    var scorer = WANDScorer.init(alloc, 5, reader.doc_count, avg_dl, .{});
    defer scorer.deinit();
    const bm = switch (lookup) {
        .postings => |p| p.block_max,
        .one_hit => null,
    };
    const cs = switch (lookup) {
        .postings => |p| p.scoringChunkSize(),
        .one_hit => 1024,
    };
    try scorer.addTerm(iter, lookup.docFreq(), bm, cs, 0);

    const results = try scorer.execute();
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 5), results.hits.len);
    try std.testing.expectEqual(TotalHitsRelation.gte, results.total_relation);
    // Results should be sorted by score descending
    for (0..results.hits.len - 1) |i| {
        try std.testing.expect(results.hits[i].score >= results.hits[i + 1].score);
    }
}

test "WAND pivot bound remains conservative across later high-impact blocks" {
    const alloc = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();
    try builder.addDocument(0, &.{.{ .term = "alpha", .freq = 1, .norm = 50 }});
    try builder.addDocument(1, &.{.{ .term = "beta", .freq = 1, .norm = 200 }});
    try builder.addDocument(2, &.{});
    try builder.addDocument(3, &.{});
    try builder.addDocument(4, &.{.{ .term = "beta", .freq = 100, .norm = 1 }});
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    var scorer = WANDScorer.init(alloc, 1, reader.doc_count, reader.avgDocLen(), .{});
    defer scorer.deinit();
    for ([_][]const u8{ "alpha", "beta" }) |term| {
        const lookup = reader.lookup(term) orelse return error.TestExpectedEqual;
        try scorer.addTerm(
            try lookup.iterator(alloc),
            lookup.docFreq(),
            switch (lookup) {
                .postings => |postings| postings.block_max,
                .one_hit => null,
            },
            switch (lookup) {
                .postings => |postings| postings.scoringChunkSize(),
                .one_hit => 2,
            },
            0,
        );
    }
    const results = try scorer.execute();
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    try std.testing.expectEqual(@as(u32, 4), results.hits[0].doc_id);
}

test "single-term block scan preserves a later higher-impact chunk" {
    const alloc = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();
    try builder.addDocument(0, &.{.{ .term = "term", .freq = 1, .norm = 100 }});
    try builder.addDocument(1, &.{.{ .term = "term", .freq = 1, .norm = 100 }});
    try builder.addDocument(2, &.{});
    try builder.addDocument(3, &.{});
    try builder.addDocument(4, &.{.{ .term = "term", .freq = 100, .norm = 1 }});
    try builder.addDocument(5, &.{.{ .term = "term", .freq = 1, .norm = 100 }});
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;
    var scorer = WANDScorer.init(alloc, 1, reader.doc_count, reader.avgDocLen(), .{});
    defer scorer.deinit();
    try scorer.addTerm(
        try lookup.iterator(alloc),
        lookup.docFreq(),
        switch (lookup) {
            .postings => |postings| postings.block_max,
            .one_hit => null,
        },
        2,
        0,
    );
    const results = try scorer.execute();
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    try std.testing.expectEqual(@as(u32, 4), results.hits[0].doc_id);
}

test "single-term equality pruning retains earliest cutoff ties" {
    const alloc = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();
    for (0..20) |doc| {
        try builder.addDocument(@intCast(doc), &.{.{ .term = "tie", .freq = 1, .norm = 10 }});
    }
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("tie") orelse return error.TestExpectedEqual;
    var scorer = WANDScorer.init(alloc, 2, reader.doc_count, reader.avgDocLen(), .{});
    defer scorer.deinit();
    try scorer.addTerm(
        try lookup.iterator(alloc),
        lookup.docFreq(),
        switch (lookup) {
            .postings => |postings| postings.block_max,
            .one_hit => null,
        },
        2,
        0,
    );
    const results = try scorer.execute();
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 2), results.hits.len);
    try std.testing.expectEqual(@as(u32, 0), results.hits[0].doc_id);
    try std.testing.expectEqual(@as(u32, 1), results.hits[1].doc_id);
    try std.testing.expect(scorer.chunks_skipped > 0);
}

test "shared top-k helper keeps best score with doc-id tie break" {
    const alloc = std.testing.allocator;

    var hits = std.ArrayListUnmanaged(ScoredHit).empty;
    defer hits.deinit(alloc);

    try insertTopK(alloc, &hits, 2, .{ .doc_id = 10, .score = 1.0 });
    try insertTopK(alloc, &hits, 2, .{ .doc_id = 20, .score = 1.0 });
    try insertTopK(alloc, &hits, 2, .{ .doc_id = 5, .score = 1.0 });
    try insertTopK(alloc, &hits, 2, .{ .doc_id = 30, .score = 2.0 });
    sortScoredHits(hits.items);

    try std.testing.expectEqual(@as(usize, 2), hits.items.len);
    try std.testing.expectEqual(@as(u32, 30), hits.items[0].doc_id);
    try std.testing.expectEqual(@as(u32, 5), hits.items[1].doc_id);
}

test "scorer executes into external top-k collector" {
    const alloc = std.testing.allocator;

    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    for (0..8) |i| {
        try builder.addDocument(@intCast(i), &.{
            .{ .term = "feedback", .freq = @as(u32, @intCast(i)) + 1, .norm = 10 },
        });
    }

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try inverted.InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("feedback") orelse return error.TestExpectedEqual;
    const iter = try lookup.iterator(alloc);

    var scorer = WANDScorer.init(alloc, 3, reader.doc_count, reader.avgDocLen(), .{});
    defer scorer.deinit();
    const bm = switch (lookup) {
        .postings => |p| p.block_max,
        .one_hit => null,
    };
    const cs = switch (lookup) {
        .postings => |p| p.scoringChunkSize(),
        .one_hit => 1024,
    };
    try scorer.addTerm(iter, lookup.docFreq(), bm, cs, 0);

    var collector = TopKCollector.init(alloc, 3);
    defer collector.deinit();
    try scorer.executeInto(&collector);
    const results = try collector.finishOwned();
    defer alloc.free(results.hits);

    try std.testing.expectEqual(TotalHitsRelation.gte, results.total_relation);
    try std.testing.expect(results.total_count > results.hits.len);
    try std.testing.expectEqual(@as(usize, 3), results.hits.len);
    try std.testing.expectEqual(@as(u32, 7), results.hits[0].doc_id);
}
