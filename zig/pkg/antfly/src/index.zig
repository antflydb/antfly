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

//! Multi-segment index with MVCC snapshots.
//!
//! Provides IndexSnapshot (immutable, ref-counted view of segments) and
//! IndexWriter (serialized writes with lock-free snapshot reads).
//!
//! Key design choices (improvements over bleve):
//!   - Lock-free snapshot reads via atomic pointer swap (no RWMutex)
//!   - Ref-counted snapshots for safe concurrent read/write access
//!   - Global BM25 stats across all segments for consistent scoring
//!   - Block-Max WAND acceleration when v4 inverted indexes are present
//!   - Per-query arena allocation for zero-alloc iteration

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const segment_mod = @import("segment.zig");
const inverted = @import("section/inverted.zig");
const roaring = @import("encoding/roaring.zig");
const scorer_mod = @import("search/scorer.zig");
const query_mod = @import("search/query.zig");
const distributed_stats_mod = @import("search/distributed_stats.zig");

fn spinOrYield() void {
    if (@import("builtin").os.tag == .freestanding) {
        std.atomic.spinLoopHint();
    } else {
        std.Thread.yield() catch {};
    }
}

/// An entry in a snapshot: one segment plus optional deletion bitmap.
pub const SegmentData = union(enum) {
    heap: []u8,
    mmap: []align(std.heap.page_size_min) u8,

    pub fn fromOwnedHeap(segment_bytes: []u8) SegmentData {
        return .{ .heap = segment_bytes };
    }

    pub fn fromMapped(segment_bytes: []align(std.heap.page_size_min) u8) SegmentData {
        return .{ .mmap = segment_bytes };
    }

    pub fn bytes(self: SegmentData) []const u8 {
        return switch (self) {
            .heap => |data| data,
            .mmap => |data| data,
        };
    }

    pub fn isFileBacked(self: SegmentData) bool {
        return switch (self) {
            .heap => false,
            .mmap => true,
        };
    }

    pub fn madviseAccessPattern(self: SegmentData) void {
        switch (self) {
            .heap => {},
            .mmap => |data| adviseMappedRandom(data),
        }
    }

    pub fn madviseDiscardCleanPages(self: SegmentData) void {
        switch (self) {
            .heap => {},
            .mmap => |data| adviseMappedDontNeed(data),
        }
    }

    pub fn deinit(self: *SegmentData, alloc: Allocator) void {
        switch (self.*) {
            .heap => |data| alloc.free(data),
            .mmap => |data| {
                if (builtin.os.tag != .freestanding) std.posix.munmap(data);
            },
        }
        self.* = undefined;
    }

    fn adviseMappedRandom(data: []align(std.heap.page_size_min) u8) void {
        switch (builtin.os.tag) {
            .linux, .emscripten, .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos, .freebsd => adviseMapped(data, std.c.MADV.RANDOM),
            else => {},
        }
    }

    fn adviseMappedDontNeed(data: []align(std.heap.page_size_min) u8) void {
        switch (builtin.os.tag) {
            .linux, .emscripten, .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos, .freebsd => adviseMapped(data, std.c.MADV.DONTNEED),
            else => {},
        }
    }

    fn adviseMapped(data: []align(std.heap.page_size_min) u8, advice: u32) void {
        std.posix.madvise(data.ptr, data.len, advice) catch {};
    }
};

/// Per-segment shared state, heap-allocated once per physical segment and
/// referenced by every snapshot generation that includes the segment.
/// Lifetime is reference-counted: each snapshot's `segments` slice owns one
/// reference per entry; the segment's resources are freed (and its retired
/// cleanup runs) when the last referencing snapshot releases. A held
/// snapshot therefore pins exactly the segments it can see — not whole
/// later generations, which is what the previous snapshot successor chain
/// pinned (under a merge storm that grew with churn rate × hold time, and a
/// leaked snapshot pinned every future generation).
pub const SegmentShared = struct {
    ref_count: u32,
    /// Deletion bitmap shared by every snapshot referencing this segment.
    /// Mutated only under the writer mutex. Readers of older snapshots may
    /// observe deletions made after their snapshot was taken — acceptable
    /// (deleted docs drop out early) and strictly safer than the previous
    /// copy-by-value field, whose stale copies aliased bitmap memory that
    /// setDeletionBitmap freed on replacement.
    deleted: ?roaring.RoaringBitmap = null,
    /// Set when the segment is replaced/removed from the live index. Runs
    /// once the last reference dies, after resources are deinited.
    retired_cleanup: ?RetiredSegmentCleanup = null,
};

pub const SegmentEntry = struct {
    id: u64,
    data: SegmentData,
    reader: segment_mod.SegmentReader,
    layout_stats: segment_mod.SegmentLayoutStats = .{},
    shared: *SegmentShared,

    fn retain(self: *const SegmentEntry) void {
        _ = @atomicRmw(u32, &self.shared.ref_count, .Add, 1, .monotonic);
    }

    /// Drop one reference. The final release deinits the segment's
    /// resources, runs its retired cleanup (if any), and destroys the
    /// shared cell.
    fn releaseRef(self: *SegmentEntry) void {
        if (@atomicRmw(u32, &self.shared.ref_count, .Sub, 1, .acq_rel) != 1) return;
        const alloc = self.reader.alloc;
        const seg_id = self.id;
        const cleanup = self.shared.retired_cleanup;
        self.data.madviseDiscardCleanPages();
        self.reader.deinit();
        if (self.shared.deleted) |*d| {
            var del = d.*;
            del.deinit();
        }
        self.data.deinit(alloc);
        alloc.destroy(self.shared);
        if (cleanup) |c| c.run(seg_id);
    }

    /// Number of live (non-deleted) documents.
    pub fn liveDocCount(self: *const SegmentEntry) u32 {
        if (self.shared.deleted) |d| {
            const del_count: u32 = @intCast(d.cardinality());
            return self.reader.doc_count -| del_count;
        }
        return self.reader.doc_count;
    }

    pub fn layoutStats(self: *const SegmentEntry, detailed_inverted: bool) segment_mod.SegmentLayoutStats {
        if (!detailed_inverted) return self.layout_stats;
        return self.reader.layoutStatsWithInvertedDetails(true);
    }
};

pub const ReplacementSegmentData = struct {
    id: u64,
    data: SegmentData,
};

pub const RetiredSegmentCleanup = struct {
    ptr: *anyopaque,
    delete: *const fn (ptr: *anyopaque, seg_id: u64) void,

    fn run(self: RetiredSegmentCleanup, seg_id: u64) void {
        self.delete(self.ptr, seg_id);
    }
};

const LiveDocCollector = struct {
    base: *scorer_mod.TopKCollector,
    deleted: ?*const roaring.RoaringBitmap = null,
    doc_offset: u32,

    pub fn topKLimit(self: *const LiveDocCollector) u32 {
        return self.base.topKLimit();
    }

    pub fn minCompetitiveScore(self: *const LiveDocCollector) f32 {
        return self.base.minCompetitiveScore();
    }

    pub fn worstCompetitiveDocId(self: *const LiveDocCollector) ?u32 {
        return self.base.worstCompetitiveDocId();
    }

    pub fn markLowerBound(self: *LiveDocCollector) void {
        self.base.markLowerBound();
    }

    pub fn collect(self: *LiveDocCollector, hit: scorer_mod.ScoredHit) !void {
        if (self.deleted) |deleted| {
            if (deleted.contains(hit.doc_id - self.doc_offset)) return;
        }
        try self.base.collect(hit);
    }
};

/// Cache key for `IndexSnapshot.termDocFreq`. Stores `field` and `term` in a
/// single owned allocation: `[field bytes][term bytes]`, with `field_len`
/// telling us where the split is. This lets us compare with borrowed
/// (field, term) inputs via `getOrPutAdapted` without allocating on hits.
const TermDocFreqKey = struct {
    storage: []const u8,
    field_len: u32,

    fn fieldBytes(self: TermDocFreqKey) []const u8 {
        return self.storage[0..self.field_len];
    }

    fn termBytes(self: TermDocFreqKey) []const u8 {
        return self.storage[self.field_len..];
    }
};

const TermDocFreqAdapted = struct {
    field: []const u8,
    term: []const u8,
};

const TermDocFreqStoredCtx = struct {
    pub fn hash(_: TermDocFreqStoredCtx, k: TermDocFreqKey) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(k.fieldBytes());
        h.update(&[_]u8{0});
        h.update(k.termBytes());
        return h.final();
    }
    pub fn eql(_: TermDocFreqStoredCtx, a: TermDocFreqKey, b: TermDocFreqKey) bool {
        if (a.field_len != b.field_len) return false;
        return std.mem.eql(u8, a.storage, b.storage);
    }
};

const TermDocFreqAdaptedCtx = struct {
    pub fn hash(_: TermDocFreqAdaptedCtx, k: TermDocFreqAdapted) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(k.field);
        h.update(&[_]u8{0});
        h.update(k.term);
        return h.final();
    }
    pub fn eql(_: TermDocFreqAdaptedCtx, a: TermDocFreqAdapted, b: TermDocFreqKey) bool {
        if (b.field_len != a.field.len) return false;
        return std.mem.eql(u8, b.fieldBytes(), a.field) and
            std.mem.eql(u8, b.termBytes(), a.term);
    }
};

const TermDocFreqCache = std.HashMapUnmanaged(
    TermDocFreqKey,
    u32,
    TermDocFreqStoredCtx,
    std.hash_map.default_max_load_percentage,
);

/// Immutable, ref-counted snapshot of the index state.
///
/// Snapshots obtained via `acquireSnapshot()` must be released via `release()`.
/// Snapshots obtained via `snapshot()` are borrowed (no ref change) and must
/// not outlive the next write operation.
pub const IndexSnapshot = struct {
    alloc: Allocator,
    ref_count: u32,
    epoch: u64,
    segments: []SegmentEntry,
    /// Global BM25 stats computed across all segments.
    global_doc_count: u32,
    global_total_field_len: std.StringHashMapUnmanaged(u64),
    // TODO: Profile significant_terms term-doc-freq lookups before adding a
    // persisted term-stat sidecar. If this cache shows up hot across snapshot
    // rebuilds/reopens, consider a sidecar keyed by segment/snapshot identity
    // instead of re-walking dictionaries/postings.
    term_doc_freq_cache_mu: std.atomic.Mutex,
    term_doc_freq_cache: TermDocFreqCache,
    term_doc_freq_cache_hits: u64,
    term_doc_freq_cache_misses: u64,
    /// Increment reference count. Returns self for chaining.
    pub fn retain(self: *IndexSnapshot) *IndexSnapshot {
        _ = @atomicRmw(u32, &self.ref_count, .Add, 1, .monotonic);
        return self;
    }

    /// Decrement reference count. Frees the snapshot when the count reaches
    /// zero, dropping one reference on each segment it includes — a segment
    /// replaced by a merge (its shared cell marked with a retired cleanup)
    /// is deinited and cleaned up when its LAST referencing snapshot dies,
    /// so a held snapshot pins exactly the segments it can see and nothing
    /// more. Observed in the field as a layoutStats segfault when a status
    /// walk held a snapshot across two publishes during a merge storm: the
    /// pre-refcount code deinited retired segments with the generation that
    /// merged them away, while earlier generations still referenced them.
    pub fn release(self: *IndexSnapshot) void {
        if (@atomicRmw(u32, &self.ref_count, .Sub, 1, .acq_rel) != 1) return;
        self.freeFinal();
    }

    /// Free everything this snapshot owns and destroy it. Only valid once
    /// the refcount has reached zero.
    fn freeFinal(self: *IndexSnapshot) void {
        const alloc = self.alloc;
        for (self.segments) |*seg| seg.releaseRef();
        alloc.free(self.segments);
        {
            const cache_mu = &self.term_doc_freq_cache_mu;
            while (!cache_mu.tryLock()) {
                spinOrYield();
            }
            defer cache_mu.unlock();
            var cache_it = self.term_doc_freq_cache.keyIterator();
            while (cache_it.next()) |key| alloc.free(key.storage);
            self.term_doc_freq_cache.deinit(alloc);
        }
        self.global_total_field_len.deinit(alloc);
        alloc.destroy(self);
    }

    /// Search across all segments for the given terms in a field.
    /// Returns up to k results sorted by BM25 score descending, along with the total match count.
    pub fn search(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
    ) !scorer_mod.SearchResults {
        return self.searchWithOverride(alloc, field, terms, k, null);
    }

    pub fn searchWithConfig(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
        bm25_config: inverted.BM25Config,
    ) !scorer_mod.SearchResults {
        return self.searchWithOverrideAndConfig(alloc, field, terms, k, null, bm25_config);
    }

    pub fn searchWithConfigDiagnostics(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
        bm25_config: inverted.BM25Config,
        diagnostics: *scorer_mod.SearchDiagnostics,
    ) !scorer_mod.SearchResults {
        return self.searchInternal(alloc, field, terms, k, null, bm25_config, diagnostics);
    }

    pub fn searchWithOverride(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
        override: ?distributed_stats_mod.TextFieldStats,
    ) !scorer_mod.SearchResults {
        return self.searchWithOverrideAndConfig(alloc, field, terms, k, override, .{});
    }

    pub fn searchWithOverrideAndConfig(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
        override: ?distributed_stats_mod.TextFieldStats,
        bm25_config: inverted.BM25Config,
    ) !scorer_mod.SearchResults {
        return self.searchInternal(alloc, field, terms, k, override, bm25_config, null);
    }

    fn searchInternal(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
        override: ?distributed_stats_mod.TextFieldStats,
        bm25_config: inverted.BM25Config,
        diagnostics: ?*scorer_mod.SearchDiagnostics,
    ) !scorer_mod.SearchResults {
        if (self.global_doc_count == 0 or terms.len == 0) return .{ .hits = try alloc.alloc(scorer_mod.ScoredHit, 0), .total_count = 0 };

        const global_doc_count = if (override) |stats| stats.global_doc_count else self.global_doc_count;
        if (global_doc_count == 0) return .{ .hits = try alloc.alloc(scorer_mod.ScoredHit, 0), .total_count = 0 };
        const avg_dl = if (override) |stats| stats.avgDocLen() else self.avgDocLen(field);
        var term_doc_freq_stack: [16]u32 = undefined;
        const term_doc_freqs = if (terms.len <= term_doc_freq_stack.len)
            term_doc_freq_stack[0..terms.len]
        else
            try alloc.alloc(u32, terms.len);
        defer if (terms.len > term_doc_freq_stack.len) alloc.free(term_doc_freqs);
        for (terms, 0..) |term, i| {
            term_doc_freqs[i] = if (override) |stats|
                stats.termDocFreq(term) orelse try self.termDocFreq(alloc, field, term)
            else if (self.segments.len > 1)
                try self.termDocFreq(alloc, field, term)
            else
                0;
        }

        var collector = scorer_mod.TopKCollector.init(alloc, k);
        defer collector.deinit();

        // Computing query-specific bounds opens every segment dictionary and
        // walks each term's block-max table before opening them again for
        // scoring. On a healthy tiered index (normally <= 10 segments), that
        // fixed work costs more than it saves. Reserve global segment ordering
        // for genuinely fragmented snapshots where pruning can amortize the
        // prepass; WAND still performs block-level pruning inside every
        // segment in the normal production state.
        const use_segment_bound_planning = self.segments.len > 16;

        const SegmentPlan = struct {
            segment_idx: usize,
            doc_offset: u32,
            score_upper_bound: f32,
        };
        var single_plan_storage: [1]SegmentPlan = undefined;
        const plans = if (self.segments.len <= single_plan_storage.len)
            single_plan_storage[0..self.segments.len]
        else
            try alloc.alloc(SegmentPlan, self.segments.len);
        defer if (self.segments.len > single_plan_storage.len) alloc.free(plans);
        var doc_offset: u32 = 0;
        for (self.segments, 0..) |*seg, segment_idx| {
            var upper_bound: f32 = std.math.inf(f32);
            if (use_segment_bound_planning) {
                upper_bound = 0;
                if (try seg.reader.invertedIndex(field)) |inv_reader| {
                    for (terms, 0..) |term, term_idx| {
                        const lookup_result = inv_reader.lookup(term) orelse continue;
                        const df = if (term_doc_freqs[term_idx] != 0) term_doc_freqs[term_idx] else lookup_result.docFreq();
                        upper_bound += switch (lookup_result) {
                            .postings => |p| if (p.block_max) |block_max|
                                block_max.maxImpactAll(global_doc_count, df, avg_dl, bm25_config)
                            else
                                inverted.bm25MaxScore(global_doc_count, df, bm25_config),
                            .one_hit => |hit| inverted.bm25Score(1, hit.norm_bits, global_doc_count, df, avg_dl, bm25_config),
                        };
                    }
                }
            }
            plans[segment_idx] = .{
                .segment_idx = segment_idx,
                .doc_offset = doc_offset,
                .score_upper_bound = upper_bound,
            };
            doc_offset = std.math.add(u32, doc_offset, seg.reader.doc_count) catch return error.CountOverflow;
        }
        if (use_segment_bound_planning) {
            std.mem.sort(SegmentPlan, plans, {}, struct {
                fn lessThan(_: void, a: SegmentPlan, b: SegmentPlan) bool {
                    if (a.score_upper_bound == b.score_upper_bound) return a.segment_idx < b.segment_idx;
                    return a.score_upper_bound > b.score_upper_bound;
                }
            }.lessThan);
        }
        if (diagnostics) |diag| diag.segments_considered +|= @intCast(plans.len);

        for (plans) |plan| {
            const seg = &self.segments[plan.segment_idx];
            // Strict inequality preserves deterministic tie-breaking: a
            // segment whose bound equals the threshold may still contain a
            // lower document ID at the cutoff.
            const threshold = collector.minCompetitiveScore();
            if (threshold > 0 and plan.score_upper_bound < threshold) {
                collector.markLowerBound();
                if (diagnostics) |diag| diag.segments_pruned +|= 1;
                continue;
            }
            const inv_reader = (try seg.reader.invertedIndex(field)) orelse {
                continue;
            };

            {
                var wand = scorer_mod.WANDScorer.init(alloc, k, global_doc_count, avg_dl, bm25_config);
                defer wand.deinit();
                var added_terms: usize = 0;

                for (terms, 0..) |term, term_idx| {
                    const lookup_result = inv_reader.lookup(term) orelse continue;
                    const iter = try lookup_result.iterator(alloc);

                    const block_max: ?inverted.BlockMaxInfo = switch (lookup_result) {
                        .postings => |p| p.block_max,
                        .one_hit => null,
                    };
                    const chunk_size: u32 = switch (lookup_result) {
                        .postings => |p| p.scoringChunkSize(),
                        .one_hit => 1024,
                    };

                    try wand.addTerm(
                        iter,
                        if (term_doc_freqs[term_idx] != 0) term_doc_freqs[term_idx] else lookup_result.docFreq(),
                        block_max,
                        chunk_size,
                        plan.doc_offset,
                    );
                    added_terms += 1;
                }

                if (added_terms == 0) {
                    continue;
                }
                if (diagnostics) |diag| diag.segments_searched +|= 1;

                var live_collector = LiveDocCollector{
                    .base = &collector,
                    .doc_offset = plan.doc_offset,
                };
                if (seg.shared.deleted) |*deleted| {
                    live_collector.deleted = deleted;
                }
                try wand.executeInto(&live_collector);
                if (diagnostics) |diag| diag.addWand(&wand);
            }
        }

        return collector.finishOwned();
    }

    /// Execute a filter across all segments, returning matching global doc IDs.
    pub fn executeFilter(self: *const IndexSnapshot, alloc: Allocator, filter: query_mod.Filter) ![]u32 {
        return query_mod.executeFilter(alloc, self, filter);
    }

    /// Count filter matches without materializing global document IDs.
    pub fn countFilter(self: *const IndexSnapshot, alloc: Allocator, filter: query_mod.Filter) !usize {
        return query_mod.countFilter(alloc, self, filter);
    }

    /// Map a global doc ID back to the segment and local doc ID.
    pub fn resolveDocId(self: *const IndexSnapshot, global_id: u32) ?struct { seg_idx: usize, local_id: u32 } {
        var offset: u32 = 0;
        for (self.segments, 0..) |*seg, i| {
            const count = seg.reader.doc_count;
            if (global_id < offset + count) {
                return .{ .seg_idx = i, .local_id = global_id - offset };
            }
            offset += count;
        }
        return null;
    }

    /// Get a stored document by global doc ID.
    pub fn storedDoc(self: *const IndexSnapshot, global_id: u32) ?segment_mod.SegmentReader.StoredDocRef {
        const resolved = self.resolveDocId(global_id) orelse return null;
        return self.segments[resolved.seg_idx].reader.storedDoc(resolved.local_id);
    }

    pub fn docOrdinal(self: *const IndexSnapshot, global_id: u32) !?u32 {
        const resolved = self.resolveDocId(global_id) orelse return null;
        return try self.segments[resolved.seg_idx].reader.docOrdinal(resolved.local_id);
    }

    /// Get and decompress a stored document by global doc ID into `alloc`.
    /// The caller owns the returned data.
    pub const DecompressedDoc = struct { id: []const u8, data: []u8 };

    pub fn storedDocDecompressed(self: *const IndexSnapshot, alloc: Allocator, global_id: u32) !?DecompressedDoc {
        const resolved = self.resolveDocId(global_id) orelse return null;
        const result = (try self.segments[resolved.seg_idx].reader.storedDocDecompressed(alloc, resolved.local_id)) orelse return null;
        return DecompressedDoc{ .id = result.id, .data = result.data };
    }

    pub fn docNumsForOrdinalsAlloc(self: *const IndexSnapshot, alloc: Allocator, ordinals: []const u32) ![]u32 {
        if (ordinals.len == 0) return try alloc.alloc(u32, 0);
        const sorted_ordinals = try alloc.dupe(u32, ordinals);
        defer alloc.free(sorted_ordinals);
        std.mem.sort(u32, sorted_ordinals, {}, u32LessThan);
        const unique_ordinals = sorted_ordinals[0..uniqueSortedU32(sorted_ordinals)];

        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);

        var doc_offset: u32 = 0;
        for (self.segments) |*seg| {
            for (0..seg.reader.doc_count) |local_usize| {
                const local_doc: u32 = @intCast(local_usize);
                if (seg.shared.deleted) |deleted| {
                    if (deleted.contains(local_doc)) continue;
                }
                const ordinal = (try seg.reader.docOrdinal(local_doc)) orelse continue;
                if (!containsSortedU32(unique_ordinals, ordinal)) continue;
                const global_doc = doc_offset + local_doc;
                try out.append(alloc, global_doc);
            }
            doc_offset += seg.reader.doc_count;
        }

        return try out.toOwnedSlice(alloc);
    }

    pub fn docOrdinalsForDocNumsAlloc(self: *const IndexSnapshot, alloc: Allocator, doc_nums: []const u32) !?[]u32 {
        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);

        for (doc_nums) |doc_num| {
            const ordinal = (try self.docOrdinal(doc_num)) orelse return null;
            try out.append(alloc, ordinal);
        }

        return try out.toOwnedSlice(alloc);
    }

    pub fn hasDocOrdinalCoverage(self: *const IndexSnapshot) bool {
        for (self.segments) |*seg| {
            if (seg.reader.doc_count == 0) continue;
            if (seg.reader.getSection(segment_mod.doc_ordinals_field, .doc_ordinals) == null) return false;
        }
        return true;
    }

    pub fn hasInvertedField(self: *const IndexSnapshot, field: []const u8) !bool {
        for (self.segments) |*seg| {
            if (try seg.reader.invertedIndex(field) != null) return true;
        }
        return false;
    }

    pub fn termDocFreq(self: *const IndexSnapshot, alloc: Allocator, field: []const u8, term: []const u8) !u32 {
        if (self.global_doc_count == 0) return 0;
        const mutable = @constCast(self);
        const adapted = TermDocFreqAdapted{ .field = field, .term = term };
        const adapted_ctx = TermDocFreqAdaptedCtx{};

        const cache_mu = &mutable.term_doc_freq_cache_mu;
        while (!cache_mu.tryLock()) {
            spinOrYield();
        }
        if (mutable.term_doc_freq_cache.getAdapted(adapted, adapted_ctx)) |cached| {
            mutable.term_doc_freq_cache_hits += 1;
            cache_mu.unlock();
            return cached;
        }
        mutable.term_doc_freq_cache_misses += 1;
        cache_mu.unlock();

        var total: u32 = 0;
        for (self.segments) |*seg| {
            const inv_reader = (try seg.reader.invertedIndex(field)) orelse continue;
            const lookup_result = inv_reader.lookup(term) orelse continue;

            if (seg.shared.deleted == null) {
                total += lookup_result.docFreq();
                continue;
            }

            var iter = try lookup_result.iterator(alloc);
            defer iter.deinit();
            while (try iter.next()) |hit| {
                if (!seg.shared.deleted.?.contains(hit.doc_id)) total += 1;
            }
        }

        while (!cache_mu.tryLock()) {
            spinOrYield();
        }
        defer cache_mu.unlock();
        const gop = try mutable.term_doc_freq_cache.getOrPutAdapted(self.alloc, adapted, adapted_ctx);
        if (gop.found_existing) {
            // Another caller raced us to the insert.
            return gop.value_ptr.*;
        }
        // Allocate the owning key only on the slow path.
        const storage = try self.alloc.alloc(u8, field.len + term.len);
        @memcpy(storage[0..field.len], field);
        @memcpy(storage[field.len..], term);
        gop.key_ptr.* = .{ .storage = storage, .field_len = @intCast(field.len) };
        gop.value_ptr.* = total;
        return total;
    }

    pub fn textAvgDocLen(self: *const IndexSnapshot, field: []const u8) f32 {
        return self.avgDocLen(field);
    }

    /// Return the exact scoring inputs for one term/document pair. This is a
    /// read-only engineering diagnostic used by the correctness-gated search
    /// kernel benchmark; production search does not call it.
    pub fn textTermStats(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        term: []const u8,
        doc_ordinal: u32,
    ) !?TextTermStats {
        const doc_nums = try self.docNumsForOrdinalsAlloc(alloc, &.{doc_ordinal});
        defer alloc.free(doc_nums);
        if (doc_nums.len != 1) return null;

        const resolved = self.resolveDocId(doc_nums[0]) orelse return null;
        const inv_reader = (try self.segments[resolved.seg_idx].reader.invertedIndex(field)) orelse return null;
        const lookup = inv_reader.lookup(term) orelse return null;
        var postings = try lookup.iterator(alloc);
        defer postings.deinit();
        const hit = (try postings.advanceTo(resolved.local_id)) orelse return null;
        if (hit.doc_id != resolved.local_id) return null;

        const total_field_len = self.global_total_field_len.get(field) orelse 0;
        const document_frequency = try self.termDocFreq(alloc, field, term);
        return .{
            .global_doc_count = self.global_doc_count,
            .total_field_len = total_field_len,
            .average_doc_length = self.avgDocLen(field),
            .document_frequency = document_frequency,
            .document_length = hit.norm,
            .term_frequency = hit.freq,
            .score = inverted.bm25Score(
                hit.freq,
                hit.norm,
                self.global_doc_count,
                document_frequency,
                self.avgDocLen(field),
                .{},
            ),
        };
    }

    fn avgDocLen(self: *const IndexSnapshot, field: []const u8) f32 {
        if (self.global_doc_count == 0) return 0;
        const total = self.global_total_field_len.get(field) orelse 0;
        return @as(f32, @floatFromInt(total)) / @as(f32, @floatFromInt(self.global_doc_count));
    }
};

pub const TextTermStats = struct {
    global_doc_count: u32,
    total_field_len: u64,
    average_doc_length: f32,
    document_frequency: u32,
    document_length: u32,
    term_frequency: u32,
    score: f32,
};

fn containsOrdinal(ordinals: []const u32, expected: u32) bool {
    for (ordinals) |ordinal| {
        if (ordinal == expected) return true;
    }
    return false;
}

fn u32LessThan(_: void, left: u32, right: u32) bool {
    return left < right;
}

fn uniqueSortedU32(values: []u32) usize {
    if (values.len == 0) return 0;
    var out: usize = 1;
    for (values[1..]) |value| {
        if (value == values[out - 1]) continue;
        values[out] = value;
        out += 1;
    }
    return out;
}

fn containsSortedU32(values: []const u32, expected: u32) bool {
    return std.sort.binarySearch(u32, values, expected, compareU32) != null;
}

fn compareU32(expected: u32, item: u32) std.math.Order {
    return std.math.order(expected, item);
}

/// Coordinates writes and maintains the current snapshot.
/// Reads are lock-free (atomic snapshot pointer).
/// Writes are serialized (mutex).
pub const IndexWriter = struct {
    alloc: Allocator,
    current: *IndexSnapshot,
    mu: std.atomic.Mutex,
    // Guards the load+retain in acquireSnapshot against the publish in the
    // swap sites. Without it a reader can load `current`, lose the CPU, and
    // retain a snapshot the writer has already swapped out and released to
    // refcount zero — resurrecting freed memory and crashing on the next
    // segment walk (observed as segfaults in layoutStats during merges).
    // Critical sections are a pointer load + refcount op, so a spinlock is
    // appropriate; the heavy release/deinit stays outside it.
    snapshot_mu: std.atomic.Mutex,
    next_segment_id: u64,
    next_epoch: u64,
    retired_segment_cleanup: ?RetiredSegmentCleanup = null,

    pub fn lockMutex(self: *IndexWriter) void {
        while (!self.mu.tryLock()) {
            spinOrYield();
        }
    }

    fn lockSnapshotMutex(self: *IndexWriter) void {
        while (!self.snapshot_mu.tryLock()) {
            spinOrYield();
        }
    }

    /// Publish a new snapshot. Must be the only way `current` is replaced.
    fn publishSnapshot(self: *IndexWriter, new_snap: *IndexSnapshot) void {
        self.lockSnapshotMutex();
        @atomicStore(*IndexSnapshot, &self.current, new_snap, .release);
        self.snapshot_mu.unlock();
    }

    pub fn init(alloc: Allocator) !IndexWriter {
        const snap = try alloc.create(IndexSnapshot);
        snap.* = .{
            .alloc = alloc,
            .ref_count = 1, // writer holds one ref
            .epoch = 0,
            .segments = &.{},
            .global_doc_count = 0,
            .global_total_field_len = .empty,
            .term_doc_freq_cache_mu = .unlocked,
            .term_doc_freq_cache = .empty,
            .term_doc_freq_cache_hits = 0,
            .term_doc_freq_cache_misses = 0,
        };
        return .{
            .alloc = alloc,
            .current = snap,
            .mu = .unlocked,
            .snapshot_mu = .unlocked,
            .next_segment_id = 1,
            .next_epoch = 1,
            .retired_segment_cleanup = null,
        };
    }

    pub fn setRetiredSegmentCleanup(self: *IndexWriter, cleanup: ?RetiredSegmentCleanup) void {
        self.retired_segment_cleanup = cleanup;
    }

    pub fn deinit(self: *IndexWriter) void {
        // Releases the writer's reference; with no outstanding readers this
        // frees the snapshot and drops the final reference on every live
        // segment (whose cells carry no retired cleanup, so closing an index
        // never deletes persisted segment files).
        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        snap.release();
    }

    /// Get the current snapshot (lock-free read, no ref change).
    /// The returned pointer is valid as long as the writer is alive
    /// and no concurrent writes occur. For concurrent safety, use acquireSnapshot().
    pub fn snapshot(self: *IndexWriter) *IndexSnapshot {
        return @atomicLoad(*IndexSnapshot, &self.current, .acquire);
    }

    /// Get the current snapshot with an incremented ref count.
    /// Caller MUST call release() when done. Safe for concurrent use.
    /// Load+retain happens under snapshot_mu so a concurrent publish cannot
    /// release the loaded snapshot to zero before we retain it.
    pub fn acquireSnapshot(self: *IndexWriter) *IndexSnapshot {
        self.lockSnapshotMutex();
        defer self.snapshot_mu.unlock();
        return @atomicLoad(*IndexSnapshot, &self.current, .acquire).retain();
    }

    /// Add a pre-built segment to the index.
    /// The data is duped internally; caller retains ownership of segment_bytes.
    pub fn addSegment(self: *IndexWriter, segment_bytes: []const u8) !void {
        self.lockMutex();
        defer self.mu.unlock();

        const owned = try self.alloc.dupe(u8, segment_bytes);
        errdefer self.alloc.free(owned);

        var data = SegmentData.fromOwnedHeap(owned);
        var reader = try segment_mod.SegmentReader.init(self.alloc, data.bytes());
        errdefer reader.deinit();

        const seg_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Build new snapshot with this segment appended
        const old = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        const new_segments = try self.alloc.alloc(SegmentEntry, old.segments.len + 1);
        errdefer self.alloc.free(new_segments);
        @memcpy(new_segments[0..old.segments.len], old.segments);
        const shared = try self.alloc.create(SegmentShared);
        shared.* = .{ .ref_count = 1 };
        errdefer self.alloc.destroy(shared);
        new_segments[old.segments.len] = .{
            .id = seg_id,
            .data = data,
            .reader = reader,
            .layout_stats = reader.layoutStats(),
            .shared = shared,
        };

        try self.rebuildSnapshot(new_segments, old.segments.len, &.{});
    }

    /// Rebuild and swap the index snapshot from a new segment list.
    ///
    /// `new_segments` is laid out as [carried..., brand-new...] with
    /// `carried_count` entries carried over from the current snapshot: this
    /// function retains one segment reference per carried entry on success
    /// (brand-new entries arrive with their creation reference already owned
    /// by the slice). `retired` stages entries being merged/removed away —
    /// their reference stays with the old snapshot; this function only marks
    /// their shared cells with the retired cleanup and frees the staging
    /// slice. On error the caller still owns everything it staged.
    fn rebuildSnapshot(self: *IndexWriter, new_segments: []SegmentEntry, carried_count: usize, retired: []SegmentEntry) !void {
        var global_doc_count: u32 = 0;
        var global_field_lens = std.StringHashMapUnmanaged(u64).empty;
        errdefer global_field_lens.deinit(self.alloc);
        for (new_segments) |*seg| {
            global_doc_count += seg.liveDocCount();
            for (seg.reader.fields) |*fi| {
                for (fi.sections) |*si| {
                    if (si.section_type == .inverted_text) {
                        const sec_data = seg.reader.data[@intCast(si.offset)..][0..@intCast(si.length)];
                        const inv = inverted.InvertedIndexReader.init(self.alloc, sec_data) catch continue;
                        const gop = try global_field_lens.getOrPut(self.alloc, fi.name);
                        if (!gop.found_existing) gop.value_ptr.* = 0;
                        gop.value_ptr.* += inv.total_field_len;
                    }
                }
            }
        }
        // Keep active mmap-backed segments warm once they are published. The
        // cleanup path below still drops retired source pages before those old
        // mappings are released.
        for (retired) |*seg| seg.data.madviseDiscardCleanPages();

        const new_snap = try self.alloc.create(IndexSnapshot);
        new_snap.* = .{
            .alloc = self.alloc,
            .ref_count = 1, // writer holds one ref
            .epoch = self.next_epoch,
            .segments = new_segments,
            .global_doc_count = global_doc_count,
            .global_total_field_len = global_field_lens,
            .term_doc_freq_cache_mu = .unlocked,
            .term_doc_freq_cache = .empty,
            .term_doc_freq_cache_hits = 0,
            .term_doc_freq_cache_misses = 0,
        };
        self.next_epoch += 1;

        const old = @atomicLoad(*IndexSnapshot, &self.current, .acquire);

        // Commit point — nothing below can fail. Retain the carried
        // segments for the new snapshot, and mark the retired segments'
        // cells so the LAST snapshot referencing each runs its cleanup.
        // Safe to write the cells without synchronization: the writer still
        // holds a reference on `old`, so no releaseRef can be completing
        // concurrently for segments old references.
        for (new_segments[0..carried_count]) |*seg| seg.retain();
        for (retired) |*seg| seg.shared.retired_cleanup = self.retired_segment_cleanup;
        if (retired.len > 0) self.alloc.free(retired);

        // Atomic swap so concurrent readers see a consistent pointer.
        self.publishSnapshot(new_snap);

        // Release writer's reference to old snapshot.
        old.release();
    }

    fn cloneGlobalFieldLens(alloc: Allocator, src: std.StringHashMapUnmanaged(u64)) !std.StringHashMapUnmanaged(u64) {
        var cloned = std.StringHashMapUnmanaged(u64).empty;
        errdefer cloned.deinit(alloc);

        var it = src.iterator();
        while (it.next()) |entry| {
            const gop = try cloned.getOrPut(alloc, entry.key_ptr.*);
            if (!gop.found_existing) gop.value_ptr.* = 0;
            gop.value_ptr.* = entry.value_ptr.*;
        }

        return cloned;
    }

    fn addSegmentFieldLens(
        alloc: Allocator,
        global_field_lens: *std.StringHashMapUnmanaged(u64),
        reader: *const segment_mod.SegmentReader,
    ) !void {
        for (reader.fields) |*fi| {
            if (std.mem.eql(u8, fi.name, segment_mod.doc_ordinals_field)) continue;
            for (fi.sections) |*si| {
                if (si.section_type != .inverted_text) continue;
                const offset: usize = @intCast(si.offset);
                if (offset > reader.data.len or si.length > reader.data.len - offset) continue;
                const sec_data = reader.data[offset..][0..@intCast(si.length)];
                const inv = inverted.InvertedIndexReader.init(alloc, sec_data) catch continue;
                const gop = try global_field_lens.getOrPut(alloc, fi.name);
                if (!gop.found_existing) gop.value_ptr.* = 0;
                gop.value_ptr.* += inv.total_field_len;
            }
        }
    }

    test "global field lens ignores invalid inverted section slice" {
        var sections = [_]segment_mod.SegmentReader.SectionInfo{.{
            .section_type = .inverted_text,
            .offset = 60,
            .length = 8,
        }};
        var fields = [_]segment_mod.SegmentReader.FieldInfo{.{
            .name = "content",
            .sections = sections[0..],
        }};
        const data = [_]u8{0} ** 64;
        const reader = segment_mod.SegmentReader{
            .alloc = std.testing.allocator,
            .data = &data,
            .stored_offset = data.len - 40,
            .index_offset = data.len - 40,
            .doc_count = 0,
            .num_fields = 1,
            .fields = fields[0..],
        };
        var field_lens = std.StringHashMapUnmanaged(u64).empty;
        defer field_lens.deinit(std.testing.allocator);

        try IndexWriter.addSegmentFieldLens(std.testing.allocator, &field_lens, &reader);
        try std.testing.expectEqual(@as(usize, 0), field_lens.count());
    }

    /// Like addSegment() but with an explicit segment ID (for recovery).
    pub fn addSegmentWithId(self: *IndexWriter, seg_id: u64, segment_bytes: []const u8) !void {
        const owned = try self.alloc.dupe(u8, segment_bytes);
        try self.addSegmentWithIdOwned(seg_id, owned);
    }

    /// Like addSegmentWithId(), but takes ownership of segment_bytes on success.
    pub fn addSegmentWithIdOwned(self: *IndexWriter, seg_id: u64, segment_bytes: []u8) !void {
        try self.addSegmentWithIdData(seg_id, SegmentData.fromOwnedHeap(segment_bytes));
    }

    pub fn addSegmentWithIdData(self: *IndexWriter, seg_id: u64, segment_data: SegmentData) !void {
        self.lockMutex();
        defer self.mu.unlock();

        var owned: ?SegmentData = segment_data;
        errdefer if (owned) |*data| data.deinit(self.alloc);

        var reader = try segment_mod.SegmentReader.init(self.alloc, owned.?.bytes());
        errdefer reader.deinit();

        const old = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        const new_segments = try self.alloc.alloc(SegmentEntry, old.segments.len + 1);
        errdefer self.alloc.free(new_segments);
        @memcpy(new_segments[0..old.segments.len], old.segments);
        const shared = try self.alloc.create(SegmentShared);
        shared.* = .{ .ref_count = 1 };
        errdefer self.alloc.destroy(shared);
        new_segments[old.segments.len] = .{
            .id = seg_id,
            .data = owned.?,
            .reader = reader,
            .layout_stats = reader.layoutStats(),
            .shared = shared,
        };

        if (seg_id >= self.next_segment_id) self.next_segment_id = seg_id + 1;

        var global_field_lens = try cloneGlobalFieldLens(self.alloc, old.global_total_field_len);
        errdefer global_field_lens.deinit(self.alloc);
        try addSegmentFieldLens(self.alloc, &global_field_lens, &reader);

        const new_snap = try self.alloc.create(IndexSnapshot);
        errdefer self.alloc.destroy(new_snap);
        new_snap.* = .{
            .alloc = self.alloc,
            .ref_count = 1,
            .epoch = self.next_epoch,
            .segments = new_segments,
            .global_doc_count = old.global_doc_count + new_segments[new_segments.len - 1].liveDocCount(),
            .global_total_field_len = global_field_lens,
            .term_doc_freq_cache_mu = .unlocked,
            .term_doc_freq_cache = .empty,
            .term_doc_freq_cache_hits = 0,
            .term_doc_freq_cache_misses = 0,
        };
        self.next_epoch += 1;

        // Commit point — retain the carried segments for the new snapshot
        // (the appended entry's creation reference is already owned by the
        // slice), then publish.
        for (new_segments[0..old.segments.len]) |*seg| seg.retain();
        self.publishSnapshot(new_snap);
        old.release();
        owned = null;
    }

    /// Atomically replace source segments with a merged segment (for merge).
    pub fn replaceSegments(self: *IndexWriter, old_ids: []const u64, new_id: u64, segment_bytes: []const u8) !void {
        const owned = try self.alloc.dupe(u8, segment_bytes);
        try self.replaceSegmentsOwned(old_ids, new_id, owned);
    }

    /// Like replaceSegments(), but takes ownership of segment_bytes on success.
    pub fn replaceSegmentsOwned(self: *IndexWriter, old_ids: []const u64, new_id: u64, segment_bytes: []u8) !void {
        var data: ?SegmentData = SegmentData.fromOwnedHeap(segment_bytes);
        errdefer if (data) |*owned| owned.deinit(self.alloc);
        try self.replaceSegmentsData(old_ids, new_id, data.?);
        data = null;
    }

    /// Takes ownership of segment_data only after the replacement snapshot is
    /// published successfully. On error, the caller still owns segment_data.
    pub fn replaceSegmentsData(self: *IndexWriter, old_ids: []const u64, new_id: u64, segment_data: SegmentData) !void {
        var replacement = [_]ReplacementSegmentData{.{
            .id = new_id,
            .data = segment_data,
        }};
        try self.replaceSegmentsManyData(old_ids, &replacement);
    }

    /// Takes ownership of replacement data only after the replacement snapshot
    /// is published successfully. On error, the caller still owns each data item.
    pub fn replaceSegmentsManyData(self: *IndexWriter, old_ids: []const u64, replacements: []ReplacementSegmentData) !void {
        if (replacements.len == 0) {
            try self.removeSegments(old_ids);
            return;
        }

        self.lockMutex();
        defer self.mu.unlock();

        const replacement_readers = try self.alloc.alloc(segment_mod.SegmentReader, replacements.len);
        var replacement_readers_initialized: usize = 0;
        defer self.alloc.free(replacement_readers);
        errdefer {
            for (replacement_readers[0..replacement_readers_initialized]) |*reader| reader.deinit();
        }

        for (replacements, 0..) |*replacement, i| {
            replacement_readers[i] = try segment_mod.SegmentReader.init(self.alloc, replacement.data.bytes());
            replacement_readers_initialized += 1;
        }

        const old = @atomicLoad(*IndexSnapshot, &self.current, .acquire);

        var keep_count: usize = 0;
        var retire_count: usize = 0;
        for (old.segments) |*seg| {
            var is_old = false;
            for (old_ids) |oid| {
                if (seg.id == oid) {
                    is_old = true;
                    break;
                }
            }
            if (is_old) {
                retire_count += 1;
            } else {
                keep_count += 1;
            }
        }

        const new_segments = try self.alloc.alloc(SegmentEntry, keep_count + replacements.len);
        errdefer self.alloc.free(new_segments);
        const retired = try self.alloc.alloc(SegmentEntry, retire_count);
        errdefer self.alloc.free(retired);
        var idx: usize = 0;
        var ret_idx: usize = 0;
        for (old.segments) |seg| {
            var is_old = false;
            for (old_ids) |oid| {
                if (seg.id == oid) {
                    is_old = true;
                    break;
                }
            }
            if (is_old) {
                retired[ret_idx] = seg;
                ret_idx += 1;
            } else {
                new_segments[idx] = seg;
                idx += 1;
            }
        }

        var cells_created: usize = 0;
        errdefer for (new_segments[keep_count .. keep_count + cells_created]) |*seg| self.alloc.destroy(seg.shared);
        for (replacements, 0..) |replacement, i| {
            const shared = try self.alloc.create(SegmentShared);
            shared.* = .{ .ref_count = 1 };
            new_segments[idx] = .{
                .id = replacement.id,
                .data = replacement.data,
                .reader = replacement_readers[i],
                .layout_stats = replacement_readers[i].layoutStats(),
                .shared = shared,
            };
            cells_created += 1;
            idx += 1;
            if (replacement.id >= self.next_segment_id) self.next_segment_id = replacement.id + 1;
        }

        try self.rebuildSnapshot(new_segments, keep_count, retired);
        replacement_readers_initialized = 0;
    }

    /// Atomically remove one or more segments without replacement.
    pub fn removeSegments(self: *IndexWriter, old_ids: []const u64) !void {
        if (old_ids.len == 0) return;

        self.lockMutex();
        defer self.mu.unlock();

        const old = @atomicLoad(*IndexSnapshot, &self.current, .acquire);

        var keep_count: usize = 0;
        var retire_count: usize = 0;
        for (old.segments) |*seg| {
            var is_old = false;
            for (old_ids) |oid| {
                if (seg.id == oid) {
                    is_old = true;
                    break;
                }
            }
            if (is_old) {
                retire_count += 1;
            } else {
                keep_count += 1;
            }
        }

        const new_segments = try self.alloc.alloc(SegmentEntry, keep_count);
        errdefer self.alloc.free(new_segments);
        const retired = try self.alloc.alloc(SegmentEntry, retire_count);
        errdefer self.alloc.free(retired);
        var idx: usize = 0;
        var ret_idx: usize = 0;
        for (old.segments) |seg| {
            var is_old = false;
            for (old_ids) |oid| {
                if (seg.id == oid) {
                    is_old = true;
                    break;
                }
            }
            if (is_old) {
                retired[ret_idx] = seg;
                ret_idx += 1;
            } else {
                new_segments[idx] = seg;
                idx += 1;
            }
        }

        try self.rebuildSnapshot(new_segments, keep_count, retired);
    }

    /// Set deletion bitmap for a segment by ID (for recovery).
    pub fn setDeletionBitmap(self: *IndexWriter, seg_id: u64, bitmap: roaring.RoaringBitmap) void {
        self.lockMutex();
        defer self.mu.unlock();

        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        for (snap.segments) |*seg| {
            if (seg.id == seg_id) {
                if (seg.shared.deleted) |*d| {
                    var old_del = d.*;
                    old_del.deinit();
                }
                seg.shared.deleted = bitmap;
                // Recompute global doc count
                var total: u32 = 0;
                for (snap.segments) |*s| {
                    total += s.liveDocCount();
                }
                snap.global_doc_count = total;
                return;
            }
        }
    }

    /// Delete a document by its external ID.
    /// Returns true if the document was found and deleted, false if not found.
    pub fn deleteById(self: *IndexWriter, doc_id: []const u8) !bool {
        const delete_infos = try self.deleteAllByIdTracked(self.alloc, doc_id);
        defer freeDeleteInfos(self.alloc, delete_infos);
        return delete_infos.len != 0;
    }

    pub const DeleteInfo = struct {
        seg_id: u64,
        bitmap_bytes: []u8,
        local_ids: []u32,
        applied_count: usize,
        created_bitmap: bool,
    };

    pub fn freeDeleteInfos(alloc: Allocator, delete_infos: []DeleteInfo) void {
        for (delete_infos) |delete_info| {
            if (delete_info.bitmap_bytes.len > 0) alloc.free(delete_info.bitmap_bytes);
            alloc.free(delete_info.local_ids);
        }
        alloc.free(delete_infos);
    }

    pub fn rollbackDeleteInfos(self: *IndexWriter, delete_infos: []const DeleteInfo) void {
        self.lockMutex();
        defer self.mu.unlock();
        self.rollbackDeleteInfosLocked(delete_infos);
    }

    fn rollbackDeleteInfosLocked(self: *IndexWriter, delete_infos: []const DeleteInfo) void {
        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        for (delete_infos) |delete_info| {
            for (snap.segments) |*seg| {
                if (seg.id != delete_info.seg_id) continue;
                if (seg.shared.deleted) |*deleted| {
                    for (delete_info.local_ids[0..delete_info.applied_count]) |local_id| deleted.removeRetainingStorage(local_id);
                    if (delete_info.created_bitmap and deleted.cardinality() == 0) {
                        deleted.deinit();
                        seg.shared.deleted = null;
                    }
                }
                snap.global_doc_count +|= @intCast(delete_info.applied_count);
                break;
            }
        }
    }

    /// Delete every live copy of a document and return one updated deletion
    /// bitmap per affected segment for atomic persistence by the caller.
    pub fn deleteAllByIdTracked(self: *IndexWriter, alloc: Allocator, doc_id: []const u8) ![]DeleteInfo {
        self.lockMutex();
        defer self.mu.unlock();

        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        var delete_infos = std.ArrayListUnmanaged(DeleteInfo).empty;
        errdefer {
            self.rollbackDeleteInfosLocked(delete_infos.items);
            for (delete_infos.items) |delete_info| {
                if (delete_info.bitmap_bytes.len > 0) alloc.free(delete_info.bitmap_bytes);
                alloc.free(delete_info.local_ids);
            }
            delete_infos.deinit(alloc);
        }

        for (snap.segments) |*seg| {
            var local_ids = std.ArrayListUnmanaged(u32).empty;
            defer local_ids.deinit(alloc);
            for (0..seg.reader.doc_count) |local_id| {
                const stored = seg.reader.storedDoc(@intCast(local_id)) orelse continue;
                if (std.mem.eql(u8, stored.id, doc_id)) {
                    if (seg.shared.deleted) |d| {
                        if (d.contains(@intCast(local_id))) continue;
                    }
                    try local_ids.append(alloc, @intCast(local_id));
                }
            }
            if (local_ids.items.len == 0) continue;

            const owned_local_ids = try local_ids.toOwnedSlice(alloc);
            var local_ids_transferred = false;
            errdefer if (!local_ids_transferred) alloc.free(owned_local_ids);
            const created_bitmap = seg.shared.deleted == null;
            try delete_infos.append(alloc, .{
                .seg_id = seg.id,
                .bitmap_bytes = &.{},
                .local_ids = owned_local_ids,
                .applied_count = 0,
                .created_bitmap = created_bitmap,
            });
            local_ids_transferred = true;
            if (created_bitmap) seg.shared.deleted = roaring.RoaringBitmap.init(self.alloc);
            for (owned_local_ids) |local_id| {
                try seg.shared.deleted.?.add(local_id);
                delete_infos.items[delete_infos.items.len - 1].applied_count += 1;
                snap.global_doc_count -|= 1;
            }
            delete_infos.items[delete_infos.items.len - 1].bitmap_bytes = try seg.shared.deleted.?.toBytes(alloc);
        }
        return try delete_infos.toOwnedSlice(alloc);
    }

    /// Update a document by its external ID: deletes the old version and indexes the new one.
    pub fn updateById(self: *IndexWriter, doc_id: []const u8, segment_bytes: []const u8) !void {
        _ = try self.deleteById(doc_id);
        try self.addSegment(segment_bytes);
    }
};

// ============================================================================
// Tests
// ============================================================================

fn buildTestSegment(alloc: Allocator, docs: []const struct { terms: []const inverted.InvertedIndexBuilder.TermHit }) ![]u8 {
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();

    for (docs, 0..) |doc, i| {
        try inv_builder.addDocument(@intCast(i), doc.terms);
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);

    for (docs, 0..) |_, i| {
        var id_buf: [16]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{i}) catch unreachable;
        try seg_writer.addStoredDoc(id_str, "{}");
    }

    return seg_writer.build();
}

test "single segment search" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{ .{ .term = "hello", .freq = 3, .norm = 15 }, .{ .term = "world", .freq = 1, .norm = 15 } } },
        .{ .terms = &.{.{ .term = "world", .freq = 2, .norm = 8 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    try std.testing.expectEqual(@as(u32, 3), snap.global_doc_count);
    try std.testing.expectEqual(@as(usize, 1), snap.segments.len);

    const results = try snap.search(alloc, "body", &.{"hello"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 2), results.hits.len);
    // Doc 1 (freq=3) should score higher
    try std.testing.expectEqual(@as(u32, 1), results.hits[0].doc_id);
}

test "multi-segment search" {
    const alloc = std.testing.allocator;

    const seg1 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "search", .freq = 2, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "engine", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);

    const seg2 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "search", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{ .{ .term = "search", .freq = 5, .norm = 12 }, .{ .term = "engine", .freq = 2, .norm = 12 } } },
    });
    defer alloc.free(seg2);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    const snap = writer.snapshot();
    try std.testing.expectEqual(@as(u32, 4), snap.global_doc_count);
    try std.testing.expectEqual(@as(usize, 2), snap.segments.len);

    const results = try snap.search(alloc, "body", &.{"search"}, 10);
    defer alloc.free(results.hits);
    // 3 docs contain "search" across 2 segments
    try std.testing.expectEqual(@as(usize, 3), results.hits.len);
    try std.testing.expect(results.hits[0].score >= results.hits[1].score);
    try std.testing.expect(results.hits[1].score >= results.hits[2].score);
}

test "multi-segment search merges per-segment top-k globally" {
    const alloc = std.testing.allocator;

    const seg1 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "rare", .freq = 10, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "rare", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);

    const seg2 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "rare", .freq = 9, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "rare", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    const snap = writer.snapshot();
    try std.testing.expectEqual(@as(usize, 2), snap.segments.len);

    const results = try snap.search(alloc, "body", &.{"rare"}, 2);
    defer alloc.free(results.hits);

    try std.testing.expectEqual(@as(usize, 2), results.hits.len);
    try std.testing.expectEqual(scorer_mod.TotalHitsRelation.gte, results.total_relation);
    try std.testing.expectEqual(@as(u32, 0), results.hits[0].doc_id);
    try std.testing.expectEqual(@as(u32, 2), results.hits[1].doc_id);
    try std.testing.expect(results.hits[0].score >= results.hits[1].score);
}

test "fragmented snapshot retains segment bound pruning" {
    const alloc = std.testing.allocator;
    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();

    for (0..17) |segment_idx| {
        const freq: u32 = if (segment_idx == 0) 100 else 1;
        const norm: u32 = if (segment_idx == 0) 1 else 100;
        const segment = try buildTestSegment(alloc, &.{
            .{ .terms = &.{.{ .term = "rare", .freq = freq, .norm = norm }} },
        });
        defer alloc.free(segment);
        try writer.addSegment(segment);
    }

    var diagnostics: scorer_mod.SearchDiagnostics = .{};
    const results = try writer.snapshot().searchWithConfigDiagnostics(
        alloc,
        "body",
        &.{"rare"},
        1,
        .{},
        &diagnostics,
    );
    defer alloc.free(results.hits);

    try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    try std.testing.expectEqual(@as(u32, 0), results.hits[0].doc_id);
    try std.testing.expectEqual(@as(u64, 17), diagnostics.segments_considered);
    try std.testing.expect(diagnostics.segments_pruned > 0);
}

test "retained snapshot remains readable after segment replacement" {
    const alloc = std.testing.allocator;

    const seg1 = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "old-a", .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);

    const seg2 = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "old-b", .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    const retained = writer.acquireSnapshot();
    defer retained.release();

    const merged = try segment_mod.mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);
    try writer.replaceSegments(&.{ 1, 2 }, 3, merged);

    const old_results = try retained.search(alloc, "body", &.{"alpha"}, 10);
    defer alloc.free(old_results.hits);
    try std.testing.expectEqual(@as(usize, 2), old_results.hits.len);

    const current_results = try writer.snapshot().search(alloc, "body", &.{"alpha"}, 10);
    defer alloc.free(current_results.hits);
    try std.testing.expectEqual(@as(usize, 2), current_results.hits.len);
    try std.testing.expectEqual(@as(usize, 1), writer.snapshot().segments.len);
}

test "empty index search" {
    const alloc = std.testing.allocator;

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();

    const snap = writer.snapshot();
    const results = try snap.search(alloc, "body", &.{"hello"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 0), results.hits.len);
}

fn buildTestSegmentWithIds(alloc: Allocator, docs: []const struct {
    id: []const u8,
    terms: []const inverted.InvertedIndexBuilder.TermHit,
}) ![]u8 {
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();

    for (docs, 0..) |doc, i| {
        try inv_builder.addDocument(@intCast(i), doc.terms);
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);

    for (docs) |doc| {
        try seg_writer.addStoredDoc(doc.id, "{}");
    }

    return seg_writer.build();
}

test "deleteById removes document from search results" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc-a", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "doc-b", .terms = &.{.{ .term = "hello", .freq = 2, .norm = 10 }} },
        .{ .id = "doc-c", .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    // Before delete: 3 docs, 2 match "hello"
    try std.testing.expectEqual(@as(u32, 3), writer.snapshot().global_doc_count);
    {
        const results = try writer.snapshot().search(alloc, "body", &.{"hello"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expectEqual(@as(usize, 2), results.hits.len);
    }

    // Delete doc-a
    const deleted = try writer.deleteById("doc-a");
    try std.testing.expect(deleted);
    try std.testing.expectEqual(@as(u32, 2), writer.snapshot().global_doc_count);

    // After delete: only doc-b should match "hello"
    {
        const results = try writer.snapshot().search(alloc, "body", &.{"hello"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    }

    // Delete non-existent doc
    const not_found = try writer.deleteById("doc-xyz");
    try std.testing.expect(!not_found);

    // Double delete returns false
    const double_del = try writer.deleteById("doc-a");
    try std.testing.expect(!double_del);
}

test "deleteById across multiple segments" {
    const alloc = std.testing.allocator;

    const seg1 = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "s1-a", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);

    const seg2 = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "s2-a", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    try std.testing.expectEqual(@as(u32, 2), writer.snapshot().global_doc_count);

    // Delete from second segment
    const del = try writer.deleteById("s2-a");
    try std.testing.expect(del);
    try std.testing.expectEqual(@as(u32, 1), writer.snapshot().global_doc_count);

    // Only s1-a should remain
    {
        const results = try writer.snapshot().search(alloc, "body", &.{"x"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    }
}

test "deleteById removes every live duplicate across historical segments" {
    const alloc = std.testing.allocator;

    const old_seg = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc:a", .terms = &.{.{ .term = "old", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(old_seg);
    const current_seg = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc:a", .terms = &.{.{ .term = "current", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(current_seg);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(old_seg);
    try std.testing.expect(try writer.deleteById("doc:a"));
    try writer.addSegment(current_seg);

    try std.testing.expect(try writer.deleteById("doc:a"));
    try std.testing.expectEqual(@as(u32, 0), writer.snapshot().global_doc_count);
    try std.testing.expect(!try writer.deleteById("doc:a"));

    const old_results = try writer.snapshot().search(alloc, "body", &.{"old"}, 10);
    defer alloc.free(old_results.hits);
    try std.testing.expectEqual(@as(usize, 0), old_results.hits.len);
    const current_results = try writer.snapshot().search(alloc, "body", &.{"current"}, 10);
    defer alloc.free(current_results.hits);
    try std.testing.expectEqual(@as(usize, 0), current_results.hits.len);
}

test "tracked multi-segment deletion can roll back before persistence" {
    const alloc = std.testing.allocator;
    const old_seg = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc:a", .terms = &.{.{ .term = "old", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(old_seg);
    const current_seg = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc:a", .terms = &.{.{ .term = "current", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(current_seg);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(old_seg);
    try writer.addSegment(current_seg);

    const delete_infos = try writer.deleteAllByIdTracked(alloc, "doc:a");
    defer IndexWriter.freeDeleteInfos(alloc, delete_infos);
    try std.testing.expectEqual(@as(usize, 2), delete_infos.len);
    try std.testing.expectEqual(@as(u32, 0), writer.snapshot().global_doc_count);

    writer.rollbackDeleteInfos(delete_infos);
    try std.testing.expectEqual(@as(u32, 2), writer.snapshot().global_doc_count);
    const old_results = try writer.snapshot().search(alloc, "body", &.{"old"}, 10);
    defer alloc.free(old_results.hits);
    try std.testing.expectEqual(@as(usize, 1), old_results.hits.len);
    const current_results = try writer.snapshot().search(alloc, "body", &.{"current"}, 10);
    defer alloc.free(current_results.hits);
    try std.testing.expectEqual(@as(usize, 1), current_results.hits.len);
}

test "index writer removeSegments frees staged segment list when retired allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();

    const seg1 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);
    const seg2 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "beta", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    failing.fail_index = failing.alloc_index + 1;
    try std.testing.expectError(error.OutOfMemory, writer.removeSegments(&.{1}));
    failing.fail_index = std.math.maxInt(usize);

    try std.testing.expectEqual(@as(usize, 2), writer.snapshot().segments.len);
}

test "index writer removeSegments frees staged segment lists when rebuild allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();

    const seg1 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);
    const seg2 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "beta", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    failing.fail_index = failing.alloc_index + 2;
    try std.testing.expectError(error.OutOfMemory, writer.removeSegments(&.{1}));
    failing.fail_index = std.math.maxInt(usize);

    try std.testing.expectEqual(@as(usize, 2), writer.snapshot().segments.len);
}

test "held snapshot pins exactly the retired segments it references" {
    const alloc = std.testing.allocator;

    const Cleaned = struct {
        ids: std.ArrayListUnmanaged(u64) = .empty,
        fn record(ptr: *anyopaque, seg_id: u64) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.ids.append(std.testing.allocator, seg_id) catch unreachable;
        }
    };
    var cleaned = Cleaned{};
    defer cleaned.ids.deinit(alloc);

    const seg1 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);
    const seg2 = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "beta", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);
    const merged = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "gamma", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(merged);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    writer.setRetiredSegmentCleanup(.{ .ptr = &cleaned, .delete = Cleaned.record });

    try writer.addSegment(seg1);

    // Hold the generation-1 snapshot across two later publishes — the
    // pattern of a status walk racing a merge storm.
    const held = writer.acquireSnapshot();

    try writer.addSegment(seg2); // generation 2
    const id1 = writer.snapshot().segments[0].id;
    const id2 = writer.snapshot().segments[1].id;
    // Generation 3: retires both segments. seg1 is still referenced by the
    // held gen-1 snapshot; seg2 is referenced only by gen 2, which dies at
    // the gen-3 publish.
    try writer.replaceSegments(&.{ id1, id2 }, id2 + 1, merged);

    // Per-segment refcounts pin exactly what the holder can see: seg2 (never
    // referenced by gen 1) is cleaned up the moment gen 2 dies, while seg1
    // stays alive for the holder. The old successor-chain design pinned BOTH
    // until the holder released.
    try std.testing.expectEqual(@as(usize, 1), cleaned.ids.items.len);
    try std.testing.expectEqual(id2, cleaned.ids.items[0]);

    // Walking the held snapshot must read live segment memory.
    var terms: u64 = 0;
    for (held.segments) |*seg| {
        const layout = seg.layoutStats(true);
        terms +|= layout.inverted_one_hit_terms +| layout.inverted_postings_terms;
    }
    try std.testing.expect(terms > 0);

    // Releasing the holder drops the last reference on seg1; only now does
    // its retired cleanup run.
    held.release();
    try std.testing.expectEqual(@as(usize, 2), cleaned.ids.items.len);
    try std.testing.expectEqual(id1, cleaned.ids.items[1]);
}
