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
const platform_time = @import("antfly_platform").time;
const resource_manager_mod = @import("storage/resource_manager.zig");

const mapped_residency_cold: u8 = 0;
const mapped_residency_resident: u8 = 1;
const mapped_residency_evicting: u8 = 2;
const mapped_residency_check_interval_ns: u64 = std.time.ns_per_s;
const mapped_residency_recent_ns: u64 = 30 * std.time.ns_per_s;
const mapped_residency_hard_min_age_ns: u64 = 5 * std.time.ns_per_s;

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
    /// Conservative per-mapping residency state. The virtual mapping remains
    /// intact when this transitions to cold; only clean file-backed pages are
    /// advised away. A subsequent query marks the segment resident again.
    mapped_residency_state: std.atomic.Value(u8) = .init(mapped_residency_cold),
    last_mapped_access_ns: std.atomic.Value(u64) = .init(0),
    active_mapped_readers: std.atomic.Value(u32) = .init(0),
    /// Deletion bitmap shared by every snapshot referencing this segment.
    /// Mutated only under the writer mutex. Readers of older snapshots may
    /// observe deletions made after their snapshot was taken — acceptable
    /// (deleted docs drop out early) and strictly safer than the previous
    /// copy-by-value field, whose stale copies aliased bitmap memory that
    /// setDeletionBitmap freed on replacement.
    deleted: ?roaring.RoaringBitmap = null,
    /// Monotonic epoch for in-place deletion changes. Every snapshot that
    /// references this segment observes the same epoch, so snapshot-local
    /// caches cannot retain pre-delete values after a newer snapshot mutates
    /// the shared tombstone bitmap.
    deletion_generation: std.atomic.Value(u64) = .init(0),
    /// Deleted BM25 field length by SegmentReader.fields index. This fixed-size
    /// vector is allocated with the segment and updated without allocation on
    /// delete/rollback. It keeps average field length proportional to live
    /// documents without rescanning term dictionaries at query time.
    deleted_field_lens: []std.atomic.Value(u64) = &.{},
    /// Set when the segment is replaced/removed from the live index. Runs
    /// once the last reference dies, after resources are deinited.
    retired_cleanup: ?RetiredSegmentCleanup = null,

    fn noteMappedAccess(self: *SegmentShared) void {
        self.last_mapped_access_ns.store(platform_time.monotonicNs(), .release);
        self.mapped_residency_state.store(mapped_residency_resident, .release);
    }
};

pub const SegmentEntry = struct {
    id: u64,
    data: SegmentData,
    reader: segment_mod.SegmentReader,
    layout_stats: segment_mod.SegmentLayoutStats = .{},
    shared: *SegmentShared,

    fn initShared(alloc: Allocator, shared: *SegmentShared, data: SegmentData, reader: *const segment_mod.SegmentReader) !void {
        const deleted_field_lens = try alloc.alloc(std.atomic.Value(u64), reader.fields.len);
        for (deleted_field_lens) |*field_len| field_len.* = .init(0);
        shared.* = .{
            .ref_count = 1,
            .deleted_field_lens = deleted_field_lens,
        };
        if (data.isFileBacked()) {
            // Opening a segment reads headers and dictionaries. Count the
            // whole mapping conservatively until the owner advises it cold.
            shared.mapped_residency_state.store(mapped_residency_resident, .release);
        }
    }

    fn createShared(alloc: Allocator, data: SegmentData, reader: *const segment_mod.SegmentReader) !*SegmentShared {
        const shared = try alloc.create(SegmentShared);
        errdefer alloc.destroy(shared);
        try initShared(alloc, shared, data, reader);
        return shared;
    }

    fn deinitSharedMetadata(self: *SegmentShared, alloc: Allocator) void {
        if (self.deleted_field_lens.len > 0) alloc.free(self.deleted_field_lens);
        self.deleted_field_lens = &.{};
    }

    pub fn noteAccess(self: *const SegmentEntry) void {
        if (self.data.isFileBacked()) self.shared.noteMappedAccess();
    }

    pub fn beginAccess(self: *const SegmentEntry) void {
        if (!self.data.isFileBacked()) return;
        _ = self.shared.active_mapped_readers.fetchAdd(1, .acq_rel);
        self.shared.noteMappedAccess();
    }

    pub fn endAccess(self: *const SegmentEntry) void {
        if (!self.data.isFileBacked()) return;
        _ = self.shared.active_mapped_readers.fetchSub(1, .acq_rel);
    }

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
        deinitSharedMetadata(self.shared, alloc);
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

const TermDocFreqCacheValue = struct {
    deletion_generation: u64,
    doc_freq: u32,
};

const TermDocFreqCache = std.HashMapUnmanaged(
    TermDocFreqKey,
    TermDocFreqCacheValue,
    TermDocFreqStoredCtx,
    std.hash_map.default_max_load_percentage,
);

const BM25BoundTableKey = struct {
    avg_doc_len_bits: u32,
    k1_bits: u32,
    b_bits: u32,
};

const BM25BoundTableCache = std.AutoHashMapUnmanaged(BM25BoundTableKey, *inverted.BM25BoundTable);
const max_bm25_bound_tables_per_snapshot: usize = 4;

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
    bm25_bound_table_cache_mu: std.atomic.Mutex,
    bm25_bound_table_cache: BM25BoundTableCache,
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
        {
            const cache_mu = &self.bm25_bound_table_cache_mu;
            while (!cache_mu.tryLock()) spinOrYield();
            defer cache_mu.unlock();
            var table_it = self.bm25_bound_table_cache.valueIterator();
            while (table_it.next()) |table| alloc.destroy(table.*);
            self.bm25_bound_table_cache.deinit(alloc);
        }
        self.global_total_field_len.deinit(alloc);
        alloc.destroy(self);
    }

    fn bm25BoundTable(
        self: *const IndexSnapshot,
        avg_doc_len: f32,
        config: inverted.BM25Config,
    ) !?*const inverted.BM25BoundTable {
        const key = BM25BoundTableKey{
            .avg_doc_len_bits = @bitCast(avg_doc_len),
            .k1_bits = @bitCast(config.k1),
            .b_bits = @bitCast(config.b),
        };
        const mutable = @constCast(self);
        const cache_mu = &mutable.bm25_bound_table_cache_mu;
        while (!cache_mu.tryLock()) spinOrYield();
        defer cache_mu.unlock();

        if (mutable.bm25_bound_table_cache.get(key)) |table| return table;
        if (mutable.bm25_bound_table_cache.count() >= max_bm25_bound_tables_per_snapshot) return null;

        const table = try self.alloc.create(inverted.BM25BoundTable);
        errdefer self.alloc.destroy(table);
        table.* = inverted.BM25BoundTable.init(avg_doc_len, config);
        try mutable.bm25_bound_table_cache.put(self.alloc, key, table);
        return table;
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
        const live_doc_count = self.liveDocCount();
        if (live_doc_count == 0 or terms.len == 0) return .{ .hits = try alloc.alloc(scorer_mod.ScoredHit, 0), .total_count = 0 };

        const global_doc_count = if (override) |stats| stats.global_doc_count else live_doc_count;
        if (global_doc_count == 0) return .{ .hits = try alloc.alloc(scorer_mod.ScoredHit, 0), .total_count = 0 };
        const avg_dl = if (override) |stats| stats.avgDocLen() else self.avgDocLen(field);
        const bound_table = try self.bm25BoundTable(avg_dl, bm25_config);
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
            seg.beginAccess();
            defer seg.endAccess();
            const inv_reader = (try seg.reader.invertedIndex(field)) orelse {
                continue;
            };

            {
                var wand = scorer_mod.WANDScorer.init(alloc, k, global_doc_count, avg_dl, bm25_config);
                defer wand.deinit();
                if (bound_table) |table| wand.setBoundTable(table);
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

    pub fn executeFilterBitmap(self: *const IndexSnapshot, alloc: Allocator, filter: query_mod.Filter) !roaring.RoaringBitmap {
        return query_mod.executeFilterBitmap(alloc, self, filter);
    }

    pub fn countFilterIntersection(
        self: *const IndexSnapshot,
        alloc: Allocator,
        filter: query_mod.Filter,
        global_docs: *const roaring.RoaringBitmap,
    ) !usize {
        return query_mod.countFilterIntersection(alloc, self, filter, global_docs);
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
    pub fn storedDoc(self: *const IndexSnapshot, global_id: u32) !?segment_mod.SegmentReader.StoredDocRef {
        const resolved = self.resolveDocId(global_id) orelse return null;
        self.segments[resolved.seg_idx].noteAccess();
        return try self.segments[resolved.seg_idx].reader.storedDoc(resolved.local_id);
    }

    pub fn docOrdinal(self: *const IndexSnapshot, global_id: u32) !?u32 {
        const resolved = self.resolveDocId(global_id) orelse return null;
        self.segments[resolved.seg_idx].noteAccess();
        return try self.segments[resolved.seg_idx].reader.docOrdinal(resolved.local_id);
    }

    /// Get and decompress a stored document by global doc ID into `alloc`.
    /// The caller owns the returned data.
    pub const DecompressedDoc = struct { id: []const u8, data: []u8 };

    pub fn storedDocDecompressed(self: *const IndexSnapshot, alloc: Allocator, global_id: u32) !?DecompressedDoc {
        const resolved = self.resolveDocId(global_id) orelse return null;
        self.segments[resolved.seg_idx].noteAccess();
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

    pub fn hasDocOrdinalCoverage(self: *const IndexSnapshot) !bool {
        for (self.segments) |*seg| {
            if (seg.reader.doc_count == 0) continue;
            if ((try seg.reader.getSection(segment_mod.doc_ordinals_field, .doc_ordinals)) == null) return false;
        }
        return true;
    }

    pub fn hasInvertedField(self: *const IndexSnapshot, field: []const u8) !bool {
        for (self.segments) |*seg| {
            if (try seg.reader.invertedIndex(field) != null) return true;
        }
        return false;
    }

    /// Aggregate the monotonic deletion epochs of exactly the segments visible
    /// to this snapshot. Epochs live in SegmentShared, so a retained older
    /// snapshot observes mutations to carried segments without being
    /// invalidated by changes to segments it does not contain.
    fn deletionGeneration(self: *const IndexSnapshot) u64 {
        var generation: u64 = 0;
        for (self.segments) |*seg| {
            generation +%= seg.shared.deletion_generation.load(.acquire);
        }
        return generation;
    }

    /// Current live count rather than the publish-time count. Segment deletion
    /// state is shared across retained snapshots, so scoring must derive the
    /// denominator from the same state used to mask hits.
    pub fn liveDocCount(self: *const IndexSnapshot) u32 {
        var total: u32 = 0;
        for (self.segments) |*seg| total +|= seg.liveDocCount();
        return total;
    }

    /// Count live documents for one segment/term without turning a sparse
    /// tombstone into a full postings scan. For sparse deletions, seek the
    /// postings iterator to each deleted document; for dense deletions, scan
    /// the smaller postings side once. Both paths preserve exact BM25 stats.
    fn liveSegmentTermDocFreq(
        alloc: Allocator,
        lookup_result: inverted.LookupResult,
        deleted: *const roaring.RoaringBitmap,
    ) !u32 {
        const raw_doc_freq = lookup_result.docFreq();
        if (raw_doc_freq == 0 or deleted.isEmpty()) return raw_doc_freq;

        var removed: u32 = 0;
        var postings = try lookup_result.iterator(alloc);
        defer postings.deinit();
        postings.decode_positions = false;

        if (deleted.cardinality() >= raw_doc_freq) {
            while (try postings.nextScoring()) |hit| {
                if (deleted.contains(hit.doc_id)) removed += 1;
            }
            return raw_doc_freq -| removed;
        }

        // advanceTo consumes the first posting at or after its target. Keep a
        // one-item lookahead when that posting belongs to a later tombstone so
        // it can still be reconciled without restarting the iterator.
        var pending_doc: ?u32 = null;
        var deleted_it = deleted.iterator();
        while (deleted_it.next()) |deleted_doc| {
            if (pending_doc) |pending| {
                if (pending == deleted_doc) {
                    removed += 1;
                    pending_doc = null;
                    continue;
                }
                if (pending > deleted_doc) continue;
                pending_doc = null;
            }

            const hit = (try postings.advanceTo(deleted_doc)) orelse break;
            if (hit.doc_id == deleted_doc) {
                removed += 1;
            } else {
                pending_doc = hit.doc_id;
            }
        }
        return raw_doc_freq -| removed;
    }

    pub fn termDocFreq(self: *const IndexSnapshot, alloc: Allocator, field: []const u8, term: []const u8) !u32 {
        if (self.liveDocCount() == 0) return 0;
        const mutable = @constCast(self);
        const adapted = TermDocFreqAdapted{ .field = field, .term = term };
        const adapted_ctx = TermDocFreqAdaptedCtx{};

        while (true) {
            const deletion_generation = self.deletionGeneration();
            const cache_mu = &mutable.term_doc_freq_cache_mu;
            while (!cache_mu.tryLock()) spinOrYield();
            if (mutable.term_doc_freq_cache.getAdapted(adapted, adapted_ctx)) |cached| {
                if (cached.deletion_generation == deletion_generation) {
                    mutable.term_doc_freq_cache_hits += 1;
                    cache_mu.unlock();
                    return cached.doc_freq;
                }
            }
            mutable.term_doc_freq_cache_misses += 1;
            cache_mu.unlock();

            var total: u32 = 0;
            for (self.segments) |*seg| {
                const inv_reader = (try seg.reader.invertedIndex(field)) orelse continue;
                const lookup_result = inv_reader.lookup(term) orelse continue;
                total += if (seg.shared.deleted) |*deleted|
                    try liveSegmentTermDocFreq(alloc, lookup_result, deleted)
                else
                    lookup_result.docFreq();
            }

            // A shared segment changed while postings were reconciled. Retry
            // instead of publishing a mixed-generation statistic.
            if (self.deletionGeneration() != deletion_generation) continue;

            while (!cache_mu.tryLock()) spinOrYield();
            if (self.deletionGeneration() != deletion_generation) {
                cache_mu.unlock();
                continue;
            }
            defer cache_mu.unlock();

            // Allocate before claiming a vacant hash slot. If allocation
            // fails after getOrPut, the map would otherwise retain an
            // uninitialized key/value entry.
            const storage = try self.alloc.alloc(u8, field.len + term.len);
            errdefer self.alloc.free(storage);
            const gop = try mutable.term_doc_freq_cache.getOrPutAdapted(self.alloc, adapted, adapted_ctx);
            if (gop.found_existing) {
                self.alloc.free(storage);
                // Another caller may have populated this generation while we
                // computed. Prefer its identical exact result; otherwise
                // replace the stale generation in place without growing or
                // clearing the cache.
                if (gop.value_ptr.deletion_generation == deletion_generation) return gop.value_ptr.doc_freq;
                gop.value_ptr.* = .{ .deletion_generation = deletion_generation, .doc_freq = total };
                return total;
            }
            @memcpy(storage[0..field.len], field);
            @memcpy(storage[field.len..], term);
            gop.key_ptr.* = .{ .storage = storage, .field_len = @intCast(field.len) };
            gop.value_ptr.* = .{ .deletion_generation = deletion_generation, .doc_freq = total };
            return total;
        }
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

        const global_doc_count = self.liveDocCount();
        const total_field_len = self.liveTotalFieldLen(field);
        const document_frequency = try self.termDocFreq(alloc, field, term);
        return .{
            .global_doc_count = global_doc_count,
            .total_field_len = total_field_len,
            .average_doc_length = self.avgDocLen(field),
            .document_frequency = document_frequency,
            .document_length = hit.norm,
            .term_frequency = hit.freq,
            .score = inverted.bm25Score(
                hit.freq,
                hit.norm,
                global_doc_count,
                document_frequency,
                self.avgDocLen(field),
                .{},
            ),
        };
    }

    fn avgDocLen(self: *const IndexSnapshot, field: []const u8) f32 {
        const live_doc_count = self.liveDocCount();
        if (live_doc_count == 0) return 0;
        return @as(f32, @floatFromInt(self.liveTotalFieldLen(field))) /
            @as(f32, @floatFromInt(live_doc_count));
    }

    pub fn liveTotalFieldLen(self: *const IndexSnapshot, field: []const u8) u64 {
        var deleted_total: u64 = 0;
        for (self.segments) |*seg| {
            for (seg.reader.fields, 0..) |*field_info, field_idx| {
                if (!std.mem.eql(u8, field_info.name, field)) continue;
                if (field_idx < seg.shared.deleted_field_lens.len) {
                    deleted_total +|= seg.shared.deleted_field_lens[field_idx].load(.acquire);
                }
                break;
            }
        }
        const total = self.global_total_field_len.get(field) orelse 0;
        return total -| deleted_total;
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
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    mapped_residency_mu: std.atomic.Mutex,
    mapped_residency_accounted_bytes: u64,
    mapped_residency_next_check_ns: std.atomic.Value(u64),
    mapped_residency_evictions: u64,

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
            .bm25_bound_table_cache_mu = .unlocked,
            .bm25_bound_table_cache = .empty,
        };
        return .{
            .alloc = alloc,
            .current = snap,
            .mu = .unlocked,
            .snapshot_mu = .unlocked,
            .next_segment_id = 1,
            .next_epoch = 1,
            .retired_segment_cleanup = null,
            .resource_manager = null,
            .mapped_residency_mu = .unlocked,
            .mapped_residency_accounted_bytes = 0,
            .mapped_residency_next_check_ns = .init(0),
            .mapped_residency_evictions = 0,
        };
    }

    pub fn attachResourceManager(self: *IndexWriter, manager: *resource_manager_mod.ResourceManager) void {
        self.lockMappedResidencyMutex();
        self.resource_manager = manager;
        self.mapped_residency_next_check_ns.store(0, .release);
        self.mapped_residency_mu.unlock();

        const snap = self.acquireSnapshotRaw();
        defer snap.release();
        self.maybeMaintainMappedResidency(snap, platform_time.monotonicNs());
    }

    pub fn setRetiredSegmentCleanup(self: *IndexWriter, cleanup: ?RetiredSegmentCleanup) void {
        self.retired_segment_cleanup = cleanup;
    }

    pub fn deinit(self: *IndexWriter) void {
        self.lockMappedResidencyMutex();
        if (self.resource_manager) |manager| {
            manager.observeUsage(
                .full_text_segment_residency,
                &self.mapped_residency_accounted_bytes,
                0,
            );
        }
        self.resource_manager = null;
        self.mapped_residency_mu.unlock();

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
        const snap = self.acquireSnapshotRaw();
        self.maybeMaintainMappedResidency(snap, platform_time.monotonicNs());
        return snap;
    }

    fn acquireSnapshotRaw(self: *IndexWriter) *IndexSnapshot {
        self.lockSnapshotMutex();
        defer self.snapshot_mu.unlock();
        return @atomicLoad(*IndexSnapshot, &self.current, .acquire).retain();
    }

    fn lockMappedResidencyMutex(self: *IndexWriter) void {
        while (!self.mapped_residency_mu.tryLock()) spinOrYield();
    }

    pub const MappedResidencyStats = struct {
        virtual_mapped_bytes: u64 = 0,
        estimated_resident_bytes: u64 = 0,
        recently_touched_bytes: u64 = 0,
        cold_mapped_bytes: u64 = 0,
        eviction_count: u64 = 0,
    };

    fn mappedResidencyStatsForSnapshot(self: *const IndexWriter, snap: *const IndexSnapshot, now_ns: u64) MappedResidencyStats {
        var stats = MappedResidencyStats{ .eviction_count = self.mapped_residency_evictions };
        for (snap.segments) |*seg| {
            if (!seg.data.isFileBacked()) continue;
            const bytes: u64 = @intCast(seg.data.bytes().len);
            stats.virtual_mapped_bytes +|= bytes;
            if (seg.shared.mapped_residency_state.load(.acquire) != mapped_residency_cold) {
                stats.estimated_resident_bytes +|= bytes;
            } else {
                stats.cold_mapped_bytes +|= bytes;
            }
            const last_access_ns = seg.shared.last_mapped_access_ns.load(.acquire);
            if (last_access_ns != 0 and now_ns -| last_access_ns <= mapped_residency_recent_ns) {
                stats.recently_touched_bytes +|= bytes;
            }
        }
        return stats;
    }

    pub fn mappedResidencyStats(self: *IndexWriter) MappedResidencyStats {
        const snap = self.acquireSnapshotRaw();
        defer snap.release();
        self.lockMappedResidencyMutex();
        defer self.mapped_residency_mu.unlock();
        return self.mappedResidencyStatsForSnapshot(snap, platform_time.monotonicNs());
    }

    fn maybeMaintainMappedResidency(self: *IndexWriter, snap: *IndexSnapshot, now_ns: u64) void {
        const scheduled_ns = self.mapped_residency_next_check_ns.load(.acquire);
        if (scheduled_ns != 0 and now_ns < scheduled_ns) return;
        if (self.mapped_residency_next_check_ns.cmpxchgStrong(
            scheduled_ns,
            now_ns +| mapped_residency_check_interval_ns,
            .acq_rel,
            .acquire,
        ) != null) return;
        self.maintainMappedResidencyAt(snap, now_ns);
    }

    fn maintainMappedResidencyAt(self: *IndexWriter, snap: *IndexSnapshot, now_ns: u64) void {
        self.lockMappedResidencyMutex();
        defer self.mapped_residency_mu.unlock();

        const manager = self.resource_manager orelse return;
        var stats = self.mappedResidencyStatsForSnapshot(snap, now_ns);
        manager.observeUsage(
            .full_text_segment_residency,
            &self.mapped_residency_accounted_bytes,
            stats.estimated_resident_bytes,
        );

        var decision = manager.pressureDecision(.full_text_segment_residency);
        if (decision.action != .shrink_cache or decision.pressure == .normal) return;
        const min_age_ns = if (decision.pressure == .hard)
            mapped_residency_hard_min_age_ns
        else
            mapped_residency_recent_ns;

        // The manager only returns a decision. The index owner selects and
        // advises its own clean mappings after the manager mutex is released.
        // Re-evaluate aggregate pressure after each segment so one index does
        // not evict more than is needed when several writers share a manager.
        while (decision.action == .shrink_cache and decision.pressure != .normal) {
            var candidate: ?*SegmentEntry = null;
            var candidate_access_ns: u64 = std.math.maxInt(u64);
            for (snap.segments) |*seg| {
                if (!seg.data.isFileBacked()) continue;
                if (seg.shared.mapped_residency_state.load(.acquire) != mapped_residency_resident) continue;
                if (seg.shared.active_mapped_readers.load(.acquire) != 0) continue;
                const last_access_ns = seg.shared.last_mapped_access_ns.load(.acquire);
                if (last_access_ns != 0 and now_ns -| last_access_ns < min_age_ns) continue;
                if (candidate == null or last_access_ns < candidate_access_ns) {
                    candidate = seg;
                    candidate_access_ns = last_access_ns;
                }
            }
            const coldest = candidate orelse break;
            if (coldest.shared.mapped_residency_state.cmpxchgStrong(
                mapped_residency_resident,
                mapped_residency_evicting,
                .acq_rel,
                .acquire,
            ) != null) continue;

            coldest.data.madviseDiscardCleanPages();
            if (coldest.shared.last_mapped_access_ns.load(.acquire) == candidate_access_ns) {
                _ = coldest.shared.mapped_residency_state.cmpxchgStrong(
                    mapped_residency_evicting,
                    mapped_residency_cold,
                    .acq_rel,
                    .acquire,
                );
            } else {
                _ = coldest.shared.mapped_residency_state.cmpxchgStrong(
                    mapped_residency_evicting,
                    mapped_residency_resident,
                    .acq_rel,
                    .acquire,
                );
            }
            self.mapped_residency_evictions +|= 1;
            stats = self.mappedResidencyStatsForSnapshot(snap, now_ns);
            manager.observeUsage(
                .full_text_segment_residency,
                &self.mapped_residency_accounted_bytes,
                stats.estimated_resident_bytes,
            );
            decision = manager.pressureDecision(.full_text_segment_residency);
        }
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
        const shared = try SegmentEntry.createShared(self.alloc, data, &reader);
        errdefer {
            SegmentEntry.deinitSharedMetadata(shared, self.alloc);
            self.alloc.destroy(shared);
        }
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
            .bm25_bound_table_cache_mu = .unlocked,
            .bm25_bound_table_cache = .empty,
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
            .stored_offset = data.len - 56,
            .index_offset = data.len - 56,
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
        const shared = try SegmentEntry.createShared(self.alloc, owned.?, &reader);
        errdefer {
            SegmentEntry.deinitSharedMetadata(shared, self.alloc);
            self.alloc.destroy(shared);
        }
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
            .bm25_bound_table_cache_mu = .unlocked,
            .bm25_bound_table_cache = .empty,
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
        errdefer for (new_segments[keep_count .. keep_count + cells_created]) |*seg| {
            SegmentEntry.deinitSharedMetadata(seg.shared, self.alloc);
            self.alloc.destroy(seg.shared);
        };
        for (replacements, 0..) |replacement, i| {
            const shared = try SegmentEntry.createShared(self.alloc, replacement.data, &replacement_readers[i]);
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

    fn computeDeletedFieldLensForBitmap(seg: *const SegmentEntry, deleted: *const roaring.RoaringBitmap, out: []std.atomic.Value(u64)) void {
        for (out) |*field_len| field_len.store(0, .release);
        for (seg.reader.fields, 0..) |*field_info, field_idx| {
            if (field_idx >= out.len) break;
            const inv_reader = (seg.reader.invertedIndex(field_info.name) catch continue) orelse continue;
            var total: u64 = 0;
            var deleted_it = deleted.iterator();
            while (deleted_it.next()) |local_id| {
                total +|= inv_reader.docLength(local_id);
            }
            out[field_idx].store(total, .release);
        }
    }

    fn adjustDeletedFieldLensForDocs(seg: *SegmentEntry, local_ids: []const u32, add: bool) void {
        for (seg.reader.fields, 0..) |*field_info, field_idx| {
            if (field_idx >= seg.shared.deleted_field_lens.len) break;
            const inv_reader = (seg.reader.invertedIndex(field_info.name) catch continue) orelse continue;
            var delta: u64 = 0;
            for (local_ids) |local_id| delta +|= inv_reader.docLength(local_id);
            if (add) {
                _ = seg.shared.deleted_field_lens[field_idx].fetchAdd(delta, .release);
            } else {
                _ = seg.shared.deleted_field_lens[field_idx].fetchSub(delta, .release);
            }
        }
    }

    fn markDeletionMutation(seg: *SegmentEntry) void {
        _ = seg.shared.deletion_generation.fetchAdd(1, .release);
    }

    /// Set deletion bitmap for a segment by ID (for recovery).
    pub fn setDeletionBitmap(self: *IndexWriter, seg_id: u64, bitmap: roaring.RoaringBitmap) void {
        self.lockMutex();
        defer self.mu.unlock();

        var owned_bitmap = bitmap;
        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        for (snap.segments) |*seg| {
            if (seg.id == seg_id) {
                computeDeletedFieldLensForBitmap(seg, &owned_bitmap, seg.shared.deleted_field_lens);
                if (seg.shared.deleted) |*d| {
                    var old_del = d.*;
                    old_del.deinit();
                }
                seg.shared.deleted = owned_bitmap;
                // Recompute global doc count
                var total: u32 = 0;
                for (snap.segments) |*s| {
                    total += s.liveDocCount();
                }
                snap.global_doc_count = total;
                markDeletionMutation(seg);
                return;
            }
        }
        // Ownership is transferred to this API even when recovery references
        // a segment that is no longer active.
        owned_bitmap.deinit();
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
        field_lengths_applied: bool,
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
                if (delete_info.field_lengths_applied) {
                    adjustDeletedFieldLensForDocs(seg, delete_info.local_ids[0..delete_info.applied_count], false);
                }
                snap.global_doc_count +|= @intCast(delete_info.applied_count);
                if (delete_info.applied_count > 0) markDeletionMutation(seg);
                break;
            }
        }
    }

    /// Delete every live copy of a document and return one updated deletion
    /// bitmap per affected segment for atomic persistence by the caller.
    pub fn deleteAllByIdTracked(self: *IndexWriter, alloc: Allocator, doc_id: []const u8) ![]DeleteInfo {
        return self.deleteAllByIdsTracked(alloc, &.{doc_id});
    }

    /// Delete every live copy of a set of external document IDs with one
    /// traversal of each segment. Returning one bitmap per affected segment
    /// lets persistent callers commit the complete batch atomically.
    pub fn deleteAllByIdsTracked(self: *IndexWriter, alloc: Allocator, doc_ids: []const []const u8) ![]DeleteInfo {
        self.lockMutex();
        defer self.mu.unlock();

        var wanted = std.StringHashMapUnmanaged(void).empty;
        defer wanted.deinit(alloc);
        for (doc_ids) |doc_id| {
            if (doc_id.len > 0) try wanted.put(alloc, doc_id, {});
        }

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
                const stored = (try seg.reader.storedDoc(@intCast(local_id))) orelse continue;
                if (wanted.contains(stored.id)) {
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
                .field_lengths_applied = false,
            });
            local_ids_transferred = true;
            if (created_bitmap) seg.shared.deleted = roaring.RoaringBitmap.init(self.alloc);
            for (owned_local_ids) |local_id| {
                try seg.shared.deleted.?.add(local_id);
                delete_infos.items[delete_infos.items.len - 1].applied_count += 1;
                snap.global_doc_count -|= 1;
            }
            delete_infos.items[delete_infos.items.len - 1].bitmap_bytes = try seg.shared.deleted.?.toBytes(alloc);
            adjustDeletedFieldLensForDocs(seg, owned_local_ids, true);
            delete_infos.items[delete_infos.items.len - 1].field_lengths_applied = true;
        }
        for (delete_infos.items) |delete_info| {
            if (delete_info.applied_count == 0) continue;
            for (snap.segments) |*seg| {
                if (seg.id != delete_info.seg_id) continue;
                markDeletionMutation(seg);
                break;
            }
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

fn mapTestSegment(segment_bytes: []const u8) !SegmentData {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) {
        return error.SkipZigTest;
    }
    const mapped = try std.heap.page_allocator.alignedAlloc(
        u8,
        std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
        segment_bytes.len,
    );
    @memcpy(mapped, segment_bytes);
    return SegmentData.fromMapped(mapped);
}

test "resource-managed mapped residency evicts cold segments and preserves hot mappings" {
    const alloc = std.testing.allocator;
    const seg_bytes = try buildTestSegment(alloc, &.{
        .{ .terms = &.{.{ .term = "resident", .freq = 1, .norm = 8 }} },
    });
    defer alloc.free(seg_bytes);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.full_text_segment_residency)] = .{
        .soft_limit_bytes = @intCast(seg_bytes.len),
        .hard_limit_bytes = @intCast(seg_bytes.len * 8),
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var writer = try IndexWriter.init(alloc);
    var writer_live = true;
    defer if (writer_live) writer.deinit();

    try writer.addSegmentWithIdData(1, try mapTestSegment(seg_bytes));
    try writer.addSegmentWithIdData(2, try mapTestSegment(seg_bytes));
    writer.snapshot().segments[1].noteAccess();
    writer.attachResourceManager(&manager);

    var stats = writer.mappedResidencyStats();
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len * 2)), stats.virtual_mapped_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len)), stats.estimated_resident_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len)), stats.recently_touched_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len)), stats.cold_mapped_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.eviction_count);
    try std.testing.expectEqual(
        @as(u64, @intCast(seg_bytes.len)),
        manager.sliceStats(.full_text_segment_residency).used_bytes,
    );

    // A query faults the cold mapping back into the conservative estimate.
    // Both mappings are then inside the soft-pressure recent-access window,
    // so the controller reports pressure but does not churn either mapping.
    writer.snapshot().segments[0].noteAccess();
    const now_ns = platform_time.monotonicNs();
    writer.maintainMappedResidencyAt(writer.snapshot(), now_ns);
    stats = writer.mappedResidencyStats();
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len * 2)), stats.estimated_resident_bytes);
    try std.testing.expectEqual(resource_manager_mod.Pressure.soft, manager.sliceStats(.full_text_segment_residency).pressure);

    // Once the older mapping ages beyond the hysteresis window, it is the
    // sole eviction candidate. An active reader still pins it until that
    // access completes, even under pressure.
    const old_segment = &writer.snapshot().segments[0];
    old_segment.beginAccess();
    old_segment.shared.last_mapped_access_ns.store(1, .release);
    const aged_now_ns = @max(now_ns, mapped_residency_recent_ns + std.time.ns_per_s);
    writer.maintainMappedResidencyAt(writer.snapshot(), aged_now_ns);
    stats = writer.mappedResidencyStats();
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len * 2)), stats.estimated_resident_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.eviction_count);

    old_segment.endAccess();
    writer.maintainMappedResidencyAt(writer.snapshot(), aged_now_ns);
    stats = writer.mappedResidencyStats();
    try std.testing.expectEqual(@as(u64, @intCast(seg_bytes.len)), stats.estimated_resident_bytes);
    try std.testing.expectEqual(@as(u64, 2), stats.eviction_count);

    writer.deinit();
    writer_live = false;
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.full_text_segment_residency).used_bytes,
    );
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

test "snapshot BM25 bound table cache is reused and bounded" {
    const alloc = std.testing.allocator;
    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    const snap = writer.snapshot();

    const first = (try snap.bm25BoundTable(100.0, .{})) orelse return error.TestExpectedEqual;
    const same = (try snap.bm25BoundTable(100.0, .{})) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(first, same);

    for (1..max_bm25_bound_tables_per_snapshot) |i| {
        const config = inverted.BM25Config{ .k1 = 1.2 + @as(f32, @floatFromInt(i)) * 0.1 };
        try std.testing.expect((try snap.bm25BoundTable(100.0, config)) != null);
    }
    try std.testing.expect((try snap.bm25BoundTable(100.0, .{ .k1 = 9.0 })) == null);
    try std.testing.expectEqual(max_bm25_bound_tables_per_snapshot, snap.bm25_bound_table_cache.count());
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

test "term doc frequency invalidates on real deletes and reconciles sparse tombstones" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc-a", .terms = &.{.{ .term = "other", .freq = 1, .norm = 10 }} },
        .{ .id = "doc-b", .terms = &.{.{ .term = "common", .freq = 1, .norm = 10 }} },
        .{ .id = "doc-c", .terms = &.{.{ .term = "common", .freq = 1, .norm = 10 }} },
        .{ .id = "doc-d", .terms = &.{.{ .term = "common", .freq = 1, .norm = 10 }} },
        .{ .id = "doc-e", .terms = &.{.{ .term = "common", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    try std.testing.expectEqual(@as(u32, 4), try snap.termDocFreq(alloc, "body", "common"));
    try std.testing.expectEqual(@as(u64, 1), snap.term_doc_freq_cache_misses);

    // A phantom delete neither changes live statistics nor evicts the cache.
    const generation_before_phantom = snap.deletionGeneration();
    try std.testing.expect(!try writer.deleteById("doc-missing"));
    try std.testing.expectEqual(generation_before_phantom, snap.deletionGeneration());
    try std.testing.expectEqual(@as(u32, 4), try snap.termDocFreq(alloc, "body", "common"));
    try std.testing.expectEqual(@as(u64, 1), snap.term_doc_freq_cache_hits);

    // The first tombstone does not contain the term and the later one does.
    // This exercises the sparse seek path's consumed-posting lookahead.
    const tracked = try writer.deleteAllByIdsTracked(alloc, &.{ "doc-a", "doc-d" });
    defer IndexWriter.freeDeleteInfos(alloc, tracked);
    try std.testing.expectEqual(@as(usize, 1), tracked.len);
    try std.testing.expectEqual(@as(u32, 3), try snap.termDocFreq(alloc, "body", "common"));
    try std.testing.expect(snap.deletionGeneration() > generation_before_phantom);

    writer.rollbackDeleteInfos(tracked);
    try std.testing.expectEqual(@as(u32, 4), try snap.termDocFreq(alloc, "body", "common"));

    // A committed real delete invalidates the primed value and remains exact.
    try std.testing.expect(try writer.deleteById("doc-d"));
    try std.testing.expectEqual(@as(u32, 3), try snap.termDocFreq(alloc, "body", "common"));
    const results = try snap.search(alloc, "body", &.{"common"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 3), results.hits.len);
}

test "retained snapshots observe shared deletion epochs and live BM25 aggregates" {
    const alloc = std.testing.allocator;

    const first_segment = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc-a", .terms = &.{.{ .term = "common", .freq = 1, .norm = 1 }} },
        .{ .id = "doc-b", .terms = &.{.{ .term = "common", .freq = 1, .norm = 1 }} },
    });
    defer alloc.free(first_segment);
    const second_segment = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc-c", .terms = &.{.{ .term = "other", .freq = 1, .norm = 1 }} },
    });
    defer alloc.free(second_segment);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(first_segment);

    const retained = writer.acquireSnapshot();
    defer retained.release();
    try std.testing.expectEqual(@as(u32, 2), try retained.termDocFreq(alloc, "body", "common"));
    try std.testing.expectEqual(@as(u32, 2), retained.liveDocCount());
    try std.testing.expectApproxEqAbs(@as(f32, 1), retained.textAvgDocLen("body"), 0.00001);

    // Publishing a newer snapshot leaves `retained` alive with its own term
    // cache, while the carried segment deletion state remains shared.
    try writer.addSegment(second_segment);
    try std.testing.expect(try writer.deleteById("doc-a"));

    try std.testing.expectEqual(@as(u32, 1), try retained.termDocFreq(alloc, "body", "common"));
    try std.testing.expectEqual(@as(u32, 1), retained.liveDocCount());
    try std.testing.expectApproxEqAbs(@as(f32, 1), retained.textAvgDocLen("body"), 0.00001);
    const results = try retained.search(alloc, "body", &.{"common"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    try std.testing.expectEqual(@as(u32, 1), results.hits[0].doc_id);
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

test "tracked batch deletion removes many IDs with one result per affected segment" {
    const alloc = std.testing.allocator;
    const first = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc:a", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
        .{ .id = "doc:b", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(first);
    const second = try buildTestSegmentWithIds(alloc, &.{
        .{ .id = "doc:b", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
        .{ .id = "doc:c", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(second);

    var writer = try IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(first);
    try writer.addSegment(second);

    const delete_infos = try writer.deleteAllByIdsTracked(alloc, &.{ "doc:b", "missing", "doc:a", "doc:b" });
    defer IndexWriter.freeDeleteInfos(alloc, delete_infos);
    try std.testing.expectEqual(@as(usize, 2), delete_infos.len);
    try std.testing.expectEqual(@as(u32, 1), writer.snapshot().global_doc_count);

    const results = try writer.snapshot().search(alloc, "body", &.{"x"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 1), results.hits.len);
    const remaining = (try writer.snapshot().storedDoc(results.hits[0].doc_id)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:c", remaining.id);
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
