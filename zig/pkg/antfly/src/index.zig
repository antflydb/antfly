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

    pub fn deinit(self: *SegmentData, alloc: Allocator) void {
        switch (self.*) {
            .heap => |data| alloc.free(data),
            .mmap => |data| {
                if (builtin.os.tag != .freestanding) std.posix.munmap(data);
            },
        }
        self.* = undefined;
    }
};

pub const SegmentEntry = struct {
    id: u64,
    data: SegmentData,
    reader: segment_mod.SegmentReader,
    deleted: ?roaring.RoaringBitmap,

    pub fn deinit(self: *SegmentEntry) void {
        self.reader.deinit();
        if (self.deleted) |*d| {
            var del = d.*;
            del.deinit();
        }
        self.data.deinit(self.reader.alloc);
    }

    /// Number of live (non-deleted) documents.
    pub fn liveDocCount(self: *const SegmentEntry) u32 {
        if (self.deleted) |d| {
            const del_count: u32 = @intCast(d.cardinality());
            return self.reader.doc_count -| del_count;
        }
        return self.reader.doc_count;
    }
};

pub const ReplacementSegmentData = struct {
    id: u64,
    data: SegmentData,
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
    /// Segments whose readers should be deinit'd when this snapshot is released.
    /// Set by replaceSegments for the segments being merged away.
    retired_segments: []SegmentEntry,

    /// Increment reference count. Returns self for chaining.
    pub fn retain(self: *IndexSnapshot) *IndexSnapshot {
        _ = @atomicRmw(u32, &self.ref_count, .Add, 1, .monotonic);
        return self;
    }

    /// Decrement reference count. Frees snapshot when count reaches 0.
    pub fn release(self: *IndexSnapshot) void {
        if (@atomicRmw(u32, &self.ref_count, .Sub, 1, .acq_rel) == 1) {
            const alloc = self.alloc;
            // Deinit retired segments (replaced during merge)
            for (self.retired_segments) |*seg| {
                var s = seg.*;
                s.deinit();
            }
            if (self.retired_segments.len > 0) alloc.free(self.retired_segments);
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
    }

    /// Full cleanup including ALL segment entries. Only for IndexWriter.deinit().
    fn deinitAll(self: *IndexSnapshot) void {
        for (self.segments) |*seg| seg.deinit();
        for (self.retired_segments) |*seg| {
            var s = seg.*;
            s.deinit();
        }
        if (self.retired_segments.len > 0) self.alloc.free(self.retired_segments);
        self.alloc.free(self.segments);
        const cache_mu = &self.term_doc_freq_cache_mu;
        while (!cache_mu.tryLock()) {
            spinOrYield();
        }
        defer cache_mu.unlock();
        var cache_it = self.term_doc_freq_cache.keyIterator();
        while (cache_it.next()) |key| self.alloc.free(key.storage);
        self.term_doc_freq_cache.deinit(self.alloc);
        self.global_total_field_len.deinit(self.alloc);
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

    pub fn searchWithOverride(
        self: *const IndexSnapshot,
        alloc: Allocator,
        field: []const u8,
        terms: []const []const u8,
        k: u32,
        override: ?distributed_stats_mod.TextFieldStats,
    ) !scorer_mod.SearchResults {
        if (self.global_doc_count == 0 or terms.len == 0) return .{ .hits = try alloc.alloc(scorer_mod.ScoredHit, 0), .total_count = 0 };

        const global_doc_count = if (override) |stats| stats.global_doc_count else self.global_doc_count;
        if (global_doc_count == 0) return .{ .hits = try alloc.alloc(scorer_mod.ScoredHit, 0), .total_count = 0 };
        const avg_dl = if (override) |stats| stats.avgDocLen() else self.avgDocLen(field);
        const term_doc_freqs = try alloc.alloc(u32, terms.len);
        defer alloc.free(term_doc_freqs);
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

        var doc_offset: u32 = 0;
        for (self.segments) |*seg| {
            const inv_reader = (try seg.reader.invertedIndex(field)) orelse {
                doc_offset += seg.reader.doc_count;
                continue;
            };

            {
                var wand = scorer_mod.WANDScorer.init(alloc, k, global_doc_count, avg_dl, .{});
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
                        .postings => |p| p.chunk_size,
                        .one_hit => 1024,
                    };

                    try wand.addTerm(
                        iter,
                        if (term_doc_freqs[term_idx] != 0) term_doc_freqs[term_idx] else lookup_result.docFreq(),
                        block_max,
                        chunk_size,
                        doc_offset,
                    );
                    added_terms += 1;
                }

                if (added_terms == 0) {
                    doc_offset += seg.reader.doc_count;
                    continue;
                }

                var live_collector = LiveDocCollector{
                    .base = &collector,
                    .doc_offset = doc_offset,
                };
                if (seg.deleted) |*deleted| {
                    live_collector.deleted = deleted;
                }
                try wand.executeInto(&live_collector);
            }
            doc_offset += seg.reader.doc_count;
        }

        return collector.finishOwned();
    }

    /// Execute a filter across all segments, returning matching global doc IDs.
    pub fn executeFilter(self: *const IndexSnapshot, alloc: Allocator, filter: query_mod.Filter) ![]u32 {
        return query_mod.executeFilter(alloc, self, filter);
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

    /// Get and decompress a stored document by global doc ID. Caller owns returned data.
    pub const DecompressedDoc = struct { id: []const u8, data: []u8 };

    pub fn storedDocDecompressed(self: *const IndexSnapshot, global_id: u32) !?DecompressedDoc {
        const resolved = self.resolveDocId(global_id) orelse return null;
        const result = (try self.segments[resolved.seg_idx].reader.storedDocDecompressed(resolved.local_id)) orelse return null;
        return DecompressedDoc{ .id = result.id, .data = result.data };
    }

    pub fn docNumsForOrdinalsAlloc(self: *const IndexSnapshot, alloc: Allocator, ordinals: []const u32) ![]u32 {
        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);

        var doc_offset: u32 = 0;
        for (self.segments) |*seg| {
            for (0..seg.reader.doc_count) |local_usize| {
                const local_doc: u32 = @intCast(local_usize);
                if (seg.deleted) |deleted| {
                    if (deleted.contains(local_doc)) continue;
                }
                const ordinal = (try seg.reader.docOrdinal(local_doc)) orelse continue;
                if (!containsOrdinal(ordinals, ordinal)) continue;
                const global_doc = doc_offset + local_doc;
                if (!containsDocNum(out.items, global_doc)) try out.append(alloc, global_doc);
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
            if (!containsOrdinal(out.items, ordinal)) try out.append(alloc, ordinal);
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

            if (seg.deleted == null) {
                total += lookup_result.docFreq();
                continue;
            }

            var iter = try lookup_result.iterator(alloc);
            defer iter.deinit();
            while (try iter.next()) |hit| {
                if (!seg.deleted.?.contains(hit.doc_id)) total += 1;
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

    fn avgDocLen(self: *const IndexSnapshot, field: []const u8) f32 {
        if (self.global_doc_count == 0) return 0;
        const total = self.global_total_field_len.get(field) orelse 0;
        return @as(f32, @floatFromInt(total)) / @as(f32, @floatFromInt(self.global_doc_count));
    }
};

fn containsOrdinal(ordinals: []const u32, expected: u32) bool {
    for (ordinals) |ordinal| {
        if (ordinal == expected) return true;
    }
    return false;
}

fn containsDocNum(doc_nums: []const u32, expected: u32) bool {
    for (doc_nums) |doc_num| {
        if (doc_num == expected) return true;
    }
    return false;
}

/// Coordinates writes and maintains the current snapshot.
/// Reads are lock-free (atomic snapshot pointer).
/// Writes are serialized (mutex).
pub const IndexWriter = struct {
    alloc: Allocator,
    current: *IndexSnapshot,
    mu: std.atomic.Mutex,
    next_segment_id: u64,
    next_epoch: u64,

    pub fn lockMutex(self: *IndexWriter) void {
        while (!self.mu.tryLock()) {
            spinOrYield();
        }
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
            .retired_segments = &.{},
        };
        return .{
            .alloc = alloc,
            .current = snap,
            .mu = .unlocked,
            .next_segment_id = 1,
            .next_epoch = 1,
        };
    }

    pub fn deinit(self: *IndexWriter) void {
        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        snap.deinitAll();
        self.alloc.destroy(snap);
    }

    /// Get the current snapshot (lock-free read, no ref change).
    /// The returned pointer is valid as long as the writer is alive
    /// and no concurrent writes occur. For concurrent safety, use acquireSnapshot().
    pub fn snapshot(self: *IndexWriter) *IndexSnapshot {
        return @atomicLoad(*IndexSnapshot, &self.current, .acquire);
    }

    /// Get the current snapshot with an incremented ref count.
    /// Caller MUST call release() when done. Safe for concurrent use.
    pub fn acquireSnapshot(self: *IndexWriter) *IndexSnapshot {
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
        new_segments[old.segments.len] = .{
            .id = seg_id,
            .data = data,
            .reader = reader,
            .deleted = null,
        };

        try self.rebuildSnapshot(new_segments, &.{});
    }

    /// Rebuild and swap the index snapshot from a new segment list.
    /// `retired` contains segments whose readers should be cleaned up when
    /// the old snapshot is fully released.
    fn rebuildSnapshot(self: *IndexWriter, new_segments: []SegmentEntry, retired: []SegmentEntry) !void {
        var global_doc_count: u32 = 0;
        var global_field_lens = std.StringHashMapUnmanaged(u64).empty;
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
            .retired_segments = &.{},
        };
        self.next_epoch += 1;

        const old = @atomicLoad(*IndexSnapshot, &self.current, .acquire);

        // Attach retired segments to the old snapshot so they get cleaned up
        // when all readers release it.
        old.retired_segments = retired;

        // Atomic swap so concurrent readers see a consistent pointer.
        @atomicStore(*IndexSnapshot, &self.current, new_snap, .release);

        // Release writer's reference to old snapshot.
        old.release();
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
        new_segments[old.segments.len] = .{
            .id = seg_id,
            .data = owned.?,
            .reader = reader,
            .deleted = null,
        };

        if (seg_id >= self.next_segment_id) self.next_segment_id = seg_id + 1;

        try self.rebuildSnapshot(new_segments, &.{});
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

        for (replacements, 0..) |replacement, i| {
            new_segments[idx] = .{
                .id = replacement.id,
                .data = replacement.data,
                .reader = replacement_readers[i],
                .deleted = null,
            };
            idx += 1;
            if (replacement.id >= self.next_segment_id) self.next_segment_id = replacement.id + 1;
        }

        try self.rebuildSnapshot(new_segments, retired);
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

        try self.rebuildSnapshot(new_segments, retired);
    }

    /// Set deletion bitmap for a segment by ID (for recovery).
    pub fn setDeletionBitmap(self: *IndexWriter, seg_id: u64, bitmap: roaring.RoaringBitmap) void {
        self.lockMutex();
        defer self.mu.unlock();

        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);
        for (snap.segments) |*seg| {
            if (seg.id == seg_id) {
                if (seg.deleted) |*d| {
                    var old_del = d.*;
                    old_del.deinit();
                }
                seg.deleted = bitmap;
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
        const delete_info = (try self.deleteByIdTracked(self.alloc, doc_id)) orelse return false;
        self.alloc.free(delete_info.bitmap_bytes);
        return true;
    }

    pub const DeleteInfo = struct {
        seg_id: u64,
        bitmap_bytes: []u8,
    };

    /// Delete a document and return the updated deletion bitmap for persistence.
    /// Caller owns `bitmap_bytes`.
    pub fn deleteByIdTracked(self: *IndexWriter, alloc: Allocator, doc_id: []const u8) !?DeleteInfo {
        self.lockMutex();
        defer self.mu.unlock();

        const snap = @atomicLoad(*IndexSnapshot, &self.current, .acquire);

        // Find which segment contains this doc ID
        for (snap.segments) |*seg| {
            for (0..seg.reader.doc_count) |local_id| {
                const stored = seg.reader.storedDoc(@intCast(local_id)) orelse continue;
                if (std.mem.eql(u8, stored.id, doc_id)) {
                    // Already deleted?
                    if (seg.deleted) |d| {
                        if (d.contains(@intCast(local_id))) return null;
                    }

                    // Set deletion bit
                    if (seg.deleted == null) {
                        seg.deleted = roaring.RoaringBitmap.init(self.alloc);
                    }
                    try seg.deleted.?.add(@intCast(local_id));

                    // Update global doc count in snapshot
                    snap.global_doc_count -|= 1;

                    return .{
                        .seg_id = seg.id,
                        .bitmap_bytes = try seg.deleted.?.toBytes(alloc),
                    };
                }
            }
        }
        return null;
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
