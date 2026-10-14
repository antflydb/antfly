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

//! Backend-backed chunked inverted index for sparse vectors.
//!
//! Matches Go antfly's lib/sparseindex/ design:
//!   - Sparse vectors: sorted (indices: []u32, values: []f32)
//!   - Posting list chunks: delta-encoded doc nums + quantized weights
//!   - DAAT (Document-At-A-Time) scoring via dot product accumulation
//!
//! Sparse layout v2 uses binary typed keys. Bulk-built inverted postings are
//! stored as sparse segment blobs; legacy-shaped chunk rows remain as the small
//! delta path for incremental writes.

const std = @import("std");

/// DAAT reuses its current document's decision. Unordered spill passes may
/// evict and re-probe, without retaining archive-sized decision bitmaps.
const PredicateDecisionCache = struct {
    const Value = struct { doc: u32, allowed: bool };
    slots: [256]?Value = @splat(null),
    fn get(self: *const @This(), doc: u32) ?bool {
        const value = self.slots[doc % self.slots.len] orelse return null;
        return if (value.doc == doc) value.allowed else null;
    }
    fn put(self: *@This(), doc: u32, allowed: bool) void {
        self.slots[doc % self.slots.len] = .{ .doc = doc, .allowed = allowed };
    }
};
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const builtin = @import("builtin");
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../storage/backend_erased.zig");
const backend_types = @import("../storage/backend_types.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const platform_time = @import("antfly_platform").time;
const mem_backend = @import("../storage/mem_backend.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");

// ============================================================================
// Types
// ============================================================================

pub const SparseVector = struct {
    indices: []const u32, // sorted dimension indices
    values: []const f32, // weights per dimension
};

pub const SparseWrite = struct {
    doc_id: []const u8,
    vec: SparseVector,
    doc_num: ?u32 = null,
};

// Keep already canonical batches allocation-free. Coalesce repeated dimensions
// in input order before either the delta or immutable posting producer sees them.
fn canonicalWorkingBytes(input: []const SparseWrite) !u64 {
    var values: u64 = 0;
    var scratch: u64 = 0;
    for (input) |write| {
        if (write.vec.indices.len != write.vec.values.len) return error.InvalidSparseVector;
        var canonical = true;
        for (write.vec.indices, 0..) |dimension, i| if (i != 0 and dimension <= write.vec.indices[i - 1]) {
            canonical = false;
        };
        if (canonical) continue;
        const length: u64 = @intCast(write.vec.indices.len);
        values = try std.math.add(u64, values, try std.math.mul(u64, length, @sizeOf(u32) + @sizeOf(f32)));
        scratch = @max(scratch, try std.math.mul(u64, length, @sizeOf(u32) + @sizeOf(f32) + @sizeOf(usize)));
    }
    if (values == 0) return 0;
    return std.math.add(u64, try std.math.add(u64, values, scratch), try std.math.mul(u64, input.len, @sizeOf(SparseWrite)));
}
fn canonicalWrites(a: Allocator, backing: Allocator, input: []const SparseWrite) ![]const SparseWrite {
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    var output: ?[]SparseWrite = null;
    for (input, 0..) |write, wi| {
        if (write.vec.indices.len != write.vec.values.len) return error.InvalidSparseVector;
        var canonical = true;
        for (write.vec.indices, 0..) |dimension, i| {
            if (i != 0 and dimension <= write.vec.indices[i - 1]) canonical = false;
        }
        if (canonical) continue;
        if (output == null) output = try a.dupe(SparseWrite, input);
        const Coordinate = struct { dimension: u32, value: f32, order: usize };
        _ = scratch.reset(.retain_capacity);
        const coordinates = try scratch.allocator().alloc(Coordinate, write.vec.indices.len);
        for (coordinates, write.vec.indices, write.vec.values, 0..) |*coordinate, dimension, value, order| coordinate.* = .{ .dimension = dimension, .value = value, .order = order };
        std.mem.sort(Coordinate, coordinates, {}, struct {
            fn less(_: void, x: Coordinate, y: Coordinate) bool {
                return x.dimension < y.dimension or (x.dimension == y.dimension and x.order < y.order);
            }
        }.less);
        const indices = try a.alloc(u32, coordinates.len);
        const values = try a.alloc(f32, coordinates.len);
        var count: usize = 0;
        for (coordinates) |coordinate| {
            if (count != 0 and indices[count - 1] == coordinate.dimension) {
                values[count - 1] = daat.addScore(values[count - 1], coordinate.value) catch return error.InvalidSparseVector;
            } else {
                indices[count] = coordinate.dimension;
                values[count] = coordinate.value;
                count += 1;
            }
        }
        output.?[wi].vec = .{ .indices = indices[0..count], .values = values[0..count] };
    }
    return if (output) |writes| writes else input;
}

pub const OrdinalDocNumLookup = struct {
    doc_nums: []const u32,
    missing_ordinals: []const u32,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_nums);
        alloc.free(self.missing_ordinals);
    }
};

pub const BatchOptions = struct {
    defer_term_range_updates: bool = false,
    backend_batch_options: backend_types.BatchOptions = .{},
    prefer_bulk_build: bool = false,
    assume_new_doc_ids: bool = false,
};

pub const WriteProfile = struct {
    batch_calls: u64 = 0,
    incremental_calls: u64 = 0,
    bulk_append_calls: u64 = 0,
    bulk_append_fallbacks: u64 = 0,
    writes: u64 = 0,
    deletes: u64 = 0,
    postings: u64 = 0,
    terms: u64 = 0,
    reserve_ns: u64 = 0,
    dedupe_ns: u64 = 0,
    existence_check_ns: u64 = 0,
    doc_num_ns: u64 = 0,
    fwd_rev_put_ns: u64 = 0,
    posting_collect_ns: u64 = 0,
    posting_sort_ns: u64 = 0,
    posting_write_ns: u64 = 0,
    chunk_read_ns: u64 = 0,
    chunk_encode_ns: u64 = 0,
    chunk_put_ns: u64 = 0,
    range_meta_encode_ns: u64 = 0,
    range_meta_put_ns: u64 = 0,
    term_meta_ns: u64 = 0,
    commit_ns: u64 = 0,
    incremental_delete_ns: u64 = 0,
    incremental_insert_ns: u64 = 0,
    incremental_refresh_ns: u64 = 0,
    incremental_commit_ns: u64 = 0,

    pub fn delta(after: WriteProfile, before: WriteProfile) WriteProfile {
        var out: WriteProfile = .{};
        inline for (comptime std.meta.fieldNames(WriteProfile)) |reflected_name| {
            @field(out, reflected_name) = @field(after, reflected_name) -| @field(before, reflected_name);
        }
        return out;
    }

    pub fn add(self: *WriteProfile, other: WriteProfile) void {
        inline for (comptime std.meta.fieldNames(WriteProfile)) |reflected_name| {
            @field(self.*, reflected_name) += @field(other, reflected_name);
        }
    }
};

pub const SearchResult = struct {
    doc_id: []u8,
    doc_num: ?u32 = null,
    score: f32,
};

const SearchCandidate = struct {
    doc_num: u32,
    score: f32,
    doc_id: ?[]u8 = null,
};

const SearchProfile = struct {
    filter_resolve_ns: u64 = 0,
    filter_forward_ns: u64 = 0,
    segment_seek_ns: u64 = 0,
    segment_decode_ns: u64 = 0,
    delta_chunk_ns: u64 = 0,
    score_collect_ns: u64 = 0,
    sort_ns: u64 = 0,
    hydrate_ns: u64 = 0,
    terms: usize = 0,
    segment_entries: usize = 0,
    segment_chunks: usize = 0,
    delta_chunks: usize = 0,
    scored_docs: usize = 0,
    filter_forward_docs: usize = 0,
    filter_forward_path: bool = false,
    results: usize = 0,
};

pub const OrdinalLookup = @import("ordinal_lookup.zig").Lookup;
pub const KeyPredicate = struct {
    ptr: *anyopaque,
    allows: *const fn (*anyopaque, []const u8) anyerror!bool,
    /// Changes only when exact query-owned constraints become materialized.
    constraint_revision: ?*const fn (*anyopaque) u64 = null,
    /// Resolve exact include/exclude masks without expanding a complement.
    select_constraints: ?*const fn (*anyopaque, Allocator, OrdinalLookup) anyerror!?@import("ordinal_lookup.zig").Selection = null,
    select_range: ?*const fn (*anyopaque, Allocator, OrdinalLookup, u32, u32) anyerror!?@import("ordinal_lookup.zig").Selection = null,
    /// Resolve an exact positive physical selection against this read transaction.
    /// Null retains the key predicate path (for example exclusion-only queries).
    select_ordinals: ?*const fn (*anyopaque, Allocator, OrdinalLookup) anyerror!?@import("../encoding/roaring.zig").RoaringBitmap = null,
};

/// Query-owned masks are always resolved against the same pinned read txn.
/// A revision changes only on an adaptive provider's one-way completion; this
/// avoids rebuilding masks per candidate while making the current DAAT seekable.
const AdaptiveOrdinalConstraints = struct {
    a: Allocator,
    predicate: ?KeyPredicate,
    lookup: OrdinalLookup,
    revision: u64 = 0,
    include: ?@import("../encoding/roaring.zig").RoaringBitmap = null,
    exclude: ?@import("../encoding/roaring.zig").RoaringBitmap = null,
    resolved: bool = false,
    borrowed: bool = false,
    budget: ?*@import("ordinal_lookup.zig").MaskBudget = null,
    deferred: bool = false,
    window: ?std.heap.ArenaAllocator = null,
    range: ?@import("ordinal_lookup.zig").Selection = null,
    range_first: u32 = 0,
    range_last: u32 = 0,
    probe_window: ?u32 = null,
    last_probe: ?u32 = null,
    probes: usize = 0,
    /// A point seek costs roughly a directory path; amortize a 1024-row
    /// sequential translation only after enough competitive candidates arrive.
    fn refineRange(self: *@This(), doc: u32) !void {
        try self.refresh();
        if (!self.deferred or (self.range != null and doc >= self.range_first and doc <= self.range_last)) return;
        const base = doc & ~@as(u32, 1023);
        if (self.probe_window == null or self.probe_window.? != base) {
            self.probe_window = base;
            self.last_probe = null;
            self.probes = 0;
        }
        if (self.last_probe == null or self.last_probe.? != doc) {
            self.probes += 1;
            self.last_probe = doc;
        }
        if (self.probes > 16) try self.ensureRange(doc);
    }
    fn clear(self: *@This()) void {
        if (!self.borrowed) {
            if (self.include) |*bitmap| bitmap.deinit();
            if (self.exclude) |*bitmap| bitmap.deinit();
            if (self.budget) |budget| budget.destroy();
        }
        self.borrowed = false;
        if (self.range) |*selection| selection.deinit();
        self.range = null;
    }
    fn deinit(self: *@This()) void {
        self.clear();
        if (self.window) |*arena| arena.deinit();
    }
    fn ensureRange(self: *@This(), doc: u32) !void {
        if (!self.deferred) return;
        if (self.range != null and doc >= self.range_first and doc <= self.range_last) return;
        if (self.range) |*selection| selection.deinit();
        self.range = null;
        if (self.window == null) self.window = .init(self.a);
        _ = self.window.?.reset(.retain_capacity);
        self.range_first = doc & ~@as(u32, 1023);
        self.range_last = self.range_first | 1023;
        const select = self.predicate.?.select_range orelse {
            self.deferred = false;
            return;
        };
        var selected = (try select(self.predicate.?.ptr, self.window.?.allocator(), self.lookup, self.range_first, self.range_last)) orelse {
            self.deferred = false;
            return;
        };
        errdefer selected.deinit();
        if (selected.residual or selected.deferred) {
            selected.deinit();
            self.deferred = false;
            return;
        }
        try selected.prepareRead();
        self.range = selected;
    }
    fn allowsOrdinal(self: *@This(), doc: u32) !bool {
        try self.refresh();
        if (self.include) |*bitmap| if (!bitmap.contains(doc)) return false;
        if (self.exclude) |*bitmap| if (bitmap.contains(doc)) return false;
        if (self.range != null and doc >= self.range_first and doc <= self.range_last) {
            const selection = &self.range.?;
            if (selection.include) |*bitmap| if (!bitmap.contains(doc)) return false;
            if (selection.exclude) |*bitmap| if (bitmap.contains(doc)) return false;
        }
        return true;
    }
    fn resolvedAt(self: *const @This(), doc: u32) bool {
        return self.resolved or (self.range != null and doc >= self.range_first and doc <= self.range_last);
    }
    fn load(self: *@This()) !void {
        const predicate = self.predicate orelse return;
        const masks = if (predicate.select_constraints) |select| try select(predicate.ptr, self.a, self.lookup) else null;
        var selected = masks orelse blk: {
            const include = if (predicate.select_ordinals) |select| try select(predicate.ptr, self.a, self.lookup) else null;
            break :blk @import("ordinal_lookup.zig").Selection{ .include = include, .residual = include == null };
        };
        errdefer selected.deinit();
        if (selected.include) |*bitmap| try bitmap.prepareRead();
        if (selected.exclude) |*bitmap| try bitmap.prepareRead();
        self.clear();
        self.include = selected.include;
        self.exclude = selected.exclude;
        self.budget = selected.budget;
        self.resolved = !selected.residual and !selected.deferred;
        self.deferred = selected.deferred;
        self.probe_window = null;
        self.last_probe = null;
        self.probes = 0;
        self.revision = if (predicate.constraint_revision) |get| get(predicate.ptr) else 0;
    }
    fn refresh(self: *@This()) !void {
        const predicate = self.predicate orelse return;
        const get = predicate.constraint_revision orelse return;
        if (get(predicate.ptr) != self.revision) try self.load();
    }
};

const SparseCompetitiveCutoff = struct {
    value: std.atomic.Value(u64) = .init((@as(u64, @as(u32, @bitCast(-std.math.inf(f32)))) << 32) | std.math.maxInt(u32)),
    fn get(self: *@This()) ?daat.Entry {
        const bits = self.value.load(.acquire);
        const entry: daat.Entry = .{ .doc_num = @truncate(bits), .score = @bitCast(@as(u32, @truncate(bits >> 32))) };
        return if (std.math.isFinite(entry.score)) entry else null;
    }
    fn publish(self: *@This(), entry: daat.Entry) void {
        var prior = self.value.load(.monotonic);
        while (true) {
            const old: daat.Entry = .{ .doc_num = @truncate(prior), .score = @bitCast(@as(u32, @truncate(prior >> 32))) };
            if (!daat.Entry.better({}, entry, old)) return;
            const next = (@as(u64, @as(u32, @bitCast(entry.score))) << 32) | entry.doc_num;
            prior = self.value.cmpxchgWeak(prior, next, .release, .monotonic) orelse return;
        }
    }
};

pub const SearchConstraints = struct {
    score_spill: ?@import("../spill_sort.zig").Options = null,
    max_score_docs: usize = 65536,
    parallel_workspace_bytes: usize = 16 * 1024 * 1024,
    parallel_tasks: ?*u64 = null,
    key_predicate: ?KeyPredicate = null,
    filter_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},
    filter_doc_nums: []const u32 = &.{},
    exclude_doc_nums: []const u32 = &.{},
    cancellation: ?CancellationToken = null,
};

fn checkSearchCancellation(cancellation: ?CancellationToken) !void {
    if (cancellation) |value| {
        if (value.isCancelled()) return error.Cancelled;
    }
}

const daat = @import("daat.zig");
const posting_pages = @import("posting_pages.zig");
const PageReader = posting_pages.Reader(backend_erased.ReadTxn);
const ScoreEntry = daat.Entry;

/// Preserve native f32 addition order while bounding accumulation memory.
/// Each document's partial sum precedes subsequent contributions in the spill.
const ScoreAccumulator = struct {
    const spill = @import("../spill_sort.zig");
    alloc: Allocator,
    options: ?spill.Options,
    limit: usize,
    cancellation: ?CancellationToken = null,
    values: std.AutoHashMapUnmanaged(u32, f32) = .empty,
    sort: ?spill.Sorter = null,
    sequence: u32 = 0,
    cursor: ?spill.Cursor = null,
    head: ?spill.Record = null,
    iterator: ?std.AutoHashMapUnmanaged(u32, f32).Iterator = null,
    fn deinit(self: *@This()) void {
        if (self.cursor) |*cursor| cursor.deinit();
        if (self.sort) |*sort| sort.deinit();
        self.values.deinit(self.alloc);
    }
    fn write(self: *@This(), doc: u32, value: f32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @bitCast(value), .little);
        const key = (@as(u64, doc) << 32) | self.sequence;
        self.sequence = std.math.add(u32, self.sequence, 1) catch return error.ResourceBudgetExceeded;
        try self.sort.?.add(key, &bytes);
    }
    fn add(self: *@This(), doc: u32, value: f32) !void {
        if (!std.math.isFinite(value)) return error.SparseScoreOverflow;
        if (self.sort == null and !self.values.contains(doc) and self.values.count() >= self.limit) {
            var options = self.options orelse return error.ResourceBudgetExceeded;
            options.cancellation = self.cancellation;
            options.max_input_bytes = @min(options.max_input_bytes, 1024 * 1024 * 1024);
            self.sort = try spill.Sorter.init(self.alloc, options);
            var it = self.values.iterator();
            while (it.next()) |entry| try self.write(entry.key_ptr.*, entry.value_ptr.*);
            self.values.deinit(self.alloc);
            self.values = .empty;
        }
        if (self.sort != null) return self.write(doc, value);
        const entry = try self.values.getOrPut(self.alloc, doc);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* = try daat.addScore(entry.value_ptr.*, value);
    }
    fn finish(self: *@This()) !void {
        if (self.sort) |*sort| {
            if (try sort.finish()) |range| self.cursor = spill.Cursor.init(self.alloc, sort.run, range);
        } else self.iterator = self.values.iterator();
    }
    fn next(self: *@This(), cancellation: ?CancellationToken) !?ScoreEntry {
        if (self.iterator) |*it| {
            const entry = it.next() orelse return null;
            return .{ .doc_num = entry.key_ptr.*, .score = entry.value_ptr.* };
        }
        const cursor = if (self.cursor) |*c| c else return null;
        var record = self.head orelse (try cursor.next() orelse return null);
        self.head = null;
        const doc: u32 = @intCast(record.key >> 32);
        var score: f32 = 0;
        var count: usize = 0;
        while (true) {
            if (count % 256 == 0) try checkSearchCancellation(cancellation);
            if (record.payload.len != 4) return error.InvalidSparseSegment;
            score = try daat.addScore(score, @bitCast(std.mem.readInt(u32, record.payload[0..4], .little)));
            record = try cursor.next() orelse break;
            if (record.key >> 32 != doc) {
                self.head = record;
                break;
            }
            count += 1;
        }
        return .{ .doc_num = doc, .score = score };
    }
};

const BulkPosting = struct {
    term_id: u32,
    doc_num: u32,
    weight: f32,
    doc_id: []const u8,
};

const BulkDoc = struct {
    write_idx: usize,
    doc_num: u64,
    incarnation: u64 = 0,
};

pub const SplitRebuildResult = struct {
    doc_ids: [][]u8,
    select_docs_ns: u64 = 0,
    terms_ns: u64 = 0,
    commit_ns: u64 = 0,

    pub fn deinit(self: *SplitRebuildResult, alloc: Allocator) void {
        for (self.doc_ids) |doc_id| alloc.free(doc_id);
        alloc.free(self.doc_ids);
        self.* = undefined;
    }
};

pub const SplitPlanningStats = struct {
    selected_docs: usize = 0,
    touched_terms: usize = 0,
    right_only_chunks: usize = 0,
    mixed_chunks: usize = 0,
    right_only_postings: usize = 0,
    mixed_right_postings: usize = 0,
};

const RetainedChunk = struct {
    chunk_bytes: []u8,
    meta_bytes: []u8,
    max_weight: f32,

    pub fn deinit(self: *RetainedChunk, alloc: Allocator) void {
        alloc.free(self.chunk_bytes);
        alloc.free(self.meta_bytes);
        self.* = undefined;
    }
};

// ============================================================================
// Chunk encoding (compatible Go v1 payload; optional ordinal range metadata)
// ============================================================================

const CHUNK_FORMAT_VERSION: u8 = 1;

fn encodeChunk(alloc: Allocator, doc_nums: []const u32, weights: []const f32) ![]u8 {
    const n: u32 = @intCast(doc_nums.len);
    if (n == 0) return try alloc.alloc(u8, 0);

    // Compute min/max weights
    var min_w: f32 = weights[0];
    var max_w: f32 = weights[0];
    for (weights[1..]) |w| {
        if (w < min_w) min_w = w;
        if (w > max_w) max_w = w;
    }

    // Delta-encode doc nums
    var deltas = try alloc.alloc(u32, n);
    defer alloc.free(deltas);
    deltas[0] = doc_nums[0];
    for (1..n) |i| {
        deltas[i] = doc_nums[i] - doc_nums[i - 1];
    }

    // Quantize weights to u8
    var quant = try alloc.alloc(u8, n);
    defer alloc.free(quant);
    const scale = if (max_w > min_w) max_w - min_w else 1.0;
    for (weights, 0..) |w, i| {
        quant[i] = @intFromFloat(@min(255.0, (w - min_w) / scale * 255.0));
    }

    // Encode: [version:u8][n:u32 LE][max_w:f32 LE][min_w:f32 LE][deltas:n*u32 LE][quant:n*u8]
    const size = 1 + 4 + 4 + 4 + n * 4 + n;
    var buf = try alloc.alloc(u8, size);
    var pos: usize = 0;

    buf[pos] = CHUNK_FORMAT_VERSION;
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], n, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(max_w), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(min_w), .little);
    pos += 4;

    for (deltas) |d| {
        std.mem.writeInt(u32, buf[pos..][0..4], d, .little);
        pos += 4;
    }
    @memcpy(buf[pos .. pos + n], quant);

    return buf;
}

const DecodedChunk = struct {
    doc_nums: []u32,
    weights: []f32,
};

fn decodeChunkMaxWeight(data: []const u8) !f32 {
    if (data.len < 9) return error.InvalidChunk;
    const bits = std.mem.readInt(u32, data[5..9], .little);
    return @bitCast(bits);
}

// Unextended legacy chunks can still reject disjoint selections without
// allocating doc-number/weight arrays. New range metadata makes this O(1).
fn chunkMayMatch(data: []const u8, bitmap: ?*const @import("../encoding/roaring.zig").RoaringBitmap) !bool {
    if (data.len < 13 or data[0] != CHUNK_FORMAT_VERSION) return error.InvalidChunk;
    const n = std.mem.readInt(u32, data[1..5], .little);
    if (n == 0 or 13 + @as(u64, n) * 5 != data.len) return error.InvalidChunk;
    if (bitmap == null) return true;
    const lower = std.mem.readInt(u32, data[13..17], .little);
    var upper = lower;
    for (1..n) |i| upper = std.math.add(u32, upper, std.mem.readInt(u32, data[13 + i * 4 ..][0..4], .little)) catch return error.InvalidChunk;
    return bitmap.?.rangeCardinality(lower, @as(u64, upper) + 1) != 0;
}
fn postingRangeMayMatch(range: []const u8, chunk: []const u8, bitmap: ?*const @import("../encoding/roaring.zig").RoaringBitmap) !bool {
    if (bitmap == null) return true;
    if (range.len < 8) return error.InvalidChunk;
    const end = 8 + @as(u64, std.mem.readInt(u32, range[0..4], .little)) + std.mem.readInt(u32, range[4..8], .little);
    if (end > range.len) return error.InvalidChunk;
    const tail = range[@intCast(end)..];
    if (tail.len != 0) {
        if (tail.len != 12 or (!std.mem.eql(u8, tail[0..4], "O32B") and !std.mem.eql(u8, tail[0..4], "O32U"))) return error.InvalidChunk;
        const lower = std.mem.readInt(u32, tail[4..8], .little);
        const upper = std.mem.readInt(u32, tail[8..12], .little);
        if (lower > upper) return error.InvalidChunk;
        return bitmap.?.rangeCardinality(lower, @as(u64, upper) + 1) != 0;
    }
    return chunkMayMatch(chunk, bitmap);
}

fn decodeChunk(alloc: Allocator, data: []const u8) !DecodedChunk {
    if (data.len < 13) return error.InvalidChunk;
    var pos: usize = 0;

    const version = data[pos];
    pos += 1;
    if (version != CHUNK_FORMAT_VERSION) return error.InvalidChunk;

    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const max_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const max_w: f32 = @bitCast(max_w_bits);
    pos += 4;
    const min_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const min_w: f32 = @bitCast(min_w_bits);
    pos += 4;

    if (@as(u64, pos) + @as(u64, n) * 5 != data.len or n == 0) return error.InvalidChunk;
    // Delta-decode doc nums
    var doc_nums = try alloc.alloc(u32, n);
    errdefer alloc.free(doc_nums);
    for (0..n) |i| {
        doc_nums[i] = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
    }
    // Undo deltas
    for (1..n) |i| {
        doc_nums[i] += doc_nums[i - 1];
    }

    // Dequantize weights
    var weights = try alloc.alloc(f32, n);
    errdefer alloc.free(weights);
    const scale = if (max_w > min_w) max_w - min_w else 1.0;
    for (0..n) |i| {
        const q: f32 = @floatFromInt(data[pos + i]);
        weights[i] = min_w + q * (scale / 255.0);
    }

    return .{ .doc_nums = doc_nums, .weights = weights };
}

fn collectSelectedChunkEntries(
    alloc: Allocator,
    data: []const u8,
    selected_docs: *const SelectedDocLookup,
    out_doc_nums: *std.ArrayListUnmanaged(u32),
    out_weights: *std.ArrayListUnmanaged(f32),
    out_min_doc_id: *?[]const u8,
    out_max_doc_id: *?[]const u8,
    out_max_weight: *f32,
) !void {
    if (data.len < 13) return error.InvalidChunk;
    var pos: usize = 0;

    const version = data[pos];
    pos += 1;
    if (version != CHUNK_FORMAT_VERSION) return error.InvalidChunk;

    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const max_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const max_w: f32 = @bitCast(max_w_bits);
    pos += 4;
    const min_w_bits = std.mem.readInt(u32, data[pos..][0..4], .little);
    const min_w: f32 = @bitCast(min_w_bits);
    pos += 4;

    const delta_start = pos;
    const delta_bytes = @as(usize, n) * 4;
    const quant_start = delta_start + delta_bytes;
    if (data.len < quant_start + n) return error.InvalidChunk;

    try out_doc_nums.ensureTotalCapacity(alloc, n);
    try out_weights.ensureTotalCapacity(alloc, n);

    const scale = if (max_w > min_w) max_w - min_w else 1.0;
    var current_doc_num: u32 = 0;
    var first = true;
    for (0..n) |i| {
        const delta = std.mem.readInt(u32, data[delta_start + i * 4 ..][0..4], .little);
        current_doc_num = if (first) blk: {
            first = false;
            break :blk delta;
        } else current_doc_num + delta;

        const doc_id = selected_docs.get(current_doc_num) orelse continue;
        const q: f32 = @floatFromInt(data[quant_start + i]);
        const weight = min_w + q * (scale / 255.0);
        try out_doc_nums.append(alloc, current_doc_num);
        try out_weights.append(alloc, weight);
        out_max_weight.* = if (out_doc_nums.items.len == 1) weight else @max(out_max_weight.*, weight);
        updateBorrowedRangeBounds(out_min_doc_id, out_max_doc_id, doc_id, doc_id);
    }
}

// ============================================================================
// Forward index entry encoding
// ============================================================================

fn encodeFwdEntry(alloc: Allocator, doc_num: u64, term_ids: []const u32, weights: []const f32) ![]u8 {
    const n: u32 = @intCast(term_ids.len);
    const size = 8 + 4 + n * 4 + n * 4;
    var buf = try alloc.alloc(u8, size);
    var pos: usize = 0;

    std.mem.writeInt(u64, buf[pos..][0..8], doc_num, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], n, .little);
    pos += 4;
    for (term_ids) |tid| {
        std.mem.writeInt(u32, buf[pos..][0..4], tid, .little);
        pos += 4;
    }
    for (weights) |w| {
        std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(w), .little);
        pos += 4;
    }
    return buf;
}

fn encodedFwdEntryLen(term_ids: []const u32) usize {
    return 8 + 4 + term_ids.len * 4 + term_ids.len * 4;
}

fn appendFwdEntry(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), doc_num: u64, term_ids: []const u32, weights: []const f32) !void {
    try appendU64Le(alloc, out, doc_num);
    try appendU32Le(alloc, out, @intCast(term_ids.len));
    for (term_ids) |tid| try appendU32Le(alloc, out, tid);
    for (weights) |weight| try appendU32Le(alloc, out, @bitCast(weight));
}

const DecodedFwdEntry = struct {
    doc_num: u64,
    term_ids: []u32,
    weights: []f32,
};

fn decodeFwdDocNum(data: []const u8) !u64 {
    if (data.len < 8) return error.InvalidChunk;
    return std.mem.readInt(u64, data[0..8], .little);
}

fn decodeFwdEntry(alloc: Allocator, data: []const u8) !DecodedFwdEntry {
    var pos: usize = 0;
    const doc_num = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;

    var term_ids = try alloc.alloc(u32, n);
    errdefer alloc.free(term_ids);
    for (0..n) |i| {
        term_ids[i] = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
    }

    var weights = try alloc.alloc(f32, n);
    errdefer alloc.free(weights);
    for (0..n) |i| {
        const bits = std.mem.readInt(u32, data[pos..][0..4], .little);
        weights[i] = @bitCast(bits);
        pos += 4;
    }

    return .{ .doc_num = doc_num, .term_ids = term_ids, .weights = weights };
}

fn parseFwdDocNumAndTermCount(data: []const u8) !struct { doc_num: u64, term_count: u32, terms_start: usize } {
    if (data.len < 12) return error.InvalidChunk;
    var pos: usize = 0;
    const doc_num = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const n = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (data.len < 12 + @as(usize, n) * 4) return error.InvalidChunk;
    return .{ .doc_num = doc_num, .term_count = n, .terms_start = pos };
}

fn forEachFwdTermId(data: []const u8, comptime Context: type, context: *Context, comptime func: fn (*Context, u32) anyerror!void) !u64 {
    const parsed = try parseFwdDocNumAndTermCount(data);
    var pos = parsed.terms_start;
    for (0..parsed.term_count) |_| {
        const term_id = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        try func(context, term_id);
    }
    return parsed.doc_num;
}

// ============================================================================
// Term metadata encoding
// ============================================================================

fn encodeTermMeta(max_weight: f32, chunk_count: u32) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], @bitCast(max_weight), .little);
    std.mem.writeInt(u32, buf[4..8], chunk_count, .little);
    return buf;
}

fn decodeTermMeta(data: []const u8) struct { max_weight: f32, chunk_count: u32 } {
    const mw_bits = std.mem.readInt(u32, data[0..4], .little);
    const cc = std.mem.readInt(u32, data[4..8], .little);
    return .{ .max_weight = @bitCast(mw_bits), .chunk_count = cc };
}

const ChunkRangeMeta = struct {
    min_doc_id: []const u8,
    max_doc_id: []const u8,
};

const SEGMENT_FORMAT_VERSION: u32 = 2;
const segment_magic = "ASPSSEG1";
const segment_header_len: usize = segment_magic.len + 8;
const segment_dir_entry_len: usize = 20;
const docmap_magic = "ASPSMAP1";
const docmap_header_len: usize = docmap_magic.len + 8;

const SegmentTermPayload = struct {
    term_id: u32,
    bytes: []u8,

    pub fn deinit(self: *SegmentTermPayload, alloc: Allocator) void {
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

const DocMapLookup = struct {
    doc_num: u64,
    doc_id: []const u8,
    fwd_data: []const u8,
};

const CollectedSparseDocs = struct {
    alloc: Allocator,
    writes: std.ArrayListUnmanaged(SparseWrite) = .empty,
    doc_ids: std.ArrayListUnmanaged([]u8) = .empty,
    term_ids: std.ArrayListUnmanaged(u32) = .empty,
    selected_doc_nums: std.AutoHashMapUnmanaged(u32, void) = .empty,

    pub fn deinit(self: *@This()) void {
        for (self.writes.items) |write| {
            self.alloc.free(@constCast(write.doc_id));
            self.alloc.free(@constCast(write.vec.indices));
            self.alloc.free(@constCast(write.vec.values));
        }
        self.writes.deinit(self.alloc);
        for (self.doc_ids.items) |doc_id| self.alloc.free(doc_id);
        self.doc_ids.deinit(self.alloc);
        self.term_ids.deinit(self.alloc);
        self.selected_doc_nums.deinit(self.alloc);
        self.* = undefined;
    }

    fn takeDocIds(self: *@This()) ![][]u8 {
        const out = try self.doc_ids.toOwnedSlice(self.alloc);
        self.doc_ids = .empty;
        return out;
    }
};

fn appendU32Le(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendU64Le(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn encodeSegmentFromSortedPostings(
    alloc: Allocator,
    postings: []const BulkPosting,
    chunk_size: u32,
) ![]u8 {
    var payloads = std.ArrayListUnmanaged(SegmentTermPayload).empty;
    defer {
        for (payloads.items) |*payload| payload.deinit(alloc);
        payloads.deinit(alloc);
    }

    var start: usize = 0;
    while (start < postings.len) {
        const term_id = postings[start].term_id;
        var end = start + 1;
        while (end < postings.len and postings[end].term_id == term_id) : (end += 1) {}

        var unique = true;
        for (postings[start + 1 .. end], postings[start .. end - 1]) |next, previous| if (next.doc_num <= previous.doc_num) {
            unique = false;
        };
        var term_payload = std.ArrayListUnmanaged(u8).empty;
        errdefer term_payload.deinit(alloc);
        var cursor = start;
        while (cursor < end) {
            const take = @min(@as(usize, @intCast(chunk_size)), end - cursor);
            var doc_nums = try alloc.alloc(u32, take);
            defer alloc.free(doc_nums);
            var weights = try alloc.alloc(f32, take);
            defer alloc.free(weights);
            var min_doc_id: ?[]const u8 = null;
            var max_doc_id: ?[]const u8 = null;

            for (postings[cursor .. cursor + take], 0..) |posting, i| {
                doc_nums[i] = posting.doc_num;
                weights[i] = posting.weight;
                updateBorrowedRangeBounds(&min_doc_id, &max_doc_id, posting.doc_id, posting.doc_id);
            }

            const chunk = try encodeChunk(alloc, doc_nums, weights);
            defer alloc.free(chunk);
            const range = try encodeChunkOrdinalRange(alloc, min_doc_id.?, max_doc_id.?, doc_nums, unique);
            defer alloc.free(range);
            if (!unique) @memcpy(range[range.len - 12 ..][0..4], "O32B");

            try appendU32Le(alloc, &term_payload, @intCast(chunk.len));
            try appendU32Le(alloc, &term_payload, @intCast(range.len));
            try term_payload.appendSlice(alloc, chunk);
            try term_payload.appendSlice(alloc, range);
            cursor += take;
        }

        try payloads.append(alloc, .{
            .term_id = term_id,
            .bytes = try term_payload.toOwnedSlice(alloc),
        });
        start = end;
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, segment_magic);
    try appendU32Le(alloc, &out, SEGMENT_FORMAT_VERSION);
    try appendU32Le(alloc, &out, @intCast(payloads.items.len));

    var payload_offset: u64 = segment_header_len + @as(u64, @intCast(payloads.items.len)) * segment_dir_entry_len;
    for (payloads.items) |payload| {
        try appendU32Le(alloc, &out, payload.term_id);
        try appendU32Le(alloc, &out, countSegmentPayloadChunks(payload.bytes));
        try appendU64Le(alloc, &out, payload_offset);
        try appendU32Le(alloc, &out, @intCast(payload.bytes.len));
        payload_offset += payload.bytes.len;
    }
    for (payloads.items) |payload| try out.appendSlice(alloc, payload.bytes);
    return try out.toOwnedSlice(alloc);
}

fn countSegmentPayloadChunks(payload: []const u8) u32 {
    var count: u32 = 0;
    var pos: usize = 0;
    while (pos + 8 <= payload.len) : (count += 1) {
        const chunk_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        const range_len = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little);
        pos += 8 + @as(usize, chunk_len) + @as(usize, range_len);
        if (pos > payload.len) return count;
    }
    return count;
}

fn appendCompactionBlock(a: Allocator, builder: *posting_pages.Builder, postings: []const BulkPosting) !void {
    const doc_nums = try a.alloc(u32, postings.len);
    defer a.free(doc_nums);
    const weights = try a.alloc(f32, postings.len);
    defer a.free(weights);
    var minimum: ?[]const u8 = null;
    var maximum: ?[]const u8 = null;
    for (postings, 0..) |posting, i| {
        doc_nums[i] = posting.doc_num;
        weights[i] = posting.weight;
        updateBorrowedRangeBounds(&minimum, &maximum, posting.doc_id, posting.doc_id);
    }
    const chunk = try encodeChunk(a, doc_nums, weights);
    defer a.free(chunk);
    const range = try encodeChunkOrdinalRange(a, minimum.?, maximum.?, doc_nums, true);
    defer a.free(range);
    var frame: std.ArrayList(u8) = .empty;
    defer frame.deinit(a);
    try appendU32Le(a, &frame, @intCast(chunk.len));
    try appendU32Le(a, &frame, @intCast(range.len));
    try frame.appendSlice(a, chunk);
    try frame.appendSlice(a, range);
    try builder.append(postings[0].term_id, doc_nums[0], doc_nums[doc_nums.len - 1], frame.items);
}

fn segmentTermPayload(data: []const u8, term_id: u32) !?[]const u8 {
    if (data.len < segment_header_len) return error.InvalidSparseSegment;
    if (!std.mem.eql(u8, data[0..segment_magic.len], segment_magic)) return error.InvalidSparseSegment;
    const version = std.mem.readInt(u32, data[segment_magic.len..][0..4], .little);
    if (version != 1 and version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseSegment;
    const term_count = std.mem.readInt(u32, data[segment_magic.len + 4 ..][0..4], .little);
    const dir_start = segment_header_len;
    const dir_len = @as(usize, term_count) * segment_dir_entry_len;
    if (dir_start + dir_len > data.len) return error.InvalidSparseSegment;

    var pos = dir_start;
    for (0..term_count) |_| {
        const current_term = std.mem.readInt(u32, data[pos..][0..4], .little);
        const offset = std.mem.readInt(u64, data[pos + 8 ..][0..8], .little);
        const len = std.mem.readInt(u32, data[pos + 16 ..][0..4], .little);
        if (current_term == term_id) {
            const start: usize = @intCast(offset);
            const end = start + @as(usize, len);
            if (end > data.len) return error.InvalidSparseSegment;
            return data[start..end];
        }
        pos += segment_dir_entry_len;
    }
    return null;
}

fn forEachSegmentTermPayload(
    segment: []const u8,
    context: anytype,
    comptime func: fn (@TypeOf(context), u32, []const u8) anyerror!void,
) !void {
    if (segment.len < segment_header_len) return error.InvalidSparseSegment;
    if (!std.mem.eql(u8, segment[0..segment_magic.len], segment_magic)) return error.InvalidSparseSegment;
    const version = std.mem.readInt(u32, segment[segment_magic.len..][0..4], .little);
    if (version != 1 and version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseSegment;
    const term_count = std.mem.readInt(u32, segment[segment_magic.len + 4 ..][0..4], .little);
    const dir_start = segment_header_len;
    const dir_len = @as(usize, term_count) * segment_dir_entry_len;
    if (dir_start + dir_len > segment.len) return error.InvalidSparseSegment;

    var pos = dir_start;
    for (0..term_count) |_| {
        const term_id = std.mem.readInt(u32, segment[pos..][0..4], .little);
        const offset = std.mem.readInt(u64, segment[pos + 8 ..][0..8], .little);
        const len = std.mem.readInt(u32, segment[pos + 16 ..][0..4], .little);
        const start: usize = @intCast(offset);
        const end = start + @as(usize, len);
        if (end > segment.len) return error.InvalidSparseSegment;
        try func(context, term_id, segment[start..end]);
        pos += segment_dir_entry_len;
    }
}

fn forEachSegmentChunk(
    alloc: Allocator,
    segment: []const u8,
    term_id: u32,
    context: anytype,
    comptime func: fn (@TypeOf(context), DecodedChunk) anyerror!void,
) !void {
    const payload = (try segmentTermPayload(segment, term_id)) orelse return;
    return forEachPayloadChunk(alloc, payload, context, func);
}
fn forEachPayloadChunk(alloc: Allocator, payload: []const u8, context: anytype, comptime func: fn (@TypeOf(context), DecodedChunk) anyerror!void) !void {
    var pos: usize = 0;
    while (pos < payload.len) {
        if (pos + 8 > payload.len) return error.InvalidSparseSegment;
        const chunk_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        const range_len = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little);
        pos += 8;
        const chunk_end = pos + @as(usize, chunk_len);
        const range_end = chunk_end + @as(usize, range_len);
        if (range_end > payload.len) return error.InvalidSparseSegment;
        if (@hasDecl(@typeInfo(@TypeOf(context)).pointer.child, "shouldDecode")) {
            if (!try context.shouldDecode(payload[pos..chunk_end], payload[chunk_end..range_end])) {
                pos = range_end;
                continue;
            }
        }
        const decoded = try decodeChunk(alloc, payload[pos..chunk_end]);
        defer alloc.free(decoded.doc_nums);
        defer alloc.free(decoded.weights);
        try func(context, decoded);
        pos = range_end;
    }
}

fn forEachStoredSegmentChunk(alloc: Allocator, txn: anytype, id: u64, root: []const u8, term: u32, context: anytype, comptime func: fn (@TypeOf(context), DecodedChunk) anyerror!void) !void {
    if (!posting_pages.paged(root)) return forEachSegmentChunk(alloc, root, term, context, func);
    const Visitor = struct {
        a: Allocator,
        context: @TypeOf(context),
        fn visit(self: *@This(), _: u32, payload: []const u8) !void {
            try forEachPayloadChunk(self.a, payload, self.context, func);
        }
    };
    var visitor: Visitor = .{ .a = alloc, .context = context };
    try posting_pages.forEachBlock(txn, id, term, &visitor, Visitor.visit);
}

fn encodeDocMapSegment(alloc: Allocator, writes: []const SparseWrite, docs: []const BulkDoc) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, docmap_magic);
    try appendU32Le(alloc, &out, SEGMENT_FORMAT_VERSION);
    try appendU32Le(alloc, &out, @intCast(docs.len));
    for (docs) |doc| {
        const write = writes[doc.write_idx];
        const fwd_len = encodedFwdEntryLen(write.vec.indices);
        try appendU64Le(alloc, &out, doc.doc_num);
        try appendU32Le(alloc, &out, @intCast(write.doc_id.len));
        try appendU32Le(alloc, &out, @intCast(fwd_len));
        try out.appendSlice(alloc, write.doc_id);
        try appendFwdEntry(alloc, &out, doc.doc_num, write.vec.indices, write.vec.values);
    }
    return try out.toOwnedSlice(alloc);
}

const paged_docmap_magic = "ASPSDM02";
fn pagedDocMap(data: []const u8) bool {
    return data.len == 16 and std.mem.eql(u8, data[0..8], paged_docmap_magic);
}
fn docMapEntryKey(id: u64, doc: u64) [17]u8 {
    var key: [17]u8 = undefined;
    key[0] = 0x13;
    std.mem.writeInt(u64, key[1..9], id, .big);
    std.mem.writeInt(u64, key[9..17], doc, .big);
    return key;
}
fn forEachStoredDocMapEntry(txn: anytype, id: u64, data: []const u8, context: anytype, comptime visit: fn (@TypeOf(context), DocMapLookup) anyerror!bool) !bool {
    if (!pagedDocMap(data)) return forEachDocMapEntry(data, context, visit);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const start = docMapEntryKey(id, 0);
    var next = try cursor.seekAtOrAfter(start[0..9]);
    var count: u64 = 0;
    while (next) |entry| {
        if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..9], start[0..9])) break;
        if (entry.value.len < 4) return error.InvalidSparseDocMapSegment;
        const length = std.mem.readInt(u32, entry.value[0..4], .little);
        if (length > entry.value.len - 4) return error.InvalidSparseDocMapSegment;
        if (try visit(context, .{ .doc_num = std.mem.readInt(u64, entry.key[9..17], .big), .doc_id = entry.value[4..][0..length], .fwd_data = entry.value[4 + length ..] })) return true;
        count += 1;
        next = try cursor.next();
    }
    if (count != std.mem.readInt(u32, data[12..16], .little)) return error.InvalidSparseDocMapSegment;
    return false;
}
fn forEachDocMapEntry(
    data: []const u8,
    context: anytype,
    comptime func: fn (@TypeOf(context), DocMapLookup) anyerror!bool,
) !bool {
    if (data.len < docmap_header_len) return error.InvalidSparseDocMapSegment;
    if (!std.mem.eql(u8, data[0..docmap_magic.len], docmap_magic)) return error.InvalidSparseDocMapSegment;
    const version = std.mem.readInt(u32, data[docmap_magic.len..][0..4], .little);
    if (version != 1 and version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseDocMapSegment;
    const count = std.mem.readInt(u32, data[docmap_magic.len + 4 ..][0..4], .little);
    var pos: usize = docmap_header_len;
    for (0..count) |_| {
        if (pos + 16 > data.len) return error.InvalidSparseDocMapSegment;
        const doc_num = std.mem.readInt(u64, data[pos..][0..8], .little);
        const doc_id_len = std.mem.readInt(u32, data[pos + 8 ..][0..4], .little);
        const fwd_len = std.mem.readInt(u32, data[pos + 12 ..][0..4], .little);
        pos += 16;
        const doc_id_end = pos + @as(usize, doc_id_len);
        const fwd_end = doc_id_end + @as(usize, fwd_len);
        if (fwd_end > data.len) return error.InvalidSparseDocMapSegment;
        if (try func(context, .{
            .doc_num = doc_num,
            .doc_id = data[pos..doc_id_end],
            .fwd_data = data[doc_id_end..fwd_end],
        })) return true;
        pos = fwd_end;
    }
    return false;
}

const SelectedDocLookup = struct {
    map: ?*const std.AutoHashMapUnmanaged(u32, []u8) = null,
    owned_map: std.AutoHashMapUnmanaged(u32, []const u8) = .empty,
    dense_min_doc_num: u32 = 0,
    dense_doc_ids: []?[]const u8 = &.{},

    fn init(alloc: Allocator, map: *const std.AutoHashMapUnmanaged(u32, []u8)) !SelectedDocLookup {
        var lookup: SelectedDocLookup = .{ .map = map };
        if (map.count() == 0) return lookup;

        var min_doc_num: u32 = std.math.maxInt(u32);
        var max_doc_num: u32 = 0;
        var it = map.iterator();
        while (it.next()) |entry| {
            const doc_num = entry.key_ptr.*;
            min_doc_num = @min(min_doc_num, doc_num);
            max_doc_num = @max(max_doc_num, doc_num);
        }

        const span = @as(usize, max_doc_num) - @as(usize, min_doc_num) + 1;
        if (span > map.count() * 8) return lookup;

        const dense_doc_ids = try alloc.alloc(?[]const u8, span);
        errdefer alloc.free(dense_doc_ids);
        @memset(dense_doc_ids, null);

        it = map.iterator();
        while (it.next()) |entry| {
            dense_doc_ids[@as(usize, entry.key_ptr.*) - min_doc_num] = entry.value_ptr.*;
        }

        lookup.dense_min_doc_num = min_doc_num;
        lookup.dense_doc_ids = dense_doc_ids;
        return lookup;
    }

    fn initFromPairs(alloc: Allocator, doc_nums: []const u32, doc_ids: []const []const u8) !SelectedDocLookup {
        std.debug.assert(doc_nums.len == doc_ids.len);
        var lookup: SelectedDocLookup = .{};
        if (doc_nums.len == 0) return lookup;

        var min_doc_num: u32 = std.math.maxInt(u32);
        var max_doc_num: u32 = 0;
        for (doc_nums) |doc_num| {
            min_doc_num = @min(min_doc_num, doc_num);
            max_doc_num = @max(max_doc_num, doc_num);
        }

        const span = @as(usize, max_doc_num) - @as(usize, min_doc_num) + 1;
        if (span <= doc_nums.len * 8) {
            const dense_doc_ids = try alloc.alloc(?[]const u8, span);
            errdefer alloc.free(dense_doc_ids);
            @memset(dense_doc_ids, null);
            for (doc_nums, doc_ids) |doc_num, doc_id| {
                dense_doc_ids[@as(usize, doc_num) - min_doc_num] = doc_id;
            }
            lookup.dense_min_doc_num = min_doc_num;
            lookup.dense_doc_ids = dense_doc_ids;
            return lookup;
        }

        try lookup.owned_map.ensureTotalCapacity(alloc, @intCast(doc_nums.len));
        for (doc_nums, doc_ids) |doc_num, doc_id| {
            lookup.owned_map.putAssumeCapacity(doc_num, doc_id);
        }
        return lookup;
    }

    pub fn deinit(self: *SelectedDocLookup, alloc: Allocator) void {
        if (self.dense_doc_ids.len > 0) alloc.free(self.dense_doc_ids);
        self.owned_map.deinit(alloc);
        self.* = undefined;
    }

    fn get(self: *const SelectedDocLookup, doc_num: u32) ?[]const u8 {
        if (self.dense_doc_ids.len > 0) {
            if (doc_num < self.dense_min_doc_num) return null;
            const idx = @as(usize, doc_num) - self.dense_min_doc_num;
            if (idx >= self.dense_doc_ids.len) return null;
            return self.dense_doc_ids[idx];
        }
        if (self.owned_map.count() > 0) return self.owned_map.get(doc_num);
        if (self.map) |map| return map.get(doc_num);
        return null;
    }
};

fn encodeChunkRangeMeta(alloc: Allocator, min_doc_id: []const u8, max_doc_id: []const u8) ![]u8 {
    const total = 8 + min_doc_id.len + max_doc_id.len;
    var buf = try alloc.alloc(u8, total);
    std.mem.writeInt(u32, buf[0..4], @intCast(min_doc_id.len), .little);
    std.mem.writeInt(u32, buf[4..8], @intCast(max_doc_id.len), .little);
    @memcpy(buf[8 .. 8 + min_doc_id.len], min_doc_id);
    @memcpy(buf[8 + min_doc_id.len ..][0..max_doc_id.len], max_doc_id);
    return buf;
}

// Readers before ordinal bounds ignore trailing range bytes. Preserve that
// wire contract: the posting payload and document-ID range prefix stay V1.
fn encodeChunkOrdinalRange(alloc: Allocator, min_id: []const u8, max_id: []const u8, nums: []const u32, producer_unique: bool) ![]u8 {
    if (nums.len == 0) return error.InvalidChunk;
    const prefix = try encodeChunkRangeMeta(alloc, min_id, max_id);
    defer alloc.free(prefix);
    const result = try alloc.alloc(u8, prefix.len + 12);
    @memcpy(result[0..prefix.len], prefix);
    const tail = result[prefix.len..];
    var unique = producer_unique;
    for (nums[1..], nums[0 .. nums.len - 1]) |next, previous| if (next <= previous) {
        unique = false;
    };
    @memcpy(tail[0..4], if (unique) "O32U" else "O32B");
    std.mem.writeInt(u32, tail[4..8], nums[0], .little);
    std.mem.writeInt(u32, tail[8..12], nums[nums.len - 1], .little);
    return result;
}

fn decodeChunkRangeMeta(data: []const u8) !ChunkRangeMeta {
    if (data.len < 8) return error.InvalidChunk;
    const min_len = std.mem.readInt(u32, data[0..4], .little);
    const max_len = std.mem.readInt(u32, data[4..8], .little);
    const min_start: usize = 8;
    const min_end = min_start + min_len;
    const max_end = min_end + max_len;
    if (max_end > data.len) return error.InvalidChunk;
    return .{
        .min_doc_id = data[min_start..min_end],
        .max_doc_id = data[min_end..max_end],
    };
}

fn updateOwnedRangeBounds(
    alloc: Allocator,
    min_out: *?[]u8,
    max_out: *?[]u8,
    min_doc_id: []const u8,
    max_doc_id: []const u8,
) !void {
    if (min_out.* == null or std.mem.order(u8, min_doc_id, min_out.*.?) == .lt) {
        if (min_out.*) |existing| alloc.free(existing);
        min_out.* = try alloc.dupe(u8, min_doc_id);
    }
    if (max_out.* == null or std.mem.order(u8, max_doc_id, max_out.*.?) == .gt) {
        if (max_out.*) |existing| alloc.free(existing);
        max_out.* = try alloc.dupe(u8, max_doc_id);
    }
}

fn updateBorrowedRangeBounds(
    min_out: *?[]const u8,
    max_out: *?[]const u8,
    min_doc_id: []const u8,
    max_doc_id: []const u8,
) void {
    if (min_out.* == null or std.mem.order(u8, min_doc_id, min_out.*.?) == .lt) {
        min_out.* = min_doc_id;
    }
    if (max_out.* == null or std.mem.order(u8, max_doc_id, max_out.*.?) == .gt) {
        max_out.* = max_doc_id;
    }
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

var sparse_search_profile_enabled_cache: std.atomic.Value(u8) = .init(0);

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

fn envBoolEnabled(raw_z: [*:0]const u8) bool {
    const raw = std.mem.span(raw_z);
    return !(std.mem.eql(u8, raw, "0") or
        std.ascii.eqlIgnoreCase(raw, "false") or
        std.ascii.eqlIgnoreCase(raw, "no"));
}

fn sparseSearchProfileEnabled() bool {
    const cached = sparse_search_profile_enabled_cache.load(.monotonic);
    if (cached != 0) return cached == 2;
    if (comptime builtin.os.tag == .freestanding) {
        sparse_search_profile_enabled_cache.store(1, .monotonic);
        return false;
    }
    const raw_z = getenv("ANTFLY_BENCH_SPARSE_SEARCH_PROFILE") orelse
        getenv("ANTFLY_BENCH_METRICS") orelse {
        sparse_search_profile_enabled_cache.store(1, .monotonic);
        return false;
    };
    const enabled = envBoolEnabled(raw_z);
    sparse_search_profile_enabled_cache.store(if (enabled) 2 else 1, .monotonic);
    return enabled;
}

fn sortAndDedupU32(items: []u32) []u32 {
    if (items.len <= 1) return items;
    std.mem.sort(u32, items, {}, struct {
        fn lessThan(_: void, a: u32, b: u32) bool {
            return a < b;
        }
    }.lessThan);
    var out_len: usize = 1;
    for (items[1..]) |item| {
        if (item == items[out_len - 1]) continue;
        items[out_len] = item;
        out_len += 1;
    }
    return items[0..out_len];
}

fn u32SliceContains(items: []const u32, needle: u32) bool {
    for (items) |item| {
        if (item == needle) return true;
    }
    return false;
}

fn sortDocNumsAndWeights(doc_nums: []u32, weights: []f32) void {
    std.debug.assert(doc_nums.len == weights.len);
    if (doc_nums.len <= 1) return;
    for (1..doc_nums.len) |i| {
        var j = i;
        while (j > 0 and doc_nums[j] < doc_nums[j - 1]) : (j -= 1) {
            std.mem.swap(u32, &doc_nums[j], &doc_nums[j - 1]);
            std.mem.swap(f32, &weights[j], &weights[j - 1]);
        }
    }
}

// ============================================================================
// Key builders
// ============================================================================

const key_fwd: u8 = 0x01;
const key_rev: u8 = 0x02;
const key_segment: u8 = 0x03;
const key_meta: u8 = 0x04;
const key_term_catalog: u8 = 0x05;
const key_docmap_segment: u8 = 0x06;
const key_doc_tombstone: u8 = 0x07;
const key_doc_incarnation: u8 = 0x08;
const key_segment_incarnation: u8 = 0x09;
const key_docmap_incarnation: u8 = 0x0a;
const key_docmap_locator: u8 = 0x0b;
const key_physical_block: u8 = 0x0c;
const key_inv: u8 = 0x10;

const meta_next_doc_num: u8 = 0x01;
const meta_doc_count: u8 = 0x02;
const meta_term_count: u8 = 0x03;
const meta_next_segment_id: u8 = 0x04;
const meta_complete_locators: u8 = 0x05;
const meta_complete_physical: u8 = 0x06;

const inv_kind_meta: u8 = 0x01;
const inv_kind_chunk: u8 = 0x02;
const inv_kind_chunk_meta: u8 = 0x03;
const inv_kind_term_range: u8 = 0x04;

fn taggedPrefix(comptime tag: u8) *const [1]u8 {
    return &.{tag};
}

fn fwdKey(buf: []u8, doc_id: []const u8) []const u8 {
    std.debug.assert(buf.len >= 1 + doc_id.len);
    buf[0] = key_fwd;
    @memcpy(buf[1..][0..doc_id.len], doc_id);
    return buf[0 .. 1 + doc_id.len];
}

fn fwdKeyAlloc(alloc: Allocator, doc_id: []const u8) ![]u8 {
    const key = try alloc.alloc(u8, 1 + doc_id.len);
    key[0] = key_fwd;
    @memcpy(key[1..], doc_id);
    return key;
}

fn fwdDocIdFromKey(key: []const u8) ?[]const u8 {
    if (key.len == 0 or key[0] != key_fwd) return null;
    return key[1..];
}

fn revKey(buf: []u8, doc_num: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_rev;
    std.mem.writeInt(u64, buf[1..][0..8], doc_num, .big);
    return buf[0..9];
}

fn invMetaKey(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 6);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_meta;
    return buf[0..6];
}

fn invChunkKey(buf: []u8, term_id: u32, chunk_num: u32) []const u8 {
    std.debug.assert(buf.len >= 10);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_chunk;
    std.mem.writeInt(u32, buf[6..][0..4], chunk_num, .big);
    return buf[0..10];
}

fn invChunkMetaKey(buf: []u8, term_id: u32, chunk_num: u32) []const u8 {
    std.debug.assert(buf.len >= 10);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_chunk_meta;
    std.mem.writeInt(u32, buf[6..][0..4], chunk_num, .big);
    return buf[0..10];
}

fn termRangeKey(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 6);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_term_range;
    return buf[0..6];
}

fn invChunkPrefix(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 6);
    buf[0] = key_inv;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    buf[5] = inv_kind_chunk;
    return buf[0..6];
}

fn segmentKey(buf: []u8, segment_id: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_segment;
    std.mem.writeInt(u64, buf[1..][0..8], segment_id, .big);
    return buf[0..9];
}

fn docMapSegmentKey(buf: []u8, segment_id: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_docmap_segment;
    std.mem.writeInt(u64, buf[1..][0..8], segment_id, .big);
    return buf[0..9];
}

fn docTombstoneKey(buf: []u8, doc_num: u64) []const u8 {
    std.debug.assert(buf.len >= 9);
    buf[0] = key_doc_tombstone;
    std.mem.writeInt(u64, buf[1..][0..8], doc_num, .big);
    return buf[0..9];
}

fn docIncarnationKey(buf: *[9]u8, doc_num: u64) []const u8 {
    buf[0] = key_doc_incarnation;
    std.mem.writeInt(u64, buf[1..9], doc_num, .big);
    return buf;
}

fn segmentIncarnationKey(buf: *[17]u8, segment_id: u64, doc_num: u64, docmap: bool) []const u8 {
    buf[0] = if (docmap) key_docmap_incarnation else key_segment_incarnation;
    std.mem.writeInt(u64, buf[1..9], segment_id, .big);
    std.mem.writeInt(u64, buf[9..17], doc_num, .big);
    return buf;
}

// Point-addressable physical key -> native ordinal and forward-vector location.
// The checkpoint authenticates these small typed records with the docmap data.
fn locatorKey(buf: []u8, doc_id: []const u8) []const u8 {
    std.debug.assert(doc_id.len + 1 <= buf.len);
    buf[0] = key_docmap_locator;
    @memcpy(buf[1..][0..doc_id.len], doc_id);
    return buf[0 .. doc_id.len + 1];
}
const DocMapLocator = struct { num: u64, segment: u64, offset: u64, length: u32, epoch: u64 };
fn readLocator(txn: anytype, doc_id: []const u8) !?DocMapLocator {
    if (doc_id.len >= 256) return null;
    var key: [256]u8 = undefined;
    const bytes = txn.get(locatorKey(&key, doc_id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != 36) return error.InvalidSparseDocMapSegment;
    const locator: DocMapLocator = .{
        .num = std.mem.readInt(u64, bytes[0..8], .little),
        .segment = std.mem.readInt(u64, bytes[8..16], .little),
        .offset = std.mem.readInt(u64, bytes[16..24], .little),
        .length = std.mem.readInt(u32, bytes[24..28], .little),
        .epoch = std.mem.readInt(u64, bytes[28..36], .little),
    };
    const current = try currentIncarnation(txn, locator.num);
    if (locator.epoch > current) return error.InvalidSparseSegment;
    if (locator.epoch != current) return null;
    return locator;
}
// Lake private identities encode immutable object/group/row coordinates. Each
// durable 1024-row block stores only (low-row:u16, native-ordinal:u32) pairs.
// The write overlay coalesces all modifications before one put per block.
const PhysicalMaps = struct {
    const Key = [89]u8; // tag + 80-byte physical prefix + block ordinal
    a: Allocator,
    blocks: std.AutoHashMapUnmanaged(Key, *[1024]u64) = .empty,
    fn deinit(self: *@This()) void {
        var values = self.blocks.valueIterator();
        while (values.next()) |value| self.a.destroy(value.*);
        self.blocks.deinit(self.a);
    }
    fn key(prefix: []const u8, block: u64) !Key {
        if (prefix.len != 80 or !std.mem.startsWith(u8, prefix, "lake2:") or prefix[70] != ':' or prefix[79] != ':') return error.InvalidSparsePhysicalMap;
        var result: Key = undefined;
        result[0] = key_physical_block;
        @memcpy(result[1..81], prefix);
        std.mem.writeInt(u64, result[81..89], block, .big);
        return result;
    }
    fn load(self: *@This(), txn: anytype, block_key: Key) !*[1024]u64 {
        if (!self.blocks.contains(block_key) and self.blocks.count() >= 64) {
            try self.flush(txn);
            self.deinit();
            self.blocks = .empty;
        }
        const entry = try self.blocks.getOrPut(self.a, block_key);
        if (entry.found_existing) return entry.value_ptr.*;
        errdefer _ = self.blocks.remove(block_key);
        const values = try self.a.create([1024]u64);
        errdefer self.a.destroy(values);
        @memset(values, 0);
        const data = txn.get(&block_key) catch |err| switch (err) {
            error.NotFound => &.{},
            else => return err,
        };
        if (data.len % 6 != 0) return error.InvalidSparsePhysicalMap;
        var position: usize = 0;
        while (position < data.len) : (position += 6) {
            const row = std.mem.readInt(u16, data[position..][0..2], .little);
            if (row >= 1024 or values[row] != 0) return error.InvalidSparsePhysicalMap;
            values[row] = @as(u64, std.mem.readInt(u32, data[position + 2 ..][0..4], .little)) + 1;
        }
        entry.value_ptr.* = values;
        return values;
    }
    fn update(self: *@This(), txn: anytype, id: []const u8, num: ?u32) !void {
        if (id.len != 96 or !std.mem.startsWith(u8, id, "lake2:")) return;
        const row = std.fmt.parseInt(u64, id[80..96], 16) catch return error.InvalidSparsePhysicalMap;
        const values = try self.load(txn, try key(id[0..80], row >> 10));
        values[row & 1023] = if (num) |value| @as(u64, value) + 1 else 0;
    }
    fn flush(self: *@This(), txn: anytype) !void {
        var entries = self.blocks.iterator();
        var bytes: [1024 * 6]u8 = undefined;
        while (entries.next()) |entry| {
            var size: usize = 0;
            for (entry.value_ptr.*, 0..) |num, row| {
                if (num == 0) continue;
                std.mem.writeInt(u16, bytes[size..][0..2], @intCast(row), .little);
                std.mem.writeInt(u32, bytes[size + 2 ..][0..4], @intCast(num - 1), .little);
                size += 6;
            }
            try txn.put(entry.key_ptr, bytes[0..size]);
        }
    }
};
fn selectPhysicalBlock(txn: anytype, a: Allocator, prefix: []const u8, high: u32, rows: *const @import("../encoding/roaring.zig").RoaringBitmap, result: *@import("../encoding/roaring.zig").RoaringBitmap, cancellation: ?CancellationToken) !bool {
    return selectPhysicalBlockBounded(txn, a, prefix, high, rows, result, cancellation, null);
}
fn selectPhysicalBlockBounded(txn: anytype, a: Allocator, prefix: []const u8, high: u32, rows: *const @import("../encoding/roaring.zig").RoaringBitmap, result: *@import("../encoding/roaring.zig").RoaringBitmap, cancellation: ?CancellationToken, budget: ?*@import("ordinal_lookup.zig").WorkBudget) !bool {
    _ = a;
    const marker = txn.get(metaKey(meta_complete_physical)) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, marker, &.{1})) return error.InvalidSparsePhysicalMap;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var iterator = rows.iterator();
    while (iterator.next()) |first| {
        try checkSearchCancellation(cancellation);
        const base = first & ~@as(u32, 1023);
        const row = (@as(u64, high) << 32) | base;
        if (budget) |work| try work.takeBlock();
        const block_key = try PhysicalMaps.key(prefix, row >> 10);
        const found = try cursor.seekAtOrAfter(&block_key);
        const data: []const u8 = if (found) |entry| (if (std.mem.eql(u8, entry.key, &block_key)) entry.value else &.{}) else &.{};
        if (data.len % 6 != 0) return error.InvalidSparsePhysicalMap;
        var pos: usize = 0;
        var run_first: ?u32 = null;
        var run_last: u32 = 0;
        while (pos < data.len) : (pos += 6) {
            const low = std.mem.readInt(u16, data[pos..][0..2], .little);
            if (low >= 1024) return error.InvalidSparsePhysicalMap;
            if (!rows.contains(base | low)) continue;
            const ordinal = std.mem.readInt(u32, data[pos + 2 ..][0..4], .little);
            if (run_first != null and @as(u64, run_last) + 1 != ordinal) {
                try result.addRange(run_first.?, @as(u64, run_last) + 1);
                run_first = null;
            }
            if (run_first == null) run_first = ordinal;
            run_last = ordinal;
        }
        if (run_first) |start| try result.addRange(start, @as(u64, run_last) + 1);
        if (base == std.math.maxInt(u32) - 1023) break;
        iterator.seek(base + 1024);
    }
    return true;
}

fn publishDocMapLocators(a: Allocator, txn: anytype, segment: u64, data: []const u8) !void {
    var maps: PhysicalMaps = .{ .a = a };
    defer maps.deinit();
    const Context = struct {
        txn: @TypeOf(txn),
        segment: u64,
        data: []const u8,
        maps: *PhysicalMaps,
        fn visit(self: *@This(), entry: DocMapLookup) !bool {
            if (entry.doc_id.len >= 256) return false;
            const epoch = try segmentIncarnation(self.txn, self.segment, try immutableVersion(self.data), entry.doc_num, true);
            if (epoch != try currentIncarnation(self.txn, entry.doc_num)) return false;
            var tombstone: [16]u8 = undefined;
            const deleted = self.txn.get(docTombstoneKey(&tombstone, entry.doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (deleted != null) return false;
            try self.maps.update(self.txn, entry.doc_id, std.math.cast(u32, entry.doc_num) orelse return error.DocNumOverflow);
            var bytes: [36]u8 = undefined;
            std.mem.writeInt(u64, bytes[0..8], entry.doc_num, .little);
            std.mem.writeInt(u64, bytes[8..16], self.segment, .little);
            std.mem.writeInt(u64, bytes[16..24], @intFromPtr(entry.fwd_data.ptr) - @intFromPtr(self.data.ptr), .little);
            std.mem.writeInt(u32, bytes[24..28], @intCast(entry.fwd_data.len), .little);
            std.mem.writeInt(u64, bytes[28..36], epoch, .little);
            var key: [256]u8 = undefined;
            try self.txn.put(locatorKey(&key, entry.doc_id), &bytes);
            return false;
        }
    };
    var context: Context = .{ .txn = txn, .segment = segment, .data = data, .maps = &maps };
    _ = try forEachDocMapEntry(data, &context, Context.visit);
    try maps.flush(txn);
}
// A published generation retains its intent until every locator is refreshed.
// During that gap, point-read only these in-flight roots and native ordinals;
// never consult unpublished staging or scan document-map payloads.
fn pendingForwardBytes(txn: anytype, locator: DocMapLocator, doc_id: []const u8) !?[]const u8 {
    var cursor = try txn.openCursor();
    defer cursor.close();
    var next = try cursor.seekAtOrAfter(&.{0x12});
    while (next) |intent| {
        if (intent.key.len != 9 or intent.key[0] != 0x12) break;
        const id = std.mem.readInt(u64, intent.key[1..9], .big);
        var key: [16]u8 = undefined;
        const root = txn.get(docMapSegmentKey(&key, id)) catch |err| switch (err) {
            error.NotFound => {
                next = try cursor.next();
                continue;
            },
            else => return err,
        };
        if (pagedDocMap(root)) {
            const data = txn.get(&docMapEntryKey(id, locator.num)) catch |err| switch (err) {
                error.NotFound => {
                    next = try cursor.next();
                    continue;
                },
                else => return err,
            };
            if (data.len < 4) return error.InvalidSparseDocMapSegment;
            const len = std.mem.readInt(u32, data[0..4], .little);
            if (len > data.len - 4) return error.InvalidSparseDocMapSegment;
            if (std.mem.eql(u8, data[4..][0..len], doc_id) and
                try segmentIncarnation(txn, id, SEGMENT_FORMAT_VERSION, locator.num, true) == locator.epoch)
            {
                const forward = data[4 + len ..];
                if (try decodeFwdDocNum(forward) != locator.num) return error.InvalidSparseDocMapSegment;
                return forward;
            }
        }
        next = try cursor.next();
    }
    return null;
}
fn locatedForwardBytes(txn: anytype, doc_id: []const u8) !?[]const u8 {
    const locator = (try readLocator(txn, doc_id)) orelse return null;
    var key: [16]u8 = undefined;
    const root = txn.get(docMapSegmentKey(&key, locator.segment)) catch |err| switch (err) {
        error.NotFound => return pendingForwardBytes(txn, locator, doc_id),
        else => return err,
    };
    const paged = pagedDocMap(root);
    const data = if (paged) try txn.get(&docMapEntryKey(locator.segment, locator.num)) else root;
    const end = std.math.add(u64, locator.offset, locator.length) catch return error.InvalidSparseDocMapSegment;
    if (locator.offset < doc_id.len or end > data.len) return error.InvalidSparseDocMapSegment;
    const offset: usize = @intCast(locator.offset);
    if (!std.mem.eql(u8, data[offset - doc_id.len .. offset], doc_id)) return error.InvalidSparseDocMapSegment;
    const forward = data[offset..@intCast(end)];
    if (try decodeFwdDocNum(forward) != locator.num) return error.InvalidSparseDocMapSegment;
    return forward;
}

fn collectPostingStreams(a: Allocator, txn: anytype, query: *const SparseVector, cancellation: ?CancellationToken, byte_budget: usize, page_reader: *PageReader, selected: ?*const @import("../encoding/roaring.zig").RoaringBitmap) !?[]daat.Stream {
    var streams: std.ArrayList(daat.Stream) = .empty;
    defer streams.deinit(a);
    const max_streams = byte_budget / (@sizeOf(daat.Stream) + 64); // navigation budget is independent of archive bytes
    var retained_bytes: usize = 0;
    var cursor = try txn.openCursor();
    defer cursor.close();
    if (try posting_pages.routed(txn)) {
        var roots = try txn.openCursor();
        defer roots.close();
        const bucketed = try posting_pages.bucketed(txn);
        var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer seen.deinit(a);
        var nodes: [8 * 17]u32 = undefined;
        var node_count: usize = 0;
        const narrow = bucketed and selected != null and selected.?.keys.items.len <= 8;
        if (narrow) for (selected.?.keys.items) |bucket| {
            for (0..17) |depth| {
                const node = (@as(u32, @intCast(depth)) << 16) | (@as(u32, bucket) >> @as(u5, @intCast(16 - depth)));
                if (std.mem.indexOfScalar(u32, nodes[0..node_count], node) != null) continue;
                nodes[node_count] = node;
                node_count += 1;
            }
        };
        for (query.indices, query.values, 0..) |term, weight, query_order| {
            seen.clearRetainingCapacity();
            for (0..if (narrow) node_count else @as(usize, 1)) |node_index| {
                const primary = posting_pages.routeKey(term, 0);
                const secondary = posting_pages.bucketKey(term, if (narrow) nodes[node_index] else 0, 0);
                const prefix: []const u8 = if (narrow) secondary[0..8] else primary[0..5];
                var next_route = try cursor.seekAtOrAfter(prefix);
                while (next_route) |route| {
                    try checkSearchCancellation(cancellation);
                    if (route.key.len != prefix.len + 8 or !std.mem.eql(u8, route.key[0..prefix.len], prefix)) break;
                    const id = std.mem.readInt(u64, route.key[prefix.len..][0..8], .big);
                    if (seen.contains(id)) {
                        next_route = try cursor.next();
                        continue;
                    }
                    if (route.value.len == 8 or route.value.len == 9) {
                        if (route.value.len == 9) {
                            if (route.value[8] != 1) return error.InvalidSparseSegment;
                            var root_key: [16]u8 = undefined;
                            const lookup = segmentKey(&root_key, id);
                            const active = try roots.seekAtOrAfter(lookup);
                            if (active == null or !std.mem.eql(u8, active.?.key, lookup)) {
                                next_route = try cursor.next();
                                continue;
                            }
                        }
                        const lower = std.mem.readInt(u32, route.value[0..4], .little);
                        const upper = std.mem.readInt(u32, route.value[4..8], .little);
                        if (lower > upper) return error.InvalidSparseSegment;
                        if (selected) |bitmap| if (bitmap.rangeCardinality(lower, @as(u64, upper) + 1) == 0) {
                            next_route = try cursor.next();
                            continue;
                        };
                        if (streams.items.len == max_streams) return null;
                        try streams.append(a, .{ .weight = weight, .query_order = query_order, .segment = id, .version = SEGMENT_FORMAT_VERSION, .term = term, .reader = page_reader.interface(), .allocator = a, .ordinal_range = .{ .first = lower, .last = upper } });
                    } else {
                        if (route.value.len != 0) return error.InvalidSparseSegment;
                        var root_key: [16]u8 = undefined;
                        const lookup = segmentKey(&root_key, id);
                        const entry = (try roots.seekAtOrAfter(lookup)) orelse return error.InvalidSparseSegment;
                        if (!std.mem.eql(u8, entry.key, lookup)) return error.InvalidSparseSegment;
                        const root = entry.value;
                        const version = try immutableVersion(root);
                        if (streams.items.len == max_streams) return null;
                        if (posting_pages.paged(root)) {
                            try streams.append(a, .{ .weight = weight, .query_order = query_order, .segment = id, .version = version, .term = term, .reader = page_reader.interface(), .allocator = a });
                        } else {
                            if (root.len > byte_budget - retained_bytes) return null;
                            retained_bytes += root.len;
                            const retained = try txn.get(lookup);
                            const payload = (try segmentTermPayload(retained, term)) orelse return error.InvalidSparseSegment;
                            try streams.append(a, .{ .weight = weight, .query_order = query_order, .payload = payload, .segment = id, .version = version });
                        }
                    }
                    if (narrow) try seen.put(a, id, {});
                    next_route = try cursor.next();
                }
            }
        }
        // Preserve canonical segment-major f32 accumulation order even though
        // discovery is term-major. Query order also preserves duplicate terms.
        std.mem.sort(daat.Stream, streams.items, {}, struct {
            fn less(_: void, left: daat.Stream, right: daat.Stream) bool {
                if (left.segment.? != right.segment.?) return left.segment.? < right.segment.?;
                return left.query_order < right.query_order;
            }
        }.less);
    } else {
        var next = try cursor.seekAtOrAfter(taggedPrefix(key_segment));
        while (next) |entry| {
            try checkSearchCancellation(cancellation);
            if (entry.key.len == 0 or entry.key[0] != key_segment) break;
            const id = SparseIndex.segmentIdFromKey(entry.key) orelse return error.InvalidSparseSegment;
            const version = try immutableVersion(entry.value);
            if (selected) |bitmap| if (try posting_pages.ordinalBounds(entry.value)) |bounds| {
                if (bitmap.rangeCardinality(bounds[0], @as(u64, bounds[1]) + 1) == 0) {
                    next = try cursor.next();
                    continue;
                }
            };
            var retained: ?[]const u8 = null;
            for (query.indices, query.values) |term, weight| {
                if (posting_pages.paged(entry.value)) {
                    if (!try posting_pages.hasTermTxn(txn, id, entry.value, term)) continue;
                    if (streams.items.len == max_streams) return null;
                    try streams.append(a, .{ .weight = weight, .segment = id, .version = version, .term = term, .reader = page_reader.interface(), .allocator = a });
                } else if (try segmentTermPayload(entry.value, term) != null) {
                    if (streams.items.len == max_streams) return null;
                    // Retain only matching segments. Cursor entries may be recycled;
                    // transaction point reads remain pinned until query completion.
                    if (retained == null) {
                        if (entry.value.len > byte_budget - retained_bytes) return null;
                        retained = try txn.get(entry.key);
                        retained_bytes += retained.?.len;
                    }
                    const payload = (try segmentTermPayload(retained.?, term)) orelse return error.InvalidSparseSegment;
                    try streams.append(a, .{ .weight = weight, .payload = payload, .segment = id, .version = version });
                }
            }
            next = try cursor.next();
        }
    }
    for (query.indices, query.values) |term, weight| {
        var key: [256]u8 = undefined;
        const bytes = txn.get(invMetaKey(&key, term)) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        const metadata = decodeTermMeta(bytes);
        for (0..metadata.chunk_count) |i| {
            try checkSearchCancellation(cancellation);
            if (streams.items.len == max_streams) return null;
            const chunk = txn.get(invChunkKey(&key, term, @intCast(i))) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            if (chunk.len > byte_budget - retained_bytes) return null;
            retained_bytes += chunk.len;
            try streams.append(a, .{ .weight = weight, .single = chunk });
        }
    }
    return try streams.toOwnedSlice(a);
}

fn completeLocatorMap(txn: anytype) !bool {
    const marker = txn.get(metaKey(meta_complete_locators)) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, marker, &.{1})) return error.InvalidSparseDocMapSegment;
    return true;
}

fn currentIncarnation(txn: anytype, doc_num: u64) !u64 {
    var key: [9]u8 = undefined;
    const raw = txn.get(docIncarnationKey(&key, doc_num)) catch |err| switch (err) {
        error.NotFound => return 0, // Existing v1 segments predate incarnations.
        else => return err,
    };
    if (raw.len != 8) return error.InvalidSparseSegment;
    return std.mem.readInt(u64, raw[0..8], .little);
}

fn advanceIncarnation(txn: anytype, doc_num: u64) !u64 {
    const next = std.math.add(u64, try currentIncarnation(txn, doc_num), 1) catch return error.InvalidSparseSegment;
    var key: [9]u8 = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, next, .little);
    try txn.put(docIncarnationKey(&key, doc_num), &raw);
    return next;
}

fn putSegmentIncarnation(txn: anytype, segment_id: u64, doc_num: u64, incarnation: u64, docmap: bool) !void {
    var key: [17]u8 = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, incarnation, .little);
    try txn.put(segmentIncarnationKey(&key, segment_id, doc_num, docmap), &raw);
}

fn segmentIncarnation(txn: anytype, segment_id: u64, version: u32, doc_num: u64, docmap: bool) !u64 {
    if (version == 1) return 0;
    if (version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseSegment;
    var key: [17]u8 = undefined;
    const raw = txn.get(segmentIncarnationKey(&key, segment_id, doc_num, docmap)) catch |err| switch (err) {
        error.NotFound => return error.InvalidSparseSegment,
        else => return err,
    };
    if (raw.len != 8) return error.InvalidSparseSegment;
    return std.mem.readInt(u64, raw[0..8], .little);
}

fn immutableVersion(data: []const u8) !u32 {
    if (data.len < segment_magic.len + 4) return error.InvalidSparseSegment;
    const version = std.mem.readInt(u32, data[segment_magic.len..][0..4], .little);
    if (version != 1 and version != SEGMENT_FORMAT_VERSION) return error.InvalidSparseSegment;
    return version;
}

/// Independent cursor leases release backend scratch as visibility advances.
/// Epochs are copied into the bounded cache; no point-get payload survives in
/// the parent query transaction, including when its allocator is an arena.
const VisibilityReader = struct {
    // Pin a bounded set of metadata prefixes. A prefix keeps its own cursor
    // and a 64-ordinal page, so interleaved segments never restart one another.
    // Once admitted lanes fill, other prefixes retain the point/gap fallback;
    // they cannot evict hot lanes or trigger page-cache thrashing.
    const Prefix = struct { tag: u8, segment: u64 };
    const Page = struct {
        cursor: ?backend_erased.Cursor = null,
        prefix: [9]u8 = @splat(0),
        prefix_len: u8 = 0,
        block: ?u64 = null,
        last_probe: ?u64 = null,
        present: u64 = 0,
        values: [64][8]u8 = undefined,
        fn get(self: *Page, key: []const u8) ![]const u8 {
            const doc = std.mem.readInt(u64, key[key.len - 8 ..][0..8], .big);
            const block = doc >> 6;
            if (self.block == null or self.block.? != block) {
                self.block = null; // Failed reads never leave a valid page.
                self.present = 0;
                var lower: [17]u8 = undefined;
                @memcpy(lower[0..self.prefix_len], self.prefix[0..self.prefix_len]);
                std.mem.writeInt(u64, lower[self.prefix_len..][0..8], block << 6, .big);
                var next = try self.cursor.?.seekAtOrAfter(lower[0 .. self.prefix_len + 8]);
                while (next) |entry| {
                    if (entry.key.len != self.prefix_len + 8 or !std.mem.eql(u8, entry.key[0..self.prefix_len], self.prefix[0..self.prefix_len])) break;
                    const ordinal = std.mem.readInt(u64, entry.key[self.prefix_len..][0..8], .big);
                    if (ordinal >> 6 != block) break;
                    if (entry.value.len != 8) return error.InvalidSparseSegment;
                    const position: u6 = @truncate(ordinal);
                    self.values[position] = entry.value[0..8].*;
                    self.present |= @as(u64, 1) << position;
                    next = try self.cursor.?.next();
                }
                self.block = block;
            }
            const position: u6 = @truncate(doc);
            if (self.present & (@as(u64, 1) << position) == 0) return error.NotFound;
            return &self.values[position];
        }
    };
    txn: ?*backend_erased.ReadTxn = null,
    pages: [32]Page = @splat(.{}),
    page_count: usize = 0,
    page_slots: std.AutoHashMapUnmanaged(Prefix, usize) = .empty,
    page_loads: usize = 0,
    cursors: [3]backend_erased.Cursor,
    entries: [3]?backend_erased.Entry = @splat(null),
    lower_keys: [3][17]u8 = undefined,
    lower_lengths: [3]u8 = @splat(0),
    positioned: [3]bool = @splat(false),
    fn init(txn: *backend_erased.ReadTxn) !@This() {
        var cursors: [3]backend_erased.Cursor = undefined;
        var count: usize = 0;
        errdefer for (cursors[0..count]) |*cursor| cursor.close();
        for (&cursors) |*cursor| {
            cursor.* = try txn.openCursor();
            count += 1;
        }
        return .{ .txn = txn, .cursors = cursors };
    }
    fn deinit(self: *@This()) void {
        for (&self.cursors) |*cursor| cursor.close();
        for (self.pages[0..self.page_count]) |*page| if (page.cursor) |*cursor| cursor.close();
        if (self.txn) |txn| self.page_slots.deinit(txn.allocator);
    }
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        const txn = self.txn orelse return self.getPoint(key);
        if (!((key.len == 9 and key[0] == key_doc_incarnation) or
            (key.len == 17 and (key[0] == key_segment_incarnation or key[0] == key_docmap_incarnation)))) return self.getPoint(key);
        const prefix: Prefix = .{ .tag = key[0], .segment = if (key.len == 17) std.mem.readInt(u64, key[1..9], .big) else 0 };
        const slot = if (self.page_slots.get(prefix)) |index| index else {
            // Keep proven-absent intervals cheap; allocate a page only after
            // reaching positive metadata in this pinned transaction.
            const value = try self.getPoint(key);
            if (self.page_count == self.pages.len) return value;
            const index = self.page_count;
            try self.page_slots.put(txn.allocator, prefix, index);
            self.pages[index].last_probe = std.mem.readInt(u64, key[key.len - 8 ..][0..8], .big) >> 6;
            self.pages[index].prefix_len = @intCast(key.len - 8);
            @memcpy(self.pages[index].prefix[0 .. key.len - 8], key[0 .. key.len - 8]);
            self.page_count += 1;
            return value;
        };
        const page = &self.pages[slot];
        const block = std.mem.readInt(u64, key[key.len - 8 ..][0..8], .big) >> 6;
        if (page.cursor == null) {
            // Selective or widely scattered probes retain point reads. Admit
            // the scan page only after observing locality within one block.
            const value = try self.getPoint(key);
            if (page.last_probe == null or page.last_probe.? != block) {
                page.last_probe = block;
                return value;
            }
            page.cursor = try txn.openCursor();
        }
        if (page.block == null or page.block.? != block) self.page_loads += 1;
        return page.get(key);
    }
    fn getPoint(self: *@This(), key: []const u8) ![]const u8 {
        const lane: usize = if (key[0] == key_doc_tombstone) 0 else if (key[0] == key_doc_incarnation) 1 else 2;
        if (key.len > self.lower_keys[lane].len) return error.InvalidSparseSegment;
        // A lower-bound seek proves the entire gap before its returned key
        // absent. Reuse that borrowed result (including EOF) while requests
        // stay in the proven interval. Missing metadata families must not
        // restart a merged LSM cursor for every ascending posting ordinal.
        if (self.positioned[lane] and std.mem.order(u8, key, self.lower_keys[lane][0..self.lower_lengths[lane]]) != .lt) {
            if (self.entries[lane]) |entry| {
                switch (std.mem.order(u8, key, entry.key)) {
                    .eq => return entry.value,
                    .lt => return error.NotFound,
                    .gt => {},
                }
            } else return error.NotFound;
        }
        self.positioned[lane] = false;
        self.entries[lane] = try self.cursors[lane].seekAtOrAfter(key);
        @memcpy(self.lower_keys[lane][0..key.len], key);
        self.lower_lengths[lane] = @intCast(key.len);
        self.positioned[lane] = true;
        const entry = self.entries[lane] orelse return error.NotFound;
        if (!std.mem.eql(u8, entry.key, key)) return error.NotFound;
        return entry.value;
    }
};

/// Query-local and hard capped: selective queries point-read only candidate
/// incarnations, never load a segment-sized metadata blob. The caller's query
/// allocator accounts both tables; clearing preserves the fixed high-water.
const IncarnationCache = struct {
    current: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    segments: std.AutoHashMapUnmanaged([2]u64, u64) = .empty,
    const max_entries = 4096;
    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.current.deinit(alloc);
        self.segments.deinit(alloc);
    }
    fn matches(self: *@This(), alloc: Allocator, txn: anytype, segment_id: u64, version: u32, doc_num: u64) !bool {
        const current = self.current.get(doc_num) orelse value: {
            const value = try currentIncarnation(txn, doc_num);
            if (self.current.count() == max_entries) self.current.clearRetainingCapacity();
            try self.current.put(alloc, doc_num, value);
            break :value value;
        };
        const key: [2]u64 = .{ segment_id, doc_num };
        const source = self.segments.get(key) orelse value: {
            const value = try segmentIncarnation(txn, segment_id, version, doc_num, false);
            if (self.segments.count() == max_entries) self.segments.clearRetainingCapacity();
            try self.segments.put(alloc, key, value);
            break :value value;
        };
        if (source > current) return error.InvalidSparseSegment;
        return source == current;
    }
};

const DiskProofs = @import("compaction_proofs.zig").Proofs;
/// Source epoch keys are already ordered by native ordinal. A separate cursor
/// resolves current visibility without retaining point-get results in the txn.
fn captureDiskProofs(a: Allocator, txn: anytype, id: u64, docmap: bool, options: @import("../spill_sort.zig").Options) !DiskProofs {
    var result = try DiskProofs.init(a, options);
    errdefer result.deinit();
    var cursor = try txn.openCursor();
    defer cursor.close();
    var current = try VisibilityReader.init(txn);
    defer current.deinit();
    var key: [17]u8 = undefined;
    const prefix = segmentIncarnationKey(&key, id, 0, docmap)[0..9];
    var next = try cursor.seekAtOrAfter(prefix);
    while (next) |entry| {
        if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..9], prefix)) break;
        if (entry.value.len != 8) return error.InvalidSparseSegment;
        if (options.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        const doc = std.math.cast(u32, std.mem.readInt(u64, entry.key[9..17], .big)) orelse return error.DocNumOverflow;
        const epoch = std.mem.readInt(u64, entry.value[0..8], .little);
        var current_key: [9]u8 = undefined;
        const now = try currentIncarnation(&current, doc);
        if (epoch > now) return error.InvalidSparseSegment;
        const deleted_key = docTombstoneKey(&current_key, doc);
        const deleted = if (current.get(deleted_key)) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => return err,
        };
        try result.append(.{ .doc_num = doc, .epoch = epoch, .live = epoch == now and !deleted });
        next = try cursor.next();
    }
    try result.seal();
    return result;
}
const CapturedIncarnation = struct {
    doc_num: u32,
    epoch: u64,
    live: bool,
    doc_id: ?[]u8 = null,
};

fn findCaptured(entries: []const CapturedIncarnation, doc_num: u32) ?CapturedIncarnation {
    var lo: usize = 0;
    var hi = entries.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (entries[mid].doc_num < doc_num) lo = mid + 1 else hi = mid;
    }
    return if (lo < entries.len and entries[lo].doc_num == doc_num) entries[lo] else null;
}

/// Allocation-free upper bound. Posting buffers are charged per occurrence;
/// incarnation metadata uses a bounded cache (collisions only overestimate).
/// Reverse values are borrowed from the same snapshot used by actual capture.
fn incarnationWorkingBytes(txn: anytype, data: []const u8, docmap: bool, id: ?u64, streaming: bool) !u64 {
    const Context = struct {
        txn: @TypeOf(txn),
        bytes: u64,
        streaming: bool,
        seen: [4096]?u64 = @splat(null),
        fn documentNumber(self: *@This(), doc_num: u64) !void {
            const slot = doc_num % self.seen.len;
            if (self.seen[slot]) |previous| if (previous == doc_num) return;
            self.seen[slot] = doc_num;
            var reverse: [16]u8 = undefined;
            const doc_id = self.txn.get(revKey(&reverse, doc_num)) catch |err| switch (err) {
                error.NotFound => &.{},
                else => return err,
            };
            const ids = std.math.mul(u64, doc_id.len, 4) catch return error.ResourceBudgetExceeded;
            self.bytes = std.math.add(u64, self.bytes, ids) catch return error.ResourceBudgetExceeded;
            self.bytes = std.math.add(u64, self.bytes, 512) catch return error.ResourceBudgetExceeded;
        }
        fn document(self: *@This(), entry: DocMapLookup) !bool {
            try self.documentNumber(entry.doc_num);
            return false;
        }
        fn posting(self: *@This(), term: u32, payload: []const u8) !void {
            _ = term;
            var pos: usize = 0;
            while (pos < payload.len) {
                if (payload.len - pos < 8) return error.InvalidSparseSegment;
                const length = std.mem.readInt(u32, payload[pos..][0..4], .little);
                const range_length = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little);
                pos += 8;
                const end = std.math.add(usize, pos, length) catch return error.InvalidSparseSegment;
                const next = std.math.add(usize, end, range_length) catch return error.InvalidSparseSegment;
                if (next > payload.len or length < 13) return error.InvalidSparseSegment;
                const chunk = payload[pos..end];
                if (chunk[0] != CHUNK_FORMAT_VERSION) return error.InvalidChunk;
                const header: usize = 13;
                if (chunk.len < header) return error.InvalidChunk;
                const count = std.mem.readInt(u32, chunk[1..5], .little);
                const encoded = std.math.mul(usize, count, 5) catch return error.InvalidChunk;
                if (encoded != chunk.len - header) return error.InvalidChunk;
                var doc_num: u32 = 0;
                for (0..count) |i| {
                    const delta = std.mem.readInt(u32, chunk[header + i * 4 ..][0..4], .little);
                    doc_num = std.math.add(u32, doc_num, delta) catch return error.InvalidChunk;
                    try self.documentNumber(doc_num);
                }
                const posting_bytes = std.math.mul(u64, count, 2 * @sizeOf(BulkPosting) + @sizeOf(u32) + @sizeOf(f32)) catch return error.ResourceBudgetExceeded;
                if (!self.streaming) self.bytes = std.math.add(u64, self.bytes, posting_bytes) catch return error.ResourceBudgetExceeded;
                pos = next;
            }
        }
    };
    _ = try immutableVersion(data);
    var context: Context = .{ .txn = txn, .streaming = streaming, .bytes = std.math.mul(u64, data.len, 4) catch return error.ResourceBudgetExceeded };
    if (docmap) {
        _ = try forEachStoredDocMapEntry(txn, id.?, data, &context, Context.document);
    } else if (posting_pages.paged(data)) {
        context.bytes = if (streaming) try std.math.add(u64, context.bytes, posting_pages.max_block_bytes) else std.math.mul(u64, try posting_pages.materializedSize(data), 4) catch return error.ResourceBudgetExceeded;
        try posting_pages.forEachBlock(txn, id.?, null, &context, Context.posting);
    } else try forEachSegmentTermPayload(data, &context, Context.posting);
    return context.bytes;
}

fn captureIncarnations(alloc: Allocator, txn: anytype, id: u64, data: []const u8, docmap: bool) ![]CapturedIncarnation {
    const Context = struct {
        alloc: Allocator,
        txn: @TypeOf(txn),
        id: u64,
        version: u32,
        docmap: bool,
        entries: std.AutoHashMapUnmanaged(u32, CapturedIncarnation) = .empty,
        fn capture(self: *@This(), doc_num: u32) !void {
            if (self.entries.contains(doc_num)) return;
            const epoch = try segmentIncarnation(self.txn, self.id, self.version, doc_num, self.docmap);
            var tombstone: [16]u8 = undefined;
            const deleted = if (self.txn.get(docTombstoneKey(&tombstone, doc_num))) |_| true else |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
            const current = try currentIncarnation(self.txn, doc_num);
            if (epoch > current) return error.InvalidSparseSegment;
            const live = !deleted and epoch == current;
            var reverse: [16]u8 = undefined;
            const doc_id = if (live) try self.alloc.dupe(u8, self.txn.get(revKey(&reverse, doc_num)) catch |err| switch (err) {
                error.NotFound => return error.InvalidSparseSegment,
                else => return err,
            }) else null;
            errdefer if (doc_id) |bytes| self.alloc.free(bytes);
            try self.entries.put(self.alloc, doc_num, .{ .doc_num = doc_num, .epoch = epoch, .live = live, .doc_id = doc_id });
        }
        fn document(self: *@This(), entry: DocMapLookup) !bool {
            try self.capture(std.math.cast(u32, entry.doc_num) orelse return error.DocNumOverflow);
            return false;
        }
        fn posting(self: *@This(), term: u32, payload: []const u8) !void {
            _ = term;
            var pos: usize = 0;
            while (pos < payload.len) {
                if (payload.len - pos < 8) return error.InvalidSparseSegment;
                const length = std.mem.readInt(u32, payload[pos..][0..4], .little);
                const range_length = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little);
                pos += 8;
                const end = std.math.add(usize, pos, length) catch return error.InvalidSparseSegment;
                const next = std.math.add(usize, end, range_length) catch return error.InvalidSparseSegment;
                if (next > payload.len) return error.InvalidSparseSegment;
                const decoded = try decodeChunk(self.alloc, payload[pos..end]);
                defer self.alloc.free(decoded.doc_nums);
                defer self.alloc.free(decoded.weights);
                for (decoded.doc_nums) |doc_num| try self.capture(doc_num);
                pos = next;
            }
        }
    };
    var context: Context = .{ .alloc = alloc, .txn = txn, .id = id, .version = try immutableVersion(data), .docmap = docmap };
    defer context.entries.deinit(alloc);
    errdefer {
        var it = context.entries.valueIterator();
        while (it.next()) |entry| if (entry.doc_id) |bytes| alloc.free(bytes);
    }
    if (docmap) {
        _ = try forEachStoredDocMapEntry(txn, id, data, &context, Context.document);
    } else if (posting_pages.paged(data)) {
        try posting_pages.forEachBlock(txn, id, null, &context, Context.posting);
    } else try forEachSegmentTermPayload(data, &context, Context.posting);
    const result = try alloc.alloc(CapturedIncarnation, context.entries.count());
    var it = context.entries.valueIterator();
    for (result) |*entry| entry.* = it.next().?.*;
    std.mem.sort(CapturedIncarnation, result, {}, struct {
        fn less(_: void, a: CapturedIncarnation, b: CapturedIncarnation) bool {
            return a.doc_num < b.doc_num;
        }
    }.less);
    return result;
}

fn forEachCurrentDocMapEntry(txn: anytype, key: []const u8, data: []const u8, context: anytype, comptime func: fn (@TypeOf(context), DocMapLookup) anyerror!bool) !bool {
    if (key.len != 9 or key[0] != key_docmap_segment) return error.InvalidSparseDocMapSegment;
    const Context = struct {
        txn: @TypeOf(txn),
        segment_id: u64,
        version: u32,
        caller: @TypeOf(context),
        fn visit(self: *@This(), entry: DocMapLookup) !bool {
            const epoch = try segmentIncarnation(self.txn, self.segment_id, self.version, entry.doc_num, true);
            const current = try currentIncarnation(self.txn, entry.doc_num);
            if (epoch > current) return error.InvalidSparseSegment;
            if (epoch != current) return false;
            return func(self.caller, entry);
        }
    };
    var filtered: Context = .{ .txn = txn, .segment_id = std.mem.readInt(u64, key[1..9], .big), .version = try immutableVersion(data), .caller = context };
    return forEachStoredDocMapEntry(txn, filtered.segment_id, data, &filtered, Context.visit);
}

fn metaKey(kind: u8) *const [2]u8 {
    return switch (kind) {
        meta_next_doc_num => &.{ key_meta, meta_next_doc_num },
        meta_doc_count => &.{ key_meta, meta_doc_count },
        meta_term_count => &.{ key_meta, meta_term_count },
        meta_next_segment_id => &.{ key_meta, meta_next_segment_id },
        meta_complete_locators => &.{ key_meta, meta_complete_locators },
        meta_complete_physical => &.{ key_meta, meta_complete_physical },
        else => unreachable,
    };
}

fn termCatalogKey(buf: []u8, term_id: u32) []const u8 {
    std.debug.assert(buf.len >= 5);
    buf[0] = key_term_catalog;
    std.mem.writeInt(u32, buf[1..][0..4], term_id, .big);
    return buf[0..5];
}

fn parseTermRangeKey(key: []const u8) ?u32 {
    if (key.len != 6 or key[0] != key_inv or key[5] != inv_kind_term_range) return null;
    return std.mem.readInt(u32, key[1..][0..4], .big);
}

// ============================================================================
// SparseIndex
// ============================================================================

pub const SparseIndexOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    chunk_size: u32 = 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    backend: SparseBackend = .lsm,
    lsm_storage: ?lsm_backend.Storage = null,
    lsm_cache: ?*lsm_backend.Cache = null,
    lsm_options: lsm_backend.Options = .{ .flush_threshold = 1 },
    lsm_root_generation: u64 = 0,
};

pub const SparseBackend = enum {
    mem,
    lsm_memory,
    lsm,
};

pub const SparseIndex = struct {
    alloc: Allocator,
    store: backend_erased.Store,
    owner: StoreOwner,
    dbi: void = {},
    chunk_size: u32,
    next_doc_num: u64,
    next_segment_id: u64,
    doc_count: u64,
    term_count: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    write_profile: WriteProfile = .{},

    const StoreOwner = union(enum) {
        none,
        mem: *mem_backend.Backend,
        lsm: lsm_backend.BackendHandle,

        fn close(self: *StoreOwner, alloc: Allocator) void {
            switch (self.*) {
                .none => {},
                .mem => |backend| {
                    backend.close();
                    alloc.destroy(backend);
                },
                .lsm => |*handle| handle.close(),
            }
            self.* = .none;
        }

        fn abandonAfterCrash(self: *StoreOwner, alloc: Allocator) void {
            switch (self.*) {
                .none => {},
                .mem => |backend| {
                    backend.close();
                    alloc.destroy(backend);
                },
                .lsm => |*handle| handle.abandonAfterCrash(),
            }
            self.* = .none;
        }

        pub fn sync(self: *StoreOwner, force: bool) !void {
            switch (self.*) {
                .none, .mem => {},
                .lsm => |*handle| try handle.backend.sync(force),
            }
        }

        fn checkpointLsmWalAfterDurableBoundary(self: *StoreOwner) !void {
            switch (self.*) {
                .none, .mem => {},
                .lsm => |*handle| try handle.backend.checkpointWalAfterDurableBoundary(),
            }
        }

        fn pinNativeCheckpoint(self: *StoreOwner) !lsm_backend.Backend.NativeCheckpoint {
            return switch (self.*) {
                .lsm => |*handle| try handle.backend.pinNativeCheckpoint(),
                .none, .mem => error.Unsupported,
            };
        }
    };

    const OpenedStore = struct {
        store: backend_erased.Store,
        owner: StoreOwner,
    };

    fn resolvedLsmOptions(opts: SparseIndexOptions, memory_only: bool) lsm_backend.Options {
        var lsm_options = opts.lsm_options;
        lsm_options.backend.durability = if (memory_only or opts.no_sync) .none else lsm_options.backend.durability;
        if (!memory_only) lsm_options.storage = opts.lsm_storage orelse lsm_options.storage;
        lsm_options.cache = opts.lsm_cache orelse lsm_options.cache;
        if (opts.lsm_root_generation != 0 and lsm_options.root_generation == 0) {
            lsm_options.root_generation = opts.lsm_root_generation;
        }
        return lsm_options;
    }

    fn openStore(alloc: Allocator, path: [*:0]const u8, opts: SparseIndexOptions) !OpenedStore {
        switch (opts.backend) {
            .mem => {
                const backend = try alloc.create(mem_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = mem_backend.Backend.init(alloc, .{});
                errdefer backend.close();

                var runtime = try backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .mem = backend } };
            },
            .lsm_memory => {
                var handle = try lsm_backend.BackendHandle.init(alloc, resolvedLsmOptions(opts, true));
                errdefer handle.close();

                var runtime = try handle.backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .lsm = handle } };
            },
            .lsm => {
                var handle = try lsm_backend.BackendHandle.open(alloc, std.mem.span(path), resolvedLsmOptions(opts, false));
                errdefer handle.close();

                var runtime = try handle.backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{ .store = runtime, .owner = .{ .lsm = handle } };
            },
        }
    }

    pub fn beginReadTxn(self: *SparseIndex) !backend_erased.ReadTxn {
        return try self.store.beginRead();
    }

    fn beginWriteTxn(self: *SparseIndex) !backend_erased.WriteTxn {
        return try self.store.beginWrite();
    }

    fn beginBatchTxn(self: *SparseIndex, options: backend_types.BatchOptions) !backend_erased.Batch {
        return try self.store.beginBatchWithOptions(options);
    }

    pub fn backendStore(self: *SparseIndex) *backend_erased.Store {
        return &self.store;
    }

    pub fn attachResourceManager(self: *SparseIndex, manager: *resource_manager_mod.ResourceManager) void {
        self.resource_manager = manager;
    }

    pub fn getWriteProfile(self: *SparseIndex) WriteProfile {
        return self.write_profile;
    }

    pub fn beginBulkIngestSession(self: *SparseIndex) !void {
        try self.store.beginBulkIngestSession();
    }

    pub fn finishBulkIngestSessionWithOptions(self: *SparseIndex, options: backend_types.BulkIngestFinishOptions) !void {
        try self.store.finishBulkIngestSessionWithOptions(options);
    }

    pub fn checkpointLsmWalAfterDurableBoundary(self: *SparseIndex) !void {
        try self.owner.checkpointLsmWalAfterDurableBoundary();
    }

    pub fn pinNativeCheckpoint(self: *SparseIndex) !lsm_backend.Backend.NativeCheckpoint {
        return try self.owner.pinNativeCheckpoint();
    }

    pub fn abortBulkIngestSession(self: *SparseIndex) void {
        self.store.abortBulkIngestSession();
    }

    pub fn open(alloc: Allocator, path: [*:0]const u8, opts: SparseIndexOptions) !SparseIndex {
        var opened = try openStore(alloc, path, opts);
        errdefer {
            opened.store.deinit();
            opened.owner.close(alloc);
        }

        var next_doc_num: u64 = 0;
        var next_segment_id: u64 = 1;
        var doc_count: u64 = 0;
        var term_count: u64 = 0;
        if (opts.lsm_options.backend.read_only) {
            var txn = try opened.store.beginRead();
            defer txn.abort();
            const ndn_data = txn.get(metaKey(meta_next_doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (ndn_data) |d| {
                next_doc_num = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_next_segment_id)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                next_segment_id = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_doc_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                doc_count = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_term_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                term_count = std.mem.readInt(u64, d[0..8], .little);
            }
        } else {
            var txn = try opened.store.beginWrite();
            errdefer txn.abort();

            const ndn_data = txn.get(metaKey(meta_next_doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (ndn_data) |d| {
                next_doc_num = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_next_segment_id)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                next_segment_id = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_doc_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                doc_count = std.mem.readInt(u64, d[0..8], .little);
            }
            if (txn.get(metaKey(meta_term_count)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) |d| {
                term_count = std.mem.readInt(u64, d[0..8], .little);
            }

            try txn.commit();
        }

        var result: SparseIndex = .{
            .alloc = alloc,
            .store = opened.store,
            .owner = opened.owner,
            .chunk_size = opts.chunk_size,
            .next_doc_num = next_doc_num,
            .next_segment_id = next_segment_id,
            .doc_count = doc_count,
            .term_count = term_count,
        };
        if (!opts.lsm_options.backend.read_only) try result.recoverCompactionIntents();
        return result;
    }

    pub fn close(self: *SparseIndex) void {
        self.store.deinit();
        self.owner.close(self.alloc);
        self.* = undefined;
    }

    pub fn abandonAfterCrash(self: *SparseIndex) void {
        self.store.deinit();
        self.owner.abandonAfterCrash(self.alloc);
        self.* = undefined;
    }

    pub fn sync(self: *SparseIndex, force: bool) !void {
        try self.owner.sync(force);
    }

    pub fn syncReplayState(self: *SparseIndex) !void {
        try self.owner.sync(false);
    }

    pub const Stats = struct {
        doc_count: u64 = 0,
        term_count: u64 = 0,
    };

    pub fn stats(self: *SparseIndex) Stats {
        if (self.doc_count == 0 and self.term_count == 0) {
            if (self.loadPersistedStats() catch null) |persisted| {
                if (persisted.doc_count != 0 or persisted.term_count != 0) {
                    self.doc_count = persisted.doc_count;
                    self.term_count = persisted.term_count;
                    return persisted;
                }
            }
        }
        return .{
            .doc_count = self.doc_count,
            .term_count = self.term_count,
        };
    }

    fn loadPersistedStats(self: *SparseIndex) !Stats {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var out = Stats{};
        if (txn.get(metaKey(meta_doc_count)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            if (raw.len >= 8) out.doc_count = std.mem.readInt(u64, raw[0..8], .little);
        }
        if (txn.get(metaKey(meta_term_count)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            if (raw.len >= 8) out.term_count = std.mem.readInt(u64, raw[0..8], .little);
        }
        return out;
    }

    pub fn scanStats(self: *SparseIndex) !Stats {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        var out = Stats{};
        var maybe_entry = try cur.first();
        while (maybe_entry) |entry| {
            if (entry.key.len > 0 and entry.key[0] == key_rev) out.doc_count += 1;
            if (entry.key.len > 0 and entry.key[0] == key_term_catalog) out.term_count += 1;
            maybe_entry = try cur.next();
        }
        return out;
    }

    pub fn refreshPersistedStatsFromScan(self: *SparseIndex) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        const scanned = try scanStatsInTxn(&txn, self.dbi);
        self.doc_count = scanned.doc_count;
        self.term_count = scanned.term_count;
        try persistSparseCounters(self, &txn);
        try txn.commit();
    }

    pub fn persistBackfillDocCount(self: *SparseIndex, doc_count: u64) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        self.doc_count = doc_count;
        try persistSparseCounters(self, &txn);
        try txn.commit();
    }

    /// Batch insert and delete sparse vectors.
    pub fn batch(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8) !void {
        return try self.batchWithOptions(writes, deletes, .{});
    }

    pub fn batchWithOptions(self: *SparseIndex, input: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        var normalized = std.heap.ArenaAllocator.init(self.alloc);
        defer normalized.deinit();
        const working = try canonicalWorkingBytes(input);
        var reservation: ?resource_manager_mod.Reservation = if (self.resource_manager != null and working != 0) try self.resource_manager.?.reserve(.sparse_apply_working_set, working) else null;
        defer if (reservation) |*held| held.release();
        const writes = try canonicalWrites(normalized.allocator(), self.alloc, input);
        self.write_profile.batch_calls += 1;
        self.write_profile.writes += writes.len;
        self.write_profile.deletes += deletes.len;
        if (options.prefer_bulk_build and writes.len > 0) {
            if (try self.tryBulkAppend(writes, deletes, options)) return;
            self.write_profile.bulk_append_fallbacks += 1;
        }
        try self.batchIncrementalWithOptions(writes, deletes, options);
    }

    fn batchIncrementalWithOptions(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        self.write_profile.incremental_calls += 1;
        if (options.backend_batch_options.mode == .bulk_ingest) {
            var txn = try self.beginBatchTxn(options.backend_batch_options);
            try self.batchIncrementalTxn(&txn, writes, deletes, options);
            return;
        }

        var txn = try self.beginWriteTxn();
        try self.batchIncrementalTxn(&txn, writes, deletes, options);
    }

    fn batchIncrementalTxn(self: *SparseIndex, txn: anytype, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !void {
        errdefer txn.abort();
        const prev_next_doc_num = self.next_doc_num;
        const prev_next_segment_id = self.next_segment_id;
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.next_doc_num = prev_next_doc_num;
            self.next_segment_id = prev_next_segment_id;
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }
        var scratch_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch_arena.deinit();
        const scratch = scratch_arena.allocator();
        var touched_terms = std.AutoHashMapUnmanaged(u32, void).empty;
        defer touched_terms.deinit(self.alloc);
        const touched_terms_ptr = if (options.defer_term_range_updates) &touched_terms else null;
        var physical_maps: PhysicalMaps = .{ .a = self.alloc };
        defer physical_maps.deinit();

        // Process deletes
        var phase_start_ns = nowNs();
        for (deletes) |doc_id| {
            const effect = try self.processDelete(scratch, txn, doc_id, touched_terms_ptr);
            self.applyDeleteEffect(effect);
            try physical_maps.update(txn, doc_id, null);
            _ = scratch_arena.reset(.retain_capacity);
        }
        self.write_profile.incremental_delete_ns += elapsedSince(phase_start_ns);

        // Process inserts
        phase_start_ns = nowNs();
        for (writes) |w| {
            try self.processInsert(scratch, txn, w.doc_id, w.vec, w.doc_num, touched_terms_ptr);
            try physical_maps.update(txn, w.doc_id, try self.docNumForDocIdTxn(txn, w.doc_id));
            _ = scratch_arena.reset(.retain_capacity);
        }
        try physical_maps.flush(txn);
        self.write_profile.incremental_insert_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        if (touched_terms_ptr) |map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try self.refreshTermRangeMeta(scratch, txn, entry.key_ptr.*);
                _ = scratch_arena.reset(.retain_capacity);
            }
        }
        self.write_profile.incremental_refresh_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        try persistSparseCounters(self, txn);

        try txn.commit();
        self.write_profile.incremental_commit_ns += elapsedSince(phase_start_ns);
    }

    fn estimateSparseBulkWorkingBytes(writes: []const SparseWrite) u64 {
        var total: u64 = 0;
        for (writes) |write| {
            total +|= write.doc_id.len;
            total +|= @sizeOf(BulkDoc) + 128; // Incarnation counter and two bounded sidecar records.
            total +|= write.doc_id.len + 37; // Typed forward locator key/value.
            total +|= @as(u64, @intCast(write.vec.indices.len)) * (@sizeOf(BulkPosting) + @sizeOf(u32) + @sizeOf(f32));
        }
        return total;
    }

    fn tryReserveSparseBulkWorkingSet(self: *SparseIndex, writes: []const SparseWrite) !?resource_manager_mod.Reservation {
        const manager = self.resource_manager orelse return null;
        const estimated = estimateSparseBulkWorkingBytes(writes);
        return try manager.reserve(.sparse_apply_working_set, estimated);
    }

    pub const SegmentCompactionOptions = struct {
        min_segments: usize = 32,
        max_segments: usize = 128,
        scratch: ?@import("../spill_sort.zig").Options = null,
        background_publication: bool = false,
    };

    pub const SegmentCompactionSource = struct {
        id: u64,
        data: []u8,
        incarnations: []CapturedIncarnation,
        proofs: ?DiskProofs = null,
        fn captured(self: @This(), doc: u32, position: *usize) !?CapturedIncarnation {
            if (self.proofs) |proofs| return if (try proofs.findFrom(doc, position)) |entry| .{ .doc_num = doc, .epoch = entry.epoch, .live = entry.live } else null;
            return findCaptured(self.incarnations, doc);
        }

        pub fn deinit(self: @This(), alloc: Allocator) void {
            if (self.proofs) |proofs| {
                var owned = proofs;
                owned.deinit();
            }
            alloc.free(self.data);
            for (self.incarnations) |entry| if (entry.doc_id) |bytes| alloc.free(bytes);
            alloc.free(self.incarnations);
        }
    };

    pub const SegmentCompactionTask = struct {
        sources: []SegmentCompactionSource,
        docmaps: []SegmentCompactionSource,
        buffer_reservation: ?resource_manager_mod.Reservation = null,
        snapshot: ?backend_erased.ReadTxn = null,
        scratch: ?@import("../spill_sort.zig").Options = null,
        generation: ?u64 = null,
        // The maintenance owner pins the store lifecycle; this copy remains
        // stable if the IndexManager's sparse entry array moves during apply.
        store: ?backend_erased.Store = null,

        pub fn deinit(self: *SegmentCompactionTask, alloc: Allocator) void {
            if (self.snapshot) |*snapshot| snapshot.abort();
            if (self.generation) |id| if (self.store) |store| {
                var writer: CompactionWriter = .{ .alloc = alloc, .store = store };
                discardStaging(&writer, id) catch {};
            };
            if (self.buffer_reservation) |*reservation| reservation.release();
            for (self.sources) |source| source.deinit(alloc);
            alloc.free(self.sources);
            for (self.docmaps) |docmap| docmap.deinit(alloc);
            alloc.free(self.docmaps);
            self.* = undefined;
        }
    };

    pub const SegmentCompactionResult = struct {
        data: ?[]u8 = null,
        pages: ?*posting_pages.Builder.Run = null,
        directory_run: ?*posting_pages.Builder.Run = null,
        staged: bool = false,
        generation: ?u64 = null,
        docmap: ?[]u8 = null,
        map_sort: ?@import("../spill_sort.zig").Sorter = null,
        map_range: ?@import("../spill_sort.zig").Range = null,
        incarnations: []CapturedIncarnation = &.{},
        docmap_incarnations: []CapturedIncarnation = &.{},
        proofs: ?DiskProofs = null,
        source_segments: u64 = 0,
        postings: u64 = 0,

        pub fn deinit(self: *SegmentCompactionResult, alloc: Allocator) void {
            if (self.data) |data| alloc.free(data);
            if (self.pages) |pages| pages.deinit();
            if (self.directory_run) |run| run.deinit();
            if (self.proofs) |*proofs| proofs.deinit();
            if (self.docmap) |data| alloc.free(data);
            if (self.map_sort) |*sort| sort.deinit();
            alloc.free(self.incarnations);
            alloc.free(self.docmap_incarnations);
            self.* = undefined;
        }
    };

    fn segmentIdFromKey(key: []const u8) ?u64 {
        if (key.len != 9 or key[0] != key_segment) return null;
        return std.mem.readInt(u64, key[1..][0..8], .big);
    }

    pub fn segmentCount(self: *SparseIndex) !usize {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        return try self.segmentCountTxn(&txn);
    }

    fn segmentCountTxn(self: *SparseIndex, txn: anytype) !usize {
        _ = self;
        var count: usize = 0;
        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_segment) break;
            count += 1;
            maybe_entry = try cur.next();
        }
        return count;
    }

    fn reserveSparseSegmentCompactionBuffers(
        self: *SparseIndex,
        source_bytes: u64,
        source_segments: usize,
    ) !?resource_manager_mod.Reservation {
        const manager = self.resource_manager orelse return null;
        const output_bytes = source_bytes;
        const segment_overhead = std.math.mul(u64, @as(u64, @intCast(source_segments)), 1024) catch return error.ResourceBudgetExceeded;
        const source_and_output = std.math.add(u64, source_bytes, output_bytes) catch return error.ResourceBudgetExceeded;
        const reservation_bytes = std.math.add(u64, source_and_output, segment_overhead) catch return error.ResourceBudgetExceeded;
        return try manager.reserve(.sparse_apply_working_set, reservation_bytes);
    }

    pub fn beginSegmentCompactionTask(
        self: *SparseIndex,
        alloc: Allocator,
        options: SegmentCompactionOptions,
    ) !?SegmentCompactionTask {
        const min_segments = @max(@as(usize, 2), options.min_segments);
        const max_segments = @max(min_segments, options.max_segments);

        var txn = try self.beginReadTxn();
        var snapshot_transferred = false;
        defer if (!snapshot_transferred) txn.abort();

        // Preflight borrows immutable source bytes; reserve before any source
        // clone, hash table, decoded posting array or captured ID allocation.
        var preflight_bytes: u64 = 0;
        var counts: [2]usize = .{ 0, 0 };
        for ([_]u8{ key_segment, key_docmap_segment }, 0..) |tag, kind| {
            var preflight = try txn.openCursor();
            defer preflight.close();
            const prefix = [_]u8{tag};
            var entry_opt = try preflight.seekAtOrAfter(&prefix);
            while (entry_opt) |entry| {
                if (entry.key.len == 0 or entry.key[0] != tag) break;
                const bytes = if (options.scratch != null and try immutableVersion(entry.value) == SEGMENT_FORMAT_VERSION)
                    @as(u64, entry.value.len) * 4 + posting_pages.max_block_bytes + 256 * 1024
                else
                    try incarnationWorkingBytes(&txn, entry.value, kind == 1, if (kind == 0) segmentIdFromKey(entry.key) else std.mem.readInt(u64, entry.key[1..9], .big), true);
                preflight_bytes = std.math.add(u64, preflight_bytes, bytes) catch return error.ResourceBudgetExceeded;
                counts[kind] += 1;
                if (counts[kind] >= max_segments) break;
                entry_opt = try preflight.next();
            }
        }
        if (counts[0] < min_segments and counts[1] < min_segments) return null;
        var reservation = try self.reserveSparseSegmentCompactionBuffers(preflight_bytes, counts[0] + counts[1]);
        errdefer if (reservation) |*held| held.release();

        var sources = std.ArrayListUnmanaged(SegmentCompactionSource).empty;
        errdefer {
            for (sources.items) |source| source.deinit(alloc);
            sources.deinit(alloc);
        }

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_segment = try cur.seekAtOrAfter(taggedPrefix(key_segment));
        while (maybe_segment) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_segment) break;
            const id = segmentIdFromKey(entry.key) orelse return error.InvalidSparseSegment;
            const data = try alloc.dupe(u8, entry.value);
            var transferred = false;
            errdefer if (!transferred) alloc.free(data);
            const incarnations = if (options.scratch != null and try immutableVersion(data) == SEGMENT_FORMAT_VERSION) try alloc.alloc(CapturedIncarnation, 0) else try captureIncarnations(alloc, &txn, id, data, false);
            const source: SegmentCompactionSource = .{ .id = id, .data = data, .incarnations = incarnations };
            sources.append(alloc, source) catch |err| {
                for (incarnations) |item| if (item.doc_id) |bytes| alloc.free(bytes);
                alloc.free(incarnations);
                return err;
            };
            transferred = true;
            if (sources.items.len >= max_segments) break;
            maybe_segment = try cur.next();
        }
        var docmaps = std.ArrayListUnmanaged(SegmentCompactionSource).empty;
        errdefer {
            for (docmaps.items) |docmap| docmap.deinit(alloc);
            docmaps.deinit(alloc);
        }
        var docmap_cur = try txn.openCursor();
        defer docmap_cur.close();
        var maybe_docmap = try docmap_cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_docmap) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            if (entry.key.len != 9) return error.InvalidSparseDocMapSegment;
            const id = std.mem.readInt(u64, entry.key[1..9], .big);
            const data = try alloc.dupe(u8, entry.value);
            var transferred = false;
            errdefer if (!transferred) alloc.free(data);
            const incarnations = if (options.scratch != null and try immutableVersion(data) == SEGMENT_FORMAT_VERSION) try alloc.alloc(CapturedIncarnation, 0) else try captureIncarnations(alloc, &txn, id, data, true);
            docmaps.append(alloc, .{ .id = id, .data = data, .incarnations = incarnations }) catch |err| {
                for (incarnations) |item| if (item.doc_id) |bytes| alloc.free(bytes);
                alloc.free(incarnations);
                return err;
            };
            transferred = true;
            if (docmaps.items.len >= max_segments) break;
            maybe_docmap = try docmap_cur.next();
        }

        if (sources.items.len < min_segments and docmaps.items.len < min_segments) {
            if (reservation) |*held| held.release();
            for (sources.items) |source| source.deinit(alloc);
            sources.deinit(alloc);
            for (docmaps.items) |source| source.deinit(alloc);
            docmaps.deinit(alloc);
            return null;
        }

        const owned_sources = try sources.toOwnedSlice(alloc);
        errdefer {
            for (owned_sources) |source| source.deinit(alloc);
            alloc.free(owned_sources);
        }
        const owned_docmaps = try docmaps.toOwnedSlice(alloc);
        errdefer {
            for (owned_docmaps) |docmap| docmap.deinit(alloc);
            alloc.free(owned_docmaps);
        }

        const generation = if (options.background_publication and options.scratch != null) try self.reserveCompactionId() else null;
        snapshot_transferred = true;
        return .{
            .snapshot = txn,
            .generation = generation,
            .store = self.store,
            .scratch = options.scratch,
            .sources = owned_sources,
            .docmaps = owned_docmaps,
            .buffer_reservation = reservation,
        };
    }

    pub fn executeSegmentCompactionTask(
        alloc: Allocator,
        task: *SegmentCompactionTask,
        chunk_size: u32,
    ) !SegmentCompactionResult {
        var epochs = std.AutoHashMapUnmanaged(u32, CapturedIncarnation).empty;
        defer epochs.deinit(alloc);
        // Execute owns the pinned read lease until all input blocks have been
        // consumed. Publication retains only immutable roots and epoch proofs.
        defer {
            if (task.snapshot) |*snapshot| snapshot.abort();
            task.snapshot = null;
        }
        if (task.scratch) |options| {
            for (task.sources) |*source| if (source.proofs == null and try immutableVersion(source.data) == SEGMENT_FORMAT_VERSION) {
                source.proofs = try captureDiskProofs(alloc, &task.snapshot.?, source.id, false, options);
            };
            for (task.docmaps) |*source| if (source.proofs == null and try immutableVersion(source.data) == SEGMENT_FORMAT_VERSION) {
                source.proofs = try captureDiskProofs(alloc, &task.snapshot.?, source.id, true, options);
            };
        }
        var reader: PageReader = .{ .txn = if (task.snapshot) |*snapshot| snapshot else return error.InvalidSparseSegment };
        defer reader.deinit();
        var builder = try posting_pages.Builder.init(alloc, task.scratch);
        defer builder.deinit();
        const TermIterator = posting_pages.Terms(backend_erased.ReadTxn);
        const term_iterators = try alloc.alloc(TermIterator, task.sources.len);
        defer alloc.free(term_iterators);
        var opened: usize = 0;
        defer for (term_iterators[0..opened]) |*iterator| iterator.deinit();
        const term_heads = try alloc.alloc(?[20]u8, task.sources.len);
        defer alloc.free(term_heads);
        for (task.sources, term_iterators, term_heads) |source, *iterator, *head| {
            iterator.* = try TermIterator.init(&task.snapshot.?, source.id, source.data);
            opened += 1;
            head.* = try iterator.next();
        }
        const proof_positions = try alloc.alloc(usize, task.sources.len);
        defer alloc.free(proof_positions);
        var streams = try alloc.alloc(daat.Stream, task.sources.len);
        defer alloc.free(streams);
        var chunk: std.ArrayListUnmanaged(BulkPosting) = .empty;
        defer chunk.deinit(alloc);
        const output_chunk_size = @min(@max(1, chunk_size), 65536);
        var posting_count: u64 = 0;
        while (true) {
            var term: ?u32 = null;
            for (term_heads) |head| if (head) |entry| {
                const candidate = std.mem.readInt(u32, entry[0..4], .little);
                term = if (term) |prior| @min(prior, candidate) else candidate;
            };
            const current = term orelse break;
            @memset(proof_positions, 0);
            var active: usize = 0;
            defer for (streams[0..active]) |*stream| stream.deinit();
            for (task.sources, term_heads, 0..) |source, head, source_index| {
                const entry = head orelse continue;
                if (std.mem.readInt(u32, entry[0..4], .little) != current) continue;
                term_heads[source_index] = try term_iterators[source_index].next();
                streams[active] = if (posting_pages.paged(source.data))
                    .{ .weight = 1, .query_order = source_index, .segment = source.id, .term = current, .reader = reader.interface(), .allocator = alloc }
                else
                    .{ .weight = 1, .query_order = source_index, .payload = (try segmentTermPayload(source.data, current)) orelse return error.InvalidSparseSegment };
                active += 1;
                try streams[active - 1].advance();
            }
            const Navigation = struct {
                fn order(input: []daat.Stream, left: usize, right: usize) std.math.Order {
                    const relation = std.math.order(input[left].doc.?, input[right].doc.?);
                    return if (relation == .eq) std.math.order(left, right) else relation;
                }
            };
            var queue = std.PriorityQueue(usize, []daat.Stream, Navigation.order).initContext(streams[0..active]);
            defer queue.deinit(alloc);
            for (streams[0..active], 0..) |stream, index| if (stream.doc != null) {
                try queue.push(alloc, index);
            };
            var pending: ?BulkPosting = null;
            while (queue.pop()) |index| {
                const stream = &streams[index];
                const doc = stream.doc.?;
                const captured = (try task.sources[stream.query_order].captured(doc, &proof_positions[stream.query_order])) orelse return error.InvalidSparseSegment;
                if (captured.live) {
                    const contribution = try stream.contribution();
                    if (pending != null and pending.?.doc_num == doc) {
                        pending.?.weight = try daat.addScore(pending.?.weight, contribution);
                    } else {
                        if (pending) |posting| {
                            try chunk.append(alloc, posting);
                            if (chunk.items.len >= output_chunk_size) {
                                try appendCompactionBlock(alloc, &builder, chunk.items);
                                chunk.clearRetainingCapacity();
                            }
                        }
                        if (task.scratch == null) try epochs.put(alloc, doc, .{ .doc_num = doc, .epoch = captured.epoch, .live = true });
                        pending = .{ .term_id = current, .doc_num = doc, .weight = contribution, .doc_id = captured.doc_id orelse &.{} };
                        posting_count += 1;
                    }
                }
                try stream.advance();
                if (stream.doc != null) try queue.push(alloc, index);
            }
            if (pending) |posting| try chunk.append(alloc, posting);
            if (chunk.items.len != 0) {
                try appendCompactionBlock(alloc, &builder, chunk.items);
                chunk.clearRetainingCapacity();
            }
        }
        const merged = try builder.finish();
        errdefer if (merged) |bytes| alloc.free(bytes);
        const incarnations = try alloc.alloc(CapturedIncarnation, epochs.count());
        errdefer alloc.free(incarnations);
        var epoch_it = epochs.valueIterator();
        for (incarnations) |*epoch| epoch.* = epoch_it.next().?.*;

        var proofs: ?DiskProofs = if (task.scratch) |options| try DiskProofs.init(alloc, options) else null;
        errdefer if (proofs) |*owned| owned.deinit();
        if (proofs) |*output| {
            const positions = try alloc.alloc(usize, task.sources.len);
            defer alloc.free(positions);
            @memset(positions, 0);
            const heads = try alloc.alloc(CapturedIncarnation, task.sources.len);
            defer alloc.free(heads);
            const Merge = struct {
                fn at(source: SegmentCompactionSource, position: usize) !?CapturedIncarnation {
                    if (source.proofs) |disk| {
                        if (position >= disk.count()) return null;
                        const entry = try disk.at(position);
                        return .{ .doc_num = entry.doc_num, .epoch = entry.epoch, .live = entry.live };
                    }
                    return if (position < source.incarnations.len) source.incarnations[position] else null;
                }
                fn order(entries: []CapturedIncarnation, left: usize, right: usize) std.math.Order {
                    return std.math.order(entries[left].doc_num, entries[right].doc_num);
                }
            };
            var queue = std.PriorityQueue(usize, []CapturedIncarnation, Merge.order).initContext(heads);
            defer queue.deinit(alloc);
            for (task.sources, 0..) |source, index| if (try Merge.at(source, 0)) |entry| {
                heads[index] = entry;
                try queue.push(alloc, index);
            };
            var pending: ?CapturedIncarnation = null;
            while (queue.pop()) |index| {
                const entry = heads[index];
                if (pending) |prior| if (prior.doc_num != entry.doc_num) {
                    if (prior.live) try output.append(.{ .doc_num = prior.doc_num, .epoch = prior.epoch, .live = true });
                    pending = null;
                };
                if (pending == null or entry.live) pending = entry;
                positions[index] += 1;
                if (try Merge.at(task.sources[index], positions[index])) |next| {
                    heads[index] = next;
                    try queue.push(alloc, index);
                }
            }
            if (pending) |entry| if (entry.live) try output.append(.{ .doc_num = entry.doc_num, .epoch = entry.epoch, .live = true });
            try output.seal();
        }
        if (task.scratch) |options| {
            var map_sort = try @import("../spill_sort.zig").Sorter.init(alloc, options);
            errdefer map_sort.deinit();
            const Context = struct {
                a: Allocator,
                source: SegmentCompactionSource,
                sort: *@import("../spill_sort.zig").Sorter,
                position: usize = 0,
                fn visit(self: *@This(), entry: DocMapLookup) !bool {
                    const doc = std.math.cast(u32, entry.doc_num) orelse return error.DocNumOverflow;
                    const captured = (try self.source.captured(doc, &self.position)) orelse return error.InvalidSparseDocMapSegment;
                    if (!captured.live) return false;
                    const bytes = try self.a.alloc(u8, 12 + entry.doc_id.len + entry.fwd_data.len);
                    defer self.a.free(bytes);
                    std.mem.writeInt(u64, bytes[0..8], captured.epoch, .little);
                    std.mem.writeInt(u32, bytes[8..12], @intCast(entry.doc_id.len), .little);
                    @memcpy(bytes[12..][0..entry.doc_id.len], entry.doc_id);
                    @memcpy(bytes[12 + entry.doc_id.len ..], entry.fwd_data);
                    try self.sort.add(doc, bytes);
                    return false;
                }
            };
            for (task.docmaps) |source| {
                var context: Context = .{ .a = alloc, .source = source, .sort = &map_sort };
                _ = try forEachStoredDocMapEntry(&task.snapshot.?, source.id, source.data, &context, Context.visit);
            }
            const range = try map_sort.finish();
            var count: u32 = 0;
            if (range) |records| {
                var cursor = @import("../spill_sort.zig").Cursor.init(alloc, map_sort.run, records);
                defer cursor.deinit();
                var previous: ?u64 = null;
                while (try cursor.nextView()) |entry| {
                    if (previous == null or previous.? != entry.key) {
                        count = try std.math.add(u32, count, 1);
                        previous = entry.key;
                    }
                }
            }
            const docmap = if (count != 0) try alloc.alloc(u8, 16) else null;
            errdefer if (docmap) |data| alloc.free(data);
            if (docmap) |data| {
                @memcpy(data[0..8], paged_docmap_magic);
                std.mem.writeInt(u32, data[8..12], SEGMENT_FORMAT_VERSION, .little);
                std.mem.writeInt(u32, data[12..16], count, .little);
            }
            var result: SegmentCompactionResult = .{ .data = merged, .pages = builder.run, .directory_run = builder.terms, .incarnations = incarnations, .proofs = proofs, .docmap = docmap, .map_sort = map_sort, .map_range = range, .source_segments = @intCast(task.sources.len), .postings = posting_count };
            if (task.generation) |id| {
                var writer: CompactionWriter = .{ .alloc = alloc, .store = task.store.? };
                try stageCompaction(&writer, id, &result);
                result.staged = true;
            }
            builder.run = null;
            builder.terms = null;
            return result;
        }
        var docmap_bytes = std.ArrayListUnmanaged(u8).empty;
        defer docmap_bytes.deinit(alloc);
        try docmap_bytes.appendSlice(alloc, docmap_magic);
        try appendU32Le(alloc, &docmap_bytes, SEGMENT_FORMAT_VERSION);
        try appendU32Le(alloc, &docmap_bytes, 0);
        var docmap_epochs = std.AutoHashMapUnmanaged(u32, CapturedIncarnation).empty;
        defer docmap_epochs.deinit(alloc);
        const DocMapContext = struct {
            alloc: Allocator,
            source_epochs: []const CapturedIncarnation,
            epochs: *std.AutoHashMapUnmanaged(u32, CapturedIncarnation),
            bytes: *std.ArrayListUnmanaged(u8),
            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                const doc_num = std.math.cast(u32, entry.doc_num) orelse return error.DocNumOverflow;
                const captured = findCaptured(ctx.source_epochs, doc_num) orelse return error.InvalidSparseDocMapSegment;
                if (!captured.live or ctx.epochs.contains(doc_num)) return false;
                try ctx.epochs.put(ctx.alloc, doc_num, .{ .doc_num = doc_num, .epoch = captured.epoch, .live = true });
                try appendU64Le(ctx.alloc, ctx.bytes, doc_num);
                try appendU32Le(ctx.alloc, ctx.bytes, @intCast(entry.doc_id.len));
                try appendU32Le(ctx.alloc, ctx.bytes, @intCast(entry.fwd_data.len));
                try ctx.bytes.appendSlice(ctx.alloc, entry.doc_id);
                try ctx.bytes.appendSlice(ctx.alloc, entry.fwd_data);
                return false;
            }
        };
        for (task.docmaps) |source| {
            var ctx: DocMapContext = .{ .alloc = alloc, .source_epochs = source.incarnations, .epochs = &docmap_epochs, .bytes = &docmap_bytes };
            _ = try forEachStoredDocMapEntry(&task.snapshot.?, source.id, source.data, &ctx, DocMapContext.visit);
        }
        std.mem.writeInt(u32, docmap_bytes.items[docmap_magic.len + 4 ..][0..4], @intCast(docmap_epochs.count()), .little);
        const docmap_incarnations = try alloc.alloc(CapturedIncarnation, docmap_epochs.count());
        errdefer alloc.free(docmap_incarnations);
        var docmap_it = docmap_epochs.valueIterator();
        for (docmap_incarnations) |*epoch| epoch.* = docmap_it.next().?.*;
        const docmap = if (docmap_epochs.count() != 0) try docmap_bytes.toOwnedSlice(alloc) else null;
        const pages = builder.run;
        builder.run = null;
        errdefer if (pages) |run| run.deinit();
        return .{
            .data = merged,
            .pages = pages,
            .docmap = docmap,
            .incarnations = incarnations,
            .proofs = proofs,
            .docmap_incarnations = docmap_incarnations,
            .source_segments = @intCast(task.sources.len),
            .postings = posting_count,
        };
    }

    // Durable intent records make abandoned staging and retired generations
    // recoverable without scanning the archive or exposing partial roots.
    fn pendingKey(id: u64) [9]u8 {
        var key: [9]u8 = undefined;
        key[0] = 0x12;
        std.mem.writeInt(u64, key[1..9], id, .big);
        return key;
    }
    fn reserveCompactionId(self: *SparseIndex) !u64 {
        var txn = try self.beginBatchTxn(.{});
        errdefer txn.abort();
        const id = self.next_segment_id;
        self.next_segment_id = try std.math.add(u64, id, 1);
        errdefer self.next_segment_id = id;
        try persistNextSegmentId(self, &txn);
        try txn.put(&pendingKey(id), &.{});
        try txn.commit();
        return id;
    }
    fn reclaimCompactionId(self: anytype, id: u64) !void {
        var read = try self.beginReadTxn();
        var root_key: [16]u8 = undefined;
        const posting_live = if (read.get(segmentKey(&root_key, id))) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => {
                read.abort();
                return err;
            },
        };
        const map_live = if (read.get(docMapSegmentKey(&root_key, id))) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => {
                read.abort();
                return err;
            },
        };
        read.abort();
        if (!posting_live) {
            while (true) {
                var txn = try self.beginBatchTxn(.{});
                errdefer txn.abort();
                const more = try posting_pages.reclaimDirectoryPage(&txn, id);
                try txn.commit();
                if (!more) break;
            }
        }
        for ([_]u8{ 0x0d, 0x11, key_segment_incarnation, key_docmap_incarnation, 0x13 }) |tag| {
            if (if (tag == key_docmap_incarnation or tag == 0x13) map_live else posting_live) continue;
            while (true) {
                var txn = try self.beginBatchTxn(.{});
                var batch_open = true;
                defer if (batch_open) txn.abort();
                var keys: [128][17]u8 = undefined;
                var count: usize = 0;
                {
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    var prefix: [9]u8 = undefined;
                    prefix[0] = tag;
                    std.mem.writeInt(u64, prefix[1..9], id, .big);
                    var next = try cursor.seekAtOrAfter(&prefix);
                    while (next) |entry| {
                        if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..9], &prefix)) break;
                        keys[count] = entry.key[0..17].*;
                        count += 1;
                        if (count == keys.len) break;
                        next = try cursor.next();
                    }
                }
                if (count == 0) break;
                for (keys[0..count]) |key| txn.delete(&key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                try txn.commit();
                batch_open = false;
            }
        }
        // Another worker may retire only this generation's posting root
        // while its live docmap still needs locator refresh. Its durable
        // intent belongs to the refreshing owner (or restart recovery).
        if (!map_live) try clearCompactionIntent(self, id);
    }
    fn clearCompactionIntent(self: anytype, id: u64) !void {
        var txn = try self.beginBatchTxn(.{});
        errdefer txn.abort();
        txn.delete(&pendingKey(id)) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try txn.commit();
    }
    fn refreshPagedLocators(self: anytype, id: u64, gate: ?MaintenanceGate) !void {
        var read = try self.beginReadTxn();
        defer read.abort();
        var root_key: [16]u8 = undefined;
        const root = read.get(docMapSegmentKey(&root_key, id)) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        if (!pagedDocMap(root)) return;
        var cursor = try read.openCursor();
        defer cursor.close();
        var epochs = try read.openCursor();
        defer epochs.close();
        const prefix = docMapEntryKey(id, 0);
        var next = try cursor.seekAtOrAfter(prefix[0..9]);
        while (next != null) {
            if (gate) |g| try g.enter(g.ptr);
            defer if (gate) |g| g.leave(g.ptr);
            var updates = try self.beginBatchTxn(.{});
            var updates_open = true;
            defer if (updates_open) updates.abort();
            const active = updates.get(docMapSegmentKey(&root_key, id)) catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            if (!std.mem.eql(u8, active, root)) return;
            var count: usize = 0;
            while (next) |entry| {
                if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..9], prefix[0..9])) {
                    next = null;
                    break;
                }
                const doc = std.mem.readInt(u64, entry.key[9..17], .big);
                if (entry.value.len < 4) return error.InvalidSparseDocMapSegment;
                const len = std.mem.readInt(u32, entry.value[0..4], .little);
                if (len > entry.value.len - 4) return error.InvalidSparseDocMapSegment;
                if (len < 256) {
                    var epoch_key: [17]u8 = undefined;
                    const wanted = segmentIncarnationKey(&epoch_key, id, doc, true);
                    const proof = (try epochs.seekAtOrAfter(wanted)) orelse return error.InvalidSparseDocMapSegment;
                    if (!std.mem.eql(u8, proof.key, wanted) or proof.value.len != 8) return error.InvalidSparseDocMapSegment;
                    const epoch = std.mem.readInt(u64, proof.value[0..8], .little);
                    var tombstone: [16]u8 = undefined;
                    const deleted = if (updates.get(docTombstoneKey(&tombstone, doc))) |_| true else |err| switch (err) {
                        error.NotFound => false,
                        else => return err,
                    };
                    if (!deleted and epoch == try currentIncarnation(&updates, doc)) {
                        var locator: [36]u8 = undefined;
                        std.mem.writeInt(u64, locator[0..8], doc, .little);
                        std.mem.writeInt(u64, locator[8..16], id, .little);
                        std.mem.writeInt(u64, locator[16..24], 4 + @as(u64, len), .little);
                        std.mem.writeInt(u32, locator[24..28], @intCast(entry.value.len - 4 - len), .little);
                        std.mem.writeInt(u64, locator[28..36], epoch, .little);
                        var key: [256]u8 = undefined;
                        try updates.put(locatorKey(&key, entry.value[4..][0..len]), &locator);
                    }
                }
                count += 1;
                next = try cursor.next();
                if (count == 256) break;
            }
            try updates.commit();
            updates_open = false;
        }
    }
    fn recoverCompactionIntents(self: *SparseIndex) !void {
        while (true) {
            const id = blk: {
                var txn = try self.beginReadTxn();
                defer txn.abort();
                var cursor = try txn.openCursor();
                defer cursor.close();
                const entry = (try cursor.seekAtOrAfter(&.{0x12})) orelse return;
                if (entry.key.len != 9 or entry.key[0] != 0x12) return;
                break :blk std.mem.readInt(u64, entry.key[1..9], .big);
            };
            try self.refreshPagedLocators(id, null);
            try self.reclaimCompactionId(id);
            try clearCompactionIntent(self, id);
        }
    }
    fn stageCompaction(self: anytype, id: u64, result: *SegmentCompactionResult) !void {
        if (result.pages) |run| {
            const view = try run.sealedView();
            var offset: u64 = 0;
            while (offset < view.length) {
                var txn = try self.beginBatchTxn(.{});
                errdefer txn.abort();
                var batch_bytes: usize = 0;
                while (offset < view.length and batch_bytes < 4 * 1024 * 1024) {
                    var header: [12]u8 = undefined;
                    try view.readInto(offset, &header);
                    offset += 12;
                    const term = std.mem.readInt(u32, header[0..4], .little);
                    const last = std.mem.readInt(u32, header[4..8], .little);
                    const len = std.mem.readInt(u32, header[8..12], .little);
                    if (len > posting_pages.max_block_bytes or len > view.length - offset) return error.InvalidSparseSegment;
                    const bytes = try self.alloc.alloc(u8, len);
                    defer self.alloc.free(bytes);
                    try view.readInto(offset, bytes);
                    try posting_pages.putBlock(&txn, id, term, last, bytes);
                    offset += len;
                    batch_bytes += len;
                }
                try txn.commit();
            }
        }
        if (result.directory_run) |run| {
            const root = result.data orelse return error.InvalidSparseSegment;
            const view = try run.sealedView();
            const header_len = 16 + @as(u64, std.mem.readInt(u32, root[12..16], .little)) * 20;
            var offset: u64 = 0;
            while (offset < view.length) {
                var entries: [posting_pages.directory_page_terms * 20]u8 = undefined;
                const len: usize = @intCast(@min(entries.len, view.length - offset));
                if (len % 20 != 0) return error.InvalidSparseSegment;
                try view.readInto(offset, entries[0..len]);
                var at: usize = 0;
                while (at < len) : (at += 20) {
                    const relative = std.mem.readInt(u64, entries[at + 8 ..][0..8], .little);
                    std.mem.writeInt(u64, entries[at + 8 ..][0..8], try std.math.add(u64, header_len, relative), .little);
                }
                var txn = try self.beginBatchTxn(.{});
                errdefer txn.abort();
                try posting_pages.putDirectoryPage(&txn, id, root, entries[0..len]);
                try txn.commit();
                offset += len;
            }
        }
        if (result.map_range) |range| {
            var cursor = @import("../spill_sort.zig").Cursor.init(self.alloc, result.map_sort.?.run, range);
            defer cursor.deinit();
            var previous: ?u64 = null;
            var done = false;
            while (!done) {
                var txn = try self.beginBatchTxn(.{});
                errdefer txn.abort();
                var bytes: usize = 0;
                while (bytes < 4 * 1024 * 1024) {
                    const entry = (try cursor.next()) orelse {
                        done = true;
                        break;
                    };
                    if (previous != null and previous.? == entry.key) continue;
                    if (entry.payload.len < 12) return error.ResourceBudgetExceeded;
                    const epoch = std.mem.readInt(u64, entry.payload[0..8], .little);
                    try txn.put(&docMapEntryKey(id, entry.key), entry.payload[8..]);
                    try putSegmentIncarnation(&txn, id, entry.key, epoch, true);
                    previous = entry.key;
                    bytes += entry.payload.len;
                }
                try txn.commit();
            }
        }
        if (result.proofs) |proofs| {
            var offset: usize = 0;
            while (offset < proofs.count()) {
                var txn = try self.beginBatchTxn(.{});
                errdefer txn.abort();
                const end = @min(offset + 1024, proofs.count());
                for (offset..end) |i| {
                    const entry = try proofs.at(i);
                    try putSegmentIncarnation(&txn, id, entry.doc_num, entry.epoch, false);
                }
                try txn.commit();
                offset = end;
            }
        }
    }
    pub fn publishSegmentCompactionTask(
        self: *SparseIndex,
        task: *const SegmentCompactionTask,
        result: *SegmentCompactionResult,
    ) !bool {
        if (task.sources.len < 2 and task.docmaps.len < 2) return false;

        const segment_id = task.generation orelse try self.reserveCompactionId();
        var published = false;
        defer if (!published and task.generation == null) self.reclaimCompactionId(segment_id) catch {};
        if (!result.staged) try self.stageCompaction(segment_id, result);
        var txn = try self.beginBatchTxn(.{});
        var txn_open = true;
        defer if (txn_open) txn.abort();

        for (task.sources) |source| {
            var key_buf: [16]u8 = undefined;
            const current = txn.get(segmentKey(&key_buf, source.id)) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            if (!std.mem.eql(u8, current, source.data)) return false;
        }
        for (task.docmaps) |source| {
            var key_buf: [16]u8 = undefined;
            const current = txn.get(docMapSegmentKey(&key_buf, source.id)) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            if (!std.mem.eql(u8, current, source.data)) return false;
        }

        if (result.data != null or result.docmap != null) {
            var key_buf: [16]u8 = undefined;
            if (result.data) |data| {
                if (result.pages) |pages| {
                    _ = pages;
                    try posting_pages.ensureRoutes(self.alloc, &txn);
                    try posting_pages.publishRoutes(&txn, segment_id, data);
                    try txn.put(segmentKey(&key_buf, segment_id), data);
                } else {
                    const root = try posting_pages.publish(self.alloc, &txn, segment_id, data);
                    defer self.alloc.free(root);
                    try txn.put(segmentKey(&key_buf, segment_id), root);
                }
            }
            if (result.docmap) |data| try txnAppendPut(&txn, self.dbi, docMapSegmentKey(&key_buf, segment_id), data);
            for (result.incarnations) |epoch| try putSegmentIncarnation(&txn, segment_id, epoch.doc_num, epoch.epoch, false);
            for (result.docmap_incarnations) |epoch| try putSegmentIncarnation(&txn, segment_id, epoch.doc_num, epoch.epoch, true);
            if (result.docmap) |data| if (!pagedDocMap(data)) try publishDocMapLocators(self.alloc, &txn, segment_id, data);
        }

        for (task.sources) |source| {
            var key_buf: [16]u8 = undefined;
            try posting_pages.removeRoutes(&txn, source.id);
            try txn.delete(segmentKey(&key_buf, source.id));
            try txn.put(&pendingKey(source.id), &.{});
        }
        for (task.docmaps) |source| {
            var key_buf: [16]u8 = undefined;
            try txn.delete(docMapSegmentKey(&key_buf, source.id));
            try txn.put(&pendingKey(source.id), &.{});
        }
        try txn.commit();
        txn_open = false;
        published = true;
        result.generation = segment_id;
        return true;
    }

    pub const MaintenanceGate = struct {
        ptr: *anyopaque,
        enter: *const fn (*anyopaque) anyerror!void,
        leave: *const fn (*anyopaque) void,
    };
    const CompactionWriter = struct {
        alloc: Allocator,
        store: backend_erased.Store,
        fn beginReadTxn(self: *@This()) !backend_erased.ReadTxn {
            return self.store.beginRead();
        }
        fn beginBatchTxn(self: *@This(), options: backend_types.BatchOptions) !backend_erased.Batch {
            return self.store.beginBatchWithOptions(options);
        }
    };
    fn discardStaging(writer: *CompactionWriter, id: u64) !void {
        var read = try writer.beginReadTxn();
        var key_buf: [16]u8 = undefined;
        const live = blk: {
            const posting = read.get(segmentKey(&key_buf, id)) catch |err| switch (err) {
                error.NotFound => null,
                else => {
                    read.abort();
                    return err;
                },
            };
            if (posting != null) break :blk true;
            const map = read.get(docMapSegmentKey(&key_buf, id)) catch |err| switch (err) {
                error.NotFound => null,
                else => {
                    read.abort();
                    return err;
                },
            };
            if (map != null) break :blk true;
            break :blk false;
        };
        read.abort();
        // A live root retains its intent until locator refresh completes.
        if (!live) try reclaimCompactionId(writer, id);
    }
    pub fn completeSegmentCompactionTask(alloc: Allocator, task: *const SegmentCompactionTask, result: *const SegmentCompactionResult, gate: ?MaintenanceGate) !void {
        const id = result.generation orelse return;
        var writer: CompactionWriter = .{ .alloc = alloc, .store = task.store.? };
        try refreshPagedLocators(&writer, id, gate);
        try reclaimCompactionId(&writer, id);
        try clearCompactionIntent(&writer, id);
        for (task.sources) |source| try reclaimCompactionId(&writer, source.id);
        for (task.docmaps) |source| try reclaimCompactionId(&writer, source.id);
    }
    pub fn finishSegmentCompactionTask(self: *SparseIndex, task: *const SegmentCompactionTask, result: *SegmentCompactionResult) !bool {
        const published = try self.publishSegmentCompactionTask(task, result);
        if (published) try completeSegmentCompactionTask(self.alloc, task, result, null);
        return published;
    }

    pub fn compactSegmentsWithOptions(self: *SparseIndex, alloc: Allocator, options: SegmentCompactionOptions) !bool {
        var task = (try self.beginSegmentCompactionTask(alloc, options)) orelse return false;
        defer task.deinit(alloc);
        var result = try executeSegmentCompactionTask(alloc, &task, self.chunk_size);
        defer result.deinit(alloc);
        return try self.finishSegmentCompactionTask(&task, &result);
    }

    fn tryBulkAppend(self: *SparseIndex, writes: []const SparseWrite, deletes: []const []const u8, options: BatchOptions) !bool {
        if (deletes.len != 0) return false;

        var phase_start_ns = nowNs();
        var reservation = try self.tryReserveSparseBulkWorkingSet(writes);
        self.write_profile.reserve_ns += elapsedSince(phase_start_ns);
        defer if (reservation) |*held| held.release();

        const bulk_batch_options: backend_types.BatchOptions = if (options.backend_batch_options.mode == .bulk_ingest)
            options.backend_batch_options
        else
            .{ .mode = .bulk_ingest };
        var txn = try self.beginBatchTxn(bulk_batch_options);
        errdefer txn.abort();

        var last_index_by_doc = std.StringHashMapUnmanaged(usize).empty;
        defer last_index_by_doc.deinit(self.alloc);
        phase_start_ns = nowNs();
        try last_index_by_doc.ensureTotalCapacity(self.alloc, @intCast(writes.len));
        for (writes, 0..) |write, i| {
            try last_index_by_doc.put(self.alloc, write.doc_id, i);
        }
        self.write_profile.dedupe_ns += elapsedSince(phase_start_ns);

        var active_indices = std.ArrayListUnmanaged(usize).empty;
        defer active_indices.deinit(self.alloc);
        try active_indices.ensureTotalCapacity(self.alloc, writes.len);

        var posting_count: usize = 0;
        phase_start_ns = nowNs();
        for (writes, 0..) |write, i| {
            if (last_index_by_doc.get(write.doc_id).? != i) continue;
            if (write.vec.indices.len != write.vec.values.len) return error.InvalidSparseVector;
            if (!options.assume_new_doc_ids) {
                var fwd_key_buf: [256]u8 = undefined;
                const existing = txn.get(fwdKey(&fwd_key_buf, write.doc_id)) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (existing != null) {
                    self.write_profile.existence_check_ns += elapsedSince(phase_start_ns);
                    txn.abort();
                    return false;
                }
            }
            try active_indices.append(self.alloc, i);
            posting_count += write.vec.indices.len;
        }
        self.write_profile.existence_check_ns += elapsedSince(phase_start_ns);
        if (active_indices.items.len == 0) {
            phase_start_ns = nowNs();
            try txn.commit();
            self.write_profile.commit_ns += elapsedSince(phase_start_ns);
            self.write_profile.bulk_append_calls += 1;
            return true;
        }

        const prev_next_doc_num = self.next_doc_num;
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.next_doc_num = prev_next_doc_num;
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }

        var occupied_doc_nums = std.AutoHashMapUnmanaged(u64, []const u8).empty;
        defer occupied_doc_nums.deinit(self.alloc);
        try occupied_doc_nums.ensureTotalCapacity(self.alloc, @intCast(active_indices.items.len));

        var bulk_docs = std.ArrayListUnmanaged(BulkDoc).empty;
        defer bulk_docs.deinit(self.alloc);
        try bulk_docs.ensureTotalCapacity(self.alloc, active_indices.items.len);

        var postings = std.ArrayListUnmanaged(BulkPosting).empty;
        defer postings.deinit(self.alloc);
        try postings.ensureTotalCapacity(self.alloc, posting_count);

        for (active_indices.items) |write_idx| {
            const write = writes[write_idx];
            phase_start_ns = nowNs();
            const doc_num = try self.allocateBulkDocNum(write.doc_id, write.doc_num, &occupied_doc_nums);
            if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
            self.doc_count += 1;
            self.write_profile.doc_num_ns += elapsedSince(phase_start_ns);

            const incarnation = try advanceIncarnation(&txn, doc_num);
            var tombstone_buf: [16]u8 = undefined;
            txnDelete(&txn, self.dbi, docTombstoneKey(&tombstone_buf, doc_num)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try bulk_docs.append(self.alloc, .{ .write_idx = write_idx, .doc_num = doc_num, .incarnation = incarnation });

            const doc_num_u32: u32 = @intCast(doc_num);
            phase_start_ns = nowNs();
            for (write.vec.indices, 0..) |term_id, term_idx| {
                try postings.append(self.alloc, .{
                    .term_id = term_id,
                    .doc_num = doc_num_u32,
                    .weight = write.vec.values[term_idx],
                    .doc_id = write.doc_id,
                });
            }
            self.write_profile.posting_collect_ns += elapsedSince(phase_start_ns);
        }

        const segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        phase_start_ns = nowNs();
        const docmap_data = try encodeDocMapSegment(self.alloc, writes, bulk_docs.items);
        defer self.alloc.free(docmap_data);
        var docmap_key_buf: [16]u8 = undefined;
        try txnAppendPut(&txn, self.dbi, docMapSegmentKey(&docmap_key_buf, segment_id), docmap_data);
        for (bulk_docs.items) |doc| {
            const write = writes[doc.write_idx];
            try putSegmentIncarnation(&txn, segment_id, doc.doc_num, doc.incarnation, true);
            if (write.vec.indices.len != 0) try putSegmentIncarnation(&txn, segment_id, doc.doc_num, doc.incarnation, false);
            var rev_key_buf: [16]u8 = undefined;
            try txnAppendPut(&txn, self.dbi, revKey(&rev_key_buf, doc.doc_num), write.doc_id);
        }
        try publishDocMapLocators(self.alloc, &txn, segment_id, docmap_data);
        const complete = for (bulk_docs.items) |doc| {
            if (writes[doc.write_idx].doc_id.len >= 256) break false;
        } else true;
        if (!complete) {
            txn.delete(metaKey(meta_complete_locators)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        } else if (prev_next_doc_num == 0) try txn.put(metaKey(meta_complete_locators), &.{1});
        if (prev_next_doc_num == 0) try txn.put(metaKey(meta_complete_physical), &.{1});
        self.write_profile.fwd_rev_put_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        std.mem.sort(BulkPosting, postings.items, {}, struct {
            fn lessThan(_: void, a: BulkPosting, b: BulkPosting) bool {
                if (a.term_id != b.term_id) return a.term_id < b.term_id;
                return a.doc_num < b.doc_num;
            }
        }.lessThan);
        self.write_profile.posting_sort_ns += elapsedSince(phase_start_ns);

        var start: usize = 0;
        var term_groups: u64 = 0;
        phase_start_ns = nowNs();
        while (start < postings.items.len) {
            var end = start + 1;
            while (end < postings.items.len and postings.items[end].term_id == postings.items[start].term_id) : (end += 1) {}
            term_groups += 1;
            if (try self.ensureTermCatalogEntry(&txn, postings.items[start].term_id)) self.term_count += 1;
            start = end;
        }
        if (postings.items.len > 0) {
            const segment_data = try encodeSegmentFromSortedPostings(self.alloc, postings.items, self.chunk_size);
            defer self.alloc.free(segment_data);
            var segment_key_buf: [16]u8 = undefined;
            const root = try posting_pages.publish(self.alloc, &txn, segment_id, segment_data);
            defer self.alloc.free(root);
            try txn.put(segmentKey(&segment_key_buf, segment_id), root);
        }
        self.write_profile.posting_write_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        try persistSparseCounters(self, &txn);
        try txn.commit();
        self.write_profile.commit_ns += elapsedSince(phase_start_ns);
        self.write_profile.bulk_append_calls += 1;
        self.write_profile.postings += posting_count;
        self.write_profile.terms += term_groups;
        return true;
    }

    fn allocateBulkDocNum(
        self: *SparseIndex,
        doc_id: []const u8,
        preferred_doc_num: ?u32,
        occupied_doc_nums: *std.AutoHashMapUnmanaged(u64, []const u8),
    ) !u64 {
        if (preferred_doc_num) |doc_num_u32| {
            const doc_num: u64 = doc_num_u32;
            if (occupied_doc_nums.get(doc_num)) |existing_doc_id| {
                if (std.mem.eql(u8, existing_doc_id, doc_id)) return doc_num;
            } else {
                try occupied_doc_nums.put(self.alloc, doc_num, doc_id);
                if (doc_num >= self.next_doc_num) self.next_doc_num = doc_num + 1;
                return doc_num;
            }
        }

        var doc_num = self.next_doc_num;
        while (occupied_doc_nums.contains(doc_num)) : (doc_num += 1) {}
        self.next_doc_num = doc_num + 1;
        try occupied_doc_nums.put(self.alloc, doc_num, doc_id);
        return doc_num;
    }

    fn ensureTermCatalogEntry(self: *SparseIndex, txn: anytype, term_id: u32) !bool {
        var key_buf: [16]u8 = undefined;
        const key = termCatalogKey(&key_buf, term_id);
        _ = txnGet(txn, self.dbi, key) catch |err| switch (err) {
            error.NotFound => {
                try txnPut(txn, self.dbi, key, &.{});
                return true;
            },
            else => return err,
        };
        return false;
    }

    fn bulkAppendTermPostings(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        postings: []const BulkPosting,
    ) !bool {
        if (postings.len == 0) return false;
        const term_id = postings[0].term_id;
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);
        var chunk_count: u32 = 0;
        var max_weight: f32 = postings[0].weight;
        var created_term = false;
        var term_min_doc_id: ?[]const u8 = null;
        var term_max_doc_id: ?[]const u8 = null;

        var phase_start_ns = nowNs();
        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (meta_data) |data| {
            const tm = decodeTermMeta(data);
            chunk_count = tm.chunk_count;
            max_weight = tm.max_weight;
        } else {
            created_term = true;
        }
        for (postings) |posting| max_weight = @max(max_weight, posting.weight);
        var range_key_buf: [256]u8 = undefined;
        const existing_range_data = txnGet(txn, self.dbi, termRangeKey(&range_key_buf, term_id)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing_range_data) |data| {
            const range = try decodeChunkRangeMeta(data);
            updateBorrowedRangeBounds(&term_min_doc_id, &term_max_doc_id, range.min_doc_id, range.max_doc_id);
        }
        self.write_profile.term_meta_ns += elapsedSince(phase_start_ns);

        var cursor: usize = 0;
        if (chunk_count > 0 and cursor < postings.len) {
            const last_chunk_idx = chunk_count - 1;
            var ck_buf: [256]u8 = undefined;
            phase_start_ns = nowNs();
            const chunk_data = txnGet(txn, self.dbi, invChunkKey(&ck_buf, term_id, last_chunk_idx)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (chunk_data) |data| {
                const decoded = try decodeChunk(alloc, data);
                self.write_profile.chunk_read_ns += elapsedSince(phase_start_ns);
                defer alloc.free(decoded.doc_nums);
                defer alloc.free(decoded.weights);
                if (decoded.doc_nums.len < self.chunk_size) {
                    const available = @as(usize, @intCast(self.chunk_size)) - decoded.doc_nums.len;
                    const take = @min(available, postings.len - cursor);
                    var doc_nums = try alloc.alloc(u32, decoded.doc_nums.len + take);
                    defer alloc.free(doc_nums);
                    var weights = try alloc.alloc(f32, decoded.weights.len + take);
                    defer alloc.free(weights);
                    @memcpy(doc_nums[0..decoded.doc_nums.len], decoded.doc_nums);
                    @memcpy(weights[0..decoded.weights.len], decoded.weights);
                    for (postings[cursor .. cursor + take], 0..) |posting, i| {
                        doc_nums[decoded.doc_nums.len + i] = posting.doc_num;
                        weights[decoded.weights.len + i] = posting.weight;
                    }
                    sortDocNumsAndWeights(doc_nums, weights);
                    var chunk_min_doc_id: ?[]const u8 = null;
                    var chunk_max_doc_id: ?[]const u8 = null;
                    var meta_ck_buf: [256]u8 = undefined;
                    const existing_chunk_range_data = txnGet(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, last_chunk_idx)) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (existing_chunk_range_data) |range_data| {
                        const range = try decodeChunkRangeMeta(range_data);
                        updateBorrowedRangeBounds(&chunk_min_doc_id, &chunk_max_doc_id, range.min_doc_id, range.max_doc_id);
                    }
                    for (postings[cursor .. cursor + take]) |posting| {
                        updateBorrowedRangeBounds(&chunk_min_doc_id, &chunk_max_doc_id, posting.doc_id, posting.doc_id);
                        updateBorrowedRangeBounds(&term_min_doc_id, &term_max_doc_id, posting.doc_id, posting.doc_id);
                    }
                    try self.writeChunkWithRangeMetaProfiled(alloc, txn, term_id, last_chunk_idx, doc_nums, weights, chunk_min_doc_id.?, chunk_max_doc_id.?, &self.write_profile);
                    cursor += take;
                }
            } else {
                self.write_profile.chunk_read_ns += elapsedSince(phase_start_ns);
            }
        }

        while (cursor < postings.len) {
            const take = @min(@as(usize, @intCast(self.chunk_size)), postings.len - cursor);
            var doc_nums = try alloc.alloc(u32, take);
            defer alloc.free(doc_nums);
            var weights = try alloc.alloc(f32, take);
            defer alloc.free(weights);
            var min_doc_id: ?[]const u8 = null;
            var max_doc_id: ?[]const u8 = null;
            for (postings[cursor .. cursor + take], 0..) |posting, i| {
                doc_nums[i] = posting.doc_num;
                weights[i] = posting.weight;
                updateBorrowedRangeBounds(&min_doc_id, &max_doc_id, posting.doc_id, posting.doc_id);
                updateBorrowedRangeBounds(&term_min_doc_id, &term_max_doc_id, posting.doc_id, posting.doc_id);
            }
            try self.writeChunkWithRangeMetaProfiled(alloc, txn, term_id, chunk_count, doc_nums, weights, min_doc_id.?, max_doc_id.?, &self.write_profile);
            chunk_count += 1;
            cursor += take;
        }

        phase_start_ns = nowNs();
        const meta = encodeTermMeta(max_weight, chunk_count);
        try txnAppendPut(txn, self.dbi, mk, &meta);
        if (term_min_doc_id != null and term_max_doc_id != null) {
            try self.writeTermRangeMeta(alloc, txn, term_id, term_min_doc_id.?, term_max_doc_id.?);
        }
        self.write_profile.term_meta_ns += elapsedSince(phase_start_ns);
        return created_term;
    }

    const DeleteEffect = struct {
        deleted_doc: bool = false,
        removed_terms: u64 = 0,
    };

    fn applyDeleteEffect(self: *SparseIndex, effect: DeleteEffect) void {
        if (effect.deleted_doc and self.doc_count > 0) self.doc_count -= 1;
        self.term_count = if (effect.removed_terms > self.term_count) 0 else self.term_count - effect.removed_terms;
    }

    fn processDelete(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_id: []const u8,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !DeleteEffect {
        // Read forward entry
        var key_buf: [256]u8 = undefined;
        const fk = fwdKey(&key_buf, doc_id);
        const fwd = self.lookupFwdEntryByDocIdAlloc(alloc, txn, doc_id) catch |err| switch (err) {
            error.NotFound => return .{}, // doc not found, nothing to delete
            else => return err,
        };
        defer alloc.free(fwd.term_ids);
        defer alloc.free(fwd.weights);

        var effect = DeleteEffect{ .deleted_doc = true };
        // Remove from posting chunks for each term
        for (fwd.term_ids) |term_id| {
            if (try self.removeFromPostings(alloc, txn, term_id, @intCast(fwd.doc_num), touched_terms)) {
                effect.removed_terms += 1;
            }
        }

        // Delete forward and reverse entries
        txnDelete(txn, self.dbi, fk) catch {};
        var rev_buf: [256]u8 = undefined;
        const rk = revKey(&rev_buf, fwd.doc_num);
        txnDelete(txn, self.dbi, rk) catch {};
        var tombstone_buf: [16]u8 = undefined;
        try txnPut(txn, self.dbi, docTombstoneKey(&tombstone_buf, fwd.doc_num), &.{});
        return effect;
    }

    fn processInsert(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_id: []const u8,
        vec: SparseVector,
        preferred_doc_num: ?u32,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !void {
        // If doc_id already exists, delete the old mapping first to avoid orphaning
        self.applyDeleteEffect(try self.processDelete(alloc, txn, doc_id, touched_terms));

        const doc_num = try self.allocateDocNumForInsert(txn, doc_id, preferred_doc_num);
        _ = try advanceIncarnation(txn, doc_num);
        self.doc_count += 1;
        var tombstone_buf: [16]u8 = undefined;
        txnDelete(txn, self.dbi, docTombstoneKey(&tombstone_buf, doc_num)) catch {};

        // Write forward entry
        const fwd_data = try encodeFwdEntry(alloc, doc_num, vec.indices, vec.values);
        defer alloc.free(fwd_data);
        var fwd_key_buf: [256]u8 = undefined;
        const fk = fwdKey(&fwd_key_buf, doc_id);
        try txnPut(txn, self.dbi, fk, fwd_data);

        // Write reverse entry (docNum → docID)
        var rev_key_buf: [256]u8 = undefined;
        const rk = revKey(&rev_key_buf, doc_num);
        try txnPut(txn, self.dbi, rk, doc_id);

        // Update posting chunks for each term
        if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
        const doc_num_u32: u32 = @intCast(doc_num);
        for (vec.indices, 0..) |term_id, i| {
            if (try self.addToPostings(alloc, txn, term_id, doc_id, doc_num_u32, vec.values[i], touched_terms)) {
                self.term_count += 1;
            }
        }
    }

    fn allocateDocNumForInsert(self: *SparseIndex, txn: anytype, doc_id: []const u8, preferred_doc_num: ?u32) !u64 {
        if (preferred_doc_num) |doc_num_u32| {
            const doc_num: u64 = doc_num_u32;
            var rev_buf: [256]u8 = undefined;
            const existing = txn.get(revKey(&rev_buf, doc_num)) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (existing == null or std.mem.eql(u8, existing.?, doc_id)) {
                if (doc_num >= self.next_doc_num) self.next_doc_num = doc_num + 1;
                return doc_num;
            }
        }

        const doc_num = self.next_doc_num;
        self.next_doc_num += 1;
        return doc_num;
    }

    fn addToPostings(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        doc_id: []const u8,
        doc_num: u32,
        weight: f32,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !bool {
        // Read term metadata
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);
        var chunk_count: u32 = 0;
        var max_weight: f32 = weight;
        var created_term = false;

        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (meta_data) |d| {
            const tm = decodeTermMeta(d);
            chunk_count = tm.chunk_count;
            max_weight = @max(tm.max_weight, weight);
        } else {
            created_term = true;
        }

        // Find the right chunk to insert into (last chunk or create new one)
        if (chunk_count == 0) {
            // Create first chunk
            try self.writeChunkWithRangeMeta(alloc, txn, term_id, 0, &.{doc_num}, &.{weight}, doc_id, doc_id);
            chunk_count = 1;
        } else {
            // Append to last chunk
            const last_chunk_idx = chunk_count - 1;
            var ck_buf: [256]u8 = undefined;
            const ck = invChunkKey(&ck_buf, term_id, last_chunk_idx);
            const chunk_data = try txnGet(txn, self.dbi, ck);

            const decoded = try decodeChunk(alloc, chunk_data);
            defer alloc.free(decoded.doc_nums);
            defer alloc.free(decoded.weights);

            if (decoded.doc_nums.len >= self.chunk_size) {
                // Create new chunk
                try self.writeChunkWithRangeMeta(alloc, txn, term_id, chunk_count, &.{doc_num}, &.{weight}, doc_id, doc_id);
                chunk_count += 1;
            } else {
                // Append to existing chunk
                const new_len = decoded.doc_nums.len + 1;
                var new_doc_nums = try alloc.alloc(u32, new_len);
                defer alloc.free(new_doc_nums);
                var new_weights = try alloc.alloc(f32, new_len);
                defer alloc.free(new_weights);

                @memcpy(new_doc_nums[0..decoded.doc_nums.len], decoded.doc_nums);
                new_doc_nums[decoded.doc_nums.len] = doc_num;
                @memcpy(new_weights[0..decoded.weights.len], decoded.weights);
                new_weights[decoded.weights.len] = weight;
                sortDocNumsAndWeights(new_doc_nums, new_weights);

                const existing_range = self.readChunkRangeMeta(txn, term_id, last_chunk_idx) catch null;
                const min_doc_id = if (existing_range) |range|
                    if (std.mem.order(u8, doc_id, range.min_doc_id) == .lt) doc_id else range.min_doc_id
                else
                    doc_id;
                const max_doc_id = if (existing_range) |range|
                    if (std.mem.order(u8, doc_id, range.max_doc_id) == .gt) doc_id else range.max_doc_id
                else
                    doc_id;
                try self.writeChunkWithRangeMeta(alloc, txn, term_id, last_chunk_idx, new_doc_nums, new_weights, min_doc_id, max_doc_id);
            }
        }

        // Update term metadata
        const meta = encodeTermMeta(max_weight, chunk_count);
        try txnPut(txn, self.dbi, mk, &meta);
        if (created_term) _ = try self.ensureTermCatalogEntry(txn, term_id);
        if (touched_terms) |map| {
            try map.put(self.alloc, term_id, {});
        } else {
            try self.updateTermRangeMetaOnInsert(alloc, txn, term_id, doc_id);
        }
        return created_term;
    }

    fn removeFromPostings(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        doc_num: u32,
        touched_terms: ?*std.AutoHashMapUnmanaged(u32, void),
    ) !bool {
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);

        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const tm = decodeTermMeta(meta_data);

        for (0..tm.chunk_count) |ci| {
            var ck_buf: [256]u8 = undefined;
            const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
            const chunk_data = txnGet(txn, self.dbi, ck) catch continue;

            const decoded = try decodeChunk(alloc, chunk_data);
            defer alloc.free(decoded.doc_nums);
            defer alloc.free(decoded.weights);

            // Find and remove doc_num
            var found: ?usize = null;
            for (decoded.doc_nums, 0..) |dn, i| {
                if (dn == doc_num) {
                    found = i;
                    break;
                }
            }
            if (found) |idx| {
                if (decoded.doc_nums.len == 1) {
                    // Remove empty chunk
                    txnDelete(txn, self.dbi, ck) catch {};
                    var meta_ck_buf: [256]u8 = undefined;
                    txnDelete(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch {};
                    // Update metadata chunk_count
                    const new_meta = encodeTermMeta(tm.max_weight, tm.chunk_count - 1);
                    var term_range_buf: [256]u8 = undefined;
                    if (tm.chunk_count - 1 == 0) {
                        txnDelete(txn, self.dbi, mk) catch {};
                        txnDelete(txn, self.dbi, termRangeKey(&term_range_buf, term_id)) catch {};
                        var catalog_buf: [16]u8 = undefined;
                        txnDelete(txn, self.dbi, termCatalogKey(&catalog_buf, term_id)) catch {};
                        return true;
                    } else {
                        try txnPut(txn, self.dbi, mk, &new_meta);
                        if (touched_terms) |map| {
                            try map.put(self.alloc, term_id, {});
                        } else {
                            try self.recomputeTermRangeMeta(alloc, txn, term_id, tm.chunk_count - 1);
                        }
                    }
                } else {
                    // Remove entry and re-encode
                    const new_len = decoded.doc_nums.len - 1;
                    var new_dns = try alloc.alloc(u32, new_len);
                    defer alloc.free(new_dns);
                    var new_ws = try alloc.alloc(f32, new_len);
                    defer alloc.free(new_ws);

                    var wi: usize = 0;
                    for (0..decoded.doc_nums.len) |i| {
                        if (i != idx) {
                            new_dns[wi] = decoded.doc_nums[i];
                            new_ws[wi] = decoded.weights[i];
                            wi += 1;
                        }
                    }

                    const range = try self.computeChunkRangeMeta(alloc, txn, new_dns);
                    defer {
                        alloc.free(range.min_doc_id);
                        alloc.free(range.max_doc_id);
                    }
                    try self.writeChunkWithRangeMeta(alloc, txn, term_id, @intCast(ci), new_dns, new_ws, range.min_doc_id, range.max_doc_id);
                    if (touched_terms) |map| {
                        try map.put(self.alloc, term_id, {});
                    } else {
                        try self.recomputeTermRangeMeta(alloc, txn, term_id, tm.chunk_count);
                    }
                }
                return false;
            }
        }
        return false;
    }

    fn writeChunkWithRangeMeta(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        chunk_idx: u32,
        doc_nums: []const u32,
        weights: []const f32,
        min_doc_id: []const u8,
        max_doc_id: []const u8,
    ) !void {
        try self.writeChunkWithRangeMetaProfiled(alloc, txn, term_id, chunk_idx, doc_nums, weights, min_doc_id, max_doc_id, null);
    }

    fn writeChunkWithRangeMetaProfiled(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        term_id: u32,
        chunk_idx: u32,
        doc_nums: []const u32,
        weights: []const f32,
        min_doc_id: []const u8,
        max_doc_id: []const u8,
        profile: ?*WriteProfile,
    ) !void {
        var phase_start_ns = nowNs();
        const encoded = try encodeChunk(alloc, doc_nums, weights);
        if (profile) |active_profile| active_profile.chunk_encode_ns += elapsedSince(phase_start_ns);
        defer alloc.free(encoded);
        var ck_buf: [256]u8 = undefined;
        phase_start_ns = nowNs();
        try txnAppendPut(txn, self.dbi, invChunkKey(&ck_buf, term_id, chunk_idx), encoded);
        if (profile) |active_profile| active_profile.chunk_put_ns += elapsedSince(phase_start_ns);

        phase_start_ns = nowNs();
        const meta = try encodeChunkOrdinalRange(alloc, min_doc_id, max_doc_id, doc_nums, false);
        if (profile) |active_profile| active_profile.range_meta_encode_ns += elapsedSince(phase_start_ns);
        defer alloc.free(meta);
        var meta_ck_buf: [256]u8 = undefined;
        phase_start_ns = nowNs();
        try txnAppendPut(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx), meta);
        if (profile) |active_profile| active_profile.range_meta_put_ns += elapsedSince(phase_start_ns);
    }

    fn writeTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, min_doc_id: []const u8, max_doc_id: []const u8) !void {
        const meta = try encodeChunkRangeMeta(alloc, min_doc_id, max_doc_id);
        defer alloc.free(meta);
        var key_buf: [256]u8 = undefined;
        try txnAppendPut(txn, self.dbi, termRangeKey(&key_buf, term_id), meta);
    }

    fn updateTermRangeMetaOnInsert(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, doc_id: []const u8) !void {
        var key_buf: [256]u8 = undefined;
        const existing = txnGet(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch null;
        if (existing) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            const min_doc_id = if (std.mem.order(u8, doc_id, range.min_doc_id) == .lt) doc_id else range.min_doc_id;
            const max_doc_id = if (std.mem.order(u8, doc_id, range.max_doc_id) == .gt) doc_id else range.max_doc_id;
            try self.writeTermRangeMeta(alloc, txn, term_id, min_doc_id, max_doc_id);
            return;
        }

        try self.writeTermRangeMeta(alloc, txn, term_id, doc_id, doc_id);
    }

    fn recomputeTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32, chunk_count: u32) !void {
        var min_doc_id: ?[]const u8 = null;
        var max_doc_id: ?[]const u8 = null;

        for (0..chunk_count) |ci| {
            const range = self.readChunkRangeMeta(txn, term_id, @intCast(ci)) catch continue;
            if (min_doc_id == null or std.mem.order(u8, range.min_doc_id, min_doc_id.?) == .lt) {
                min_doc_id = range.min_doc_id;
            }
            if (max_doc_id == null or std.mem.order(u8, range.max_doc_id, max_doc_id.?) == .gt) {
                max_doc_id = range.max_doc_id;
            }
        }

        var key_buf: [256]u8 = undefined;
        if (min_doc_id == null or max_doc_id == null) {
            txnDelete(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch {};
            return;
        }

        try self.writeTermRangeMeta(alloc, txn, term_id, min_doc_id.?, max_doc_id.?);
    }

    fn refreshTermRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, term_id: u32) !void {
        var meta_key_buf: [256]u8 = undefined;
        const mk = invMetaKey(&meta_key_buf, term_id);
        const meta_data = txnGet(txn, self.dbi, mk) catch |err| switch (err) {
            error.NotFound => {
                var key_buf: [256]u8 = undefined;
                txnDelete(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch {};
                return;
            },
            else => return err,
        };
        const tm = decodeTermMeta(meta_data);
        if (tm.chunk_count == 0) {
            var key_buf: [256]u8 = undefined;
            txnDelete(txn, self.dbi, termRangeKey(&key_buf, term_id)) catch {};
            return;
        }
        try self.recomputeTermRangeMeta(alloc, txn, term_id, tm.chunk_count);
    }

    fn computeChunkRangeMeta(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_nums: []const u32) !struct { min_doc_id: []u8, max_doc_id: []u8 } {
        std.debug.assert(doc_nums.len > 0);
        var min_doc_id: ?[]u8 = null;
        var max_doc_id: ?[]u8 = null;
        errdefer {
            if (min_doc_id) |doc_id| alloc.free(doc_id);
            if (max_doc_id) |doc_id| alloc.free(doc_id);
        }

        for (doc_nums) |doc_num| {
            const doc_id = try self.resolveDocIdByDocNum(alloc, txn, doc_num);
            defer alloc.free(doc_id);
            if (min_doc_id == null or std.mem.order(u8, doc_id, min_doc_id.?) == .lt) {
                if (min_doc_id) |existing| alloc.free(existing);
                min_doc_id = try alloc.dupe(u8, doc_id);
            }
            if (max_doc_id == null or std.mem.order(u8, doc_id, max_doc_id.?) == .gt) {
                if (max_doc_id) |existing| alloc.free(existing);
                max_doc_id = try alloc.dupe(u8, doc_id);
            }
        }

        return .{
            .min_doc_id = min_doc_id.?,
            .max_doc_id = max_doc_id.?,
        };
    }

    fn resolveDocIdByDocNum(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_num: u32) ![]u8 {
        if (self.docNumDeleted(txn, doc_num)) return error.NotFound;
        var rev_buf: [256]u8 = undefined;
        if (txnGet(txn, self.dbi, revKey(&rev_buf, doc_num))) |doc_id| {
            return alloc.dupe(u8, doc_id);
        } else |_| {}

        const LookupContext = struct {
            wanted_doc_num: u64,
            found: ?[]const u8 = null,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (entry.doc_num == ctx.wanted_doc_num) {
                    ctx.found = entry.doc_id;
                    return true;
                }
                return false;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{ .wanted_doc_num = doc_num };
            if (try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &ctx, LookupContext.visit)) {
                return alloc.dupe(u8, ctx.found.?);
            }
            maybe_entry = try cur.next();
        }
        return error.NotFound;
    }

    fn appendCollectedSparseDoc(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        collected: *CollectedSparseDocs,
        doc_id: []const u8,
        fwd_data: []const u8,
        collect_doc_ids: bool,
    ) !void {
        const decoded = try decodeFwdEntry(alloc, fwd_data);
        defer alloc.free(decoded.term_ids);
        defer alloc.free(decoded.weights);
        if (decoded.doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
        if (self.docNumDeleted(txn, decoded.doc_num)) return;

        const doc_num_u32: u32 = @intCast(decoded.doc_num);
        const gop = try collected.selected_doc_nums.getOrPut(alloc, doc_num_u32);
        if (gop.found_existing) return;
        errdefer _ = collected.selected_doc_nums.remove(doc_num_u32);

        const owned_doc_id = try alloc.dupe(u8, doc_id);
        const owned_indices = try alloc.dupe(u32, decoded.term_ids);
        const owned_values = try alloc.dupe(f32, decoded.weights);
        var write_ownership_transferred = false;
        errdefer if (!write_ownership_transferred) {
            alloc.free(owned_doc_id);
            alloc.free(owned_indices);
            alloc.free(owned_values);
        };

        try collected.term_ids.appendSlice(alloc, decoded.term_ids);
        try collected.writes.append(alloc, .{
            .doc_id = owned_doc_id,
            .vec = .{
                .indices = owned_indices,
                .values = owned_values,
            },
            .doc_num = doc_num_u32,
        });
        write_ownership_transferred = true;
        if (collect_doc_ids) {
            const listed_doc_id = try alloc.dupe(u8, doc_id);
            errdefer alloc.free(listed_doc_id);
            try collected.doc_ids.append(alloc, listed_doc_id);
        }
    }

    fn collectRangeSparseDocs(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        lower: []const u8,
        upper: []const u8,
        collect_doc_ids: bool,
    ) !CollectedSparseDocs {
        var collected = CollectedSparseDocs{ .alloc = alloc };
        errdefer collected.deinit();

        var cur = try txn.openCursor();
        defer cur.close();

        const fwd_first = if (lower.len == 0)
            try cur.seekAtOrAfter(taggedPrefix(key_fwd))
        else blk: {
            const start_key = try fwdKeyAlloc(alloc, lower);
            defer alloc.free(start_key);
            break :blk try cur.seekAtOrAfter(start_key);
        };

        var maybe_entry = fwd_first;
        while (maybe_entry) |entry| {
            const doc_id = fwdDocIdFromKey(entry.key) orelse break;
            if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) break;
            if (docIdInOwnedRange(doc_id, lower, upper)) {
                try self.appendCollectedSparseDoc(alloc, txn, &collected, doc_id, entry.value, collect_doc_ids);
            }
            maybe_entry = try cur.next();
        }

        const DocMapContext = struct {
            index: *SparseIndex,
            alloc: Allocator,
            txn: @TypeOf(txn),
            lower: []const u8,
            upper: []const u8,
            collect_doc_ids: bool,
            collected: *CollectedSparseDocs,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (!docIdInOwnedRange(entry.doc_id, ctx.lower, ctx.upper)) return false;
                try ctx.index.appendCollectedSparseDoc(ctx.alloc, ctx.txn, ctx.collected, entry.doc_id, entry.fwd_data, ctx.collect_doc_ids);
                return false;
            }
        };
        var docmap_ctx = DocMapContext{
            .index = self,
            .alloc = alloc,
            .txn = txn,
            .lower = lower,
            .upper = upper,
            .collect_doc_ids = collect_doc_ids,
            .collected = &collected,
        };
        maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            _ = try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &docmap_ctx, DocMapContext.visit);
            maybe_entry = try cur.next();
        }

        return collected;
    }

    fn collectPreparedSparseDocs(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_ids_in: []const []const u8,
        collect_doc_ids: bool,
    ) !CollectedSparseDocs {
        var collected = CollectedSparseDocs{ .alloc = alloc };
        errdefer collected.deinit();

        for (doc_ids_in) |doc_id| {
            const fwd = self.lookupFwdEntryByDocIdAlloc(alloc, txn, doc_id) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            defer {
                alloc.free(fwd.term_ids);
                alloc.free(fwd.weights);
            }
            const fwd_data = try encodeFwdEntry(alloc, fwd.doc_num, fwd.term_ids, fwd.weights);
            defer alloc.free(fwd_data);
            try self.appendCollectedSparseDoc(alloc, txn, &collected, doc_id, fwd_data, collect_doc_ids);
        }

        return collected;
    }

    fn docNumDeleted(self: *SparseIndex, txn: anytype, doc_num: u64) bool {
        _ = self;
        var tombstone_buf: [16]u8 = undefined;
        _ = txnGet(txn, {}, docTombstoneKey(&tombstone_buf, doc_num)) catch return false;
        return true;
    }

    fn resolveSearchCandidateDocIds(self: *SparseIndex, alloc: Allocator, txn: anytype, candidates: []SearchCandidate) !void {
        var wanted = std.AutoHashMapUnmanaged(u32, usize).empty;
        defer wanted.deinit(alloc);

        for (candidates, 0..) |candidate, i| {
            if (self.docNumDeleted(txn, candidate.doc_num)) continue;
            try wanted.put(alloc, candidate.doc_num, i);
        }
        if (wanted.count() == 0) return;

        for (candidates) |*candidate| {
            if (!wanted.contains(candidate.doc_num)) continue;
            var rev_buf: [256]u8 = undefined;
            const doc_id = txnGet(txn, self.dbi, revKey(&rev_buf, candidate.doc_num)) catch continue;
            candidate.doc_id = try alloc.dupe(u8, doc_id);
            _ = wanted.remove(candidate.doc_num);
            if (wanted.count() == 0) return;
        }

        const LookupContext = struct {
            alloc: Allocator,
            wanted: *std.AutoHashMapUnmanaged(u32, usize),
            candidates: []SearchCandidate,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (entry.doc_num > std.math.maxInt(u32)) return false;
                const doc_num: u32 = @intCast(entry.doc_num);
                const idx = ctx.wanted.get(doc_num) orelse return false;
                if (ctx.candidates[idx].doc_id == null) {
                    ctx.candidates[idx].doc_id = try ctx.alloc.dupe(u8, entry.doc_id);
                }
                _ = ctx.wanted.remove(doc_num);
                return ctx.wanted.count() == 0;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{
                .alloc = alloc,
                .wanted = &wanted,
                .candidates = candidates,
            };
            if (try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &ctx, LookupContext.visit)) return;
            maybe_entry = try cur.next();
        }
    }

    fn lookupDocNumByDocId(self: *SparseIndex, txn: anytype, doc_id: []const u8) !u64 {
        var key_buf: [256]u8 = undefined;
        if (txnGet(txn, self.dbi, fwdKey(&key_buf, doc_id))) |fwd_data| {
            const doc_num = try decodeFwdDocNum(fwd_data);
            if (self.docNumDeleted(txn, doc_num)) return error.NotFound;
            return doc_num;
        } else |_| {}

        if (try readLocator(txn, doc_id)) |locator| {
            if (self.docNumDeleted(txn, locator.num)) return error.NotFound;
            return locator.num;
        }
        if (try completeLocatorMap(txn)) return error.NotFound;

        const LookupContext = struct {
            wanted_doc_id: []const u8,
            index: *SparseIndex,
            txn: @TypeOf(txn),
            found: ?u64 = null,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (std.mem.eql(u8, entry.doc_id, ctx.wanted_doc_id)) {
                    if (ctx.index.docNumDeleted(ctx.txn, entry.doc_num)) return false;
                    ctx.found = entry.doc_num;
                    return true;
                }
                return false;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{ .wanted_doc_id = doc_id, .index = self, .txn = txn };
            if (try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &ctx, LookupContext.visit)) return ctx.found.?;
            maybe_entry = try cur.next();
        }
        return error.NotFound;
    }

    fn lookupFwdEntryByDocIdAlloc(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_id: []const u8) !DecodedFwdEntry {
        var key_buf: [256]u8 = undefined;
        if (txnGet(txn, self.dbi, fwdKey(&key_buf, doc_id))) |fwd_data| {
            const doc_num = try decodeFwdDocNum(fwd_data);
            if (self.docNumDeleted(txn, doc_num)) return error.NotFound;
            return try decodeFwdEntry(alloc, fwd_data);
        } else |_| {}

        if (try locatedForwardBytes(txn, doc_id)) |bytes| {
            const num = try decodeFwdDocNum(bytes);
            if (self.docNumDeleted(txn, num)) return error.NotFound;
            return try decodeFwdEntry(alloc, bytes);
        }
        if (try completeLocatorMap(txn)) return error.NotFound;

        const LookupContext = struct {
            alloc: Allocator,
            wanted_doc_id: []const u8,
            index: *SparseIndex,
            txn: @TypeOf(txn),
            found: ?DecodedFwdEntry = null,

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (std.mem.eql(u8, entry.doc_id, ctx.wanted_doc_id)) {
                    if (ctx.index.docNumDeleted(ctx.txn, entry.doc_num)) return false;
                    ctx.found = try decodeFwdEntry(ctx.alloc, entry.fwd_data);
                    return true;
                }
                return false;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{ .alloc = alloc, .wanted_doc_id = doc_id, .index = self, .txn = txn };
            if (try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &ctx, LookupContext.visit)) return ctx.found.?;
            maybe_entry = try cur.next();
        }
        return error.NotFound;
    }

    fn readChunkRangeMeta(self: *SparseIndex, txn: anytype, term_id: u32, chunk_idx: u32) !ChunkRangeMeta {
        var meta_ck_buf: [256]u8 = undefined;
        const data = try txnGet(txn, self.dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx));
        return decodeChunkRangeMeta(data);
    }

    pub fn search(self: *SparseIndex, alloc: Allocator, query_vec: *const SparseVector, k: u32) ![]SearchResult {
        return try self.searchConstrained(alloc, query_vec, k, .{});
    }

    pub fn searchConstrained(
        self: *SparseIndex,
        alloc: Allocator,
        query_vec: *const SparseVector,
        k: u32,
        constraints: SearchConstraints,
    ) ![]SearchResult {
        try checkSearchCancellation(constraints.cancellation);
        const profile_enabled = sparseSearchProfileEnabled();
        const total_start_ns = if (profile_enabled) nowNs() else 0;
        var profile: SearchProfile = .{};
        var txn = try self.beginReadTxn();
        defer txn.abort();

        const filter_start_ns = if (profile_enabled) nowNs() else 0;
        var filter_doc_nums = try self.resolveDocNumSetAlloc(alloc, &txn, constraints.filter_doc_ids);
        defer filter_doc_nums.deinit(alloc);
        if (constraints.filter_doc_ids.len > 0 and filter_doc_nums.count() == 0) {
            return try alloc.alloc(SearchResult, 0);
        }
        var direct_filter_doc_nums = try self.docNumSetFromSliceAlloc(alloc, constraints.filter_doc_nums);
        defer direct_filter_doc_nums.deinit(alloc);
        if (constraints.filter_doc_nums.len > 0 and direct_filter_doc_nums.count() == 0) {
            return try alloc.alloc(SearchResult, 0);
        }

        var exclude_doc_nums = try self.resolveDocNumSetAlloc(alloc, &txn, constraints.exclude_doc_ids);
        defer exclude_doc_nums.deinit(alloc);
        var direct_exclude_doc_nums = try self.docNumSetFromSliceAlloc(alloc, constraints.exclude_doc_nums);
        defer direct_exclude_doc_nums.deinit(alloc);
        if (profile_enabled) profile.filter_resolve_ns = nowNs() - filter_start_ns;

        const Lookup = struct {
            index: *SparseIndex,
            txn: @TypeOf(&txn),
            cancellation: ?CancellationToken,
            fn block(raw: *anyopaque, a: Allocator, prefix: []const u8, high: u32, rows: *const @import("../encoding/roaring.zig").RoaringBitmap, result: *@import("../encoding/roaring.zig").RoaringBitmap) !bool {
                const ctx: *@This() = @ptrCast(@alignCast(raw));
                return selectPhysicalBlock(ctx.txn, a, prefix, high, rows, result, ctx.cancellation);
            }
            fn boundedBlock(raw: *anyopaque, a: Allocator, prefix: []const u8, high: u32, rows: *const @import("../encoding/roaring.zig").RoaringBitmap, result: *@import("../encoding/roaring.zig").RoaringBitmap, work: *@import("ordinal_lookup.zig").WorkBudget) !bool {
                const ctx: *@This() = @ptrCast(@alignCast(raw));
                return selectPhysicalBlockBounded(ctx.txn, a, prefix, high, rows, result, ctx.cancellation, work);
            }
            fn nativeRange(raw: *anyopaque, _: Allocator, first: u32, last: u32, filter: @import("ordinal_lookup.zig").KeyFilter, result: *@import("../encoding/roaring.zig").RoaringBitmap) !bool {
                const ctx: *@This() = @ptrCast(@alignCast(raw));
                if (@as(u64, last) + 1 - first > 1024) return error.InvalidSparsePhysicalMap;
                if (!try completeLocatorMap(ctx.txn)) return false;
                var cursor = try ctx.txn.openCursor();
                defer cursor.close();
                var key: [9]u8 = undefined;
                var next = try cursor.seekAtOrAfter(revKey(&key, first));
                while (next) |entry| {
                    try checkSearchCancellation(ctx.cancellation);
                    if (entry.key.len != 9 or entry.key[0] != key_rev) break;
                    const ordinal = std.mem.readInt(u64, entry.key[1..9], .big);
                    if (ordinal > last) break;
                    if (ordinal < first) return error.InvalidSparsePhysicalMap;
                    if (try filter.allows(filter.ptr, entry.value)) try result.add(@intCast(ordinal));
                    next = try cursor.next();
                }
                return true;
            }
            fn lookup(raw: *anyopaque, key: []const u8) !?u32 {
                const ctx: *@This() = @ptrCast(@alignCast(raw));
                return ctx.index.docNumForDocIdTxn(ctx.txn, key);
            }
        };
        var lookup: Lookup = .{ .index = self, .txn = &txn, .cancellation = constraints.cancellation };
        const complete_native_map = try completeLocatorMap(&txn);
        var ordinal_state: AdaptiveOrdinalConstraints = .{ .a = alloc, .predicate = constraints.key_predicate, .lookup = .{ .ptr = &lookup, .one = Lookup.lookup, .block = Lookup.block, .bounded_block = Lookup.boundedBlock, .native_range = if (complete_native_map) Lookup.nativeRange else null } };
        defer ordinal_state.deinit();
        try ordinal_state.load();
        if (constraints.key_predicate == null and ordinal_state.include == null and (filter_doc_nums.count() != 0 or direct_filter_doc_nums.count() != 0)) {
            ordinal_state.include = @import("../encoding/roaring.zig").RoaringBitmap.init(alloc);
            const first = if (filter_doc_nums.count() != 0) &filter_doc_nums else &direct_filter_doc_nums;
            var it = first.keyIterator();
            while (it.next()) |num| {
                if (filter_doc_nums.count() != 0 and direct_filter_doc_nums.count() != 0 and !direct_filter_doc_nums.contains(num.*)) continue;
                try ordinal_state.include.?.add(num.*);
            }
        }
        if (ordinal_state.include) |*bitmap| {
            try bitmap.prepareRead();
            if (bitmap.isEmpty()) return try alloc.alloc(SearchResult, 0);
        }

        // Accumulate scores: docNum → score
        var score_spill = constraints.score_spill;
        if (score_spill) |*options| if (options.resource_manager == null) {
            options.resource_manager = self.resource_manager;
        };
        var scores: ScoreAccumulator = .{ .alloc = alloc, .options = score_spill, .limit = constraints.max_score_docs, .cancellation = constraints.cancellation };
        defer scores.deinit();
        var incarnations: IncarnationCache = .{};
        defer incarnations.deinit(alloc);
        var visibility = try VisibilityReader.init(&txn);
        defer visibility.deinit();

        // Point-seek the native reverse identity only for postings reached by
        // this query. Bound decision reuse across terms; bulk publication
        // already persists reverse keys, so no corpus dictionary pass is needed.
        const Decisions = struct {
            values: PredicateDecisionCache = .{},
            complete: bool,
            identities: ?backend_erased.Cursor = null,
        };
        var decisions: Decisions = .{ .complete = complete_native_map };
        defer if (decisions.identities) |*cursor| cursor.close();
        const ScoreSource = enum { segment, delta };
        const AccumulateContext = struct {
            alloc: Allocator,
            txn: @TypeOf(&txn),
            incarnations: *IncarnationCache,
            visibility: *VisibilityReader,
            segment_id: ?u64 = null,
            segment_version: u32 = 1,
            query_weight: f32,
            scores: *ScoreAccumulator,
            filter_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            direct_filter_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            exclude_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            direct_exclude_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
            profile: ?*SearchProfile,
            source: ScoreSource,
            cancellation: ?CancellationToken,
            key_predicate: ?KeyPredicate,
            ordinals: *AdaptiveOrdinalConstraints,
            index: *SparseIndex,
            decisions: *Decisions,

            shared_cutoff: ?*SparseCompetitiveCutoff = null,
            pub fn competitiveCutoff(ctx: *@This()) ?daat.Entry {
                return if (ctx.shared_cutoff) |cutoff| cutoff.get() else null;
            }
            pub fn publishCompetitiveCutoff(ctx: *@This(), entry: daat.Entry) void {
                if (ctx.shared_cutoff) |cutoff| cutoff.publish(entry);
            }

            pub fn check(ctx: *@This()) !void {
                try checkSearchCancellation(ctx.cancellation);
            }
            pub fn nextCandidate(ctx: *@This(), first: u32) !u64 {
                try ctx.ordinals.refresh();
                const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
                var includes: [2]*const Bitmap = undefined;
                var excludes: [2]*const Bitmap = undefined;
                var ni: usize = 0;
                var ne: usize = 0;
                if (ctx.ordinals.include) |*bitmap| {
                    includes[0] = bitmap;
                    ni = 1;
                }
                if (ctx.ordinals.exclude) |*bitmap| {
                    excludes[0] = bitmap;
                    ne = 1;
                }
                var end: u64 = 0x1_0000_0000;
                if (ctx.ordinals.range != null and first >= ctx.ordinals.range_first and first <= ctx.ordinals.range_last) {
                    const selection = &ctx.ordinals.range.?;
                    end = @as(u64, ctx.ordinals.range_last) + 1;
                    if (selection.include) |*bitmap| {
                        includes[ni] = bitmap;
                        ni += 1;
                    }
                    if (selection.exclude) |*bitmap| {
                        excludes[ne] = bitmap;
                        ne += 1;
                    }
                }
                return Bitmap.candidateLowerBound(first, end, includes[0..ni], excludes[0..ne]);
            }
            /// Called only after score bounds admit the candidate. Sparse
            /// winners may need just a few exact points, never an eager window.
            pub fn refineCandidate(ctx: *@This(), first: u32) !u64 {
                try ctx.ordinals.refineRange(first);
                return ctx.nextCandidate(first);
            }
            pub fn mayMatch(ctx: *@This(), first: u32, last: u32) bool {
                if (ctx.ordinals.include) |*bitmap| if (bitmap.rangeCardinality(first, @as(u64, last) + 1) == 0) return false;
                if (ctx.ordinals.exclude) |*bitmap| if (bitmap.rangeCardinality(first, @as(u64, last) + 1) == @as(u64, last) + 1 - first) return false;
                return true;
            }
            fn allowsKey(ctx: *@This(), doc_num: u32) !bool {
                if (!ctx.ordinals.resolvedAt(doc_num)) if (ctx.key_predicate) |predicate| {
                    if (ctx.decisions.values.get(doc_num)) |allowed| return allowed;
                    if (ctx.index.docNumDeleted(ctx.visibility, doc_num)) return false;
                    var owned: ?[]u8 = null;
                    defer if (owned) |id| ctx.alloc.free(id);
                    const id = if (ctx.decisions.complete) blk: {
                        // Borrow one reverse identity from independent cursor
                        // scratch. Point gets would retain every reached key
                        // until the parent transaction closes on arena backends.
                        if (ctx.decisions.identities == null) ctx.decisions.identities = try ctx.txn.openCursor();
                        var key: [9]u8 = undefined;
                        const wanted = revKey(&key, doc_num);
                        const entry = (try ctx.decisions.identities.?.seekAtOrAfter(wanted)) orelse return false;
                        if (!std.mem.eql(u8, entry.key, wanted)) return false;
                        break :blk entry.value;
                    } else blk: {
                        owned = ctx.index.resolveDocIdByDocNum(ctx.alloc, ctx.txn, doc_num) catch |err| switch (err) {
                            error.NotFound => return false,
                            else => return err,
                        };
                        break :blk owned.?;
                    };
                    const allowed = try predicate.allows(predicate.ptr, id);
                    ctx.decisions.values.put(doc_num, allowed);
                    return allowed;
                };
                return true;
            }
            pub fn allows(ctx: *@This(), stream: daat.Stream, doc_num: u32) !bool {
                if (!try ctx.ordinals.allowsOrdinal(doc_num)) return false;
                if (ctx.filter_doc_nums.count() > 0 and !ctx.filter_doc_nums.contains(doc_num)) return false;
                if (ctx.direct_filter_doc_nums.count() > 0 and !ctx.direct_filter_doc_nums.contains(doc_num)) return false;
                if (ctx.exclude_doc_nums.contains(doc_num) or ctx.direct_exclude_doc_nums.contains(doc_num)) return false;
                if (ctx.index.docNumDeleted(ctx.visibility, doc_num)) return false;
                if (stream.segment) |id| if (!try ctx.incarnations.matches(ctx.alloc, ctx.visibility, id, stream.version, doc_num)) return false;
                return ctx.allowsKey(doc_num);
            }
            fn shouldDecode(ctx: *@This(), bytes: []const u8, range: []const u8) !bool {
                try checkSearchCancellation(ctx.cancellation);
                try ctx.ordinals.refresh();
                return postingRangeMayMatch(range, bytes, if (ctx.ordinals.include) |*bitmap| bitmap else null);
            }
            fn visit(ctx: *@This(), decoded: DecodedChunk) !void {
                const collect_start_ns = if (ctx.profile != null) nowNs() else 0;
                for (decoded.doc_nums, 0..) |doc_num, di| {
                    try ctx.ordinals.refresh();
                    if (di % 256 == 0) try checkSearchCancellation(ctx.cancellation);
                    try ctx.ordinals.refineRange(doc_num);
                    if (!try ctx.ordinals.allowsOrdinal(doc_num)) continue;
                    if (ctx.filter_doc_nums.count() > 0 and !ctx.filter_doc_nums.contains(doc_num)) continue;
                    if (ctx.direct_filter_doc_nums.count() > 0 and !ctx.direct_filter_doc_nums.contains(doc_num)) continue;
                    if (ctx.exclude_doc_nums.contains(doc_num)) continue;
                    if (ctx.direct_exclude_doc_nums.contains(doc_num)) continue;
                    if (ctx.segment_id) |id| if (!try ctx.incarnations.matches(ctx.alloc, ctx.visibility, id, ctx.segment_version, doc_num)) continue;
                    if (!try ctx.allowsKey(doc_num)) continue;
                    const doc_weight = decoded.weights[di];
                    try ctx.scores.add(doc_num, ctx.query_weight * doc_weight);
                }
                if (ctx.profile) |p| {
                    p.score_collect_ns += nowNs() - collect_start_ns;
                    switch (ctx.source) {
                        .segment => p.segment_chunks += 1,
                        .delta => p.delta_chunks += 1,
                    }
                }
            }
        };

        const Parallel = struct {
            const scheduler = @import("../sql/parallel_scheduler.zig");
            io: std.Io,
            parent: *backend_erased.ReadTxn,
            base: AccumulateContext,
            input: []const daat.Stream,
            span: u64,
            work: []const daat.Range = &.{},
            routing: daat.Routing = undefined,
            k: u32,
            workspace: usize,
            a: Allocator,
            winners: std.PriorityQueue(daat.Entry, void, daat.Entry.worse) = .initContext({}),
            mutex: std.Io.Mutex = .init,
            next: std.atomic.Value(usize) = .init(0),
            capped: std.atomic.Value(bool) = .init(false),
            cutoff: SparseCompetitiveCutoff = .{},
            scored: usize = 0,
            fn predicate(lane_self: *@This()) ?KeyPredicate {
                const original = lane_self.base.key_predicate orelse return null;
                return .{ .ptr = lane_self, .allows = allows, .select_constraints = if (original.select_constraints != null) select else null, .select_ordinals = if (original.select_ordinals != null) selectOrdinals else null, .select_range = if (original.select_range != null) selectRange else null, .constraint_revision = if (original.constraint_revision != null) revision else null };
            }
            fn allows(raw: *anyopaque, key: []const u8) !bool {
                const lane_self: *@This() = @ptrCast(@alignCast(raw));
                try lane_self.mutex.lock(lane_self.io);
                defer lane_self.mutex.unlock(lane_self.io);
                const original = lane_self.base.key_predicate.?;
                return original.allows(original.ptr, key);
            }
            fn select(raw: *anyopaque, a: Allocator, lane_lookup: OrdinalLookup) !?@import("ordinal_lookup.zig").Selection {
                const lane_self: *@This() = @ptrCast(@alignCast(raw));
                try lane_self.mutex.lock(lane_self.io);
                defer lane_self.mutex.unlock(lane_self.io);
                const original = lane_self.base.key_predicate.?;
                return original.select_constraints.?(original.ptr, a, lane_lookup);
            }
            fn selectOrdinals(raw: *anyopaque, a: Allocator, lane_lookup: OrdinalLookup) !?@import("../encoding/roaring.zig").RoaringBitmap {
                const lane_self: *@This() = @ptrCast(@alignCast(raw));
                try lane_self.mutex.lock(lane_self.io);
                defer lane_self.mutex.unlock(lane_self.io);
                const original = lane_self.base.key_predicate.?;
                return original.select_ordinals.?(original.ptr, a, lane_lookup);
            }
            fn selectRange(raw: *anyopaque, a: Allocator, lane_lookup: OrdinalLookup, first: u32, last: u32) !?@import("ordinal_lookup.zig").Selection {
                const lane_self: *@This() = @ptrCast(@alignCast(raw));
                try lane_self.mutex.lock(lane_self.io);
                defer lane_self.mutex.unlock(lane_self.io);
                const original = lane_self.base.key_predicate.?;
                return original.select_range.?(original.ptr, a, lane_lookup, first, last);
            }
            fn revision(raw: *anyopaque) u64 {
                const lane_self: *@This() = @ptrCast(@alignCast(raw));
                lane_self.mutex.lockUncancelable(lane_self.io);
                defer lane_self.mutex.unlock(lane_self.io);
                const original = lane_self.base.key_predicate.?;
                return original.constraint_revision.?(original.ptr);
            }
            fn run(lane_self: *@This()) anyerror!void {
                var budget: @import("ordinal_lookup.zig").MaskBudget = .{ .backing = std.heap.page_allocator, .limit = lane_self.workspace };
                lane_self.runBounded(budget.allocator()) catch |err| {
                    if ((err == error.OutOfMemory and budget.exhausted) or err == error.ResourceBudgetExceeded) {
                        lane_self.capped.store(true, .release);
                        return;
                    }
                    return err;
                };
            }
            fn runBounded(lane_self: *@This(), a: Allocator) !void {
                try lane_self.mutex.lock(lane_self.io);
                var lane_txn = lane_self.parent.vtable.fork_read.?(a, lane_self.parent.ptr) catch |err| {
                    lane_self.mutex.unlock(lane_self.io);
                    return err;
                };
                lane_self.mutex.unlock(lane_self.io);
                defer lane_txn.abort();
                var reader: PageReader = .{ .txn = &lane_txn, .budget = lane_self.workspace / 2 };
                defer reader.deinit();
                var lane_visibility = try VisibilityReader.init(&lane_txn);
                defer lane_visibility.deinit();
                var lane_incarnations: IncarnationCache = .{};
                defer lane_incarnations.deinit(a);
                var lane_decisions: Decisions = .{ .complete = true };
                defer if (lane_decisions.identities) |*cursor| cursor.close();
                var lane_lookup: Lookup = .{ .index = lane_self.base.index, .txn = &lane_txn, .cancellation = lane_self.base.cancellation };
                var ordinals: AdaptiveOrdinalConstraints = .{
                    .a = a,
                    .predicate = lane_self.predicate(),
                    .lookup = .{ .ptr = &lane_lookup, .one = Lookup.lookup, .block = Lookup.block, .bounded_block = Lookup.boundedBlock, .native_range = Lookup.nativeRange },
                    .include = lane_self.base.ordinals.include,
                    .exclude = lane_self.base.ordinals.exclude,
                    .resolved = lane_self.base.ordinals.resolved,
                    .deferred = lane_self.base.ordinals.deferred,
                    .revision = lane_self.base.ordinals.revision,
                    .borrowed = true,
                };
                defer ordinals.deinit();
                var lane_context = lane_self.base;
                lane_context.alloc = a;
                lane_context.txn = &lane_txn;
                lane_context.incarnations = &lane_incarnations;
                lane_context.visibility = &lane_visibility;
                lane_context.ordinals = &ordinals;
                lane_context.decisions = &lane_decisions;
                lane_context.key_predicate = lane_self.predicate();
                lane_context.profile = null;
                lane_context.shared_cutoff = &lane_self.cutoff;
                var indices: std.ArrayListUnmanaged(u32) = .empty;
                defer indices.deinit(a);
                var streams: std.ArrayListUnmanaged(daat.Stream) = .empty;
                defer streams.deinit(a);
                while (!lane_self.capped.load(.acquire)) {
                    const work_index = lane_self.next.fetchAdd(1, .monotonic);
                    if (work_index >= lane_self.work.len) return;
                    const range = lane_self.work[work_index];
                    try checkSearchCancellation(lane_self.base.cancellation);
                    _ = try lane_self.routing.active(a, range, &indices);
                    try streams.resize(a, indices.items.len);
                    for (indices.items, streams.items) |index, *stream| {
                        const input = lane_self.input[index];
                        stream.* = input;
                        stream.allocator = a;
                        if (stream.reader != null) stream.reader = reader.interface();
                    }
                    var lane_stats: daat.Stats = .{};
                    const result = daat.collectRange(a, streams.items, lane_self.k, &lane_context, &lane_stats, range.first, range.end) catch |err| {
                        for (streams.items) |*stream| stream.deinit();
                        return err;
                    };
                    defer a.free(result);
                    for (streams.items) |*stream| stream.deinit();
                    try lane_self.mutex.lock(lane_self.io);
                    defer lane_self.mutex.unlock(lane_self.io);
                    lane_self.scored += lane_stats.scored;
                    for (result) |entry| {
                        if (lane_self.winners.items.len < lane_self.k) try lane_self.winners.push(lane_self.a, entry) else if (daat.Entry.better({}, entry, lane_self.winners.peek().?)) {
                            _ = lane_self.winners.pop();
                            try lane_self.winners.push(lane_self.a, entry);
                        }
                    }
                }
            }
            fn execute(lane_self: *@This(), tasks_count: ?*u64) !?[]ScoreEntry {
                defer lane_self.winners.deinit(lane_self.a);
                var routing_budget: @import("ordinal_lookup.zig").MaskBudget = .{ .backing = lane_self.a, .limit = 4 * 1024 * 1024 };
                const routing_a = routing_budget.allocator();
                const work = daat.planRanges(routing_a, lane_self.input, lane_self.span, if (lane_self.base.ordinals.include) |*bitmap| bitmap else null, if (lane_self.base.ordinals.exclude) |*bitmap| bitmap else null) catch |err| {
                    if (err == error.OutOfMemory and routing_budget.exhausted) return null;
                    return err;
                };
                defer routing_a.free(work);
                lane_self.work = work;
                const lanes = @min(4, scheduler.global().fanout(work.len, 64 * 1024 * 1024, lane_self.workspace));
                if (lanes < 2) return null;
                lane_self.routing = daat.Routing.init(routing_a, lane_self.input, lane_self.span) catch |err| {
                    if (err == error.OutOfMemory and routing_budget.exhausted) return null;
                    return err;
                };
                defer lane_self.routing.deinit(routing_a);
                try lane_self.winners.ensureTotalCapacity(lane_self.a, lane_self.k);
                var tasks: [3]?scheduler.Task(anyerror!void) = @splat(null);
                defer for (&tasks) |*slot| if (slot.*) |*task| if (task.future != null) task.cancel(lane_self.io) catch {};
                var submitted: u64 = 0;
                for (tasks[0 .. lanes - 1]) |*slot| {
                    slot.* = scheduler.global().submit(lane_self.io, lane_self.workspace, run, .{lane_self});
                    if (slot.* == null) try lane_self.run() else submitted += 1;
                }
                try lane_self.run();
                for (&tasks) |*slot| if (slot.*) |*task| try task.await(lane_self.io);
                if (tasks_count) |count| count.* += submitted;
                if (lane_self.capped.load(.acquire)) return null;
                return try lane_self.a.dupe(ScoreEntry, lane_self.winners.items);
            }
        };

        var page_reader: PageReader = .{ .txn = &txn, .io = if (constraints.score_spill) |options| options.io else null };
        defer page_reader.deinit();
        var fast_entries: ?[]ScoreEntry = null;
        defer if (fast_entries) |values| alloc.free(values);
        if (decisions.complete) {
            if (try collectPostingStreams(alloc, &txn, query_vec, constraints.cancellation, 64 * 1024 * 1024, &page_reader, if (ordinal_state.include) |*bitmap| bitmap else null)) |input| {
                defer {
                    for (input) |*stream| stream.deinit();
                    alloc.free(input);
                }
                var context = AccumulateContext{
                    .alloc = alloc,
                    .txn = &txn,
                    .incarnations = &incarnations,
                    .visibility = &visibility,
                    .query_weight = 0,
                    .scores = &scores,
                    .filter_doc_nums = &filter_doc_nums,
                    .direct_filter_doc_nums = &direct_filter_doc_nums,
                    .exclude_doc_nums = &exclude_doc_nums,
                    .direct_exclude_doc_nums = &direct_exclude_doc_nums,
                    .profile = null,
                    .source = .segment,
                    .cancellation = constraints.cancellation,
                    .key_predicate = if (!ordinal_state.resolved) constraints.key_predicate else null,
                    .ordinals = &ordinal_state,
                    .index = self,
                    .decisions = &decisions,
                };
                var scoring_stats: daat.Stats = .{};
                const span_data = txn.get(metaKey(meta_next_doc_num)) catch |err| switch (err) {
                    error.NotFound => &.{},
                    else => return err,
                };
                const span: u64 = if (span_data.len == 8) std.mem.readInt(u64, span_data[0..8], .little) else 0;
                if (page_reader.io != null and txn.vtable.fork_read != null and span >= 8192 and span <= 0x1_0000_0000 and k != 0 and (ordinal_state.include == null or ordinal_state.include.?.cardinality() >= 8192)) {
                    var parallel: Parallel = .{ .io = page_reader.io.?, .parent = &txn, .base = context, .input = input, .span = span, .k = k, .workspace = constraints.parallel_workspace_bytes, .a = alloc };
                    fast_entries = try parallel.execute(constraints.parallel_tasks);
                    if (fast_entries != null) scoring_stats.scored = parallel.scored;
                }
                if (fast_entries == null) fast_entries = daat.collect(alloc, input, k, &context, &scoring_stats) catch |err| switch (err) {
                    // Page sizes are learned lazily. A query exceeding the live
                    // block budget retries through the existing bounded spill
                    // collector after every admitted stream is released.
                    error.ResourceBudgetExceeded => null,
                    else => return err,
                };
                if (profile_enabled) profile.scored_docs = scoring_stats.scored;
            }
        }
        if (fast_entries == null) {
            if (profile_enabled) profile.terms = query_vec.indices.len;
            const segment_seek_start_ns = if (profile_enabled) nowNs() else 0;
            var segment_cur = try txn.openCursor();
            defer segment_cur.close();
            var maybe_segment = try segment_cur.seekAtOrAfter(taggedPrefix(key_segment));
            if (profile_enabled) profile.segment_seek_ns += nowNs() - segment_seek_start_ns;
            while (maybe_segment) |segment_entry| {
                try checkSearchCancellation(constraints.cancellation);
                if (segment_entry.key.len == 0 or segment_entry.key[0] != key_segment) break;
                if (profile_enabled) profile.segment_entries += 1;
                for (query_vec.indices, 0..) |term_id, qi| {
                    var ctx = AccumulateContext{
                        .alloc = alloc,
                        .txn = &txn,
                        .incarnations = &incarnations,
                        .visibility = &visibility,
                        .segment_id = segmentIdFromKey(segment_entry.key) orelse return error.InvalidSparseSegment,
                        .segment_version = try immutableVersion(segment_entry.value),
                        .query_weight = query_vec.values[qi],
                        .scores = &scores,
                        .filter_doc_nums = &filter_doc_nums,
                        .direct_filter_doc_nums = &direct_filter_doc_nums,
                        .exclude_doc_nums = &exclude_doc_nums,
                        .direct_exclude_doc_nums = &direct_exclude_doc_nums,
                        .profile = if (profile_enabled) &profile else null,
                        .source = .segment,
                        .cancellation = constraints.cancellation,
                        .key_predicate = if (!ordinal_state.resolved) constraints.key_predicate else null,
                        .ordinals = &ordinal_state,
                        .index = self,
                        .decisions = &decisions,
                    };
                    const segment_decode_start_ns = if (profile_enabled) nowNs() else 0;
                    try forEachStoredSegmentChunk(alloc, &txn, ctx.segment_id.?, segment_entry.value, term_id, &ctx, AccumulateContext.visit);
                    if (profile_enabled) profile.segment_decode_ns += nowNs() - segment_decode_start_ns;
                }
                const segment_next_start_ns = if (profile_enabled) nowNs() else 0;
                maybe_segment = try segment_cur.next();
                if (profile_enabled) profile.segment_seek_ns += nowNs() - segment_next_start_ns;
            }

            for (query_vec.indices, 0..) |term_id, qi| {
                try checkSearchCancellation(constraints.cancellation);
                const query_weight = query_vec.values[qi];
                // Check term metadata
                var meta_key_buf: [256]u8 = undefined;
                const mk = invMetaKey(&meta_key_buf, term_id);
                const meta_data = txn.get(mk) catch continue;
                const tm = decodeTermMeta(meta_data);

                // Scan all chunks for this term
                for (0..tm.chunk_count) |ci| {
                    if (ci % 32 == 0) try checkSearchCancellation(constraints.cancellation);
                    var ck_buf: [256]u8 = undefined;
                    const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
                    const chunk_data = txn.get(ck) catch continue;

                    if (!try chunkMayMatch(chunk_data, if (ordinal_state.include) |*bitmap| bitmap else null)) continue;
                    const delta_start_ns = if (profile_enabled) nowNs() else 0;
                    const decoded = try decodeChunk(alloc, chunk_data);
                    defer alloc.free(decoded.doc_nums);
                    defer alloc.free(decoded.weights);

                    var ctx = AccumulateContext{
                        .alloc = alloc,
                        .txn = &txn,
                        .incarnations = &incarnations,
                        .visibility = &visibility,
                        .query_weight = query_weight,
                        .scores = &scores,
                        .filter_doc_nums = &filter_doc_nums,
                        .direct_filter_doc_nums = &direct_filter_doc_nums,
                        .exclude_doc_nums = &exclude_doc_nums,
                        .direct_exclude_doc_nums = &direct_exclude_doc_nums,
                        .profile = if (profile_enabled) &profile else null,
                        .source = .delta,
                        .cancellation = constraints.cancellation,
                        .key_predicate = if (!ordinal_state.resolved) constraints.key_predicate else null,
                        .ordinals = &ordinal_state,
                        .index = self,
                        .decisions = &decisions,
                    };
                    try AccumulateContext.visit(&ctx, decoded);
                    if (profile_enabled) profile.delta_chunk_ns += nowNs() - delta_start_ns;
                }
            }
        }
        var entries = std.ArrayListUnmanaged(ScoreEntry).empty;
        defer entries.deinit(alloc);
        var heap = std.PriorityQueue(ScoreEntry, void, ScoreEntry.worse).initContext({});
        defer heap.deinit(alloc);
        // New checkpoints prove complete reverse identities. Tombstones must be
        // removed before heap admission or deleted winners could hide live hits.
        const bounded = try completeLocatorMap(&txn);
        const sort_start_ns = if (profile_enabled) nowNs() else 0;
        if (fast_entries) |values| try entries.appendSlice(alloc, values) else {
            try scores.finish();
            while (try scores.next(constraints.cancellation)) |entry| {
                try checkSearchCancellation(constraints.cancellation);
                if (profile_enabled) profile.scored_docs += 1;
                if (self.docNumDeleted(&visibility, entry.doc_num)) continue;
                if (!bounded) {
                    try entries.append(alloc, entry);
                } else if (heap.items.len < k) {
                    try heap.push(alloc, entry);
                } else if (heap.peek()) |worst| {
                    if (ScoreEntry.better({}, entry, worst)) {
                        _ = heap.pop();
                        try heap.push(alloc, entry);
                    }
                }
            }
            if (bounded) try entries.appendSlice(alloc, heap.items);
        }

        std.mem.sort(ScoreEntry, entries.items, {}, ScoreEntry.better);
        if (profile_enabled) profile.sort_ns = nowNs() - sort_start_ns;

        const n = @min(k, @as(u32, @intCast(entries.items.len)));
        if (n == 0) return try alloc.alloc(SearchResult, 0);

        // Resolve docNums to docIDs
        var results = try alloc.alloc(SearchResult, n);
        errdefer alloc.free(results);
        var valid: usize = 0;
        errdefer {
            for (results[0..valid]) |result| alloc.free(result.doc_id);
        }

        const candidate_batch_size = @max(@as(usize, 32), @as(usize, n) * 2);
        var cursor: usize = 0;
        const hydrate_start_ns = if (profile_enabled) nowNs() else 0;
        while (cursor < entries.items.len and valid < n) {
            try checkSearchCancellation(constraints.cancellation);
            const batch_len = @min(candidate_batch_size, entries.items.len - cursor);
            var candidates = try alloc.alloc(SearchCandidate, batch_len);
            defer {
                for (candidates) |candidate| {
                    if (candidate.doc_id) |doc_id| alloc.free(doc_id);
                }
                alloc.free(candidates);
            }

            for (entries.items[cursor .. cursor + batch_len], 0..) |entry, i| {
                candidates[i] = .{
                    .doc_num = entry.doc_num,
                    .score = entry.score,
                };
            }
            try self.resolveSearchCandidateDocIds(alloc, &txn, candidates);

            for (candidates) |*candidate| {
                if (valid >= n) break;
                const doc_id = candidate.doc_id orelse continue;
                candidate.doc_id = null;
                results[valid] = .{
                    .doc_id = doc_id,
                    .doc_num = candidate.doc_num,
                    .score = candidate.score,
                };
                valid += 1;
            }
            cursor += batch_len;
        }
        if (profile_enabled) {
            profile.hydrate_ns = nowNs() - hydrate_start_ns;
            profile.results = valid;
        }

        if (valid < n) {
            // Shrink if some doc nums couldn't be resolved
            return try alloc.realloc(results, valid);
        }
        if (profile_enabled) {
            std.log.info(
                "antfly_bench_sparse_search k={d} terms={d} results={d} scored_docs={d} total_ms={d} filter_ms={d} segment_seek_ms={d} segment_decode_ms={d} delta_chunk_ms={d} score_collect_ms={d} sort_ms={d} hydrate_ms={d} segment_entries={d} segment_chunks={d} delta_chunks={d}",
                .{
                    k,
                    profile.terms,
                    profile.results,
                    profile.scored_docs,
                    (nowNs() - total_start_ns) / std.time.ns_per_ms,
                    profile.filter_resolve_ns / std.time.ns_per_ms,
                    profile.segment_seek_ns / std.time.ns_per_ms,
                    profile.segment_decode_ns / std.time.ns_per_ms,
                    profile.delta_chunk_ns / std.time.ns_per_ms,
                    profile.score_collect_ns / std.time.ns_per_ms,
                    profile.sort_ns / std.time.ns_per_ms,
                    profile.hydrate_ns / std.time.ns_per_ms,
                    profile.segment_entries,
                    profile.segment_chunks,
                    profile.delta_chunks,
                },
            );
        }
        return results;
    }

    fn resolveDocNumSetAlloc(
        self: *SparseIndex,
        alloc: Allocator,
        txn: anytype,
        doc_ids: []const []const u8,
    ) !std.AutoHashMapUnmanaged(u32, void) {
        var out = std.AutoHashMapUnmanaged(u32, void).empty;
        errdefer out.deinit(alloc);
        for (doc_ids) |doc_id| {
            const doc_num = self.lookupDocNumByDocId(txn, doc_id) catch continue;
            if (doc_num > std.math.maxInt(u32)) continue;
            try out.put(alloc, @intCast(doc_num), {});
        }
        return out;
    }

    fn docNumSetFromSliceAlloc(
        self: *SparseIndex,
        alloc: Allocator,
        doc_nums: []const u32,
    ) !std.AutoHashMapUnmanaged(u32, void) {
        _ = self;
        var out = std.AutoHashMapUnmanaged(u32, void).empty;
        errdefer out.deinit(alloc);
        for (doc_nums) |doc_num| try out.put(alloc, doc_num, {});
        return out;
    }

    pub fn debugDocNumForDocId(self: *SparseIndex, doc_id: []const u8) !?u32 {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        return try self.docNumForDocIdTxn(&txn, doc_id);
    }

    pub fn docNumForDocIdTxn(self: *SparseIndex, txn: anytype, doc_id: []const u8) !?u32 {
        const doc_num = self.lookupDocNumByDocId(txn, doc_id) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        if (doc_num > std.math.maxInt(u32)) return error.DocNumOverflow;
        return @intCast(doc_num);
    }

    pub fn docNumsForDocIdsAlloc(self: *SparseIndex, alloc: Allocator, txn: anytype, doc_ids: []const []const u8) ![]const u32 {
        var wanted = std.StringHashMapUnmanaged(void).empty;
        defer wanted.deinit(alloc);
        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);
        var seen = std.AutoHashMapUnmanaged(u32, void).empty;
        defer seen.deinit(alloc);

        for (doc_ids) |doc_id| {
            const gop = try wanted.getOrPut(alloc, doc_id);
            if (gop.found_existing) continue;
            var key_buf: [256]u8 = undefined;
            if (txnGet(txn, self.dbi, fwdKey(&key_buf, doc_id))) |fwd_data| {
                const doc_num_u64 = try decodeFwdDocNum(fwd_data);
                if (doc_num_u64 <= std.math.maxInt(u32) and !self.docNumDeleted(txn, doc_num_u64)) {
                    const doc_num: u32 = @intCast(doc_num_u64);
                    const seen_gop = try seen.getOrPut(alloc, doc_num);
                    if (!seen_gop.found_existing) try out.append(alloc, doc_num);
                }
                _ = wanted.remove(doc_id);
            } else |_| {}
        }
        if (wanted.count() == 0) return try out.toOwnedSlice(alloc);

        const LookupContext = struct {
            alloc: Allocator,
            index: *SparseIndex,
            txn: @TypeOf(txn),
            wanted: *std.StringHashMapUnmanaged(void),
            seen: *std.AutoHashMapUnmanaged(u32, void),
            out: *std.ArrayListUnmanaged(u32),

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (!ctx.wanted.contains(entry.doc_id)) return false;
                if (entry.doc_num <= std.math.maxInt(u32) and !ctx.index.docNumDeleted(ctx.txn, entry.doc_num)) {
                    const doc_num: u32 = @intCast(entry.doc_num);
                    const seen_gop = try ctx.seen.getOrPut(ctx.alloc, doc_num);
                    if (!seen_gop.found_existing) try ctx.out.append(ctx.alloc, doc_num);
                }
                _ = ctx.wanted.remove(entry.doc_id);
                return ctx.wanted.count() == 0;
            }
        };

        var cur = try txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_entry) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            var ctx = LookupContext{
                .alloc = alloc,
                .index = self,
                .txn = txn,
                .wanted = &wanted,
                .seen = &seen,
                .out = &out,
            };
            if (try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &ctx, LookupContext.visit)) break;
            maybe_entry = try cur.next();
        }

        return try out.toOwnedSlice(alloc);
    }

    pub fn docNumsForOrdinalDocNumsAlloc(self: *SparseIndex, alloc: Allocator, txn: anytype, ordinals: []const u32) !OrdinalDocNumLookup {
        _ = self;
        _ = txn;
        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);
        var seen = std.AutoHashMapUnmanaged(u32, void).empty;
        defer seen.deinit(alloc);

        const sorted = try alloc.dupe(u32, ordinals);
        defer alloc.free(sorted);
        std.mem.sort(u32, sorted, {}, std.sort.asc(u32));

        var previous: ?u32 = null;
        for (sorted) |ordinal| {
            if (previous != null and previous.? == ordinal) continue;
            previous = ordinal;
            const gop = try seen.getOrPut(alloc, ordinal);
            if (!gop.found_existing) try out.append(alloc, ordinal);
        }

        return .{
            .doc_nums = try out.toOwnedSlice(alloc),
            .missing_ordinals = try alloc.alloc(u32, 0),
        };
    }

    /// Free search results.
    pub fn freeResults(alloc: Allocator, results: []SearchResult) void {
        for (results) |r| alloc.free(r.doc_id);
        alloc.free(results);
    }

    pub fn rebuildRangeInto(self: *SparseIndex, dest: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !SplitRebuildResult {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var collected = try self.collectRangeSparseDocs(alloc, &txn, lower, upper, true);
        defer collected.deinit();

        if (collected.writes.items.len > 0) {
            try dest.batchWithOptions(collected.writes.items, &.{}, .{
                .defer_term_range_updates = true,
                .prefer_bulk_build = true,
                .assume_new_doc_ids = true,
            });
        }
        return .{ .doc_ids = try collected.takeDocIds() };
    }

    pub fn handoffRangeInto(self: *SparseIndex, dest: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8, collect_doc_ids: bool) !SplitRebuildResult {
        var src_txn = try self.beginReadTxn();
        defer src_txn.abort();

        const select_started = nowNs();
        var collected = try self.collectRangeSparseDocs(alloc, &src_txn, lower, upper, collect_doc_ids);
        defer collected.deinit();
        const select_docs_ns = elapsedSince(select_started);

        const commit_started = nowNs();
        if (collected.writes.items.len > 0) {
            try dest.batchWithOptions(collected.writes.items, &.{}, .{
                .defer_term_range_updates = true,
                .prefer_bulk_build = true,
                .assume_new_doc_ids = true,
            });
        }
        const commit_ns = elapsedSince(commit_started);

        return .{
            .doc_ids = try collected.takeDocIds(),
            .select_docs_ns = select_docs_ns,
            .commit_ns = commit_ns,
        };
    }

    pub fn handoffPreparedDocIdsInto(self: *SparseIndex, dest: *SparseIndex, alloc: Allocator, doc_ids_in: []const []const u8, lower: []const u8, upper: []const u8, collect_doc_ids: bool) !SplitRebuildResult {
        var src_txn = try self.beginReadTxn();
        defer src_txn.abort();

        const select_started = nowNs();
        _ = lower;
        _ = upper;
        var collected = try self.collectPreparedSparseDocs(alloc, &src_txn, doc_ids_in, collect_doc_ids);
        defer collected.deinit();
        const select_docs_ns = elapsedSince(select_started);

        const commit_started = nowNs();
        if (collected.writes.items.len > 0) {
            try dest.batchWithOptions(collected.writes.items, &.{}, .{
                .defer_term_range_updates = true,
                .prefer_bulk_build = true,
                .assume_new_doc_ids = true,
            });
        }
        const commit_ns = elapsedSince(commit_started);

        return .{
            .doc_ids = try collected.takeDocIds(),
            .select_docs_ns = select_docs_ns,
            .commit_ns = commit_ns,
        };
    }

    pub fn splitPlanningStats(self: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !SplitPlanningStats {
        var src_txn = try self.beginReadTxn();
        defer src_txn.abort();

        var collected = try self.collectRangeSparseDocs(alloc, &src_txn, lower, upper, false);
        defer collected.deinit();

        var out: SplitPlanningStats = .{
            .selected_docs = collected.selected_doc_nums.size,
        };

        const term_ids = sortAndDedupU32(collected.term_ids.items);
        out.touched_terms = term_ids.len;

        var src_cur = try src_txn.openCursor();
        defer src_cur.close();
        if (try src_cur.seekAtOrAfter(taggedPrefix(key_inv))) |first_term| {
            var entry = first_term;
            while (true) {
                if (entry.key.len == 0 or entry.key[0] != key_inv) break;
                const term_id = parseTermRangeKey(entry.key) orelse {
                    entry = (try src_cur.next()) orelse break;
                    continue;
                };
                if (!u32SliceContains(term_ids, term_id)) {
                    entry = (try src_cur.next()) orelse break;
                    continue;
                }
                const range = try decodeChunkRangeMeta(entry.value);
                switch (classifyChunkRange(range, lower, upper)) {
                    .outside => {},
                    .right_only, .mixed => {
                        try accumulateSplitPlanningStats(alloc, &src_txn, self.dbi, term_id, lower, upper, &collected.selected_doc_nums, &out);
                    },
                }
                entry = (try src_cur.next()) orelse break;
            }
        }

        var maybe_segment = try src_cur.seekAtOrAfter(taggedPrefix(key_segment));
        while (maybe_segment) |segment_entry| {
            if (segment_entry.key.len == 0 or segment_entry.key[0] != key_segment) break;
            for (term_ids) |term_id| {
                const SegmentStatsContext = struct {
                    selected_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
                    out: *SplitPlanningStats,

                    fn visit(ctx: *@This(), decoded: DecodedChunk) !void {
                        var kept: usize = 0;
                        for (decoded.doc_nums) |doc_num| {
                            if (ctx.selected_doc_nums.contains(doc_num)) kept += 1;
                        }
                        if (kept == 0) return;
                        if (kept == decoded.doc_nums.len) {
                            ctx.out.right_only_chunks += 1;
                            ctx.out.right_only_postings += kept;
                        } else {
                            ctx.out.mixed_chunks += 1;
                            ctx.out.mixed_right_postings += kept;
                        }
                    }
                };
                var segment_ctx = SegmentStatsContext{
                    .selected_doc_nums = &collected.selected_doc_nums,
                    .out = &out,
                };
                try forEachStoredSegmentChunk(alloc, &src_txn, segmentIdFromKey(segment_entry.key).?, segment_entry.value, term_id, &segment_ctx, SegmentStatsContext.visit);
            }
            maybe_segment = try src_cur.next();
        }

        return out;
    }

    pub fn pruneRange(self: *SparseIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !void {
        var read_txn = try self.beginReadTxn();
        defer read_txn.abort();

        var cur = try read_txn.openCursor();
        defer cur.close();

        const PrunedDoc = struct {
            doc_num: u64,
            doc_id: []u8,

            pub fn deinit(doc: *@This(), allocator: Allocator) void {
                allocator.free(doc.doc_id);
                doc.* = undefined;
            }
        };

        var term_ids = std.ArrayListUnmanaged(u32).empty;
        defer term_ids.deinit(alloc);
        var pruned_docs = std.ArrayListUnmanaged(PrunedDoc).empty;
        defer {
            for (pruned_docs.items) |*doc| doc.deinit(alloc);
            pruned_docs.deinit(alloc);
        }
        var seen_doc_nums = std.AutoHashMapUnmanaged(u64, void).empty;
        defer seen_doc_nums.deinit(alloc);

        const CollectTermsContext = struct {
            alloc: Allocator,
            terms: *std.ArrayListUnmanaged(u32),

            fn visit(ctx: *@This(), term_id: u32) !void {
                try ctx.terms.append(ctx.alloc, term_id);
            }
        };
        var collect_terms_ctx = CollectTermsContext{ .alloc = alloc, .terms = &term_ids };

        const appendPrunedDoc = struct {
            fn run(
                allocator: Allocator,
                docs: *std.ArrayListUnmanaged(PrunedDoc),
                seen: *std.AutoHashMapUnmanaged(u64, void),
                doc_num: u64,
                doc_id: []const u8,
            ) !void {
                const gop = try seen.getOrPut(allocator, doc_num);
                if (gop.found_existing) return;
                errdefer _ = seen.remove(doc_num);
                try docs.append(allocator, .{
                    .doc_num = doc_num,
                    .doc_id = try allocator.dupe(u8, doc_id),
                });
            }
        }.run;

        const fwd_first = if (lower.len == 0)
            (try cur.seekAtOrAfter(taggedPrefix(key_fwd)))
        else blk: {
            const start_key = try fwdKeyAlloc(alloc, lower);
            defer alloc.free(start_key);
            break :blk try cur.seekAtOrAfter(start_key);
        };
        var maybe_fwd_entry = fwd_first;
        while (maybe_fwd_entry) |entry| {
            const doc_id = fwdDocIdFromKey(entry.key) orelse break;
            if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) break;
            if (docIdInOwnedRange(doc_id, lower, upper)) {
                const doc_num = try forEachFwdTermId(entry.value, CollectTermsContext, &collect_terms_ctx, CollectTermsContext.visit);
                try appendPrunedDoc(alloc, &pruned_docs, &seen_doc_nums, doc_num, doc_id);
            }
            maybe_fwd_entry = try cur.next();
        }

        const DocMapPruneContext = struct {
            alloc: Allocator,
            lower: []const u8,
            upper: []const u8,
            terms_ctx: *CollectTermsContext,
            docs: *std.ArrayListUnmanaged(PrunedDoc),
            seen: *std.AutoHashMapUnmanaged(u64, void),

            fn visit(ctx: *@This(), entry: DocMapLookup) !bool {
                if (!docIdInOwnedRange(entry.doc_id, ctx.lower, ctx.upper)) return false;
                _ = try forEachFwdTermId(entry.fwd_data, CollectTermsContext, ctx.terms_ctx, CollectTermsContext.visit);
                try appendPrunedDoc(ctx.alloc, ctx.docs, ctx.seen, entry.doc_num, entry.doc_id);
                return false;
            }
        };
        var docmap_ctx = DocMapPruneContext{
            .alloc = alloc,
            .lower = lower,
            .upper = upper,
            .terms_ctx = &collect_terms_ctx,
            .docs = &pruned_docs,
            .seen = &seen_doc_nums,
        };
        var maybe_docmap = try cur.seekAtOrAfter(taggedPrefix(key_docmap_segment));
        while (maybe_docmap) |entry| {
            if (entry.key.len == 0 or entry.key[0] != key_docmap_segment) break;
            _ = try forEachCurrentDocMapEntry(&read_txn, entry.key, entry.value, &docmap_ctx, DocMapPruneContext.visit);
            maybe_docmap = try cur.next();
        }

        if (try cur.seekAtOrAfter(taggedPrefix(key_inv))) |first| {
            var entry = first;
            while (true) {
                if (entry.key.len == 0 or entry.key[0] != key_inv) break;
                const term_id = parseTermRangeKey(entry.key) orelse {
                    entry = (try cur.next()) orelse break;
                    continue;
                };
                const range = try decodeChunkRangeMeta(entry.value);
                switch (classifyChunkRange(range, lower, upper)) {
                    .outside => {},
                    .right_only, .mixed => try term_ids.append(alloc, term_id),
                }
                entry = (try cur.next()) orelse break;
            }
        }

        if (term_ids.items.len == 0 and pruned_docs.items.len == 0) return;

        var write_txn = try self.beginWriteTxn();
        errdefer write_txn.abort();
        const prev_doc_count = self.doc_count;
        const prev_term_count = self.term_count;
        errdefer {
            self.doc_count = prev_doc_count;
            self.term_count = prev_term_count;
        }
        const pruned_term_ids = sortAndDedupU32(term_ids.items);
        for (pruned_term_ids) |term_id| {
            try pruneTermPostings(alloc, &write_txn, self.dbi, term_id, lower, upper);
        }
        for (pruned_docs.items) |doc| {
            const fwd_key = try fwdKeyAlloc(alloc, doc.doc_id);
            defer alloc.free(fwd_key);
            txnDelete(&write_txn, self.dbi, fwd_key) catch {};
            var rev_key_buf: [256]u8 = undefined;
            txnDelete(&write_txn, self.dbi, revKey(&rev_key_buf, doc.doc_num)) catch {};
            var tombstone_key_buf: [16]u8 = undefined;
            try txnPut(&write_txn, self.dbi, docTombstoneKey(&tombstone_key_buf, doc.doc_num), &.{});
        }
        const stats_after = try scanStatsInTxn(&write_txn, self.dbi);
        self.doc_count = stats_after.doc_count;
        self.term_count = stats_after.term_count;
        try persistSparseCounters(self, &write_txn);
        try write_txn.commit();
    }
};

fn txnGet(txn: anytype, dbi: anytype, key: []const u8) ![]const u8 {
    _ = dbi;
    return try txn.get(key);
}

fn txnPut(txn: anytype, dbi: anytype, key: []const u8, value: []const u8) !void {
    _ = dbi;
    try txn.put(key, value);
}

fn txnTypeSupportsAppendPut(comptime T: type) bool {
    const base = switch (@typeInfo(T)) {
        .pointer => |ptr| ptr.child,
        else => T,
    };
    return @hasDecl(base, "appendPut");
}

fn txnAppendPut(txn: anytype, dbi: anytype, key: []const u8, value: []const u8) !void {
    if (comptime txnTypeSupportsAppendPut(@TypeOf(txn))) {
        txn.appendPut(key, value) catch |err| switch (err) {
            error.Unsupported => return try txnPut(txn, dbi, key, value),
            else => return err,
        };
        return;
    }
    try txnPut(txn, dbi, key, value);
}

fn txnDelete(txn: anytype, dbi: anytype, key: []const u8) !void {
    _ = dbi;
    try txn.delete(key);
}

fn persistNextDocNum(idx: *SparseIndex, txn: anytype) !void {
    var ndn_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &ndn_buf, idx.next_doc_num, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_next_doc_num), &ndn_buf);
}

fn persistNextSegmentId(idx: *SparseIndex, txn: anytype) !void {
    var segment_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &segment_buf, idx.next_segment_id, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_next_segment_id), &segment_buf);
}

fn persistSparseCounters(idx: *SparseIndex, txn: anytype) !void {
    try persistNextDocNum(idx, txn);
    try persistNextSegmentId(idx, txn);
    var doc_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &doc_buf, idx.doc_count, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_doc_count), &doc_buf);
    var term_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &term_buf, idx.term_count, .little);
    try txnPut(txn, idx.dbi, metaKey(meta_term_count), &term_buf);
}

fn scanStatsInTxn(txn: anytype, dbi: anytype) !SparseIndex.Stats {
    _ = dbi;
    var cur = try txn.openCursor();
    defer cur.close();
    var out = SparseIndex.Stats{};
    var maybe_entry = try cur.first();
    while (maybe_entry) |entry| {
        if (entry.key.len > 0 and entry.key[0] == key_rev) out.doc_count += 1;
        if (entry.key.len > 0 and entry.key[0] == key_term_catalog) out.term_count += 1;
        if (entry.key.len > 0 and entry.key[0] == key_docmap_segment) {
            const CountDocMapContext = struct {
                txn: @TypeOf(txn),
                count: u64 = 0,

                fn visit(ctx: *@This(), item: DocMapLookup) !bool {
                    var tombstone_buf: [16]u8 = undefined;
                    _ = txnGet(ctx.txn, {}, docTombstoneKey(&tombstone_buf, item.doc_num)) catch |err| switch (err) {
                        error.NotFound => {
                            ctx.count += 1;
                            return false;
                        },
                        else => return err,
                    };
                    return false;
                }
            };
            var ctx = CountDocMapContext{ .txn = txn };
            _ = try forEachCurrentDocMapEntry(txn, entry.key, entry.value, &ctx, CountDocMapContext.visit);
            out.doc_count += ctx.count;
        }
        maybe_entry = try cur.next();
    }
    return out;
}

fn handoffTermPostings(
    alloc: Allocator,
    src_txn: anytype,
    src_dbi: anytype,
    dest_txn: anytype,
    dest_dbi: anytype,
    term_id: u32,
    lower: []const u8,
    upper: []const u8,
    selected_docs: *const SelectedDocLookup,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(src_txn, src_dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    var out_chunk_count: u32 = 0;
    var out_max_weight: f32 = 0;
    var out_min_doc_id: ?[]const u8 = null;
    var out_max_doc_id: ?[]const u8 = null;

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(src_txn, src_dbi, ck) catch continue;

        var meta_ck_buf: [256]u8 = undefined;
        const chunk_meta_data = txnGet(src_txn, src_dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch null;
        if (chunk_meta_data) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => continue,
                .right_only => {
                    const chunk_max_weight = try decodeChunkMaxWeight(chunk_data);
                    out_max_weight = if (out_chunk_count == 0) chunk_max_weight else @max(out_max_weight, chunk_max_weight);
                    var out_ck_buf: [256]u8 = undefined;
                    const out_ck = invChunkKey(&out_ck_buf, term_id, out_chunk_count);
                    try txnPut(dest_txn, dest_dbi, out_ck, chunk_data);
                    var out_meta_ck_buf: [256]u8 = undefined;
                    const out_meta_ck = invChunkMetaKey(&out_meta_ck_buf, term_id, out_chunk_count);
                    try txnPut(dest_txn, dest_dbi, out_meta_ck, raw_meta);
                    updateBorrowedRangeBounds(&out_min_doc_id, &out_max_doc_id, range.min_doc_id, range.max_doc_id);
                    out_chunk_count += 1;
                    continue;
                },
                .mixed => {},
            }
        }

        var out_doc_nums = std.ArrayListUnmanaged(u32).empty;
        defer out_doc_nums.deinit(alloc);
        var out_weights = std.ArrayListUnmanaged(f32).empty;
        defer out_weights.deinit(alloc);
        var chunk_min_doc_id: ?[]const u8 = null;
        var chunk_max_doc_id: ?[]const u8 = null;
        try collectSelectedChunkEntries(
            alloc,
            chunk_data,
            selected_docs,
            &out_doc_nums,
            &out_weights,
            &chunk_min_doc_id,
            &chunk_max_doc_id,
            &out_max_weight,
        );
        if (out_doc_nums.items.len == 0) continue;
        try writeChunkWithRangeMetaToTxn(
            alloc,
            src_txn,
            src_dbi,
            dest_txn,
            dest_dbi,
            term_id,
            out_chunk_count,
            out_doc_nums.items,
            out_weights.items,
            chunk_min_doc_id.?,
            chunk_max_doc_id.?,
        );
        updateBorrowedRangeBounds(&out_min_doc_id, &out_max_doc_id, chunk_min_doc_id.?, chunk_max_doc_id.?);
        out_chunk_count += 1;
    }

    if (out_chunk_count == 0) return;
    const out_meta = encodeTermMeta(out_max_weight, out_chunk_count);
    try txnPut(dest_txn, dest_dbi, mk, &out_meta);
    const out_range_meta = try encodeChunkRangeMeta(alloc, out_min_doc_id.?, out_max_doc_id.?);
    defer alloc.free(out_range_meta);
    var range_key_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, termRangeKey(&range_key_buf, term_id), out_range_meta);
}

fn handoffWholeTermPostings(
    src_txn: anytype,
    src_dbi: anytype,
    dest_txn: anytype,
    dest_dbi: anytype,
    term_id: u32,
    term_range_meta: []const u8,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(src_txn, src_dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    try txnPut(dest_txn, dest_dbi, mk, meta_data);
    var range_key_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, termRangeKey(&range_key_buf, term_id), term_range_meta);

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(src_txn, src_dbi, ck) catch continue;
        try txnPut(dest_txn, dest_dbi, ck, chunk_data);

        var meta_ck_buf: [256]u8 = undefined;
        const meta_ck = invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci));
        const chunk_meta_data = txnGet(src_txn, src_dbi, meta_ck) catch continue;
        try txnPut(dest_txn, dest_dbi, meta_ck, chunk_meta_data);
    }
}

fn pruneTermPostings(
    alloc: Allocator,
    txn: anytype,
    dbi: anytype,
    term_id: u32,
    lower: []const u8,
    upper: []const u8,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(txn, dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    var kept = std.ArrayListUnmanaged(RetainedChunk).empty;
    defer {
        for (kept.items) |*chunk| chunk.deinit(alloc);
        kept.deinit(alloc);
    }

    var out_max_weight: f32 = 0;
    var out_min_doc_id: ?[]u8 = null;
    defer if (out_min_doc_id) |value| alloc.free(value);
    var out_max_doc_id: ?[]u8 = null;
    defer if (out_max_doc_id) |value| alloc.free(value);

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(txn, dbi, ck) catch continue;

        var meta_ck_buf: [256]u8 = undefined;
        const meta_ck = invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci));
        const chunk_meta_data = txnGet(txn, dbi, meta_ck) catch null;
        if (chunk_meta_data) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => {
                    const chunk_copy = try alloc.dupe(u8, chunk_data);
                    errdefer alloc.free(chunk_copy);
                    const meta_copy = try alloc.dupe(u8, raw_meta);
                    errdefer alloc.free(meta_copy);
                    try kept.append(alloc, .{
                        .chunk_bytes = chunk_copy,
                        .meta_bytes = meta_copy,
                        .max_weight = try decodeChunkMaxWeight(chunk_data),
                    });
                    out_max_weight = if (kept.items.len == 1) kept.items[0].max_weight else @max(out_max_weight, kept.items[kept.items.len - 1].max_weight);
                    try updateOwnedRangeBounds(alloc, &out_min_doc_id, &out_max_doc_id, range.min_doc_id, range.max_doc_id);
                    continue;
                },
                .right_only => continue,
                .mixed => {},
            }
        }

        const decoded = try decodeChunk(alloc, chunk_data);
        defer alloc.free(decoded.doc_nums);
        defer alloc.free(decoded.weights);

        var keep_count: usize = 0;
        for (decoded.doc_nums) |doc_num| {
            if (try shouldKeepDocNumOutsideRange(alloc, txn, dbi, doc_num, lower, upper)) keep_count += 1;
        }
        if (keep_count == 0) continue;

        var out_doc_nums = try alloc.alloc(u32, keep_count);
        defer alloc.free(out_doc_nums);
        var out_weights = try alloc.alloc(f32, keep_count);
        defer alloc.free(out_weights);

        var wi: usize = 0;
        for (decoded.doc_nums, 0..) |doc_num, i| {
            if (!(try shouldKeepDocNumOutsideRange(alloc, txn, dbi, doc_num, lower, upper))) continue;
            out_doc_nums[wi] = doc_num;
            out_weights[wi] = decoded.weights[i];
            out_max_weight = if (kept.items.len == 0 and wi == 0 and out_max_weight == 0) decoded.weights[i] else @max(out_max_weight, decoded.weights[i]);
            wi += 1;
        }

        const range = try computeChunkRangeMetaFromDocNums(alloc, txn, dbi, out_doc_nums);
        defer {
            alloc.free(range.min_doc_id);
            alloc.free(range.max_doc_id);
        }
        const encoded = try encodeChunk(alloc, out_doc_nums, out_weights);
        errdefer alloc.free(encoded);
        const encoded_meta = try encodeChunkOrdinalRange(alloc, range.min_doc_id, range.max_doc_id, out_doc_nums, false);
        errdefer alloc.free(encoded_meta);
        try kept.append(alloc, .{
            .chunk_bytes = encoded,
            .meta_bytes = encoded_meta,
            .max_weight = try decodeChunkMaxWeight(encoded),
        });
        try updateOwnedRangeBounds(alloc, &out_min_doc_id, &out_max_doc_id, range.min_doc_id, range.max_doc_id);
    }

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        txnDelete(txn, dbi, invChunkKey(&ck_buf, term_id, @intCast(ci))) catch {};
        var meta_ck_buf: [256]u8 = undefined;
        txnDelete(txn, dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch {};
    }

    var range_key_buf: [256]u8 = undefined;
    if (kept.items.len == 0) {
        txnDelete(txn, dbi, mk) catch {};
        txnDelete(txn, dbi, termRangeKey(&range_key_buf, term_id)) catch {};
        return;
    }

    for (kept.items, 0..) |chunk, out_idx| {
        var ck_buf: [256]u8 = undefined;
        try txnPut(txn, dbi, invChunkKey(&ck_buf, term_id, @intCast(out_idx)), chunk.chunk_bytes);
        var meta_ck_buf: [256]u8 = undefined;
        try txnPut(txn, dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(out_idx)), chunk.meta_bytes);
    }

    const out_meta = encodeTermMeta(out_max_weight, @intCast(kept.items.len));
    try txnPut(txn, dbi, mk, &out_meta);
    const out_range_meta = try encodeChunkRangeMeta(alloc, out_min_doc_id.?, out_max_doc_id.?);
    defer alloc.free(out_range_meta);
    try txnPut(txn, dbi, termRangeKey(&range_key_buf, term_id), out_range_meta);
}

fn shouldKeepDocNumOutsideRange(
    alloc: Allocator,
    txn: anytype,
    dbi: anytype,
    doc_num: u32,
    lower: []const u8,
    upper: []const u8,
) !bool {
    _ = alloc;
    const doc_id = try resolveDocIdByDocNumInTxnBorrowed(txn, dbi, doc_num);
    if (std.mem.order(u8, doc_id, lower) == .lt) return true;
    if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) return true;
    return false;
}

fn docIdInOwnedRange(doc_id: []const u8, lower: []const u8, upper: []const u8) bool {
    if (std.mem.order(u8, doc_id, lower) == .lt) return false;
    if (upper.len > 0 and std.mem.order(u8, doc_id, upper) != .lt) return false;
    return true;
}

fn accumulateSplitPlanningStats(
    alloc: Allocator,
    src_txn: anytype,
    src_dbi: anytype,
    term_id: u32,
    lower: []const u8,
    upper: []const u8,
    selected_doc_nums: *const std.AutoHashMapUnmanaged(u32, void),
    out: *SplitPlanningStats,
) !void {
    var meta_key_buf: [256]u8 = undefined;
    const mk = invMetaKey(&meta_key_buf, term_id);
    const meta_data = txnGet(src_txn, src_dbi, mk) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const tm = decodeTermMeta(meta_data);

    for (0..tm.chunk_count) |ci| {
        var ck_buf: [256]u8 = undefined;
        const ck = invChunkKey(&ck_buf, term_id, @intCast(ci));
        const chunk_data = txnGet(src_txn, src_dbi, ck) catch continue;

        var meta_ck_buf: [256]u8 = undefined;
        const chunk_meta_data = txnGet(src_txn, src_dbi, invChunkMetaKey(&meta_ck_buf, term_id, @intCast(ci))) catch null;
        if (chunk_meta_data) |raw_meta| {
            const range = try decodeChunkRangeMeta(raw_meta);
            switch (classifyChunkRange(range, lower, upper)) {
                .outside => continue,
                .right_only => {
                    const decoded_full = try decodeChunk(alloc, chunk_data);
                    defer alloc.free(decoded_full.doc_nums);
                    defer alloc.free(decoded_full.weights);
                    out.right_only_chunks += 1;
                    out.right_only_postings += decoded_full.doc_nums.len;
                    continue;
                },
                .mixed => {},
            }
        }

        const decoded = try decodeChunk(alloc, chunk_data);
        defer alloc.free(decoded.doc_nums);
        defer alloc.free(decoded.weights);

        var kept: usize = 0;
        for (decoded.doc_nums) |doc_num| {
            if (selected_doc_nums.contains(doc_num)) kept += 1;
        }
        if (kept == 0) continue;
        if (kept == decoded.doc_nums.len) {
            out.right_only_chunks += 1;
            out.right_only_postings += kept;
        } else {
            out.mixed_chunks += 1;
            out.mixed_right_postings += kept;
        }
    }
}

const ChunkSplitClass = enum {
    outside,
    right_only,
    mixed,
};

fn classifyChunkRange(range: ChunkRangeMeta, lower: []const u8, upper: []const u8) ChunkSplitClass {
    if (std.mem.order(u8, range.max_doc_id, lower) == .lt) return .outside;
    if (upper.len > 0 and std.mem.order(u8, range.min_doc_id, upper) != .lt) return .outside;

    const min_in = std.mem.order(u8, range.min_doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, range.min_doc_id, upper) == .lt);
    const max_in = std.mem.order(u8, range.max_doc_id, lower) != .lt and (upper.len == 0 or std.mem.order(u8, range.max_doc_id, upper) == .lt);
    if (min_in and max_in) return .right_only;
    return .mixed;
}

fn resolveDocIdByDocNumInTxnBorrowed(txn: anytype, dbi: anytype, doc_num: u32) ![]const u8 {
    var rev_buf: [256]u8 = undefined;
    return txnGet(txn, dbi, revKey(&rev_buf, doc_num));
}

fn resolveDocIdByDocNumInTxn(alloc: Allocator, txn: anytype, dbi: anytype, doc_num: u32) ![]u8 {
    return alloc.dupe(u8, try resolveDocIdByDocNumInTxnBorrowed(txn, dbi, doc_num));
}

fn computeChunkRangeMetaFromDocNums(
    alloc: Allocator,
    txn: anytype,
    dbi: anytype,
    doc_nums: []const u32,
) !struct { min_doc_id: []u8, max_doc_id: []u8 } {
    std.debug.assert(doc_nums.len > 0);
    var min_doc_id: ?[]u8 = null;
    var max_doc_id: ?[]u8 = null;
    errdefer {
        if (min_doc_id) |doc_id| alloc.free(doc_id);
        if (max_doc_id) |doc_id| alloc.free(doc_id);
    }

    for (doc_nums) |doc_num| {
        const doc_id = try resolveDocIdByDocNumInTxnBorrowed(txn, dbi, doc_num);
        if (min_doc_id == null or std.mem.order(u8, doc_id, min_doc_id.?) == .lt) {
            if (min_doc_id) |existing| alloc.free(existing);
            min_doc_id = try alloc.dupe(u8, doc_id);
        }
        if (max_doc_id == null or std.mem.order(u8, doc_id, max_doc_id.?) == .gt) {
            if (max_doc_id) |existing| alloc.free(existing);
            max_doc_id = try alloc.dupe(u8, doc_id);
        }
    }
    return .{ .min_doc_id = min_doc_id.?, .max_doc_id = max_doc_id.? };
}

fn writeChunkWithRangeMetaToTxn(
    alloc: Allocator,
    src_txn: anytype,
    src_dbi: anytype,
    dest_txn: anytype,
    dest_dbi: anytype,
    term_id: u32,
    chunk_idx: u32,
    doc_nums: []const u32,
    weights: []const f32,
    min_doc_id: []const u8,
    max_doc_id: []const u8,
) !void {
    _ = src_txn;
    _ = src_dbi;
    const encoded = try encodeChunk(alloc, doc_nums, weights);
    defer alloc.free(encoded);
    var ck_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, invChunkKey(&ck_buf, term_id, chunk_idx), encoded);

    const meta = try encodeChunkOrdinalRange(alloc, min_doc_id, max_doc_id, doc_nums, false);
    defer alloc.free(meta);
    var meta_ck_buf: [256]u8 = undefined;
    try txnPut(dest_txn, dest_dbi, invChunkMetaKey(&meta_ck_buf, term_id, chunk_idx), meta);
}

// ============================================================================
// Tests
// ============================================================================

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ts = nowNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-sparse-{s}-{d}\x00", .{ label, ts }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn expectIncarnationSearch(index: *SparseIndex, term: u32, expected: []const struct { id: []const u8, score: f32 }, filtered: bool) !void {
    const alloc = std.testing.allocator;
    const query: SparseVector = .{ .indices = &.{term}, .values = &.{1} };
    const hits = try index.searchConstrained(alloc, &query, 10, if (filtered) .{ .filter_doc_nums = &.{ 1, 2 } } else .{});
    defer SparseIndex.freeResults(alloc, hits);
    try std.testing.expectEqual(expected.len, hits.len);
    for (expected) |item| {
        const score = for (hits) |hit| {
            if (std.mem.eql(u8, hit.doc_id, item.id)) break hit.score;
        } else return error.TestUnexpectedResult;
        try std.testing.expectApproxEqAbs(item.score, score, @as(f32, 0.05));
    }
}

test "sparse incarnation replacement preserves exact scores across compaction late mutation and reopen" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |legacy| {
        var pb: [256]u8 = undefined;
        const path = tmpPath(&pb, if (legacy) "sparse-incarnation-legacy" else "sparse-incarnation-v2");
        defer cleanupTmp(path);
        var index = try SparseIndex.open(alloc, path, .{ .backend = .lsm });
        defer index.close();
        const bulk: BatchOptions = .{ .backend_batch_options = .{ .mode = .bulk_ingest }, .prefer_bulk_build = true, .assume_new_doc_ids = true };
        try index.batchWithOptions(&.{
            .{ .doc_id = "a", .doc_num = 1, .vec = .{ .indices = &.{ 1, 3 }, .values = &.{ 2, 4 } } },
            .{ .doc_id = "b", .doc_num = 2, .vec = .{ .indices = &.{1}, .values = &.{5} } },
        }, &.{}, bulk);
        if (legacy) {
            var txn = try index.beginWriteTxn();
            errdefer txn.abort();
            const id = index.next_segment_id - 1;
            inline for (.{ false, true }) |docmap| {
                var key: [16]u8 = undefined;
                const physical = if (docmap) docMapSegmentKey(&key, id) else segmentKey(&key, id);
                const value = try alloc.dupe(u8, try txn.get(physical));
                defer alloc.free(value);
                std.mem.writeInt(u32, value[segment_magic.len..][0..4], 1, .little);
                try txn.put(physical, value);
                if (!docmap) try posting_pages.publishRoutes(&txn, id, value);
                for ([_]u64{ 1, 2 }) |doc| {
                    var sidecar: [17]u8 = undefined;
                    try txn.delete(segmentIncarnationKey(&sidecar, id, doc, docmap));
                }
            }
            for ([_]u64{ 1, 2 }) |doc| {
                var key: [9]u8 = undefined;
                try txn.delete(docIncarnationKey(&key, doc));
            }
            // A real v1 checkpoint predates the point locator map too.
            try txn.delete(metaKey(meta_complete_locators));
            for ([_][]const u8{ "a", "b" }) |doc_id| {
                var key: [256]u8 = undefined;
                try txn.delete(locatorKey(&key, doc_id));
            }
            try txn.commit();
        }
        try expectIncarnationSearch(&index, 1, &.{ .{ .id = "a", .score = 2 }, .{ .id = "b", .score = 5 } }, false);
        try index.batch(&.{}, &.{"a"});
        try index.batchWithOptions(&.{.{ .doc_id = "a", .doc_num = 1, .vec = .{ .indices = &.{1}, .values = &.{9} } }}, &.{}, bulk);
        try expectIncarnationSearch(&index, 1, &.{ .{ .id = "a", .score = 9 }, .{ .id = "b", .score = 5 } }, false);
        try expectIncarnationSearch(&index, 3, &.{}, false);
        var task = (try index.beginSegmentCompactionTask(alloc, .{ .min_segments = 2, .max_segments = 2, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } })) orelse return error.TestUnexpectedResult;
        defer task.deinit(alloc);
        var compacted = try SparseIndex.executeSegmentCompactionTask(alloc, &task, index.chunk_size);
        defer compacted.deinit(alloc);
        try std.testing.expect(compacted.pages != null);
        try std.testing.expect(posting_pages.paged(compacted.data.?));
        // A fresh delta incarnation must invalidate already-built compaction
        // output without requiring any scan or rewrite in the mutation path.
        try index.batch(&.{.{ .doc_id = "a", .doc_num = 1, .vec = .{ .indices = &.{7}, .values = &.{6} } }}, &.{});
        try std.testing.expect(try index.finishSegmentCompactionTask(&task, &compacted));
        for ([_]bool{ false, true }) |filtered| {
            try expectIncarnationSearch(&index, 1, &.{.{ .id = "b", .score = 5 }}, filtered);
            try expectIncarnationSearch(&index, 7, &.{.{ .id = "a", .score = 6 }}, filtered);
            try expectIncarnationSearch(&index, 3, &.{}, filtered);
        }
        index.close();
        index = try SparseIndex.open(alloc, path, .{ .backend = .lsm });
        try std.testing.expectEqual(@as(u64, 2), index.stats().doc_count);
        try expectIncarnationSearch(&index, 1, &.{.{ .id = "b", .score = 5 }}, false);
        try expectIncarnationSearch(&index, 7, &.{.{ .id = "a", .score = 6 }}, true);
        var txn = try index.beginReadTxn();
        defer txn.abort();
        for (task.sources) |source| for (source.incarnations) |epoch| {
            var key: [17]u8 = undefined;
            try std.testing.expectError(error.NotFound, txn.get(segmentIncarnationKey(&key, source.id, epoch.doc_num, false)));
        };
        for (task.docmaps) |source| for (source.incarnations) |epoch| {
            var key: [17]u8 = undefined;
            try std.testing.expectError(error.NotFound, txn.get(segmentIncarnationKey(&key, source.id, epoch.doc_num, true)));
        };
    }
}

test "sparse incarnation missing sidecar fails closed and empty docmap compaction collects metadata" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "sparse-incarnation-sidecars");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(alloc, path, .{ .backend = .lsm });
    defer index.close();
    const bulk: BatchOptions = .{ .backend_batch_options = .{ .mode = .bulk_ingest }, .prefer_bulk_build = true, .assume_new_doc_ids = true };
    try index.batchWithOptions(&.{.{ .doc_id = "a", .doc_num = 1, .vec = .{ .indices = &.{1}, .values = &.{2} } }}, &.{}, bulk);
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        var key: [17]u8 = undefined;
        try txn.delete(segmentIncarnationKey(&key, index.next_segment_id - 1, 1, false));
        try txn.commit();
    }
    try std.testing.expectError(error.InvalidSparseSegment, expectIncarnationSearch(&index, 1, &.{}, false));
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        try putSegmentIncarnation(&txn, index.next_segment_id - 1, 1, 2, false);
        try txn.commit();
    }
    try std.testing.expectError(error.InvalidSparseSegment, expectIncarnationSearch(&index, 1, &.{}, false));
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        try putSegmentIncarnation(&txn, index.next_segment_id - 1, 1, 1, false);
        try txn.commit();
    }
    try index.batch(&.{}, &.{"a"});
    try index.batchWithOptions(&.{.{ .doc_id = "a", .doc_num = 1, .vec = .{ .indices = &.{}, .values = &.{} } }}, &.{}, bulk);
    try std.testing.expect(try index.compactSegmentsWithOptions(alloc, .{ .min_segments = 2, .max_segments = 2 }));
    try std.testing.expectEqual(@as(usize, 0), try index.segmentCount());
    try std.testing.expectEqual(@as(u64, 1), index.stats().doc_count);
    try expectIncarnationSearch(&index, 1, &.{}, false);
}

test "sparse incarnation budget denial precedes capture and bulk mutation" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "sparse-incarnation-budget");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(alloc, path, .{ .backend = .lsm });
    defer index.close();
    const bulk: BatchOptions = .{ .backend_batch_options = .{ .mode = .bulk_ingest }, .prefer_bulk_build = true, .assume_new_doc_ids = true };
    try index.batchWithOptions(&.{.{ .doc_id = "a", .doc_num = 1, .vec = .{ .indices = &.{1}, .values = &.{2} } }}, &.{}, bulk);
    try index.batchWithOptions(&.{.{ .doc_id = "b", .doc_num = 2, .vec = .{ .indices = &.{1}, .values = &.{3} } }}, &.{}, bulk);
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
    index.attachResourceManager(&manager);
    // A failing task allocator proves admission happens before capture allocates.
    try std.testing.expectError(error.ResourceBudgetExceeded, index.beginSegmentCompactionTask(std.testing.failing_allocator, .{ .min_segments = 2, .max_segments = 2 }));
    try std.testing.expectError(error.ResourceBudgetExceeded, index.batchWithOptions(&.{.{ .doc_id = "c", .doc_num = 3, .vec = .{ .indices = &.{1}, .values = &.{4} } }}, &.{}, bulk));
    try std.testing.expectEqual(@as(usize, 2), try index.segmentCount());
    try std.testing.expectEqual(@as(u64, 2), index.stats().doc_count);
    const stats = manager.snapshot().slices[@backingInt(resource_manager_mod.Slice.sparse_apply_working_set)];
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    index.resource_manager = null;
    try expectIncarnationSearch(&index, 1, &.{ .{ .id = "a", .score = 2 }, .{ .id = "b", .score = 3 } }, false);
}

test "sparse incarnation preflight amortizes repeated terms and conservatively counts cache collisions" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "sparse-incarnation-preflight");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(alloc, path, .{ .backend = .lsm });
    defer index.close();
    var postings: [128]BulkPosting = undefined;
    for (&postings, 0..) |*posting, i| posting.* = .{ .term_id = @intCast(i), .doc_num = 1, .weight = 1, .doc_id = "a" };
    const repeated = try encodeSegmentFromSortedPostings(alloc, &postings, 128);
    defer alloc.free(repeated);
    var txn = try index.beginReadTxn();
    defer txn.abort();
    const repeated_bytes = try incarnationWorkingBytes(&txn, repeated, false, null, false);
    // Includes encoded buffers and posting arrays, not 512 metadata bytes for
    // every occurrence of the same document. Final reservation doubles this.
    try std.testing.expect(repeated_bytes < postings.len * 512);
    for (&postings, 0..) |*posting, i| posting.doc_num = if (i % 2 == 0) 1 else 4097;
    const collisions = try encodeSegmentFromSortedPostings(alloc, &postings, 128);
    defer alloc.free(collisions);
    const collision_bytes = try incarnationWorkingBytes(&txn, collisions, false, null, false);
    try std.testing.expect(collision_bytes >= repeated_bytes + (postings.len - 1) * 512);
}

test "sparse chunk encoding round-trip" {
    const alloc = std.testing.allocator;
    const doc_nums = [_]u32{ 10, 20, 35 };
    const weights = [_]f32{ 0.5, 0.8, 0.3 };

    const encoded = try encodeChunk(alloc, &doc_nums, &weights);
    defer alloc.free(encoded);

    const decoded = try decodeChunk(alloc, encoded);
    defer alloc.free(decoded.doc_nums);
    defer alloc.free(decoded.weights);

    try std.testing.expectEqual(@as(usize, 3), decoded.doc_nums.len);
    try std.testing.expectEqual(@as(u32, 10), decoded.doc_nums[0]);
    try std.testing.expectEqual(@as(u32, 20), decoded.doc_nums[1]);
    try std.testing.expectEqual(@as(u32, 35), decoded.doc_nums[2]);

    // Weights are quantized so check approximate equality
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.weights[0], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), decoded.weights[1], 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), decoded.weights[2], 0.01);
}

test "sparse forward entry encoding round-trip" {
    const alloc = std.testing.allocator;
    const term_ids = [_]u32{ 5, 10, 42 };
    const weights = [_]f32{ 0.1, 0.9, 0.5 };

    const encoded = try encodeFwdEntry(alloc, 12345, &term_ids, &weights);
    defer alloc.free(encoded);

    const decoded = try decodeFwdEntry(alloc, encoded);
    defer alloc.free(decoded.term_ids);
    defer alloc.free(decoded.weights);

    try std.testing.expectEqual(@as(u64, 12345), decoded.doc_num);
    try std.testing.expectEqual(@as(usize, 3), decoded.term_ids.len);
    try std.testing.expectEqual(@as(u32, 5), decoded.term_ids[0]);
    try std.testing.expectEqual(@as(u32, 42), decoded.term_ids[2]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), decoded.weights[1], 0.001);
}

test "sparse insert and search single doc" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s1");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const indices = [_]u32{ 0, 5, 10 };
    const values = [_]f32{ 0.5, 0.8, 0.3 };
    const writes = [_]SparseWrite{.{
        .doc_id = "doc1",
        .vec = .{ .indices = &indices, .values = &values },
    }};
    try idx.batch(&writes, &.{});

    // Search with matching terms
    const q_indices = [_]u32{ 0, 5 };
    const q_values = [_]f32{ 1.0, 1.0 };
    const query = SparseVector{ .indices = &q_indices, .values = &q_values };

    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("doc1", results[0].doc_id);
    // Score ≈ 1.0*0.5 + 1.0*0.8 ≈ 1.3 (with quantization noise)
    try std.testing.expect(results[0].score > 1.0);
}

test "sparse multi-doc top-k search" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    // doc1: strong on term 0
    const w1_i = [_]u32{0};
    const w1_v = [_]f32{0.9};
    // doc2: strong on term 1
    const w2_i = [_]u32{1};
    const w2_v = [_]f32{0.9};
    // doc3: weak on term 0
    const w3_i = [_]u32{0};
    const w3_v = [_]f32{0.1};

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &w1_i, .values = &w1_v } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &w2_i, .values = &w2_v } },
        .{ .doc_id = "doc3", .vec = .{ .indices = &w3_i, .values = &w3_v } },
    };
    try idx.batch(&writes, &.{});

    // Search for term 0 only
    const q_i = [_]u32{0};
    const q_v = [_]f32{1.0};
    const query = SparseVector{ .indices = &q_i, .values = &q_v };

    const results = try idx.search(alloc, &query, 2);
    defer SparseIndex.freeResults(alloc, results);

    // Should return doc1 and doc3 (both have term 0), doc1 ranked higher
    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("doc1", results[0].doc_id);
    try std.testing.expect(results[0].score > results[1].score);
}

test "sparse bulk append builds searchable postings" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-append");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{ .chunk_size = 2 });
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 1.0, 0.25 } } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{1}, .values = &.{0.5} } },
        .{ .doc_id = "doc3", .vec = .{ .indices = &.{2}, .values = &.{0.75} } },
    };
    try idx.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .backend_batch_options = .{ .mode = .bulk_ingest },
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const stats = idx.stats();
    try std.testing.expectEqual(@as(u64, 3), stats.doc_count);
    try std.testing.expectEqual(@as(u64, 2), stats.term_count);

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("doc1", results[0].doc_id);
}

test "sparse lsm durable boundary checkpoint retires retained wal" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-lsm-wal-checkpoint");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{
        .backend = .lsm,
        .lsm_options = .{ .flush_threshold = 1024 },
    });
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{2}, .values = &.{0.5} } },
    };
    try idx.batch(&writes, &.{});

    const before = switch (idx.owner) {
        .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
        else => return error.ExpectedLsmOwner,
    };
    try std.testing.expect(before.wal_retained_bytes > 0);

    try idx.checkpointLsmWalAfterDurableBoundary();

    const after = switch (idx.owner) {
        .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
        else => return error.ExpectedLsmOwner,
    };
    try std.testing.expectEqual(@as(u64, 0), after.wal_retained_bytes);
}

test "sparse bulk append extends existing partial chunk" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-append-existing");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{ .chunk_size = 3 });
    defer idx.close();

    try idx.batch(&[_]SparseWrite{.{
        .doc_id = "doc1",
        .vec = .{ .indices = &.{1}, .values = &.{0.25} },
    }}, &.{});

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{1}, .values = &.{0.5} } },
        .{ .doc_id = "doc3", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc4", .vec = .{ .indices = &.{1}, .values = &.{0.75} } },
    };
    try idx.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .backend_batch_options = .{ .mode = .bulk_ingest },
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 4), results.len);
    try std.testing.expectEqualStrings("doc3", results[0].doc_id);
}

test "sparse bulk append accounts resource working set" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-resource");
    defer cleanupTmp(path);

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();
    idx.attachResourceManager(&manager);

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &.{ 1, 2, 3 }, .values = &.{ 1.0, 0.5, 0.25 } } },
        .{ .doc_id = "doc2", .vec = .{ .indices = &.{ 1, 3 }, .values = &.{ 0.75, 0.5 } } },
    };
    try idx.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .backend_batch_options = .{ .mode = .bulk_ingest },
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const resource_stats = manager.snapshot().slices[@backingInt(resource_manager_mod.Slice.sparse_apply_working_set)];
    try std.testing.expectEqual(@as(u64, 0), resource_stats.used_bytes);
    try std.testing.expect(resource_stats.peak_bytes > 0);
}

test "sparse segment compaction preserves bulk search results" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-bulk-compact");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{ .chunk_size = 2 });
    defer idx.close();

    for (0..4) |batch_idx| {
        const doc_a = try std.fmt.allocPrint(alloc, "doc-{d}-a", .{batch_idx});
        defer alloc.free(doc_a);
        const doc_b = try std.fmt.allocPrint(alloc, "doc-{d}-b", .{batch_idx});
        defer alloc.free(doc_b);
        const writes = [_]SparseWrite{
            .{ .doc_id = doc_a, .vec = .{ .indices = &.{ 7, 9 }, .values = &.{ 1.0, 0.25 } } },
            .{ .doc_id = doc_b, .vec = .{ .indices = &.{7}, .values = &.{0.5} } },
        };
        try idx.batchWithOptions(&writes, &.{}, .{
            .defer_term_range_updates = true,
            .backend_batch_options = .{ .mode = .bulk_ingest },
            .prefer_bulk_build = true,
            .assume_new_doc_ids = true,
        });
    }

    try std.testing.expectEqual(@as(usize, 4), try idx.segmentCount());

    const query = SparseVector{ .indices = &.{7}, .values = &.{1.0} };
    const before = try idx.search(alloc, &query, 20);
    defer SparseIndex.freeResults(alloc, before);
    try std.testing.expectEqual(@as(usize, 8), before.len);

    try std.testing.expect(try idx.compactSegmentsWithOptions(alloc, .{ .min_segments = 2, .max_segments = 16 }));
    try std.testing.expectEqual(@as(usize, 1), try idx.segmentCount());

    const after = try idx.search(alloc, &query, 20);
    defer SparseIndex.freeResults(alloc, after);
    try std.testing.expectEqual(before.len, after.len);
    for (before, 0..) |hit, i| {
        try std.testing.expectEqualStrings(hit.doc_id, after[i].doc_id);
        try std.testing.expectApproxEqAbs(hit.score, after[i].score, 0.01);
    }
}

test "sparse batch delete removes from posting lists" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s3");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const idx1 = [_]u32{0};
    const val1 = [_]f32{1.0};
    const writes = [_]SparseWrite{
        .{ .doc_id = "doc1", .vec = .{ .indices = &idx1, .values = &val1 } },
    };
    try idx.batch(&writes, &.{});

    // Delete doc1
    const deletes = [_][]const u8{"doc1"};
    try idx.batch(&.{}, &deletes);

    // Search should return empty
    const q_i = [_]u32{0};
    const q_v = [_]f32{1.0};
    const query = SparseVector{ .indices = &q_i, .values = &q_v };

    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sparse constrained search filters before top-k ranking" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-constrained");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc:a", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc:b", .vec = .{ .indices = &.{1}, .values = &.{0.1} } },
        .{ .doc_id = "doc:c", .vec = .{ .indices = &.{1}, .values = &.{0.9} } },
    };
    try idx.batch(&writes, &.{});

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const filtered = try idx.searchConstrained(alloc, &query, 1, .{
        .filter_doc_ids = &.{ "doc:b", "doc:c" },
        .exclude_doc_ids = &.{"doc:c"},
    });
    defer SparseIndex.freeResults(alloc, filtered);

    try std.testing.expectEqual(@as(usize, 1), filtered.len);
    try std.testing.expectEqualStrings("doc:b", filtered[0].doc_id);
}

test "external lake sparse native key membership is applied before top-k and propagates callback errors" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "native-key-predicate");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();
    const writes = [_]SparseWrite{
        .{ .doc_id = "high", .vec = .{ .indices = &.{1}, .values = &.{10} } },
        .{ .doc_id = "low", .vec = .{ .indices = &.{1}, .values = &.{1} } },
    };
    const Predicate = struct {
        fail: bool = false,
        fn allows(raw: *anyopaque, key: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.Cancelled;
            return std.mem.eql(u8, key, "low");
        }
    };
    var predicate: Predicate = .{};
    try idx.batchWithOptions(&writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    const result = try idx.searchConstrained(alloc, &query, 1, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows } });
    defer SparseIndex.freeResults(alloc, result);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("low", result[0].doc_id);
    predicate.fail = true;
    try std.testing.expectError(error.Cancelled, idx.searchConstrained(alloc, &query, 1, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows } }));
}

test "sparse search supports caller supplied ordinal doc nums" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s2-ordinal-doc-nums");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "doc:a", .doc_num = 42, .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc:b", .doc_num = 7, .vec = .{ .indices = &.{1}, .values = &.{0.5} } },
    };
    try idx.batch(&writes, &.{});

    try std.testing.expectEqual(@as(?u32, 42), try idx.debugDocNumForDocId("doc:a"));
    try std.testing.expectEqual(@as(?u32, 7), try idx.debugDocNumForDocId("doc:b"));

    const query = SparseVector{ .indices = &.{1}, .values = &.{1.0} };
    const filtered = try idx.searchConstrained(alloc, &query, 10, .{
        .filter_doc_nums = &.{42},
    });
    defer SparseIndex.freeResults(alloc, filtered);

    try std.testing.expectEqual(@as(usize, 1), filtered.len);
    try std.testing.expectEqualStrings("doc:a", filtered[0].doc_id);
    try std.testing.expectEqual(@as(?u32, 42), filtered[0].doc_num);

    const excluded = try idx.searchConstrained(alloc, &query, 10, .{
        .exclude_doc_nums = &.{42},
    });
    defer SparseIndex.freeResults(alloc, excluded);

    try std.testing.expectEqual(@as(usize, 1), excluded.len);
    try std.testing.expectEqualStrings("doc:b", excluded[0].doc_id);
    try std.testing.expectEqual(@as(?u32, 7), excluded[0].doc_num);
}

test "sparse handoff range preserves doc numbers and postings" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = tmpPath(&src_buf, "split-src");
    defer cleanupTmp(src_path);
    var dest_buf: [256]u8 = undefined;
    const dest_path = tmpPath(&dest_buf, "split-dest");
    defer cleanupTmp(dest_path);

    var src = try SparseIndex.open(alloc, src_path, .{});
    defer src.close();
    var dest = try SparseIndex.open(alloc, dest_path, .{});
    defer dest.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "a", .vec = .{ .indices = &.{ 1, 3 }, .values = &.{ 0.5, 0.2 } } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.8, 0.7 } } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{ 2, 4 }, .values = &.{ 0.9, 0.4 } } },
    };
    try src.batch(&writes, &.{});

    var rebuilt = try src.handoffRangeInto(&dest, alloc, "b", "", true);
    defer rebuilt.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), rebuilt.doc_ids.len);
    try std.testing.expectEqualStrings("b", rebuilt.doc_ids[0]);
    try std.testing.expectEqualStrings("c", rebuilt.doc_ids[1]);
    try std.testing.expectEqual(@as(u64, 3), dest.next_doc_num);

    const query = SparseVector{
        .indices = &.{2},
        .values = &.{1.0},
    };
    const results = try dest.search(alloc, &query, 4);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("c", results[0].doc_id);
    try std.testing.expectEqualStrings("b", results[1].doc_id);
}

test "sparse handoff range includes bulk-built docmap docs" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = tmpPath(&src_buf, "split-src-bulk");
    defer cleanupTmp(src_path);
    var dest_buf: [256]u8 = undefined;
    const dest_path = tmpPath(&dest_buf, "split-dest-bulk");
    defer cleanupTmp(dest_path);

    var src = try SparseIndex.open(alloc, src_path, .{ .chunk_size = 2 });
    defer src.close();
    var dest = try SparseIndex.open(alloc, dest_path, .{ .chunk_size = 2 });
    defer dest.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "a", .vec = .{ .indices = &.{1}, .values = &.{0.1} } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{1}, .values = &.{0.2} } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.9, 0.7 } } },
        .{ .doc_id = "d", .vec = .{ .indices = &.{2}, .values = &.{0.8} } },
    };
    try src.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    var rebuilt = try src.handoffRangeInto(&dest, alloc, "c", "", true);
    defer rebuilt.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), rebuilt.doc_ids.len);
    try std.testing.expectEqual(@as(u64, 4), dest.next_doc_num);
    try std.testing.expectEqual(@as(u64, 2), dest.stats().doc_count);

    const query = SparseVector{ .indices = &.{2}, .values = &.{1.0} };
    const results = try dest.search(alloc, &query, 4);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("d", results[0].doc_id);
    try std.testing.expectEqualStrings("c", results[1].doc_id);
}

test "sparse split planning stats classify right-only and mixed chunks" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = tmpPath(&src_buf, "split-plan");
    defer cleanupTmp(src_path);

    var src = try SparseIndex.open(alloc, src_path, .{ .chunk_size = 2 });
    defer src.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "a", .vec = .{ .indices = &.{1}, .values = &.{0.1} } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{1}, .values = &.{0.2} } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{1}, .values = &.{0.3} } },
        .{ .doc_id = "d", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.4, 0.9 } } },
    };
    try src.batch(&writes, &.{});

    const stats = try src.splitPlanningStats(alloc, "b", "");
    try std.testing.expectEqual(@as(usize, 3), stats.selected_docs);
    try std.testing.expectEqual(@as(usize, 2), stats.touched_terms);
    try std.testing.expectEqual(@as(usize, 2), stats.right_only_chunks);
    try std.testing.expectEqual(@as(usize, 1), stats.mixed_chunks);
    try std.testing.expectEqual(@as(usize, 3), stats.right_only_postings);
    try std.testing.expectEqual(@as(usize, 1), stats.mixed_right_postings);
}

test "sparse split planning stats include bulk-built segment docs" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = tmpPath(&src_buf, "split-plan-bulk");
    defer cleanupTmp(src_path);

    var src = try SparseIndex.open(alloc, src_path, .{ .chunk_size = 2 });
    defer src.close();

    const writes = [_]SparseWrite{
        .{ .doc_id = "a", .vec = .{ .indices = &.{1}, .values = &.{0.1} } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{1}, .values = &.{0.2} } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{1}, .values = &.{0.9} } },
        .{ .doc_id = "d", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.8, 0.7 } } },
    };
    try src.batchWithOptions(&writes, &.{}, .{
        .defer_term_range_updates = true,
        .prefer_bulk_build = true,
        .assume_new_doc_ids = true,
    });

    const stats = try src.splitPlanningStats(alloc, "c", "");
    try std.testing.expectEqual(@as(usize, 2), stats.selected_docs);
    try std.testing.expectEqual(@as(usize, 2), stats.touched_terms);
    try std.testing.expectEqual(@as(usize, 2), stats.right_only_chunks);
    try std.testing.expectEqual(@as(usize, 0), stats.mixed_chunks);
    try std.testing.expectEqual(@as(usize, 3), stats.right_only_postings);
}

test "sparse empty search returns empty" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s4");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    const q_i = [_]u32{0};
    const q_v = [_]f32{1.0};
    const query = SparseVector{ .indices = &q_i, .values = &q_v };

    const results = try idx.search(alloc, &query, 10);
    defer SparseIndex.freeResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "sparse reopen preserves data" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s5");
    defer cleanupTmp(path);

    // Insert
    {
        var idx = try SparseIndex.open(alloc, path, .{});
        defer idx.close();
        const idx1 = [_]u32{0};
        const val1 = [_]f32{0.7};
        const writes = [_]SparseWrite{
            .{ .doc_id = "persist_doc", .vec = .{ .indices = &idx1, .values = &val1 } },
        };
        try idx.batch(&writes, &.{});
    }

    // Reopen and search
    {
        var idx = try SparseIndex.open(alloc, path, .{});
        defer idx.close();

        const q_i = [_]u32{0};
        const q_v = [_]f32{1.0};
        const query = SparseVector{ .indices = &q_i, .values = &q_v };
        const results = try idx.search(alloc, &query, 10);
        defer SparseIndex.freeResults(alloc, results);

        try std.testing.expectEqual(@as(usize, 1), results.len);
        try std.testing.expectEqualStrings("persist_doc", results[0].doc_id);
    }
}

test "sparse backend adapters expose txn cursor operations" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s-adapter");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    {
        var txn = try idx.beginWriteTxn();
        errdefer txn.abort();
        const encoded = std.mem.toBytes(@as(u64, 7));
        try txn.put("meta:next_doc_num", &encoded);
        {
            // Cursors must close before commit: committing with an open
            // cursor fails closed with error.TransactionCursorActive.
            var cur = try txn.openCursor();
            defer cur.close();
            try std.testing.expectEqualStrings("meta:next_doc_num", (try cur.first()).?.key);
        }
        try txn.commit();
    }

    {
        var txn = try idx.beginReadTxn();
        defer txn.abort();
        const encoded = try txn.get("meta:next_doc_num");
        try std.testing.expectEqual(@as(u64, 7), std.mem.readInt(u64, encoded[0..8], .little));
    }
}

test "sparse backend store opens concrete txn handles" {
    const alloc = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "s-store");
    defer cleanupTmp(path);

    var idx = try SparseIndex.open(alloc, path, .{});
    defer idx.close();

    var backend = idx.backendStore();
    try std.testing.expect(backend.capabilities().cursors);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        const encoded = std.mem.toBytes(@as(u64, 9));
        try txn.put("meta:next_doc_num", &encoded);
        try txn.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        const encoded = try txn.get("meta:next_doc_num");
        try std.testing.expectEqual(@as(u64, 9), std.mem.readInt(u64, encoded[0..8], .little));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        const encoded = std.mem.toBytes(@as(u64, 10));
        try batch.put("meta:next_doc_num", &encoded);
        try batch.commit();
    }
}

test "sparse physical ordinal selection uses canonical scoring and intersects direct constraints" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "physical-ordinals");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{});
    defer idx.close();
    try idx.batchWithOptions(&.{
        .{ .doc_id = "high", .vec = .{ .indices = &.{1}, .values = &.{10} } },
        .{ .doc_id = "low", .vec = .{ .indices = &.{1}, .values = &.{1} } },
    }, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    const Predicate = struct {
        key: []const u8 = "low",
        fn allows(_: *anyopaque, _: []const u8) !bool {
            return error.UnexpectedReverseIdentityRead;
        }
        fn select(raw: *anyopaque, alloc: Allocator, lookup: OrdinalLookup) !?@import("../encoding/roaring.zig").RoaringBitmap {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var result = @import("../encoding/roaring.zig").RoaringBitmap.init(alloc);
            errdefer result.deinit();
            if (try lookup.one(lookup.ptr, self.key)) |num| try result.add(num);
            return result;
        }
    };
    var predicate: Predicate = .{};
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    const selected = try idx.searchConstrained(a, &query, 1, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_ordinals = Predicate.select } });
    defer SparseIndex.freeResults(a, selected);
    try std.testing.expectEqual(@as(usize, 1), selected.len);
    try std.testing.expectEqualStrings("low", selected[0].doc_id);
    const excluded = try idx.searchConstrained(a, &query, 1, .{ .exclude_doc_ids = &.{"low"}, .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_ordinals = Predicate.select } });
    defer SparseIndex.freeResults(a, excluded);
    try std.testing.expectEqual(@as(usize, 0), excluded.len);
    for ([_]f32{ -1, 0 }) |weight| {
        const signed_query: SparseVector = .{ .indices = &.{1}, .values = &.{weight} };
        const signed = try idx.searchConstrained(a, &signed_query, 1, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_ordinals = Predicate.select } });
        defer SparseIndex.freeResults(a, signed);
        try std.testing.expectEqual(@as(usize, 1), signed.len);
        try std.testing.expectEqual(weight, signed[0].score);
    }
    const absent_query: SparseVector = .{ .indices = &.{2}, .values = &.{-1} };
    const absent = try idx.searchConstrained(a, &absent_query, 1, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_ordinals = Predicate.select } });
    defer SparseIndex.freeResults(a, absent);
    try std.testing.expectEqual(@as(usize, 0), absent.len);
    predicate.key = "missing";
    const empty = try idx.searchConstrained(a, &query, 1, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_ordinals = Predicate.select } });
    defer SparseIndex.freeResults(a, empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
}

test "sparse forward locators survive compaction replacement deletion and restart" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "forward-locators");
    defer cleanupTmp(path);
    {
        var idx = try SparseIndex.open(a, path, .{});
        defer idx.close();
        for ([_][]const u8{ "first", "second" }) |id| try idx.batchWithOptions(&.{.{ .doc_id = id, .vec = .{ .indices = &.{1}, .values = &.{1} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
        {
            var txn = try idx.beginReadTxn();
            defer txn.abort();
            try std.testing.expect((try readLocator(&txn, "first")) != null);
            const bytes = (try locatedForwardBytes(&txn, "first")).?;
            try std.testing.expectEqual((try idx.docNumForDocIdTxn(&txn, "first")).?, try decodeFwdDocNum(bytes));
        }
        try std.testing.expect(try idx.compactSegmentsWithOptions(a, .{ .min_segments = 2 }));
        {
            var txn = try idx.beginReadTxn();
            defer txn.abort();
            try std.testing.expect((try locatedForwardBytes(&txn, "first")) != null);
            try std.testing.expect((try locatedForwardBytes(&txn, "second")) != null);
        }
        try idx.batch(&.{.{ .doc_id = "first", .vec = .{ .indices = &.{1}, .values = &.{5} } }}, &.{"second"});
    }
    var reopened = try SparseIndex.open(a, path, .{});
    defer reopened.close();
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    const hits = try reopened.searchConstrained(a, &query, 3, .{ .filter_doc_ids = &.{ "first", "second" } });
    defer SparseIndex.freeResults(a, hits);
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    try std.testing.expectEqualStrings("first", hits[0].doc_id);
    try std.testing.expectApproxEqAbs(@as(f32, 5), hits[0].score, 0.001);
}

test "sparse posting block bounds preserve v1 decoding and reject disjoint selections" {
    const a = std.testing.allocator;
    const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
    const nums = &.{ @as(u32, 7), 100, std.math.maxInt(u32) };
    const bytes = try encodeChunk(a, nums, &.{ -2, 0, 3 });
    defer a.free(bytes);
    try std.testing.expectEqual(@as(u8, 1), bytes[0]);
    try std.testing.expectEqual(@as(usize, 13 + 3 * 5), bytes.len);
    const extended = try encodeChunkOrdinalRange(a, "first", "last", nums, true);
    defer a.free(extended);
    const legacy = try encodeChunkRangeMeta(a, "first", "last");
    defer a.free(legacy);
    // The unmodified legacy decoder ignores the ordinal trailer.
    const old_range = try decodeChunkRangeMeta(extended);
    try std.testing.expectEqualStrings("first", old_range.min_doc_id);
    try std.testing.expectEqualStrings("last", old_range.max_doc_id);
    var rows = Bitmap.init(a);
    defer rows.deinit();
    try rows.add(1);
    try std.testing.expect(!try chunkMayMatch(bytes, &rows));
    try std.testing.expect(!try postingRangeMayMatch(extended, bytes, &rows));
    try rows.add(std.math.maxInt(u32));
    try std.testing.expect(try postingRangeMayMatch(extended, bytes, &rows));
    try std.testing.expect(try postingRangeMayMatch(legacy, bytes, &rows));
    const decoded = try decodeChunk(a, bytes);
    defer a.free(decoded.doc_nums);
    defer a.free(decoded.weights);
    try std.testing.expectEqualSlices(u32, nums, decoded.doc_nums);
    try std.testing.expectError(error.InvalidChunk, decodeChunk(a, bytes[0 .. bytes.len - 1]));
}

test "sparse physical block maps retain current identities across compaction edits and restart" {
    const a = std.testing.allocator;
    const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
    const prefix = "lake2:" ++ "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" ++ ":00000000:";
    const first = prefix ++ "0000000000000001";
    const second = prefix ++ "0000000000000402";
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "physical-block-maps");
    defer cleanupTmp(path);
    {
        var idx = try SparseIndex.open(a, path, .{});
        defer idx.close();
        for ([_][]const u8{ first, second }) |id| try idx.batchWithOptions(&.{.{ .doc_id = id, .vec = .{ .indices = &.{1}, .values = &.{1} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
        try std.testing.expect(try idx.compactSegmentsWithOptions(a, .{ .min_segments = 2 }));
        try idx.batch(&.{.{ .doc_id = first, .vec = .{ .indices = &.{1}, .values = &.{5} } }}, &.{second});
    }
    var idx = try SparseIndex.open(a, path, .{});
    defer idx.close();
    var txn = try idx.beginReadTxn();
    defer txn.abort();
    var rows = Bitmap.init(a);
    defer rows.deinit();
    try rows.addRange(0, 2048);
    var selected = Bitmap.init(a);
    defer selected.deinit();
    try std.testing.expect(try selectPhysicalBlock(&txn, a, prefix, 0, &rows, &selected, null));
    try std.testing.expectEqual(@as(usize, 1), selected.cardinality());
    try std.testing.expect(selected.contains((try idx.docNumForDocIdTxn(&txn, first)).?));
    // A legacy checkpoint without the completeness proof must request fallback.
    txn.abort();
    var write = try idx.beginWriteTxn();
    errdefer write.abort();
    try write.delete(metaKey(meta_complete_physical));
    try write.commit();
    txn = try idx.beginReadTxn();
    try std.testing.expect(!try selectPhysicalBlock(&txn, a, prefix, 0, &rows, &selected, null));
}

test "sparse bounded top k matches legacy collection for signed terms and deleted winners" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "bounded-top-k");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{});
    defer idx.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const writes = try ca.alloc(SparseWrite, 300);
    for (writes, 0..) |*write, i| {
        const weights = try ca.alloc(f32, 2);
        weights[0] = @floatFromInt(i);
        weights[1] = @floatFromInt(i % 7);
        write.* = .{ .doc_id = try std.fmt.allocPrint(ca, "doc-{d}", .{i}), .vec = .{ .indices = &.{ 1, 2 }, .values = weights } };
    }
    try idx.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    try idx.batch(&.{}, &.{ "doc-0", "doc-1", "doc-2" });
    const query: SparseVector = .{ .indices = &.{ 1, 2 }, .values = &.{ -1, 3 } };
    const bounded = try idx.search(a, &query, 7);
    defer SparseIndex.freeResults(a, bounded);
    try std.testing.expectEqual(@as(usize, 7), bounded.len);
    var txn = try idx.beginWriteTxn();
    errdefer txn.abort();
    try txn.delete(metaKey(meta_complete_locators));
    try txn.commit();
    const legacy = try idx.search(a, &query, 7);
    defer SparseIndex.freeResults(a, legacy);
    try std.testing.expectEqual(bounded.len, legacy.len);
    for (bounded, legacy) |actual, expected| {
        try std.testing.expectEqualStrings(expected.doc_id, actual.doc_id);
        try std.testing.expectEqual(expected.score, actual.score);
    }
}

test "sparse bounded accumulation preserves signed f32 addition order across spill" {
    const a = std.testing.allocator;
    var scores: ScoreAccumulator = .{ .alloc = a, .limit = 2, .options = .{ .io = std.testing.io, .directory = "/tmp", .chunk_records = 2, .chunk_bytes = 128 } };
    defer scores.deinit();
    var expected: [100]f32 = @splat(0);
    for ([_]f32{ 16777216, 1, -16777216, -0.25, 0.25, -2 }) |term| {
        for (0..100) |doc| {
            const contribution = term * @as(f32, @floatFromInt(doc + 1));
            expected[doc] += contribution;
            try scores.add(@intCast(doc), contribution);
            try std.testing.expect(scores.values.count() <= 2);
        }
    }
    try scores.finish();
    var count: usize = 0;
    while (try scores.next(null)) |entry| {
        try std.testing.expectEqual(@as(u32, @intCast(count)), entry.doc_num);
        try std.testing.expectEqual(@as(u32, @bitCast(expected[count])), @as(u32, @bitCast(entry.score)));
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "sparse document streams bound retained segment bytes before scoring" {
    const a = std.testing.allocator;
    var bytes: [256]u8 = undefined;
    const path = tmpPath(&bytes, "stream-byte-admission");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{});
    defer index.close();
    try index.batchWithOptions(&.{.{ .doc_id = "one", .vec = .{ .indices = &.{1}, .values = &.{1} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    var txn = try index.beginReadTxn();
    defer txn.abort();
    var query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    var page_reader: PageReader = .{ .txn = &txn };
    defer page_reader.deinit();
    try std.testing.expect((try collectPostingStreams(a, &txn, &query, null, 0, &page_reader, null)) == null);
    const admitted = (try collectPostingStreams(a, &txn, &query, null, 64 * 1024 * 1024, &page_reader, null)).?;
    defer a.free(admitted);
    try std.testing.expectEqual(@as(usize, 1), admitted.len);
    query = .{ .indices = &.{999}, .values = &.{1} };
    const unmatched = (try collectPostingStreams(a, &txn, &query, null, 0, &page_reader, null)).?;
    defer a.free(unmatched);
    try std.testing.expectEqual(@as(usize, 0), unmatched.len);
}

test "sparse paged postings roundtrip seek and reclaim immutable blocks" {
    const a = std.testing.allocator;
    var path_bytes: [256]u8 = undefined;
    const path = tmpPath(&path_bytes, "paged-postings");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{});
    defer index.close();
    const postings = [_]BulkPosting{
        .{ .term_id = 1, .doc_num = 0, .weight = 1, .doc_id = "a" },
        .{ .term_id = 1, .doc_num = 1, .weight = 1.1, .doc_id = "b" },
        .{ .term_id = 1, .doc_num = 2, .weight = 100, .doc_id = "c" },
        .{ .term_id = 2, .doc_num = 2, .weight = -2, .doc_id = "c" },
    };
    const legacy = try encodeSegmentFromSortedPostings(a, &postings, 2);
    defer a.free(legacy);
    {
        var txn = try index.beginBatchTxn(.{});
        errdefer txn.abort();
        const root = try posting_pages.publish(a, &txn, 42, legacy);
        defer a.free(root);
        const restored = try posting_pages.materialize(a, &txn, 42, root);
        defer a.free(restored);
        try std.testing.expectEqualSlices(u8, legacy, restored);
        try std.testing.expect(try posting_pages.hasTermTxn(&txn, 42, root, 1));
        try std.testing.expect(!try posting_pages.hasTermTxn(&txn, 42, root, 99));
        try txn.commit();
    }
    {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        var reader: PageReader = .{ .txn = &txn, .io = std.testing.io };
        defer reader.deinit();
        var stream: daat.Stream = .{ .weight = 1, .segment = 42, .version = 2, .term = 1, .reader = reader.interface(), .allocator = a };
        defer stream.deinit();
        try stream.seek(0);
        try stream.seek(2);
        try std.testing.expectEqual(@as(?u32, 2), stream.doc);
        try std.testing.expectEqual(@as(f32, 100), try stream.contribution());
        try std.testing.expectEqual(@as(usize, 1), reader.blocks);
        try stream.advance();
        try std.testing.expectEqual(null, stream.doc);
        try std.testing.expectEqual(@as(usize, 0), reader.resident);
    }
    {
        var txn = try index.beginBatchTxn(.{});
        errdefer txn.abort();
        try posting_pages.remove(&txn, 42);
        try txn.commit();
    }
    var txn = try index.beginReadTxn();
    defer txn.abort();
    var reader: PageReader = .{ .txn = &txn, .io = std.testing.io };
    defer reader.deinit();
    var stream: daat.Stream = .{ .weight = 1, .segment = 42, .term = 1, .reader = reader.interface(), .allocator = a };
    defer stream.deinit();
    try stream.seek(0);
    try std.testing.expectEqual(null, stream.doc);
}

test "sparse paged archive admission depends on live blocks rather than segment bytes" {
    const a = std.testing.allocator;
    var path_bytes: [256]u8 = undefined;
    const path = tmpPath(&path_bytes, "paged-archive-admission");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{ .chunk_size = 128 });
    defer index.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const writes = try arena.allocator().alloc(SparseWrite, 2048);
    for (writes, 0..) |*write, i| write.* = .{ .doc_id = try std.fmt.allocPrint(arena.allocator(), "doc-{d:0>4}", .{i}), .vec = .{ .indices = &.{1}, .values = &.{1} } };
    try index.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    for (writes, 0..) |*write, i| write.doc_id = try std.fmt.allocPrint(arena.allocator(), "doc-{d:0>4}", .{i + 2048});
    try index.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    var selected = @import("../encoding/roaring.zig").RoaringBitmap.init(a);
    defer selected.deinit();
    try selected.add(2047);
    var txn = try index.beginReadTxn();
    defer txn.abort();
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        const root = (try cursor.seekAtOrAfter(taggedPrefix(key_segment))).?.value;
        try std.testing.expect(posting_pages.paged(root));
        try std.testing.expect(try posting_pages.materializedSize(root) > 8192);
    }
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    var reader: PageReader = .{ .txn = &txn, .budget = 8192 };
    defer reader.deinit();
    const streams = (try collectPostingStreams(a, &txn, &query, null, 1024, &reader, &selected)).?;
    defer {
        for (streams) |*stream| stream.deinit();
        a.free(streams);
    }
    try std.testing.expectEqual(@as(usize, 1), streams.len);
    const Context = struct {
        pub fn check(_: *@This()) !void {}
        pub fn allows(_: *@This(), _: daat.Stream, _: u32) !bool {
            return true;
        }
        pub fn nextCandidate(_: *@This(), first: u32) !u64 {
            return if (first <= 2047) 2047 else @as(u64, std.math.maxInt(u32)) + 1;
        }
    };
    var context: Context = .{};
    var stats: daat.Stats = .{};
    const results = try daat.collect(a, streams, 1, &context, &stats);
    defer a.free(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(u32, 2047), results[0].doc_num);
    try std.testing.expectEqual(@as(usize, 1), reader.blocks);
    try std.testing.expect(reader.bytes < 8192);
}

test "sparse selective filters retain quantized scores and tie order" {
    const a = std.testing.allocator;
    var path_bytes: [256]u8 = undefined;
    const path = tmpPath(&path_bytes, "canonical-filter-scores");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{});
    defer index.close();
    try index.batchWithOptions(&.{
        .{ .doc_id = "a", .vec = .{ .indices = &.{1}, .values = &.{1} } },
        .{ .doc_id = "b", .vec = .{ .indices = &.{1}, .values = &.{1.1} } },
        .{ .doc_id = "c", .vec = .{ .indices = &.{1}, .values = &.{100} } },
    }, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    const all = try index.search(a, &query, 3);
    defer SparseIndex.freeResults(a, all);
    const filtered = try index.searchConstrained(a, &query, 3, .{ .filter_doc_ids = &.{ "a", "b" } });
    defer SparseIndex.freeResults(a, filtered);
    try std.testing.expectEqual(@as(usize, 2), filtered.len);
    for (filtered, all[1..]) |actual, expected| {
        try std.testing.expectEqualStrings(expected.doc_id, actual.doc_id);
        try std.testing.expectEqual(@as(u32, @bitCast(expected.score)), @as(u32, @bitCast(actual.score)));
    }
}

test "sparse score accumulation rejects overflow in memory and spill" {
    const a = std.testing.allocator;
    var memory: ScoreAccumulator = .{ .alloc = a, .limit = 2, .options = null };
    defer memory.deinit();
    try memory.add(0, 3e38);
    try std.testing.expectError(error.SparseScoreOverflow, memory.add(0, 3e38));
    try std.testing.expectError(error.SparseScoreOverflow, memory.add(1, std.math.inf(f32)));
    var spilled: ScoreAccumulator = .{ .alloc = a, .limit = 0, .options = .{ .io = std.testing.io, .directory = "/tmp", .chunk_records = 2, .chunk_bytes = 128 } };
    defer spilled.deinit();
    try spilled.add(0, 3e38);
    try spilled.add(0, 3e38);
    try spilled.finish();
    try std.testing.expectError(error.SparseScoreOverflow, spilled.next(null));
}

test "sparse term routing skips unrelated roots and migrates legacy discovery atomically" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "sparse-term-routing");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{ .backend = .lsm });
    defer index.close();
    const bulk: BatchOptions = .{ .prefer_bulk_build = true, .assume_new_doc_ids = true };
    try index.batchWithOptions(&.{.{ .doc_id = "match", .vec = .{ .indices = &.{7}, .values = &.{2} } }}, &.{}, bulk);
    try index.batchWithOptions(&.{.{ .doc_id = "unrelated", .vec = .{ .indices = &.{9}, .values = &.{3} } }}, &.{}, bulk);
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(&.{0x0f});
        try txn.commit();
    }
    try expectIncarnationSearch(&index, 7, &.{.{ .id = "match", .score = 2 }}, false);
    try index.batchWithOptions(&.{.{ .doc_id = "new", .vec = .{ .indices = &.{11}, .values = &.{4} } }}, &.{}, bulk);
    {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        try std.testing.expect(try posting_pages.routed(&txn));
        _ = try txn.get(&posting_pages.routeKey(7, 1));
        _ = try txn.get(&posting_pages.routeKey(9, 2));
    }
    {
        var txn = try index.beginWriteTxn();
        errdefer txn.abort();
        var key_buf: [16]u8 = undefined;
        try txn.put(segmentKey(&key_buf, 2), "unreadable unrelated root");
        try txn.commit();
    }
    try expectIncarnationSearch(&index, 7, &.{.{ .id = "match", .score = 2 }}, false);
}

test "sparse streamed compaction keeps input pages pinned across competing publication" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "sparse-streamed-compaction-pin");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{ .backend = .lsm, .chunk_size = 1 });
    defer index.close();
    const bulk: BatchOptions = .{ .prefer_bulk_build = true, .assume_new_doc_ids = true };
    try index.batchWithOptions(&.{.{ .doc_id = "a", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 2, -3 } } }}, &.{}, bulk);
    try index.batchWithOptions(&.{.{ .doc_id = "b", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 5, -7 } } }}, &.{}, bulk);
    const options: SparseIndex.SegmentCompactionOptions = .{ .min_segments = 2, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } };
    var older = (try index.beginSegmentCompactionTask(a, options)).?;
    defer older.deinit(a);
    for (older.sources) |source| try std.testing.expect(posting_pages.paged(source.data));
    try std.testing.expect(try index.compactSegmentsWithOptions(a, options));
    var result = try SparseIndex.executeSegmentCompactionTask(a, &older, 1);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(u64, 4), result.postings);
    try std.testing.expect(result.pages != null);
    try std.testing.expect(older.snapshot == null);
    try std.testing.expect(!try index.finishSegmentCompactionTask(&older, &result));
    try expectIncarnationSearch(&index, 1, &.{ .{ .id = "a", .score = 2 }, .{ .id = "b", .score = 5 } }, false);
}

test "sparse streamed compaction releases snapshots blocks and scratch on allocation failures" {
    const Probe = struct {
        var nonce: u64 = 0;
        fn random(_: ?*anyopaque, bytes: []u8) std.Io.RandomSecureError!void {
            @memset(bytes, 0x80);
            std.mem.writeInt(u64, bytes[0..8], nonce, .little);
            nonce += 1;
        }
        fn run(output: Allocator) !void {
            // Private scratch filenames must repeat across fault-injection
            // runs: random formatting lengths change allocator growth counts.
            nonce = 0;
            var io = std.testing.io;
            var vtable = io.vtable.*;
            vtable.randomSecure = random;
            io.vtable = &vtable;
            const a = std.testing.allocator;
            var index = try SparseIndex.open(a, "unused", .{ .backend = .mem, .chunk_size = 1 });
            defer index.close();
            const bulk: BatchOptions = .{ .prefer_bulk_build = true, .assume_new_doc_ids = true };
            try index.batchWithOptions(&.{.{ .doc_id = "a", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 2, -3 } } }}, &.{}, bulk);
            try index.batchWithOptions(&.{.{ .doc_id = "b", .vec = .{ .indices = &.{ 1, 2 }, .values = &.{ 5, -7 } } }}, &.{}, bulk);
            var task = (try index.beginSegmentCompactionTask(a, .{ .min_segments = 2, .background_publication = true, .scratch = .{ .io = io, .directory = "/tmp" } })).?;
            defer task.deinit(a);
            var result = try SparseIndex.executeSegmentCompactionTask(output, &task, 1);
            defer result.deinit(output);
            try std.testing.expect(task.snapshot == null);
            try std.testing.expectEqual(@as(u64, 4), result.postings);
        }
    };
    // Force grow/copy allocation paths instead of address-dependent in-place
    // remaps, so each fault index denotes the same allocation on every run.
    var vtable = std.testing.allocator.vtable.*;
    vtable.resize = Allocator.noResize;
    vtable.remap = Allocator.noRemap;
    const backing: Allocator = .{ .ptr = std.testing.allocator.ptr, .vtable = &vtable };
    try Probe.run(backing);
    try std.testing.checkAllAllocationFailures(backing, Probe.run, .{});
}

test "sparse staged generations hide partial output recover intents and retain old readers" {
    const a = std.testing.allocator;
    var path_bytes: [256]u8 = undefined;
    const path = tmpPath(&path_bytes, "staged-generations");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{});
    defer index.close();
    const long_id: [255]u8 = @splat('x');
    for ([_][]const u8{ "a", "b", &long_id }) |id| try index.batchWithOptions(&.{.{ .doc_id = id, .vec = .{ .indices = &.{1}, .values = &.{2} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    var task = (try index.beginSegmentCompactionTask(a, .{ .min_segments = 2, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } })).?;
    defer task.deinit(a);
    for (task.sources) |source| {
        try std.testing.expect(source.proofs == null);
        try std.testing.expectEqual(@as(usize, 0), source.incarnations.len);
    }
    for (task.docmaps) |source| {
        try std.testing.expect(source.proofs == null);
        try std.testing.expectEqual(@as(usize, 0), source.incarnations.len);
    }
    var old = try index.beginReadTxn();
    var locator_key: [256]u8 = undefined;
    const old_locator = try a.dupe(u8, try old.get(locatorKey(&locator_key, "a")));
    defer a.free(old_locator);
    var old_open = true;
    defer if (old_open) old.abort();
    var result = try SparseIndex.executeSegmentCompactionTask(a, &task, 1);
    defer result.deinit(a);
    try std.testing.expect(result.proofs != null);
    try std.testing.expectEqual(@as(usize, 0), result.incarnations.len);
    try std.testing.expect(pagedDocMap(result.docmap.?));
    const abandoned = try index.reserveCompactionId();
    try index.stageCompaction(abandoned, &result);
    {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        var key: [16]u8 = undefined;
        try std.testing.expectError(error.NotFound, txn.get(segmentKey(&key, abandoned)));
    }
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    const partial = try index.search(a, &query, 5);
    defer SparseIndex.freeResults(a, partial);
    try std.testing.expectEqual(@as(usize, 3), partial.len);
    try index.recoverCompactionIntents();
    {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        var cursor = try txn.openCursor();
        defer cursor.close();
        const prefix = posting_pages.key(abandoned, 0, 0);
        if (try cursor.seekAtOrAfter(prefix[0..9])) |entry| try std.testing.expect(!std.mem.startsWith(u8, entry.key, prefix[0..9]));
        try std.testing.expectError(error.NotFound, txn.get(&SparseIndex.pendingKey(abandoned)));
    }
    try std.testing.expect(try index.finishSegmentCompactionTask(&task, &result));
    for ([_][]const u8{ "a", "b", &long_id }) |id| {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        try std.testing.expect((try locatedForwardBytes(&txn, id)) != null);
        try std.testing.expect((try locatedForwardBytes(&old, id)) != null);
    }
    // Simulate a crash after root publication but before locator refresh.
    {
        var txn = try index.beginBatchTxn(.{});
        errdefer txn.abort();
        const locator = (try readLocator(&txn, "a")).?;
        var key: [256]u8 = undefined;
        try txn.put(locatorKey(&key, "a"), old_locator);
        try txn.put(&SparseIndex.pendingKey(locator.segment), &.{});
        try txn.commit();
    }
    {
        var read = try index.beginReadTxn();
        defer read.abort();
        const forward = try index.lookupFwdEntryByDocIdAlloc(a, &read, "a");
        defer a.free(forward.term_ids);
        defer a.free(forward.weights);
        try std.testing.expectEqual(@as(usize, 1), forward.term_ids.len);
    }
    old.abort();
    old_open = false;
    index.close();
    index = try SparseIndex.open(a, path, .{});
    {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        try std.testing.expect((try locatedForwardBytes(&txn, "a")) != null);
    }
    const hits = try index.search(a, &query, 5);
    defer SparseIndex.freeResults(a, hits);
    try std.testing.expectEqual(@as(usize, 3), hits.len);
    // A later compatibility compaction must consume the paged document map too.
    try index.batchWithOptions(&.{.{ .doc_id = "c", .vec = .{ .indices = &.{1}, .values = &.{2} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    try std.testing.expect(try index.compactSegmentsWithOptions(a, .{ .min_segments = 2 }));
    const next_hits = try index.search(a, &query, 5);
    defer SparseIndex.freeResults(a, next_hits);
    try std.testing.expectEqual(@as(usize, 4), next_hits.len);
    var read = try index.beginReadTxn();
    defer read.abort();
    try std.testing.expect((try locatedForwardBytes(&read, "a")) != null);
}

test "sparse physical translation borrows one map block instead of retaining point reads" {
    const Probe = struct {
        const Self = @This();
        key: [89]u8 = undefined,
        value: [6]u8 = undefined,
        seeks: usize = 0,
        gets: usize = 0,
        closed: bool = false,
        pub fn get(self: *@This(), k: []const u8) anyerror![]const u8 {
            // An LSM point read survives until transaction close. Only the
            // constant completeness marker may use that ownership contract.
            if (!std.mem.eql(u8, k, metaKey(meta_complete_physical))) return error.RetainedPhysicalMapRead;
            self.gets += 1;
            return &.{1};
        }
        const Cursor = struct {
            p: *Self,
            pub fn close(self: *@This()) void {
                self.p.closed = true;
            }
            pub fn seekAtOrAfter(self: *@This(), k: []const u8) !?struct { key: []const u8, value: []const u8 } {
                self.p.seeks += 1;
                self.p.key = k[0..89].*;
                std.mem.writeInt(u16, self.p.value[0..2], 7, .little);
                std.mem.writeInt(u32, self.p.value[2..6], @intCast(self.p.seeks), .little);
                return .{ .key = &self.p.key, .value = &self.p.value };
            }
        };
        pub fn openCursor(self: *@This()) !Cursor {
            return .{ .p = self };
        }
    };
    const a = std.testing.allocator;
    var rows = @import("../encoding/roaring.zig").RoaringBitmap.init(a);
    defer rows.deinit();
    try rows.addRange(0, 1024 * 128);
    var result = @import("../encoding/roaring.zig").RoaringBitmap.init(a);
    defer result.deinit();
    var txn: Probe = .{};
    var prefix: [80]u8 = @splat('a');
    @memcpy(prefix[0..6], "lake2:");
    prefix[70] = ':';
    prefix[79] = ':';
    try std.testing.expect(try selectPhysicalBlock(&txn, a, &prefix, 0, &rows, &result, null));
    try std.testing.expectEqual(@as(usize, 128), txn.seeks);
    try std.testing.expectEqual(@as(usize, 1), txn.gets);
    try std.testing.expect(txn.closed);
    try std.testing.expectEqual(@as(u64, 128), result.cardinality());
}

test "sparse background directory pages stage routes before activation and reclaim abandoned output" {
    const a = std.testing.allocator;
    var index = try SparseIndex.open(a, "unused", .{ .backend = .mem });
    defer index.close();
    var terms: [130]u32 = undefined;
    const weights: [130]f32 = @splat(2);
    for (&terms, 0..) |*term, i| term.* = @intCast(i * 2);
    const vector: SparseVector = .{ .indices = &terms, .values = &weights };
    const bulk: BatchOptions = .{ .prefer_bulk_build = true, .assume_new_doc_ids = true };
    for ([_][]const u8{ "a", "b" }) |id| try index.batchWithOptions(&.{.{ .doc_id = id, .vec = vector }}, &.{}, bulk);
    const options: SparseIndex.SegmentCompactionOptions = .{ .min_segments = 2, .background_publication = true, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } };
    var task = (try index.beginSegmentCompactionTask(a, options)).?;
    var task_open = true;
    defer if (task_open) task.deinit(a);
    const id = task.generation.?;
    var result = try SparseIndex.executeSegmentCompactionTask(a, &task, 8);
    defer result.deinit(a);
    try std.testing.expect(result.staged);
    try std.testing.expectEqual(@as(usize, 32), result.data.?.len);
    {
        var read = try index.beginReadTxn();
        defer read.abort();
        var key_buf: [16]u8 = undefined;
        try std.testing.expectError(error.NotFound, read.get(segmentKey(&key_buf, id)));
        var iterator = try posting_pages.Terms(backend_erased.ReadTxn).init(&read, id, result.data.?);
        defer iterator.deinit();
        var count: usize = 0;
        while (try iterator.next()) |entry| {
            try std.testing.expectEqual(terms[count], std.mem.readInt(u32, entry[0..4], .little));
            count += 1;
        }
        try std.testing.expectEqual(terms.len, count);
        for (terms) |term| try std.testing.expect(try posting_pages.hasTermTxn(&read, id, result.data.?, term));
        try std.testing.expect(!try posting_pages.hasTermTxn(&read, id, result.data.?, 127));
        try std.testing.expectEqual(@as(usize, 9), (try read.get(&posting_pages.routeKey(258, id))).len);
    }
    try expectIncarnationSearch(&index, 258, &.{ .{ .id = "a", .score = 2 }, .{ .id = "b", .score = 2 } }, false);
    // Cancellation before activation must reclaim both route families using
    // their page ledger, even though no generation root ever existed.
    task.deinit(a);
    task_open = false;
    {
        var read = try index.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get(&posting_pages.routeKey(258, id)));
        try std.testing.expectError(error.NotFound, read.get(&posting_pages.bucketKey(258, posting_pages.intervalNode(0, 1), id)));
        try std.testing.expectError(error.NotFound, read.get(&posting_pages.directoryKey(id, 258)));
        try std.testing.expectError(error.NotFound, read.get(&SparseIndex.pendingKey(id)));
    }
    var active = (try index.beginSegmentCompactionTask(a, options)).?;
    defer active.deinit(a);
    var published = try SparseIndex.executeSegmentCompactionTask(a, &active, 8);
    defer published.deinit(a);
    try std.testing.expect(try index.publishSegmentCompactionTask(&active, &published));
    try SparseIndex.completeSegmentCompactionTask(a, &active, &published, null);
    try expectIncarnationSearch(&index, 258, &.{ .{ .id = "a", .score = 2 }, .{ .id = "b", .score = 2 } }, false);
}

test "sparse locator refresh stops after a generation retires between bounded batches" {
    const a = std.testing.allocator;
    var index = try SparseIndex.open(a, "unused", .{ .backend = .mem });
    defer index.close();
    const vector: SparseVector = .{ .indices = &.{1}, .values = &.{2} };
    var ids: [512][16]u8 = undefined;
    var writes: [512]SparseWrite = undefined;
    for (&writes, &ids, 0..) |*write, *id, i| write.* = .{ .doc_id = try std.fmt.bufPrint(id, "doc-{d:0>4}", .{i}), .vec = vector };
    const bulk: BatchOptions = .{ .prefer_bulk_build = true, .assume_new_doc_ids = true };
    try index.batchWithOptions(writes[0..256], &.{}, bulk);
    try index.batchWithOptions(writes[256..], &.{}, bulk);
    const options: SparseIndex.SegmentCompactionOptions = .{ .min_segments = 2, .background_publication = true, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } };
    var task = (try index.beginSegmentCompactionTask(a, options)).?;
    defer task.deinit(a);
    var result = try SparseIndex.executeSegmentCompactionTask(a, &task, 64);
    defer result.deinit(a);
    try std.testing.expect(try index.publishSegmentCompactionTask(&task, &result));
    try index.batchWithOptions(&.{.{ .doc_id = "later", .vec = vector }}, &.{}, bulk);
    const Gate = struct {
        index: *SparseIndex,
        enters: usize = 0,
        leaves: usize = 0,
        fn enter(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.enters += 1;
            // A competing compaction fully installs the newer locators after
            // the first 256 old records have been refreshed.
            if (self.enters == 2) try std.testing.expect(try self.index.compactSegmentsWithOptions(std.testing.allocator, .{ .min_segments = 2, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } }));
        }
        fn leave(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.leaves += 1;
        }
    };
    var gate: Gate = .{ .index = &index };
    try SparseIndex.completeSegmentCompactionTask(a, &task, &result, .{ .ptr = &gate, .enter = Gate.enter, .leave = Gate.leave });
    try std.testing.expectEqual(@as(usize, 2), gate.enters);
    try std.testing.expectEqual(gate.enters, gate.leaves);
    var read = try index.beginReadTxn();
    defer read.abort();
    for (writes) |write| {
        const locator = (try readLocator(&read, write.doc_id)).?;
        try std.testing.expect(locator.segment != result.generation.?);
        try std.testing.expect((try locatedForwardBytes(&read, write.doc_id)) != null);
    }
}

test "sparse background compaction retires fully deleted inputs without publishing empty spools" {
    const a = std.testing.allocator;
    var index = try SparseIndex.open(a, "unused", .{ .backend = .mem });
    defer index.close();
    for ([_][]const u8{ "a", "b" }) |id| try index.batchWithOptions(&.{.{ .doc_id = id, .vec = .{ .indices = &.{1}, .values = &.{2} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    try index.batch(&.{}, &.{ "a", "b" });
    var task = (try index.beginSegmentCompactionTask(a, .{ .min_segments = 2, .background_publication = true, .scratch = .{ .io = std.testing.io, .directory = "/tmp" } })).?;
    defer task.deinit(a);
    var result = try SparseIndex.executeSegmentCompactionTask(a, &task, 8);
    defer result.deinit(a);
    try std.testing.expect(result.staged);
    try std.testing.expect(result.data == null);
    try std.testing.expect(result.pages == null);
    try std.testing.expect(result.directory_run == null);
    try std.testing.expect(try index.publishSegmentCompactionTask(&task, &result));
    try SparseIndex.completeSegmentCompactionTask(a, &task, &result, null);
    try std.testing.expectEqual(@as(usize, 0), try index.segmentCount());
}

test "sparse visibility reuses proven missing intervals and EOF without losing backward lookups" {
    const Fake = struct {
        entry: backend_erased.Entry,
        seeks: *usize,
        fail_next: *bool,
        pub fn close(_: *@This()) void {}
        pub fn first(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn last(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn next(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn prev(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_erased.Entry {
            self.seeks.* += 1;
            if (self.fail_next.*) {
                self.fail_next.* = false;
                return error.InjectedSeekFailure;
            }
            return if (std.mem.order(u8, self.entry.key, key) != .lt) self.entry else null;
        }
    };
    var source_key: [17]u8 = undefined;
    _ = segmentIncarnationKey(&source_key, 42, 9000, false);
    var seeks: [3]usize = @splat(0);
    var failures: [3]bool = @splat(false);
    var cursors: [3]backend_erased.Cursor = undefined;
    var opened: usize = 0;
    errdefer for (cursors[0..opened]) |*cursor| cursor.close();
    for (&cursors, 0..) |*cursor, lane| {
        cursor.* = try backend_erased.cursorFrom(std.testing.allocator, Fake{
            .entry = .{ .key = &source_key, .value = "epoch" },
            .seeks = &seeks[lane],
            .fail_next = &failures[lane],
        });
        opened += 1;
    }
    var reader: VisibilityReader = .{ .cursors = cursors };
    opened = 0;
    defer reader.deinit();
    var key: [9]u8 = undefined;
    for (0..100_003) |doc| {
        try std.testing.expectError(error.NotFound, reader.get(docTombstoneKey(&key, doc)));
        try std.testing.expectError(error.NotFound, reader.get(docIncarnationKey(&key, doc)));
    }
    try std.testing.expectEqualSlices(usize, &.{ 1, 1, 0 }, &seeks);
    var requested: [17]u8 = undefined;
    try std.testing.expectError(error.NotFound, reader.get(segmentIncarnationKey(&requested, 42, 8999, false)));
    try std.testing.expectEqualStrings("epoch", try reader.get(&source_key));
    try std.testing.expectEqual(@as(usize, 1), seeks[2]);
    try std.testing.expectError(error.NotFound, reader.get(segmentIncarnationKey(&requested, 42, 9001, false)));
    try std.testing.expectError(error.NotFound, reader.get(segmentIncarnationKey(&requested, 42, 100_003, false)));
    try std.testing.expectEqual(@as(usize, 2), seeks[2]);
    try std.testing.expectEqualStrings("epoch", try reader.get(&source_key));
    try std.testing.expectError(error.NotFound, reader.get(segmentIncarnationKey(&requested, 42, 8999, false)));
    try std.testing.expectEqual(@as(usize, 4), seeks[2]);
    failures[2] = true;
    try std.testing.expectError(error.InjectedSeekFailure, reader.get(segmentIncarnationKey(&requested, 42, 9001, false)));
    try std.testing.expectEqualStrings("epoch", try reader.get(&source_key));
    try std.testing.expectEqual(@as(usize, 6), seeks[2]);
}

test "sparse canonical dimensions agree between bulk delta and reopen" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |bulk| {
        var pb: [256]u8 = undefined;
        const path = tmpPath(&pb, if (bulk) "canonical-bulk" else "canonical-delta");
        defer cleanupTmp(path);
        var index = try SparseIndex.open(a, path, .{ .chunk_size = 2 });
        defer index.close();
        try index.batchWithOptions(&.{
            .{ .doc_id = "winner", .vec = .{ .indices = &.{ 2, 1, 1 }, .values = &.{ 0, 6, 6 } } },
            .{ .doc_id = "other", .vec = .{ .indices = &.{1}, .values = &.{10} } },
        }, &.{}, .{ .prefer_bulk_build = bulk, .assume_new_doc_ids = bulk });
        const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
        for (0..2) |_| {
            const result = try index.search(a, &query, 1);
            defer SparseIndex.freeResults(a, result);
            try std.testing.expectEqualStrings("winner", result[0].doc_id);
            try std.testing.expectApproxEqAbs(@as(f32, 12), result[0].score, 0.01);
            index.close();
            index = try SparseIndex.open(a, path, .{ .chunk_size = 2 });
        }
    }
}

test "sparse legacy repeated postings survive paged scoring compaction and restart" {
    const a = std.testing.allocator;
    for ([_]u32{ 2, 3 }) |chunk_size| {
        var pb: [256]u8 = undefined;
        const path = tmpPath(&pb, if (chunk_size == 2) "legacy-repeated-split" else "legacy-repeated-block");
        defer cleanupTmp(path);
        var index = try SparseIndex.open(a, path, .{ .chunk_size = chunk_size });
        defer index.close();
        const id = index.next_segment_id;
        const writes = [_]SparseWrite{
            .{ .doc_id = "zero", .doc_num = 0, .vec = .{ .indices = &.{1}, .values = &.{10} } },
            .{ .doc_id = "one", .doc_num = 1, .vec = .{ .indices = &.{1}, .values = &.{10} } },
            .{ .doc_id = "two", .doc_num = 2, .vec = .{ .indices = &.{1}, .values = &.{10} } },
            .{ .doc_id = "winner", .doc_num = 3, .vec = .{ .indices = &.{1}, .values = &.{6} } },
            .{ .doc_id = "four", .doc_num = 4, .vec = .{ .indices = &.{1}, .values = &.{6} } },
        };
        try index.batchWithOptions(&writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
        try index.batchWithOptions(&.{.{ .doc_id = "unrelated", .doc_num = 5, .vec = .{ .indices = &.{99}, .values = &.{1} } }}, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
        const postings = [_]BulkPosting{
            .{ .term_id = 1, .doc_num = 0, .weight = 10, .doc_id = "zero" },
            .{ .term_id = 1, .doc_num = 1, .weight = 10, .doc_id = "one" },
            .{ .term_id = 1, .doc_num = 2, .weight = 10, .doc_id = "two" },
            .{ .term_id = 1, .doc_num = 3, .weight = 6, .doc_id = "winner" },
            .{ .term_id = 1, .doc_num = 3, .weight = 6, .doc_id = "winner" },
            .{ .term_id = 1, .doc_num = 4, .weight = 6, .doc_id = "four" },
        };
        const legacy = try encodeSegmentFromSortedPostings(a, &postings, chunk_size);
        defer a.free(legacy);
        {
            var txn = try index.beginWriteTxn();
            errdefer txn.abort();
            const root = try posting_pages.publish(a, &txn, id, legacy);
            defer a.free(root);
            var key_buf: [16]u8 = undefined;
            try txn.put(segmentKey(&key_buf, id), root);
            try txn.commit();
        }
        const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
        for (0..3) |phase| {
            const result = try index.search(a, &query, 1);
            defer SparseIndex.freeResults(a, result);
            try std.testing.expectEqualStrings("winner", result[0].doc_id);
            try std.testing.expectEqual(@as(f32, 12), result[0].score);
            if (phase == 0) try std.testing.expect(try index.compactSegmentsWithOptions(a, .{ .min_segments = 2 })) else if (phase == 1) {
                index.close();
                index = try SparseIndex.open(a, path, .{ .chunk_size = chunk_size });
            }
        }
    }
}

test "sparse visibility pages bound interleaved epochs and retain pinned snapshots" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "visibility-pages");
    defer cleanupTmp(path);
    var index = try SparseIndex.open(a, path, .{ .backend = .lsm });
    defer index.close();
    {
        var write = try index.beginWriteTxn();
        errdefer write.abort();
        var key: [9]u8 = undefined;
        const epoch = [_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 };
        for (0..256) |doc| {
            if (doc == 17) continue;
            try write.put(docIncarnationKey(&key, doc), &epoch);
            for (0..40) |segment| try putSegmentIncarnation(&write, segment, doc, 7, false);
        }
        try write.commit();
    }
    var old = try index.beginReadTxn();
    defer old.abort();
    var reader = try VisibilityReader.init(&old);
    defer reader.deinit();
    var current: [9]u8 = undefined;
    var source: [17]u8 = undefined;
    for ([_]u64{ 0, 64, 128, 192 }) |doc| try std.testing.expectEqual(@as(u64, 7), try currentIncarnation(&reader, doc));
    try std.testing.expectEqual(@as(usize, 0), reader.page_loads);
    for (0..256) |doc| {
        if (doc == 17) {
            try std.testing.expectError(error.NotFound, reader.get(docIncarnationKey(&current, doc)));
            continue;
        }
        try std.testing.expectEqual(@as(u64, 7), try currentIncarnation(&reader, doc));
        for (0..40) |segment| try std.testing.expectEqual(@as(u64, 7), try segmentIncarnation(&reader, segment, SEGMENT_FORMAT_VERSION, doc, false));
    }
    try std.testing.expectEqual(@as(usize, 32), reader.page_count);
    try std.testing.expectEqual(@as(usize, 32 * 4), reader.page_loads);
    // A backward hole and a point outside the last page cannot reuse a false
    // presence bit from another block or an interleaved segment.
    try std.testing.expectError(error.NotFound, reader.get(docIncarnationKey(&current, 17)));
    try std.testing.expectError(error.NotFound, reader.get(segmentIncarnationKey(&source, 2, 256, false)));
    try std.testing.expectEqual(@as(u64, 7), try currentIncarnation(&reader, 0));
    {
        var write = try index.beginWriteTxn();
        errdefer write.abort();
        const epoch = [_]u8{ 9, 0, 0, 0, 0, 0, 0, 0 };
        try write.put(docIncarnationKey(&current, 200), &epoch);
        try write.commit();
    }
    try std.testing.expectEqual(@as(u64, 7), try currentIncarnation(&reader, 200));
    var newer = try index.beginReadTxn();
    defer newer.abort();
    var newer_reader = try VisibilityReader.init(&newer);
    defer newer_reader.deinit();
    try std.testing.expectEqual(@as(u64, 9), try currentIncarnation(&newer_reader, 200));
}

test "sparse visibility partial page failures never become cached absence" {
    const Fake = struct {
        key: []const u8,
        value: []const u8,
        fail: *bool,
        pub fn close(_: *@This()) void {}
        pub fn first(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn last(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn prev(_: *@This()) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?backend_erased.Entry {
            return error.UnexpectedTraversal;
        }
        pub fn next(self: *@This()) !?backend_erased.Entry {
            if (self.fail.*) {
                self.fail.* = false;
                return error.InjectedPageReadFailure;
            }
            return null;
        }
        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_erased.Entry {
            return if (std.mem.order(u8, self.key, key) != .lt) .{ .key = self.key, .value = self.value } else null;
        }
    };
    var key: [9]u8 = undefined;
    var failure = true;
    const epoch = [_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 };
    var cursor = try backend_erased.cursorFrom(std.testing.allocator, Fake{ .key = docIncarnationKey(&key, 7), .value = &epoch, .fail = &failure });
    defer cursor.close();
    var page: VisibilityReader.Page = .{ .cursor = cursor, .prefix_len = 1 };
    page.prefix[0] = key_doc_incarnation;
    try std.testing.expectError(error.InjectedPageReadFailure, page.get(&key));
    try std.testing.expect(page.block == null);
    try std.testing.expectEqualSlices(u8, &epoch, try page.get(&key));
    try std.testing.expectEqual(@as(?u64, 0), page.block);
    var missing: [9]u8 = undefined;
    try std.testing.expectError(error.NotFound, page.get(docIncarnationKey(&missing, 8)));
}

test "sparse exclusion masks retain bounded scoring without reverse key callbacks" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "exclusion-ordinals");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{ .chunk_size = 32 });
    defer idx.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const writes = try ca.alloc(SparseWrite, 512);
    for (writes, 0..) |*write, i| {
        const values = try ca.alloc(f32, 2);
        values[0] = @floatFromInt(i + 1);
        values[1] = 1;
        write.* = .{ .doc_id = try std.fmt.allocPrint(ca, "doc-{d}", .{i}), .vec = .{ .indices = &.{ 1, 2 }, .values = values } };
    }
    try idx.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    const Selection = @import("ordinal_lookup.zig").Selection;
    const Predicate = struct {
        mode: enum { exclude, both, empty, residual } = .exclude,
        calls: usize = 0,
        fn allows(raw: *anyopaque, key: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.mode != .residual) return error.UnexpectedReverseIdentityRead;
            self.calls += 1;
            return try std.fmt.parseInt(usize, key[4..], 10) >= 300;
        }
        fn select(raw: *anyopaque, alloc: Allocator, lookup: OrdinalLookup) !?Selection {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var result: Selection = .{};
            errdefer result.deinit();
            if (self.mode == .residual) {
                result.residual = true;
                return result;
            }
            result.exclude = .init(alloc);
            for (0..300) |i| {
                var key: [32]u8 = undefined;
                if (try lookup.one(lookup.ptr, try std.fmt.bufPrint(&key, "doc-{d}", .{i}))) |num| try result.exclude.?.add(num);
            }
            if (self.mode != .exclude) result.include = .init(alloc);
            if (self.mode == .both) for (400..450) |i| {
                var key: [32]u8 = undefined;
                if (try lookup.one(lookup.ptr, try std.fmt.bufPrint(&key, "doc-{d}", .{i}))) |num| try result.include.?.add(num);
            };
            return result;
        }
    };
    var predicate: Predicate = .{};
    const excluded = try ca.alloc([]const u8, 300);
    for (excluded, 0..) |*key, i| key.* = writes[i].doc_id;
    const included = try ca.alloc([]const u8, 50);
    for (included, 400..) |*key, i| key.* = writes[i].doc_id;
    for ([_]@TypeOf(predicate.mode){ .exclude, .both, .empty, .residual }) |mode| {
        predicate.mode = mode;
        for ([_]f32{ 1, -1, 0 }) |weight| {
            const query: SparseVector = .{ .indices = &.{ 1, 2 }, .values = &.{ weight, -1 } };
            predicate.calls = 0;
            const expected = try idx.searchConstrained(a, &query, 3, .{ .exclude_doc_ids = excluded, .filter_doc_ids = if (mode == .both) included else if (mode == .empty) &.{"missing"} else &.{} });
            defer SparseIndex.freeResults(a, expected);
            const actual = try idx.searchConstrained(a, &query, 3, .{ .max_score_docs = 1, .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select } });
            defer SparseIndex.freeResults(a, actual);
            if (mode == .residual) try std.testing.expect(predicate.calls > 0 and predicate.calls <= 512);
            try std.testing.expectEqual(expected.len, actual.len);
            for (actual, expected) |hit, reference| {
                try std.testing.expectEqualStrings(reference.doc_id, hit.doc_id);
                try std.testing.expectEqual(reference.score, hit.score);
            }
        }
    }
}

test "sparse predicate decisions remain bounded and exact through collisions" {
    var cache: PredicateDecisionCache = .{};
    for (0..100000) |i| {
        const doc: u32 = @intCast(i);
        try std.testing.expect(cache.get(doc) == null);
        cache.put(doc, doc % 3 != 0);
        try std.testing.expectEqual(doc % 3 != 0, cache.get(doc).?);
        if (i >= 256) try std.testing.expect(cache.get(doc - 256) == null);
    }
    try std.testing.expect(@sizeOf(PredicateDecisionCache) <= 4096);
}

test "sparse adaptive ordinal constraints seek on upfront and mid query completion" {
    const a = std.testing.allocator;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "adaptive-seeks");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{ .chunk_size = 32 });
    defer idx.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const writes = try ca.alloc(SparseWrite, 10000);
    for (writes, 0..) |*write, i| write.* = .{ .doc_id = try std.fmt.allocPrint(ca, "doc-{d}", .{i}), .vec = .{ .indices = &.{1}, .values = &.{1} } };
    try idx.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    const Predicate = struct {
        calls: usize = 0,
        selections: usize = 0,
        complete: bool = false,
        fn revision(raw: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return @intFromBool(self.complete);
        }
        fn allows(raw: *anyopaque, key: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == 3) self.complete = true;
            return std.mem.eql(u8, key, "doc-9999");
        }
        fn select(raw: *anyopaque, alloc: Allocator, lookup: OrdinalLookup) !?@import("ordinal_lookup.zig").Selection {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.selections += 1;
            if (!self.complete) return .{ .residual = true };
            var selected: @import("ordinal_lookup.zig").Selection = .{ .include = .init(alloc) };
            errdefer selected.deinit();
            try selected.include.?.add((try lookup.one(lookup.ptr, "doc-9999")).?);
            return selected;
        }
    };
    for ([_]bool{ false, true }) |upfront| {
        var predicate: Predicate = .{ .complete = upfront };
        const query: SparseVector = .{ .indices = &.{1}, .values = &.{-1} };
        const hits = try idx.searchConstrained(a, &query, 10, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select, .constraint_revision = Predicate.revision } });
        defer SparseIndex.freeResults(a, hits);
        try std.testing.expectEqual(@as(usize, 1), hits.len);
        try std.testing.expectEqualStrings("doc-9999", hits[0].doc_id);
        try std.testing.expectEqual(@as(usize, if (upfront) 0 else 3), predicate.calls);
        try std.testing.expectEqual(@as(usize, if (upfront) 1 else 2), predicate.selections);
    }
}

test "sparse broad physical masks seek through the bounded native directory" {
    const a = std.testing.allocator;
    const ordinals = @import("ordinal_lookup.zig");
    const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
    const prefix = "lake2:" ++ "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" ++ ":00000000:";
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "broad-physical-seeks");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{ .chunk_size = 32 });
    defer idx.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const writes = try ca.alloc(SparseWrite, 10000);
    for (writes, 0..) |*write, i| write.* = .{ .doc_id = try std.fmt.allocPrint(ca, prefix ++ "{x:0>16}", .{i}), .vec = .{ .indices = &.{1}, .values = &.{1} } };
    try idx.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    const Predicate = struct {
        blocks: usize = 0,
        fn allows(_: *anyopaque, _: []const u8) !bool {
            return error.UnexpectedCandidatePredicate;
        }
        fn select(raw: *anyopaque, alloc: Allocator, lookup: OrdinalLookup) !?ordinals.Selection {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var selected = try ordinals.Selection.initBounded(alloc, 4 * 1024 * 1024);
            errdefer selected.deinit();
            selected.include = Bitmap.init(selected.allocator());
            var rows = Bitmap.init(selected.allocator());
            defer rows.deinit();
            try rows.addRange(5000, 10000);
            var work: ordinals.WorkBudget = .{};
            try std.testing.expect(try lookup.bounded_block.?(lookup.ptr, selected.allocator(), prefix, 0, &rows, &selected.include.?, &work));
            self.blocks = 4096 - work.blocks;
            try selected.prepareRead();
            try std.testing.expect(selected.budget.?.live < 64 * 1024);
            return selected;
        }
    };
    for ([_]f32{ 1, -1, 0 }) |weight| {
        var predicate: Predicate = .{};
        const query: SparseVector = .{ .indices = &.{1}, .values = &.{weight} };
        const hits = try idx.searchConstrained(a, &query, 10, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select } });
        defer SparseIndex.freeResults(a, hits);
        try std.testing.expectEqual(@as(usize, 10), hits.len);
        try std.testing.expectEqual(@as(usize, 6), predicate.blocks);
        for (hits, 5000..) |hit, row| {
            var expected: [96]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, prefix ++ "{x:0>16}", .{row}), hit.doc_id);
            try std.testing.expectEqual(weight, hit.score);
        }
    }
    // Work exhaustion cannot silently return a partially translated selection.
    var txn = try idx.beginReadTxn();
    defer txn.abort();
    var physical = Bitmap.init(a);
    defer physical.deinit();
    try physical.addRange(0, 10000);
    var partial = Bitmap.init(a);
    defer partial.deinit();
    var work: ordinals.WorkBudget = .{ .blocks = 1 };
    try std.testing.expectError(error.OrdinalPlanningBudgetExceeded, selectPhysicalBlockBounded(&txn, a, prefix, 0, &physical, &partial, null, &work));
    try std.testing.expect(partial.cardinality() < physical.cardinality());
}

test "sparse lazy native windows intersect reached postings and preserve spill legacy and signed scores" {
    const a = std.testing.allocator;
    const ordinals = @import("ordinal_lookup.zig");
    const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "lazy-native-windows");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{ .chunk_size = 32 });
    defer idx.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const writes = try ca.alloc(SparseWrite, 10000);
    for (writes, 0..) |*write, i| write.* = .{ .doc_id = try std.fmt.allocPrint(ca, "doc-{d:0>5}", .{i}), .vec = if (i < 9900) .{ .indices = &.{1}, .values = &.{1} } else .{ .indices = &.{ 1, 2 }, .values = &.{ 1, 2 } } };
    try idx.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    try idx.batch(&.{}, &.{"doc-09901"});
    const Predicate = struct {
        ranges: usize = 0,
        rows: usize = 0,
        fallback: usize = 0,
        fail: bool = false,
        all: bool = false,
        fn selected(key: []const u8) !bool {
            const row = try std.fmt.parseInt(u32, key[4..], 10);
            return row >= 9900 and row % 7 != 0;
        }
        fn allows(raw: *anyopaque, key: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.fallback += 1;
            return self.all or try selected(key);
        }
        fn windowKey(raw: *anyopaque, value: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.rows += 1;
            if (self.fail) return error.TestWindowFailed;
            return self.all or try selected(value);
        }
        fn select(_: *anyopaque, _: Allocator, _: OrdinalLookup) !?ordinals.Selection {
            return .{ .residual = true, .deferred = true };
        }
        fn range(raw: *anyopaque, alloc: Allocator, lookup: OrdinalLookup, first: u32, last: u32) !?ordinals.Selection {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ranges += 1;
            var result: ordinals.Selection = .{ .include = Bitmap.init(alloc) };
            errdefer result.deinit();
            const translate = lookup.native_range orelse {
                result.deinit();
                return null;
            };
            if (!try translate(lookup.ptr, alloc, first, last, .{ .ptr = self, .allows = windowKey }, &result.include.?)) {
                result.deinit();
                return null;
            }
            return result;
        }
    };
    var selected_ids: std.ArrayList([]const u8) = .empty;
    defer selected_ids.deinit(a);
    for (writes[9900..]) |write| if (try Predicate.selected(write.doc_id)) {
        try selected_ids.append(a, write.doc_id);
    };
    for ([_]f32{ 1, -1, 0 }) |weight| {
        // A rare dimension starts in the last window. No directory walk over
        // intervening windows is required, even with an archive-wide predicate.
        const query: SparseVector = .{ .indices = &.{2}, .values = &.{weight} };
        const expected = try idx.searchConstrained(a, &query, 9, .{ .filter_doc_ids = selected_ids.items });
        defer SparseIndex.freeResults(a, expected);
        var predicate: Predicate = .{};
        const actual = try idx.searchConstrained(a, &query, 9, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select, .select_range = Predicate.range } });
        defer SparseIndex.freeResults(a, actual);
        try std.testing.expect(predicate.ranges <= 2);
        try std.testing.expect(predicate.rows <= 2048);
        try std.testing.expect(predicate.fallback <= 32);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |left, right| {
            try std.testing.expectEqualStrings(left.doc_id, right.doc_id);
            try std.testing.expectEqual(left.score, right.score);
        }
    }
    // Exercise changing ordinal windows across multiple dimensions while
    // preserving canonical signed f32 accumulation.
    const query: SparseVector = .{ .indices = &.{ 1, 2 }, .values = &.{ -1, 0.25 } };
    const expected = try idx.searchConstrained(a, &query, 9, .{ .filter_doc_ids = selected_ids.items, .max_score_docs = 1 });
    defer SparseIndex.freeResults(a, expected);
    var predicate: Predicate = .{};
    const actual = try idx.searchConstrained(a, &query, 9, .{ .max_score_docs = 1, .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select, .select_range = Predicate.range } });
    defer SparseIndex.freeResults(a, actual);
    try std.testing.expect(predicate.fallback <= 16 * 10);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |left, right| {
        try std.testing.expectEqualStrings(left.doc_id, right.doc_id);
        try std.testing.expectEqual(left.score, right.score);
    }
    predicate = .{ .fail = true };
    try std.testing.expectError(error.TestWindowFailed, idx.searchConstrained(a, &query, 9, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select, .select_range = Predicate.range } }));
    // Once these nine equal-score winners fill top-k, every later block is
    // noncompetitive. No 1024-row window may be translated before that proof.
    const broad_query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    var broad: Predicate = .{ .all = true };
    const ranked = try idx.searchConstrained(a, &broad_query, 9, .{ .key_predicate = .{ .ptr = &broad, .allows = Predicate.allows, .select_constraints = Predicate.select, .select_range = Predicate.range } });
    defer SparseIndex.freeResults(a, ranked);
    try std.testing.expectEqual(@as(usize, 9), ranked.len);
    for (ranked, 0..) |hit, i| try std.testing.expectEqualStrings(writes[i].doc_id, hit.doc_id);
    try std.testing.expectEqual(@as(usize, 9), broad.fallback);
    try std.testing.expectEqual(@as(usize, 0), broad.rows);
    try std.testing.expectEqual(@as(usize, 0), broad.ranges);
    // Old generations without a complete directory proof retain exact keys.
    // Disabling DAAT also forces spill with a one-score memory allowance.
    var txn = try idx.beginWriteTxn();
    errdefer txn.abort();
    try txn.delete(metaKey(meta_complete_locators));
    try txn.commit();
    predicate = .{};
    const legacy = try idx.searchConstrained(a, &query, 9, .{ .max_score_docs = 1, .score_spill = .{ .io = std.testing.io, .directory = "/tmp", .chunk_records = 32, .chunk_bytes = 1024 }, .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows, .select_constraints = Predicate.select, .select_range = Predicate.range } });
    defer SparseIndex.freeResults(a, legacy);
    try std.testing.expectEqual(@as(usize, 1), predicate.ranges);
    try std.testing.expect(predicate.fallback > 0);
    for (expected, legacy) |left, right| {
        try std.testing.expectEqualStrings(left.doc_id, right.doc_id);
        try std.testing.expectEqual(left.score, right.score);
    }
}

test "sparse planning caps do not hide backing allocation failures after optional growth" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var budget: @import("ordinal_lookup.zig").MaskBudget = .{ .backing = failing.allocator(), .limit = 16 };
    const a = budget.allocator();
    const bytes = try a.alloc(u8, 8);
    defer a.free(bytes);
    try std.testing.expect(!a.resize(bytes, 32));
    try std.testing.expect(budget.exhausted);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expect(!budget.exhausted);
    try std.testing.expect(a.remap(bytes, 32) == null);
    try std.testing.expect(budget.exhausted);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expect(!budget.exhausted);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 32));
    try std.testing.expect(budget.exhausted);
}

test "sparse shared parallel ranges preserve signed canonical scores filters and cap fallback" {
    const a = std.testing.allocator;
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    var pb: [256]u8 = undefined;
    const path = tmpPath(&pb, "parallel-ordinals");
    defer cleanupTmp(path);
    var idx = try SparseIndex.open(a, path, .{ .chunk_size = 64 });
    defer idx.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const writes = try ca.alloc(SparseWrite, 12288);
    for (writes, 0..) |*write, i| {
        const values = try ca.alloc(f32, 3);
        values[0] = @floatFromInt(i % 97 + 1);
        values[1] = @floatFromInt(i % 31 + 1);
        values[2] = @floatFromInt(i % 7 + 1);
        write.* = .{ .doc_id = try std.fmt.allocPrint(ca, "doc-{d}", .{i}), .vec = .{ .indices = &.{ 1, 2, 3 }, .values = values } };
    }
    try idx.batchWithOptions(writes, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    try idx.batch(&.{}, &.{writes[12287].doc_id});
    const Predicate = struct {
        calls: usize = 0,
        fail: bool = false,
        fn allows(raw: *anyopaque, key: []const u8) !bool {
            const state: *@This() = @ptrCast(@alignCast(raw));
            state.calls += 1;
            if (state.fail) return error.TestPredicateFailed;
            return (try std.fmt.parseInt(usize, key[4..], 10)) % 3 != 0;
        }
    };
    for ([_][3]f32{ .{ 1, -2, 0 }, .{ -1, 0.25, 2 }, .{ 0, 0, 0 } }) |weights| {
        const query: SparseVector = .{ .indices = &.{ 1, 2, 3 }, .values = &weights };
        var predicate: Predicate = .{};
        const key_predicate: KeyPredicate = .{ .ptr = &predicate, .allows = Predicate.allows };
        const expected = try idx.searchConstrained(a, &query, 17, .{ .key_predicate = key_predicate });
        defer SparseIndex.freeResults(a, expected);
        for ([_]usize{ 16 * 1024 * 1024, 1 }) |limit| {
            var tasks: u64 = 0;
            const actual = try idx.searchConstrained(a, &query, 17, .{ .key_predicate = key_predicate, .score_spill = .{ .io = threaded.io(), .directory = "/tmp", .chunk_records = 32, .chunk_bytes = 1024 }, .parallel_tasks = &tasks, .parallel_workspace_bytes = limit });
            defer SparseIndex.freeResults(a, actual);
            try std.testing.expect(tasks > 0);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |reference, hit| {
                try std.testing.expectEqualStrings(reference.doc_id, hit.doc_id);
                try std.testing.expectEqual(reference.score, hit.score);
            }
        }
    }
    var predicate: Predicate = .{ .fail = true };
    const query: SparseVector = .{ .indices = &.{1}, .values = &.{1} };
    try std.testing.expectError(error.TestPredicateFailed, idx.searchConstrained(a, &query, 17, .{ .key_predicate = .{ .ptr = &predicate, .allows = Predicate.allows }, .score_spill = .{ .io = threaded.io(), .directory = "/tmp" } }));
}
