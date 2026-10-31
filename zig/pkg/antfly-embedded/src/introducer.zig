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

//! Pipelined batch introducer for the index.
//!
//! Accepts document batches and builds segments in a background thread.
//! While one batch builds, the next can be queued (pipelining).
//! On completion, atomically swaps the IndexSnapshot.

const std = @import("std");
const Allocator = std.mem.Allocator;
const index_mod = @import("index.zig");
const segment_mod = @import("segment.zig");
const inverted = @import("section/inverted.zig");
const typed_dv = @import("section/typed_doc_values.zig");
const analysis_mod = @import("search/analysis.zig");
pub const AnalyzedContributionCache = @import("search/analyzed_contribution_cache.zig").Cache;
const contribution_cache_mod = @import("search/analyzed_contribution_cache.zig");
const geo_mod = @import("search/geo.zig");
const platform_time = @import("antfly_platform").time;
const process_memory = @import("antfly_platform").process_memory;
const resource_manager_mod = @import("storage/resource_manager.zig");

/// A batch of documents to index.
pub const Batch = struct {
    docs: []const Document,

    pub const Document = struct {
        id: []const u8,
        stored_data: []const u8,
        fields: []const FieldTerms,
        doc_ordinal: ?u32 = null,
    };

    pub const FieldTerms = struct {
        field_name: []const u8,
        hits: []const inverted.InvertedIndexBuilder.TermHit,
    };
};

/// Builds a segment from a batch of documents.
/// This is the core build step that can run in a background thread.
pub fn buildSegment(alloc: Allocator, batch: Batch) ![]u8 {
    return buildSegmentWithExtraSections(alloc, batch, &.{});
}

const ExtraSection = struct {
    field_name: []const u8,
    section_type: segment_mod.SectionType,
    data: []const u8,
};

const default_build_memory_target_bytes: usize = 96 * 1024 * 1024;
const default_doc_scratch_retained_bytes: usize = 1024 * 1024;

const TextBuildScratch = struct {
    hits: std.ArrayListUnmanaged(inverted.InvertedIndexBuilder.TermHit) = .empty,
    positions: std.ArrayListUnmanaged(u32) = .empty,
    field_order: std.ArrayListUnmanaged(usize) = .empty,

    fn reset(self: *TextBuildScratch, alloc: Allocator, retained_bytes: usize) void {
        resetScratchList(inverted.InvertedIndexBuilder.TermHit, alloc, &self.hits, retained_bytes);
        resetScratchList(u32, alloc, &self.positions, retained_bytes);
        resetScratchList(usize, alloc, &self.field_order, retained_bytes);
    }

    pub fn deinit(self: *TextBuildScratch, alloc: Allocator) void {
        self.hits.deinit(alloc);
        self.positions.deinit(alloc);
        self.field_order.deinit(alloc);
    }

    pub fn estimatedMemoryBytes(self: *const TextBuildScratch) u64 {
        return (@as(u64, @intCast(self.hits.capacity)) * @sizeOf(inverted.InvertedIndexBuilder.TermHit)) +
            (@as(u64, @intCast(self.positions.capacity)) * @sizeOf(u32)) +
            (@as(u64, @intCast(self.field_order.capacity)) * @sizeOf(usize));
    }
};

fn resetScratchList(comptime T: type, alloc: Allocator, list: *std.ArrayListUnmanaged(T), retained_bytes: usize) void {
    const retained: u64 = @intCast(retained_bytes);
    const capacity_bytes = @as(u64, @intCast(list.capacity)) * @sizeOf(T);
    if (capacity_bytes > retained) {
        list.deinit(alloc);
        list.* = .empty;
    } else {
        list.clearRetainingCapacity();
    }
}

const PostingRun = @import("postings_run.zig").Run;
const SourceCapture = @import("section/source_capture.zig").Capture;
const AnalysisReuse = @import("section/analysis_reuse.zig").Cache;
const PostingRunRange = struct {
    spool: *PostingRun,
    offset: usize,
    length: usize,
    level: u8 = 0,
    doc_base: u32 = 0,
    doc_space: u32 = 0,
    field_docs: u32 = 0,
    ids_offset: ?usize = null,

    fn idsView(self: @This()) !?@import("segment_source.zig").View {
        const offset = self.ids_offset orelse return null;
        return try @import("segment_source.zig").View.init((try self.spool.sealedView()).source, offset, @as(u64, self.doc_space) * 4);
    }
    fn writeIds(self: @This(), sink: anytype) !void {
        if (try self.idsView()) |ids| {
            var bytes: [16 * 1024]u8 = undefined;
            var copied: u64 = 0;
            while (copied < ids.length) {
                const take: usize = @intCast(@min(bytes.len, ids.length - copied));
                try ids.readInto(copied, bytes[0..take]);
                try sink.appendSlice(bytes[0..take]);
                copied += take;
            }
        } else try writeLinearDocIds(sink, self.doc_base, self.doc_space);
    }

    fn view(self: @This()) !@import("segment_source.zig").View {
        const whole = try self.spool.sealedView();
        return @import("segment_source.zig").View.init(whole.source, self.offset, self.length);
    }
};

fn writeLinearDocIds(sink: anytype, base: u32, count: u32) !void {
    var bytes: [4096]u8 = undefined;
    var first: u32 = 0;
    while (first < count) {
        const take = @min(count - first, bytes.len / 4);
        for (0..take) |i| std.mem.writeInt(u32, bytes[i * 4 ..][0..4], base + first + @as(u32, @intCast(i)), .little);
        try sink.appendSlice(bytes[0 .. take * 4]);
        first += @intCast(take);
    }
}

fn CollectorMap(comptime T: type) type {
    return struct {
        const Map = std.StringHashMapUnmanaged(T);
        map: Map = .empty,
        memory_bytes: u64 = 0,
        pending_bytes: u64 = 0,
        pending: std.ArrayListUnmanaged([]const u8) = .empty,
        staging: ?struct { options: BuildTextOptions, owner: *?*PostingRun } = null,
        pub const empty: @This() = .{};
        fn iterator(self: *@This()) Map.Iterator {
            return self.map.iterator();
        }
        fn valueIterator(self: *@This()) Map.ValueIterator {
            return self.map.valueIterator();
        }
        fn keyIterator(self: *@This()) Map.KeyIterator {
            return self.map.keyIterator();
        }
        fn capacity(self: *const @This()) u32 {
            return self.map.capacity();
        }
        fn count(self: *const @This()) u32 {
            return self.map.count();
        }
        fn getPtr(self: *@This(), key: []const u8) ?*T {
            return self.map.getPtr(key);
        }
        fn getOrPut(self: *@This(), a: Allocator, key: []const u8) !Map.GetOrPutResult {
            return self.map.getOrPut(a, key);
        }
        fn deinit(self: *@This(), a: Allocator) void {
            self.pending.deinit(a);
            self.map.deinit(a);
        }
    };
}
const FieldPostingsBuilders = CollectorMap(FieldPostingsBuilder);
const TypedFieldCollectors = CollectorMap(TypedFieldCollector);

const FieldPostingsBuilder = struct {
    active: bool = false,
    builder: inverted.InvertedIndexBuilder = undefined,
    runs: std.ArrayListUnmanaged(PostingRunRange) = .empty,
    field_doc_count: u32 = 0,
    doc_base: ?u32 = null,
    memory_tracker: ?*u64 = null,
    spillable_tracker: ?*u64 = null,
    queued: bool = false,
    dense_ids: std.ArrayListUnmanaged(u32) = .empty,

    fn init(alloc: Allocator) !FieldPostingsBuilder {
        return .{
            .active = true,
            .builder = inverted.InvertedIndexBuilder.init(alloc, inverted.productionIndexConfig()),
        };
    }

    pub fn deinit(self: *FieldPostingsBuilder, alloc: Allocator) void {
        if (!self.active) return;
        if (self.memory_tracker) |bytes| bytes.* -= self.estimatedMemoryBytes();
        if (self.spillable_tracker) |bytes| bytes.* -= self.estimatedMemoryBytes();
        self.builder.deinit();
        self.dense_ids.deinit(alloc);
        for (self.runs.items) |run| run.spool.releaseRange(run.offset);
        self.runs.deinit(alloc);
        self.active = false;
        self.builder = undefined;
    }

    fn addDocument(self: *FieldPostingsBuilder, doc_idx: u32, hits: []const inverted.InvertedIndexBuilder.TermHit) !void {
        const before = self.estimatedMemoryBytes();
        defer {
            if (self.memory_tracker) |bytes| bytes.* = bytes.* - before + self.estimatedMemoryBytes();
            if (self.spillable_tracker) |bytes| bytes.* = bytes.* - before + self.estimatedMemoryBytes();
        }
        if (self.doc_base == null) self.doc_base = doc_idx;
        const local = self.builder.doc_count;
        if (self.dense_ids.items.len != 0 or doc_idx != self.doc_base.? + local) {
            if (self.dense_ids.items.len == 0) {
                try self.dense_ids.ensureTotalCapacity(self.builder.alloc, local + 1);
                for (0..local) |i| self.dense_ids.appendAssumeCapacity(self.doc_base.? + @as(u32, @intCast(i)));
            }
            if (local > 0 and self.dense_ids.items[local - 1] >= doc_idx) return error.InvalidData;
            try self.dense_ids.append(self.builder.alloc, doc_idx);
        }
        try self.builder.addDocument(local, hits);
        self.field_doc_count = try std.math.add(u32, self.field_doc_count, 1);
    }

    fn spill(self: *FieldPostingsBuilder, alloc: Allocator, options: BuildTextOptions, count: u32, spool_owner: *?*PostingRun) !void {
        if (self.builder.terms.count() == 0) return;
        const io = options.postings_run_io orelse return;
        const before = self.estimatedMemoryBytes();
        defer {
            if (self.memory_tracker) |bytes| bytes.* = bytes.* - before + self.estimatedMemoryBytes();
            if (self.spillable_tracker) |bytes| bytes.* = bytes.* - before + self.estimatedMemoryBytes();
        }
        const start_ns = if (options.profile != null and options.profile_timings) platform_time.monotonicNs() else 0;
        defer if (options.profile) |p| {
            if (options.profile_timings) p.inverted_build_ns +|= platform_time.monotonicNs() - start_ns;
        };
        if (spool_owner.* == null) {
            spool_owner.* = try PostingRun.createWithResources(alloc, io, options.postings_run_directory, options.resource_manager);
            if (options.profile) |p| p.postings_spool_count +|= 1;
        }
        const spool = spool_owner.*.?;
        const start = spool.len();
        try self.builder.writeToSink(alloc, spool);
        const length = spool.len() - start;
        const ids_offset: ?usize = if (self.dense_ids.items.len == 0) null else spool.len();
        if (ids_offset != null) try self.writeIds(spool);
        try spool.seal(start);
        self.runs.append(alloc, .{ .spool = spool, .offset = start, .length = length, .doc_base = self.doc_base.?, .doc_space = self.builder.doc_count, .field_docs = self.builder.doc_count, .ids_offset = ids_offset }) catch |err| {
            spool.releaseRange(start);
            return err;
        };
        if (options.profile) |p| {
            p.postings_run_count +|= 1;
            p.postings_run_bytes +|= spool.len() - start;
        }
        const config = self.builder.config;
        self.builder.deinit();
        self.builder = inverted.InvertedIndexBuilder.init(alloc, config);
        self.doc_base = null;
        self.dense_ids.deinit(alloc);
        self.dense_ids = .empty;
        // A base-16 carry merges only peers of the same level. Each posting
        // is rewritten once per level, rather than repeatedly rewriting an
        // ever-growing prefix. All fields share one seekable private spool.
        while (self.runs.items.len >= 16) {
            const tail = self.runs.items[self.runs.items.len - 16 ..];
            const level = tail[0].level;
            var peers = true;
            for (tail) |run| if (run.level != level) {
                peers = false;
                break;
            };
            if (!peers) break;
            try self.packTail(alloc, count, spool, options.profile);
        }
    }

    fn packTail(self: *FieldPostingsBuilder, alloc: Allocator, count: u32, spool: *PostingRun, profile: ?*BuildTextProfile) !void {
        _ = count;
        const begin = self.runs.items.len - 16;
        const tail = self.runs.items[begin..];
        var level: u8 = 0;
        var field_docs: u32 = 0;
        for (tail) |run| {
            level = @max(level, run.level);
            field_docs = try std.math.add(u32, field_docs, run.field_docs);
        }
        const base = tail[0].doc_base;
        var space: u32 = 0;
        var linear = true;
        for (tail) |run| {
            if (run.ids_offset != null or run.doc_base != base + space) linear = false;
            space = try std.math.add(u32, space, run.doc_space);
        }
        const start = spool.len();
        try self.mergeRunRanges(alloc, spool, space, true, field_docs, tail);
        const length = spool.len() - start;
        const ids_offset: ?usize = if (linear) null else spool.len();
        if (!linear) for (tail) |run| try run.writeIds(spool);
        try spool.seal(start);
        for (tail) |run| spool.releaseRange(run.offset);
        self.runs.shrinkRetainingCapacity(begin);
        self.runs.appendAssumeCapacity(.{ .spool = spool, .offset = start, .length = length, .level = level + 1, .doc_base = base, .doc_space = space, .field_docs = field_docs, .ids_offset = ids_offset });
        try spool.compact();
        if (profile) |p| {
            p.postings_run_merge_count +|= 1;
            p.postings_run_max_fan_in = @max(p.postings_run_max_fan_in, 16);
        }
    }

    fn finishRuns(self: *FieldPostingsBuilder, alloc: Allocator, options: BuildTextOptions, count: u32, spool_owner: *?*PostingRun) !void {
        if (self.runs.items.len == 0) return;
        try self.spill(alloc, options, count, spool_owner);
        // At most 15 runs per level (eight levels for u32 document IDs).
        // Collapse the remaining chronological tail before opening the final
        // readers, keeping every merge's cursor fan-in at sixteen or less.
        while (self.runs.items.len > 16) try self.packTail(alloc, count, spool_owner.*.?, options.profile);
    }

    fn mergeRunRanges(self: *FieldPostingsBuilder, alloc: Allocator, sink: anytype, count: u32, dense: bool, field_docs: u32, runs: []const PostingRunRange) !void {
        const views = try alloc.alloc(?@import("segment_source.zig").View, runs.len);
        defer alloc.free(views);
        const maps = try alloc.alloc(inverted.AffineDocMap, runs.len);
        defer alloc.free(maps);
        var offset: u32 = 0;
        for (runs, views, maps) |run, *view, *map| {
            view.* = try run.view();
            map.* = if (dense) .{ .len = run.doc_space, .offset = offset } else .{ .len = run.doc_space, .offset = if (run.ids_offset == null) run.doc_base else 0, .ids = try run.idsView() };
            offset = try std.math.add(u32, offset, run.doc_space);
        }
        try inverted.writeMergedInitialRunsToSink(alloc, sink, views, maps, count, field_docs, self.builder.config);
    }

    fn mergeRuns(self: *FieldPostingsBuilder, alloc: Allocator, sink: anytype, count: u32) !void {
        try self.mergeRunRanges(alloc, sink, count, false, self.field_doc_count, self.runs.items);
    }

    fn writeUnspilled(self: *FieldPostingsBuilder, alloc: Allocator, sink: anytype, count: u32, profile: ?*inverted.InvertedIndexBuildProfile) !void {
        if (self.builder.terms.count() == 0) return;
        if ((self.doc_base orelse 0) == 0 and self.dense_ids.items.len == 0) return self.builder.writeToSinkProfile(alloc, sink, profile);
        // Expand sparse local coordinates only in the final artifact.
        var local = segment_mod.MemorySegmentSink.init(alloc);
        defer local.deinit();
        var local_sink = local.sink();
        try self.builder.writeToSinkProfile(alloc, &local_sink, profile);
        const length = local.out.items.len;
        if (self.dense_ids.items.len != 0) try self.writeIds(&local_sink);
        const source = @import("segment_source.zig").Source{ .contiguous = local.out.items };
        const view = try @import("segment_source.zig").View.init(source, 0, length);
        const ids = if (self.dense_ids.items.len != 0) try @import("segment_source.zig").View.init(source, length, local.out.items.len - length) else null;
        try inverted.writeMergedInitialRunsToSink(alloc, sink, &.{view}, &.{.{ .len = self.builder.doc_count, .offset = if (ids == null) self.doc_base.? else 0, .ids = ids }}, count, self.field_doc_count, self.builder.config);
    }

    fn writeIds(self: *const FieldPostingsBuilder, sink: anytype) !void {
        for (self.dense_ids.items) |id| {
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, id, .little);
            try sink.appendSlice(&bytes);
        }
    }

    fn buildAlloc(self: *FieldPostingsBuilder, alloc: Allocator, count: u32) ![]u8 {
        var output = segment_mod.MemorySegmentSink.init(alloc);
        defer output.deinit();
        var sink = output.sink();
        try self.writeUnspilled(alloc, &sink, count, null);
        return output.finishOwned();
    }

    pub fn estimatedMemoryBytes(self: *const FieldPostingsBuilder) u64 {
        if (!self.active) return 0;
        return self.builder.estimatedMemoryBytes() + self.dense_ids.capacity * @sizeOf(u32);
    }
};

fn deinitFieldPostingsBuilders(
    alloc: Allocator,
    builders: *FieldPostingsBuilders,
) void {
    var it = builders.valueIterator();
    while (it.next()) |builder| builder.deinit(alloc);
    builders.deinit(alloc);
}

fn ensureFieldPostingsBuilder(
    alloc: Allocator,
    builders: *FieldPostingsBuilders,
    field_name: []const u8,
) !*FieldPostingsBuilder {
    const gop = try builders.getOrPut(alloc, field_name);
    if (!gop.found_existing) {
        gop.key_ptr.* = field_name;
        gop.value_ptr.* = try FieldPostingsBuilder.init(alloc);
        gop.value_ptr.memory_tracker = &builders.memory_bytes;
        gop.value_ptr.spillable_tracker = &builders.pending_bytes;
        builders.pending_bytes += gop.value_ptr.estimatedMemoryBytes();
        builders.memory_bytes += field_name.len + gop.value_ptr.estimatedMemoryBytes();
    }
    if (!gop.value_ptr.queued) {
        try builders.pending.append(alloc, gop.key_ptr.*);
        gop.value_ptr.queued = true;
    }
    return gop.value_ptr;
}

fn buildSegmentWithExtraSections(
    alloc: Allocator,
    batch: Batch,
    extra_sections: []const ExtraSection,
) ![]u8 {
    var field_builders = FieldPostingsBuilders.empty;
    defer deinitFieldPostingsBuilders(alloc, &field_builders);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    // Track field indices for the segment
    var field_indices = std.StringHashMapUnmanaged(u16).empty;
    defer field_indices.deinit(alloc);

    for (batch.docs, 0..) |doc, doc_idx| {
        // Store the document
        try seg_writer.addStoredDocBorrowed(doc.id, doc.stored_data);

        // Process each field's term hits
        for (doc.fields) |field| {
            const builder = try ensureFieldPostingsBuilder(alloc, &field_builders, field.field_name);
            try builder.addDocument(@intCast(doc_idx), field.hits);

            // Ensure field exists in segment
            const fi_gop = try field_indices.getOrPut(alloc, field.field_name);
            if (!fi_gop.found_existing) {
                fi_gop.value_ptr.* = try seg_writer.addField(field.field_name);
            }
        }
    }

    var doc_ordinals = std.ArrayListUnmanaged(u32).empty;
    defer doc_ordinals.deinit(alloc);
    var has_doc_ordinal = false;
    for (batch.docs) |doc| {
        const ordinal = doc.doc_ordinal orelse 0;
        has_doc_ordinal = has_doc_ordinal or ordinal != 0;
        try doc_ordinals.append(alloc, ordinal);
    }
    if (has_doc_ordinal) try seg_writer.addDocOrdinals(doc_ordinals.items);

    // Build inverted indexes and attach to segment
    var fit = field_builders.iterator();
    while (fit.next()) |entry| {
        const field_name = entry.key_ptr.*;
        const inv_data = try entry.value_ptr.buildAlloc(alloc, @intCast(batch.docs.len));
        errdefer alloc.free(inv_data);

        if (inv_data.len == 0) {
            alloc.free(inv_data);
            entry.value_ptr.deinit(alloc);
            continue;
        }

        const field_idx = field_indices.get(field_name).?;
        try seg_writer.addSectionOwned(field_idx, .inverted_text, inv_data);
        entry.value_ptr.deinit(alloc);
    }

    // Attach any additional field sections, such as typed doc values.
    for (extra_sections) |section| {
        const gop = try field_indices.getOrPut(alloc, section.field_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = try seg_writer.addField(section.field_name);
        }
        try seg_writer.addSection(gop.value_ptr.*, section.section_type, section.data);
    }

    return seg_writer.build();
}

/// Pipelined introducer that builds segments and introduces them to the index.
pub const Introducer = struct {
    alloc: Allocator,
    writer: *index_mod.IndexWriter,

    pub fn init(alloc: Allocator, writer: *index_mod.IndexWriter) Introducer {
        return .{ .alloc = alloc, .writer = writer };
    }

    /// Synchronously build a segment from the batch and add it to the index.
    pub fn submit(self: *Introducer, batch: Batch) !void {
        const seg_bytes = try buildSegment(self.alloc, batch);
        defer self.alloc.free(seg_bytes);
        try self.writer.addSegment(seg_bytes);
    }
};

// ============================================================================
// Text document support (analysis-aware indexing)
// ============================================================================

pub const TypedFieldValue = struct {
    field_name: []const u8,
    value_type: typed_dv.ValueType,
    value: typed_dv.TypedValue,
    conflicted: bool = false,
};

/// A document with raw text fields that will be analyzed before indexing.
pub const NamedSortValue = struct { field_name: []const u8, value: segment_mod.SegmentIndexSortBoundValue };
pub const TextDocument = struct {
    /// Native navigation tuples may include nulls without inventing typed column values.
    index_sort_values: []const NamedSortValue = &.{},
    /// A mapped projection may reject a non-null scalar or multivalued key.
    index_sort_projection_valid: bool = true,
    id: []const u8,
    stored_data: []const u8,
    text_fields: []const TextField,
    doc_ordinal: ?u32 = null,
    recursive_typed_fields: bool = false,
    infer_type_dynamic_paths: []const []const u8 = &.{},
    /// Subtrees the document schema declares with `x-antfly-index: false`.
    /// Typed doc values are never inferred at or below these paths.
    unindexed_paths: []const []const u8 = &.{},
    typed_fields: ?[]const TypedFieldValue = null,
    typed_source: ?std.json.Value = null,
    stored_source: ?std.json.Value = null,
    /// Exact typed source size for columnar builders without a JSON body.
    stored_source_bytes: u64 = 0,
};

pub const TextField = struct {
    field_name: []const u8,
    /// Stored source path, including for generated companions and `_all`.
    source_field: ?[]const u8 = null,
    text: []const u8,
    analyzer: ?*const analysis_mod.Analyzer = null,
};

pub const BuildTextOptions = struct {
    contribution_cache: ?*AnalyzedContributionCache = null,
    recursive_typed_fields: bool = false,
    infer_type_dynamic_paths: []const []const u8 = &.{},
    unindexed_paths: []const []const u8 = &.{},
    index_sort: []const segment_mod.SegmentIndexSortField = &.{},
    /// Attest an existing physical order instead of reordering source rows.
    /// A non-monotone or missing key keeps the segment unordered.
    attest_index_sort_in_place: bool = false,
    profile: ?*BuildTextProfile = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    build_memory_target_bytes: usize = default_build_memory_target_bytes,
    /// Optional seekable scratch backend. Native production enables private
    /// runs; memory-only hosts retain the existing segment splitting policy.
    postings_run_io: ?std.Io = null,
    postings_run_directory: []const u8 = ".",
    postings_run_target_bytes: usize = 8 * 1024 * 1024,
    doc_scratch_retained_bytes: usize = default_doc_scratch_retained_bytes,
    profile_timings: bool = true,
    profile_working_set: bool = true,
    /// Omit per-document primary keys and stored source. Stable result IDs
    /// must then be supplied by `doc_ordinal`. This is used by the embedded
    /// kernel benchmark only; normal database/product segments keep storage.
    store_documents: bool = true,
    /// Retain the document key but omit the source body from the text segment.
    /// The primary database remains authoritative for source projection.
    store_document_source: bool = true,
    /// Store source as independently compressed typed field columns.
    store_source_columns: bool = false,
};

const ContributionSpan = struct {
    ptr: usize,
    len: usize,
    analyzer: usize,
    fn init(text: []const u8, analyzer: *const analysis_mod.Analyzer) @This() {
        return .{ .ptr = @intFromPtr(text.ptr), .len = text.len, .analyzer = @intFromPtr(analyzer) };
    }
};
const ContributionUses = std.AutoHashMapUnmanaged(ContributionSpan, u8);
fn repeatedContribution(uses: *const ContributionUses, field: TextField, analyzer: *const analysis_mod.Analyzer) bool {
    return (uses.get(ContributionSpan.init(field.text, analyzer)) orelse 0) >= 2;
}

pub const BuildTextProfile = struct {
    contribution_cache_hits: u64 = 0,
    contribution_tokens_reused: u64 = 0,
    typed_staging_checks: u64 = 0,
    postings_spill_checks: u64 = 0,
    doc_count: u64 = 0,
    text_field_count: u64 = 0,
    token_count: u64 = 0,
    shared_analysis_runs: u64 = 0,
    shared_analysis_reuses: u64 = 0,
    source_vectors_captured: u64 = 0,
    source_vectors_reused: u64 = 0,
    source_vector_spilled_bytes: u64 = 0,
    term_hit_count: u64 = 0,
    typed_value_count: u64 = 0,
    segment_bytes: u64 = 0,
    analyzer_ns: u64 = 0,
    term_accum_ns: u64 = 0,
    hit_materialize_ns: u64 = 0,
    typed_collect_ns: u64 = 0,
    typed_build_ns: u64 = 0,
    inverted_build_ns: u64 = 0,
    postings_run_count: u64 = 0,
    postings_spool_count: u64 = 0,
    postings_run_max_fan_in: u64 = 0,
    postings_run_bytes: u64 = 0,
    postings_run_merge_count: u64 = 0,
    inverted_sort_ns: u64 = 0,
    inverted_postings_serialize_ns: u64 = 0,
    inverted_term_dict_ns: u64 = 0,
    inverted_norms_ns: u64 = 0,
    inverted_bloom_finish_ns: u64 = 0,
    inverted_final_assembly_ns: u64 = 0,
    section_attach_ns: u64 = 0,
    stored_doc_attach_ns: u64 = 0,
    stored_compress_ns: u64 = 0,
    stored_raw_bytes: u64 = 0,
    stored_compressed_bytes: u64 = 0,
    segment_assembly_ns: u64 = 0,
    segment_encode_ns: u64 = 0,
    doc_arena_peak_bytes: u64 = 0,
    field_postings_estimated_bytes: u64 = 0,
    typed_doc_values_estimated_bytes: u64 = 0,
    stored_docs_estimated_bytes: u64 = 0,
    section_bytes: u64 = 0,
    fst_and_term_metadata_bytes: u64 = 0,
    segment_sink_bytes: u64 = 0,
    resource_peak_bytes: u64 = 0,
    build_memory_target_bytes: u64 = 0,
    doc_scratch_retained_bytes: u64 = 0,
    peak_doc_scratch_bytes: u64 = 0,
    builder_scratch_peak_bytes: u64 = 0,
    postings_live_bytes: u64 = 0,
    typed_live_bytes: u64 = 0,
    section_live_bytes: u64 = 0,
    sink_live_bytes: u64 = 0,
    flush_build_memory_count: u64 = 0,
    flush_segment_bytes_count: u64 = 0,
    flush_end_count: u64 = 0,
    oversized_doc_count: u64 = 0,
    estimated_build_bytes: u64 = 0,
    estimated_segment_bytes: u64 = 0,
    rss_before: u64 = 0,
    rss_after_analyze: u64 = 0,
    rss_after_postings_build: u64 = 0,
    rss_after_sections: u64 = 0,
    rss_after_publish: u64 = 0,
};

const TextBuildResourceTracker = struct {
    manager: ?*resource_manager_mod.ResourceManager,
    profile: ?*BuildTextProfile,
    current_bytes: u64 = 0,

    fn init(manager: ?*resource_manager_mod.ResourceManager, profile: ?*BuildTextProfile) TextBuildResourceTracker {
        return .{ .manager = manager, .profile = profile };
    }

    fn adjust(self: *TextBuildResourceTracker, next: u64) !void {
        if (self.manager) |manager| {
            try manager.adjustUsage(.full_text_build_working_set, &self.current_bytes, next);
        } else {
            self.current_bytes = next;
        }
        if (self.profile) |profile| profile.resource_peak_bytes = @max(profile.resource_peak_bytes, self.current_bytes);
    }

    fn release(self: *TextBuildResourceTracker) void {
        if (self.manager) |manager| {
            manager.adjustUsage(
                .full_text_build_working_set,
                &self.current_bytes,
                0,
            ) catch {};
        }
        self.current_bytes = 0;
    }
};

fn estimateTextDocInputBytes(docs: []const TextDocument) u64 {
    var total: u64 = 0;
    for (docs) |doc| {
        total +|= estimateTextDocumentInputBytes(doc);
    }
    return total;
}

/// Reclaiming small-object allocation on native hosts; large encoder buffers
/// can be returned immediately. Freestanding builds retain their page backend.
pub fn textBuildScratchAllocator() Allocator {
    return if (comptime @import("builtin").link_libc) std.heap.c_allocator else std.heap.page_allocator;
}

fn aliasesInput(doc: TextDocument, bytes: []const u8) bool {
    if (bytes.len == 0) return true;
    const start = @intFromPtr(bytes.ptr);
    const stored = @intFromPtr(doc.stored_data.ptr);
    if (start >= stored and start - stored <= doc.stored_data.len and bytes.len <= doc.stored_data.len - (start - stored)) return true;
    for (doc.text_fields) |field| {
        const text = @intFromPtr(field.text.ptr);
        if (start >= text and start - text <= field.text.len and bytes.len <= field.text.len - (start - text)) return true;
    }
    return false;
}

fn estimateTypedSourceBytes(doc: TextDocument, value: std.json.Value, include_aliases: bool) u64 {
    return switch (value) {
        .string, .number_string => |bytes| if (!include_aliases and aliasesInput(doc, bytes)) 0 else @intCast(bytes.len),
        .array => |array| blk: {
            var total: u64 = @as(u64, @intCast(array.capacity)) *| @sizeOf(std.json.Value);
            for (array.items) |child| total +|= estimateTypedSourceBytes(doc, child, include_aliases);
            break :blk total;
        },
        .object => |object| blk: {
            var total: u64 = @as(u64, @intCast(object.capacity())) *| (@sizeOf(std.json.Value) + @sizeOf([]const u8) + 16);
            var it = object.iterator();
            while (it.next()) |entry| {
                if (include_aliases or !aliasesInput(doc, entry.key_ptr.*)) total +|= @intCast(entry.key_ptr.*.len);
                total +|= estimateTypedSourceBytes(doc, entry.value_ptr.*, include_aliases);
            }
            break :blk total;
        },
        else => 0,
    };
}

fn estimateTextDocumentInputBytes(doc: TextDocument) u64 {
    var total: u64 = @intCast(doc.id.len + doc.stored_data.len);
    if (doc.stored_source_bytes != 0) total +|= estimateTypedSourceBytes(doc, doc.stored_source.?, false);
    total +|= @as(u64, @intCast(doc.text_fields.len)) * (@sizeOf(TextField) + 16);
    for (doc.text_fields) |field| {
        total +|= @intCast(field.field_name.len + field.text.len);
    }
    if (doc.typed_fields) |typed_fields| {
        total +|= @as(u64, @intCast(typed_fields.len)) * (@sizeOf(TypedFieldValue) + 16);
        for (typed_fields) |field| {
            total +|= @intCast(field.field_name.len);
            if (field.value == .bytes_val and !aliasesInput(doc, field.value.bytes_val)) total +|= @intCast(field.value.bytes_val.len);
        }
    } else if (doc.stored_source_bytes == 0) {
        if (doc.typed_source) |source| total +|= estimateTypedSourceBytes(doc, source, false);
    }
    return total;
}

pub fn estimateTextDocumentSegmentBytes(doc: TextDocument) u64 {
    var total: u64 = 64 + @as(u64, @intCast(doc.id.len)) + @max(doc.stored_data.len, doc.stored_source_bytes);
    for (doc.text_fields) |field| {
        total +|= 16 + @as(u64, @intCast(field.field_name.len + field.text.len));
    }
    if (doc.typed_fields) |typed_fields| {
        total +|= @as(u64, @intCast(typed_fields.len)) * 32;
        for (typed_fields) |field| {
            total +|= @intCast(field.field_name.len);
            if (field.value == .bytes_val) total +|= @intCast(field.value.bytes_val.len);
        }
    } else if (doc.typed_source) |source| total +|= estimateTypedSourceBytes(doc, source, true);
    return total;
}

pub fn estimateTextDocumentBuildMemoryBytes(doc: TextDocument) u64 {
    var text_bytes: u64 = 0;
    for (doc.text_fields) |field| {
        text_bytes +|= @intCast(field.field_name.len + field.text.len);
    }
    const typed_count: u64 = if (doc.typed_fields) |typed_fields| @intCast(typed_fields.len) else 0;
    const field_count: u64 = @intCast(doc.text_fields.len);
    return estimateTextDocumentInputBytes(doc) +
        estimateStoredDocBytes(&.{doc}) +
        text_bytes * 4 +
        field_count * 384 +
        typed_count * 128 +
        1024;
}

pub const TextBuildSplitReason = enum {
    end,
    build_memory,
    segment_bytes,
};

pub const TextBuildSplitOptions = struct {
    target_build_memory_bytes: usize = default_build_memory_target_bytes,
    target_segment_bytes: usize = std.math.maxInt(usize),
};

pub const TextBuildSplit = struct {
    end: usize,
    reason: TextBuildSplitReason,
    estimated_build_bytes: u64,
    estimated_segment_bytes: u64,
    oversized_doc: bool = false,
};

pub fn splitTextDocumentsForBuildBudget(
    docs: []const TextDocument,
    start: usize,
    options: TextBuildSplitOptions,
) TextBuildSplit {
    const target_build: u64 = @max(@as(u64, 1), @as(u64, @intCast(options.target_build_memory_bytes)));
    const target_segment: u64 = @max(@as(u64, 1), @as(u64, @intCast(options.target_segment_bytes)));
    var end = start;
    var build_bytes: u64 = 0;
    var segment_bytes: u64 = 0;
    while (end < docs.len) {
        const doc_build_bytes = estimateTextDocumentBuildMemoryBytes(docs[end]);
        const doc_segment_bytes = estimateTextDocumentSegmentBytes(docs[end]);
        const next_build_bytes = build_bytes +| doc_build_bytes;
        const next_segment_bytes = segment_bytes +| doc_segment_bytes;
        if (end > start and next_build_bytes > target_build) {
            return .{
                .end = end,
                .reason = .build_memory,
                .estimated_build_bytes = build_bytes,
                .estimated_segment_bytes = segment_bytes,
            };
        }
        if (end > start and next_segment_bytes > target_segment) {
            return .{
                .end = end,
                .reason = .segment_bytes,
                .estimated_build_bytes = build_bytes,
                .estimated_segment_bytes = segment_bytes,
            };
        }
        end += 1;
        build_bytes = next_build_bytes;
        segment_bytes = next_segment_bytes;
        if (end == start + 1 and (build_bytes > target_build or segment_bytes > target_segment)) {
            return .{
                .end = end,
                .reason = if (build_bytes > target_build) .build_memory else .segment_bytes,
                .estimated_build_bytes = build_bytes,
                .estimated_segment_bytes = segment_bytes,
                .oversized_doc = true,
            };
        }
    }
    return .{
        .end = end,
        .reason = .end,
        .estimated_build_bytes = build_bytes,
        .estimated_segment_bytes = segment_bytes,
    };
}

fn estimateStoredDocBytes(docs: []const TextDocument) u64 {
    var total: u64 = 0;
    for (docs) |doc| {
        total +|= @as(u64, @intCast(doc.id.len)) + @max(doc.stored_data.len, doc.stored_source_bytes);
        total +|= 32;
    }
    return total;
}

fn estimateFieldPostingsBuilderBytes(builders: *FieldPostingsBuilders) u64 {
    return @as(u64, builders.capacity()) * (@sizeOf([]const u8) + @sizeOf(FieldPostingsBuilder) + 24) + builders.pending.capacity * @sizeOf([]const u8) + builders.memory_bytes;
}

fn estimateTypedDocValuesBytes(typed_fields: *TypedFieldCollectors) u64 {
    var total: u64 = @as(u64, @intCast(typed_fields.capacity())) * (@sizeOf([]const u8) + @sizeOf(TypedFieldCollector) + 24);
    var it = typed_fields.iterator();
    while (it.next()) |entry| {
        total +|= @intCast(entry.key_ptr.*.len);
        if (entry.value_ptr.writer) |*writer| total +|= writer.estimatedMemoryBytes();
        total +|= entry.value_ptr.staged.capacity * @sizeOf(PostingRunRange);
    }
    return total;
}

fn noteBuildMemorySample(profile: ?*BuildTextProfile, enabled: bool, comptime field: []const u8) void {
    if (enabled) {
        const p = profile orelse return;
        const stats = process_memory.snapshot();
        @field(p, field) = stats.resident_bytes;
    }
}

/// Build a segment from text documents, analyzing each text field.
/// The default_analyzer is used for fields without an explicit analyzer.
pub fn buildSegmentFromText(
    alloc: Allocator,
    docs: []const TextDocument,
    default_analyzer: *const analysis_mod.Analyzer,
    config_json: ?[]const u8,
) ![]u8 {
    var config_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer config_arena_state.deinit();
    const config_arena = config_arena_state.allocator();
    const text_analysis = try parseTextAnalysisConfig(config_arena, config_json);
    return try buildSegmentFromTextWithAnalysisOptions(alloc, docs, default_analyzer, text_analysis, .{});
}

pub fn buildSegmentFromTextWithAnalysis(
    alloc: Allocator,
    docs: []const TextDocument,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
) ![]u8 {
    return try buildSegmentFromTextWithAnalysisOptions(alloc, docs, default_analyzer, text_analysis, .{});
}

pub fn buildSegmentFromTextWithAnalysisOptions(
    alloc: Allocator,
    docs: []const TextDocument,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
) ![]u8 {
    var sink_impl = segment_mod.MemorySegmentSink.init(alloc);
    errdefer sink_impl.deinit();
    var sink = sink_impl.sink();
    try writeSegmentFromTextWithAnalysisOptions(alloc, docs, default_analyzer, text_analysis, options, &sink);
    return try sink_impl.finishOwned();
}

pub fn writeSegmentFromTextWithAnalysisOptions(
    backing_alloc: Allocator,
    docs: []const TextDocument,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
    sink: *segment_mod.SegmentSink,
) !void {
    // Borrowed inputs are a separate lease. Charge construction at actual
    // allocator boundaries, including transient capacity growth and encoder
    // scratch, rather than subtracting logical frees from an outer arena.
    var input_tracker = TextBuildResourceTracker.init(options.resource_manager, null);
    defer input_tracker.release();
    const input_estimated_bytes = estimateTextDocInputBytes(docs);
    try input_tracker.adjust(input_estimated_bytes);
    var build_budget: ?resource_manager_mod.BudgetedAllocator = if (options.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .full_text_build_working_set, backing_alloc, 1)
    else
        null;
    defer if (build_budget) |*budget| budget.deinit();
    const alloc = if (build_budget) |*budget| budget.allocator() else backing_alloc;
    writeTextSegmentWithScratch(alloc, docs, default_analyzer, text_analysis, options, sink) catch |err| {
        if (build_budget) |*budget| if (err == error.OutOfMemory and budget.budget_denied) return error.ResourceBudgetExceeded;
        return err;
    };
}

/// Spill at physical-field boundaries as well as row boundaries. A document
/// may have many analyzed companions, each larger than the entire run target.
/// Waiting for its last field defeats the working-set bound on small nodes.
/// Explicit non-default targets remain deterministic for tests and callers.
/// Adaptive growth uses advisory slice/node headroom; every allocation still
/// passes normal admission. Small nodes retain the proven 8-MiB run policy.
fn postingsRunTarget(options: BuildTextOptions, pending: u64) u64 {
    const base = options.postings_run_target_bytes;
    if (base != 8 * 1024 * 1024) return base;
    const manager = options.resource_manager orelse return base;
    if (manager.memoryHardLimitForSlice(.full_text_build_working_set) <= 128 * 1024 * 1024) return base;
    const available = manager.availableAdmissionBytes(.full_text_build_working_set);
    return @max(base, @min(64 * 1024 * 1024, (pending +| available) / 4));
}

fn spillPendingFields(alloc: Allocator, builders: *FieldPostingsBuilders, options: BuildTextOptions, count: u32, spool: *?*PostingRun) !void {
    if (options.postings_run_io == null or builders.pending_bytes < options.postings_run_target_bytes) return;
    const target = postingsRunTarget(options, builders.pending_bytes);
    if (builders.pending_bytes < target) return;
    // Spill largest fields first. Tiny companion fields retain their builders
    // instead of creating a run every time a large field crosses its target.
    std.sort.pdq([]const u8, builders.pending.items, builders, struct {
        fn less(context: *FieldPostingsBuilders, left: []const u8, right: []const u8) bool {
            return context.getPtr(left).?.estimatedMemoryBytes() > context.getPtr(right).?.estimatedMemoryBytes();
        }
    }.less);
    var spilled: usize = 0;
    while (spilled < builders.pending.items.len and builders.pending_bytes >= target / 2) : (spilled += 1) {
        const builder = builders.getPtr(builders.pending.items[spilled]).?;
        if (options.profile) |p| p.postings_spill_checks +|= 1;
        try builder.spill(alloc, options, count, spool);
        builder.queued = false;
    }
    std.mem.copyForwards([]const u8, builders.pending.items, builders.pending.items[spilled..]);
    builders.pending.items.len -= spilled;
}

fn writeTextSegmentWithScratch(
    alloc: Allocator,
    docs: []const TextDocument,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
    sink: *segment_mod.SegmentSink,
) !void {
    const profile = options.profile;
    const profile_timings = profile != null and options.profile_timings;
    const profile_working_set = profile != null and options.profile_working_set;
    if (profile) |p| {
        p.doc_count +|= @intCast(docs.len);
        p.build_memory_target_bytes = @intCast(options.build_memory_target_bytes);
        p.doc_scratch_retained_bytes = @intCast(options.doc_scratch_retained_bytes);
    }
    noteBuildMemorySample(profile, profile_working_set, "rss_before");

    const input_estimated_bytes = estimateTextDocInputBytes(docs);
    // Estimates remain useful for diagnostics; they never charge the same
    // scratch a second time or impose a fictitious admission bound.
    var resource_tracker = TextBuildResourceTracker.init(null, profile);
    defer resource_tracker.release();
    try resource_tracker.adjust(input_estimated_bytes);

    var source_capture = SourceCapture.init(alloc, options.store_source_columns and options.store_document_source, options.postings_run_io, options.postings_run_directory, options.resource_manager);
    defer source_capture.deinit();
    var postings_spool: ?*PostingRun = null;
    defer if (postings_spool) |spool| spool.deinit();

    var field_builders = FieldPostingsBuilders.empty;
    defer deinitFieldPostingsBuilders(alloc, &field_builders);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    var field_indices = std.StringHashMapUnmanaged(u16).empty;
    defer field_indices.deinit(alloc);

    var analyzer_cache = std.StringHashMapUnmanaged(?*const analysis_mod.Analyzer).empty;
    defer analyzer_cache.deinit(alloc);
    var contribution_uses: ContributionUses = .empty;
    defer contribution_uses.deinit(alloc);

    var doc_ordinals = std.ArrayListUnmanaged(u32).empty;
    defer doc_ordinals.deinit(alloc);
    var has_doc_ordinal = false;
    var min_doc_key: ?[]const u8 = null;
    var max_doc_key: ?[]const u8 = null;

    var typed_fields = TypedFieldCollectors.empty;
    if (options.postings_run_io != null) typed_fields.staging = .{ .options = options, .owner = &postings_spool };
    defer {
        var it = typed_fields.valueIterator();
        while (it.next()) |collector| {
            collector.deinit(alloc);
        }
        var key_it = typed_fields.keyIterator();
        while (key_it.next()) |key| alloc.free(key.*);
        typed_fields.deinit(alloc);
    }

    var doc_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer doc_arena_state.deinit();

    var scratch = TextBuildScratch{};
    defer scratch.deinit(alloc);

    const attested = options.attest_index_sort_in_place and options.index_sort.len != 0 and
        (textDocumentsAreIndexSorted(alloc, docs, text_analysis, options.index_sort) catch |err| switch (err) {
            error.OutOfMemory => false,
            else => return err,
        });
    var index_sort: []const segment_mod.SegmentIndexSortField = if (options.attest_index_sort_in_place and !attested) &.{} else options.index_sort;
    const sort_docs: []const TextDocument = if (attested and docs.len > 1) &.{ docs[0], docs[docs.len - 1] } else docs;
    var empty_doc_order: [0]TextIndexSortEntry = .{};
    const doc_order: []TextIndexSortEntry = if (index_sort.len > 0)
        buildTextDocumentOrderAlloc(alloc, sort_docs, text_analysis, index_sort) catch |err| switch (err) {
            error.InvalidSegment, error.UnsupportedTypedDocValues => declined: {
                index_sort = &.{};
                break :declined empty_doc_order[0..];
            },
            else => return err,
        }
    else
        empty_doc_order[0..];
    defer freeTextDocumentOrder(alloc, doc_order);

    for (0..docs.len) |doc_idx| {
        const source_doc_idx = if (index_sort.len > 0 and !attested) doc_order[doc_idx].doc_index else doc_idx;
        const text_doc = docs[source_doc_idx];
        contribution_uses.clearRetainingCapacity();
        if (options.contribution_cache != null) {
            // Bounded prediction admits the first analysis only when another
            // actual, unskipped consumer will use these same bytes/pipeline.
            for (text_doc.text_fields) |field| {
                if (field.text.len < contribution_cache_mod.minimum_input) continue;
                if (contribution_uses.count() >= 256) break;
                const analyzer = try cachedFieldAnalyzer(alloc, &analyzer_cache, field, default_analyzer, text_analysis);
                const use = contribution_uses.getOrPut(alloc, ContributionSpan.init(field.text, analyzer)) catch {
                    contribution_uses.clearRetainingCapacity();
                    break;
                };
                if (!use.found_existing) use.value_ptr.* = 0;
                use.value_ptr.* = @min(2, use.value_ptr.* + 1);
            }
        }
        _ = doc_arena_state.reset(.{ .retain_with_limit = options.doc_scratch_retained_bytes });
        scratch.reset(alloc, options.doc_scratch_retained_bytes);
        const doc_alloc = doc_arena_state.allocator();

        var reuse = AnalysisReuse.init(alloc);
        defer {
            if (profile) |p| {
                p.shared_analysis_runs +|= reuse.analyses;
                p.shared_analysis_reuses +|= reuse.reuses;
            }
            reuse.deinit();
        }
        if (text_doc.text_fields.len > 1) {
            var sources: std.StringHashMapUnmanaged(usize) = .empty;
            defer sources.deinit(alloc);
            for (text_doc.text_fields) |field| {
                const item = try sources.getOrPut(alloc, field.source_field orelse field.field_name);
                if (!item.found_existing) item.value_ptr.* = 0;
                item.value_ptr.* += 1;
            }
            for (text_doc.text_fields) |field| {
                const source_field = field.source_field orelse field.field_name;
                if (sources.get(source_field).? < 2) continue;
                const analyzer = try cachedFieldAnalyzer(alloc, &analyzer_cache, field, default_analyzer, text_analysis);
                // Streaming suffix expansion keeps its existing bounded cursor.
                if (analyzer.tokenStream(field.text) != null and !(source_capture.active and field.text.len >= @import("section/source_column.zig").offset_min_bytes and @import("section/source_column.zig").eligibleAnalyzer(analyzer))) continue;
                try reuse.note(.{ .field = source_field, .text = field.text, .signature = try source_capture.signatureFor(analyzer) });
            }
        }

        const stored_attach_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        if (options.store_documents) {
            try seg_writer.addStoredDocFromInput(
                text_doc.id,
                if (options.store_document_source) (if (options.store_source_columns) segment_mod.columnar_source_marker else text_doc.stored_data) else "{}",
            );
        } else {
            if (text_doc.doc_ordinal == null) return error.MissingTextDocOrdinal;
            try seg_writer.addUnstoredDoc();
            if (min_doc_key == null or std.mem.order(u8, text_doc.id, min_doc_key.?) == .lt) min_doc_key = text_doc.id;
            if (max_doc_key == null or std.mem.order(u8, text_doc.id, max_doc_key.?) == .gt) max_doc_key = text_doc.id;
        }
        if (profile_timings) {
            if (profile) |p| p.stored_doc_attach_ns +|= platform_time.monotonicNs() - stored_attach_start_ns;
        }

        const doc_ordinal = text_doc.doc_ordinal orelse 0;
        has_doc_ordinal = has_doc_ordinal or doc_ordinal != 0;
        try doc_ordinals.append(alloc, doc_ordinal);

        if (!try orderTextFields(alloc, &scratch.field_order, text_doc.text_fields)) {
            for (text_doc.text_fields) |tf| {

                // Unique contributions do not share token ownership. Release
                // copied term keys between physical representations, rather
                // than retaining every representation until the row finishes.
                _ = doc_arena_state.reset(.{ .retain_with_limit = options.doc_scratch_retained_bytes });
                try addSingleTextFieldToBuilders(
                    alloc,
                    doc_alloc,
                    &field_builders,
                    &field_indices,
                    &seg_writer,
                    &analyzer_cache,
                    @intCast(doc_idx),
                    tf,
                    default_analyzer,
                    text_analysis,
                    profile,
                    profile_timings,
                    &scratch,
                    &source_capture,
                    text_doc.stored_source,
                    &reuse,
                    options.contribution_cache,
                    &contribution_uses,
                );
                scratch.hits.clearRetainingCapacity();
                try spillPendingFields(alloc, &field_builders, options, @intCast(doc_idx + 1), &postings_spool);
            }
        } else {
            // Group contributions without changing their order within a physical
            // field. Only one field's tokens and copied keys live at a time;
            // spilling between contributions would duplicate a document's posting.
            var start: usize = 0;
            while (start < scratch.field_order.items.len) {
                const field_name = text_doc.text_fields[scratch.field_order.items[start]].field_name;
                var end = start + 1;
                while (end < scratch.field_order.items.len and std.mem.eql(u8, field_name, text_doc.text_fields[scratch.field_order.items[end]].field_name)) : (end += 1) {}
                _ = doc_arena_state.reset(.{ .retain_with_limit = options.doc_scratch_retained_bytes });
                if (end == start + 1) {
                    try addSingleTextFieldToBuilders(alloc, doc_alloc, &field_builders, &field_indices, &seg_writer, &analyzer_cache, @intCast(doc_idx), text_doc.text_fields[scratch.field_order.items[start]], default_analyzer, text_analysis, profile, profile_timings, &scratch, &source_capture, text_doc.stored_source, &reuse, options.contribution_cache, &contribution_uses);
                    scratch.hits.clearRetainingCapacity();
                } else {
                    var field_maps = std.StringHashMapUnmanaged(FieldAcc).empty;
                    defer {
                        var fields = field_maps.valueIterator();
                        while (fields.next()) |field| field.deinit();
                    }
                    for (scratch.field_order.items[start..end]) |index| {
                        const tf = text_doc.text_fields[index];
                        if (profile) |p| p.text_field_count +|= 1;
                        const analyzer = try cachedFieldAnalyzer(alloc, &analyzer_cache, tf, default_analyzer, text_analysis);
                        try accumulateAnalyzedField(doc_alloc, alloc, &field_maps, tf.field_name, analyzer, tf.text, profile, profile_timings, &source_capture, @intCast(doc_idx), tf.source_field orelse tf.field_name, text_doc.stored_source, &reuse, options.contribution_cache, repeatedContribution(&contribution_uses, tf, analyzer));
                    }
                    const hit_materialize_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
                    scratch.hits.clearRetainingCapacity();
                    if (field_maps.getPtr(field_name)) |field| {
                        var it = field.term_map.iterator();
                        while (it.next()) |entry| {
                            try scratch.hits.append(alloc, .{
                                .term = entry.key_ptr.*,
                                .freq = entry.value_ptr.freq,
                                .norm = field.token_offset,
                                .positions = entry.value_ptr.positionSlice(),
                            });
                            if (profile) |p| p.term_hit_count +|= 1;
                        }
                        if (scratch.hits.items.len != 0) try addFieldHitsToBuilder(alloc, &field_builders, &field_indices, &seg_writer, @intCast(doc_idx), field_name, scratch.hits.items);
                    }
                    scratch.hits.clearRetainingCapacity();
                    if (profile_timings) {
                        if (profile) |p| p.hit_materialize_ns +|= platform_time.monotonicNs() - hit_materialize_start_ns;
                    }
                }
                _ = doc_arena_state.reset(.{ .retain_with_limit = options.doc_scratch_retained_bytes });
                try spillPendingFields(alloc, &field_builders, options, @intCast(doc_idx + 1), &postings_spool);
                start = end;
            }
        }

        var doc_options = options;
        doc_options.recursive_typed_fields = doc_options.recursive_typed_fields or text_doc.recursive_typed_fields;
        if (text_doc.infer_type_dynamic_paths.len > 0) {
            doc_options.infer_type_dynamic_paths = text_doc.infer_type_dynamic_paths;
        }
        if (text_doc.unindexed_paths.len > 0) {
            doc_options.unindexed_paths = text_doc.unindexed_paths;
        }
        const typed_collect_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        if (text_doc.typed_fields) |projected_typed_fields| {
            for (projected_typed_fields) |field| {
                if (field.conflicted) {
                    try markTypedFieldConflict(alloc, &typed_fields, field.field_name);
                    continue;
                }
                try appendTypedFieldValue(alloc, &typed_fields, field.field_name, @intCast(doc_idx), .{
                    .value_type = field.value_type,
                    .value = field.value,
                }, profile, true);
            }
        } else if (text_doc.typed_source) |typed_source| {
            try collectTypedFieldValuesFromValue(alloc, typed_source, @intCast(doc_idx), &typed_fields, text_analysis, doc_options, true);
        } else {
            try collectTypedFieldValues(alloc, text_doc.stored_data, @intCast(doc_idx), &typed_fields, text_analysis, doc_options);
        }
        if (profile_timings) {
            if (profile) |p| p.typed_collect_ns +|= platform_time.monotonicNs() - typed_collect_start_ns;
        }
        try spillPendingFields(alloc, &field_builders, options, @intCast(doc_idx + 1), &postings_spool);
        if (postings_spool) |spool| try spool.compact();
        if (profile_working_set) {
            if (profile) |p| {
                p.peak_doc_scratch_bytes = @max(p.peak_doc_scratch_bytes, @as(u64, @intCast(doc_arena_state.queryCapacity())));
                p.builder_scratch_peak_bytes = @max(p.builder_scratch_peak_bytes, scratch.estimatedMemoryBytes());
            }
        }
    }
    if (profile) |p| {
        p.source_vectors_captured +|= source_capture.captured;
        p.source_vectors_reused +|= source_capture.reused;
        if (source_capture.spool) |run| p.source_vector_spilled_bytes +|= run.len();
    }
    var finalize_typed = typed_fields.valueIterator();
    while (finalize_typed.next()) |collector| {
        if (collector.staged.items.len > 0) try collector.stage(alloc, options, &postings_spool);
    }
    var finalize_runs = field_builders.valueIterator();
    while (finalize_runs.next()) |builder| {
        try builder.finishRuns(alloc, options, @intCast(docs.len), &postings_spool);
    }
    if (profile_working_set) {
        if (profile) |p| {
            p.stored_docs_estimated_bytes = estimateStoredDocBytes(docs);
            p.field_postings_estimated_bytes = estimateFieldPostingsBuilderBytes(&field_builders);
            p.typed_doc_values_estimated_bytes = estimateTypedDocValuesBytes(&typed_fields);
            p.postings_live_bytes = p.field_postings_estimated_bytes;
            p.typed_live_bytes = p.typed_doc_values_estimated_bytes;
            p.doc_arena_peak_bytes = @max(p.doc_arena_peak_bytes, p.field_postings_estimated_bytes + p.typed_doc_values_estimated_bytes);
        }
    }
    noteBuildMemorySample(profile, profile_working_set, "rss_after_analyze");
    const stored_docs_estimated_bytes = estimateStoredDocBytes(docs);
    const remaining_postings_estimated_bytes = estimateFieldPostingsBuilderBytes(&field_builders);
    const typed_doc_values_estimated_bytes = estimateTypedDocValuesBytes(&typed_fields);
    try resource_tracker.adjust(input_estimated_bytes +
        stored_docs_estimated_bytes +
        remaining_postings_estimated_bytes +
        typed_doc_values_estimated_bytes);

    // Freeze both maps before storing producer pointers. Their values and the
    // immutable input batch stay alive until all sections have been written.
    var build_state = InitialSectionBuildState{
        .tracker = &resource_tracker,
        .output = sink,
        .profile = profile,
        .profile_timings = profile_timings,
        .profile_working_set = profile_working_set,
        .input_bytes = input_estimated_bytes + stored_docs_estimated_bytes,
        .postings_bytes = remaining_postings_estimated_bytes,
        .typed_bytes = typed_doc_values_estimated_bytes,
    };
    const producers = try alloc.alloc(InitialSectionProducer, field_builders.count() + typed_fields.count());
    defer alloc.free(producers);
    var producer_count: usize = 0;

    const segment_encode_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    if (has_doc_ordinal) try seg_writer.addDocOrdinals(doc_ordinals.items);
    if (!options.store_documents) {
        try seg_writer.addDocKeyRange(min_doc_key orelse return error.InvalidSegment, max_doc_key orelse return error.InvalidSegment);
    }
    if (index_sort.len > 0) {
        const complete_fields = try alloc.dupe(segment_mod.SegmentIndexSortField, index_sort);
        defer alloc.free(complete_fields);
        for (complete_fields) |*field| field.nulls_complete = true;
        if (doc_order.len > 0) {
            var bounds = try textIndexSortBoundsAlloc(alloc, doc_order[0].keys, doc_order[doc_order.len - 1].keys);
            defer bounds.deinit(alloc);
            try seg_writer.addIndexSortMetadataWithBounds(complete_fields, bounds);
        } else {
            try seg_writer.addIndexSortMetadata(complete_fields);
        }
    }

    var fit = field_builders.iterator();
    while (fit.next()) |entry| {
        producers[producer_count] = .{ .state = &build_state, .source = .{ .postings = entry.value_ptr }, .doc_count = @intCast(docs.len) };
        try seg_writer.addSectionBuilder(field_indices.get(entry.key_ptr.*).?, .inverted_text, .{
            .context = &producers[producer_count],
            .write = InitialSectionProducer.write,
        });
        producer_count += 1;
    }
    var typed_it = typed_fields.iterator();
    while (typed_it.next()) |entry| {
        if (entry.value_ptr.conflicted or entry.value_ptr.writer == null) continue;
        if (entry.value_ptr.writer.?.entries.items.len == 0 and entry.value_ptr.staged.items.len == 0) continue;
        const field_idx = field_indices.get(entry.key_ptr.*) orelse try seg_writer.addField(entry.key_ptr.*);
        producers[producer_count] = .{ .state = &build_state, .source = .{ .typed = entry.value_ptr } };
        try seg_writer.addSectionBuilder(field_idx, .typed_doc_values, .{
            .context = &producers[producer_count],
            .write = InitialSectionProducer.write,
        });
        producer_count += 1;
    }
    var source_fields: std.StringHashMapUnmanaged(void) = .empty;
    defer source_fields.deinit(alloc);
    if (options.store_source_columns and options.store_document_source) {
        for (docs) |doc| {
            const root = doc.stored_source orelse return error.InvalidSegment;
            if (root != .object) return error.InvalidSegment;
            for (root.object.keys()) |key| try source_fields.put(alloc, key, {});
        }
    }
    const source_producers = try alloc.alloc(SourceColumnProducer, source_fields.count());
    defer alloc.free(source_producers);
    var source_names = source_fields.keyIterator();
    for (source_producers) |*producer| {
        const name = source_names.next().?.*;
        const field_idx = blk: {
            for (seg_writer.fields.items, 0..) |field, i| if (std.mem.eql(u8, field.name, name)) break :blk @as(u16, @intCast(i));
            break :blk try seg_writer.addField(name);
        };
        producer.* = .{ .state = &build_state, .docs = docs, .order = if (index_sort.len > 0 and !attested) doc_order else &.{}, .field = name, .capture = &source_capture };
        try seg_writer.addSectionBuilder(field_idx, .columnar_stored, .{ .context = producer, .write = SourceColumnProducer.write });
    }
    noteBuildMemorySample(profile, profile_working_set, "rss_after_postings_build");

    const segment_assembly_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    const segment_start_len = sink.len();
    try seg_writer.writeToSink(sink);
    if (profile) |p| {
        if (profile_timings) {
            p.segment_assembly_ns +|= platform_time.monotonicNs() - segment_assembly_start_ns;
            p.stored_compress_ns +|= seg_writer.last_stored_compress_ns;
            p.segment_encode_ns +|= platform_time.monotonicNs() - segment_encode_start_ns;
        }
        p.stored_raw_bytes +|= seg_writer.last_stored_raw_bytes;
        p.stored_compressed_bytes +|= seg_writer.last_stored_compressed_bytes;
        p.segment_bytes +|= @intCast(sink.len() - segment_start_len);
        if (profile_working_set) {
            p.segment_sink_bytes = @intCast(sink.len() - segment_start_len);
            p.sink_live_bytes = @intCast(sink.residentBytes());
        }
    }
    noteBuildMemorySample(profile, profile_working_set, "rss_after_sections");
    try build_state.adjust(0);
    noteBuildMemorySample(profile, profile_working_set, "rss_after_publish");
}

const TermAcc = struct {
    freq: u32 = 0,
    // Most dictionary terms occur once or twice. Reuse the vector's storage
    // for those positions and allocate only when a term becomes frequent.
    positions: union { small: [2]u32, heap: std.ArrayListUnmanaged(u32) } = .{ .small = undefined },
    inline fn push(self: *@This(), a: Allocator, position: u32) !void {
        const next = try std.math.add(u32, self.freq, 1);
        if (self.freq < 2) {
            self.positions.small[self.freq] = position;
        } else if (self.freq == 2) {
            var values: std.ArrayListUnmanaged(u32) = .empty;
            errdefer values.deinit(a);
            try values.appendSlice(a, &self.positions.small);
            try values.append(a, position);
            self.positions = .{ .heap = values };
        } else try self.positions.heap.append(a, position);
        self.freq = next;
    }
    fn appendShifted(self: *@This(), a: Allocator, positions: []const u32, base: u32) !void {
        if (positions.len == 0) return;
        const total = try std.math.add(u32, self.freq, std.math.cast(u32, positions.len) orelse return error.Overflow);
        // Check every position before mutating the accumulator (custom token
        // filters may emit non-monotonic/duplicate positions).
        for (positions) |position| _ = try std.math.add(u32, base, position);
        if (total <= 2) {
            for (positions, self.freq..) |position, index| self.positions.small[index] = base + position;
        } else {
            if (self.freq <= 2) {
                var values: std.ArrayListUnmanaged(u32) = .empty;
                errdefer values.deinit(a);
                try values.ensureTotalCapacity(a, total);
                values.appendSliceAssumeCapacity(self.positions.small[0..self.freq]);
                self.positions = .{ .heap = values };
            } else try self.positions.heap.ensureTotalCapacity(a, total);
            for (positions) |position| self.positions.heap.appendAssumeCapacity(base + position);
        }
        self.freq = total;
    }
    fn appendEncodedShifted(self: *@This(), a: Allocator, hit: contribution_cache_mod.EncodedHit, base: u32) !void {
        const total = try std.math.add(u32, self.freq, hit.count);
        _ = try std.math.add(u32, base, hit.maximum_position);
        if (total > 2) {
            if (self.freq <= 2) {
                var values: std.ArrayListUnmanaged(u32) = .empty;
                errdefer values.deinit(a);
                try values.ensureTotalCapacity(a, total);
                values.appendSliceAssumeCapacity(self.positions.small[0..self.freq]);
                self.positions = .{ .heap = values };
            } else try self.positions.heap.ensureTotalCapacity(a, total);
        }
        var iterator = hit.iterator();
        var index = self.freq;
        var position: u32 = undefined;
        while (iterator.read(&position)) {
            if (total <= 2) self.positions.small[index] = base + position else self.positions.heap.appendAssumeCapacity(base + position);
            index += 1;
        }
        self.freq = total;
    }
    fn positionSlice(self: *const @This()) []const u32 {
        return if (self.freq <= 2) self.positions.small[0..self.freq] else self.positions.heap.items;
    }
    fn deinit(self: *@This(), a: Allocator) void {
        if (self.freq > 2) self.positions.heap.deinit(a);
    }
};

const FieldAcc = struct {
    scratch: Allocator,
    token_offset: u32 = 0,
    term_chunk: []u8 = &.{},
    term_used: usize = 0,
    term_map: std.StringHashMapUnmanaged(TermAcc) = .empty,
    fn init(scratch: Allocator) FieldAcc {
        return .{ .scratch = scratch };
    }
    // Keys have document lifetime; pack short borrowed terms into arena slabs
    // instead of invoking the arena allocator for every distinct suffix.
    inline fn intern(self: *@This(), a: Allocator, term: []const u8) ![]const u8 {
        if (self.term_chunk.len - self.term_used < term.len) {
            self.term_chunk = try a.alloc(u8, @max(4096, term.len));
            self.term_used = 0;
        }
        const owned = self.term_chunk[self.term_used..][0..term.len];
        @memcpy(owned, term);
        self.term_used += term.len;
        return owned;
    }
    fn deinit(self: *@This()) void {
        var values = self.term_map.valueIterator();
        while (values.next()) |value| value.deinit(self.scratch);
        self.term_map.deinit(self.scratch);
        self.* = undefined;
    }
};

// Sorting indices bounds discovery at O(fields log fields). The original index
// breaks ties so repeated values keep their token offsets and field norms.
fn orderTextFields(alloc: Allocator, order: *std.ArrayListUnmanaged(usize), fields: []const TextField) !bool {
    try order.resize(alloc, fields.len);
    for (order.items, 0..) |*index, i| index.* = i;
    std.sort.pdq(usize, order.items, fields, struct {
        fn lessThan(context: []const TextField, a: usize, b: usize) bool {
            return switch (std.mem.order(u8, context[a].field_name, context[b].field_name)) {
                .lt => true,
                .gt => false,
                .eq => a < b,
            };
        }
    }.lessThan);
    for (order.items[0..order.items.len -| 1], order.items[@min(1, order.items.len)..]) |a, b| {
        if (std.mem.eql(u8, fields[a].field_name, fields[b].field_name)) return true;
    }
    return false;
}

const TextIndexSortEntry = struct {
    doc_index: usize,
    keys: []TextIndexSortValue,

    pub fn deinit(self: *TextIndexSortEntry, alloc: Allocator) void {
        for (self.keys) |*key| key.deinit(alloc);
        alloc.free(self.keys);
        self.* = undefined;
    }
};

const TextIndexSortValue = union(enum) {
    null_value,
    datetime_ns: i128,
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
    bool_val: bool,
    bytes_val: []u8,
    id: []const u8,
    numeric_val: typed_dv.NumericValue,

    pub fn deinit(self: *TextIndexSortValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes_val => |bytes| alloc.free(bytes),
            else => {},
        }
        self.* = undefined;
    }
};

const TextIndexSortValueTag = std.meta.Tag(TextIndexSortValue);

fn textIndexSortEntryLessThan(index_sort: []const segment_mod.SegmentIndexSortField, a: TextIndexSortEntry, b: TextIndexSortEntry) bool {
    for (index_sort, 0..) |field, i| {
        const order = compareTextIndexSortValues(a.keys[i], b.keys[i]);
        if (order == .eq) continue;
        return if (field.desc) order == .gt else order == .lt;
    }
    return a.doc_index < b.doc_index;
}

/// Prove existing physical order without changing document/source ordinals.
/// Only two tuples are resident. Missing/null or incompatible scalar values
/// conservatively leave the segment unordered; ordinary encoding still runs.
pub fn textDocumentsAreIndexSorted(alloc: Allocator, docs: []const TextDocument, analysis: TextAnalysisConfig, fields: []const segment_mod.SegmentIndexSortField) !bool {
    if (fields.len == 0 or docs.len == 0) return false;
    var regions = [_]std.heap.ArenaAllocator{ .init(alloc), .init(alloc) };
    defer for (&regions) |*region| region.deinit();
    const domains = try alloc.alloc(?TextIndexSortValueTag, fields.len);
    defer alloc.free(domains);
    @memset(domains, null);
    var previous: ?TextIndexSortEntry = null;
    for (docs, 0..) |doc, index| {
        const region = &regions[index % 2];
        _ = region.reset(.retain_capacity);
        const scratch = region.allocator();
        const next: TextIndexSortEntry = .{ .doc_index = index, .keys = try scratch.alloc(TextIndexSortValue, fields.len) };
        for (fields, next.keys) |field, *key| {
            key.* = textDocumentSortValueAlloc(scratch, doc, field.field, analysis) catch |err| switch (err) {
                error.InvalidSegment, error.UnsupportedTypedDocValues => {
                    return false;
                },
                else => return err,
            };
        }
        for (next.keys, domains) |key, *domain| {
            if (key == .null_value) continue;
            const tag = std.meta.activeTag(key);
            if (domain.*) |held| {
                if (held != tag) return false;
            } else domain.* = tag;
        }
        if (previous) |entry| {
            for (entry.keys, next.keys) |left, right| if (left != .null_value and right != .null_value and std.meta.activeTag(left) != std.meta.activeTag(right)) {
                return false;
            };
            if (textIndexSortEntryLessThan(fields, next, entry)) {
                return false;
            }
        }
        previous = next;
    }
    return true;
}

fn buildTextDocumentOrderAlloc(
    alloc: Allocator,
    docs: []const TextDocument,
    text_analysis: TextAnalysisConfig,
    index_sort: []const segment_mod.SegmentIndexSortField,
) ![]TextIndexSortEntry {
    const order = try alloc.alloc(TextIndexSortEntry, docs.len);
    var initialized: usize = 0;
    errdefer {
        for (order[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(order);
    }
    const expected_key_tags = try alloc.alloc(?TextIndexSortValueTag, index_sort.len);
    defer alloc.free(expected_key_tags);
    @memset(expected_key_tags, null);
    for (order, 0..) |*entry, i| {
        entry.* = .{
            .doc_index = i,
            .keys = try alloc.alloc(TextIndexSortValue, index_sort.len),
        };
        var keys_initialized: usize = 0;
        errdefer {
            for (entry.keys[0..keys_initialized]) |*key| key.deinit(alloc);
            alloc.free(entry.keys);
        }
        for (index_sort) |field| {
            var key = try textDocumentSortValueAlloc(alloc, docs[i], field.field, text_analysis);
            errdefer key.deinit(alloc);
            const key_tag = std.meta.activeTag(key);
            if (key != .null_value) {
                if (expected_key_tags[keys_initialized]) |expected| {
                    if (expected != key_tag) return error.InvalidSegment;
                } else expected_key_tags[keys_initialized] = key_tag;
            }
            entry.keys[keys_initialized] = key;
            keys_initialized += 1;
        }
        initialized += 1;
    }
    std.sort.pdq(TextIndexSortEntry, order, index_sort, textIndexSortEntryLessThan);
    return order;
}

/// Sort only bounded navigation keys; payloads retain their capture ownership.
pub fn sortedTextDocumentIndicesAlloc(a: Allocator, docs: []const TextDocument, fields: []const segment_mod.SegmentIndexSortField) ![]u32 {
    const order = try buildTextDocumentOrderAlloc(a, docs, .{}, fields);
    defer freeTextDocumentOrder(a, order);
    const indices = try a.alloc(u32, order.len);
    for (order, indices) |entry, *index| index.* = @intCast(entry.doc_index);
    return indices;
}

fn freeTextDocumentOrder(alloc: Allocator, order: []TextIndexSortEntry) void {
    for (order) |*entry| entry.deinit(alloc);
    if (order.len > 0) alloc.free(order);
}

fn textIndexSortBoundsAlloc(
    alloc: Allocator,
    first: []const TextIndexSortValue,
    last: []const TextIndexSortValue,
) !segment_mod.SegmentIndexSortBounds {
    if (first.len == 0 or first.len != last.len) return error.InvalidSegment;
    const first_bounds = try textIndexSortBoundValuesAlloc(alloc, first);
    errdefer {
        for (first_bounds) |*value| value.deinit(alloc);
        alloc.free(first_bounds);
    }
    return .{
        .first = first_bounds,
        .last = try textIndexSortBoundValuesAlloc(alloc, last),
    };
}

fn textIndexSortBoundValuesAlloc(
    alloc: Allocator,
    values: []const TextIndexSortValue,
) ![]segment_mod.SegmentIndexSortBoundValue {
    const out = try alloc.alloc(segment_mod.SegmentIndexSortBoundValue, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*value| value.deinit(alloc);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try textIndexSortBoundValueAlloc(alloc, value);
        initialized += 1;
    }
    return out;
}

fn textIndexSortBoundValueAlloc(alloc: Allocator, value: TextIndexSortValue) !segment_mod.SegmentIndexSortBoundValue {
    return switch (value) {
        .null_value => .null_value,
        .datetime_ns => |v| .{ .datetime_ns = v },
        .u64_val => |v| .{ .u64_val = v },
        .i64_val => |v| .{ .i64_val = v },
        .f64_val => |v| if (std.math.isFinite(v)) .{ .f64_val = v } else error.InvalidSegment,
        .bool_val => |v| .{ .bool_val = v },
        .bytes_val => |v| .{ .bytes_val = try alloc.dupe(u8, v) },
        .id => |v| .{ .id = try alloc.dupe(u8, v) },
        .numeric_val => |v| switch (v) {
            .u64_val => |number| .{ .u64_val = number },
            .i64_val => |number| .{ .i64_val = number },
            .f64_val => |number| .{ .f64_val = number },
        },
    };
}

fn textDocumentSortValueAlloc(
    alloc: Allocator,
    doc: TextDocument,
    field: []const u8,
    text_analysis: TextAnalysisConfig,
) !TextIndexSortValue {
    if (!doc.index_sort_projection_valid) return error.UnsupportedTypedDocValues;
    if (std.mem.eql(u8, field, "_id")) return .{ .id = doc.id };
    for (doc.index_sort_values) |key| if (std.mem.eql(u8, key.field_name, field)) return switch (key.value) {
        .null_value => .null_value,
        .bytes_val => |v| .{ .bytes_val = try alloc.dupe(u8, v) },
        .id => |v| .{ .id = v },
        inline .u64_val, .i64_val, .f64_val => |v, tag| .{ .numeric_val = @unionInit(typed_dv.NumericValue, @tagName(tag), v) },
        inline else => |v, tag| @unionInit(TextIndexSortValue, @tagName(tag), v),
    };
    if (doc.typed_fields) |typed_fields| {
        var found: ?typed_dv.TypedValue = null;
        for (typed_fields) |typed_field| {
            if (!std.mem.eql(u8, typed_field.field_name, field)) continue;
            if (typed_field.conflicted) return error.UnsupportedTypedDocValues;
            if (found != null) return error.InvalidSegment;
            found = typed_field.value;
        }
        if (found) |value| return try textIndexSortValueFromTypedValueAlloc(alloc, value);
        // Explicit projections must not attest omitted non-null source values
        // as null coverage. The encoder consumes this same typed projection.
        if (doc.typed_source orelse doc.stored_source) |source| {
            if (jsonPathValue(source, field)) |value| if (value != .null) return error.UnsupportedTypedDocValues;
            return .null_value;
        }
        if (doc.stored_data.len != 0) {
            var source = try std.json.parseFromSlice(std.json.Value, alloc, doc.stored_data, .{});
            defer source.deinit();
            if (jsonPathValue(source.value, field)) |value| if (value != .null) return error.UnsupportedTypedDocValues;
        }
        return .null_value;
    }
    if (doc.typed_source) |source| {
        if (jsonPathValue(source, field)) |value| {
            return try textIndexSortValueFromJsonAlloc(alloc, field, value, text_analysis);
        }
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc.stored_data, .{});
    defer parsed.deinit();
    const value = jsonPathValue(parsed.value, field) orelse return .null_value;
    return try textIndexSortValueFromJsonAlloc(alloc, field, value, text_analysis);
}

fn textIndexSortValueFromJsonAlloc(
    alloc: Allocator,
    field: []const u8,
    value: std.json.Value,
    text_analysis: TextAnalysisConfig,
) !TextIndexSortValue {
    if (value == .null) return .null_value;
    const detected = detectTypedValue(field, value, text_analysis) orelse return error.InvalidSegment;
    return try textIndexSortValueFromTypedValueAlloc(alloc, detected.value);
}

fn textIndexSortValueFromTypedValueAlloc(alloc: Allocator, value: typed_dv.TypedValue) !TextIndexSortValue {
    return switch (value) {
        .datetime_ns => |v| .{ .datetime_ns = v },
        .u64_val => |v| .{ .u64_val = v },
        .i64_val => |v| .{ .i64_val = v },
        .f64_val => |v| if (std.math.isFinite(v)) .{ .f64_val = v } else error.InvalidSegment,
        .bool_val => |v| .{ .bool_val = v },
        .bytes_val => |v| .{ .bytes_val = try alloc.dupe(u8, v) },
        .geo_point => error.UnsupportedTypedDocValues,
        .numeric_val => |v| .{ .numeric_val = v },
    };
}

fn jsonPathValue(value: std.json.Value, path: []const u8) ?std.json.Value {
    var current = value;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn compareTextIndexSortValues(a: TextIndexSortValue, b: TextIndexSortValue) std.math.Order {
    if (b == .null_value) return if (a == .null_value) .eq else .gt;
    return switch (a) {
        .null_value => .lt,
        .datetime_ns => |av| if (b == .datetime_ns) std.math.order(av, b.datetime_ns) else .lt,
        .u64_val => |av| switch (b) {
            .u64_val => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .i64_val => |av| switch (b) {
            .i64_val => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .f64_val => |av| switch (b) {
            .f64_val => |bv| compareSortF64(av, bv),
            else => .lt,
        },
        .bool_val => |av| switch (b) {
            .bool_val => |bv| std.math.order(@intFromBool(av), @intFromBool(bv)),
            else => .lt,
        },
        .bytes_val => |av| switch (b) {
            .bytes_val => |bv| std.mem.order(u8, av, bv),
            else => .lt,
        },
        .id => |av| switch (b) {
            .id => |bv| std.mem.order(u8, av, bv),
            else => .lt,
        },
        .numeric_val => |av| switch (b) {
            .numeric_val => |bv| typed_dv.compareNumericValues(av, bv),
            else => .lt,
        },
    };
}

fn compareSortF64(a: f64, b: f64) std.math.Order {
    const a_nan = std.math.isNan(a);
    const b_nan = std.math.isNan(b);
    if (a_nan and b_nan) return .eq;
    if (a_nan) return .gt;
    if (b_nan) return .lt;
    return std.math.order(a, b);
}

/// Resolve the analyzer used to index this exact field contribution. `_all`
/// can receive values with different schema analyzers, so preserve the
/// contribution's analyzer unless the index explicitly overrides this field.
pub fn effectiveTextFieldAnalyzer(field: TextField, text_analysis: TextAnalysisConfig) *const analysis_mod.Analyzer {
    return resolveFieldAnalyzer(field.field_name, text_analysis) orelse field.analyzer orelse &analysis_mod.default_analyzer;
}

fn cachedFieldAnalyzer(
    alloc: Allocator,
    cache: *std.StringHashMapUnmanaged(?*const analysis_mod.Analyzer),
    field: TextField,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
) !*const analysis_mod.Analyzer {
    const gop = try cache.getOrPut(alloc, field.field_name);
    if (!gop.found_existing) {
        gop.key_ptr.* = field.field_name;
        gop.value_ptr.* = resolveFieldAnalyzer(field.field_name, text_analysis);
    }
    return gop.value_ptr.* orelse field.analyzer orelse default_analyzer;
}

fn addSingleTextFieldToBuilders(
    alloc: Allocator,
    doc_alloc: Allocator,
    field_builders: *FieldPostingsBuilders,
    field_indices: *std.StringHashMapUnmanaged(u16),
    seg_writer: *segment_mod.SegmentWriter,
    analyzer_cache: *std.StringHashMapUnmanaged(?*const analysis_mod.Analyzer),
    doc_idx: u32,
    field: TextField,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
    profile: ?*BuildTextProfile,
    profile_timings: bool,
    scratch: *TextBuildScratch,
    source_capture: *SourceCapture,
    source_root: ?std.json.Value,
    reuse: *AnalysisReuse,
    contribution_cache: ?*AnalyzedContributionCache,
    contribution_uses: *const ContributionUses,
) !void {
    if (profile) |p| p.text_field_count +|= 1;
    const analyzer = try cachedFieldAnalyzer(alloc, analyzer_cache, field, default_analyzer, text_analysis);
    const analyzer_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    const capture = try source_capture.wants(doc_idx, field.source_field orelse field.field_name, field.text, source_root, analyzer);
    var streaming = if (capture != null and field.text.len >= SourceCapture.stream_min_bytes and !reuse.repeated(.{ .field = field.source_field orelse field.field_name, .text = field.text, .signature = capture.?.signature })) analyzer.sourceStream(alloc, field.text) else null;
    defer if (streaming) |*stream| stream.deinit();
    const producer = if (capture == null and field.text.len >= 128) analyzer.tokenStream(field.text) else null;
    var shared: ?AnalysisReuse.Lease = null;
    if (streaming == null and producer == null and reuse.entries.count() != 0) shared = try reuse.acquire(.{ .field = field.source_field orelse field.field_name, .text = field.text, .signature = try source_capture.signatureFor(analyzer) }, analyzer, capture != null);
    defer if (shared) |held| held.release();
    var field_acc = FieldAcc.init(alloc);
    defer field_acc.deinit();
    // Compiled contributions contain positions, but not original source spans.
    // Source capture and document-local token leases must finish before this
    // optional cross-document acceleration can replace analysis.
    if (streaming) |*stream| {
        const Consumer = struct {
            a: Allocator,
            field: *FieldAcc,
            fn accept(raw: *anyopaque, token: analysis_mod.Token) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try accumulateToken(self.a, self.field, token, 0, true);
            }
        };
        var consumer: Consumer = .{ .a = doc_alloc, .field = &field_acc };
        const count = try source_capture.recordStream(capture.?, stream, .{ .ptr = &consumer, .accept = Consumer.accept });
        field_acc.token_offset = count;
        if (profile) |p| {
            p.token_count +|= count;
            if (profile_timings) p.analyzer_ns +|= platform_time.monotonicNs() - analyzer_start_ns;
        }
    } else if (capture == null and shared == null and try accumulateCachedContribution(doc_alloc, &field_acc, analyzer, field.text, profile, contribution_cache, repeatedContribution(contribution_uses, field, analyzer))) {
        // The cached term contribution already has exact norms and positions.
    } else if (producer) |stream| {
        const started = if (profile_timings) platform_time.monotonicNs() else 0;
        const count = try accumulateTokenStream(doc_alloc, &field_acc, stream);
        if (profile) |p| {
            p.token_count +|= count;
            if (profile_timings) p.term_accum_ns +|= platform_time.monotonicNs() - started;
        }
    } else {
        const tokens = if (shared) |held| held.tokens else if (capture != null) try analyzer.analyzeWithSourceOffsets(doc_alloc, field.text) else try analyzer.analyze(doc_alloc, field.text);
        if (capture) |request| try source_capture.record(request, tokens);
        if (profile) |p| {
            if (profile_timings) p.analyzer_ns +|= platform_time.monotonicNs() - analyzer_start_ns;
            p.token_count +|= @intCast(tokens.len);
        }
        if (tokens.len == 0) return;
        const term_accum_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        if (tokens.len <= 64 and tokenTermsAreUnique(tokens)) {
            if (profile_timings) {
                if (profile) |p| p.term_accum_ns +|= platform_time.monotonicNs() - term_accum_start_ns;
            }

            const hit_materialize_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
            scratch.positions.clearRetainingCapacity();
            scratch.hits.clearRetainingCapacity();
            try scratch.positions.ensureTotalCapacity(alloc, tokens.len);
            try scratch.hits.ensureTotalCapacity(alloc, tokens.len);
            const norm: u32 = @intCast(tokens.len);
            for (tokens) |tok| {
                scratch.positions.appendAssumeCapacity(tok.position);
                const pos_index = scratch.positions.items.len - 1;
                scratch.hits.appendAssumeCapacity(.{
                    .term = tok.term,
                    .freq = 1,
                    .norm = norm,
                    .positions = scratch.positions.items[pos_index .. pos_index + 1],
                });
            }
            if (profile) |p| {
                p.term_hit_count +|= @intCast(scratch.hits.items.len);
                if (profile_timings) p.hit_materialize_ns +|= platform_time.monotonicNs() - hit_materialize_start_ns;
            }
            try addFieldHitsToBuilder(alloc, field_builders, field_indices, seg_writer, doc_idx, field.field_name, scratch.hits.items);
            return;
        }

        for (tokens) |tok| try accumulateToken(doc_alloc, &field_acc, tok, 0, false);
        field_acc.token_offset = @intCast(tokens.len);
        if (profile_timings) {
            if (profile) |p| p.term_accum_ns +|= platform_time.monotonicNs() - term_accum_start_ns;
        }
    }

    const hit_materialize_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    scratch.hits.clearRetainingCapacity();
    var it = field_acc.term_map.iterator();
    while (it.next()) |entry| {
        try scratch.hits.append(alloc, .{
            .term = entry.key_ptr.*,
            .freq = entry.value_ptr.freq,
            .norm = field_acc.token_offset,
            .positions = entry.value_ptr.positionSlice(),
        });
        if (profile) |p| p.term_hit_count +|= 1;
    }
    if (profile_timings) {
        if (profile) |p| p.hit_materialize_ns +|= platform_time.monotonicNs() - hit_materialize_start_ns;
    }
    if (scratch.hits.items.len == 0) return;

    try addFieldHitsToBuilder(alloc, field_builders, field_indices, seg_writer, doc_idx, field.field_name, scratch.hits.items);
}

// Stream terms are ephemeral; intern only first occurrences into the document
// arena. Positions and field norms remain identical to owned-token analysis.
inline fn accumulateToken(a: Allocator, field: *FieldAcc, token: analysis_mod.Token, base: u32, borrowed: bool) !void {
    const slot = try field.term_map.getOrPut(field.scratch, token.term);
    if (!slot.found_existing) {
        // Initialize before allocating so caller cleanup remains valid on OOM.
        slot.value_ptr.* = .{};
        slot.key_ptr.* = if (borrowed) field.intern(a, token.term) catch |err| {
            _ = field.term_map.remove(token.term);
            return err;
        } else token.term;
    }
    try slot.value_ptr.push(field.scratch, try std.math.add(u32, base, token.position));
}
fn accumulateTokenStream(a: Allocator, field: *FieldAcc, input: analysis_mod.TokenStream) !u32 {
    var stream = input;
    var count: u32 = 0;
    const base = field.token_offset;
    while (stream.next()) |token| {
        try accumulateToken(a, field, token, base, true);
        count = try std.math.add(u32, count, 1);
    }
    field.token_offset = try std.math.add(u32, base, count);
    return count;
}
fn accumulateAnalyzedField(a: Allocator, scratch: Allocator, fields: *std.StringHashMapUnmanaged(FieldAcc), name: []const u8, analyzer: *const analysis_mod.Analyzer, text: []const u8, profile: ?*BuildTextProfile, timings: bool, source_capture: *SourceCapture, doc: u32, source_field: []const u8, source_root: ?std.json.Value, reuse: *AnalysisReuse, contribution_cache: ?*AnalyzedContributionCache, known_reuse: bool) !void {
    const analysis_started = if (timings) platform_time.monotonicNs() else 0;
    const capture = try source_capture.wants(doc, source_field, text, source_root, analyzer);
    var streaming = if (capture != null and text.len >= SourceCapture.stream_min_bytes and !reuse.repeated(.{ .field = source_field, .text = text, .signature = capture.?.signature })) analyzer.sourceStream(scratch, text) else null;
    defer if (streaming) |*stream| stream.deinit();
    if (streaming) |*stream| {
        const Consumer = struct {
            a: Allocator,
            scratch: Allocator,
            fields: *std.StringHashMapUnmanaged(FieldAcc),
            name: []const u8,
            field: ?*FieldAcc = null,
            base: u32 = 0,
            fn accept(raw: *anyopaque, token: analysis_mod.Token) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.field == null) {
                    const slot = try self.fields.getOrPut(self.a, self.name);
                    if (!slot.found_existing) {
                        slot.key_ptr.* = self.name;
                        slot.value_ptr.* = .init(self.scratch);
                    }
                    self.field = slot.value_ptr;
                    self.base = slot.value_ptr.token_offset;
                }
                try accumulateToken(self.a, self.field.?, token, self.base, true);
            }
        };
        var consumer: Consumer = .{ .a = a, .scratch = scratch, .fields = fields, .name = name };
        const count = try source_capture.recordStream(capture.?, stream, .{ .ptr = &consumer, .accept = Consumer.accept });
        if (consumer.field) |field| field.token_offset = try std.math.add(u32, consumer.base, count);
        if (profile) |p| {
            p.token_count +|= count;
            if (timings) p.analyzer_ns +|= platform_time.monotonicNs() - analysis_started;
        }
        return;
    }
    const producer = if (capture == null and text.len >= 128) analyzer.tokenStream(text) else null;
    const shared = if (producer == null and reuse.entries.count() != 0) try reuse.acquire(.{ .field = source_field, .text = text, .signature = try source_capture.signatureFor(analyzer) }, analyzer, capture != null) else null;
    defer if (shared) |held| held.release();
    if (capture == null and shared == null and contribution_cache != null and text.len >= contribution_cache_mod.minimum_input) {
        const existing = fields.getPtr(name);
        var fresh = FieldAcc.init(scratch);
        var transferred = false;
        defer if (existing == null and !transferred) fresh.deinit();
        if (try accumulateCachedContribution(a, existing orelse &fresh, analyzer, text, profile, contribution_cache, known_reuse)) {
            if (existing == null and fresh.token_offset != 0) {
                try fields.put(a, name, fresh);
                transferred = true;
            }
            return;
        }
    }
    if (producer) |stream| {
        const started = if (timings) platform_time.monotonicNs() else 0;
        const existing = fields.getPtr(name);
        var fresh = FieldAcc.init(scratch);
        var transferred = false;
        defer if (existing == null and !transferred) fresh.deinit();
        const count = try accumulateTokenStream(a, existing orelse &fresh, stream);
        // Empty fields must not populate a per-document dictionary. Normal
        // analyzers retain their analyze-before-admission fast path below.
        if (existing == null and count != 0) {
            try fields.put(a, name, fresh);
            transferred = true;
        }
        if (profile) |p| {
            p.token_count +|= count;
            if (timings) p.term_accum_ns +|= platform_time.monotonicNs() - started;
        }
        return;
    }
    const tokens = if (shared) |held| held.tokens else if (capture != null) try analyzer.analyzeWithSourceOffsets(a, text) else try analyzer.analyze(a, text);
    if (capture) |request| try source_capture.record(request, tokens);
    if (profile) |p| {
        p.token_count +|= @intCast(tokens.len);
        if (timings) p.analyzer_ns +|= platform_time.monotonicNs() - analysis_started;
    }
    if (tokens.len == 0) return;
    const accum_started = if (timings) platform_time.monotonicNs() else 0;
    const slot = try fields.getOrPut(a, name);
    if (!slot.found_existing) {
        slot.key_ptr.* = name;
        slot.value_ptr.* = .init(scratch);
    }
    const field = slot.value_ptr;
    const base = field.token_offset;
    for (tokens) |token| try accumulateToken(a, field, token, base, shared != null);
    field.token_offset = try std.math.add(u32, base, @intCast(tokens.len));
    if (timings) {
        if (profile) |p| p.term_accum_ns +|= platform_time.monotonicNs() - accum_started;
    }
}

// Merge a compiled contribution once per distinct term, rather than looking
// up and allocating every token again. Copied keys have the document arena's
// lifetime; the cache lease ends before the postings builder is invoked.
fn mergeContribution(a: Allocator, target: *FieldAcc, source: *const FieldAcc) !void {
    const next = try std.math.add(u32, target.token_offset, source.token_offset);
    var entries = source.term_map.iterator();
    while (entries.next()) |entry| {
        const term = entry.key_ptr.*;
        const slot = try target.term_map.getOrPut(target.scratch, term);
        if (!slot.found_existing) {
            slot.value_ptr.* = .{};
            slot.key_ptr.* = target.intern(a, term) catch |err| {
                _ = target.term_map.remove(term);
                return err;
            };
        }
        try slot.value_ptr.appendShifted(target.scratch, entry.value_ptr.positionSlice(), target.token_offset);
    }
    target.token_offset = next;
}

fn mergeEncodedContribution(a: Allocator, target: *FieldAcc, lease: *const AnalyzedContributionCache.Lease, count: u32) !void {
    const next = try std.math.add(u32, target.token_offset, count);
    for (0..lease.hitCount()) |index| {
        const hit = lease.hitAt(index);
        const slot = try target.term_map.getOrPut(target.scratch, hit.term);
        if (!slot.found_existing) {
            slot.value_ptr.* = .{};
            slot.key_ptr.* = target.intern(a, hit.term) catch |err| {
                _ = target.term_map.remove(hit.term);
                return err;
            };
        }
        try slot.value_ptr.appendEncodedShifted(target.scratch, hit, target.token_offset);
    }
    target.token_offset = next;
}

fn accumulateCachedContribution(a: Allocator, target: *FieldAcc, analyzer: *const analysis_mod.Analyzer, text: []const u8, profile: ?*BuildTextProfile, optional_cache: ?*AnalyzedContributionCache, known_reuse: bool) !bool {
    const cache = optional_cache orelse return false;
    if (text.len < contribution_cache_mod.minimum_input) return false;
    const digest = try AnalyzedContributionCache.key(analyzer, text);
    if (cache.borrow(digest)) |borrowed| {
        var lease = borrowed;
        defer lease.deinit();
        try mergeEncodedContribution(a, target, &lease, lease.tokens);
        if (profile) |p| {
            p.contribution_cache_hits +|= 1;
            p.contribution_tokens_reused +|= lease.tokens;
            p.token_count +|= lease.tokens;
        }
        return true;
    }
    if (!cache.observe(digest, known_reuse)) return false;
    // Build a contribution independent of the aggregate's token offset. This
    // permits the same body to participate in body/_all and reordered batches.
    var local = FieldAcc.init(target.scratch);
    defer local.deinit();
    if (analyzer.tokenStream(text)) |stream| {
        _ = try accumulateTokenStream(a, &local, stream);
    } else {
        const tokens = try analyzer.analyze(a, text);
        for (tokens) |token| try accumulateToken(a, &local, token, 0, false);
        local.token_offset = std.math.cast(u32, tokens.len) orelse return error.Overflow;
    }
    if (local.term_map.count() >= AnalyzedContributionCache.maximumTerms(text.len)) {
        // Reject obvious high-entropy dictionaries before allocating a second
        // hit vector. Their ordinary compiler state is already the fallback.
        cache.reject(digest);
    } else {
        var hits: std.ArrayListUnmanaged(contribution_cache_mod.Hit) = .empty;
        defer hits.deinit(target.scratch);
        try hits.ensureTotalCapacity(target.scratch, local.term_map.count());
        var it = local.term_map.iterator();
        while (it.next()) |entry| hits.appendAssumeCapacity(.{ .term = entry.key_ptr.*, .positions = entry.value_ptr.positionSlice() });
        cache.put(digest, hits.items, local.token_offset, text.len);
    }
    const tokens = local.token_offset;
    if (target.term_map.count() == 0 and target.token_offset == 0) {
        target.deinit();
        target.* = local;
        local = FieldAcc.init(target.scratch);
    } else try mergeContribution(a, target, &local);
    if (profile) |p| p.token_count +|= tokens;
    return true;
}

fn tokenTermsAreUnique(tokens: []const analysis_mod.Token) bool {
    for (tokens, 0..) |token, i| {
        for (tokens[0..i]) |prev| {
            if (std.mem.eql(u8, token.term, prev.term)) return false;
        }
    }
    return true;
}

fn addFieldHitsToBuilder(
    alloc: Allocator,
    field_builders: *FieldPostingsBuilders,
    field_indices: *std.StringHashMapUnmanaged(u16),
    seg_writer: *segment_mod.SegmentWriter,
    doc_idx: u32,
    field_name: []const u8,
    hits: []const inverted.InvertedIndexBuilder.TermHit,
) !void {
    const builder = try ensureFieldPostingsBuilder(alloc, field_builders, field_name);
    try builder.addDocument(doc_idx, hits);

    const field_index_gop = try field_indices.getOrPut(alloc, field_name);
    if (!field_index_gop.found_existing) {
        field_index_gop.key_ptr.* = field_name;
        field_index_gop.value_ptr.* = try seg_writer.addField(field_name);
    }
}

// Producers are borrowed by SegmentWriter only for this synchronous build.
// Each producer releases its collector after writing, so serialized payloads
// never accumulate alongside all of the raw collectors.
const InitialSectionBuildState = struct {
    tracker: *TextBuildResourceTracker,
    output: *segment_mod.SegmentSink,
    profile: ?*BuildTextProfile,
    profile_timings: bool,
    profile_working_set: bool,
    input_bytes: u64,
    postings_bytes: u64,
    typed_bytes: u64,
    section_bytes: u64 = 0,

    fn adjust(self: *@This(), extra: u64) !void {
        try self.tracker.adjust(self.input_bytes + self.postings_bytes + self.typed_bytes + extra + @as(u64, @intCast(self.output.residentBytes())));
    }
};

const SourceColumnProducer = struct {
    state: *InitialSectionBuildState,
    docs: []const TextDocument,
    order: []const TextIndexSortEntry,
    field: []const u8,
    capture: *SourceCapture,
    fn write(a: Allocator, raw: *anyopaque, sink: *segment_mod.SegmentSink) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var writer = try @import("section/source_column.zig").Writer.init(a, sink);
        defer writer.deinit();
        for (0..self.docs.len) |i| {
            const original = if (self.order.len != 0) self.order[i].doc_index else i;
            const doc = self.docs[original];
            const value = doc.stored_source.?.object.get(self.field) orelse continue;
            // Input rows remain leased; one encoded value and one raw/compressed
            // field chunk coexist. Reserve that scratch under the build owner.
            try self.state.adjust(writer.retained + 3 *| (@max(@as(u64, @intCast(doc.stored_data.len)), doc.stored_source_bytes) +| 64 * 1024));
            try writer.addPrepared(@intCast(i), value, try self.capture.variants(@intCast(i), self.field));
            try self.state.adjust(writer.retained + 3 *| (@max(@as(u64, @intCast(doc.stored_data.len)), doc.stored_source_bytes) +| 64 * 1024));
        }
        if (!try writer.finish()) return error.InvalidSegment;
        try self.state.adjust(0);
    }
};

const InitialSectionProducer = struct {
    state: *InitialSectionBuildState,
    source: union(enum) { postings: *FieldPostingsBuilder, typed: *TypedFieldCollector },
    doc_count: u32 = 0,

    fn write(alloc: Allocator, context: *anyopaque, sink: *segment_mod.SegmentSink) !void {
        const self: *@This() = @ptrCast(@alignCast(context));
        const state = self.state;
        const start = sink.len();
        const start_ns = if (state.profile_timings) platform_time.monotonicNs() else 0;
        switch (self.source) {
            .postings => |builder| {
                const estimated_bytes = builder.estimatedMemoryBytes();
                try state.adjust(0);
                if (builder.runs.items.len == 0) {
                    var inverted_profile = inverted.InvertedIndexBuildProfile{};
                    try builder.writeUnspilled(alloc, sink, self.doc_count, if (state.profile_timings) &inverted_profile else null);
                    if (state.profile_timings) if (state.profile) |p| {
                        p.inverted_sort_ns +|= inverted_profile.sort_ns;
                        p.inverted_postings_serialize_ns +|= inverted_profile.postings_serialize_ns;
                        p.inverted_term_dict_ns +|= inverted_profile.term_dict_ns;
                        p.inverted_norms_ns +|= inverted_profile.norms_ns;
                        p.inverted_bloom_finish_ns +|= inverted_profile.bloom_finish_ns;
                        p.inverted_final_assembly_ns +|= inverted_profile.final_assembly_ns;
                    };
                } else {
                    if (state.profile) |p| p.postings_run_max_fan_in = @max(p.postings_run_max_fan_in, builder.runs.items.len);
                    try builder.mergeRuns(alloc, sink, self.doc_count);
                }
                builder.deinit(alloc);
                state.postings_bytes -|= estimated_bytes;
                if (state.profile_timings) if (state.profile) |p| {
                    p.inverted_build_ns +|= platform_time.monotonicNs() - start_ns;
                };
            },
            .typed => |collector| {
                const values = &collector.writer.?;
                const estimated_bytes = values.estimatedMemoryBytes();
                if (collector.staged.items.len > 0) {
                    const views = try alloc.alloc(@import("segment_source.zig").View, collector.staged.items.len);
                    defer alloc.free(views);
                    for (collector.staged.items, views) |run, *view| view.* = try run.view();
                    try typed_dv.concatenateStreams(alloc, sink, values.value_type, views);
                } else {
                    // The streamed encoder owns one byte-bounded chunk. One large
                    // value is allowed, and its temporary bytes are admitted too.
                    var scratch_bound: u64 = 1024 * 1024;
                    for (values.entries.items) |entry| {
                        if (entry.value == .bytes_val) scratch_bound = @max(scratch_bound, @as(u64, @intCast(entry.value.bytes_val.len)) *| 4 +| 1024 * 1024);
                    }
                    scratch_bound +|= @as(u64, @intCast(values.entries.items.len)) *| 8;
                    try state.adjust(scratch_bound);
                    var writer = typed_dv.StreamingWriter.init(alloc, sink, values.value_type);
                    defer writer.deinit();
                    for (values.entries.items) |entry| try writer.add(entry.doc_id, entry.value);
                    if (!try writer.finish()) return error.InvalidSegment;
                }
                collector.deinit(alloc);
                state.typed_bytes -|= estimated_bytes;
                if (state.profile_timings) if (state.profile) |p| {
                    p.typed_build_ns +|= platform_time.monotonicNs() - start_ns;
                };
            },
        }
        state.section_bytes +|= @intCast(sink.len() - start);
        try state.adjust(0);
        if (state.profile_working_set) if (state.profile) |p| {
            p.section_bytes = state.section_bytes;
            p.section_live_bytes = 0;
            p.postings_live_bytes = state.postings_bytes;
            p.typed_live_bytes = state.typed_bytes;
        };
    }
};

const TypedFieldCollector = struct {
    value_type: ?typed_dv.ValueType = null,
    writer: ?typed_dv.TypedDocValuesWriter = null,
    conflicted: bool = false,
    last_doc_id: ?u32 = null,
    staged: std.ArrayListUnmanaged(PostingRunRange) = .empty,

    fn releaseStaged(self: *@This()) void {
        for (self.staged.items) |run| run.spool.releaseRange(run.offset);
        self.staged.clearRetainingCapacity();
    }
    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.writer) |*writer| writer.deinit();
        self.writer = null;
        self.releaseStaged();
        self.staged.deinit(alloc);
        self.staged = .empty;
    }
    fn stage(self: *@This(), alloc: Allocator, options: BuildTextOptions, owner: *?*PostingRun) !void {
        const pending = if (self.writer) |*writer| writer else return;
        if (pending.entries.items.len == 0) return;
        if (owner.* == null) {
            owner.* = try PostingRun.createWithResources(alloc, options.postings_run_io.?, options.postings_run_directory, options.resource_manager);
            if (options.profile) |profile| profile.postings_spool_count +|= 1;
        }
        const spool = owner.*.?;
        const start = spool.len();
        var sink = spool.sink();
        var writer = typed_dv.StreamingWriter.init(alloc, &sink, pending.value_type);
        defer writer.deinit();
        for (pending.entries.items) |entry| try writer.add(entry.doc_id, entry.value);
        if (!try writer.finish()) return error.InvalidData;
        try spool.seal(start);
        errdefer spool.releaseRange(start);
        try self.staged.append(alloc, .{ .spool = spool, .offset = start, .length = spool.len() - start });
        const value_type = pending.value_type;
        pending.deinit();
        pending.* = typed_dv.TypedDocValuesWriter.init(alloc, value_type, typed_dv.default_chunk_size);
    }
};

const DetectedTypedValue = struct {
    value_type: typed_dv.ValueType,
    value: typed_dv.TypedValue,
};

pub fn collectTypedFieldProjection(
    alloc: Allocator,
    value: std.json.Value,
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
) ![]TypedFieldValue {
    if (value != .object) return &.{};

    var fields = std.ArrayListUnmanaged(TypedFieldValue).empty;
    defer fields.deinit(alloc);

    if (options.recursive_typed_fields) {
        try collectTypedFieldProjectionRecursive(alloc, value, "", text_analysis, &fields);
    } else if (options.infer_type_dynamic_paths.len > 0) {
        try collectTypedFieldProjectionRecursiveScoped(alloc, value, "", text_analysis, options.infer_type_dynamic_paths, options.unindexed_paths, &fields);
    } else {
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const field_name = entry.key_ptr.*;
            if (field_name.len > 0 and field_name[0] == '_') continue;
            if (pathFallsUnderAnyScopedPath(options.unindexed_paths, field_name)) continue;

            const detected = detectTypedValue(field_name, entry.value_ptr.*, text_analysis) orelse continue;
            try appendTypedFieldProjectionValue(alloc, &fields, field_name, detected);
        }
    }

    if (fields.items.len == 0) return &.{};
    return try alloc.dupe(TypedFieldValue, fields.items);
}

fn collectTypedFieldValues(
    alloc: Allocator,
    raw_json: []const u8,
    doc_id: u32,
    typed_fields: *TypedFieldCollectors,
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();
    try collectTypedFieldValuesFromValue(alloc, parsed.value, doc_id, typed_fields, text_analysis, options, false);
}

fn collectTypedFieldValuesFromValue(
    alloc: Allocator,
    value: std.json.Value,
    doc_id: u32,
    typed_fields: *TypedFieldCollectors,
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
    borrow_values: bool,
) !void {
    if (value != .object) return;

    if (options.recursive_typed_fields) {
        try collectTypedFieldValuesRecursive(alloc, value, "", doc_id, typed_fields, text_analysis, options.profile, borrow_values);
        return;
    }
    if (options.infer_type_dynamic_paths.len > 0) {
        try collectTypedFieldValuesRecursiveScoped(alloc, value, "", doc_id, typed_fields, text_analysis, options.infer_type_dynamic_paths, options.unindexed_paths, options.profile, borrow_values);
        return;
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        const field_name = entry.key_ptr.*;
        if (field_name.len > 0 and field_name[0] == '_') continue;
        if (pathFallsUnderAnyScopedPath(options.unindexed_paths, field_name)) continue;

        const detected = detectTypedValue(field_name, entry.value_ptr.*, text_analysis) orelse continue;
        try appendTypedFieldValue(alloc, typed_fields, field_name, doc_id, detected, options.profile, borrow_values);
    }
}

fn collectTypedFieldProjectionRecursive(
    alloc: Allocator,
    value: std.json.Value,
    path: []const u8,
    text_analysis: TextAnalysisConfig,
    fields: *std.ArrayListUnmanaged(TypedFieldValue),
) !void {
    if (path.len > 0) {
        if (detectTypedValue(path, value, text_analysis)) |detected| {
            try appendTypedFieldProjectionValue(alloc, fields, path, detected);
            if (value == .object and detected.value_type == .geo_point) return;
        }
    }

    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
                const child_path = if (path.len == 0)
                    try alloc.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(alloc, "{s}.{s}", .{ path, entry.key_ptr.* });
                defer alloc.free(child_path);
                try collectTypedFieldProjectionRecursive(alloc, entry.value_ptr.*, child_path, text_analysis, fields);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldProjectionRecursive(alloc, item, path, text_analysis, fields);
            }
        },
        else => {},
    }
}

fn collectTypedFieldProjectionRecursiveScoped(
    alloc: Allocator,
    value: std.json.Value,
    path: []const u8,
    text_analysis: TextAnalysisConfig,
    scoped_paths: []const []const u8,
    unindexed_paths: []const []const u8,
    fields: *std.ArrayListUnmanaged(TypedFieldValue),
) !void {
    if (path.len > 0 and pathFallsUnderAnyScopedPath(unindexed_paths, path)) return;
    if (path.len > 0 and pathFallsUnderAnyScopedPath(scoped_paths, path)) {
        if (detectTypedValue(path, value, text_analysis)) |detected| {
            try appendTypedFieldProjectionValue(alloc, fields, path, detected);
            if (value == .object and detected.value_type == .geo_point) return;
        }
    }

    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
                const child_path = if (path.len == 0)
                    try alloc.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(alloc, "{s}.{s}", .{ path, entry.key_ptr.* });
                defer alloc.free(child_path);
                try collectTypedFieldProjectionRecursiveScoped(alloc, entry.value_ptr.*, child_path, text_analysis, scoped_paths, unindexed_paths, fields);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldProjectionRecursiveScoped(alloc, item, path, text_analysis, scoped_paths, unindexed_paths, fields);
            }
        },
        else => {},
    }
}

fn appendTypedFieldProjectionValue(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(TypedFieldValue),
    field_name: []const u8,
    detected: DetectedTypedValue,
) !void {
    try fields.append(alloc, .{
        .field_name = try alloc.dupe(u8, field_name),
        .value_type = detected.value_type,
        .value = try cloneTypedValue(alloc, detected.value),
    });
}

pub fn detectTypedFieldProjectionValue(
    alloc: Allocator,
    field_name: []const u8,
    value: std.json.Value,
    text_analysis: TextAnalysisConfig,
) !?TypedFieldValue {
    const detected = detectTypedValue(field_name, value, text_analysis) orelse return null;
    return .{
        .field_name = try alloc.dupe(u8, field_name),
        .value_type = detected.value_type,
        .value = try cloneTypedValue(alloc, detected.value),
    };
}

fn cloneTypedValue(alloc: Allocator, value: typed_dv.TypedValue) !typed_dv.TypedValue {
    return switch (value) {
        .bytes_val => |bytes| .{ .bytes_val = try alloc.dupe(u8, bytes) },
        .u64_val => |number| .{ .u64_val = number },
        .i64_val => |number| .{ .i64_val = number },
        .f64_val => |number| .{ .f64_val = number },
        .geo_point => |point| .{ .geo_point = point },
        .bool_val => |boolean| .{ .bool_val = boolean },
        .numeric_val => |number| .{ .numeric_val = number },
        .datetime_ns => |ns| .{ .datetime_ns = ns },
    };
}

fn collectTypedFieldValuesRecursive(
    alloc: Allocator,
    value: std.json.Value,
    path: []const u8,
    doc_id: u32,
    typed_fields: *TypedFieldCollectors,
    text_analysis: TextAnalysisConfig,
    profile: ?*BuildTextProfile,
    borrow_values: bool,
) !void {
    if (path.len > 0) {
        if (detectTypedValue(path, value, text_analysis)) |detected| {
            try appendTypedFieldValue(alloc, typed_fields, path, doc_id, detected, profile, borrow_values);
            if (value == .object and detected.value_type == .geo_point) return;
        }
    }

    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
                const child_path = if (path.len == 0)
                    try alloc.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(alloc, "{s}.{s}", .{ path, entry.key_ptr.* });
                defer alloc.free(child_path);
                try collectTypedFieldValuesRecursive(alloc, entry.value_ptr.*, child_path, doc_id, typed_fields, text_analysis, profile, borrow_values);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldValuesRecursive(alloc, item, path, doc_id, typed_fields, text_analysis, profile, borrow_values);
            }
        },
        else => {},
    }
}

fn collectTypedFieldValuesRecursiveScoped(
    alloc: Allocator,
    value: std.json.Value,
    path: []const u8,
    doc_id: u32,
    typed_fields: *TypedFieldCollectors,
    text_analysis: TextAnalysisConfig,
    scoped_paths: []const []const u8,
    unindexed_paths: []const []const u8,
    profile: ?*BuildTextProfile,
    borrow_values: bool,
) !void {
    if (path.len > 0 and pathFallsUnderAnyScopedPath(unindexed_paths, path)) return;
    if (path.len > 0 and pathFallsUnderAnyScopedPath(scoped_paths, path)) {
        if (detectTypedValue(path, value, text_analysis)) |detected| {
            try appendTypedFieldValue(alloc, typed_fields, path, doc_id, detected, profile, borrow_values);
            if (value == .object and detected.value_type == .geo_point) return;
        }
    }

    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
                const child_path = if (path.len == 0)
                    try alloc.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(alloc, "{s}.{s}", .{ path, entry.key_ptr.* });
                defer alloc.free(child_path);
                try collectTypedFieldValuesRecursiveScoped(alloc, entry.value_ptr.*, child_path, doc_id, typed_fields, text_analysis, scoped_paths, unindexed_paths, profile, borrow_values);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldValuesRecursiveScoped(alloc, item, path, doc_id, typed_fields, text_analysis, scoped_paths, unindexed_paths, profile, borrow_values);
            }
        },
        else => {},
    }
}

fn pathFallsUnderAnyScopedPath(scoped_paths: []const []const u8, path: []const u8) bool {
    for (scoped_paths) |scoped_path| {
        if (scoped_path.len == 0) return true;
        if (!std.mem.startsWith(u8, path, scoped_path)) continue;
        if (path.len == scoped_path.len) return true;
        if (path.len > scoped_path.len and path[scoped_path.len] == '.') return true;
    }
    return false;
}

fn ensureTypedFieldCollector(alloc: Allocator, typed_fields: *TypedFieldCollectors, field_name: []const u8) !*TypedFieldCollector {
    if (typed_fields.getPtr(field_name)) |collector| return collector;
    const name = try alloc.dupe(u8, field_name);
    errdefer alloc.free(name);
    const gop = try typed_fields.getOrPut(alloc, name);
    gop.key_ptr.* = name;
    gop.value_ptr.* = .{};
    return gop.value_ptr;
}

fn appendTypedFieldValue(
    alloc: Allocator,
    typed_fields: *TypedFieldCollectors,
    field_name: []const u8,
    doc_id: u32,
    detected: DetectedTypedValue,
    profile: ?*BuildTextProfile,
    borrow_values: bool,
) !void {
    const collector = try ensureTypedFieldCollector(alloc, typed_fields, field_name);
    if (collector.conflicted) return;
    if (collector.last_doc_id != null and collector.last_doc_id.? == doc_id) {
        markTypedFieldCollectorConflicted(collector);
        return;
    }

    if (collector.value_type == null) {
        collector.value_type = detected.value_type;
        collector.writer = typed_dv.TypedDocValuesWriter.init(alloc, detected.value_type, typed_dv.default_chunk_size);
    } else if (collector.value_type.? != detected.value_type) {
        markTypedFieldCollectorConflicted(collector);
        return;
    }

    if (borrow_values) {
        try collector.writer.?.addBorrowed(doc_id, detected.value);
    } else {
        try collector.writer.?.add(doc_id, detected.value);
    }
    collector.last_doc_id = doc_id;
    if (typed_fields.staging) |staging| {
        if (profile) |p| p.typed_staging_checks +|= 1;
        const writer = &collector.writer.?;
        if (writer.entries.items.len >= typed_dv.default_chunk_size or writer.raw_value_bytes >= 64 * 1024)
            try collector.stage(alloc, staging.options, staging.owner);
    }
    if (profile) |p| p.typed_value_count +|= 1;
}

fn markTypedFieldConflict(
    alloc: Allocator,
    typed_fields: *TypedFieldCollectors,
    field_name: []const u8,
) !void {
    const collector = try ensureTypedFieldCollector(alloc, typed_fields, field_name);
    markTypedFieldCollectorConflicted(collector);
}

fn markTypedFieldCollectorConflicted(collector: *TypedFieldCollector) void {
    if (collector.writer) |*writer| writer.deinit();
    collector.writer = null;
    collector.releaseStaged();
    collector.conflicted = true;
}

const FieldDateTimeParser = struct {
    field_name: []const u8,
    parser_name: []const u8,
};

const FieldAnalyzer = struct {
    field_name: []const u8,
    analyzer_name: []const u8,
};

const DateTimeParserConfig = struct {
    name: []const u8,
    parser_type: []const u8,
    layouts: []const []const u8 = &.{},
};

const NamedTokenFilter = struct {
    name: []const u8,
    filter: analysis_mod.TokenFilter,
};

const NamedCharFilter = struct {
    name: []const u8,
    filter: analysis_mod.CharFilter,
};

const NamedTokenizer = struct {
    name: []const u8,
    tokenizer: analysis_mod.Tokenizer,
};

const NamedAnalyzer = struct {
    name: []const u8,
    analyzer: analysis_mod.Analyzer,
};

const EdgeNgramSide = @TypeOf(@as(analysis_mod.EdgeNgramConfig, .{}).side);

pub const TextAnalysisConfig = struct {
    default_datetime_parser: ?[]const u8 = null,
    field_datetime_parsers: []const FieldDateTimeParser = &.{},
    datetime_parsers: []const DateTimeParserConfig = &.{},
    field_analyzers: []const FieldAnalyzer = &.{},
    char_filters: []const NamedCharFilter = &.{},
    token_filters: []const NamedTokenFilter = &.{},
    tokenizers: []const NamedTokenizer = &.{},
    analyzers: []const NamedAnalyzer = &.{},
};

pub fn freeTextAnalysisConfig(alloc: Allocator, cfg: TextAnalysisConfig) void {
    if (cfg.default_datetime_parser) |name| alloc.free(name);
    for (cfg.field_datetime_parsers) |item| {
        alloc.free(item.field_name);
        alloc.free(item.parser_name);
    }
    if (cfg.field_datetime_parsers.len > 0) alloc.free(cfg.field_datetime_parsers);
    for (cfg.datetime_parsers) |parser| {
        alloc.free(parser.name);
        alloc.free(parser.parser_type);
        for (parser.layouts) |layout| alloc.free(layout);
        if (parser.layouts.len > 0) alloc.free(parser.layouts);
    }
    if (cfg.datetime_parsers.len > 0) alloc.free(cfg.datetime_parsers);
    for (cfg.field_analyzers) |item| {
        alloc.free(item.field_name);
        alloc.free(item.analyzer_name);
    }
    if (cfg.field_analyzers.len > 0) alloc.free(cfg.field_analyzers);
    for (cfg.char_filters) |item| alloc.free(item.name);
    if (cfg.char_filters.len > 0) alloc.free(cfg.char_filters);
    for (cfg.token_filters) |item| alloc.free(item.name);
    if (cfg.token_filters.len > 0) alloc.free(cfg.token_filters);
    for (cfg.tokenizers) |item| alloc.free(item.name);
    if (cfg.tokenizers.len > 0) alloc.free(cfg.tokenizers);
    for (cfg.analyzers) |analyzer| {
        alloc.free(analyzer.name);
        if (analyzer.analyzer.char_filters.len > 0) alloc.free(analyzer.analyzer.char_filters);
        if (analyzer.analyzer.filters.len > 0) alloc.free(analyzer.analyzer.filters);
    }
    if (cfg.analyzers.len > 0) alloc.free(cfg.analyzers);
}

fn shrinkOwnedSlice(comptime T: type, alloc: Allocator, items: []T, len: usize) ![]T {
    if (len == 0) {
        alloc.free(items);
        return &.{};
    }
    if (len == items.len) return items;
    return try alloc.realloc(items, len);
}

fn detectTypedValue(field_name: []const u8, value: std.json.Value, text_analysis: TextAnalysisConfig) ?DetectedTypedValue {
    return switch (value) {
        .integer => |number| .{
            .value_type = .f64_val,
            .value = .{ .f64_val = @floatFromInt(number) },
        },
        .float => |number| .{
            .value_type = .f64_val,
            .value = .{ .f64_val = number },
        },
        .number_string => |number| blk: {
            const parsed = std.fmt.parseFloat(f64, number) catch break :blk null;
            break :blk .{
                .value_type = .f64_val,
                .value = .{ .f64_val = parsed },
            };
        },
        .bool => |boolean| .{
            .value_type = .bool_val,
            .value = .{ .bool_val = boolean },
        },
        .string => |text| blk: {
            const timestamp_ns = parseConfiguredDateTimeToNs(text, field_name, text_analysis) catch break :blk null;
            if (timestamp_ns == null) break :blk null;
            break :blk .{
                .value_type = .u64_val,
                .value = .{ .u64_val = timestamp_ns.? },
            };
        },
        .object => blk: {
            const point = jsonValueToGeoPoint(value) orelse break :blk null;
            break :blk .{
                .value_type = .geo_point,
                .value = .{ .geo_point = .{ .lat = point.lat, .lon = point.lon } },
            };
        },
        else => null,
    };
}

pub fn parseTextAnalysisConfig(alloc: Allocator, raw: ?[]const u8) !TextAnalysisConfig {
    const config_json = raw orelse return .{};
    if (config_json.len == 0) return .{};

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, config_json, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return .{};
    const analysis_val = root.object.get("analysis_config") orelse return .{};
    if (analysis_val != .object) return .{};

    var cfg = TextAnalysisConfig{};
    errdefer freeTextAnalysisConfig(alloc, cfg);
    if (analysis_val.object.get("default_datetime_parser")) |value| {
        if (value == .string and value.string.len > 0) {
            cfg.default_datetime_parser = try alloc.dupe(u8, value.string);
        }
    }

    if (analysis_val.object.get("field_date_time_parsers")) |value| {
        if (value == .object) {
            var count: usize = 0;
            var it = value.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* == .string and entry.value_ptr.string.len > 0) count += 1;
            }
            if (count > 0) {
                const items = try alloc.alloc(FieldDateTimeParser, count);
                errdefer {
                    for (items[0..count]) |item| {
                        if (item.field_name.len > 0) alloc.free(item.field_name);
                        if (item.parser_name.len > 0) alloc.free(item.parser_name);
                    }
                    alloc.free(items);
                }
                @memset(items, std.mem.zeroes(FieldDateTimeParser));
                var idx: usize = 0;
                it = value.object.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* != .string or entry.value_ptr.string.len == 0) continue;
                    items[idx] = .{
                        .field_name = try alloc.dupe(u8, entry.key_ptr.*),
                        .parser_name = try alloc.dupe(u8, entry.value_ptr.string),
                    };
                    idx += 1;
                }
                cfg.field_datetime_parsers = try shrinkOwnedSlice(FieldDateTimeParser, alloc, items, idx);
            }
        }
    }

    if (analysis_val.object.get("field_analyzers")) |value| {
        if (value == .object) {
            var count: usize = 0;
            var it = value.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* == .string and entry.value_ptr.string.len > 0) count += 1;
            }
            if (count > 0) {
                const items = try alloc.alloc(FieldAnalyzer, count);
                errdefer {
                    for (items[0..count]) |item| {
                        if (item.field_name.len > 0) alloc.free(item.field_name);
                        if (item.analyzer_name.len > 0) alloc.free(item.analyzer_name);
                    }
                    alloc.free(items);
                }
                @memset(items, std.mem.zeroes(FieldAnalyzer));
                var idx: usize = 0;
                it = value.object.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* != .string or entry.value_ptr.string.len == 0) continue;
                    items[idx] = .{
                        .field_name = try alloc.dupe(u8, entry.key_ptr.*),
                        .analyzer_name = try alloc.dupe(u8, entry.value_ptr.string),
                    };
                    idx += 1;
                }
                cfg.field_analyzers = try shrinkOwnedSlice(FieldAnalyzer, alloc, items, idx);
            }
        }
    }

    if (analysis_val.object.get("date_time_parsers")) |value| {
        if (value == .object) {
            var count: usize = 0;
            var it = value.object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* == .object) count += 1;
            }
            if (count > 0) {
                const items = try alloc.alloc(DateTimeParserConfig, count);
                errdefer {
                    for (items[0..count]) |item| {
                        if (item.name.len > 0) alloc.free(item.name);
                        if (item.parser_type.len > 0) alloc.free(item.parser_type);
                        for (item.layouts) |layout| alloc.free(layout);
                        if (item.layouts.len > 0) alloc.free(item.layouts);
                    }
                    alloc.free(items);
                }
                @memset(items, std.mem.zeroes(DateTimeParserConfig));
                var idx: usize = 0;
                it = value.object.iterator();
                while (it.next()) |entry| {
                    const parser_val = entry.value_ptr.*;
                    if (parser_val != .object) continue;
                    const type_val = parser_val.object.get("type") orelse continue;
                    if (type_val != .string or type_val.string.len == 0) continue;

                    var layouts: []const []const u8 = &.{};
                    if (parser_val.object.get("config")) |cfg_val| {
                        if (cfg_val == .object) {
                            if (cfg_val.object.get("layouts")) |layouts_val| {
                                if (layouts_val == .array) {
                                    var layout_count: usize = 0;
                                    for (layouts_val.array.items) |layout_item| {
                                        if (layout_item == .string and layout_item.string.len > 0) layout_count += 1;
                                    }
                                    if (layout_count > 0) {
                                        const layout_items = try alloc.alloc([]const u8, layout_count);
                                        errdefer {
                                            for (layout_items[0..layout_count]) |layout| {
                                                if (layout.len > 0) alloc.free(layout);
                                            }
                                            alloc.free(layout_items);
                                        }
                                        @memset(layout_items, std.mem.zeroes([]const u8));
                                        var layout_idx: usize = 0;
                                        for (layouts_val.array.items) |layout_item| {
                                            if (layout_item != .string or layout_item.string.len == 0) continue;
                                            layout_items[layout_idx] = try alloc.dupe(u8, layout_item.string);
                                            layout_idx += 1;
                                        }
                                        layouts = try shrinkOwnedSlice([]const u8, alloc, layout_items, layout_idx);
                                    }
                                }
                            }
                        }
                    }

                    items[idx] = .{
                        .name = try alloc.dupe(u8, entry.key_ptr.*),
                        .parser_type = try alloc.dupe(u8, type_val.string),
                        .layouts = layouts,
                    };
                    idx += 1;
                }
                cfg.datetime_parsers = try shrinkOwnedSlice(DateTimeParserConfig, alloc, items, idx);
            }
        }
    }

    if (analysis_val.object.get("char_filters")) |value| {
        if (value == .object) {
            cfg.char_filters = try parseNamedCharFilters(alloc, value);
        }
    }

    if (analysis_val.object.get("token_filters")) |value| {
        if (value == .object) {
            cfg.token_filters = try parseNamedTokenFilters(alloc, value);
        }
    }

    if (analysis_val.object.get("tokenizers")) |value| {
        if (value == .object) {
            cfg.tokenizers = try parseNamedTokenizers(alloc, value);
        }
    }

    if (analysis_val.object.get("analyzers")) |value| {
        if (value == .object) {
            cfg.analyzers = try parseNamedAnalyzers(alloc, value, cfg.char_filters, cfg.token_filters, cfg.tokenizers);
        }
    }

    return cfg;
}

fn parseNamedTokenFilters(alloc: Allocator, value: std.json.Value) ![]const NamedTokenFilter {
    var count: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* == .object) count += 1;
    }
    if (count == 0) return &.{};

    var idx: usize = 0;
    const items = try alloc.alloc(NamedTokenFilter, count);
    errdefer {
        for (items[0..idx]) |item| alloc.free(item.name);
        alloc.free(items);
    }
    it = value.object.iterator();
    while (it.next()) |entry| {
        const component = entry.value_ptr.*;
        if (component != .object) continue;
        const type_val = component.object.get("type") orelse continue;
        if (type_val != .string or type_val.string.len == 0) continue;
        items[idx] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .filter = try resolveTokenFilterComponent(component, type_val.string),
        };
        idx += 1;
    }
    return try shrinkOwnedSlice(NamedTokenFilter, alloc, items, idx);
}

fn parseNamedCharFilters(alloc: Allocator, value: std.json.Value) ![]const NamedCharFilter {
    var count: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* == .object) count += 1;
    }
    if (count == 0) return &.{};

    var idx: usize = 0;
    const items = try alloc.alloc(NamedCharFilter, count);
    errdefer {
        for (items[0..idx]) |item| alloc.free(item.name);
        alloc.free(items);
    }
    it = value.object.iterator();
    while (it.next()) |entry| {
        const component = entry.value_ptr.*;
        if (component != .object) continue;
        const type_val = component.object.get("type") orelse continue;
        if (type_val != .string or type_val.string.len == 0) continue;
        items[idx] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .filter = try resolveCharFilterComponent(type_val.string),
        };
        idx += 1;
    }
    return try shrinkOwnedSlice(NamedCharFilter, alloc, items, idx);
}

fn parseNamedTokenizers(alloc: Allocator, value: std.json.Value) ![]const NamedTokenizer {
    var count: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* == .object) count += 1;
    }
    if (count == 0) return &.{};

    var idx: usize = 0;
    const items = try alloc.alloc(NamedTokenizer, count);
    errdefer {
        for (items[0..idx]) |item| alloc.free(item.name);
        alloc.free(items);
    }
    it = value.object.iterator();
    while (it.next()) |entry| {
        const component = entry.value_ptr.*;
        if (component != .object) continue;
        const type_val = component.object.get("type") orelse continue;
        if (type_val != .string or type_val.string.len == 0) continue;
        items[idx] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .tokenizer = try resolveTokenizerComponent(component, type_val.string),
        };
        idx += 1;
    }
    return try shrinkOwnedSlice(NamedTokenizer, alloc, items, idx);
}

fn parseNamedAnalyzers(
    alloc: Allocator,
    value: std.json.Value,
    char_filters: []const NamedCharFilter,
    token_filters: []const NamedTokenFilter,
    tokenizers: []const NamedTokenizer,
) ![]const NamedAnalyzer {
    var count: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* == .object) count += 1;
    }
    if (count == 0) return &.{};

    var idx: usize = 0;
    const items = try alloc.alloc(NamedAnalyzer, count);
    errdefer {
        for (items[0..idx]) |item| {
            alloc.free(item.name);
            if (item.analyzer.char_filters.len > 0) alloc.free(item.analyzer.char_filters);
            if (item.analyzer.filters.len > 0) alloc.free(item.analyzer.filters);
        }
        alloc.free(items);
    }
    it = value.object.iterator();
    while (it.next()) |entry| {
        const component = entry.value_ptr.*;
        if (component != .object) continue;
        const type_val = component.object.get("type") orelse continue;
        if (type_val != .string or type_val.string.len == 0) continue;
        items[idx] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .analyzer = try resolveAnalyzerComponent(alloc, component, type_val.string, char_filters, token_filters, tokenizers),
        };
        idx += 1;
    }
    return try shrinkOwnedSlice(NamedAnalyzer, alloc, items, idx);
}

fn resolveAnalyzerComponent(
    alloc: Allocator,
    component: std.json.Value,
    component_type: []const u8,
    char_filters: []const NamedCharFilter,
    token_filters: []const NamedTokenFilter,
    tokenizers: []const NamedTokenizer,
) !analysis_mod.Analyzer {
    if (!std.mem.eql(u8, component_type, "custom")) return error.InvalidArgument;
    const config_val = component.object.get("config") orelse return error.InvalidArgument;
    if (config_val != .object) return error.InvalidArgument;

    const tokenizer_name = if (config_val.object.get("tokenizer")) |value|
        if (value == .string) value.string else return error.InvalidArgument
    else
        "unicode";

    const analyzer_char_filters = if (config_val.object.get("char_filters")) |value|
        try parseCharFilters(alloc, value, char_filters)
    else
        &.{};
    const filters = if (config_val.object.get("token_filters")) |value|
        try parseTokenFilters(alloc, value, token_filters)
    else
        &.{};

    return .{
        .char_filters = analyzer_char_filters,
        .tokenizer = try resolveTokenizerName(tokenizer_name, tokenizers),
        .filters = filters,
    };
}

fn parseCharFilters(alloc: Allocator, value: std.json.Value, named: []const NamedCharFilter) ![]const analysis_mod.CharFilter {
    if (value != .array) return error.InvalidArgument;
    if (value.array.items.len == 0) return &.{};
    const items = try alloc.alloc(analysis_mod.CharFilter, value.array.items.len);
    var idx: usize = 0;
    for (value.array.items) |item| {
        if (item != .string or item.string.len == 0) continue;
        items[idx] = try resolveCharFilterName(item.string, named);
        idx += 1;
    }
    return try shrinkOwnedSlice(analysis_mod.CharFilter, alloc, items, idx);
}

fn parseTokenFilters(alloc: Allocator, value: std.json.Value, named: []const NamedTokenFilter) ![]const analysis_mod.TokenFilter {
    if (value != .array) return error.InvalidArgument;
    if (value.array.items.len == 0) return &.{};
    const items = try alloc.alloc(analysis_mod.TokenFilter, value.array.items.len);
    var idx: usize = 0;
    for (value.array.items) |item| {
        if (item != .string or item.string.len == 0) continue;
        items[idx] = try resolveTokenFilterName(item.string, named);
        idx += 1;
    }
    return try shrinkOwnedSlice(analysis_mod.TokenFilter, alloc, items, idx);
}

fn resolveTokenizerName(name: []const u8, named: []const NamedTokenizer) !analysis_mod.Tokenizer {
    if (std.mem.eql(u8, name, "unicode") or std.mem.eql(u8, name, "unicode_words")) return .unicode_words;
    if (std.mem.eql(u8, name, "whitespace")) return .whitespace;
    if (std.mem.eql(u8, name, "keyword")) return .keyword;
    if (std.mem.eql(u8, name, "character")) return .character;
    for (named) |item| {
        if (std.mem.eql(u8, item.name, name)) return item.tokenizer;
    }
    return error.InvalidArgument;
}

fn resolveCharFilterName(name: []const u8, named: []const NamedCharFilter) !analysis_mod.CharFilter {
    if (std.mem.eql(u8, name, "html") or std.mem.eql(u8, name, "html_strip")) return .html_strip;
    if (std.mem.eql(u8, name, "ascii_fold")) return .ascii_fold;
    if (std.mem.eql(u8, name, "zero_width_non_joiner")) return .zero_width_non_joiner;
    for (named) |item| {
        if (std.mem.eql(u8, item.name, name)) return item.filter;
    }
    return error.InvalidArgument;
}

fn resolveCharFilterComponent(component_type: []const u8) !analysis_mod.CharFilter {
    if (std.mem.eql(u8, component_type, "html") or std.mem.eql(u8, component_type, "html_strip")) return .html_strip;
    if (std.mem.eql(u8, component_type, "ascii_fold")) return .ascii_fold;
    if (std.mem.eql(u8, component_type, "zero_width_non_joiner")) return .zero_width_non_joiner;
    return error.InvalidArgument;
}

fn resolveTokenizerComponent(component: std.json.Value, component_type: []const u8) !analysis_mod.Tokenizer {
    const config_val = component.object.get("config");
    if (std.mem.eql(u8, component_type, "unicode") or std.mem.eql(u8, component_type, "unicode_words")) return .unicode_words;
    if (std.mem.eql(u8, component_type, "whitespace")) return .whitespace;
    if (std.mem.eql(u8, component_type, "keyword")) return .keyword;
    if (std.mem.eql(u8, component_type, "character")) return .character;
    if (std.mem.eql(u8, component_type, "edge_ngram")) {
        return .{ .edge_ngram = .{
            .min = try configU8(config_val, "min", 1),
            .max = try configU8(config_val, "max", 3),
            .side = try configEdgeSide(config_val),
        } };
    }
    if (std.mem.eql(u8, component_type, "ngram")) {
        return .{ .ngram = .{
            .min = try configU8(config_val, "min", 2),
            .max = try configU8(config_val, "max", 3),
        } };
    }
    return error.InvalidArgument;
}

/// Token filters that need no configuration can be referenced directly by
/// name from an analyzer's `token_filters` list, without declaring a named
/// component first. Named components declared in `token_filters` shadow these.
fn builtinTokenFilterByName(name: []const u8) ?analysis_mod.TokenFilter {
    if (std.mem.eql(u8, name, "to_lower") or std.mem.eql(u8, name, "lowercase")) return .lowercase;
    if (std.mem.eql(u8, name, "stop_en") or std.mem.eql(u8, name, "stop_words")) return .stop_words;
    if (std.mem.eql(u8, name, "stemmer_en") or std.mem.eql(u8, name, "stemmer")) return .stemmer;
    if (std.mem.eql(u8, name, "camel_case")) return .camel_case;
    if (std.mem.eql(u8, name, "unique")) return .unique;
    if (std.mem.eql(u8, name, "reverse")) return .reverse;
    if (std.mem.eql(u8, name, "elision")) return .elision;
    if (std.mem.eql(u8, name, "apostrophe")) return .apostrophe;
    if (std.mem.eql(u8, name, "suffix")) return .{ .suffix = .{} };
    return null;
}

fn resolveTokenFilterName(name: []const u8, named: []const NamedTokenFilter) !analysis_mod.TokenFilter {
    for (named) |item| {
        if (std.mem.eql(u8, item.name, name)) return item.filter;
    }
    if (builtinTokenFilterByName(name)) |filter| return filter;
    return error.InvalidArgument;
}

fn resolveTokenFilterComponent(component: std.json.Value, component_type: []const u8) !analysis_mod.TokenFilter {
    const config_val = component.object.get("config");
    if (std.mem.eql(u8, component_type, "edge_ngram")) {
        return .{ .edge_ngram = .{
            .min = try configU8(config_val, "min", 1),
            .max = try configU8(config_val, "max", 3),
            .side = try configEdgeSide(config_val),
        } };
    }
    if (std.mem.eql(u8, component_type, "ngram")) {
        return .{ .ngram = .{
            .min = try configU8(config_val, "min", 2),
            .max = try configU8(config_val, "max", 3),
        } };
    }
    if (std.mem.eql(u8, component_type, "shingle")) {
        const min = try configU8(config_val, "min", 2);
        const max = try configU8(config_val, "max", 2);
        if (min == 0 or max < min) return error.InvalidArgument;
        return .{ .shingle = .{ .min = min, .max = max, .separator = try configShingleSeparator(config_val) } };
    }
    if (std.mem.eql(u8, component_type, "suffix")) {
        return .{ .suffix = .{
            .min = try configU8(config_val, "min", analysis_mod.substring_min_query_length),
            .max = try configU8(config_val, "max", analysis_mod.substring_max_query_length),
        } };
    }
    if (std.mem.eql(u8, component_type, "length")) {
        return .{ .length = .{
            .min = try configU8(config_val, "min", 0),
            .max = try configU8(config_val, "max", 255),
        } };
    }
    if (std.mem.eql(u8, component_type, "truncate")) {
        return .{ .truncate = .{ .max_len = try configU8(config_val, "length", 255) } };
    }
    if (std.mem.eql(u8, component_type, "stop_words") or std.mem.eql(u8, component_type, "stop")) {
        if (try configLanguage(config_val)) |lang| return .{ .stop_words_lang = lang };
        return .stop_words;
    }
    if (std.mem.eql(u8, component_type, "stemmer")) {
        if (try configLanguage(config_val)) |lang| return .{ .stemmer_lang = lang };
        return .stemmer;
    }
    if (builtinTokenFilterByName(component_type)) |filter| return filter;
    return error.InvalidArgument;
}

fn configShingleSeparator(config_val: ?std.json.Value) !analysis_mod.TokenFilter.ShingleConfig.Separator {
    const cfg = config_val orelse return .space;
    if (cfg != .object) return .space;
    const raw = cfg.object.get("separator") orelse return .space;
    if (raw != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, raw.string, "none") or raw.string.len == 0) return .none;
    if (std.mem.eql(u8, raw.string, "space") or std.mem.eql(u8, raw.string, " ")) return .space;
    return error.InvalidArgument;
}

fn configLanguage(config_val: ?std.json.Value) !?analysis_mod.Language {
    const cfg = config_val orelse return null;
    if (cfg != .object) return null;
    const raw = cfg.object.get("language") orelse cfg.object.get("lang") orelse return null;
    if (raw != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, raw.string, "en")) return .english;
    return std.meta.stringToEnum(analysis_mod.Language, raw.string) orelse error.InvalidArgument;
}

fn configU8(config_val: ?std.json.Value, field: []const u8, default_value: u8) !u8 {
    const cfg = config_val orelse return default_value;
    if (cfg != .object) return default_value;
    const raw = cfg.object.get(field) orelse return default_value;
    return switch (raw) {
        .integer => std.math.cast(u8, raw.integer) orelse return error.InvalidArgument,
        .float => blk: {
            if (raw.float < 0 or raw.float > 255) return error.InvalidArgument;
            break :blk @intFromFloat(raw.float);
        },
        .number_string => std.fmt.parseInt(u8, raw.number_string, 10) catch return error.InvalidArgument,
        else => return error.InvalidArgument,
    };
}

fn configEdgeSide(config_val: ?std.json.Value) !EdgeNgramSide {
    const cfg = config_val orelse return .front;
    if (cfg != .object) return .front;
    const raw = cfg.object.get("side") orelse return .front;
    if (raw != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, raw.string, "back")) return .back;
    return .front;
}

fn resolveFieldAnalyzer(field_name: []const u8, cfg: TextAnalysisConfig) ?*const analysis_mod.Analyzer {
    const analyzer_name = fieldAnalyzerName(field_name, cfg) orelse return null;
    return resolveAnalyzerName(analyzer_name, cfg);
}

fn fieldAnalyzerName(field_name: []const u8, cfg: TextAnalysisConfig) ?[]const u8 {
    for (cfg.field_analyzers) |item| {
        if (std.mem.eql(u8, item.field_name, field_name)) return item.analyzer_name;
    }
    return null;
}

pub fn resolveAnalyzerName(name: []const u8, cfg: TextAnalysisConfig) ?*const analysis_mod.Analyzer {
    if (analysis_mod.builtinAnalyzerByName(name)) |analyzer| return analyzer;
    for (cfg.analyzers) |*item| {
        if (std.mem.eql(u8, item.name, name)) return &item.analyzer;
    }
    return null;
}

fn parseConfiguredDateTimeToNs(text: []const u8, field_name: []const u8, cfg: TextAnalysisConfig) !?u64 {
    const parser_name = fieldDateTimeParserName(field_name, cfg) orelse cfg.default_datetime_parser;
    if (parser_name) |name| {
        if (parseBuiltInDateTimeToNs(text, name)) |ts| return ts;
        if (findDateTimeParser(name, cfg)) |parser| {
            if (std.mem.eql(u8, parser.parser_type, "sanitizedgo")) {
                for (parser.layouts) |layout| {
                    if (try parseGoLayoutToNs(text, layout)) |ts| return ts;
                }
            }
        }
    }
    return try parseRfc3339ToNs(text);
}

fn fieldDateTimeParserName(field_name: []const u8, cfg: TextAnalysisConfig) ?[]const u8 {
    for (cfg.field_datetime_parsers) |item| {
        if (std.mem.eql(u8, item.field_name, field_name)) return item.parser_name;
    }
    return null;
}

fn findDateTimeParser(name: []const u8, cfg: TextAnalysisConfig) ?DateTimeParserConfig {
    for (cfg.datetime_parsers) |parser| {
        if (std.mem.eql(u8, parser.name, name)) return parser;
    }
    return null;
}

fn parseBuiltInDateTimeToNs(text: []const u8, parser_name: []const u8) ?u64 {
    if (std.mem.eql(u8, parser_name, "dateTimeOptional")) {
        return parseDateTimeOptionalToNs(text) catch null;
    }
    if (std.mem.eql(u8, parser_name, "unix_sec")) {
        const secs = std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return null;
        return secs * std.time.ns_per_s;
    }
    if (std.mem.eql(u8, parser_name, "unix_milli")) {
        const millis = std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return null;
        return millis * std.time.ns_per_ms;
    }
    if (std.mem.eql(u8, parser_name, "unix_micro")) {
        const micros = std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return null;
        return micros * std.time.ns_per_us;
    }
    if (std.mem.eql(u8, parser_name, "unix_nano")) {
        return std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch null;
    }
    return null;
}

fn parseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try parseRfc3339ToNs(text)) |ts| return ts;
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    return civilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

fn parseGoLayoutToNs(text: []const u8, layout: []const u8) !?u64 {
    const value = std.mem.trim(u8, text, " \t\r\n");
    var i: usize = 0;
    var j: usize = 0;

    var year: ?i64 = null;
    var month: ?i64 = null;
    var day: ?i64 = null;
    var hour24: ?i64 = null;
    var hour12: ?i64 = null;
    var minute: i64 = 0;
    var second: i64 = 0;
    const nanos: u64 = 0;
    var pm: ?bool = null;

    while (i < layout.len) {
        if (std.mem.startsWith(u8, layout[i..], "2006")) {
            year = try parseFixedDigitsI64(value, &j, 4);
            i += 4;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "01")) {
            month = try parseFixedDigitsI64(value, &j, 2);
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "02")) {
            day = try parseFixedDigitsI64(value, &j, 2);
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "15")) {
            hour24 = try parseFixedDigitsI64(value, &j, 2);
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "03")) {
            hour12 = try parseFixedDigitsI64(value, &j, 2);
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "04")) {
            minute = try parseFixedDigitsI64(value, &j, 2) orelse return null;
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "05")) {
            second = try parseFixedDigitsI64(value, &j, 2) orelse return null;
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "PM")) {
            const token = parseMeridiemToken(value, &j) orelse return null;
            pm = token;
            i += 2;
            continue;
        }
        if (std.mem.startsWith(u8, layout[i..], "pm")) {
            const token = parseMeridiemToken(value, &j) orelse return null;
            pm = token;
            i += 2;
            continue;
        }
        if (layout[i] == '1') {
            month = try parseVariableDigitsI64(value, &j, 1, 2);
            i += 1;
            continue;
        }
        if (layout[i] == '2') {
            day = try parseVariableDigitsI64(value, &j, 1, 2);
            i += 1;
            continue;
        }
        if (layout[i] == '3') {
            hour12 = try parseVariableDigitsI64(value, &j, 1, 2);
            i += 1;
            continue;
        }
        if (layout[i] == '4') {
            minute = try parseVariableDigitsI64(value, &j, 1, 2) orelse return null;
            i += 1;
            continue;
        }
        if (layout[i] == '5') {
            second = try parseVariableDigitsI64(value, &j, 1, 2) orelse return null;
            i += 1;
            continue;
        }

        if (j >= value.len or value[j] != layout[i]) return null;
        j += 1;
        i += 1;
    }

    if (j != value.len or year == null or month == null or day == null) return null;

    const hour = blk: {
        if (hour24) |value24| break :blk value24;
        if (hour12) |value12| {
            const is_pm = pm orelse return null;
            if (value12 < 1 or value12 > 12) return null;
            if (is_pm) {
                break :blk if (value12 == 12) @as(i64, 12) else value12 + 12;
            }
            break :blk if (value12 == 12) @as(i64, 0) else value12;
        }
        break :blk @as(i64, 0);
    };

    return civilDateTimeToNs(year.?, month.?, day.?, hour, minute, second, nanos);
}

fn parseFixedDigitsI64(text: []const u8, index: *usize, len: usize) !?i64 {
    if (index.* + len > text.len) return null;
    for (text[index.* .. index.* + len]) |char| {
        if (char < '0' or char > '9') return null;
    }
    const out = std.fmt.parseInt(i64, text[index.* .. index.* + len], 10) catch return null;
    index.* += len;
    return out;
}

fn parseVariableDigitsI64(text: []const u8, index: *usize, min_len: usize, max_len: usize) !?i64 {
    var end = index.*;
    while (end < text.len and end - index.* < max_len and text[end] >= '0' and text[end] <= '9') : (end += 1) {}
    if (end - index.* < min_len) return null;
    const out = std.fmt.parseInt(i64, text[index.*..end], 10) catch return null;
    index.* = end;
    return out;
}

fn parseMeridiemToken(text: []const u8, index: *usize) ?bool {
    if (index.* + 2 > text.len) return null;
    const token = text[index.* .. index.* + 2];
    index.* += 2;
    if (std.ascii.eqlIgnoreCase(token, "AM")) return false;
    if (std.ascii.eqlIgnoreCase(token, "PM")) return true;
    return null;
}

fn civilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    if (hour < 0 or hour > 23) return null;
    if (minute < 0 or minute > 59) return null;
    if (second < 0 or second > 60) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn jsonValueToF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string => |number| std.fmt.parseFloat(f64, number) catch null,
        else => null,
    };
}

fn jsonValueToGeoPoint(value: std.json.Value) ?geo_mod.GeoPoint {
    if (value != .object) return null;
    const lat_val = value.object.get("lat") orelse return null;
    const lon_val = value.object.get("lon") orelse return null;
    const lat = jsonValueToF64(lat_val) orelse return null;
    const lon = jsonValueToF64(lon_val) orelse return null;
    return .{ .lat = lat, .lon = lon };
}

fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

// ============================================================================
// Tests
// ============================================================================

test "introducer builds and indexes a batch" {
    const alloc = std.testing.allocator;
    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    var introducer = Introducer.init(alloc, &writer);

    try introducer.submit(.{ .docs = &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\": \"hello world\"}",
            .fields = &.{.{
                .field_name = "title",
                .hits = &.{
                    .{ .term = "hello", .freq = 1, .norm = 10 },
                    .{ .term = "world", .freq = 1, .norm = 10 },
                },
            }},
        },
        .{
            .id = "doc2",
            .stored_data = "{\"title\": \"hello zig\"}",
            .fields = &.{.{
                .field_name = "title",
                .hits = &.{
                    .{ .term = "hello", .freq = 1, .norm = 10 },
                    .{ .term = "zig", .freq = 2, .norm = 10 },
                },
            }},
        },
    } });

    const snap = writer.snapshot();
    try std.testing.expectEqual(@as(u32, 2), snap.liveDocCount());

    const results = try snap.search(alloc, "title", &.{"hello"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 2), results.hits.len);

    // Verify stored docs accessible
    const stored = (try snap.segments[0].reader.storedDoc(0)).?;
    try std.testing.expectEqualStrings("doc1", stored.id);
}

test "multiple batches create multiple segments" {
    const alloc = std.testing.allocator;

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    var introducer = Introducer.init(alloc, &writer);

    // Batch 1
    try introducer.submit(.{ .docs = &.{
        .{ .id = "a", .stored_data = "{}", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "alpha", .freq = 1, .norm = 5 }},
        }} },
    } });

    try std.testing.expectEqual(@as(usize, 1), writer.snapshot().segments.len);

    // Batch 2
    try introducer.submit(.{ .docs = &.{
        .{ .id = "b", .stored_data = "{}", .fields = &.{.{
            .field_name = "body",
            .hits = &.{.{ .term = "beta", .freq = 1, .norm = 5 }},
        }} },
    } });

    try std.testing.expectEqual(@as(usize, 2), writer.snapshot().segments.len);
    try std.testing.expectEqual(@as(u32, 2), writer.snapshot().liveDocCount());
}

test "buildSegmentFromText analyzes and indexes text" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildSegmentFromText(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\": \"The Running Dogs\"}",
            .text_fields = &.{.{
                .field_name = "title",
                .text = "The Running Dogs",
            }},
        },
        .{
            .id = "doc2",
            .stored_data = "{\"title\": \"Walking Cats\"}",
            .text_fields = &.{.{
                .field_name = "title",
                .text = "Walking Cats",
            }},
        },
    }, &analysis_mod.default_analyzer, null);
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    try std.testing.expectEqual(@as(u32, 2), snap.liveDocCount());

    // Check what terms the default analyzer produces for "Running"
    const check_tokens = try analysis_mod.default_analyzer.analyze(alloc, "Running");
    defer analysis_mod.Analyzer.freeTokens(alloc, check_tokens);

    // Search for stemmed term
    if (check_tokens.len > 0) {
        const results = try snap.search(alloc, "title", &.{check_tokens[0].term}, 10);
        defer alloc.free(results.hits);
        try std.testing.expect(results.hits.len >= 1);
    }

    // "the" is a stop word — should not be in the index
    const stop_results = try snap.search(alloc, "title", &.{"the"}, 10);
    defer alloc.free(stop_results.hits);
    try std.testing.expectEqual(@as(usize, 0), stop_results.hits.len);
}

test "buildSegmentFromTextWithAnalysisOptions records build profile" {
    const alloc = std.testing.allocator;

    var profile = BuildTextProfile{};
    const seg_bytes = try buildSegmentFromTextWithAnalysisOptions(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"alpha beta\",\"price\":42}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha beta" }},
        },
    }, &analysis_mod.default_analyzer, .{}, .{
        .profile = &profile,
    });
    defer alloc.free(seg_bytes);

    try std.testing.expectEqual(@as(u64, 1), profile.doc_count);
    try std.testing.expectEqual(@as(u64, 1), profile.text_field_count);
    try std.testing.expect(profile.token_count > 0);
    try std.testing.expect(profile.term_hit_count > 0);
    try std.testing.expect(profile.segment_bytes > 0);
    try std.testing.expect(profile.resource_peak_bytes > 0);
    try std.testing.expect(profile.stored_docs_estimated_bytes > 0);
    try std.testing.expect(profile.field_postings_estimated_bytes > 0);
    try std.testing.expect(profile.section_bytes > 0);
}

test "splitTextDocumentsForBuildBudget flushes on build memory before segment bytes" {
    const docs = [_]TextDocument{
        .{
            .id = "doc1",
            .stored_data = "{}",
            .text_fields = &.{.{ .field_name = "body", .text = "alpha beta gamma delta epsilon" }},
        },
        .{
            .id = "doc2",
            .stored_data = "{}",
            .text_fields = &.{.{ .field_name = "body", .text = "zeta eta theta iota kappa" }},
        },
        .{
            .id = "doc3",
            .stored_data = "{}",
            .text_fields = &.{.{ .field_name = "body", .text = "lambda mu nu xi omicron" }},
        },
    };
    const first_estimate = estimateTextDocumentBuildMemoryBytes(docs[0]);
    const split = splitTextDocumentsForBuildBudget(&docs, 0, .{
        .target_build_memory_bytes = @intCast(first_estimate + 1),
        .target_segment_bytes = std.math.maxInt(usize),
    });

    try std.testing.expectEqual(@as(usize, 1), split.end);
    try std.testing.expectEqual(TextBuildSplitReason.build_memory, split.reason);
    try std.testing.expect(!split.oversized_doc);
    try std.testing.expect(split.estimated_build_bytes > 0);
}

test "splitTextDocumentsForBuildBudget keeps oversized single document" {
    const docs = [_]TextDocument{
        .{
            .id = "doc1",
            .stored_data = "{}",
            .text_fields = &.{.{ .field_name = "body", .text = "alpha beta gamma delta epsilon" }},
        },
    };
    const split = splitTextDocumentsForBuildBudget(&docs, 0, .{
        .target_build_memory_bytes = 1,
        .target_segment_bytes = std.math.maxInt(usize),
    });

    try std.testing.expectEqual(@as(usize, 1), split.end);
    try std.testing.expectEqual(TextBuildSplitReason.build_memory, split.reason);
    try std.testing.expect(split.oversized_doc);
    try std.testing.expect(split.estimated_build_bytes > 1);
}

test "buildSegmentFromTextWithAnalysisOptions accounts and releases full text working set" {
    const alloc = std.testing.allocator;

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var profile = BuildTextProfile{};
    const seg_bytes = try buildSegmentFromTextWithAnalysisOptions(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"alpha beta\"}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha beta gamma" }},
        },
    }, &analysis_mod.default_analyzer, .{}, .{
        .profile = &profile,
        .resource_manager = &manager,
    });
    defer alloc.free(seg_bytes);

    const stats = manager.sliceStats(.full_text_build_working_set);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes > 0);
    try std.testing.expect(profile.resource_peak_bytes > 0);
}

test "buildSegmentFromTextWithAnalysisOptions releases full text working set after budget rejection" {
    const alloc = std.testing.allocator;

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var profile = BuildTextProfile{};
    try std.testing.expectError(error.ResourceBudgetExceeded, buildSegmentFromTextWithAnalysisOptions(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"alpha beta\"}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha beta gamma" }},
        },
    }, &analysis_mod.default_analyzer, .{}, .{
        .profile = &profile,
        .resource_manager = &manager,
    }));

    const stats = manager.sliceStats(.full_text_build_working_set);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.hard_limit_rejections > 0);
}

test "buildSegmentFromText omits empty inverted sections" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildSegmentFromText(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"the\",\"body\":\"alpha beta\"}",
            .text_fields = &.{
                .{ .field_name = "title", .text = "the" },
                .{ .field_name = "body", .text = "alpha beta" },
            },
        },
    }, &analysis_mod.default_analyzer, null);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    try std.testing.expect((try reader.getSection("title", .inverted_text)) == null);
    try std.testing.expect(try reader.invertedIndex("title") == null);
    try std.testing.expect((try reader.invertedIndex("body")) != null);
}

test "buildSegmentFromText emits typed doc values from stored JSON" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildSegmentFromText(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"alpha\",\"price\":10.5,\"published_at\":\"2026-01-02T03:04:05Z\",\"location\":{\"lat\":37.7749,\"lon\":-122.4194},\"active\":true}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha" }},
        },
    }, &analysis_mod.default_analyzer, null);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    const price_section = (try reader.getSection("price", .typed_doc_values)) orelse return error.TestExpectedEqual;
    var price_reader = try typed_dv.TypedDocValuesReader.init(alloc, price_section);
    try std.testing.expectEqual(typed_dv.ValueType.f64_val, price_reader.value_type);
    try std.testing.expectEqual(@as(?f64, 10.5), try price_reader.getF64(0));

    const ts_section = (try reader.getSection("published_at", .typed_doc_values)) orelse return error.TestExpectedEqual;
    var ts_reader = try typed_dv.TypedDocValuesReader.init(alloc, ts_section);
    try std.testing.expectEqual(typed_dv.ValueType.u64_val, ts_reader.value_type);
    try std.testing.expect((try ts_reader.getU64(0)) != null);

    const geo_section = (try reader.getSection("location", .typed_doc_values)) orelse return error.TestExpectedEqual;
    var geo_reader = try typed_dv.TypedDocValuesReader.init(alloc, geo_section);
    try std.testing.expectEqual(typed_dv.ValueType.geo_point, geo_reader.value_type);
    const point = (try geo_reader.getGeoPoint(0)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 37.7749), point.lat, 0.00001);
    try std.testing.expectApproxEqAbs(@as(f64, -122.4194), point.lon, 0.00001);

    const bool_section = (try reader.getSection("active", .typed_doc_values)) orelse return error.TestExpectedEqual;
    var bool_reader = try typed_dv.TypedDocValuesReader.init(alloc, bool_section);
    try std.testing.expectEqual(typed_dv.ValueType.bool_val, bool_reader.value_type);
    try std.testing.expectEqual(@as(?bool, true), try bool_reader.getBool(0));
}

test "buildSegmentFromText omits multi-valued typed doc values" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildSegmentFromTextWithAnalysisOptions(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"alpha\",\"price\":10,\"scores\":[3,5]}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha" }},
        },
        .{
            .id = "doc2",
            .stored_data = "{\"title\":\"beta\",\"price\":20,\"scores\":[7]}",
            .text_fields = &.{.{ .field_name = "title", .text = "beta" }},
        },
    }, &analysis_mod.default_analyzer, .{}, .{
        .recursive_typed_fields = true,
    });
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    const price_section = (try reader.getSection("price", .typed_doc_values)) orelse return error.TestExpectedEqual;
    var price_reader = try typed_dv.TypedDocValuesReader.init(alloc, price_section);
    try std.testing.expectEqual(typed_dv.ValueType.f64_val, price_reader.value_type);
    try std.testing.expectEqual(@as(?f64, 10.0), try price_reader.getF64(0));
    try std.testing.expectEqual(@as(?f64, 20.0), try price_reader.getF64(1));

    try std.testing.expect((try reader.getSection("scores", .typed_doc_values)) == null);
}

test "buildSegmentFromText declines sort metadata for multi-valued projected fields" {
    const alloc = std.testing.allocator;

    const typed_fields = [_]TypedFieldValue{
        .{
            .field_name = "price",
            .value_type = .f64_val,
            .value = .{ .f64_val = 10.0 },
        },
        .{
            .field_name = "price",
            .value_type = .f64_val,
            .value = .{ .f64_val = 12.0 },
        },
    };
    const index_sort = [_]segment_mod.SegmentIndexSortField{
        .{ .field = "price" },
        .{ .field = "_id" },
    };

    const bytes = try buildSegmentFromTextWithAnalysisOptions(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"price\":10}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha" }},
            .typed_fields = &typed_fields,
        },
    }, &analysis_mod.default_analyzer, .{}, .{
        .index_sort = &index_sort,
    });
    defer alloc.free(bytes);
    var reader = try segment_mod.SegmentReader.init(alloc, bytes);
    defer reader.deinit();
    const sort_fields = try reader.indexSortFieldsAlloc(alloc);
    defer if (sort_fields) |fields| segment_mod.freeIndexSortFields(alloc, fields);
    try std.testing.expect(sort_fields == null);
    try std.testing.expect((try reader.getSection("price", .typed_doc_values)) == null);
    try std.testing.expectEqualStrings("doc1", (try reader.storedDoc(0)).?.id);
}

test "buildSegmentFromText rejects non-finite projected typed values" {
    const alloc = std.testing.allocator;

    const typed_fields = [_]TypedFieldValue{.{
        .field_name = "price",
        .value_type = .f64_val,
        .value = .{ .f64_val = std.math.nan(f64) },
    }};
    const index_sort = [_]segment_mod.SegmentIndexSortField{
        .{ .field = "price" },
        .{ .field = "_id" },
    };

    try std.testing.expectError(error.InvalidData, buildSegmentFromTextWithAnalysisOptions(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"price\":10}",
            .text_fields = &.{.{ .field_name = "title", .text = "alpha" }},
            .typed_fields = &typed_fields,
        },
    }, &analysis_mod.default_analyzer, .{}, .{
        .index_sort = &index_sort,
    }));
}

test "buildSegmentFromText uses configured custom datetime parsers for typed doc values" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildSegmentFromText(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"published_at\":\"01/02/2025 3:04PM\"}",
            .text_fields = &.{.{ .field_name = "published_at", .text = "01/02/2025 3:04PM" }},
        },
    }, &analysis_mod.default_analyzer, "{\"analysis_config\":{\"default_datetime_parser\":\"queryDT\",\"field_date_time_parsers\":{\"published_at\":\"queryDT\"},\"date_time_parsers\":{\"queryDT\":{\"type\":\"sanitizedgo\",\"config\":{\"layouts\":[\"02/01/2006 3:04PM\"]}}}}}");
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    const ts_section = (try reader.getSection("published_at", .typed_doc_values)) orelse return error.TestExpectedEqual;
    var ts_reader = try typed_dv.TypedDocValuesReader.init(alloc, ts_section);
    try std.testing.expectEqual(typed_dv.ValueType.u64_val, ts_reader.value_type);
    try std.testing.expect((try ts_reader.getU64(0)) != null);
}

test "analysis config resolves built-in filter names and configured filter types" {
    const alloc = std.testing.allocator;
    const cfg_json =
        \\{"analysis_config":{
        \\  "field_analyzers":{"code":"code_analyzer","compound":"compound_analyzer"},
        \\  "token_filters":{
        \\    "pairs":{"type":"shingle","config":{"min":1,"max":2,"separator":"none"}},
        \\    "tails":{"type":"suffix","config":{"min":3,"max":8}},
        \\    "short":{"type":"length","config":{"min":2,"max":10}},
        \\    "stems_de":{"type":"stemmer","config":{"language":"german"}}
        \\  },
        \\  "analyzers":{
        \\    "code_analyzer":{"type":"custom","config":{"tokenizer":"whitespace","token_filters":["camel_case","unique","short"]}},
        \\    "compound_analyzer":{"type":"custom","config":{"tokenizer":"unicode","token_filters":["lowercase","pairs","tails","stems_de"]}}
        \\  }
        \\}}
    ;

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const cfg = try parseTextAnalysisConfig(arena_state.allocator(), cfg_json);
    try std.testing.expectEqual(@as(usize, 2), cfg.analyzers.len);

    const code = resolveFieldAnalyzer("code", cfg) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), code.filters.len);
    try std.testing.expect(code.filters[0] == .camel_case);
    try std.testing.expect(code.filters[1] == .unique);
    try std.testing.expect(code.filters[2] == .length);
    const code_tokens = try code.analyze(alloc, "parseHttpRequest parseHttpRequest");
    defer analysis_mod.Analyzer.freeTokens(alloc, code_tokens);
    try std.testing.expectEqual(@as(usize, 3), code_tokens.len);
    try std.testing.expectEqualStrings("parse", code_tokens[0].term);
    try std.testing.expectEqualStrings("http", code_tokens[1].term);
    try std.testing.expectEqualStrings("request", code_tokens[2].term);

    const compound = resolveFieldAnalyzer("compound", cfg) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 4), compound.filters.len);
    try std.testing.expect(compound.filters[0] == .lowercase);
    switch (compound.filters[1]) {
        .shingle => |shingle| {
            try std.testing.expectEqual(@as(u8, 1), shingle.min);
            try std.testing.expectEqual(@as(u8, 2), shingle.max);
            try std.testing.expect(shingle.separator == .none);
        },
        else => return error.TestExpectedEqual,
    }
    switch (compound.filters[2]) {
        .suffix => |suffix| {
            try std.testing.expectEqual(@as(u8, 3), suffix.min);
            try std.testing.expectEqual(@as(u8, 8), suffix.max);
        },
        else => return error.TestExpectedEqual,
    }
    switch (compound.filters[3]) {
        .stemmer_lang => |lang| try std.testing.expect(lang == .german),
        else => return error.TestExpectedEqual,
    }

    // Unknown filter names and languages are rejected rather than ignored.
    try std.testing.expectError(error.InvalidArgument, parseTextAnalysisConfig(
        arena_state.allocator(),
        "{\"analysis_config\":{\"analyzers\":{\"a\":{\"type\":\"custom\",\"config\":{\"tokenizer\":\"unicode\",\"token_filters\":[\"no_such_filter\"]}}}}}",
    ));
    try std.testing.expectError(error.InvalidArgument, parseTextAnalysisConfig(
        arena_state.allocator(),
        "{\"analysis_config\":{\"token_filters\":{\"x\":{\"type\":\"stemmer\",\"config\":{\"language\":\"klingon\"}}}}}",
    ));
}

test "buildSegmentFromText uses configured custom field analyzer" {
    const alloc = std.testing.allocator;
    const cfg_json = "{\"analysis_config\":{\"field_analyzers\":{\"title\":\"tri_edge_analyzer\"},\"token_filters\":{\"tri_edge\":{\"type\":\"edge_ngram\",\"config\":{\"min\":3,\"max\":5}}},\"analyzers\":{\"tri_edge_analyzer\":{\"type\":\"custom\",\"config\":{\"tokenizer\":\"unicode\",\"token_filters\":[\"to_lower\",\"tri_edge\"]}}}}}";

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const cfg = try parseTextAnalysisConfig(arena_state.allocator(), cfg_json);
    try std.testing.expectEqual(@as(usize, 1), cfg.analyzers.len);
    const analyzer = resolveFieldAnalyzer("title", cfg) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), analyzer.filters.len);
    switch (analyzer.filters[1]) {
        .edge_ngram => |edge| {
            try std.testing.expectEqual(@as(u8, 3), edge.min);
            try std.testing.expectEqual(@as(u8, 5), edge.max);
        },
        else => return error.TestExpectedEqual,
    }
    const tokens = try analyzer.analyze(alloc, "hello");
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("hel", tokens[0].term);
    try std.testing.expectEqualStrings("hell", tokens[1].term);
    try std.testing.expectEqualStrings("hello", tokens[2].term);

    const seg_bytes = try buildSegmentFromText(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"hello\"}",
            .text_fields = &.{.{ .field_name = "title", .text = "hello" }},
        },
    }, &analysis_mod.default_analyzer, cfg_json);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    var inverted_index = (try reader.invertedIndex("title")).?;
    try std.testing.expect(inverted_index.lookup("hel") != null);
    try std.testing.expect(inverted_index.lookup("hell") != null);
    try std.testing.expect(inverted_index.lookup("hello") != null);
}

test "buildSegmentFromText uses configured custom tokenizer and char filter" {
    const alloc = std.testing.allocator;
    const cfg_json =
        "{\"analysis_config\":{\"char_filters\":{\"strip_html_alias\":{\"type\":\"html\"}},\"token_filters\":{\"tri_gram_filter\":{\"type\":\"ngram\",\"config\":{\"min\":3,\"max\":3}}},\"tokenizers\":{\"whitespace_alias\":{\"type\":\"whitespace\"}},\"field_analyzers\":{\"title\":\"tri_html_analyzer\"},\"analyzers\":{\"tri_html_analyzer\":{\"type\":\"custom\",\"config\":{\"tokenizer\":\"whitespace_alias\",\"char_filters\":[\"strip_html_alias\"],\"token_filters\":[\"to_lower\",\"tri_gram_filter\"]}}}}}";

    const seg_bytes = try buildSegmentFromText(alloc, &.{
        .{
            .id = "doc1",
            .stored_data = "{\"title\":\"<b>Hello</b>\"}",
            .text_fields = &.{.{ .field_name = "title", .text = "<b>Hello</b>" }},
        },
    }, &analysis_mod.default_analyzer, cfg_json);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    var inverted_index = (try reader.invertedIndex("title")).?;
    try std.testing.expect(inverted_index.lookup("hel") != null);
    try std.testing.expect(inverted_index.lookup("ell") != null);
    try std.testing.expect(inverted_index.lookup("llo") != null);
}

test "initial streamed sections preserve sparse conflicts sorting and allocation failure ownership" {
    const Scenario = struct {
        fn run(backing: Allocator) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            const a = stable.allocator();
            const docs = [_]TextDocument{
                .{ .id = "b", .stored_data = "body b", .doc_ordinal = 20, .text_fields = &.{.{ .field_name = "title", .text = "beta" }}, .typed_fields = &.{
                    .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = 2 } },
                    .{ .field_name = "label", .value_type = .bytes_val, .value = .{ .bytes_val = "second" } },
                    .{ .field_name = "conflict", .value_type = .u64_val, .value = .{ .u64_val = 1 } },
                } },
                .{ .id = "a", .stored_data = "body a", .doc_ordinal = 10, .text_fields = &.{.{ .field_name = "title", .text = "alpha" }}, .typed_fields = &.{
                    .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = 1 } },
                    .{ .field_name = "conflict", .value_type = .bytes_val, .value = .{ .bytes_val = "incompatible" } },
                } },
            };
            const bytes = try buildSegmentFromTextWithAnalysisOptions(a, &docs, &analysis_mod.default_analyzer, .{}, .{
                .index_sort = &.{.{ .field = "rank", .desc = false }},
            });
            defer a.free(bytes);
            var reader = try segment_mod.SegmentReader.init(a, bytes);
            defer reader.deinit();
            try std.testing.expectEqualStrings("a", (try reader.storedDoc(0)).?.id);
            try std.testing.expectEqual(@as(?u32, 10), try reader.docOrdinal(0));
            try std.testing.expect((try reader.getSection("conflict", .typed_doc_values)) == null);
            var labels = (try reader.typedDocValuesScoped(a, "label")).?;
            defer labels.deinit();
            try std.testing.expect((try labels.getBytesAlloc(0)) == null);
            const label = (try labels.getBytesAlloc(1)).?;
            defer a.free(label);
            try std.testing.expectEqualStrings("second", label);
        }
    };
    try Scenario.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "initial typed streaming reduces heap staging across large projected columns" {
    const a = std.testing.allocator;
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    const count = 1024;
    const columns = 4;
    const names = [_][]const u8{ "a", "b", "c", "d" };
    const input = try a.alloc(u8, count * columns * 1024);
    defer a.free(input);
    var random = std.Random.DefaultPrng.init(42);
    random.random().bytes(input);
    const fields = try a.alloc(TypedFieldValue, count * columns);
    defer a.free(fields);
    const docs = try a.alloc(TextDocument, count);
    defer a.free(docs);
    for (docs, 0..) |*doc, row| {
        for (0..columns) |column| {
            const slot = row * columns + column;
            fields[slot] = .{ .field_name = names[column], .value_type = .bytes_val, .value = .{ .bytes_val = input[slot * 1024 ..][0..1024] } };
        }
        doc.* = .{ .id = "doc", .stored_data = "{}", .text_fields = &.{}, .typed_fields = fields[row * columns ..][0..columns] };
    }
    // Output lives outside both counters, modelling a private file-backed sink.
    // The baseline reproduces owning collection plus all-section staging used
    // by the previous initial builder; immutable input is excluded in both.
    var reference_output = segment_mod.MemorySegmentSink.init(a);
    defer reference_output.deinit();
    var reference_sink = reference_output.sink();
    var reference = Budget{ .backing = a, .limit = std.math.maxInt(usize) };
    const baseline_start = platform_time.monotonicNs();
    {
        const ba = reference.allocator();
        var builder = segment_mod.SegmentWriter.init(ba);
        defer builder.deinit();
        for (docs) |doc| try builder.addStoredDocBorrowed(doc.id, doc.stored_data);
        var writers: [columns]typed_dv.TypedDocValuesWriter = undefined;
        for (&writers) |*writer| writer.* = typed_dv.TypedDocValuesWriter.init(ba, .bytes_val, typed_dv.default_chunk_size);
        defer for (&writers) |*writer| writer.deinit();
        for (docs, 0..) |doc, row| for (doc.typed_fields.?, 0..) |field, column| try writers[column].add(@intCast(row), field.value);
        for (&writers, names) |*writer, name| {
            const data = try writer.build();
            errdefer ba.free(data);
            const field = try builder.addField(name);
            try builder.addSectionOwned(field, .typed_doc_values, data);
        }
        try builder.writeToSink(&reference_sink);
    }
    const baseline_ns = platform_time.monotonicNs() - baseline_start;
    try std.testing.expectEqual(@as(usize, 0), reference.live);
    var streamed_output = segment_mod.MemorySegmentSink.init(a);
    defer streamed_output.deinit();
    var streamed_sink = streamed_output.sink();
    var streamed = Budget{ .backing = a, .limit = std.math.maxInt(usize) };
    const streamed_start = platform_time.monotonicNs();
    try writeSegmentFromTextWithAnalysisOptions(streamed.allocator(), docs, &analysis_mod.default_analyzer, .{}, .{}, &streamed_sink);
    const streamed_ns = platform_time.monotonicNs() - streamed_start;
    try std.testing.expectEqual(@as(usize, 0), streamed.live);
    try std.testing.expect(streamed.peak < reference.peak / 3);
    var reader = try segment_mod.SegmentReader.init(a, streamed_output.out.items);
    defer reader.deinit();
    for (names, 0..) |name, column| {
        var values = (try reader.typedDocValuesScoped(a, name)).?;
        defer values.deinit();
        const value = (try values.getBytesAlloc(count - 1)).?;
        defer a.free(value);
        try std.testing.expectEqualSlices(u8, fields[(count - 1) * columns + column].value.bytes_val, value);
    }
    std.debug.print("LITE_INITIAL_STREAM input_bytes={d} baseline_peak={d} streamed_peak={d} baseline_allocs={d} streamed_allocs={d} baseline_ns={d} streamed_ns={d}\n", .{ input.len, reference.peak, streamed.peak, reference.alloc_calls, streamed.alloc_calls, baseline_ns, streamed_ns });
}

test "large typed inputs split and reclaim production scratch between rows" {
    const a = std.testing.allocator;
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    const count = 16;
    const width = 600 * 1024;
    const input = try a.alloc(u8, count * width);
    defer a.free(input);
    var random = std.Random.DefaultPrng.init(59);
    random.random().bytes(input);
    var fields: [count]TypedFieldValue = undefined;
    var docs: [count]TextDocument = undefined;
    for (&fields, &docs, 0..) |*field, *doc, i| {
        field.* = .{ .field_name = "payload", .value_type = .bytes_val, .value = .{ .bytes_val = input[i * width ..][0..width] } };
        doc.* = .{ .id = "doc", .stored_data = "{}", .text_fields = &.{}, .typed_fields = fields[i..][0..1] };
    }
    const split = splitTextDocumentsForBuildBudget(&docs, 0, .{ .target_build_memory_bytes = 1024 * 1024, .target_segment_bytes = 1024 * 1024 });
    try std.testing.expectEqual(@as(usize, 1), split.end);
    try std.testing.expect(estimateTextDocumentInputBytes(docs[0]) >= width);
    try std.testing.expect(estimateTextDocumentSegmentBytes(docs[0]) >= width);
    // Borrowed aliases count once in input memory, but every encoded typed
    // payload still counts toward artifact splitting even when shared with text.
    var source: std.json.ObjectMap = .empty;
    defer source.deinit(a);
    try source.put(a, "payload", .{ .string = input[0..width] });
    const aliased = TextDocument{ .id = "row", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = input[0..width] }}, .typed_source = .{ .object = source } };
    try std.testing.expect(estimateTextDocumentInputBytes(aliased) < 2 * width);
    try std.testing.expect(estimateTextDocumentSegmentBytes(aliased) >= 2 * width);
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    var heap = Budget{ .backing = textBuildScratchAllocator() };
    var manager = resource_manager_mod.ResourceManager.init(.{});
    try writeSegmentFromTextWithAnalysisOptions(heap.allocator(), &docs, &analysis_mod.default_analyzer, .{}, .{ .resource_manager = &manager }, &sink);
    try std.testing.expectEqual(@as(usize, 0), heap.live);
    try std.testing.expect(heap.peak < 4 * 1024 * 1024);
    const stats = manager.sliceStats(.full_text_build_working_set);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes >= input.len);
    std.debug.print("production typed scratch source={d} peak={d} admitted_peak={d}\n", .{ input.len, heap.peak, stats.peak_bytes });
}

test "bounded postings runs preserve sorted sparse fields and reduce production heap" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var input_arena = std.heap.ArenaAllocator.init(a);
    defer input_arena.deinit();
    const input = input_arena.allocator();
    const count = 2048;
    const docs = try input.alloc(TextDocument, count);
    for (docs, 0..) |*doc, i| {
        const fields = try input.alloc(TextField, if (i % 3 == 0) 1 else 2);
        const text = try std.fmt.allocPrint(input, "common phrase row{d} word{d} token{d} unique{d} value{d} entry{d} data{d} record{d}", .{ i, i, i, i, i, i, i, i });
        fields[0] = .{ .field_name = "body", .text = text };
        if (fields.len == 2) fields[1] = .{ .field_name = "title", .text = text };
        const typed = try input.alloc(TypedFieldValue, 1);
        typed[0] = .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = count - i } };
        doc.* = .{ .id = try std.fmt.allocPrint(input, "id{d}", .{i}), .stored_data = "{}", .text_fields = fields, .typed_fields = typed };
    }
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var baseline = Budget{ .backing = textBuildScratchAllocator() };
    var bounded = Budget{ .backing = textBuildScratchAllocator() };
    var before = segment_mod.MemorySegmentSink.init(a);
    defer before.deinit();
    var after = segment_mod.MemorySegmentSink.init(a);
    defer after.deinit();
    var before_sink = before.sink();
    var after_sink = after.sink();
    var options = BuildTextOptions{ .index_sort = &.{.{ .field = "rank", .desc = false }} };
    const baseline_start = platform_time.monotonicNs();
    try writeSegmentFromTextWithAnalysisOptions(baseline.allocator(), docs, &analysis_mod.default_analyzer, .{}, options, &before_sink);
    const baseline_ns = platform_time.monotonicNs() - baseline_start;
    options.postings_run_io = std.testing.io;
    options.postings_run_directory = directory;
    options.postings_run_target_bytes = 128 * 1024;
    var run_profile = BuildTextProfile{};
    options.profile = &run_profile;
    options.profile_timings = false;
    options.profile_working_set = false;
    const bounded_start = platform_time.monotonicNs();
    try writeSegmentFromTextWithAnalysisOptions(bounded.allocator(), docs, &analysis_mod.default_analyzer, .{}, options, &after_sink);
    const bounded_ns = platform_time.monotonicNs() - bounded_start;
    try std.testing.expectEqual(@as(usize, 0), baseline.live);
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expect(bounded.peak < baseline.peak);
    try std.testing.expect(run_profile.postings_run_count > 16);
    try std.testing.expect(run_profile.postings_run_merge_count > 0);
    try std.testing.expectEqual(@as(u64, 1), run_profile.postings_spool_count);
    try std.testing.expectEqual(@as(u64, 16), run_profile.postings_run_max_fan_in);
    var before_reader = try segment_mod.SegmentReader.init(a, before.out.items);
    defer before_reader.deinit();
    var after_reader = try segment_mod.SegmentReader.init(a, after.out.items);
    defer after_reader.deinit();
    for (0..count) |doc| try std.testing.expectEqualStrings((try before_reader.storedDoc(@intCast(doc))).?.id, (try after_reader.storedDoc(@intCast(doc))).?.id);
    for ([_][]const u8{ "body", "title" }) |name| {
        var left = (try before_reader.invertedIndexScoped(a, name)).?;
        defer left.deinit();
        var right = (try after_reader.invertedIndexScoped(a, name)).?;
        defer right.deinit();
        try std.testing.expectEqual(left.doc_count, right.doc_count);
        try std.testing.expectEqual(left.total_field_len, right.total_field_len);
        for ([_][]const u8{ "common", "phrase", "row0", "row17", "word2047" }) |term| {
            const l = try left.lookup(term);
            const r = try right.lookup(term);
            try std.testing.expectEqual(l == null, r == null);
            if (l) |lookup| {
                try std.testing.expectEqual(lookup.docFreq(), r.?.docFreq());
                var li = try lookup.iterator(a);
                defer li.deinit();
                var ri = try r.?.iterator(a);
                defer ri.deinit();
                while (try li.next()) |hit| {
                    const other = (try ri.next()).?;
                    try std.testing.expectEqual(hit.doc_id, other.doc_id);
                    try std.testing.expectEqual(hit.freq, other.freq);
                    try std.testing.expectEqual(hit.norm, other.norm);
                    try std.testing.expectEqualSlices(u32, hit.positions, other.positions);
                }
                try std.testing.expect((try ri.next()) == null);
            }
        }
    }
    var production = Budget{ .backing = textBuildScratchAllocator() };
    var production_output = segment_mod.MemorySegmentSink.init(a);
    defer production_output.deinit();
    var production_sink = production_output.sink();
    options.postings_run_target_bytes = 8 * 1024 * 1024;
    const production_start = platform_time.monotonicNs();
    try writeSegmentFromTextWithAnalysisOptions(production.allocator(), docs, &analysis_mod.default_analyzer, .{}, options, &production_sink);
    const production_ns = platform_time.monotonicNs() - production_start;
    try std.testing.expectEqual(@as(usize, 0), production.live);
    try std.testing.expect(production.peak < baseline.peak);
    std.debug.print("postings production default peak={d} ns={d}\n", .{ production.peak, production_ns });
    var remaining = tmp.dir.iterate();
    try std.testing.expect((try remaining.next(std.testing.io)) == null);
    std.debug.print("postings production peak baseline={d} bounded={d} ns baseline={d} bounded={d}\n", .{ baseline.peak, bounded.peak, baseline_ns, bounded_ns });
}

test "postings run allocation failures abort every private owner including fan in merge" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const Scenario = struct {
        fn run(backing: Allocator, dir: []const u8) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            const alloc = stable.allocator();
            var docs: [16]TextDocument = undefined;
            for (&docs) |*doc| doc.* = .{ .id = "row", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "common phrase" }}, .typed_fields = &.{} };
            const data = try buildSegmentFromTextWithAnalysisOptions(alloc, &docs, &analysis_mod.default_analyzer, .{}, .{
                .postings_run_io = std.testing.io,
                .postings_run_directory = dir,
                .postings_run_target_bytes = 1,
            });
            defer alloc.free(data);
        }
    };
    try Scenario.run(a, directory);
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{directory});
    var remaining = tmp.dir.iterate();
    try std.testing.expect((try remaining.next(std.testing.io)) == null);
}

test "construction budget rejects actual allocations and translates admission failures" {
    const a = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{ .soft_limit_bytes = 1024, .hard_limit_bytes = 2048 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    try std.testing.expectError(error.ResourceBudgetExceeded, buildSegmentFromTextWithAnalysisOptions(a, &.{
        .{ .id = "row", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "word" }}, .typed_fields = &.{} },
    }, &analysis_mod.default_analyzer, .{}, .{ .resource_manager = &manager }));
    const stats = manager.sliceStats(.full_text_build_working_set);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes <= 2048);
}

test "postings spool carries two merge levels with one file and bounded readers" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var docs: [512]TextDocument = undefined;
    for (&docs) |*doc| doc.* = .{ .id = "row", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "common phrase" }}, .typed_fields = &.{} };
    var profile = BuildTextProfile{};
    const data = try buildSegmentFromTextWithAnalysisOptions(a, &docs, &analysis_mod.default_analyzer, .{}, .{
        .postings_run_io = std.testing.io,
        .postings_run_directory = directory,
        .postings_run_target_bytes = 1,
        .profile = &profile,
        .profile_timings = false,
        .profile_working_set = false,
    });
    defer a.free(data);
    try std.testing.expectEqual(@as(u64, 512), profile.postings_run_count);
    // 32 level-zero carries and two level-one carries, with no repeated
    // prefix compactions or extra descriptors as field/run count grows.
    try std.testing.expectEqual(@as(u64, 34), profile.postings_run_merge_count);
    try std.testing.expectEqual(@as(u64, 1), profile.postings_spool_count);
    try std.testing.expectEqual(@as(u64, 16), profile.postings_run_max_fan_in);
    var reader = try segment_mod.SegmentReader.init(a, data);
    defer reader.deinit();
    var field = (try reader.invertedIndexScoped(a, "body")).?;
    defer field.deinit();
    try std.testing.expectEqual(@as(u32, 512), field.doc_count);
    try std.testing.expectEqual(@as(u64, 1024), field.total_field_len);
    const lookup = (try field.lookup("phrase")).?;
    var iterator = try lookup.iterator(a);
    defer iterator.deinit();
    for (0..512) |id| {
        const hit = (try iterator.next()).?;
        try std.testing.expectEqual(@as(u32, @intCast(id)), hit.doc_id);
        try std.testing.expectEqualSlices(u32, &.{1}, hit.positions);
    }
    try std.testing.expect((try iterator.next()) == null);
    var remaining = tmp.dir.iterate();
    try std.testing.expect((try remaining.next(std.testing.io)) == null);
}

test "sparse late postings keep local run norms and bounded physical spool" {
    const a = std.testing.allocator;
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = textBuildScratchAllocator() };
    const alloc = backing.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var spool: ?*PostingRun = null;
    defer if (spool) |owner| owner.deinit();
    var builders: [64]FieldPostingsBuilder = undefined;
    var initialized: usize = 0;
    defer for (builders[0..initialized]) |*builder| builder.deinit(alloc);
    for (&builders) |*builder| {
        builder.* = try FieldPostingsBuilder.init(alloc);
        initialized += 1;
        try builder.addDocument(49_999, &.{.{ .term = "word", .freq = 1, .norm = 1 }});
        try std.testing.expectEqual(@as(usize, 1), builder.builder.doc_norms.items.len);
    }
    const peak = backing.peak;
    const options = BuildTextOptions{ .postings_run_io = std.testing.io, .postings_run_directory = directory };
    for (&builders) |*builder| try builder.spill(alloc, options, 50_000, &spool);
    std.debug.print("sparse local runs scratch={d} spool={d} (old 19288832 / 3207680)\n", .{ peak, spool.?.persisted });
    try std.testing.expect(peak < 128 * 1024);
    try std.testing.expect(spool.?.persisted < 32 * 1024);
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    try builders[0].mergeRuns(alloc, &sink, 50_000);
    var reader = try inverted.ScopedInvertedIndexReader.initContiguous(a, output.out.items);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 1), reader.doc_count);
    var result = (try reader.lookup("word")).?;
    var iterator = try result.iterator(a);
    defer iterator.deinit();
    try std.testing.expectEqual(@as(u32, 49_999), (try iterator.next()).?.doc_id);
    try std.testing.expect((try iterator.next()) == null);
}

test "typed staging preserves sparse values and discards late conflicting columns" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var input = std.heap.ArenaAllocator.init(a);
    defer input.deinit();
    const docs = try input.allocator().alloc(TextDocument, 4096);
    for (docs, 0..) |*doc, i| {
        const fields = try input.allocator().alloc(TypedFieldValue, 2);
        fields[0] = .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = i } };
        fields[1] = if (i == docs.len - 1) .{ .field_name = "conflict", .value_type = .bytes_val, .value = .{ .bytes_val = "late" } } else .{ .field_name = "conflict", .value_type = .u64_val, .value = .{ .u64_val = i } };
        doc.* = .{ .id = "row", .stored_data = "{}", .text_fields = &.{}, .typed_fields = fields[0..if (i % 3 == 0) 1 else 2] };
    }
    // Ensure the conflict arrives after several chunks have been staged.
    docs[docs.len - 1].typed_fields = try input.allocator().dupe(TypedFieldValue, &.{ .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = docs.len - 1 } }, .{ .field_name = "conflict", .value_type = .bytes_val, .value = .{ .bytes_val = "late" } } });
    const data = try buildSegmentFromTextWithAnalysisOptions(a, docs, &analysis_mod.default_analyzer, .{}, .{ .postings_run_io = std.testing.io, .postings_run_directory = directory });
    defer a.free(data);
    var reader = try segment_mod.SegmentReader.init(a, data);
    defer reader.deinit();
    try std.testing.expect((try reader.typedDocValuesScoped(a, "conflict")) == null);
    var column = (try reader.typedDocValuesScoped(a, "rank")).?;
    defer column.deinit();
    var cursor = typed_dv.TypedDocValuesReader.Cursor.init(&column);
    defer cursor.deinit();
    for (0..docs.len) |i| {
        const value = (try cursor.next()).?;
        try std.testing.expectEqual(@as(u32, @intCast(i)), value.doc_id);
        try std.testing.expectEqual(@as(u64, i), value.value.u64_val);
    }
    try std.testing.expect((try cursor.next()) == null);
}

test "postings spool reclaims dead physical ranges without invalidating logical views" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const spool = try PostingRun.create(a, std.testing.io, directory);
    defer spool.deinit();
    const payload = try a.alloc(u8, 800 * 1024);
    defer a.free(payload);
    var offsets: [4]usize = undefined;
    for (&offsets, 0..) |*offset, i| {
        offset.* = spool.len();
        @memset(payload, @intCast(i + 1));
        try spool.appendSlice(payload);
        try spool.seal(offset.*);
    }
    const logical_end = spool.len();
    var source = (try spool.view()).source;
    spool.releaseRange(offsets[0]);
    spool.releaseRange(offsets[2]);
    try spool.compact();
    try std.testing.expectEqual(logical_end / 2, spool.persisted);
    try std.testing.expectEqual(logical_end / 2, (try spool.file.stat(std.testing.io)).size);
    var out: [73]u8 = undefined;
    try std.testing.expectError(error.EndOfStream, source.readInto(offsets[0], &out));
    try std.testing.expectError(error.EndOfStream, source.readInto(offsets[2], &out));
    try source.readInto(offsets[1] + 777, &out);
    try std.testing.expectEqualSlices(u8, &@as([73]u8, @splat(2)), &out);
    try source.readInto(offsets[3] + 777, &out);
    try std.testing.expectEqualSlices(u8, &@as([73]u8, @splat(4)), &out);
    const next = spool.len();
    try spool.appendSlice("new tail");
    try spool.seal(next);
    source = (try spool.view()).source;
    var tail: [8]u8 = undefined;
    try source.readInto(next, &tail);
    try std.testing.expectEqualStrings("new tail", &tail);
}

test "private spool reserves filesystem headroom before writing" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var manager = resource_manager_mod.ResourceManager.init(.{ .disk_safety_floor_bytes = std.math.maxInt(u64), .disk_safety_floor_divisor = 0 });
    defer manager.deinit(a);
    const spool = try PostingRun.createWithResources(a, std.testing.io, directory, &manager);
    defer spool.deinit();
    try std.testing.expectError(error.CapacityUnavailable, spool.appendSlice("denied"));
    try std.testing.expectEqual(@as(usize, 0), spool.len());
    try std.testing.expectEqual(@as(u64, 0), (try spool.file.stat(std.testing.io)).size);
}

test "typed staging allocation failures release pending chunks and private extents" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const Scenario = struct {
        fn run(backing: Allocator, dir: []const u8) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            const alloc = stable.allocator();
            const bytes: [40 * 1024]u8 = @splat('x');
            const field = TypedFieldValue{ .field_name = "value", .value_type = .bytes_val, .value = .{ .bytes_val = &bytes } };
            var docs: [4]TextDocument = undefined;
            for (&docs) |*doc| doc.* = .{ .id = "row", .stored_data = "{}", .text_fields = &.{}, .typed_fields = &.{field} };
            const data = try buildSegmentFromTextWithAnalysisOptions(alloc, &docs, &analysis_mod.default_analyzer, .{}, .{ .postings_run_io = std.testing.io, .postings_run_directory = dir });
            defer alloc.free(data);
        }
    };
    try Scenario.run(a, directory);
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{directory});
}

test "typed collection staging bounds descriptor memory for fifty thousand rows" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var input = std.heap.ArenaAllocator.init(a);
    defer input.deinit();
    const docs = try input.allocator().alloc(TextDocument, 50_000);
    for (docs, 0..) |*doc, i| {
        const fields = try input.allocator().alloc(TypedFieldValue, 8);
        for (fields, 0..) |*field, col| field.* = .{ .field_name = try std.fmt.allocPrint(input.allocator(), "field-{d}", .{col}), .value_type = .u64_val, .value = .{ .u64_val = i + col } };
        doc.* = .{ .id = "row", .stored_data = "{}", .text_fields = &.{}, .typed_fields = fields, .doc_ordinal = @intCast(i + 1) };
    }
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var baseline = Budget{ .backing = textBuildScratchAllocator() };
    var staged = Budget{ .backing = textBuildScratchAllocator() };
    var first = segment_mod.MemorySegmentSink.init(a);
    defer first.deinit();
    var second = segment_mod.MemorySegmentSink.init(a);
    defer second.deinit();
    var first_sink = first.sink();
    var second_sink = second.sink();
    var options = BuildTextOptions{ .store_documents = false, .profile_timings = false, .profile_working_set = false };
    try writeSegmentFromTextWithAnalysisOptions(baseline.allocator(), docs, &analysis_mod.default_analyzer, .{}, options, &first_sink);
    options.postings_run_io = std.testing.io;
    options.postings_run_directory = directory;
    try writeSegmentFromTextWithAnalysisOptions(staged.allocator(), docs, &analysis_mod.default_analyzer, .{}, options, &second_sink);
    std.debug.print("typed staging rows=50000 columns=8 scratch baseline={d} staged={d}\n", .{ baseline.peak, staged.peak });
    try std.testing.expect(staged.peak * 4 < baseline.peak);
    try std.testing.expectEqual(@as(usize, 0), staged.live);
    var reader = try segment_mod.SegmentReader.init(a, second.out.items);
    defer reader.deinit();
    var column = (try reader.typedDocValuesScoped(a, "field-7")).?;
    defer column.deinit();
    var cursor = typed_dv.TypedDocValuesReader.Cursor.init(&column);
    defer cursor.deinit();
    for (0..50_000) |i| {
        const entry = (try cursor.next()).?;
        try std.testing.expectEqual(@as(u64, i + 7), entry.value.u64_val);
    }
    try std.testing.expect((try cursor.next()) == null);
}

test "typed staging checks only touched dynamic columns" {
    const a = std.testing.allocator;
    var fields = TypedFieldCollectors.empty;
    var spool: ?*PostingRun = null;
    defer if (spool) |owner| owner.deinit();
    defer {
        var values = fields.valueIterator();
        while (values.next()) |collector| collector.deinit(a);
        var keys = fields.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        fields.deinit(a);
    }
    var profile = BuildTextProfile{};
    fields.staging = .{ .options = .{ .postings_run_io = std.testing.io, .profile = &profile }, .owner = &spool };
    for (0..10_000) |i| {
        var name: [40]u8 = undefined;
        const key = try std.fmt.bufPrint(&name, "field-{d}", .{i});
        try appendTypedFieldValue(a, &fields, key, @intCast(i), .{ .value_type = .u64_val, .value = .{ .u64_val = i } }, &profile, false);
    }
    try std.testing.expectEqual(@as(u64, 10_000), profile.typed_staging_checks);
    try std.testing.expectEqual(@as(u32, 10_000), fields.count());
    try std.testing.expect(spool == null);
    std.debug.print("dynamic typed columns staging checks=10000 old=50005000\n", .{});
}

test "sparse dense runs survive gaps carry levels and bounded final norms" {
    const a = std.testing.allocator;
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var budget = Budget{ .backing = textBuildScratchAllocator() };
    const alloc = budget.allocator();
    var spool: ?*PostingRun = null;
    defer if (spool) |owner| owner.deinit();
    var builder = try FieldPostingsBuilder.init(alloc);
    defer builder.deinit(alloc);
    const options = BuildTextOptions{ .postings_run_io = std.testing.io, .postings_run_directory = directory };
    // 512 sparse documents force two base-16 carries. Each raw run spans a
    // large hole; both raw and carried norms must stay dense.
    for (0..256) |i| {
        try builder.addDocument(@intCast(i * 200), &.{.{ .term = "word", .freq = 2, .norm = 7 }});
        try builder.addDocument(@intCast(i * 200 + 199), &.{.{ .term = "word", .freq = 2, .norm = 7 }});
        try std.testing.expectEqual(@as(usize, 2), builder.builder.doc_norms.items.len);
        try builder.spill(alloc, options, 51_200, &spool);
    }
    const collected_peak = budget.peak;
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    try builder.mergeRuns(alloc, &sink, 51_200);
    var reader = try inverted.ScopedInvertedIndexReader.initContiguous(a, output.out.items);
    defer reader.deinit();
    var result = (try reader.lookup("word")).?;
    var iterator = try result.iterator(a);
    defer iterator.deinit();
    for (0..256) |i| for ([_]u32{ @intCast(i * 200), @intCast(i * 200 + 199) }) |id| {
        const hit = (try iterator.next()).?;
        try std.testing.expectEqual(id, hit.doc_id);
        try std.testing.expectEqual(@as(u32, 7), try reader.docLength(id));
    };
    try std.testing.expect((try iterator.next()) == null);
    try std.testing.expectEqual(@as(u32, 0), try reader.docLength(1));
    // Increase only the final document space by 20x. Final norm encoding must
    // stream zero padding rather than allocating four million raw norm bytes.
    var larger = segment_mod.MemorySegmentSink.init(a);
    defer larger.deinit();
    var larger_sink = larger.sink();
    const before = budget.peak;
    try builder.mergeRuns(alloc, &larger_sink, 1_024_000);
    try std.testing.expect(budget.peak <= before + 16 * 1024);
    std.debug.print("sparse gap carries collect_peak={d} final_peak={d} enlarged_final_peak={d}\n", .{ collected_peak, before, budget.peak });
}

test "private spool caches tiny metadata and patches pending bytes without flushing" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const spool = try PostingRun.create(a, std.testing.io, directory);
    defer spool.deinit();
    const bytes: [8192]u8 = @splat('a');
    try spool.appendSlice(&bytes);
    try spool.writeAt(0, "head");
    try std.testing.expectEqual(@as(usize, 0), spool.write_calls);
    try spool.seal(0);
    try std.testing.expectEqual(@as(usize, 1), spool.write_calls);
    const view = try spool.view();
    var header: [4]u8 = undefined;
    try view.readInto(0, &header);
    try std.testing.expectEqualStrings("head", &header);
    var byte: [1]u8 = undefined;
    for (0..8192) |i| try view.readInto(i, &byte);
    try std.testing.expectEqual(@as(usize, 1), spool.read_calls);
    std.debug.print("spool tiny reads physical_calls=1 logical_calls=8193 patch_writes=1\n", .{});
}

test "sparse final run sidecars share input caches across many rare terms" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var spool: ?*PostingRun = null;
    defer if (spool) |owner| owner.deinit();
    var builder = try FieldPostingsBuilder.init(a);
    defer builder.deinit(a);
    const options = BuildTextOptions{ .postings_run_io = std.testing.io, .postings_run_directory = directory };
    // Fifteen inputs stay below the carry threshold. Repeated rare terms
    // visit every input and its sparse-ID sidecar in shuffled local order.
    for (0..15) |run| {
        for (0..512) |doc| {
            var term: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&term, "common-prefix-term-{d:0>4}", .{doc * 37 % 512});
            try builder.addDocument(@intCast((run * 512 + doc) * 10), &.{.{ .term = key, .freq = 1, .norm = 7 }});
        }
        try builder.spill(a, options, 76_800, &spool);
    }
    try std.testing.expectEqual(@as(usize, 15), builder.runs.items.len);
    const reads_before = spool.?.read_calls;
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    try builder.mergeRuns(a, &sink, 76_800);
    const physical_reads = spool.?.read_calls - reads_before;
    // One bounded cache per input also serves original-ID lookups. A shared
    // small file cache alone would thrash on thousands of four-byte reads.
    try std.testing.expect(physical_reads < 100);
    var reader = try inverted.ScopedInvertedIndexReader.initContiguous(a, output.out.items);
    defer reader.deinit();
    var result = (try reader.lookup("common-prefix-term-0000")).?;
    var iterator = try result.iterator(a);
    defer iterator.deinit();
    for (0..15) |run| try std.testing.expectEqual(@as(u32, @intCast(run * 5120)), (try iterator.next()).?.doc_id);
    try std.testing.expect((try iterator.next()) == null);
    std.debug.print("sparse sidecar rare terms=512 inputs=15 physical_reads={d}\n", .{physical_reads});
}

test "dynamic text fields spill only queued payloads beyond retained metadata" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var names: [128][32]u8 = undefined;
    var fields: [128]TextField = undefined;
    var docs: [128]TextDocument = undefined;
    for (&docs, &fields, &names, 0..) |*doc, *field, *name, i| {
        const key = try std.fmt.bufPrint(name, "field-{d}", .{i});
        field.* = .{ .field_name = key, .text = "token" };
        doc.* = .{ .id = key, .stored_data = "{}", .text_fields = @as(*[1]TextField, @ptrCast(field)), .typed_fields = &.{} };
    }
    var profile = BuildTextProfile{};
    const bytes = try buildSegmentFromTextWithAnalysisOptions(a, &docs, &analysis_mod.default_analyzer, .{}, .{
        .postings_run_io = std.testing.io,
        .postings_run_directory = directory,
        .postings_run_target_bytes = 1024,
        .profile = &profile,
    });
    defer a.free(bytes);
    try std.testing.expect(profile.postings_spill_checks <= 128);
    var reader = try segment_mod.SegmentReader.init(a, bytes);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 128), reader.doc_count);
    std.debug.print("queued spills documents=128 visits={d} runs={d}\n", .{ profile.postings_spill_checks, profile.postings_run_count });
}

// Test oracle pinned to the pre-stream document accumulator at c4ca33d22f.
// It intentionally retains owned token arrays and arena-grown positions.
const LegacyFieldFixture = struct {
    const Term = struct {
        freq: u32 = 0,
        positions: std.ArrayListUnmanaged(u32) = .empty,
        fn positionSlice(self: *const @This()) []const u32 {
            return self.positions.items;
        }
    };
    token_offset: u32 = 0,
    term_map: std.StringHashMapUnmanaged(Term) = .empty,
    fn add(self: *@This(), a: Allocator, text: []const u8) !void {
        const tokens = try analysis_mod.substring_analyzer.analyze(a, text);
        for (tokens) |token| {
            const slot = try self.term_map.getOrPut(a, token.term);
            if (!slot.found_existing) {
                slot.key_ptr.* = token.term;
                slot.value_ptr.* = .{};
            }
            slot.value_ptr.freq += 1;
            try slot.value_ptr.positions.append(a, self.token_offset + token.position);
        }
        self.token_offset += @intCast(tokens.len);
    }
};
fn postingsFixtureDigest(field: anytype) [32]u8 {
    var combined: [32]u8 = @splat(0);
    var entries = field.term_map.iterator();
    while (entries.next()) |entry| {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, entry.key_ptr.len, .little);
        hash.update(&encoded);
        hash.update(entry.key_ptr.*);
        std.mem.writeInt(u32, encoded[0..4], entry.value_ptr.freq, .little);
        std.mem.writeInt(u32, encoded[4..8], field.token_offset, .little);
        hash.update(&encoded);
        for (entry.value_ptr.positionSlice()) |position| {
            std.mem.writeInt(u32, encoded[0..4], position, .little);
            hash.update(encoded[0..4]);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        for (&combined, digest) |*byte, value| byte.* ^= value;
    }
    return combined;
}

test "introducer.substring streaming preserves repeated field postings and bounded scratch" {
    const a = std.testing.allocator;
    var streamed_arena = std.heap.ArenaAllocator.init(a);
    defer streamed_arena.deinit();
    var owned_arena = std.heap.ArenaAllocator.init(a);
    defer owned_arena.deinit();
    var streamed = FieldAcc.init(a);
    defer streamed.deinit();
    var owned: LegacyFieldFixture = .{};
    for ([_][]const u8{ "Needle café_NEEDLE", "ab--cd café ééééééééééééééééééééééééééééééééééé" }) |text| {
        _ = try accumulateTokenStream(streamed_arena.allocator(), &streamed, analysis_mod.substring_analyzer.tokenStream(text).?);
        try owned.add(owned_arena.allocator(), text);
    }
    try std.testing.expectEqual(owned.token_offset, streamed.token_offset);
    try std.testing.expectEqual(owned.term_map.count(), streamed.term_map.count());
    var iterator = owned.term_map.iterator();
    while (iterator.next()) |entry| {
        const actual = streamed.term_map.get(entry.key_ptr.*).?;
        try std.testing.expectEqual(entry.value_ptr.freq, actual.freq);
        try std.testing.expectEqualSlices(u32, entry.value_ptr.positionSlice(), actual.positionSlice());
    }
    // A repetitive 180 KiB source must fit in an 8 MiB document allocation
    // budget. The old expanded token arrays exceeded 85 MiB in this workload.
    const text = try a.alloc(u8, 180 * 1024);
    defer a.free(text);
    for (text, 0..) |*byte, i| byte.* = "needle-"[i % 7];
    var budget = @import("sql/memory_budget.zig"){ .backing = a, .limit = 8 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    var field = FieldAcc.init(budget.allocator());
    defer field.deinit();
    const count = try accumulateTokenStream(arena.allocator(), &field, analysis_mod.substring_analyzer.tokenStream(text).?);
    try std.testing.expect(count > 100_000);
    try std.testing.expect(field.term_map.count() < 100);
    try std.testing.expect(budget.peak < budget.limit);
    std.debug.print("substring_stream_qualification tokens={d} terms={d} peak_bytes={d}\n", .{ count, field.term_map.count(), budget.peak });
}

test "introducer.substring streaming allocation failures unwind document ownership" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var field = FieldAcc.init(a);
            defer field.deinit();
            _ = try accumulateTokenStream(arena.allocator(), &field, analysis_mod.substring_analyzer.tokenStream("Needle café_needle xy").?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "introducer.substring streaming benchmark" {
    if (@import("antfly_platform").env.getenv("ANTFLY_BENCH_SUBSTRING_STREAM") == null) return error.SkipZigTest;
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 180 * 1024);
    defer a.free(text);
    for ([_][]const u8{ "repetitive", "source_code", "distinct_word" }) |corpus| {
        if (std.mem.eql(u8, corpus, "repetitive")) {
            for (text, 0..) |*byte, i| byte.* = "needle-"[i % 7];
        } else if (std.mem.eql(u8, corpus, "source_code")) {
            @memset(text, ' ');
            var offset: usize = 0;
            var line: usize = 0;
            while (offset < text.len) : (line += 1) {
                const value = std.fmt.bufPrint(text[offset..], "pub fn function_{d:0>6}(value: usize) usize {{ return value + {d}; }}\n", .{ line, line % 997 }) catch break;
                offset += value.len;
            }
        } else {
            var random = std.Random.DefaultPrng.init(0x51ff1c);
            for (text) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        }
        var source_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(text, &source_digest, .{});
        const source_hex = std.fmt.bytesToHex(source_digest, .lower);
        var counter = analysis_mod.substring_analyzer.tokenStream(text).?;
        var expected: u32 = 0;
        while (counter.next() != null) expected += 1;
        var expected_postings: ?[32]u8 = null;
        for (0..9) |trial| {
            for (0..3) |order| {
                const variant = (order + trial) % 3;
                const streamed = variant == 2;
                var budget = @import("sql/memory_budget.zig"){ .backing = a, .limit = 512 * 1024 * 1024 };
                var arena = std.heap.ArenaAllocator.init(budget.allocator());
                const started = platform_time.monotonicNs();
                var field = FieldAcc.init(budget.allocator());
                var legacy: LegacyFieldFixture = .{};
                if (variant == 0) {
                    try legacy.add(arena.allocator(), text);
                } else if (streamed) {
                    _ = try accumulateTokenStream(arena.allocator(), &field, analysis_mod.substring_analyzer.tokenStream(text).?);
                } else {
                    const tokens = try analysis_mod.substring_analyzer.analyze(arena.allocator(), text);
                    for (tokens) |token| try accumulateToken(arena.allocator(), &field, token, 0, false);
                    field.token_offset = @intCast(tokens.len);
                }
                const elapsed = platform_time.monotonicNs() - started;
                const peak = budget.peak;
                const terms = if (variant == 0) legacy.term_map.count() else field.term_map.count();
                const token_count = if (variant == 0) legacy.token_offset else field.token_offset;
                const digest = if (variant == 0) postingsFixtureDigest(legacy) else postingsFixtureDigest(field);
                if (expected_postings) |oracle| try std.testing.expectEqualSlices(u8, &oracle, &digest) else expected_postings = digest;
                try std.testing.expectEqual(expected, token_count);
                field.deinit();
                arena.deinit();
                std.debug.print("SUBSTRING_STREAM_BENCH {{\"corpus\":\"{s}\",\"trial\":{d},\"variant\":{d},\"streamed\":{},\"source_bytes\":{d},\"corpus_sha256\":\"{s}\",\"postings_digest\":\"{s}\",\"tokens\":{d},\"terms\":{d},\"elapsed_ns\":{d},\"peak_bytes\":{d}}}\n", .{ corpus, trial, variant, streamed, text.len, source_hex, std.fmt.bytesToHex(digest, .lower), token_count, terms, elapsed, peak });
            }
        }
    }
}

test "introducer.substring stream admits only nonempty field contributions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var capture = SourceCapture.init(a, false, null, ".", null);
    defer capture.deinit();
    var reuse = AnalysisReuse.init(std.testing.allocator);
    defer reuse.deinit();
    var fields: std.StringHashMapUnmanaged(FieldAcc) = .empty;
    defer {
        var entries = fields.valueIterator();
        while (entries.next()) |entry| entry.deinit();
    }
    for ([_]*const analysis_mod.Analyzer{ &analysis_mod.substring_analyzer, &analysis_mod.default_analyzer }) |analyzer| {
        try accumulateAnalyzedField(a, std.testing.allocator, &fields, "empty", analyzer, "", null, false, &capture, 0, "source", null, &reuse, null, false);
        try accumulateAnalyzedField(a, std.testing.allocator, &fields, "punctuation", analyzer, "...", null, false, &capture, 0, "source", null, &reuse, null, false);
        try std.testing.expectEqual(@as(u32, 0), fields.count());
    }
    try accumulateAnalyzedField(a, std.testing.allocator, &fields, "source", &analysis_mod.substring_analyzer, "NEEDLE", null, false, &capture, 0, "source", null, &reuse, null, false);
    const before = fields.get("source").?.token_offset;
    try accumulateAnalyzedField(a, std.testing.allocator, &fields, "source", &analysis_mod.substring_analyzer, "", null, false, &capture, 0, "source", null, &reuse, null, false);
    try std.testing.expectEqual(before, fields.get("source").?.token_offset);
    try std.testing.expectEqual(@as(u32, 1), fields.count());
}

test "bounded postings spill between large companion fields under small node allowance" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const text = try a.alloc(u8, 180 * 1024);
    defer a.free(text);
    var random = std.Random.DefaultPrng.init(0x51ff1c);
    for (text) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
    const fields = [_]TextField{
        .{ .field_name = "content", .text = text },
        .{ .field_name = "content._substring", .text = text, .analyzer = &analysis_mod.substring_analyzer },
        .{ .field_name = "second._substring", .text = text, .analyzer = &analysis_mod.substring_analyzer },
        .{ .field_name = "third._substring", .text = text, .analyzer = &analysis_mod.substring_analyzer },
    };
    const docs = [_]TextDocument{
        .{ .id = "a", .stored_data = "{}", .text_fields = &fields, .typed_fields = &.{} },
        .{ .id = "b", .stored_data = "{}", .text_fields = &fields, .typed_fields = &.{} },
    };
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{
        .soft_limit_bytes = 96 * 1024 * 1024,
        .hard_limit_bytes = 128 * 1024 * 1024,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    var profile: BuildTextProfile = .{};
    writeSegmentFromTextWithAnalysisOptions(textBuildScratchAllocator(), &docs, &analysis_mod.default_analyzer, .{}, .{
        .resource_manager = &manager,
        .postings_run_io = std.testing.io,
        .postings_run_directory = directory,
        .profile = &profile,
        .profile_timings = false,
        .profile_working_set = false,
    }, &sink) catch |err| {
        std.debug.print("COMPANION_FAILURE fields={d} hits={d} runs={d} peak={d} err={s}\n", .{ profile.text_field_count, profile.term_hit_count, profile.postings_run_count, manager.sliceStats(.full_text_build_working_set).peak_bytes, @errorName(err) });
        return err;
    };
    const stats = manager.sliceStats(.full_text_build_working_set);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes <= 128 * 1024 * 1024);
    try std.testing.expect(profile.postings_run_count >= 6);
    var reader = try segment_mod.SegmentReader.init(a, output.out.items);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);
    std.debug.print("COMPANION_SPILL source_bytes={d} fields={d} peak_bytes={d} runs={d}\n", .{ text.len, fields.len, stats.peak_bytes, profile.postings_run_count });
}

test "bounded postings shared all contributions under small node allowance" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const text = try a.alloc(u8, 180 * 1024);
    defer a.free(text);
    var random = std.Random.DefaultPrng.init(0x51ff1c);
    for (text) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
    const fields = [_]TextField{
        .{ .field_name = "content", .text = text },
        .{ .field_name = "content._substring", .text = text, .analyzer = &analysis_mod.substring_analyzer },
        .{ .field_name = "_all", .text = text },
        .{ .field_name = "_all", .text = text, .analyzer = &analysis_mod.substring_analyzer },
    };
    const docs = [_]TextDocument{
        .{ .id = "a", .stored_data = "{}", .text_fields = &fields, .typed_fields = &.{} },
        .{ .id = "b", .stored_data = "{}", .text_fields = &fields, .typed_fields = &.{} },
    };
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{
        .soft_limit_bytes = 96 * 1024 * 1024,
        .hard_limit_bytes = 128 * 1024 * 1024,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    var profile: BuildTextProfile = .{};
    writeSegmentFromTextWithAnalysisOptions(textBuildScratchAllocator(), &docs, &analysis_mod.default_analyzer, .{}, .{
        .resource_manager = &manager,
        .postings_run_io = std.testing.io,
        .postings_run_directory = directory,
        .profile = &profile,
        .profile_timings = false,
        .profile_working_set = false,
    }, &sink) catch |err| {
        std.debug.print("COMPANION_FAILURE fields={d} hits={d} runs={d} peak={d} err={s}\n", .{ profile.text_field_count, profile.term_hit_count, profile.postings_run_count, manager.sliceStats(.full_text_build_working_set).peak_bytes, @errorName(err) });
        return err;
    };
    const stats = manager.sliceStats(.full_text_build_working_set);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes <= 128 * 1024 * 1024);
    try std.testing.expect(profile.postings_run_count >= 4);
    var reader = try segment_mod.SegmentReader.init(a, output.out.items);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);
    std.debug.print("COMPANION_SPILL source_bytes={d} fields={d} peak_bytes={d} runs={d}\n", .{ text.len, fields.len, stats.peak_bytes, profile.postings_run_count });
}

test "grouped physical fields preserve interleaved contribution positions and norms" {
    const a = std.testing.allocator;
    const fields = [_]TextField{
        .{ .field_name = "z", .text = "alpha beta" },
        .{ .field_name = "a", .text = "independent" },
        .{ .field_name = "z", .text = "alpha gamma" },
        .{ .field_name = "empty", .text = "" },
        .{ .field_name = "a", .text = "second" },
    };
    var order: std.ArrayListUnmanaged(usize) = .empty;
    defer order.deinit(a);
    try std.testing.expect(try orderTextFields(a, &order, &fields));
    try std.testing.expectEqualSlices(usize, &.{ 1, 4, 3, 0, 2 }, order.items);
    const docs = [_]TextDocument{.{ .id = "row", .stored_data = "{}", .text_fields = &fields, .typed_fields = &.{} }};
    const data = try buildSegmentFromTextWithAnalysisOptions(a, &docs, &analysis_mod.default_analyzer, .{}, .{});
    defer a.free(data);
    var reader = try segment_mod.SegmentReader.init(a, data);
    defer reader.deinit();
    var field = (try reader.invertedIndexScoped(a, "z")).?;
    defer field.deinit();
    try std.testing.expectEqual(@as(u64, 4), field.total_field_len);
    var iterator = try (try field.lookup("alpha")).?.iterator(a);
    defer iterator.deinit();
    const hit = (try iterator.next()).?;
    try std.testing.expectEqual(@as(u32, 2), hit.freq);
    try std.testing.expectEqual(@as(u32, 4), hit.norm);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2 }, hit.positions);
    try std.testing.expect((try iterator.next()) == null);
    var other = (try reader.invertedIndexScoped(a, "a")).?;
    defer other.deinit();
    try std.testing.expectEqual(@as(u64, 2), other.total_field_len);
    var second = try (try other.lookup("second")).?.iterator(a);
    defer second.deinit();
    try std.testing.expectEqualSlices(u32, &.{1}, (try second.next()).?.positions);
}

test "bounded postings schema projection fits small node allowance" {
    const a = std.testing.allocator;
    const schema_api = @import("schema/mod.zig");
    const runtime_schema = @import("storage/schema.zig");
    const mapper = @import("storage/db/document_mapper.zig");
    var parsed = try schema_api.parseValidatedTableSchema(a,
        \\{ "default_type":"doc", "document_schemas":{"doc":{"schema":{"type":"object","properties":{"content":{"type":"string","x-antfly-types":["text","substring"]}}}}} }
    );
    defer parsed.deinit(a);
    const schema = try schema_api.deriveRuntimeTableSchema(a, parsed);
    defer runtime_schema.freeSchema(a, schema);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const text = if (@import("antfly_platform").env.getenv("ANTFLY_BENCH_POSTINGS_CORPUS")) |path|
        try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, arena.allocator(), .limited(1024 * 1024))
    else blk: {
        const bytes = try arena.allocator().alloc(u8, 180 * 1024);
        var random = std.Random.DefaultPrng.init(0x51ff1c);
        for (bytes) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        break :blk bytes;
    };
    var root: std.json.ObjectMap = .empty;
    try root.put(arena.allocator(), "content", .{ .string = text });
    var docs: [8]mapper.TextProjectionSourceDoc = undefined;
    for (&docs, 0..) |*doc, i| doc.* = .{ .key = "row", .stored_data = "{}", .root = .{ .object = root }, .typed_source = .{ .object = root }, .doc_ordinal = @intCast(i + 1) };
    const projection = try mapper.buildTextProjectionBatchFromSource(arena.allocator(), &docs, .{}, schema, null);
    for (projection.docs[0].text_fields) |field| std.debug.print("SCHEMA_FIELD name={s} bytes={d} stream={}\n", .{ field.field_name, field.text.len, if (field.analyzer) |analyzer| analyzer.tokenStream(field.text) != null else false });
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 128 * 1024 * 1024 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var output = segment_mod.MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    var profile: BuildTextProfile = .{};
    mapper.writeTextSegmentFromProjectionBatchToSink(textBuildScratchAllocator(), projection, .{}, .{
        .resource_manager = &manager,
        .postings_run_io = std.testing.io,
        .postings_run_directory = directory,
        .store_document_source = false,
        .profile = &profile,
        .profile_timings = false,
        .profile_working_set = false,
    }, &sink) catch |err| {
        std.debug.print("SCHEMA_FAILURE fields={d} hits={d} runs={d} peak={d} err={s}\n", .{ profile.text_field_count, profile.term_hit_count, profile.postings_run_count, manager.sliceStats(.full_text_build_working_set).peak_bytes, @errorName(err) });
        return err;
    };
    std.debug.print("SCHEMA_SPILL fields={d} peak_bytes={d} runs={d}\n", .{ profile.text_field_count, manager.sliceStats(.full_text_build_working_set).peak_bytes, profile.postings_run_count });
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.full_text_build_working_set).used_bytes);
    var reader = try segment_mod.SegmentReader.init(a, output.out.items);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 8), reader.doc_count);
}

test "bounded postings field maps use explicit reclaiming scratch behind opaque term regions" {
    const a = std.testing.allocator;
    const Budget = @import("sql/memory_budget.zig");
    const Opaque = @import("storage/lite/test_allocator.zig").NoResizeAllocator;
    const text = try a.alloc(u8, 180 * 1024);
    defer a.free(text);
    var random = std.Random.DefaultPrng.init(0x51ff1c);
    for (text) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
    var peaks: [2]usize = undefined;
    var digests: [2][32]u8 = undefined;
    for (0..2) |variant| {
        var budget = Budget{ .backing = a, .limit = 128 * 1024 * 1024 };
        {
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            var region_wrapper = Opaque{ .backing = arena.allocator() };
            // Inferring scratch through an unknown region wrapper retains old
            // hash tables. Production passes its reclaiming budget explicitly.
            var field = FieldAcc.init(if (variant == 0) region_wrapper.allocator() else budget.allocator());
            defer field.deinit();
            _ = try accumulateTokenStream(region_wrapper.allocator(), &field, analysis_mod.substring_analyzer.tokenStream(text).?);
            digests[variant] = postingsFixtureDigest(field);
            peaks[variant] = budget.peak;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expectEqualSlices(u8, &digests[0], &digests[1]);
    try std.testing.expect(peaks[1] < peaks[0]);
    try std.testing.expect(peaks[1] < 32 * 1024 * 1024);
    std.debug.print("EXPLICIT_FIELD_SCRATCH region_peak={d} reclaiming_peak={d}\n", .{ peaks[0], peaks[1] });
}

test "bounded postings prepared segment owners unwind partial allocation failures" {
    const Scenario = struct {
        fn run(a: Allocator) !void {
            const mapper = @import("storage/db/document_mapper.zig");
            const Stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator;
            var output_owner = Stable{ .backing = a };
            var scratch_owner = Stable{ .backing = a };
            const output_alloc = output_owner.allocator();
            const docs = [_]TextDocument{
                .{ .id = "a", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "common alpha" }}, .typed_fields = &.{} },
                .{ .id = "b", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = "common beta" }}, .typed_fields = &.{} },
            };
            const segments = try mapper.buildTextSegmentsFromProjectionBatch(output_alloc, .{ .docs = &docs }, .{}, .{
                .scratch_allocator = scratch_owner.allocator(),
                .target_segment_bytes = 1,
            });
            defer mapper.freeTextSegments(output_alloc, segments);
            try std.testing.expectEqual(@as(usize, 2), segments.len);
            for (segments) |bytes| {
                var reader = try segment_mod.SegmentReader.init(a, bytes);
                defer reader.deinit();
                try std.testing.expectEqual(@as(u32, 1), reader.doc_count);
                var field = (try reader.invertedIndexScoped(a, "body")).?;
                defer field.deinit();
                try std.testing.expect((try field.lookup("common")) != null);
            }
        }
    };
    try Scenario.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "postings merge caches sixteen interleaved dictionary and posting cursors" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const spool = try PostingRun.create(a, std.testing.io, directory);
    defer spool.deinit();
    var payload: [32 * 1024]u8 = undefined;
    for (0..16) |lane| {
        @memset(&payload, @intCast(lane));
        const start = spool.len();
        try spool.appendSlice(&payload);
        try spool.seal(start);
    }
    const view = try spool.view();
    var byte: [1]u8 = undefined;
    for (0..64) |step| for (0..16) |lane| {
        for ([_]usize{ 0, 16 * 1024 }) |section| {
            try view.readInto(lane * payload.len + section + step, &byte);
            try std.testing.expectEqual(@as(u8, @intCast(lane)), byte[0]);
        }
    };
    try std.testing.expect(spool.read_calls <= 32);
    try std.testing.expect(spool.read_buffer.len <= 512 * 1024);
    std.debug.print("interleaved merge logical_reads=2048 physical_reads={d} cache_bytes={d}\n", .{ spool.read_calls, spool.read_buffer.len });
}

test "postings spill policy retains small-node bounds and adapts to live headroom" {
    const MiB = 1024 * 1024;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{ .hard_limit_bytes = 128 * MiB };
    var small = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    try std.testing.expectEqual(@as(u64, 8 * MiB), postingsRunTarget(.{ .resource_manager = &small }, 8 * MiB));
    budgets[@backingInt(resource_manager_mod.Slice.full_text_build_working_set)] = .{ .hard_limit_bytes = 512 * MiB };
    var large = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    try std.testing.expectEqual(@as(u64, 64 * MiB), postingsRunTarget(.{ .resource_manager = &large }, 8 * MiB));
    var held = try large.reserve(.full_text_build_working_set, 480 * MiB);
    defer held.release();
    try std.testing.expectEqual(@as(u64, 10 * MiB), postingsRunTarget(.{ .resource_manager = &large }, 8 * MiB));
    try std.testing.expectEqual(@as(u64, 1024), postingsRunTarget(.{ .resource_manager = &large, .postings_run_target_bytes = 1024 }, 8 * MiB));
}

test "observed physical ordering proves directions ties and native nulls" {
    const a = std.testing.allocator;
    const docs = [_]TextDocument{
        .{ .id = "a", .stored_data = "{\"rank\":1}", .text_fields = &.{} },
        .{ .id = "b", .stored_data = "{\"rank\":1}", .text_fields = &.{} },
        .{ .id = "c", .stored_data = "{\"rank\":2}", .text_fields = &.{} },
    };
    const fields = [_]segment_mod.SegmentIndexSortField{ .{ .field = "rank" }, .{ .field = "_id" } };
    try std.testing.expect(try textDocumentsAreIndexSorted(a, &docs, .{}, &fields));
    try std.testing.expect(!try textDocumentsAreIndexSorted(a, &.{ docs[1], docs[0] }, .{}, &fields));
    try std.testing.expect(!try textDocumentsAreIndexSorted(a, &.{ docs[2], docs[0] }, .{}, &fields));
    try std.testing.expect(try textDocumentsAreIndexSorted(a, &.{ docs[2], docs[0], docs[1] }, .{}, &.{ .{ .field = "rank", .desc = true }, .{ .field = "_id" } }));
    try std.testing.expect(try textDocumentsAreIndexSorted(a, &.{ .{ .id = "z", .stored_data = "{\"rank\":null}", .text_fields = &.{} }, docs[0] }, .{}, &fields));
}

test "observed physical ordering encoder keeps all source ordinals and declines unsorted input" {
    const a = std.testing.allocator;
    const docs = [_]TextDocument{
        .{ .id = "a", .stored_data = "{\"rank\":1}", .text_fields = &.{} },
        .{ .id = "b", .stored_data = "{\"rank\":2}", .text_fields = &.{} },
        .{ .id = "c", .stored_data = "{\"rank\":3}", .text_fields = &.{} },
    };
    for (0..2) |unordered| {
        const input = if (unordered == 0) docs else [_]TextDocument{ docs[2], docs[0], docs[1] };
        const bytes = try buildSegmentFromTextWithAnalysisOptions(a, &input, &analysis_mod.default_analyzer, .{}, .{ .index_sort = &.{ .{ .field = "rank" }, .{ .field = "_id" } }, .attest_index_sort_in_place = true });
        defer a.free(bytes);
        var reader = try segment_mod.SegmentReader.init(a, bytes);
        defer reader.deinit();
        try std.testing.expectEqual(@as(u32, 3), reader.doc_count);
        for (input, 0..) |doc, i| try std.testing.expectEqualStrings(doc.id, (try reader.storedDoc(@intCast(i))).?.id);
        const order = try reader.indexSortFieldsAlloc(a);
        defer if (order) |fields| segment_mod.freeIndexSortFields(a, fields);
        try std.testing.expectEqual(unordered == 0, order != null);
    }
}

test "observed physical ordering uses signed timestamp keys and releases failed scratch" {
    const Check = struct {
        fn run(a: Allocator) !void {
            const values = [_]TypedFieldValue{
                .{ .field_name = "at", .value_type = .datetime_ns, .value = .{ .datetime_ns = -1 } },
                .{ .field_name = "at", .value_type = .datetime_ns, .value = .{ .datetime_ns = 1 } },
            };
            const docs = [_]TextDocument{
                .{ .id = "a", .stored_data = "{}", .text_fields = &.{}, .typed_fields = values[0..1] },
                .{ .id = "b", .stored_data = "{}", .text_fields = &.{}, .typed_fields = values[1..2] },
            };
            try std.testing.expect(try textDocumentsAreIndexSorted(a, &docs, .{}, &.{ .{ .field = "at" }, .{ .field = "_id" } }));
            try std.testing.expect(!try textDocumentsAreIndexSorted(a, &docs, .{}, &.{ .{ .field = "at", .desc = true }, .{ .field = "_id" } }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "highlight indexing captures original offsets once across physical bindings without column reanalysis" {
    const testing = std.testing;
    if (comptime @import("builtin").os.tag == .freestanding) return;
    const a = testing.allocator;
    const text = try a.alloc(u8, 8192);
    defer a.free(text);
    @memset(text, '.');
    @memcpy(text[4090..][0.."needle".len], "needle");
    var source: std.json.ObjectMap = .empty;
    defer source.deinit(a);
    try source.put(a, "body", .{ .string = text });
    // Compiled positions cannot replace original byte spans on a later build.
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    const cache = try AnalyzedContributionCache.create(&manager);
    defer cache.deinit();
    for (0..3) |_| {
        const warm = try buildSegmentFromTextWithAnalysisOptions(a, &.{.{ .id = "warm", .stored_data = "{}", .text_fields = &.{.{ .field_name = "body", .text = text }} }}, &analysis_mod.default_analyzer, .{}, .{ .contribution_cache = cache });
        a.free(warm);
    }
    var held = cache.borrow(try AnalyzedContributionCache.key(&analysis_mod.default_analyzer, text)) orelse return error.MissingCompiledContribution;
    held.deinit();
    const fields = [_]TextField{
        .{ .field_name = "body", .source_field = "body", .text = text },
        .{ .field_name = "_all", .source_field = "body", .text = text },
        .{ .field_name = "_all", .source_field = "tail", .text = "tail" },
    };
    for ([_]usize{ 2, 3 }) |field_count| {
        var profile: BuildTextProfile = .{};
        const bytes = try buildSegmentFromTextWithAnalysisOptions(a, &.{.{
            .id = "row",
            .stored_data = "{}",
            .stored_source = .{ .object = source },
            .stored_source_bytes = try @import("storage/typed_json.zig").encodedSize(.{ .object = source }),
            .text_fields = fields[0..field_count],
        }}, &analysis_mod.default_analyzer, .{}, .{ .store_source_columns = true, .profile = &profile, .postings_run_io = testing.io, .contribution_cache = cache });
        defer a.free(bytes);
        try testing.expectEqual(@as(u64, 1), profile.shared_analysis_runs);
        try testing.expectEqual(@as(u64, 1), profile.shared_analysis_reuses);
        try testing.expectEqual(@as(u64, 0), profile.contribution_cache_hits);
        try testing.expectEqual(@as(u64, 1), profile.source_vectors_captured);
        try testing.expectEqual(@as(u64, 1), profile.source_vectors_reused);
        try testing.expect(profile.source_vector_spilled_bytes > 0);
        var reader = try segment_mod.SegmentReader.init(a, bytes);
        defer reader.deinit();
        const column = @import("section/source_column.zig");
        var cursor = column.Cursor.init((try reader.storedColumnScoped(a, "body")).?);
        defer cursor.deinit();
        const stored = (try cursor.text(0)).?;
        const tokens = (try stored.tokens(a, try column.fingerprint(a, &analysis_mod.default_analyzer), .none)).?;
        defer tokens.deinit();
        try testing.expectEqual(@as(usize, 1), tokens.items.len);
        try testing.expectEqualStrings("needl", tokens.items[0].term);
        try testing.expectEqual(@as(u32, 4090), tokens.items[0].start_byte);
        std.debug.print("SOURCE_ANALYSIS captured={d} bindings_reused={d} spilled_bytes={d}\n", .{ profile.source_vectors_captured, profile.source_vectors_reused, profile.source_vector_spilled_bytes });
    }
}

test "highlight indexing capture follows sorted output ordinals rather than input positions" {
    const a = std.testing.allocator;
    const values = try a.alloc(u8, 16384);
    defer a.free(values);
    @memset(values, '.');
    const last = values[0..8192];
    const first = values[8192..];
    @memcpy(last[32..][0.."zebra target".len], "zebra target");
    @memcpy(first[7000..][0.."alpha needle".len], "alpha needle");
    var roots: [2]std.json.ObjectMap = @splat(.empty);
    defer for (&roots) |*root| root.deinit(a);
    try roots[0].put(a, "body", .{ .string = last });
    try roots[1].put(a, "body", .{ .string = first });
    const docs = [_]TextDocument{
        .{ .id = "z", .stored_data = "{}", .stored_source = .{ .object = roots[0] }, .text_fields = &.{.{ .field_name = "body", .source_field = "body", .text = last }} },
        .{ .id = "a", .stored_data = "{}", .stored_source = .{ .object = roots[1] }, .text_fields = &.{.{ .field_name = "body", .source_field = "body", .text = first }} },
    };
    const bytes = try buildSegmentFromTextWithAnalysisOptions(a, &docs, &analysis_mod.simple_analyzer, .{}, .{ .store_source_columns = true, .index_sort = &.{.{ .field = "_id" }} });
    defer a.free(bytes);
    var reader = try segment_mod.SegmentReader.init(a, bytes);
    defer reader.deinit();
    const column = @import("section/source_column.zig");
    var cursor = column.Cursor.init((try reader.storedColumnScoped(a, "body")).?);
    defer cursor.deinit();
    const signature = try column.fingerprint(a, &analysis_mod.simple_analyzer);
    for ([_][]const u8{ first, last }, [_][]const u8{ "alpha", "zebra" }, [_]u32{ 7000, 32 }, 0..) |expected, term, offset, doc| {
        const stored = (try cursor.text(@intCast(doc))).?;
        const text = try stored.read(a, 0, stored.length, .none);
        defer a.free(text);
        try std.testing.expectEqualStrings(expected, text);
        const tokens = (try stored.tokens(a, signature, .none)).?;
        defer tokens.deinit();
        const found = for (tokens.items) |token| {
            if (std.mem.eql(u8, token.term, term)) break token;
        } else return error.MissingSourceTerm;
        try std.testing.expectEqual(offset, found.start_byte);
    }
}

test "highlight streaming indexing preserves native postings source positions and field norms" {
    const a = std.testing.allocator;
    const pattern = "Running dogs café ";
    const repeats = 8192;
    const text = try a.alloc(u8, pattern.len * repeats);
    defer a.free(text);
    for (0..repeats) |i| @memcpy(text[i * pattern.len ..][0..pattern.len], pattern);
    var source: std.json.ObjectMap = .empty;
    defer source.deinit(a);
    try source.put(a, "body", .{ .string = text });
    var reference = std.heap.ArenaAllocator.init(a);
    defer reference.deinit();
    const expected = try analysis_mod.default_analyzer.analyzeWithSourceOffsets(reference.allocator(), text);
    const fields = [_]TextField{
        .{ .field_name = "body", .source_field = "body", .text = text },
        .{ .field_name = "tail", .source_field = "tail", .text = "extra" },
    };
    for ([_]bool{ false, true }) |disk| for ([_]usize{ 1, 2 }) |field_count| {
        var profile: BuildTextProfile = .{};
        const bytes = try buildSegmentFromTextWithAnalysisOptions(a, &.{.{
            .id = "row",
            .stored_data = "{}",
            .stored_source = .{ .object = source },
            .text_fields = fields[0..field_count],
        }}, &analysis_mod.default_analyzer, .{}, .{ .store_source_columns = true, .profile = &profile, .postings_run_io = if (disk) std.testing.io else null });
        defer a.free(bytes);
        try std.testing.expectEqual(expected.len + field_count - 1, profile.token_count);
        try std.testing.expectEqual(@as(u64, 1), profile.source_vectors_captured);
        var reader = try segment_mod.SegmentReader.init(a, bytes);
        defer reader.deinit();
        const column = @import("section/source_column.zig");
        var cursor = column.Cursor.init((try reader.storedColumnScoped(a, "body")).?);
        defer cursor.deinit();
        const stored = (try cursor.text(0)).?;
        const tokens = (try stored.tokens(a, try column.fingerprint(a, &analysis_mod.default_analyzer), .none)).?;
        defer tokens.deinit();
        try std.testing.expectEqual(expected.len, tokens.items.len);
        for (expected, tokens.items) |left, right| {
            try std.testing.expectEqualStrings(left.term, right.term);
            try std.testing.expectEqual(left.position, right.position);
            try std.testing.expectEqual(left.start_byte, right.start_byte);
            try std.testing.expectEqual(left.end_byte, right.end_byte);
        }
        var writer = try index_mod.IndexWriter.init(a);
        defer writer.deinit();
        try writer.addSegment(bytes);
        const result = try writer.snapshot().search(a, "body", &.{"run"}, 10);
        defer a.free(result.hits);
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    };
}
