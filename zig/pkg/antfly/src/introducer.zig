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
const geo_mod = @import("search/geo.zig");
const platform_time = @import("platform/time.zig");
const process_memory = @import("platform/process_memory.zig");
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

    fn reset(self: *TextBuildScratch, alloc: Allocator, retained_bytes: usize) void {
        resetScratchList(inverted.InvertedIndexBuilder.TermHit, alloc, &self.hits, retained_bytes);
        resetScratchList(u32, alloc, &self.positions, retained_bytes);
    }

    fn deinit(self: *TextBuildScratch, alloc: Allocator) void {
        self.hits.deinit(alloc);
        self.positions.deinit(alloc);
    }

    fn estimatedMemoryBytes(self: *const TextBuildScratch) u64 {
        return (@as(u64, @intCast(self.hits.capacity)) * @sizeOf(inverted.InvertedIndexBuilder.TermHit)) +
            (@as(u64, @intCast(self.positions.capacity)) * @sizeOf(u32));
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

const FieldPostingsBuilder = struct {
    active: bool = false,
    builder: inverted.InvertedIndexBuilder = undefined,

    fn init(alloc: Allocator) !FieldPostingsBuilder {
        return .{
            .active = true,
            .builder = inverted.InvertedIndexBuilder.init(alloc, .{}),
        };
    }

    fn deinit(self: *FieldPostingsBuilder, alloc: Allocator) void {
        _ = alloc;
        if (!self.active) return;
        self.builder.deinit();
        self.active = false;
        self.builder = undefined;
    }

    fn addDocument(self: *FieldPostingsBuilder, doc_idx: u32, hits: []const inverted.InvertedIndexBuilder.TermHit) !void {
        try self.builder.addDocument(doc_idx, hits);
    }

    fn buildAlloc(self: *FieldPostingsBuilder, output_alloc: Allocator) ![]u8 {
        return try self.builder.buildAlloc(output_alloc);
    }

    fn estimatedMemoryBytes(self: *const FieldPostingsBuilder) u64 {
        if (!self.active) return 0;
        return self.builder.estimatedMemoryBytes();
    }
};

fn deinitFieldPostingsBuilders(
    alloc: Allocator,
    builders: *std.StringHashMapUnmanaged(FieldPostingsBuilder),
) void {
    var it = builders.valueIterator();
    while (it.next()) |builder| builder.deinit(alloc);
    builders.deinit(alloc);
}

fn ensureFieldPostingsBuilder(
    alloc: Allocator,
    builders: *std.StringHashMapUnmanaged(FieldPostingsBuilder),
    field_name: []const u8,
) !*FieldPostingsBuilder {
    const gop = try builders.getOrPut(alloc, field_name);
    if (!gop.found_existing) {
        gop.key_ptr.* = field_name;
        gop.value_ptr.* = try FieldPostingsBuilder.init(alloc);
    }
    return gop.value_ptr;
}

fn buildSegmentWithExtraSections(
    alloc: Allocator,
    batch: Batch,
    extra_sections: []const ExtraSection,
) ![]u8 {
    var field_builders = std.StringHashMapUnmanaged(FieldPostingsBuilder).empty;
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
        const inv_data = try entry.value_ptr.buildAlloc(alloc);
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
};

/// A document with raw text fields that will be analyzed before indexing.
pub const TextDocument = struct {
    id: []const u8,
    stored_data: []const u8,
    text_fields: []const TextField,
    doc_ordinal: ?u32 = null,
    recursive_typed_fields: bool = false,
    infer_type_dynamic_paths: []const []const u8 = &.{},
    typed_fields: ?[]const TypedFieldValue = null,
    typed_source: ?std.json.Value = null,
};

pub const TextField = struct {
    field_name: []const u8,
    text: []const u8,
    analyzer: ?*const analysis_mod.Analyzer = null,
};

pub const BuildTextOptions = struct {
    recursive_typed_fields: bool = false,
    infer_type_dynamic_paths: []const []const u8 = &.{},
    profile: ?*BuildTextProfile = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    build_memory_target_bytes: usize = default_build_memory_target_bytes,
    doc_scratch_retained_bytes: usize = default_doc_scratch_retained_bytes,
    profile_timings: bool = true,
    profile_working_set: bool = true,
};

pub const BuildTextProfile = struct {
    doc_count: u64 = 0,
    text_field_count: u64 = 0,
    token_count: u64 = 0,
    term_hit_count: u64 = 0,
    typed_value_count: u64 = 0,
    segment_bytes: u64 = 0,
    analyzer_ns: u64 = 0,
    term_accum_ns: u64 = 0,
    hit_materialize_ns: u64 = 0,
    typed_collect_ns: u64 = 0,
    typed_build_ns: u64 = 0,
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
            manager.releaseBytes(.full_text_build_working_set, self.current_bytes);
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

fn estimateTextDocumentInputBytes(doc: TextDocument) u64 {
    var total: u64 = @intCast(doc.id.len + doc.stored_data.len);
    total +|= @as(u64, @intCast(doc.text_fields.len)) * (@sizeOf(TextField) + 16);
    for (doc.text_fields) |field| {
        total +|= @intCast(field.field_name.len + field.text.len);
    }
    if (doc.typed_fields) |typed_fields| {
        total +|= @as(u64, @intCast(typed_fields.len)) * (@sizeOf(TypedFieldValue) + 16);
    }
    return total;
}

pub fn estimateTextDocumentSegmentBytes(doc: TextDocument) u64 {
    var total: u64 = 64 + @as(u64, @intCast(doc.id.len + doc.stored_data.len));
    for (doc.text_fields) |field| {
        total +|= 16 + @as(u64, @intCast(field.field_name.len + field.text.len));
    }
    if (doc.typed_fields) |typed_fields| {
        total +|= @as(u64, @intCast(typed_fields.len)) * 32;
        for (typed_fields) |field| total +|= @intCast(field.field_name.len);
    }
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
        total +|= @intCast(doc.id.len + doc.stored_data.len);
        total +|= 32;
    }
    return total;
}

fn estimateFieldPostingsBuilderBytes(builders: *std.StringHashMapUnmanaged(FieldPostingsBuilder)) u64 {
    var total: u64 = @as(u64, @intCast(builders.capacity())) * (@sizeOf([]const u8) + @sizeOf(FieldPostingsBuilder) + 24);
    var it = builders.iterator();
    while (it.next()) |entry| {
        total +|= @intCast(entry.key_ptr.*.len);
        total +|= entry.value_ptr.estimatedMemoryBytes();
    }
    return total;
}

fn estimateTypedDocValuesBytes(typed_fields: *std.StringHashMapUnmanaged(TypedFieldCollector)) u64 {
    var total: u64 = @as(u64, @intCast(typed_fields.capacity())) * (@sizeOf([]const u8) + @sizeOf(TypedFieldCollector) + 24);
    var it = typed_fields.iterator();
    while (it.next()) |entry| {
        total +|= @intCast(entry.key_ptr.*.len);
        if (entry.value_ptr.writer) |*writer| total +|= writer.estimatedMemoryBytes();
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

    var resource_tracker = TextBuildResourceTracker.init(options.resource_manager, profile);
    defer resource_tracker.release();
    const input_estimated_bytes = estimateTextDocInputBytes(docs);
    try resource_tracker.adjust(input_estimated_bytes);

    var typed_sections = std.ArrayListUnmanaged(ExtraSection).empty;
    defer {
        for (typed_sections.items) |section| alloc.free(@constCast(section.data));
        typed_sections.deinit(alloc);
    }

    var field_builders = std.StringHashMapUnmanaged(FieldPostingsBuilder).empty;
    defer deinitFieldPostingsBuilders(alloc, &field_builders);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    var field_indices = std.StringHashMapUnmanaged(u16).empty;
    defer field_indices.deinit(alloc);

    var analyzer_cache = std.StringHashMapUnmanaged(*const analysis_mod.Analyzer).empty;
    defer analyzer_cache.deinit(alloc);

    var doc_ordinals = std.ArrayListUnmanaged(u32).empty;
    defer doc_ordinals.deinit(alloc);
    var has_doc_ordinal = false;

    var typed_fields = std.StringHashMapUnmanaged(TypedFieldCollector).empty;
    defer {
        var it = typed_fields.valueIterator();
        while (it.next()) |collector| {
            if (collector.writer) |*writer| writer.deinit();
        }
        var key_it = typed_fields.keyIterator();
        while (key_it.next()) |key| alloc.free(key.*);
        typed_fields.deinit(alloc);
    }

    var doc_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer doc_arena_state.deinit();

    var scratch = TextBuildScratch{};
    defer scratch.deinit(alloc);

    for (docs, 0..) |text_doc, doc_idx| {
        _ = doc_arena_state.reset(.{ .retain_with_limit = options.doc_scratch_retained_bytes });
        scratch.reset(alloc, options.doc_scratch_retained_bytes);
        const doc_alloc = doc_arena_state.allocator();

        const stored_attach_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        try seg_writer.addStoredDocBorrowed(text_doc.id, text_doc.stored_data);
        if (profile_timings) {
            if (profile) |p| p.stored_doc_attach_ns +|= platform_time.monotonicNs() - stored_attach_start_ns;
        }

        const doc_ordinal = text_doc.doc_ordinal orelse 0;
        has_doc_ordinal = has_doc_ordinal or doc_ordinal != 0;
        try doc_ordinals.append(alloc, doc_ordinal);

        if (!hasDuplicateTextFieldNames(text_doc.text_fields)) {
            for (text_doc.text_fields) |tf| {
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
                );
            }
        } else {
            var field_maps = std.StringHashMapUnmanaged(FieldAcc).empty;

            for (text_doc.text_fields) |tf| {
                if (profile) |p| p.text_field_count +|= 1;
                const analyzer = try cachedFieldAnalyzer(alloc, &analyzer_cache, tf, default_analyzer, text_analysis);
                const analyzer_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
                const tokens = try analyzer.analyze(doc_alloc, tf.text);
                if (profile) |p| {
                    if (profile_timings) p.analyzer_ns +|= platform_time.monotonicNs() - analyzer_start_ns;
                    p.token_count +|= @intCast(tokens.len);
                }
                if (tokens.len == 0) {
                    continue;
                }

                const term_accum_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
                const field_gop = try field_maps.getOrPut(doc_alloc, tf.field_name);
                if (!field_gop.found_existing) {
                    field_gop.key_ptr.* = tf.field_name;
                    field_gop.value_ptr.* = .{};
                }
                const base_position = field_gop.value_ptr.token_offset;

                for (tokens) |tok| {
                    const gop = try field_gop.value_ptr.term_map.getOrPut(doc_alloc, tok.term);
                    if (!gop.found_existing) {
                        gop.key_ptr.* = tok.term;
                        gop.value_ptr.* = .{ .freq = 0, .positions = .empty };
                    }
                    gop.value_ptr.freq += 1;
                    try gop.value_ptr.positions.append(doc_alloc, base_position + tok.position);
                }
                field_gop.value_ptr.token_offset += @intCast(tokens.len);
                if (profile_timings) {
                    if (profile) |p| p.term_accum_ns +|= platform_time.monotonicNs() - term_accum_start_ns;
                }
            }

            const hit_materialize_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
            var field_it = field_maps.iterator();
            while (field_it.next()) |field_entry| {
                scratch.hits.clearRetainingCapacity();

                var it = field_entry.value_ptr.term_map.iterator();
                while (it.next()) |entry| {
                    try scratch.hits.append(alloc, .{
                        .term = entry.key_ptr.*,
                        .freq = entry.value_ptr.freq,
                        .norm = field_entry.value_ptr.token_offset,
                        .positions = entry.value_ptr.positions.items,
                    });
                    if (profile) |p| p.term_hit_count +|= 1;
                }

                if (scratch.hits.items.len == 0) continue;

                const field_name = field_entry.key_ptr.*;
                const builder = try ensureFieldPostingsBuilder(alloc, &field_builders, field_name);
                try builder.addDocument(@intCast(doc_idx), scratch.hits.items);

                const field_index_gop = try field_indices.getOrPut(alloc, field_name);
                if (!field_index_gop.found_existing) {
                    field_index_gop.key_ptr.* = field_name;
                    field_index_gop.value_ptr.* = try seg_writer.addField(field_name);
                }
            }
            if (profile_timings) {
                if (profile) |p| p.hit_materialize_ns +|= platform_time.monotonicNs() - hit_materialize_start_ns;
            }
        }

        var doc_options = options;
        doc_options.recursive_typed_fields = doc_options.recursive_typed_fields or text_doc.recursive_typed_fields;
        if (text_doc.infer_type_dynamic_paths.len > 0) {
            doc_options.infer_type_dynamic_paths = text_doc.infer_type_dynamic_paths;
        }
        const typed_collect_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        if (text_doc.typed_fields) |projected_typed_fields| {
            for (projected_typed_fields) |field| {
                try appendTypedFieldValue(alloc, &typed_fields, field.field_name, @intCast(doc_idx), .{
                    .value_type = field.value_type,
                    .value = field.value,
                }, profile);
            }
        } else if (text_doc.typed_source) |typed_source| {
            try collectTypedFieldValuesFromValue(alloc, typed_source, @intCast(doc_idx), &typed_fields, text_analysis, doc_options);
        } else {
            try collectTypedFieldValues(alloc, text_doc.stored_data, @intCast(doc_idx), &typed_fields, text_analysis, doc_options);
        }
        if (profile_timings) {
            if (profile) |p| p.typed_collect_ns +|= platform_time.monotonicNs() - typed_collect_start_ns;
        }
        if (profile_working_set) {
            if (profile) |p| {
                p.peak_doc_scratch_bytes = @max(p.peak_doc_scratch_bytes, @as(u64, @intCast(doc_arena_state.queryCapacity())));
                p.builder_scratch_peak_bytes = @max(p.builder_scratch_peak_bytes, scratch.estimatedMemoryBytes());
            }
        }
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
    try resource_tracker.adjust(input_estimated_bytes +
        estimateStoredDocBytes(docs) +
        estimateFieldPostingsBuilderBytes(&field_builders) +
        estimateTypedDocValuesBytes(&typed_fields));

    const typed_build_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    var section_bytes: u64 = 0;
    var typed_it = typed_fields.iterator();
    while (typed_it.next()) |entry| {
        if (entry.value_ptr.conflicted or entry.value_ptr.writer == null) continue;
        const writer = &entry.value_ptr.writer.?;
        if (writer.entries.items.len == 0) continue;
        const section_data = try writer.build();
        section_bytes +|= @intCast(section_data.len);
        try typed_sections.append(alloc, .{
            .field_name = entry.key_ptr.*,
            .section_type = .typed_doc_values,
            .data = section_data,
        });
    }
    if (profile_timings) {
        if (profile) |p| p.typed_build_ns +|= platform_time.monotonicNs() - typed_build_start_ns;
    }

    const segment_encode_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    if (has_doc_ordinal) try seg_writer.addDocOrdinals(doc_ordinals.items);

    const section_attach_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    var fit = field_builders.iterator();
    while (fit.next()) |entry| {
        const field_name = entry.key_ptr.*;
        const inv_data = try entry.value_ptr.buildAlloc(alloc);
        errdefer alloc.free(inv_data);
        section_bytes +|= @intCast(inv_data.len);

        if (inv_data.len == 0) {
            alloc.free(inv_data);
            entry.value_ptr.deinit(alloc);
            continue;
        }

        const field_idx = field_indices.get(field_name).?;
        try seg_writer.addSectionOwned(field_idx, .inverted_text, inv_data);
        entry.value_ptr.deinit(alloc);
        if (profile_working_set) {
            if (profile) |p| {
                p.section_bytes = section_bytes;
                p.field_postings_estimated_bytes = estimateFieldPostingsBuilderBytes(&field_builders);
                p.postings_live_bytes = p.field_postings_estimated_bytes;
                p.fst_and_term_metadata_bytes = @max(p.fst_and_term_metadata_bytes, section_bytes);
            }
        }
        try resource_tracker.adjust(input_estimated_bytes +
            estimateStoredDocBytes(docs) +
            section_bytes +
            estimateFieldPostingsBuilderBytes(&field_builders) +
            estimateTypedDocValuesBytes(&typed_fields));
    }

    for (typed_sections.items) |*section| {
        const gop = try field_indices.getOrPut(alloc, section.field_name);
        if (!gop.found_existing) {
            gop.key_ptr.* = section.field_name;
            gop.value_ptr.* = try seg_writer.addField(section.field_name);
        }
        const owned = @constCast(section.data);
        try seg_writer.addSectionOwned(gop.value_ptr.*, section.section_type, owned);
        section.data = &.{};
    }
    if (profile_timings) {
        if (profile) |p| p.section_attach_ns +|= platform_time.monotonicNs() - section_attach_start_ns;
    }
    if (profile_working_set) {
        if (profile) |p| {
            p.section_bytes = section_bytes;
            p.section_live_bytes = section_bytes;
            p.field_postings_estimated_bytes = estimateFieldPostingsBuilderBytes(&field_builders);
            p.typed_doc_values_estimated_bytes = estimateTypedDocValuesBytes(&typed_fields);
            p.postings_live_bytes = p.field_postings_estimated_bytes;
            p.typed_live_bytes = p.typed_doc_values_estimated_bytes;
            p.fst_and_term_metadata_bytes = @max(p.fst_and_term_metadata_bytes, section_bytes);
        }
    }
    noteBuildMemorySample(profile, profile_working_set, "rss_after_postings_build");
    try resource_tracker.adjust(input_estimated_bytes + estimateStoredDocBytes(docs) + section_bytes);

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
            p.sink_live_bytes = p.segment_sink_bytes;
        }
    }
    noteBuildMemorySample(profile, profile_working_set, "rss_after_sections");
    try resource_tracker.adjust(section_bytes + @as(u64, @intCast(sink.len() - segment_start_len)));
    noteBuildMemorySample(profile, profile_working_set, "rss_after_publish");
}

const TermAcc = struct {
    freq: u32,
    positions: std.ArrayListUnmanaged(u32),
};

const FieldAcc = struct {
    token_offset: u32 = 0,
    term_map: std.StringHashMapUnmanaged(TermAcc) = .empty,
};

fn hasDuplicateTextFieldNames(fields: []const TextField) bool {
    for (fields, 0..) |field, i| {
        for (fields[0..i]) |prev| {
            if (std.mem.eql(u8, prev.field_name, field.field_name)) return true;
        }
    }
    return false;
}

fn cachedFieldAnalyzer(
    alloc: Allocator,
    cache: *std.StringHashMapUnmanaged(*const analysis_mod.Analyzer),
    field: TextField,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
) !*const analysis_mod.Analyzer {
    if (field.analyzer) |analyzer| return analyzer;
    const gop = try cache.getOrPut(alloc, field.field_name);
    if (!gop.found_existing) {
        gop.key_ptr.* = field.field_name;
        gop.value_ptr.* = resolveFieldAnalyzer(field.field_name, text_analysis) orelse default_analyzer;
    }
    return gop.value_ptr.*;
}

fn addSingleTextFieldToBuilders(
    alloc: Allocator,
    doc_alloc: Allocator,
    field_builders: *std.StringHashMapUnmanaged(FieldPostingsBuilder),
    field_indices: *std.StringHashMapUnmanaged(u16),
    seg_writer: *segment_mod.SegmentWriter,
    analyzer_cache: *std.StringHashMapUnmanaged(*const analysis_mod.Analyzer),
    doc_idx: u32,
    field: TextField,
    default_analyzer: *const analysis_mod.Analyzer,
    text_analysis: TextAnalysisConfig,
    profile: ?*BuildTextProfile,
    profile_timings: bool,
    scratch: *TextBuildScratch,
) !void {
    if (profile) |p| p.text_field_count +|= 1;
    const analyzer = try cachedFieldAnalyzer(alloc, analyzer_cache, field, default_analyzer, text_analysis);
    const analyzer_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    const tokens = try analyzer.analyze(doc_alloc, field.text);
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

    var field_acc = FieldAcc{};
    for (tokens) |tok| {
        const gop = try field_acc.term_map.getOrPut(doc_alloc, tok.term);
        if (!gop.found_existing) {
            gop.key_ptr.* = tok.term;
            gop.value_ptr.* = .{ .freq = 0, .positions = .empty };
        }
        gop.value_ptr.freq += 1;
        try gop.value_ptr.positions.append(doc_alloc, tok.position);
    }
    field_acc.token_offset = @intCast(tokens.len);
    if (profile_timings) {
        if (profile) |p| p.term_accum_ns +|= platform_time.monotonicNs() - term_accum_start_ns;
    }

    const hit_materialize_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
    scratch.hits.clearRetainingCapacity();
    var it = field_acc.term_map.iterator();
    while (it.next()) |entry| {
        try scratch.hits.append(alloc, .{
            .term = entry.key_ptr.*,
            .freq = entry.value_ptr.freq,
            .norm = field_acc.token_offset,
            .positions = entry.value_ptr.positions.items,
        });
        if (profile) |p| p.term_hit_count +|= 1;
    }
    if (profile_timings) {
        if (profile) |p| p.hit_materialize_ns +|= platform_time.monotonicNs() - hit_materialize_start_ns;
    }
    if (scratch.hits.items.len == 0) return;

    try addFieldHitsToBuilder(alloc, field_builders, field_indices, seg_writer, doc_idx, field.field_name, scratch.hits.items);
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
    field_builders: *std.StringHashMapUnmanaged(FieldPostingsBuilder),
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

const TypedFieldCollector = struct {
    value_type: ?typed_dv.ValueType = null,
    writer: ?typed_dv.TypedDocValuesWriter = null,
    conflicted: bool = false,
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
        try collectTypedFieldProjectionRecursiveScoped(alloc, value, "", text_analysis, options.infer_type_dynamic_paths, &fields);
    } else {
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const field_name = entry.key_ptr.*;
            if (field_name.len > 0 and field_name[0] == '_') continue;

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
    typed_fields: *std.StringHashMapUnmanaged(TypedFieldCollector),
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();
    try collectTypedFieldValuesFromValue(alloc, parsed.value, doc_id, typed_fields, text_analysis, options);
}

fn collectTypedFieldValuesFromValue(
    alloc: Allocator,
    value: std.json.Value,
    doc_id: u32,
    typed_fields: *std.StringHashMapUnmanaged(TypedFieldCollector),
    text_analysis: TextAnalysisConfig,
    options: BuildTextOptions,
) !void {
    if (value != .object) return;

    if (options.recursive_typed_fields) {
        try collectTypedFieldValuesRecursive(alloc, value, "", doc_id, typed_fields, text_analysis, options.profile);
        return;
    }
    if (options.infer_type_dynamic_paths.len > 0) {
        try collectTypedFieldValuesRecursiveScoped(alloc, value, "", doc_id, typed_fields, text_analysis, options.infer_type_dynamic_paths, options.profile);
        return;
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        const field_name = entry.key_ptr.*;
        if (field_name.len > 0 and field_name[0] == '_') continue;

        const detected = detectTypedValue(field_name, entry.value_ptr.*, text_analysis) orelse continue;
        try appendTypedFieldValue(alloc, typed_fields, field_name, doc_id, detected, options.profile);
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
    fields: *std.ArrayListUnmanaged(TypedFieldValue),
) !void {
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
                try collectTypedFieldProjectionRecursiveScoped(alloc, entry.value_ptr.*, child_path, text_analysis, scoped_paths, fields);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldProjectionRecursiveScoped(alloc, item, path, text_analysis, scoped_paths, fields);
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
        .f64_val => |number| .{ .f64_val = number },
        .geo_point => |point| .{ .geo_point = point },
        .bool_val => |boolean| .{ .bool_val = boolean },
    };
}

fn collectTypedFieldValuesRecursive(
    alloc: Allocator,
    value: std.json.Value,
    path: []const u8,
    doc_id: u32,
    typed_fields: *std.StringHashMapUnmanaged(TypedFieldCollector),
    text_analysis: TextAnalysisConfig,
    profile: ?*BuildTextProfile,
) !void {
    if (path.len > 0) {
        if (detectTypedValue(path, value, text_analysis)) |detected| {
            try appendTypedFieldValue(alloc, typed_fields, path, doc_id, detected, profile);
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
                try collectTypedFieldValuesRecursive(alloc, entry.value_ptr.*, child_path, doc_id, typed_fields, text_analysis, profile);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldValuesRecursive(alloc, item, path, doc_id, typed_fields, text_analysis, profile);
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
    typed_fields: *std.StringHashMapUnmanaged(TypedFieldCollector),
    text_analysis: TextAnalysisConfig,
    scoped_paths: []const []const u8,
    profile: ?*BuildTextProfile,
) !void {
    if (path.len > 0 and pathFallsUnderAnyScopedPath(scoped_paths, path)) {
        if (detectTypedValue(path, value, text_analysis)) |detected| {
            try appendTypedFieldValue(alloc, typed_fields, path, doc_id, detected, profile);
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
                try collectTypedFieldValuesRecursiveScoped(alloc, entry.value_ptr.*, child_path, doc_id, typed_fields, text_analysis, scoped_paths, profile);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectTypedFieldValuesRecursiveScoped(alloc, item, path, doc_id, typed_fields, text_analysis, scoped_paths, profile);
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

fn appendTypedFieldValue(
    alloc: Allocator,
    typed_fields: *std.StringHashMapUnmanaged(TypedFieldCollector),
    field_name: []const u8,
    doc_id: u32,
    detected: DetectedTypedValue,
    profile: ?*BuildTextProfile,
) !void {
    const gop = try typed_fields.getOrPut(alloc, field_name);
    if (!gop.found_existing) {
        gop.key_ptr.* = try alloc.dupe(u8, field_name);
        gop.value_ptr.* = .{};
    }
    if (gop.value_ptr.conflicted) return;

    if (gop.value_ptr.value_type == null) {
        gop.value_ptr.value_type = detected.value_type;
        gop.value_ptr.writer = typed_dv.TypedDocValuesWriter.init(alloc, detected.value_type, typed_dv.default_chunk_size);
    } else if (gop.value_ptr.value_type.? != detected.value_type) {
        if (gop.value_ptr.writer) |*writer| writer.deinit();
        gop.value_ptr.writer = null;
        gop.value_ptr.conflicted = true;
        return;
    }

    try gop.value_ptr.writer.?.add(doc_id, detected.value);
    if (profile) |p| p.typed_value_count +|= 1;
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

fn resolveTokenFilterName(name: []const u8, named: []const NamedTokenFilter) !analysis_mod.TokenFilter {
    if (std.mem.eql(u8, name, "to_lower")) return .lowercase;
    if (std.mem.eql(u8, name, "stop_en")) return .stop_words;
    if (std.mem.eql(u8, name, "stemmer_en")) return .stemmer;
    for (named) |item| {
        if (std.mem.eql(u8, item.name, name)) return item.filter;
    }
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
    return error.InvalidArgument;
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
    try std.testing.expectEqual(@as(u32, 2), snap.global_doc_count);

    const results = try snap.search(alloc, "title", &.{"hello"}, 10);
    defer alloc.free(results.hits);
    try std.testing.expectEqual(@as(usize, 2), results.hits.len);

    // Verify stored docs accessible
    const stored = snap.segments[0].reader.storedDoc(0).?;
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
    try std.testing.expectEqual(@as(u32, 2), writer.snapshot().global_doc_count);
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
    try std.testing.expectEqual(@as(u32, 2), snap.global_doc_count);

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
    budgets[@intFromEnum(resource_manager_mod.Slice.full_text_build_working_set)] = .{
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

    try std.testing.expect(reader.getSection("title", .inverted_text) == null);
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

    const price_section = reader.getSection("price", .typed_doc_values) orelse return error.TestExpectedEqual;
    var price_reader = try typed_dv.TypedDocValuesReader.init(alloc, price_section);
    try std.testing.expectEqual(typed_dv.ValueType.f64_val, price_reader.value_type);
    try std.testing.expectEqual(@as(?f64, 10.5), try price_reader.getF64(0));

    const ts_section = reader.getSection("published_at", .typed_doc_values) orelse return error.TestExpectedEqual;
    var ts_reader = try typed_dv.TypedDocValuesReader.init(alloc, ts_section);
    try std.testing.expectEqual(typed_dv.ValueType.u64_val, ts_reader.value_type);
    try std.testing.expect((try ts_reader.getU64(0)) != null);

    const geo_section = reader.getSection("location", .typed_doc_values) orelse return error.TestExpectedEqual;
    var geo_reader = try typed_dv.TypedDocValuesReader.init(alloc, geo_section);
    try std.testing.expectEqual(typed_dv.ValueType.geo_point, geo_reader.value_type);
    const point = (try geo_reader.getGeoPoint(0)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 37.7749), point.lat, 0.00001);
    try std.testing.expectApproxEqAbs(@as(f64, -122.4194), point.lon, 0.00001);

    const bool_section = reader.getSection("active", .typed_doc_values) orelse return error.TestExpectedEqual;
    var bool_reader = try typed_dv.TypedDocValuesReader.init(alloc, bool_section);
    try std.testing.expectEqual(typed_dv.ValueType.bool_val, bool_reader.value_type);
    try std.testing.expectEqual(@as(?bool, true), try bool_reader.getBool(0));
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

    const ts_section = reader.getSection("published_at", .typed_doc_values) orelse return error.TestExpectedEqual;
    var ts_reader = try typed_dv.TypedDocValuesReader.init(alloc, ts_section);
    try std.testing.expectEqual(typed_dv.ValueType.u64_val, ts_reader.value_type);
    try std.testing.expect((try ts_reader.getU64(0)) != null);
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
