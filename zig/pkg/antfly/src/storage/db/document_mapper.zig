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

const std = @import("std");
const Allocator = std.mem.Allocator;
const vector_codec = @import("antfly_vector").codec;
const regex_mod = @import("antfly_regex");
const introducer_mod = @import("../../introducer.zig");
const typed_dv = @import("../../section/typed_doc_values.zig");
const relational_manifest = @import("../../section/relational_manifest.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const analysis_mod = @import("../../search/analysis.zig");
const schema_api = @import("../../schema/mod.zig");
const runtime_schema = @import("../schema.zig");
const types = @import("types.zig");

pub const schema_less_exact_field_suffix = ".keyword";
pub const schema_less_exact_max_bytes: usize = 1024;

pub const MapperDoc = struct {
    key: []const u8,
    value: []const u8,
    doc_ordinal: ?u32 = null,
};

pub const SparseVectorData = struct {
    indices: []u32,
    values: []f32,

    pub fn deinit(self: *SparseVectorData, alloc: Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub const ExtractedWrite = struct {
    cleaned_value: ?[]u8,
    graph_writes: []types.GraphEdgeWrite,
    mentioned_graph_indexes: [][]u8,
    dense_embeddings: []DenseEmbeddingWrite,
    sparse_embeddings: []SparseEmbeddingWrite,

    pub fn deinit(self: *ExtractedWrite, alloc: Allocator) void {
        if (self.cleaned_value) |value| alloc.free(value);
        for (self.graph_writes) |graph_write| {
            alloc.free(@constCast(graph_write.index_name));
            alloc.free(@constCast(graph_write.source));
            alloc.free(@constCast(graph_write.target));
            alloc.free(@constCast(graph_write.edge_type));
            if (graph_write.metadata_json.len > 0) alloc.free(@constCast(graph_write.metadata_json));
        }
        if (self.graph_writes.len > 0) alloc.free(self.graph_writes);
        for (self.mentioned_graph_indexes) |index_name| alloc.free(index_name);
        if (self.mentioned_graph_indexes.len > 0) alloc.free(self.mentioned_graph_indexes);
        for (self.dense_embeddings) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            if (embedding.vector.len > 0) alloc.free(embedding.vector);
        }
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.sparse_embeddings) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            if (embedding.indices.len > 0) alloc.free(embedding.indices);
            if (embedding.values.len > 0) alloc.free(embedding.values);
        }
        if (self.sparse_embeddings.len > 0) alloc.free(self.sparse_embeddings);
        self.* = undefined;
    }
};

pub const DenseEmbeddingWrite = struct {
    index_name: []u8,
    doc_key: []u8,
    parent_doc_key: ?[]const u8 = null,
    artifact_key: ?[]u8 = null,
    vector: []f32,
};

pub const SparseEmbeddingWrite = struct {
    index_name: []u8,
    doc_key: []u8,
    artifact_key: ?[]u8 = null,
    indices: []u32,
    values: []f32,
};

pub const ObservedFieldAnalyzer = struct {
    field_name: []u8,
    analyzer_name: []u8,
};

pub const BuildTextSegmentResult = struct {
    segment: ?[]u8 = null,
    observed_field_analyzers: []ObservedFieldAnalyzer = &.{},

    pub fn deinit(self: *BuildTextSegmentResult, alloc: Allocator) void {
        if (self.segment) |segment| alloc.free(segment);
        for (self.observed_field_analyzers) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        if (self.observed_field_analyzers.len > 0) alloc.free(self.observed_field_analyzers);
        self.* = undefined;
    }
};

pub const default_text_segment_target_bytes: usize = 256 * 1024 * 1024;

pub const BuildTextSegmentsOptions = struct {
    target_segment_bytes: usize = default_text_segment_target_bytes,
    profile: ?*introducer_mod.BuildTextProfile = null,
};

pub const BuildTextSegmentsResult = struct {
    segments: [][]u8 = &.{},
    observed_field_analyzers: []ObservedFieldAnalyzer = &.{},

    pub fn deinit(self: *BuildTextSegmentsResult, alloc: Allocator) void {
        for (self.segments) |segment| {
            if (segment.len > 0) alloc.free(segment);
        }
        if (self.segments.len > 0) alloc.free(self.segments);
        for (self.observed_field_analyzers) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        if (self.observed_field_analyzers.len > 0) alloc.free(self.observed_field_analyzers);
        self.* = undefined;
    }
};

pub const TextProjectionBatch = struct {
    docs: []const introducer_mod.TextDocument,
    observed_field_analyzers: []const ObservedFieldAnalyzer = &.{},
    /// Non-null for relational tables: the column catalog the segment's manifest
    /// is written from and document bodies are reconstructed against on read.
    relational_manifest_columns: ?[]const relational_manifest.ManifestColumn = null,
};

/// Derive the segment manifest column catalog from a runtime schema, or null for
/// document-mode tables. Allocated on `alloc`; `name`/`path` borrow the schema's
/// column strings (valid as long as the schema is).
pub fn relationalManifestColumnsAlloc(
    alloc: Allocator,
    schema: ?runtime_schema.TableSchema,
) !?[]const relational_manifest.ManifestColumn {
    const runtime = schema orelse return null;
    if (runtime.storage_mode != .relational or runtime.relational_columns.len == 0) return null;
    const columns = try alloc.alloc(relational_manifest.ManifestColumn, runtime.relational_columns.len);
    for (runtime.relational_columns, 0..) |column, i| {
        columns[i] = .{
            .name = column.name,
            .path = column.path,
            .value_type = relationalStorageValueType(column.field_type),
            .is_json = column.field_type == .json,
        };
    }
    return columns;
}

pub const TextProjectionSourceDoc = struct {
    key: []const u8,
    root: std.json.Value,
    stored_data: []const u8,
    typed_source: ?std.json.Value,
    doc_ordinal: ?u32 = null,
    schema_less_text_fields: []const introducer_mod.TextField = &.{},
    schema_less_fast_projection: bool = false,
};

pub const TextProjectionSourceBatch = struct {
    docs: []const TextProjectionSourceDoc,
};

pub const TextProjectionOptions = struct {
    vector_field_paths: []const []const u8 = &.{},
    strip_numeric_array_heuristic: bool = true,
    schema_less_fast_projection: bool = false,
};

const ExtractedTextFields = struct {
    fields: []const introducer_mod.TextField,
    recursive_typed_fields: bool = false,
    infer_type_dynamic_paths: []const []const u8 = &.{},
    typed_fields: ?[]const introducer_mod.TypedFieldValue = null,
};

pub fn buildTextSegmentFromDocuments(
    alloc: Allocator,
    docs: []const MapperDoc,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
) !?[]u8 {
    const result = try buildTextSegmentFromDocumentsWithMetadata(alloc, docs, text_analysis, schema);
    defer {
        for (result.observed_field_analyzers) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        if (result.observed_field_analyzers.len > 0) alloc.free(result.observed_field_analyzers);
    }
    return result.segment;
}

pub fn buildTextSegmentFromDocumentsWithMetadata(
    alloc: Allocator,
    docs: []const MapperDoc,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
) !BuildTextSegmentResult {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var observed_field_analyzers = std.ArrayListUnmanaged(ObservedFieldAnalyzer).empty;

    const projection_batch = try buildTextProjectionBatch(arena, docs, text_analysis, schema, &observed_field_analyzers);

    if (projection_batch.docs.len == 0) {
        return .{
            .segment = null,
            .observed_field_analyzers = try cloneObservedFieldAnalyzers(alloc, observed_field_analyzers.items),
        };
    }
    const segment = try buildTextSegmentFromProjectionBatch(alloc, projection_batch, text_analysis);
    return .{
        .segment = segment,
        .observed_field_analyzers = try cloneObservedFieldAnalyzers(alloc, observed_field_analyzers.items),
    };
}

pub fn buildTextSegmentsFromDocumentsWithMetadata(
    alloc: Allocator,
    docs: []const MapperDoc,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
    options: BuildTextSegmentsOptions,
) !BuildTextSegmentsResult {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var observed_field_analyzers = std.ArrayListUnmanaged(ObservedFieldAnalyzer).empty;
    const projection_batch = try buildTextProjectionBatch(arena, docs, text_analysis, schema, &observed_field_analyzers);

    const segments = try buildTextSegmentsFromProjectionBatch(alloc, projection_batch, text_analysis, options);
    errdefer freeTextSegments(alloc, segments);

    return .{
        .segments = segments,
        .observed_field_analyzers = try cloneObservedFieldAnalyzers(alloc, observed_field_analyzers.items),
    };
}

pub fn buildTextProjectionBatch(
    arena: Allocator,
    docs: []const MapperDoc,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !TextProjectionBatch {
    const source = try buildTextProjectionSourceBatchWithOptions(arena, docs, .{
        .schema_less_fast_projection = schema == null,
    });
    return try buildTextProjectionBatchFromSource(arena, source.docs, text_analysis, schema, observed_field_analyzers);
}

pub fn buildTextProjectionSourceBatch(
    arena: Allocator,
    docs: []const MapperDoc,
) !TextProjectionSourceBatch {
    return try buildTextProjectionSourceBatchWithOptions(arena, docs, .{});
}

pub fn buildTextProjectionSourceBatchWithOptions(
    arena: Allocator,
    docs: []const MapperDoc,
    opts: TextProjectionOptions,
) !TextProjectionSourceBatch {
    var source_docs = std.ArrayListUnmanaged(TextProjectionSourceDoc).empty;
    defer source_docs.deinit(arena);

    for (docs) |doc| {
        try appendTextProjectionSourceDoc(arena, &source_docs, doc.key, doc.value, doc.doc_ordinal, opts);
    }

    return .{
        .docs = try arena.dupe(TextProjectionSourceDoc, source_docs.items),
    };
}

pub fn buildTextProjectionSourceBatchFromWrites(
    arena: Allocator,
    writes: []const types.BatchWrite,
) !TextProjectionSourceBatch {
    return try buildTextProjectionSourceBatchFromWritesWithOptions(arena, writes, .{});
}

pub fn buildTextProjectionSourceBatchFromWritesWithOptions(
    arena: Allocator,
    writes: []const types.BatchWrite,
    opts: TextProjectionOptions,
) !TextProjectionSourceBatch {
    var source_docs = std.ArrayListUnmanaged(TextProjectionSourceDoc).empty;
    defer source_docs.deinit(arena);

    for (writes) |write| {
        try appendTextProjectionSourceDoc(arena, &source_docs, write.key, write.value, null, opts);
    }

    return .{
        .docs = try arena.dupe(TextProjectionSourceDoc, source_docs.items),
    };
}

fn appendTextProjectionSourceDoc(
    arena: Allocator,
    source_docs: *std.ArrayListUnmanaged(TextProjectionSourceDoc),
    key: []const u8,
    value: []const u8,
    doc_ordinal: ?u32,
    opts: TextProjectionOptions,
) !void {
    if (opts.schema_less_fast_projection and canUseSchemaLessRawTextFastPath(value, opts)) {
        try source_docs.append(arena, .{
            .key = key,
            .root = .null,
            .stored_data = value,
            .typed_source = null,
            .doc_ordinal = doc_ordinal,
            .schema_less_text_fields = try extractStringFieldsNoSchemaRaw(arena, value, opts),
            .schema_less_fast_projection = true,
        });
        return;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, value, .{});
    const root = parsed.value;
    const stored_projection = try fullTextStoredProjection(arena, root, value, opts);
    try source_docs.append(arena, .{
        .key = key,
        .root = root,
        .stored_data = stored_projection.stored_data,
        .typed_source = stored_projection.typed_source,
        .doc_ordinal = doc_ordinal,
    });
}

pub fn buildTextProjectionBatchFromSource(
    arena: Allocator,
    source_docs: []const TextProjectionSourceDoc,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !TextProjectionBatch {
    var text_docs = std.ArrayListUnmanaged(introducer_mod.TextDocument).empty;
    defer text_docs.deinit(arena);

    for (source_docs) |doc| {
        const extraction_root = doc.typed_source orelse doc.root;
        const extracted = if (schema == null and doc.schema_less_fast_projection)
            ExtractedTextFields{ .fields = doc.schema_less_text_fields }
        else
            try extractTextFieldsFromValue(arena, extraction_root, text_analysis, schema, observed_field_analyzers);
        if (extracted.fields.len == 0 and !extracted.recursive_typed_fields and extracted.infer_type_dynamic_paths.len == 0) continue;

        try text_docs.append(arena, .{
            .id = doc.key,
            .stored_data = doc.stored_data,
            .doc_ordinal = doc.doc_ordinal,
            .text_fields = extracted.fields,
            .recursive_typed_fields = extracted.recursive_typed_fields,
            .infer_type_dynamic_paths = extracted.infer_type_dynamic_paths,
            .typed_fields = extracted.typed_fields orelse if (doc.typed_source == null) &.{} else null,
            .typed_source = if (extracted.typed_fields == null) doc.typed_source else null,
        });
    }

    return .{
        .docs = try arena.dupe(introducer_mod.TextDocument, text_docs.items),
        .observed_field_analyzers = if (observed_field_analyzers) |items| items.items else &.{},
        .relational_manifest_columns = try relationalManifestColumnsAlloc(arena, schema),
    };
}

pub fn buildTextSegmentFromProjectionBatch(
    alloc: Allocator,
    projection_batch: TextProjectionBatch,
    text_analysis: introducer_mod.TextAnalysisConfig,
) !?[]u8 {
    if (projection_batch.docs.len == 0) return null;
    return try buildTextSegmentFromProjectionBatchWithProfile(alloc, projection_batch, text_analysis, null);
}

fn buildTextSegmentFromProjectionBatchWithProfile(
    alloc: Allocator,
    projection_batch: TextProjectionBatch,
    text_analysis: introducer_mod.TextAnalysisConfig,
    profile: ?*introducer_mod.BuildTextProfile,
) !?[]u8 {
    if (projection_batch.docs.len == 0) return null;
    return try introducer_mod.buildSegmentFromTextWithAnalysisOptions(alloc, projection_batch.docs, &analysis_mod.default_analyzer, text_analysis, .{
        .profile = profile,
        .relational_manifest_columns = projection_batch.relational_manifest_columns,
    });
}

pub fn buildTextSegmentsFromProjectionBatch(
    alloc: Allocator,
    projection_batch: TextProjectionBatch,
    text_analysis: introducer_mod.TextAnalysisConfig,
    options: BuildTextSegmentsOptions,
) ![][]u8 {
    if (projection_batch.docs.len == 0) return try alloc.alloc([]u8, 0);

    var segments = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (segments.items) |segment| alloc.free(segment);
        segments.deinit(alloc);
    }

    const target_bytes = @max(@as(usize, 1), options.target_segment_bytes);
    var start: usize = 0;
    while (start < projection_batch.docs.len) {
        const end = splitProjectionDocsEnd(projection_batch.docs, start, target_bytes);
        const chunk: TextProjectionBatch = .{
            .docs = projection_batch.docs[start..end],
            .observed_field_analyzers = &.{},
            .relational_manifest_columns = projection_batch.relational_manifest_columns,
        };
        if (try buildTextSegmentFromProjectionBatchWithProfile(alloc, chunk, text_analysis, options.profile)) |segment| {
            try segments.append(alloc, segment);
        }
        start = end;
    }

    return try segments.toOwnedSlice(alloc);
}

pub fn freeTextSegments(alloc: Allocator, segments: [][]u8) void {
    for (segments) |segment| {
        if (segment.len > 0) alloc.free(segment);
    }
    if (segments.len > 0) alloc.free(segments);
}

fn splitProjectionDocsEnd(docs: []const introducer_mod.TextDocument, start: usize, target_bytes: usize) usize {
    var total: usize = 0;
    var end = start;
    while (end < docs.len) : (end += 1) {
        const doc_bytes = estimateProjectedTextDocBytes(docs[end]);
        if (end > start and total +| doc_bytes > target_bytes) break;
        total +|= doc_bytes;
    }
    return end;
}

fn estimateProjectedTextDocBytes(doc: introducer_mod.TextDocument) usize {
    var total: usize = 64 + doc.id.len + doc.stored_data.len;
    for (doc.text_fields) |field| {
        total +|= 16 + field.field_name.len + field.text.len;
    }
    if (doc.typed_fields) |typed_fields| {
        total +|= typed_fields.len * 32;
        for (typed_fields) |field| total +|= field.field_name.len;
    }
    for (doc.infer_type_dynamic_paths) |path| total +|= path.len;
    return total;
}

fn cloneObservedFieldAnalyzers(
    alloc: Allocator,
    items: []const ObservedFieldAnalyzer,
) ![]ObservedFieldAnalyzer {
    if (items.len == 0) return &.{};

    const cloned = try alloc.alloc(ObservedFieldAnalyzer, items.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        alloc.free(cloned);
    }
    for (items, 0..) |item, i| {
        cloned[i] = .{
            .field_name = try alloc.dupe(u8, item.field_name),
            .analyzer_name = try alloc.dupe(u8, item.analyzer_name),
        };
        initialized += 1;
    }
    return cloned;
}

pub fn extractDenseVectorField(
    alloc: Allocator,
    data: []const u8,
    field_name: []const u8,
    dims: u32,
) !?[]f32 {
    var scanner = std.json.Scanner.initCompleteInput(alloc, data);
    defer scanner.deinit();

    switch (try scanner.next()) {
        .object_begin => {},
        else => return null,
    }

    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .object_end => {
                _ = try scanner.next();
                return null;
            },
            .string => {},
            else => return null,
        }

        const key_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, key_token);
        const key = jsonTokenSlice(key_token) orelse return error.InvalidEmbeddingField;
        if (!std.mem.eql(u8, key, field_name)) {
            try scanner.skipValue();
            continue;
        }

        switch (try scanner.next()) {
            .array_begin => {},
            else => return null,
        }

        const values = try alloc.alloc(f32, dims);
        errdefer alloc.free(values);

        var count: usize = 0;
        while (true) {
            switch (try scanner.peekNextTokenType()) {
                .array_end => {
                    _ = try scanner.next();
                    if (count != dims) return error.InvalidVectorDimensions;
                    return values;
                },
                .number => {},
                else => return error.InvalidVectorValue,
            }

            if (count >= dims) return error.InvalidVectorDimensions;
            const value_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
            defer freeJsonAllocatedToken(alloc, value_token);
            const value_bytes = jsonTokenSlice(value_token) orelse return error.InvalidVectorValue;
            values[count] = try std.fmt.parseFloat(f32, value_bytes);
            count += 1;
        }
    }
}

pub fn extractSparseVectorField(
    alloc: Allocator,
    data: []const u8,
    field_name: []const u8,
) !?SparseVectorData {
    return extractSparseVectorFieldRawFast(alloc, data, field_name) catch |raw_err| switch (raw_err) {
        error.UnsupportedSparseFastPath => return extractSparseVectorFieldFast(alloc, data, field_name) catch |err| switch (err) {
            error.UnsupportedSparseFastPath => return extractSparseVectorFieldSlow(alloc, data, field_name),
            else => return err,
        },
        else => return raw_err,
    };
}

fn extractSparseVectorFieldRawFast(
    alloc: Allocator,
    data: []const u8,
    field_name: []const u8,
) !?SparseVectorData {
    var pos: usize = 0;
    skipJsonWhitespace(data, &pos);
    if (pos >= data.len or data[pos] != '{') return null;
    pos += 1;

    while (true) {
        skipJsonWhitespace(data, &pos);
        if (pos >= data.len) return error.SyntaxError;
        if (data[pos] == '}') return null;
        const key = try parseRawJsonString(data, &pos);
        skipJsonWhitespace(data, &pos);
        if (pos >= data.len or data[pos] != ':') return error.SyntaxError;
        pos += 1;
        skipJsonWhitespace(data, &pos);
        if (std.mem.eql(u8, key, field_name)) {
            if (pos >= data.len or data[pos] != '{') return null;
            return try parseSparseVectorObjectRawFast(alloc, data, &pos);
        }
        try skipRawJsonValue(data, &pos);
        skipJsonWhitespace(data, &pos);
        if (pos >= data.len) return error.SyntaxError;
        if (data[pos] == ',') {
            pos += 1;
            continue;
        }
        if (data[pos] == '}') return null;
        return error.SyntaxError;
    }
}

fn parseSparseVectorObjectRawFast(alloc: Allocator, data: []const u8, pos: *usize) !SparseVectorData {
    if (pos.* >= data.len or data[pos.*] != '{') return error.SyntaxError;
    pos.* += 1;

    var indices: ?[]u32 = null;
    var values: ?[]f32 = null;
    var saw_supported_field = false;
    errdefer {
        if (indices) |items| alloc.free(items);
        if (values) |items| alloc.free(items);
    }

    while (true) {
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == '}') {
            pos.* += 1;
            break;
        }
        const key = try parseRawJsonString(data, pos);
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len or data[pos.*] != ':') return error.SyntaxError;
        pos.* += 1;
        skipJsonWhitespace(data, pos);

        if (std.mem.eql(u8, key, "indices")) {
            if (indices != null) return error.InvalidSparseVector;
            indices = try parseRawU32Array(alloc, data, pos);
            saw_supported_field = true;
        } else if (std.mem.eql(u8, key, "values")) {
            if (values != null) return error.InvalidSparseVector;
            values = try parseRawF32Array(alloc, data, pos);
            saw_supported_field = true;
        } else if (saw_supported_field) {
            try skipRawJsonValue(data, pos);
        } else {
            return error.UnsupportedSparseFastPath;
        }

        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == ',') {
            pos.* += 1;
            continue;
        }
        if (data[pos.*] == '}') {
            pos.* += 1;
            break;
        }
        return error.SyntaxError;
    }

    const out_indices = indices orelse return error.UnsupportedSparseFastPath;
    const out_values = values orelse return error.InvalidSparseVector;
    if (out_indices.len != out_values.len) return error.InvalidSparseVector;
    indices = null;
    values = null;
    return .{
        .indices = out_indices,
        .values = out_values,
    };
}

fn parseRawU32Array(alloc: Allocator, data: []const u8, pos: *usize) ![]u32 {
    if (pos.* >= data.len or data[pos.*] != '[') return error.InvalidSparseVector;
    pos.* += 1;
    var out = std.ArrayListUnmanaged(u32).empty;
    errdefer out.deinit(alloc);
    while (true) {
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == ']') {
            pos.* += 1;
            return try out.toOwnedSlice(alloc);
        }
        const raw = try parseRawJsonNumber(data, pos);
        try out.append(alloc, try std.fmt.parseInt(u32, raw, 10));
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == ',') {
            pos.* += 1;
            continue;
        }
        if (data[pos.*] == ']') {
            pos.* += 1;
            return try out.toOwnedSlice(alloc);
        }
        return error.SyntaxError;
    }
}

fn parseRawF32Array(alloc: Allocator, data: []const u8, pos: *usize) ![]f32 {
    if (pos.* >= data.len or data[pos.*] != '[') return error.InvalidSparseVector;
    pos.* += 1;
    var out = std.ArrayListUnmanaged(f32).empty;
    errdefer out.deinit(alloc);
    while (true) {
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == ']') {
            pos.* += 1;
            return try out.toOwnedSlice(alloc);
        }
        const raw = try parseRawJsonNumber(data, pos);
        try out.append(alloc, try std.fmt.parseFloat(f32, raw));
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == ',') {
            pos.* += 1;
            continue;
        }
        if (data[pos.*] == ']') {
            pos.* += 1;
            return try out.toOwnedSlice(alloc);
        }
        return error.SyntaxError;
    }
}

fn parseRawJsonNumber(data: []const u8, pos: *usize) ![]const u8 {
    const start = pos.*;
    if (pos.* < data.len and (data[pos.*] == '-' or data[pos.*] == '+')) pos.* += 1;
    var saw_digit = false;
    while (pos.* < data.len and std.ascii.isDigit(data[pos.*])) : (pos.* += 1) saw_digit = true;
    if (pos.* < data.len and data[pos.*] == '.') {
        pos.* += 1;
        while (pos.* < data.len and std.ascii.isDigit(data[pos.*])) : (pos.* += 1) saw_digit = true;
    }
    if (!saw_digit) return error.InvalidSparseVector;
    if (pos.* < data.len and (data[pos.*] == 'e' or data[pos.*] == 'E')) {
        pos.* += 1;
        if (pos.* < data.len and (data[pos.*] == '-' or data[pos.*] == '+')) pos.* += 1;
        var saw_exponent_digit = false;
        while (pos.* < data.len and std.ascii.isDigit(data[pos.*])) : (pos.* += 1) saw_exponent_digit = true;
        if (!saw_exponent_digit) return error.InvalidSparseVector;
    }
    return data[start..pos.*];
}

fn parseRawJsonString(data: []const u8, pos: *usize) ![]const u8 {
    if (pos.* >= data.len or data[pos.*] != '"') return error.SyntaxError;
    pos.* += 1;
    const start = pos.*;
    while (pos.* < data.len) : (pos.* += 1) {
        switch (data[pos.*]) {
            '"' => {
                const out = data[start..pos.*];
                pos.* += 1;
                return out;
            },
            '\\' => return error.UnsupportedSparseFastPath,
            else => {},
        }
    }
    return error.SyntaxError;
}

fn skipJsonWhitespace(data: []const u8, pos: *usize) void {
    while (pos.* < data.len) : (pos.* += 1) {
        switch (data[pos.*]) {
            ' ', '\n', '\r', '\t' => {},
            else => return,
        }
    }
}

fn skipRawJsonValue(data: []const u8, pos: *usize) !void {
    skipJsonWhitespace(data, pos);
    if (pos.* >= data.len) return error.SyntaxError;
    switch (data[pos.*]) {
        '"' => {
            _ = try parseRawJsonStringAllowEscapes(data, pos);
            return;
        },
        '{', '[' => {},
        else => {
            while (pos.* < data.len) : (pos.* += 1) {
                switch (data[pos.*]) {
                    ',', '}', ']', ' ', '\n', '\r', '\t' => return,
                    else => {},
                }
            }
            return;
        },
    }

    var depth: usize = 0;
    while (pos.* < data.len) {
        switch (data[pos.*]) {
            '"' => {
                _ = try parseRawJsonStringAllowEscapes(data, pos);
                continue;
            },
            '{', '[' => {
                depth += 1;
                pos.* += 1;
            },
            '}', ']' => {
                if (depth == 0) return error.SyntaxError;
                depth -= 1;
                pos.* += 1;
                if (depth == 0) return;
            },
            else => pos.* += 1,
        }
    }
    return error.SyntaxError;
}

fn parseRawJsonStringAllowEscapes(data: []const u8, pos: *usize) !void {
    if (pos.* >= data.len or data[pos.*] != '"') return error.SyntaxError;
    pos.* += 1;
    while (pos.* < data.len) : (pos.* += 1) {
        switch (data[pos.*]) {
            '"' => {
                pos.* += 1;
                return;
            },
            '\\' => {
                pos.* += 1;
                if (pos.* >= data.len) return error.SyntaxError;
            },
            else => {},
        }
    }
    return error.SyntaxError;
}

fn extractSparseVectorFieldFast(
    alloc: Allocator,
    data: []const u8,
    field_name: []const u8,
) !?SparseVectorData {
    var scanner = std.json.Scanner.initCompleteInput(alloc, data);
    defer scanner.deinit();

    switch (try scanner.next()) {
        .object_begin => {},
        else => return null,
    }

    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .object_end => {
                _ = try scanner.next();
                return null;
            },
            .string => {},
            else => return null,
        }

        const key_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, key_token);
        const key = jsonTokenSlice(key_token) orelse return error.InvalidSparseVector;
        if (!std.mem.eql(u8, key, field_name)) {
            try scanner.skipValue();
            continue;
        }

        switch (try scanner.peekNextTokenType()) {
            .object_begin => return try parseSparseVectorObjectFast(alloc, &scanner),
            else => return null,
        }
    }
}

fn extractSparseVectorFieldSlow(
    alloc: Allocator,
    data: []const u8,
    field_name: []const u8,
) !?SparseVectorData {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, data, .{});
    defer parsed.deinit();
    const root = parsed.value;

    if (root != .object) return null;
    const field = root.object.get(field_name) orelse return null;
    if (field != .object) return null;

    const indices_val = field.object.get("indices") orelse return error.InvalidSparseVector;
    const values_val = field.object.get("values") orelse return error.InvalidSparseVector;
    if (indices_val != .array or values_val != .array) return error.InvalidSparseVector;
    if (indices_val.array.items.len != values_val.array.items.len) return error.InvalidSparseVector;

    const indices = try alloc.alloc(u32, indices_val.array.items.len);
    errdefer alloc.free(indices);
    const values = try alloc.alloc(f32, values_val.array.items.len);
    errdefer alloc.free(values);

    for (indices_val.array.items, 0..) |item, i| {
        indices[i] = try jsonNumberToU32(item);
    }
    for (values_val.array.items, 0..) |item, i| {
        values[i] = try jsonNumberToF32(item);
    }

    return .{
        .indices = indices,
        .values = values,
    };
}

fn parseSparseVectorObjectFast(alloc: Allocator, scanner: *std.json.Scanner) !SparseVectorData {
    switch (try scanner.next()) {
        .object_begin => {},
        else => return error.InvalidSparseVector,
    }

    var indices: ?[]u32 = null;
    var values: ?[]f32 = null;
    var packed_indices: ?[]u8 = null;
    var packed_values: ?[]u8 = null;
    var saw_supported_field = false;
    errdefer {
        if (indices) |items| alloc.free(items);
        if (values) |items| alloc.free(items);
        if (packed_indices) |items| alloc.free(items);
        if (packed_values) |items| alloc.free(items);
    }

    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .object_end => {
                _ = try scanner.next();
                break;
            },
            .string => {},
            else => return error.InvalidSparseVector,
        }

        const key_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, key_token);
        const key = jsonTokenSlice(key_token) orelse return error.InvalidSparseVector;
        if (std.mem.eql(u8, key, "indices")) {
            if (indices != null) return error.InvalidSparseVector;
            indices = try parseSparseU32ArrayFast(alloc, scanner);
            saw_supported_field = true;
        } else if (std.mem.eql(u8, key, "values")) {
            if (values != null) return error.InvalidSparseVector;
            values = try parseSparseF32ArrayFast(alloc, scanner);
            saw_supported_field = true;
        } else if (std.mem.eql(u8, key, "packed_indices")) {
            if (packed_indices != null) return error.InvalidSparseVector;
            packed_indices = try parseSparseStringDupFast(alloc, scanner);
            saw_supported_field = true;
        } else if (std.mem.eql(u8, key, "packed_values")) {
            if (packed_values != null) return error.InvalidSparseVector;
            packed_values = try parseSparseStringDupFast(alloc, scanner);
            saw_supported_field = true;
        } else if (saw_supported_field) {
            try scanner.skipValue();
        } else {
            return error.UnsupportedSparseFastPath;
        }
    }

    if (packed_indices != null or packed_values != null) {
        const raw_indices = packed_indices orelse return error.InvalidSparseVector;
        const raw_values = packed_values orelse return error.InvalidSparseVector;
        var sparse = vector_codec.decodePackedSparseBase64Alloc(alloc, raw_indices, raw_values) catch return error.InvalidSparseVector;
        errdefer sparse.deinit(alloc);
        alloc.free(raw_indices);
        packed_indices = null;
        alloc.free(raw_values);
        packed_values = null;
        return .{
            .indices = sparse.indices,
            .values = sparse.values,
        };
    }

    const out_indices = indices orelse return error.UnsupportedSparseFastPath;
    const out_values = values orelse return error.InvalidSparseVector;
    if (out_indices.len != out_values.len) return error.InvalidSparseVector;
    indices = null;
    values = null;
    return .{
        .indices = out_indices,
        .values = out_values,
    };
}

fn parseSparseU32ArrayFast(alloc: Allocator, scanner: *std.json.Scanner) ![]u32 {
    switch (try scanner.next()) {
        .array_begin => {},
        else => return error.InvalidSparseVector,
    }

    var out = std.ArrayListUnmanaged(u32).empty;
    errdefer out.deinit(alloc);
    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .array_end => {
                _ = try scanner.next();
                return try out.toOwnedSlice(alloc);
            },
            .number => {},
            else => return error.InvalidSparseVector,
        }
        const token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, token);
        const raw = jsonTokenSlice(token) orelse return error.InvalidSparseVector;
        try out.append(alloc, try std.fmt.parseInt(u32, raw, 10));
    }
}

fn parseSparseF32ArrayFast(alloc: Allocator, scanner: *std.json.Scanner) ![]f32 {
    switch (try scanner.next()) {
        .array_begin => {},
        else => return error.InvalidSparseVector,
    }

    var out = std.ArrayListUnmanaged(f32).empty;
    errdefer out.deinit(alloc);
    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .array_end => {
                _ = try scanner.next();
                return try out.toOwnedSlice(alloc);
            },
            .number => {},
            else => return error.InvalidSparseVector,
        }
        const token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, token);
        const raw = jsonTokenSlice(token) orelse return error.InvalidSparseVector;
        try out.append(alloc, try std.fmt.parseFloat(f32, raw));
    }
}

fn parseSparseStringDupFast(alloc: Allocator, scanner: *std.json.Scanner) ![]u8 {
    const token = try scanner.nextAlloc(alloc, .alloc_if_needed);
    defer freeJsonAllocatedToken(alloc, token);
    const raw = switch (token) {
        .string, .allocated_string => jsonTokenSlice(token) orelse return error.InvalidSparseVector,
        else => return error.InvalidSparseVector,
    };
    return try alloc.dupe(u8, raw);
}

pub fn extractWrite(alloc: Allocator, key: []const u8, data: []const u8) !ExtractedWrite {
    if (canUseOpaqueJsonFastPath(data)) {
        try validateJsonDocumentNoAlloc(alloc, data);
        return .{
            .cleaned_value = try alloc.dupe(u8, data),
            .graph_writes = &.{},
            .mentioned_graph_indexes = &.{},
            .dense_embeddings = &.{},
            .sparse_embeddings = &.{},
        };
    }

    if (try extractWriteFastDenseEmbeddingsOnly(alloc, key, data)) |extracted| {
        return extracted;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, data, .{});
    defer parsed.deinit();
    const root = parsed.value;

    if (root != .object) {
        return .{
            .cleaned_value = try alloc.dupe(u8, data),
            .graph_writes = &.{},
            .mentioned_graph_indexes = &.{},
            .dense_embeddings = &.{},
            .sparse_embeddings = &.{},
        };
    }
    if (root.object.contains("_summaries")) return error.UnsupportedReservedField;

    var graph_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer {
        for (graph_writes.items) |graph_write| {
            alloc.free(@constCast(graph_write.index_name));
            alloc.free(@constCast(graph_write.source));
            alloc.free(@constCast(graph_write.target));
            alloc.free(@constCast(graph_write.edge_type));
            if (graph_write.metadata_json.len > 0) alloc.free(@constCast(graph_write.metadata_json));
        }
        graph_writes.deinit(alloc);
    }
    var dense_embeddings = std.ArrayListUnmanaged(DenseEmbeddingWrite).empty;
    errdefer {
        for (dense_embeddings.items) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            alloc.free(embedding.vector);
        }
        dense_embeddings.deinit(alloc);
    }
    var sparse_embeddings = std.ArrayListUnmanaged(SparseEmbeddingWrite).empty;
    errdefer {
        for (sparse_embeddings.items) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            alloc.free(embedding.indices);
            alloc.free(embedding.values);
        }
        sparse_embeddings.deinit(alloc);
    }
    var mentioned_indexes = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (mentioned_indexes.items) |index_name| alloc.free(index_name);
        mentioned_indexes.deinit(alloc);
    }

    if (root.object.get("_edges")) |edges_field| {
        if (edges_field != .object) return error.InvalidGraphEdges;

        var index_it = edges_field.object.iterator();
        while (index_it.next()) |index_entry| {
            const index_name = index_entry.key_ptr.*;
            try appendUniqueString(alloc, &mentioned_indexes, index_name);

            if (index_entry.value_ptr.* != .object) return error.InvalidGraphEdges;
            var edge_type_it = index_entry.value_ptr.object.iterator();
            while (edge_type_it.next()) |edge_type_entry| {
                const edge_type = edge_type_entry.key_ptr.*;
                const edges_value = edge_type_entry.value_ptr.*;
                if (edges_value != .array) return error.InvalidGraphEdges;

                for (edges_value.array.items) |edge_item| {
                    if (edge_item != .object) return error.InvalidGraphEdges;

                    const target_value = edge_item.object.get("target") orelse return error.InvalidGraphEdges;
                    if (target_value != .string) return error.InvalidGraphEdges;

                    var metadata_json: []const u8 = "";
                    if (edge_item.object.get("metadata")) |metadata_value| {
                        metadata_json = try std.json.Stringify.valueAlloc(alloc, metadata_value, .{});
                    }
                    errdefer if (metadata_json.len > 0) alloc.free(@constCast(metadata_json));

                    try graph_writes.append(alloc, .{
                        .index_name = try alloc.dupe(u8, index_name),
                        .source = try alloc.dupe(u8, key),
                        .target = try alloc.dupe(u8, target_value.string),
                        .edge_type = try alloc.dupe(u8, edge_type),
                        .weight = if (edge_item.object.get("weight")) |weight_value|
                            try jsonNumberToF64(weight_value)
                        else
                            1.0,
                        .created_at = 0,
                        .updated_at = 0,
                        .metadata_json = metadata_json,
                    });
                }
            }
        }
    }

    if (root.object.get("_embeddings")) |embeddings_field| {
        if (embeddings_field != .object) return error.InvalidEmbeddingField;

        var emb_it = embeddings_field.object.iterator();
        while (emb_it.next()) |emb_entry| {
            const index_name = emb_entry.key_ptr.*;
            const emb_value = emb_entry.value_ptr.*;
            switch (emb_value) {
                .array, .string => {
                    const vector = try parseDenseEmbeddingValue(alloc, emb_value);
                    errdefer alloc.free(vector);
                    try dense_embeddings.append(alloc, .{
                        .index_name = try alloc.dupe(u8, index_name),
                        .doc_key = try alloc.dupe(u8, key),
                        .vector = vector,
                    });
                },
                .object => {
                    const sparse_vec = try parseSparseValue(alloc, emb_value);
                    errdefer {
                        alloc.free(sparse_vec.indices);
                        alloc.free(sparse_vec.values);
                    }
                    try sparse_embeddings.append(alloc, .{
                        .index_name = try alloc.dupe(u8, index_name),
                        .doc_key = try alloc.dupe(u8, key),
                        .indices = sparse_vec.indices,
                        .values = sparse_vec.values,
                    });
                },
                else => return error.InvalidEmbeddingField,
            }
        }
    }

    const has_special_fields = root.object.contains("_edges") or root.object.contains("_embeddings");
    const has_non_special_fields = hasNonSpecialFields(root);
    const cleaned_value = if (has_special_fields) blk: {
        if (!has_non_special_fields) break :blk null;
        var cleaned = try cloneWithoutSpecialFields(alloc, root);
        defer freeJsonValue(alloc, &cleaned);
        break :blk try std.json.Stringify.valueAlloc(alloc, cleaned, .{});
    } else try alloc.dupe(u8, data);

    return .{
        .cleaned_value = cleaned_value,
        .graph_writes = try graph_writes.toOwnedSlice(alloc),
        .mentioned_graph_indexes = try mentioned_indexes.toOwnedSlice(alloc),
        .dense_embeddings = try dense_embeddings.toOwnedSlice(alloc),
        .sparse_embeddings = try sparse_embeddings.toOwnedSlice(alloc),
    };
}

fn extractWriteFastDenseEmbeddingsOnly(alloc: Allocator, key: []const u8, data: []const u8) !?ExtractedWrite {
    var scanner = std.json.Scanner.initCompleteInput(alloc, data);
    defer scanner.deinit();

    switch (try scanner.next()) {
        .object_begin => {},
        else => return null,
    }

    var dense_embeddings = std.ArrayListUnmanaged(DenseEmbeddingWrite).empty;
    var dense_owned = false;
    defer if (!dense_owned) {
        for (dense_embeddings.items) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            alloc.free(embedding.vector);
        }
        dense_embeddings.deinit(alloc);
    };

    var cleaned_writer: std.Io.Writer.Allocating = .init(alloc);
    defer cleaned_writer.deinit();
    var json_writer: std.json.Stringify = .{
        .writer = &cleaned_writer.writer,
        .options = .{},
    };
    try json_writer.beginObject();

    var has_non_special_fields = false;
    var saw_embeddings = false;
    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .object_end => {
                _ = try scanner.next();
                break;
            },
            .string => {},
            else => return null,
        }

        const key_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, key_token);
        const field_name = jsonTokenSlice(key_token) orelse return error.InvalidEmbeddingField;

        if (std.mem.eql(u8, field_name, "_embeddings")) {
            if (!(try extractFastDenseEmbeddingsField(alloc, key, &scanner, &dense_embeddings))) return null;
            saw_embeddings = true;
            continue;
        }
        if (std.mem.eql(u8, field_name, "_summaries")) return error.UnsupportedReservedField;
        if (std.mem.eql(u8, field_name, "_edges")) return null;
        if (!(try appendFastScalarField(alloc, &scanner, field_name, &json_writer))) return null;
        has_non_special_fields = true;
    }
    try json_writer.endObject();

    if (try scanner.next() != .end_of_document) return error.SyntaxError;
    if (!saw_embeddings) return null;

    dense_owned = true;
    return .{
        .cleaned_value = if (has_non_special_fields) try alloc.dupe(u8, cleaned_writer.writer.buffered()) else null,
        .graph_writes = &.{},
        .mentioned_graph_indexes = &.{},
        .dense_embeddings = try dense_embeddings.toOwnedSlice(alloc),
        .sparse_embeddings = &.{},
    };
}

fn appendFastScalarField(
    alloc: Allocator,
    scanner: *std.json.Scanner,
    field_name: []const u8,
    json_writer: *std.json.Stringify,
) !bool {
    switch (try scanner.peekNextTokenType()) {
        .string => {
            const value_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
            defer freeJsonAllocatedToken(alloc, value_token);
            const value = jsonTokenSlice(value_token) orelse return error.InvalidEmbeddingField;
            try json_writer.objectField(field_name);
            try json_writer.write(value);
            return true;
        },
        .number => {
            const value_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
            defer freeJsonAllocatedToken(alloc, value_token);
            const value = jsonTokenSlice(value_token) orelse return error.InvalidEmbeddingField;
            try json_writer.objectField(field_name);
            try json_writer.beginWriteRaw();
            try json_writer.writer.writeAll(value);
            json_writer.endWriteRaw();
            return true;
        },
        .true => {
            _ = try scanner.next();
            try json_writer.objectField(field_name);
            try json_writer.write(true);
            return true;
        },
        .false => {
            _ = try scanner.next();
            try json_writer.objectField(field_name);
            try json_writer.write(false);
            return true;
        },
        .null => {
            _ = try scanner.next();
            try json_writer.objectField(field_name);
            try json_writer.write(null);
            return true;
        },
        else => return false,
    }
}

fn extractFastDenseEmbeddingsField(
    alloc: Allocator,
    key: []const u8,
    scanner: *std.json.Scanner,
    dense_embeddings: *std.ArrayListUnmanaged(DenseEmbeddingWrite),
) !bool {
    switch (try scanner.next()) {
        .object_begin => {},
        else => return error.InvalidEmbeddingField,
    }

    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .object_end => {
                _ = try scanner.next();
                return true;
            },
            .string => {},
            else => return false,
        }

        const key_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, key_token);
        const index_name = jsonTokenSlice(key_token) orelse return error.InvalidEmbeddingField;

        const vector = switch (try scanner.peekNextTokenType()) {
            .string => try parseFastDenseEmbeddingString(alloc, scanner),
            .array_begin => try parseFastDenseEmbeddingArray(alloc, scanner),
            else => return false,
        };
        errdefer alloc.free(vector);

        try dense_embeddings.append(alloc, .{
            .index_name = try alloc.dupe(u8, index_name),
            .doc_key = try alloc.dupe(u8, key),
            .vector = vector,
        });
    }
}

fn parseFastDenseEmbeddingString(alloc: Allocator, scanner: *std.json.Scanner) ![]f32 {
    const value_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
    defer freeJsonAllocatedToken(alloc, value_token);
    const value = jsonTokenSlice(value_token) orelse return error.InvalidEmbeddingField;
    return vector_codec.decodePackedF32Base64Alloc(alloc, value) catch return error.InvalidEmbeddingField;
}

fn parseFastDenseEmbeddingArray(alloc: Allocator, scanner: *std.json.Scanner) ![]f32 {
    switch (try scanner.next()) {
        .array_begin => {},
        else => return error.InvalidEmbeddingField,
    }

    var values = std.ArrayListUnmanaged(f32).empty;
    errdefer values.deinit(alloc);

    while (true) {
        switch (try scanner.peekNextTokenType()) {
            .array_end => {
                _ = try scanner.next();
                return try values.toOwnedSlice(alloc);
            },
            .number => {},
            else => return error.InvalidVectorValue,
        }

        const value_token = try scanner.nextAlloc(alloc, .alloc_if_needed);
        defer freeJsonAllocatedToken(alloc, value_token);
        const value = jsonTokenSlice(value_token) orelse return error.InvalidVectorValue;
        try values.append(alloc, try std.fmt.parseFloat(f32, value));
    }
}

fn canUseOpaqueJsonFastPath(data: []const u8) bool {
    // Escaped field names can spell a special field without containing its
    // literal bytes, so keep those on the full parser path.
    if (std.mem.indexOfScalar(u8, data, '\\') != null) return false;
    if (std.mem.indexOf(u8, data, "_edges") != null) return false;
    if (std.mem.indexOf(u8, data, "_embeddings") != null) return false;
    if (std.mem.indexOf(u8, data, "_summaries") != null) return false;
    return true;
}

fn validateJsonDocumentNoAlloc(alloc: Allocator, data: []const u8) !void {
    var scanner = std.json.Scanner.initCompleteInput(alloc, data);
    defer scanner.deinit();
    try scanner.skipValue();
    if (try scanner.next() != .end_of_document) return error.SyntaxError;
}

fn extractTextFields(
    alloc: Allocator,
    data: []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !ExtractedTextFields {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, data, .{});
    defer parsed.deinit();
    return try extractTextFieldsFromValue(alloc, parsed.value, text_analysis, schema, observed_field_analyzers);
}

fn extractTextFieldsFromValue(
    alloc: Allocator,
    root: std.json.Value,
    text_analysis: introducer_mod.TextAnalysisConfig,
    schema: ?runtime_schema.TableSchema,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !ExtractedTextFields {
    if (root != .object) return .{ .fields = &.{} };

    if (schema) |runtime| {
        // Relational tables drive typed columns from the declared catalog rather
        // than value detection (authoritative types). NOT NULL is enforced
        // upstream by JSON-schema `required` validation, so missing nullable
        // columns are simply skipped here. keyword/text columns flow through the
        // text path below; json columns are indexed as subtrees.
        const relational_typed_fields: ?[]const introducer_mod.TypedFieldValue =
            if (runtime.storage_mode == .relational)
                try buildRelationalTypedFields(alloc, root, runtime.relational_columns)
            else
                null;
        errdefer if (relational_typed_fields) |relational_fields| alloc.free(relational_fields);

        if (!runtimeHasSchemaDrivenText(runtime)) {
            return .{
                .fields = try extractStringFieldsNoSchema(alloc, root.object),
                .typed_fields = relational_typed_fields,
            };
        }

        var fields = std.ArrayListUnmanaged(introducer_mod.TextField).empty;
        defer fields.deinit(alloc);

        const document_schema = if (runtime.full_text_documents.len > 0)
            resolveFullTextDocument(runtime, root.object)
        else
            null;

        if (document_schema) |resolved| {
            try appendSchemaTextFields(alloc, &fields, root, resolved, text_analysis);
        }
        try appendDynamicSchemaTextFields(alloc, &fields, root, runtime, document_schema, text_analysis, observed_field_analyzers);
        return .{
            .fields = if (fields.items.len > 0) try alloc.dupe(introducer_mod.TextField, fields.items) else &.{},
            .infer_type_dynamic_paths = if (document_schema) |resolved| resolved.infer_type_dynamic_paths else &.{},
            .typed_fields = relational_typed_fields,
        };
    }

    return try extractSchemaLessTextAndTypedFields(alloc, root.object, text_analysis);
}

/// Build the introducer's typed-field input for a relational document from the
/// declared column catalog. Every present column is emitted as a typed-doc-value
/// so the segment is fully reconstructable (Phase 5, authoritative columns):
/// numeric -> f64, datetime -> u64 epoch ns, boolean, geopoint, and
/// string/blob/geoshape/json -> bytes_val. Numeric/datetime/boolean/geopoint
/// sections double as predicate-scan columns; string columns additionally keep
/// their analyzed inverted-index entries for term queries. NOT NULL is enforced
/// upstream; missing nullable columns are skipped.
fn buildRelationalTypedFields(
    alloc: Allocator,
    root: std.json.Value,
    columns: []const runtime_schema.RelationalColumn,
) ![]const introducer_mod.TypedFieldValue {
    var fields = std.ArrayListUnmanaged(introducer_mod.TypedFieldValue).empty;
    errdefer fields.deinit(alloc);

    for (columns) |column| {
        const value_type = relationalStorageValueType(column.field_type);
        const found = valueAtJsonPath(root, column.path) orelse continue;
        if (found == .null) continue;
        const value = try coerceRelationalStorageValue(alloc, value_type, found) orelse continue;
        try fields.append(alloc, .{
            .field_name = column.name,
            .value_type = value_type,
            .value = value,
        });
    }

    return try fields.toOwnedSlice(alloc);
}

/// typed_doc_values type used to persist a relational column for reconstruction.
/// numeric/datetime/boolean/geopoint keep their scan-friendly encodings; every
/// other declared type (keyword/text/link/html/search_as_you_type/blob/geoshape
/// /json) is persisted as bytes_val.
fn relationalStorageValueType(field_type: runtime_schema.AntflyType) typed_dv.ValueType {
    return switch (field_type) {
        .numeric => .f64_val,
        .datetime => .u64_val,
        .boolean => .bool_val,
        .geopoint => .geo_point,
        else => .bytes_val,
    };
}

fn coerceRelationalStorageValue(alloc: Allocator, value_type: typed_dv.ValueType, json_value: std.json.Value) !?typed_dv.TypedValue {
    if (value_type == .bytes_val) {
        // Strings are stored verbatim; structured (object/array) and any other
        // value is stored as its canonical JSON text. The writer dupes bytes,
        // so a stringified buffer can be freed by the caller's arena.
        switch (json_value) {
            .string => |text| return .{ .bytes_val = text },
            else => {
                const encoded = try std.json.Stringify.valueAlloc(alloc, json_value, .{});
                return .{ .bytes_val = encoded };
            },
        }
    }
    return coerceRelationalTypedValue(value_type, json_value);
}

fn relationalTypedValueType(field_type: runtime_schema.AntflyType) ?typed_dv.ValueType {
    return switch (field_type) {
        .numeric => .f64_val,
        .datetime => .u64_val,
        .boolean => .bool_val,
        .geopoint => .geo_point,
        else => null,
    };
}

fn coerceRelationalTypedValue(value_type: typed_dv.ValueType, json_value: std.json.Value) ?typed_dv.TypedValue {
    switch (value_type) {
        .f64_val => switch (json_value) {
            .float => |number| return .{ .f64_val = number },
            .integer => |number| return .{ .f64_val = @floatFromInt(number) },
            else => return null,
        },
        .u64_val => {
            const number: i64 = switch (json_value) {
                .integer => |value| value,
                .string => |text| std.fmt.parseInt(i64, text, 10) catch return null,
                else => return null,
            };
            return .{ .u64_val = @bitCast(number) };
        },
        .bool_val => switch (json_value) {
            .bool => |flag| return .{ .bool_val = flag },
            else => return null,
        },
        .geo_point => {
            if (json_value != .object) return null;
            const lat = jsonNumberValue(json_value.object.get("lat") orelse return null) orelse return null;
            const lon = jsonNumberValue(json_value.object.get("lon") orelse return null) orelse return null;
            return .{ .geo_point = .{ .lat = lat, .lon = lon } };
        },
        else => return null,
    }
}

fn jsonNumberValue(json_value: std.json.Value) ?f64 {
    switch (json_value) {
        .float => |number| return number,
        .integer => |number| return @floatFromInt(number),
        else => return null,
    }
}

fn valueAtJsonPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var it = std.mem.splitScalar(u8, path, '.');
    while (it.next()) |segment| {
        switch (current) {
            .object => |object| current = object.get(segment) orelse return null,
            else => return null,
        }
    }
    return current;
}

/// Reconstruct a relational document's JSON from the persisted column sections
/// of a segment (Phase 5, authoritative columns -- the read counterpart of the
/// write-path column persistence). For each declared column it reads the
/// `typed_doc_values` section by name and pulls the value at `doc_ordinal`,
/// emitting JSON keyed by the column path. Columns with no section (absent
/// nullable values) are omitted. `json` and string columns are stored as
/// `bytes_val`; `json` bytes are already valid JSON and embedded verbatim,
/// strings are JSON-escaped. Returns the JSON document; caller owns the bytes.
///
/// This does not yet replace the stored_data blob on the read path; it proves
/// the round trip (write columns -> persist segment -> reconstruct) works on a
/// real segment, which is the prerequisite for dropping the blob.
pub fn reconstructRelationalDocumentFromSegmentAlloc(
    alloc: Allocator,
    reader: anytype,
    columns: []const runtime_schema.RelationalColumn,
    doc_ordinal: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    var emitted: usize = 0;
    for (columns) |column| {
        const section = reader.getSection(column.name, .typed_doc_values) orelse continue;
        const dv = typed_dv.TypedDocValuesReader.init(alloc, section) catch continue;
        if (try appendReconstructedColumn(alloc, &out, column, dv, doc_ordinal, emitted > 0)) {
            emitted += 1;
        }
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

/// Append one column's `path: value` to `out`. Returns true if a value was
/// emitted (the doc had a value in this column's section), false otherwise.
///
/// Reads the typed value from the segment column, then formats it through the
/// shared `relational_row_codec.appendCellValue` — the *same* formatter the KV
/// store read path uses — so a relational document reconstructs byte-for-byte
/// identically whether served from a segment (full-text reads) or from the KV
/// store (point lookups / transforms / vector reads).
fn appendReconstructedColumn(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    column: runtime_schema.RelationalColumn,
    dv: typed_dv.TypedDocValuesReader,
    doc_ordinal: u32,
    needs_comma: bool,
) !bool {
    const value_type = relationalStorageValueType(column.field_type);
    const is_json = column.field_type == .json;

    const value: typed_dv.TypedValue = switch (value_type) {
        .f64_val => .{ .f64_val = (try dv.getF64(doc_ordinal)) orelse return false },
        .u64_val => .{ .u64_val = (try dv.getU64(doc_ordinal)) orelse return false },
        .bool_val => .{ .bool_val = (try dv.getBool(doc_ordinal)) orelse return false },
        .geo_point => .{ .geo_point = (try dv.getGeoPoint(doc_ordinal)) orelse return false },
        .bytes_val => .{ .bytes_val = (try dv.getBytes(doc_ordinal)) orelse return false },
    };
    // getBytes returns an owned dupe; free it after formatting.
    defer if (value_type == .bytes_val) alloc.free(@constCast(value.bytes_val));

    try relational_row_codec.appendCellValue(alloc, out, column.path, value_type, is_json, value, needs_comma);
    return true;
}

fn runtimeHasSchemaDrivenText(schema: runtime_schema.TableSchema) bool {
    if (schema.dynamic_templates.len > 0) return true;
    for (schema.full_text_documents) |doc| {
        if (doc.fields.len > 0) return true;
        if (doc.dynamic_rules.len > 0) return true;
        if (doc.open_dynamic_paths.len > 0) return true;
        if (doc.infer_type_dynamic_paths.len > 0) return true;
    }
    return false;
}

fn appendSchemaTextFields(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    root: std.json.Value,
    document_schema: runtime_schema.FullTextDocument,
    text_analysis: introducer_mod.TextAnalysisConfig,
) !void {
    for (document_schema.fields) |field| {
        var values = std.ArrayListUnmanaged([]const u8).empty;
        defer values.deinit(alloc);
        try collectFieldValues(alloc, &values, root, field.path);
        if (values.items.len == 0) continue;

        const analyzer = introducer_mod.resolveAnalyzerName(field.analyzer, text_analysis);
        for (values.items) |text| {
            try fields.append(alloc, .{
                .field_name = field.emitted_name,
                .text = text,
                .analyzer = analyzer,
            });
            if (field.include_in_all) {
                try fields.append(alloc, .{
                    .field_name = "_all",
                    .text = text,
                    .analyzer = analyzer,
                });
            }
        }
    }
}

fn appendDynamicSchemaTextFields(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    root: std.json.Value,
    schema: runtime_schema.TableSchema,
    document_schema: ?runtime_schema.FullTextDocument,
    text_analysis: introducer_mod.TextAnalysisConfig,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !void {
    var explicit_paths = std.ArrayListUnmanaged([]const u8).empty;
    defer explicit_paths.deinit(alloc);

    if (document_schema) |resolved| {
        for (resolved.fields) |field| try explicit_paths.append(alloc, field.path);
    }

    try collectDynamicSchemaTextFields(alloc, fields, root, "", schema, document_schema, explicit_paths.items, text_analysis, observed_field_analyzers);
}

fn collectDynamicSchemaTextFields(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    value: std.json.Value,
    path: []const u8,
    schema: runtime_schema.TableSchema,
    document_schema: ?runtime_schema.FullTextDocument,
    explicit_paths: []const []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !void {
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
                try collectDynamicSchemaTextFields(
                    alloc,
                    fields,
                    entry.value_ptr.*,
                    child_path,
                    schema,
                    document_schema,
                    explicit_paths,
                    text_analysis,
                    observed_field_analyzers,
                );
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectDynamicSchemaTextFields(alloc, fields, item, path, schema, document_schema, explicit_paths, text_analysis, observed_field_analyzers);
            }
        },
        .string => |text| {
            if (path.len == 0) return;
            if (containsStringSlice(explicit_paths, path)) return;
            if (document_schema) |resolved| {
                if (resolveDynamicRule(resolved, path)) |rule| {
                    try appendDynamicRuleTextField(alloc, fields, path, text, rule, text_analysis);
                    return;
                }
            }
            if (resolveDynamicTextMapping(schema, path, text)) |mapping| {
                try appendMappedTextField(alloc, fields, path, text, mapping, text_analysis);
                if (observed_field_analyzers) |collector| {
                    try appendObservedFieldAnalyzer(alloc, collector, path, mapping.analyzer);
                }
                return;
            }
            if (document_schema) |resolved| {
                if (pathFallsUnderInferTypeDynamicPath(resolved, path)) {
                    try appendDynamicSchemaLessStringTextFields(alloc, fields, path, text, text_analysis, observed_field_analyzers);
                    return;
                }
                if (pathFallsUnderOpenDynamicPath(resolved, path)) {
                    try appendDynamicSchemaLessStringTextFields(alloc, fields, path, text, text_analysis, observed_field_analyzers);
                }
            }
        },
        else => {},
    }
}

fn appendObservedFieldAnalyzer(
    alloc: Allocator,
    observed: *std.ArrayListUnmanaged(ObservedFieldAnalyzer),
    field_name: []const u8,
    analyzer_name: []const u8,
) !void {
    for (observed.items) |item| {
        if (std.mem.eql(u8, item.field_name, field_name) and std.mem.eql(u8, item.analyzer_name, analyzer_name)) return;
    }
    try observed.append(alloc, .{
        .field_name = try alloc.dupe(u8, field_name),
        .analyzer_name = try alloc.dupe(u8, analyzer_name),
    });
}

fn resolveDynamicRule(document_schema: runtime_schema.FullTextDocument, path: []const u8) ?runtime_schema.FullTextDynamicRule {
    for (document_schema.dynamic_rules) |rule| {
        if (pathMatchesDynamicRule(path, rule)) return rule;
    }
    return null;
}

fn resolveDynamicTextMapping(schema: runtime_schema.TableSchema, path: []const u8, text: []const u8) ?runtime_schema.FieldMapping {
    const value = std.json.Value{ .string = text };
    if (runtime_schema.resolveFieldTypeForValue(schema, path, value)) |mapping| {
        if (isTextFieldType(mapping.field_type)) return mapping;
    }

    const field_name = fieldNameFromPath(path);
    if (runtime_schema.resolveFieldTypeForValue(schema, field_name, value)) |mapping| {
        if (isTextFieldType(mapping.field_type)) return mapping;
    }
    return null;
}

fn pathFallsUnderOpenDynamicPath(document_schema: runtime_schema.FullTextDocument, path: []const u8) bool {
    return pathFallsUnderAnyDynamicPath(document_schema.open_dynamic_paths, path);
}

fn pathFallsUnderInferTypeDynamicPath(document_schema: runtime_schema.FullTextDocument, path: []const u8) bool {
    return pathFallsUnderAnyDynamicPath(document_schema.infer_type_dynamic_paths, path);
}

fn pathFallsUnderAnyDynamicPath(paths: []const []const u8, path: []const u8) bool {
    for (paths) |open_path| {
        if (open_path.len == 0) return true;
        if (!std.mem.startsWith(u8, path, open_path)) continue;
        if (path.len == open_path.len) return true;
        if (path.len > open_path.len and path[open_path.len] == '.') return true;
    }
    return false;
}

fn appendMappedTextField(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    path: []const u8,
    text: []const u8,
    mapping: runtime_schema.FieldMapping,
    text_analysis: introducer_mod.TextAnalysisConfig,
) !void {
    if (!mapping.do_index) return;

    switch (mapping.field_type) {
        .text, .html, .keyword, .link, .search_as_you_type => try appendNamedTextField(alloc, fields, path, text, mapping.analyzer, mapping.include_in_all, text_analysis),
        else => {},
    }
}

fn appendDynamicRuleTextField(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    path: []const u8,
    text: []const u8,
    rule: runtime_schema.FullTextDynamicRule,
    text_analysis: introducer_mod.TextAnalysisConfig,
) !void {
    for (rule.variants) |variant| {
        const field_name = if (variant.suffix.len == 0)
            try alloc.dupe(u8, path)
        else
            try std.fmt.allocPrint(alloc, "{s}{s}", .{ path, variant.suffix });
        defer alloc.free(field_name);
        try appendNamedTextField(
            alloc,
            fields,
            field_name,
            text,
            variant.analyzer,
            variant.include_in_all,
            text_analysis,
        );
    }
}

fn appendNamedTextField(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    field_name: []const u8,
    text: []const u8,
    analyzer_name: []const u8,
    include_in_all: bool,
    text_analysis: introducer_mod.TextAnalysisConfig,
) !void {
    const analyzer = introducer_mod.resolveAnalyzerName(analyzer_name, text_analysis);
    try fields.append(alloc, .{
        .field_name = try alloc.dupe(u8, field_name),
        .text = text,
        .analyzer = analyzer,
    });
    if (include_in_all) {
        try fields.append(alloc, .{
            .field_name = "_all",
            .text = text,
            .analyzer = analyzer,
        });
    }
}

fn appendDynamicSchemaLessStringTextFields(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    path: []const u8,
    text: []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    observed_field_analyzers: ?*std.ArrayListUnmanaged(ObservedFieldAnalyzer),
) !void {
    try appendNamedTextField(alloc, fields, path, text, "standard", false, text_analysis);
    if (observed_field_analyzers) |collector| {
        try appendObservedFieldAnalyzer(alloc, collector, path, "standard");
    }
    if (text.len > schema_less_exact_max_bytes or std.mem.endsWith(u8, path, schema_less_exact_field_suffix)) return;

    const exact_field = try schemaLessExactFieldNameAlloc(alloc, path);
    defer alloc.free(exact_field);
    try appendNamedTextField(alloc, fields, exact_field, text, "keyword", false, text_analysis);
    if (observed_field_analyzers) |collector| {
        try appendObservedFieldAnalyzer(alloc, collector, exact_field, "keyword");
    }
}

fn extractStringFieldsNoSchema(alloc: Allocator, object: std.json.ObjectMap) ![]introducer_mod.TextField {
    var fields = std.ArrayListUnmanaged(introducer_mod.TextField).empty;
    defer fields.deinit(alloc);
    try collectStringFieldsNoSchema(alloc, &fields, .{ .object = object }, "");
    return try alloc.dupe(introducer_mod.TextField, fields.items);
}

fn extractSchemaLessTextAndTypedFields(
    alloc: Allocator,
    object: std.json.ObjectMap,
    text_analysis: introducer_mod.TextAnalysisConfig,
) !ExtractedTextFields {
    var text_fields = std.ArrayListUnmanaged(introducer_mod.TextField).empty;
    defer text_fields.deinit(alloc);
    var typed_fields = std.ArrayListUnmanaged(introducer_mod.TypedFieldValue).empty;
    defer typed_fields.deinit(alloc);
    var path = std.ArrayListUnmanaged(u8).empty;
    defer path.deinit(alloc);

    try collectSchemaLessTextAndTypedFieldsRecursive(
        alloc,
        .{ .object = object },
        &path,
        &text_fields,
        &typed_fields,
        text_analysis,
    );

    return .{
        .fields = if (text_fields.items.len > 0) try alloc.dupe(introducer_mod.TextField, text_fields.items) else &.{},
        .typed_fields = if (typed_fields.items.len > 0) try alloc.dupe(introducer_mod.TypedFieldValue, typed_fields.items) else &.{},
    };
}

fn collectSchemaLessTextAndTypedFieldsRecursive(
    alloc: Allocator,
    value: std.json.Value,
    path: *std.ArrayListUnmanaged(u8),
    text_fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    typed_fields: *std.ArrayListUnmanaged(introducer_mod.TypedFieldValue),
    text_analysis: introducer_mod.TextAnalysisConfig,
) !void {
    if (path.items.len > 0) {
        if (try introducer_mod.detectTypedFieldProjectionValue(alloc, path.items, value, text_analysis)) |typed_field| {
            try typed_fields.append(alloc, typed_field);
            if (value == .object and typed_field.value_type == .geo_point) return;
        }
    }

    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
                const old_len = try pushProjectionPath(alloc, path, entry.key_ptr.*);
                defer path.shrinkRetainingCapacity(old_len);
                try collectSchemaLessTextAndTypedFieldsRecursive(
                    alloc,
                    entry.value_ptr.*,
                    path,
                    text_fields,
                    typed_fields,
                    text_analysis,
                );
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectSchemaLessTextAndTypedFieldsRecursive(
                    alloc,
                    item,
                    path,
                    text_fields,
                    typed_fields,
                    text_analysis,
                );
            }
        },
        .string => |text| {
            if (path.items.len > 0) try appendSchemaLessStringTextFields(alloc, text_fields, path.items, text);
        },
        else => {},
    }
}

fn canUseSchemaLessRawTextFastPath(data: []const u8, opts: TextProjectionOptions) bool {
    if (opts.strip_numeric_array_heuristic) return false;
    if (rawJsonMayContainTypedField(data)) return false;
    // Raw strings are borrowed from the document. Escapes require JSON string
    // decoding, so keep those on the full parser path.
    if (std.mem.indexOfScalar(u8, data, '\\') != null) return false;
    if (std.mem.indexOf(u8, data, "\"_edges\"") != null) return false;
    if (std.mem.indexOf(u8, data, "\"_embeddings\"") != null) return false;
    for (opts.vector_field_paths) |path| {
        const first = firstProjectionPathSegment(path);
        if (first.len == 0) continue;
        if (rawJsonObjectMayContainField(data, first)) return false;
    }
    return true;
}

fn rawJsonMayContainTypedField(data: []const u8) bool {
    var pos: usize = 0;
    while (pos < data.len) : (pos += 1) {
        if (data[pos] != ':') continue;
        pos += 1;
        while (pos < data.len and std.ascii.isWhitespace(data[pos])) : (pos += 1) {}
        if (pos >= data.len) return false;
        switch (data[pos]) {
            '-', '0'...'9', 't', 'f', 'n' => return true,
            '[' => {
                var scan = pos + 1;
                while (scan < data.len and std.ascii.isWhitespace(data[scan])) : (scan += 1) {}
                if (scan < data.len and data[scan] != '"' and data[scan] != ']') return true;
            },
            else => {},
        }
    }
    return false;
}

fn firstProjectionPathSegment(path: []const u8) []const u8 {
    const dot = std.mem.indexOfScalar(u8, path, '.') orelse return path;
    return path[0..dot];
}

fn rawJsonObjectMayContainField(data: []const u8, field: []const u8) bool {
    var pos: usize = 0;
    while (std.mem.indexOfScalarPos(u8, data, pos, '"')) |start| {
        pos = start + 1;
        const end = std.mem.indexOfScalarPos(u8, data, pos, '"') orelse return false;
        if (std.mem.eql(u8, data[pos..end], field)) return true;
        pos = end + 1;
    }
    return false;
}

fn extractStringFieldsNoSchemaRaw(
    alloc: Allocator,
    data: []const u8,
    opts: TextProjectionOptions,
) ![]introducer_mod.TextField {
    var fields = std.ArrayListUnmanaged(introducer_mod.TextField).empty;
    defer fields.deinit(alloc);
    var path = std.ArrayListUnmanaged(u8).empty;
    defer path.deinit(alloc);

    var pos: usize = 0;
    skipJsonWhitespace(data, &pos);
    if (pos >= data.len or data[pos] != '{') return error.SyntaxError;
    pos += 1;
    try collectStringFieldsNoSchemaRawObject(alloc, &fields, data, &pos, &path, opts);
    skipJsonWhitespace(data, &pos);
    if (pos != data.len) return error.SyntaxError;
    return try alloc.dupe(introducer_mod.TextField, fields.items);
}

fn collectStringFieldsNoSchemaRawObject(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    data: []const u8,
    pos: *usize,
    path: *std.ArrayListUnmanaged(u8),
    opts: TextProjectionOptions,
) anyerror!void {
    var first = true;
    while (true) {
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == '}') {
            pos.* += 1;
            return;
        }
        if (!first) {
            if (data[pos.*] != ',') return error.SyntaxError;
            pos.* += 1;
            skipJsonWhitespace(data, pos);
        }
        first = false;

        const key = try parseRawJsonString(data, pos);
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len or data[pos.*] != ':') return error.SyntaxError;
        pos.* += 1;

        if (key.len > 0 and key[0] == '_') {
            try skipRawJsonValue(data, pos);
            continue;
        }

        const old_len = try pushProjectionPath(alloc, path, key);
        defer path.shrinkRetainingCapacity(old_len);
        if (projectionPathMatchesAny(opts.vector_field_paths, path.items)) {
            try skipRawJsonValue(data, pos);
            continue;
        }
        try collectStringFieldsNoSchemaRawValue(alloc, fields, data, pos, path, opts);
    }
}

fn collectStringFieldsNoSchemaRawArray(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    data: []const u8,
    pos: *usize,
    path: *std.ArrayListUnmanaged(u8),
    opts: TextProjectionOptions,
) anyerror!void {
    var first = true;
    while (true) {
        skipJsonWhitespace(data, pos);
        if (pos.* >= data.len) return error.SyntaxError;
        if (data[pos.*] == ']') {
            pos.* += 1;
            return;
        }
        if (!first) {
            if (data[pos.*] != ',') return error.SyntaxError;
            pos.* += 1;
            skipJsonWhitespace(data, pos);
        }
        first = false;
        try collectStringFieldsNoSchemaRawValue(alloc, fields, data, pos, path, opts);
    }
}

fn collectStringFieldsNoSchemaRawValue(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    data: []const u8,
    pos: *usize,
    path: *std.ArrayListUnmanaged(u8),
    opts: TextProjectionOptions,
) anyerror!void {
    skipJsonWhitespace(data, pos);
    if (pos.* >= data.len) return error.SyntaxError;
    switch (data[pos.*]) {
        '"' => {
            const text = try parseRawJsonString(data, pos);
            if (path.items.len == 0) return;
            try appendSchemaLessStringTextFields(alloc, fields, path.items, text);
        },
        '{' => {
            pos.* += 1;
            try collectStringFieldsNoSchemaRawObject(alloc, fields, data, pos, path, opts);
        },
        '[' => {
            pos.* += 1;
            try collectStringFieldsNoSchemaRawArray(alloc, fields, data, pos, path, opts);
        },
        else => try skipRawJsonValue(data, pos),
    }
}

fn collectStringFieldsNoSchema(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    value: std.json.Value,
    path: []const u8,
) !void {
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
                try collectStringFieldsNoSchema(alloc, fields, entry.value_ptr.*, child_path);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try collectStringFieldsNoSchema(alloc, fields, item, path);
            }
        },
        .string => |text| {
            if (path.len == 0) return;
            try appendSchemaLessStringTextFields(alloc, fields, path, text);
        },
        else => {},
    }
}

fn appendSchemaLessStringTextFields(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(introducer_mod.TextField),
    path: []const u8,
    text: []const u8,
) !void {
    try fields.append(alloc, .{
        .field_name = try alloc.dupe(u8, path),
        .text = text,
    });
    if (text.len > schema_less_exact_max_bytes or std.mem.endsWith(u8, path, schema_less_exact_field_suffix)) return;
    const exact_field = try schemaLessExactFieldNameAlloc(alloc, path);
    try fields.append(alloc, .{
        .field_name = exact_field,
        .text = text,
        .analyzer = &analysis_mod.keyword_analyzer,
    });
}

pub fn schemaLessExactFieldNameAlloc(alloc: Allocator, field: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ field, schema_less_exact_field_suffix });
}

fn resolveFullTextDocument(schema: runtime_schema.TableSchema, root: std.json.ObjectMap) ?runtime_schema.FullTextDocument {
    if (root.get("_type")) |type_value| {
        if (type_value == .string) {
            for (schema.full_text_documents) |document_schema| {
                if (std.mem.eql(u8, document_schema.name, type_value.string)) return document_schema;
            }
            return null;
        }
    }

    if (schema.default_type.len > 0) {
        for (schema.full_text_documents) |document_schema| {
            if (std.mem.eql(u8, document_schema.name, schema.default_type)) return document_schema;
        }
    }
    if (schema.full_text_documents.len == 1) return schema.full_text_documents[0];
    return null;
}

fn collectFieldValues(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    value: std.json.Value,
    path: []const u8,
) !void {
    if (path.len == 0) {
        switch (value) {
            .string => |text| try out.append(alloc, text),
            .array => |array| for (array.items) |item| try collectFieldValues(alloc, out, item, ""),
            else => {},
        }
        return;
    }

    switch (value) {
        .object => |object| {
            const dot = std.mem.indexOfScalar(u8, path, '.');
            const head = if (dot) |idx| path[0..idx] else path;
            const tail = if (dot) |idx| path[idx + 1 ..] else "";
            if (object.get(head)) |child| try collectFieldValues(alloc, out, child, tail);
        },
        .array => |array| {
            for (array.items) |item| try collectFieldValues(alloc, out, item, path);
        },
        else => {},
    }
}

fn isTextFieldType(field_type: runtime_schema.AntflyType) bool {
    return switch (field_type) {
        .text, .html, .keyword, .link, .search_as_you_type => true,
        else => false,
    };
}

fn containsStringSlice(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn fieldNameFromPath(path: []const u8) []const u8 {
    const last_dot = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[last_dot + 1 ..];
}

fn parentPath(path: []const u8) []const u8 {
    const last_dot = std.mem.lastIndexOfScalar(u8, path, '.') orelse return "";
    return path[0..last_dot];
}

fn pathMatchesDynamicRule(path: []const u8, rule: runtime_schema.FullTextDynamicRule) bool {
    if (rule.parent_path.len == 0) {
        const first_dot = std.mem.indexOfScalar(u8, path, '.');
        const dynamic_segment = if (first_dot) |idx| path[0..idx] else path;
        const remainder = if (first_dot) |idx| path[idx + 1 ..] else "";
        if (!segmentMatchesPattern(dynamic_segment, rule.segment_pattern)) return false;
        return std.mem.eql(u8, remainder, rule.relative_path);
    }

    if (!std.mem.startsWith(u8, path, rule.parent_path)) return false;
    if (path.len <= rule.parent_path.len or path[rule.parent_path.len] != '.') return false;

    const after_parent = path[rule.parent_path.len + 1 ..];
    const dynamic_end = std.mem.indexOfScalar(u8, after_parent, '.');
    const dynamic_segment = if (dynamic_end) |idx| after_parent[0..idx] else after_parent;
    const remainder = if (dynamic_end) |idx| after_parent[idx + 1 ..] else "";
    if (!segmentMatchesPattern(dynamic_segment, rule.segment_pattern)) return false;
    return std.mem.eql(u8, remainder, rule.relative_path);
}

fn segmentMatchesPattern(segment: []const u8, pattern: ?[]const u8) bool {
    if (segment.len == 0) return false;
    if (pattern) |compiled| {
        return regex_mod.matches(std.heap.page_allocator, compiled, segment) catch false;
    }
    return true;
}

fn jsonNumberToF32(value: std.json.Value) !f32 {
    return switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| @floatCast(f),
        .number_string => |s| try std.fmt.parseFloat(f32, s),
        else => error.InvalidVectorValue,
    };
}

fn jsonTokenSlice(token: std.json.Token) ?[]const u8 {
    return switch (token) {
        .string => |s| s,
        .allocated_string => |s| s,
        .number => |s| s,
        .allocated_number => |s| s,
        else => null,
    };
}

fn freeJsonAllocatedToken(alloc: Allocator, token: std.json.Token) void {
    switch (token) {
        .allocated_string => |s| alloc.free(s),
        .allocated_number => |s| alloc.free(s),
        else => {},
    }
}

fn jsonNumberToU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |i| std.math.cast(u32, i) orelse return error.InvalidVectorValue,
        .number_string => |s| try std.fmt.parseInt(u32, s, 10),
        else => error.InvalidVectorValue,
    };
}

fn parseDenseEmbeddingValue(alloc: Allocator, value: std.json.Value) ![]f32 {
    return switch (value) {
        .array => blk: {
            const vector = try alloc.alloc(f32, value.array.items.len);
            errdefer alloc.free(vector);
            for (value.array.items, 0..) |item, i| {
                vector[i] = try jsonNumberToF32(item);
            }
            break :blk vector;
        },
        .string => vector_codec.decodePackedF32Base64Alloc(alloc, value.string) catch return error.InvalidEmbeddingField,
        else => error.InvalidEmbeddingField,
    };
}

fn parseSparseValue(alloc: Allocator, value: std.json.Value) !SparseVectorData {
    if (value != .object) return error.InvalidEmbeddingField;
    if (value.object.get("packed_indices") != null or value.object.get("packed_values") != null) {
        const packed_indices = value.object.get("packed_indices") orelse return error.InvalidSparseVector;
        const packed_values = value.object.get("packed_values") orelse return error.InvalidSparseVector;
        if (packed_indices != .string or packed_values != .string) return error.InvalidSparseVector;

        var sparse = vector_codec.decodePackedSparseBase64Alloc(alloc, packed_indices.string, packed_values.string) catch return error.InvalidSparseVector;
        errdefer sparse.deinit(alloc);

        return .{
            .indices = sparse.indices,
            .values = sparse.values,
        };
    }
    if (value.object.get("indices") != null or value.object.get("values") != null) {
        const indices_val = value.object.get("indices") orelse return error.InvalidSparseVector;
        const values_val = value.object.get("values") orelse return error.InvalidSparseVector;
        if (indices_val != .array or values_val != .array) return error.InvalidSparseVector;
        if (indices_val.array.items.len != values_val.array.items.len) return error.InvalidSparseVector;

        const indices = try alloc.alloc(u32, indices_val.array.items.len);
        errdefer alloc.free(indices);
        const values = try alloc.alloc(f32, values_val.array.items.len);
        errdefer alloc.free(values);

        for (indices_val.array.items, 0..) |item, i| indices[i] = try jsonNumberToU32(item);
        for (values_val.array.items, 0..) |item, i| values[i] = try jsonNumberToF32(item);

        return .{
            .indices = indices,
            .values = values,
        };
    }

    const indices = try alloc.alloc(u32, value.object.count());
    errdefer alloc.free(indices);
    const values = try alloc.alloc(f32, value.object.count());
    errdefer alloc.free(values);

    var count: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        indices[count] = try std.fmt.parseInt(u32, entry.key_ptr.*, 10);
        values[count] = try jsonNumberToF32(entry.value_ptr.*);
        count += 1;
    }

    // Re-sort sparse coordinates by index for deterministic downstream behavior.
    var i: usize = 0;
    while (i < count) : (i += 1) {
        var j = i + 1;
        while (j < count) : (j += 1) {
            if (indices[j] < indices[i]) {
                std.mem.swap(u32, &indices[i], &indices[j]);
                std.mem.swap(f32, &values[i], &values[j]);
            }
        }
    }

    return .{
        .indices = indices,
        .values = values,
    };
}

fn jsonNumberToF64(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .number_string => |s| try std.fmt.parseFloat(f64, s),
        else => error.InvalidVectorValue,
    };
}

fn hasNonSpecialFields(root: std.json.Value) bool {
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (!isSpecialField(entry.key_ptr.*)) return true;
    }
    return false;
}

const FullTextStoredProjection = struct {
    stored_data: []const u8,
    typed_source: ?std.json.Value,
};

fn fullTextStoredProjection(alloc: Allocator, root: std.json.Value, original: []const u8, opts: TextProjectionOptions) !FullTextStoredProjection {
    if (!try fullTextProjectionNeedsSanitization(alloc, root, opts)) {
        return .{
            .stored_data = original,
            .typed_source = root,
        };
    }
    var path = std.ArrayListUnmanaged(u8).empty;
    defer path.deinit(alloc);
    const projected = (try cloneFullTextProjectionValue(alloc, root, &path, opts)) orelse std.json.Value{ .object = std.json.ObjectMap.empty };
    return .{
        .stored_data = try std.json.Stringify.valueAlloc(alloc, projected, .{}),
        .typed_source = projected,
    };
}

fn fullTextProjectionNeedsSanitization(alloc: Allocator, value: std.json.Value, opts: TextProjectionOptions) !bool {
    var path = std.ArrayListUnmanaged(u8).empty;
    defer path.deinit(alloc);
    return try fullTextProjectionNeedsSanitizationAtPath(alloc, value, &path, opts);
}

fn fullTextProjectionNeedsSanitizationAtPath(alloc: Allocator, value: std.json.Value, path: *std.ArrayListUnmanaged(u8), opts: TextProjectionOptions) !bool {
    if (isVectorLikeFullTextValue(value, path.items, opts)) return true;
    return switch (value) {
        .object => |object| blk: {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (isSpecialField(entry.key_ptr.*)) break :blk true;
                const old_len = try pushProjectionPath(alloc, path, entry.key_ptr.*);
                defer path.shrinkRetainingCapacity(old_len);
                if (try fullTextProjectionNeedsSanitizationAtPath(alloc, entry.value_ptr.*, path, opts)) break :blk true;
            }
            break :blk false;
        },
        .array => |array| blk: {
            for (array.items) |item| {
                if (try fullTextProjectionNeedsSanitizationAtPath(alloc, item, path, opts)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn cloneFullTextProjectionValue(alloc: Allocator, value: std.json.Value, path: *std.ArrayListUnmanaged(u8), opts: TextProjectionOptions) !?std.json.Value {
    if (isVectorLikeFullTextValue(value, path.items, opts)) return null;
    return switch (value) {
        .null => .null,
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .number_string = try alloc.dupe(u8, s) },
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .array => |arr| blk: {
            var cloned = std.json.Array.init(alloc);
            for (arr.items) |item| {
                if (try cloneFullTextProjectionValue(alloc, item, path, opts)) |child| {
                    try cloned.append(child);
                }
            }
            break :blk .{ .array = cloned };
        },
        .object => |obj| blk: {
            var cloned = std.json.ObjectMap.empty;
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (isSpecialField(entry.key_ptr.*)) continue;
                const old_len = try pushProjectionPath(alloc, path, entry.key_ptr.*);
                defer path.shrinkRetainingCapacity(old_len);
                if (try cloneFullTextProjectionValue(alloc, entry.value_ptr.*, path, opts)) |child| {
                    try cloned.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), child);
                }
            }
            break :blk .{ .object = cloned };
        },
    };
}

fn isVectorLikeFullTextValue(value: std.json.Value, path: []const u8, opts: TextProjectionOptions) bool {
    const configured_vector_field = projectionPathMatchesAny(opts.vector_field_paths, path);
    if (configured_vector_field) {
        return isNumericArrayValue(value) or isSparseVectorObjectValue(value);
    }
    if (!opts.strip_numeric_array_heuristic) return false;
    return isNumericArrayValue(value) or isSparseVectorObjectValue(value);
}

fn pushProjectionPath(alloc: Allocator, path: *std.ArrayListUnmanaged(u8), child: []const u8) !usize {
    const old_len = path.items.len;
    if (old_len > 0) try path.append(alloc, '.');
    try path.appendSlice(alloc, child);
    return old_len;
}

fn projectionPathMatchesAny(paths: []const []const u8, path: []const u8) bool {
    if (path.len == 0) return false;
    for (paths) |configured| {
        if (std.mem.eql(u8, configured, path)) return true;
    }
    return false;
}

fn isNumericArrayValue(value: std.json.Value) bool {
    if (value != .array) return false;
    if (value.array.items.len == 0) return false;
    for (value.array.items) |item| {
        if (!isJsonNumericValue(item)) return false;
    }
    return true;
}

fn isSparseVectorObjectValue(value: std.json.Value) bool {
    if (value != .object) return false;
    const indices = value.object.get("indices") orelse return false;
    const values = value.object.get("values") orelse return false;
    if (!isNumericArrayValue(indices) or !isNumericArrayValue(values)) return false;
    return indices.array.items.len == values.array.items.len;
}

fn isJsonNumericValue(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float, .number_string => true,
        else => false,
    };
}

fn cloneWithoutSpecialFields(alloc: Allocator, root: std.json.Value) !std.json.Value {
    var value = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer freeJsonValue(alloc, &value);

    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (isSpecialField(entry.key_ptr.*)) continue;
        try value.object.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValue(alloc, entry.value_ptr.*));
    }

    return value;
}

pub fn stripTopLevelFieldsAlloc(alloc: Allocator, data: []const u8, fields: []const []const u8) !?[]u8 {
    if (fields.len == 0) return try alloc.dupe(u8, data);
    fast_path: {
        const stripped = stripTopLevelFieldsRawFastAlloc(alloc, data, fields) catch |err| switch (err) {
            error.UnsupportedSparseFastPath => break :fast_path,
            else => return err,
        };
        return stripped;
    }

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, data, .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.dupe(u8, data);

    var value = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer freeJsonValue(alloc, &value);

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (containsTopLevelField(fields, entry.key_ptr.*)) continue;
        try value.object.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValue(alloc, entry.value_ptr.*));
    }

    if (value.object.count() == 0) return null;
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn stripTopLevelFieldsRawFastAlloc(alloc: Allocator, data: []const u8, fields: []const []const u8) !?[]u8 {
    var pos: usize = 0;
    skipJsonWhitespace(data, &pos);
    if (pos >= data.len or data[pos] != '{') return try alloc.dupe(u8, data);
    pos += 1;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var wrote_any = false;
    var removed_any = false;

    while (true) {
        skipJsonWhitespace(data, &pos);
        if (pos >= data.len) return error.SyntaxError;
        if (data[pos] == '}') {
            pos += 1;
            break;
        }

        const entry_start = pos;
        const field = try parseRawJsonString(data, &pos);
        skipJsonWhitespace(data, &pos);
        if (pos >= data.len or data[pos] != ':') return error.SyntaxError;
        pos += 1;
        skipJsonWhitespace(data, &pos);
        try skipRawJsonValue(data, &pos);
        const entry_end = pos;

        if (containsTopLevelField(fields, field)) {
            removed_any = true;
        } else {
            if (wrote_any) try out.append(alloc, ',');
            try out.appendSlice(alloc, data[entry_start..entry_end]);
            wrote_any = true;
        }

        skipJsonWhitespace(data, &pos);
        if (pos >= data.len) return error.SyntaxError;
        if (data[pos] == ',') {
            pos += 1;
            continue;
        }
        if (data[pos] == '}') {
            pos += 1;
            break;
        }
        return error.SyntaxError;
    }

    skipJsonWhitespace(data, &pos);
    if (pos != data.len) return error.SyntaxError;
    if (!removed_any) {
        out.deinit(alloc);
        return try alloc.dupe(u8, data);
    }
    if (!wrote_any) {
        out.deinit(alloc);
        return null;
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn containsTopLevelField(fields: []const []const u8, field: []const u8) bool {
    for (fields) |item| {
        if (std.mem.eql(u8, item, field)) return true;
    }
    return false;
}

fn cloneJsonValue(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .number_string = try alloc.dupe(u8, s) },
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .array => |arr| blk: {
            var cloned = std.json.Array.init(alloc);
            errdefer cloned.deinit();
            for (arr.items) |item| try cloned.append(try cloneJsonValue(alloc, item));
            break :blk .{ .array = cloned };
        },
        .object => |obj| blk: {
            var cloned = std.json.ObjectMap.empty;
            errdefer {
                var it = cloned.iterator();
                while (it.next()) |entry| {
                    alloc.free(entry.key_ptr.*);
                    freeJsonValue(alloc, entry.value_ptr);
                }
                cloned.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                try cloned.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValue(alloc, entry.value_ptr.*));
            }
            break :blk .{ .object = cloned };
        },
    };
}

fn freeJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .null, .bool, .integer, .float => {},
        .number_string => |s| alloc.free(s),
        .string => |s| alloc.free(s),
        .array => |*arr| {
            for (arr.items) |*item| freeJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                freeJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
    }
    value.* = undefined;
}

fn isSpecialField(field_name: []const u8) bool {
    return std.mem.eql(u8, field_name, "_edges") or
        std.mem.eql(u8, field_name, "_embeddings");
}

fn appendUniqueString(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8), value: []const u8) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try list.append(alloc, try alloc.dupe(u8, value));
}

test "document mapper builds text segment from top-level string fields" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"title\":\"alpha\",\"count\":1,\"body\":\"beta gamma\"}" },
    }, text_analysis, null)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u32, 1), reader.doc_count);
    try std.testing.expect((try reader.invertedIndex("title")) != null);
    try std.testing.expect((try reader.invertedIndex("title.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("body")) != null);
    try std.testing.expect((try reader.invertedIndex("body.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("count")) == null);
}

test "document mapper splits oversized text batches into bounded segments" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};

    var built = try buildTextSegmentsFromDocumentsWithMetadata(alloc, &.{
        .{ .key = "doc:1", .value = "{\"title\":\"alpha one\",\"body\":\"first document text\"}" },
        .{ .key = "doc:2", .value = "{\"title\":\"beta two\",\"body\":\"second document text\"}" },
        .{ .key = "doc:3", .value = "{\"title\":\"gamma three\",\"body\":\"third document text\"}" },
    }, text_analysis, null, .{ .target_segment_bytes = 1 });
    defer built.deinit(alloc);

    try std.testing.expect(built.segments.len > 1);
    var total_docs: u32 = 0;
    for (built.segments) |segment| {
        var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
        defer reader.deinit();
        try std.testing.expect(reader.doc_count > 0);
        try std.testing.expect((try reader.invertedIndex("title")) != null);
        total_docs += reader.doc_count;
    }
    try std.testing.expectEqual(@as(u32, 3), total_docs);
}

test "document mapper full text projection omits vector-like stored payloads" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const source =
        \\{"title":"alpha","embedding":[0.1,0.2,0.3],"sparse":{"indices":[1,5],"values":[0.25,0.75]},"tags":["keep","me"]}
    ;

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = source },
    }, text_analysis, null)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();
    const stored = (try reader.storedDocDecompressed(0)).?;
    defer alloc.free(stored.data);

    try std.testing.expect(std.mem.indexOf(u8, source, "\"embedding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, source, "\"sparse\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored.data, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored.data, "\"sparse\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored.data, "\"title\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored.data, "\"tags\"") != null);
    try std.testing.expect(reader.getSection("embedding", .typed_doc_values) == null);
    try std.testing.expect(reader.getSection("sparse.indices", .typed_doc_values) == null);
    try std.testing.expect(reader.getSection("sparse.values", .typed_doc_values) == null);
}

test "document mapper full text projection uses configured vector fields before numeric array heuristic" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const source =
        \\{"title":"alpha","embedding":[0.1,0.2,0.3],"ratings":[1,2,3],"score":9.5}
    ;

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const source_batch = try buildTextProjectionSourceBatchWithOptions(arena, &.{
        .{ .key = "doc:1", .value = source },
    }, .{
        .vector_field_paths = &.{"embedding"},
        .strip_numeric_array_heuristic = false,
    });
    const projection_batch = try buildTextProjectionBatchFromSource(arena, source_batch.docs, text_analysis, null, null);
    const segment = (try buildTextSegmentFromProjectionBatch(alloc, projection_batch, text_analysis)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();
    const stored = (try reader.storedDocDecompressed(0)).?;
    defer alloc.free(stored.data);

    try std.testing.expect(std.mem.indexOf(u8, stored.data, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored.data, "\"ratings\"") != null);
    try std.testing.expect(reader.getSection("embedding", .typed_doc_values) == null);
    try std.testing.expect(reader.getSection("score", .typed_doc_values) != null);
}

test "document mapper builds text segment from nested string fields without schema" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"title\":\"alpha\",\"meta\":{\"summary\":\"beta gamma\",\"tags\":[\"delta\"]}}" },
    }, text_analysis, null)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("title")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.summary")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.tags")) != null);
}

test "document mapper schema-less fast projection indexes nested string fields" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const source_batch = try buildTextProjectionSourceBatchWithOptions(arena, &.{
        .{ .key = "doc:1", .value = "{\"title\":\"alpha\",\"meta\":{\"summary\":\"beta gamma\",\"tags\":[\"delta\"]}}" },
    }, .{
        .strip_numeric_array_heuristic = false,
        .schema_less_fast_projection = true,
    });
    try std.testing.expect(source_batch.docs[0].schema_less_fast_projection);

    const projection_batch = try buildTextProjectionBatchFromSource(arena, source_batch.docs, text_analysis, null, null);
    const segment = (try buildTextSegmentFromProjectionBatch(alloc, projection_batch, text_analysis)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("title")) != null);
    try std.testing.expect((try reader.invertedIndex("title.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.summary")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.summary.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.tags")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.tags.keyword")) != null);
}

test "document mapper schema-less projection indexes exact fields with embeddings stripped" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"status\":\"active\",\"tenant\":\"tenanta\",\"_embeddings\":{\"dense_idx\":\"AACAPwAAAEAAAEBA\"}}" },
    }, text_analysis, null)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("status")) != null);
    try std.testing.expect((try reader.invertedIndex("status.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("tenant")) != null);
    try std.testing.expect((try reader.invertedIndex("tenant.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("_embeddings")) == null);
}

test "document mapper emits schema-driven search_as_you_type variants" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .full_text_documents = &.{
            .{
                .name = "product",
                .fields = &.{
                    .{
                        .path = "name",
                        .emitted_name = "name",
                        .analyzer = "standard",
                    },
                    .{
                        .path = "name",
                        .emitted_name = "name.keyword",
                        .analyzer = "keyword",
                    },
                    .{
                        .path = "name",
                        .emitted_name = "name._2gram",
                        .analyzer = "search_as_you_type_2gram",
                    },
                    .{
                        .path = "name",
                        .emitted_name = "name._3gram",
                        .analyzer = "search_as_you_type_3gram",
                    },
                    .{
                        .path = "name",
                        .emitted_name = "name._index_prefix",
                        .analyzer = "search_as_you_type_index_prefix",
                    },
                },
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"name\":\"Smartphone Apple iPhone\"}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("name")) != null);
    try std.testing.expect((try reader.invertedIndex("name.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("name._2gram")) != null);
    try std.testing.expect((try reader.invertedIndex("name._3gram")) != null);
    try std.testing.expect((try reader.invertedIndex("name._index_prefix")) != null);
}

test "document mapper emits Go-style dynamic-template search_as_you_type field" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .dynamic_templates = &.{
            .{
                .name = "meta_search",
                .path_match = "meta.*",
                .mapping = .{
                    .field_type = .search_as_you_type,
                    .analyzer = "search_as_you_type_index_prefix",
                },
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"meta\":{\"nickname\":\"Gamma\"}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.nickname")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.nickname._2gram")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.nickname._3gram")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.nickname._index_prefix")) == null);
}

test "document mapper honors dynamic-template exclusions and mapping type" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .dynamic_templates = &.{
            .{
                .name = "dates_only",
                .match_pattern = "*_at",
                .unmatch_pattern = "skip_*",
                .path_match = "meta.*",
                .path_unmatch = "meta.private.*",
                .match_mapping_type = "date",
                .mapping = .{
                    .field_type = .keyword,
                    .do_index = true,
                    .include_in_all = true,
                    .analyzer = "keyword",
                },
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"meta\":{\"created_at\":\"2026-01-03T00:00:00Z\",\"skip_created_at\":\"2026-01-03T00:00:00Z\",\"private\":{\"archived_at\":\"2026-01-03T00:00:00Z\"},\"updated_at\":\"not-a-date\"}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.created_at")) != null);
    try std.testing.expect((try reader.invertedIndex("_all")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.skip_created_at")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.private.archived_at")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.updated_at")) == null);
}

test "document mapper records observed dynamic-template field analyzers" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .dynamic_templates = &.{
            .{
                .name = "meta_text",
                .path_match = "meta.*",
                .match_mapping_type = "string",
                .mapping = .{
                    .field_type = .text,
                    .analyzer = "french",
                },
            },
            .{
                .name = "meta_date",
                .path_match = "meta.*",
                .match_mapping_type = "date",
                .mapping = .{
                    .field_type = .keyword,
                    .analyzer = "keyword",
                },
            },
        },
    };

    var result = try buildTextSegmentFromDocumentsWithMetadata(alloc, &.{
        .{ .key = "doc:1", .value = "{\"meta\":{\"body\":\"les maisons\",\"published\":\"2025-01-02\"}}" },
    }, text_analysis, schema);
    defer result.deinit(alloc);

    try std.testing.expect(result.segment != null);
    try std.testing.expectEqual(@as(usize, 2), result.observed_field_analyzers.len);
    try std.testing.expectEqualStrings("meta.body", result.observed_field_analyzers[0].field_name);
    try std.testing.expectEqualStrings("french", result.observed_field_analyzers[0].analyzer_name);
    try std.testing.expectEqualStrings("meta.published", result.observed_field_analyzers[1].field_name);
    try std.testing.expectEqualStrings("keyword", result.observed_field_analyzers[1].analyzer_name);
}

test "document mapper emits additional-properties search_as_you_type variants" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .full_text_documents = &.{
            .{
                .name = "product",
                .dynamic_rules = &.{
                    .{
                        .parent_path = "meta",
                        .variants = &.{
                            .{
                                .suffix = "",
                                .analyzer = "standard",
                            },
                            .{
                                .suffix = "._2gram",
                                .analyzer = "search_as_you_type_2gram",
                            },
                            .{
                                .suffix = "._3gram",
                                .analyzer = "search_as_you_type_3gram",
                            },
                            .{
                                .suffix = "._index_prefix",
                                .analyzer = "search_as_you_type_index_prefix",
                            },
                        },
                    },
                },
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"_type\":\"product\",\"meta\":{\"nickname\":\"Gamma Ray Burst\"}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.nickname")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.nickname._2gram")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.nickname._3gram")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.nickname._index_prefix")) != null);
}

test "document mapper emits nested additional-properties search_as_you_type variants" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .full_text_documents = &.{
            .{
                .name = "product",
                .dynamic_rules = &.{
                    .{
                        .parent_path = "meta",
                        .relative_path = "title",
                        .variants = &.{
                            .{
                                .suffix = "",
                                .analyzer = "standard",
                            },
                            .{
                                .suffix = "._2gram",
                                .analyzer = "search_as_you_type_2gram",
                            },
                            .{
                                .suffix = "._3gram",
                                .analyzer = "search_as_you_type_3gram",
                            },
                            .{
                                .suffix = "._index_prefix",
                                .analyzer = "search_as_you_type_index_prefix",
                            },
                        },
                    },
                },
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"_type\":\"product\",\"meta\":{\"foo\":{\"title\":\"Gamma Ray Burst\"}}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.foo.title")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.foo.title._2gram")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.foo.title._3gram")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.foo.title._index_prefix")) != null);
}

test "document mapper emits pattern-properties search_as_you_type variants" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .full_text_documents = &.{
            .{
                .name = "product",
                .dynamic_rules = &.{
                    .{
                        .parent_path = "meta",
                        .segment_pattern = "^tag_[a-z]+$",
                        .relative_path = "title",
                        .variants = &.{
                            .{
                                .suffix = "",
                                .analyzer = "standard",
                            },
                            .{
                                .suffix = "._2gram",
                                .analyzer = "search_as_you_type_2gram",
                            },
                            .{
                                .suffix = "._3gram",
                                .analyzer = "search_as_you_type_3gram",
                            },
                            .{
                                .suffix = "._index_prefix",
                                .analyzer = "search_as_you_type_index_prefix",
                            },
                        },
                    },
                },
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"_type\":\"product\",\"meta\":{\"tag_blue\":{\"title\":\"Gamma Ray Burst\"},\"skip\":{\"title\":\"Nope\"}}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.tag_blue.title")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.tag_blue.title._2gram")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.tag_blue.title._3gram")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.tag_blue.title._index_prefix")) != null);
    try std.testing.expect((try reader.invertedIndex("meta.skip.title")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.skip.title._2gram")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.skip.title._3gram")) == null);
    try std.testing.expect((try reader.invertedIndex("meta.skip.title._index_prefix")) == null);
}

test "document mapper emits additionalProperties true fallback text fields" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .full_text_documents = &.{
            .{
                .name = "product",
                .open_dynamic_paths = &.{"meta"},
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"_type\":\"product\",\"meta\":{\"foo\":{\"title\":\"Gamma\"}},\"skip\":{\"title\":\"Nope\"}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.foo.title")) != null);
    try std.testing.expect((try reader.invertedIndex("skip.title")) == null);
}

test "document mapper emits schema-present infer_types text fields" {
    const alloc = std.testing.allocator;
    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const schema: runtime_schema.TableSchema = .{
        .version = 0,
        .default_type = "product",
        .ttl_field = "_timestamp",
        .full_text_documents = &.{
            .{
                .name = "product",
                .infer_type_dynamic_paths = &.{"meta"},
            },
        },
    };

    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"_type\":\"product\",\"meta\":{\"foo\":{\"title\":\"Gamma\"}},\"skip\":{\"title\":\"Nope\"}}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("meta.foo.title")) != null);
    try std.testing.expect((try reader.invertedIndex("skip.title")) == null);
}

test "document mapper emits default dynamic schema text fields" {
    const alloc = std.testing.allocator;
    const default_schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true,"x-antfly-dynamic-indexing":{"mode":"infer_types"}}}}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, default_schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const segment = (try buildTextSegmentFromDocuments(alloc, &.{
        .{ .key = "doc:1", .value = "{\"title\":\"Document One\",\"body\":\"alpha benchmark body\",\"status\":\"active\",\"tenant\":\"tenanta\"}" },
    }, text_analysis, schema)).?;
    defer alloc.free(segment);

    var reader = try @import("../../segment.zig").SegmentReader.init(alloc, segment);
    defer reader.deinit();

    try std.testing.expect((try reader.invertedIndex("title")) != null);
    try std.testing.expect((try reader.invertedIndex("body")) != null);
    try std.testing.expect((try reader.invertedIndex("status")) != null);
    try std.testing.expect((try reader.invertedIndex("status.keyword")) != null);
    try std.testing.expect((try reader.invertedIndex("tenant")) != null);
    try std.testing.expect((try reader.invertedIndex("tenant.keyword")) != null);
}

test "document mapper extracts dense vector from configured field" {
    const alloc = std.testing.allocator;

    const values = (try extractDenseVectorField(alloc, "{\"embedding\":[1,2.5,3]}", "embedding", 3)).?;
    defer alloc.free(values);

    try std.testing.expectEqual(@as(usize, 3), values.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), values[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), values[2], 0.0001);
}

test "document mapper dense extractor skips unrelated top-level values" {
    const alloc = std.testing.allocator;

    const values = (try extractDenseVectorField(alloc,
        \\{"title":"alpha","meta":{"nested":[1,2,3]},"embedding":[1,2.5,3],"tail":true}
    , "embedding", 3)).?;
    defer alloc.free(values);

    try std.testing.expectEqual(@as(usize, 3), values.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), values[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), values[2], 0.0001);
}

test "document mapper dense extractor returns null when field is absent" {
    const alloc = std.testing.allocator;
    try std.testing.expect((try extractDenseVectorField(alloc, "{\"title\":\"alpha\"}", "embedding", 3)) == null);
}

test "document mapper extracts sparse vector from configured field" {
    const alloc = std.testing.allocator;

    var vec = (try extractSparseVectorField(alloc, "{\"sparse\":{\"indices\":[1,5],\"values\":[0.25,0.75]}}", "sparse")).?;
    defer vec.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), vec.indices.len);
    try std.testing.expectEqual(@as(u32, 1), vec.indices[0]);
    try std.testing.expectEqual(@as(u32, 5), vec.indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), vec.values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), vec.values[1], 0.0001);
}

test "document mapper strips top-level vector fields with raw fast path" {
    const alloc = std.testing.allocator;

    const stripped = (try stripTopLevelFieldsAlloc(
        alloc,
        "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]},\"tail\":true}",
        &.{"sparse"},
    )).?;
    defer alloc.free(stripped);

    try std.testing.expectEqualStrings("{\"title\":\"alpha\",\"tail\":true}", stripped);
}

test "document mapper strips all selected top-level fields to null document" {
    const alloc = std.testing.allocator;

    try std.testing.expect((try stripTopLevelFieldsAlloc(
        alloc,
        "{\"sparse\":{\"indices\":[1],\"values\":[1.0]}}",
        &.{"sparse"},
    )) == null);
}

test "document mapper extracts sparse vector from token weight map" {
    const alloc = std.testing.allocator;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"42":2.0,"7":1.5}
    , .{});
    defer parsed.deinit();

    var vec = try parseSparseValue(alloc, parsed.value);
    defer vec.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), vec.indices.len);
    try std.testing.expectEqual(@as(u32, 7), vec.indices[0]);
    try std.testing.expectEqual(@as(u32, 42), vec.indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), vec.values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), vec.values[1], 0.0001);
}

test "document mapper extracts packed dense embeddings from _embeddings" {
    const alloc = std.testing.allocator;

    var extracted = try extractWrite(alloc, "doc:a",
        \\{"title":"alpha","_embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"}}
    );
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqualStrings("dense_idx", extracted.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), extracted.dense_embeddings[0].vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), extracted.dense_embeddings[0].vector[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), extracted.dense_embeddings[0].vector[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), extracted.dense_embeddings[0].vector[2], 0.0001);
}

test "document mapper fast path extracts benchmark-shaped packed dense embeddings" {
    const alloc = std.testing.allocator;

    var extracted = try extractWrite(alloc, "key:42",
        \\{"id":42,"metadata":42,"source":"42","_embeddings":{"vec":"AACAPwAAAEAAAEBA"}}
    );
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqualStrings("vec", extracted.dense_embeddings[0].index_name);
    try std.testing.expectEqualStrings("key:42", extracted.dense_embeddings[0].doc_key);
    try std.testing.expectEqual(@as(usize, 3), extracted.dense_embeddings[0].vector.len);
    try std.testing.expectEqualStrings(
        \\{"id":42,"metadata":42,"source":"42"}
    , extracted.cleaned_value.?);
}

test "document mapper falls back for nested non-special fields with dense embeddings" {
    const alloc = std.testing.allocator;

    var extracted = try extractWrite(alloc, "doc:a",
        \\{"title":"alpha","meta":{"nested":true},"_embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"}}
    );
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqualStrings(
        \\{"title":"alpha","meta":{"nested":true}}
    , extracted.cleaned_value.?);
}

test "document mapper fast path keeps plain vector docs opaque" {
    const alloc = std.testing.allocator;

    var extracted = try extractWrite(alloc, "doc:a",
        \\{"title":"alpha","embedding":[1,2,3]}
    );
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), extracted.dense_embeddings.len);
    try std.testing.expectEqual(@as(usize, 0), extracted.sparse_embeddings.len);
    try std.testing.expect(extracted.cleaned_value != null);
    try std.testing.expectEqualStrings(
        \\{"title":"alpha","embedding":[1,2,3]}
    , extracted.cleaned_value.?);
}

test "document mapper fast path still rejects invalid json" {
    try std.testing.expectError(error.UnexpectedEndOfInput, extractWrite(std.testing.allocator, "doc:a", "{\"embedding\":[1,2,3]"));
}

test "document mapper escaped special field uses full parser" {
    const alloc = std.testing.allocator;

    var extracted = try extractWrite(alloc, "doc:a",
        \\{"title":"alpha","_\u0065mbeddings":{"dense_idx":"AACAPwAAAEAAAEBA"}}
    );
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqualStrings("dense_idx", extracted.dense_embeddings[0].index_name);
}

test "document mapper extracts packed sparse embeddings from _embeddings" {
    const alloc = std.testing.allocator;

    var extracted = try extractWrite(alloc, "doc:a",
        \\{"title":"alpha","_embeddings":{"sparse_idx":{"packed_indices":"AQAAAAUAAAA=","packed_values":"AAAAPwAAQD8="}}}
    );
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.sparse_embeddings.len);
    try std.testing.expectEqualStrings("sparse_idx", extracted.sparse_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 2), extracted.sparse_embeddings[0].indices.len);
    try std.testing.expectEqual(@as(u32, 1), extracted.sparse_embeddings[0].indices[0]);
    try std.testing.expectEqual(@as(u32, 5), extracted.sparse_embeddings[0].indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), extracted.sparse_embeddings[0].values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), extracted.sparse_embeddings[0].values[1], 0.0001);
}

test "buildRelationalTypedFields persists every column for reconstruction" {
    // Use an arena: bytes_val for json/string columns are stringified into the
    // passed allocator, matching the real projection arena's lifetime.
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":12.5,"ts":1000,"active":true,"payload":{"k":1}}
    , .{});
    defer doc.deinit();

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "ts", .path = "ts", .field_type = .datetime, .nullable = true },
        .{ .name = "active", .path = "active", .field_type = .boolean, .nullable = true },
        .{ .name = "payload", .path = "payload", .field_type = .json, .nullable = true },
    };

    const fields = try buildRelationalTypedFields(alloc, doc.value, &columns);

    // All five columns are now persisted; string (id) and json (payload) as
    // bytes_val, the rest with their scan-friendly encodings.
    try std.testing.expectEqual(@as(usize, 5), fields.len);
    for (fields) |field| {
        if (std.mem.eql(u8, field.field_name, "id")) {
            try std.testing.expectEqual(typed_dv.ValueType.bytes_val, field.value_type);
            try std.testing.expectEqualStrings("abc", field.value.bytes_val);
        } else if (std.mem.eql(u8, field.field_name, "amount")) {
            try std.testing.expectEqual(typed_dv.ValueType.f64_val, field.value_type);
            try std.testing.expectEqual(@as(f64, 12.5), field.value.f64_val);
        } else if (std.mem.eql(u8, field.field_name, "ts")) {
            try std.testing.expectEqual(typed_dv.ValueType.u64_val, field.value_type);
            try std.testing.expectEqual(@as(u64, 1000), field.value.u64_val);
        } else if (std.mem.eql(u8, field.field_name, "active")) {
            try std.testing.expectEqual(typed_dv.ValueType.bool_val, field.value_type);
            try std.testing.expect(field.value.bool_val);
        } else if (std.mem.eql(u8, field.field_name, "payload")) {
            try std.testing.expectEqual(typed_dv.ValueType.bytes_val, field.value_type);
            // json column persisted as its canonical text
            try std.testing.expect(std.mem.indexOf(u8, field.value.bytes_val, "\"k\"") != null);
        } else {
            return error.UnexpectedColumn;
        }
    }
}

test "extractTextFieldsFromValue attaches relational typed fields" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "ts", .path = "ts", .field_type = .datetime, .nullable = true },
        .{ .name = "active", .path = "active", .field_type = .boolean, .nullable = true },
        .{ .name = "payload", .path = "payload", .field_type = .json, .nullable = true },
    };
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &columns,
    };

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"amount":1.5,"ts":1000,"active":true,"payload":{"k":1}}
    , .{});
    defer doc.deinit();

    const extracted = try extractTextFieldsFromValue(alloc, doc.value, .{}, schema, null);

    // Every present column is now persisted (numeric/datetime/boolean + the
    // json payload as bytes_val), so the segment can reconstruct the document.
    try std.testing.expect(extracted.typed_fields != null);
    try std.testing.expectEqual(@as(usize, 4), extracted.typed_fields.?.len);
}

test "relational numeric column is range-scannable end to end" {
    const alloc = std.testing.allocator;
    const segment_mod = @import("../../segment.zig");

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };

    const doc_json = [_][]const u8{
        "{\"amount\":10.0}",
        "{\"amount\":25.0}",
        "{\"amount\":50.0}",
    };
    const ids = [_][]const u8{ "a", "b", "c" };

    var parsed: [3]std.json.Parsed(std.json.Value) = undefined;
    var typed: [3][]const introducer_mod.TypedFieldValue = undefined;
    var text_docs: [3]introducer_mod.TextDocument = undefined;
    var built: usize = 0;
    defer for (0..built) |i| {
        alloc.free(typed[i]);
        parsed[i].deinit();
    };

    for (doc_json, 0..) |json, i| {
        parsed[i] = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        typed[i] = try buildRelationalTypedFields(alloc, parsed[i].value, &columns);
        text_docs[i] = .{
            .id = ids[i],
            .stored_data = json,
            .text_fields = &.{},
            .typed_fields = typed[i],
        };
        built += 1;
    }

    const seg_bytes = try introducer_mod.buildSegmentFromText(alloc, &text_docs, &analysis_mod.default_analyzer, null);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    const section = reader.getSection("amount", .typed_doc_values) orelse return error.TestExpectedEqual;
    const dv = try typed_dv.TypedDocValuesReader.init(alloc, section);
    try std.testing.expectEqual(typed_dv.ValueType.f64_val, dv.value_type);

    // Range scan [15, 40): only the amount=25 document matches.
    var matches: usize = 0;
    var matched_doc: ?u32 = null;
    for (0..3) |doc_id| {
        const value = (try dv.getF64(@intCast(doc_id))) orelse continue;
        if (value >= 15.0 and value < 40.0) {
            matches += 1;
            matched_doc = @intCast(doc_id);
        }
    }
    try std.testing.expectEqual(@as(usize, 1), matches);
    try std.testing.expectEqual(@as(f64, 25.0), (try dv.getF64(matched_doc.?)).?);
}

test "relational document reconstructs from a persisted segment" {
    // Full write -> persist -> read -> reconstruct cycle on a real segment.
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();
    const segment_mod = @import("../../segment.zig");

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "ts", .path = "ts", .field_type = .datetime, .nullable = true },
        .{ .name = "active", .path = "active", .field_type = .boolean, .nullable = true },
        .{ .name = "payload", .path = "payload", .field_type = .json, .nullable = true },
    };

    const doc_json =
        \\{"id":"abc","amount":12.5,"ts":1000,"active":true,"payload":{"k":1}}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{});
    defer parsed.deinit();

    // Write path: project all columns and build a segment via the introducer.
    const typed = try buildRelationalTypedFields(alloc, parsed.value, &columns);
    const text_docs = [_]introducer_mod.TextDocument{.{
        .id = "abc",
        .stored_data = doc_json,
        .text_fields = &.{},
        .typed_fields = typed,
    }};
    const seg_bytes = try introducer_mod.buildSegmentFromText(alloc, &text_docs, &analysis_mod.default_analyzer, null);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    // Read path: reconstruct the document from the persisted column sections.
    const rebuilt_json = try reconstructRelationalDocumentFromSegmentAlloc(alloc, &reader, &columns, 0);
    defer alloc.free(rebuilt_json);

    var rebuilt = try std.json.parseFromSlice(std.json.Value, alloc, rebuilt_json, .{});
    defer rebuilt.deinit();
    const obj = rebuilt.value.object;

    try std.testing.expectEqual(@as(usize, 5), obj.count());
    try std.testing.expectEqualStrings("abc", obj.get("id").?.string);
    const amount = obj.get("amount").?;
    const amount_num: f64 = switch (amount) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => unreachable,
    };
    try std.testing.expectEqual(@as(f64, 12.5), amount_num);
    try std.testing.expectEqual(@as(i64, 1000), obj.get("ts").?.integer);
    try std.testing.expect(obj.get("active").?.bool);
    try std.testing.expectEqual(@as(i64, 1), obj.get("payload").?.object.get("k").?.integer);
}

test "relational segment reconstruction omits absent nullable columns" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();
    const segment_mod = @import("../../segment.zig");

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "note", .path = "note", .field_type = .keyword, .nullable = true },
    };

    // "note" is absent -> no section -> omitted from reconstruction.
    const doc_json = "{\"id\":\"x\",\"amount\":7.0}";
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{});
    defer parsed.deinit();

    const typed = try buildRelationalTypedFields(alloc, parsed.value, &columns);
    const text_docs = [_]introducer_mod.TextDocument{.{
        .id = "x",
        .stored_data = doc_json,
        .text_fields = &.{},
        .typed_fields = typed,
    }};
    const seg_bytes = try introducer_mod.buildSegmentFromText(alloc, &text_docs, &analysis_mod.default_analyzer, null);
    defer alloc.free(seg_bytes);

    var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    const rebuilt_json = try reconstructRelationalDocumentFromSegmentAlloc(alloc, &reader, &columns, 0);
    defer alloc.free(rebuilt_json);

    var rebuilt = try std.json.parseFromSlice(std.json.Value, alloc, rebuilt_json, .{});
    defer rebuilt.deinit();
    const obj = rebuilt.value.object;

    try std.testing.expectEqual(@as(usize, 2), obj.count());
    try std.testing.expectEqualStrings("x", obj.get("id").?.string);
    try std.testing.expect(obj.get("note") == null);
}

test "relational segment stores empty body and reconstructs via manifest after merge" {
    // Proves the full write -> empty-body -> merge -> reconstruct path: the
    // introducer writes an empty stored-doc body plus a manifest section, the
    // merge code carries the manifest forward (a schema-less data-movement
    // path), and the reader's storedDocDecompressed chokepoint reconstructs the
    // document from typed columns on the *merged* segment.
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();
    const segment_mod = @import("../../segment.zig");

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "active", .path = "active", .field_type = .boolean, .nullable = true },
    };
    const manifest_columns = (try relationalManifestColumnsAlloc(alloc, .{
        .storage_mode = .relational,
        .relational_columns = &columns,
    })).?;

    const buildSeg = struct {
        fn run(a: std.mem.Allocator, cols: []const runtime_schema.RelationalColumn, mcols: []const relational_manifest.ManifestColumn, key: []const u8, doc_json: []const u8) ![]u8 {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, doc_json, .{});
            defer parsed.deinit();
            const typed = try buildRelationalTypedFields(a, parsed.value, cols);
            const text_docs = [_]introducer_mod.TextDocument{.{
                .id = key,
                .stored_data = doc_json,
                .text_fields = &.{},
                .typed_fields = typed,
            }};
            return try introducer_mod.buildSegmentFromTextWithAnalysisOptions(
                a,
                &text_docs,
                &analysis_mod.default_analyzer,
                .{},
                .{ .relational_manifest_columns = mcols },
            );
        }
    }.run;

    const seg1 = try buildSeg(alloc, &columns, manifest_columns, "doc:a", "{\"id\":\"doc:a\",\"amount\":1.5,\"active\":true}");
    defer alloc.free(seg1);
    const seg2 = try buildSeg(alloc, &columns, manifest_columns, "doc:b", "{\"id\":\"doc:b\",\"amount\":2.5}");
    defer alloc.free(seg2);

    // Each input segment stores an empty body (reconstruction-only).
    {
        var r1 = try segment_mod.SegmentReader.init(alloc, seg1);
        defer r1.deinit();
        try std.testing.expectEqual(@as(usize, 0), r1.storedDoc(0).?.data.len);
    }

    const merged = try segment_mod.mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var reader = try segment_mod.SegmentReader.init(alloc, merged);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);

    // The merged segment still carries the manifest, so the reader chokepoint
    // reconstructs both documents from typed columns.
    const checkDoc = struct {
        fn run(a: std.mem.Allocator, rdr: *const segment_mod.SegmentReader, idx: u32, want_id: []const u8, want_amount: f64, want_active: ?bool) !void {
            const stored = (try rdr.storedDocDecompressed(idx)).?;
            defer a.free(stored.data);
            try std.testing.expectEqualStrings(want_id, stored.id);
            var doc = try std.json.parseFromSlice(std.json.Value, a, stored.data, .{});
            defer doc.deinit();
            const obj = doc.value.object;
            try std.testing.expectEqualStrings(want_id, obj.get("id").?.string);
            const amt = obj.get("amount").?;
            const amt_num: f64 = switch (amt) {
                .float => |f| f,
                .integer => |i| @floatFromInt(i),
                else => unreachable,
            };
            try std.testing.expectEqual(want_amount, amt_num);
            if (want_active) |flag| {
                try std.testing.expectEqual(flag, obj.get("active").?.bool);
            } else {
                try std.testing.expect(obj.get("active") == null);
            }
        }
    }.run;

    // Merge preserves input order: seg1's doc (doc:a) then seg2's (doc:b).
    try checkDoc(alloc, &reader, 0, "doc:a", 1.5, true);
    try checkDoc(alloc, &reader, 1, "doc:b", 2.5, null);
}
