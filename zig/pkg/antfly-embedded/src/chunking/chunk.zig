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

const std = @import("std");

const Allocator = std.mem.Allocator;

/// Borrowed model-chunker input. Parsing never reads a source or decodes the
/// inline payload, so runtime execution and offline inspection share the same
/// source-reference contract without allocating binary media.
pub const ModelSource = union(enum) {
    none,
    text: []const u8,
    source: @import("inference_chunker").SourceReference,
    media: struct { mime_type: []const u8, data: []const u8 },
};

/// Model-aware source fields may contain plain media locators. Keep this
/// recognition shared with offline backup inspection; parsing never fetches.
pub fn isRemoteModelLocator(value: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(value, "https://") or std.ascii.startsWithIgnoreCase(value, "http://") or std.ascii.startsWithIgnoreCase(value, "s3://") or std.ascii.startsWithIgnoreCase(value, "gs://");
}

pub fn modelSourceFromValue(field: std.json.Value) !ModelSource {
    if (field == .null) return .none;
    if (field == .string) return .{ .text = field.string };
    if (field != .object) return error.InvalidChunkSource;
    const kind = field.object.get("type") orelse return error.InvalidChunkSource;
    if (kind == .string and std.mem.eql(u8, kind.string, "source")) {
        const uri = field.object.get("uri") orelse return error.InvalidChunkSource;
        const mime = field.object.get("mime_type") orelse return error.InvalidChunkSource;
        const version = field.object.get("version") orelse return error.InvalidChunkSource;
        const length = field.object.get("length") orelse return error.InvalidChunkSource;
        if (uri != .string or mime != .string or version != .string or length != .integer or length.integer <= 0 or mime.string.len == 0) return error.InvalidChunkSource;
        var reference = @import("inference_chunker").SourceReference{ .uri = uri.string, .mime_type = mime.string, .version = version.string, .length = @intCast(length.integer) };
        if (field.object.get("version_kind")) |value| {
            if (value != .string) return error.InvalidChunkSource;
            reference.version_kind = std.meta.stringToEnum(@TypeOf(reference.version_kind), value.string) orelse return error.InvalidChunkSource;
        }
        _ = try @import("antfly_source_reference").validateReference(reference.uri, reference.version, switch (reference.version_kind) {
            .strong_etag => .strong_etag,
            .s3_version_id => .s3_version_id,
        }, reference.length);
        return .{ .source = reference };
    }
    if (kind != .string or !std.mem.eql(u8, kind.string, "media")) return error.InvalidChunkSource;
    const mime = field.object.get("mime_type") orelse return error.InvalidChunkSource;
    const data = field.object.get("data") orelse return error.InvalidChunkSource;
    if (mime != .string or data != .string or mime.string.len == 0) return error.InvalidChunkSource;
    return .{ .media = .{ .mime_type = mime.string, .data = data.string } };
}

pub const Chunk = struct {
    chunk_id: u32,
    mime_type: []const u8 = "text/plain",
    owns_mime_type: bool = false,
    text: ?[]u8 = null,
    data: ?[]u8 = null,
    start_offset: ?u32 = null,
    end_offset: ?u32 = null,
    start_time_ms: ?f32 = null,
    end_time_ms: ?f32 = null,
    /// Which speaker this chunk's audio belongs to, when the transcript was
    /// diarized and the chunk does not straddle a turn. Stored as the index
    /// the unit's transcript spans carry; the document field is its label.
    speaker_index: ?u8 = null,
    /// Owned exact phrase clocks and parent media selectors for transcript chunks.
    media_spans_json: ?[]const u8 = null,
    frame_index: ?u32 = null,
    frame_delay_ms: ?u32 = null,
    metadata: ?*@import("inference_chunker").types.ModelMetadata = null,

    pub fn deinit(self: *Chunk, alloc: Allocator) void {
        if (self.media_spans_json) |json| alloc.free(json);
        if (self.metadata) |metadata| metadata.deinit(alloc);
        if (self.text) |text| alloc.free(text);
        if (self.data) |data| alloc.free(data);
        if (self.owns_mime_type) alloc.free(@constCast(self.mime_type));
        self.* = undefined;
    }

    pub fn isText(self: Chunk) bool {
        return self.text != null and std.mem.eql(u8, self.mime_type, "text/plain");
    }
};

pub const OffsetPair = struct {
    start: ?u32,
    end: ?u32,
};

pub fn completeTextOffsetPair(start: ?u32, end: ?u32, text_len: usize) OffsetPair {
    const start_value = start orelse return .{ .start = null, .end = null };
    const end_value = end orelse return .{ .start = null, .end = null };
    if (start_value > end_value) return .{ .start = null, .end = null };
    if (@as(usize, @intCast(end_value)) > text_len) return .{ .start = null, .end = null };
    return .{ .start = start_value, .end = end_value };
}

pub const ProvenanceScope = enum {
    document,
    unit,
};

pub const NativeTextQualityProvenance = struct {
    too_short: bool,
    garbled: bool,
    font_corrupted: bool,
    replacement_corrupted: bool,
    single_char_word_ratio: f64,
    corrupted_line_ratio: f64,
    replacement_char_ratio: f64,
    trimmed_len: usize,
    non_whitespace_chars: usize,
};

pub const NativeTextDiagnosticsProvenance = struct {
    policy: []const u8,
    extraction_complete: bool,
    semantic_coverage: []const u8,
    actual_text_replacements: usize,
    outside_page_runs: usize,
    invisible_runs: usize,
    unaligned_runs: usize,
    unsupported_ordering_runs: usize,
    source_run_count: usize,
    emitted_run_count: usize,
    outside_page_spans: usize = 0,
    invisible_spans: usize = 0,
    unpositioned_spans: usize = 0,
    actual_text_incomplete: bool = false,
    outline_fallback: bool,
    has_nontext_visual: bool,
    has_unrendered_annotations: bool,
};

pub const RenderPolicyProvenance = struct {
    version: u16,
    sampling: []const u8,
    resolved_profile: ?[]const u8,
    resolution: []const u8,

    pub fn from(policy: anytype) ?@This() {
        const value = policy orelse return null;
        return .{ .version = value.version, .sampling = @tagName(value.sampling), .resolved_profile = if (value.resolved_profile) |profile| @tagName(profile) else null, .resolution = @tagName(value.resolution) };
    }
};

pub const ProvenanceOptions = struct {
    scope: ProvenanceScope = .document,
    parent_doc_key: ?[]const u8 = null,
    parent_unit_key: ?[]const u8 = null,
    parent_unit_id: ?[]const u8 = null,
    source_artifact_name: ?[]const u8 = null,
    document_char_base: ?u32 = null,
    page_number: ?u32 = null,
    page_label: ?[]const u8 = null,
    page_bbox: ?[4]f64 = null,
    page_rotation: ?i32 = null,
    extraction_method: ?[]const u8 = null,
    extraction_status: ?[]const u8 = null,
    confidence: ?f64 = null,
    native_text_quality: ?NativeTextQualityProvenance = null,
    native_text_diagnostics: ?NativeTextDiagnosticsProvenance = null,
    ocr_used: bool = false,
    ocr_attempted: bool = false,
    ocr_render_dpi: ?u16 = null,
    ocr_render_policy: ?RenderPolicyProvenance = null,
    ocr_effective_render_dpi: ?u16 = null,
    ocr_rendered_width: ?u32 = null,
    ocr_rendered_height: ?u32 = null,
    ocr_rendered_bytes: ?u64 = null,
    ocr_failure_stage: ?[]const u8 = null,
    ocr_failure_retryable: ?bool = null,
    ocr_trigger_reasons: ?[]const u8 = null,
    ocr_embedded_quality: ?[]const u8 = null,
    ocr_output_quality: ?[]const u8 = null,
    ocr_confidence: ?f64 = null,
    ocr_bbox: ?[4]f64 = null,
    transcript_used: bool = false,
    transcript_confidence: ?f64 = null,
    extraction_warning: ?[]const u8 = null,
};

pub fn appendArtifactFields(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    source_field: []const u8,
    chunk: Chunk,
    include_payload: bool,
) !void {
    return try appendArtifactFieldsWithProvenance(alloc, obj, source_field, chunk, include_payload, .{});
}

pub fn appendArtifactFieldsWithProvenance(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    source_field: []const u8,
    chunk: Chunk,
    include_payload: bool,
    provenance: ProvenanceOptions,
) !void {
    try obj.put(alloc, try alloc.dupe(u8, "_chunk_id"), .{ .integer = chunk.chunk_id });
    try obj.put(alloc, try alloc.dupe(u8, "_mime_type"), .{ .string = try alloc.dupe(u8, chunk.mime_type) });
    if (chunk.start_offset) |value| try obj.put(alloc, try alloc.dupe(u8, "_start_offset"), .{ .integer = value });
    if (chunk.end_offset) |value| try obj.put(alloc, try alloc.dupe(u8, "_end_offset"), .{ .integer = value });
    if (chunk.start_time_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_start_time_ms"), .{ .float = value });
    if (chunk.end_time_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_end_time_ms"), .{ .float = value });
    if (chunk.speaker_index) |index| {
        var label_buf: [16]u8 = undefined;
        const label = std.fmt.bufPrint(&label_buf, "SPEAKER_{d:0>2}", .{index}) catch unreachable;
        try obj.put(alloc, try alloc.dupe(u8, "_speaker"), .{ .string = try alloc.dupe(u8, label) });
    }
    if (chunk.media_spans_json) |json| {
        var parsed_media = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed_media.deinit();
        try putClonedJsonValue(alloc, obj, "_media", parsed_media.value);
    }
    if (chunk.frame_index) |value| try obj.put(alloc, try alloc.dupe(u8, "_frame_index"), .{ .integer = value });
    if (chunk.frame_delay_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_frame_delay_ms"), .{ .integer = value });
    try appendProvenanceFields(alloc, obj, source_field, chunk, provenance);
    if (chunk.metadata) |metadata| {
        // Keep vectors in the embedding artifact family. This recipe is enough
        // to navigate or reconstruct the source chunk without duplicated media.
        const encoded = try std.json.Stringify.valueAlloc(alloc, .{ .descriptor = metadata.descriptor, .inference_id = metadata.inference_id, .source_fingerprint = metadata.source_fingerprint, .source_version = metadata.source_version, .plan_id = metadata.plan_id, .model = metadata.model, .model_identity = metadata.model_identity, .model_revision = metadata.model_revision, .task_type = metadata.task_type, .dimensions = metadata.dimensions, .policy = metadata.policy, .embedding_error = metadata.embedding_error }, .{ .emit_null_optional_fields = false });
        defer alloc.free(encoded);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        if (metadata.outputs_json) |results| {
            var values = try std.json.parseFromSlice(std.json.Value, alloc, results, .{});
            defer values.deinit();
            if (values.value != .array) return error.InvalidChunkOutput;
            for (values.value.array.items) |*value| {
                if (value.* != .object) return error.InvalidChunkOutput;
                // Retain identities/statuses on the recipe. Vectors, transcript
                // spans and supervised predictions belong to output artifacts.
                for ([_][]const u8{ "embedding", "spans", "labels" }) |name| _ = value.object.swapRemove(name);
            }
            try parsed.value.object.put(parsed.arena.allocator(), "outputs", values.value);
            try putClonedJsonValue(alloc, obj, "_chunk", parsed.value);
        } else {
            try putClonedJsonValue(alloc, obj, "_chunk", parsed.value);
        }
    }

    if (!include_payload) return;

    if (chunk.text) |text| {
        try obj.put(alloc, try alloc.dupe(u8, source_field), .{ .string = try alloc.dupe(u8, text) });
    } else if (chunk.data) |data| {
        const encoded = try base64EncodeAlloc(alloc, data);
        errdefer alloc.free(encoded);
        try obj.put(alloc, try alloc.dupe(u8, "_data"), .{ .string = encoded });
    }
}

/// Resolve the textual payload of a persisted chunk artifact.
///
/// Chunk artifacts retain the producer's source field for compatibility, but
/// downstream artifact consumers use `text` as the canonical payload selector.
/// Falling back through `_source_field` lets those consumers read both newly
/// generated and already-persisted chunks without changing the artifact format.
pub fn artifactTextAlloc(
    alloc: Allocator,
    payload: []const u8,
    source_field: []const u8,
) !?[]u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, payload, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return null;

    if (parsed.value.object.get(source_field)) |source| {
        if (source == .string and source.string.len > 0) return try alloc.dupe(u8, source.string);
        return null;
    }
    if (!std.mem.eql(u8, source_field, "text")) return null;

    const stored_source_field = parsed.value.object.get("_source_field") orelse return null;
    if (stored_source_field != .string or stored_source_field.string.len == 0) return null;
    const source = parsed.value.object.get(stored_source_field.string) orelse return null;
    if (source != .string or source.string.len == 0) return null;
    return try alloc.dupe(u8, source.string);
}

fn appendProvenanceFields(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    source_field: []const u8,
    chunk: Chunk,
    options: ProvenanceOptions,
) !void {
    var provenance = std.json.ObjectMap.empty;
    errdefer {
        var it = provenance.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        provenance.deinit(alloc);
    }

    if (chunk.start_time_ms) |value| try putOwnedValue(alloc, &provenance, "start_time_ms", .{ .float = value });
    if (chunk.end_time_ms) |value| try putOwnedValue(alloc, &provenance, "end_time_ms", .{ .float = value });
    if (chunk.speaker_index) |index| {
        var label_buf: [16]u8 = undefined;
        const label = std.fmt.bufPrint(&label_buf, "SPEAKER_{d:0>2}", .{index}) catch unreachable;
        try putString(alloc, &provenance, "speaker", label);
    }
    try putString(alloc, &provenance, "source_field", source_field);
    try putString(alloc, &provenance, "offset_basis", switch (options.scope) {
        .document => "document",
        .unit => "unit",
    });
    if (options.parent_doc_key) |value| try putString(alloc, &provenance, "parent_doc_key", value);
    if (options.parent_unit_key) |value| try putString(alloc, &provenance, "parent_unit_key", value);
    if (options.parent_unit_id) |value| try putString(alloc, &provenance, "parent_unit_id", value);
    if (options.source_artifact_name) |value| try putString(alloc, &provenance, "source_artifact_name", value);
    if (options.page_number) |value| try provenance.put(alloc, try alloc.dupe(u8, "page_number"), .{ .integer = value });
    if (options.page_label) |value| try putString(alloc, &provenance, "page_label", value);
    if (options.page_bbox) |bbox| try provenance.put(alloc, try alloc.dupe(u8, "page_bbox"), .{ .array = try jsonFloatArrayAlloc(alloc, &bbox) });
    if (options.page_rotation) |value| try provenance.put(alloc, try alloc.dupe(u8, "page_rotation"), .{ .integer = value });
    if (options.extraction_status) |value| try putString(alloc, &provenance, "extraction_status", value);
    if (options.confidence) |value| try provenance.put(alloc, try alloc.dupe(u8, "confidence"), .{ .float = value });
    if (options.native_text_quality) |value| try putStructJsonValue(alloc, &provenance, "native_text_quality", value);
    if (options.native_text_diagnostics) |value| try putStructJsonValue(alloc, &provenance, "native_text_diagnostics", value);
    try provenance.put(alloc, try alloc.dupe(u8, "ocr_used"), .{ .bool = options.ocr_used });
    try provenance.put(alloc, try alloc.dupe(u8, "ocr_attempted"), .{ .bool = options.ocr_attempted });
    if (options.ocr_render_dpi) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_render_dpi"), .{ .integer = value });
    if (options.ocr_render_policy) |value| try putStructJsonValue(alloc, &provenance, "ocr_render_policy", value);
    if (options.ocr_effective_render_dpi) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_effective_render_dpi"), .{ .integer = value });
    if (options.ocr_rendered_width) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_rendered_width"), .{ .integer = value });
    if (options.ocr_rendered_height) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_rendered_height"), .{ .integer = value });
    if (options.ocr_rendered_bytes) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_rendered_bytes"), .{ .integer = @intCast(value) });
    if (options.ocr_failure_stage) |value| try putString(alloc, &provenance, "ocr_failure_stage", value);
    if (options.ocr_failure_retryable) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_failure_retryable"), .{ .bool = value });
    if (options.ocr_trigger_reasons) |value| try putString(alloc, &provenance, "ocr_trigger_reasons", value);
    if (options.ocr_embedded_quality) |value| try putString(alloc, &provenance, "ocr_embedded_quality", value);
    if (options.ocr_output_quality) |value| try putString(alloc, &provenance, "ocr_output_quality", value);
    if (options.ocr_confidence) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_confidence"), .{ .float = value });
    if (options.ocr_bbox) |bbox| try provenance.put(alloc, try alloc.dupe(u8, "ocr_bbox"), .{ .array = try jsonFloatArrayAlloc(alloc, &bbox) });
    try provenance.put(alloc, try alloc.dupe(u8, "transcript_used"), .{ .bool = options.transcript_used });
    if (options.transcript_confidence) |value| try provenance.put(alloc, try alloc.dupe(u8, "transcript_confidence"), .{ .float = value });
    if (options.extraction_warning) |value| try putString(alloc, &provenance, "extraction_warning", value);
    try appendFormatProvenance(alloc, &provenance, options);

    if (chunk.start_offset) |start| {
        try provenance.put(alloc, try alloc.dupe(u8, "char_start"), .{ .integer = start });
        switch (options.scope) {
            .document => try provenance.put(alloc, try alloc.dupe(u8, "document_char_start"), .{ .integer = start }),
            .unit => {
                try provenance.put(alloc, try alloc.dupe(u8, "unit_char_start"), .{ .integer = start });
                if (options.document_char_base) |base| {
                    try provenance.put(alloc, try alloc.dupe(u8, "document_char_start"), .{ .integer = @as(i64, @intCast(base)) + @as(i64, @intCast(start)) });
                }
            },
        }
    }
    if (chunk.end_offset) |end| {
        try provenance.put(alloc, try alloc.dupe(u8, "char_end"), .{ .integer = end });
        switch (options.scope) {
            .document => try provenance.put(alloc, try alloc.dupe(u8, "document_char_end"), .{ .integer = end }),
            .unit => {
                try provenance.put(alloc, try alloc.dupe(u8, "unit_char_end"), .{ .integer = end });
                if (options.document_char_base) |base| {
                    try provenance.put(alloc, try alloc.dupe(u8, "document_char_end"), .{ .integer = @as(i64, @intCast(base)) + @as(i64, @intCast(end)) });
                }
            },
        }
    }

    try obj.put(alloc, try alloc.dupe(u8, "provenance"), .{ .object = provenance });
}

fn appendFormatProvenance(
    alloc: Allocator,
    provenance: *std.json.ObjectMap,
    options: ProvenanceOptions,
) !void {
    if (options.page_number == null and options.page_label == null and options.page_bbox == null and options.page_rotation == null and options.extraction_status == null and options.confidence == null and options.native_text_quality == null and options.native_text_diagnostics == null and !options.ocr_used and !options.ocr_attempted and options.ocr_confidence == null and options.ocr_bbox == null and options.ocr_failure_stage == null and options.ocr_failure_retryable == null and !options.transcript_used and options.transcript_confidence == null and options.extraction_warning == null) return;

    var format = std.json.ObjectMap.empty;
    errdefer {
        var it = format.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        format.deinit(alloc);
    }

    try putString(alloc, &format, "schema", "antfly.document_format_provenance.v1");
    try putString(alloc, &format, "coordinate_system", "source_page_points");
    try putString(alloc, &format, "extraction_method", options.extraction_method orelse "mechanical_text");
    if (options.extraction_status) |value| try putString(alloc, &format, "extraction_status", value);
    if (options.confidence) |value| try format.put(alloc, try alloc.dupe(u8, "confidence"), .{ .float = value });
    if (options.native_text_quality) |value| try putStructJsonValue(alloc, &format, "native_text_quality", value);
    if (options.native_text_diagnostics) |value| try putStructJsonValue(alloc, &format, "native_text_diagnostics", value);
    try format.put(alloc, try alloc.dupe(u8, "ocr_used"), .{ .bool = options.ocr_used });
    try format.put(alloc, try alloc.dupe(u8, "ocr_attempted"), .{ .bool = options.ocr_attempted });
    if (options.ocr_render_dpi) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_render_dpi"), .{ .integer = value });
    if (options.ocr_render_policy) |value| try putStructJsonValue(alloc, &format, "ocr_render_policy", value);
    if (options.ocr_effective_render_dpi) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_effective_render_dpi"), .{ .integer = value });
    if (options.ocr_rendered_width) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_rendered_width"), .{ .integer = value });
    if (options.ocr_rendered_height) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_rendered_height"), .{ .integer = value });
    if (options.ocr_rendered_bytes) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_rendered_bytes"), .{ .integer = @intCast(value) });
    if (options.ocr_failure_stage) |value| try putString(alloc, &format, "ocr_failure_stage", value);
    if (options.ocr_failure_retryable) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_failure_retryable"), .{ .bool = value });
    if (options.ocr_trigger_reasons) |value| try putString(alloc, &format, "ocr_trigger_reasons", value);
    if (options.ocr_embedded_quality) |value| try putString(alloc, &format, "ocr_embedded_quality", value);
    if (options.ocr_output_quality) |value| try putString(alloc, &format, "ocr_output_quality", value);
    if (options.ocr_confidence) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_confidence"), .{ .float = value });
    if (options.ocr_bbox) |bbox| try format.put(alloc, try alloc.dupe(u8, "ocr_bbox"), .{ .array = try jsonFloatArrayAlloc(alloc, &bbox) });
    try format.put(alloc, try alloc.dupe(u8, "transcript_used"), .{ .bool = options.transcript_used });
    if (options.transcript_confidence) |value| try format.put(alloc, try alloc.dupe(u8, "transcript_confidence"), .{ .float = value });
    if (options.extraction_warning) |value| try putString(alloc, &format, "extraction_warning", value);
    if (options.page_number) |value| try format.put(alloc, try alloc.dupe(u8, "page_number"), .{ .integer = value });
    if (options.page_label) |value| try putString(alloc, &format, "page_label", value);
    if (options.page_bbox) |bbox| try format.put(alloc, try alloc.dupe(u8, "page_bbox"), .{ .array = try jsonFloatArrayAlloc(alloc, &bbox) });
    if (options.page_rotation) |value| try format.put(alloc, try alloc.dupe(u8, "page_rotation"), .{ .integer = value });

    try provenance.put(alloc, try alloc.dupe(u8, "format_provenance"), .{ .object = format });
}

fn putStructJsonValue(alloc: Allocator, object: *std.json.ObjectMap, key: []const u8, value: anytype) !void {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const encoded = try structJsonValueAlloc(alloc, value);
    errdefer freeJsonValue(alloc, encoded);
    try object.put(alloc, owned_key, encoded);
}

fn provenanceScalarJsonValueAlloc(alloc: Allocator, value: anytype) !std.json.Value {
    return switch (@typeInfo(@TypeOf(value))) {
        .optional => if (value) |item| try provenanceScalarJsonValueAlloc(alloc, item) else .null,
        .bool => .{ .bool = value },
        .int => .{ .integer = std.math.cast(i64, value) orelse return error.Overflow },
        .float => .{ .float = value },
        .pointer => |pointer| blk: {
            if (pointer.size != .slice or pointer.child != u8) @compileError("unsupported provenance field");
            break :blk .{ .string = try alloc.dupe(u8, value) };
        },
        else => @compileError("unsupported provenance field"),
    };
}

fn structJsonValueAlloc(alloc: Allocator, value: anytype) !std.json.Value {
    const T = @TypeOf(value);
    const info = switch (@typeInfo(T)) {
        .@"struct" => |struct_info| struct_info,
        else => @compileError("expected provenance struct"),
    };
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        object.deinit(alloc);
    }
    inline for (info.field_names) |field_name| {
        const encoded = try provenanceScalarJsonValueAlloc(alloc, @field(value, field_name));
        errdefer freeJsonValue(alloc, encoded);
        const key = try alloc.dupe(u8, field_name);
        errdefer alloc.free(key);
        try object.put(alloc, key, encoded);
    }
    return .{ .object = object };
}

fn putString(alloc: Allocator, obj: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try putOwnedValue(alloc, obj, key, .{ .string = try alloc.dupe(u8, value) });
}

fn putOwnedValue(alloc: Allocator, object: *std.json.ObjectMap, key: []const u8, value: std.json.Value) !void {
    errdefer freeJsonValue(alloc, value);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try object.put(alloc, owned_key, value);
}

fn jsonFloatArrayAlloc(alloc: Allocator, values: []const f64) !std.json.Array {
    var array = std.json.Array.init(alloc);
    errdefer array.deinit();
    for (values) |value| try array.append(.{ .float = value });
    return array;
}

fn base64EncodeAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

test "artifact text resolves canonical selector through stored source field" {
    const alloc = std.testing.allocator;
    const payload =
        \\{"_source_field":"body","body":"chunk payload"}
    ;

    const original = (try artifactTextAlloc(alloc, payload, "body")) orelse return error.TestUnexpectedResult;
    defer alloc.free(original);
    try std.testing.expectEqualStrings("chunk payload", original);

    const canonical = (try artifactTextAlloc(alloc, payload, "text")) orelse return error.TestUnexpectedResult;
    defer alloc.free(canonical);
    try std.testing.expectEqualStrings("chunk payload", canonical);

    try std.testing.expect((try artifactTextAlloc(alloc, payload, "typo")) == null);
}

test "append artifact fields stores text offsets and payload" {
    const alloc = std.testing.allocator;
    var obj = std.json.ObjectMap.empty;
    defer {
        var it = obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        obj.deinit(alloc);
    }

    const chunk = Chunk{
        .chunk_id = 3,
        .text = try alloc.dupe(u8, "hello"),
        .start_offset = 7,
        .end_offset = 12,
    };
    defer {
        var mutable = chunk;
        mutable.deinit(alloc);
    }

    try appendArtifactFields(alloc, &obj, "body", chunk, true);
    try std.testing.expectEqual(@as(i64, 3), obj.get("_chunk_id").?.integer);
    try std.testing.expectEqualStrings("text/plain", obj.get("_mime_type").?.string);
    try std.testing.expectEqual(@as(i64, 7), obj.get("_start_offset").?.integer);
    try std.testing.expectEqualStrings("hello", obj.get("body").?.string);
    const provenance = obj.get("provenance").?.object;
    try std.testing.expectEqualStrings("document", provenance.get("offset_basis").?.string);
    try std.testing.expectEqualStrings("body", provenance.get("source_field").?.string);
    try std.testing.expectEqual(@as(i64, 7), provenance.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 12), provenance.get("char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 7), provenance.get("document_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 12), provenance.get("document_char_end").?.integer);
}

test "complete text offset pair only returns complete valid source spans" {
    try std.testing.expectEqual(OffsetPair{ .start = null, .end = null }, completeTextOffsetPair(0, null, 5));
    try std.testing.expectEqual(OffsetPair{ .start = null, .end = null }, completeTextOffsetPair(null, 5, 5));
    try std.testing.expectEqual(OffsetPair{ .start = null, .end = null }, completeTextOffsetPair(6, 5, 10));
    try std.testing.expectEqual(OffsetPair{ .start = null, .end = null }, completeTextOffsetPair(0, 6, 5));
    try std.testing.expectEqual(OffsetPair{ .start = 0, .end = 5 }, completeTextOffsetPair(0, 5, 5));
}

test "append artifact fields stores unit-local and document-global provenance" {
    const alloc = std.testing.allocator;
    var obj = std.json.ObjectMap.empty;
    defer {
        var it = obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        obj.deinit(alloc);
    }

    const chunk = Chunk{
        .chunk_id = 2,
        .text = try alloc.dupe(u8, "world"),
        .start_offset = 5,
        .end_offset = 10,
    };
    defer {
        var mutable = chunk;
        mutable.deinit(alloc);
    }

    try appendArtifactFieldsWithProvenance(alloc, &obj, "text", chunk, true, .{
        .scope = .unit,
        .parent_doc_key = "doc:a",
        .parent_unit_key = "doc:a/document_units_v1/page:000001",
        .parent_unit_id = "page:000001",
        .source_artifact_name = "document_units_v1",
        .document_char_base = 100,
        .page_number = 1,
        .page_label = "i",
        .page_bbox = .{ 0, 0, 612, 792 },
        .page_rotation = 90,
        .extraction_method = "pdf_text",
        .confidence = 0.94,
        .native_text_quality = .{
            .too_short = false,
            .garbled = false,
            .font_corrupted = false,
            .replacement_corrupted = false,
            .single_char_word_ratio = 0.1,
            .corrupted_line_ratio = 0,
            .replacement_char_ratio = 0,
            .trimmed_len = 5,
            .non_whitespace_chars = 5,
        },
        .native_text_diagnostics = .{
            .policy = "page",
            .extraction_complete = true,
            .semantic_coverage = "partial",
            .actual_text_replacements = 1,
            .outside_page_runs = 2,
            .invisible_runs = 3,
            .unaligned_runs = 1,
            .unsupported_ordering_runs = 4,
            .source_run_count = 5,
            .emitted_run_count = 4,
            .outline_fallback = false,
            .has_nontext_visual = true,
            .has_unrendered_annotations = false,
        },
    });
    const provenance = obj.get("provenance").?.object;
    try std.testing.expectEqualStrings("unit", provenance.get("offset_basis").?.string);
    try std.testing.expectEqualStrings("doc:a", provenance.get("parent_doc_key").?.string);
    try std.testing.expectEqualStrings("page:000001", provenance.get("parent_unit_id").?.string);
    try std.testing.expectEqualStrings("document_units_v1", provenance.get("source_artifact_name").?.string);
    try std.testing.expectEqual(@as(i64, 1), provenance.get("page_number").?.integer);
    try std.testing.expectEqualStrings("i", provenance.get("page_label").?.string);
    const page_bbox = provenance.get("page_bbox").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), page_bbox.len);
    try std.testing.expectEqual(@as(f64, 612), page_bbox[2].float);
    try std.testing.expectEqual(@as(i64, 90), provenance.get("page_rotation").?.integer);
    const format_provenance = provenance.get("format_provenance").?.object;
    try std.testing.expectEqualStrings("antfly.document_format_provenance.v1", format_provenance.get("schema").?.string);
    try std.testing.expectEqualStrings("source_page_points", format_provenance.get("coordinate_system").?.string);
    try std.testing.expectEqualStrings("pdf_text", format_provenance.get("extraction_method").?.string);
    try std.testing.expectEqual(@as(f64, 0.94), provenance.get("confidence").?.float);
    try std.testing.expectEqual(@as(f64, 0.94), format_provenance.get("confidence").?.float);
    try std.testing.expectEqualStrings("page", provenance.get("native_text_diagnostics").?.object.get("policy").?.string);
    try std.testing.expectEqual(@as(i64, 2), format_provenance.get("native_text_diagnostics").?.object.get("outside_page_runs").?.integer);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("native_text_quality").?.object.get("trimmed_len").?.integer);
    try std.testing.expect(!format_provenance.get("ocr_used").?.bool);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("unit_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 10), provenance.get("unit_char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 105), provenance.get("document_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 110), provenance.get("document_char_end").?.integer);
}

fn exerciseNativeTextProvenanceOwnership(alloc: Allocator) !void {
    var object = std.json.ObjectMap.empty;
    defer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        object.deinit(alloc);
    }
    try putStructJsonValue(alloc, &object, "native_text_quality", NativeTextQualityProvenance{
        .too_short = false,
        .garbled = false,
        .font_corrupted = false,
        .replacement_corrupted = false,
        .single_char_word_ratio = 0.1,
        .corrupted_line_ratio = 0,
        .replacement_char_ratio = 0,
        .trimmed_len = 5,
        .non_whitespace_chars = 5,
    });
    try putStructJsonValue(alloc, &object, "native_text_diagnostics", NativeTextDiagnosticsProvenance{
        .policy = "visible",
        .extraction_complete = true,
        .semantic_coverage = "text_only",
        .actual_text_replacements = 2,
        .outside_page_runs = 1,
        .invisible_runs = 3,
        .unaligned_runs = 0,
        .unsupported_ordering_runs = 0,
        .source_run_count = 6,
        .emitted_run_count = 5,
        .outside_page_spans = 2,
        .invisible_spans = 3,
        .unpositioned_spans = 4,
        .outline_fallback = false,
        .has_nontext_visual = false,
        .has_unrendered_annotations = false,
    });
}

test "native text neutral provenance owns nested JSON through allocation failure" {
    try exerciseNativeTextProvenanceOwnership(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseNativeTextProvenanceOwnership, .{});
}

test "append artifact fields stores binary metadata and base64 payload" {
    const alloc = std.testing.allocator;
    var obj = std.json.ObjectMap.empty;
    defer {
        var it = obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr.*);
        }
        obj.deinit(alloc);
    }

    const chunk = Chunk{
        .chunk_id = 1,
        .mime_type = "image/png",
        .data = try alloc.dupe(u8, &.{ 1, 2, 3 }),
        .frame_index = 0,
        .frame_delay_ms = 50,
    };
    defer {
        var mutable = chunk;
        mutable.deinit(alloc);
    }

    try appendArtifactFields(alloc, &obj, "body", chunk, true);
    try std.testing.expectEqualStrings("image/png", obj.get("_mime_type").?.string);
    try std.testing.expectEqual(@as(i64, 0), obj.get("_frame_index").?.integer);
    try std.testing.expectEqualStrings("AQID", obj.get("_data").?.string);
}

fn freeJsonValue(alloc: Allocator, value: std.json.Value) void {
    switch (value) {
        .string => |s| alloc.free(s),
        .array => |arr| {
            for (arr.items) |item| freeJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |obj| {
            var mutable = obj;
            var it = mutable.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                freeJsonValue(alloc, entry.value_ptr.*);
            }
            mutable.deinit(alloc);
        },
        else => {},
    }
}

fn cloneJsonValue(a: Allocator, value: std.json.Value) Allocator.Error!std.json.Value {
    return switch (value) {
        .string => |text| .{ .string = try a.dupe(u8, text) },
        .array => |array| blk: {
            var copy = std.json.Array.init(a);
            errdefer freeJsonValue(a, .{ .array = copy });
            for (array.items) |item| {
                const owned = try cloneJsonValue(a, item);
                errdefer freeJsonValue(a, owned);
                try copy.append(owned);
            }
            break :blk .{ .array = copy };
        },
        .object => |object| blk: {
            var copy = std.json.ObjectMap.empty;
            errdefer freeJsonValue(a, .{ .object = copy });
            var it = object.iterator();
            while (it.next()) |item| try putClonedJsonValue(a, &copy, item.key_ptr.*, item.value_ptr.*);
            break :blk .{ .object = copy };
        },
        else => value,
    };
}

fn putClonedJsonValue(a: Allocator, obj: *std.json.ObjectMap, key: []const u8, value: std.json.Value) !void {
    const owned_key = try a.dupe(u8, key);
    errdefer a.free(owned_key);
    const owned_value = try cloneJsonValue(a, value);
    errdefer freeJsonValue(a, owned_value);
    try obj.put(a, owned_key, owned_value);
}

/// An owned typed replay recipe; payload ownership stays in the source artifact.
pub fn modelMetadataAlloc(a: Allocator, payload: []const u8) !?*@import("inference_chunker").types.ModelMetadata {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, payload, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const raw = parsed.value.object.get("_chunk") orelse return null;
    var metadata = try std.json.parseFromValue(@import("inference_chunker").types.ModelMetadata, a, raw, .{ .ignore_unknown_fields = true });
    defer metadata.deinit();
    // Artifact recipes retain output identities under `outputs`, separately
    // from payloads. Restore those receipts for independent vector consumers.
    if (raw == .object) if (raw.object.get("outputs")) |outputs| {
        metadata.value.outputs_json = try std.json.Stringify.valueAlloc(metadata.arena.allocator(), outputs, .{});
    };
    return try metadata.value.clone(a);
}

/// A pure-computation key: it carries no publication or source-read authority.
pub fn modelMemoKey(a: Allocator, metadata: @import("inference_chunker").types.ModelMetadata) ![32]u8 {
    var recipe = metadata;
    recipe.embedding = null;
    recipe.embedding_error = null;
    recipe.outputs_json = null;
    const bytes = try std.json.Stringify.valueAlloc(a, recipe, .{ .emit_null_optional_fields = false });
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-model-chunk-vector-v1");
    hash.update(bytes);
    return hash.finalResult();
}

test "model chunk artifact retains reconstruction coordinates independently of vectors" {
    const a = std.testing.allocator;
    const M = @import("inference_chunker").types.ModelMetadata;
    const metadata = M{ .descriptor = .{ .ordinal = 3, .coordinates = .{ .kind = .video, .start_ns = 30_000_000_000, .end_ns = 60_000_000_000, .selected_pictures = &.{ 900, 930 }, .picture_basis = .logical }, .input_tokens = 282 }, .inference_id = "plan:3", .source_fingerprint = "immutable-source", .source_version = "v1", .plan_id = "plan", .model = "model", .model_identity = "weights-v1", .model_revision = "recipe-v1", .task_type = "RETRIEVAL_DOCUMENT", .dimensions = 2, .policy = .{}, .embedding = &.{ 0.6, 0.8 } };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var object: std.json.ObjectMap = .empty;
    var chunk = Chunk{ .chunk_id = 3, .mime_type = "video/mp4", .metadata = try metadata.clone(a) };
    defer chunk.deinit(a);
    try appendArtifactFields(arena.allocator(), &object, "body", chunk, false);
    const encoded = try std.json.Stringify.valueAlloc(a, std.json.Value{ .object = object }, .{});
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "embedding") == null);
    const replay = (try modelMetadataAlloc(a, encoded)).?;
    defer replay.deinit(a);
    try std.testing.expectEqualSlices(u64, &.{ 900, 930 }, replay.descriptor.coordinates.selected_pictures.?);
    try std.testing.expectEqual(@as(?[]const f32, null), replay.embedding);
    try std.testing.expectEqualSlices(u8, &try modelMemoKey(a, metadata), &try modelMemoKey(a, replay.*));
    var independent = metadata;
    independent.outputs_json = "changed independent transcription receipt";
    independent.embedding_error = "repaired primary attempt";
    try std.testing.expectEqualSlices(u8, &try modelMemoKey(a, metadata), &try modelMemoKey(a, independent));
    var changed = replay.*;
    changed.model_identity = "weights-v2";
    try std.testing.expect(!std.mem.eql(u8, &try modelMemoKey(a, replay.*), &try modelMemoKey(a, changed)));
}

/// A search hit can name a consumer window while its source remains the
/// immutable parent chunk. Project exact receipt clocks without inventing a
/// new plan or persisting duplicate media/chunk records.
pub fn mediaWindowPayloadAlloc(a: Allocator, payload: []const u8, index: u32) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var object = try std.json.parseFromSliceLeaky(std.json.Value, scratch, payload, .{});
    const metadata = try modelMetadataAlloc(scratch, payload) orelse return error.InvalidChunkSource;
    const values = try std.json.parseFromSliceLeaky([]@import("inference_chunker").outputs.Result, scratch, metadata.outputs_json orelse return error.InvalidChunkSource, .{});
    const selected = for (values) |value| {
        if (value.kind == .embedding and std.mem.eql(u8, value.name, "embedding") and value.sub_index == index and value.status == .complete) break value;
    } else return error.InvalidChunkSource;
    var recipe = object.object.get("_chunk") orelse return error.InvalidChunkSource;
    try recipe.object.put(scratch, "output_sub_index", .{ .integer = index });
    try recipe.object.put(scratch, "output_windowed", .{ .bool = true });
    const encoded = try std.json.Stringify.valueAlloc(scratch, selected, .{ .emit_null_optional_fields = false });
    const receipt = try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded, .{});
    var outputs = std.ArrayList(std.json.Value).empty;
    try outputs.append(scratch, receipt);
    try recipe.object.put(scratch, "outputs", .{ .array = outputs.toManaged(scratch) });
    _ = recipe.object.swapRemove("outputs_json");
    var descriptor = recipe.object.get("descriptor") orelse return error.InvalidChunkSource;
    var coordinates = descriptor.object.get("coordinates") orelse return error.InvalidChunkSource;
    if (selected.start_ns) |start| {
        try coordinates.object.put(scratch, "start_ns", .{ .integer = start });
        try object.object.put(scratch, "start_time_ms", .{ .float = @as(f64, @floatFromInt(start)) / std.time.ns_per_ms });
    }
    if (selected.end_ns) |end| {
        try coordinates.object.put(scratch, "end_ns", .{ .integer = end });
        try object.object.put(scratch, "end_time_ms", .{ .float = @as(f64, @floatFromInt(end)) / std.time.ns_per_ms });
    }
    try descriptor.object.put(scratch, "coordinates", coordinates);
    try recipe.object.put(scratch, "descriptor", descriptor);
    try object.object.put(scratch, "_chunk", recipe);
    return std.json.Stringify.valueAlloc(a, object, .{});
}

// Unit spans are sorted and disjoint in byte coordinates. Retain only segments
// overlapping this chunk: O(log S + K), rather than copying an entire recording's
// timing inventory into every chunk. Coordinates remain relative to the unit.
pub fn appendTranscriptSpans(alloc: Allocator, object: *std.json.ObjectMap, spans: anytype, chunk: Chunk) !void {
    if (spans.len == 0) return;
    const start = chunk.start_offset orelse return;
    const end = chunk.end_offset orelse return;
    if (end <= start) return;
    var low: usize = 0;
    var high: usize = spans.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (spans[mid].char_end <= start) low = mid + 1 else high = mid;
    }
    var stop = low;
    while (stop < spans.len and spans[stop].char_start < end) : (stop += 1) {}
    if (stop == low) return;
    const provenance = object.getPtr("provenance") orelse return error.InvalidChunkSource;
    if (provenance.* != .object) return error.InvalidChunkSource;
    var selected = std.json.Array.init(alloc);
    errdefer {
        for (selected.items) |item| freeJsonValue(alloc, item);
        selected.deinit();
    }
    for (spans[low..stop]) |span| {
        var item = std.json.ObjectMap.empty;
        errdefer freeJsonValue(alloc, .{ .object = item });
        inline for (.{ "char_start", "char_end", "start_ms", "end_ms" }) |field| {
            const integer = std.math.cast(i64, @field(span, field)) orelse return error.Overflow;
            try putOwnedValue(alloc, &item, field, .{ .integer = integer });
        }
        if (span.speaker_index) |speaker| {
            try putOwnedValue(alloc, &item, "speaker_index", .{ .integer = speaker });
            var label_buf: [16]u8 = undefined;
            const label = std.fmt.bufPrint(&label_buf, "SPEAKER_{d:0>2}", .{speaker}) catch unreachable;
            try putString(alloc, &item, "speaker", label);
        }
        try selected.append(.{ .object = item });
    }
    const key = try alloc.dupe(u8, "transcript_spans");
    errdefer alloc.free(key);
    try provenance.object.put(alloc, key, .{ .array = selected });
}

test "transcript chunk public provenance retains bounded timing and speakers" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const Span = struct { char_start: u32, char_end: u32, start_ms: u64, end_ms: u64, speaker_index: ?u8 = null };
    const spans = [_]Span{
        .{ .char_start = 0, .char_end = 5, .start_ms = 100, .end_ms = 500, .speaker_index = 0 },
        .{ .char_start = 6, .char_end = 12, .start_ms = 600, .end_ms = 900, .speaker_index = 1 },
        .{ .char_start = 13, .char_end = 18, .start_ms = 1000, .end_ms = 1500, .speaker_index = 0 },
    };
    var object = std.json.ObjectMap.empty;
    const chunk = Chunk{ .chunk_id = 0, .text = null, .start_offset = 3, .end_offset = 12, .start_time_ms = 100, .end_time_ms = 900 };
    try appendArtifactFieldsWithProvenance(scratch, &object, "text", chunk, true, .{ .scope = .unit });
    try appendTranscriptSpans(scratch, &object, &spans, chunk);
    const provenance = object.get("provenance").?.object;
    try std.testing.expectEqual(@as(f64, 100), provenance.get("start_time_ms").?.float);
    const selected = provenance.get("transcript_spans").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), selected.len);
    try std.testing.expectEqual(@as(i64, 1), selected[1].object.get("speaker_index").?.integer);
    try std.testing.expect(provenance.get("speaker") == null);
    var single = std.json.ObjectMap.empty;
    try appendArtifactFieldsWithProvenance(scratch, &single, "text", .{ .chunk_id = 1, .speaker_index = 1 }, false, .{});
    try std.testing.expectEqualStrings("SPEAKER_01", single.get("provenance").?.object.get("speaker").?.string);
}

test "transcript chunk public provenance releases partial span allocations" {
    const Probe = struct {
        fn build(alloc: Allocator) !void {
            var object = std.json.ObjectMap.empty;
            defer freeJsonValue(alloc, .{ .object = object });
            try putOwnedValue(alloc, &object, "provenance", .{ .object = std.json.ObjectMap.empty });
            const Span = struct { char_start: u32, char_end: u32, start_ms: u64, end_ms: u64, speaker_index: ?u8 = null };
            const spans = [_]Span{.{ .char_start = 0, .char_end = 5, .start_ms = 100, .end_ms = 500, .speaker_index = 0 }};
            try appendTranscriptSpans(alloc, &object, &spans, .{ .chunk_id = 0, .start_offset = 0, .end_offset = 5 });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.build, .{});
}
