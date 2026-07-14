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
    frame_index: ?u32 = null,
    frame_delay_ms: ?u32 = null,

    pub fn deinit(self: *Chunk, alloc: Allocator) void {
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
    ocr_used: bool = false,
    ocr_attempted: bool = false,
    ocr_render_dpi: ?u16 = null,
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
    if (chunk.frame_index) |value| try obj.put(alloc, try alloc.dupe(u8, "_frame_index"), .{ .integer = value });
    if (chunk.frame_delay_ms) |value| try obj.put(alloc, try alloc.dupe(u8, "_frame_delay_ms"), .{ .integer = value });
    try appendProvenanceFields(alloc, obj, source_field, chunk, provenance);

    if (!include_payload) return;

    if (chunk.text) |text| {
        try obj.put(alloc, try alloc.dupe(u8, source_field), .{ .string = try alloc.dupe(u8, text) });
    } else if (chunk.data) |data| {
        const encoded = try base64EncodeAlloc(alloc, data);
        errdefer alloc.free(encoded);
        try obj.put(alloc, try alloc.dupe(u8, "_data"), .{ .string = encoded });
    }
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
    try provenance.put(alloc, try alloc.dupe(u8, "ocr_used"), .{ .bool = options.ocr_used });
    try provenance.put(alloc, try alloc.dupe(u8, "ocr_attempted"), .{ .bool = options.ocr_attempted });
    if (options.ocr_render_dpi) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_render_dpi"), .{ .integer = value });
    if (options.ocr_trigger_reasons) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_trigger_reasons"), .{ .string = try alloc.dupe(u8, value) });
    if (options.ocr_embedded_quality) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_embedded_quality"), .{ .string = try alloc.dupe(u8, value) });
    if (options.ocr_output_quality) |value| try provenance.put(alloc, try alloc.dupe(u8, "ocr_output_quality"), .{ .string = try alloc.dupe(u8, value) });
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
    if (options.page_number == null and options.page_label == null and options.page_bbox == null and options.page_rotation == null and options.extraction_status == null and options.confidence == null and !options.ocr_used and !options.ocr_attempted and options.ocr_confidence == null and options.ocr_bbox == null and !options.transcript_used and options.transcript_confidence == null and options.extraction_warning == null) return;

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
    try format.put(alloc, try alloc.dupe(u8, "ocr_used"), .{ .bool = options.ocr_used });
    try format.put(alloc, try alloc.dupe(u8, "ocr_attempted"), .{ .bool = options.ocr_attempted });
    if (options.ocr_render_dpi) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_render_dpi"), .{ .integer = value });
    if (options.ocr_trigger_reasons) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_trigger_reasons"), .{ .string = try alloc.dupe(u8, value) });
    if (options.ocr_embedded_quality) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_embedded_quality"), .{ .string = try alloc.dupe(u8, value) });
    if (options.ocr_output_quality) |value| try format.put(alloc, try alloc.dupe(u8, "ocr_output_quality"), .{ .string = try alloc.dupe(u8, value) });
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

fn putString(alloc: Allocator, obj: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try obj.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
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
    try std.testing.expect(!format_provenance.get("ocr_used").?.bool);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("unit_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 10), provenance.get("unit_char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 105), provenance.get("document_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 110), provenance.get("document_char_end").?.integer);
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
