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

//! Typed persisted extraction units. Ordered consumers parse once, bind the
//! envelope identity, and verify the logical fingerprint before provider work.
const std = @import("std");
const extraction = @import("document_extraction.zig");
const fingerprints = @import("document_unit_fingerprint.zig");
const Allocator = std.mem.Allocator;

pub const Route = struct {
    range_id: []const u8,
    route_status: []const u8 = "local_committed",
    owner_group_id: u64 = 0,

    pub fn validate(self: Route) !void {
        if (self.range_id.len == 0 or self.owner_group_id > std.math.maxInt(i64)) return error.InvalidDocumentExtractionManifest;
        if (std.mem.eql(u8, self.route_status, "local_committed")) {
            if (self.owner_group_id != 0) return error.InvalidDocumentExtractionManifest;
        } else if (std.mem.eql(u8, self.route_status, "remote_committed")) {
            if (self.owner_group_id == 0) return error.InvalidDocumentExtractionManifest;
        } else return error.InvalidDocumentExtractionManifest;
    }
};
pub const Identity = struct {
    document: []const u8,
    producer: []const u8,
    unit: []const u8,
    fingerprint: ?[]const u8 = null,
};
pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    unit: extraction.Unit,
    fingerprint: []const u8,
    route: Route,
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn string(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidDocumentExtractionManifest;
    if (value != .string) return error.InvalidDocumentExtractionManifest;
    return value.string;
}
fn decodeField(comptime T: type, alloc: Allocator, value: std.json.Value) !T {
    return std.json.parseFromValueLeaky(T, alloc, value, .{ .ignore_unknown_fields = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDocumentExtractionManifest,
    };
}
/// Compare public metadata mirrors without allocating another typed value.
/// The payload JSON is parsed once, including potentially large region arrays.
fn matches(comptime T: type, expected: T, actual: std.json.Value) bool {
    return switch (@typeInfo(T)) {
        .optional => |optional| if (expected) |value| actual != .null and matches(optional.child, value, actual) else actual == .null,
        .bool => actual == .bool and expected == actual.bool,
        .int => switch (actual) {
            .integer => |value| expected == (std.math.cast(T, value) orelse return false),
            .number_string => |value| expected == (std.fmt.parseInt(T, value, 10) catch return false),
            else => false,
        },
        .float => switch (actual) {
            .integer => |value| expected == @as(T, @floatFromInt(value)),
            .float => |value| expected == value,
            .number_string => |value| expected == (std.fmt.parseFloat(T, value) catch return false),
            else => false,
        },
        .pointer => |pointer| blk: {
            if (pointer.size != .slice) @compileError("unit field must be a slice");
            if (pointer.child == u8) break :blk actual == .string and std.mem.eql(u8, expected, actual.string);
            if (actual != .array or expected.len != actual.array.items.len) break :blk false;
            for (expected, actual.array.items) |value, other| if (!matches(pointer.child, value, other)) break :blk false;
            break :blk true;
        },
        .array => |array| blk: {
            if (actual != .array or array.len != actual.array.items.len) break :blk false;
            for (expected, actual.array.items) |value, other| if (!matches(array.child, value, other)) break :blk false;
            break :blk true;
        },
        .@"struct" => |info| blk: {
            if (actual != .object) break :blk false;
            inline for (info.field_names, info.field_types) |reflected_name, field_type| {
                const item = actual.object.get(reflected_name) orelse break :blk false;
                if (!matches(field_type, @field(expected, reflected_name), item)) break :blk false;
            }
            break :blk true;
        },
        else => @compileError("unsupported unit metadata field"),
    };
}

pub fn decodeAlloc(alloc: Allocator, raw: []const u8, expected: Identity) !Owned {
    if (raw.len > @import("../artifact_publication.zig").max_payload_bytes) return error.ResourceBudgetExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const value = std.json.parseFromSliceLeaky(std.json.Value, owned, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDocumentExtractionManifest,
    };
    if (value != .object) return error.InvalidDocumentExtractionManifest;
    const object = value.object;
    if (!std.mem.eql(u8, try string(object, "_parent_doc_key"), expected.document) or
        !std.mem.eql(u8, try string(object, "_artifact_name"), expected.producer) or
        !std.mem.eql(u8, try string(object, "unit_id"), expected.unit) or expected.unit.len == 0 or
        !std.mem.eql(u8, try string(object, "_artifact_range_kind"), "unit") or
        !std.mem.eql(u8, try string(object, "content_type"), "text/plain")) return error.InvalidDocumentExtractionManifest;
    const provenance_value = object.get("provenance") orelse return error.InvalidDocumentExtractionManifest;
    if (provenance_value != .object) return error.InvalidDocumentExtractionManifest;
    const provenance = provenance_value.object;
    var unit: extraction.Unit = .{ .unit_id = undefined, .unit_type = undefined, .text = undefined, .method = undefined };
    inline for (@typeInfo(extraction.Unit).@"struct".field_names, @typeInfo(extraction.Unit).@"struct".field_types, @typeInfo(extraction.Unit).@"struct".field_attrs) |reflected_name, field_type, field_attrs| {
        const root = comptime std.mem.eql(u8, reflected_name, "unit_id") or std.mem.eql(u8, reflected_name, "unit_type") or std.mem.eql(u8, reflected_name, "text");
        if (root and provenance.contains(reflected_name)) return error.InvalidDocumentExtractionManifest;
        const selected = (if (root) object else provenance).get(reflected_name);
        if (selected) |entry| {
            if (comptime std.mem.eql(u8, reflected_name, "transcript_spans")) {
                // The persisted empty representation is null; Unit uses [].
                if (entry != .null) unit.transcript_spans = try decodeField(field_type, owned, entry);
            } else @field(unit, reflected_name) = try decodeField(field_type, owned, entry);
        } else if (comptime field_attrs.default_value_ptr == null) return error.InvalidDocumentExtractionManifest;
        if (!root) if (object.get(reflected_name)) |mirror| {
            if (!matches(field_type, @field(unit, reflected_name), mirror)) return error.InvalidDocumentExtractionManifest;
        };
    }
    if (unit.unit_type.len == 0 or unit.method.len == 0) return error.InvalidDocumentExtractionManifest;
    try validateCoordinates(unit);
    try validateFormatMirrors(object, provenance, unit);
    const fingerprint = try string(object, "_artifact_unit_fingerprint");
    const computed = try fingerprints.fingerprintAlloc(owned, unit);
    if (!std.mem.eql(u8, fingerprint, computed)) return error.InvalidDocumentExtractionManifest;
    if (expected.fingerprint) |declared| if (!std.mem.eql(u8, fingerprint, declared)) return error.InvalidDocumentExtractionManifest;
    const route: Route = .{
        .range_id = try string(object, "_artifact_range_id"),
        .route_status = try string(object, "_artifact_route_status"),
        .owner_group_id = try decodeField(u64, owned, object.get("_artifact_owner_group_id") orelse return error.InvalidDocumentExtractionManifest),
    };
    try route.validate();
    return .{ .arena = arena, .unit = unit, .fingerprint = fingerprint, .route = route };
}

fn validateFormatMirrors(object: std.json.ObjectMap, provenance: std.json.ObjectMap, unit: extraction.Unit) !void {
    const format_value = provenance.get("format_provenance") orelse return error.InvalidDocumentExtractionManifest;
    if (format_value != .object) return error.InvalidDocumentExtractionManifest;
    const format = format_value.object;
    if (!std.mem.eql(u8, try string(format, "schema"), "antfly.document_format_provenance.v1") or
        !std.mem.eql(u8, try string(format, "coordinate_system"), "source_page_points") or
        !std.mem.eql(u8, try string(format, "extraction_method"), unit.method) or
        !std.mem.eql(u8, try string(format, "source_content_type"), try string(provenance, "source_content_type"))) return error.InvalidDocumentExtractionManifest;
    _ = try string(provenance, "source_url");
    const confidence = unit.ocr_confidence orelse unit.transcript_confidence;
    for ([_]std.json.ObjectMap{ object, provenance, format }) |mirror| {
        if (!matches(?f64, confidence, mirror.get("confidence") orelse return error.InvalidDocumentExtractionManifest)) return error.InvalidDocumentExtractionManifest;
    }
    inline for (@typeInfo(extraction.Unit).@"struct".field_names, @typeInfo(extraction.Unit).@"struct".field_types) |reflected_name, field_type| {
        const excluded = comptime std.mem.eql(u8, reflected_name, "unit_id") or std.mem.eql(u8, reflected_name, "unit_type") or
            std.mem.eql(u8, reflected_name, "text") or std.mem.eql(u8, reflected_name, "method") or
            std.mem.eql(u8, reflected_name, "char_start") or std.mem.eql(u8, reflected_name, "char_end") or std.mem.eql(u8, reflected_name, "transcript_spans");
        if (!excluded) {
            if (!matches(field_type, @field(unit, reflected_name), format.get(reflected_name) orelse return error.InvalidDocumentExtractionManifest)) return error.InvalidDocumentExtractionManifest;
        }
    }
}

fn finiteBox(box: [4]f64) !void {
    for (box) |coordinate| if (!std.math.isFinite(coordinate)) return error.InvalidDocumentExtractionManifest;
}
fn validateCoordinates(unit: extraction.Unit) !void {
    if ((unit.char_start == null) != (unit.char_end == null)) return error.InvalidDocumentExtractionManifest;
    if (unit.char_start) |start| if (unit.char_end.? < start or unit.char_end.? - start != unit.text.len) return error.InvalidDocumentExtractionManifest;
    for (unit.transcript_spans) |span| {
        if (span.char_end < span.char_start or span.char_end > unit.text.len or span.end_ms < span.start_ms) return error.InvalidDocumentExtractionManifest;
    }
    for (unit.text_regions) |region| {
        if (region.span[1] < region.span[0] or region.span[1] > unit.text.len) return error.InvalidDocumentExtractionManifest;
        try finiteBox(region.bbox);
    }
    if (unit.ocr_bbox) |box| try finiteBox(box);
    if (unit.page_bbox) |box| try finiteBox(box);
    if (unit.ocr_confidence) |value| if (!std.math.isFinite(value)) return error.InvalidDocumentExtractionManifest;
    if (unit.transcript_confidence) |value| if (!std.math.isFinite(value)) return error.InvalidDocumentExtractionManifest;
}

pub fn encodeAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, unit: extraction.Unit, unit_fingerprint: []const u8, source_url: []const u8, content_type: []const u8, route: Route) ![]u8 {
    try route.validate();
    const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
    const confidence = unit.ocr_confidence orelse unit.transcript_confidence;
    return std.json.Stringify.valueAlloc(alloc, .{
        ._parent_doc_key = doc_key,
        ._artifact_name = artifact_name,
        ._artifact_range_id = route.range_id,
        ._artifact_range_kind = "unit",
        ._artifact_route_status = route.route_status,
        ._artifact_owner_group_id = owner_group_id,
        ._artifact_unit_fingerprint = unit_fingerprint,
        .unit_id = unit.unit_id,
        .unit_type = unit.unit_type,
        .text = unit.text,
        .content_type = "text/plain",
        .language = "",
        .source_path = unit.source_path,
        .extraction_status = unit.extraction_status,
        .source_sha256 = unit.source_sha256,
        .byte_length = unit.byte_length,
        .confidence = confidence,
        .ocr_attempted = unit.ocr_attempted,
        .ocr_render_dpi = unit.ocr_render_dpi,
        .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
        .ocr_rendered_width = unit.ocr_rendered_width,
        .ocr_rendered_height = unit.ocr_rendered_height,
        .ocr_rendered_bytes = unit.ocr_rendered_bytes,
        .ocr_failure_stage = unit.ocr_failure_stage,
        .ocr_failure_retryable = unit.ocr_failure_retryable,
        .ocr_trigger_reasons = unit.ocr_trigger_reasons,
        .ocr_embedded_quality = unit.ocr_embedded_quality,
        .ocr_output_quality = unit.ocr_output_quality,
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
        .provenance = .{
            .source_url = source_url,
            .source_path = unit.source_path,
            .method = unit.method,
            .extraction_status = unit.extraction_status,
            .source_sha256 = unit.source_sha256,
            .byte_length = unit.byte_length,
            .confidence = confidence,
            .ocr_used = unit.ocr_used,
            .ocr_attempted = unit.ocr_attempted,
            .ocr_render_dpi = unit.ocr_render_dpi,
            .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
            .ocr_rendered_width = unit.ocr_rendered_width,
            .ocr_rendered_height = unit.ocr_rendered_height,
            .ocr_rendered_bytes = unit.ocr_rendered_bytes,
            .ocr_failure_stage = unit.ocr_failure_stage,
            .ocr_failure_retryable = unit.ocr_failure_retryable,
            .ocr_trigger_reasons = unit.ocr_trigger_reasons,
            .ocr_embedded_quality = unit.ocr_embedded_quality,
            .ocr_output_quality = unit.ocr_output_quality,
            .ocr_confidence = unit.ocr_confidence,
            .ocr_bbox = unit.ocr_bbox,
            .transcript_used = unit.transcript_used,
            .transcript_confidence = unit.transcript_confidence,
            .transcript_spans = if (unit.transcript_spans.len > 0) unit.transcript_spans else null,
            .extraction_warning = unit.extraction_warning,
            .page_number = unit.page_number,
            .page_label = unit.page_label,
            .page_bbox = unit.page_bbox,
            .page_rotation = unit.page_rotation,
            .text_regions = unit.text_regions,
            .char_start = unit.char_start,
            .char_end = unit.char_end,
            .source_content_type = content_type,
            .format_provenance = .{
                .schema = "antfly.document_format_provenance.v1",
                .source_content_type = content_type,
                .source_path = unit.source_path,
                .coordinate_system = "source_page_points",
                .extraction_method = unit.method,
                .extraction_status = unit.extraction_status,
                .source_sha256 = unit.source_sha256,
                .byte_length = unit.byte_length,
                .confidence = confidence,
                .ocr_used = unit.ocr_used,
                .ocr_attempted = unit.ocr_attempted,
                .ocr_render_dpi = unit.ocr_render_dpi,
                .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
                .ocr_rendered_width = unit.ocr_rendered_width,
                .ocr_rendered_height = unit.ocr_rendered_height,
                .ocr_rendered_bytes = unit.ocr_rendered_bytes,
                .ocr_failure_stage = unit.ocr_failure_stage,
                .ocr_failure_retryable = unit.ocr_failure_retryable,
                .ocr_trigger_reasons = unit.ocr_trigger_reasons,
                .ocr_embedded_quality = unit.ocr_embedded_quality,
                .ocr_output_quality = unit.ocr_output_quality,
                .ocr_confidence = unit.ocr_confidence,
                .ocr_bbox = unit.ocr_bbox,
                .transcript_used = unit.transcript_used,
                .transcript_confidence = unit.transcript_confidence,
                .extraction_warning = unit.extraction_warning,
                .page_number = unit.page_number,
                .page_label = unit.page_label,
                .page_bbox = unit.page_bbox,
                .page_rotation = unit.page_rotation,
                .text_regions = unit.text_regions,
            },
        },
    }, .{});
}

test "ordered artifact inventory typed unit payload owns fields and authenticates identity metadata and fingerprint" {
    const alloc = std.testing.allocator;
    var spans = [_]extraction.TranscriptSpan{.{ .char_start = 0, .char_end = 5, .start_ms = 10, .end_ms = 20, .speaker_index = 2 }};
    var regions = [_]extraction.TextRegion{.{ .span = .{ 0, 5 }, .bbox = .{ 1, 2, 3, 4 } }};
    const unit: extraction.Unit = .{
        .unit_id = @constCast("unit\x00x"),
        .unit_type = @constCast("page"),
        .text = @constCast("hello"),
        .method = @constCast("text"),
        .source_path = @constCast("page/1"),
        .byte_length = 512,
        .page_number = 7,
        .page_rotation = 90,
        .ocr_attempted = true,
        .ocr_used = true,
        .ocr_render_dpi = 150,
        .ocr_confidence = 0.75,
        .ocr_bbox = .{ 0, 1, 4, 8 },
        .transcript_used = true,
        .transcript_confidence = 0.5,
        .transcript_spans = &spans,
        .page_bbox = .{ 0, 0, 4, 8 },
        .text_regions = &regions,
        .char_start = 30,
        .char_end = 35,
    };
    const identity: Identity = .{ .document = "doc", .producer = "units", .unit = unit.unit_id };
    const fingerprint = try fingerprints.fingerprintAlloc(alloc, unit);
    defer alloc.free(fingerprint);
    const raw = try encodeAlloc(alloc, identity.document, identity.producer, unit, fingerprint, "input", "text/plain", .{ .range_id = "range:0", .route_status = "remote_committed", .owner_group_id = 9 });
    defer alloc.free(raw);
    var parsed = try decodeAlloc(alloc, raw, identity);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(unit, parsed.unit);
    try std.testing.expectEqualStrings(fingerprint, parsed.fingerprint);
    try std.testing.expectEqual(@as(u64, 9), parsed.route.owner_group_id);
    const Check = struct {
        fn run(a: Allocator, original: extraction.Unit, expected: Identity) !void {
            var result = parse: {
                const fp = try fingerprints.fingerprintAlloc(a, original);
                defer a.free(fp);
                const bytes = try encodeAlloc(a, expected.document, expected.producer, original, fp, "input", "text/plain", .{ .range_id = "range:0" });
                defer a.free(bytes);
                break :parse try decodeAlloc(a, bytes, expected);
            };
            defer result.deinit();
            // Every typed slice survives releasing the raw JSON and digest.
            try std.testing.expectEqualDeep(original, result.unit);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ unit, identity });
    var wrong = identity;
    wrong.document = "other";
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, decodeAlloc(alloc, raw, wrong));
    wrong = identity;
    wrong.producer = "other";
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, decodeAlloc(alloc, raw, wrong));
    wrong = identity;
    wrong.unit = "other";
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, decodeAlloc(alloc, raw, wrong));
    wrong = identity;
    wrong.fingerprint = "other";
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, decodeAlloc(alloc, raw, wrong));
    for (0..12) |variant| {
        var json = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
        defer json.deinit();
        const owned = json.arena.allocator();
        switch (variant) {
            0 => try json.value.object.put(owned, "text", .{ .string = "jello" }),
            1 => try json.value.object.getPtr("provenance").?.object.put(owned, "text", .{ .string = "hello" }),
            2 => try json.value.object.put(owned, "byte_length", .{ .integer = 7 }),
            3 => try json.value.object.getPtr("provenance").?.object.put(owned, "method", .{ .string = "changed" }),
            4 => try json.value.object.put(owned, "_artifact_owner_group_id", .{ .integer = -1 }),
            5 => try json.value.object.put(owned, "_artifact_range_kind", .{ .string = "chunk" }),
            6 => try json.value.object.put(owned, "_artifact_unit_fingerprint", .{ .string = "unversioned" }),
            7 => try json.value.object.getPtr("provenance").?.object.getPtr("format_provenance").?.object.put(owned, "page_number", .{ .integer = 8 }),
            8 => try json.value.object.put(owned, "confidence", .{ .float = 0.1 }),
            9 => try json.value.object.put(owned, "_artifact_owner_group_id", .{ .integer = 0 }),
            10 => try json.value.object.put(owned, "_artifact_route_status", .{ .string = "local_committed" }),
            11 => try json.value.object.put(owned, "_artifact_route_status", .{ .string = "transferring" }),
            else => unreachable,
        }
        const invalid = try std.json.Stringify.valueAlloc(alloc, json.value, .{});
        defer alloc.free(invalid);
        try std.testing.expectError(error.InvalidDocumentExtractionManifest, decodeAlloc(alloc, invalid, identity));
    }
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, encodeAlloc(alloc, identity.document, identity.producer, unit, fingerprint, "input", "text/plain", .{ .range_id = "range:0", .owner_group_id = 9 }));
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, encodeAlloc(alloc, identity.document, identity.producer, unit, fingerprint, "input", "text/plain", .{ .range_id = "range:0", .route_status = "remote_committed" }));
}

test "ordered artifact inventory typed unit payload validates offsets before accepting a matching fingerprint" {
    const alloc = std.testing.allocator;
    const good: extraction.Unit = .{ .unit_id = @constCast("unit"), .unit_type = @constCast("text"), .text = @constCast("hello"), .method = @constCast("text") };
    const identity: Identity = .{ .document = "doc", .producer = "units", .unit = "unit" };
    const good_fp = try fingerprints.fingerprintAlloc(alloc, good);
    defer alloc.free(good_fp);
    const raw = try encodeAlloc(alloc, "doc", "units", good, good_fp, "input", "text/plain", .{ .range_id = "range" });
    defer alloc.free(raw);
    var parsed = try decodeAlloc(alloc, raw, identity);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.unit.transcript_spans.len);
    for (0..4) |variant| {
        var invalid = good;
        var spans = [_]extraction.TranscriptSpan{.{ .char_start = 0, .char_end = 6, .start_ms = 20, .end_ms = 10 }};
        var regions = [_]extraction.TextRegion{.{ .span = .{ 0, 6 }, .bbox = .{ 0, 0, 1, 1 } }};
        switch (variant) {
            0 => invalid.char_start = 1,
            1 => {
                invalid.char_start = 1;
                invalid.char_end = 9;
            },
            2 => invalid.transcript_spans = &spans,
            3 => invalid.text_regions = &regions,
            else => unreachable,
        }
        const fp = try fingerprints.fingerprintAlloc(alloc, invalid);
        defer alloc.free(fp);
        const encoded = try encodeAlloc(alloc, "doc", "units", invalid, fp, "input", "text/plain", .{ .range_id = "range" });
        defer alloc.free(encoded);
        try std.testing.expectError(error.InvalidDocumentExtractionManifest, decodeAlloc(alloc, encoded, identity));
    }
}
