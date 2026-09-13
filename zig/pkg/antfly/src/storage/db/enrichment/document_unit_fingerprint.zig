// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const document_extraction = @import("document_extraction.zig");

const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;

/// Persisted prefix for the canonical, injective unit-fingerprint encoding.
/// Keeping the version in the value makes payload markers self-describing and
/// lets readers distinguish them from the unversioned legacy digest.
pub const current_prefix = "duf2:";
pub const current_state_version: u8 = 2;
const current_domain = "antfly-document-unit-fingerprint-v2";

// Adding extraction metadata is a persisted-format change: update the encoder,
// assign a new stable tag, and bump the fingerprint version deliberately.
comptime {
    if (@typeInfo(document_extraction.Unit).@"struct".fields.len != 32) {
        @compileError("document_extraction.Unit changed; update and version document_unit_fingerprint");
    }
}

const Field = enum(u8) {
    unit_id = 1,
    unit_type = 2,
    text = 3,
    method = 4,
    source_path = 5,
    extraction_status = 6,
    source_sha256 = 7,
    byte_length = 8,
    ocr_used = 9,
    ocr_attempted = 10,
    ocr_render_dpi = 11,
    ocr_effective_render_dpi = 12,
    ocr_rendered_width = 13,
    ocr_rendered_height = 14,
    ocr_rendered_bytes = 15,
    ocr_failure_stage = 16,
    ocr_failure_retryable = 17,
    ocr_trigger_reasons = 18,
    ocr_embedded_quality = 19,
    ocr_output_quality = 20,
    ocr_confidence = 21,
    ocr_bbox = 22,
    transcript_used = 23,
    transcript_confidence = 24,
    extraction_warning = 25,
    page_number = 26,
    page_label = 27,
    page_bbox = 28,
    page_rotation = 29,
    text_regions = 30,
    char_start = 31,
    char_end = 32,
};

/// Computes the current document-unit fingerprint without constructing an
/// intermediate serialization. Every variable-width field is length-prefixed,
/// every optional field carries an explicit presence byte, and all scalars use
/// a fixed byte order. The result is therefore unambiguous and portable while
/// retaining one allocation for the returned persisted value.
pub fn fingerprintAlloc(alloc: Allocator, unit: document_extraction.Unit) ![]u8 {
    var hasher = Sha256.init(.{});
    hashLengthPrefixed(&hasher, current_domain);
    hashTaggedBytes(&hasher, .unit_id, unit.unit_id);
    hashTaggedBytes(&hasher, .unit_type, unit.unit_type);
    hashTaggedBytes(&hasher, .text, unit.text);
    hashTaggedBytes(&hasher, .method, unit.method);
    hashTaggedOptionalBytes(&hasher, .source_path, unit.source_path);
    hashTaggedOptionalBytes(&hasher, .extraction_status, unit.extraction_status);
    hashTaggedOptionalBytes(&hasher, .source_sha256, unit.source_sha256);
    hashTaggedOptionalU64(&hasher, .byte_length, unit.byte_length);
    hashTaggedBool(&hasher, .ocr_used, unit.ocr_used);
    hashTaggedBool(&hasher, .ocr_attempted, unit.ocr_attempted);
    hashTaggedOptionalU16(&hasher, .ocr_render_dpi, unit.ocr_render_dpi);
    hashTaggedOptionalU16(&hasher, .ocr_effective_render_dpi, unit.ocr_effective_render_dpi);
    hashTaggedOptionalU32(&hasher, .ocr_rendered_width, unit.ocr_rendered_width);
    hashTaggedOptionalU32(&hasher, .ocr_rendered_height, unit.ocr_rendered_height);
    hashTaggedOptionalU64(&hasher, .ocr_rendered_bytes, unit.ocr_rendered_bytes);
    hashTaggedOptionalBytes(&hasher, .ocr_failure_stage, unit.ocr_failure_stage);
    hashTaggedOptionalBool(&hasher, .ocr_failure_retryable, unit.ocr_failure_retryable);
    hashTaggedOptionalBytes(&hasher, .ocr_trigger_reasons, unit.ocr_trigger_reasons);
    hashTaggedOptionalBytes(&hasher, .ocr_embedded_quality, unit.ocr_embedded_quality);
    hashTaggedOptionalBytes(&hasher, .ocr_output_quality, unit.ocr_output_quality);
    hashTaggedOptionalF64(&hasher, .ocr_confidence, unit.ocr_confidence);
    hashTaggedOptionalBbox(&hasher, .ocr_bbox, unit.ocr_bbox);
    hashTaggedBool(&hasher, .transcript_used, unit.transcript_used);
    hashTaggedOptionalF64(&hasher, .transcript_confidence, unit.transcript_confidence);
    hashTaggedOptionalBytes(&hasher, .extraction_warning, unit.extraction_warning);
    hashTaggedOptionalU32(&hasher, .page_number, unit.page_number);
    hashTaggedOptionalBytes(&hasher, .page_label, unit.page_label);
    hashTaggedOptionalBbox(&hasher, .page_bbox, unit.page_bbox);
    hashTaggedOptionalI32(&hasher, .page_rotation, unit.page_rotation);
    hashTaggedTextRegions(&hasher, unit.text_regions);
    hashTaggedOptionalU32(&hasher, .char_start, unit.char_start);
    hashTaggedOptionalU32(&hasher, .char_end, unit.char_end);

    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try prefixedLowerHexAlloc(alloc, current_prefix, &digest);
}

/// Reproduces the unversioned fingerprint emitted before `duf2`. It is
/// intentionally available only for validating stored payloads that have no
/// fingerprint marker. New state, navigation revisions, and skip decisions
/// must use `fingerprintAlloc`.
pub fn legacyFingerprintAlloc(alloc: Allocator, unit: document_extraction.Unit) ![]u8 {
    var hasher = Sha256.init(.{});
    hasher.update(unit.unit_id);
    hasher.update(unit.unit_type);
    hasher.update(unit.text);
    hasher.update(unit.method);
    if (unit.source_path) |source_path| hasher.update(source_path);
    if (unit.extraction_status) |extraction_status| hasher.update(extraction_status);
    if (unit.source_sha256) |source_sha256| hasher.update(source_sha256);
    if (unit.byte_length) |byte_length| {
        var buf: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &buf, byte_length, .big);
        hasher.update(&buf);
    }
    hasher.update(if (unit.ocr_used) "ocr:1" else "ocr:0");
    hasher.update(if (unit.ocr_attempted) "ocr_attempted:1" else "ocr_attempted:0");
    if (unit.ocr_render_dpi) |dpi| {
        var buf: [@sizeOf(u16)]u8 = undefined;
        std.mem.writeInt(u16, &buf, dpi, .big);
        hasher.update(&buf);
    }
    if (unit.ocr_effective_render_dpi) |dpi| {
        var buf: [@sizeOf(u16)]u8 = undefined;
        std.mem.writeInt(u16, &buf, dpi, .big);
        hasher.update(&buf);
    }
    if (unit.ocr_rendered_width) |value| hasher.update(std.mem.asBytes(&value));
    if (unit.ocr_rendered_height) |value| hasher.update(std.mem.asBytes(&value));
    if (unit.ocr_rendered_bytes) |value| hasher.update(std.mem.asBytes(&value));
    if (unit.ocr_failure_stage) |value| hasher.update(value);
    if (unit.ocr_failure_retryable) |value| hasher.update(if (value) "ocr_failure_retryable:1" else "ocr_failure_retryable:0");
    if (unit.ocr_trigger_reasons) |value| hasher.update(value);
    if (unit.ocr_embedded_quality) |value| hasher.update(value);
    if (unit.ocr_output_quality) |value| hasher.update(value);
    if (unit.ocr_confidence) |confidence| {
        var value = confidence;
        hasher.update(std.mem.asBytes(&value));
    }
    if (unit.ocr_bbox) |bbox| for (bbox) |coord| {
        var value = coord;
        hasher.update(std.mem.asBytes(&value));
    };
    hasher.update(if (unit.transcript_used) "transcript:1" else "transcript:0");
    if (unit.transcript_confidence) |confidence| {
        var value = confidence;
        hasher.update(std.mem.asBytes(&value));
    }
    if (unit.extraction_warning) |warning| hasher.update(warning);
    if (unit.page_number) |page_number| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, page_number, .big);
        hasher.update(&buf);
    }
    if (unit.page_label) |page_label| hasher.update(page_label);
    if (unit.page_bbox) |bbox| for (bbox) |coord| {
        var value = coord;
        hasher.update(std.mem.asBytes(&value));
    };
    if (unit.page_rotation) |rotation| {
        var buf: [@sizeOf(i32)]u8 = undefined;
        std.mem.writeInt(i32, &buf, rotation, .big);
        hasher.update(&buf);
    }
    for (unit.text_regions) |region| {
        for (region.span) |span| {
            var buf: [@sizeOf(u32)]u8 = undefined;
            std.mem.writeInt(u32, &buf, span, .big);
            hasher.update(&buf);
        }
        for (region.bbox) |coord| {
            var value = coord;
            hasher.update(std.mem.asBytes(&value));
        }
    }
    if (unit.char_start) |char_start| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, char_start, .big);
        hasher.update(&buf);
    }
    if (unit.char_end) |char_end| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, char_end, .big);
        hasher.update(&buf);
    }
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try prefixedLowerHexAlloc(alloc, "", &digest);
}

/// Reconstructs the legacy digest from a stored unit that predates the
/// persisted `duf2:` marker. Kept in this contract module so control-side
/// projection validation does not import the physical DB implementation.
pub fn storedPayloadLegacyFingerprintAlloc(alloc: Allocator, stored: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, scratch, stored, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentExtractionState;
    const object = parsed.value.object;
    const provenance_value = object.get("provenance") orelse return error.InvalidDocumentExtractionState;
    if (provenance_value != .object) return error.InvalidDocumentExtractionState;
    const provenance = provenance_value.object;

    const unit = document_extraction.Unit{
        .unit_id = @constCast(try requiredString(object, "unit_id")),
        .unit_type = @constCast(try requiredString(object, "unit_type")),
        .text = @constCast(try requiredString(object, "text")),
        .method = @constCast(try requiredString(provenance, "method")),
        .source_path = if (try optionalString(object, "source_path")) |value| @constCast(value) else null,
        .extraction_status = if (try optionalString(object, "extraction_status")) |value| @constCast(value) else null,
        .source_sha256 = if (try optionalString(object, "source_sha256")) |value| @constCast(value) else null,
        .byte_length = try optionalInteger(u64, object, "byte_length"),
        .ocr_used = try requiredBool(provenance, "ocr_used"),
        .ocr_attempted = try requiredBool(object, "ocr_attempted"),
        .ocr_render_dpi = try optionalInteger(u16, object, "ocr_render_dpi"),
        .ocr_effective_render_dpi = try optionalInteger(u16, object, "ocr_effective_render_dpi"),
        .ocr_rendered_width = try optionalInteger(u32, object, "ocr_rendered_width"),
        .ocr_rendered_height = try optionalInteger(u32, object, "ocr_rendered_height"),
        .ocr_rendered_bytes = try optionalInteger(u64, object, "ocr_rendered_bytes"),
        .ocr_failure_stage = if (try optionalString(object, "ocr_failure_stage")) |value| @constCast(value) else null,
        .ocr_failure_retryable = try optionalBool(object, "ocr_failure_retryable"),
        .ocr_trigger_reasons = if (try optionalString(object, "ocr_trigger_reasons")) |value| @constCast(value) else null,
        .ocr_embedded_quality = if (try optionalString(object, "ocr_embedded_quality")) |value| @constCast(value) else null,
        .ocr_output_quality = if (try optionalString(object, "ocr_output_quality")) |value| @constCast(value) else null,
        .ocr_confidence = try optionalFloat(object, "ocr_confidence"),
        .ocr_bbox = try optionalBbox(object, "ocr_bbox"),
        .transcript_used = try requiredBool(provenance, "transcript_used"),
        .transcript_confidence = try optionalFloat(object, "transcript_confidence"),
        .extraction_warning = if (try optionalString(object, "extraction_warning")) |value| @constCast(value) else null,
        .page_number = try optionalInteger(u32, provenance, "page_number"),
        .page_label = if (try optionalString(provenance, "page_label")) |value| @constCast(value) else null,
        .page_bbox = try optionalBbox(provenance, "page_bbox"),
        .page_rotation = try optionalInteger(i32, provenance, "page_rotation"),
        .text_regions = try textRegionsAlloc(scratch, provenance.get("text_regions")),
        .char_start = try optionalInteger(u32, provenance, "char_start"),
        .char_end = try optionalInteger(u32, provenance, "char_end"),
    };
    return try legacyFingerprintAlloc(alloc, unit);
}

fn requiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    return (try optionalString(object, name)) orelse error.InvalidDocumentExtractionState;
}

fn optionalString(object: std.json.ObjectMap, name: []const u8) !?[]const u8 {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .null => null,
        .string => |text| text,
        else => error.InvalidDocumentExtractionState,
    };
}

fn requiredBool(object: std.json.ObjectMap, name: []const u8) !bool {
    return (try optionalBool(object, name)) orelse error.InvalidDocumentExtractionState;
}

fn optionalBool(object: std.json.ObjectMap, name: []const u8) !?bool {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .null => null,
        .bool => |boolean| boolean,
        else => error.InvalidDocumentExtractionState,
    };
}

fn optionalInteger(comptime T: type, object: std.json.ObjectMap, name: []const u8) !?T {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .null => null,
        .integer => |integer| std.math.cast(T, integer) orelse error.InvalidDocumentExtractionState,
        .number_string => |text| std.fmt.parseInt(T, text, 10) catch error.InvalidDocumentExtractionState,
        else => error.InvalidDocumentExtractionState,
    };
}

fn optionalFloat(object: std.json.ObjectMap, name: []const u8) !?f64 {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .null => null,
        .integer => |integer| @floatFromInt(integer),
        .float => |float| if (std.math.isFinite(float)) float else error.InvalidDocumentExtractionState,
        .number_string => |text| blk: {
            const parsed = std.fmt.parseFloat(f64, text) catch return error.InvalidDocumentExtractionState;
            if (!std.math.isFinite(parsed)) return error.InvalidDocumentExtractionState;
            break :blk parsed;
        },
        else => error.InvalidDocumentExtractionState,
    };
}

fn optionalBbox(object: std.json.ObjectMap, name: []const u8) !?[4]f64 {
    const value = object.get(name) orelse return null;
    if (value == .null) return null;
    if (value != .array or value.array.items.len != 4) return error.InvalidDocumentExtractionState;
    var bbox: [4]f64 = undefined;
    for (value.array.items, 0..) |coordinate, i| {
        bbox[i] = switch (coordinate) {
            .integer => |integer| @floatFromInt(integer),
            .float => |float| if (std.math.isFinite(float)) float else return error.InvalidDocumentExtractionState,
            .number_string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidDocumentExtractionState,
            else => return error.InvalidDocumentExtractionState,
        };
        if (!std.math.isFinite(bbox[i])) return error.InvalidDocumentExtractionState;
    }
    return bbox;
}

fn textRegionsAlloc(alloc: Allocator, value: ?std.json.Value) ![]document_extraction.TextRegion {
    const regions_value = value orelse return &.{};
    if (regions_value == .null) return &.{};
    if (regions_value != .array) return error.InvalidDocumentExtractionState;
    const regions = try alloc.alloc(document_extraction.TextRegion, regions_value.array.items.len);
    for (regions_value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidDocumentExtractionState;
        const span_value = item.object.get("span") orelse return error.InvalidDocumentExtractionState;
        if (span_value != .array or span_value.array.items.len != 2) return error.InvalidDocumentExtractionState;
        regions[i] = .{
            .span = .{
                try integerValue(u32, span_value.array.items[0]),
                try integerValue(u32, span_value.array.items[1]),
            },
            .bbox = (try optionalBbox(item.object, "bbox")) orelse return error.InvalidDocumentExtractionState,
        };
    }
    return regions;
}

fn integerValue(comptime T: type, value: std.json.Value) !T {
    return switch (value) {
        .integer => |integer| std.math.cast(T, integer) orelse error.InvalidDocumentExtractionState,
        .number_string => |text| std.fmt.parseInt(T, text, 10) catch error.InvalidDocumentExtractionState,
        else => error.InvalidDocumentExtractionState,
    };
}

pub fn isCurrent(fingerprint: []const u8) bool {
    return fingerprint.len == current_prefix.len + Sha256.digest_length * 2 and
        std.mem.startsWith(u8, fingerprint, current_prefix) and
        isLowerHex(fingerprint[current_prefix.len..]);
}

/// Returns whether a stored extraction state was written with this or a newer
/// unit-fingerprint capability. Older states must be re-extracted so their
/// unit and chunk artifacts acquire an unambiguous revision marker.
pub fn stateVersionIsCurrent(value: std.json.Value) bool {
    return value == .integer and value.integer >= current_state_version;
}

fn hashField(hasher: *Sha256, field: Field) void {
    hasher.update(&.{@intFromEnum(field)});
}

fn hashLengthPrefixed(hasher: *Sha256, value: []const u8) void {
    var length: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &length, @intCast(value.len), .big);
    hasher.update(&length);
    hasher.update(value);
}

fn hashTaggedBytes(hasher: *Sha256, field: Field, value: []const u8) void {
    hashField(hasher, field);
    hashLengthPrefixed(hasher, value);
}

fn hashTaggedOptionalBytes(hasher: *Sha256, field: Field, value: ?[]const u8) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |bytes| hashLengthPrefixed(hasher, bytes);
}

fn hashPresence(hasher: *Sha256, present: bool) void {
    hasher.update(&.{if (present) 1 else 0});
}

fn hashTaggedBool(hasher: *Sha256, field: Field, value: bool) void {
    hashField(hasher, field);
    hasher.update(&.{if (value) 1 else 0});
}

fn hashTaggedOptionalBool(hasher: *Sha256, field: Field, value: ?bool) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |boolean| hasher.update(&.{if (boolean) 1 else 0});
}

fn hashTaggedOptionalU16(hasher: *Sha256, field: Field, value: ?u16) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |integer| {
        var encoded: [@sizeOf(u16)]u8 = undefined;
        std.mem.writeInt(u16, &encoded, integer, .big);
        hasher.update(&encoded);
    }
}

fn hashTaggedOptionalU32(hasher: *Sha256, field: Field, value: ?u32) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |integer| {
        var encoded: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &encoded, integer, .big);
        hasher.update(&encoded);
    }
}

fn hashTaggedOptionalI32(hasher: *Sha256, field: Field, value: ?i32) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |integer| {
        var encoded: [@sizeOf(i32)]u8 = undefined;
        std.mem.writeInt(i32, &encoded, integer, .big);
        hasher.update(&encoded);
    }
}

fn hashTaggedOptionalU64(hasher: *Sha256, field: Field, value: ?u64) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |integer| hashU64(hasher, integer);
}

fn hashTaggedOptionalF64(hasher: *Sha256, field: Field, value: ?f64) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |float| hashU64(hasher, @bitCast(float));
}

fn hashTaggedOptionalBbox(hasher: *Sha256, field: Field, value: ?[4]f64) void {
    hashField(hasher, field);
    hashPresence(hasher, value != null);
    if (value) |bbox| for (bbox) |coordinate| hashU64(hasher, @bitCast(coordinate));
}

fn hashTaggedTextRegions(hasher: *Sha256, regions: []const document_extraction.TextRegion) void {
    hashField(hasher, .text_regions);
    hashU64(hasher, @intCast(regions.len));
    for (regions) |region| {
        for (region.span) |offset| {
            var encoded: [@sizeOf(u32)]u8 = undefined;
            std.mem.writeInt(u32, &encoded, offset, .big);
            hasher.update(&encoded);
        }
        for (region.bbox) |coordinate| hashU64(hasher, @bitCast(coordinate));
    }
}

fn hashU64(hasher: *Sha256, value: u64) void {
    var encoded: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .big);
    hasher.update(&encoded);
}

fn prefixedLowerHexAlloc(alloc: Allocator, prefix: []const u8, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, prefix.len + bytes.len * 2);
    @memcpy(out[0..prefix.len], prefix);
    for (bytes, 0..) |byte, index| {
        out[prefix.len + index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[prefix.len + index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn isLowerHex(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |byte| {
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
    }
    return true;
}

test "document unit fingerprint canonically separates variable field boundaries" {
    const alloc = std.testing.allocator;
    const left = document_extraction.Unit{
        .unit_id = @constCast("archive:entry:000001"),
        .unit_type = @constCast("archive_entry"),
        .text = @constCast("X"),
        .method = @constCast("zip_html"),
        .source_path = @constCast("zip_textfoo.txt"),
    };
    const right = document_extraction.Unit{
        .unit_id = @constCast("archive:entry:000001"),
        .unit_type = @constCast("archive_entry"),
        .text = @constCast("Xzip_html"),
        .method = @constCast("zip_text"),
        .source_path = @constCast("foo.txt"),
    };

    const legacy_left = try legacyFingerprintAlloc(alloc, left);
    defer alloc.free(legacy_left);
    const legacy_right = try legacyFingerprintAlloc(alloc, right);
    defer alloc.free(legacy_right);
    try std.testing.expectEqualStrings(legacy_left, legacy_right);

    const current_left = try fingerprintAlloc(alloc, left);
    defer alloc.free(current_left);
    const current_right = try fingerprintAlloc(alloc, right);
    defer alloc.free(current_right);
    try std.testing.expect(isCurrent(current_left));
    try std.testing.expect(isCurrent(current_right));
    try std.testing.expect(!std.mem.eql(u8, current_left, current_right));
}

test "document unit fingerprint distinguishes absent and empty optional fields" {
    const alloc = std.testing.allocator;
    const absent = document_extraction.Unit{
        .unit_id = @constCast("document:000001"),
        .unit_type = @constCast("document"),
        .text = @constCast("text"),
        .method = @constCast("text"),
    };
    var empty = absent;
    empty.source_path = @constCast("");

    const absent_fingerprint = try fingerprintAlloc(alloc, absent);
    defer alloc.free(absent_fingerprint);
    const empty_fingerprint = try fingerprintAlloc(alloc, empty);
    defer alloc.free(empty_fingerprint);
    try std.testing.expect(!std.mem.eql(u8, absent_fingerprint, empty_fingerprint));
}

test "document unit fingerprint state version rejects legacy encodings" {
    try std.testing.expect(!stateVersionIsCurrent(.{ .integer = 1 }));
    try std.testing.expect(stateVersionIsCurrent(.{ .integer = current_state_version }));
    try std.testing.expect(!stateVersionIsCurrent(.{ .string = "2" }));
}
