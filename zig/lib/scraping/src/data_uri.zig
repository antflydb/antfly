// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

const std = @import("std");

pub const Encoding = enum { base64, percent };

/// URI schemes are case-insensitive. Keep this predicate next to the parser so
/// admission, transport selection, and decoding cannot disagree about whether
/// a source is an inline data URI.
pub fn hasScheme(value: []const u8) bool {
    return value.len >= "data:".len and
        std.ascii.eqlIgnoreCase(value[0.."data:".len], "data:");
}

/// A parsed RFC 2397 data URI. The media type is optional in the grammar; when
/// omitted, RFC 2397 defines `text/plain;charset=US-ASCII` as the effective
/// value. Callers that require an explicit image/audio type must enforce that
/// policy through `has_explicit_media_type` rather than weakening the parser.
pub const Parsed = struct {
    media_type: []const u8,
    media_type_essence: []const u8,
    payload: []const u8,
    encoding: Encoding,
    has_explicit_media_type: bool,

    pub fn decodedSize(self: Parsed) !usize {
        return switch (self.encoding) {
            .base64 => validateCanonicalStandardBase64(self.payload),
            .percent => percentDecodedSize(self.payload),
        };
    }

    pub fn effectiveMediaTypeEssence(self: Parsed) []const u8 {
        return if (self.has_explicit_media_type) self.media_type_essence else "text/plain";
    }
};

pub const Decoded = struct {
    media_type: []u8,
    media_type_essence: []const u8,
    data: []u8,
    has_explicit_media_type: bool,

    pub fn deinit(self: *Decoded, alloc: std.mem.Allocator) void {
        alloc.free(self.media_type);
        alloc.free(self.data);
        self.* = undefined;
    }
};

pub fn parse(value: []const u8) !?Parsed {
    if (!hasScheme(value)) return null;
    const comma = std.mem.indexOfScalar(u8, value, ',') orelse return error.InvalidDataUri;
    const metadata = value["data:".len..comma];
    const is_base64 = std.ascii.endsWithIgnoreCase(metadata, ";base64");
    const media_type = if (is_base64) metadata[0 .. metadata.len - ";base64".len] else metadata;
    const essence_end = std.mem.indexOfScalar(u8, media_type, ';') orelse media_type.len;
    var essence = media_type[0..essence_end];
    // RFC 2397 permits parameters without an explicit media type, for example
    // `data:;charset=utf-8,...`. An explicitly present type may not be blank.
    const has_explicit = essence.len > 0;
    if (!has_explicit and media_type.len > 0 and media_type[0] != ';') return error.InvalidDataUri;
    if (has_explicit) {
        const parsed_media_type = parseMediaType(media_type) catch return error.InvalidDataUri;
        essence = parsed_media_type.essence;
    } else {
        try validateParameters(media_type[essence_end..]);
    }
    return .{
        .media_type = media_type,
        .media_type_essence = essence,
        .payload = value[comma + 1 ..],
        .encoding = if (is_base64) .base64 else .percent,
        .has_explicit_media_type = has_explicit,
    };
}

pub fn parseRequired(value: []const u8) !Parsed {
    return (try parse(value)) orelse error.InvalidDataUri;
}

/// A validated HTTP media type. `essence` is the policy-relevant type/subtype;
/// `parameters` retains validated transport metadata for declaration checks.
pub const MediaType = struct {
    value: []const u8,
    essence: []const u8,
    parameters: []const u8,

    /// A declaration without parameters is compatible with a more-specific
    /// attachment. Named parameters, such as codecs, must agree.
    pub fn isCompatibleDeclaration(self: MediaType, attachment: MediaType) bool {
        if (!std.ascii.eqlIgnoreCase(self.essence, attachment.essence)) return false;
        var iter = ParameterIterator.init(self.parameters);
        while (iter.next() catch return false) |declared| {
            const maybe_actual = attachment.parameter(declared.name) catch return false;
            const actual = maybe_actual orelse return false;
            if (!parameterValuesEqual(declared.value, actual)) return false;
        }
        return true;
    }

    pub fn parameter(self: MediaType, name: []const u8) !?[]const u8 {
        var iter = ParameterIterator.init(self.parameters);
        while (try iter.next()) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return entry.value;
        }
        return null;
    }
};

pub fn parseMediaType(value: []const u8) !MediaType {
    const trimmed = std.mem.trim(u8, value, " \t");
    if (trimmed.len == 0) return error.InvalidMediaType;
    for (trimmed) |byte| {
        if ((byte < 0x20 and byte != '\t') or byte == 0x7f)
            return error.InvalidMediaType;
    }
    const end = std.mem.indexOfScalar(u8, trimmed, ';') orelse trimmed.len;
    const essence = std.mem.trim(u8, trimmed[0..end], " \t");
    if (!validMediaTypeEssence(essence)) return error.InvalidMediaType;
    const parameters = trimmed[end..];
    var iter = ParameterIterator.init(parameters);
    var names: [32][]const u8 = undefined;
    var count: usize = 0;
    while (try iter.next()) |entry| {
        if (count == names.len) return error.InvalidMediaType;
        for (names[0..count]) |prior| {
            if (std.ascii.eqlIgnoreCase(prior, entry.name)) return error.InvalidMediaType;
        }
        names[count] = entry.name;
        count += 1;
    }
    return .{ .value = trimmed, .essence = essence, .parameters = parameters };
}

/// Return the policy-relevant portion of a fully validated Content-Type.
pub fn mediaTypeEssence(value: []const u8) ![]const u8 {
    return (try parseMediaType(value)).essence;
}

pub fn mediaTypesCompatible(declared: []const u8, attachment: []const u8) bool {
    const declared_type = parseMediaType(declared) catch return false;
    const attachment_type = parseMediaType(attachment) catch return false;
    return declared_type.isCompatibleDeclaration(attachment_type);
}

pub fn decodeAlloc(alloc: std.mem.Allocator, value: []const u8) !Decoded {
    const parsed = try parseRequired(value);
    const effective_media_type = if (parsed.has_explicit_media_type)
        try alloc.dupe(u8, parsed.media_type)
    else if (parsed.media_type.len == 0)
        try alloc.dupe(u8, "text/plain;charset=US-ASCII")
    else
        try std.fmt.allocPrint(alloc, "text/plain{s}", .{parsed.media_type});
    errdefer alloc.free(effective_media_type);
    const decoded_len = try parsed.decodedSize();
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    switch (parsed.encoding) {
        .base64 => std.base64.standard.Decoder.decode(decoded, parsed.payload) catch return error.InvalidBase64,
        .percent => try percentDecode(decoded, parsed.payload),
    }
    return .{
        .media_type = effective_media_type,
        .media_type_essence = effective_media_type[0 .. std.mem.indexOfScalar(u8, effective_media_type, ';') orelse effective_media_type.len],
        .data = decoded,
        .has_explicit_media_type = parsed.has_explicit_media_type,
    };
}

fn validMediaTypeEssence(value: []const u8) bool {
    const slash = std.mem.indexOfScalar(u8, value, '/') orelse return false;
    if (slash == 0 or slash + 1 == value.len or std.mem.indexOfScalarPos(u8, value, slash + 1, '/') != null)
        return false;
    return validMimeToken(value[0..slash]) and validMimeToken(value[slash + 1 ..]);
}

fn validMimeToken(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |byte| switch (byte) {
        0...0x20, 0x7f...0xff, '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=' => return false,
        else => {},
    };
    return true;
}

const Parameter = struct {
    name: []const u8,
    value: []const u8,
};

const ParameterIterator = struct {
    rest: []const u8,

    fn init(parameters: []const u8) ParameterIterator {
        return .{ .rest = parameters };
    }

    fn next(self: *ParameterIterator) !?Parameter {
        self.rest = std.mem.trimStart(u8, self.rest, " \t");
        if (self.rest.len == 0) return null;
        if (self.rest[0] != ';') return error.InvalidMediaType;
        self.rest = std.mem.trimStart(u8, self.rest[1..], " \t");
        if (self.rest.len == 0) return error.InvalidMediaType;

        var name_end: usize = 0;
        while (name_end < self.rest.len and isMimeTokenByte(self.rest[name_end])) : (name_end += 1) {}
        if (name_end == 0) return error.InvalidMediaType;
        const name = self.rest[0..name_end];
        self.rest = std.mem.trimStart(u8, self.rest[name_end..], " \t");
        if (self.rest.len == 0 or self.rest[0] != '=') return error.InvalidMediaType;
        self.rest = std.mem.trimStart(u8, self.rest[1..], " \t");
        if (self.rest.len == 0) return error.InvalidMediaType;

        const value = if (self.rest[0] == '"') blk: {
            var index: usize = 1;
            while (index < self.rest.len) {
                const byte = self.rest[index];
                if (byte == '"') {
                    const result = self.rest[0 .. index + 1];
                    self.rest = self.rest[index + 1 ..];
                    break :blk result;
                }
                if (byte == '\\') {
                    index += 1;
                    if (index >= self.rest.len or self.rest[index] < 0x20 or self.rest[index] > 0x7e)
                        return error.InvalidMediaType;
                } else if (byte == '\t' or byte == ' ' or byte == 0x21 or
                    (byte >= 0x23 and byte <= 0x5b) or (byte >= 0x5d and byte <= 0x7e))
                {
                    // Valid quoted-string byte.
                } else return error.InvalidMediaType;
                index += 1;
            }
            return error.InvalidMediaType;
        } else blk: {
            var value_end: usize = 0;
            while (value_end < self.rest.len and isMimeTokenByte(self.rest[value_end])) : (value_end += 1) {}
            if (value_end == 0) return error.InvalidMediaType;
            const result = self.rest[0..value_end];
            self.rest = self.rest[value_end..];
            break :blk result;
        };
        self.rest = std.mem.trimStart(u8, self.rest, " \t");
        if (self.rest.len > 0 and self.rest[0] != ';') return error.InvalidMediaType;
        return .{ .name = name, .value = value };
    }
};

fn isMimeTokenByte(byte: u8) bool {
    return switch (byte) {
        0...0x20, 0x7f...0xff, '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=' => false,
        else => true,
    };
}

fn parameterValuesEqual(a: []const u8, b: []const u8) bool {
    const a_value = if (a.len >= 2 and a[0] == '"' and a[a.len - 1] == '"') a[1 .. a.len - 1] else a;
    const b_value = if (b.len >= 2 and b[0] == '"' and b[b.len - 1] == '"') b[1 .. b.len - 1] else b;
    return quotedValuesEqual(a_value, b_value);
}

fn quotedValuesEqual(a: []const u8, b: []const u8) bool {
    var a_index: usize = 0;
    var b_index: usize = 0;
    while (a_index < a.len and b_index < b.len) {
        if (a[a_index] == '\\') a_index += 1;
        if (b[b_index] == '\\') b_index += 1;
        if (a_index >= a.len or b_index >= b.len or a[a_index] != b[b_index]) return false;
        a_index += 1;
        b_index += 1;
    }
    return a_index == a.len and b_index == b.len;
}

fn validateParameters(parameters: []const u8) !void {
    var iter = ParameterIterator.init(parameters);
    while (iter.next() catch return error.InvalidDataUri) |_| {}
}

pub fn validateCanonicalStandardBase64(data: []const u8) !usize {
    const decoded_size = std.base64.standard.Decoder.calcSizeForSlice(data) catch
        return error.InvalidBase64;
    var padding: usize = 0;
    while (padding < data.len and data[data.len - 1 - padding] == '=') : (padding += 1) {}
    if (padding > 2) return error.InvalidBase64;
    const content_end = data.len - padding;
    for (data[0..content_end]) |byte| _ = standardBase64Index(byte) orelse return error.InvalidBase64;
    for (data[content_end..]) |byte| if (byte != '=') return error.InvalidBase64;
    if (padding == 1) {
        if (content_end < 3 or (standardBase64Index(data[content_end - 1]).? & 0x03) != 0)
            return error.InvalidBase64;
    } else if (padding == 2) {
        if (content_end < 2 or (standardBase64Index(data[content_end - 1]).? & 0x0f) != 0)
            return error.InvalidBase64;
    }
    return decoded_size;
}

fn standardBase64Index(byte: u8) ?u8 {
    return switch (byte) {
        'A'...'Z' => byte - 'A',
        'a'...'z' => byte - 'a' + 26,
        '0'...'9' => byte - '0' + 52,
        '+' => 62,
        '/' => 63,
        else => null,
    };
}

fn percentDecodedSize(data: []const u8) !usize {
    var size: usize = 0;
    var index: usize = 0;
    while (index < data.len) {
        if (data[index] == '%') {
            if (index + 2 >= data.len or hexValue(data[index + 1]) == null or hexValue(data[index + 2]) == null)
                return error.InvalidDataUri;
            index += 3;
        } else {
            index += 1;
        }
        size = std.math.add(usize, size, 1) catch return error.InvalidDataUri;
    }
    return size;
}

fn percentDecode(out: []u8, data: []const u8) !void {
    var source_index: usize = 0;
    var output_index: usize = 0;
    while (source_index < data.len) : (output_index += 1) {
        if (data[source_index] == '%') {
            const high = hexValue(data[source_index + 1]) orelse return error.InvalidDataUri;
            const low = hexValue(data[source_index + 2]) orelse return error.InvalidDataUri;
            out[output_index] = (high << 4) | low;
            source_index += 3;
        } else {
            out[output_index] = data[source_index];
            source_index += 1;
        }
    }
    if (output_index != out.len) return error.InvalidDataUri;
}

fn hexValue(byte: u8) ?u8 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => null,
    };
}

test "RFC 2397 parser supports omitted media types, parameters, and canonical base64" {
    const alloc = std.testing.allocator;
    const omitted = (try parse("data:,hello%20world")).?;
    try std.testing.expect(!omitted.has_explicit_media_type);
    try std.testing.expectEqualStrings("text/plain", omitted.effectiveMediaTypeEssence());
    try std.testing.expectEqual(@as(usize, 11), try omitted.decodedSize());

    const parameter_only = (try parse("data:;charset=utf-8,hello")).?;
    try std.testing.expect(!parameter_only.has_explicit_media_type);
    var parameter_decoded = try decodeAlloc(alloc, "data:;charset=utf-8,hello");
    defer parameter_decoded.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain;charset=utf-8", parameter_decoded.media_type);
    const image = (try parse("DATA:image/png;charset=binary;BASE64,AQID")).?;
    try std.testing.expect(image.has_explicit_media_type);
    try std.testing.expectEqualStrings("image/png", image.media_type_essence);
    try std.testing.expectEqual(@as(usize, 3), try image.decodedSize());
    try std.testing.expectError(error.InvalidBase64, validateCanonicalStandardBase64("YR=="));
    try std.testing.expectError(error.InvalidDataUri, parse("data:image;base64,AQID"));
    try std.testing.expectError(error.InvalidDataUri, parse("data:image/png;base64;foo,AQID"));
}

test "media type parser validates complete parameters and declaration compatibility" {
    try std.testing.expectEqualStrings(
        "Image/PNG",
        (try parseMediaType(" Image/PNG ; charset=binary")).essence,
    );
    try std.testing.expectError(error.InvalidMediaType, parseMediaType("image/png;"));
    try std.testing.expectError(error.InvalidMediaType, parseMediaType("image/png; charset"));
    try std.testing.expectError(error.InvalidMediaType, parseMediaType("image/png; charset=\"unterminated"));
    try std.testing.expect(mediaTypesCompatible("audio/webm", "audio/webm; codecs=opus"));
    try std.testing.expect(mediaTypesCompatible("audio/webm; codecs=opus", "audio/webm;codecs=\"opus\""));
    try std.testing.expect(!mediaTypesCompatible("audio/webm; codecs=opus", "audio/webm;codecs=OPUS"));
    try std.testing.expect(!mediaTypesCompatible("audio/webm;codecs=opus", "audio/webm;codecs=vorbis"));
}

test "RFC 2397 decoder materializes base64 and percent payloads" {
    const alloc = std.testing.allocator;
    var percent = try decodeAlloc(alloc, "data:,%89PNG%0a");
    defer percent.deinit(alloc);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\n' }, percent.data);
    var base64 = try decodeAlloc(alloc, "data:image/png;base64,AQID");
    defer base64.deinit(alloc);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, base64.data);
}

fn decodeAllocationFixture(alloc: std.mem.Allocator) !void {
    var decoded = try decodeAlloc(alloc, "data:;charset=utf-8;base64,AQID");
    defer decoded.deinit(alloc);
}

test "RFC 2397 decoder is allocation-failure safe" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, decodeAllocationFixture, .{});
}
