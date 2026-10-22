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

//! Revision-bound source units shared by connectors, search and presentation.
//! Media locators are logical fields on the same authorized parent document.
//! They are never remote URLs or credentials.
const std = @import("std");
const input = @import("antfly_decision_evidence");
const images = @import("antfly_image");
const V = std.json.Value;
const A = std.mem.Allocator;
pub const max_units = 4096;
pub const Asset = struct {
    field: []const u8,
    mime_type: []const u8,
    sha256: []const u8,
    recipe: []const u8,
    byte_length: usize,
};
pub const Location = struct {
    page: ?u32 = null,
    slide: ?u32 = null,
    pub fn jsonStringify(self: Location, writer: *std.json.Stringify) std.Io.Writer.Error!void {
        try writer.beginObject();
        if (self.page) |n| {
            try writer.objectField("page");
            try writer.write(n);
        }
        if (self.slide) |n| {
            try writer.objectField("slide");
            try writer.write(n);
        }
        try writer.endObject();
    }
};
pub const Unit = struct {
    id: []const u8,
    kind: enum { image, page, slide },
    location: Location = .{},
    text: []const u8 = "",
    label: ?[]const u8 = null,
    asset: Asset,
    pub fn parse(a: A, value: V) !Unit {
        const self = std.json.parseFromValueLeaky(Unit, a, value, .{ .ignore_unknown_fields = false }) catch return error.InvalidEvidenceUnit;
        try self.validate();
        return self;
    }
    pub fn validate(self: Unit) !void {
        if (self.asset.byte_length == 0) return error.InvalidEvidenceSize;
        if (!validText(self.id, 256) or !validText(self.asset.recipe, 1024) or !validField(self.asset.field)) return error.InvalidEvidenceUnit;
        if (!std.unicode.utf8ValidateSlice(self.text) or self.text.len > 128 * 1024) return error.InvalidEvidenceUnit;
        if (self.label) |label| if (!validText(label, 1024)) return error.InvalidEvidenceUnit;
        if (self.asset.sha256.len != 64) return error.InvalidEvidenceDigest;
        for (self.asset.sha256) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidEvidenceDigest;
        if (!std.mem.eql(u8, self.asset.mime_type, "image/png") and !std.mem.eql(u8, self.asset.mime_type, "image/jpeg") and !std.mem.eql(u8, self.asset.mime_type, "image/webp") and !std.mem.eql(u8, self.asset.mime_type, "image/gif")) return error.UnsupportedEvidenceMedia;
        switch (self.kind) {
            .image => if (self.location.page != null or self.location.slide != null) return error.InvalidEvidenceLocation,
            .page => if ((self.location.page orelse 0) == 0 or self.location.slide != null) return error.InvalidEvidenceLocation,
            .slide => if ((self.location.slide orelse 0) == 0 or self.location.page != null) return error.InvalidEvidenceLocation,
        }
    }
};
pub fn validField(field: []const u8) bool {
    if (field.len == 0 or field.len > 1024) return false;
    var parts = std.mem.splitScalar(u8, field, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or part[0] == '_') return false;
        for (part) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return false;
    }
    return true;
}
fn validText(text: []const u8, max: usize) bool {
    return text.len > 0 and text.len <= max and std.unicode.utf8ValidateSlice(text) and std.mem.indexOfScalar(u8, text, 0) == null;
}
pub const Image = struct {
    data_url: []const u8,
    sha256: [32]u8,
    mime_type: []const u8,
    width: u32,
    height: u32,
    bytes: usize,
    pub fn fromBytes(a: A, bytes: []const u8, max_bytes: usize, max_pixels: u64) !Image {
        if (bytes.len > max_bytes) return error.ImageByteBudgetExceeded;
        const mime = imageMime(bytes) orelse return error.UnsupportedEvidenceMedia;
        const info = try images.inspectEncoded(bytes);
        if (try info.pixels() > max_pixels) return error.ImagePixelBudgetExceeded;
        const prefix = try std.fmt.allocPrint(a, "data:{s};base64,", .{mime});
        defer a.free(prefix);
        const url = try a.alloc(u8, prefix.len + std.base64.standard.Encoder.calcSize(bytes.len));
        @memcpy(url[0..prefix.len], prefix);
        _ = std.base64.standard.Encoder.encode(url[prefix.len..], bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return .{ .data_url = url, .sha256 = digest, .mime_type = mime, .width = info.width, .height = info.height, .bytes = bytes.len };
    }
    /// Every caller validates before cache access. Decoded bytes are temporary;
    /// no raster is allocated, retained, or written to the decision cache.
    pub fn fromDataUrl(a: A, url: []const u8, expected: ?Asset, max_bytes: usize, max_pixels: u64) !Image {
        if (url.len > std.base64.standard.Encoder.calcSize(max_bytes) + 64) return error.ImageByteBudgetExceeded;
        const inline_image = try input.inlineImage(url);
        const size = try std.base64.standard.Decoder.calcSizeForSlice(inline_image.encoded);
        if (size > max_bytes) return error.ImageByteBudgetExceeded;
        const bytes = try a.alloc(u8, size);
        defer a.free(bytes);
        try std.base64.standard.Decoder.decode(bytes, inline_image.encoded);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (expected) |asset| {
            if (asset.byte_length != size) return error.EvidenceSizeMismatch;
            if (!std.mem.eql(u8, asset.mime_type, inline_image.mime)) return error.EvidenceMimeMismatch;
            if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), asset.sha256)) return error.EvidenceDigestMismatch;
        }
        const format = images.detectFormat(bytes);
        const mime = images.inferenceMimeEssenceForFormat(format) orelse return error.UnsupportedEvidenceMedia;
        if (!std.mem.eql(u8, mime, inline_image.mime)) return error.EvidenceMimeMismatch;
        const info = try images.inspectEncoded(bytes);
        if (try info.pixels() > max_pixels) return error.ImagePixelBudgetExceeded;
        return .{ .data_url = url, .sha256 = digest, .mime_type = mime, .width = info.width, .height = info.height, .bytes = size };
    }
};

pub fn parseUnits(a: A, value: V) ![]const Unit {
    if (value != .array or value.array.items.len > max_units) return error.InvalidEvidenceUnits;
    const units = try a.alloc(Unit, value.array.items.len);
    errdefer a.free(units);
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    defer ids.deinit(a);
    for (value.array.items, units) |raw, *unit| {
        unit.* = try Unit.parse(a, raw);
        const entry = try ids.getOrPut(a, unit.id);
        if (entry.found_existing) return error.DuplicateEvidenceUnit;
    }
    return units;
}
pub const Loaded = union(enum) { image: Image, failed: anyerror };
pub const Loader = struct {
    ctx: *anyopaque,
    load: *const fn (*anyopaque, A, Unit) anyerror!Image,
    load_many: ?*const fn (*anyopaque, A, []const Unit, []Loaded) anyerror!void = null,
    pub fn batch(self: Loader, a: A, units: []const Unit, output: []Loaded) !void {
        std.debug.assert(units.len == output.len);
        if (self.load_many) |many| return many(self.ctx, a, units, output);
        for (units, output) |unit, *item| {
            item.* = if (self.load(self.ctx, a, unit)) |value| .{ .image = value } else |err| .{ .failed = err };
        }
    }
};

pub fn imageMime(bytes: []const u8) ?[]const u8 {
    return images.inferenceMimeEssenceForFormat(images.detectFormat(bytes));
}

test "units reject ambiguous ownership, locations, fields and digests" {
    var unit: Unit = .{ .id = "slide-1", .kind = .slide, .location = .{ .slide = 1 }, .asset = .{ .field = "payloads.preview", .mime_type = "image/png", .sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .recipe = "render-v1", .byte_length = 70 } };
    try unit.validate();
    for ([_][]const u8{ "https://drive.google.com/file", "_source.image", "payloads..image", ".image", "payloads._private" }) |field| {
        unit.asset.field = field;
        try std.testing.expectError(error.InvalidEvidenceUnit, unit.validate());
    }
    unit.asset.field = "payloads.preview";
    unit.location.page = 1;
    try std.testing.expectError(error.InvalidEvidenceLocation, unit.validate());
    unit.location.page = null;
    unit.asset.sha256 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    try std.testing.expectError(error.InvalidEvidenceDigest, unit.validate());
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    unit.asset.sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const json = try std.json.Stringify.valueAlloc(arena.allocator(), .{ unit, unit }, .{});
    const value = try std.json.parseFromSliceLeaky(V, arena.allocator(), json, .{});
    try std.testing.expectError(error.DuplicateEvidenceUnit, parseUnits(arena.allocator(), value));
}

test "bounded image inspection checks content not declared MIME or dimensions" {
    // A valid 1x1 PNG: header inspection needs no raster allocation.
    const encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII=";
    var bytes: [68]u8 = undefined;
    try std.base64.standard.Decoder.decode(&bytes, encoded);
    const a = std.testing.allocator;
    const image = try Image.fromBytes(a, &bytes, 1024, 16);
    defer a.free(image.data_url);
    try std.testing.expectEqual(@as(u32, 1), image.width);
    try std.testing.expectEqual(@as(u32, 1), image.height);
    var expected: Asset = .{ .field = "payload.image", .mime_type = "image/png", .sha256 = &std.fmt.bytesToHex(image.sha256, .lower), .recipe = "original-v1", .byte_length = bytes.len };
    _ = try Image.fromDataUrl(a, image.data_url, expected, 1024, 16);
    expected.sha256 = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expectError(error.EvidenceDigestMismatch, Image.fromDataUrl(a, image.data_url, expected, 1024, 16));
    try std.testing.expectError(error.ImagePixelBudgetExceeded, Image.fromDataUrl(a, image.data_url, null, 1024, 0));
    try std.testing.expectError(error.ImageByteBudgetExceeded, Image.fromDataUrl(a, image.data_url, null, 1, 16));
    try std.testing.expectError(error.UnsupportedEvidenceMedia, Image.fromBytes(a, "not an image", 1024, 16));
}
