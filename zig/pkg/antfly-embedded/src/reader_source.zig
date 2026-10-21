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

//! Pure reader source selection, shared by execution and offline inspection.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Source = struct {
    images: []const []const u8,
    prompt: ?[]const u8 = null,

    pub fn deinit(self: *Source, alloc: Allocator) void {
        for (self.images) |image| alloc.free(@constCast(image));
        alloc.free(self.images);
        if (self.prompt) |prompt| alloc.free(@constCast(prompt));
        self.* = undefined;
    }
};

pub fn parse(
    alloc: Allocator,
    source_text: []const u8,
    source_parts_json: ?[]const u8,
    allow_metadata_only: bool,
) !Source {
    if (source_parts_json) |raw_parts| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
        defer parsed.deinit();
        if (parsed.value == .array) {
            var images = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (images.items) |image| alloc.free(@constCast(image));
                images.deinit(alloc);
            }
            var prompt = std.ArrayListUnmanaged(u8).empty;
            errdefer prompt.deinit(alloc);

            for (parsed.value.array.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "text")) {
                    const text = part.object.get("text") orelse continue;
                    if (text != .string) continue;
                    if (prompt.items.len > 0) try prompt.append(alloc, '\n');
                    try prompt.appendSlice(alloc, text.string);
                } else if (std.mem.eql(u8, type_value.string, "media")) {
                    if (part.object.get("url")) |url| {
                        if (url == .string) try images.append(alloc, try alloc.dupe(u8, url.string));
                    } else if (part.object.get("mime_type")) |mime| {
                        const data = part.object.get("data") orelse continue;
                        if (mime == .string and data == .string) {
                            try images.append(alloc, try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ mime.string, data.string }));
                        }
                    }
                }
            }

            if (images.items.len > 0 or allow_metadata_only) {
                const owned_images = try images.toOwnedSlice(alloc);
                errdefer {
                    for (owned_images) |image| alloc.free(@constCast(image));
                    alloc.free(owned_images);
                }
                return .{
                    .images = owned_images,
                    .prompt = if (prompt.items.len > 0) try prompt.toOwnedSlice(alloc) else null,
                };
            }
            // Preserve the established source_text fallback for ordinary URL
            // readers. Reset both lists after deinit so their errdefers remain
            // harmless if parsing source_text subsequently fails.
            prompt.deinit(alloc);
            prompt = .empty;
            images.deinit(alloc);
            images = .empty;
        }
    }

    return try parseText(alloc, source_text);
}

pub fn parseText(alloc: Allocator, source_text: []const u8) !Source {
    const trimmed = std.mem.trim(u8, source_text, &std.ascii.whitespace);
    if (trimmed.len > 0 and (trimmed[0] == '[' or trimmed[0] == '"')) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch |err| switch (err) {
            std.mem.Allocator.Error.OutOfMemory => return err,
            else => null,
        };
        if (parsed) |*value| {
            defer value.deinit();
            switch (value.value) {
                .string => |url| return try singleReaderImage(alloc, url),
                .array => |array| return parseArray(alloc, array.items),
                else => {},
            }
        }
    }
    return try singleReaderImage(alloc, source_text);
}

/// Parse known JSON structure without choosing values for substitutions.
/// The NUL marker remains in decoded locator strings, including its position.
/// Unknown JSON structure/escapes return null instead of inventing a URL list.
pub fn parseTemplate(alloc: Allocator, text: []const u8, max_bytes: usize) !?Source {
    var normalized: std.ArrayList(u8) = .empty;
    defer normalized.deinit(alloc);
    var in_string = false;
    var escaped = false;
    for (text) |byte| {
        if (byte == 0) {
            if (!in_string or escaped) return null;
            if (6 > max_bytes -| normalized.items.len) return error.StreamTooLong;
            try normalized.appendSlice(alloc, "\\u0000");
            continue;
        }
        if (normalized.items.len >= max_bytes) return error.StreamTooLong;
        try normalized.append(alloc, byte);
        if (escaped) {
            escaped = false;
        } else if (in_string and byte == '\\') {
            escaped = true;
        } else if (byte == '"') in_string = !in_string;
    }
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, normalized.items, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    defer parsed.deinit();
    return switch (parsed.value) {
        .string => |url| try singleReaderImage(alloc, url),
        .array => |array| try parseArray(alloc, array.items),
        else => null,
    };
}

fn singleReaderImage(alloc: Allocator, url: []const u8) !Source {
    const images = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(images);
    images[0] = try alloc.dupe(u8, url);
    return .{ .images = images };
}

/// A reader JSON URL list ignores non-string elements.
pub fn parseArray(alloc: Allocator, values: []const std.json.Value) !Source {
    var images = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (images.items) |image| alloc.free(@constCast(image));
        images.deinit(alloc);
    }
    for (values) |item| {
        if (item == .string) try images.append(alloc, try alloc.dupe(u8, item.string));
    }
    return .{ .images = try images.toOwnedSlice(alloc) };
}

test "backup preflight shared reader source precedence and fallback" {
    const a = std.testing.allocator;
    const Case = struct { text: []const u8, parts: ?[]const u8 = null, metadata_only: bool = false, images: []const []const u8, prompt: ?[]const u8 = null };
    for ([_]Case{
        .{ .text = "file:///fallback", .parts = "[{\"type\":\"text\",\"text\":\"prompt\"},{\"type\":\"media\",\"url\":\"https://example.org/image\"}]", .images = &.{"https://example.org/image"}, .prompt = "prompt" },
        .{ .text = "https://example.org/image", .parts = "[{\"type\":\"text\",\"text\":\"prompt\"}]", .images = &.{"https://example.org/image"} },
        .{ .text = "file:///ignored", .parts = "[{\"type\":\"text\",\"text\":\"prompt\"}]", .metadata_only = true, .images = &.{}, .prompt = "prompt" },
        .{ .text = "[\"https://example.org/image\",null,12,{\"uri\":\"file:///ignored\"},[\"file:///ignored\"]]", .images = &.{"https://example.org/image"} },
        .{ .text = "\"file:///image\"", .images = &.{"file:///image"} },
        .{ .text = "[]", .images = &.{} },
        .{ .text = "[invalid JSON", .images = &.{"[invalid JSON"} },
    }) |case| {
        var source = try parse(a, case.text, case.parts, case.metadata_only);
        defer source.deinit(a);
        try std.testing.expectEqual(case.images.len, source.images.len);
        for (case.images, source.images) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
        if (case.prompt) |expected| {
            try std.testing.expectEqualStrings(expected, source.prompt.?);
        } else try std.testing.expect(source.prompt == null);
    }
}

test "backup preflight reader JSON templates preserve unknown string positions and reject unknown structure" {
    const a = std.testing.allocator;
    const Case = struct { text: []const u8, expected: ?[]const []const u8 };
    for ([_]Case{
        .{ .text = "[\"file:///static\",\"https://example.org/\x00\"]", .expected = &.{ "file:///static", "https://example.org/\x00" } },
        .{ .text = "[\"https://\x00\",\"file:///static\"]", .expected = &.{ "https://\x00", "file:///static" } },
        .{ .text = "\"https:\\/\\/localhost\\u002f\x00\"", .expected = &.{"https://localhost/\x00"} },
        .{ .text = "[\"https://\x00:\x00/\x00\"]", .expected = &.{"https://\x00:\x00/\x00"} },
        .{ .text = "[{\"url\":\"file:///ignored/\x00\"},123,null,\"https://example.org/static\"]", .expected = &.{"https://example.org/static"} },
        .{ .text = "[\x00,\"file:///static\"]", .expected = null },
        .{ .text = "\"file:///\\\x00\"", .expected = null },
        .{ .text = "\"file:///\\u00\x00\"", .expected = null },
        .{ .text = "[\"file:///\x00\"", .expected = null },
    }) |case| {
        var parsed = try parseTemplate(a, case.text, 4096);
        defer if (parsed) |*source| source.deinit(a);
        if (case.expected) |expected| {
            const source = parsed orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(expected.len, source.images.len);
            for (expected, source.images) |want, actual| try std.testing.expectEqualStrings(want, actual);
        } else try std.testing.expect(parsed == null);
    }
    try std.testing.expectError(error.StreamTooLong, parseTemplate(a, "\"\x00\"", 7));
}
