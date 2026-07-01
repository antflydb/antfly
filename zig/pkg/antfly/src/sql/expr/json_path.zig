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

pub fn segmentsToDottedPathAlloc(alloc: std.mem.Allocator, segments: []const []const u8) ![]const u8 {
    if (segments.len == 0) return error.UnsupportedSqlShape;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (segments, 0..) |segment, i| {
        if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '.') != null) return error.UnsupportedSqlShape;
        if (i != 0) try out.append(alloc, '.');
        try out.appendSlice(alloc, segment);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parsePostgresTextAlloc(alloc: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    if (path.len < 3 or path[0] != '{' or path[path.len - 1] != '}') return error.UnsupportedSqlShape;
    const inner = path[1 .. path.len - 1];
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |segment| alloc.free(segment);
        out.deinit(alloc);
    }
    var parts = std.mem.splitScalar(u8, inner, ',');
    while (parts.next()) |part| {
        if (part.len == 0 or std.mem.indexOfScalar(u8, part, '.') != null) return error.UnsupportedSqlShape;
        const segment = try alloc.dupe(u8, part);
        var segment_transferred = false;
        errdefer if (!segment_transferred) alloc.free(segment);
        try out.append(alloc, segment);
        segment_transferred = true;
    }
    if (out.items.len == 0) return error.UnsupportedSqlShape;
    return try out.toOwnedSlice(alloc);
}

pub fn parsePostgresJsonArrayAlloc(alloc: std.mem.Allocator, path_json: []const u8) ![]const []const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, path_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |segment| alloc.free(segment);
        out.deinit(alloc);
    }
    for (parsed.value.array.items) |item| {
        if (item != .string or item.string.len == 0 or std.mem.indexOfScalar(u8, item.string, '.') != null) return error.UnsupportedSqlShape;
        const segment = try alloc.dupe(u8, item.string);
        var segment_transferred = false;
        errdefer if (!segment_transferred) alloc.free(segment);
        try out.append(alloc, segment);
        segment_transferred = true;
    }
    return try out.toOwnedSlice(alloc);
}

test "sql expr_json_path parses postgres paths" {
    const alloc = std.testing.allocator;

    const text_segments = try parsePostgresTextAlloc(alloc, "{metadata,source}");
    defer {
        for (text_segments) |segment| alloc.free(segment);
        alloc.free(text_segments);
    }
    try std.testing.expectEqual(@as(usize, 2), text_segments.len);
    try std.testing.expectEqualStrings("metadata", text_segments[0]);
    try std.testing.expectEqualStrings("source", text_segments[1]);

    const dotted = try segmentsToDottedPathAlloc(alloc, text_segments);
    defer alloc.free(dotted);
    try std.testing.expectEqualStrings("metadata.source", dotted);

    const json_segments = try parsePostgresJsonArrayAlloc(alloc, "[\"metadata\",\"source\"]");
    defer {
        for (json_segments) |segment| alloc.free(segment);
        alloc.free(json_segments);
    }
    try std.testing.expectEqualStrings("metadata", json_segments[0]);
    try std.testing.expectEqualStrings("source", json_segments[1]);

    try std.testing.expectError(error.UnsupportedSqlShape, parsePostgresTextAlloc(alloc, "{}"));
    try std.testing.expectError(error.UnsupportedSqlShape, parsePostgresJsonArrayAlloc(alloc, "[\"metadata.source\"]"));
    try std.testing.expectError(error.UnsupportedSqlShape, segmentsToDottedPathAlloc(alloc, &.{"metadata.source"}));
}
