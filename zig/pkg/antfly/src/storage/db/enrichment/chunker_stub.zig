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
const chunk_mod = @import("../../../chunking/chunk.zig");
const chunking_types = @import("../../../chunking/types.zig");
const utf8_text = @import("utf8_text.zig");

const Allocator = std.mem.Allocator;

pub const Chunk = chunk_mod.Chunk;

pub fn chunkText(
    alloc: Allocator,
    text: []const u8,
    chunk_size: u32,
    chunk_overlap: u32,
) ![]Chunk {
    if (chunk_size == 0) return &.{};
    if (chunk_overlap >= chunk_size) return error.InvalidChunkOverlap;

    var sanitized = try utf8_text.sanitizeAlloc(alloc, text, "enrichment chunker");
    defer sanitized.deinit(alloc);
    const source_text = sanitized.text;

    var chunks = std.ArrayListUnmanaged(Chunk).empty;
    errdefer {
        for (chunks.items) |*chunk| chunk.deinit(alloc);
        chunks.deinit(alloc);
    }

    var chunk_id: u32 = 0;
    const step = chunk_size - chunk_overlap;
    var start: usize = 0;
    while (start < source_text.len) {
        start = utf8_text.snapToBoundary(source_text, start, .backward);
        if (start >= source_text.len) break;
        const raw_end = @min(start + chunk_size, source_text.len);
        const backward_end = utf8_text.snapToBoundary(source_text, raw_end, .backward);
        const end = if (backward_end > start) backward_end else utf8_text.snapToBoundary(source_text, raw_end, .forward);
        if (end <= start) break;
        try chunks.append(alloc, .{
            .chunk_id = chunk_id,
            .text = try alloc.dupe(u8, source_text[start..end]),
            .start_offset = mapOffset(&sanitized, start, .start),
            .end_offset = mapOffset(&sanitized, end, .end),
        });
        chunk_id += 1;
        if (end == source_text.len) break;
        const previous_start = start;
        const raw_next = previous_start + step;
        var next = utf8_text.snapToBoundary(source_text, raw_next, .backward);
        if (next <= previous_start) next = utf8_text.snapToBoundary(source_text, raw_next, .forward);
        start = if (next > previous_start) next else end;
    }

    return try chunks.toOwnedSlice(alloc);
}

pub fn chunkTextWithConfigJson(
    alloc: Allocator,
    text: []const u8,
    config_json: []const u8,
) ![]Chunk {
    var cfg = try chunking_types.parseConfigFromSlice(alloc, config_json);
    defer cfg.deinit(alloc);

    var sanitized = try utf8_text.sanitizeAlloc(alloc, text, "configured enrichment chunker");
    defer sanitized.deinit(alloc);
    const source_text = sanitized.text;

    const chunks = try switch (cfg.provider) {
        .mock => chunkText(
            alloc,
            source_text,
            cfg.defaultedTargetTokens(),
            cfg.defaultedOverlapTokens(),
        ),
        .antfly => if (cfg.model.len == 0)
            chunkText(
                alloc,
                source_text,
                cfg.defaultedTargetTokens(),
                cfg.defaultedOverlapTokens(),
            )
        else
            error.UnsupportedPlatform,
    };
    errdefer freeChunks(alloc, chunks);
    remapChunkOffsets(&sanitized, chunks);
    return chunks;
}

pub fn chunkTextWithConfigJsonAndProvider(
    alloc: Allocator,
    text: []const u8,
    config_json: []const u8,
    _: anytype,
) ![]Chunk {
    return try chunkTextWithConfigJson(alloc, text, config_json);
}

pub fn freeChunks(alloc: Allocator, chunks: []Chunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
}

fn remapChunkOffsets(sanitized: *const utf8_text.SanitizedText, chunks: []Chunk) void {
    for (chunks) |*chunk| {
        if (chunk.start_offset) |start| chunk.start_offset = mapU32Offset(sanitized, start, .start);
        if (chunk.end_offset) |end| chunk.end_offset = mapU32Offset(sanitized, end, .end);
    }
}

fn mapOffset(sanitized: *const utf8_text.SanitizedText, offset: usize, boundary: utf8_text.SourceBoundary) ?u32 {
    const checked = std.math.cast(u32, offset) orelse return null;
    return mapU32Offset(sanitized, checked, boundary);
}

fn mapU32Offset(sanitized: *const utf8_text.SanitizedText, offset: u32, boundary: utf8_text.SourceBoundary) u32 {
    if (sanitized.source_map) |source_map| return source_map.mapBoundary(offset, boundary);
    return offset;
}

test "chunker stub supports local configured chunking" {
    const alloc = std.testing.allocator;

    const chunks = try chunkTextWithConfigJson(alloc, "abcdefghij",
        \\{"provider":"antfly","text":{"target_tokens":4,"overlap_tokens":1}}
    );
    defer freeChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expectEqualStrings("abcd", chunks[0].text.?);
    try std.testing.expectEqualStrings("defg", chunks[1].text.?);
    try std.testing.expectEqualStrings("ghij", chunks[2].text.?);
}

test "enrichment chunker stub preserves utf8 boundaries across default byte windows" {
    const alloc = std.testing.allocator;

    var text = try alloc.alloc(u8, 520);
    defer alloc.free(text);
    @memset(text, 'a');
    text[447] = 0xc2;
    text[448] = 0xa0;

    const chunks = try chunkText(alloc, text, 512, 64);
    defer freeChunks(alloc, chunks);

    try std.testing.expect(chunks.len >= 2);
    for (chunks) |chunk| {
        try std.testing.expect(std.unicode.utf8ValidateSlice(chunk.text.?));
    }
    try std.testing.expectEqual(@as(?u32, 447), chunks[1].start_offset);
}

test "enrichment chunker stub makes progress when byte window is smaller than utf8 scalar" {
    const alloc = std.testing.allocator;

    const chunks = try chunkText(alloc, "\xc3\xa9x", 1, 0);
    defer freeChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqualStrings("\xc3\xa9", chunks[0].text.?);
    try std.testing.expect(std.unicode.utf8ValidateSlice(chunks[0].text.?));
}

test "enrichment chunker stub replaces invalid utf8 before storing chunk text" {
    const alloc = std.testing.allocator;
    const repairs_before = utf8_text.invalidUtf8RepairCount();

    const chunks = try chunkText(alloc, "bad\xc2 text", 512, 64);
    defer freeChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expect(std.unicode.utf8ValidateSlice(chunks[0].text.?));
    try std.testing.expect(std.mem.indexOf(u8, chunks[0].text.?, &std.unicode.replacement_character_utf8) != null);
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].start_offset);
    try std.testing.expectEqual(@as(?u32, 9), chunks[0].end_offset);
    try std.testing.expect(utf8_text.invalidUtf8RepairCount() > repairs_before);
}

test "enrichment chunker stub configured chunking replaces invalid utf8 before dispatch" {
    const alloc = std.testing.allocator;
    const repairs_before = utf8_text.invalidUtf8RepairCount();

    const chunks = try chunkTextWithConfigJson(alloc, "bad\xc2 text",
        \\{"provider":"antfly","text":{"target_tokens":512,"overlap_tokens":0}}
    );
    defer freeChunks(alloc, chunks);

    try std.testing.expect(chunks.len > 0);
    for (chunks) |chunk| {
        try std.testing.expect(std.unicode.utf8ValidateSlice(chunk.text.?));
    }
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].start_offset);
    try std.testing.expectEqual(@as(?u32, 9), chunks[0].end_offset);
    try std.testing.expect(utf8_text.invalidUtf8RepairCount() > repairs_before);
}

test "chunker stub rejects antfly configured chunking" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(
        error.UnsupportedPlatform,
        chunkTextWithConfigJson(alloc, "abcdefghij",
            \\{"provider":"antfly","model":"fixed"}
        ),
    );
}
