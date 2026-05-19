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
const chunking = @import("../../../chunking/mod.zig");

pub const Chunk = chunking.chunk.Chunk;

pub fn chunkText(
    alloc: Allocator,
    text: []const u8,
    chunk_size: u32,
    chunk_overlap: u32,
) ![]Chunk {
    if (chunk_size == 0) return &.{};
    if (chunk_overlap >= chunk_size) return error.InvalidChunkOverlap;

    var chunks = std.ArrayListUnmanaged(Chunk).empty;
    errdefer {
        for (chunks.items) |*chunk| chunk.deinit(alloc);
        chunks.deinit(alloc);
    }

    var chunk_id: u32 = 0;
    const step = chunk_size - chunk_overlap;
    var start: usize = 0;
    while (start < text.len) : (start += step) {
        const end = @min(start + chunk_size, text.len);
        try chunks.append(alloc, .{
            .chunk_id = chunk_id,
            .text = try alloc.dupe(u8, text[start..end]),
            .start_offset = @intCast(start),
            .end_offset = @intCast(end),
        });
        chunk_id += 1;
        if (end == text.len) break;
    }

    return try chunks.toOwnedSlice(alloc);
}

pub fn chunkTextWithConfigJson(
    alloc: Allocator,
    text: []const u8,
    config_json: []const u8,
) ![]Chunk {
    var cfg = try chunking.types.parseConfigFromSlice(alloc, config_json);
    defer cfg.deinit(alloc);

    return switch (cfg.provider) {
        .antfly => blk: {
            var local_cfg = cfg;
            local_cfg.api_url = "";
            break :blk try chunking.termite.chunkText(alloc, local_cfg, text);
        },
        .mock => try chunking.fixed.chunkText(alloc, text, cfg),
        .termite => try chunking.termite.chunkText(alloc, cfg, text),
    };
}

pub fn freeChunks(alloc: Allocator, chunks: []Chunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
}

test "chunker splits overlapping windows" {
    const alloc = std.testing.allocator;

    const chunks = try chunkText(alloc, "abcdefghij", 4, 1);
    defer freeChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expectEqualStrings("abcd", chunks[0].text.?);
    try std.testing.expectEqualStrings("defg", chunks[1].text.?);
    try std.testing.expectEqualStrings("ghij", chunks[2].text.?);
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].start_offset);
    try std.testing.expectEqual(@as(?u32, 4), chunks[0].end_offset);
    try std.testing.expectEqual(@as(?u32, 3), chunks[1].start_offset);
}
