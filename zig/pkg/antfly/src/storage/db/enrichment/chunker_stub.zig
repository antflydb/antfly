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
    var cfg = try chunking_types.parseConfigFromSlice(alloc, config_json);
    defer cfg.deinit(alloc);

    return switch (cfg.provider) {
        .antfly, .mock => try chunkText(
            alloc,
            text,
            cfg.defaultedTargetTokens(),
            cfg.defaultedOverlapTokens(),
        ),
        .termite => error.UnsupportedPlatform,
    };
}

pub fn freeChunks(alloc: Allocator, chunks: []Chunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
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

test "chunker stub rejects termite configured chunking" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(
        error.UnsupportedPlatform,
        chunkTextWithConfigJson(alloc, "abcdefghij",
            \\{"provider":"termite","model":"fixed"}
        ),
    );
}
