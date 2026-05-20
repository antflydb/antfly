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
const chunking_types = @import("types.zig");
const Chunk = @import("chunk.zig").Chunk;

const termite_chunker = @import("termite_chunker");

const Allocator = std.mem.Allocator;

pub fn chunkText(alloc: Allocator, text: []const u8, cfg: chunking_types.Config) ![]Chunk {
    const shared_chunks = try termite_chunker.fixed_text.chunkText(alloc, text, .{
        .target_tokens = cfg.defaultedTargetTokens(),
        .overlap_tokens = cfg.defaultedOverlapTokens(),
        .max_chunks = if (cfg.max_chunks > 0) cfg.max_chunks else 50,
        .separator = cfg.defaultedSeparator(),
    });
    defer termite_chunker.types.freeChunks(alloc, shared_chunks);

    var chunks = try alloc.alloc(Chunk, shared_chunks.len);
    errdefer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }

    for (shared_chunks, 0..) |shared, i| {
        if (!std.mem.eql(u8, shared.mime_type, "text/plain")) return error.UnsupportedChunkMediaType;
        const shared_text = shared.text orelse return error.InvalidChunkerResponse;
        chunks[i] = .{
            .chunk_id = shared.id,
            .text = try alloc.dupe(u8, shared_text),
            .start_offset = shared.start_char,
            .end_offset = shared.end_char orelse std.math.cast(u32, shared_text.len),
        };
    }

    return chunks;
}

pub fn freeChunks(alloc: Allocator, chunks: []Chunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
}

test "fixed chunker splits by token target" {
    const alloc = std.testing.allocator;
    const cfg = chunking_types.Config{
        .provider = .antfly,
        .text = .{ .target_tokens = 4, .separator = "\n\n" },
    };
    const text =
        \\alpha beta gamma delta
        \\
        \\epsilon zeta eta theta
    ;
    const chunks = try chunkText(alloc, text, cfg);
    defer freeChunks(alloc, chunks);
    try std.testing.expect(chunks.len >= 2);
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].start_offset);
}
