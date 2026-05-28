// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const chunker = @import("termite_chunker");

pub const Chunk = struct {
    text: []const u8,
    start: usize,
    end: usize,
};

pub const ChunkingConfig = struct {
    /// Target token count per chunk.
    target_tokens: usize = 500,
    /// Overlap between adjacent chunks in tokens.
    overlap_tokens: usize = 50,
    /// Maximum number of chunks to return (0 = default chunker limit).
    max_chunks: usize = 0,
    /// Separator to prefer before falling back to finer token windows.
    separator: []const u8 = "\n\n",
};

pub const ChunkingPipeline = struct {
    allocator: std.mem.Allocator,
    config: ChunkingConfig,

    pub fn init(allocator: std.mem.Allocator, config: ChunkingConfig) ChunkingPipeline {
        return .{
            .allocator = allocator,
            .config = config,
        };
    }

    pub fn chunk(self: *ChunkingPipeline, text: []const u8) ![]Chunk {
        const fixed_chunks = try chunker.fixed_text.chunkText(self.allocator, text, .{
            .target_tokens = self.config.target_tokens,
            .overlap_tokens = self.config.overlap_tokens,
            .max_chunks = self.config.max_chunks,
            .separator = self.config.separator,
        });
        defer self.allocator.free(fixed_chunks);

        const chunks = try self.allocator.alloc(Chunk, fixed_chunks.len);
        for (fixed_chunks, 0..) |chunk_item, i| {
            chunks[i] = .{
                .text = chunk_item.text.?,
                .start = chunk_item.start_char.?,
                .end = chunk_item.end_char.?,
            };
        }
        return chunks;
    }
};

test "empty text" {
    const allocator = std.testing.allocator;
    var pipeline = ChunkingPipeline.init(allocator, .{});
    const chunks = try pipeline.chunk("");
    defer allocator.free(chunks);
    try std.testing.expectEqual(@as(usize, 0), chunks.len);
}

test "short text" {
    const allocator = std.testing.allocator;
    var pipeline = ChunkingPipeline.init(allocator, .{ .target_tokens = 100 });
    const text = "Hello world.";
    const chunks = try pipeline.chunk(text);
    defer allocator.free(chunks);
    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expectEqualStrings("Hello world.", chunks[0].text);
}

test "token-based chunking with overlap" {
    const allocator = std.testing.allocator;
    var pipeline = ChunkingPipeline.init(allocator, .{
        .target_tokens = 4,
        .overlap_tokens = 1,
        .separator = " ",
    });
    const text = "alpha beta gamma delta epsilon zeta eta theta";
    const chunks = try pipeline.chunk(text);
    defer allocator.free(chunks);
    try std.testing.expect(chunks.len >= 2);
    try std.testing.expectEqual(@as(usize, 0), chunks[0].start);
}
