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

pub const max_chunk_results: usize = 4096;
pub const max_chunk_target_tokens: usize = 1_000_000;
pub const max_chunk_audio_window_ms: usize = 24 * 60 * 60 * 1000;
/// Frame-count ceiling for GIF chunking when the caller leaves `max_chunks`
/// unset. Animated GIFs decode every frame to raw RGBA before chunking, so
/// unlike text (already fully resident, and now unbounded by default) this
/// number bounds real decode work/memory — a decompression-bomb guard, not
/// the "silent truncation" default this removes for text. Kept as its own
/// named constant (rather than reading `FixedChunkConfig{}.max_chunks`) so
/// the two defaults can no longer drift together by accident.
pub const default_gif_max_frames: usize = 50;
/// Hard retained-output ceiling used by direct/local fixed multimodal calls.
/// HTTP callers may select a smaller request-scoped ceiling.
pub const default_max_chunk_owned_output_bytes: usize = 100 * 1024 * 1024;

pub const Chunk = struct {
    id: u32,
    mime_type: []const u8,
    text: ?[]const u8 = null,
    start_char: ?u32 = null,
    end_char: ?u32 = null,
    data: ?[]const u8 = null,
    start_time_ms: ?f32 = null,
    end_time_ms: ?f32 = null,
    frame_index: ?u32 = null,
    frame_delay_ms: ?u32 = null,
    owns_mime_type: bool = false,
    owns_text: bool = false,
    owns_data: bool = false,

    pub fn initText(id: u32, text: []const u8, start: usize, end: usize) Chunk {
        return .{
            .id = id,
            .mime_type = "text/plain",
            .text = text,
            .start_char = std.math.cast(u32, start),
            .end_char = std.math.cast(u32, end),
        };
    }

    pub fn initBinary(id: u32, mime_type: []const u8, data: []const u8) Chunk {
        return .{
            .id = id,
            .mime_type = mime_type,
            .data = data,
        };
    }

    pub fn initOwnedBinary(id: u32, mime_type: []const u8, data: []const u8) Chunk {
        return .{
            .id = id,
            .mime_type = mime_type,
            .data = data,
            .owns_data = true,
        };
    }

    pub fn deinit(self: *Chunk, alloc: std.mem.Allocator) void {
        if (self.owns_mime_type) alloc.free(@constCast(self.mime_type));
        if (self.owns_text and self.text != null) alloc.free(self.text.?);
        if (self.owns_data and self.data != null) alloc.free(self.data.?);
        self.* = undefined;
    }
};

pub const FixedTextConfig = struct {
    target_tokens: usize = 500,
    overlap_tokens: usize = 50,
    /// 0 = unlimited: chunk the entire unit. An explicit value is still
    /// bounded by `max_chunk_results` in `validate()`.
    max_chunks: usize = 0,
    separator: []const u8 = "\n\n",

    pub fn validate(self: @This()) !void {
        const target = if (self.target_tokens > 0) self.target_tokens else 500;
        if (target > max_chunk_target_tokens) return error.InvalidChunkTargetTokens;
        if (self.overlap_tokens > max_chunk_target_tokens or self.overlap_tokens >= target)
            return error.InvalidChunkOverlapTokens;
        if (self.max_chunks > max_chunk_results) return error.InvalidMaxChunks;
    }
};

pub const AudioChunkOptions = struct {
    window_duration_ms: usize = 30_000,
    overlap_duration_ms: usize = 0,

    pub fn validate(self: @This()) !void {
        const window = if (self.window_duration_ms > 0) self.window_duration_ms else 30_000;
        if (window > max_chunk_audio_window_ms) return error.InvalidChunkAudioWindow;
        if (self.overlap_duration_ms > max_chunk_audio_window_ms or
            self.overlap_duration_ms >= window)
            return error.InvalidChunkAudioOverlap;
    }
};

pub const FixedChunkConfig = struct {
    model: []const u8 = "fixed",
    /// 0 = unlimited for text (see `FixedTextConfig.max_chunks`) and for WAV
    /// windows (`chunkWav` already only enforces this when > 0). GIF frame
    /// decoding uses the dedicated `default_gif_max_frames` instead of this
    /// default — see that constant's doc comment.
    max_chunks: usize = 0,
    threshold: ?f32 = null,
    text: FixedTextConfig = .{},
    audio: AudioChunkOptions = .{},

    pub fn validate(self: @This()) !void {
        if (self.max_chunks > max_chunk_results) return error.InvalidMaxChunks;
        if (self.threshold) |value| {
            if (!std.math.isFinite(value) or value < 0 or value > 1)
                return error.InvalidChunkThreshold;
        }
        var text = self.text;
        text.max_chunks = self.max_chunks;
        try text.validate();
        try self.audio.validate();
    }
};

pub const BinaryInput = struct {
    mime_type: []const u8,
    data: []const u8,
};

pub const Input = union(enum) {
    text: []const u8,
    binary: BinaryInput,
};

pub fn freeChunks(alloc: std.mem.Allocator, chunks: []Chunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
}

test "text chunk offsets are nullable when they exceed u32 range" {
    const too_large = @as(usize, std.math.maxInt(u32)) + 1;
    const chunk = Chunk.initText(0, "", too_large, too_large + 1);
    try std.testing.expectEqual(@as(?u32, null), chunk.start_char);
    try std.testing.expectEqual(@as(?u32, null), chunk.end_char);
}
