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
    owns_text: bool = false,
    owns_data: bool = false,

    pub fn initText(id: u32, text: []const u8, start: usize, end: usize) Chunk {
        return .{
            .id = id,
            .mime_type = "text/plain",
            .text = text,
            .start_char = @intCast(start),
            .end_char = @intCast(end),
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
        if (self.owns_text and self.text != null) alloc.free(self.text.?);
        if (self.owns_data and self.data != null) alloc.free(self.data.?);
        self.* = undefined;
    }
};

pub const FixedTextConfig = struct {
    target_tokens: usize = 500,
    overlap_tokens: usize = 50,
    max_chunks: usize = 50,
    separator: []const u8 = "\n\n",
};

pub const AudioChunkOptions = struct {
    window_duration_ms: usize = 30_000,
    overlap_duration_ms: usize = 0,
};

pub const FixedChunkConfig = struct {
    model: []const u8 = "fixed-bert-tokenizer",
    max_chunks: usize = 50,
    threshold: ?f32 = null,
    text: FixedTextConfig = .{},
    audio: AudioChunkOptions = .{},
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
