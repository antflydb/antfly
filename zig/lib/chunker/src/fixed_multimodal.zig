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
const antfly_image = @import("antfly_image");
const fixed_text = @import("fixed_text.zig");
const types = @import("types.zig");
const wav = @import("wav.zig");
const png = @import("png.zig");

const Allocator = std.mem.Allocator;

pub fn chunkInput(alloc: Allocator, input: types.Input, cfg: types.FixedChunkConfig) ![]types.Chunk {
    return switch (input) {
        .text => |text| fixed_text.chunkText(alloc, text, .{
            .target_tokens = cfg.text.target_tokens,
            .overlap_tokens = cfg.text.overlap_tokens,
            .max_chunks = cfg.max_chunks,
            .separator = cfg.text.separator,
        }),
        .binary => |binary| chunkBinary(alloc, binary, cfg),
    };
}

fn chunkBinary(alloc: Allocator, binary: types.BinaryInput, cfg: types.FixedChunkConfig) ![]types.Chunk {
    if (std.mem.eql(u8, binary.mime_type, "audio/wav")) {
        return try chunkWav(alloc, binary, cfg);
    }
    if (std.mem.eql(u8, binary.mime_type, "image/gif")) {
        return try chunkGif(alloc, binary, cfg);
    }
    const chunks = try alloc.alloc(types.Chunk, 1);
    chunks[0] = types.Chunk.initBinary(0, binary.mime_type, binary.data);
    return chunks;
}

fn chunkWav(alloc: Allocator, binary: types.BinaryInput, cfg: types.FixedChunkConfig) ![]types.Chunk {
    const decoded = try wav.decodeMono(alloc, binary.data);
    defer alloc.free(decoded.samples);

    const window_ms: usize = if (cfg.audio.window_duration_ms > 0) cfg.audio.window_duration_ms else 30_000;
    const overlap_ms: usize = cfg.audio.overlap_duration_ms;
    const window_samples = (@as(usize, decoded.format.sample_rate) * window_ms) / 1000;
    const overlap_samples = (@as(usize, decoded.format.sample_rate) * overlap_ms) / 1000;
    if (window_samples == 0) return error.InvalidAudioWindow;
    if (overlap_samples >= window_samples) return error.InvalidAudioOverlap;
    const step_samples = window_samples - overlap_samples;

    var chunks = std.ArrayListUnmanaged(types.Chunk).empty;
    errdefer {
        for (chunks.items) |*chunk| chunk.deinit(alloc);
        chunks.deinit(alloc);
    }

    var offset: usize = 0;
    while (offset < decoded.samples.len) : (offset += step_samples) {
        const end = @min(offset + window_samples, decoded.samples.len);
        const wav_bytes = try wav.encodeMono(alloc, decoded.samples[offset..end], decoded.format);
        var chunk = types.Chunk.initOwnedBinary(@intCast(chunks.items.len), "audio/wav", wav_bytes);
        chunk.start_time_ms = @as(f32, @floatFromInt(offset)) * 1000.0 / @as(f32, @floatFromInt(decoded.format.sample_rate));
        chunk.end_time_ms = @as(f32, @floatFromInt(end)) * 1000.0 / @as(f32, @floatFromInt(decoded.format.sample_rate));
        try chunks.append(alloc, chunk);
        if (cfg.max_chunks > 0 and chunks.items.len >= cfg.max_chunks) break;
    }

    return try chunks.toOwnedSlice(alloc);
}

fn chunkGif(alloc: Allocator, binary: types.BinaryInput, cfg: types.FixedChunkConfig) ![]types.Chunk {
    const frames = antfly_image.gif.decodeFramesAlloc(alloc, binary.data) catch |err| switch (err) {
        error.UnsupportedGifFormat, error.GifDecodeFailed => return error.ImageDecodeFailed,
        else => return err,
    };
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    var chunks = std.ArrayListUnmanaged(types.Chunk).empty;
    errdefer {
        for (chunks.items) |*chunk| chunk.deinit(alloc);
        chunks.deinit(alloc);
    }

    for (frames, 0..) |frame, i| {
        const png_bytes = try png.encodeRgba(alloc, frame.width, frame.height, frame.rgba);
        var chunk = types.Chunk.initOwnedBinary(@intCast(i), "image/png", png_bytes);
        chunk.frame_index = @intCast(i);
        chunk.frame_delay_ms = frame.delay_ms;
        try chunks.append(alloc, chunk);
        if (cfg.max_chunks > 0 and chunks.items.len >= cfg.max_chunks) break;
    }

    return try chunks.toOwnedSlice(alloc);
}

test "fixed multimodal chunks wav windows" {
    const alloc = std.testing.allocator;
    const wav_bytes = try wav.encodeMono(alloc, &.{ 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 }, .{
        .audio_format = 1,
        .sample_rate = 1000,
        .bits_per_sample = 16,
    });
    defer alloc.free(wav_bytes);

    const chunks = try chunkInput(alloc, .{ .binary = .{
        .mime_type = "audio/wav",
        .data = wav_bytes,
    } }, .{
        .audio = .{ .window_duration_ms = 3, .overlap_duration_ms = 1 },
        .max_chunks = 8,
    });
    defer types.freeChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expectEqualStrings("audio/wav", chunks[0].mime_type);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), chunks[0].start_time_ms.?, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), chunks[0].end_time_ms.?, 0.001);
}

test "fixed multimodal chunks animated gif frames" {
    const alloc = std.testing.allocator;
    const gif_hex = "4749463839610100010000000021ff0b4e45545343415045322e30030100000021f90401050000002c000000000100010081000000ff000000ff0000000002024c010021f90401070000002c000000000100010081000000ff000000ff0000000002025401003b";
    var gif_bytes: [gif_hex.len / 2]u8 = undefined;
    _ = try std.fmt.hexToBytes(&gif_bytes, gif_hex);

    const chunks = try chunkInput(alloc, .{ .binary = .{
        .mime_type = "image/gif",
        .data = &gif_bytes,
    } }, .{ .max_chunks = 8 });
    defer types.freeChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqualStrings("image/png", chunks[0].mime_type);
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].frame_index);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, chunks[0].data.?[0..8]);
    try std.testing.expectEqual(@as(?u32, 1), chunks[1].frame_index);
    try std.testing.expect(chunks[1].frame_delay_ms != null and chunks[1].frame_delay_ms.? > 0);
}
