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
    return chunkInputBounded(alloc, input, cfg, types.default_max_chunk_owned_output_bytes);
}

/// Chunk an input while bounding bytes owned by transformed binary results.
/// Pass-through media remains borrowed from the input and therefore does not
/// consume this output budget.
pub fn chunkInputBounded(
    alloc: Allocator,
    input: types.Input,
    cfg: types.FixedChunkConfig,
    max_owned_output_bytes: usize,
) ![]types.Chunk {
    try cfg.validate();
    return switch (input) {
        .text => |text| fixed_text.chunkText(alloc, text, .{
            .target_tokens = cfg.text.target_tokens,
            .overlap_tokens = cfg.text.overlap_tokens,
            .max_chunks = cfg.max_chunks,
            .separator = cfg.text.separator,
        }),
        .binary => |binary| chunkBinary(alloc, binary, cfg, max_owned_output_bytes),
    };
}

fn chunkBinary(alloc: Allocator, binary: types.BinaryInput, cfg: types.FixedChunkConfig, max_owned_output_bytes: usize) ![]types.Chunk {
    const mime_type = mimeEssence(binary.mime_type);
    if (std.ascii.eqlIgnoreCase(mime_type, "audio/wav") or
        std.ascii.eqlIgnoreCase(mime_type, "audio/x-wav"))
    {
        return try chunkWav(alloc, binary, cfg, max_owned_output_bytes);
    }
    if (std.ascii.eqlIgnoreCase(mime_type, "image/gif")) {
        return try chunkGif(alloc, binary, cfg, max_owned_output_bytes);
    }
    const chunks = try alloc.alloc(types.Chunk, 1);
    chunks[0] = types.Chunk.initBinary(0, binary.mime_type, binary.data);
    return chunks;
}

fn mimeEssence(content_type: []const u8) []const u8 {
    const separator = std.mem.indexOfScalar(u8, content_type, ';') orelse content_type.len;
    return std.mem.trim(u8, content_type[0..separator], " \t");
}

fn chunkWav(alloc: Allocator, binary: types.BinaryInput, cfg: types.FixedChunkConfig, max_owned_output_bytes: usize) ![]types.Chunk {
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
    var retained_bytes: usize = 0;
    while (offset < decoded.samples.len) : (offset += step_samples) {
        const end = @min(offset + window_samples, decoded.samples.len);
        const bytes_per_sample = @as(usize, decoded.format.bits_per_sample) / 8;
        const data_bytes = std.math.mul(usize, end - offset, bytes_per_sample) catch
            return error.ChunkOutputTooLarge;
        const encoded_bytes = std.math.add(usize, 44, data_bytes) catch
            return error.ChunkOutputTooLarge;
        retained_bytes = std.math.add(usize, retained_bytes, encoded_bytes) catch
            return error.ChunkOutputTooLarge;
        if (retained_bytes > max_owned_output_bytes) return error.ChunkOutputTooLarge;
        const wav_bytes = try wav.encodeMono(alloc, decoded.samples[offset..end], decoded.format);
        var chunk = types.Chunk.initOwnedBinary(@intCast(chunks.items.len), "audio/wav", wav_bytes);
        chunk.start_time_ms = @as(f32, @floatFromInt(offset)) * 1000.0 / @as(f32, @floatFromInt(decoded.format.sample_rate));
        chunk.end_time_ms = @as(f32, @floatFromInt(end)) * 1000.0 / @as(f32, @floatFromInt(decoded.format.sample_rate));
        try chunks.append(alloc, chunk);
        if (cfg.max_chunks > 0 and chunks.items.len >= cfg.max_chunks) break;
    }

    return try chunks.toOwnedSlice(alloc);
}

fn chunkGif(alloc: Allocator, binary: types.BinaryInput, cfg: types.FixedChunkConfig, max_owned_output_bytes: usize) ![]types.Chunk {
    const max_frames = if (cfg.max_chunks > 0) cfg.max_chunks else (types.FixedChunkConfig{}).max_chunks;
    const frames = antfly_image.gif.decodeFramesAllocBounded(
        alloc,
        binary.data,
        max_frames,
        antfly_image.DecodeLimits.inference_default.max_rgba_bytes,
    ) catch |err| switch (err) {
        error.UnsupportedGifFormat, error.GifDecodeFailed, error.ImageTooLarge => return error.ImageDecodeFailed,
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

    var retained_bytes: usize = 0;
    for (frames, 0..) |frame, i| {
        const encoded_bytes = png.encodedRgbaSize(frame.width, frame.height) catch
            return error.ChunkOutputTooLarge;
        retained_bytes = std.math.add(usize, retained_bytes, encoded_bytes) catch
            return error.ChunkOutputTooLarge;
        if (retained_bytes > max_owned_output_bytes) return error.ChunkOutputTooLarge;
        const png_bytes = try png.encodeRgba(alloc, frame.width, frame.height, frame.rgba);
        var chunk = types.Chunk.initOwnedBinary(@intCast(i), "image/png", png_bytes);
        chunk.frame_index = @intCast(i);
        chunk.frame_delay_ms = frame.delay_ms;
        try chunks.append(alloc, chunk);
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

    try std.testing.expectError(
        error.ChunkOutputTooLarge,
        chunkInputBounded(alloc, .{ .binary = .{
            .mime_type = "audio/wav",
            .data = wav_bytes,
        } }, .{
            .audio = .{ .window_duration_ms = 3, .overlap_duration_ms = 1 },
            .max_chunks = 8,
        }, 1),
    );
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

    const first_only = try chunkInput(alloc, .{ .binary = .{
        .mime_type = "image/gif",
        .data = &gif_bytes,
    } }, .{ .max_chunks = 1 });
    defer types.freeChunks(alloc, first_only);
    try std.testing.expectEqual(@as(usize, 1), first_only.len);

    try std.testing.expectError(
        error.ChunkOutputTooLarge,
        chunkInputBounded(alloc, .{ .binary = .{
            .mime_type = "image/gif",
            .data = &gif_bytes,
        } }, .{ .max_chunks = 1 }, 1),
    );
}
