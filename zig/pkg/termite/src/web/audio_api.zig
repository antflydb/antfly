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
const audio = @import("../pipelines/audio.zig");

pub fn whisperMel(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) !u32 {
    const mel = try audio.whisperMelFromPcm(allocator, samples, sample_rate);
    defer allocator.free(mel);

    @memcpy(out_ptr[0..mel.len], mel);
    out_meta_ptr[0] = audio.WHISPER_N_MELS;
    out_meta_ptr[1] = audio.WHISPER_N_FRAMES;
    return @intCast(mel.len);
}

pub fn whisperMelInterleaved(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    input_channels: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) !u32 {
    const channels: u8 = if (input_channels == 0) 1 else @intCast(input_channels);
    const mono = try audio.downmixToMono(allocator, samples, channels);
    defer allocator.free(mono);

    return whisperMel(allocator, mono, sample_rate, out_ptr, out_meta_ptr);
}

pub fn clapFeatures(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    channels: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) !u32 {
    const requested_channels: usize = if (channels == 0) 1 else @intCast(channels);
    var features = try audio.clapFeaturesFromPcm(allocator, samples, sample_rate, requested_channels);
    defer features.deinit();

    @memcpy(out_ptr[0..features.data.len], features.data);
    out_meta_ptr[0] = @intCast(features.channels);
    out_meta_ptr[1] = @intCast(features.time_frames);
    out_meta_ptr[2] = @intCast(features.mel_bins);
    out_meta_ptr[3] = if (features.is_longer) 1 else 0;
    return @intCast(features.data.len);
}

pub fn clapFeaturesInterleaved(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    input_channels: u32,
    output_channels: u32,
    out_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) !u32 {
    const channels: u8 = if (input_channels == 0) 1 else @intCast(input_channels);
    const mono = try audio.downmixToMono(allocator, samples, channels);
    defer allocator.free(mono);

    return clapFeatures(allocator, mono, sample_rate, output_channels, out_ptr, out_meta_ptr);
}
