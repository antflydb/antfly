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
const generic = @import("termite_audio");

pub const wav = generic.wav;
pub const NormalizationType = generic.NormalizationType;
pub const AudioConfig = generic.AudioConfig;
pub const Audio = generic.Audio;
pub const AudioInterleaved = generic.AudioInterleaved;
pub const PcmAudio = generic.PcmAudio;
pub const PcmAudioInterleaved = generic.PcmAudioInterleaved;
pub const EncodedFormat = generic.EncodedFormat;
pub const DecodeOptions = generic.DecodeOptions;
pub const decodeWav = generic.decodeWav;
pub const detectFormat = generic.detectFormat;
pub const detectFormatFromMime = generic.detectFormatFromMime;
pub const detectFormatFromFilename = generic.detectFormatFromFilename;
pub const decode = generic.decode;
pub const decodeInterleaved = generic.decodeInterleaved;
pub const canDecodeFormat = generic.canDecodeFormat;
pub const canDecodeMime = generic.canDecodeMime;
pub const canDecodeFilename = generic.canDecodeFilename;
pub const canDecodeWithOptions = generic.canDecodeWithOptions;
pub const downmixToMono = generic.downmixToMono;
pub const resample = generic.resample;
pub const copyOrResample = generic.copyOrResample;
pub const logMelSpectrogramWithConfig = generic.logMelSpectrogramWithConfig;

pub const WHISPER_SAMPLE_RATE: u32 = 16000;
pub const WHISPER_N_FFT: u32 = 400;
pub const WHISPER_HOP_LENGTH: u32 = 160;
pub const WHISPER_N_MELS: u32 = 80;
pub const WHISPER_CHUNK_LENGTH: u32 = 30;
pub const WHISPER_N_FRAMES: u32 = 3000;

pub const WHISPER_CONFIG = AudioConfig{
    .sample_rate = WHISPER_SAMPLE_RATE,
    .n_fft = WHISPER_N_FFT,
    .hop_length = WHISPER_HOP_LENGTH,
    .n_mels = WHISPER_N_MELS,
    .chunk_length_s = WHISPER_CHUNK_LENGTH,
    .normalization = .whisper,
};

pub const CLAP_CONFIG = AudioConfig{
    .sample_rate = 48000,
    .n_fft = 1024,
    .hop_length = 480,
    .n_mels = 64,
    .chunk_length_s = 10,
    .normalization = .simple,
};

pub const ClapFeatures = struct {
    data: []f32,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
    is_longer: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ClapFeatures) void {
        self.allocator.free(self.data);
    }
};

pub fn logMelSpectrogram(
    allocator: std.mem.Allocator,
    samples: []const f32,
) ![]f32 {
    return logMelSpectrogramWithConfig(allocator, samples, WHISPER_CONFIG);
}

pub fn whisperMelFromPcm(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
) ![]f32 {
    const prepared = try copyOrResample(allocator, samples, sample_rate, WHISPER_SAMPLE_RATE);
    defer allocator.free(prepared);
    return logMelSpectrogram(allocator, prepared);
}

pub fn clapFeaturesFromPcm(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    channels: usize,
) !ClapFeatures {
    const prepared = try copyOrResample(allocator, samples, sample_rate, CLAP_CONFIG.sample_rate);
    defer allocator.free(prepared);
    return clapInputFeatures(allocator, prepared, channels);
}

/// Build official-style short-audio CLAP inputs.
/// Current native path supports the short/repeatpad branch and returns
/// 4 identical channels for fused checkpoints, matching the HF processor.
pub fn clapInputFeatures(
    allocator: std.mem.Allocator,
    samples: []const f32,
    channels: usize,
) !ClapFeatures {
    const max_samples = CLAP_CONFIG.chunk_length_s * CLAP_CONFIG.sample_rate;
    const chunk_frames = clapFrameCount(max_samples);
    if (samples.len > max_samples) {
        const requested_frames = clapFrameCount(samples.len);
        const full_mel = try clapLogMelSpectrogramForFrames(allocator, samples, requested_frames);
        defer allocator.free(full_mel);
        const mel_bins = CLAP_CONFIG.n_mels;
        const long_frames = full_mel.len / mel_bins;
        const fusion = try clapLongFusionFeatures(allocator, full_mel, long_frames, chunk_frames, mel_bins);
        defer allocator.free(fusion);

        const out_channels = if (channels >= 4) channels else 4;
        const plane = chunk_frames * mel_bins;
        const out = try allocator.alloc(f32, out_channels * plane);
        errdefer allocator.free(out);

        for (0..4) |ch| {
            @memcpy(out[ch * plane ..][0..plane], fusion[ch * plane ..][0..plane]);
        }
        if (out_channels > 4) {
            for (4..out_channels) |ch| {
                @memcpy(out[ch * plane ..][0..plane], fusion[ch % 4 * plane ..][0..plane]);
            }
        }

        return .{
            .data = out,
            .channels = out_channels,
            .time_frames = chunk_frames,
            .mel_bins = mel_bins,
            .is_longer = true,
            .allocator = allocator,
        };
    }

    var padded = try allocator.alloc(f32, max_samples);
    defer allocator.free(padded);
    const n_repeat: usize = if (samples.len == 0) 0 else max_samples / samples.len;
    var filled: usize = 0;
    if (samples.len == 0) {
        @memset(padded, 0);
    } else {
        var repeat_i: usize = 0;
        while (repeat_i < n_repeat and filled + samples.len <= max_samples) : (repeat_i += 1) {
            @memcpy(padded[filled..][0..samples.len], samples);
            filled += samples.len;
        }
        if (filled < max_samples) @memset(padded[filled..], 0);
    }

    const mel = try clapLogMelSpectrogram(allocator, padded);
    defer allocator.free(mel);

    const time_frames = chunk_frames;
    const mel_bins = CLAP_CONFIG.n_mels;
    const plane = time_frames * mel_bins;
    const out = try allocator.alloc(f32, channels * plane);
    errdefer allocator.free(out);

    for (0..channels) |ch| {
        @memcpy(out[ch * plane ..][0..plane], mel);
    }

    return .{
        .data = out,
        .channels = channels,
        .time_frames = time_frames,
        .mel_bins = mel_bins,
        .is_longer = false,
        .allocator = allocator,
    };
}

fn clapLogMelSpectrogram(
    allocator: std.mem.Allocator,
    samples: []const f32,
) ![]f32 {
    const time_frames = clapFrameCount(samples.len);
    return clapLogMelSpectrogramForFrames(allocator, samples, time_frames);
}

fn clapLogMelSpectrogramForFrames(
    allocator: std.mem.Allocator,
    samples: []const f32,
    time_frames: usize,
) ![]f32 {
    const n_fft = CLAP_CONFIG.n_fft;
    const hop = CLAP_CONFIG.hop_length;
    const n_mels = CLAP_CONFIG.n_mels;
    const n_freq = n_fft / 2 + 1;

    const filters = try generic.melFilterbankWithRange(allocator, n_mels, n_fft, CLAP_CONFIG.sample_rate, 50.0, 14_000.0);
    defer allocator.free(filters);

    const window = try allocator.alloc(f32, n_fft);
    defer allocator.free(window);
    for (0..n_fft) |i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(n_fft));
        window[i] = 0.5 * (1.0 - @cos(2.0 * std.math.pi * t));
    }

    const output = try allocator.alloc(f32, n_mels * time_frames);
    @memset(output, 0);

    const center = n_fft / 2;
    const padded = try allocator.alloc(f32, samples.len + n_fft);
    defer allocator.free(padded);
    for (0..padded.len) |i| {
        const src = reflectIndex(@as(isize, @intCast(i)) - @as(isize, @intCast(center)), samples.len);
        padded[i] = if (samples.len == 0) 0 else samples[src];
    }

    const magnitudes = try allocator.alloc(f32, n_freq);
    defer allocator.free(magnitudes);
    var fft_plan = try generic.FftPlan.init(allocator, @intCast(n_fft));
    defer fft_plan.deinit(allocator);

    const actual_frames = @min(time_frames, (padded.len - n_fft) / hop + 1);
    for (0..actual_frames) |frame| {
        const start = frame * hop;
        try fft_plan.powerSpectrumWindowed(padded[start .. start + n_fft], window, magnitudes);

        for (0..n_mels) |m| {
            var sum: f32 = 0;
            for (0..n_freq) |k| sum += filters[m * n_freq + k] * magnitudes[k];
            output[m * time_frames + frame] = 10.0 * std.math.log10(@max(sum, 1e-10));
        }
    }

    const transposed = try allocator.alloc(f32, n_mels * time_frames);
    errdefer allocator.free(transposed);
    for (0..time_frames) |t| {
        for (0..n_mels) |m| {
            transposed[t * n_mels + m] = output[m * time_frames + t];
        }
    }
    allocator.free(output);
    return transposed;
}

fn clapFrameCount(sample_count: usize) usize {
    return sample_count / CLAP_CONFIG.hop_length + 1;
}

fn reflectIndex(idx: isize, len: usize) usize {
    if (len == 0) return 0;
    if (len == 1) return 0;
    const n: isize = @intCast(len);
    const period = 2 * (n - 1);
    var mapped = @mod(idx, period);
    if (mapped >= n) mapped = period - mapped;
    return @intCast(mapped);
}

fn clapLongFusionFeatures(
    allocator: std.mem.Allocator,
    mel: []const f32,
    total_frames: usize,
    chunk_frames: usize,
    mel_bins: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, 4 * chunk_frames * mel_bins);
    errdefer allocator.free(out);

    const front = 0;
    const middle = if (total_frames > chunk_frames) (total_frames - chunk_frames) / 2 else 0;
    const back = if (total_frames > chunk_frames) total_frames - chunk_frames else 0;

    const shrink = try resizeMel2D(allocator, mel, total_frames, mel_bins, chunk_frames, mel_bins);
    defer allocator.free(shrink);
    @memcpy(out[0 .. chunk_frames * mel_bins], shrink);

    const starts = [_]usize{ front, middle, back };
    for (starts, 0..) |start, idx| {
        const dst = out[(idx + 1) * chunk_frames * mel_bins ..][0 .. chunk_frames * mel_bins];
        for (0..chunk_frames) |t| {
            const src_t = @min(start + t, total_frames - 1);
            @memcpy(
                dst[t * mel_bins ..][0..mel_bins],
                mel[src_t * mel_bins ..][0..mel_bins],
            );
        }
    }

    return out;
}

fn resizeMel2D(
    allocator: std.mem.Allocator,
    input: []const f32,
    src_h: usize,
    src_w: usize,
    dst_h: usize,
    dst_w: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, dst_h * dst_w);
    errdefer allocator.free(out);
    for (0..dst_h) |y| {
        const src_yf = if (dst_h == 1)
            0.0
        else
            (@as(f32, @floatFromInt(y)) * @as(f32, @floatFromInt(src_h - 1))) / @as(f32, @floatFromInt(dst_h - 1));
        const y0: usize = @intFromFloat(@floor(src_yf));
        const y1 = @min(y0 + 1, src_h - 1);
        const wy: f32 = src_yf - @as(f32, @floatFromInt(y0));
        for (0..dst_w) |x| {
            const src_xf = if (dst_w == 1)
                0.0
            else
                (@as(f32, @floatFromInt(x)) * @as(f32, @floatFromInt(src_w - 1))) / @as(f32, @floatFromInt(dst_w - 1));
            const x0: usize = @intFromFloat(@floor(src_xf));
            const x1 = @min(x0 + 1, src_w - 1);
            const wx: f32 = src_xf - @as(f32, @floatFromInt(x0));

            const v00 = input[y0 * src_w + x0];
            const v01 = input[y0 * src_w + x1];
            const v10 = input[y1 * src_w + x0];
            const v11 = input[y1 * src_w + x1];
            const top = v00 * (1.0 - wx) + v01 * wx;
            const bottom = v10 * (1.0 - wx) + v11 * wx;
            out[y * dst_w + x] = top * (1.0 - wy) + bottom * wy;
        }
    }
    return out;
}

fn hzToMel(hz: f32) f32 {
    return 2595.0 * std.math.log10(1.0 + hz / 700.0);
}

fn melToHz(mel: f32) f32 {
    return 700.0 * (std.math.pow(f32, 10.0, mel / 2595.0) - 1.0);
}

fn clapLogMelSpectrogramForFramesNaiveTestOnly(
    allocator: std.mem.Allocator,
    samples: []const f32,
    time_frames: usize,
) ![]f32 {
    const n_fft = CLAP_CONFIG.n_fft;
    const hop = CLAP_CONFIG.hop_length;
    const n_mels = CLAP_CONFIG.n_mels;
    const n_freq = n_fft / 2 + 1;

    const filters = try generic.melFilterbankWithRange(allocator, n_mels, n_fft, CLAP_CONFIG.sample_rate, 50.0, 14_000.0);
    defer allocator.free(filters);

    const window = try allocator.alloc(f32, n_fft);
    defer allocator.free(window);
    for (0..n_fft) |i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(n_fft));
        window[i] = 0.5 * (1.0 - @cos(2.0 * std.math.pi * t));
    }

    const output = try allocator.alloc(f32, n_mels * time_frames);
    @memset(output, 0);

    const center = n_fft / 2;
    const padded = try allocator.alloc(f32, samples.len + n_fft);
    defer allocator.free(padded);
    for (0..padded.len) |i| {
        const src = reflectIndex(@as(isize, @intCast(i)) - @as(isize, @intCast(center)), samples.len);
        padded[i] = if (samples.len == 0) 0 else samples[src];
    }

    const magnitudes = try allocator.alloc(f32, n_freq);
    defer allocator.free(magnitudes);

    const actual_frames = @min(time_frames, (padded.len - n_fft) / hop + 1);
    for (0..actual_frames) |frame| {
        const start = frame * hop;
        for (0..n_freq) |k| {
            var re: f64 = 0;
            var im: f64 = 0;
            const k_f = @as(f64, @floatFromInt(k));
            for (0..n_fft) |n| {
                const val: f64 = @floatCast(padded[start + n] * window[n]);
                const angle = -2.0 * std.math.pi * k_f * @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(n_fft));
                re += val * @cos(angle);
                im += val * @sin(angle);
            }
            magnitudes[k] = @floatCast(re * re + im * im);
        }

        for (0..n_mels) |m| {
            var sum: f32 = 0;
            for (0..n_freq) |k| sum += filters[m * n_freq + k] * magnitudes[k];
            output[m * time_frames + frame] = 10.0 * std.math.log10(@max(sum, 1e-10));
        }
    }

    const transposed = try allocator.alloc(f32, n_mels * time_frames);
    errdefer allocator.free(transposed);
    for (0..time_frames) |t| {
        for (0..n_mels) |m| {
            transposed[t * n_mels + m] = output[m * time_frames + t];
        }
    }
    allocator.free(output);
    return transposed;
}

test "whisper mel from pcm returns whisper-shaped output" {
    const samples = [_]f32{0.0} ** 1600;
    const mel = try whisperMelFromPcm(std.testing.allocator, &samples, WHISPER_SAMPLE_RATE);
    defer std.testing.allocator.free(mel);
    try std.testing.expectEqual(@as(usize, WHISPER_N_MELS * WHISPER_N_FRAMES), mel.len);
}

test "clap long fusion features crops and downsamples" {
    const mel = [_]f32{
        0,  1,
        10, 11,
        20, 21,
        30, 31,
        40, 41,
        50, 51,
    };
    const out = try clapLongFusionFeatures(std.testing.allocator, &mel, 6, 4, 2);
    defer std.testing.allocator.free(out);

    try std.testing.expectEqual(@as(usize, 4 * 4 * 2), out.len);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 10, 11, 20, 21, 30, 31 }, out[8..16]);
    try std.testing.expectEqualSlices(f32, &.{ 10, 11, 20, 21, 30, 31, 40, 41 }, out[16..24]);
    try std.testing.expectEqualSlices(f32, &.{ 20, 21, 30, 31, 40, 41, 50, 51 }, out[24..32]);
}

test "clap input features marks long audio and returns 4 channels" {
    const allocator = std.testing.allocator;
    const sample_count = CLAP_CONFIG.sample_rate * 11;
    const samples = try allocator.alloc(f32, sample_count);
    defer allocator.free(samples);
    for (samples, 0..) |*s, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(CLAP_CONFIG.sample_rate));
        s.* = 0.1 * @sin(2.0 * std.math.pi * 440.0 * t);
    }

    var features = try clapInputFeatures(allocator, samples, 4);
    defer features.deinit();

    try std.testing.expect(features.is_longer);
    try std.testing.expectEqual(@as(usize, 4), features.channels);
    try std.testing.expectEqual(@as(usize, CLAP_CONFIG.n_mels), features.mel_bins);
    try std.testing.expectEqual(@as(usize, clapFrameCount(CLAP_CONFIG.chunk_length_s * CLAP_CONFIG.sample_rate)), features.time_frames);
    try std.testing.expectEqual(features.channels * features.time_frames * features.mel_bins, features.data.len);
}

test "clap frame count matches centered STFT convention" {
    try std.testing.expectEqual(@as(usize, 1001), clapFrameCount(CLAP_CONFIG.chunk_length_s * CLAP_CONFIG.sample_rate));
    try std.testing.expectEqual(@as(usize, 3), clapFrameCount(960));
}

test "clap FFT log mel matches naive implementation on a small clip" {
    const allocator = std.testing.allocator;
    const sample_count = 4096;
    const samples = try allocator.alloc(f32, sample_count);
    defer allocator.free(samples);
    for (samples, 0..) |*s, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(CLAP_CONFIG.sample_rate));
        s.* = 0.2 * @sin(2.0 * std.math.pi * 440.0 * t) + 0.05 * @cos(2.0 * std.math.pi * 880.0 * t);
    }

    const frames = clapFrameCount(samples.len);
    const fast = try clapLogMelSpectrogramForFrames(allocator, samples, frames);
    defer allocator.free(fast);
    const naive = try clapLogMelSpectrogramForFramesNaiveTestOnly(allocator, samples, frames);
    defer allocator.free(naive);

    try std.testing.expectEqual(fast.len, naive.len);
    for (fast, naive) |actual, expected| {
        try std.testing.expectApproxEqAbs(expected, actual, 5e-3);
    }
}
