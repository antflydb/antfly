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
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const generic = @import("inference_audio");

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
pub const decodeBounded = generic.decodeBounded;
pub const default_decode_working_bytes = generic.default_decode_working_bytes;
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
/// CLAP keeps long-audio fusion semantics, but rejects inputs beyond this
/// finite duration before resampling or building a full-duration mel tensor.
pub const CLAP_MAX_INPUT_SECONDS: u32 = 60;

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
    return whisperMelFromPcmSeconds(allocator, samples, sample_rate, WHISPER_CHUNK_LENGTH);
}

/// Log-mel over a `seconds`-long context (1 to 30). Audio beyond it is
/// dropped; shorter audio is zero-padded to it. `WHISPER_CHUNK_LENGTH`
/// reproduces the reference 30 s input; smaller values are the dynamic
/// audio context used to encode short segments cheaply.
pub fn whisperMelFromPcmSeconds(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    seconds: u32,
) ![]f32 {
    return whisperMelFromPcmSecondsMels(allocator, samples, sample_rate, seconds, WHISPER_N_MELS);
}

/// `whisperMelFromPcmSeconds` with the checkpoint's mel bin count: 80 for
/// the original models, 128 for large-v3 and large-v3-turbo. The filterbank
/// is derived the same way for both.
pub fn whisperMelFromPcmSecondsMels(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    seconds: u32,
    n_mels: u32,
) ![]f32 {
    if (seconds == 0 or seconds > WHISPER_CHUNK_LENGTH) return error.UnsupportedAudioFormat;
    if (n_mels == 0) return error.UnsupportedAudioFormat;
    const window = try whisperInputWindow(samples, sample_rate);
    const prepared = try copyOrResample(allocator, window, sample_rate, WHISPER_SAMPLE_RATE);
    defer allocator.free(prepared);
    const bounded = prepared[0..@min(prepared.len, @as(usize, seconds) * WHISPER_SAMPLE_RATE)];
    if (blas_available and !platform.env.getenvBool("TERMITE_WHISPER_DISABLE_BLAS_MEL")) {
        return whisperLogMelBlas(allocator, bounded, seconds, n_mels);
    }
    var config = WHISPER_CONFIG;
    config.chunk_length_s = seconds;
    config.n_mels = n_mels;
    return logMelSpectrogramWithConfig(allocator, bounded, config);
}

pub const blas_available = build_options.enable_system_blas;

pub const blas = if (blas_available) struct {
    pub const row_major: c_int = 101;
    pub const no_trans: c_int = 111;
    pub const trans: c_int = 112;
    pub extern "c" fn cblas_sgemm(
        layout: c_int,
        transa: c_int,
        transb: c_int,
        m: c_int,
        n: c_int,
        k: c_int,
        alpha: f32,
        a: [*]const f32,
        lda: c_int,
        b: [*]const f32,
        ldb: c_int,
        beta: f32,
        c_out: [*]f32,
        ldc: c_int,
    ) void;
} else struct {};

/// Whisper log-mel as two dense matrix products on the system BLAS: the
/// windowed frames against the 400-point DFT basis, then the power
/// spectrum against the mel filterbank. Same math as the FFT path within
/// float rounding, and several times faster than the per-frame Bluestein
/// transform on a 30 s window. Output layout and normalization match
/// `logMelSpectrogramWithConfig` with the Whisper configuration.
pub fn whisperLogMelBlas(allocator: std.mem.Allocator, samples: []const f32, seconds: u32, mel_bins: u32) ![]f32 {
    const n_fft: usize = WHISPER_CONFIG.n_fft;
    const hop: usize = WHISPER_CONFIG.hop_length;
    const n_mels: usize = mel_bins;
    const n_freq = n_fft / 2 + 1;
    const max_frames = @as(usize, seconds) * WHISPER_SAMPLE_RATE / hop;
    const min_samples = @as(usize, seconds) * WHISPER_SAMPLE_RATE;
    const padded_len = @max(samples.len, min_samples);
    const frames = @min((padded_len - n_fft) / hop + 1, max_frames);
    if (frames == 0 or frames > std.math.maxInt(c_int) / 2) return error.UnsupportedAudioFormat;

    // Windowed frames [frames, n_fft].
    const frame_matrix = try allocator.alloc(f32, frames * n_fft);
    defer allocator.free(frame_matrix);
    for (0..frames) |f| {
        const start = f * hop;
        const row = frame_matrix[f * n_fft ..][0..n_fft];
        for (0..n_fft) |i| {
            const idx = start + i;
            const sample: f32 = if (idx < samples.len) samples[idx] else 0;
            const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(n_fft));
            row[i] = sample * 0.5 * (1.0 - @cos(2.0 * std.math.pi * t));
        }
    }

    // DFT basis [n_fft, 2 * n_freq]: cos and -sin per bin.
    const basis_cols = 2 * n_freq;
    const basis = try allocator.alloc(f32, n_fft * basis_cols);
    defer allocator.free(basis);
    for (0..n_fft) |i| {
        for (0..n_freq) |k| {
            const angle = 2.0 * std.math.pi * @as(f64, @floatFromInt(i * k)) / @as(f64, @floatFromInt(n_fft));
            basis[i * basis_cols + 2 * k] = @floatCast(@cos(angle));
            basis[i * basis_cols + 2 * k + 1] = @floatCast(-@sin(angle));
        }
    }

    const spectrum = try allocator.alloc(f32, frames * basis_cols);
    defer allocator.free(spectrum);
    blas.cblas_sgemm(
        blas.row_major,
        blas.no_trans,
        blas.no_trans,
        @intCast(frames),
        @intCast(basis_cols),
        @intCast(n_fft),
        1.0,
        frame_matrix.ptr,
        @intCast(n_fft),
        basis.ptr,
        @intCast(basis_cols),
        0.0,
        spectrum.ptr,
        @intCast(basis_cols),
    );

    // Power spectrum [frames, n_freq].
    const power = try allocator.alloc(f32, frames * n_freq);
    defer allocator.free(power);
    for (0..frames) |f| {
        const src = spectrum[f * basis_cols ..][0..basis_cols];
        const dst = power[f * n_freq ..][0..n_freq];
        for (0..n_freq) |k| {
            const re = src[2 * k];
            const im = src[2 * k + 1];
            dst[k] = re * re + im * im;
        }
    }

    // Mel energies [frames, n_mels] = power x filters^T.
    const filters = try generic.melFilterbankWithRange(allocator, @intCast(n_mels), @intCast(n_fft), WHISPER_SAMPLE_RATE, 0, 0);
    defer allocator.free(filters);
    const mel = try allocator.alloc(f32, frames * n_mels);
    defer allocator.free(mel);
    blas.cblas_sgemm(
        blas.row_major,
        blas.no_trans,
        blas.trans,
        @intCast(frames),
        @intCast(n_mels),
        @intCast(n_freq),
        1.0,
        power.ptr,
        @intCast(n_freq),
        filters.ptr,
        @intCast(n_freq),
        0.0,
        mel.ptr,
        @intCast(n_mels),
    );

    // [n_mels, max_frames] with Whisper's log10, clamp, and scale.
    const output = try allocator.alloc(f32, n_mels * max_frames);
    errdefer allocator.free(output);
    @memset(output, 0);
    for (0..frames) |f| {
        for (0..n_mels) |m| {
            output[m * max_frames + f] = @max(mel[f * n_mels + m], 1e-10);
        }
    }
    var max_val: f32 = -std.math.inf(f32);
    for (output) |*v| {
        v.* = @log(v.*) / @log(10.0);
        if (v.* > max_val) max_val = v.*;
    }
    const floor_val = max_val - 8.0;
    const offset_val = max_val - 4.0;
    for (output) |*v| v.* = (@max(v.*, floor_val) - offset_val) * 0.25;
    return output;
}

test "blas log-mel matches the fft path" {
    if (!blas_available) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const seconds: u32 = 2;
    const samples = try allocator.alloc(f32, seconds * WHISPER_SAMPLE_RATE - 3000);
    defer allocator.free(samples);
    var prng = std.Random.DefaultPrng.init(0x3e1);
    const random = prng.random();
    for (samples, 0..) |*s, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(WHISPER_SAMPLE_RATE));
        s.* = 0.4 * @sin(2.0 * std.math.pi * 440.0 * t) + 0.2 * @sin(2.0 * std.math.pi * 3100.0 * t) + 0.05 * (random.float(f32) - 0.5);
    }
    var config = WHISPER_CONFIG;
    config.chunk_length_s = seconds;
    const reference = try logMelSpectrogramWithConfig(allocator, samples, config);
    defer allocator.free(reference);
    const fast = try whisperLogMelBlas(allocator, samples, seconds, WHISPER_N_MELS);
    defer allocator.free(fast);
    try std.testing.expectEqual(reference.len, fast.len);
    var max_diff: f32 = 0;
    for (reference, fast) |a, b| max_diff = @max(max_diff, @abs(a - b));
    try std.testing.expect(max_diff < 2e-3);
}

/// Mel frames produced for a `seconds` context (100 per second at 16 kHz).
pub fn whisperFramesForSeconds(seconds: u32) usize {
    return @as(usize, seconds) * (WHISPER_SAMPLE_RATE / WHISPER_HOP_LENGTH);
}

/// Whole seconds of context for `sample_count` samples: the audio rounded
/// up, plus one second of silence so the decoder sees the utterance end,
/// clamped to the model window.
pub fn dynamicContextSeconds(sample_count: usize, sample_rate: u32) u32 {
    if (sample_rate == 0) return WHISPER_CHUNK_LENGTH;
    const whole: usize = (sample_count + sample_rate - 1) / sample_rate;
    const padded = whole + 1;
    return @intCast(@min(@as(usize, WHISPER_CHUNK_LENGTH), @max(@as(usize, 1), padded)));
}

fn whisperInputWindow(samples: []const f32, sample_rate: u32) ![]const f32 {
    if (sample_rate == 0 or samples.len == 0) return error.UnsupportedAudioFormat;
    const max_samples = std.math.mul(
        usize,
        @as(usize, sample_rate),
        @as(usize, WHISPER_CHUNK_LENGTH),
    ) catch return error.UnsupportedAudioFormat;
    return samples[0..@min(samples.len, max_samples)];
}

pub fn clapFeaturesFromPcm(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    channels: usize,
) !ClapFeatures {
    try validateClapInput(samples, sample_rate);
    const prepared = try copyOrResample(allocator, samples, sample_rate, CLAP_CONFIG.sample_rate);
    defer allocator.free(prepared);
    return clapInputFeatures(allocator, prepared, channels);
}

fn validateClapInput(samples: []const f32, sample_rate: u32) !void {
    if (sample_rate == 0 or samples.len == 0) return error.UnsupportedAudioFormat;
    const max_source_samples = std.math.mul(
        usize,
        @as(usize, sample_rate),
        @as(usize, CLAP_MAX_INPUT_SECONDS),
    ) catch return error.AudioInputTooLong;
    if (samples.len > max_source_samples) return error.AudioInputTooLong;
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

test "whisper mel takes the checkpoint's mel bin count" {
    const allocator = std.testing.allocator;
    const seconds: u32 = 2;
    const samples = try allocator.alloc(f32, seconds * WHISPER_SAMPLE_RATE - 1234);
    defer allocator.free(samples);
    var prng = std.Random.DefaultPrng.init(0x5a7);
    const random = prng.random();
    for (samples, 0..) |*s, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(WHISPER_SAMPLE_RATE));
        s.* = 0.3 * @sin(2.0 * std.math.pi * 620.0 * t) + 0.1 * (random.float(f32) - 0.5);
    }
    // 128 bins (large-v3) through the public entry point: [128, frames].
    const wide = try whisperMelFromPcmSecondsMels(allocator, samples, WHISPER_SAMPLE_RATE, seconds, 128);
    defer allocator.free(wide);
    try std.testing.expectEqual(@as(usize, 128 * 200), wide.len);
    const narrow = try whisperMelFromPcmSecondsMels(allocator, samples, WHISPER_SAMPLE_RATE, seconds, 80);
    defer allocator.free(narrow);
    try std.testing.expectEqual(@as(usize, 80 * 200), narrow.len);
    // The bins are a different filterbank, not a padded 80.
    var differs = false;
    for (0..80 * 200) |i| {
        if (@abs(wide[i] - narrow[i]) > 1e-3) {
            differs = true;
            break;
        }
    }
    try std.testing.expect(differs);
    try std.testing.expectError(error.UnsupportedAudioFormat, whisperMelFromPcmSecondsMels(allocator, samples, WHISPER_SAMPLE_RATE, seconds, 0));
    // BLAS and FFT paths agree at 128 bins as they do at 80.
    if (blas_available) {
        var config = WHISPER_CONFIG;
        config.chunk_length_s = seconds;
        config.n_mels = 128;
        const reference = try logMelSpectrogramWithConfig(allocator, samples, config);
        defer allocator.free(reference);
        const fast = try whisperLogMelBlas(allocator, samples, seconds, 128);
        defer allocator.free(fast);
        try std.testing.expectEqual(reference.len, fast.len);
        try std.testing.expectEqual(@as(usize, 128 * 200), fast.len);
        var max_diff: f32 = 0;
        for (reference, fast) |a, b| max_diff = @max(max_diff, @abs(a - b));
        try std.testing.expect(max_diff < 2e-3);
    }
}

test "whisper mel from pcm returns whisper-shaped output" {
    const samples = [_]f32{0.0} ** 1600;
    const mel = try whisperMelFromPcm(std.testing.allocator, &samples, WHISPER_SAMPLE_RATE);
    defer std.testing.allocator.free(mel);
    try std.testing.expectEqual(@as(usize, WHISPER_N_MELS * WHISPER_N_FRAMES), mel.len);
}

test "whisper dynamic context sizes the mel to the audio" {
    try std.testing.expectEqual(@as(u32, 4), dynamicContextSeconds(WHISPER_SAMPLE_RATE * 5 / 2, WHISPER_SAMPLE_RATE));
    try std.testing.expectEqual(@as(u32, 30), dynamicContextSeconds(WHISPER_SAMPLE_RATE * 60, WHISPER_SAMPLE_RATE));
    try std.testing.expectEqual(@as(u32, 1), dynamicContextSeconds(0, WHISPER_SAMPLE_RATE));
    try std.testing.expectEqual(@as(usize, 400), whisperFramesForSeconds(4));
    const samples = [_]f32{0.0} ** 1600;
    const mel = try whisperMelFromPcmSeconds(std.testing.allocator, &samples, WHISPER_SAMPLE_RATE, 2);
    defer std.testing.allocator.free(mel);
    try std.testing.expectEqual(@as(usize, WHISPER_N_MELS * 200), mel.len);
    try std.testing.expectError(error.UnsupportedAudioFormat, whisperMelFromPcmSeconds(std.testing.allocator, &samples, WHISPER_SAMPLE_RATE, 31));
}

test "whisper input is validated and sliced before resampling" {
    const samples = [_]f32{0.0} ** 31;
    try std.testing.expectEqual(@as(usize, 30), (try whisperInputWindow(&samples, 1)).len);
    try std.testing.expectError(error.UnsupportedAudioFormat, whisperInputWindow(&.{}, 16_000));
    try std.testing.expectError(error.UnsupportedAudioFormat, whisperInputWindow(&samples, 0));
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

test "clap source duration is bounded before resampling" {
    const allowed = [_]f32{0.0} ** CLAP_MAX_INPUT_SECONDS;
    try validateClapInput(&allowed, 1);
    const too_long = [_]f32{0.0} ** (CLAP_MAX_INPUT_SECONDS + 1);
    try std.testing.expectError(error.AudioInputTooLong, validateClapInput(&too_long, 1));
    try std.testing.expectError(error.UnsupportedAudioFormat, validateClapInput(&.{}, 48_000));
    try std.testing.expectError(error.UnsupportedAudioFormat, validateClapInput(&allowed, 0));
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
