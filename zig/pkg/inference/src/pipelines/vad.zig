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

//! Voice activity detection.
//!
//! Frames mono PCM into fixed windows, classifies each frame as speech, and
//! turns the frame decisions into speech segments with onset/offset
//! hysteresis. Two classifiers share the segment logic: an RMS energy rule
//! that needs no model, and Silero VAD (`silero_vad.zig`) when the config
//! carries loaded weights and the audio is 16 kHz. Silero scores 512-sample
//! frames with a recurrent state that runs forward over one buffer, so every
//! entry point classifies frames in order from the buffer start.

const std = @import("std");
const silero_vad = @import("silero_vad.zig");

pub const Config = struct {
    /// Analysis frame length. 20 ms matches common telephony/VAD framing.
    frame_ms: u32 = 20,
    /// RMS amplitude on [-1, 1] PCM at or above which a frame counts as
    /// speech. 0.012 is about -38 dBFS, above typical room noise for a
    /// close microphone.
    threshold: f32 = 0.012,
    /// Consecutive speech needed to open a segment. Filters clicks and pops.
    min_speech_ms: u32 = 120,
    /// Continuous silence that closes a segment (the endpoint).
    min_silence_ms: u32 = 600,
    /// Padding added on both sides of each detected segment so Whisper sees
    /// word onsets and decays.
    speech_pad_ms: u32 = 120,
    /// Neural classifier. When set and the audio is 16 kHz, frames are 512
    /// samples (32 ms) scored by Silero instead of the energy rule. The
    /// weights outlive every config that points at them.
    silero: ?*const silero_vad.Weights = null,
    /// Speech probability at or above which a Silero frame counts as speech.
    silero_threshold: f32 = 0.5,

    pub fn validate(self: Config) !void {
        if (self.frame_ms == 0) return error.InvalidVadConfig;
        if (!(self.threshold >= 0) or !(self.threshold <= 1)) return error.InvalidVadConfig;
        if (!(self.silero_threshold >= 0) or !(self.silero_threshold <= 1)) return error.InvalidVadConfig;
        if (self.min_silence_ms == 0) return error.InvalidVadConfig;
    }

    pub fn usesSilero(self: Config, sample_rate: u32) bool {
        return self.silero != null and sample_rate == silero_vad.sample_rate;
    }
};

/// Frame-level speech classifier for one forward pass over a buffer.
pub const Classifier = struct {
    config: Config,
    /// Samples per frame.
    frame: usize,
    neural: bool,
    state: silero_vad.State = .{},

    pub fn init(sample_rate: u32, config: Config) Classifier {
        const neural = config.usesSilero(sample_rate);
        return .{
            .config = config,
            .frame = if (neural) silero_vad.chunk_samples else frameSamples(sample_rate, config),
            .neural = neural,
        };
    }

    /// Classify the next frame in buffer order. A short final frame is
    /// zero-padded for Silero.
    pub fn isSpeech(self: *Classifier, samples: []const f32) bool {
        if (!self.neural) return frameIsSpeech(samples, self.config);
        var chunk: [silero_vad.chunk_samples]f32 = [_]f32{0} ** silero_vad.chunk_samples;
        const n = @min(samples.len, chunk.len);
        @memcpy(chunk[0..n], samples[0..n]);
        return silero_vad.probability(self.config.silero.?, &self.state, &chunk) >= self.config.silero_threshold;
    }
};

/// Half-open sample range `[start, end)` into the analyzed buffer.
pub const Segment = struct {
    start: usize,
    end: usize,

    pub fn len(self: Segment) usize {
        return self.end - self.start;
    }
};

pub fn msToSamples(sample_rate: u32, ms: u32) usize {
    return (@as(usize, sample_rate) * @as(usize, ms)) / 1000;
}

pub fn samplesToMs(sample_rate: u32, samples: u64) u64 {
    if (sample_rate == 0) return 0;
    return (samples * 1000) / sample_rate;
}

pub fn frameSamples(sample_rate: u32, config: Config) usize {
    return @max(@as(usize, 1), msToSamples(sample_rate, config.frame_ms));
}

pub fn rms(samples: []const f32) f32 {
    if (samples.len == 0) return 0;
    var acc: f64 = 0;
    for (samples) |s| acc += @as(f64, s) * @as(f64, s);
    return @floatCast(@sqrt(acc / @as(f64, @floatFromInt(samples.len))));
}

pub fn frameIsSpeech(samples: []const f32, config: Config) bool {
    return rms(samples) >= config.threshold;
}

/// True when any frame of `samples` is classified as speech.
pub fn hasSpeech(samples: []const f32, sample_rate: u32, config: Config) bool {
    var classifier = Classifier.init(sample_rate, config);
    var offset: usize = 0;
    while (offset < samples.len) : (offset += classifier.frame) {
        const end = @min(samples.len, offset + classifier.frame);
        if (classifier.isSpeech(samples[offset..end])) return true;
    }
    return false;
}

/// Number of trailing samples after the last speech frame.
pub fn trailingSilenceSamples(samples: []const f32, sample_rate: u32, config: Config) usize {
    var classifier = Classifier.init(sample_rate, config);
    if (!classifier.neural) {
        const frame = classifier.frame;
        var end = samples.len;
        while (end > 0) {
            const start = if (end >= frame) end - frame else 0;
            if (frameIsSpeech(samples[start..end], config)) break;
            end = start;
        }
        return samples.len - end;
    }
    var last_speech_end: usize = 0;
    var offset: usize = 0;
    while (offset < samples.len) : (offset += classifier.frame) {
        const end = @min(samples.len, offset + classifier.frame);
        if (classifier.isSpeech(samples[offset..end])) last_speech_end = end;
    }
    return samples.len - last_speech_end;
}

/// Detect speech segments with onset/offset hysteresis. The returned slice is
/// owned by the caller. Segments are padded by `speech_pad_ms`, clamped to the
/// buffer, and non-overlapping after padding.
pub fn detectSegments(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    config: Config,
) ![]Segment {
    try config.validate();
    if (sample_rate == 0) return error.UnsupportedAudioFormat;
    var out = std.ArrayListUnmanaged(Segment).empty;
    errdefer out.deinit(allocator);
    if (samples.len == 0) return out.toOwnedSlice(allocator);

    var classifier = Classifier.init(sample_rate, config);
    const frame = classifier.frame;
    const min_speech_frames = @max(@as(usize, 1), ceilDiv(msToSamples(sample_rate, config.min_speech_ms), frame));
    const min_silence_frames = @max(@as(usize, 1), ceilDiv(msToSamples(sample_rate, config.min_silence_ms), frame));
    const pad = msToSamples(sample_rate, config.speech_pad_ms);

    var in_speech = false;
    var speech_start: usize = 0;
    var speech_run: usize = 0;
    var silence_run: usize = 0;
    var candidate_start: usize = 0;
    var last_speech_end: usize = 0;

    var offset: usize = 0;
    while (offset < samples.len) : (offset += frame) {
        const end = @min(samples.len, offset + frame);
        const active = classifier.isSpeech(samples[offset..end]);
        if (active) {
            if (speech_run == 0) candidate_start = offset;
            speech_run += 1;
            silence_run = 0;
            last_speech_end = end;
            if (!in_speech and speech_run >= min_speech_frames) {
                in_speech = true;
                speech_start = candidate_start;
            }
        } else {
            speech_run = 0;
            silence_run += 1;
            if (in_speech and silence_run >= min_silence_frames) {
                try appendPadded(allocator, &out, speech_start, last_speech_end, pad, samples.len);
                in_speech = false;
            }
        }
    }
    if (in_speech) try appendPadded(allocator, &out, speech_start, samples.len, pad, samples.len);
    return out.toOwnedSlice(allocator);
}

fn appendPadded(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(Segment),
    start: usize,
    end: usize,
    pad: usize,
    total: usize,
) !void {
    var padded_start = if (start > pad) start - pad else 0;
    const padded_end = @min(total, end +| pad);
    if (out.items.len > 0) {
        const previous = &out.items[out.items.len - 1];
        if (padded_start < previous.end) padded_start = previous.end;
        if (padded_start >= padded_end) {
            previous.end = @max(previous.end, padded_end);
            return;
        }
    }
    try out.append(allocator, .{ .start = padded_start, .end = padded_end });
}

/// Return the sample index at the centre of the quietest frame inside
/// `[lo, hi)`. Used to split long audio where a cut is least likely to
/// land in the middle of a word. Falls back to `hi` when the range is
/// shorter than one frame.
pub fn quietestSplit(samples: []const f32, sample_rate: u32, lo: usize, hi: usize, config: Config) usize {
    const clamped_hi = @min(hi, samples.len);
    if (lo >= clamped_hi) return clamped_hi;
    const frame = frameSamples(sample_rate, config);
    if (clamped_hi - lo < frame) return clamped_hi;
    var best_start = lo;
    var best_energy: f32 = std.math.inf(f32);
    var offset = lo;
    while (offset + frame <= clamped_hi) : (offset += frame) {
        const energy = rms(samples[offset .. offset + frame]);
        // `<=` prefers the latest quiet frame so windows stay as long as possible.
        if (energy <= best_energy) {
            best_energy = energy;
            best_start = offset;
        }
    }
    return best_start + frame / 2;
}

fn ceilDiv(a: usize, b: usize) usize {
    return (a + b - 1) / b;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const test_rate: u32 = 16_000;

fn fillTone(buffer: []f32, start_ms: u32, end_ms: u32, amplitude: f32) void {
    const start = msToSamples(test_rate, start_ms);
    const end = @min(buffer.len, msToSamples(test_rate, end_ms));
    var i = start;
    while (i < end) : (i += 1) {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(test_rate));
        buffer[i] = amplitude * @sin(2.0 * std.math.pi * 220.0 * t);
    }
}

test "vad detects one padded segment around a tone burst" {
    const allocator = std.testing.allocator;
    const samples = try allocator.alloc(f32, msToSamples(test_rate, 3000));
    defer allocator.free(samples);
    @memset(samples, 0);
    fillTone(samples, 1000, 2000, 0.2);

    const segments = try detectSegments(allocator, samples, test_rate, .{});
    defer allocator.free(segments);
    try std.testing.expectEqual(@as(usize, 1), segments.len);
    const start_ms = samplesToMs(test_rate, segments[0].start);
    const end_ms = samplesToMs(test_rate, segments[0].end);
    try std.testing.expect(start_ms >= 860 and start_ms <= 1000);
    try std.testing.expect(end_ms >= 2000 and end_ms <= 2140);
}

test "vad merges bursts separated by less than min_silence and splits longer gaps" {
    const allocator = std.testing.allocator;
    const samples = try allocator.alloc(f32, msToSamples(test_rate, 6000));
    defer allocator.free(samples);
    @memset(samples, 0);
    fillTone(samples, 500, 1500, 0.2);
    fillTone(samples, 1700, 2500, 0.2); // 200 ms gap: merged
    fillTone(samples, 4000, 5000, 0.2); // 1.5 s gap: split

    const segments = try detectSegments(allocator, samples, test_rate, .{});
    defer allocator.free(segments);
    try std.testing.expectEqual(@as(usize, 2), segments.len);
    try std.testing.expect(samplesToMs(test_rate, segments[0].end) >= 2500);
    try std.testing.expect(samplesToMs(test_rate, segments[1].start) >= 3800);
}

test "vad ignores clicks shorter than min_speech" {
    const allocator = std.testing.allocator;
    const samples = try allocator.alloc(f32, msToSamples(test_rate, 1000));
    defer allocator.free(samples);
    @memset(samples, 0);
    fillTone(samples, 400, 440, 0.5);

    const segments = try detectSegments(allocator, samples, test_rate, .{});
    defer allocator.free(segments);
    try std.testing.expectEqual(@as(usize, 0), segments.len);
    try std.testing.expect(hasSpeech(samples, test_rate, .{}));
}

test "vad reports open segments and trailing silence" {
    const allocator = std.testing.allocator;
    const samples = try allocator.alloc(f32, msToSamples(test_rate, 2000));
    defer allocator.free(samples);
    @memset(samples, 0);
    fillTone(samples, 1000, 2000, 0.2);

    const segments = try detectSegments(allocator, samples, test_rate, .{});
    defer allocator.free(segments);
    try std.testing.expectEqual(@as(usize, 1), segments.len);
    try std.testing.expectEqual(samples.len, segments[0].end);
    try std.testing.expectEqual(@as(usize, 0), trailingSilenceSamples(samples, test_rate, .{}));

    @memset(samples[msToSamples(test_rate, 1500)..], 0);
    const trailing = trailingSilenceSamples(samples, test_rate, .{});
    try std.testing.expect(samplesToMs(test_rate, trailing) >= 480 and samplesToMs(test_rate, trailing) <= 520);
}

test "vad quietest split lands in the gap between bursts" {
    const allocator = std.testing.allocator;
    const samples = try allocator.alloc(f32, msToSamples(test_rate, 3000));
    defer allocator.free(samples);
    @memset(samples, 0);
    fillTone(samples, 0, 1200, 0.2);
    fillTone(samples, 1400, 3000, 0.2);

    const split = quietestSplit(samples, test_rate, msToSamples(test_rate, 500), samples.len, .{});
    const split_ms = samplesToMs(test_rate, split);
    try std.testing.expect(split_ms >= 1200 and split_ms <= 1400);
    try std.testing.expectEqual(samples.len, quietestSplit(samples, test_rate, samples.len, samples.len + 10, .{}));
}

test "vad config rejects degenerate values" {
    try std.testing.expectError(error.InvalidVadConfig, (Config{ .frame_ms = 0 }).validate());
    try std.testing.expectError(error.InvalidVadConfig, (Config{ .threshold = 2 }).validate());
    try std.testing.expectError(error.InvalidVadConfig, (Config{ .min_silence_ms = 0 }).validate());
}

fn testSileroWeights(allocator: std.mem.Allocator) ?silero_vad.Weights {
    const home_z = std.c.getenv("HOME") orelse return null;
    const path = std.fs.path.join(allocator, &.{ std.mem.span(home_z), ".antfly", "inference", "models", "onnx-community", "silero-vad", "onnx", "model.onnx" }) catch return null;
    defer allocator.free(path);
    return silero_vad.Weights.load(allocator, path) catch null;
}

test "vad with silero rejects a loud tone that the energy rule accepts" {
    const allocator = std.testing.allocator;
    var weights = testSileroWeights(allocator) orelse return error.SkipZigTest;
    defer weights.deinit();
    const samples = try allocator.alloc(f32, msToSamples(test_rate, 3000));
    defer allocator.free(samples);
    @memset(samples, 0);
    fillTone(samples, 500, 2500, 0.2);

    const energy = try detectSegments(allocator, samples, test_rate, .{});
    defer allocator.free(energy);
    try std.testing.expectEqual(@as(usize, 1), energy.len);

    const neural_config = Config{ .silero = &weights };
    const neural = try detectSegments(allocator, samples, test_rate, neural_config);
    defer allocator.free(neural);
    try std.testing.expectEqual(@as(usize, 0), neural.len);
    try std.testing.expect(!hasSpeech(samples, test_rate, neural_config));
    try std.testing.expectEqual(samples.len, trailingSilenceSamples(samples, test_rate, neural_config));
    // Silero falls back to the energy rule at other sample rates.
    try std.testing.expect(!neural_config.usesSilero(8_000));
    try std.testing.expect(Classifier.init(8_000, neural_config).frame == frameSamples(8_000, neural_config));
}
