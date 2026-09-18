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

//! Speaker embeddings and clustering for local diarization.
//!
//! The recognizer gives phrase-level segments with timestamps; this module
//! answers "who said each phrase" without a cloud provider. Fixed 3 s
//! windows of each segment's audio go through a speaker verification
//! network exported to ONNX — by default 3D-Speaker's CAM++
//! (`csukuangfj/speaker-embedding-models`, 512-d, 16 kHz) — and the
//! resulting unit vectors are clustered by cosine similarity. Clusters are
//! reported as `SPEAKER_00`, `SPEAKER_01`, ... in order of first appearance.
//!
//! Windows are fixed-length because the native graph executor resolves the
//! model's dynamic time axis at import: the CAM++ export derives its
//! segment-pooling reshapes from `Shape` nodes, which the importer folds
//! for one declared length. One session therefore serves one window size,
//! and 3 s is where verification embeddings stop improving noticeably.
//!
//! Feature extraction mirrors the training recipe exactly: Kaldi filterbanks
//! as computed by `torchaudio.compliance.kaldi.fbank` (25 ms Povey window,
//! 10 ms hop, `snip_edges=true`, DC removal, 0.97 pre-emphasis, 512-point
//! power spectrum, 80 HTK-mel bins from 20 Hz to Nyquist, natural log) with
//! the per-utterance mean removed, which is what the model metadata calls
//! `feature_normalize_type = global-mean`.

const std = @import("std");
const backends = @import("../backends/backends.zig");
const long_transcription = @import("long_transcription.zig");
const onnx_graph = @import("onnx_graph");
const Tensor = backends.Tensor;
const Session = @import("../backends/session.zig").Session;

pub const sample_rate: u32 = 16_000;
pub const num_mel_bins: usize = 80;
pub const embedding_dim: usize = 512;
/// Audio per model run.
pub const window_seconds: usize = 3;
pub const window_samples: usize = window_seconds * @as(usize, sample_rate);

/// Registry reference of the default speaker model.
pub const default_model_ref = "csukuangfj/speaker-embedding-models:3dspeaker_speech_campplus_sv_en_voxceleb_16k.onnx";
/// File name of the default model inside its install directory.
pub const default_model_file = "3dspeaker_speech_campplus_sv_en_voxceleb_16k.onnx";

const frame_length: usize = 400; // 25 ms
const frame_shift: usize = 160; // 10 ms
const padded_frame_length: usize = 512;
const spectrum_bins: usize = padded_frame_length / 2 + 1;
const preemphasis: f32 = 0.97;
const low_freq_hz: f32 = 20.0;
const log_floor: f32 = 1.1920929e-07; // torch.finfo(float32).eps

/// Number of frames `torchaudio` produces for `samples` with `snip_edges=true`:
/// only windows that fit entirely inside the clip.
pub fn fbankFrameCount(samples: usize) usize {
    if (samples < frame_length) return 0;
    return 1 + (samples - frame_length) / frame_shift;
}

fn melScale(freq: f32) f32 {
    return 1127.0 * @log(1.0 + freq / 700.0);
}

/// HTK mel filterbank weights, `[num_mel_bins][spectrum_bins]`. Matches
/// `torchaudio.compliance.kaldi.get_mel_banks` with the Nyquist bin zeroed.
fn melBanksAlloc(allocator: std.mem.Allocator) ![]f32 {
    const banks = try allocator.alloc(f32, num_mel_bins * spectrum_bins);
    @memset(banks, 0);
    const nyquist: f32 = @as(f32, @floatFromInt(sample_rate)) / 2.0;
    const fft_bin_width: f32 = @as(f32, @floatFromInt(sample_rate)) / @as(f32, @floatFromInt(padded_frame_length));
    const mel_low = melScale(low_freq_hz);
    const mel_high = melScale(nyquist);
    const mel_delta = (mel_high - mel_low) / @as(f32, @floatFromInt(num_mel_bins + 1));
    for (0..num_mel_bins) |bin| {
        const left = mel_low + @as(f32, @floatFromInt(bin)) * mel_delta;
        const center = left + mel_delta;
        const right = left + 2.0 * mel_delta;
        // The last (Nyquist) column stays zero, as in the reference.
        for (0..padded_frame_length / 2) |k| {
            const mel = melScale(fft_bin_width * @as(f32, @floatFromInt(k)));
            const up = (mel - left) / (center - left);
            const down = (right - mel) / (right - center);
            banks[bin * spectrum_bins + k] = @max(0.0, @min(up, down));
        }
    }
    return banks;
}

/// Povey window: `hann(N, periodic=false) ** 0.85`.
fn poveyWindow(window: *[frame_length]f32) void {
    for (window, 0..) |*w, n| {
        const hann = 0.5 - 0.5 * @cos(2.0 * std.math.pi * @as(f32, @floatFromInt(n)) / @as(f32, @floatFromInt(frame_length - 1)));
        w.* = std.math.pow(f32, hann, 0.85);
    }
}

/// In-place iterative radix-2 FFT on `padded_frame_length` complex points.
fn fft512(re: *[padded_frame_length]f32, im: *[padded_frame_length]f32) void {
    const n = padded_frame_length;
    // Bit reversal.
    var j: usize = 0;
    for (1..n) |i| {
        var bit = n >> 1;
        while (j & bit != 0) : (bit >>= 1) j ^= bit;
        j ^= bit;
        if (i < j) {
            std.mem.swap(f32, &re[i], &re[j]);
            std.mem.swap(f32, &im[i], &im[j]);
        }
    }
    var len: usize = 2;
    while (len <= n) : (len <<= 1) {
        const angle = -2.0 * std.math.pi / @as(f64, @floatFromInt(len));
        const wr_step: f64 = @cos(angle);
        const wi_step: f64 = @sin(angle);
        var start: usize = 0;
        while (start < n) : (start += len) {
            var wr: f64 = 1.0;
            var wi: f64 = 0.0;
            for (0..len / 2) |k| {
                const a = start + k;
                const b = a + len / 2;
                const tr = @as(f64, re[b]) * wr - @as(f64, im[b]) * wi;
                const ti = @as(f64, re[b]) * wi + @as(f64, im[b]) * wr;
                re[b] = @floatCast(@as(f64, re[a]) - tr);
                im[b] = @floatCast(@as(f64, im[a]) - ti);
                re[a] = @floatCast(@as(f64, re[a]) + tr);
                im[a] = @floatCast(@as(f64, im[a]) + ti);
                const next_wr = wr * wr_step - wi * wi_step;
                wi = wr * wi_step + wi * wr_step;
                wr = next_wr;
            }
        }
    }
}

/// Kaldi log-mel filterbank features, `[frames][num_mel_bins]`, for 16 kHz
/// mono samples in [-1, 1]. No dither, no mean removal (see
/// `subtractGlobalMean`). Returns an empty slice for clips shorter than one
/// window.
pub fn kaldiFbankAlloc(allocator: std.mem.Allocator, samples: []const f32) ![]f32 {
    const frames = fbankFrameCount(samples.len);
    const features = try allocator.alloc(f32, frames * num_mel_bins);
    errdefer allocator.free(features);
    if (frames == 0) return features;

    const banks = try melBanksAlloc(allocator);
    defer allocator.free(banks);
    var window: [frame_length]f32 = undefined;
    poveyWindow(&window);

    var re: [padded_frame_length]f32 = undefined;
    var im: [padded_frame_length]f32 = undefined;
    var frame: [frame_length]f32 = undefined;
    for (0..frames) |t| {
        const base = t * frame_shift;
        var sum: f64 = 0;
        for (0..frame_length) |i| {
            frame[i] = samples[base + i];
            sum += frame[i];
        }
        // Remove DC, then pre-emphasise with a replicated first sample.
        const mean: f32 = @floatCast(sum / @as(f64, @floatFromInt(frame_length)));
        for (&frame) |*v| v.* -= mean;
        var previous = frame[0];
        for (&frame) |*v| {
            const current = v.*;
            v.* = current - preemphasis * previous;
            previous = current;
        }
        for (0..frame_length) |i| {
            re[i] = frame[i] * window[i];
            im[i] = 0;
        }
        @memset(re[frame_length..], 0);
        @memset(im[frame_length..], 0);
        fft512(&re, &im);

        var power: [spectrum_bins]f32 = undefined;
        for (0..spectrum_bins) |k| power[k] = re[k] * re[k] + im[k] * im[k];
        const out = features[t * num_mel_bins ..][0..num_mel_bins];
        for (0..num_mel_bins) |bin| {
            const weights = banks[bin * spectrum_bins ..][0..spectrum_bins];
            var energy: f64 = 0;
            for (weights, power) |w, p| energy += @as(f64, w) * @as(f64, p);
            out[bin] = @log(@max(@as(f32, @floatCast(energy)), log_floor));
        }
    }
    return features;
}

/// Removes the per-utterance mean of every mel bin in place.
pub fn subtractGlobalMean(features: []f32) void {
    const frames = features.len / num_mel_bins;
    if (frames == 0) return;
    for (0..num_mel_bins) |bin| {
        var sum: f64 = 0;
        for (0..frames) |t| sum += features[t * num_mel_bins + bin];
        const mean: f32 = @floatCast(sum / @as(f64, @floatFromInt(frames)));
        for (0..frames) |t| features[t * num_mel_bins + bin] -= mean;
    }
}

/// One loaded speaker verification model, specialised to `window_samples`.
pub const Embedder = struct {
    allocator: std.mem.Allocator,
    session: Session,
    /// Model runs are serialised: the native executor keeps per-session
    /// scratch, and diarization is a small tail on a transcription anyway.
    run_lock: std.atomic.Mutex = .unlocked,

    pub fn load(allocator: std.mem.Allocator, onnx_path: []const u8) !Embedder {
        var dims: onnx_graph.DimOverrides = .empty;
        defer dims.deinit(allocator);
        try dims.put(allocator, "N", 1);
        try dims.put(allocator, "T", @intCast(fbankFrameCount(window_samples)));
        const session = try backends.imported_onnx_session.createSessionWithOptions(allocator, onnx_path, .native, .{
            .dim_overrides = &dims,
        });
        return .{ .allocator = allocator, .session = session };
    }

    pub fn deinit(self: *Embedder) void {
        self.session.close();
    }

    /// The unit-norm embedding of exactly `window_samples` 16 kHz samples.
    /// Caller frees the result.
    pub fn embedAlloc(self: *Embedder, allocator: std.mem.Allocator, samples: []const f32) ![]f32 {
        if (samples.len != window_samples) return error.UnexpectedWindowLength;
        const features = try kaldiFbankAlloc(allocator, samples);
        defer allocator.free(features);
        const frames = features.len / num_mel_bins;
        subtractGlobalMean(features);

        const shape = [_]i64{ 1, @intCast(frames), @intCast(num_mel_bins) };
        var input = try Tensor.initFloat32(allocator, "x", &shape, features);
        defer input.deinit();

        while (!self.run_lock.tryLock()) std.atomic.spinLoopHint();
        defer self.run_lock.unlock();
        const outputs = try self.session.run(&[_]Tensor{input}, allocator);
        defer {
            for (outputs) |*output| output.deinit();
            allocator.free(outputs);
        }
        if (outputs.len == 0) return error.NoOutputTensors;
        const raw = outputs[0].asFloat32();
        if (raw.len < embedding_dim) return error.UnexpectedEmbeddingShape;

        const embedding = try allocator.alloc(f32, embedding_dim);
        errdefer allocator.free(embedding);
        var norm: f64 = 0;
        for (raw[0..embedding_dim]) |v| norm += @as(f64, v) * @as(f64, v);
        const scale: f32 = if (norm > 0) @floatCast(1.0 / @sqrt(norm)) else 0;
        for (embedding, raw[0..embedding_dim]) |*dst, src| dst.* = src * scale;
        return embedding;
    }
};

pub fn cosine(a: []const f32, b: []const f32) f32 {
    var dot: f64 = 0;
    for (a, b) |x, y| dot += @as(f64, x) * @as(f64, y);
    return @floatCast(dot);
}

/// Cosine similarity at or above which two embeddings count as the same
/// speaker. Verification-quality embeddings of one speaker score around
/// 0.6–0.9 on clean speech and different speakers 0.1–0.4; windows that
/// straddle a turn sit in between, which is why linkage below averages
/// over members instead of comparing centroids (whose shrinking norms
/// inflate the cosine of mixed clusters).
pub const default_similarity_threshold: f32 = 0.45;

/// Groups unit-norm embeddings into speakers by average-linkage
/// agglomeration on cosine similarity: a greedy pass seeds clusters, clusters
/// whose members still agree on average are merged, and every embedding is
/// then assigned to the cluster it agrees with most. Labels are dense and
/// numbered in order of first appearance. Needs an `n×n` similarity matrix,
/// so callers bound `n` (see `DiarizeOptions.max_windows`). Caller frees.
pub fn clusterSpeakers(allocator: std.mem.Allocator, embeddings: []const []const f32, threshold: f32) ![]u8 {
    const n = embeddings.len;
    const labels = try allocator.alloc(u8, n);
    errdefer allocator.free(labels);
    if (n == 0) return labels;
    const dim = embeddings[0].len;
    for (embeddings) |embedding| if (embedding.len != dim) return error.UnexpectedEmbeddingShape;

    const sim = try allocator.alloc(f32, n * n);
    defer allocator.free(sim);
    for (0..n) |i| {
        sim[i * n + i] = 1.0;
        for (i + 1..n) |j| {
            const value = cosine(embeddings[i], embeddings[j]);
            sim[i * n + j] = value;
            sim[j * n + i] = value;
        }
    }

    // cluster[i] is the provisional cluster of embedding i; clusters are
    // ids in 0..cluster_count and may become empty after merges.
    const cluster = try allocator.alloc(usize, n);
    defer allocator.free(cluster);
    var cluster_count: usize = 0;
    const linkage = struct {
        /// Mean similarity between embedding `i` and the members of `c`
        /// (excluding `i` itself); null when `c` has no other members.
        fn toCluster(s: []const f32, members: []const usize, count: usize, i: usize, c: usize) ?f32 {
            var total: f64 = 0;
            var found: usize = 0;
            for (members, 0..) |m, j| {
                if (m != c or j == i) continue;
                total += s[i * count + j];
                found += 1;
            }
            if (found == 0) return null;
            return @floatCast(total / @as(f64, @floatFromInt(found)));
        }
        /// Mean similarity between the members of clusters `a` and `b`.
        fn between(s: []const f32, members: []const usize, count: usize, a: usize, b: usize) ?f32 {
            var total: f64 = 0;
            var found: usize = 0;
            for (members, 0..) |ma, i| {
                if (ma != a) continue;
                for (members, 0..) |mb, j| {
                    if (mb != b) continue;
                    total += s[i * count + j];
                    found += 1;
                }
            }
            if (found == 0) return null;
            return @floatCast(total / @as(f64, @floatFromInt(found)));
        }
    };

    // Greedy seeding.
    for (0..n) |i| {
        var best: ?usize = null;
        var best_score: f32 = threshold;
        for (0..cluster_count) |c| {
            const score = linkage.toCluster(sim, cluster[0..i], n, i, c) orelse continue;
            if (score >= best_score) {
                best_score = score;
                best = c;
            }
        }
        if (best) |c| {
            cluster[i] = c;
        } else {
            cluster[i] = cluster_count;
            cluster_count += 1;
        }
    }

    // Merge clusters that still agree on average.
    while (cluster_count > 1) {
        var best_pair: ?[2]usize = null;
        var best_score: f32 = threshold;
        for (0..cluster_count) |a| {
            for (a + 1..cluster_count) |b| {
                const score = linkage.between(sim, cluster, n, a, b) orelse continue;
                if (score >= best_score) {
                    best_score = score;
                    best_pair = .{ a, b };
                }
            }
        }
        const pair = best_pair orelse break;
        for (cluster) |*c| {
            if (c.* == pair[1]) c.* = pair[0];
        }
    }

    // Final assignment: an embedding moves to another cluster only when it
    // agrees with that cluster more than with its own (a singleton keeps
    // its cluster unless another one clears the threshold). Then
    // first-appearance numbering.
    const final = try allocator.alloc(usize, n);
    defer allocator.free(final);
    for (0..n) |i| {
        var best: usize = cluster[i];
        var best_score: f32 = linkage.toCluster(sim, cluster, n, i, cluster[i]) orelse threshold;
        for (0..cluster_count) |c| {
            if (c == cluster[i]) continue;
            const score = linkage.toCluster(sim, cluster, n, i, c) orelse continue;
            if (score > best_score) {
                best_score = score;
                best = c;
            }
        }
        final[i] = best;
    }
    const renumber = try allocator.alloc(?u8, cluster_count);
    defer allocator.free(renumber);
    @memset(renumber, null);
    var next_label: u8 = 0;
    for (final, labels) |c, *label| {
        if (renumber[c] == null) {
            renumber[c] = next_label;
            next_label +|= 1;
        }
        label.* = renumber[c].?;
    }
    return labels;
}

pub const DiarizeOptions = struct {
    /// Distance between consecutive window starts. Windows overlap by
    /// `window_seconds - hop_s`, so every instant is covered twice.
    hop_s: f32 = 1.5,
    /// Upper bound on model runs (and on the side of the similarity matrix)
    /// for one recording; the hop widens to fit. 2048 windows at a 1.5 s hop
    /// cover 51 minutes of speech.
    max_windows: usize = 2048,
    similarity_threshold: f32 = default_similarity_threshold,
};

/// Fills `out` (one window) with the clip audio starting at `start`, tiling
/// the clip when it is shorter than a window so short recordings still get
/// a full-length input.
fn fillWindow(out: []f32, samples: []const f32, start: usize) void {
    if (samples.len >= out.len) {
        const begin = @min(start, samples.len - out.len);
        @memcpy(out, samples[begin .. begin + out.len]);
        return;
    }
    var cursor: usize = 0;
    while (cursor < out.len) {
        const n = @min(samples.len, out.len - cursor);
        @memcpy(out[cursor .. cursor + n], samples[0..n]);
        cursor += n;
    }
}

const Window = struct { start: usize, label: u8 = 0 };

/// Window starts covering the spoken parts of the clip: a window every
/// `hop` samples from the clip start, keeping only windows that overlap a
/// transcript phrase, clamped so the last window ends at the clip end.
fn speechWindowsAlloc(
    allocator: std.mem.Allocator,
    samples_len: usize,
    segments: []const long_transcription.Segment,
    hop: usize,
) ![]Window {
    var windows = std.ArrayList(Window).empty;
    errdefer windows.deinit(allocator);
    const last_start = samples_len -| window_samples;
    var start: usize = 0;
    while (true) {
        const clamped = @min(start, last_start);
        const end = clamped + window_samples;
        var overlaps = false;
        for (segments) |segment| {
            const seg_start = msToSamples(segment.start_ms);
            const seg_end = @max(msToSamples(segment.end_ms), seg_start + 1);
            if (seg_start < end and seg_end > clamped) {
                overlaps = true;
                break;
            }
        }
        if (overlaps and (windows.items.len == 0 or windows.items[windows.items.len - 1].start != clamped)) {
            try windows.append(allocator, .{ .start = clamped });
        }
        if (clamped >= last_start) break;
        start += hop;
    }
    return windows.toOwnedSlice(allocator);
}

fn msToSamples(ms: u64) usize {
    return @intCast(ms * sample_rate / 1000);
}

/// Label of the window whose centre is nearest to `position`.
fn nearestWindowLabel(windows: []const Window, position: usize) u8 {
    var best: usize = 0;
    var best_distance: usize = std.math.maxInt(usize);
    for (windows, 0..) |window, i| {
        const centre = window.start + window_samples / 2;
        const distance = if (centre > position) centre - position else position - centre;
        if (distance < best_distance) {
            best_distance = distance;
            best = i;
        }
    }
    return windows[best].label;
}

/// Speaker-attributed phrases for a transcript: 3 s windows over the spoken
/// parts of `samples` (16 kHz mono) are embedded and clustered, every word
/// takes the speaker of the window nearest its centre, and a phrase whose
/// words change speaker is split at the change so each returned segment has
/// one `speaker_index`. Phrases keep their order; text outside word spans
/// is not lost because words are cut from the phrase text. Caller owns the
/// result (free with `long_transcription.freeSegments`).
pub fn diarizeSegmentsAlloc(
    allocator: std.mem.Allocator,
    embedder: *Embedder,
    samples: []const f32,
    segments: []const long_transcription.Segment,
    options: DiarizeOptions,
) ![]long_transcription.Segment {
    var out = std.ArrayList(long_transcription.Segment).empty;
    errdefer {
        for (out.items) |segment| freeSegment(allocator, segment);
        out.deinit(allocator);
    }
    if (segments.len == 0 or samples.len == 0) return out.toOwnedSlice(allocator);

    var hop: usize = @max(@as(usize, 1), @as(usize, @intFromFloat(@as(f64, options.hop_s) * @as(f64, @floatFromInt(sample_rate)))));
    var windows = try speechWindowsAlloc(allocator, samples.len, segments, hop);
    while (windows.len > @max(options.max_windows, 1)) {
        allocator.free(windows);
        hop *= 2;
        windows = try speechWindowsAlloc(allocator, samples.len, segments, hop);
    }
    defer allocator.free(windows);

    // Embed and cluster the windows.
    const embeddings = try allocator.alloc([]const f32, windows.len);
    var embedded: usize = 0;
    defer {
        for (embeddings[0..embedded]) |embedding| allocator.free(embedding);
        allocator.free(embeddings);
    }
    const window = try allocator.alloc(f32, window_samples);
    defer allocator.free(window);
    for (windows) |w| {
        fillWindow(window, samples, w.start);
        embeddings[embedded] = try embedder.embedAlloc(allocator, window);
        embedded += 1;
    }
    const labels = try clusterSpeakers(allocator, embeddings, options.similarity_threshold);
    defer allocator.free(labels);
    for (windows, labels) |*w, label| w.label = label;

    // Assign words, smooth, split.
    var word_labels = std.ArrayList(u8).empty;
    defer word_labels.deinit(allocator);
    for (segments) |segment| {
        if (segment.words.len == 0) {
            const centre = (msToSamples(segment.start_ms) + msToSamples(segment.end_ms)) / 2;
            var copy = try dupeSegment(allocator, segment, segment.words, segment.start_ms, segment.end_ms);
            copy.speaker_index = nearestWindowLabel(windows, centre);
            try out.append(allocator, copy);
            continue;
        }
        word_labels.clearRetainingCapacity();
        for (segment.words) |word| {
            const centre = (msToSamples(word.start_ms) + msToSamples(word.end_ms)) / 2;
            try word_labels.append(allocator, nearestWindowLabel(windows, centre));
        }
        // A single word attributed differently from both neighbours is
        // window jitter, not a turn.
        const wl = word_labels.items;
        if (wl.len >= 3) {
            for (1..wl.len - 1) |i| {
                if (wl[i - 1] == wl[i + 1] and wl[i] != wl[i - 1]) wl[i] = wl[i - 1];
            }
        }
        var run_start: usize = 0;
        while (run_start < wl.len) {
            var run_end = run_start + 1;
            while (run_end < wl.len and wl[run_end] == wl[run_start]) run_end += 1;
            const words = segment.words[run_start..run_end];
            const start_ms = if (run_start == 0) segment.start_ms else words[0].start_ms;
            const end_ms = if (run_end == wl.len) segment.end_ms else words[words.len - 1].end_ms;
            var piece = try dupeSegment(allocator, segment, words, start_ms, end_ms);
            piece.speaker_index = wl[run_start];
            try out.append(allocator, piece);
            run_start = run_end;
        }
    }
    return out.toOwnedSlice(allocator);
}

/// Copies `words` of `segment` into a new segment; the text is the phrase
/// text when all words are kept, else the words joined with spaces.
fn dupeSegment(
    allocator: std.mem.Allocator,
    segment: long_transcription.Segment,
    words: []const long_transcription.Word,
    start_ms: u64,
    end_ms: u64,
) !long_transcription.Segment {
    const text = if (words.len == segment.words.len)
        try allocator.dupe(u8, segment.text)
    else blk: {
        var joined = std.ArrayList(u8).empty;
        errdefer joined.deinit(allocator);
        for (words, 0..) |word, i| {
            if (i > 0) try joined.append(allocator, ' ');
            try joined.appendSlice(allocator, std.mem.trim(u8, word.word, " "));
        }
        break :blk try joined.toOwnedSlice(allocator);
    };
    errdefer allocator.free(text);
    const owned_words = try allocator.alloc(long_transcription.Word, words.len);
    var filled: usize = 0;
    errdefer {
        for (owned_words[0..filled]) |word| allocator.free(word.word);
        allocator.free(owned_words);
    }
    for (words, owned_words) |src, *dst| {
        dst.* = .{ .word = try allocator.dupe(u8, src.word), .start_ms = src.start_ms, .end_ms = src.end_ms };
        filled += 1;
    }
    return .{ .text = text, .start_ms = start_ms, .end_ms = end_ms, .words = owned_words, .speaker_index = null };
}

fn freeSegment(allocator: std.mem.Allocator, segment: long_transcription.Segment) void {
    allocator.free(segment.text);
    for (segment.words) |word| allocator.free(word.word);
    allocator.free(segment.words);
}

/// `SPEAKER_00`-style label for a cluster index. The buffer must hold 11 bytes.
pub fn speakerLabel(buf: []u8, index: u8) []const u8 {
    return std.fmt.bufPrint(buf, "SPEAKER_{d:0>2}", .{index}) catch unreachable;
}

const static_labels = [_][]const u8{
    "SPEAKER_00",  "SPEAKER_01",  "SPEAKER_02",  "SPEAKER_03",  "SPEAKER_04",  "SPEAKER_05",  "SPEAKER_06",  "SPEAKER_07",
    "SPEAKER_08",  "SPEAKER_09",  "SPEAKER_10",  "SPEAKER_11",  "SPEAKER_12",  "SPEAKER_13",  "SPEAKER_14",  "SPEAKER_15",
    "SPEAKER_16",  "SPEAKER_17",  "SPEAKER_18",  "SPEAKER_19",  "SPEAKER_20",  "SPEAKER_21",  "SPEAKER_22",  "SPEAKER_23",
    "SPEAKER_24",  "SPEAKER_25",  "SPEAKER_26",  "SPEAKER_27",  "SPEAKER_28",  "SPEAKER_29",  "SPEAKER_30",  "SPEAKER_31",
    "SPEAKER_32",  "SPEAKER_33",  "SPEAKER_34",  "SPEAKER_35",  "SPEAKER_36",  "SPEAKER_37",  "SPEAKER_38",  "SPEAKER_39",
    "SPEAKER_40",  "SPEAKER_41",  "SPEAKER_42",  "SPEAKER_43",  "SPEAKER_44",  "SPEAKER_45",  "SPEAKER_46",  "SPEAKER_47",
    "SPEAKER_48",  "SPEAKER_49",  "SPEAKER_50",  "SPEAKER_51",  "SPEAKER_52",  "SPEAKER_53",  "SPEAKER_54",  "SPEAKER_55",
    "SPEAKER_56",  "SPEAKER_57",  "SPEAKER_58",  "SPEAKER_59",  "SPEAKER_60",  "SPEAKER_61",  "SPEAKER_62",  "SPEAKER_63",
    "SPEAKER_64",  "SPEAKER_65",  "SPEAKER_66",  "SPEAKER_67",  "SPEAKER_68",  "SPEAKER_69",  "SPEAKER_70",  "SPEAKER_71",
    "SPEAKER_72",  "SPEAKER_73",  "SPEAKER_74",  "SPEAKER_75",  "SPEAKER_76",  "SPEAKER_77",  "SPEAKER_78",  "SPEAKER_79",
    "SPEAKER_80",  "SPEAKER_81",  "SPEAKER_82",  "SPEAKER_83",  "SPEAKER_84",  "SPEAKER_85",  "SPEAKER_86",  "SPEAKER_87",
    "SPEAKER_88",  "SPEAKER_89",  "SPEAKER_90",  "SPEAKER_91",  "SPEAKER_92",  "SPEAKER_93",  "SPEAKER_94",  "SPEAKER_95",
    "SPEAKER_96",  "SPEAKER_97",  "SPEAKER_98",  "SPEAKER_99",  "SPEAKER_100", "SPEAKER_101", "SPEAKER_102", "SPEAKER_103",
    "SPEAKER_104", "SPEAKER_105", "SPEAKER_106", "SPEAKER_107", "SPEAKER_108", "SPEAKER_109", "SPEAKER_110", "SPEAKER_111",
    "SPEAKER_112", "SPEAKER_113", "SPEAKER_114", "SPEAKER_115", "SPEAKER_116", "SPEAKER_117", "SPEAKER_118", "SPEAKER_119",
    "SPEAKER_120", "SPEAKER_121", "SPEAKER_122", "SPEAKER_123", "SPEAKER_124", "SPEAKER_125", "SPEAKER_126", "SPEAKER_127",
    "SPEAKER_128", "SPEAKER_129", "SPEAKER_130", "SPEAKER_131", "SPEAKER_132", "SPEAKER_133", "SPEAKER_134", "SPEAKER_135",
    "SPEAKER_136", "SPEAKER_137", "SPEAKER_138", "SPEAKER_139", "SPEAKER_140", "SPEAKER_141", "SPEAKER_142", "SPEAKER_143",
    "SPEAKER_144", "SPEAKER_145", "SPEAKER_146", "SPEAKER_147", "SPEAKER_148", "SPEAKER_149", "SPEAKER_150", "SPEAKER_151",
    "SPEAKER_152", "SPEAKER_153", "SPEAKER_154", "SPEAKER_155", "SPEAKER_156", "SPEAKER_157", "SPEAKER_158", "SPEAKER_159",
    "SPEAKER_160", "SPEAKER_161", "SPEAKER_162", "SPEAKER_163", "SPEAKER_164", "SPEAKER_165", "SPEAKER_166", "SPEAKER_167",
    "SPEAKER_168", "SPEAKER_169", "SPEAKER_170", "SPEAKER_171", "SPEAKER_172", "SPEAKER_173", "SPEAKER_174", "SPEAKER_175",
    "SPEAKER_176", "SPEAKER_177", "SPEAKER_178", "SPEAKER_179", "SPEAKER_180", "SPEAKER_181", "SPEAKER_182", "SPEAKER_183",
    "SPEAKER_184", "SPEAKER_185", "SPEAKER_186", "SPEAKER_187", "SPEAKER_188", "SPEAKER_189", "SPEAKER_190", "SPEAKER_191",
    "SPEAKER_192", "SPEAKER_193", "SPEAKER_194", "SPEAKER_195", "SPEAKER_196", "SPEAKER_197", "SPEAKER_198", "SPEAKER_199",
    "SPEAKER_200", "SPEAKER_201", "SPEAKER_202", "SPEAKER_203", "SPEAKER_204", "SPEAKER_205", "SPEAKER_206", "SPEAKER_207",
    "SPEAKER_208", "SPEAKER_209", "SPEAKER_210", "SPEAKER_211", "SPEAKER_212", "SPEAKER_213", "SPEAKER_214", "SPEAKER_215",
    "SPEAKER_216", "SPEAKER_217", "SPEAKER_218", "SPEAKER_219", "SPEAKER_220", "SPEAKER_221", "SPEAKER_222", "SPEAKER_223",
    "SPEAKER_224", "SPEAKER_225", "SPEAKER_226", "SPEAKER_227", "SPEAKER_228", "SPEAKER_229", "SPEAKER_230", "SPEAKER_231",
    "SPEAKER_232", "SPEAKER_233", "SPEAKER_234", "SPEAKER_235", "SPEAKER_236", "SPEAKER_237", "SPEAKER_238", "SPEAKER_239",
    "SPEAKER_240", "SPEAKER_241", "SPEAKER_242", "SPEAKER_243", "SPEAKER_244", "SPEAKER_245", "SPEAKER_246", "SPEAKER_247",
    "SPEAKER_248", "SPEAKER_249", "SPEAKER_250", "SPEAKER_251", "SPEAKER_252", "SPEAKER_253", "SPEAKER_254", "SPEAKER_255",
};

/// `speakerLabel` with static storage, for responses that borrow strings.
pub fn speakerLabelStatic(index: u8) []const u8 {
    return static_labels[index];
}

fn syntheticTestSignal(out: []f32) void {
    var state: u32 = 12345;
    for (out, 0..) |*sample, i| {
        state = state *% 1664525 +% 1013904223;
        const noise = @as(f32, @floatFromInt(state >> 8)) / 16777216.0 * 2.0 - 1.0;
        const phase = 2.0 * std.math.pi * 440.0 * @as(f64, @floatFromInt(i)) / 16000.0;
        sample.* = 0.1 * noise + 0.5 * @as(f32, @floatCast(@sin(phase)));
    }
}

test "fbank frame count follows torchaudio snip_edges=true" {
    try std.testing.expectEqual(@as(usize, 0), fbankFrameCount(0));
    try std.testing.expectEqual(@as(usize, 0), fbankFrameCount(399));
    try std.testing.expectEqual(@as(usize, 1), fbankFrameCount(400));
    try std.testing.expectEqual(@as(usize, 48), fbankFrameCount(8000));
    try std.testing.expectEqual(@as(usize, 298), fbankFrameCount(48000));
}

test "kaldi fbank matches torchaudio on a synthetic signal" {
    const reference = @import("speaker_embedding_test_reference.zig");
    var signal: [8000]f32 = undefined;
    syntheticTestSignal(&signal);
    const features = try kaldiFbankAlloc(std.testing.allocator, &signal);
    defer std.testing.allocator.free(features);
    try std.testing.expectEqual(@as(usize, 48 * num_mel_bins), features.len);
    inline for (reference.frames) |frame| {
        const got = features[frame.index * num_mel_bins ..][0..num_mel_bins];
        for (got, frame.values) |g, want| try std.testing.expectApproxEqAbs(want, g, 2e-3);
    }
    subtractGlobalMean(features);
    for (0..num_mel_bins) |bin| {
        var sum: f64 = 0;
        for (0..48) |t| sum += features[t * num_mel_bins + bin];
        try std.testing.expectApproxEqAbs(@as(f64, 0), sum / 48.0, 1e-4);
    }
}

test "cluster speakers groups by cosine and numbers by first appearance" {
    const a = [_]f32{ 1, 0, 0 };
    const a2 = [_]f32{ 0.96, 0.28, 0 };
    const b = [_]f32{ 0, 1, 0 };
    const b2 = [_]f32{ 0.28, 0.96, 0 };
    const c = [_]f32{ 0, 0, 1 };
    const embeddings = [_][]const f32{ &b, &a, &b2, &c, &a2 };
    const labels = try clusterSpeakers(std.testing.allocator, &embeddings, 0.8);
    defer std.testing.allocator.free(labels);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 2, 1 }, labels);

    const none = try clusterSpeakers(std.testing.allocator, &.{}, 0.8);
    defer std.testing.allocator.free(none);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}

test "speech windows cover phrases at the hop and clamp to the clip" {
    const words = [_]long_transcription.Word{};
    var segments = [_]long_transcription.Segment{
        .{ .text = @constCast("a"), .start_ms = 0, .end_ms = 2000, .words = @constCast(&words) },
        .{ .text = @constCast("b"), .start_ms = 9000, .end_ms = 10000, .words = @constCast(&words) },
    };
    // 10 s clip, 1.5 s hop: windows at 0 and 1.5 s overlap phrase a; the
    // one at 6 s ends exactly where phrase b starts, so only the clamped
    // last window (7 s) covers it; 3 s and 4.5 s touch neither.
    const windows = try speechWindowsAlloc(std.testing.allocator, 160000, &segments, 24000);
    defer std.testing.allocator.free(windows);
    var starts: [8]usize = undefined;
    for (windows, 0..) |w, i| starts[i] = w.start;
    try std.testing.expectEqualSlices(usize, &[_]usize{ 0, 24000, 112000 }, starts[0..windows.len]);

    // A clip shorter than a window still yields one window.
    const short = try speechWindowsAlloc(std.testing.allocator, 8000, segments[0..1], 24000);
    defer std.testing.allocator.free(short);
    try std.testing.expectEqual(@as(usize, 1), short.len);
}

test "nearest window label picks the window centred closest" {
    const windows = [_]Window{ .{ .start = 0, .label = 0 }, .{ .start = 48000, .label = 1 }, .{ .start = 96000, .label = 2 } };
    try std.testing.expectEqual(@as(u8, 0), nearestWindowLabel(&windows, 10000));
    try std.testing.expectEqual(@as(u8, 1), nearestWindowLabel(&windows, 70000));
    try std.testing.expectEqual(@as(u8, 2), nearestWindowLabel(&windows, 200000));
}

test "fill window tiles clips shorter than a window" {
    var out: [8]f32 = undefined;
    const clip = [_]f32{ 1, 2, 3 };
    fillWindow(&out, &clip, 0);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2, 3, 1, 2, 3, 1, 2 }, &out);
    const long = [_]f32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    fillWindow(&out, &long, 5);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 2, 3, 4, 5, 6, 7, 8, 9 }, &out);
}

test "speaker label formatting" {
    var buf: [10]u8 = undefined;
    try std.testing.expectEqualStrings("SPEAKER_00", speakerLabel(&buf, 0));
    try std.testing.expectEqualStrings("SPEAKER_12", speakerLabel(&buf, 12));
    try std.testing.expectEqualStrings("SPEAKER_00", speakerLabelStatic(0));
    try std.testing.expectEqualStrings("SPEAKER_255", speakerLabelStatic(255));
}
