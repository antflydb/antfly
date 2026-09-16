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

//! Windowed transcription for audio longer than one Whisper context.
//!
//! Whisper encodes exactly 30 s per pass and `audio.whisperMelFromPcm`
//! truncates anything longer. This module plans windows that never exceed
//! the model context, cuts them at the quietest point near the boundary so
//! words are not split, transcribes each window through any `transcriber`
//! exposing `transcribePcm(samples, sample_rate) !TranscribeResult`, and
//! stitches the results into timed segments.

const std = @import("std");
const audio = @import("audio.zig");
const vad = @import("vad.zig");
const transcription = @import("transcription.zig");
const whisper_timestamps = @import("whisper_timestamps.zig");

pub const Options = struct {
    /// Longest window handed to the model. Whisper is trained on 30 s.
    max_window_s: u32 = audio.WHISPER_CHUNK_LENGTH,
    /// Skip windows with no frame above the VAD threshold. Whisper tends to
    /// hallucinate ("Thank you.") on pure silence.
    skip_silent_windows: bool = true,
    vad: vad.Config = .{},
    /// Text presented to the decoder as preceding context for every window:
    /// names and terms the recognizer should prefer.
    initial_prompt: ?[]const u8 = null,
    /// Also condition each window on the text decoded from the previous one,
    /// which keeps casing, punctuation, and phrasing consistent across cuts.
    condition_on_previous: bool = true,
};

pub const Word = struct {
    word: []u8,
    start_ms: u64,
    end_ms: u64,
};

pub const Window = struct {
    start: usize,
    end: usize,
};

pub const Segment = struct {
    text: []u8,
    start_ms: u64,
    end_ms: u64,
    /// Word spans estimated inside the phrase; see `whisper_timestamps.splitWords`.
    words: []Word,
};

pub fn freeSegments(allocator: std.mem.Allocator, segments: []Segment) void {
    for (segments) |segment| {
        allocator.free(segment.text);
        for (segment.words) |word| allocator.free(word.word);
        allocator.free(segment.words);
    }
    allocator.free(segments);
}

/// Build an owned segment with word spans from a phrase.
pub fn makeSegment(allocator: std.mem.Allocator, text: []const u8, start_ms: u64, end_ms: u64) !Segment {
    const owned_text = try allocator.dupe(u8, text);
    errdefer allocator.free(owned_text);
    const spans = try whisper_timestamps.splitWords(allocator, owned_text, start_ms, end_ms);
    defer allocator.free(spans);
    var words = try allocator.alloc(Word, spans.len);
    var built: usize = 0;
    errdefer {
        for (words[0..built]) |word| allocator.free(word.word);
        allocator.free(words);
    }
    for (spans, 0..) |span, i| {
        words[i] = .{ .word = try allocator.dupe(u8, span.word), .start_ms = span.start_ms, .end_ms = span.end_ms };
        built += 1;
    }
    return .{ .text = owned_text, .start_ms = start_ms, .end_ms = end_ms, .words = words };
}

pub const Result = struct {
    allocator: std.mem.Allocator,
    segments: []Segment,
    /// Segment texts joined with single spaces.
    text: []u8,
    language: ?[]u8,
    duration_ms: u64,
    windows: usize,
    /// Summed over decoded windows.
    timing: transcription.Timing = .{},

    pub fn deinit(self: *Result) void {
        freeSegments(self.allocator, self.segments);
        self.allocator.free(self.text);
        if (self.language) |language| self.allocator.free(language);
    }
};

/// Plan model windows over `samples`. Every window is at most
/// `max_window_s` long. Long inputs are cut at the quietest frame in the
/// last quarter of the allowed span so the boundary prefers a pause.
pub fn planWindows(
    allocator: std.mem.Allocator,
    samples: []const f32,
    sample_rate: u32,
    options: Options,
) ![]Window {
    if (sample_rate == 0 or options.max_window_s == 0) return error.UnsupportedAudioFormat;
    var out = std.ArrayListUnmanaged(Window).empty;
    errdefer out.deinit(allocator);
    if (samples.len == 0) return out.toOwnedSlice(allocator);

    const max_samples = std.math.mul(usize, @as(usize, sample_rate), @as(usize, options.max_window_s)) catch
        return error.UnsupportedAudioFormat;
    var start: usize = 0;
    while (samples.len - start > max_samples) {
        const hard_end = start + max_samples;
        const search_lo = start + (max_samples / 4) * 3;
        var split = vad.quietestSplit(samples, sample_rate, search_lo, hard_end, options.vad);
        if (split <= start or split > hard_end) split = hard_end;
        try out.append(allocator, .{ .start = start, .end = split });
        start = split;
    }
    try out.append(allocator, .{ .start = start, .end = samples.len });
    return out.toOwnedSlice(allocator);
}

/// Transcribe `samples` window by window. `transcriber` is any value with
/// `transcribePcmConditioned(self, samples, sample_rate, prompt_prefix: []const i32)
/// !transcription.TranscribeResult` and
/// `encodePromptText(self, allocator, text) ![]i32`.
pub fn transcribeLong(
    allocator: std.mem.Allocator,
    transcriber: anytype,
    samples: []const f32,
    sample_rate: u32,
    options: Options,
) !Result {
    if (sample_rate == 0 or samples.len == 0) return error.UnsupportedAudioFormat;
    const windows = try planWindows(allocator, samples, sample_rate, options);
    defer allocator.free(windows);

    var segments = std.ArrayListUnmanaged(Segment).empty;
    errdefer {
        for (segments.items) |segment| {
            allocator.free(segment.text);
            for (segment.words) |word| allocator.free(word.word);
            allocator.free(segment.words);
        }
        segments.deinit(allocator);
    }
    var language: ?[]u8 = null;
    errdefer if (language) |value| allocator.free(value);
    var timing = transcription.Timing{};

    const initial_tokens: []i32 = if (options.initial_prompt) |prompt|
        try transcriber.encodePromptText(allocator, prompt)
    else
        try allocator.alloc(i32, 0);
    defer allocator.free(initial_tokens);
    var previous_tokens: []i32 = try allocator.alloc(i32, 0);
    defer allocator.free(previous_tokens);
    var prefix = std.ArrayListUnmanaged(i32).empty;
    defer prefix.deinit(allocator);

    for (windows) |window| {
        const window_samples = samples[window.start..window.end];
        if (window_samples.len == 0) continue;
        if (options.skip_silent_windows and !vad.hasSpeech(window_samples, sample_rate, options.vad)) continue;

        prefix.clearRetainingCapacity();
        try prefix.appendSlice(allocator, initial_tokens);
        if (options.condition_on_previous) try prefix.appendSlice(allocator, previous_tokens);

        var result: transcription.TranscribeResult = try transcriber.transcribePcmConditioned(window_samples, sample_rate, prefix.items);
        defer result.deinit();
        timing.add(result.timing);
        if (language == null) if (result.language) |detected| {
            language = try allocator.dupe(u8, detected);
            // Detect once per clip; later windows keep the same language so a
            // name-heavy window cannot flip it mid-clip.
            _ = transcriber.lockLanguage(detected);
        };
        const window_start_ms = vad.samplesToMs(sample_rate, window.start);
        const window_end_ms = vad.samplesToMs(sample_rate, window.end);
        if (result.segments.len > 0) {
            for (result.segments) |timed| {
                const start_ms = @min(window_end_ms, window_start_ms + timed.start_ms);
                const end_ms = @min(window_end_ms, @max(start_ms, window_start_ms + timed.end_ms));
                try segments.append(allocator, try makeSegment(allocator, timed.text, start_ms, end_ms));
            }
        } else {
            const trimmed = std.mem.trim(u8, result.text, " \t\r\n");
            if (trimmed.len > 0) try segments.append(allocator, try makeSegment(allocator, trimmed, window_start_ms, window_end_ms));
        }
        if (options.condition_on_previous) {
            allocator.free(previous_tokens);
            previous_tokens = try allocator.dupe(i32, result.tokens);
        }
    }

    const text = try joinSegments(allocator, segments.items);
    errdefer allocator.free(text);
    return .{
        .allocator = allocator,
        .segments = try segments.toOwnedSlice(allocator),
        .text = text,
        .language = language,
        .duration_ms = vad.samplesToMs(sample_rate, samples.len),
        .windows = windows.len,
        .timing = timing,
    };
}

pub fn joinSegments(allocator: std.mem.Allocator, segments: []const Segment) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    for (segments, 0..) |segment, index| {
        if (index > 0) try out.append(allocator, ' ');
        try out.appendSlice(allocator, segment.text);
    }
    return out.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const FakeTranscriber = struct {
    allocator: std.mem.Allocator,
    calls: usize = 0,
    text: []const u8 = "hello",
    language: ?[]const u8 = "en",
    /// Prefix length observed on each call, for conditioning assertions.
    prefix_lens: [8]usize = [_]usize{0} ** 8,
    /// When set, every result carries two timed phrases instead of plain text.
    timed: bool = false,
    /// Copied, because the window result that carried the code is freed
    /// before the test inspects it.
    locked_language_buf: [8]u8 = undefined,
    locked_language: ?[]const u8 = null,

    pub fn encodePromptText(_: *FakeTranscriber, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        var count: usize = 0;
        var it = std.mem.tokenizeScalar(u8, text, ' ');
        while (it.next()) |_| count += 1;
        const out = try allocator.alloc(i32, count);
        for (out, 0..) |*t, i| t.* = @intCast(i + 1);
        return out;
    }

    pub fn lockLanguage(self: *FakeTranscriber, code: []const u8) bool {
        const n = @min(code.len, self.locked_language_buf.len);
        @memcpy(self.locked_language_buf[0..n], code[0..n]);
        self.locked_language = self.locked_language_buf[0..n];
        return true;
    }

    pub fn transcribePcmConditioned(self: *FakeTranscriber, samples: []const f32, sample_rate: u32, prefix: []const i32) !transcription.TranscribeResult {
        if (self.calls < self.prefix_lens.len) self.prefix_lens[self.calls] = prefix.len;
        self.calls += 1;
        const seconds = samples.len / sample_rate;
        const text = try std.fmt.allocPrint(self.allocator, "{s}{d}({d}s)", .{ self.text, self.calls, seconds });
        errdefer self.allocator.free(text);
        const tokens = try self.allocator.alloc(i32, 3);
        errdefer self.allocator.free(tokens);
        @memset(tokens, @intCast(self.calls));
        var segments: []transcription.TimedSegment = &.{};
        if (self.timed) {
            const timed = try self.allocator.alloc(transcription.TimedSegment, 2);
            timed[0] = .{ .text = try self.allocator.dupe(u8, "first phrase"), .start_ms = 0, .end_ms = 1000 };
            timed[1] = .{ .text = try self.allocator.dupe(u8, "second"), .start_ms = 1000, .end_ms = 2500 };
            segments = timed;
        }
        return .{
            .text = text,
            .language = if (self.language) |l| try self.allocator.dupe(u8, l) else null,
            .allocator = self.allocator,
            .segments = segments,
            .tokens = tokens,
        };
    }
};

fn tone(buffer: []f32, sample_rate: u32, start_s: usize, end_s: usize) void {
    var i = start_s * sample_rate;
    const end = @min(buffer.len, end_s * sample_rate);
    while (i < end) : (i += 1) {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(sample_rate));
        buffer[i] = 0.2 * @sin(2.0 * std.math.pi * 180.0 * t);
    }
}

test "long transcription keeps short audio in one window" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    const samples = try allocator.alloc(f32, rate * 12);
    defer allocator.free(samples);
    @memset(samples, 0);
    tone(samples, rate, 0, 12);

    const windows = try planWindows(allocator, samples, rate, .{});
    defer allocator.free(windows);
    try std.testing.expectEqual(@as(usize, 1), windows.len);
    try std.testing.expectEqual(@as(usize, 0), windows[0].start);
    try std.testing.expectEqual(samples.len, windows[0].end);
}

test "long transcription splits at the pause nearest the window boundary" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    // 50 s: speech 0-26, pause 26-27, speech 27-50.
    const samples = try allocator.alloc(f32, rate * 50);
    defer allocator.free(samples);
    @memset(samples, 0);
    tone(samples, rate, 0, 26);
    tone(samples, rate, 27, 50);

    const windows = try planWindows(allocator, samples, rate, .{});
    defer allocator.free(windows);
    try std.testing.expectEqual(@as(usize, 2), windows.len);
    try std.testing.expect(windows[0].end >= 26 * rate and windows[0].end <= 27 * rate);
    try std.testing.expectEqual(windows[0].end, windows[1].start);
    try std.testing.expectEqual(samples.len, windows[1].end);
    for (windows) |window| try std.testing.expect(window.end - window.start <= 30 * rate);
}

test "long transcription falls back to hard cuts without pauses" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    const samples = try allocator.alloc(f32, rate * 95);
    defer allocator.free(samples);
    @memset(samples, 0);
    tone(samples, rate, 0, 95);

    const windows = try planWindows(allocator, samples, rate, .{});
    defer allocator.free(windows);
    try std.testing.expect(windows.len >= 4);
    var covered: usize = 0;
    for (windows) |window| {
        try std.testing.expectEqual(covered, window.start);
        try std.testing.expect(window.end - window.start <= 30 * rate);
        try std.testing.expect(window.end > window.start);
        covered = window.end;
    }
    try std.testing.expectEqual(samples.len, covered);
}

test "long transcription stitches timed segments and skips silent windows" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    // 70 s: speech 0-20, silence 20-45 (whole middle window silent), speech 45-70.
    const samples = try allocator.alloc(f32, rate * 70);
    defer allocator.free(samples);
    @memset(samples, 0);
    tone(samples, rate, 0, 20);
    tone(samples, rate, 45, 70);

    var fake = FakeTranscriber{ .allocator = allocator };
    var result = try transcribeLong(allocator, &fake, samples, rate, .{ .max_window_s = 25 });
    defer result.deinit();

    // Windows: [0, ~25) speech, [~25, ~45) silent, then the tail (one or
    // two windows depending on where the quietest split landed).
    try std.testing.expect(result.windows >= 3 and result.windows <= 4);
    try std.testing.expectEqual(result.windows - 1, fake.calls);
    try std.testing.expectEqual(fake.calls, result.segments.len);
    try std.testing.expectEqual(@as(u64, 0), result.segments[0].start_ms);
    try std.testing.expect(result.segments[1].start_ms >= 40_000);
    try std.testing.expectEqual(@as(u64, 70_000), result.segments[result.segments.len - 1].end_ms);
    try std.testing.expectEqualStrings("en", result.language.?);
    try std.testing.expectEqualStrings("en", fake.locked_language.?);
    try std.testing.expectEqual(@as(u64, 70_000), result.duration_ms);
    try std.testing.expect(std.mem.indexOf(u8, result.text, "hello1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.text, " hello2") != null);
    // Words are estimated inside each window-level segment.
    try std.testing.expect(result.segments[0].words.len >= 1);
    try std.testing.expect(std.mem.startsWith(u8, result.segments[0].words[0].word, "hello1("));
}

test "long transcription conditions each window on the prompt and the previous window" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    const samples = try allocator.alloc(f32, rate * 50);
    defer allocator.free(samples);
    @memset(samples, 0);
    tone(samples, rate, 0, 50);

    var fake = FakeTranscriber{ .allocator = allocator };
    var result = try transcribeLong(allocator, &fake, samples, rate, .{
        .max_window_s = 20,
        .initial_prompt = "Antfly Colony",
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    // First window: the two-word prompt only; later windows add the previous
    // window's three tokens.
    try std.testing.expectEqual(@as(usize, 2), fake.prefix_lens[0]);
    try std.testing.expectEqual(@as(usize, 5), fake.prefix_lens[1]);
    try std.testing.expectEqual(@as(usize, 5), fake.prefix_lens[2]);

    var plain = FakeTranscriber{ .allocator = allocator };
    var unconditioned = try transcribeLong(allocator, &plain, samples, rate, .{ .max_window_s = 20, .condition_on_previous = false });
    defer unconditioned.deinit();
    try std.testing.expectEqual(@as(usize, 0), plain.prefix_lens[1]);
}

test "long transcription maps timestamped phrases onto the clip timeline" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    const samples = try allocator.alloc(f32, rate * 45);
    defer allocator.free(samples);
    @memset(samples, 0);
    tone(samples, rate, 0, 45);

    var fake = FakeTranscriber{ .allocator = allocator, .timed = true };
    var result = try transcribeLong(allocator, &fake, samples, rate, .{ .max_window_s = 30 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqual(@as(usize, 4), result.segments.len);
    try std.testing.expectEqual(@as(u64, 0), result.segments[0].start_ms);
    try std.testing.expectEqual(@as(u64, 1000), result.segments[0].end_ms);
    try std.testing.expectEqualStrings("first phrase", result.segments[0].text);
    try std.testing.expectEqual(@as(usize, 2), result.segments[0].words.len);
    try std.testing.expectEqualStrings("first", result.segments[0].words[0].word);
    try std.testing.expect(result.segments[0].words[1].end_ms == 1000);
    // Second window's phrases are offset by the window start (>= 22.5 s).
    try std.testing.expect(result.segments[2].start_ms >= 22_500);
    try std.testing.expect(result.segments[3].end_ms <= 45_000);
    try std.testing.expectEqualStrings("first phrase second first phrase second", result.text);
}

test "long transcription drops empty window text" {
    const allocator = std.testing.allocator;
    const rate: u32 = 1000;
    const samples = try allocator.alloc(f32, rate * 5);
    defer allocator.free(samples);
    tone(samples, rate, 0, 5);

    var fake = FakeTranscriber{ .allocator = allocator, .text = "   ", .language = null };
    // The fake appends a counter, so force emptiness through trim of a spaces-only text.
    fake.text = "";
    var result = try transcribeLong(allocator, &fake, samples, rate, .{});
    defer result.deinit();
    // "1(5s)" is not empty, so one segment. Verify the joined text is the segment text.
    try std.testing.expectEqual(@as(usize, 1), result.segments.len);
    try std.testing.expectEqualStrings(result.segments[0].text, result.text);
    try std.testing.expect(result.language == null);
}
