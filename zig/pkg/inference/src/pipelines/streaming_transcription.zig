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

//! Incremental transcription over an append-only audio stream.
//!
//! A `Session` buffers 16 kHz mono PCM as the client appends chunks. Each
//! `process` pass runs VAD over the buffer and either:
//!
//! - finalizes every speech segment that is followed by an endpoint
//!   (`min_silence_ms` of silence) or exceeds `max_segment_ms`, emitting a
//!   `final` event and dropping that audio, or
//! - re-decodes the still-open segment and emits a `partial` event whose
//!   `stable_text` is the word prefix shared by the last two hypotheses
//!   (LocalAgreement-2), so clients can render text that will not change.
//!
//! The decoder is any `transcriber` with
//! `transcribePcm(samples, sample_rate) !transcription.TranscribeResult`, so
//! the state machine is testable without a model.

const std = @import("std");
const audio = @import("audio.zig");
const vad = @import("vad.zig");
const transcription = @import("transcription.zig");
const whisper_timestamps = @import("whisper_timestamps.zig");

pub const Config = struct {
    /// Internal buffer rate. Whisper consumes 16 kHz.
    sample_rate: u32 = audio.WHISPER_SAMPLE_RATE,
    vad: vad.Config = .{},
    /// Minimum new audio since the last partial decode before decoding the
    /// open segment again. Every partial is a full Whisper pass over a 30 s
    /// window (about 1.5 to 2 s for whisper-tiny on an M-series laptop), so
    /// the default is sized to keep pace with speech rather than to minimize
    /// display latency.
    partial_interval_ms: u32 = 2000,
    /// Force a segment boundary once uninterrupted speech reaches this
    /// length. Must stay below the model window.
    max_segment_ms: u32 = 25_000,
    /// Hard cap on buffered audio; appends beyond it fail.
    max_buffer_ms: u32 = 60_000,
    emit_partials: bool = true,
    /// Text the decoder is told preceded the audio: names and terms to prefer.
    initial_prompt: ?[]const u8 = null,
    /// Condition each decode on the text of the previous final segment.
    condition_on_previous: bool = true,
    /// Encoder window policy for every decode this session runs. Partials
    /// re-decode short open segments many times, so trimming the encoder to
    /// the audio present is the default here.
    audio_context: transcription.AudioContext = .dynamic,

    pub fn validate(self: Config) !void {
        try self.vad.validate();
        if (self.sample_rate == 0) return error.InvalidStreamingConfig;
        if (self.max_segment_ms == 0 or self.max_segment_ms > audio.WHISPER_CHUNK_LENGTH * 1000) return error.InvalidStreamingConfig;
        if (self.max_buffer_ms < self.max_segment_ms) return error.InvalidStreamingConfig;
        if (self.partial_interval_ms == 0) return error.InvalidStreamingConfig;
    }
};

pub const EventKind = enum { partial, final };

pub const Word = struct {
    word: []u8,
    start_ms: u64,
    end_ms: u64,
};

pub const Event = struct {
    kind: EventKind,
    sequence: u64,
    /// Current best hypothesis for the segment.
    text: []u8,
    /// Prefix of `text` that agreed with the previous hypothesis. Equals
    /// `text` for `final` events.
    stable_text: []u8,
    start_ms: u64,
    end_ms: u64,
    language: ?[]u8,
    /// Word spans on the session timeline. Empty for partials.
    words: []Word = &.{},

    pub fn deinit(self: *Event, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        allocator.free(self.stable_text);
        if (self.language) |language| allocator.free(language);
        for (self.words) |word| allocator.free(word.word);
        allocator.free(self.words);
    }
};

pub const Stats = struct {
    buffered_ms: u64,
    total_ms: u64,
    decodes: u64,
    finals: u64,
    partials: u64,
};

pub const Session = struct {
    allocator: std.mem.Allocator,
    config: Config,
    buffer: std.ArrayListUnmanaged(f32) = .empty,
    /// Absolute sample index of `buffer.items[0]` in the whole stream.
    buffer_start: u64 = 0,
    total_samples: u64 = 0,
    /// Buffer length at the last partial decode of the current open segment.
    analyzed_len: usize = 0,
    previous_hypothesis: ?[]u8 = null,
    /// Tokens of `config.initial_prompt`, encoded on first use.
    initial_prompt_tokens: ?[]i32 = null,
    /// Text tokens of the last final segment, for conditioning.
    previous_final_tokens: ?[]i32 = null,
    /// Scratch for the combined conditioning prefix.
    prefix: std.ArrayListUnmanaged(i32) = .empty,
    next_sequence: u64 = 0,
    decodes: u64 = 0,
    finals: u64 = 0,
    partials: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Session {
        try config.validate();
        return .{ .allocator = allocator, .config = config };
    }

    pub fn deinit(self: *Session) void {
        self.buffer.deinit(self.allocator);
        self.clearHypothesis();
        if (self.initial_prompt_tokens) |tokens| self.allocator.free(tokens);
        if (self.previous_final_tokens) |tokens| self.allocator.free(tokens);
        self.prefix.deinit(self.allocator);
    }

    /// Conditioning prefix for the next decode: the initial prompt followed
    /// by the previous final's text tokens.
    fn conditioningPrefix(self: *Session, transcriber: anytype) ![]const i32 {
        if (self.initial_prompt_tokens == null) {
            self.initial_prompt_tokens = if (self.config.initial_prompt) |prompt|
                try transcriber.encodePromptText(self.allocator, prompt)
            else
                try self.allocator.alloc(i32, 0);
        }
        self.prefix.clearRetainingCapacity();
        try self.prefix.appendSlice(self.allocator, self.initial_prompt_tokens.?);
        if (self.config.condition_on_previous) if (self.previous_final_tokens) |tokens| {
            try self.prefix.appendSlice(self.allocator, tokens);
        };
        return self.prefix.items;
    }

    pub fn stats(self: *const Session) Stats {
        return .{
            .buffered_ms = vad.samplesToMs(self.config.sample_rate, self.buffer.items.len),
            .total_ms = vad.samplesToMs(self.config.sample_rate, self.total_samples),
            .decodes = self.decodes,
            .finals = self.finals,
            .partials = self.partials,
        };
    }

    /// Append mono PCM at any rate; it is resampled to the session rate.
    pub fn append(self: *Session, samples: []const f32, sample_rate: u32) !void {
        if (samples.len == 0) return;
        if (sample_rate == 0) return error.UnsupportedAudioFormat;
        const prepared = try audio.copyOrResample(self.allocator, samples, sample_rate, self.config.sample_rate);
        defer self.allocator.free(prepared);
        const max_samples = vad.msToSamples(self.config.sample_rate, self.config.max_buffer_ms);
        if (self.buffer.items.len + prepared.len > max_samples) return error.SessionBufferFull;
        try self.buffer.appendSlice(self.allocator, prepared);
        self.total_samples += prepared.len;
    }

    /// Run endpointing and decoding over the buffered audio. Appends every
    /// produced event to `events`; the caller owns them. With `commit`, all
    /// buffered speech is finalized regardless of trailing silence and the
    /// buffer is emptied.
    pub fn process(
        self: *Session,
        transcriber: anytype,
        events: *std.ArrayListUnmanaged(Event),
        commit: bool,
    ) !void {
        const rate = self.config.sample_rate;
        const min_silence = vad.msToSamples(rate, self.config.vad.min_silence_ms);
        const max_segment = vad.msToSamples(rate, self.config.max_segment_ms);
        const partial_interval = vad.msToSamples(rate, self.config.partial_interval_ms);

        while (true) {
            const segments = try vad.detectSegments(self.allocator, self.buffer.items, rate, self.config.vad);
            defer self.allocator.free(segments);

            if (segments.len == 0) {
                // No speech. Keep one silence window so an onset that straddles
                // the next append is still detected, then stop.
                if (commit) {
                    self.drop(self.buffer.items.len);
                } else if (self.buffer.items.len > min_silence) {
                    self.drop(self.buffer.items.len - min_silence);
                }
                self.clearHypothesis();
                return;
            }

            const first = segments[0];
            const closed = segments.len > 1 or (self.buffer.items.len - first.end >= min_silence);
            const settled = closed or commit;

            // Speech longer than one decode window is cut at the quietest
            // point in the window's last quarter, whether it is still open
            // or already settled (a long utterance followed by silence, or
            // a commit): a decode covers at most max_segment, so a longer
            // span would lose its tail.
            const span_end = if (settled) first.end else self.buffer.items.len;
            const span_len = span_end - first.start;
            if (if (settled) span_len > max_segment else span_len >= max_segment) {
                const search_lo = first.start + (max_segment / 4) * 3;
                var split = vad.quietestSplit(self.buffer.items, rate, search_lo, first.start + max_segment, self.config.vad);
                if (split <= first.start) split = first.start + max_segment;
                try self.finalize(transcriber, events, first.start, split);
                self.drop(split);
                continue;
            }

            if (settled) {
                try self.finalize(transcriber, events, first.start, first.end);
                self.drop(first.end);
                continue;
            }

            if (first.start > 0) {
                // Leading silence carries no information; dropping it keeps
                // partial decodes cheap. analyzed_len tracks buffer length,
                // so shift it too.
                const removed = first.start;
                self.drop(removed);
                self.analyzed_len = if (self.analyzed_len > removed) self.analyzed_len - removed else 0;
            }
            if (self.config.emit_partials and self.buffer.items.len - self.analyzed_len >= partial_interval) {
                try self.partial(transcriber, events);
            }
            return;
        }
    }

    fn finalize(
        self: *Session,
        transcriber: anytype,
        events: *std.ArrayListUnmanaged(Event),
        start: usize,
        end: usize,
    ) !void {
        defer self.clearHypothesis();
        if (end <= start) return;
        const prefix = try self.conditioningPrefix(transcriber);
        var result: transcription.TranscribeResult = try transcriber.transcribePcmConditioned(self.buffer.items[start..end], self.config.sample_rate, prefix);
        defer result.deinit();
        self.decodes += 1;
        const trimmed = std.mem.trim(u8, result.text, " \t\r\n");
        if (trimmed.len == 0) return;
        if (self.config.condition_on_previous) {
            if (self.previous_final_tokens) |tokens| self.allocator.free(tokens);
            self.previous_final_tokens = try self.allocator.dupe(i32, result.tokens);
        }
        if (result.language) |code| _ = transcriber.lockLanguage(code);
        const segment_start_ms = vad.samplesToMs(self.config.sample_rate, self.buffer_start + start);
        const segment_end_ms = vad.samplesToMs(self.config.sample_rate, self.buffer_start + end);
        const text = try self.allocator.dupe(u8, trimmed);
        errdefer self.allocator.free(text);
        const stable = try self.allocator.dupe(u8, trimmed);
        errdefer self.allocator.free(stable);
        const language = if (result.language) |l| try self.allocator.dupe(u8, l) else null;
        errdefer if (language) |l| self.allocator.free(l);
        const words = try self.wordsForFinal(&result, trimmed, segment_start_ms, segment_end_ms);
        errdefer {
            for (words) |word| self.allocator.free(word.word);
            self.allocator.free(words);
        }
        try events.append(self.allocator, .{
            .kind = .final,
            .sequence = self.nextSequence(),
            .text = text,
            .stable_text = stable,
            .start_ms = segment_start_ms,
            .end_ms = segment_end_ms,
            .language = language,
            .words = words,
        });
        self.finals += 1;
    }

    /// Word spans for a final: per timestamped phrase when the decoder
    /// produced them, else spread across the whole segment.
    fn wordsForFinal(
        self: *Session,
        result: *const transcription.TranscribeResult,
        text: []const u8,
        segment_start_ms: u64,
        segment_end_ms: u64,
    ) ![]Word {
        var out = std.ArrayListUnmanaged(Word).empty;
        errdefer {
            for (out.items) |word| self.allocator.free(word.word);
            out.deinit(self.allocator);
        }
        if (result.segments.len > 0) {
            for (result.segments) |phrase| {
                const start = @min(segment_end_ms, segment_start_ms + phrase.start_ms);
                const end = @min(segment_end_ms, @max(start, segment_start_ms + phrase.end_ms));
                try self.appendWords(&out, phrase.text, start, end);
            }
        } else {
            try self.appendWords(&out, text, segment_start_ms, segment_end_ms);
        }
        return out.toOwnedSlice(self.allocator);
    }

    fn appendWords(self: *Session, out: *std.ArrayListUnmanaged(Word), text: []const u8, start_ms: u64, end_ms: u64) !void {
        const spans = try whisper_timestamps.splitWords(self.allocator, text, start_ms, end_ms);
        defer self.allocator.free(spans);
        for (spans) |span| {
            const owned = try self.allocator.dupe(u8, span.word);
            errdefer self.allocator.free(owned);
            try out.append(self.allocator, .{ .word = owned, .start_ms = span.start_ms, .end_ms = span.end_ms });
        }
    }

    fn partial(self: *Session, transcriber: anytype, events: *std.ArrayListUnmanaged(Event)) !void {
        const prefix = try self.conditioningPrefix(transcriber);
        var result: transcription.TranscribeResult = try transcriber.transcribePcmConditioned(self.buffer.items, self.config.sample_rate, prefix);
        defer result.deinit();
        self.decodes += 1;
        self.analyzed_len = self.buffer.items.len;
        const trimmed = std.mem.trim(u8, result.text, " \t\r\n");
        const hypothesis = try self.allocator.dupe(u8, trimmed);
        errdefer self.allocator.free(hypothesis);
        const stable = try commonWordPrefix(self.allocator, self.previous_hypothesis orelse "", hypothesis);
        errdefer self.allocator.free(stable);
        const language = if (result.language) |l| try self.allocator.dupe(u8, l) else null;
        errdefer if (language) |l| self.allocator.free(l);
        try events.append(self.allocator, .{
            .kind = .partial,
            .sequence = self.nextSequence(),
            .text = hypothesis,
            .stable_text = stable,
            .start_ms = vad.samplesToMs(self.config.sample_rate, self.buffer_start),
            .end_ms = vad.samplesToMs(self.config.sample_rate, self.buffer_start + self.buffer.items.len),
            .language = language,
        });
        self.partials += 1;
        self.clearHypothesis();
        self.previous_hypothesis = try self.allocator.dupe(u8, hypothesis);
    }

    fn drop(self: *Session, count: usize) void {
        const n = @min(count, self.buffer.items.len);
        if (n == 0) return;
        const remaining = self.buffer.items.len - n;
        std.mem.copyForwards(f32, self.buffer.items[0..remaining], self.buffer.items[n..]);
        self.buffer.items.len = remaining;
        self.buffer_start += n;
        self.analyzed_len = 0;
    }

    fn clearHypothesis(self: *Session) void {
        if (self.previous_hypothesis) |h| self.allocator.free(h);
        self.previous_hypothesis = null;
    }

    fn nextSequence(self: *Session) u64 {
        const value = self.next_sequence;
        self.next_sequence += 1;
        return value;
    }
};

/// Longest run of leading whitespace-separated words shared by `a` and `b`,
/// joined by single spaces.
pub fn commonWordPrefix(allocator: std.mem.Allocator, a: []const u8, b: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var words_a = std.mem.tokenizeAny(u8, a, " \t\r\n");
    var words_b = std.mem.tokenizeAny(u8, b, " \t\r\n");
    var first = true;
    while (true) {
        const wa = words_a.next() orelse break;
        const wb = words_b.next() orelse break;
        if (!std.mem.eql(u8, wa, wb)) break;
        if (!first) try out.append(allocator, ' ');
        try out.appendSlice(allocator, wa);
        first = false;
    }
    return out.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const test_rate: u32 = 16_000;

const FakeTranscriber = struct {
    allocator: std.mem.Allocator,
    calls: usize = 0,
    /// Text returned per call, cycled. Empty list returns a length-derived text.
    scripted: []const []const u8 = &.{},

    prefix_lens: [16]usize = [_]usize{0} ** 16,

    pub fn encodePromptText(_: *FakeTranscriber, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        var count: usize = 0;
        var it = std.mem.tokenizeScalar(u8, text, ' ');
        while (it.next()) |_| count += 1;
        return allocator.alloc(i32, count);
    }

    pub fn lockLanguage(_: *FakeTranscriber, _: []const u8) bool {
        return true;
    }

    pub fn transcribePcmConditioned(self: *FakeTranscriber, samples: []const f32, sample_rate: u32, prefix: []const i32) !transcription.TranscribeResult {
        if (self.calls < self.prefix_lens.len) self.prefix_lens[self.calls] = prefix.len;
        defer self.calls += 1;
        const text = if (self.scripted.len > 0)
            try self.allocator.dupe(u8, self.scripted[self.calls % self.scripted.len])
        else
            try std.fmt.allocPrint(self.allocator, "seg{d} {d}ms", .{ self.calls, vad.samplesToMs(sample_rate, samples.len) });
        errdefer self.allocator.free(text);
        // Two tokens per call stand in for the decoded text tokens.
        const tokens = try self.allocator.alloc(i32, 2);
        @memset(tokens, @intCast(self.calls));
        return .{ .text = text, .language = try self.allocator.dupe(u8, "en"), .allocator = self.allocator, .tokens = tokens };
    }
};

fn toneChunk(allocator: std.mem.Allocator, ms: u32, amplitude: f32) ![]f32 {
    const samples = try allocator.alloc(f32, vad.msToSamples(test_rate, ms));
    for (samples, 0..) |*s, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(test_rate));
        s.* = amplitude * @sin(2.0 * std.math.pi * 200.0 * t);
    }
    return samples;
}

fn freeEvents(allocator: std.mem.Allocator, events: *std.ArrayListUnmanaged(Event)) void {
    for (events.items) |*event| event.deinit(allocator);
    events.deinit(allocator);
}

test "streaming session emits partials then a final at the endpoint" {
    const allocator = std.testing.allocator;
    var session = try Session.init(allocator, .{ .partial_interval_ms = 1000 });
    defer session.deinit();
    const scripted = [_][]const u8{ "the quick", "the quick brown", "the quick brown fox" };
    var fake = FakeTranscriber{ .allocator = allocator, .scripted = &scripted };
    var events = std.ArrayListUnmanaged(Event).empty;
    defer freeEvents(allocator, &events);

    const speech = try toneChunk(allocator, 1000, 0.2);
    defer allocator.free(speech);
    const silence = try toneChunk(allocator, 1000, 0.0);
    defer allocator.free(silence);

    // 1 s of speech: one partial with nothing stable yet.
    try session.append(speech, test_rate);
    try session.process(&fake, &events, false);
    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    try std.testing.expectEqual(EventKind.partial, events.items[0].kind);
    try std.testing.expectEqualStrings("the quick", events.items[0].text);
    try std.testing.expectEqualStrings("", events.items[0].stable_text);

    // Another second: the prefix agrees.
    try session.append(speech, test_rate);
    try session.process(&fake, &events, false);
    try std.testing.expectEqual(@as(usize, 2), events.items.len);
    try std.testing.expectEqualStrings("the quick brown", events.items[1].text);
    try std.testing.expectEqualStrings("the quick", events.items[1].stable_text);

    // Silence closes the segment.
    try session.append(silence, test_rate);
    try session.process(&fake, &events, false);
    try std.testing.expectEqual(@as(usize, 3), events.items.len);
    const final = events.items[2];
    try std.testing.expectEqual(EventKind.final, final.kind);
    try std.testing.expectEqualStrings("the quick brown fox", final.text);
    try std.testing.expectEqualStrings(final.text, final.stable_text);
    try std.testing.expectEqual(@as(u64, 0), final.start_ms);
    try std.testing.expect(final.end_ms >= 2000 and final.end_ms <= 2200);
    try std.testing.expectEqualStrings("en", final.language.?);
    try std.testing.expectEqual(@as(u64, 2), final.sequence);
    try std.testing.expectEqual(@as(usize, 4), final.words.len);
    try std.testing.expectEqualStrings("the", final.words[0].word);
    try std.testing.expectEqual(final.start_ms, final.words[0].start_ms);
    try std.testing.expectEqual(final.end_ms, final.words[3].end_ms);
    try std.testing.expectEqual(@as(usize, 0), events.items[0].words.len);

    const s = session.stats();
    try std.testing.expectEqual(@as(u64, 1), s.finals);
    try std.testing.expectEqual(@as(u64, 2), s.partials);
    try std.testing.expectEqual(@as(u64, 3000), s.total_ms);
    try std.testing.expect(s.buffered_ms <= 1000);
}

test "streaming session ignores silence and commit flushes open speech" {
    const allocator = std.testing.allocator;
    var session = try Session.init(allocator, .{ .emit_partials = false });
    defer session.deinit();
    var fake = FakeTranscriber{ .allocator = allocator };
    var events = std.ArrayListUnmanaged(Event).empty;
    defer freeEvents(allocator, &events);

    const silence = try toneChunk(allocator, 2000, 0.0);
    defer allocator.free(silence);
    try session.append(silence, test_rate);
    try session.process(&fake, &events, false);
    try std.testing.expectEqual(@as(usize, 0), events.items.len);
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    // Only one silence window is retained.
    try std.testing.expect(session.stats().buffered_ms <= 600);

    const speech = try toneChunk(allocator, 1500, 0.2);
    defer allocator.free(speech);
    try session.append(speech, test_rate);
    try session.process(&fake, &events, false);
    try std.testing.expectEqual(@as(usize, 0), events.items.len);

    try session.process(&fake, &events, true);
    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    try std.testing.expectEqual(EventKind.final, events.items[0].kind);
    try std.testing.expect(events.items[0].start_ms >= 1300 and events.items[0].start_ms <= 2000);
    try std.testing.expectEqual(@as(u64, 0), session.stats().buffered_ms);
}

test "streaming session force-splits speech longer than max_segment" {
    const allocator = std.testing.allocator;
    var session = try Session.init(allocator, .{ .max_segment_ms = 3000, .emit_partials = false });
    defer session.deinit();
    var fake = FakeTranscriber{ .allocator = allocator };
    var events = std.ArrayListUnmanaged(Event).empty;
    defer freeEvents(allocator, &events);

    const speech = try toneChunk(allocator, 7000, 0.2);
    defer allocator.free(speech);
    try session.append(speech, test_rate);
    try session.process(&fake, &events, false);
    try std.testing.expectEqual(@as(usize, 2), events.items.len);
    for (events.items) |event| {
        try std.testing.expectEqual(EventKind.final, event.kind);
        try std.testing.expect(event.end_ms - event.start_ms <= 3000);
    }
    try std.testing.expectEqual(events.items[0].end_ms, events.items[1].start_ms);
    try std.testing.expect(session.stats().buffered_ms >= 900);
}

test "streaming session splits settled speech longer than max_segment" {
    const allocator = std.testing.allocator;
    const speech = try toneChunk(allocator, 7000, 0.2);
    defer allocator.free(speech);
    const silence = try toneChunk(allocator, 1000, 0.0);
    defer allocator.free(silence);

    // Closed by trailing silence: the whole utterance arrives before the
    // session runs, so it is one settled segment of 7 s.
    {
        var session = try Session.init(allocator, .{ .max_segment_ms = 3000, .emit_partials = false });
        defer session.deinit();
        var fake = FakeTranscriber{ .allocator = allocator };
        var events = std.ArrayListUnmanaged(Event).empty;
        defer freeEvents(allocator, &events);
        try session.append(speech, test_rate);
        try session.append(silence, test_rate);
        try session.process(&fake, &events, false);
        try std.testing.expectEqual(@as(usize, 3), events.items.len);
        for (events.items) |event| {
            try std.testing.expectEqual(EventKind.final, event.kind);
            try std.testing.expect(event.end_ms - event.start_ms <= 3000);
        }
        try std.testing.expectEqual(events.items[0].end_ms, events.items[1].start_ms);
        try std.testing.expectEqual(events.items[1].end_ms, events.items[2].start_ms);
        try std.testing.expect(events.items[2].end_ms >= 6900);
    }

    // Committed while still open.
    {
        var session = try Session.init(allocator, .{ .max_segment_ms = 3000, .emit_partials = false });
        defer session.deinit();
        var fake = FakeTranscriber{ .allocator = allocator };
        var events = std.ArrayListUnmanaged(Event).empty;
        defer freeEvents(allocator, &events);
        try session.append(speech, test_rate);
        try session.process(&fake, &events, true);
        try std.testing.expectEqual(@as(usize, 3), events.items.len);
        for (events.items) |event| try std.testing.expect(event.end_ms - event.start_ms <= 3000);
        try std.testing.expect(events.items[2].end_ms >= 6900);
        try std.testing.expectEqual(@as(u64, 0), session.stats().buffered_ms);
    }
}

test "streaming session resamples appends and enforces the buffer cap" {
    const allocator = std.testing.allocator;
    var session = try Session.init(allocator, .{ .max_buffer_ms = 30_000, .max_segment_ms = 25_000 });
    defer session.deinit();
    const chunk = try allocator.alloc(f32, 8000);
    defer allocator.free(chunk);
    @memset(chunk, 0);
    try session.append(chunk, 8000); // 1 s at 8 kHz becomes 1 s at 16 kHz
    try std.testing.expectEqual(@as(u64, 1000), session.stats().total_ms);

    const big = try allocator.alloc(f32, vad.msToSamples(test_rate, 30_000));
    defer allocator.free(big);
    @memset(big, 0);
    try std.testing.expectError(error.SessionBufferFull, session.append(big, test_rate));
    try std.testing.expectError(error.UnsupportedAudioFormat, session.append(chunk, 0));
}

test "streaming config validation" {
    try std.testing.expectError(error.InvalidStreamingConfig, Session.init(std.testing.allocator, .{ .max_segment_ms = 40_000 }));
    try std.testing.expectError(error.InvalidStreamingConfig, Session.init(std.testing.allocator, .{ .max_buffer_ms = 1000 }));
    try std.testing.expectError(error.InvalidStreamingConfig, Session.init(std.testing.allocator, .{ .partial_interval_ms = 0 }));
}

test "common word prefix" {
    const allocator = std.testing.allocator;
    const cases = [_]struct { a: []const u8, b: []const u8, want: []const u8 }{
        .{ .a = "the quick brown", .b = "the quick brown fox", .want = "the quick brown" },
        .{ .a = "the quick", .b = "a quick", .want = "" },
        .{ .a = "", .b = "hello", .want = "" },
        .{ .a = "  hello   world ", .b = "hello world!", .want = "hello" },
    };
    for (cases) |case| {
        const got = try commonWordPrefix(allocator, case.a, case.b);
        defer allocator.free(got);
        try std.testing.expectEqualStrings(case.want, got);
    }
}

test "streaming session conditions decodes on the prompt and the previous final" {
    const allocator = std.testing.allocator;
    var session = try Session.init(allocator, .{ .emit_partials = false, .initial_prompt = "Antfly Colony Roetker" });
    defer session.deinit();
    var fake = FakeTranscriber{ .allocator = allocator };
    var events = std.ArrayListUnmanaged(Event).empty;
    defer freeEvents(allocator, &events);

    const speech = try toneChunk(allocator, 1500, 0.2);
    defer allocator.free(speech);
    try session.append(speech, test_rate);
    try session.process(&fake, &events, true);
    try session.append(speech, test_rate);
    try session.process(&fake, &events, true);
    try std.testing.expectEqual(@as(usize, 2), events.items.len);
    // First final: prompt only (3 tokens); second: prompt + previous final (2 tokens).
    try std.testing.expectEqual(@as(usize, 3), fake.prefix_lens[0]);
    try std.testing.expectEqual(@as(usize, 5), fake.prefix_lens[1]);
}
