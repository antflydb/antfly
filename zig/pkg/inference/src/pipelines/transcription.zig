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

// Speech2Seq audio transcription pipeline (Whisper via ONNX or native).
//
// Architecture: audio preprocessing → encoder → text decoder.
// Converts audio to text using Whisper-style models.
//
// Required model files:
//   - encoder_model.onnx (or via native architectures)
//   - decoder_model.onnx
//   - tokenizer.json
//   - config.json (model_type: whisper)
//   - preprocessor_config.json (mel spectrogram params)

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const session_factory = @import("../architectures/session_factory.zig");
const tokenizer_mod = @import("inference_tokenizer");
const audio = @import("audio.zig");
const whisper_prompt = @import("whisper_prompt.zig");
const whisper_timestamps = @import("whisper_timestamps.zig");
const ops = @import("../ops/ops.zig");
const whisper_arch = @import("../architectures/whisper.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const TranscribeConfig = struct {
    vocab_size: usize = 51865,
    max_length: usize = 448,
    language: ?[]const u8 = null,
    sample_rate: usize = 16000,
    n_mels: usize = 80,
    chunk_length_s: usize = 30,
    /// Peak live allocation budget for encoded-audio decode. The returned mono
    /// PCM is included in this budget.
    max_decode_working_bytes: usize = audio.default_decode_working_bytes,
    decoder_start_token_id: i32 = 50258,
    eos_token_id: i32 = 50257,
    /// Decoder prompt entries from generation_config.json. A null token leaves
    /// that position model-generated (Whisper uses this for language detection).
    forced_decoder_ids: ?[]const whisper_prompt.ForcedDecoderId = null,
    /// Immutable language-token vocabulary prepared with the loaded model.
    language_tokens: []const whisper_prompt.LanguageToken = &.{},
    /// Decoder generation settings (suppression lists, timestamp ids).
    decode: whisper_prompt.DecodeSettings = .{},
    /// `<|notimestamps|>`, suppressed while decoding with timestamps.
    no_timestamps_id: i32 = 50363,
    /// Emit and parse `<|t|>` timestamp tokens into `TranscribeResult.segments`.
    /// The forced decoder prompt must then omit `<|notimestamps|>`; see
    /// `PromptCache.resolveWithTimestamps`.
    timestamps: bool = false,
    /// Encoder input length. `full` is the reference 30 s window; `dynamic`
    /// encodes the audio plus one second of padding (whole seconds), which
    /// is cheaper on short segments at a small accuracy cost. Native
    /// sessions only; ONNX encoders keep the fixed window.
    audio_context: AudioContext = .full,
    /// Reference silence and hallucination guards.
    no_speech_threshold: f32 = 0.6,
    logprob_threshold: f32 = -1.0,
    compression_ratio_threshold: f32 = 2.4,
    /// Retry at rising sampling temperature when the greedy pass fails the
    /// guards (repetitive or unconfident output).
    temperature_fallback: bool = true,
    /// Force this language token in the dynamic slot instead of detecting.
    language_lock_token: ?i32 = null,
};

pub const AudioContext = enum { full, dynamic };

/// Phrase timed by Whisper timestamp tokens, relative to the window start.
pub const TimedSegment = struct {
    text: []const u8,
    start_ms: u64,
    end_ms: u64,
};

/// Wall-clock breakdown of one window, for the timing log and benchmarks.
pub const Timing = struct {
    mel_ns: u64 = 0,
    encoder_ns: u64 = 0,
    /// Decoder prompt pass (all forced tokens at once).
    prefill_ns: u64 = 0,
    /// Generated tokens after the prompt.
    decode_ns: u64 = 0,
    decode_steps: usize = 0,
    /// True when the native KV-cached decoder ran (false for the ONNX
    /// merged-cache and full-prefix fallbacks).
    kv_cached: bool = false,

    pub fn add(self: *Timing, other: Timing) void {
        self.mel_ns += other.mel_ns;
        self.encoder_ns += other.encoder_ns;
        self.prefill_ns += other.prefill_ns;
        self.decode_ns += other.decode_ns;
        self.decode_steps += other.decode_steps;
        self.kv_cached = self.kv_cached or other.kv_cached;
    }
};

pub const TranscribeResult = struct {
    text: []const u8,
    language: ?[]const u8,
    allocator: std.mem.Allocator,
    timing: Timing = .{},
    /// Empty unless the pipeline decoded with `timestamps`.
    segments: []const TimedSegment = &.{},
    /// Generated text tokens (timestamps stripped), usable as the
    /// conditioning prefix of the next window.
    tokens: []const i32 = &.{},
    /// Probability of `<|nospeech|>` at the first free decode position.
    no_speech_prob: f32 = 0,
    /// Mean log-probability of the generated tokens.
    avg_logprob: f32 = 0,
    /// zlib compression ratio of the text; high values mean repetition.
    compression_ratio: f32 = 0,
    /// Sampling temperature of the accepted attempt (0 = greedy).
    temperature: f32 = 0,
    /// True when the window was judged silence; `text` is empty then.
    silent: bool = false,

    pub fn deinit(self: *TranscribeResult) void {
        self.allocator.free(self.text);
        if (self.language) |l| self.allocator.free(l);
        for (self.segments) |segment| self.allocator.free(segment.text);
        self.allocator.free(self.segments);
        self.allocator.free(self.tokens);
    }
};

/// Longest conditioning prefix Whisper accepts: half the decoder context
/// minus the `<|startofprev|>` token.
pub fn maxPromptPrefixTokens(max_length: usize) usize {
    return (max_length / 2) -| 1;
}

pub const TranscriptionPipeline = struct {
    batch_dispatch: ?@import("../server/tensor_microbatch.zig").Dispatch = null,
    allocator: std.mem.Allocator,
    encoder: backends.Session,
    decoder: backends.Session,
    tokenizer: tokenizer_mod.Tokenizer,
    config: TranscribeConfig,
    execution_control: ?InferenceExecutionControl = null,

    pub fn init(
        allocator: std.mem.Allocator,
        encoder: backends.Session,
        decoder: backends.Session,
        tokenizer: tokenizer_mod.Tokenizer,
        config: TranscribeConfig,
    ) TranscriptionPipeline {
        return .{
            .allocator = allocator,
            .encoder = encoder,
            .decoder = decoder,
            .tokenizer = tokenizer,
            .config = config,
        };
    }

    /// Transcribe audio from supported encoded audio bytes.
    pub fn transcribe(self: *TranscriptionPipeline, audio_data: []const u8) !TranscribeResult {
        return self.transcribeWithOptions(audio_data, .{});
    }

    /// Transcribe audio from supported encoded audio bytes, allowing format or
    /// MIME hints when container sniffing is ambiguous.
    pub fn transcribeWithOptions(
        self: *TranscriptionPipeline,
        audio_data: []const u8,
        decode_options: audio.DecodeOptions,
    ) !TranscribeResult {
        var decoded = try audio.decodeBounded(
            self.allocator,
            audio_data,
            decode_options,
            self.config.max_decode_working_bytes,
        );
        defer decoded.deinit();

        return self.transcribePcm(decoded.samples, decoded.sample_rate);
    }

    /// Transcribe interleaved PCM audio samples at the given sample rate.
    /// Multi-channel input is explicitly downmixed at the pipeline boundary.
    pub fn transcribeInterleavedPcm(
        self: *TranscriptionPipeline,
        samples: []const f32,
        sample_rate: u32,
        channels: u8,
    ) !TranscribeResult {
        const mono = try audio.downmixToMono(self.allocator, samples, channels);
        defer self.allocator.free(mono);

        return self.transcribePcm(mono, sample_rate);
    }

    /// Tokenize free text into a conditioning prefix (`transcribePcmConditioned`).
    /// The result carries plain text tokens only; the pipeline adds the
    /// `<|startofprev|>` marker itself. Caller owns the slice.
    pub fn encodePromptText(self: *TranscriptionPipeline, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        const trimmed = std.mem.trim(u8, text, " \t\r\n");
        if (trimmed.len == 0) return allocator.alloc(i32, 0);
        // Whisper prompts are tokenized with a leading space, like transcript text.
        const spaced = try std.fmt.allocPrint(allocator, " {s}", .{trimmed});
        defer allocator.free(spaced);
        const ids = try self.tokenizer.encode(allocator, spaced);
        errdefer allocator.free(ids);
        // Drop anything the tokenizer treated as a special token; the prompt
        // must not be able to inject control tokens.
        var kept: usize = 0;
        for (ids) |id| {
            if (id < 0 or id >= self.config.eos_token_id) continue;
            ids[kept] = id;
            kept += 1;
        }
        return allocator.realloc(ids, kept);
    }

    /// Transcribe PCM audio samples at the given sample rate.
    pub fn transcribePcm(self: *TranscriptionPipeline, samples: []const f32, sample_rate: u32) !TranscribeResult {
        return self.transcribePcmConditioned(samples, sample_rate, &.{});
    }

    /// Transcribe with `prompt_prefix` (from `encodePromptText`) supplied as
    /// the decoder's previous-text context. Whisper uses it for continuity
    /// across windows and to prefer the spellings it contains. Ignored when
    /// the model has no `<|startofprev|>` token.
    pub fn transcribePcmConditioned(
        self: *TranscriptionPipeline,
        samples: []const f32,
        sample_rate: u32,
        prompt_prefix: []const i32,
    ) !TranscribeResult {
        if (self.execution_control) |control| try control.update(.tokenizing, 0, 1);
        const allocator = self.allocator;
        var timing = Timing{};

        // Encoder input length: the reference 30 s window, or the dynamic
        // context (audio plus one second) on native sessions.
        const native_encoder = session_factory.getWhisperConfig(self.encoder) != null;
        const context_seconds: u32 = if (self.config.audio_context == .dynamic and native_encoder)
            audio.dynamicContextSeconds(samples.len, sample_rate)
        else
            audio.WHISPER_CHUNK_LENGTH;
        const n_frames = audio.whisperFramesForSeconds(context_seconds);
        const n_mels = self.config.n_mels;
        if (n_mels == 0 or n_mels > std.math.maxInt(u32)) return error.InvalidInputShape;
        const mel_elements = std.math.mul(usize, n_mels, n_frames) catch return error.ResourceLimitExceeded;
        const mel_bytes = std.math.mul(usize, mel_elements, @sizeOf(f32)) catch return error.ResourceLimitExceeded;
        const pcm_bytes = std.math.mul(usize, samples.len, @sizeOf(f32)) catch return error.ResourceLimitExceeded;
        var encoder_permit = try self.encoder.admit(.{
            .batch = 1,
            .sequence = n_frames,
            .input_bytes = mel_bytes,
            .host_preprocess_bytes = std.math.add(usize, pcm_bytes, mel_bytes) catch return error.ResourceLimitExceeded,
        });
        defer encoder_permit.deinit();

        const mel_started = platform.time.monotonicNs();
        const mel = try audio.whisperMelFromPcmSecondsMels(allocator, samples, sample_rate, context_seconds, @intCast(n_mels));
        defer allocator.free(mel);
        timing.mel_ns = platform.time.monotonicNs() -| mel_started;

        // 1. Run encoder on [1, n_mels, n_frames] log-mel input.
        const encoder_started = platform.time.monotonicNs();
        const mel_shape = [_]i64{ 1, @intCast(n_mels), @intCast(n_frames) };
        var mel_tensor = try backends.Tensor.initFloat32(allocator, "input_features", &mel_shape, mel);
        defer mel_tensor.deinit();
        const encoder_outputs = if (self.batch_dispatch) |dispatch|
            try dispatch.run(allocator, self.encoder, &encoder_permit, null, &.{mel_tensor}, self.execution_control)
        else
            try encoder_permit.runWithControl(&.{mel_tensor}, allocator, self.execution_control);
        defer {
            for (encoder_outputs) |*t| {
                var mt = t.*;
                mt.deinit();
            }
            allocator.free(encoder_outputs);
        }
        if (encoder_outputs.len == 0) return error.NoEncoderOutput;
        timing.encoder_ns = platform.time.monotonicNs() -| encoder_started;
        const enc_seq_len: usize = if (encoder_outputs[0].shape.len >= 2) @intCast(encoder_outputs[0].shape[1]) else 1;
        const enc_mask = try allocator.alloc(i64, enc_seq_len);
        defer allocator.free(enc_mask);
        @memset(enc_mask, 1);

        // 2. Decode, retrying at rising temperature when the greedy pass
        //    fails the reference guards (repetitive text or low confidence).
        const temperatures = [_]f32{ 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 };
        var chosen: ?Attempt = null;
        defer if (chosen) |*attempt| attempt.deinit(allocator);
        for (temperatures, 0..) |temperature, attempt_index| {
            if (attempt_index > 0 and !self.config.temperature_fallback) break;
            var attempt = try self.decodeAttempt(encoder_outputs[0], enc_seq_len, enc_mask, prompt_prefix, temperature, attempt_index, &timing);
            errdefer attempt.deinit(allocator);
            const needs_fallback = attempt.compression_ratio > self.config.compression_ratio_threshold or
                attempt.avg_logprob < self.config.logprob_threshold;
            if (chosen) |*previous| previous.deinit(allocator);
            chosen = attempt;
            if (!needs_fallback) break;
        }
        var attempt = chosen.?;
        // Whisper treats a window as silence when the no-speech token was
        // likely at the first step and the decode was unconfident anyway.
        const silent = attempt.no_speech_prob > self.config.no_speech_threshold and
            attempt.avg_logprob < self.config.logprob_threshold;

        // 3. Text and timed segments.
        const rules = self.timestampRules();
        const language = if (self.config.language) |language|
            try allocator.dupe(u8, language)
        else if (attempt.detected_language) |language|
            try allocator.dupe(u8, language)
        else
            null;
        errdefer if (language) |l| allocator.free(l);

        var text_tokens = std.ArrayListUnmanaged(i32).empty;
        errdefer text_tokens.deinit(allocator);
        if (!silent) for (attempt.generated.items) |token| {
            if (self.config.timestamps and rules.isTimestamp(token)) continue;
            try text_tokens.append(allocator, token);
        };
        const text = try self.tokenizer.decode(allocator, text_tokens.items);
        errdefer allocator.free(text);

        var segments = std.ArrayListUnmanaged(TimedSegment).empty;
        errdefer {
            for (segments.items) |segment| allocator.free(segment.text);
            segments.deinit(allocator);
        }
        if (self.config.timestamps and !silent) {
            const window_ms = windowDurationMs(samples.len, sample_rate, @as(usize, context_seconds));
            const token_segments = try whisper_timestamps.parseSegments(allocator, rules, attempt.generated.items, window_ms);
            defer allocator.free(token_segments);
            for (token_segments) |segment| {
                const raw = try self.tokenizer.decode(allocator, attempt.generated.items[segment.token_start..segment.token_end]);
                defer allocator.free(raw);
                const trimmed = std.mem.trim(u8, raw, " \t\r\n");
                if (trimmed.len == 0) continue;
                try segments.append(allocator, .{
                    .text = try allocator.dupe(u8, trimmed),
                    .start_ms = segment.start_ms,
                    .end_ms = segment.end_ms,
                });
            }
        }
        return .{
            .text = text,
            .language = language,
            .allocator = allocator,
            .timing = timing,
            .segments = try segments.toOwnedSlice(allocator),
            .tokens = try text_tokens.toOwnedSlice(allocator),
            .no_speech_prob = attempt.no_speech_prob,
            .avg_logprob = attempt.avg_logprob,
            .compression_ratio = attempt.compression_ratio,
            .temperature = attempt.temperature,
            .silent = silent,
        };
    }

    /// Lock the dynamic language slot to `code` for later windows. Returns
    /// false when the model has no token for it.
    pub fn lockLanguage(self: *TranscriptionPipeline, code: []const u8) bool {
        const token = whisper_prompt.languageTokenForCode(self.config.language_tokens, code) orelse return false;
        self.config.language_lock_token = token;
        return true;
    }

    fn timestampRules(self: *const TranscriptionPipeline) whisper_timestamps.Rules {
        return .{
            .timestamp_begin = self.config.decode.timestamp_begin_id,
            .eot = self.config.eos_token_id,
            .no_timestamps = self.config.no_timestamps_id,
            .max_initial_timestamp_index = self.config.decode.max_initial_timestamp_index,
        };
    }

    /// One full decode of the window at `temperature` (0 = greedy).
    const Attempt = struct {
        generated: std.ArrayListUnmanaged(i32),
        detected_language: ?[]const u8,
        no_speech_prob: f32,
        avg_logprob: f32,
        compression_ratio: f32,
        temperature: f32,

        fn deinit(self: *Attempt, allocator: std.mem.Allocator) void {
            self.generated.deinit(allocator);
        }
    };

    fn decodeAttempt(
        self: *TranscriptionPipeline,
        encoder_output: backends.Tensor,
        enc_seq_len: usize,
        enc_mask: []const i64,
        prompt_prefix: []const i32,
        temperature: f32,
        seed: usize,
        timing: *Timing,
    ) !Attempt {
        const allocator = self.allocator;
        const max_len = self.config.max_length;
        if (max_len == 0) return error.InvalidTranscriptionMaxLength;
        var dec_ids = try allocator.alloc(i64, max_len);
        defer allocator.free(dec_ids);

        // Optional conditioning prefix: <|startofprev|> p1..pk, then the
        // ordinary <|startoftranscript|> prompt. Forced prompt positions are
        // relative to <|startoftranscript|>, so they shift by the prefix.
        const prefix_capacity = @min(maxPromptPrefixTokens(max_len), max_len -| 2);
        const prefix_len = if (self.config.decode.start_of_prev_id != null) @min(prompt_prefix.len, prefix_capacity) else 0;
        const prefix = prompt_prefix[prompt_prefix.len - prefix_len ..];
        var dec_len: usize = 0;
        if (prefix_len > 0) {
            dec_ids[0] = self.config.decode.start_of_prev_id.?;
            for (prefix, 0..) |token, i| dec_ids[1 + i] = token;
            dec_len = prefix_len + 1;
        }
        const offset = dec_len;
        dec_ids[dec_len] = self.config.decoder_start_token_id;
        dec_len += 1;
        const forced = self.config.forced_decoder_ids orelse &.{};
        try validateForcedDecoderIds(forced, max_len -| offset);
        const prompt_end = offset + forcedDecoderPromptEnd(forced);
        var forced_index: usize = 0;
        var detected_language: ?[]const u8 = null;

        const scratch_logits = try allocator.alloc(f32, self.config.vocab_size);
        defer allocator.free(scratch_logits);
        var generated = std.ArrayListUnmanaged(i32).empty;
        errdefer generated.deinit(allocator);
        const rules = self.timestampRules();
        var prng = std.Random.DefaultPrng.init(0x5eed_0000 + @as(u64, seed));
        const random = prng.random();
        var logprob_sum: f64 = 0;
        var no_speech_prob: f32 = 0;

        // Native Whisper sessions decode through the KV cache; ONNX merged
        // bundles keep their own incremental state; anything else re-runs
        // the full prefix per step.
        var native_decoder: ?session_factory.WhisperNativeDecoder = try session_factory.whisperNativeDecoder(
            self.decoder,
            allocator,
            self.execution_control,
            encoder_output.asFloat32(),
            enc_seq_len,
        );
        defer if (native_decoder) |*decoder| decoder.deinit();
        timing.kv_cached = native_decoder != null;
        var incremental = if (native_decoder != null) null else @import("seq2seq_decode.zig").State.init(allocator, self.decoder, encoder_output, enc_mask, self.config.vocab_size);
        defer if (incremental) |*state| state.deinit();
        // Suppression list handed to the device token-choice kernel.
        var suppress_scratch = std.ArrayListUnmanaged(i32).empty;
        defer suppress_scratch.deinit(allocator);
        const rules_active = self.config.timestamps and rules.timestamp_begin >= 0 and
            @as(usize, @intCast(rules.timestamp_begin)) < self.config.vocab_size;

        while (dec_len < max_len) {
            if (forced_index < forced.len and forced[forced_index].position + offset == dec_len) {
                if (forced[forced_index].token_id) |token_id| {
                    dec_ids[dec_len] = token_id;
                    dec_len += 1;
                    forced_index += 1;
                    continue;
                }
            }

            const generated_position = dec_len;
            if (self.execution_control) |control| try control.update(.executing, @intCast(generated_position), @intCast(self.config.max_length));
            const step_started = platform.time.monotonicNs();
            const dynamic_language_slot = forced_index < forced.len and
                forced[forced_index].position + offset == generated_position and
                forced[forced_index].token_id == null and
                generated_position == offset + 1;

            var native_logits: ?[]f32 = null;
            defer if (native_logits) |row| allocator.free(row);
            var dec_outputs: []backends.Tensor = &.{};
            defer {
                for (dec_outputs) |*t| {
                    var mt = t.*;
                    mt.deinit();
                }
                if (dec_outputs.len > 0) allocator.free(dec_outputs);
            }
            var last_logits: []const f32 = &.{};
            // Greedy steps let the device choose the token and report the
            // log-sum-exp terms, so the vocabulary row never leaves the GPU.
            var step_stats: ?ops.WhisperLogitsStatsRaw = null;
            if (native_decoder) |*decoder| {
                const pending = dec_ids[decoder.positions()..dec_len];
                if (temperature == 0 and !dynamic_language_slot) {
                    const free_text = generated_position >= prompt_end;
                    suppress_scratch.clearRetainingCapacity();
                    if (free_text) {
                        try suppress_scratch.appendSlice(allocator, self.config.decode.suppress_tokens);
                        if (generated_position == prompt_end) try suppress_scratch.appendSlice(allocator, self.config.decode.begin_suppress_tokens);
                        if (rules_active) try suppress_scratch.append(allocator, rules.no_timestamps);
                    }
                    const vocab = self.config.vocab_size;
                    const window = if (free_text and rules_active)
                        whisper_timestamps.ruleWindow(rules, generated.items, vocab)
                    else
                        whisper_timestamps.RuleWindow{ .text_allowed = true, .ts_min = vocab, .ts_max = vocab };
                    const request = whisper_arch.StatsRequest{
                        .params = .{
                            .out_dim = @intCast(vocab),
                            .suppress_count = @intCast(suppress_scratch.items.len),
                            .ts_begin = if (free_text and rules_active) @intCast(rules.timestamp_begin) else @intCast(vocab),
                            .text_allowed = @intFromBool(window.text_allowed),
                            .ts_min = @intCast(window.ts_min),
                            .ts_max = @intCast(window.ts_max),
                            .eot = if (self.config.eos_token_id >= 0) @intCast(self.config.eos_token_id) else @intCast(vocab),
                            .probe_id = if (free_text and generated_position == prompt_end and self.config.no_timestamps_id > 0) @intCast(self.config.no_timestamps_id - 1) else @intCast(vocab),
                        },
                        .suppress = suppress_scratch.items,
                    };
                    switch (try decoder.stepWith(pending, .{ .stats = request })) {
                        .stats => |stats| step_stats = stats,
                        .logits => |row| {
                            native_logits = row;
                            last_logits = row;
                        },
                        .none, .encoded => return error.NoDecoderOutput,
                    }
                } else {
                    native_logits = try decoder.step(pending);
                    last_logits = native_logits.?;
                }
            } else {
                const dec_seq: i64 = @intCast(dec_len);
                const dec_shape = [_]i64{ 1, dec_seq };
                var dec_tensor = try backends.Tensor.initInt64(allocator, "input_ids", &dec_shape, dec_ids[0..dec_len]);
                defer dec_tensor.deinit();
                const enc_hidden = encoder_output.borrowedView("encoder_hidden_states");
                dec_outputs = if (incremental) |*state| try state.stepOutputs(dec_ids[0..dec_len], self.batch_dispatch, self.execution_control) else if (self.batch_dispatch) |dispatch| try dispatch.run(allocator, self.decoder, null, null, &.{ dec_tensor, enc_hidden }, self.execution_control) else try self.decoder.runWithControl(
                    &.{ dec_tensor, enc_hidden },
                    allocator,
                    self.execution_control,
                );
                if (dec_outputs.len == 0) return error.NoDecoderOutput;
                const logits = dec_outputs[0].asFloat32();
                const vocab_size = if (dec_outputs[0].shape.len >= 3)
                    @as(usize, @intCast(dec_outputs[0].shape[2]))
                else
                    return error.InvalidLogitsShape;
                if (vocab_size == 0 or logits.len < vocab_size) return error.InvalidLogitsShape;
                last_logits = logits[logits.len - vocab_size ..];
            }
            const vocab_size = last_logits.len;
            const step_ns = platform.time.monotonicNs() -| step_started;
            if (generated_position >= prompt_end) {
                timing.decode_ns += step_ns;
                timing.decode_steps += 1;
            } else timing.prefill_ns += step_ns;

            var best_token: i32 = undefined;
            if (step_stats) |*stats| {
                if (generated_position >= prompt_end) {
                    if (generated_position == prompt_end) no_speech_prob = whisper_timestamps.statsProbeProbability(stats);
                    const eot: u32 = if (self.config.eos_token_id >= 0) @intCast(self.config.eos_token_id) else 0;
                    var eot_allowed = self.config.eos_token_id >= 0;
                    for (suppress_scratch.items) |t| if (t == self.config.eos_token_id) {
                        eot_allowed = false;
                    };
                    if (whisper_timestamps.chooseFromStats(stats, rules_active, eot, eot_allowed)) |choice| {
                        best_token = @intCast(choice.token);
                        logprob_sum += choice.logprob;
                    } else {
                        // Every token suppressed: mirror the host argmax over
                        // an all -inf row.
                        best_token = 0;
                        logprob_sum += -100.0;
                    }
                } else {
                    best_token = @intCast(whisper_timestamps.statsId(stats, whisper_timestamps.stats_raw_id) orelse 0);
                }
            } else if (dynamic_language_slot) {
                if (self.config.language_lock_token) |locked| {
                    best_token = locked;
                    detected_language = whisper_prompt.languageCodeForToken(self.config.language_tokens, locked);
                } else if (whisper_prompt.detectLanguageToken(self.config.language_tokens, last_logits)) |detected| {
                    detected_language = detected.code;
                    best_token = detected.token_id;
                } else best_token = @intCast(argmax(last_logits));
            } else if (generated_position >= prompt_end) {
                if (generated_position == prompt_end) {
                    no_speech_prob = tokenProbability(last_logits, self.config.no_timestamps_id - 1);
                }
                // Free text position: apply the model's suppression lists and
                // the timestamp grammar before choosing.
                const scored = scratch_logits[0..@min(vocab_size, scratch_logits.len)];
                @memcpy(scored, last_logits[0..scored.len]);
                whisper_timestamps.suppressTokens(scored, self.config.decode.suppress_tokens);
                if (generated_position == prompt_end)
                    whisper_timestamps.suppressTokens(scored, self.config.decode.begin_suppress_tokens);
                if (self.config.timestamps) whisper_timestamps.applyRules(rules, scored, generated.items);
                const choice = if (temperature > 0) sampleToken(scored, temperature, random) else argmax(scored);
                best_token = @intCast(choice);
                logprob_sum += tokenLogProbability(scored, choice);
            } else {
                best_token = @intCast(argmax(last_logits));
            }
            // Prompt slots are control tokens. Do not terminate before the
            // artifact-defined prompt is complete even if a malformed model
            // predicts EOS for a dynamic slot.
            if (best_token == self.config.eos_token_id and generated_position >= prompt_end) break;

            dec_ids[dec_len] = best_token;
            dec_len += 1;
            if (generated_position >= prompt_end) try generated.append(allocator, best_token);
            if (forced_index < forced.len and forced[forced_index].position + offset == generated_position) {
                forced_index += 1;
            }

            // Past the first free token every greedy step has the same
            // shape, so the device can choose tokens and keep the grammar
            // itself while the host runs one step ahead. Anything the
            // backend declines leaves the loop on the synchronous path.
            if (native_decoder) |*decoder| {
                if (temperature == 0 and step_stats != null and generated_position == prompt_end and dec_len < max_len) {
                    if (try self.decodePipelined(decoder, dec_ids, &dec_len, &generated, &suppress_scratch, rules, rules_active, &logprob_sum, timing)) break;
                }
            }
        }

        // The reference divides by the token count plus the EOT that ended it.
        const avg_logprob: f32 = @floatCast(logprob_sum / @as(f64, @floatFromInt(generated.items.len + 1)));
        const text = try self.tokenizer.decode(allocator, generated.items);
        defer allocator.free(text);
        return .{
            .generated = generated,
            .detected_language = detected_language,
            .no_speech_prob = no_speech_prob,
            .avg_logprob = avg_logprob,
            .compression_ratio = try compressionRatio(allocator, text),
            .temperature = temperature,
        };
    }

    /// Greedy free-text steps with one step in flight: while the frame
    /// for position p runs, the host encodes the frame for p+1 (embedding
    /// the token slot the frame for p writes), then waits for p and
    /// submits p+1. Returns true when decoding finished here (end of text
    /// or the length limit); false when the backend could not pipeline
    /// and the caller must continue synchronously from `dec_len`, which
    /// is then consistent with the decoder cache.
    fn decodePipelined(
        self: *TranscriptionPipeline,
        decoder: *session_factory.WhisperNativeDecoder,
        dec_ids: []i64,
        dec_len: *usize,
        generated: *std.ArrayListUnmanaged(i32),
        suppress_scratch: *std.ArrayListUnmanaged(i32),
        rules: whisper_timestamps.Rules,
        rules_active: bool,
        logprob_sum: *f64,
        timing: *Timing,
    ) !bool {
        const allocator = self.allocator;
        const max_len = self.config.max_length;
        const trace = platform.env.getenvBool("TERMITE_WHISPER_TRACE_PIPELINE");
        if (!decoder.setPipelined(true)) {
            if (trace) std.debug.print("whisper_pipeline: declined (backend cannot pipeline)\n", .{});
            return false;
        }
        defer _ = decoder.setPipelined(false);
        // Whatever happens, no frame may outlive this call: the cache slabs
        // it writes are freed with the decoder.
        defer decoder.drain();

        suppress_scratch.clearRetainingCapacity();
        try suppress_scratch.appendSlice(allocator, self.config.decode.suppress_tokens);
        if (rules_active) try suppress_scratch.append(allocator, rules.no_timestamps);
        var eot_allowed = self.config.eos_token_id >= 0;
        for (suppress_scratch.items) |t| if (t == self.config.eos_token_id) {
            eot_allowed = false;
        };
        const vocab = self.config.vocab_size;
        const state = whisper_timestamps.grammarState(rules, generated.items, vocab, rules_active);
        if (!decoder.seedGrammar(&state)) {
            if (trace) std.debug.print("whisper_pipeline: declined (grammar seed)\n", .{});
            return false;
        }
        var mode: u32 = ops.whisper_logits_mode_device_window | ops.whisper_logits_mode_choose;
        if (rules_active) mode |= ops.whisper_logits_mode_timestamps;
        if (eot_allowed) mode |= ops.whisper_logits_mode_eot_allowed;
        var params = ops.WhisperLogitsParams{
            .out_dim = @intCast(vocab),
            .suppress_count = @intCast(suppress_scratch.items.len),
            .ts_begin = if (rules_active) @intCast(rules.timestamp_begin) else @intCast(vocab),
            .text_allowed = 1,
            .ts_min = @intCast(vocab),
            .ts_max = @intCast(vocab),
            .eot = if (self.config.eos_token_id >= 0) @intCast(self.config.eos_token_id) else @intCast(vocab),
            .probe_id = @intCast(vocab),
            .mode = mode,
        };

        // The first in-flight step embeds the token the host just chose.
        var slot: usize = 0;
        params.token_slot = 0;
        params.stats_slot = 0;
        var step_started = platform.time.monotonicNs();
        const pending = dec_ids[decoder.positions()..dec_len.*];
        const first = decoder.stepWith(pending, .{ .pipelined = .{ .params = params, .suppress = suppress_scratch.items } }) catch |err| switch (err) {
            error.UnsupportedOperation => {
                if (trace) std.debug.print("whisper_pipeline: declined (first step unsupported)\n", .{});
                return false;
            },
            else => return err,
        };
        if (first != .encoded) return false;
        try decoder.submit();
        var steps: usize = 0;
        defer if (trace) std.debug.print("whisper_pipeline: {d} steps in flight mode\n", .{steps});

        while (true) {
            const position = dec_len.*;
            if (self.execution_control) |control| try control.update(.executing, @intCast(position), @intCast(self.config.max_length));
            // Encode the next step while this one runs; it reads the token
            // this step's choice kernel writes to `slot`.
            var lookahead = false;
            if (position + 1 < max_len) {
                params.token_slot = @intCast(1 - slot);
                params.stats_slot = @intCast(1 - slot);
                const placeholder = [_]i64{0};
                const next = decoder.stepWith(&placeholder, .{ .pipelined = .{ .params = params, .suppress = suppress_scratch.items, .device_token_slot = slot } }) catch |err| switch (err) {
                    error.UnsupportedOperation => null,
                    else => return err,
                };
                lookahead = next != null and next.? == .encoded;
            }
            const stats = (try decoder.awaitStats(slot)) orelse return error.NoDecoderOutput;
            steps += 1;
            if (trace and !lookahead and position + 1 < max_len) std.debug.print("whisper_pipeline: lookahead unsupported at position {d}\n", .{position});
            const now = platform.time.monotonicNs();
            timing.decode_ns += now -| step_started;
            timing.decode_steps += 1;
            step_started = now;
            const token: i32 = @intCast(whisper_timestamps.statsId(&stats, whisper_timestamps.stats_choice_token) orelse 0);
            logprob_sum.* += stats[whisper_timestamps.stats_choice_logprob];
            if (token == self.config.eos_token_id) {
                // The encoded lookahead, if any, predicted past the end.
                if (lookahead) decoder.discard();
                return true;
            }
            dec_ids[dec_len.*] = token;
            dec_len.* += 1;
            try generated.append(allocator, token);
            if (!lookahead) {
                // Either the length limit or a backend that could not
                // encode the lookahead; the cache is consistent with
                // `dec_len` either way.
                return dec_len.* >= max_len;
            }
            try decoder.submit();
            slot = 1 - slot;
        }
    }

    pub fn deinit(_: *TranscriptionPipeline) void {
        // Sessions and tokenizer are borrowed — caller manages their lifetime.
    }
};

fn forcedDecoderPromptEnd(forced: []const whisper_prompt.ForcedDecoderId) usize {
    return if (forced.len == 0) 1 else forced[forced.len - 1].position +| 1;
}

fn argmax(values: []const f32) usize {
    var best_id: usize = 0;
    var best_val: f32 = -std.math.inf(f32);
    for (values, 0..) |value, i| {
        if (value > best_val) {
            best_val = value;
            best_id = i;
        }
    }
    return best_id;
}

fn logSumExp(values: []const f32) f32 {
    var max_value: f32 = -std.math.inf(f32);
    for (values) |v| if (v > max_value) {
        max_value = v;
    };
    if (max_value == -std.math.inf(f32)) return max_value;
    var sum: f64 = 0;
    for (values) |v| if (v != -std.math.inf(f32)) {
        sum += @exp(@as(f64, v - max_value));
    };
    return max_value + @as(f32, @floatCast(@log(sum)));
}

fn tokenLogProbability(logits: []const f32, token: usize) f64 {
    if (token >= logits.len or logits[token] == -std.math.inf(f32)) return -100.0;
    return @as(f64, logits[token]) - @as(f64, logSumExp(logits));
}

fn tokenProbability(logits: []const f32, token: i32) f32 {
    if (token < 0 or @as(usize, @intCast(token)) >= logits.len) return 0;
    const lse = logSumExp(logits);
    return @exp(logits[@intCast(token)] - lse);
}

/// Sample from softmax(logits / temperature); -inf entries are excluded.
fn sampleToken(logits: []const f32, temperature: f32, random: std.Random) usize {
    var max_value: f32 = -std.math.inf(f32);
    for (logits) |v| if (v > max_value) {
        max_value = v;
    };
    if (max_value == -std.math.inf(f32)) return 0;
    var total: f64 = 0;
    for (logits) |v| if (v != -std.math.inf(f32)) {
        total += @exp(@as(f64, (v - max_value) / temperature));
    };
    var target = random.float(f64) * total;
    var last_valid: usize = 0;
    for (logits, 0..) |v, i| {
        if (v == -std.math.inf(f32)) continue;
        last_valid = i;
        target -= @exp(@as(f64, (v - max_value) / temperature));
        if (target <= 0) return i;
    }
    return last_valid;
}

/// Bytes of `text` over its zlib-compressed size, the reference repetition
/// signal. Short texts compress poorly and score below 1.
pub fn compressionRatio(allocator: std.mem.Allocator, text: []const u8) !f32 {
    if (text.len == 0) return 0;
    // The compressor needs a writable output buffer up front.
    var sink: std.Io.Writer.Allocating = try .initCapacity(allocator, @max(@as(usize, 256), text.len));
    defer sink.deinit();
    const window = try allocator.alloc(u8, std.compress.flate.max_window_len);
    defer allocator.free(window);
    var compress = try std.compress.flate.Compress.init(&sink.writer, window, .zlib, .default);
    try compress.writer.writeAll(text);
    try compress.finish();
    const compressed = sink.written().len;
    if (compressed == 0) return 0;
    return @as(f32, @floatFromInt(text.len)) / @as(f32, @floatFromInt(compressed));
}

test "hallucination guard helpers" {
    const allocator = std.testing.allocator;
    const repetitive = "the the the the the the the the the the the the the the the the the the the the the the the the the the";
    const varied = "quick brown fox jumps over one lazy dog near the riverbank at dusk";
    try std.testing.expect((try compressionRatio(allocator, repetitive)) > 2.4);
    try std.testing.expect((try compressionRatio(allocator, varied)) < 2.4);
    try std.testing.expectEqual(@as(f32, 0), try compressionRatio(allocator, ""));

    const logits = [_]f32{ 0, 0, 2.0, -std.math.inf(f32) };
    try std.testing.expectApproxEqAbs(@as(f32, 0.7869), tokenProbability(&logits, 2), 1e-3);
    try std.testing.expect(tokenLogProbability(&logits, 2) > tokenLogProbability(&logits, 0));
    try std.testing.expectEqual(@as(f64, -100.0), tokenLogProbability(&logits, 3));
    var prng = std.Random.DefaultPrng.init(7);
    for (0..20) |_| try std.testing.expect(sampleToken(&logits, 0.5, prng.random()) != 3);
}

fn windowDurationMs(sample_count: usize, sample_rate: u32, chunk_length_s: usize) u64 {
    if (sample_rate == 0) return 0;
    const ms = (@as(u64, sample_count) * 1000) / sample_rate;
    return @min(ms, @as(u64, chunk_length_s) * 1000);
}

test "argmax and window duration helpers" {
    try std.testing.expectEqual(@as(usize, 2), argmax(&.{ 0.1, 0.5, 0.9, 0.2 }));
    try std.testing.expectEqual(@as(usize, 0), argmax(&.{}));
    try std.testing.expectEqual(@as(u64, 2500), windowDurationMs(40_000, 16_000, 30));
    try std.testing.expectEqual(@as(u64, 30_000), windowDurationMs(16_000 * 45, 16_000, 30));
    try std.testing.expectEqual(@as(usize, 223), maxPromptPrefixTokens(448));
}

fn validateForcedDecoderIds(forced: []const whisper_prompt.ForcedDecoderId, max_len: usize) !void {
    var previous_position: usize = 0;
    for (forced) |entry| {
        if (entry.position == 0 or entry.position <= previous_position or entry.position >= max_len) {
            return error.InvalidWhisperDecoderPrompt;
        }
        previous_position = entry.position;
    }
}

test "Whisper decoder prompts retain dynamic language slots without gaps" {
    const forced = [_]whisper_prompt.ForcedDecoderId{
        .{ .position = 1, .token_id = null },
        .{ .position = 2, .token_id = 50359 },
        .{ .position = 3, .token_id = 50363 },
    };
    try validateForcedDecoderIds(&forced, 16);
    try std.testing.expectEqual(@as(usize, 4), forcedDecoderPromptEnd(&forced));
}

test "Whisper decoder prompts reject unsorted and out of bounds entries" {
    try std.testing.expectError(error.InvalidWhisperDecoderPrompt, validateForcedDecoderIds(&.{
        .{ .position = 2, .token_id = 10 },
        .{ .position = 1, .token_id = null },
    }, 8));
    try std.testing.expectError(error.InvalidWhisperDecoderPrompt, validateForcedDecoderIds(&.{
        .{ .position = 8, .token_id = 10 },
    }, 8));
}
