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
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("inference_tokenizer");
const audio = @import("audio.zig");
const whisper_prompt = @import("whisper_prompt.zig");

pub const TranscribeConfig = struct {
    max_length: usize = 448,
    language: ?[]const u8 = null,
    sample_rate: usize = 16000,
    n_mels: usize = 80,
    chunk_length_s: usize = 30,
    decoder_start_token_id: i32 = 50258,
    eos_token_id: i32 = 50257,
    /// Decoder prompt entries from generation_config.json. A null token leaves
    /// that position model-generated (Whisper uses this for language detection).
    forced_decoder_ids: ?[]const whisper_prompt.ForcedDecoderId = null,
};

pub const TranscribeResult = struct {
    text: []const u8,
    language: ?[]const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *TranscribeResult) void {
        self.allocator.free(self.text);
        if (self.language) |l| self.allocator.free(l);
    }
};

pub const TranscriptionPipeline = struct {
    allocator: std.mem.Allocator,
    encoder: backends.Session,
    decoder: backends.Session,
    tokenizer: tokenizer_mod.Tokenizer,
    config: TranscribeConfig,

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
        var decoded = try audio.decode(self.allocator, audio_data, decode_options);
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

    /// Transcribe PCM audio samples at the given sample rate.
    pub fn transcribePcm(self: *TranscriptionPipeline, samples: []const f32, sample_rate: u32) !TranscribeResult {
        const allocator = self.allocator;
        const mel_elements = std.math.mul(
            usize,
            audio.WHISPER_N_MELS,
            audio.WHISPER_N_FRAMES,
        ) catch return error.ResourceLimitExceeded;
        const mel_bytes = std.math.mul(usize, mel_elements, @sizeOf(f32)) catch
            return error.ResourceLimitExceeded;
        const pcm_bytes = std.math.mul(usize, samples.len, @sizeOf(f32)) catch
            return error.ResourceLimitExceeded;
        var encoder_permit = try self.encoder.admit(.{
            .batch = 1,
            .sequence = audio.WHISPER_N_FRAMES,
            .input_bytes = mel_bytes,
            .host_preprocess_bytes = std.math.add(
                usize,
                pcm_bytes,
                mel_bytes,
            ) catch return error.ResourceLimitExceeded,
        });
        defer encoder_permit.deinit();

        const mel = try audio.whisperMelFromPcm(allocator, samples, sample_rate);
        defer allocator.free(mel);

        // 1. Run encoder on [1, 80, 3000] log-mel input.
        const n_mels: i64 = @intCast(audio.WHISPER_N_MELS);
        const n_frames: i64 = @intCast(audio.WHISPER_N_FRAMES);
        const mel_shape = [_]i64{ 1, n_mels, n_frames };
        var mel_tensor = try backends.Tensor.initFloat32(allocator, "input_features", &mel_shape, mel);
        defer mel_tensor.deinit();

        const encoder_outputs = try encoder_permit.run(&.{mel_tensor}, allocator);
        defer {
            for (encoder_outputs) |*t| {
                var mt = t.*;
                mt.deinit();
            }
            allocator.free(encoder_outputs);
        }

        if (encoder_outputs.len == 0) return error.NoEncoderOutput;

        // Get encoder sequence length
        const enc_seq_len: usize = if (encoder_outputs[0].shape.len >= 2) @intCast(encoder_outputs[0].shape[1]) else 1;

        // 2. Autoregressive decode
        const max_len = self.config.max_length;
        if (max_len == 0) return error.InvalidTranscriptionMaxLength;
        var dec_ids = try allocator.alloc(i64, max_len);
        defer allocator.free(dec_ids);

        // Initial decoder token. Concrete prompt positions are appended without
        // a model invocation; nullable positions are generated autoregressively.
        dec_ids[0] = self.config.decoder_start_token_id;
        var dec_len: usize = 1;
        const forced = self.config.forced_decoder_ids orelse &.{};
        try validateForcedDecoderIds(forced, max_len);
        const prompt_end = forcedDecoderPromptEnd(forced);
        var forced_index: usize = 0;
        var detected_language: ?[]const u8 = null;

        // Encoder mask
        const enc_mask = try allocator.alloc(i64, enc_seq_len);
        defer allocator.free(enc_mask);
        @memset(enc_mask, 1);

        while (dec_len < max_len) {
            if (forced_index < forced.len and forced[forced_index].position == dec_len) {
                if (forced[forced_index].token_id) |token_id| {
                    dec_ids[dec_len] = token_id;
                    dec_len += 1;
                    forced_index += 1;
                    continue;
                }
            }

            const generated_position = dec_len;
            const dec_seq: i64 = @intCast(dec_len);
            const dec_shape = [_]i64{ 1, dec_seq };

            var dec_tensor = try backends.Tensor.initInt64(allocator, "input_ids", &dec_shape, dec_ids[0..dec_len]);
            defer dec_tensor.deinit();

            // Rename encoder output to match decoder's expected input name
            const enc_hidden = encoder_outputs[0].borrowedView("encoder_hidden_states");

            const dec_outputs = try self.decoder.run(&.{ dec_tensor, enc_hidden }, allocator);
            defer {
                for (dec_outputs) |*t| {
                    var mt = t.*;
                    mt.deinit();
                }
                allocator.free(dec_outputs);
            }

            if (dec_outputs.len == 0) return error.NoDecoderOutput;

            const logits = dec_outputs[0].asFloat32();
            const vocab_size = if (dec_outputs[0].shape.len >= 3)
                @as(usize, @intCast(dec_outputs[0].shape[2]))
            else
                return error.InvalidLogitsShape;

            const last_logits = logits[(dec_len - 1) * vocab_size ..][0..vocab_size];

            // Greedy argmax
            var best_id: usize = 0;
            var best_val: f32 = last_logits[0];
            for (1..vocab_size) |i| {
                if (last_logits[i] > best_val) {
                    best_val = last_logits[i];
                    best_id = i;
                }
            }

            const dynamic_language_slot = forced_index < forced.len and
                forced[forced_index].position == generated_position and
                forced[forced_index].token_id == null and
                generated_position == 1;
            const best_token: i32 = if (dynamic_language_slot)
                if (whisper_prompt.detectLanguageToken(allocator, self.tokenizer, last_logits)) |detected| blk: {
                    detected_language = detected.code;
                    break :blk detected.token_id;
                } else @intCast(best_id)
            else
                @intCast(best_id);
            // Prompt slots are control tokens. Do not terminate before the
            // artifact-defined prompt is complete even if a malformed model
            // predicts EOS for a dynamic slot.
            if (best_token == self.config.eos_token_id and generated_position >= prompt_end) break;

            dec_ids[dec_len] = best_token;
            dec_len += 1;
            if (forced_index < forced.len and forced[forced_index].position == generated_position) {
                forced_index += 1;
            }
        }

        // 3. Decode tokens to text (skip forced prefix tokens)
        const text_start: usize = @min(prompt_end, dec_len);
        const text_len = if (dec_len > text_start) dec_len - text_start else 0;

        const token_ids = try allocator.alloc(i32, text_len);
        defer allocator.free(token_ids);
        for (0..text_len) |i| token_ids[i] = @intCast(dec_ids[text_start + i]);

        const text = try self.tokenizer.decode(allocator, token_ids);
        errdefer allocator.free(text);
        const language = if (self.config.language) |language|
            try allocator.dupe(u8, language)
        else if (detected_language) |language|
            try allocator.dupe(u8, language)
        else
            null;
        return .{ .text = text, .language = language, .allocator = allocator };
    }

    pub fn deinit(_: *TranscriptionPipeline) void {
        // Sessions and tokenizer are borrowed — caller manages their lifetime.
    }
};

fn forcedDecoderPromptEnd(forced: []const whisper_prompt.ForcedDecoderId) usize {
    return if (forced.len == 0) 1 else forced[forced.len - 1].position +| 1;
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
