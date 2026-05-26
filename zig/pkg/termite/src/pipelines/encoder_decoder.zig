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

// Encoder-decoder pipeline for Seq2Seq models (T5, BART, Whisper, Florence2).
//
// Handles the autoregressive generation loop: given an encoder Session and
// a decoder Session, runs encode → greedy decode → return token IDs.
//
// Also provides helpers to detect encoder-decoder model directories and
// parse DecoderConfig from config.json.
//
// This is a pipeline concern, not a backend concern. The backend just provides
// Sessions that can run forward passes. This pipeline orchestrates them.
//
// Matches Go termite's lib/pipelines/encoder_decoder.go pattern.

const std = @import("std");
const backends = @import("../backends/backends.zig");
const c_file = @import("../util/c_file.zig");
const manifest_mod = @import("../models/manifest.zig");

/// Configuration parsed from config.json for the decoder architecture.
pub const DecoderConfig = struct {
    num_layers: usize = 6,
    num_heads: usize = 8,
    head_dim: usize = 64,
    vocab_size: usize = 32128,
    decoder_start_token_id: i32 = 0,
    eos_token_id: i32 = 1,
    pad_token_id: i32 = 0,
    forced_bos_token_id: ?i32 = null,
    no_repeat_ngram_size: usize = 0,
    max_length: usize = 512,
};

/// Result of encoder-decoder generation.
pub const EncoderDecoderResult = struct {
    text_ids: []i32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EncoderDecoderResult) void {
        self.allocator.free(self.text_ids);
    }
};

/// Encoder-decoder pipeline that orchestrates encode → decode → generate.
/// Backend-agnostic: works with any pair of Sessions (ONNX, native).
pub const EncoderDecoderPipeline = struct {
    allocator: std.mem.Allocator,
    encoder: backends.Session,
    decoder: backends.Session,
    config: DecoderConfig,

    /// Run the encoder on input_ids, returning hidden state tensors.
    pub fn encode(self: *EncoderDecoderPipeline, allocator: std.mem.Allocator, input_ids: []const i64, seq_len: usize) ![]backends.Tensor {
        const batch: i64 = 1;
        const seq: i64 = @intCast(seq_len);
        const shape = &[_]i64{ batch, seq };

        // Build attention mask (all 1s)
        const mask = try allocator.alloc(i64, seq_len);
        defer allocator.free(mask);
        @memset(mask, 1);

        var input_ids_tensor = try backends.Tensor.initInt64(allocator, "input_ids", shape, input_ids);
        defer input_ids_tensor.deinit();

        var mask_tensor = try backends.Tensor.initInt64(allocator, "attention_mask", shape, mask);
        defer mask_tensor.deinit();

        return try self.encoder.run(&.{ input_ids_tensor, mask_tensor }, allocator);
    }

    /// Run one decoder step, returning logits for the last token position.
    fn decoderStep(
        self: *EncoderDecoderPipeline,
        allocator: std.mem.Allocator,
        dec_ids: []const i64,
        dec_seq_len: usize,
        encoder_hidden: *const backends.Tensor,
        encoder_attention_mask: []const i64,
        encoder_seq_len: usize,
    ) ![]f32 {
        const dec_seq: i64 = @intCast(dec_seq_len);
        const dec_shape = &[_]i64{ 1, dec_seq };

        var dec_input_ids = try backends.Tensor.initInt64(allocator, "input_ids", dec_shape, dec_ids);
        defer dec_input_ids.deinit();

        const enc_seq: i64 = @intCast(encoder_seq_len);
        const enc_mask_shape = &[_]i64{ 1, enc_seq };
        var enc_mask_tensor = try backends.Tensor.initInt64(allocator, "encoder_attention_mask", enc_mask_shape, encoder_attention_mask);
        defer enc_mask_tensor.deinit();

        // Rename encoder hidden state for decoder input compatibility
        // (encoder outputs "last_hidden_state", decoder expects "encoder_hidden_states")
        var enc_hidden_renamed = encoder_hidden.*;
        enc_hidden_renamed.name = "encoder_hidden_states";

        var decoder_outputs = try self.decoder.run(&.{
            dec_input_ids,
            enc_mask_tensor,
            enc_hidden_renamed,
        }, allocator);
        defer {
            for (decoder_outputs) |*o| o.deinit();
            allocator.free(decoder_outputs);
        }

        if (decoder_outputs.len == 0) return error.NoDecoderOutput;
        const logits_tensor = &decoder_outputs[0];
        const logits = logits_tensor.asFloat32();

        // Extract logits for the last token position
        const vocab_size = self.config.vocab_size;
        if (logits.len < vocab_size) return error.LogitsSizeMismatch;
        const last_pos_start = (dec_seq_len - 1) * vocab_size;

        // Copy because decoder_outputs are freed by defer
        const result = try allocator.alloc(f32, vocab_size);
        @memcpy(result, logits[last_pos_start .. last_pos_start + vocab_size]);
        return result;
    }

    /// Greedy autoregressive decode from encoder outputs.
    pub fn greedyDecode(
        self: *EncoderDecoderPipeline,
        allocator: std.mem.Allocator,
        encoder_outputs: []backends.Tensor,
        encoder_attention_mask: []const i64,
        encoder_seq_len: usize,
    ) !EncoderDecoderResult {
        const max_len = self.config.max_length;
        var output_ids = std.ArrayListUnmanaged(i32).empty;
        defer output_ids.deinit(allocator);

        // Start with decoder_start_token_id
        try output_ids.append(allocator, self.config.decoder_start_token_id);

        if (encoder_outputs.len == 0) return error.NoEncoderOutput;
        const encoder_hidden = &encoder_outputs[0];

        for (0..max_len) |_| {
            const dec_seq_len: usize = output_ids.items.len;

            // Convert output_ids (i32) to i64 for the decoder
            const dec_ids_i64 = try allocator.alloc(i64, dec_seq_len);
            defer allocator.free(dec_ids_i64);
            for (output_ids.items, 0..) |id, i| {
                dec_ids_i64[i] = @intCast(id);
            }

            // Run one decoder step
            const last_logits = try self.decoderStep(
                allocator,
                dec_ids_i64,
                dec_seq_len,
                encoder_hidden,
                encoder_attention_mask,
                encoder_seq_len,
            );
            defer allocator.free(last_logits);

            // Greedy: pick argmax
            var max_idx: usize = 0;
            var max_val: f32 = last_logits[0];
            for (last_logits[1..], 1..) |v, i| {
                if (v > max_val) {
                    max_val = v;
                    max_idx = i;
                }
            }

            const next_token: i32 = @intCast(max_idx);
            if (next_token == self.config.eos_token_id) break;

            try output_ids.append(allocator, next_token);
        }

        return .{
            .text_ids = try allocator.dupe(i32, output_ids.items),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EncoderDecoderPipeline) void {
        self.encoder.close();
        self.decoder.close();
    }
};

// --- Model detection and config loading helpers ---

const encoder_candidates = &[_][]const u8{
    "encoder_model.onnx",
    "vision_encoder.onnx",
    "encoder.onnx",
};

const decoder_candidates = &[_][]const u8{
    "decoder-init.onnx",
    "decoder_model.onnx",
    "decoder.onnx",
    "decoder_model_merged.onnx",
    "decoder_with_past_model.onnx",
};

/// Check if a model directory contains encoder-decoder ONNX files.
pub fn isEncoderDecoderModel(model_dir: []const u8) bool {
    const allocator = std.heap.page_allocator;
    const encoder = findModelFile(allocator, model_dir, encoder_candidates) catch return false;
    if (encoder) |e| {
        allocator.free(e);
    } else return false;

    const decoder = findModelFile(allocator, model_dir, decoder_candidates) catch return false;
    if (decoder) |d| {
        allocator.free(d);
    } else return false;

    return true;
}

/// Find encoder and decoder ONNX file paths for a model directory.
/// Returns allocated paths that the caller must free.
pub fn findEncoderDecoderPaths(allocator: std.mem.Allocator, model_dir: []const u8) !struct { encoder: []const u8, decoder: []const u8 } {
    const encoder_path = try findModelFile(allocator, model_dir, encoder_candidates);
    const decoder_path = try findModelFile(allocator, model_dir, decoder_candidates);

    if (encoder_path != null and decoder_path != null) {
        return .{ .encoder = encoder_path.?, .decoder = decoder_path.? };
    }
    if (encoder_path) |path| allocator.free(path);
    if (decoder_path) |path| allocator.free(path);

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    if (manifest.native_arch_hint == .florence and
        manifestHasNativeAssets(manifest) and
        manifest.visual_model_path != null)
    {
        return .{
            .encoder = try allocator.dupe(u8, manifest.visual_model_path.?),
            .decoder = try allocator.dupe(u8, model_dir),
        };
    }

    return error.EncoderModelNotFound;
}

fn manifestHasNativeAssets(manifest: manifest_mod.ModelManifest) bool {
    return manifest.safetensors_path != null or manifest.safetensors_index_path != null or manifest.gguf_path != null;
}

/// Parse DecoderConfig from config.json in a model directory.
pub fn loadDecoderConfig(allocator: std.mem.Allocator, model_dir: []const u8) !DecoderConfig {
    const path = try std.fmt.allocPrint(allocator, "{s}/config.json", .{model_dir});
    defer allocator.free(path);

    const data = try c_file.readFile(allocator, path);
    defer allocator.free(data);

    var config = DecoderConfig{};

    if (jsonGetInt(data, "decoder_layers")) |v| config.num_layers = @intCast(v);
    if (jsonGetInt(data, "num_decoder_layers")) |v| config.num_layers = @intCast(v);
    if (jsonGetInt(data, "num_layers")) |v| {
        if (config.num_layers == 6) config.num_layers = @intCast(v);
    }

    if (jsonGetInt(data, "decoder_attention_heads")) |v| config.num_heads = @intCast(v);
    if (jsonGetInt(data, "num_heads")) |v| {
        if (config.num_heads == 8) config.num_heads = @intCast(v);
    }

    if (jsonGetInt(data, "d_kv")) |v| {
        config.head_dim = @intCast(v);
    } else {
        var hidden: usize = 768;
        if (jsonGetInt(data, "d_model")) |v| hidden = @intCast(v);
        if (jsonGetInt(data, "hidden_size")) |v| hidden = @intCast(v);
        if (config.num_heads > 0) config.head_dim = hidden / config.num_heads;
    }

    if (jsonGetInt(data, "vocab_size")) |v| config.vocab_size = @intCast(v);
    if (jsonGetInt(data, "decoder_start_token_id")) |v| config.decoder_start_token_id = @intCast(v);
    if (jsonGetInt(data, "eos_token_id")) |v| config.eos_token_id = @intCast(v);
    if (jsonGetInt(data, "pad_token_id")) |v| config.pad_token_id = @intCast(v);
    if (jsonGetInt(data, "forced_bos_token_id")) |v| config.forced_bos_token_id = @intCast(v);
    if (jsonGetInt(data, "no_repeat_ngram_size")) |v| config.no_repeat_ngram_size = @intCast(v);

    if (jsonGetInt(data, "max_length")) |v| config.max_length = @intCast(v);
    if (jsonGetInt(data, "max_position_embeddings")) |v| {
        if (config.max_length == 512) config.max_length = @intCast(v);
    }

    return config;
}

/// Find the first existing file from a list of candidates in a directory.
fn findModelFile(allocator: std.mem.Allocator, model_dir: []const u8, candidates: []const []const u8) !?[]const u8 {
    // Search in model_dir directly and in onnx/ subdirectory
    const search_dirs = [_][]const u8{ "", "onnx" };
    for (search_dirs) |subdir| {
        const base = if (subdir.len > 0)
            try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, subdir })
        else
            try allocator.dupe(u8, model_dir);
        defer allocator.free(base);

        for (candidates) |name| {
            const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base, name });
            const path_z = try allocator.dupeZ(u8, path);
            defer allocator.free(path_z);
            if (c_file.fileExistsZ(path_z)) {
                return path;
            }
            allocator.free(path);
        }
    }
    return null;
}

/// Extract an integer value from JSON by key name (simple substring search).
fn jsonGetInt(data: []const u8, key: []const u8) ?i64 {
    var i: usize = 0;
    while (i + key.len + 4 < data.len) : (i += 1) {
        if (data[i] == '"' and i + 1 + key.len < data.len and
            std.mem.eql(u8, data[i + 1 .. i + 1 + key.len], key) and
            data[i + 1 + key.len] == '"')
        {
            var j = i + 2 + key.len;
            while (j < data.len and (data[j] == ' ' or data[j] == ':' or data[j] == '\t' or data[j] == '\n')) : (j += 1) {}

            if (j < data.len) {
                var neg = false;
                if (data[j] == '-') {
                    neg = true;
                    j += 1;
                }
                if (j < data.len and data[j] >= '0' and data[j] <= '9') {
                    var val: i64 = 0;
                    while (j < data.len and data[j] >= '0' and data[j] <= '9') : (j += 1) {
                        val = val * 10 + @as(i64, data[j] - '0');
                    }
                    return if (neg) -val else val;
                }
            }
        }
    }
    return null;
}

test "jsonGetInt" {
    const data = "{\"vocab_size\": 32128, \"num_layers\": 6, \"eos_token_id\": 1}";
    try std.testing.expectEqual(@as(?i64, 32128), jsonGetInt(data, "vocab_size"));
    try std.testing.expectEqual(@as(?i64, 6), jsonGetInt(data, "num_layers"));
    try std.testing.expectEqual(@as(?i64, 1), jsonGetInt(data, "eos_token_id"));
    try std.testing.expectEqual(@as(?i64, null), jsonGetInt(data, "nonexistent"));
}

test "findEncoderDecoderPaths falls back to native Florence decoder" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"florence2","text_config":{"d_model":1024,"decoder_layers":12,"decoder_attention_heads":16,"decoder_ffn_dim":4096,"vocab_size":51289}}
        ,
    });
    try tmp.dir.writeFile(io, .{ .sub_path = "model.safetensors", .data = "" });
    try tmp.dir.createDirPath(io, "onnx");
    try tmp.dir.writeFile(io, .{ .sub_path = "onnx/vision_encoder.onnx", .data = "" });

    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    const paths = try findEncoderDecoderPaths(allocator, model_dir);
    defer allocator.free(paths.encoder);
    defer allocator.free(paths.decoder);

    try std.testing.expect(std.mem.endsWith(u8, paths.encoder, "onnx/vision_encoder.onnx"));
    try std.testing.expectEqualStrings(model_dir, paths.decoder);
}
