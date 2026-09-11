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
// Matches Go inference's lib/pipelines/encoder_decoder.go pattern.

const std = @import("std");
const backends = @import("../backends/backends.zig");
const c_file = @import("../util/c_file.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const manifest_mod = @import("../models/manifest.zig");

/// Configuration parsed from config.json for the decoder architecture.
pub const DecoderConfig = struct {
    hidden_size: ?usize = null,
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
    /// Cached composite runtimes lend sessions for a handle-scoped invocation.
    owns_sessions: bool = true,
    batch_dispatch: ?@import("../server/tensor_microbatch.zig").Dispatch = null,
    allocator: std.mem.Allocator,
    encoder: backends.Session,
    decoder: backends.Session,
    config: DecoderConfig,
    execution_control: ?InferenceExecutionControl = null,

    /// Plan before allocating a window. Include both stages, retained encoder
    /// output and the full-prefix decoder's worst step. This conservative bound
    /// also covers workers arriving at different stages and unfused fallbacks.
    /// Admission at execution remains authoritative under concurrent pressure.
    pub fn fitsWindow(self: *const EncoderDecoderPipeline, count: usize, width: usize, preprocess_bytes: usize) !bool {
        const session_mod = @import("../backends/session.zig");
        const memory = @import("../runtime/tier/memory.zig");
        if (count <= 1) return true; // Preserve the existing singleton policy.
        const enc = self.encoder.run_admission orelse return self.decoder.run_admission == null;
        const dec = self.decoder.run_admission orelse return false;
        // Independently owned resource domains need explicit multi-domain
        // scheduling, not an invented credit between unrelated controllers.
        if (enc.controller != dec.controller or enc.backend_class != dec.backend_class) return false;
        const mul = std.math.mul;
        const add = std.math.add;
        const input_shape = [_]i64{ @intCast(count), std.math.cast(i64, width) orelse return error.ResourceLimitExceeded };
        var enc_request = try self.encoder.planShapes(&.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &input_shape },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &input_shape },
        }, count);
        const hidden_bytes = enc_request.output_bytes orelse try session_mod.estimatedOutputBytes(&input_shape, self.encoder.outputInfo());
        const hidden_width = (if (enc_request.output_bytes != null) hidden_bytes -| 24 else hidden_bytes) / count / width / 4;
        if (hidden_width == 0) return false;
        const hidden_shape = [_]i64{ @intCast(count), @intCast(width), @intCast(hidden_width) };
        const decoder_shape = [_]i64{ @intCast(count), std.math.cast(i64, self.config.max_length) orelse return error.ResourceLimitExceeded };
        var dec_request = try self.decoder.planShapes(&.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &decoder_shape },
            .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &hidden_shape },
            .{ .name = "encoder_attention_mask", .dtype = .i64, .shape = &input_shape },
        }, count);
        enc_request.host_preprocess_bytes = enc_request.input_bytes;
        dec_request.host_preprocess_bytes = dec_request.input_bytes;
        const enc_peak = try enc.estimateRequest(enc_request, self.encoder.outputInfo());
        const incremental = @import("seq2seq_decode.zig");
        const cache_bytes = if (incremental.qualified(self.decoder)) try incremental.cacheBound(self.decoder, count, self.config.max_length, width) else 0;
        if (cache_bytes > 0) {
            dec_request.input_bytes = try add(usize, dec_request.input_bytes, cache_bytes);
            dec_request.output_kv_bytes = cache_bytes;
            dec_request.output_bytes = try add(usize, cache_bytes, try mul(usize, count, try add(usize, try mul(usize, self.config.vocab_size, 4), 24)));
        }
        var dec_peak = try dec.estimateRequest(dec_request, self.decoder.outputInfo());
        // Previous cache and replacement coexist. Classify retained input K/V
        // against the KV ceiling without counting it twice against host memory.
        dec_peak.host_scratch_bytes -= cache_bytes;
        dec_peak.host_kv_bytes = try add(usize, dec_peak.host_kv_bytes, cache_bytes);
        // The stage gate covers admission and packing as well as execution:
        // at most one physical workspace per session can be resident. Different
        // encoder/decoder gates may overlap, so retain both physical peaks.
        const peak = try (memory.AdmissionAmounts{ .host_scratch_bytes = preprocess_bytes }).merge(try enc_peak.merge(dec_peak));
        return try peak.fitsLimits(enc.limits) and try peak.fitsLimits(dec.limits);
    }

    /// Run the encoder on input_ids, returning hidden state tensors.
    pub fn encode(self: *EncoderDecoderPipeline, allocator: std.mem.Allocator, input_ids: []const i64, seq_len: usize) ![]backends.Tensor {
        const mask = try allocator.alloc(i64, seq_len);
        defer allocator.free(mask);
        @memset(mask, 1);
        return self.encodeMasked(allocator, input_ids, mask);
    }

    /// Caller-owned masks allow bounded request windows to pad compatible
    /// encoder stages without changing the meaning of individual inputs.
    pub fn encodeMasked(self: *EncoderDecoderPipeline, allocator: std.mem.Allocator, input_ids: []const i64, mask: []const i64) ![]backends.Tensor {
        if (input_ids.len == 0 or input_ids.len != mask.len) return error.InvalidInputShape;
        const shape = &[_]i64{ 1, @intCast(input_ids.len) };

        var input_ids_tensor = try backends.Tensor.initInt64(allocator, "input_ids", shape, input_ids);
        defer input_ids_tensor.deinit();

        var mask_tensor = try backends.Tensor.initInt64(allocator, "attention_mask", shape, mask);
        defer mask_tensor.deinit();

        if (self.execution_control) |control| try control.update(.executing, 0, 1);
        if (self.batch_dispatch) |dispatch| return dispatch.run(allocator, self.encoder, null, null, &.{ input_ids_tensor, mask_tensor }, self.execution_control);
        return try self.encoder.runWithControl(&.{ input_ids_tensor, mask_tensor }, allocator, self.execution_control);
    }

    /// Run one decoder step, selecting directly from borrowed backend logits.
    fn decoderStep(
        self: *EncoderDecoderPipeline,
        allocator: std.mem.Allocator,
        dec_ids: []const i64,
        dec_seq_len: usize,
        encoder_hidden: *const backends.Tensor,
        encoder_attention_mask: []const i64,
        encoder_seq_len: usize,
    ) !i32 {
        const dec_seq: i64 = @intCast(dec_seq_len);
        const dec_shape = &[_]i64{ 1, dec_seq };

        const dec_input_ids = backends.Tensor{ .data = @constCast(std.mem.sliceAsBytes(dec_ids)), .dtype = .i64, .shape = dec_shape, .name = "input_ids", .allocator = allocator, .owns_data = false, .owns_shape = false };

        const enc_seq: i64 = @intCast(encoder_seq_len);
        const enc_mask_shape = &[_]i64{ 1, enc_seq };
        const enc_mask_tensor = backends.Tensor{ .data = @constCast(std.mem.sliceAsBytes(encoder_attention_mask)), .dtype = .i64, .shape = enc_mask_shape, .name = "encoder_attention_mask", .allocator = allocator, .owns_data = false, .owns_shape = false };

        // Rename encoder hidden state for decoder input compatibility
        // (encoder outputs "last_hidden_state", decoder expects "encoder_hidden_states")
        const enc_hidden_renamed = encoder_hidden.borrowedView("encoder_hidden_states");

        const inputs = &[_]backends.Tensor{
            dec_input_ids,
            enc_mask_tensor,
            enc_hidden_renamed,
        };
        var decoder_outputs = if (self.batch_dispatch) |dispatch|
            try dispatch.run(allocator, self.decoder, null, null, inputs, self.execution_control)
        else
            try self.decoder.runWithControl(inputs, allocator, self.execution_control);
        defer {
            for (decoder_outputs) |*o| o.deinit();
            allocator.free(decoder_outputs);
        }

        if (decoder_outputs.len == 0) return error.NoDecoderOutput;
        const logits_tensor = &decoder_outputs[0];
        if (logits_tensor.dtype != .f32) return error.LogitsSizeMismatch;
        const logits = logits_tensor.asFloat32();

        // Extract logits for the last token position
        const vocab_size = self.config.vocab_size;
        if (vocab_size == 0 or dec_seq_len == 0) return error.LogitsSizeMismatch;
        const end = std.math.mul(usize, dec_seq_len, vocab_size) catch return error.LogitsSizeMismatch;
        if (logits.len < end) return error.LogitsSizeMismatch;
        const last = logits[end - vocab_size .. end];
        var best: usize = 0;
        for (last[1..], 1..) |value, index| if (value > last[best]) {
            best = index;
        };
        return std.math.cast(i32, best) orelse error.LogitsSizeMismatch;
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
        const dec_ids_i64 = try allocator.alloc(i64, std.math.add(usize, max_len, 1) catch return error.ResourceLimitExceeded);
        defer allocator.free(dec_ids_i64);
        dec_ids_i64[0] = self.config.decoder_start_token_id;

        if (encoder_outputs.len == 0) return error.NoEncoderOutput;
        const encoder_hidden = &encoder_outputs[0];
        var incremental = @import("seq2seq_decode.zig").State.init(allocator, self.decoder, encoder_hidden.*, encoder_attention_mask, self.config.vocab_size);
        defer if (incremental) |*state| state.deinit();

        for (0..max_len) |step| {
            if (self.execution_control) |control|
                try control.update(.executing, @intCast(step), @intCast(max_len));
            const dec_seq_len: usize = output_ids.items.len;

            // Run one decoder step
            const next_token = if (incremental) |*state| blk: {
                var logits = try state.step(dec_ids_i64[0..dec_seq_len], self.batch_dispatch, self.execution_control);
                defer logits.deinit();
                const values = logits.asFloat32();
                const last = values[values.len - self.config.vocab_size ..];
                var best: usize = 0;
                for (last[1..], 1..) |value, index| if (value > last[best]) {
                    best = index;
                };
                break :blk std.math.cast(i32, best) orelse return error.LogitsSizeMismatch;
            } else try self.decoderStep(
                allocator,
                dec_ids_i64[0..dec_seq_len],
                dec_seq_len,
                encoder_hidden,
                encoder_attention_mask,
                encoder_seq_len,
            );
            if (next_token == self.config.eos_token_id) break;
            dec_ids_i64[dec_seq_len] = next_token;
            try output_ids.append(allocator, next_token);
        }

        return .{
            .text_ids = try allocator.dupe(i32, output_ids.items),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EncoderDecoderPipeline) void {
        if (!self.owns_sessions) return;
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

/// Discovery only. The runtime qualifies the loaded graph before choosing it;
/// filename presence is not a cache capability declaration.
pub fn findMergedDecoder(allocator: std.mem.Allocator, model_dir: []const u8) !?[]const u8 {
    return findModelFile(allocator, model_dir, &.{"decoder_model_merged.onnx"});
}

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
pub const EncoderDecoderPaths = struct { encoder: []const u8, decoder: []const u8 };

pub fn findEncoderDecoderPaths(allocator: std.mem.Allocator, model_dir: []const u8) !EncoderDecoderPaths {
    const encoder_path = try findModelFile(allocator, model_dir, encoder_candidates);
    const decoder_path = findModelFile(allocator, model_dir, decoder_candidates) catch |err| {
        if (encoder_path) |path| allocator.free(path);
        return err;
    };

    if (encoder_path != null and decoder_path != null) {
        return .{ .encoder = encoder_path.?, .decoder = decoder_path.? };
    }
    if (encoder_path) |path| allocator.free(path);
    if (decoder_path) |path| allocator.free(path);

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    return nativeFlorenceEncoderDecoderPaths(allocator, model_dir, manifest);
}

/// The Florence fallback of `findEncoderDecoderPaths`, against a manifest the caller
/// already has.
///
/// Every model without ONNX encoder/decoder files reaches that fallback, and it ends in a
/// full `manifest.loadFromDir`. For a GGUF model that means parsing the whole tokenizer
/// metadata table just to answer "is this a Florence bundle?", which is why listing every
/// GGUF model cost about a second each.
pub fn nativeFlorenceEncoderDecoderPaths(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
) !EncoderDecoderPaths {
    if (manifest.native_arch_hint == .florence and
        manifestHasNativeAssets(manifest) and
        manifest.visual_model_path != null)
    {
        const encoder = try allocator.dupe(u8, manifest.visual_model_path.?);
        errdefer allocator.free(encoder);
        return .{
            .encoder = encoder,
            .decoder = try allocator.dupe(u8, model_dir),
        };
    }

    return error.EncoderModelNotFound;
}

/// Probe paths without loading model weights or tokenizer metadata.
pub fn hasEncoderDecoderPaths(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
) !bool {
    const encoder_path = try findModelFile(allocator, model_dir, encoder_candidates);
    defer if (encoder_path) |path| allocator.free(path);
    const decoder_path = try findModelFile(allocator, model_dir, decoder_candidates);
    defer if (decoder_path) |path| allocator.free(path);
    if (encoder_path != null and decoder_path != null) return true;

    return manifest.native_arch_hint == .florence and
        manifestHasNativeAssets(manifest) and manifest.visual_model_path != null;
}

fn manifestHasNativeAssets(manifest: manifest_mod.ModelManifest) bool {
    return manifest.safetensors_path != null or manifest.safetensors_index_path != null or manifest.gguf_path != null;
}

/// Parse DecoderConfig from config.json in a model directory.
pub fn loadDecoderConfig(allocator: std.mem.Allocator, model_dir: []const u8) !DecoderConfig {
    const path = try std.fmt.allocPrint(allocator, "{s}/config.json", .{model_dir});
    defer allocator.free(path);

    return loadDecoderConfigFile(allocator, path);
}

/// Parse decoder settings from an already resolved config artifact. Managed
/// serving passes the receipt-validated canonical path through this API.
pub fn loadDecoderConfigFile(allocator: std.mem.Allocator, path: []const u8) !DecoderConfig {
    const data = try c_file.readFile(allocator, path);
    defer allocator.free(data);

    var config = DecoderConfig{};
    const hidden_width = jsonGetInt(data, "d_model") orelse jsonGetInt(data, "hidden_size");
    if (hidden_width) |value| {
        if (value <= 0) return error.InvalidDecoderConfig;
        config.hidden_size = std.math.cast(usize, value) orelse return error.InvalidDecoderConfig;
    }

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
            errdefer allocator.free(path);
            const path_z = try allocator.dupeZ(u8, path);
            defer allocator.free(path_z);
            if (try c_file.fileExistsZChecked(path_z)) {
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
