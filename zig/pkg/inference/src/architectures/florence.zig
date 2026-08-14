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

// Native Florence-2 architecture support.
//
// Implements the official safetensors layout:
// - `vision_tower.*` DaViT vision encoder
// - `image_projection` + `image_proj_norm`
// - `language_model.model.encoder.*`
// - `language_model.model.decoder.*`
//
// The vision tower is hybrid: conv/window/channel pieces run in Zig over f32
// buffers, while dense linear, attention, GELU, and LayerNorm use the active
// ComputeBackend so the same code works for native .

const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const florence_config = @import("../models/florence.zig");

pub const Config = florence_config.Config;
const ImageFeatureSource = florence_config.ImageFeatureSource;

const bart_position_offset: i64 = 2;

pub const EncoderForwardResult = struct {
    hidden: []f32,
    seq_len: usize,
};

pub const EncoderForwardTensorResult = struct {
    hidden: CT,
    seq_len: usize,
};

pub const DecoderCrossCache = struct {
    keys: []CT,
    values: []CT,

    pub fn deinit(self: *DecoderCrossCache, cb: *const ComputeBackend, allocator: std.mem.Allocator) void {
        for (self.keys) |key| cb.free(key);
        for (self.values) |value| cb.free(value);
        allocator.free(self.keys);
        allocator.free(self.values);
    }
};

pub const DecoderSelfCache = struct {
    keys: []?CT,
    values: []?CT,
    len: usize = 0,
    capacity: usize = 0,
    preallocated: bool = false,

    pub fn init(allocator: std.mem.Allocator, layers: usize) !DecoderSelfCache {
        const keys = try allocator.alloc(?CT, layers);
        errdefer allocator.free(keys);
        const values = try allocator.alloc(?CT, layers);
        errdefer allocator.free(values);
        @memset(keys, null);
        @memset(values, null);
        return .{ .keys = keys, .values = values };
    }

    pub fn tryInitPreallocated(
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        layers: usize,
        batch: usize,
        capacity: usize,
        d_model: usize,
    ) !?DecoderSelfCache {
        if (batch == 0 or capacity == 0) return null;
        const keys = try allocator.alloc(?CT, layers);
        errdefer allocator.free(keys);
        const values = try allocator.alloc(?CT, layers);
        errdefer allocator.free(values);
        @memset(keys, null);
        @memset(values, null);

        var cache = DecoderSelfCache{
            .keys = keys,
            .values = values,
            .capacity = capacity,
            .preallocated = true,
        };
        errdefer cache.deinit(cb, allocator);

        const rows = std.math.mul(usize, batch, capacity) catch return error.InvalidInputShape;
        if (rows > @as(usize, @intCast(std.math.maxInt(i32))) or d_model > @as(usize, @intCast(std.math.maxInt(i32)))) return error.InvalidInputShape;
        const shape = [_]i32{ @intCast(rows), @intCast(d_model) };
        for (0..layers) |layer| {
            cache.keys[layer] = (try cb.allocUninitF32Shape(&shape)) orelse {
                cache.deinit(cb, allocator);
                return null;
            };
            cache.values[layer] = (try cb.allocUninitF32Shape(&shape)) orelse {
                cache.deinit(cb, allocator);
                return null;
            };
        }
        return cache;
    }

    pub fn deinit(self: *DecoderSelfCache, cb: *const ComputeBackend, allocator: std.mem.Allocator) void {
        for (self.keys) |maybe_key| if (maybe_key) |key| cb.free(key);
        for (self.values) |maybe_value| if (maybe_value) |value| cb.free(value);
        allocator.free(self.keys);
        allocator.free(self.values);
    }
};

pub const DecoderIncrementalCache = struct {
    cross: DecoderCrossCache,
    self: DecoderSelfCache,
    encoder_mask: []i64,
    self_mask: []i64,
    batch: usize,
    enc_seq: usize,

    pub fn deinit(self: *DecoderIncrementalCache, cb: *const ComputeBackend, allocator: std.mem.Allocator) void {
        self.cross.deinit(cb, allocator);
        self.self.deinit(cb, allocator);
        allocator.free(self.encoder_mask);
        allocator.free(self.self_mask);
    }
};

const VisionForwardResult = struct {
    features: []f32,
    seq_len: usize,
};

const VisionForwardTensorResult = struct {
    features: CT,
    seq_len: usize,
};

pub fn encoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !EncoderForwardResult {
    const tensor = try encoderForwardTensor(cb, allocator, config, pixel_values, batch, prompt_input_ids, prompt_seq_len);
    defer cb.free(tensor.hidden);
    const profile = readProfileEnabled();
    const download_start = nowNs();
    const result = try cb.toFloat32(tensor.hidden, allocator);
    if (profile) logFlorenceProfile("encoder_output_download", download_start);
    return .{ .hidden = result, .seq_len = tensor.seq_len };
}

pub fn encoderForwardTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !EncoderForwardTensorResult {
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    if (debug_cuda_session) std.log.info("florence: encoder forward start batch={d} prompt_seq={d}", .{ batch, prompt_seq_len });
    const vision_start = nowNs();
    if (try visionEncoderForwardTensorTail(cb, allocator, config, pixel_values, batch)) |image_tensor| {
        if (profile) logFlorenceProfile("vision_total", vision_start);
        if (debug_cuda_session) std.log.info("florence: device vision encoder done seq={d}", .{image_tensor.seq_len});
        defer cb.free(image_tensor.features);
        if (debugStatsEnabled()) logDebugStatsTensor(cb, allocator, "vision_features(device)", image_tensor.features);

        const total_seq = image_tensor.seq_len + prompt_seq_len;
        const embeddings_start = nowNs();
        const hidden = try applyEncoderImagePromptEmbeddingsTensor(cb, allocator, image_tensor, config, batch, prompt_input_ids, prompt_seq_len);
        if (profile) logFlorenceProfile("encoder_embeddings", embeddings_start);
        if (debug_cuda_session) std.log.info("florence: encoder embeddings done total_seq={d}", .{total_seq});
        if (debugStatsEnabled()) logDebugStatsTensor(cb, allocator, "post_embeddings(device)", hidden);

        return runTextEncoderLayersTensor(cb, allocator, config, hidden, batch, total_seq);
    }

    const image = try visionEncoderForward(cb, allocator, config, pixel_values, batch);
    if (profile) logFlorenceProfile("vision_total", vision_start);
    if (debug_cuda_session) std.log.info("florence: vision encoder done seq={d}", .{image.seq_len});
    defer allocator.free(image.features);
    if (debugStatsEnabled()) logDebugStatsF32("vision_features(host)", image.features);

    const d_model: usize = config.d_model;
    const total_seq = image.seq_len + prompt_seq_len;
    const total_tokens = batch * total_seq;

    const prompt_embeddings = if (prompt_seq_len > 0)
        try tokenEmbeddingsData(cb, allocator, try promptEmbedWeight(cb), prompt_input_ids, batch * prompt_seq_len, d_model)
    else
        &[_]f32{};
    defer if (prompt_seq_len > 0) allocator.free(prompt_embeddings);

    const merged = try allocator.alloc(f32, total_tokens * d_model);
    errdefer allocator.free(merged);

    for (0..batch) |b| {
        const dst = b * total_seq * d_model;
        const image_src = b * image.seq_len * d_model;
        @memcpy(merged[dst..][0 .. image.seq_len * d_model], image.features[image_src..][0 .. image.seq_len * d_model]);
        if (prompt_seq_len > 0) {
            const prompt_src = b * prompt_seq_len * d_model;
            @memcpy(
                merged[dst + image.seq_len * d_model ..][0 .. prompt_seq_len * d_model],
                prompt_embeddings[prompt_src..][0 .. prompt_seq_len * d_model],
            );
        }
    }

    const embeddings_start = nowNs();
    const hidden = try applyEncoderEmbeddings(cb, allocator, merged, config, batch, total_seq);
    if (profile) logFlorenceProfile("encoder_embeddings", embeddings_start);
    if (debug_cuda_session) std.log.info("florence: encoder embeddings done total_seq={d}", .{total_seq});
    if (debugStatsEnabled()) logDebugStatsTensor(cb, allocator, "post_embeddings(host)", hidden);
    allocator.free(merged);

    return runTextEncoderLayersTensor(cb, allocator, config, hidden, batch, total_seq);
}

fn runTextEncoderLayersTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_hidden: CT,
    batch: usize,
    total_seq: usize,
) !EncoderForwardTensorResult {
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    const total_tokens = batch * total_seq;
    var hidden = input_hidden;
    var hidden_owned = true;
    errdefer if (hidden_owned) cb.free(hidden);

    const attn_mask = try allocator.alloc(i64, total_tokens);
    defer allocator.free(attn_mask);
    @memset(attn_mask, 1);

    var frame_active = try beginFlorenceMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};

    var buf: [256]u8 = undefined;
    for (0..config.encoder_layers) |layer| {
        if (debug_cuda_session) std.log.info("florence: text encoder layer start {d}", .{layer});
        const layer_start = nowNs();
        const next = try encoderBlock(cb, config, hidden, attn_mask, batch, total_seq, layer, &buf);
        if (profile) logFlorenceProfileStep("text_encoder_layer", layer, layer_start);
        cb.free(hidden);
        hidden = next;
        if (debugStatsEnabled()) {
            var label_buf: [64]u8 = undefined;
            const label = std.fmt.bufPrint(&label_buf, "text_encoder_layer_{d}", .{layer}) catch "text_encoder_layer";
            logDebugStatsTensor(cb, allocator, label, hidden);
        }
        if (debug_cuda_session) std.log.info("florence: text encoder layer done {d}", .{layer});
    }

    if (frame_active) {
        const submit_start = nowNs();
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
        if (profile) logFlorenceProfile("text_encoder_frame_submit", submit_start);
    }

    hidden_owned = false;
    return .{ .hidden = hidden, .seq_len = total_seq };
}

/// Run the Florence/BART text encoder without any vision inputs.
pub fn textEncoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const merged = try tokenEmbeddingsData(cb, allocator, try promptEmbedWeight(cb), input_ids, total, d_model);
    defer allocator.free(merged);

    var hidden = try applyEncoderEmbeddings(cb, allocator, merged, config, batch, seq_len);

    const attn_mask = try allocator.alloc(i64, total);
    defer allocator.free(attn_mask);
    @memset(attn_mask, 1);

    var buf: [256]u8 = undefined;
    for (0..config.encoder_layers) |layer| {
        const next = try encoderBlock(cb, config, hidden, attn_mask, batch, seq_len, layer, &buf);
        cb.free(hidden);
        hidden = next;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

/// Run the Florence-2 decoder in hidden-state mode.
pub fn decoderHiddenForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    var hidden = try applyDecoderEmbeddings(cb, allocator, config, input_ids, batch, seq_len);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        const new_hidden = try decoderBlockSelfOnly(cb, config, hidden, batch, seq_len, layer, &buf);
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

/// Run the Florence-2 decoder with cross-attention to encoder hidden states.
pub fn decoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) ![]f32 {
    const logits = try decoderForwardTensor(
        cb,
        allocator,
        config,
        decoder_input_ids,
        encoder_hidden,
        encoder_mask,
        batch,
        dec_seq,
        enc_seq,
    );
    defer cb.free(logits);

    return try cb.toFloat32(logits, allocator);
}

pub fn decoderForwardTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) !CT {
    const dec_total = batch * dec_seq;
    var hidden = try applyDecoderEmbeddings(cb, allocator, config, decoder_input_ids, batch, dec_seq);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        const new_hidden = try decoderBlock(cb, config, hidden, encoder_hidden, encoder_mask, batch, dec_seq, enc_seq, layer, &buf);
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    const lm_w = try lmHeadWeight(cb);
    var logits = try cb.linearNoBias(hidden, lm_w, dec_total, config.d_model, config.vocab_size);
    cb.free(hidden);
    errdefer cb.free(logits);

    if (try tryOptionalWeight(cb, "language_model.final_logits_bias")) |logits_bias| {
        const bias = try logitsBiasSlice(cb, allocator, logits_bias, config.vocab_size);
        defer if (bias.owned) cb.free(bias.tensor);

        if (try cb.addBiasRowsConsume(logits, bias.tensor, dec_total, config.vocab_size)) |biased| {
            logits = biased;
        } else {
            const biased = try cb.add(logits, bias.tensor);
            cb.free(logits);
            logits = biased;
        }
    }
    return logits;
}

pub fn buildDecoderIncrementalCache(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    encoder_hidden: CT,
    batch: usize,
    enc_seq: usize,
    max_decode_len: usize,
) !DecoderIncrementalCache {
    var cross = try buildDecoderCrossCache(cb, allocator, config, encoder_hidden, batch, enc_seq);
    errdefer cross.deinit(cb, allocator);
    var self_cache = if (try DecoderSelfCache.tryInitPreallocated(cb, allocator, config.decoder_layers, batch, max_decode_len, config.d_model)) |preallocated|
        preallocated
    else
        try DecoderSelfCache.init(allocator, config.decoder_layers);
    errdefer self_cache.deinit(cb, allocator);
    const encoder_mask_len = std.math.mul(usize, batch, enc_seq) catch return error.InvalidInputShape;
    const self_mask_len = std.math.mul(usize, batch, max_decode_len) catch return error.InvalidInputShape;
    const encoder_mask = try allocator.alloc(i64, encoder_mask_len);
    errdefer allocator.free(encoder_mask);
    @memset(encoder_mask, 1);
    const self_mask = try allocator.alloc(i64, self_mask_len);
    @memset(self_mask, 1);
    return .{ .cross = cross, .self = self_cache, .encoder_mask = encoder_mask, .self_mask = self_mask, .batch = batch, .enc_seq = enc_seq };
}

pub fn decoderForwardIncrementalStepTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_id: i64,
    cache: *DecoderIncrementalCache,
) !CT {
    const hidden = try decoderForwardIncrementalStepFinalHiddenTensor(cb, allocator, config, token_id, cache);
    errdefer cb.free(hidden);
    return try decoderLmHeadLogitsRowsFromFinalHiddenTensor(cb, allocator, config, hidden, 1);
}

pub fn decoderForwardIncrementalStepFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_id: i64,
    cache: *DecoderIncrementalCache,
) !CT {
    const ids = [_]i64{token_id};
    return decoderForwardIncrementalBatchStepFinalHiddenTensor(cb, allocator, config, &ids, cache);
}

pub fn decoderForwardIncrementalBatchStepFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_ids: []const i64,
    cache: *DecoderIncrementalCache,
) !CT {
    if (cache.cross.keys.len != config.decoder_layers or cache.cross.values.len != config.decoder_layers) return error.InvalidInputShape;
    if (cache.self.keys.len != config.decoder_layers or cache.self.values.len != config.decoder_layers) return error.InvalidInputShape;

    const batch = cache.batch;
    if (token_ids.len != batch) return error.InvalidInputShape;
    const old_len = cache.self.len;
    const new_len = old_len + 1;
    if (cache.self.preallocated and new_len > cache.self.capacity) return error.InvalidInputShape;
    const active_mask_len = std.math.mul(usize, batch, new_len) catch return error.InvalidInputShape;
    if (active_mask_len > cache.self_mask.len) return error.InvalidInputShape;

    var frame_active = try beginFlorenceMetalFrame(cb, .decode);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    var hidden = try applyDecoderEmbeddingsAt(cb, allocator, config, token_ids, batch, 1, old_len);
    var hidden_live = true;
    errdefer if (hidden_live) cb.free(hidden);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        const new_hidden = try decoderBlockIncrementalCached(
            cb,
            allocator,
            config,
            hidden,
            cache.self_mask[0..active_mask_len],
            batch,
            old_len,
            new_len,
            layer,
            cache,
            &buf,
        );
        cb.free(hidden);
        hidden = new_hidden;
    }
    cache.self.len = new_len;

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }

    hidden_live = false;
    return hidden;
}

pub fn decoderLmHeadLogitsFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
) !CT {
    return decoderLmHeadLogitsRowsFromFinalHiddenTensor(cb, allocator, config, hidden, 1);
}

pub fn decoderLmHeadLogitsRowsFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    rows: usize,
) !CT {
    const lm_w = try lmHeadWeight(cb);
    var logits = try cb.linearNoBias(hidden, lm_w, rows, config.d_model, config.vocab_size);
    errdefer cb.free(logits);

    if (try tryOptionalWeight(cb, "language_model.final_logits_bias")) |logits_bias| {
        const bias = try logitsBiasSlice(cb, allocator, logits_bias, config.vocab_size);
        defer if (bias.owned) cb.free(bias.tensor);

        if (try cb.addBiasRowsConsume(logits, bias.tensor, rows, config.vocab_size)) |biased| {
            logits = biased;
        } else {
            const biased = try cb.add(logits, bias.tensor);
            cb.free(logits);
            logits = biased;
        }
    }
    cb.free(hidden);
    return logits;
}

pub fn decoderFusedTokenFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    suppress_tokens: []const i32,
) !?CT {
    return try cb.linearNoBiasArgmaxLastRowSuppressTensor(hidden, try lmHeadWeight(cb), 1, config.d_model, config.vocab_size, suppress_tokens);
}

pub fn decoderFinalLogitsBiasIsZero(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    vocab_size: usize,
) !bool {
    return finalLogitsBiasIsZero(cb, allocator, vocab_size);
}

const LogitsBiasSlice = struct {
    tensor: CT,
    owned: bool,
};

fn logitsBiasSlice(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    logits_bias: CT,
    vocab_size: usize,
) !LogitsBiasSlice {
    const shape = try cb.tensorShape(logits_bias, allocator);
    defer allocator.free(shape);

    var element_count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return error.InvalidTensorShape;
        element_count *= @intCast(dim);
    }
    if (element_count < vocab_size) return error.InvalidTensorShape;
    if (element_count == vocab_size) {
        if (shape.len == 1 and @as(usize, @intCast(shape[0])) == vocab_size) {
            return .{ .tensor = logits_bias, .owned = false };
        }
        if (shape.len == 2 and shape[0] == 1 and @as(usize, @intCast(shape[1])) == vocab_size) {
            return .{ .tensor = logits_bias, .owned = false };
        }
        const row_shape = [_]i64{@intCast(vocab_size)};
        return .{ .tensor = try cb.primReshape(logits_bias, &row_shape), .owned = true };
    }

    const start: usize = if (element_count == vocab_size + 1) 1 else 0;
    const flat_shape = [_]i64{ 1, @intCast(element_count) };
    const flat_bias = try cb.primReshape(logits_bias, &flat_shape);
    defer cb.free(flat_bias);
    return .{
        .tensor = try cb.sliceLastDim(flat_bias, start, start + vocab_size),
        .owned = true,
    };
}

fn finalLogitsBiasIsZero(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    vocab_size: usize,
) !bool {
    const logits_bias = (try tryOptionalWeight(cb, "language_model.final_logits_bias")) orelse return true;
    const bias = try logitsBiasSlice(cb, allocator, logits_bias, vocab_size);
    defer if (bias.owned) cb.free(bias.tensor);
    const values = try cb.toFloat32(bias.tensor, allocator);
    defer allocator.free(values);
    if (values.len != vocab_size) return error.InvalidTensorShape;
    for (values) |value| {
        if (value != 0.0) return false;
    }
    return true;
}

pub fn buildDecoderCrossCache(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    encoder_hidden: CT,
    batch: usize,
    enc_seq: usize,
) !DecoderCrossCache {
    const keys = try allocator.alloc(CT, config.decoder_layers);
    errdefer allocator.free(keys);
    const values = try allocator.alloc(CT, config.decoder_layers);
    errdefer allocator.free(values);

    var filled: usize = 0;
    errdefer {
        for (keys[0..filled]) |key| cb.free(key);
        for (values[0..filled]) |value| cb.free(value);
    }

    const d_model = config.d_model;
    const enc_total = batch * enc_seq;
    var frame_active = try beginFlorenceMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        const k_weights = try decoderLinearWeights(cb, layer, "encoder_attn.k_proj", &buf);
        const v_weights = try decoderLinearWeights(cb, layer, "encoder_attn.v_proj", &buf);
        const kv = try cb.linearPair(encoder_hidden, k_weights.weight, k_weights.bias, v_weights.weight, v_weights.bias, enc_total, d_model, d_model);
        keys[layer] = kv.first;
        values[layer] = kv.second;
        filled += 1;
    }

    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }

    return .{ .keys = keys, .values = values };
}

pub fn decoderForwardCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    cross_cache: *const DecoderCrossCache,
) ![]f32 {
    if (cross_cache.keys.len != config.decoder_layers or cross_cache.values.len != config.decoder_layers) return error.InvalidInputShape;

    const dec_total = batch * dec_seq;
    var hidden = try applyDecoderEmbeddings(cb, allocator, config, decoder_input_ids, batch, dec_seq);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        const new_hidden = try decoderBlockWithCrossCache(
            cb,
            config,
            hidden,
            cross_cache.keys[layer],
            cross_cache.values[layer],
            encoder_mask,
            batch,
            dec_seq,
            enc_seq,
            layer,
            &buf,
        );
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    const lm_w = try lmHeadWeight(cb);
    const logits = try cb.linearNoBias(hidden, lm_w, dec_total, config.d_model, config.vocab_size);
    cb.free(hidden);

    const result = try cb.toFloat32(logits, allocator);
    cb.free(logits);
    if (try tryOptionalWeight(cb, "language_model.final_logits_bias")) |logits_bias| {
        const bias = try cb.toFloat32(logits_bias, allocator);
        defer allocator.free(bias);
        const bias_offset: usize = if (bias.len == config.vocab_size) 0 else if (bias.len == config.vocab_size + 1) 1 else 0;
        const bias_slice = bias[bias_offset..][0..config.vocab_size];
        for (0..dec_total) |row| {
            const row_slice = result[row * config.vocab_size ..][0..config.vocab_size];
            for (row_slice, bias_slice) |*value, bias_value| value.* += bias_value;
        }
    }
    return result;
}

fn visionEncoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
) !VisionForwardResult {
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    if (debug_cuda_session) std.log.info("florence: vision start backend={s}", .{@tagName(cb.kind())});
    var stage_tokens: ?[]f32 = null;
    errdefer if (stage_tokens) |tokens| allocator.free(tokens);
    var stage_hidden: ?CT = null;
    errdefer if (stage_hidden) |hidden| cb.free(hidden);
    var stage_h: usize = config.image_size;
    var stage_w: usize = config.image_size;

    for (0..Config.stage_count) |stage| {
        const stage_start = nowNs();
        var hidden_for_stage: ?CT = null;
        if (useTensorNativeVision(cb) and stage_hidden != null) {
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbedTensor(cb, allocator, config, stage, batch, stage_hidden.?, stage_h, stage_w);
            cb.free(stage_hidden.?);
            stage_hidden = null;
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            hidden_for_stage = embedded.tensor;
            stage_h = embedded.height;
            stage_w = embedded.width;
        } else {
            if (debug_cuda_session) std.log.info("florence: stage {d} conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbed(cb, allocator, config, stage, batch, pixel_values, stage_tokens, stage_h, stage_w);
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            if (stage_tokens) |old| allocator.free(old);
            stage_tokens = embedded.tokens;
            stage_h = embedded.height;
            stage_w = embedded.width;
        }

        const depth: usize = config.depths[stage];
        if (useTensorNativeVision(cb)) {
            var hidden = if (hidden_for_stage) |tensor| tensor else blk: {
                if (debug_cuda_session) std.log.info("florence: stage {d} tensor native upload start depth={d}", .{ stage, depth });
                const upload_start = nowNs();
                const stage_shape = [_]i32{ @intCast(batch * stage_h * stage_w), @intCast(config.dim_embed[stage]) };
                const tensor = try cb.fromFloat32Shape(stage_tokens.?, &stage_shape);
                if (profile) logFlorenceProfileStage("vision_stage_upload", stage, upload_start);
                allocator.free(stage_tokens.?);
                stage_tokens = null;
                break :blk tensor;
            };
            var hidden_owned = true;
            errdefer if (hidden_owned) cb.free(hidden);

            var stage_frame_active = if (florenceMetalStageFramesEnabled())
                try beginFlorenceMetalFrame(cb, .prefill)
            else
                false;
            errdefer if (stage_frame_active) cb.decoderRuntimeCancelFrame() catch {};

            for (0..depth) |layer| {
                if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} start", .{ stage, layer });
                const layer_start = nowNs();
                const next = try daViTBlockFramed(cb, allocator, config, hidden, batch, stage_h, stage_w, stage, layer);
                if (profile) logFlorenceProfileStageLayer("vision_stage_davit_layer", stage, layer, layer_start);
                hidden = next;
                if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} done", .{ stage, layer });
            }
            if (stage_frame_active) {
                const submit_start = nowNs();
                try cb.decoderRuntimeSubmitAndWaitFrame();
                stage_frame_active = false;
                if (profile) logFlorenceProfileStage("vision_stage_frame_submit", stage, submit_start);
            }

            if (stage + 1 == Config.stage_count) {
                if (debug_cuda_session) std.log.info("florence: stage {d} download start", .{stage});
                const download_start = nowNs();
                stage_tokens = try cb.toFloat32(hidden, allocator);
                cb.free(hidden);
                hidden_owned = false;
                if (profile) logFlorenceProfileStage("vision_stage_download", stage, download_start);
                if (debug_cuda_session) std.log.info("florence: stage {d} download done", .{stage});
            } else {
                stage_hidden = hidden;
                hidden_owned = false;
            }
        } else {
            for (0..depth) |layer| {
                stage_tokens = try daViTBlockData(cb, allocator, config, stage_tokens.?, batch, stage_h, stage_w, stage, layer);
            }
        }
        if (profile) logFlorenceProfileStage("vision_stage_total", stage, stage_start);
    }

    if (debug_cuda_session) std.log.info("florence: vision stages complete", .{});
    const tokens = stage_tokens orelse return error.MissingInputs;
    const vision_dim: usize = config.dim_embed[Config.stage_count - 1];
    const token_count = stage_h * stage_w;

    if (debug_cuda_session) std.log.info("florence: add 2d pos start token_count={d} dim={d}", .{ token_count, vision_dim });
    const pos_start = nowNs();
    try add2dPositionalEmbedding(cb, allocator, tokens, batch, token_count, vision_dim, stage_h, stage_w);
    if (profile) logFlorenceProfile("vision_add_2d_pos", pos_start);
    if (debug_cuda_session) std.log.info("florence: add 2d pos done", .{});
    if (debug_cuda_session) std.log.info("florence: add temporal start", .{});
    const temporal_start = nowNs();
    try addTemporalEmbedding(cb, allocator, tokens, batch, token_count, vision_dim);
    if (profile) logFlorenceProfile("vision_add_temporal", temporal_start);
    if (debug_cuda_session) std.log.info("florence: add temporal done", .{});

    if (debug_cuda_session) std.log.info("florence: feature source concat start", .{});
    const concat_start = nowNs();
    const sourced = try imageFeatureSourceConcat(allocator, config, tokens, batch, token_count, vision_dim);
    if (profile) logFlorenceProfile("vision_feature_source_concat", concat_start);
    if (debug_cuda_session) std.log.info("florence: feature source concat done seq={d}", .{sourced.seq_len});
    defer allocator.free(sourced.data);
    allocator.free(tokens);

    const proj_rows = batch * sourced.seq_len;
    if (debug_cuda_session) std.log.info("florence: projection linear start rows={d}", .{proj_rows});
    const proj_start = nowNs();
    const projected = try projectImageFeaturesData(cb, allocator, sourced.data, proj_rows, vision_dim, config);
    if (profile) logFlorenceProfile("vision_projection_linear", proj_start);
    if (debug_cuda_session) std.log.info("florence: projection linear done", .{});
    defer allocator.free(projected);

    if (debug_cuda_session) std.log.info("florence: image proj norm start", .{});
    const norm_start = nowNs();
    const normed = try backendLayerNormData(
        cb,
        allocator,
        projected,
        proj_rows,
        config.projection_dim,
        try cb.getWeight("image_proj_norm.weight"),
        try cb.getWeight("image_proj_norm.bias"),
        1e-5,
    );
    if (profile) logFlorenceProfile("vision_image_proj_norm", norm_start);
    if (debug_cuda_session) std.log.info("florence: image proj norm done", .{});

    return .{ .features = normed, .seq_len = sourced.seq_len };
}

fn visionEncoderForwardTensorTail(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
) !?VisionForwardTensorResult {
    if (!useDeviceVisionTail(cb, config, batch)) return null;

    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    if (debug_cuda_session) std.log.info("florence: device vision tail start backend={s}", .{@tagName(cb.kind())});
    var stage_tokens: ?[]f32 = null;
    errdefer if (stage_tokens) |tokens| allocator.free(tokens);
    var stage_hidden: ?CT = null;
    errdefer if (stage_hidden) |hidden| cb.free(hidden);
    var stage_h: usize = config.image_size;
    var stage_w: usize = config.image_size;

    for (0..Config.stage_count) |stage| {
        const stage_start = nowNs();
        var hidden_for_stage: ?CT = null;
        if (stage_hidden != null) {
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbedTensor(cb, allocator, config, stage, batch, stage_hidden.?, stage_h, stage_w);
            cb.free(stage_hidden.?);
            stage_hidden = null;
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            hidden_for_stage = embedded.tensor;
            stage_h = embedded.height;
            stage_w = embedded.width;
        } else {
            if (debug_cuda_session) std.log.info("florence: stage {d} conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbed(cb, allocator, config, stage, batch, pixel_values, stage_tokens, stage_h, stage_w);
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            if (stage_tokens) |old| allocator.free(old);
            stage_tokens = embedded.tokens;
            stage_h = embedded.height;
            stage_w = embedded.width;
        }

        const depth: usize = config.depths[stage];
        var hidden = if (hidden_for_stage) |tensor| tensor else blk: {
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor upload start depth={d}", .{ stage, depth });
            const upload_start = nowNs();
            const stage_shape = [_]i32{ @intCast(batch * stage_h * stage_w), @intCast(config.dim_embed[stage]) };
            const tensor = try cb.fromFloat32Shape(stage_tokens.?, &stage_shape);
            if (profile) logFlorenceProfileStage("vision_stage_upload", stage, upload_start);
            allocator.free(stage_tokens.?);
            stage_tokens = null;
            break :blk tensor;
        };
        var hidden_owned = true;
        errdefer if (hidden_owned) cb.free(hidden);

        var stage_frame_active = if (florenceMetalStageFramesEnabled())
            try beginFlorenceMetalFrame(cb, .prefill)
        else
            false;
        errdefer if (stage_frame_active) cb.decoderRuntimeCancelFrame() catch {};

        for (0..depth) |layer| {
            if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} start", .{ stage, layer });
            const layer_start = nowNs();
            const next = try daViTBlockFramed(cb, allocator, config, hidden, batch, stage_h, stage_w, stage, layer);
            if (profile) logFlorenceProfileStageLayer("vision_stage_davit_layer", stage, layer, layer_start);
            hidden = next;
            if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} done", .{ stage, layer });
        }
        if (stage_frame_active) {
            const submit_start = nowNs();
            try cb.decoderRuntimeSubmitAndWaitFrame();
            stage_frame_active = false;
            if (profile) logFlorenceProfileStage("vision_stage_frame_submit", stage, submit_start);
        }

        if (stage + 1 == Config.stage_count) {
            const result = (try finishVisionTailTensor(cb, allocator, config, hidden, batch, stage_h, stage_w)) orelse {
                cb.free(hidden);
                hidden_owned = false;
                return null;
            };
            cb.free(hidden);
            hidden_owned = false;
            if (profile) logFlorenceProfileStage("vision_stage_total", stage, stage_start);
            return result;
        }

        stage_hidden = hidden;
        hidden_owned = false;
        if (profile) logFlorenceProfileStage("vision_stage_total", stage, stage_start);
    }

    return null;
}

fn useDeviceVisionTail(cb: *const ComputeBackend, config: Config, batch: usize) bool {
    return cb.kind() == .cuda and
        batch == 1 and
        !florenceDeviceVisionTailDisabled() and
        config.image_feature_source_count == 2 and
        config.image_feature_sources[0] == .spatial_avg_pool and
        config.image_feature_sources[1] == .temporal_avg_pool;
}

fn finishVisionTailTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    tokens: CT,
    batch: usize,
    height: usize,
    width: usize,
) !?VisionForwardTensorResult {
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    const vision_dim: usize = config.dim_embed[Config.stage_count - 1];
    const token_count = height * width;

    if (debug_cuda_session) std.log.info("florence: device feature source tail start token_count={d} dim={d}", .{ token_count, vision_dim });
    const concat_start = nowNs();
    const temporal_w = tryOptionalWeight(cb, "visual_temporal_embed.pos_idx_to_embed") catch null;

    const sourced = (try cb.florenceVisionTailSources(
        tokens,
        try cb.getWeight("image_pos_embed.row_embeddings.weight"),
        try cb.getWeight("image_pos_embed.column_embeddings.weight"),
        temporal_w,
        batch,
        height,
        width,
        vision_dim,
    )) orelse return null;
    var sourced_owned = true;
    errdefer if (sourced_owned) cb.free(sourced);
    if (profile) logFlorenceProfile("vision_device_tail_sources", concat_start);
    if (debug_cuda_session) std.log.info("florence: device feature source tail done seq={d}", .{token_count + 1});

    const proj_rows = batch * (token_count + 1);
    if (debug_cuda_session) std.log.info("florence: projection linear start rows={d}", .{proj_rows});
    const proj_start = nowNs();
    const projected = try projectImageFeaturesTensor(cb, allocator, sourced, proj_rows, vision_dim, config);
    cb.free(sourced);
    sourced_owned = false;
    var projected_owned = true;
    errdefer if (projected_owned) cb.free(projected);
    if (profile) logFlorenceProfile("vision_projection_linear", proj_start);
    if (debug_cuda_session) std.log.info("florence: projection linear done", .{});

    if (debug_cuda_session) std.log.info("florence: image proj norm start", .{});
    const norm_start = nowNs();
    const normed = try cb.layerNorm(
        projected,
        try cb.getWeight("image_proj_norm.weight"),
        try cb.getWeight("image_proj_norm.bias"),
        config.projection_dim,
        1e-5,
    );
    cb.free(projected);
    projected_owned = false;
    if (profile) logFlorenceProfile("vision_image_proj_norm", norm_start);
    if (debug_cuda_session) std.log.info("florence: image proj norm done", .{});

    return .{ .features = normed, .seq_len = token_count + 1 };
}

fn projectImageFeaturesTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    rows: usize,
    vision_dim: usize,
    config: Config,
) !CT {
    const proj_weight = try cb.getWeight("image_projection");
    if (try cb.florenceProjectImageFeatures(input, proj_weight, rows, vision_dim, config.projection_dim)) |projected| {
        return projected;
    }

    const input_shape = [_]i64{ @intCast(vision_dim), @intCast(config.projection_dim) };
    const proj_ct = cb.primTranspose(proj_weight, &.{ 1, 0 }, &input_shape) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.InvalidShape, error.UnsupportedTensorType => {
            const proj_weight_data = try cb.toFloat32(proj_weight, allocator);
            defer allocator.free(proj_weight_data);
            const proj_weight_t = try transposeMatrix(allocator, proj_weight_data, vision_dim, config.projection_dim);
            defer allocator.free(proj_weight_t);
            const proj_shape = [_]i32{ @intCast(config.projection_dim), @intCast(vision_dim) };
            const fallback_proj_ct = try cb.fromFloat32Shape(proj_weight_t, &proj_shape);
            defer cb.free(fallback_proj_ct);
            return try cb.linearNoBias(input, fallback_proj_ct, rows, vision_dim, config.projection_dim);
        },
        else => return err,
    };
    defer cb.free(proj_ct);
    return try cb.linearNoBias(input, proj_ct, rows, vision_dim, config.projection_dim);
}

fn useTensorNativeVision(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal or cb.kind() == .cuda;
}

fn florenceDeviceVisionTailDisabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_DISABLE_DEVICE_VISION_TAIL");
}

fn daViTBlockData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    var hidden = input;
    hidden = try spatialBlockData(cb, allocator, config, hidden, batch, height, width, stage, layer);
    hidden = try channelBlockData(cb, allocator, config, hidden, batch, height, width, stage, layer);
    return hidden;
}

fn spatialBlockData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const dim: usize = config.dim_embed[stage];
    var hidden = input;

    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv1");
    hidden = try residualWindowAttentionData(cb, allocator, hidden, batch, height, width, dim, config.num_heads[stage], config.window_size, stage, layer);
    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv2");
    hidden = try residualMlpData(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "spatial_block.ffn");
    return hidden;
}

fn channelBlockData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const dim: usize = config.dim_embed[stage];
    var hidden = input;

    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv1");
    hidden = try residualChannelAttentionData(cb, allocator, hidden, batch, height * width, dim, config.num_groups[stage], stage, layer);
    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv2");
    hidden = try residualMlpData(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "channel_block.ffn");
    return hidden;
}

fn residualDepthwiseConvData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) ![]f32 {
    const weight_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.weight", .{ stage, layer, block_prefix });
    defer allocator.free(weight_name);
    const bias_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.bias", .{ stage, layer, block_prefix });
    defer allocator.free(bias_name);

    const input_shape = [_]i32{ @intCast(batch * height * width), @intCast(channels) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const updates_ct = try cb.tokenGridConv2d(
        input_ct,
        try cb.getWeight(weight_name),
        try cb.getWeight(bias_name),
        batch,
        channels,
        channels,
        height,
        width,
        3,
        3,
        1,
        1,
        1,
        1,
        channels,
    );
    defer cb.free(updates_ct);
    const updates = try cb.toFloat32(updates_ct, allocator);
    defer allocator.free(updates);

    addInPlace(input, updates);
    return input;
}

fn residualWindowAttentionData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const input_shape = [_]i32{ @intCast(batch * height * width), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const out_ct = try cb.windowedSelfAttention(
        input_ct,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        height,
        width,
        dim,
        num_heads,
        window_size,
    );
    defer cb.free(out_ct);
    const out = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(out);
    addInPlace(input, out);
    return input;
}

fn residualChannelAttentionData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const input_shape = [_]i32{ @intCast(batch * seq_len), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const out_ct = try cb.channelSelfAttention(
        input_ct,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        seq_len,
        dim,
        groups,
    );
    defer cb.free(out_ct);
    const out = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(out);
    addInPlace(input, out);
    return input;
}

fn residualMlpData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    hidden_dim: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) ![]f32 {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.weight", .{ stage, layer, block_prefix });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.bias", .{ stage, layer, block_prefix });
    defer allocator.free(norm_b_name);
    const fc1_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_w_name);
    const fc1_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_b_name);
    const fc2_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_w_name);
    const fc2_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_b_name);

    const updates = try backendNormedGeluMlpData(
        cb,
        allocator,
        input,
        batch * seq_len,
        dim,
        hidden_dim,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(fc1_w_name),
        try cb.getWeight(fc1_b_name),
        try cb.getWeight(fc2_w_name),
        try cb.getWeight(fc2_b_name),
    );
    defer allocator.free(updates);
    addInPlace(input, updates);
    return input;
}

const ConvEmbedResult = struct {
    tokens: []f32,
    height: usize,
    width: usize,
};

const ConvEmbedTensorResult = struct {
    tensor: CT,
    height: usize,
    width: usize,
};

fn convEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    stage: usize,
    batch: usize,
    pixel_values: []const f32,
    input_tokens: ?[]const f32,
    input_h: usize,
    input_w: usize,
) !ConvEmbedResult {
    const in_channels: usize = if (stage == 0) 3 else config.dim_embed[stage - 1];
    const out_channels: usize = config.dim_embed[stage];
    const patch: usize = config.patch_size[stage];
    const stride: usize = config.patch_stride[stage];
    const padding: usize = config.patch_padding[stage];

    const proj_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.weight", .{stage});
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.bias", .{stage});
    defer allocator.free(proj_b_name);
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.weight", .{stage});
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.bias", .{stage});
    defer allocator.free(norm_b_name);

    const norm_w = try cb.getWeight(norm_w_name);
    const norm_b = try cb.getWeight(norm_b_name);

    const proj_w = try cb.getWeight(proj_w_name);
    const proj_b = try cb.getWeight(proj_b_name);

    var tokens = if (stage == 0) blk: {
        const conv = try backendConv2dImageData(
            cb,
            allocator,
            pixel_values,
            batch,
            in_channels,
            input_h,
            input_w,
            out_channels,
            patch,
            patch,
            stride,
            stride,
            padding,
            padding,
            1,
            proj_w,
            proj_b,
        );
        defer allocator.free(conv.data);
        break :blk try imageToTokens(allocator, conv.data, batch, conv.height, conv.width, out_channels);
    } else blk: {
        const input = input_tokens orelse return error.MissingInputs;
        const prepped = if (config.patch_prenorm[stage])
            try backendLayerNormData(cb, allocator, input, batch * input_h * input_w, in_channels, norm_w, norm_b, 1e-5)
        else
            try allocator.dupe(f32, input);
        defer allocator.free(prepped);
        const prepped_shape = [_]i32{ @intCast(batch * input_h * input_w), @intCast(in_channels) };
        const prepped_ct = try cb.fromFloat32Shape(prepped, &prepped_shape);
        defer cb.free(prepped_ct);
        const tokens_ct = try cb.tokenGridConv2d(
            prepped_ct,
            proj_w,
            proj_b,
            batch,
            in_channels,
            out_channels,
            input_h,
            input_w,
            patch,
            patch,
            stride,
            stride,
            padding,
            padding,
            1,
        );
        defer cb.free(tokens_ct);
        break :blk try cb.toFloat32(tokens_ct, allocator);
    };

    const out_h = (input_h + 2 * padding - patch) / stride + 1;
    const out_w = (input_w + 2 * padding - patch) / stride + 1;
    if (!config.patch_prenorm[stage]) {
        const normed = try backendLayerNormData(cb, allocator, tokens, batch * out_h * out_w, out_channels, norm_w, norm_b, 1e-5);
        allocator.free(tokens);
        tokens = normed;
    }

    return .{ .tokens = tokens, .height = out_h, .width = out_w };
}

fn convEmbedTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    stage: usize,
    batch: usize,
    input: CT,
    input_h: usize,
    input_w: usize,
) !ConvEmbedTensorResult {
    if (stage == 0) return error.InvalidStage;
    const in_channels: usize = config.dim_embed[stage - 1];
    const out_channels: usize = config.dim_embed[stage];
    const patch: usize = config.patch_size[stage];
    const stride: usize = config.patch_stride[stage];
    const padding: usize = config.patch_padding[stage];

    const proj_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.weight", .{stage});
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.bias", .{stage});
    defer allocator.free(proj_b_name);
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.weight", .{stage});
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.bias", .{stage});
    defer allocator.free(norm_b_name);

    const norm_w = try cb.getWeight(norm_w_name);
    const norm_b = try cb.getWeight(norm_b_name);
    const proj_w = try cb.getWeight(proj_w_name);
    const proj_b = try cb.getWeight(proj_b_name);

    const conv_input = if (config.patch_prenorm[stage])
        try cb.layerNorm(input, norm_w, norm_b, in_channels, 1e-5)
    else
        input;
    const owns_conv_input = config.patch_prenorm[stage];
    defer if (owns_conv_input) cb.free(conv_input);

    var tokens = try cb.tokenGridConv2d(
        conv_input,
        proj_w,
        proj_b,
        batch,
        in_channels,
        out_channels,
        input_h,
        input_w,
        patch,
        patch,
        stride,
        stride,
        padding,
        padding,
        1,
    );
    errdefer cb.free(tokens);

    const out_h = (input_h + 2 * padding - patch) / stride + 1;
    const out_w = (input_w + 2 * padding - patch) / stride + 1;
    if (!config.patch_prenorm[stage]) {
        const normed = try cb.layerNorm(tokens, norm_w, norm_b, out_channels, 1e-5);
        cb.free(tokens);
        tokens = normed;
    }

    return .{ .tensor = tokens, .height = out_h, .width = out_w };
}

fn daViTBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    var hidden = input;
    var next = try spatialBlock(cb, allocator, config, hidden, batch, height, width, stage, layer);
    hidden = next;
    next = try channelBlock(cb, allocator, config, hidden, batch, height, width, stage, layer);
    hidden = next;
    return hidden;
}

fn daViTBlockFramed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    var frame_active = try beginFlorenceMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};

    const output = try daViTBlock(cb, allocator, config, input, batch, height, width, stage, layer);
    errdefer cb.free(output);
    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }
    return output;
}

fn spatialBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    const profile = readProfileEnabled();
    const dim: usize = config.dim_embed[stage];
    var hidden = input;
    var op_start = nowNs();
    var next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv1");
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_conv1", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_conv1_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualWindowAttention(cb, allocator, hidden, batch, height, width, dim, config.num_heads[stage], config.window_size, stage, layer);
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_window_attn", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_window_attn_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv2");
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_conv2", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_conv2_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualMlp(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "spatial_block.ffn");
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_mlp", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_mlp_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    return hidden;
}

fn channelBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    const profile = readProfileEnabled();
    const dim: usize = config.dim_embed[stage];
    var hidden = input;
    var op_start = nowNs();
    var next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv1");
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_conv1", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_conv1_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualChannelAttention(cb, allocator, hidden, batch, height * width, dim, config.num_groups[stage], stage, layer);
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_attn", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_attn_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv2");
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_conv2", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_conv2_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualMlp(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "channel_block.ffn");
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_mlp", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_mlp_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    return hidden;
}

fn residualDepthwiseConv(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) !CT {
    const weight_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.weight", .{ stage, layer, block_prefix });
    defer allocator.free(weight_name);
    const bias_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.bias", .{ stage, layer, block_prefix });
    defer allocator.free(bias_name);

    const updates_ct = try cb.tokenGridConv2d(
        input,
        try cb.getWeight(weight_name),
        try cb.getWeight(bias_name),
        batch,
        channels,
        channels,
        height,
        width,
        3,
        3,
        1,
        1,
        1,
        1,
        channels,
    );
    defer cb.free(updates_ct);
    return cb.add(input, updates_ct);
}

fn residualWindowAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
    stage: usize,
    layer: usize,
) !CT {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const out_ct = try cb.windowedSelfAttention(
        input,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        height,
        width,
        dim,
        num_heads,
        window_size,
    );
    defer cb.free(out_ct);
    return cb.add(input, out_ct);
}

fn residualChannelAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
    stage: usize,
    layer: usize,
) !CT {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const out_ct = try cb.channelSelfAttention(
        input,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        seq_len,
        dim,
        groups,
    );
    defer cb.free(out_ct);
    return cb.add(input, out_ct);
}

fn residualMlp(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    dim: usize,
    hidden_dim: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) !CT {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.weight", .{ stage, layer, block_prefix });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.bias", .{ stage, layer, block_prefix });
    defer allocator.free(norm_b_name);
    const fc1_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_w_name);
    const fc1_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_b_name);
    const fc2_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_w_name);
    const fc2_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_b_name);

    const rows = batch * seq_len;
    const normed_ct = try cb.layerNorm(input, try cb.getWeight(norm_w_name), try cb.getWeight(norm_b_name), dim, 1e-5);
    defer cb.free(normed_ct);

    const fc1_w = try cb.getWeight(fc1_w_name);
    const fc1_b = try cb.getWeight(fc1_b_name);
    const activated_ct = if (try cb.linearGelu(normed_ct, fc1_w, fc1_b, rows, dim, hidden_dim)) |fused|
        fused
    else blk: {
        const fc1_ct = try cb.linear(normed_ct, fc1_w, fc1_b, rows, dim, hidden_dim);
        defer cb.free(fc1_ct);
        break :blk try cb.gelu(fc1_ct);
    };
    defer cb.free(activated_ct);

    const fc2_w = try cb.getWeight(fc2_w_name);
    const fc2_b = try cb.getWeight(fc2_b_name);
    if (try cb.linearAdd(activated_ct, fc2_w, fc2_b, input, rows, hidden_dim, dim)) |fused| {
        return fused;
    }

    const fc2_ct = try cb.linear(activated_ct, fc2_w, fc2_b, rows, hidden_dim, dim);
    defer cb.free(fc2_ct);
    return cb.add(input, fc2_ct);
}

const PaddedWindows = struct {
    data: []f32,
    padded_h: usize,
    padded_w: usize,
    window_count: usize,
    window_area: usize,
};

fn padTokensToWindow(
    allocator: std.mem.Allocator,
    tokens: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    window_size: usize,
) !PaddedWindows {
    const pad_w = (window_size - (width % window_size)) % window_size;
    const pad_h = (window_size - (height % window_size)) % window_size;
    const padded_h = height + pad_h;
    const padded_w = width + pad_w;
    const windows_h = padded_h / window_size;
    const windows_w = padded_w / window_size;
    const window_count = batch * windows_h * windows_w;
    const window_area = window_size * window_size;

    const data = try allocator.alloc(f32, window_count * window_area * dim);
    @memset(data, 0.0);

    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                const window_index = ((b * windows_h) + wh) * windows_w + ww;
                for (0..window_size) |dy| {
                    for (0..window_size) |dx| {
                        const src_y = wh * window_size + dy;
                        const src_x = ww * window_size + dx;
                        if (src_y >= height or src_x >= width) continue;
                        const src_token = (b * height * width + src_y * width + src_x) * dim;
                        const dst_token = (window_index * window_area + dy * window_size + dx) * dim;
                        @memcpy(data[dst_token..][0..dim], tokens[src_token..][0..dim]);
                    }
                }
            }
        }
    }

    return .{
        .data = data,
        .padded_h = padded_h,
        .padded_w = padded_w,
        .window_count = window_count,
        .window_area = window_area,
    };
}

fn unpadWindowTokens(
    allocator: std.mem.Allocator,
    window_tokens: []const f32,
    padded: PaddedWindows,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
) ![]f32 {
    const window_size = std.math.sqrt(padded.window_area);
    const windows_h = padded.padded_h / window_size;
    const windows_w = padded.padded_w / window_size;
    const result = try allocator.alloc(f32, batch * height * width * dim);
    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                const window_index = ((b * windows_h) + wh) * windows_w + ww;
                for (0..window_size) |dy| {
                    for (0..window_size) |dx| {
                        const dst_y = wh * window_size + dy;
                        const dst_x = ww * window_size + dx;
                        if (dst_y >= height or dst_x >= width) continue;
                        const src_token = (window_index * padded.window_area + dy * window_size + dx) * dim;
                        const dst_token = (b * height * width + dst_y * width + dst_x) * dim;
                        @memcpy(result[dst_token..][0..dim], window_tokens[src_token..][0..dim]);
                    }
                }
            }
        }
    }
    return result;
}

fn add2dPositionalEmbedding(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tokens: []f32,
    batch: usize,
    token_count: usize,
    dim: usize,
    height: usize,
    width: usize,
) !void {
    const row_w = try cb.getWeight("image_pos_embed.row_embeddings.weight");
    const col_w = try cb.getWeight("image_pos_embed.column_embeddings.weight");
    const row = try cb.toFloat32(row_w, allocator);
    defer allocator.free(row);
    const col = try cb.toFloat32(col_w, allocator);
    defer allocator.free(col);

    const row_dim = dim / 2;
    const col_dim = dim - row_dim;
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const token = (b * token_count + y * width + x) * dim;
                for (0..col_dim) |i| tokens[token + i] += col[x * col_dim + i];
                for (0..row_dim) |i| tokens[token + col_dim + i] += row[y * row_dim + i];
            }
        }
    }
}

fn addTemporalEmbedding(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tokens: []f32,
    batch: usize,
    token_count: usize,
    dim: usize,
) !void {
    const temporal_w = tryOptionalWeight(cb, "visual_temporal_embed.pos_idx_to_embed") catch null;
    const weight = temporal_w orelse return;
    const temporal = try cb.toFloat32(weight, allocator);
    defer allocator.free(temporal);
    for (0..batch) |b| {
        for (0..token_count) |t| {
            const dst = (b * token_count + t) * dim;
            for (0..dim) |i| tokens[dst + i] += temporal[i];
        }
    }
}

const SourcedFeatures = struct {
    data: []f32,
    seq_len: usize,
};

fn imageFeatureSourceConcat(
    allocator: std.mem.Allocator,
    config: Config,
    tokens: []const f32,
    batch: usize,
    token_count: usize,
    dim: usize,
) !SourcedFeatures {
    var seq_len: usize = 0;
    for (0..config.image_feature_source_count) |idx| {
        seq_len += switch (config.image_feature_sources[idx]) {
            .spatial_avg_pool => 1,
            .temporal_avg_pool, .last_frame => token_count,
        };
    }

    const out = try allocator.alloc(f32, batch * seq_len * dim);
    var cursor: usize = 0;
    for (0..config.image_feature_source_count) |idx| {
        switch (config.image_feature_sources[idx]) {
            .spatial_avg_pool => {
                for (0..batch) |b| {
                    const dst = (b * seq_len + cursor) * dim;
                    @memset(out[dst..][0..dim], 0.0);
                    for (0..token_count) |t| {
                        const src = (b * token_count + t) * dim;
                        for (0..dim) |i| out[dst + i] += tokens[src + i];
                    }
                    const scale = 1.0 / @as(f32, @floatFromInt(token_count));
                    for (0..dim) |i| out[dst + i] *= scale;
                }
                cursor += 1;
            },
            .temporal_avg_pool, .last_frame => {
                for (0..batch) |b| {
                    const dst = (b * seq_len + cursor) * dim;
                    const src = b * token_count * dim;
                    @memcpy(out[dst..][0 .. token_count * dim], tokens[src..][0 .. token_count * dim]);
                }
                cursor += token_count;
            },
        }
    }
    return .{ .data = out, .seq_len = seq_len };
}

fn applyEncoderEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    merged: []const f32,
    config: Config,
    batch: usize,
    seq_len: usize,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const shape = [_]i32{ @intCast(total), @intCast(d_model) };
    var hidden = try cb.fromFloat32Shape(merged, &shape);

    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(s + bart_position_offset);
    }

    const pos_emb = try cb.embeddingLookup(try cb.getWeight("language_model.model.encoder.embed_positions.weight"), pos_ids, total, d_model);
    defer cb.free(pos_emb);
    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    const ln_w = try cb.getWeight("language_model.model.encoder.layernorm_embedding.weight");
    const ln_b = try cb.getWeight("language_model.model.encoder.layernorm_embedding.bias");
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);
    return normed;
}

fn applyEncoderImagePromptEmbeddingsTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: VisionForwardTensorResult,
    config: Config,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !CT {
    if (batch != 1) return error.UnsupportedShape;
    const d_model: usize = config.d_model;
    const total_seq = image.seq_len + prompt_seq_len;

    const merged = if (prompt_seq_len > 0) blk: {
        const prompt_embeddings = try cb.embeddingLookup(try promptEmbedWeight(cb), prompt_input_ids, batch * prompt_seq_len, d_model);
        defer cb.free(prompt_embeddings);
        break :blk try cb.concatRows2D(allocator, image.features, prompt_embeddings, image.seq_len, prompt_seq_len, d_model);
    } else try cb.sliceRows2D(allocator, image.features, 0, image.seq_len, d_model);

    return applyEncoderEmbeddingsTensor(cb, allocator, merged, config, batch, total_seq);
}

fn applyEncoderEmbeddingsTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input_hidden: CT,
    config: Config,
    batch: usize,
    seq_len: usize,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    var hidden = input_hidden;
    var hidden_owned = true;
    errdefer if (hidden_owned) cb.free(hidden);

    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(s + bart_position_offset);
    }

    const pos_emb = try cb.embeddingLookup(try cb.getWeight("language_model.model.encoder.embed_positions.weight"), pos_ids, total, d_model);
    defer cb.free(pos_emb);
    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    const ln_w = try cb.getWeight("language_model.model.encoder.layernorm_embedding.weight");
    const ln_b = try cb.getWeight("language_model.model.encoder.layernorm_embedding.bias");
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);
    hidden_owned = false;
    return normed;
}

fn applyDecoderEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    return applyDecoderEmbeddingsAt(cb, allocator, config, input_ids, batch, seq_len, 0);
}

fn applyDecoderEmbeddingsAt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    position_start: usize,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const embed_w = try promptEmbedWeight(cb);
    var hidden = try cb.embeddingLookup(embed_w, input_ids, total, d_model);

    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(position_start + s + bart_position_offset);
    }

    const pos_emb = try cb.embeddingLookup(try decoderPosWeight(cb), pos_ids, total, d_model);
    defer cb.free(pos_emb);
    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    const ln_w = try decoderLayerNormEmbeddingWeight(cb);
    const ln_b = try decoderLayerNormEmbeddingBias(cb);
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);
    return normed;
}

fn encoderBlock(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.encoder_attention_heads;
    const head_dim = config.encoderHeadDim();
    const ffn_dim = config.encoder_ffn_dim;
    const total = batch * seq_len;

    const profile = readProfileEnabled();

    const qkv_start = nowNs();
    const q_weights = try encoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_weights = try encoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_weights = try encoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv = try cb.linearTriple(hidden, q_weights.weight, q_weights.bias, k_weights.weight, k_weights.bias, v_weights.weight, v_weights.bias, total, d_model, d_model);
    defer cb.free(qkv.first);
    defer cb.free(qkv.second);
    defer cb.free(qkv.third);
    if (profile) logFlorenceProfileStep("enc_qkv_linear", layer, qkv_start);

    const attn_start = nowNs();
    const attn = try cb.scaledDotProductAttention(qkv.first, qkv.second, qkv.third, attention_mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);
    if (profile) logFlorenceProfileStep("enc_sdpa", layer, attn_start);
    const proj_start = nowNs();
    const proj = try encoderLinearProj(cb, attn, layer, "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(proj);
    const attn_res = try cb.add(hidden, proj);
    if (profile) logFlorenceProfileStep("enc_out_proj_add", layer, proj_start);

    const ln0_start = nowNs();
    const ln0_w = try encoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try encoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const attn_normed = try cb.layerNorm(attn_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(attn_res);
    if (profile) logFlorenceProfileStep("enc_ln0", layer, ln0_start);

    const fc1_start = nowNs();
    const fc1_weights = try encoderLinearWeights(cb, layer, "fc1", buf);
    const activated = if (try cb.linearGelu(attn_normed, fc1_weights.weight, fc1_weights.bias, total, d_model, ffn_dim)) |fused|
        fused
    else blk: {
        const fc1 = try cb.linear(attn_normed, fc1_weights.weight, fc1_weights.bias, total, d_model, ffn_dim);
        defer cb.free(fc1);
        break :blk try cb.gelu(fc1);
    };
    defer cb.free(activated);
    if (profile) logFlorenceProfileStep("enc_fc1_gelu", layer, fc1_start);

    const fc2_start = nowNs();
    const fc2_weights = try encoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, attn_normed, total, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, total, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(attn_normed, fc2);
    };
    cb.free(attn_normed);
    if (profile) logFlorenceProfileStep("enc_fc2_add", layer, fc2_start);

    const ln1_w = try encoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln1_b = try encoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(ffn_res);
    return result;
}

fn decoderBlockSelfOnly(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    batch: usize,
    seq_len: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const total = batch * seq_len;

    const q_weights = try decoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_weights = try decoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_weights = try decoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv = try cb.linearTriple(hidden, q_weights.weight, q_weights.bias, k_weights.weight, k_weights.bias, v_weights.weight, v_weights.bias, total, d_model, d_model);
    defer cb.free(qkv.first);
    defer cb.free(qkv.second);
    defer cb.free(qkv.third);
    const attn = try cb.causalSelfAttention(qkv.first, qkv.second, qkv.third, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);
    const proj = try decoderLinearProj(cb, attn, layer, "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(proj);
    const attn_res = try cb.add(hidden, proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const attn_normed = try cb.layerNorm(attn_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(attn_res);

    const fc1_weights = try decoderLinearWeights(cb, layer, "fc1", buf);
    const activated = if (try cb.linearGelu(attn_normed, fc1_weights.weight, fc1_weights.bias, total, d_model, ffn_dim)) |fused|
        fused
    else blk: {
        const fc1 = try cb.linear(attn_normed, fc1_weights.weight, fc1_weights.bias, total, d_model, ffn_dim);
        defer cb.free(fc1);
        break :blk try cb.gelu(fc1);
    };
    defer cb.free(activated);

    const fc2_weights = try decoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, attn_normed, total, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, total, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(attn_normed, fc2);
    };
    cb.free(attn_normed);

    const ln1_w = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln1_b = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(ffn_res);
    return result;
}

fn decoderBlock(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return decoderBlockWithOptionalCrossCache(cb, config, hidden, encoder_hidden, null, null, encoder_mask, batch, dec_seq, enc_seq, layer, buf);
}

fn decoderBlockWithCrossCache(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    cached_k_cross: CT,
    cached_v_cross: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return decoderBlockWithOptionalCrossCache(cb, config, hidden, null, cached_k_cross, cached_v_cross, encoder_mask, batch, dec_seq, enc_seq, layer, buf);
}

fn decoderBlockWithOptionalCrossCache(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    encoder_hidden: ?CT,
    cached_k_cross: ?CT,
    cached_v_cross: ?CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const dec_total = batch * dec_seq;
    const enc_total = batch * enc_seq;

    const q_self_weights = try decoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_self_weights = try decoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_self_weights = try decoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv_self = try cb.linearTriple(hidden, q_self_weights.weight, q_self_weights.bias, k_self_weights.weight, k_self_weights.bias, v_self_weights.weight, v_self_weights.bias, dec_total, d_model, d_model);
    defer cb.free(qkv_self.first);
    defer cb.free(qkv_self.second);
    defer cb.free(qkv_self.third);
    const self_attn = try cb.causalSelfAttention(qkv_self.first, qkv_self.second, qkv_self.third, null, batch, dec_seq, num_heads, head_dim);
    defer cb.free(self_attn);
    const self_proj = try decoderLinearProj(cb, self_attn, layer, "self_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(self_proj);
    const self_res = try cb.add(hidden, self_proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const self_normed = try cb.layerNorm(self_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(self_res);

    if ((cached_k_cross == null) != (cached_v_cross == null)) return error.InvalidInputShape;
    const q_cross = try decoderLinearProj(cb, self_normed, layer, "encoder_attn.q_proj", dec_total, d_model, d_model, buf);
    defer cb.free(q_cross);
    const owns_cross_k = cached_k_cross == null;
    const owns_cross_v = cached_v_cross == null;
    const k_cross = if (cached_k_cross) |cached| cached else blk: {
        const source = encoder_hidden orelse return error.MissingInputs;
        break :blk try decoderLinearProj(cb, source, layer, "encoder_attn.k_proj", enc_total, d_model, d_model, buf);
    };
    defer if (owns_cross_k) cb.free(k_cross);
    const v_cross = if (cached_v_cross) |cached| cached else blk: {
        const source = encoder_hidden orelse return error.MissingInputs;
        break :blk try decoderLinearProj(cb, source, layer, "encoder_attn.v_proj", enc_total, d_model, d_model, buf);
    };
    defer if (owns_cross_v) cb.free(v_cross);
    const cross_attn = try cb.crossAttention(q_cross, k_cross, v_cross, encoder_mask, batch, dec_seq, enc_seq, num_heads, head_dim);
    defer cb.free(cross_attn);
    const cross_proj = try decoderLinearProj(cb, cross_attn, layer, "encoder_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(cross_proj);
    const cross_res = try cb.add(self_normed, cross_proj);
    cb.free(self_normed);

    const ln1_w = try decoderLayerWeight(cb, layer, "encoder_attn_layer_norm.weight", buf);
    const ln1_b = try decoderLayerWeight(cb, layer, "encoder_attn_layer_norm.bias", buf);
    const cross_normed = try cb.layerNorm(cross_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(cross_res);

    const fc1_weights = try decoderLinearWeights(cb, layer, "fc1", buf);
    const activated = if (try cb.linearGelu(cross_normed, fc1_weights.weight, fc1_weights.bias, dec_total, d_model, ffn_dim)) |fused|
        fused
    else blk: {
        const fc1 = try cb.linear(cross_normed, fc1_weights.weight, fc1_weights.bias, dec_total, d_model, ffn_dim);
        defer cb.free(fc1);
        break :blk try cb.gelu(fc1);
    };
    defer cb.free(activated);

    const fc2_weights = try decoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, cross_normed, dec_total, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, dec_total, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(cross_normed, fc2);
    };
    cb.free(cross_normed);
    const ln2_w = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln2_b = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln2_w, ln2_b, d_model, 1e-5);
    cb.free(ffn_res);
    return result;
}

fn decoderBlockIncrementalCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    self_mask: []const i64,
    batch: usize,
    old_len: usize,
    new_len: usize,
    layer: usize,
    cache: *DecoderIncrementalCache,
    buf: *[256]u8,
) !CT {
    const expected_mask_len = std.math.mul(usize, batch, new_len) catch return error.InvalidInputShape;
    if (new_len != old_len + 1 or self_mask.len != expected_mask_len) return error.InvalidInputShape;

    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    var profile_stage_start = if (florenceDecoderStageFrameProfilingEnabled(cb)) nowNs() else 0;

    const q_self_weights = try decoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_self_weights = try decoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_self_weights = try decoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv_self = try cb.linearTriple(hidden, q_self_weights.weight, q_self_weights.bias, k_self_weights.weight, k_self_weights.bias, v_self_weights.weight, v_self_weights.bias, batch, d_model, d_model);
    defer cb.free(qkv_self.first);

    const cached_k_ref = try appendSelfCacheTensor(cb, allocator, &cache.self, &cache.self.keys[layer], qkv_self.second, batch, old_len, new_len, d_model);
    defer cached_k_ref.deinit(cb);
    const cached_v_ref = try appendSelfCacheTensor(cb, allocator, &cache.self, &cache.self.values[layer], qkv_self.third, batch, old_len, new_len, d_model);
    defer cached_v_ref.deinit(cb);

    const self_attn = try cb.crossAttention(qkv_self.first, cached_k_ref.tensor, cached_v_ref.tensor, self_mask, batch, 1, new_len, num_heads, head_dim);
    defer cb.free(self_attn);
    const self_proj = try decoderLinearProj(cb, self_attn, layer, "self_attn.out_proj", batch, d_model, d_model, buf);
    defer cb.free(self_proj);
    const self_res = try cb.add(hidden, self_proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const self_normed = try cb.layerNorm(self_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(self_res);
    profile_stage_start = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_self", layer, profile_stage_start);

    const q_cross = try decoderLinearProj(cb, self_normed, layer, "encoder_attn.q_proj", batch, d_model, d_model, buf);
    defer cb.free(q_cross);
    const cross_attn = try cb.crossAttention(q_cross, cache.cross.keys[layer], cache.cross.values[layer], cache.encoder_mask, batch, 1, cache.enc_seq, num_heads, head_dim);
    defer cb.free(cross_attn);
    const cross_proj = try decoderLinearProj(cb, cross_attn, layer, "encoder_attn.out_proj", batch, d_model, d_model, buf);
    defer cb.free(cross_proj);
    const cross_res = try cb.add(self_normed, cross_proj);
    cb.free(self_normed);

    const ln1_w = try decoderLayerWeight(cb, layer, "encoder_attn_layer_norm.weight", buf);
    const ln1_b = try decoderLayerWeight(cb, layer, "encoder_attn_layer_norm.bias", buf);
    const cross_normed = try cb.layerNorm(cross_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(cross_res);
    profile_stage_start = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_cross", layer, profile_stage_start);

    const fc1_weights = try decoderLinearWeights(cb, layer, "fc1", buf);
    const activated = if (try cb.linearGelu(cross_normed, fc1_weights.weight, fc1_weights.bias, batch, d_model, ffn_dim)) |fused|
        fused
    else blk: {
        const fc1 = try cb.linear(cross_normed, fc1_weights.weight, fc1_weights.bias, batch, d_model, ffn_dim);
        defer cb.free(fc1);
        break :blk try cb.gelu(fc1);
    };
    defer cb.free(activated);

    const fc2_weights = try decoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, cross_normed, batch, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, batch, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(cross_normed, fc2);
    };
    cb.free(cross_normed);

    const ln2_w = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln2_b = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln2_w, ln2_b, d_model, 1e-5);
    cb.free(ffn_res);
    _ = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_ffn", layer, profile_stage_start);
    return result;
}

const CachedSelfTensor = struct {
    tensor: CT,
    owned_view: bool = false,

    fn deinit(self: CachedSelfTensor, cb: *const ComputeBackend) void {
        if (self.owned_view) cb.free(self.tensor);
    }
};

fn appendSelfCacheTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    self_cache: *DecoderSelfCache,
    slot: *?CT,
    next: CT,
    batch: usize,
    old_len: usize,
    new_len: usize,
    d_model: usize,
) !CachedSelfTensor {
    if (batch == 0 or new_len != old_len + 1) {
        cb.free(next);
        return error.InvalidInputShape;
    }
    if (self_cache.preallocated) {
        if (new_len > self_cache.capacity) {
            cb.free(next);
            return error.InvalidInputShape;
        }
        const slab = slot.* orelse {
            cb.free(next);
            return error.InvalidInputShape;
        };
        var copied = true;
        for (0..batch) |b| {
            const dst_base = std.math.mul(usize, b, self_cache.capacity) catch {
                cb.free(next);
                return error.InvalidInputShape;
            };
            const dst_row = std.math.add(usize, dst_base, old_len) catch {
                cb.free(next);
                return error.InvalidInputShape;
            };
            copied = copied and (cb.copyRows2D(allocator, slab, dst_row, next, b, 1, d_model) catch |err| {
                cb.free(next);
                return err;
            });
        }
        cb.free(next);
        if (!copied) return error.UnsupportedOperation;
        const view = try compactSelfCacheRows(cb, allocator, slab, batch, self_cache.capacity, new_len, d_model);
        return .{ .tensor = view, .owned_view = true };
    } else if (slot.*) |old| {
        const merged = appendBatchSelfRows(cb, allocator, old, next, batch, old_len, d_model) catch |err| {
            cb.free(next);
            return err;
        };
        cb.free(old);
        cb.free(next);
        slot.* = merged;
        return .{ .tensor = merged };
    } else {
        if (old_len != 0) {
            cb.free(next);
            return error.InvalidInputShape;
        }
        slot.* = next;
        return .{ .tensor = next };
    }
}

fn appendBatchSelfRows(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    old: CT,
    next: CT,
    batch: usize,
    old_len: usize,
    d_model: usize,
) !CT {
    if (batch == 1) return cb.concatRows2D(allocator, old, next, old_len, 1, d_model);
    const new_len = old_len + 1;
    var acc: ?CT = null;
    var acc_rows: usize = 0;
    errdefer if (acc) |tensor| cb.free(tensor);

    for (0..batch) |b| {
        const old_start = std.math.mul(usize, b, old_len) catch return error.InvalidInputShape;
        const old_slice = try cb.sliceRows2D(allocator, old, old_start, old_len, d_model);
        defer cb.free(old_slice);
        const next_slice = try cb.sliceRows2D(allocator, next, b, 1, d_model);
        defer cb.free(next_slice);
        const item = try cb.concatRows2D(allocator, old_slice, next_slice, old_len, 1, d_model);
        var item_live = true;
        errdefer if (item_live) cb.free(item);

        if (acc) |current| {
            const merged = try cb.concatRows2D(allocator, current, item, acc_rows, new_len, d_model);
            cb.free(current);
            cb.free(item);
            item_live = false;
            acc = merged;
        } else {
            acc = item;
            item_live = false;
        }
        acc_rows += new_len;
    }

    const result = acc orelse return error.InvalidInputShape;
    acc = null;
    return result;
}

fn compactSelfCacheRows(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    slab: CT,
    batch: usize,
    capacity: usize,
    active_len: usize,
    d_model: usize,
) !CT {
    if (batch == 1) return cb.sliceRows2D(allocator, slab, 0, active_len, d_model);

    var acc: ?CT = null;
    var acc_rows: usize = 0;
    errdefer if (acc) |tensor| cb.free(tensor);

    for (0..batch) |b| {
        const start_row = std.math.mul(usize, b, capacity) catch return error.InvalidInputShape;
        const item = try cb.sliceRows2D(allocator, slab, start_row, active_len, d_model);
        var item_live = true;
        errdefer if (item_live) cb.free(item);
        if (acc) |current| {
            const merged = try cb.concatRows2D(allocator, current, item, acc_rows, active_len, d_model);
            cb.free(current);
            cb.free(item);
            item_live = false;
            acc = merged;
        } else {
            acc = item;
            item_live = false;
        }
        acc_rows += active_len;
    }

    const result = acc orelse return error.InvalidInputShape;
    acc = null;
    return result;
}

fn tokenEmbeddingsData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    embed_w: CT,
    input_ids: []const i64,
    total: usize,
    dim: usize,
) ![]f32 {
    const emb = try cb.embeddingLookup(embed_w, input_ids, total, dim);
    defer cb.free(emb);
    return cb.toFloat32(emb, allocator);
}

fn promptEmbedWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.shared.weight",
        "language_model.model.encoder.embed_tokens.weight",
        "language_model.model.decoder.embed_tokens.weight",
        "model.decoder.embed_tokens.weight",
    });
}

fn decoderPosWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.decoder.embed_positions.weight",
        "model.decoder.embed_positions.weight",
    });
}

fn decoderLayerNormEmbeddingWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.decoder.layernorm_embedding.weight",
        "model.decoder.layernorm_embedding.weight",
    });
}

fn decoderLayerNormEmbeddingBias(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.decoder.layernorm_embedding.bias",
        "model.decoder.layernorm_embedding.bias",
    });
}

fn lmHeadWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.lm_head.weight",
        "lm_head.weight",
        "language_model.model.shared.weight",
        "model.decoder.embed_tokens.weight",
    });
}

const LinearWeights = struct {
    weight: CT,
    bias: CT,
};

fn encoderLinearWeights(cb: *const ComputeBackend, layer: usize, proj: []const u8, buf: *[256]u8) !LinearWeights {
    const w_name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}.weight", .{ layer, proj });
    const w = try cb.getWeight(w_name);
    const b_name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}.bias", .{ layer, proj });
    const b = try cb.getWeight(b_name);
    return .{ .weight = w, .bias = b };
}

fn encoderLinearProj(
    cb: *const ComputeBackend,
    input: CT,
    layer: usize,
    proj: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    buf: *[256]u8,
) !CT {
    const weights = try encoderLinearWeights(cb, layer, proj, buf);
    return cb.linear(input, weights.weight, weights.bias, rows, in_dim, out_dim);
}

fn encoderLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}", .{ layer, suffix });
    return cb.getWeight(name);
}

fn decoderLinearProj(
    cb: *const ComputeBackend,
    input: CT,
    layer: usize,
    proj: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    buf: *[256]u8,
) !CT {
    const weights = try decoderLinearWeights(cb, layer, proj, buf);
    return cb.linear(input, weights.weight, weights.bias, rows, in_dim, out_dim);
}

fn decoderLinearWeights(cb: *const ComputeBackend, layer: usize, proj: []const u8, buf: *[256]u8) !LinearWeights {
    const w_name = try std.fmt.bufPrint(buf, "language_model.model.decoder.layers.{d}.{s}.weight", .{ layer, proj });
    const w = cb.getWeight(w_name) catch blk: {
        const legacy = try std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}.weight", .{ layer, proj });
        break :blk try cb.getWeight(legacy);
    };
    const b_name = try std.fmt.bufPrint(buf, "language_model.model.decoder.layers.{d}.{s}.bias", .{ layer, proj });
    const b = cb.getWeight(b_name) catch blk: {
        const legacy = try std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}.bias", .{ layer, proj });
        break :blk try cb.getWeight(legacy);
    };
    return .{ .weight = w, .bias = b };
}

fn decoderLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = try std.fmt.bufPrint(buf, "language_model.model.decoder.layers.{d}.{s}", .{ layer, suffix });
    return cb.getWeight(name) catch blk: {
        const legacy = try std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}", .{ layer, suffix });
        break :blk try cb.getWeight(legacy);
    };
}

fn getAnyWeight(cb: *const ComputeBackend, names: []const []const u8) !CT {
    var last_err: ?anyerror = null;
    for (names) |name| {
        const weight = cb.getWeight(name) catch |err| {
            last_err = err;
            continue;
        };
        return weight;
    }
    return last_err orelse error.MissingWeight;
}

fn tryOptionalWeight(cb: *const ComputeBackend, name: []const u8) !?CT {
    return cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => null,
        else => err,
    };
}

fn backendLinearData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight: CT,
    bias: CT,
) ![]f32 {
    const shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.linear(input_ct, weight, bias, rows, in_dim, out_dim);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn backendLinearNoBiasData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight: CT,
) ![]f32 {
    const shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.linearNoBias(input_ct, weight, rows, in_dim, out_dim);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn projectImageFeaturesData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    vision_dim: usize,
    config: Config,
) ![]f32 {
    const proj_weight = try cb.getWeight("image_projection");
    if (cb.kind() == .cuda and !florenceDeviceVisionTailDisabled()) {
        const input_shape = [_]i64{ @intCast(vision_dim), @intCast(config.projection_dim) };
        if (cb.primTranspose(proj_weight, &.{ 1, 0 }, &input_shape)) |proj_ct| {
            defer cb.free(proj_ct);
            return try backendLinearNoBiasData(cb, allocator, input, rows, vision_dim, config.projection_dim, proj_ct);
        } else |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.InvalidShape, error.UnsupportedTensorType => {},
            else => return err,
        }
    }

    const proj_weight_data = try cb.toFloat32(proj_weight, allocator);
    defer allocator.free(proj_weight_data);
    const proj_weight_t = try transposeMatrix(allocator, proj_weight_data, vision_dim, config.projection_dim);
    defer allocator.free(proj_weight_t);
    const proj_shape = [_]i32{ @intCast(config.projection_dim), @intCast(vision_dim) };
    const proj_ct = try cb.fromFloat32Shape(proj_weight_t, &proj_shape);
    defer cb.free(proj_ct);
    return try backendLinearNoBiasData(cb, allocator, input, rows, vision_dim, config.projection_dim, proj_ct);
}

fn backendLayerNormData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    dim: usize,
    weight: CT,
    bias: CT,
    eps: f32,
) ![]f32 {
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.layerNorm(input_ct, weight, bias, dim, eps);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn backendGeluData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    dim: usize,
) ![]f32 {
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.gelu(input_ct);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn backendNormedGeluMlpData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    norm_weight: CT,
    norm_bias: CT,
    fc1_weight: CT,
    fc1_bias: CT,
    fc2_weight: CT,
    fc2_bias: CT,
) ![]f32 {
    const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const normed_ct = try cb.layerNorm(input_ct, norm_weight, norm_bias, in_dim, 1e-5);
    defer cb.free(normed_ct);
    const fc1_ct = try cb.linear(normed_ct, fc1_weight, fc1_bias, rows, in_dim, hidden_dim);
    defer cb.free(fc1_ct);
    const activated_ct = try cb.gelu(fc1_ct);
    defer cb.free(activated_ct);
    const fc2_ct = try cb.linear(activated_ct, fc2_weight, fc2_bias, rows, hidden_dim, in_dim);
    defer cb.free(fc2_ct);
    return cb.toFloat32(fc2_ct, allocator);
}

fn splitQkv(qkv: []const f32, q: []f32, k: []f32, v: []f32, dim: usize) void {
    const rows = q.len / dim;
    for (0..rows) |row| {
        const src = row * dim * 3;
        const dst = row * dim;
        @memcpy(q[dst..][0..dim], qkv[src..][0..dim]);
        @memcpy(k[dst..][0..dim], qkv[src + dim ..][0..dim]);
        @memcpy(v[dst..][0..dim], qkv[src + 2 * dim ..][0..dim]);
    }
}

fn channelAttention(
    out: []f32,
    qkv: []const f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
) !void {
    const channels_per_group = dim / groups;
    const scale = 1.0 / std.math.sqrt(@as(f32, @floatFromInt(seq_len)));
    var scores_buf: [64 * 64]f32 = undefined;

    for (0..batch) |b| {
        for (0..groups) |g| {
            const group_offset = g * channels_per_group;
            const score_slice = scores_buf[0 .. channels_per_group * channels_per_group];
            @memset(score_slice, 0.0);

            for (0..channels_per_group) |qc| {
                for (0..channels_per_group) |kc| {
                    var acc: f32 = 0.0;
                    for (0..seq_len) |n| {
                        const base = ((b * seq_len + n) * dim * 3) + group_offset;
                        acc += qkv[base + qc] * qkv[base + dim + kc];
                    }
                    score_slice[qc * channels_per_group + kc] = acc * scale;
                }
                softmaxInPlace(score_slice[qc * channels_per_group ..][0..channels_per_group]);
            }

            for (0..seq_len) |n| {
                const dst = (b * seq_len + n) * dim + group_offset;
                for (0..channels_per_group) |qc| {
                    var acc: f32 = 0.0;
                    for (0..channels_per_group) |vc| {
                        const base = ((b * seq_len + n) * dim * 3) + 2 * dim + group_offset;
                        acc += score_slice[qc * channels_per_group + vc] * qkv[base + vc];
                    }
                    out[dst + qc] = acc;
                }
            }
        }
    }
}

fn softmaxInPlace(values: []f32) void {
    var max_val = values[0];
    for (values[1..]) |value| {
        if (value > max_val) max_val = value;
    }
    var sum: f32 = 0.0;
    for (values) |*value| {
        value.* = @exp(value.* - max_val);
        sum += value.*;
    }
    if (sum == 0.0) return;
    const inv = 1.0 / sum;
    for (values) |*value| value.* *= inv;
}

fn addInPlace(dst: []f32, src: []const f32) void {
    for (dst, src) |*d, s| d.* += s;
}

const Conv2dResult = struct {
    data: []f32,
    height: usize,
    width: usize,
};

fn backendConv2dImageData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    in_channels: usize,
    height: usize,
    width: usize,
    out_channels: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
    weight: CT,
    bias: CT,
) !Conv2dResult {
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const shape = [_]i32{ @intCast(batch), @intCast(in_channels), @intCast(height), @intCast(width) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.conv2d(input_ct, weight, bias, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups);
    defer cb.free(out_ct);
    return .{
        .data = try cb.toFloat32(out_ct, allocator),
        .height = out_h,
        .width = out_w,
    };
}

fn conv2d(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    in_channels: usize,
    height: usize,
    width: usize,
    weight: []const f32,
    bias: []const f32,
    out_channels: usize,
    kernel: usize,
    stride: usize,
    padding: usize,
) !Conv2dResult {
    const out_h = (height + 2 * padding - kernel) / stride + 1;
    const out_w = (width + 2 * padding - kernel) / stride + 1;
    const out = try allocator.alloc(f32, batch * out_channels * out_h * out_w);

    for (0..batch) |b| {
        for (0..out_channels) |oc| {
            for (0..out_h) |oy| {
                for (0..out_w) |ox| {
                    var acc = bias[oc];
                    for (0..in_channels) |ic| {
                        for (0..kernel) |ky| {
                            for (0..kernel) |kx| {
                                const in_y_signed = @as(isize, @intCast(oy * stride + ky)) - @as(isize, @intCast(padding));
                                const in_x_signed = @as(isize, @intCast(ox * stride + kx)) - @as(isize, @intCast(padding));
                                if (in_y_signed < 0 or in_x_signed < 0) continue;
                                const in_y: usize = @intCast(in_y_signed);
                                const in_x: usize = @intCast(in_x_signed);
                                if (in_y >= height or in_x >= width) continue;
                                const in_idx = ((b * in_channels + ic) * height + in_y) * width + in_x;
                                const w_idx = (((oc * in_channels + ic) * kernel) + ky) * kernel + kx;
                                acc += input[in_idx] * weight[w_idx];
                            }
                        }
                    }
                    const out_idx = ((b * out_channels + oc) * out_h + oy) * out_w + ox;
                    out[out_idx] = acc;
                }
            }
        }
    }

    return .{ .data = out, .height = out_h, .width = out_w };
}

fn conv2dTokens(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    in_channels: usize,
    height: usize,
    width: usize,
    weight: []const f32,
    bias: []const f32,
    out_channels: usize,
    kernel: usize,
    stride: usize,
    padding: usize,
) ![]f32 {
    const out_h = (height + 2 * padding - kernel) / stride + 1;
    const out_w = (width + 2 * padding - kernel) / stride + 1;
    const out = try allocator.alloc(f32, batch * out_h * out_w * out_channels);

    for (0..batch) |b| {
        for (0..out_h) |oy| {
            for (0..out_w) |ox| {
                const out_base = ((b * out_h + oy) * out_w + ox) * out_channels;
                for (0..out_channels) |oc| {
                    var acc = bias[oc];
                    for (0..in_channels) |ic| {
                        for (0..kernel) |ky| {
                            for (0..kernel) |kx| {
                                const in_y_signed = @as(isize, @intCast(oy * stride + ky)) - @as(isize, @intCast(padding));
                                const in_x_signed = @as(isize, @intCast(ox * stride + kx)) - @as(isize, @intCast(padding));
                                if (in_y_signed < 0 or in_x_signed < 0) continue;
                                const in_y: usize = @intCast(in_y_signed);
                                const in_x: usize = @intCast(in_x_signed);
                                if (in_y >= height or in_x >= width) continue;
                                const in_idx = ((b * height + in_y) * width + in_x) * in_channels + ic;
                                const w_idx = (((oc * in_channels + ic) * kernel) + ky) * kernel + kx;
                                acc += input[in_idx] * weight[w_idx];
                            }
                        }
                    }
                    out[out_base + oc] = acc;
                }
            }
        }
    }

    return out;
}

fn depthwiseConv2d(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    channels: usize,
    height: usize,
    width: usize,
    weight: []const f32,
    bias: []const f32,
    kernel: usize,
    stride: usize,
    padding: usize,
) !Conv2dResult {
    const out_h = (height + 2 * padding - kernel) / stride + 1;
    const out_w = (width + 2 * padding - kernel) / stride + 1;
    const out = try allocator.alloc(f32, batch * channels * out_h * out_w);

    for (0..batch) |b| {
        for (0..channels) |c| {
            for (0..out_h) |oy| {
                for (0..out_w) |ox| {
                    var acc = bias[c];
                    for (0..kernel) |ky| {
                        for (0..kernel) |kx| {
                            const in_y_signed = @as(isize, @intCast(oy * stride + ky)) - @as(isize, @intCast(padding));
                            const in_x_signed = @as(isize, @intCast(ox * stride + kx)) - @as(isize, @intCast(padding));
                            if (in_y_signed < 0 or in_x_signed < 0) continue;
                            const in_y: usize = @intCast(in_y_signed);
                            const in_x: usize = @intCast(in_x_signed);
                            if (in_y >= height or in_x >= width) continue;
                            const in_idx = ((b * channels + c) * height + in_y) * width + in_x;
                            const w_idx = (c * kernel + ky) * kernel + kx;
                            acc += input[in_idx] * weight[w_idx];
                        }
                    }
                    const out_idx = ((b * channels + c) * out_h + oy) * out_w + ox;
                    out[out_idx] = acc;
                }
            }
        }
    }
    return .{ .data = out, .height = out_h, .width = out_w };
}

fn depthwiseConvTokens(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    channels: usize,
    height: usize,
    width: usize,
    weight: []const f32,
    bias: []const f32,
    kernel: usize,
    stride: usize,
    padding: usize,
) ![]f32 {
    const out_h = (height + 2 * padding - kernel) / stride + 1;
    const out_w = (width + 2 * padding - kernel) / stride + 1;
    const out = try allocator.alloc(f32, batch * out_h * out_w * channels);

    for (0..batch) |b| {
        for (0..out_h) |oy| {
            for (0..out_w) |ox| {
                const out_base = ((b * out_h + oy) * out_w + ox) * channels;
                for (0..channels) |c| {
                    var acc = bias[c];
                    for (0..kernel) |ky| {
                        for (0..kernel) |kx| {
                            const in_y_signed = @as(isize, @intCast(oy * stride + ky)) - @as(isize, @intCast(padding));
                            const in_x_signed = @as(isize, @intCast(ox * stride + kx)) - @as(isize, @intCast(padding));
                            if (in_y_signed < 0 or in_x_signed < 0) continue;
                            const in_y: usize = @intCast(in_y_signed);
                            const in_x: usize = @intCast(in_x_signed);
                            if (in_y >= height or in_x >= width) continue;
                            const in_idx = ((b * height + in_y) * width + in_x) * channels + c;
                            const w_idx = (c * kernel + ky) * kernel + kx;
                            acc += input[in_idx] * weight[w_idx];
                        }
                    }
                    out[out_base + c] = acc;
                }
            }
        }
    }

    return out;
}

fn tokensToImage(
    allocator: std.mem.Allocator,
    tokens: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
) ![]f32 {
    const image = try allocator.alloc(f32, batch * channels * height * width);
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const token_idx = (b * height * width + y * width + x) * channels;
                for (0..channels) |c| {
                    image[((b * channels + c) * height + y) * width + x] = tokens[token_idx + c];
                }
            }
        }
    }
    return image;
}

fn imageToTokens(
    allocator: std.mem.Allocator,
    image: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
) ![]f32 {
    const tokens = try allocator.alloc(f32, batch * height * width * channels);
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const token_idx = (b * height * width + y * width + x) * channels;
                for (0..channels) |c| {
                    tokens[token_idx + c] = image[((b * channels + c) * height + y) * width + x];
                }
            }
        }
    }
    return tokens;
}

fn transposeMatrix(allocator: std.mem.Allocator, input: []const f32, rows: usize, cols: usize) ![]f32 {
    const transposed = try allocator.alloc(f32, input.len);
    for (0..rows) |row| {
        for (0..cols) |col| transposed[col * rows + row] = input[row * cols + col];
    }
    return transposed;
}

fn readProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_READ_PROFILE");
}

fn florenceMetalFramesEnabled() bool {
    return !platform.env.getenvBool("TERMITE_FLORENCE2_METAL_DISABLE_FRAMES");
}

fn florenceMetalStageFramesEnabled() bool {
    return !platform.env.getenvBool("TERMITE_FLORENCE2_METAL_DISABLE_STAGE_FRAMES");
}

fn beginFlorenceMetalFrame(cb: *const ComputeBackend, regime: ops.DecoderRuntimeFrameRegime) !bool {
    if (cb.kind() != .metal or !florenceMetalFramesEnabled() or debugStatsEnabled() or cb.decoderRuntimeHasActiveFrame()) return false;
    const active = try cb.decoderRuntimeBeginFrame();
    if (!active) return false;
    errdefer cb.decoderRuntimeCancelFrame() catch {};
    try cb.decoderRuntimeSetActiveFrameRegime(regime);
    return true;
}

fn florenceDecoderStageFrameProfilingEnabled(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal and
        readProfileEnabled() and
        platform.env.getenvBool("TERMITE_FLORENCE2_METAL_PROFILE_DECODER_STAGE_FRAMES");
}

fn florenceVisionOpFrameProfilingEnabled(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal and
        readProfileEnabled() and
        platform.env.getenvBool("TERMITE_FLORENCE2_METAL_PROFILE_VISION_OP_FRAMES");
}

fn profileFlorenceVisionOpFrameBoundary(
    cb: *const ComputeBackend,
    phase: []const u8,
    stage: usize,
    layer: usize,
    start_ns: u64,
) !void {
    if (!florenceVisionOpFrameProfilingEnabled(cb)) return;
    if (!cb.decoderRuntimeHasActiveFrame()) return error.UnsupportedOperation;
    const gpu_before = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    try cb.decoderRuntimeSubmitAndWaitFrame();
    const gpu_after = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    const gpu_ns = gpu_after -| gpu_before;
    std.log.info(
        "florence-profile phase={s} stage={d} layer={d} elapsed_ms={d:.3} gpu_ms={d:.3}",
        .{
            phase,
            stage,
            layer,
            nsToMs(nowNs() - start_ns),
            @as(f64, @floatFromInt(gpu_ns)) / @as(f64, std.time.ns_per_ms),
        },
    );
    if (!try beginFlorenceMetalFrame(cb, .prefill)) return error.UnsupportedOperation;
}

fn profileFlorenceDecoderStageFrameBoundary(
    cb: *const ComputeBackend,
    phase: []const u8,
    layer: usize,
    start_ns: u64,
) !u64 {
    if (!florenceDecoderStageFrameProfilingEnabled(cb)) return start_ns;
    if (!cb.decoderRuntimeHasActiveFrame()) return error.UnsupportedOperation;
    const gpu_before = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    try cb.decoderRuntimeSubmitAndWaitFrame();
    const gpu_after = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    const gpu_ns = gpu_after -| gpu_before;
    std.log.info(
        "florence-profile phase={s} layer={d} elapsed_ms={d:.3} gpu_ms={d:.3}",
        .{
            phase,
            layer,
            nsToMs(nowNs() - start_ns),
            @as(f64, @floatFromInt(gpu_ns)) / @as(f64, std.time.ns_per_ms),
        },
    );
    if (!try beginFlorenceMetalFrame(cb, .decode)) return error.UnsupportedOperation;
    return nowNs();
}

fn debugStatsEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_DEBUG_STATS");
}

fn logDebugStatsF32(label: []const u8, values: []const f32) void {
    if (values.len == 0) {
        std.log.info("florence-debug {s}: empty", .{label});
        return;
    }
    var sum: f64 = 0;
    var absmax: f32 = 0;
    for (values) |v| {
        sum += v;
        const a = @abs(v);
        if (a > absmax) absmax = a;
    }
    const mean = sum / @as(f64, @floatFromInt(values.len));
    const n_first = @min(values.len, 4);
    std.log.info("florence-debug {s}: n={d} mean={d:.6} absmax={d:.4} first={any}", .{
        label, values.len, mean, absmax, values[0..n_first],
    });
}

fn logDebugStatsTensor(cb: *const ComputeBackend, allocator: std.mem.Allocator, label: []const u8, tensor: CT) void {
    const host = cb.toFloat32(tensor, allocator) catch |err| {
        std.log.info("florence-debug {s}: download failed ({t})", .{ label, err });
        return;
    };
    defer allocator.free(host);
    logDebugStatsF32(label, host);
}

fn logFlorenceProfile(phase: []const u8, start_ns: u64) void {
    std.log.info("florence-profile phase={s} elapsed_ms={d:.3}", .{ phase, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStep(phase: []const u8, step: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} step={d} elapsed_ms={d:.3}", .{ phase, step, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStage(phase: []const u8, stage: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} stage={d} elapsed_ms={d:.3}", .{ phase, stage, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStageLayer(phase: []const u8, stage: usize, layer: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} stage={d} layer={d} elapsed_ms={d:.3}", .{ phase, stage, layer, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStageLayerOp(phase: []const u8, stage: usize, layer: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} stage={d} layer={d} elapsed_ms={d:.3}", .{ phase, stage, layer, nsToMs(nowNs() - start_ns) });
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn fmtAlloc(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ![]u8 {
    return std.fmt.allocPrint(allocator, fmt, args);
}
