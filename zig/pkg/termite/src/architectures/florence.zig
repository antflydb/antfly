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
// ComputeBackend so the same code works for native and MLX.

const std = @import("std");
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

const VisionForwardResult = struct {
    features: []f32,
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
    const image = try visionEncoderForward(cb, allocator, config, pixel_values, batch);
    defer allocator.free(image.features);

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

    var hidden = try applyEncoderEmbeddings(cb, allocator, merged, config, batch, total_seq);
    allocator.free(merged);

    const attn_mask = try allocator.alloc(i64, total_tokens);
    defer allocator.free(attn_mask);
    @memset(attn_mask, 1);

    var buf: [256]u8 = undefined;
    for (0..config.encoder_layers) |layer| {
        const next = try encoderBlock(cb, config, hidden, attn_mask, batch, total_seq, layer, &buf);
        cb.free(hidden);
        hidden = next;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return .{ .hidden = result, .seq_len = total_seq };
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
    var stage_tokens: ?[]f32 = null;
    errdefer if (stage_tokens) |tokens| allocator.free(tokens);
    var stage_h: usize = config.image_size;
    var stage_w: usize = config.image_size;

    for (0..Config.stage_count) |stage| {
        const embedded = try convEmbed(cb, allocator, config, stage, batch, pixel_values, stage_tokens, stage_h, stage_w);
        if (stage_tokens) |old| allocator.free(old);
        stage_tokens = embedded.tokens;
        stage_h = embedded.height;
        stage_w = embedded.width;

        const depth: usize = config.depths[stage];
        if (useTensorNativeVision(cb)) {
            const stage_shape = [_]i32{ @intCast(batch * stage_h * stage_w), @intCast(config.dim_embed[stage]) };
            var hidden = try cb.fromFloat32Shape(stage_tokens.?, &stage_shape);
            allocator.free(stage_tokens.?);
            stage_tokens = null;
            errdefer cb.free(hidden);

            for (0..depth) |layer| {
                const next = try daViTBlock(cb, allocator, config, hidden, batch, stage_h, stage_w, stage, layer);
                hidden = next;
            }

            stage_tokens = try cb.toFloat32(hidden, allocator);
            cb.free(hidden);
        } else {
            for (0..depth) |layer| {
                stage_tokens = try daViTBlockData(cb, allocator, config, stage_tokens.?, batch, stage_h, stage_w, stage, layer);
            }
        }
    }

    const tokens = stage_tokens orelse return error.MissingInputs;
    const vision_dim: usize = config.dim_embed[Config.stage_count - 1];
    const token_count = stage_h * stage_w;

    try add2dPositionalEmbedding(cb, allocator, tokens, batch, token_count, vision_dim, stage_h, stage_w);
    try addTemporalEmbedding(cb, allocator, tokens, batch, token_count, vision_dim);

    const sourced = try imageFeatureSourceConcat(allocator, config, tokens, batch, token_count, vision_dim);
    defer allocator.free(sourced.data);
    allocator.free(tokens);

    const proj_weight = try cb.getWeight("image_projection");
    const proj_weight_data = try cb.toFloat32(proj_weight, allocator);
    defer allocator.free(proj_weight_data);
    const proj_weight_t = try transposeMatrix(allocator, proj_weight_data, vision_dim, config.projection_dim);
    defer allocator.free(proj_weight_t);

    const proj_shape = [_]i32{ @intCast(config.projection_dim), @intCast(vision_dim) };
    const proj_ct = try cb.fromFloat32Shape(proj_weight_t, &proj_shape);
    defer cb.free(proj_ct);

    const proj_rows = batch * sourced.seq_len;
    const projected = try backendLinearNoBiasData(cb, allocator, sourced.data, proj_rows, vision_dim, config.projection_dim, proj_ct);
    defer allocator.free(projected);

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

    return .{ .features = normed, .seq_len = sourced.seq_len };
}

fn useTensorNativeVision(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal;
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
    const dim: usize = config.dim_embed[stage];
    var hidden = input;
    var next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv1");
    cb.free(hidden);
    hidden = next;
    next = try residualWindowAttention(cb, allocator, hidden, batch, height, width, dim, config.num_heads[stage], config.window_size, stage, layer);
    cb.free(hidden);
    hidden = next;
    next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv2");
    cb.free(hidden);
    hidden = next;
    next = try residualMlp(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "spatial_block.ffn");
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
    const dim: usize = config.dim_embed[stage];
    var hidden = input;
    var next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv1");
    cb.free(hidden);
    hidden = next;
    next = try residualChannelAttention(cb, allocator, hidden, batch, height * width, dim, config.num_groups[stage], stage, layer);
    cb.free(hidden);
    hidden = next;
    next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv2");
    cb.free(hidden);
    hidden = next;
    next = try residualMlp(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "channel_block.ffn");
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
    const fc1_ct = try cb.linear(normed_ct, try cb.getWeight(fc1_w_name), try cb.getWeight(fc1_b_name), rows, dim, hidden_dim);
    defer cb.free(fc1_ct);
    const activated_ct = try cb.gelu(fc1_ct);
    defer cb.free(activated_ct);
    const fc2_ct = try cb.linear(activated_ct, try cb.getWeight(fc2_w_name), try cb.getWeight(fc2_b_name), rows, hidden_dim, dim);
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

fn applyDecoderEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const embed_w = try promptEmbedWeight(cb);
    var hidden = try cb.embeddingLookup(embed_w, input_ids, total, d_model);

    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(s + bart_position_offset);
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

    const q = try encoderLinearProj(cb, hidden, layer, "self_attn.q_proj", total, d_model, d_model, buf);
    defer cb.free(q);
    const k = try encoderLinearProj(cb, hidden, layer, "self_attn.k_proj", total, d_model, d_model, buf);
    defer cb.free(k);
    const v = try encoderLinearProj(cb, hidden, layer, "self_attn.v_proj", total, d_model, d_model, buf);
    defer cb.free(v);

    const attn = try cb.scaledDotProductAttention(q, k, v, attention_mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);
    const proj = try encoderLinearProj(cb, attn, layer, "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(proj);
    const attn_res = try cb.add(hidden, proj);

    const ln0_w = try encoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try encoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const attn_normed = try cb.layerNorm(attn_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(attn_res);

    const fc1 = try encoderLinearProj(cb, attn_normed, layer, "fc1", total, d_model, ffn_dim, buf);
    defer cb.free(fc1);
    const activated = try cb.gelu(fc1);
    defer cb.free(activated);
    const fc2 = try encoderLinearProj(cb, activated, layer, "fc2", total, ffn_dim, d_model, buf);
    defer cb.free(fc2);

    const ffn_res = try cb.add(attn_normed, fc2);
    cb.free(attn_normed);

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

    const q = try decoderLinearProj(cb, hidden, layer, "self_attn.q_proj", total, d_model, d_model, buf);
    defer cb.free(q);
    const k = try decoderLinearProj(cb, hidden, layer, "self_attn.k_proj", total, d_model, d_model, buf);
    defer cb.free(k);
    const v = try decoderLinearProj(cb, hidden, layer, "self_attn.v_proj", total, d_model, d_model, buf);
    defer cb.free(v);
    const attn = try cb.causalSelfAttention(q, k, v, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);
    const proj = try decoderLinearProj(cb, attn, layer, "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(proj);
    const attn_res = try cb.add(hidden, proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const attn_normed = try cb.layerNorm(attn_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(attn_res);

    const fc1 = try decoderLinearProj(cb, attn_normed, layer, "fc1", total, d_model, ffn_dim, buf);
    defer cb.free(fc1);
    const activated = try cb.gelu(fc1);
    defer cb.free(activated);
    const fc2 = try decoderLinearProj(cb, activated, layer, "fc2", total, ffn_dim, d_model, buf);
    defer cb.free(fc2);

    const ffn_res = try cb.add(attn_normed, fc2);
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
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const dec_total = batch * dec_seq;
    const enc_total = batch * enc_seq;

    const q_self = try decoderLinearProj(cb, hidden, layer, "self_attn.q_proj", dec_total, d_model, d_model, buf);
    defer cb.free(q_self);
    const k_self = try decoderLinearProj(cb, hidden, layer, "self_attn.k_proj", dec_total, d_model, d_model, buf);
    defer cb.free(k_self);
    const v_self = try decoderLinearProj(cb, hidden, layer, "self_attn.v_proj", dec_total, d_model, d_model, buf);
    defer cb.free(v_self);
    const self_attn = try cb.causalSelfAttention(q_self, k_self, v_self, null, batch, dec_seq, num_heads, head_dim);
    defer cb.free(self_attn);
    const self_proj = try decoderLinearProj(cb, self_attn, layer, "self_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(self_proj);
    const self_res = try cb.add(hidden, self_proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const self_normed = try cb.layerNorm(self_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(self_res);

    const q_cross = try decoderLinearProj(cb, self_normed, layer, "encoder_attn.q_proj", dec_total, d_model, d_model, buf);
    defer cb.free(q_cross);
    const k_cross = try decoderLinearProj(cb, encoder_hidden, layer, "encoder_attn.k_proj", enc_total, d_model, d_model, buf);
    defer cb.free(k_cross);
    const v_cross = try decoderLinearProj(cb, encoder_hidden, layer, "encoder_attn.v_proj", enc_total, d_model, d_model, buf);
    defer cb.free(v_cross);
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

    const fc1 = try decoderLinearProj(cb, cross_normed, layer, "fc1", dec_total, d_model, ffn_dim, buf);
    defer cb.free(fc1);
    const activated = try cb.gelu(fc1);
    defer cb.free(activated);
    const fc2 = try decoderLinearProj(cb, activated, layer, "fc2", dec_total, ffn_dim, d_model, buf);
    defer cb.free(fc2);
    const ffn_res = try cb.add(cross_normed, fc2);
    cb.free(cross_normed);
    const ln2_w = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln2_b = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln2_w, ln2_b, d_model, 1e-5);
    cb.free(ffn_res);
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
    const w_name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}.weight", .{ layer, proj });
    const w = try cb.getWeight(w_name);
    const b_name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}.bias", .{ layer, proj });
    const b = try cb.getWeight(b_name);
    return cb.linear(input, w, b, rows, in_dim, out_dim);
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
    return cb.linear(input, w, b, rows, in_dim, out_dim);
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

fn fmtAlloc(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ![]u8 {
    return std.fmt.allocPrint(allocator, fmt, args);
}
