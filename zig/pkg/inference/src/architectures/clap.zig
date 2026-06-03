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

const std = @import("std");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;
const clap_mod = @import("../models/clap.zig");
const bert_mod = @import("../models/bert.zig");
const graph_runtime = @import("../graph/runtime.zig");
const clap_graph = @import("clap_graph.zig");

pub fn textEncoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const bert_cfg = cfg.text_config;
    const H = bert_cfg.hidden_size;
    const total = batch * seq_len;

    var hidden = try embeddings(cb, allocator, input_ids, token_type_ids, total, seq_len, H, cfg.text_pad_token_id);
    for (0..bert_cfg.num_hidden_layers) |layer| {
        const new_hidden = try encoderLayer(cb, hidden, attention_mask, batch, seq_len, layer, bert_cfg);
        cb.free(hidden);
        hidden = new_hidden;
    }

    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    cb.free(hidden);

    const pooled = try allocator.alloc(f32, batch * H);
    defer allocator.free(pooled);
    for (0..batch) |b| {
        @memcpy(pooled[b * H ..][0..H], hidden_data[b * seq_len * H ..][0..H]);
    }

    return projectText(cb, allocator, cfg, pooled, batch);
}

pub fn audioEncoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    input_features: []const f32,
    batch: usize,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
    is_longer: []const u8,
) ![]f32 {
    return audioEncoderForwardWithGraphTail(cb, allocator, cfg, input_features, batch, channels, time_frames, mel_bins, is_longer, null);
}

pub fn audioEncoderForwardGraphTail(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    input_features: []const f32,
    batch: usize,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
    is_longer: []const u8,
    strategy: graph_runtime.Strategy,
) ![]f32 {
    return audioEncoderForwardWithGraphTail(cb, allocator, cfg, input_features, batch, channels, time_frames, mel_bins, is_longer, strategy);
}

fn audioEncoderForwardWithGraphTail(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    input_features: []const f32,
    batch: usize,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
    is_longer: []const u8,
    graph_tail_strategy: ?graph_runtime.Strategy,
) ![]f32 {
    const ac = cfg.audio_config;
    if (mel_bins != ac.num_mel_bins) return error.InvalidInputShape;
    if (channels == 0) return error.InvalidInputShape;
    if (channels > 1 and !ac.enable_fusion) return error.InvalidInputShape;

    const batch_normed = try batchNormInput(allocator, cb, input_features, batch, channels, time_frames, mel_bins);
    defer allocator.free(batch_normed);

    const image = try reshapeMelToImage(allocator, batch_normed, batch, channels, time_frames, mel_bins, ac.spec_size);
    defer allocator.free(image);

    var height: usize = ac.spec_size;
    var width: usize = ac.spec_size;
    var hidden = try patchEmbed(cb, allocator, image, batch, channels, height, width, ac, is_longer, graph_tail_strategy);

    height = (height - ac.patch_size) / ac.patch_stride[0] + 1;
    width = (width - ac.patch_size) / ac.patch_stride[1] + 1;

    for (0..ac.depths.len) |stage| {
        for (0..ac.depths[stage]) |block| {
            const shift = if ((block % 2) == 1 and @min(height, width) > ac.window_size) ac.window_size / 2 else 0;
            hidden = try clapAudioLayer(cb, allocator, hidden, batch, height, width, ac.stageDim(stage), ac.num_attention_heads[stage], ac.window_size, shift, ac.layer_norm_eps, stage, block, graph_tail_strategy);
        }

        if (stage + 1 < ac.depths.len) {
            const merged = try patchMerge(cb, allocator, hidden, batch, height, width, ac.stageDim(stage), stage);
            allocator.free(hidden);
            hidden = merged;
            height /= 2;
            width /= 2;
        }
    }

    const hidden_dim: usize = ac.hidden_size;
    if (graph_tail_strategy) |strategy| {
        defer allocator.free(hidden);
        return clap_graph.runAudioTailGraph(cb, allocator, cfg, hidden, batch, height * width, hidden_dim, strategy);
    }

    const hidden_normed = try layerNormData(
        cb,
        allocator,
        hidden,
        batch * height * width,
        hidden_dim,
        "audio_model.audio_encoder.norm.weight",
        "audio_model.audio_encoder.norm.bias",
        ac.layer_norm_eps,
    );
    allocator.free(hidden);

    const pooled = try avgPoolTokens(allocator, hidden_normed, batch, height * width, hidden_dim);
    allocator.free(hidden_normed);
    return projectAudio(cb, allocator, cfg, pooled, batch);
}

fn projectText(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    pooled: []const f32,
    batch: usize,
) ![]f32 {
    const H = cfg.text_config.hidden_size;
    const pooled_ct = try cb.fromFloat32Shape(pooled, &[_]i32{ @intCast(batch), @intCast(H) });
    defer cb.free(pooled_ct);

    const pool_w = try cb.getWeight("text_model.pooler.dense.weight");
    defer cb.free(pool_w);
    const pool_b = try cb.getWeight("text_model.pooler.dense.bias");
    defer cb.free(pool_b);
    const pooled_proj = try cb.linear(pooled_ct, pool_w, pool_b, batch, H, H);
    defer cb.free(pooled_proj);
    const pooled_tanh = try cb.tanh_act(pooled_proj);
    defer cb.free(pooled_tanh);

    const proj1 = try linearNamed(cb, allocator, pooled_tanh, batch, H, cfg.projection_dim, "text_projection.linear1.weight", "text_projection.linear1.bias");
    defer allocator.free(proj1);
    const activated = switch (cfg.projection_hidden_act) {
        .relu => try reluData(cb, allocator, proj1, batch, cfg.projection_dim),
        .gelu => try geluData(cb, allocator, proj1, batch, cfg.projection_dim),
    };
    defer allocator.free(activated);

    return linearNamedData(cb, allocator, activated, batch, cfg.projection_dim, cfg.projection_dim, "text_projection.linear2.weight", "text_projection.linear2.bias");
}

fn projectAudio(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: clap_mod.Config,
    pooled: []const f32,
    batch: usize,
) ![]f32 {
    defer allocator.free(pooled);
    const H = cfg.audio_config.hidden_size;
    const proj1 = try linearNamedData(cb, allocator, pooled, batch, H, cfg.projection_dim, "audio_projection.linear1.weight", "audio_projection.linear1.bias");
    defer allocator.free(proj1);
    const activated = switch (cfg.projection_hidden_act) {
        .relu => try reluData(cb, allocator, proj1, batch, cfg.projection_dim),
        .gelu => try geluData(cb, allocator, proj1, batch, cfg.projection_dim),
    };
    defer allocator.free(activated);
    return linearNamedData(cb, allocator, activated, batch, cfg.projection_dim, cfg.projection_dim, "audio_projection.linear2.weight", "audio_projection.linear2.bias");
}

fn patchEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: []const f32,
    batch: usize,
    channels: usize,
    height: usize,
    width: usize,
    ac: clap_mod.Config.AudioConfig,
    is_longer: []const u8,
    graph_strategy: ?graph_runtime.Strategy,
) ![]f32 {
    const out_h = (height - ac.patch_size) / ac.patch_stride[0] + 1;
    const out_w = (width - ac.patch_size) / ac.patch_stride[1] + 1;
    const token_count = batch * out_h * out_w;
    const dim = ac.patch_embeds_hidden_size;

    if (graph_strategy) |strategy| {
        if (channels == 1) {
            return clap_graph.runAudioPatchEmbedGraph(cb, allocator, ac, image, batch, height, width, strategy);
        }
    }

    const tokens = try allocator.alloc(f32, token_count * dim);
    errdefer allocator.free(tokens);

    const plane = height * width;
    for (0..batch) |b| {
        const long_item = ac.enable_fusion and channels >= 4 and b < is_longer.len and is_longer[b] != 0;
        const sample = image[b * channels * plane ..][0 .. channels * plane];
        const embedded = if (long_item)
            try patchEmbedFusedItem(cb, allocator, sample, channels, height, width, ac)
        else
            try patchEmbedSingleItem(cb, allocator, sample[0..plane], height, width, ac);
        defer allocator.free(embedded);

        for (0..(out_h * out_w)) |tok| {
            const dst = (b * out_h * out_w + tok) * dim;
            @memcpy(tokens[dst..][0..dim], embedded[tok * dim ..][0..dim]);
        }
    }

    if (!ac.enable_patch_layer_norm) return tokens;
    const normed = try layerNormData(
        cb,
        allocator,
        tokens,
        token_count,
        dim,
        "audio_model.audio_encoder.patch_embed.norm.weight",
        "audio_model.audio_encoder.patch_embed.norm.bias",
        ac.layer_norm_eps,
    );
    allocator.free(tokens);
    return normed;
}

fn patchEmbedSingleItem(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: []const f32,
    height: usize,
    width: usize,
    ac: clap_mod.Config.AudioConfig,
) ![]f32 {
    const in_ct = try cb.fromFloat32Shape(image, &[_]i32{ 1, 1, @intCast(height), @intCast(width) });
    defer cb.free(in_ct);
    const proj_w = try cb.getWeight("audio_model.audio_encoder.patch_embed.proj.weight");
    defer cb.free(proj_w);
    const proj_b = try cb.getWeight("audio_model.audio_encoder.patch_embed.proj.bias");
    defer cb.free(proj_b);
    const out_ct = try cb.conv2d(
        in_ct,
        proj_w,
        proj_b,
        1,
        1,
        ac.patch_embeds_hidden_size,
        height,
        width,
        ac.patch_size,
        ac.patch_size,
        ac.patch_stride[0],
        ac.patch_stride[1],
        0,
        0,
        1,
    );
    defer cb.free(out_ct);
    const out_h = (height - ac.patch_size) / ac.patch_stride[0] + 1;
    const out_w = (width - ac.patch_size) / ac.patch_stride[1] + 1;
    const out = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(out);
    return convOutputToTokens(allocator, out, out_h, out_w, ac.patch_embeds_hidden_size);
}

fn patchEmbedFusedItem(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: []const f32,
    channels: usize,
    height: usize,
    width: usize,
    ac: clap_mod.Config.AudioConfig,
) ![]f32 {
    const plane = height * width;
    const global_tokens = try patchEmbedSingleItem(cb, allocator, image[0..plane], height, width, ac);
    defer allocator.free(global_tokens);

    const local_count = @min(channels - 1, 3);
    const local_tokens = try patchEmbedLocalItem(cb, allocator, image[plane..][0 .. local_count * plane], local_count, height, width, ac);
    defer allocator.free(local_tokens);

    const out_h = (height - ac.patch_size) / ac.patch_stride[0] + 1;
    const out_w = (width - ac.patch_size) / ac.patch_stride[1] + 1;
    return fusePatchEmbeddings(cb, allocator, local_tokens, global_tokens, out_h, out_w, ac.patch_embeds_hidden_size);
}

fn patchEmbedLocalItem(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: []const f32,
    local_count: usize,
    height: usize,
    width: usize,
    ac: clap_mod.Config.AudioConfig,
) ![]f32 {
    const local_ct = try cb.fromFloat32Shape(image, &[_]i32{ @intCast(local_count), 1, @intCast(height), @intCast(width) });
    defer cb.free(local_ct);
    const proj_w = try cb.getWeight("audio_model.audio_encoder.patch_embed.mel_conv2d.weight");
    defer cb.free(proj_w);
    const proj_b = try cb.getWeight("audio_model.audio_encoder.patch_embed.mel_conv2d.bias");
    defer cb.free(proj_b);
    const kernel_w = ac.patch_size * 3;
    const stride_w = ac.patch_stride[1] * 3;
    const out_ct = try cb.conv2d(
        local_ct,
        proj_w,
        proj_b,
        local_count,
        1,
        ac.patch_embeds_hidden_size,
        height,
        width,
        ac.patch_size,
        kernel_w,
        ac.patch_stride[0],
        stride_w,
        0,
        0,
        1,
    );
    defer cb.free(out_ct);

    const out_h = (height - ac.patch_size) / ac.patch_stride[0] + 1;
    const out_w = (width - kernel_w) / stride_w + 1;
    const output_width = (width - ac.patch_size) / ac.patch_stride[1] + 1;
    const features = ac.patch_embeds_hidden_size;
    const out = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(out);

    const tokens = try allocator.alloc(f32, out_h * output_width * features);
    errdefer allocator.free(tokens);
    @memset(tokens, 0);

    for (0..out_h) |y| {
        for (0..features) |c| {
            for (0..local_count) |local_idx| {
                for (0..out_w) |x| {
                    const src_base = (((local_idx * features + c) * out_h + y) * out_w + x);
                    const dst_x = local_idx * out_w + x;
                    const dst_base = ((y * output_width + dst_x) * features + c);
                    tokens[dst_base] = out[src_base];
                }
            }
        }
    }

    return tokens;
}

fn fusePatchEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    local_tokens: []const f32,
    global_tokens: []const f32,
    height: usize,
    width: usize,
    channels_u32: u32,
) ![]f32 {
    const channels: usize = channels_u32;
    const plane = height * width;
    const xa = try allocator.alloc(f32, local_tokens.len);
    defer allocator.free(xa);
    for (0..local_tokens.len) |i| xa[i] = local_tokens[i] + global_tokens[i];

    const local_att = try affBranch(cb, allocator, xa, height, width, channels, "audio_model.audio_encoder.patch_embed.fusion_model.local_att");
    defer allocator.free(local_att);
    const global_att = try affGlobalBranch(cb, allocator, xa, height, width, channels, "audio_model.audio_encoder.patch_embed.fusion_model.global_att");
    defer allocator.free(global_att);

    const out = try allocator.alloc(f32, local_tokens.len);
    errdefer allocator.free(out);
    // SIMD sigmoid + lerp fusion: gate = sigmoid(local_att + global_att), then
    // out = 2*(gate*local + (1-gate)*global).  The previous scalar loop ran
    // libm expf per element on plane*channels (≈ 256K) values for CLAP-large
    // stage 0 fusion.  Width follows linalg.primitives so we line up with
    // expVec's lane count (8 on AVX2/NEON, 16 on AVX-512).
    const linalg_prim = @import("inference_linalg").primitives;
    const VEC = linalg_prim.vec_len;
    const Vec = @Vector(VEC, f32);
    const total = plane * channels;
    const one_v: Vec = @splat(1.0);
    const two_v: Vec = @splat(2.0);
    var i: usize = 0;
    while (i + VEC <= total) : (i += VEC) {
        const la: Vec = local_att[i..][0..VEC].*;
        const ga: Vec = global_att[i..][0..VEC].*;
        const gate = one_v / (one_v + linalg_prim.expVec(-(la + ga)));
        const lt: Vec = local_tokens[i..][0..VEC].*;
        const gt: Vec = global_tokens[i..][0..VEC].*;
        out[i..][0..VEC].* = two_v * (gate * lt + (one_v - gate) * gt);
    }
    while (i < total) : (i += 1) {
        const gate = 1.0 / (1.0 + @exp(-(local_att[i] + global_att[i])));
        out[i] = 2.0 * local_tokens[i] * gate + 2.0 * global_tokens[i] * (1.0 - gate);
    }
    return out;
}

fn clapAudioLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    dim_u32: u32,
    num_heads_u32: u32,
    window_size_u32: u32,
    shift_size_u32: u32,
    eps: f32,
    stage: usize,
    block: usize,
    graph_strategy: ?graph_runtime.Strategy,
) ![]f32 {
    const dim: usize = dim_u32;
    const num_heads: usize = num_heads_u32;
    const window_size: usize = window_size_u32;
    const shift_size: usize = shift_size_u32;
    const block_graph_strategy = graph_strategy;
    const hidden = input;

    const before_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.layernorm_before.weight", .{ stage, block });
    defer allocator.free(before_w);
    const before_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.layernorm_before.bias", .{ stage, block });
    defer allocator.free(before_b);
    const normed = try layerNormData(cb, allocator, hidden, batch * height * width, dim, before_w, before_b, eps);
    defer allocator.free(normed);

    const shifted_owned = if (shift_size > 0)
        try cyclicShiftTokens(allocator, normed, batch, height, width, dim, shift_size)
    else
        null;
    defer if (shifted_owned) |shifted| allocator.free(shifted);
    const shifted = shifted_owned orelse normed;

    const windows = try windowPartition(allocator, shifted, batch, height, width, dim, window_size);
    defer allocator.free(windows);
    const num_windows = batch * (height / window_size) * (width / window_size);
    const window_area = window_size * window_size;

    const q_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.query.weight", .{ stage, block });
    defer allocator.free(q_w);
    const q_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.query.bias", .{ stage, block });
    defer allocator.free(q_b);
    const k_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.key.weight", .{ stage, block });
    defer allocator.free(k_w);
    const k_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.key.bias", .{ stage, block });
    defer allocator.free(k_b);
    const v_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.value.weight", .{ stage, block });
    defer allocator.free(v_w);
    const v_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.value.bias", .{ stage, block });
    defer allocator.free(v_b);

    const rows = num_windows * window_area;
    const projected = if (block_graph_strategy) |strategy| blk: {
        if (shift_size == 0 and try clapAudioRelativeBiasExists(cb, allocator, stage, block)) {
            break :blk try clap_graph.runAudioUnshiftedAttentionGraph(cb, allocator, windows, rows, dim, num_windows, window_area, num_heads, window_size, stage, block, strategy);
        }
        const eager = try clapAudioAttentionEager(cb, allocator, windows, rows, dim, num_windows, window_area, num_heads, window_size, shift_size, height, width, stage, block, q_w, q_b, k_w, k_b, v_w, v_b);
        break :blk eager;
    } else try clapAudioAttentionEager(cb, allocator, windows, rows, dim, num_windows, window_area, num_heads, window_size, shift_size, height, width, stage, block, q_w, q_b, k_w, k_b, v_w, v_b);
    defer allocator.free(projected);

    var merged = try windowUnpartition(allocator, projected, batch, height, width, dim, window_size);
    defer allocator.free(merged);
    if (shift_size > 0) {
        const unshifted = try cyclicShiftTokens(allocator, merged, batch, height, width, dim, height - shift_size);
        allocator.free(merged);
        merged = unshifted;
    }

    addInPlace(hidden, merged);

    const fc2 = if (block_graph_strategy) |strategy|
        try clap_graph.runAudioMlpGraph(cb, allocator, hidden, batch * height * width, dim, eps, stage, block, strategy)
    else blk: {
        const after_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.layernorm_after.weight", .{ stage, block });
        defer allocator.free(after_w);
        const after_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.layernorm_after.bias", .{ stage, block });
        defer allocator.free(after_b);
        const after = try layerNormData(cb, allocator, hidden, batch * height * width, dim, after_w, after_b, eps);
        defer allocator.free(after);

        const inner_dim: usize = dim * 4;
        const fc1_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.intermediate.dense.weight", .{ stage, block });
        defer allocator.free(fc1_w);
        const fc1_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.intermediate.dense.bias", .{ stage, block });
        defer allocator.free(fc1_b);
        const fc1 = try linearNamedData(cb, allocator, after, batch * height * width, dim, inner_dim, fc1_w, fc1_b);
        defer allocator.free(fc1);
        const act = try geluData(cb, allocator, fc1, batch * height * width, inner_dim);
        defer allocator.free(act);

        const fc2_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.output.dense.weight", .{ stage, block });
        defer allocator.free(fc2_w);
        const fc2_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.output.dense.bias", .{ stage, block });
        defer allocator.free(fc2_b);
        break :blk try linearNamedData(cb, allocator, act, batch * height * width, inner_dim, dim, fc2_w, fc2_b);
    };
    defer allocator.free(fc2);
    addInPlace(hidden, fc2);
    return hidden;
}

fn clapAudioRelativeBiasExists(cb: *const ComputeBackend, allocator: std.mem.Allocator, stage: usize, block: usize) !bool {
    const rel_bias_name = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.relative_position_bias_table", .{ stage, block });
    defer allocator.free(rel_bias_name);
    const rel_bias = cb.getWeight(rel_bias_name) catch |err| switch (err) {
        error.MissingWeight => return false,
        else => return err,
    };
    cb.free(rel_bias);
    return true;
}

fn clapAudioAttentionEager(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    windows: []const f32,
    rows: usize,
    dim: usize,
    num_windows: usize,
    window_area: usize,
    num_heads: usize,
    window_size: usize,
    shift_size: usize,
    height: usize,
    width: usize,
    stage: usize,
    block: usize,
    q_w: []const u8,
    q_b: []const u8,
    k_w: []const u8,
    k_b: []const u8,
    v_w: []const u8,
    v_b: []const u8,
) ![]f32 {
    const qkv = try linearTripleNamedData(cb, allocator, windows, rows, dim, dim, q_w, q_b, k_w, k_b, v_w, v_b);
    defer qkv.deinit(allocator);

    const rel_bias_name = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.self.relative_position_bias_table", .{ stage, block });
    defer allocator.free(rel_bias_name);
    const rel_bias = weightData(cb, allocator, rel_bias_name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => blk: {
            const side = 2 * window_size - 1;
            const zeros = try allocator.alloc(f32, side * side * num_heads);
            @memset(zeros, 0.0);
            break :blk zeros;
        },
        else => return err,
    };
    defer allocator.free(rel_bias);
    const attn_mask = if (shift_size > 0) try buildShiftedWindowMask(allocator, height, width, window_size, shift_size) else &[_]f32{};
    defer if (shift_size > 0) allocator.free(attn_mask);

    const attn_out = try windowAttention(allocator, qkv.first, qkv.second, qkv.third, rel_bias, if (shift_size > 0) attn_mask else null, num_windows, window_area, dim, num_heads, window_size);
    defer allocator.free(attn_out);

    const proj_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.output.dense.weight", .{ stage, block });
    defer allocator.free(proj_w);
    const proj_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.blocks.{d}.attention.output.dense.bias", .{ stage, block });
    defer allocator.free(proj_b);
    return linearNamedData(cb, allocator, attn_out, rows, dim, dim, proj_w, proj_b);
}

fn patchMerge(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    dim_u32: u32,
    stage: usize,
) ![]f32 {
    const dim: usize = dim_u32;
    const out_h = height / 2;
    const out_w = width / 2;
    const merged = try allocator.alloc(f32, batch * out_h * out_w * dim * 4);
    errdefer allocator.free(merged);
    for (0..batch) |b| {
        for (0..out_h) |y| {
            for (0..out_w) |x| {
                const dst_base = ((b * out_h * out_w + y * out_w + x) * 4 * dim);
                const coords = [_][2]usize{ .{ 2 * y, 2 * x }, .{ 2 * y + 1, 2 * x }, .{ 2 * y, 2 * x + 1 }, .{ 2 * y + 1, 2 * x + 1 } };
                for (coords, 0..) |coord, part| {
                    const src_base = ((b * height * width + coord[0] * width + coord[1]) * dim);
                    @memcpy(merged[dst_base + part * dim ..][0..dim], input[src_base..][0..dim]);
                }
            }
        }
    }

    const norm_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.downsample.norm.weight", .{stage});
    defer allocator.free(norm_w);
    const norm_b = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.downsample.norm.bias", .{stage});
    defer allocator.free(norm_b);
    const rows = batch * out_h * out_w;
    const in_dim = dim * 4;
    const out_dim = dim * 2;
    const merged_ct = try cb.fromFloat32Shape(merged, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    allocator.free(merged);
    defer cb.free(merged_ct);

    const norm_w_ct = try cb.getWeight(norm_w);
    defer cb.free(norm_w_ct);
    const norm_b_ct = try cb.getWeight(norm_b);
    defer cb.free(norm_b_ct);
    const normed_ct = try cb.layerNorm(merged_ct, norm_w_ct, norm_b_ct, in_dim, 1e-5);
    defer cb.free(normed_ct);

    const reduction_w = try fmtAlloc(allocator, "audio_model.audio_encoder.layers.{d}.downsample.reduction.weight", .{stage});
    defer allocator.free(reduction_w);
    const reduction_w_ct = try cb.getWeight(reduction_w);
    defer cb.free(reduction_w_ct);
    const reduction_ct = try cb.linearNoBias(normed_ct, reduction_w_ct, rows, in_dim, out_dim);
    defer cb.free(reduction_ct);
    return cb.toFloat32(reduction_ct, allocator);
}

fn convOutputToTokens(
    allocator: std.mem.Allocator,
    out: []const f32,
    out_h: usize,
    out_w: usize,
    dim_u32: u32,
) ![]f32 {
    const dim: usize = dim_u32;
    const tokens = try allocator.alloc(f32, out_h * out_w * dim);
    errdefer allocator.free(tokens);
    for (0..out_h) |y| {
        for (0..out_w) |x| {
            const token_idx = (y * out_w + x) * dim;
            const src_base = y * out_w + x;
            for (0..dim) |c| {
                tokens[token_idx + c] = out[src_base + c * out_h * out_w];
            }
        }
    }
    return tokens;
}

fn tokensToImage(
    allocator: std.mem.Allocator,
    tokens: []const f32,
    height: usize,
    width: usize,
    channels: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, channels * height * width);
    errdefer allocator.free(out);
    for (0..height) |y| {
        for (0..width) |x| {
            const tok = (y * width + x) * channels;
            const dst_base = y * width + x;
            for (0..channels) |c| out[c * height * width + dst_base] = tokens[tok + c];
        }
    }
    return out;
}

fn imageToTokens(
    allocator: std.mem.Allocator,
    image: []const f32,
    height: usize,
    width: usize,
    channels: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, channels * height * width);
    errdefer allocator.free(out);
    for (0..height) |y| {
        for (0..width) |x| {
            const dst = (y * width + x) * channels;
            const src_base = y * width + x;
            for (0..channels) |c| out[dst + c] = image[c * height * width + src_base];
        }
    }
    return out;
}

fn affBranch(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    xa_tokens: []const f32,
    height: usize,
    width: usize,
    channels: usize,
    prefix: []const u8,
) ![]f32 {
    const xa = try tokensToImage(allocator, xa_tokens, height, width, channels);
    defer allocator.free(xa);
    const inter = channels / 4;

    const conv0_w = try fmtAlloc(allocator, "{s}.0.weight", .{prefix});
    defer allocator.free(conv0_w);
    const conv0_b = try fmtAlloc(allocator, "{s}.0.bias", .{prefix});
    defer allocator.free(conv0_b);
    const hidden0 = try conv2dImageData(cb, allocator, xa, 1, channels, inter, height, width, 1, 1, 1, 1, 0, 0, conv0_w, conv0_b);
    defer allocator.free(hidden0);

    const bn1 = try batchNorm2dNamed(allocator, hidden0, 1, inter, height, width, cb, try fmtAlloc(allocator, "{s}.1.weight", .{prefix}), try fmtAlloc(allocator, "{s}.1.bias", .{prefix}), try fmtAlloc(allocator, "{s}.1.running_mean", .{prefix}), try fmtAlloc(allocator, "{s}.1.running_var", .{prefix}));
    defer allocator.free(bn1);
    const relu = try reluImage(allocator, bn1);
    defer allocator.free(relu);

    const conv2_w = try fmtAlloc(allocator, "{s}.3.weight", .{prefix});
    defer allocator.free(conv2_w);
    const conv2_b = try fmtAlloc(allocator, "{s}.3.bias", .{prefix});
    defer allocator.free(conv2_b);
    const hidden1 = try conv2dImageData(cb, allocator, relu, 1, inter, channels, height, width, 1, 1, 1, 1, 0, 0, conv2_w, conv2_b);
    defer allocator.free(hidden1);

    const bn2 = try batchNorm2dNamed(allocator, hidden1, 1, channels, height, width, cb, try fmtAlloc(allocator, "{s}.4.weight", .{prefix}), try fmtAlloc(allocator, "{s}.4.bias", .{prefix}), try fmtAlloc(allocator, "{s}.4.running_mean", .{prefix}), try fmtAlloc(allocator, "{s}.4.running_var", .{prefix}));
    defer allocator.free(bn2);
    return imageToTokens(allocator, bn2, height, width, channels);
}

fn affGlobalBranch(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    xa_tokens: []const f32,
    height: usize,
    width: usize,
    channels: usize,
    prefix: []const u8,
) ![]f32 {
    const xa = try tokensToImage(allocator, xa_tokens, height, width, channels);
    defer allocator.free(xa);
    const pooled = try globalAvgPoolImage(allocator, xa, channels, height, width);
    defer allocator.free(pooled);
    const inter = channels / 4;

    const conv0_w = try fmtAlloc(allocator, "{s}.1.weight", .{prefix});
    defer allocator.free(conv0_w);
    const conv0_b = try fmtAlloc(allocator, "{s}.1.bias", .{prefix});
    defer allocator.free(conv0_b);
    const hidden0 = try conv2dImageData(cb, allocator, pooled, 1, channels, inter, 1, 1, 1, 1, 1, 1, 0, 0, conv0_w, conv0_b);
    defer allocator.free(hidden0);

    const bn1 = try batchNorm2dNamed(allocator, hidden0, 1, inter, 1, 1, cb, try fmtAlloc(allocator, "{s}.2.weight", .{prefix}), try fmtAlloc(allocator, "{s}.2.bias", .{prefix}), try fmtAlloc(allocator, "{s}.2.running_mean", .{prefix}), try fmtAlloc(allocator, "{s}.2.running_var", .{prefix}));
    defer allocator.free(bn1);
    const relu = try reluImage(allocator, bn1);
    defer allocator.free(relu);

    const conv2_w = try fmtAlloc(allocator, "{s}.4.weight", .{prefix});
    defer allocator.free(conv2_w);
    const conv2_b = try fmtAlloc(allocator, "{s}.4.bias", .{prefix});
    defer allocator.free(conv2_b);
    const hidden1 = try conv2dImageData(cb, allocator, relu, 1, inter, channels, 1, 1, 1, 1, 1, 1, 0, 0, conv2_w, conv2_b);
    defer allocator.free(hidden1);

    const bn2 = try batchNorm2dNamed(allocator, hidden1, 1, channels, 1, 1, cb, try fmtAlloc(allocator, "{s}.5.weight", .{prefix}), try fmtAlloc(allocator, "{s}.5.bias", .{prefix}), try fmtAlloc(allocator, "{s}.5.running_mean", .{prefix}), try fmtAlloc(allocator, "{s}.5.running_var", .{prefix}));
    defer allocator.free(bn2);

    const broadcast = try allocator.alloc(f32, channels * height * width);
    errdefer allocator.free(broadcast);
    for (0..channels) |c| {
        const val = bn2[c];
        for (0..height * width) |i| broadcast[c * height * width + i] = val;
    }
    return imageToTokens(allocator, broadcast, height, width, channels);
}

fn conv2dImageData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: []const f32,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    pad_h: usize,
    pad_w: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const input_ct = try cb.fromFloat32Shape(image, &[_]i32{ @intCast(batch), @intCast(in_channels), @intCast(height), @intCast(width) });
    defer cb.free(input_ct);
    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    const out_ct = try cb.conv2d(input_ct, weight, bias, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, pad_h, pad_w, 1);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn batchNorm2dNamed(
    allocator: std.mem.Allocator,
    image: []const f32,
    batch: usize,
    channels: usize,
    height: usize,
    width: usize,
    cb: *const ComputeBackend,
    weight_name: []u8,
    bias_name: []u8,
    mean_name: []u8,
    var_name: []u8,
) ![]f32 {
    defer allocator.free(weight_name);
    defer allocator.free(bias_name);
    defer allocator.free(mean_name);
    defer allocator.free(var_name);
    const weight = try weightData(cb, allocator, weight_name);
    defer allocator.free(weight);
    const bias = try weightData(cb, allocator, bias_name);
    defer allocator.free(bias);
    const mean = try weightData(cb, allocator, mean_name);
    defer allocator.free(mean);
    const var_data = try weightData(cb, allocator, var_name);
    defer allocator.free(var_data);

    const out = try allocator.alloc(f32, image.len);
    errdefer allocator.free(out);
    const plane = height * width;
    for (0..batch) |b| {
        for (0..channels) |c| {
            const denom = @sqrt(var_data[c] + 1e-5);
            const base = (b * channels + c) * plane;
            for (0..plane) |i| {
                out[base + i] = ((image[base + i] - mean[c]) / denom) * weight[c] + bias[c];
            }
        }
    }
    return out;
}

fn reluImage(allocator: std.mem.Allocator, image: []const f32) ![]f32 {
    const out = try allocator.dupe(f32, image);
    for (out) |*v| {
        if (v.* < 0) v.* = 0;
    }
    return out;
}

fn globalAvgPoolImage(
    allocator: std.mem.Allocator,
    image: []const f32,
    channels: usize,
    height: usize,
    width: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, channels);
    errdefer allocator.free(out);
    const plane = height * width;
    const scale = 1.0 / @as(f32, @floatFromInt(plane));
    for (0..channels) |c| {
        var sum: f32 = 0;
        for (0..plane) |i| sum += image[c * plane + i];
        out[c] = sum * scale;
    }
    return out;
}

fn embeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input_ids: []const i64,
    token_type_ids: ?[]const i64,
    total: usize,
    seq_len: usize,
    H: u32,
    pad_token_id: i32,
) !CT {
    const word_emb = try cb.getWeight("text_model.embeddings.word_embeddings.weight");
    defer cb.free(word_emb);
    var result = try cb.embeddingLookup(word_emb, input_ids, total, H);

    const pos_emb = try cb.getWeight("text_model.embeddings.position_embeddings.weight");
    defer cb.free(pos_emb);
    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    buildRobertaPositionIds(pos_ids, input_ids, seq_len, pad_token_id);
    const pos_lookup = try cb.embeddingLookup(pos_emb, pos_ids, total, H);
    defer cb.free(pos_lookup);

    const with_pos = try cb.add(result, pos_lookup);
    cb.free(result);
    result = with_pos;

    if (cb.getWeight("text_model.embeddings.token_type_embeddings.weight")) |tt_emb| {
        defer cb.free(tt_emb);
        const tt_ids = try allocator.alloc(i64, total);
        defer allocator.free(tt_ids);
        if (token_type_ids) |tids| {
            @memcpy(tt_ids, tids[0..total]);
        } else {
            @memset(tt_ids, 0);
        }
        const tt_lookup = try cb.embeddingLookup(tt_emb, tt_ids, total, H);
        defer cb.free(tt_lookup);
        const with_tt = try cb.add(result, tt_lookup);
        cb.free(result);
        result = with_tt;
    } else |_| {}

    const ln_w = try cb.getWeight("text_model.embeddings.LayerNorm.weight");
    defer cb.free(ln_w);
    const ln_b = try cb.getWeight("text_model.embeddings.LayerNorm.bias");
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(result, ln_w, ln_b, H, 1e-12);
    cb.free(result);
    return normed;
}

fn buildRobertaPositionIds(
    out: []i64,
    input_ids: []const i64,
    seq_len: usize,
    pad_token_id: i32,
) void {
    std.debug.assert(out.len == input_ids.len);
    const pad_i64: i64 = @intCast(pad_token_id);
    const batch = if (seq_len == 0) 0 else input_ids.len / seq_len;
    for (0..batch) |b| {
        var next_pos: i64 = pad_i64 + 1;
        const row_base = b * seq_len;
        for (0..seq_len) |i| {
            const idx = row_base + i;
            if (input_ids[idx] == pad_i64) {
                out[idx] = pad_i64;
            } else {
                out[idx] = next_pos;
                next_pos += 1;
            }
        }
    }
}

test "build roberta position ids preserves padding" {
    const input = [_]i64{ 0, 314, 88, 1, 1, 5, 1, 7 };
    var out: [input.len]i64 = undefined;
    buildRobertaPositionIds(&out, &input, 4, 1);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 4, 1, 1, 2, 1, 3 }, &out);
}

fn encoderLayer(
    cb: *const ComputeBackend,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
    cfg: bert_mod.Config,
) !CT {
    const H = cfg.hidden_size;
    const num_heads = cfg.num_attention_heads;
    const head_dim = H / num_heads;
    const I = cfg.intermediate_size;
    const total = batch * seq_len;
    var q_w_buf: [256]u8 = undefined;
    var q_b_buf: [256]u8 = undefined;
    var k_w_buf: [256]u8 = undefined;
    var k_b_buf: [256]u8 = undefined;
    var v_w_buf: [256]u8 = undefined;
    var v_b_buf: [256]u8 = undefined;
    const q_w = try getLayerWeight(cb, layer, "attention.self.query.weight", &q_w_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, layer, "attention.self.query.bias", &q_b_buf);
    defer cb.free(q_b);
    const k_w = try getLayerWeight(cb, layer, "attention.self.key.weight", &k_w_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, layer, "attention.self.key.bias", &k_b_buf);
    defer cb.free(k_b);
    const v_w = try getLayerWeight(cb, layer, "attention.self.value.weight", &v_w_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, layer, "attention.self.value.bias", &v_b_buf);
    defer cb.free(v_b);

    const qkv = try cb.linearTriple(hidden, q_w, q_b, k_w, k_b, v_w, v_b, total, H, H);
    const Q = qkv.first;
    const K = qkv.second;
    const V = qkv.third;
    defer cb.free(Q);
    defer cb.free(K);
    defer cb.free(V);

    const attn_out = try cb.scaledDotProductAttention(Q, K, V, attention_mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn_out);

    var attn_proj_w_buf: [256]u8 = undefined;
    var attn_proj_b_buf: [256]u8 = undefined;
    const attn_proj_w = try getLayerWeight(cb, layer, "attention.output.dense.weight", &attn_proj_w_buf);
    defer cb.free(attn_proj_w);
    const attn_proj_b = try getLayerWeight(cb, layer, "attention.output.dense.bias", &attn_proj_b_buf);
    defer cb.free(attn_proj_b);
    const attn_proj = try cb.linear(attn_out, attn_proj_w, attn_proj_b, total, H, H);
    defer cb.free(attn_proj);

    const attn_res = try cb.add(attn_proj, hidden);
    defer cb.free(attn_res);

    var attn_ln_w_buf: [256]u8 = undefined;
    var attn_ln_b_buf: [256]u8 = undefined;
    const attn_ln_w = try getLayerWeight(cb, layer, "attention.output.LayerNorm.weight", &attn_ln_w_buf);
    defer cb.free(attn_ln_w);
    const attn_ln_b = try getLayerWeight(cb, layer, "attention.output.LayerNorm.bias", &attn_ln_b_buf);
    defer cb.free(attn_ln_b);
    const attn_normed = try cb.layerNorm(attn_res, attn_ln_w, attn_ln_b, H, 1e-12);

    var ffn_i_w_buf: [256]u8 = undefined;
    var ffn_i_b_buf: [256]u8 = undefined;
    const ffn_i_w = try getLayerWeight(cb, layer, "intermediate.dense.weight", &ffn_i_w_buf);
    defer cb.free(ffn_i_w);
    const ffn_i_b = try getLayerWeight(cb, layer, "intermediate.dense.bias", &ffn_i_b_buf);
    defer cb.free(ffn_i_b);
    const ffn_inter = try cb.linear(attn_normed, ffn_i_w, ffn_i_b, total, H, I);
    defer cb.free(ffn_inter);

    const ffn_gelu = try cb.gelu(ffn_inter);
    defer cb.free(ffn_gelu);

    var ffn_o_w_buf: [256]u8 = undefined;
    var ffn_o_b_buf: [256]u8 = undefined;
    const ffn_o_w = try getLayerWeight(cb, layer, "output.dense.weight", &ffn_o_w_buf);
    defer cb.free(ffn_o_w);
    const ffn_o_b = try getLayerWeight(cb, layer, "output.dense.bias", &ffn_o_b_buf);
    defer cb.free(ffn_o_b);
    const ffn_out = try cb.linear(ffn_gelu, ffn_o_w, ffn_o_b, total, I, H);
    defer cb.free(ffn_out);

    const ffn_res = try cb.add(ffn_out, attn_normed);
    cb.free(attn_normed);
    defer cb.free(ffn_res);

    var ffn_ln_w_buf: [256]u8 = undefined;
    var ffn_ln_b_buf: [256]u8 = undefined;
    const ffn_ln_w = try getLayerWeight(cb, layer, "output.LayerNorm.weight", &ffn_ln_w_buf);
    defer cb.free(ffn_ln_w);
    const ffn_ln_b = try getLayerWeight(cb, layer, "output.LayerNorm.bias", &ffn_ln_b_buf);
    defer cb.free(ffn_ln_b);
    return try cb.layerNorm(ffn_res, ffn_ln_w, ffn_ln_b, H, 1e-12);
}

fn getLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "text_model.encoder.layer.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

fn batchNormInput(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    input: []const f32,
    batch: usize,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
) ![]f32 {
    const weight = try weightData(cb, allocator, "audio_model.audio_encoder.batch_norm.weight");
    defer allocator.free(weight);
    const bias = try weightData(cb, allocator, "audio_model.audio_encoder.batch_norm.bias");
    defer allocator.free(bias);
    const mean = try weightData(cb, allocator, "audio_model.audio_encoder.batch_norm.running_mean");
    defer allocator.free(mean);
    const var_data = try weightData(cb, allocator, "audio_model.audio_encoder.batch_norm.running_var");
    defer allocator.free(var_data);

    const out = try allocator.alloc(f32, input.len);
    errdefer allocator.free(out);
    for (0..batch) |b| {
        for (0..channels) |c| {
            for (0..time_frames) |t| {
                for (0..mel_bins) |m| {
                    const idx = (((b * channels + c) * time_frames + t) * mel_bins + m);
                    const denom = @sqrt(var_data[m] + 1e-5);
                    out[idx] = ((input[idx] - mean[m]) / denom) * weight[m] + bias[m];
                }
            }
        }
    }
    return out;
}

fn reshapeMelToImage(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    channels: usize,
    time_frames: usize,
    mel_bins: usize,
    spec_size_u32: u32,
) ![]f32 {
    const spec_size: usize = spec_size_u32;
    const freq_ratio = spec_size / mel_bins;
    const spec_width = spec_size * freq_ratio;
    const resized_owned = if (time_frames != spec_width)
        try resizeTimeAxis(allocator, input, batch, channels, time_frames, mel_bins, spec_width)
    else
        null;
    defer if (resized_owned) |resized| allocator.free(resized);
    const resized = resized_owned orelse input;

    const out = try allocator.alloc(f32, batch * channels * spec_size * spec_size);
    errdefer allocator.free(out);
    const chunk_width = spec_width / freq_ratio;
    for (0..batch) |b| {
        for (0..channels) |c| {
            for (0..spec_width) |t| {
                const group = t / chunk_width;
                const dst_x = t % chunk_width;
                for (0..mel_bins) |m| {
                    const src_idx = (((b * channels + c) * spec_width + t) * mel_bins + m);
                    const dst_y = group * mel_bins + m;
                    const dst_idx = (((b * channels + c) * spec_size + dst_y) * spec_size + dst_x);
                    out[dst_idx] = resized[src_idx];
                }
            }
        }
    }
    return out;
}

test "reshape mel to image matches clap chunked time layout" {
    const allocator = std.testing.allocator;
    const input = [_]f32{
        0,  1,
        10, 11,
        20, 21,
        30, 31,
        40, 41,
        50, 51,
        60, 61,
        70, 71,
    };
    const out = try reshapeMelToImage(allocator, &input, 1, 1, 8, 2, 4);
    defer allocator.free(out);

    try std.testing.expectEqualSlices(f32, &[_]f32{
        0,  10, 20, 30,
        1,  11, 21, 31,
        40, 50, 60, 70,
        41, 51, 61, 71,
    }, out);
}

test "window attention reuses shifted mask across batch windows" {
    const allocator = std.testing.allocator;
    const num_windows: usize = 2;
    const window_size: usize = 2;
    const window_area = window_size * window_size;
    const dim: usize = 2;
    const num_heads: usize = 1;

    var q: [num_windows * window_area * dim]f32 = undefined;
    var k: [num_windows * window_area * dim]f32 = undefined;
    var v: [num_windows * window_area * dim]f32 = undefined;
    for (&q, 0..) |*value, idx| value.* = @as(f32, @floatFromInt(idx % 5)) / 5.0;
    for (&k, 0..) |*value, idx| value.* = @as(f32, @floatFromInt(idx % 7)) / 7.0;
    for (&v, 0..) |*value, idx| value.* = @as(f32, @floatFromInt(idx % 11)) / 11.0;

    var rel_bias: [(2 * window_size - 1) * (2 * window_size - 1) * num_heads]f32 = undefined;
    @memset(&rel_bias, 0.0);
    var single_image_mask: [window_area * window_area]f32 = undefined;
    @memset(&single_image_mask, 0.0);
    single_image_mask[1] = -100.0;

    const out = try windowAttention(
        allocator,
        &q,
        &k,
        &v,
        &rel_bias,
        &single_image_mask,
        num_windows,
        window_area,
        dim,
        num_heads,
        window_size,
    );
    defer allocator.free(out);
    try std.testing.expectEqual(q.len, out.len);
}

fn resizeTimeAxis(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    channels: usize,
    old_time: usize,
    mel_bins: usize,
    new_time: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, batch * channels * new_time * mel_bins);
    errdefer allocator.free(out);
    for (0..batch) |b| {
        for (0..channels) |c| {
            for (0..new_time) |t| {
                const src = if (new_time == 1)
                    0.0
                else
                    (@as(f32, @floatFromInt(t)) * @as(f32, @floatFromInt(old_time - 1))) / @as(f32, @floatFromInt(new_time - 1));
                const center: isize = @intFromFloat(@floor(src));
                const offsets = [_]isize{ -1, 0, 1, 2 };
                for (0..mel_bins) |m| {
                    var value: f32 = 0;
                    for (offsets) |offset| {
                        const src_idx = clampTimeIndex(center + offset, old_time);
                        const sample = input[(((b * channels + c) * old_time + src_idx) * mel_bins + m)];
                        value += sample * cubicWeight(src - @as(f32, @floatFromInt(center + offset)));
                    }
                    out[(((b * channels + c) * new_time + t) * mel_bins + m)] = value;
                }
            }
        }
    }
    return out;
}

fn cubicWeight(x: f32) f32 {
    const a: f32 = -0.75;
    const ax = @abs(x);
    if (ax <= 1.0) {
        return ((a + 2.0) * ax * ax * ax) - ((a + 3.0) * ax * ax) + 1.0;
    }
    if (ax < 2.0) {
        return (a * ax * ax * ax) - (5.0 * a * ax * ax) + (8.0 * a * ax) - (4.0 * a);
    }
    return 0.0;
}

fn clampTimeIndex(idx: isize, old_time: usize) usize {
    if (idx < 0) return 0;
    const bounded: usize = @intCast(idx);
    return @min(bounded, old_time - 1);
}

fn weightData(cb: *const ComputeBackend, allocator: std.mem.Allocator, name: []const u8) ![]f32 {
    const weight = try cb.getWeight(name);
    defer cb.free(weight);
    return cb.toFloat32(weight, allocator);
}

fn linearNamed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    const out_ct = try cb.linear(input, weight, bias, rows, in_dim, out_dim);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn linearNamedData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const input_ct = try cb.fromFloat32Shape(input, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer cb.free(input_ct);
    return linearNamed(cb, allocator, input_ct, rows, in_dim, out_dim, weight_name, bias_name);
}

const LinearTripleData = struct {
    first: []f32,
    second: []f32,
    third: []f32,

    fn deinit(self: LinearTripleData, allocator: std.mem.Allocator) void {
        allocator.free(self.first);
        allocator.free(self.second);
        allocator.free(self.third);
    }
};

fn linearTripleNamedData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight_a_name: []const u8,
    bias_a_name: []const u8,
    weight_b_name: []const u8,
    bias_b_name: []const u8,
    weight_c_name: []const u8,
    bias_c_name: []const u8,
) !LinearTripleData {
    const input_ct = try cb.fromFloat32Shape(input, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer cb.free(input_ct);
    const weight_a = try cb.getWeight(weight_a_name);
    defer cb.free(weight_a);
    const bias_a = try cb.getWeight(bias_a_name);
    defer cb.free(bias_a);
    const weight_b = try cb.getWeight(weight_b_name);
    defer cb.free(weight_b);
    const bias_b = try cb.getWeight(bias_b_name);
    defer cb.free(bias_b);
    const weight_c = try cb.getWeight(weight_c_name);
    defer cb.free(weight_c);
    const bias_c = try cb.getWeight(bias_c_name);
    defer cb.free(bias_c);

    const out_ct = try cb.linearTriple(input_ct, weight_a, bias_a, weight_b, bias_b, weight_c, bias_c, rows, in_dim, out_dim);
    defer {
        cb.free(out_ct.first);
        cb.free(out_ct.second);
        cb.free(out_ct.third);
    }

    const first = try cb.toFloat32(out_ct.first, allocator);
    errdefer allocator.free(first);
    const second = try cb.toFloat32(out_ct.second, allocator);
    errdefer allocator.free(second);
    const third = try cb.toFloat32(out_ct.third, allocator);
    return .{ .first = first, .second = second, .third = third };
}

fn linearNamedNoBiasData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight_name: []const u8,
) ![]f32 {
    const input_ct = try cb.fromFloat32Shape(input, &[_]i32{ @intCast(rows), @intCast(in_dim) });
    defer cb.free(input_ct);
    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const out_ct = try cb.linearNoBias(input_ct, weight, rows, in_dim, out_dim);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn layerNormData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    dim: usize,
    weight_name: []const u8,
    bias_name: []const u8,
    eps: f32,
) ![]f32 {
    const input_ct = try cb.fromFloat32Shape(input, &[_]i32{ @intCast(rows), @intCast(dim) });
    defer cb.free(input_ct);
    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    const out_ct = try cb.layerNorm(input_ct, weight, bias, dim, eps);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn reluData(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: []const f32, rows: usize, dim: usize) ![]f32 {
    const input_ct = try cb.fromFloat32Shape(input, &[_]i32{ @intCast(rows), @intCast(dim) });
    defer cb.free(input_ct);
    const out_ct = try cb.relu(input_ct);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn geluData(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: []const f32, rows: usize, dim: usize) ![]f32 {
    const input_ct = try cb.fromFloat32Shape(input, &[_]i32{ @intCast(rows), @intCast(dim) });
    defer cb.free(input_ct);
    const out_ct = try cb.gelu(input_ct);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn avgPoolTokens(allocator: std.mem.Allocator, input: []const f32, batch: usize, seq_len: usize, dim: usize) ![]f32 {
    const out = try allocator.alloc(f32, batch * dim);
    errdefer allocator.free(out);
    @memset(out, 0);
    const scale = 1.0 / @as(f32, @floatFromInt(seq_len));
    // Sum across the seq_len axis with a SIMD inner loop, then scale once at
    // the end.  Folding the scale into a single post-pass divides the FMA
    // count by seq_len (~256 for CLAP audio embeddings) and avoids losing
    // precision from repeated rounded scaling.
    const VEC = 8;
    const Vec = @Vector(VEC, f32);
    for (0..batch) |b| {
        const dst = out[b * dim ..][0..dim];
        for (0..seq_len) |t| {
            const src = input[(b * seq_len + t) * dim ..][0..dim];
            var i: usize = 0;
            while (i + VEC <= dim) : (i += VEC) {
                const dv: Vec = dst[i..][0..VEC].*;
                const sv: Vec = src[i..][0..VEC].*;
                dst[i..][0..VEC].* = dv + sv;
            }
            while (i < dim) : (i += 1) dst[i] += src[i];
        }
        const scale_v: Vec = @splat(scale);
        var i: usize = 0;
        while (i + VEC <= dim) : (i += VEC) {
            const dv: Vec = dst[i..][0..VEC].*;
            dst[i..][0..VEC].* = dv * scale_v;
        }
        while (i < dim) : (i += 1) dst[i] *= scale;
    }
    return out;
}

fn cyclicShiftTokens(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    shift: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, input.len);
    errdefer allocator.free(out);
    if (shift == 0) {
        @memcpy(out, input);
        return out;
    }
    // The x loop wraps modulo width, but it splits into two contiguous halves:
    //   x in [0,         width-shift)  reads from [shift, width)
    //   x in [width-shift, width)      reads from [0,     shift)
    // So per row we can replace `width` 1-element memcpys with 2 contiguous
    // memcpys.  For Swin's 14×14 grid with shift=ws/2=3, this turns 196
    // memcpy calls per (b, channel) into 28.
    const head_x = width - shift;
    const head_floats = head_x * dim;
    const tail_floats = shift * dim;
    for (0..batch) |b| {
        for (0..height) |y| {
            const src_y = (y + shift) % height;
            const row_dst_off = (b * height * width + y * width) * dim;
            const row_src_off = (b * height * width + src_y * width) * dim;
            // First chunk: out[0..head_x] ← in[shift..width]
            @memcpy(out[row_dst_off..][0..head_floats], input[row_src_off + shift * dim ..][0..head_floats]);
            // Second chunk: out[head_x..width] ← in[0..shift]
            @memcpy(out[row_dst_off + head_floats ..][0..tail_floats], input[row_src_off..][0..tail_floats]);
        }
    }
    return out;
}

fn windowPartition(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    window_size: usize,
) ![]f32 {
    if (window_size == 0 or height % window_size != 0 or width % window_size != 0) return error.InvalidInputShape;
    if (input.len != batch * height * width * dim) return error.InvalidInputShape;
    const windows_h = height / window_size;
    const windows_w = width / window_size;
    const window_area = window_size * window_size;
    const out = try allocator.alloc(f32, batch * windows_h * windows_w * window_area * dim);
    errdefer allocator.free(out);
    // The dx loop reads a contiguous span of `window_size * dim` floats from
    // the input row at (wh*ws+dy) and writes a contiguous span at
    // (window_index, dy).  Collapsing it into a single memcpy turns
    // window_size memcpys per dy into one, and window_size² per (wh, ww)
    // into window_size — a 7x reduction for Swin's typical ws=7.
    const row_floats = window_size * dim;
    var window_index: usize = 0;
    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                for (0..window_size) |dy| {
                    const src_y = wh * window_size + dy;
                    const src_x_start = ww * window_size;
                    const src_off = (b * height * width + src_y * width + src_x_start) * dim;
                    const dst_off = (window_index * window_area + dy * window_size) * dim;
                    @memcpy(out[dst_off..][0..row_floats], input[src_off..][0..row_floats]);
                }
                window_index += 1;
            }
        }
    }
    return out;
}

fn windowUnpartition(
    allocator: std.mem.Allocator,
    windows: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    window_size: usize,
) ![]f32 {
    if (window_size == 0 or height % window_size != 0 or width % window_size != 0) return error.InvalidInputShape;
    const out = try allocator.alloc(f32, batch * height * width * dim);
    errdefer allocator.free(out);
    const windows_h = height / window_size;
    const windows_w = width / window_size;
    const window_area = window_size * window_size;
    if (windows.len != batch * windows_h * windows_w * window_area * dim) return error.InvalidInputShape;
    // Mirror of windowPartition: the dx loop is one contiguous memcpy.
    const row_floats = window_size * dim;
    var window_index: usize = 0;
    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                for (0..window_size) |dy| {
                    const dst_y = wh * window_size + dy;
                    const dst_x_start = ww * window_size;
                    const dst_off = (b * height * width + dst_y * width + dst_x_start) * dim;
                    const src_off = (window_index * window_area + dy * window_size) * dim;
                    @memcpy(out[dst_off..][0..row_floats], windows[src_off..][0..row_floats]);
                }
                window_index += 1;
            }
        }
    }
    return out;
}

fn buildShiftedWindowMask(
    allocator: std.mem.Allocator,
    height: usize,
    width: usize,
    window_size: usize,
    shift_size: usize,
) ![]f32 {
    if (window_size == 0 or shift_size >= window_size or height % window_size != 0 or width % window_size != 0) return error.InvalidInputShape;
    const regions = try allocator.alloc(i32, height * width);
    defer allocator.free(regions);
    var count: i32 = 0;
    const h_slices = [_][2]usize{ .{ 0, height - window_size }, .{ height - window_size, height - shift_size }, .{ height - shift_size, height } };
    const w_slices = [_][2]usize{ .{ 0, width - window_size }, .{ width - window_size, width - shift_size }, .{ width - shift_size, width } };
    for (h_slices) |hs| {
        for (w_slices) |ws| {
            for (hs[0]..hs[1]) |y| {
                for (ws[0]..ws[1]) |x| regions[y * width + x] = count;
            }
            count += 1;
        }
    }

    const shifted = try allocator.alloc(i32, height * width);
    defer allocator.free(shifted);
    for (0..height) |y| {
        for (0..width) |x| {
            shifted[y * width + x] = regions[((y + shift_size) % height) * width + ((x + shift_size) % width)];
        }
    }

    const windows_h = height / window_size;
    const windows_w = width / window_size;
    const num_windows = windows_h * windows_w;
    const window_area = window_size * window_size;
    const out = try allocator.alloc(f32, num_windows * window_area * window_area);
    errdefer allocator.free(out);
    var win: usize = 0;
    const ids = try allocator.alloc(i32, window_area);
    defer allocator.free(ids);
    for (0..windows_h) |wh| {
        for (0..windows_w) |ww| {
            for (0..window_size) |dy| {
                for (0..window_size) |dx| {
                    ids[dy * window_size + dx] = shifted[(wh * window_size + dy) * width + (ww * window_size + dx)];
                }
            }
            for (0..window_area) |i| {
                for (0..window_area) |j| {
                    out[(win * window_area + i) * window_area + j] = if (ids[i] == ids[j]) 0.0 else -100.0;
                }
            }
            win += 1;
        }
    }
    return out;
}

fn windowAttention(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    rel_bias: []const f32,
    attn_mask: ?[]const f32,
    num_windows: usize,
    window_area: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
) ![]f32 {
    if (num_heads == 0 or dim % num_heads != 0) return error.InvalidInputShape;
    if (Q.len != num_windows * window_area * dim or K.len != Q.len or V.len != Q.len) return error.InvalidInputShape;
    if (window_size == 0 or window_area != window_size * window_size) return error.InvalidInputShape;
    const out = try allocator.alloc(f32, Q.len);
    errdefer allocator.free(out);
    const head_dim = dim / num_heads;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const scores = try allocator.alloc(f32, window_area);
    defer allocator.free(scores);

    // Q@K^T uses primitives.dotPtrs (SIMD), softmax uses primitives.softmaxRow
    // (vectorized exp), V projection uses axpyPtrs.  The original loop did
    // all three with per-element scalar arithmetic; this is the hottest
    // inner kernel in CLAP audio Swin attention.
    const linalg_prim = @import("inference_linalg").primitives;
    const mask_windows = if (attn_mask) |m| blk: {
        const mask_window_elems = window_area * window_area;
        if (mask_window_elems == 0 or m.len == 0 or m.len % mask_window_elems != 0) return error.InvalidInputShape;
        break :blk m.len / mask_window_elems;
    } else 0;
    for (0..num_windows) |win| {
        const mask_base = if (attn_mask) |m| blk: {
            const mask_win = win % mask_windows;
            break :blk m[mask_win * window_area * window_area ..][0 .. window_area * window_area];
        } else null;
        for (0..num_heads) |head| {
            for (0..window_area) |i| {
                const q_base = ((win * window_area + i) * dim) + head * head_dim;
                for (0..window_area) |j| {
                    const k_base = ((win * window_area + j) * dim) + head * head_dim;
                    const dot = linalg_prim.dotPtrs(Q[q_base..].ptr, K[k_base..].ptr, head_dim);
                    var score = dot * scale + relativeBias(rel_bias, window_size, num_heads, head, i, j);
                    if (mask_base) |m| score += m[i * window_area + j];
                    scores[j] = score;
                }
                // In-place numerically-stable softmax via SIMD expVec.
                linalg_prim.softmaxRow(scores[0..window_area]);

                const out_base = ((win * window_area + i) * dim) + head * head_dim;
                @memset(out[out_base..][0..head_dim], 0);
                for (0..window_area) |j| {
                    const w = scores[j];
                    if (w == 0.0) continue;
                    const v_base = ((win * window_area + j) * dim) + head * head_dim;
                    linalg_prim.axpyPtrs(w, V[v_base..].ptr, out[out_base..].ptr, head_dim);
                }
            }
        }
    }

    return out;
}

fn relativeBias(
    table: []const f32,
    window_size: usize,
    num_heads: usize,
    head: usize,
    token_i: usize,
    token_j: usize,
) f32 {
    const yi = token_i / window_size;
    const xi = token_i % window_size;
    const yj = token_j / window_size;
    const xj = token_j % window_size;
    const dy = yi + window_size - 1 - yj;
    const dx = xi + window_size - 1 - xj;
    const index = (dy * (2 * window_size - 1) + dx) * num_heads + head;
    return table[index];
}

fn addInPlace(dst: []f32, src: []const f32) void {
    for (dst, src) |*d, s| d.* += s;
}

fn fmtAlloc(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ![]u8 {
    return std.fmt.allocPrint(allocator, fmt, args);
}
