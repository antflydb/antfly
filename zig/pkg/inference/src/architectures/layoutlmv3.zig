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
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const cfg_mod = @import("../models/layoutlmv3.zig");
const mlx_compute_mod = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig") else struct {};

pub const Config = cfg_mod.Config;

pub const ForwardOutput = struct {
    hidden: []f32,
    seq_len: usize,
};

pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    bbox: []const i64,
    pixel_values: ?[]const f32,
    batch: usize,
    text_seq_len: usize,
) !ForwardOutput {
    const H = config.hidden_size;
    const total_text = batch * text_seq_len;
    const tp_world_size = tensorParallelWorldSize(cb);
    const global_num_heads: usize = @intCast(config.num_attention_heads);
    const local_num_heads = if (tp_world_size > 1) global_num_heads / tp_world_size else global_num_heads;
    const head_start = tensorParallelRank(cb) * local_num_heads;
    var hidden = try textEmbeddings(cb, allocator, config, input_ids, token_type_ids, bbox, total_text, text_seq_len, H);
    errdefer cb.free(hidden);

    var final_attention_mask = try allocator.dupe(i64, attention_mask[0 .. batch * text_seq_len]);
    defer allocator.free(final_attention_mask);
    var final_bbox = try allocator.dupe(i64, bbox[0 .. batch * text_seq_len * 4]);
    defer allocator.free(final_bbox);
    var final_position_ids = try buildSequentialPositionIds(allocator, batch, text_seq_len);
    defer allocator.free(final_position_ids);

    var seq_len = text_seq_len;
    if (pixel_values) |pixels| {
        const visual = try forwardImage(cb, allocator, config, pixels, batch);
        defer cb.free(visual.embeddings);
        const merged = try cb.concatRows2D(allocator, hidden, visual.embeddings, total_text, batch * visual.seq_len, H);
        cb.free(hidden);
        hidden = merged;
        seq_len += visual.seq_len;

        final_attention_mask = try extendAttentionMask(allocator, final_attention_mask, batch, text_seq_len, visual.seq_len);
        final_bbox = try extendBboxWithVisual(allocator, config, final_bbox, batch, text_seq_len, visual.patch_grid_h, visual.patch_grid_w);
        final_position_ids = try extendPositionIdsWithVisual(allocator, final_position_ids, batch, text_seq_len, visual.seq_len);

        var top_name_buf: [256]u8 = undefined;
        const top_ln_w = try getPrefixedWeight(cb, config.effectivePrefix(), "LayerNorm.weight", &top_name_buf);
        defer cb.free(top_ln_w);
        var top_bias_buf: [256]u8 = undefined;
        const top_ln_b = try getPrefixedWeight(cb, config.effectivePrefix(), "LayerNorm.bias", &top_bias_buf);
        defer cb.free(top_ln_b);
        const renormed = try cb.layerNorm(hidden, top_ln_w, top_ln_b, H, config.layer_norm_eps);
        cb.free(hidden);
        hidden = renormed;
    }

    const attn_bias = try buildAttentionBias(cb, allocator, config, final_position_ids, final_bbox, batch, seq_len, head_start, local_num_heads);
    defer if (attn_bias) |bias| cb.free(bias);

    for (0..config.num_hidden_layers) |layer| {
        const next = try encoderLayer(cb, allocator, config, hidden, final_attention_mask, attn_bias, batch, seq_len, layer);
        cb.free(hidden);
        hidden = next;
    }
    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return .{ .hidden = result, .seq_len = seq_len };
}

const VisualEmbeddings = struct {
    embeddings: CT,
    seq_len: usize,
    patch_grid_h: usize,
    patch_grid_w: usize,
};

fn textEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    token_type_ids: ?[]const i64,
    bbox: []const i64,
    total: usize,
    seq_len: usize,
    H: u32,
) !CT {
    const prefix = config.effectivePrefix();
    var name_buf: [256]u8 = undefined;
    const word_emb = try getPrefixedWeight(cb, prefix, "embeddings.word_embeddings.weight", &name_buf);
    defer cb.free(word_emb);
    var result = try cb.embeddingLookup(word_emb, input_ids, total, H);

    const pos_emb = try getPrefixedWeight(cb, prefix, "embeddings.position_embeddings.weight", &name_buf);
    defer cb.free(pos_emb);
    const pos_ids = try buildTextPositionIds(allocator, config, input_ids, total, seq_len);
    defer allocator.free(pos_ids);
    const pos_lookup = try cb.embeddingLookup(pos_emb, pos_ids, total, H);
    defer cb.free(pos_lookup);
    const with_pos = try cb.add(result, pos_lookup);
    cb.free(result);
    result = with_pos;

    const tt_emb = try getPrefixedWeight(cb, prefix, "embeddings.token_type_embeddings.weight", &name_buf);
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

    const bbox_lookup = try computeBboxEmbeddings(cb, allocator, config, bbox, total, H);
    defer cb.free(bbox_lookup);
    const with_bbox = try cb.add(result, bbox_lookup);
    cb.free(result);
    result = with_bbox;

    const ln_w = try getPrefixedWeight(cb, prefix, "embeddings.LayerNorm.weight", &name_buf);
    defer cb.free(ln_w);
    const ln_b = try getPrefixedWeight(cb, prefix, "embeddings.LayerNorm.bias", &name_buf);
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(result, ln_w, ln_b, H, config.layer_norm_eps);
    cb.free(result);
    return normed;
}

fn forwardImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
) !VisualEmbeddings {
    const prefix = config.effectivePrefix();
    var name_buf: [256]u8 = undefined;
    const patch_weight = try getPrefixedWeight(cb, prefix, "patch_embed.proj.weight", &name_buf);
    defer cb.free(patch_weight);
    const patch_bias = try getPrefixedWeight(cb, prefix, "patch_embed.proj.bias", &name_buf);
    defer cb.free(patch_bias);
    const input_size: usize = @intCast(config.input_size);
    const patch_size: usize = @intCast(config.patch_size);
    const grid_h = input_size / patch_size;
    const grid_w = input_size / patch_size;
    const num_patches = grid_h * grid_w;
    const H: usize = @intCast(config.hidden_size);

    const pixel_shape = [_]i32{ @intCast(batch), @intCast(config.num_channels), @intCast(input_size), @intCast(input_size) };
    const pixels_ct = try cb.fromFloat32Shape(pixel_values, &pixel_shape);
    defer cb.free(pixels_ct);
    const conv = try cb.conv2d(
        pixels_ct,
        patch_weight,
        patch_bias,
        batch,
        config.num_channels,
        H,
        input_size,
        input_size,
        patch_size,
        patch_size,
        patch_size,
        patch_size,
        0,
        0,
        1,
    );
    defer cb.free(conv);
    const conv_data = try cb.toFloat32(conv, allocator);
    defer allocator.free(conv_data);
    if (conv_data.len != batch * H * num_patches) return error.InvalidPatchEmbeddingShape;

    const pos_embed = try getPrefixedWeight(cb, prefix, "pos_embed", &name_buf);
    defer cb.free(pos_embed);
    const pos_data = try cb.toFloat32(pos_embed, allocator);
    defer allocator.free(pos_data);
    if (pos_data.len != (num_patches + 1) * H) return error.InvalidPositionEmbeddingShape;

    const cls_token = try getPrefixedWeight(cb, prefix, "cls_token", &name_buf);
    defer cb.free(cls_token);
    const cls_data = try cb.toFloat32(cls_token, allocator);
    defer allocator.free(cls_data);
    if (cls_data.len != H) return error.InvalidClsTokenShape;

    const seq_len = num_patches + 1;
    const merged = try allocator.alloc(f32, batch * seq_len * H);
    errdefer allocator.free(merged);
    for (0..batch) |b| {
        const cls_dst = (b * seq_len) * H;
        for (0..H) |i| {
            merged[cls_dst + i] = cls_data[i] + pos_data[i];
        }
        for (0..num_patches) |patch_idx| {
            const dst = (b * seq_len + patch_idx + 1) * H;
            const pos = (patch_idx + 1) * H;
            for (0..H) |ch| {
                const src = ((b * H + ch) * num_patches) + patch_idx;
                merged[dst + ch] = conv_data[src] + pos_data[pos + ch];
            }
        }
    }
    const merged_shape = [_]i32{ @intCast(batch * seq_len), @intCast(H) };
    var result = try cb.fromFloat32Shape(merged, &merged_shape);
    defer allocator.free(merged);

    const norm_w = try getPrefixedWeight(cb, prefix, "norm.weight", &name_buf);
    defer cb.free(norm_w);
    const norm_b = try getPrefixedWeight(cb, prefix, "norm.bias", &name_buf);
    defer cb.free(norm_b);
    const normed = try cb.layerNorm(result, norm_w, norm_b, config.hidden_size, 1e-6);
    cb.free(result);
    result = normed;
    return .{
        .embeddings = result,
        .seq_len = seq_len,
        .patch_grid_h = grid_h,
        .patch_grid_w = grid_w,
    };
}

fn computeBboxEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    bbox: []const i64,
    total: usize,
    H: u32,
) !CT {
    const prefix = config.effectivePrefix();
    var name_buf: [256]u8 = undefined;
    const x_emb = try getPrefixedWeight(cb, prefix, "embeddings.x_position_embeddings.weight", &name_buf);
    defer cb.free(x_emb);
    const y_emb = try getPrefixedWeight(cb, prefix, "embeddings.y_position_embeddings.weight", &name_buf);
    defer cb.free(y_emb);
    const h_emb = try getPrefixedWeight(cb, prefix, "embeddings.h_position_embeddings.weight", &name_buf);
    defer cb.free(h_emb);
    const w_emb = try getPrefixedWeight(cb, prefix, "embeddings.w_position_embeddings.weight", &name_buf);
    defer cb.free(w_emb);

    const x0 = try allocator.alloc(i64, total);
    defer allocator.free(x0);
    const y0 = try allocator.alloc(i64, total);
    defer allocator.free(y0);
    const x1 = try allocator.alloc(i64, total);
    defer allocator.free(x1);
    const y1 = try allocator.alloc(i64, total);
    defer allocator.free(y1);
    const h = try allocator.alloc(i64, total);
    defer allocator.free(h);
    const w = try allocator.alloc(i64, total);
    defer allocator.free(w);

    const max_2d: i64 = @intCast(config.max_2d_position_embeddings - 1);
    for (0..total) |i| {
        const base = i * 4;
        const bx0 = std.math.clamp(bbox[base], @as(i64, 0), max_2d);
        const by0 = std.math.clamp(bbox[base + 1], @as(i64, 0), max_2d);
        const bx1 = std.math.clamp(bbox[base + 2], @as(i64, 0), max_2d);
        const by1 = std.math.clamp(bbox[base + 3], @as(i64, 0), max_2d);
        x0[i] = bx0;
        y0[i] = by0;
        x1[i] = bx1;
        y1[i] = by1;
        h[i] = std.math.clamp(by1 - by0, @as(i64, 0), max_2d);
        w[i] = std.math.clamp(bx1 - bx0, @as(i64, 0), max_2d);
    }

    const coord_dim: usize = @intCast(config.coordinate_size);
    const shape_dim: usize = @intCast(config.shape_size);
    const expected_hidden = coord_dim * 4 + shape_dim * 2;
    if (expected_hidden != H) return error.InvalidLayoutLmV3BBoxDims;

    const x0_lookup = try cb.embeddingLookup(x_emb, x0, total, config.coordinate_size);
    defer cb.free(x0_lookup);
    const y0_lookup = try cb.embeddingLookup(y_emb, y0, total, config.coordinate_size);
    defer cb.free(y0_lookup);
    const x1_lookup = try cb.embeddingLookup(x_emb, x1, total, config.coordinate_size);
    defer cb.free(x1_lookup);
    const y1_lookup = try cb.embeddingLookup(y_emb, y1, total, config.coordinate_size);
    defer cb.free(y1_lookup);
    const h_lookup = try cb.embeddingLookup(h_emb, h, total, config.shape_size);
    defer cb.free(h_lookup);
    const w_lookup = try cb.embeddingLookup(w_emb, w, total, config.shape_size);
    defer cb.free(w_lookup);

    const x0_f = try cb.toFloat32(x0_lookup, allocator);
    defer allocator.free(x0_f);
    const y0_f = try cb.toFloat32(y0_lookup, allocator);
    defer allocator.free(y0_f);
    const x1_f = try cb.toFloat32(x1_lookup, allocator);
    defer allocator.free(x1_f);
    const y1_f = try cb.toFloat32(y1_lookup, allocator);
    defer allocator.free(y1_f);
    const h_f = try cb.toFloat32(h_lookup, allocator);
    defer allocator.free(h_f);
    const w_f = try cb.toFloat32(w_lookup, allocator);
    defer allocator.free(w_f);

    const merged = try allocator.alloc(f32, total * H);
    defer allocator.free(merged);
    for (0..total) |row| {
        const dst = row * H;
        var cursor: usize = 0;
        @memcpy(merged[dst + cursor ..][0..coord_dim], x0_f[row * coord_dim ..][0..coord_dim]);
        cursor += coord_dim;
        @memcpy(merged[dst + cursor ..][0..coord_dim], y0_f[row * coord_dim ..][0..coord_dim]);
        cursor += coord_dim;
        @memcpy(merged[dst + cursor ..][0..coord_dim], x1_f[row * coord_dim ..][0..coord_dim]);
        cursor += coord_dim;
        @memcpy(merged[dst + cursor ..][0..coord_dim], y1_f[row * coord_dim ..][0..coord_dim]);
        cursor += coord_dim;
        @memcpy(merged[dst + cursor ..][0..shape_dim], h_f[row * shape_dim ..][0..shape_dim]);
        cursor += shape_dim;
        @memcpy(merged[dst + cursor ..][0..shape_dim], w_f[row * shape_dim ..][0..shape_dim]);
    }
    const shape = [_]i32{ @intCast(total), @intCast(H) };
    return cb.fromFloat32Shape(merged, &shape);
}

fn buildAttentionBias(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    position_ids: []const i64,
    bbox: []const i64,
    batch: usize,
    seq_len: usize,
    head_start: usize,
    head_count: usize,
) !?CT {
    if (!config.has_relative_attention_bias and !config.has_spatial_attention_bias) return null;
    if (batch != 1) return null;

    const num_heads: usize = @intCast(config.num_attention_heads);
    const head_dim: usize = @intCast(config.hidden_size / config.num_attention_heads);
    const total_values = head_count * seq_len * seq_len;
    const bias = try allocator.alloc(f32, total_values);
    @memset(bias, 0);
    errdefer allocator.free(bias);

    if (config.has_relative_attention_bias) {
        var name_buf: [256]u8 = undefined;
        const rel_w = try getPrefixedWeight(cb, config.effectivePrefix(), "encoder.rel_pos_bias.weight", &name_buf);
        defer cb.free(rel_w);
        const rel_data = try cb.toFloat32(rel_w, allocator);
        defer allocator.free(rel_data);
        try add1dBias(config, rel_data, position_ids[0..seq_len], bias, num_heads, head_start, head_count, seq_len);
    }
    if (config.has_spatial_attention_bias) {
        var name_buf_x: [256]u8 = undefined;
        const rel_x_w = try getPrefixedWeight(cb, config.effectivePrefix(), "encoder.rel_pos_x_bias.weight", &name_buf_x);
        defer cb.free(rel_x_w);
        const rel_x_data = try cb.toFloat32(rel_x_w, allocator);
        defer allocator.free(rel_x_data);
        var name_buf_y: [256]u8 = undefined;
        const rel_y_w = try getPrefixedWeight(cb, config.effectivePrefix(), "encoder.rel_pos_y_bias.weight", &name_buf_y);
        defer cb.free(rel_y_w);
        const rel_y_data = try cb.toFloat32(rel_y_w, allocator);
        defer allocator.free(rel_y_data);
        try add2dBias(config, rel_x_data, rel_y_data, bbox[0 .. seq_len * 4], bias, num_heads, head_start, head_count, seq_len);
    }

    const scale: f32 = @floatCast(@sqrt(@as(f64, @floatFromInt(head_dim))));
    for (bias) |*value| value.* /= scale;

    const shape = [_]i32{ @intCast(head_count), @intCast(seq_len), @intCast(seq_len) };
    const ct = try cb.fromFloat32Shape(bias, &shape);
    allocator.free(bias);
    return ct;
}

fn add1dBias(
    config: Config,
    weights: []const f32,
    position_ids: []const i64,
    out: []f32,
    num_heads: usize,
    head_start: usize,
    head_count: usize,
    seq_len: usize,
) !void {
    const bins: usize = @intCast(config.rel_pos_bins);
    if (weights.len != num_heads * bins) return error.InvalidRelativeBiasShape;
    for (0..seq_len) |qi| {
        for (0..seq_len) |ki| {
            const rel = position_ids[ki] - position_ids[qi];
            const bucket = relativePositionBucket(rel, true, config.rel_pos_bins, config.max_rel_pos);
            for (0..head_count) |head| {
                out[(head * seq_len + qi) * seq_len + ki] += weights[(head_start + head) * bins + bucket];
            }
        }
    }
}

fn add2dBias(
    config: Config,
    x_weights: []const f32,
    y_weights: []const f32,
    bbox: []const i64,
    out: []f32,
    num_heads: usize,
    head_start: usize,
    head_count: usize,
    seq_len: usize,
) !void {
    const bins: usize = @intCast(config.rel_2d_pos_bins);
    if (x_weights.len != num_heads * bins or y_weights.len != num_heads * bins) return error.InvalidRelativeBiasShape;
    for (0..seq_len) |qi| {
        const qx = bbox[qi * 4];
        const qy = bbox[qi * 4 + 3];
        for (0..seq_len) |ki| {
            const kx = bbox[ki * 4];
            const ky = bbox[ki * 4 + 3];
            const bx = relativePositionBucket(kx - qx, true, config.rel_2d_pos_bins, config.max_rel_2d_pos);
            const by = relativePositionBucket(ky - qy, true, config.rel_2d_pos_bins, config.max_rel_2d_pos);
            for (0..head_count) |head| {
                const idx = (head * seq_len + qi) * seq_len + ki;
                out[idx] += x_weights[(head_start + head) * bins + bx] + y_weights[(head_start + head) * bins + by];
            }
        }
    }
}

fn relativePositionBucket(relative_position: i64, bidirectional: bool, num_buckets: u32, max_distance: u32) usize {
    var buckets: i64 = num_buckets;
    var result: i64 = 0;
    var n = relative_position;
    if (bidirectional) {
        buckets = @divTrunc(buckets, 2);
        if (n > 0) result += buckets;
        n = if (n < 0) -n else n;
    } else {
        n = -@min(n, 0);
    }
    const max_exact = @divTrunc(buckets, 2);
    if (n < max_exact) return @intCast(result + n);

    const n_f: f64 = @floatFromInt(n);
    const max_exact_f: f64 = @floatFromInt(max_exact);
    const max_distance_f: f64 = @floatFromInt(max_distance);
    const buckets_f: f64 = @floatFromInt(buckets);
    const val_if_large = max_exact_f + std.math.log(f64, std.math.e, n_f / max_exact_f) / std.math.log(f64, std.math.e, max_distance_f / max_exact_f) * (buckets_f - max_exact_f);
    const bucket_large: i64 = @intFromFloat(@min(val_if_large, buckets_f - 1));
    return @intCast(result + bucket_large);
}

fn encoderLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    attn_bias: ?CT,
    batch: usize,
    seq_len: usize,
    layer: usize,
) !CT {
    const hidden_dim: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const head_dim = hidden_dim / num_heads;
    const intermediate_dim: usize = @intCast(config.intermediate_size);
    const total = batch * seq_len;
    var name_buf: [256]u8 = undefined;
    const tp_world_size = tensorParallelWorldSize(cb);
    const use_tp = tp_world_size > 1;
    if (use_tp and (hidden_dim % tp_world_size != 0 or num_heads % tp_world_size != 0 or intermediate_dim % tp_world_size != 0)) {
        return error.InvalidTensorParallelShape;
    }
    const local_num_heads = if (use_tp) num_heads / tp_world_size else num_heads;

    const q_w = try getLayerWeight(cb, allocator, config, layer, "attention.self.query.weight", &name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, allocator, config, layer, "attention.self.query.bias", &name_buf);
    defer cb.free(q_b);
    const Q = try linearReplicatedToMaybeSharded(cb, hidden, q_w, q_b, total, hidden_dim, hidden_dim);
    defer cb.free(Q);

    const k_w = try getLayerWeight(cb, allocator, config, layer, "attention.self.key.weight", &name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, allocator, config, layer, "attention.self.key.bias", &name_buf);
    defer cb.free(k_b);
    const K = try linearReplicatedToMaybeSharded(cb, hidden, k_w, k_b, total, hidden_dim, hidden_dim);
    defer cb.free(K);

    const v_w = try getLayerWeight(cb, allocator, config, layer, "attention.self.value.weight", &name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, allocator, config, layer, "attention.self.value.bias", &name_buf);
    defer cb.free(v_b);
    const V = try linearReplicatedToMaybeSharded(cb, hidden, v_w, v_b, total, hidden_dim, hidden_dim);
    defer cb.free(V);

    const attn_out = try cb.scaledDotProductAttention(Q, K, V, attention_mask, attn_bias, batch, seq_len, local_num_heads, head_dim);
    defer cb.free(attn_out);

    const attn_proj_w = try getLayerWeight(cb, allocator, config, layer, "attention.output.dense.weight", &name_buf);
    defer cb.free(attn_proj_w);
    const attn_proj_b = try getLayerWeight(cb, allocator, config, layer, "attention.output.dense.bias", &name_buf);
    defer cb.free(attn_proj_b);
    const attn_proj = try linearMaybeShardedToReplicated(cb, attn_out, attn_proj_w, attn_proj_b, total, hidden_dim, hidden_dim);
    defer cb.free(attn_proj);

    const attn_res = try cb.add(attn_proj, hidden);
    defer cb.free(attn_res);

    const attn_ln_w = try getLayerWeight(cb, allocator, config, layer, "attention.output.LayerNorm.weight", &name_buf);
    defer cb.free(attn_ln_w);
    const attn_ln_b = try getLayerWeight(cb, allocator, config, layer, "attention.output.LayerNorm.bias", &name_buf);
    defer cb.free(attn_ln_b);
    const attn_normed = try cb.layerNorm(attn_res, attn_ln_w, attn_ln_b, hidden_dim, config.layer_norm_eps);

    const ffn_i_w = try getLayerWeight(cb, allocator, config, layer, "intermediate.dense.weight", &name_buf);
    defer cb.free(ffn_i_w);
    const ffn_i_b = try getLayerWeight(cb, allocator, config, layer, "intermediate.dense.bias", &name_buf);
    defer cb.free(ffn_i_b);
    const ffn_inter = try linearReplicatedToMaybeSharded(cb, attn_normed, ffn_i_w, ffn_i_b, total, hidden_dim, intermediate_dim);
    defer cb.free(ffn_inter);

    const ffn_gelu = try cb.gelu(ffn_inter);
    defer cb.free(ffn_gelu);

    const ffn_o_w = try getLayerWeight(cb, allocator, config, layer, "output.dense.weight", &name_buf);
    defer cb.free(ffn_o_w);
    const ffn_o_b = try getLayerWeight(cb, allocator, config, layer, "output.dense.bias", &name_buf);
    defer cb.free(ffn_o_b);
    const ffn_out = try linearMaybeShardedToReplicated(cb, ffn_gelu, ffn_o_w, ffn_o_b, total, intermediate_dim, hidden_dim);
    defer cb.free(ffn_out);

    const ffn_res = try cb.add(ffn_out, attn_normed);
    cb.free(attn_normed);
    defer cb.free(ffn_res);

    const ffn_ln_w = try getLayerWeight(cb, allocator, config, layer, "output.LayerNorm.weight", &name_buf);
    defer cb.free(ffn_ln_w);
    const ffn_ln_b = try getLayerWeight(cb, allocator, config, layer, "output.LayerNorm.bias", &name_buf);
    defer cb.free(ffn_ln_b);
    return try cb.layerNorm(ffn_res, ffn_ln_w, ffn_ln_b, hidden_dim, config.layer_norm_eps);
}

fn buildTextPositionIds(
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    total: usize,
    seq_len: usize,
) ![]i64 {
    const pos_ids = try allocator.alloc(i64, total);
    const pad = config.pad_token_id;
    for (0..(total / seq_len)) |b| {
        var next: i64 = pad + 1;
        for (0..seq_len) |idx| {
            const flat = b * seq_len + idx;
            if (input_ids[flat] == pad) {
                pos_ids[flat] = pad;
            } else {
                pos_ids[flat] = next;
                next += 1;
            }
        }
    }
    return pos_ids;
}

fn buildSequentialPositionIds(allocator: std.mem.Allocator, batch: usize, seq_len: usize) ![]i64 {
    const ids = try allocator.alloc(i64, batch * seq_len);
    for (0..batch) |b| {
        for (0..seq_len) |idx| {
            ids[b * seq_len + idx] = @intCast(idx);
        }
    }
    return ids;
}

fn extendPositionIdsWithVisual(
    allocator: std.mem.Allocator,
    old_ids: []i64,
    batch: usize,
    text_seq_len: usize,
    visual_seq_len: usize,
) ![]i64 {
    allocator.free(old_ids);
    const total = text_seq_len + visual_seq_len;
    const out = try allocator.alloc(i64, batch * total);
    for (0..batch) |b| {
        for (0..text_seq_len) |idx| out[b * total + idx] = @intCast(idx);
        for (0..visual_seq_len) |idx| out[b * total + text_seq_len + idx] = @intCast(idx);
    }
    return out;
}

fn extendAttentionMask(
    allocator: std.mem.Allocator,
    old_mask: []i64,
    batch: usize,
    text_seq_len: usize,
    visual_seq_len: usize,
) ![]i64 {
    defer allocator.free(old_mask);
    const total = text_seq_len + visual_seq_len;
    const out = try allocator.alloc(i64, batch * total);
    for (0..batch) |b| {
        @memcpy(out[b * total ..][0..text_seq_len], old_mask[b * text_seq_len ..][0..text_seq_len]);
        @memset(out[b * total + text_seq_len ..][0..visual_seq_len], 1);
    }
    return out;
}

fn extendBboxWithVisual(
    allocator: std.mem.Allocator,
    config: Config,
    old_bbox: []i64,
    batch: usize,
    text_seq_len: usize,
    patch_grid_h: usize,
    patch_grid_w: usize,
) ![]i64 {
    defer allocator.free(old_bbox);
    const visual_seq_len = 1 + patch_grid_h * patch_grid_w;
    const total = text_seq_len + visual_seq_len;
    const out = try allocator.alloc(i64, batch * total * 4);
    const visual = try createVisualBbox(allocator, patch_grid_h, patch_grid_w, config.visual_bbox_max_len);
    defer allocator.free(visual);
    for (0..batch) |b| {
        @memcpy(out[(b * total * 4)..][0 .. text_seq_len * 4], old_bbox[(b * text_seq_len * 4)..][0 .. text_seq_len * 4]);
        @memcpy(out[(b * total * 4 + text_seq_len * 4)..][0 .. visual_seq_len * 4], visual);
    }
    return out;
}

fn createVisualBbox(allocator: std.mem.Allocator, height: usize, width: usize, max_len: i64) ![]i64 {
    const seq_len = 1 + height * width;
    const bbox = try allocator.alloc(i64, seq_len * 4);
    bbox[0] = 1;
    bbox[1] = 1;
    bbox[2] = max_len - 1;
    bbox[3] = max_len - 1;
    var idx: usize = 1;
    for (0..height) |y| {
        const y0 = @divTrunc(@as(i64, @intCast(y)) * max_len, @as(i64, @intCast(height)));
        const y1 = @divTrunc(@as(i64, @intCast(y + 1)) * max_len, @as(i64, @intCast(height)));
        for (0..width) |x| {
            const x0 = @divTrunc(@as(i64, @intCast(x)) * max_len, @as(i64, @intCast(width)));
            const x1 = @divTrunc(@as(i64, @intCast(x + 1)) * max_len, @as(i64, @intCast(width)));
            const base = idx * 4;
            bbox[base] = x0;
            bbox[base + 1] = y0;
            bbox[base + 2] = x1;
            bbox[base + 3] = y1;
            idx += 1;
        }
    }
    return bbox;
}

fn getLayerWeight(
    cb: *const ComputeBackend,
    _: std.mem.Allocator,
    config: Config,
    layer: usize,
    suffix: []const u8,
    buf: *[256]u8,
) !CT {
    const prefix = config.effectivePrefix();
    const name = if (prefix.len > 0)
        try std.fmt.bufPrint(buf, "{s}.encoder.layer.{d}.{s}", .{ prefix, layer, suffix })
    else
        try std.fmt.bufPrint(buf, "encoder.layer.{d}.{s}", .{ layer, suffix });
    return cb.getWeight(name);
}

fn getPrefixedWeight(
    cb: *const ComputeBackend,
    prefix: []const u8,
    suffix: []const u8,
    buf: *[256]u8,
) !CT {
    const name = if (prefix.len > 0)
        try std.fmt.bufPrint(buf, "{s}.{s}", .{ prefix, suffix })
    else
        suffix;
    return cb.getWeight(name);
}

fn tensorParallelWorldSize(cb: *const ComputeBackend) usize {
    if (!build_options.enable_mlx) return 1;
    const mlx_compute = mlx_compute_mod.MlxCompute.fromComputeBackend(cb) orelse return 1;
    if (!mlx_compute.tensorParallelEnabled()) return 1;
    return mlx_compute.tensorParallelWorldSize();
}

fn tensorParallelRank(cb: *const ComputeBackend) usize {
    if (!build_options.enable_mlx) return 0;
    const mlx_compute = mlx_compute_mod.MlxCompute.fromComputeBackend(cb) orelse return 0;
    if (!mlx_compute.tensorParallelEnabled()) return 0;
    return mlx_compute.tensorParallelRank();
}

fn linearReplicatedToMaybeSharded(
    cb: *const ComputeBackend,
    input: CT,
    weight: CT,
    bias: CT,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
) !CT {
    if (build_options.enable_mlx) {
        if (mlx_compute_mod.MlxCompute.fromComputeBackend(cb)) |mlx_compute| {
            if (mlx_compute.tensorParallelEnabled()) {
                return mlx_compute.linearTensorParallelReplicatedToSharded(input, weight, bias, rows, input_dim, output_dim);
            }
        }
    }
    return cb.linear(input, weight, bias, rows, input_dim, output_dim);
}

fn linearMaybeShardedToReplicated(
    cb: *const ComputeBackend,
    input: CT,
    weight: CT,
    bias: CT,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
) !CT {
    if (build_options.enable_mlx) {
        if (mlx_compute_mod.MlxCompute.fromComputeBackend(cb)) |mlx_compute| {
            if (mlx_compute.tensorParallelEnabled()) {
                return mlx_compute.linearTensorParallelShardedToReplicated(input, weight, bias, rows, input_dim, output_dim);
            }
        }
    }
    return cb.linear(input, weight, bias, rows, input_dim, output_dim);
}
