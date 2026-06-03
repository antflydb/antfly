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

// BERT encoder architecture using abstract ComputeBackend ops.
//
// Single implementation works with any backend (native, MLX, etc).
// The compute backend handles all hardware-specific execution.

const std = @import("std");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const bert_config = @import("../models/bert.zig");
const mlx_compute_mod = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig") else struct {};

pub const Config = bert_config.Config;

/// Run the full BERT encoder forward pass.
/// Returns an owned f32 slice: [batch * seq_len * hidden_size].
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H = config.hidden_size;
    const total = batch * seq_len;

    // 1. Embeddings: word + position + token_type + LayerNorm
    var hidden = try embeddings(cb, allocator, config, input_ids, token_type_ids, total, seq_len, H);

    // 2. Encoder layers
    for (0..config.num_hidden_layers) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 3. Read out to f32
    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

pub fn forwardUntilLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
    stop_layer_exclusive: usize,
) ![]f32 {
    const H = config.hidden_size;
    const total = batch * seq_len;
    const clamped_stop = @min(stop_layer_exclusive, config.num_hidden_layers);

    var hidden = try embeddings(cb, allocator, config, input_ids, token_type_ids, total, seq_len, H);
    for (0..clamped_stop) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

pub fn forwardFromHidden(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: []const f32,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    start_layer: usize,
) ![]f32 {
    return forwardFromHiddenRange(cb, allocator, config, hidden_in, attention_mask, batch, seq_len, start_layer, config.num_hidden_layers);
}

pub fn forwardFromHiddenRange(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: []const f32,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    start_layer: usize,
    end_layer_exclusive: usize,
) ![]f32 {
    const H = config.hidden_size;
    const total = batch * seq_len;
    if (hidden_in.len != total * H) return error.ShapeMismatch;

    const shape = [_]i32{ @intCast(total), @intCast(H) };
    var hidden = try cb.fromFloat32Shape(hidden_in, &shape);
    const clamped_start = @min(start_layer, config.num_hidden_layers);
    const clamped_end = @max(clamped_start, @min(end_layer_exclusive, config.num_hidden_layers));
    for (clamped_start..clamped_end) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

fn embeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    token_type_ids: ?[]const i64,
    total: usize,
    seq_len: usize,
    H: u32,
) !CT {
    // Word embeddings
    const word_emb = try cb.getWeight("embeddings.word_embeddings.weight");
    defer cb.free(word_emb);
    var result = try cb.embeddingLookup(word_emb, input_ids, total, H);

    // Position embeddings
    const pos_emb = try cb.getWeight("embeddings.position_embeddings.weight");
    defer cb.free(pos_emb);
    // Build position IDs: [0, 1, ..., seq_len-1] repeated for each batch item
    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..total) |i| pos_ids[i] = @intCast(i % seq_len);
    const pos_lookup = try cb.embeddingLookup(pos_emb, pos_ids, total, H);
    defer cb.free(pos_lookup);

    const with_pos = try cb.add(result, pos_lookup);
    cb.free(result);
    result = with_pos;

    // Token type embeddings (optional, not present in distilbert)
    if (config.model_type != .distilbert) {
        if (cb.getWeight("embeddings.token_type_embeddings.weight")) |tt_emb| {
            defer cb.free(tt_emb);
            // Token type IDs: use provided or default to all zeros
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
    }

    // LayerNorm
    const ln_w = try cb.getWeight("embeddings.LayerNorm.weight");
    defer cb.free(ln_w);
    const ln_b = try cb.getWeight("embeddings.LayerNorm.bias");
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(result, ln_w, ln_b, H, 1e-12);
    cb.free(result);

    return normed;
}

fn encoderLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
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

    const q_w = try getLayerWeight(cb, allocator, layer, "attention.self.query.weight", &name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, allocator, layer, "attention.self.query.bias", &name_buf);
    defer cb.free(q_b);
    const Q = try linearReplicatedToMaybeSharded(cb, hidden, q_w, q_b, total, hidden_dim, hidden_dim);
    defer {
        cb.free(Q);
    }

    const k_w = try getLayerWeight(cb, allocator, layer, "attention.self.key.weight", &name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, allocator, layer, "attention.self.key.bias", &name_buf);
    defer cb.free(k_b);
    const K = try linearReplicatedToMaybeSharded(cb, hidden, k_w, k_b, total, hidden_dim, hidden_dim);
    defer {
        cb.free(K);
    }

    const v_w = try getLayerWeight(cb, allocator, layer, "attention.self.value.weight", &name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, allocator, layer, "attention.self.value.bias", &name_buf);
    defer cb.free(v_b);
    const V = try linearReplicatedToMaybeSharded(cb, hidden, v_w, v_b, total, hidden_dim, hidden_dim);
    defer {
        cb.free(V);
    }
    const attn_out = try cb.scaledDotProductAttention(Q, K, V, attention_mask, null, batch, seq_len, local_num_heads, head_dim);
    defer {
        cb.free(attn_out);
    }

    const attn_proj_w = try getLayerWeight(cb, allocator, layer, "attention.output.dense.weight", &name_buf);
    defer cb.free(attn_proj_w);
    const attn_proj_b = try getLayerWeight(cb, allocator, layer, "attention.output.dense.bias", &name_buf);
    defer cb.free(attn_proj_b);
    const attn_proj = try linearMaybeShardedToReplicated(cb, attn_out, attn_proj_w, attn_proj_b, total, hidden_dim, hidden_dim);
    defer {
        cb.free(attn_proj);
    }

    const attn_res = try cb.add(attn_proj, hidden);
    defer {
        cb.free(attn_res);
    }

    const attn_ln_w = try getLayerWeight(cb, allocator, layer, "attention.output.LayerNorm.weight", &name_buf);
    defer cb.free(attn_ln_w);
    const attn_ln_b = try getLayerWeight(cb, allocator, layer, "attention.output.LayerNorm.bias", &name_buf);
    defer cb.free(attn_ln_b);
    const attn_normed = try cb.layerNorm(attn_res, attn_ln_w, attn_ln_b, hidden_dim, 1e-12);

    const ffn_i_w = try getLayerWeight(cb, allocator, layer, "intermediate.dense.weight", &name_buf);
    defer cb.free(ffn_i_w);
    const ffn_i_b = try getLayerWeight(cb, allocator, layer, "intermediate.dense.bias", &name_buf);
    defer cb.free(ffn_i_b);
    const ffn_inter = try linearReplicatedToMaybeSharded(cb, attn_normed, ffn_i_w, ffn_i_b, total, hidden_dim, intermediate_dim);
    defer {
        cb.free(ffn_inter);
    }

    const ffn_gelu = try cb.gelu(ffn_inter);
    defer {
        cb.free(ffn_gelu);
    }

    const ffn_o_w = try getLayerWeight(cb, allocator, layer, "output.dense.weight", &name_buf);
    defer cb.free(ffn_o_w);
    const ffn_o_b = try getLayerWeight(cb, allocator, layer, "output.dense.bias", &name_buf);
    defer cb.free(ffn_o_b);
    const ffn_out = try linearMaybeShardedToReplicated(cb, ffn_gelu, ffn_o_w, ffn_o_b, total, intermediate_dim, hidden_dim);
    defer {
        cb.free(ffn_out);
    }
    const ffn_res = try cb.add(ffn_out, attn_normed);
    cb.free(attn_normed);
    defer {
        cb.free(ffn_res);
    }

    const ffn_ln_w = try getLayerWeight(cb, allocator, layer, "output.LayerNorm.weight", &name_buf);
    defer cb.free(ffn_ln_w);
    const ffn_ln_b = try getLayerWeight(cb, allocator, layer, "output.LayerNorm.bias", &name_buf);
    defer cb.free(ffn_ln_b);
    const layer_out = try cb.layerNorm(ffn_res, ffn_ln_w, ffn_ln_b, hidden_dim, 1e-12);
    return layer_out;
}

/// Build a layer weight name like "encoder.layer.N.suffix" and look it up.
fn getLayerWeight(cb: *const ComputeBackend, _: std.mem.Allocator, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "encoder.layer.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

fn tensorParallelWorldSize(cb: *const ComputeBackend) usize {
    if (!build_options.enable_mlx) return 1;
    const mlx_compute = mlx_compute_mod.MlxCompute.fromComputeBackend(cb) orelse return 1;
    if (!mlx_compute.tensorParallelEnabled()) return 1;
    return mlx_compute.tensorParallelWorldSize();
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
