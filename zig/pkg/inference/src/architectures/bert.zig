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
// Single implementation works with any ComputeBackend implementation.
// The compute backend handles all hardware-specific execution.

const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const bert_config = @import("../models/bert.zig");
const native_compute_mod = @import("../ops/native_compute.zig");

pub const Config = bert_config.Config;

const BertLinearSlotKind = enum(usize) {
    q,
    k,
    v,
    attention_output,
    intermediate,
    output,
};

const BertLayerNormSlotKind = enum(usize) {
    attention_output,
    output,
};

const bert_linear_specs = [_]struct {
    kind: BertLinearSlotKind,
    weight: []const u8,
    bias: []const u8,
    input_intermediate: bool = false,
    output_intermediate: bool = false,
}{
    .{ .kind = .q, .weight = "attention.self.query.weight", .bias = "attention.self.query.bias" },
    .{ .kind = .k, .weight = "attention.self.key.weight", .bias = "attention.self.key.bias" },
    .{ .kind = .v, .weight = "attention.self.value.weight", .bias = "attention.self.value.bias" },
    .{ .kind = .attention_output, .weight = "attention.output.dense.weight", .bias = "attention.output.dense.bias" },
    .{ .kind = .intermediate, .weight = "intermediate.dense.weight", .bias = "intermediate.dense.bias", .output_intermediate = true },
    .{ .kind = .output, .weight = "output.dense.weight", .bias = "output.dense.bias", .input_intermediate = true },
};

const bert_layer_norm_specs = [_]struct {
    kind: BertLayerNormSlotKind,
    weight: []const u8,
    bias: []const u8,
}{
    .{ .kind = .attention_output, .weight = "attention.output.LayerNorm.weight", .bias = "attention.output.LayerNorm.bias" },
    .{ .kind = .output, .weight = "output.LayerNorm.weight", .bias = "output.LayerNorm.bias" },
};

fn metalEncoderFrameEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_ENCODER_FRAME");
}

fn bertLinearSlot(layer: usize, kind: BertLinearSlotKind) usize {
    return layer * bert_linear_specs.len + @intFromEnum(kind);
}

fn bertLayerNormSlot(layer: usize, kind: BertLayerNormSlotKind) usize {
    return layer * bert_layer_norm_specs.len + @intFromEnum(kind);
}

fn bertEmbeddingLayerNormSlot(layer_count: usize) usize {
    return layer_count * bert_layer_norm_specs.len;
}

fn metalBertWeightMirrorMaxBytes() usize {
    const mb = platform.env.getenvUsize("TERMITE_METAL_BERT_WEIGHT_MIRROR_MAX_MB") orelse 768;
    return std.math.mul(usize, mb, 1024 * 1024) catch std.math.maxInt(usize);
}

fn bertDenseMirrorBytes(config: Config, bytes_per_element: usize) ?usize {
    const layers: usize = config.num_hidden_layers;
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const hidden_squared = std.math.mul(usize, hidden, hidden) catch return null;
    const hidden_intermediate = std.math.mul(usize, hidden, intermediate) catch return null;
    const hidden_projections = std.math.mul(usize, hidden_squared, 4) catch return null;
    const ffn_projections = std.math.mul(usize, hidden_intermediate, 2) catch return null;
    const elements_per_layer = std.math.add(usize, hidden_projections, ffn_projections) catch return null;
    const total_elements = std.math.mul(usize, elements_per_layer, layers) catch return null;
    return std.math.mul(usize, total_elements, bytes_per_element) catch null;
}

fn metalEncoderSlotsPrepared(cb: *const ComputeBackend, config: Config) bool {
    const layer_count: usize = @intCast(config.num_hidden_layers);
    const hidden: usize = @intCast(config.hidden_size);
    const intermediate: usize = @intCast(config.intermediate_size);

    for (0..layer_count) |layer| {
        for (bert_linear_specs) |spec| {
            if (!cb.decoderRuntimeLinearSlotPrepared(
                bertLinearSlot(layer, spec.kind),
                if (spec.input_intermediate) intermediate else hidden,
                if (spec.output_intermediate) intermediate else hidden,
            )) return false;
        }
        for (bert_layer_norm_specs) |spec| {
            if (!cb.decoderRuntimeLayerNormSlotPrepared(
                bertLayerNormSlot(layer, spec.kind),
                hidden,
            )) return false;
        }
    }
    return cb.decoderRuntimeLayerNormSlotPrepared(
        bertEmbeddingLayerNormSlot(layer_count),
        hidden,
    );
}

fn preplanMetalEncoder(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config) !bool {
    if (cb.kind() != .metal or !metalEncoderFrameEnabled() or cb.decoderRuntimeHasActiveFrame()) return false;

    const layer_count: usize = @intCast(config.num_hidden_layers);
    const hidden: usize = @intCast(config.hidden_size);
    const intermediate: usize = @intCast(config.intermediate_size);
    const heads: usize = @intCast(config.num_attention_heads);
    if (layer_count == 0 or hidden == 0 or intermediate == 0 or heads == 0 or hidden % heads != 0) return false;
    // Query backend-owned slot metadata before touching the weight store.
    // The check is allocation-free and remains valid across explicit slot
    // invalidation because it verifies every slot and dimension required by
    // this encoder.
    if (metalEncoderSlotsPrepared(cb, config)) return true;

    const weight_mirrors_requested = !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_WEIGHT_MIRRORS") and
        !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_Q8_STAGING");
    const prefer_q8_mirrors = weight_mirrors_requested and platform.env.getenvBool("TERMITE_METAL_BERT_USE_Q8_MIRRORS");
    const prefer_bf16_requested = weight_mirrors_requested and !prefer_q8_mirrors and
        platform.env.getenvBool("TERMITE_METAL_BERT_USE_BF16_MIRRORS");
    const prefer_f32_requested = weight_mirrors_requested and !prefer_q8_mirrors and !prefer_bf16_requested and
        platform.env.getenvBool("TERMITE_METAL_BERT_USE_F32_MIRRORS");
    const mirror_bytes = bertDenseMirrorBytes(config, if (prefer_f32_requested) @sizeOf(f32) else @sizeOf(u16)) orelse
        std.math.maxInt(usize);
    // ponytail: keep one explicit aggregate ceiling until the runtime exposes
    // the Metal device's working-set budget to architecture preplanning.
    const weight_mirrors_enabled = weight_mirrors_requested and mirror_bytes <= metalBertWeightMirrorMaxBytes();
    const prefer_bf16_mirrors = weight_mirrors_enabled and !prefer_q8_mirrors and
        prefer_bf16_requested;
    const prefer_f32_mps_mirrors = weight_mirrors_enabled and !prefer_q8_mirrors and !prefer_bf16_mirrors and
        prefer_f32_requested;
    const prefer_f16_mps_mirrors = weight_mirrors_enabled and !prefer_q8_mirrors and !prefer_bf16_mirrors and
        !prefer_f32_mps_mirrors;

    for (0..layer_count) |layer| {
        for (bert_linear_specs) |spec| {
            var weight_name: [256]u8 = undefined;
            var bias_name: [256]u8 = undefined;
            const weight = try getLayerWeight(cb, allocator, layer, spec.weight, &weight_name);
            defer cb.free(weight);
            const bias = try getLayerWeight(cb, allocator, layer, spec.bias, &bias_name);
            defer cb.free(bias);
            if (!(try cb.decoderRuntimePrepareLinear(&.{
                .slot = bertLinearSlot(layer, spec.kind),
                .weight = weight,
                .bias = bias,
                .in_dim = if (spec.input_intermediate) intermediate else hidden,
                .out_dim = if (spec.output_intermediate) intermediate else hidden,
                .retain_dense_fallback = weight_mirrors_enabled,
                // Prefill-shaped encoder GEMMs reuse an F16 mirror so Metal can
                // use its mixed F32/F16 matrix path, matching CUDA's hybrid-
                // residency strategy. The packed quantized weight remains the
                // source of truth; direct, Q8, BF16, and F32 mirrors stay
                // available for A/B runs.
                .dense_fallback_max_bytes = if (weight_mirrors_enabled) 32 * 1024 * 1024 else null,
                .allow_direct_quant_fallback = weight_mirrors_enabled,
                .prefer_bf16_fallback = prefer_bf16_mirrors,
                .prefer_f16_mps_fallback = prefer_f16_mps_mirrors,
                .prefer_f32_mps_fallback = prefer_f32_mps_mirrors,
            }))) return false;
        }

        for (bert_layer_norm_specs) |spec| {
            var weight_name: [256]u8 = undefined;
            var bias_name: [256]u8 = undefined;
            const weight = try getLayerWeight(cb, allocator, layer, spec.weight, &weight_name);
            defer cb.free(weight);
            const bias = try getLayerWeight(cb, allocator, layer, spec.bias, &bias_name);
            defer cb.free(bias);
            if (!(try cb.decoderRuntimePrepareLayerNorm(&.{
                .slot = bertLayerNormSlot(layer, spec.kind),
                .weight = weight,
                .bias = bias,
                .hidden_size = hidden,
            }))) return false;
        }
    }

    const embedding_norm_weight = try cb.getWeight("embeddings.LayerNorm.weight");
    defer cb.free(embedding_norm_weight);
    const embedding_norm_bias = try cb.getWeight("embeddings.LayerNorm.bias");
    defer cb.free(embedding_norm_bias);
    return cb.decoderRuntimePrepareLayerNorm(&.{
        .slot = bertEmbeddingLayerNormSlot(layer_count),
        .weight = embedding_norm_weight,
        .bias = embedding_norm_bias,
        .hidden_size = hidden,
    });
}

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
    const hidden = try forwardCt(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len);
    defer cb.free(hidden);
    return cb.toFloat32(hidden, allocator);
}

fn validateBertForwardInputs(
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) !usize {
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const heads: usize = config.num_attention_heads;
    const max_positions: usize = config.maxSequenceLength();
    if (batch == 0 or seq_len == 0 or hidden == 0 or intermediate == 0 or heads == 0 or
        hidden % heads != 0 or max_positions == 0 or seq_len > max_positions)
    {
        return error.InvalidShape;
    }
    if (batch > std.math.maxInt(i32) or seq_len > std.math.maxInt(i32) or
        hidden > std.math.maxInt(i32) or intermediate > std.math.maxInt(i32))
    {
        return error.InvalidShape;
    }
    const total = std.math.mul(usize, batch, seq_len) catch return error.InvalidShape;
    if (total > std.math.maxInt(i32)) return error.InvalidShape;
    _ = std.math.mul(usize, total, hidden) catch return error.InvalidShape;
    _ = std.math.mul(usize, total, intermediate) catch return error.InvalidShape;
    if (input_ids.len != total or attention_mask.len != total) return error.InvalidShape;

    for (input_ids) |token_id| {
        if (token_id < 0 or token_id >= config.vocab_size) return error.InvalidTokenId;
    }
    for (attention_mask) |value| {
        if (value != 0 and value != 1) return error.InvalidAttentionMask;
    }
    if (token_type_ids) |values| {
        if (values.len != total or config.type_vocab_size == 0) return error.InvalidShape;
        for (values) |token_type_id| {
            if (token_type_id < 0 or token_type_id >= config.type_vocab_size) return error.InvalidTokenTypeId;
        }
    }
    return total;
}

/// Run the encoder while keeping intermediate tensors on the backend.
pub fn forwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    const H = config.hidden_size;
    const total = try validateBertForwardInputs(config, input_ids, attention_mask, token_type_ids, batch, seq_len);
    // Preparation is lazy on the first request; subsequent requests validate
    // backend-owned slot metadata without loading or wrapping model weights.
    const resident_slots = try preplanMetalEncoder(cb, allocator, config);

    var encoder_frame_active = false;
    if (resident_slots and !cb.decoderRuntimeHasActiveFrame()) {
        encoder_frame_active = try cb.decoderRuntimeBeginFrame();
    }
    errdefer if (encoder_frame_active) cb.decoderRuntimeCancelFrame() catch {};

    // 1. Embeddings: word + position + token_type + LayerNorm
    var hidden = try embeddings(
        cb,
        allocator,
        config,
        input_ids,
        token_type_ids,
        total,
        seq_len,
        H,
        if (resident_slots) bertEmbeddingLayerNormSlot(@intCast(config.num_hidden_layers)) else null,
    );
    errdefer cb.free(hidden);

    // 2. Encoder layers
    for (0..config.num_hidden_layers) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer, resident_slots);
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (encoder_frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        encoder_frame_active = false;
    }
    return hidden;
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

    var hidden = try embeddings(cb, allocator, config, input_ids, token_type_ids, total, seq_len, H, null);
    for (0..clamped_stop) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer, false);
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
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer, false);
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
    layer_norm_slot: ?usize,
) !CT {
    // Word embeddings
    const word_emb = try cb.getWeight("embeddings.word_embeddings.weight");
    defer cb.free(word_emb);
    var result = try cb.embeddingLookup(word_emb, input_ids, total, H);

    // Position embeddings
    const pos_emb = try cb.getWeight("embeddings.position_embeddings.weight");
    defer cb.free(pos_emb);
    const pos_ids = try buildPositionIds(allocator, config, input_ids, total, seq_len);
    defer allocator.free(pos_ids);
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

    // Keep fallback weights completely off the resident Metal hot path:
    // cached getWeight calls still allocate short-lived tensor wrappers.
    const normed = embeddingLayerNormWithSlot(
        cb,
        result,
        H,
        config.layer_norm_eps,
        layer_norm_slot,
    ) catch |err| {
        cb.free(result);
        return err;
    };
    cb.free(result);

    return normed;
}

fn embeddingLayerNormWithSlot(
    cb: *const ComputeBackend,
    input: CT,
    hidden_size: usize,
    eps: f32,
    slot: ?usize,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLayerNorm(&.{
            .slot = prepared_slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        })) |output| return output;
    }

    const weight = try cb.getWeight("embeddings.LayerNorm.weight");
    defer cb.free(weight);
    const bias = try cb.getWeight("embeddings.LayerNorm.bias");
    defer cb.free(bias);
    return cb.layerNorm(input, weight, bias, hidden_size, eps);
}

fn buildPositionIds(
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    total: usize,
    seq_len: usize,
) ![]i64 {
    if (seq_len == 0 or seq_len > config.maxSequenceLength() or input_ids.len < total or total % seq_len != 0) {
        return error.InvalidShape;
    }
    const pos_ids = try allocator.alloc(i64, total);
    errdefer allocator.free(pos_ids);
    switch (config.position_id_mode) {
        .absolute => {
            for (0..total) |i| pos_ids[i] = @intCast(i % seq_len);
        },
        .roberta_padding => {
            if (config.pad_token_id < 0) return error.InvalidTokenId;
            for (0..total / seq_len) |batch_index| {
                var next = std.math.add(i64, config.pad_token_id, 1) catch return error.InvalidShape;
                for (0..seq_len) |sequence_index| {
                    const index = batch_index * seq_len + sequence_index;
                    if (input_ids[index] == config.pad_token_id) {
                        pos_ids[index] = config.pad_token_id;
                    } else {
                        pos_ids[index] = next;
                        next = std.math.add(i64, next, 1) catch return error.InvalidShape;
                    }
                }
            }
        },
    }
    return pos_ids;
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
    resident_slots: bool,
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

    const qkv: ops.LinearTripleResult = if (resident_slots and !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_QKV")) blk: {
        if (try cb.decoderRuntimeApplyLinearQkv(&.{
            .q_slot = bertLinearSlot(layer, .q),
            .k_slot = bertLinearSlot(layer, .k),
            .v_slot = bertLinearSlot(layer, .v),
            .input = hidden,
            .in_dim = hidden_dim,
            .q_out_dim = hidden_dim,
            .kv_out_dim = hidden_dim,
        })) |projected| {
            break :blk .{
                .first = projected.first,
                .second = projected.second,
                .third = projected.third,
            };
        }
        break :blk try layerLinearTriple(cb, allocator, layer, hidden, total, hidden_dim, &name_buf);
    } else try layerLinearTriple(cb, allocator, layer, hidden, total, hidden_dim, &name_buf);
    defer cb.free(qkv.first);
    defer cb.free(qkv.second);
    defer cb.free(qkv.third);
    const attn_out = if (allAttentionMaskOnes(attention_mask, total))
        (try cb.scaledDotProductAttentionFull(qkv.first, qkv.second, qkv.third, null, batch, seq_len, local_num_heads, head_dim)) orelse
            try cb.scaledDotProductAttention(qkv.first, qkv.second, qkv.third, attention_mask, null, batch, seq_len, local_num_heads, head_dim)
    else
        try cb.scaledDotProductAttention(qkv.first, qkv.second, qkv.third, attention_mask, null, batch, seq_len, local_num_heads, head_dim);
    defer {
        cb.free(attn_out);
    }

    const attn_normed = blk: {
        if (resident_slots) {
            if (try cb.decoderRuntimeApplyLinearLayerNorm(&.{
                .linear_slot = bertLinearSlot(layer, .attention_output),
                .layer_norm_slot = bertLayerNormSlot(layer, .attention_output),
                .input = attn_out,
                .residual = hidden,
                .in_dim = hidden_dim,
                .hidden_size = hidden_dim,
                .eps = config.layer_norm_eps,
            })) |output| break :blk output;
        }

        const attn_proj = try layerLinearWithSlot(
            cb,
            allocator,
            layer,
            attn_out,
            "attention.output.dense.weight",
            "attention.output.dense.bias",
            total,
            hidden_dim,
            hidden_dim,
            if (resident_slots) bertLinearSlot(layer, .attention_output) else null,
            &name_buf,
        );
        defer cb.free(attn_proj);
        const attn_res = try cb.add(attn_proj, hidden);
        defer cb.free(attn_res);
        break :blk try layerNormForLayerWithSlot(
            cb,
            allocator,
            layer,
            attn_res,
            "attention.output.LayerNorm.weight",
            "attention.output.LayerNorm.bias",
            hidden_dim,
            config.layer_norm_eps,
            if (resident_slots) bertLayerNormSlot(layer, .attention_output) else null,
            &name_buf,
        );
    };
    defer cb.free(attn_normed);

    if (resident_slots) {
        if (try cb.decoderRuntimeApplyFfnLayerNorm(&.{
            .first_linear_slot = bertLinearSlot(layer, .intermediate),
            .second_linear_slot = bertLinearSlot(layer, .output),
            .layer_norm_slot = bertLayerNormSlot(layer, .output),
            .input = attn_normed,
            .residual = attn_normed,
            .hidden_size = hidden_dim,
            .intermediate_size = intermediate_dim,
            .eps = config.layer_norm_eps,
            .activation = .gelu,
        })) |layer_out| {
            return layer_out;
        }
    }

    const ffn_res = blk: {
        if (resident_slots) {
            if (try cb.runDenseFfnResidual(&.{
                .first_linear_slot = bertLinearSlot(layer, .intermediate),
                .second_linear_slot = bertLinearSlot(layer, .output),
                .input = attn_normed,
                .residual = attn_normed,
                .hidden_size = hidden_dim,
                .intermediate_size = intermediate_dim,
                .activation = .gelu,
            })) |output| break :blk output;
        }

        const ffn_inter = try layerLinearWithSlot(
            cb,
            allocator,
            layer,
            attn_normed,
            "intermediate.dense.weight",
            "intermediate.dense.bias",
            total,
            hidden_dim,
            intermediate_dim,
            if (resident_slots) bertLinearSlot(layer, .intermediate) else null,
            &name_buf,
        );
        defer cb.free(ffn_inter);
        const ffn_gelu = try cb.gelu(ffn_inter);
        defer cb.free(ffn_gelu);
        const ffn_out = try layerLinearWithSlot(
            cb,
            allocator,
            layer,
            ffn_gelu,
            "output.dense.weight",
            "output.dense.bias",
            total,
            intermediate_dim,
            hidden_dim,
            if (resident_slots) bertLinearSlot(layer, .output) else null,
            &name_buf,
        );
        defer cb.free(ffn_out);
        break :blk try cb.add(ffn_out, attn_normed);
    };
    defer cb.free(ffn_res);

    const layer_out = try layerNormForLayerWithSlot(
        cb,
        allocator,
        layer,
        ffn_res,
        "output.LayerNorm.weight",
        "output.LayerNorm.bias",
        hidden_dim,
        config.layer_norm_eps,
        if (resident_slots) bertLayerNormSlot(layer, .output) else null,
        &name_buf,
    );
    return layer_out;
}

/// Build a layer weight name like "encoder.layer.N.suffix" and look it up.
fn getLayerWeight(cb: *const ComputeBackend, _: std.mem.Allocator, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "encoder.layer.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

fn layerLinearTriple(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    input: CT,
    rows: usize,
    hidden_dim: usize,
    name_buf: *[256]u8,
) !ops.LinearTripleResult {
    const q_w = try getLayerWeight(cb, allocator, layer, "attention.self.query.weight", name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, allocator, layer, "attention.self.query.bias", name_buf);
    defer cb.free(q_b);
    const k_w = try getLayerWeight(cb, allocator, layer, "attention.self.key.weight", name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, allocator, layer, "attention.self.key.bias", name_buf);
    defer cb.free(k_b);
    const v_w = try getLayerWeight(cb, allocator, layer, "attention.self.value.weight", name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, allocator, layer, "attention.self.value.bias", name_buf);
    defer cb.free(v_b);
    return cb.linearTriple(input, q_w, q_b, k_w, k_b, v_w, v_b, rows, hidden_dim, hidden_dim);
}

fn allAttentionMaskOnes(mask: []const i64, total: usize) bool {
    if (mask.len < total) return false;
    for (mask[0..total]) |value| if (value == 0) return false;
    return true;
}

fn tensorParallelWorldSize(cb: *const ComputeBackend) usize {
    _ = cb;
    return 1;
}

fn layerLinearWithSlot(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    input: CT,
    weight_suffix: []const u8,
    bias_suffix: []const u8,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    slot: ?usize,
    name_buf: *[256]u8,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLinear(&.{
            .slot = prepared_slot,
            .input = input,
            .in_dim = input_dim,
            .out_dim = output_dim,
        })) |output| return output;
    }
    const weight = try getLayerWeight(cb, allocator, layer, weight_suffix, name_buf);
    defer cb.free(weight);
    const bias = try getLayerWeight(cb, allocator, layer, bias_suffix, name_buf);
    defer cb.free(bias);
    return cb.linear(input, weight, bias, rows, input_dim, output_dim);
}

fn layerNormForLayerWithSlot(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    input: CT,
    weight_suffix: []const u8,
    bias_suffix: []const u8,
    hidden_size: usize,
    eps: f32,
    slot: ?usize,
    name_buf: *[256]u8,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLayerNorm(&.{
            .slot = prepared_slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        })) |output| return output;
    }
    const weight = try getLayerWeight(cb, allocator, layer, weight_suffix, name_buf);
    defer cb.free(weight);
    const bias = try getLayerWeight(cb, allocator, layer, bias_suffix, name_buf);
    defer cb.free(bias);
    return cb.layerNorm(input, weight, bias, hidden_size, eps);
}

test "BERT resident slot layout is stable and non-overlapping" {
    try std.testing.expectEqual(@as(usize, 143), bertLinearSlot(23, .output));
    try std.testing.expectEqual(@as(usize, 47), bertLayerNormSlot(23, .output));
    try std.testing.expectEqual(@as(usize, 48), bertEmbeddingLayerNormSlot(24));
}

const MetalEncoderPreparedProbe = struct {
    linear_calls: usize = 0,
    layer_norm_calls: usize = 0,
    missing_linear_slot: ?usize = null,
    missing_layer_norm_slot: ?usize = null,

    fn linearPrepared(ctx: *anyopaque, slot: usize, in_dim: usize, out_dim: usize) bool {
        const self: *MetalEncoderPreparedProbe = @ptrCast(@alignCast(ctx));
        self.linear_calls += 1;
        if (self.missing_linear_slot == slot) return false;
        const kind: BertLinearSlotKind = @enumFromInt(slot % bert_linear_specs.len);
        const spec = bert_linear_specs[@intFromEnum(kind)];
        const expected_in_dim: usize = if (spec.input_intermediate) 8 else 4;
        const expected_out_dim: usize = if (spec.output_intermediate) 8 else 4;
        return in_dim == expected_in_dim and out_dim == expected_out_dim;
    }

    fn layerNormPrepared(ctx: *anyopaque, slot: usize, hidden_size: usize) bool {
        const self: *MetalEncoderPreparedProbe = @ptrCast(@alignCast(ctx));
        self.layer_norm_calls += 1;
        return self.missing_layer_norm_slot != slot and hidden_size == 4;
    }

    const vtable = blk: {
        var vt = native_compute_mod.vtable_impl;
        vt.decoderRuntimeLinearSlotPrepared = linearPrepared;
        vt.decoderRuntimeLayerNormSlotPrepared = layerNormPrepared;
        break :blk vt;
    };
};

const EmbeddingLayerNormResidentProbe = struct {
    input_marker: u8 = 0,
    output_marker: u8 = 0,
    apply_calls: usize = 0,
    weight_calls: usize = 0,

    fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
        const self: *EmbeddingLayerNormResidentProbe = @ptrCast(@alignCast(ctx));
        _ = name;
        self.weight_calls += 1;
        return error.UnexpectedWeightLookup;
    }

    fn applyLayerNorm(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormRequest) anyerror!?CT {
        const self: *EmbeddingLayerNormResidentProbe = @ptrCast(@alignCast(ctx));
        self.apply_calls += 1;
        try std.testing.expectEqual(@as(usize, 7), request.slot);
        try std.testing.expectEqual(@as(usize, 4), request.hidden_size);
        try std.testing.expectApproxEqAbs(@as(f32, 1e-5), request.eps, 1e-9);
        try std.testing.expectEqual(@intFromPtr(&self.input_marker), @intFromPtr(request.input));
        return @ptrCast(&self.output_marker);
    }

    const vtable = blk: {
        var vt = native_compute_mod.vtable_impl;
        vt.getWeight = getWeight;
        vt.decoderRuntimeApplyLayerNorm = applyLayerNorm;
        break :blk vt;
    };
};

test "BERT Metal resident slot probe is allocation-free and detects invalidation" {
    const config = Config{
        .hidden_size = 4,
        .intermediate_size = 8,
        .num_hidden_layers = 2,
        .num_attention_heads = 2,
    };
    var probe = MetalEncoderPreparedProbe{};
    const cb = ComputeBackend{ .ptr = &probe, .vtable = &MetalEncoderPreparedProbe.vtable };

    try std.testing.expect(metalEncoderSlotsPrepared(&cb, config));
    try std.testing.expectEqual(@as(usize, 12), probe.linear_calls);
    try std.testing.expectEqual(@as(usize, 5), probe.layer_norm_calls);

    probe.linear_calls = 0;
    probe.layer_norm_calls = 0;
    probe.missing_linear_slot = bertLinearSlot(1, .k);
    try std.testing.expect(!metalEncoderSlotsPrepared(&cb, config));
    try std.testing.expectEqual(bertLinearSlot(1, .k) + 1, probe.linear_calls);
    try std.testing.expectEqual(@as(usize, 2), probe.layer_norm_calls);
}

test "BERT resident embedding LayerNorm does not materialize fallback weights" {
    var probe = EmbeddingLayerNormResidentProbe{};
    const cb = ComputeBackend{ .ptr = &probe, .vtable = &EmbeddingLayerNormResidentProbe.vtable };
    const input: CT = @ptrCast(&probe.input_marker);

    const output = try embeddingLayerNormWithSlot(&cb, input, 4, 1e-5, 7);

    try std.testing.expectEqual(@intFromPtr(&probe.output_marker), @intFromPtr(output));
    try std.testing.expectEqual(@as(usize, 1), probe.apply_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.weight_calls);
}

test "RoBERTa position ids preserve padding indices" {
    const config = Config{ .position_id_mode = .roberta_padding, .pad_token_id = 1 };
    const input_ids = [_]i64{ 0, 42, 1, 9, 1, 1, 5, 6 };
    const positions = try buildPositionIds(std.testing.allocator, config, &input_ids, input_ids.len, 4);
    defer std.testing.allocator.free(positions);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 1, 4, 1, 1, 2, 3 }, positions);
}

test "RoBERTa position ids stay within the embedding table" {
    const config = Config{
        .position_id_mode = .roberta_padding,
        .pad_token_id = 1,
        .max_position_embeddings = 4,
    };
    try std.testing.expectEqual(@as(u32, 2), config.maxSequenceLength());

    const valid_ids = [_]i64{ 10, 11 };
    const positions = try buildPositionIds(std.testing.allocator, config, &valid_ids, valid_ids.len, valid_ids.len);
    defer std.testing.allocator.free(positions);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, positions);

    const too_many_ids = [_]i64{ 10, 11, 12 };
    try std.testing.expectError(
        error.InvalidShape,
        buildPositionIds(std.testing.allocator, config, &too_many_ids, too_many_ids.len, too_many_ids.len),
    );
}

test "BERT forward input validation rejects unsafe public inputs" {
    const config = Config{
        .vocab_size = 16,
        .hidden_size = 8,
        .intermediate_size = 16,
        .num_attention_heads = 2,
        .max_position_embeddings = 4,
    };
    const ids = [_]i64{ 1, 2, 3, 4 };
    const mask = [_]i64{ 1, 1, 1, 0 };
    try std.testing.expectEqual(@as(usize, 4), try validateBertForwardInputs(config, &ids, &mask, null, 1, 4));

    const invalid_mask = [_]i64{ 1, 1, -1, 0 };
    try std.testing.expectError(
        error.InvalidAttentionMask,
        validateBertForwardInputs(config, &ids, &invalid_mask, null, 1, 4),
    );
    try std.testing.expectError(error.InvalidShape, validateBertForwardInputs(config, &ids, &mask, null, 1, 5));

    const roberta_config = Config{
        .vocab_size = 16,
        .hidden_size = 8,
        .intermediate_size = 16,
        .num_attention_heads = 2,
        .max_position_embeddings = 4,
        .position_id_mode = .roberta_padding,
        .pad_token_id = 1,
    };
    try std.testing.expectError(error.InvalidShape, validateBertForwardInputs(roberta_config, &ids, &mask, null, 1, 4));
}

test "BGE-M3 F16 mirror estimate stays within the production default" {
    const config = Config{
        .hidden_size = 1024,
        .intermediate_size = 4096,
        .num_hidden_layers = 24,
    };
    try std.testing.expectEqual(@as(usize, 576 * 1024 * 1024), bertDenseMirrorBytes(config, @sizeOf(u16)).?);
}
