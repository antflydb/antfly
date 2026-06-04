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
const gpt_arch = @import("../architectures/gpt.zig");
const decoder_rms_runtime = @import("decoder_rms_runtime.zig");
const model_runtime = @import("../graph/model_runtime.zig");
const gpt_mod = @import("../models/gpt.zig");
const decoder_tail_runtime = @import("decoder_tail_runtime.zig");
const mlx_compute = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig") else struct {};
const metal_compute = @import("../ops/metal_compute.zig");
const ops = @import("../ops/ops.zig");

const c_std = @import("../util/c_env.zig");

fn dequantizeTensorToFloat32Generic(
    cb: *const ops.ComputeBackend,
    tensor: ops.CT,
    allocator: std.mem.Allocator,
) ![]f32 {
    return switch (cb.kind()) {
        .mlx => if (comptime build_options.enable_mlx)
            mlx_compute.dequantizeTensorToFloat32(cb, tensor, allocator)
        else
            unreachable,
        .metal => metal_compute.MetalCompute.dequantizeTensorToFloat32(cb, tensor, allocator),
        else => cb.toFloat32(tensor, allocator),
    };
}

fn prepareTraceRequested() bool {
    const value = c_std.getenv("TERMITE_METAL_PREPARE_TRACE") orelse return false;
    const slice = std.mem.span(value);
    return slice.len > 0 and !std.mem.eql(u8, slice, "0");
}

fn preparedLmHeadDisabled() bool {
    const value = c_std.getenv("TERMITE_METAL_DISABLE_PREPARED_TAIL") orelse return false;
    const slice = std.mem.span(value);
    return slice.len > 0 and !std.mem.eql(u8, slice, "0");
}

pub fn layerNormSlot(layer: usize, is_ffn: bool) usize {
    return layer * 2 + @intFromBool(is_ffn);
}

pub fn linearSlot(layer: usize, kind: enum { fused_attn, attn_out_proj, mlp_fc1, mlp_fc2 }) usize {
    return switch (kind) {
        .fused_attn => layer * 4,
        .attn_out_proj => layer * 4 + 1,
        .mlp_fc1 => layer * 4 + 2,
        .mlp_fc2 => layer * 4 + 3,
    };
}

pub fn finalNormSlot(configured_layer_count: usize) usize {
    return configured_layer_count * 2;
}

pub fn finalLmHeadSlot(configured_layer_count: usize) usize {
    return configured_layer_count * 4;
}

fn forwardPreparedGreedyFromNormalizedHidden(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    final_hidden: ops.CT,
) !?i64 {
    if (preparedLmHeadDisabled()) return null;
    // Final logit softcap is monotonic, so it preserves greedy argmax.
    const token = try cb.decoderRuntimeApplyLinearArgmax(&.{
        .slot = finalLmHeadSlot(configured_layer_count),
        .input = final_hidden,
        .in_dim = gpt_config.hidden_size,
        .out_dim = gpt_config.vocab_size,
    });
    return if (token) |token_id| @intCast(token_id) else null;
}

fn forwardLogitsTensorFromNormalizedHiddenPrepared(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    final_hidden: ops.CT,
) !?ops.CT {
    if (preparedLmHeadDisabled()) return null;
    return cb.decoderRuntimeApplyLinear(&.{
        .slot = finalLmHeadSlot(configured_layer_count),
        .input = final_hidden,
        .in_dim = gpt_config.hidden_size,
        .out_dim = gpt_config.vocab_size,
    });
}

fn forwardLogitsTensorFromNormalizedHiddenFallback(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
) !ops.CT {
    const lm_head_weight = try decoder_tail_runtime.getLmHeadWeight(cb, gpt_config);
    defer cb.free(lm_head_weight);
    return cb.linearNoBias(final_hidden, lm_head_weight, 1, gpt_config.hidden_size, gpt_config.vocab_size);
}

fn forwardGreedyFromNormalizedHidden(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    final_hidden: ops.CT,
) !?i64 {
    if (try forwardPreparedGreedyFromNormalizedHidden(cb, gpt_config, configured_layer_count, final_hidden)) |token| {
        return token;
    }

    const lm_head_weight = try decoder_tail_runtime.getLmHeadWeight(cb, gpt_config);
    defer cb.free(lm_head_weight);

    if (try cb.linearNoBiasArgmaxLastRow(final_hidden, lm_head_weight, 1, gpt_config.hidden_size, gpt_config.vocab_size)) |token| {
        return @intCast(token);
    }

    const logits = try cb.linearNoBias(final_hidden, lm_head_weight, 1, gpt_config.hidden_size, gpt_config.vocab_size);
    defer cb.free(logits);

    if (try cb.argmaxLastRow(logits, 1, gpt_config.vocab_size)) |token| {
        return @intCast(token);
    }

    const logits_host = try cb.toFloat32(logits, allocator);
    defer allocator.free(logits_host);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    var best_idx: usize = 0;
    for (logits_host[1..], 1..) |value, idx| {
        if (value > logits_host[best_idx]) best_idx = idx;
    }
    return @intCast(best_idx);
}

fn forwardLogitsTensorFromNormalizedHidden(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    final_hidden: ops.CT,
) !?ops.CT {
    if (try forwardLogitsTensorFromNormalizedHiddenPrepared(cb, gpt_config, configured_layer_count, final_hidden)) |logits| {
        return logits;
    }
    return try forwardLogitsTensorFromNormalizedHiddenFallback(cb, gpt_config, final_hidden);
}

fn forwardSampledFromNormalizedHidden(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    final_hidden: ops.CT,
    sampling: model_runtime.SamplingConfig,
    token_history: []const i64,
) !?i64 {
    const logits = (try forwardLogitsTensorFromNormalizedHidden(cb, gpt_config, configured_layer_count, final_hidden)) orelse return null;
    defer cb.free(logits);

    if (gpt_config.final_logit_softcapping <= 0.0) {
        if (try cb.sampleLastRow(&.{
            .tensor = logits,
            .rows = 1,
            .dim = gpt_config.vocab_size,
            .temperature = sampling.temperature,
            .top_k = if (sampling.top_k > 0) @intCast(sampling.top_k) else 0,
            .top_p = sampling.top_p,
            .min_p = sampling.min_p,
            .repetition_penalty = sampling.repetition_penalty,
            .frequency_penalty = sampling.frequency_penalty,
            .presence_penalty = sampling.presence_penalty,
            .token_history = token_history,
        })) |token| {
            return @intCast(token);
        }
    }

    const logits_host = try cb.toFloat32(logits, allocator);
    defer allocator.free(logits_host);
    if (gpt_config.final_logit_softcapping > 0.0) {
        gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    }
    return @intCast(model_runtime.sampleTokenFromLogits(allocator, logits_host, sampling, token_history));
}

pub fn preparedLayers(configured_layers: usize) usize {
    const value = c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_GPT2_LAYERS") orelse return configured_layers;
    const parsed = std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch return configured_layers;
    return @min(configured_layers, parsed);
}

pub fn overrideLevel() usize {
    const value = c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_GPT2_OVERRIDE_LEVEL") orelse return 4;
    return std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch 4;
}

pub fn supportsConfig(gpt_config: gpt_mod.Config) bool {
    return gpt_config.family == .gpt2 and
        gpt_config.position_encoding == .absolute and
        gpt_config.norm_type == .layer_norm and
        !gpt_config.usesMoe() and
        !gpt_config.hasPle() and
        !gpt_config.isMultimodal() and
        gpt_config.sliding_window == 0;
}

pub fn buildOverrides(gpt_config: gpt_mod.Config, configured_layer_count: usize) gpt_arch.Layer0DecoderOverrides {
    const prepared_layers = preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers));
    const configured_override_level = overrideLevel();
    var overrides = gpt_arch.Layer0DecoderOverrides{};
    for (0..prepared_layers) |layer| {
        if (configured_override_level >= 1) {
            overrides.attn_norm_slots[layer] = layerNormSlot(layer, false);
        }
        if (configured_override_level >= 2) {
            overrides.fused_qkv_linear_slots[layer] = linearSlot(layer, .fused_attn);
            overrides.attn_out_proj_linear_slots[layer] = linearSlot(layer, .attn_out_proj);
        }
        if (configured_override_level >= 3) {
            overrides.ffn_norm_slots[layer] = layerNormSlot(layer, true);
        }
        if (configured_override_level >= 4) {
            overrides.mlp_fc1_slots[layer] = linearSlot(layer, .mlp_fc1);
            overrides.mlp_fc2_slots[layer] = linearSlot(layer, .mlp_fc2);
        }
    }
    return overrides;
}

fn prepareAbsoluteEmbeddings(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    token_embedding: ops.CT,
    position_embedding: ops.CT,
    gpt_config: gpt_mod.Config,
) !bool {
    const token_host = try dequantizeTensorToFloat32Generic(cb, token_embedding, allocator);
    defer allocator.free(token_host);
    const token_shape = [_]i32{ @intCast(gpt_config.vocab_size), @intCast(gpt_config.hidden_size) };
    const token_dense = try cb.fromFloat32Shape(token_host, &token_shape);
    defer cb.free(token_dense);

    const position_host = try dequantizeTensorToFloat32Generic(cb, position_embedding, allocator);
    defer allocator.free(position_host);
    const position_shape = [_]i32{ @intCast(gpt_config.max_position_embeddings), @intCast(gpt_config.hidden_size) };
    const position_dense = try cb.fromFloat32Shape(position_host, &position_shape);
    defer cb.free(position_dense);

    return cb.decoderRuntimePrepareAbsoluteEmbeddings(&.{
        .token_embedding = token_dense,
        .position_embedding = position_dense,
        .vocab_size = gpt_config.vocab_size,
        .max_position_embeddings = gpt_config.max_position_embeddings,
        .hidden_size = gpt_config.hidden_size,
    });
}

fn prepareLayerNormSlot(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    slot: usize,
    weight: ops.CT,
    bias: ops.CT,
) !bool {
    const weight_host = try dequantizeTensorToFloat32Generic(cb, weight, allocator);
    defer allocator.free(weight_host);
    if (!std.math.approxEqAbs(f32, gpt_config.norm_weight_offset, 0.0, 1e-6)) {
        for (weight_host) |*value| value.* += gpt_config.norm_weight_offset;
    }
    const weight_shape = [_]i32{@intCast(gpt_config.hidden_size)};
    const weight_dense = try cb.fromFloat32Shape(weight_host, &weight_shape);
    defer cb.free(weight_dense);

    const bias_host = try dequantizeTensorToFloat32Generic(cb, bias, allocator);
    defer allocator.free(bias_host);
    const bias_shape = [_]i32{@intCast(gpt_config.hidden_size)};
    const bias_dense = try cb.fromFloat32Shape(bias_host, &bias_shape);
    defer cb.free(bias_dense);

    return cb.decoderRuntimePrepareLayerNorm(&.{
        .slot = slot,
        .weight = weight_dense,
        .bias = bias_dense,
        .hidden_size = gpt_config.hidden_size,
    });
}

fn prepareLinearSlot(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    slot: usize,
    weight: ops.CT,
    bias: ops.CT,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const weight_shape_i64 = try cb.tensorShape(weight, allocator);
    defer allocator.free(weight_shape_i64);
    if (weight_shape_i64.len != 2) return error.InvalidTensorShape;

    const weight_host = try dequantizeTensorToFloat32Generic(cb, weight, allocator);
    defer allocator.free(weight_host);
    const weight_shape = [_]i32{ @intCast(weight_shape_i64[0]), @intCast(weight_shape_i64[1]) };
    const weight_dense = try cb.fromFloat32Shape(weight_host, &weight_shape);
    defer cb.free(weight_dense);

    const bias_host = try dequantizeTensorToFloat32Generic(cb, bias, allocator);
    defer allocator.free(bias_host);
    const bias_shape = [_]i32{@intCast(out_dim)};
    const bias_dense = try cb.fromFloat32Shape(bias_host, &bias_shape);
    defer cb.free(bias_dense);

    return cb.decoderRuntimePrepareLinear(&.{
        .slot = slot,
        .weight = weight_dense,
        .bias = bias_dense,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
}

fn prepareLinearNoBiasSlot(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    slot: usize,
    weight: ops.CT,
    in_dim: usize,
    out_dim: usize,
) !bool {
    const weight_shape_i64 = try cb.tensorShape(weight, allocator);
    defer allocator.free(weight_shape_i64);
    if (weight_shape_i64.len != 2) return error.InvalidTensorShape;

    const weight_host = try dequantizeTensorToFloat32Generic(cb, weight, allocator);
    defer allocator.free(weight_host);
    const weight_shape = [_]i32{ @intCast(weight_shape_i64[0]), @intCast(weight_shape_i64[1]) };
    const weight_dense = try cb.fromFloat32Shape(weight_host, &weight_shape);
    defer cb.free(weight_dense);

    const bias_host = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_host);
    @memset(bias_host, 0.0);
    const bias_shape = [_]i32{@intCast(out_dim)};
    const bias_dense = try cb.fromFloat32Shape(bias_host, &bias_shape);
    defer cb.free(bias_dense);

    return cb.decoderRuntimePrepareLinear(&.{
        .slot = slot,
        .weight = weight_dense,
        .bias = bias_dense,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
}

pub fn prepareDecodeRuntime(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    kv_tokens: usize,
    configured_layer_count: usize,
) !bool {
    if (!supportsConfig(gpt_config)) {
        if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 supportsConfig failed\n", .{});
        return false;
    }
    if (!(try cb.decoderRuntimePrepareGreedy(&.{
        .hidden_size = gpt_config.hidden_size,
        .intermediate_size = gpt_config.intermediate_size,
        .num_layers = gpt_config.num_hidden_layers,
        .num_heads = gpt_config.num_attention_heads,
        .num_kv_heads = gpt_config.effectiveKVHeads(),
        .head_dim = gpt_config.headDim(),
        .vocab_size = gpt_config.vocab_size,
        .kv_tokens = kv_tokens,
    }))) {
        if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 decoderRuntimePrepareGreedy failed\n", .{});
        return false;
    }

    const embed_w = try cb.getWeight("wte.weight");
    defer cb.free(embed_w);
    const pos_w = try cb.getWeight("wpe.weight");
    defer cb.free(pos_w);
    if (!(try prepareAbsoluteEmbeddings(cb, allocator, embed_w, pos_w, gpt_config))) {
        if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 prepareAbsoluteEmbeddings failed\n", .{});
        return false;
    }

    const prepared_layer_count = preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers));
    for (0..prepared_layer_count) |layer| {
        var name_buf: [64]u8 = undefined;

        const ln1_w = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.ln_1.weight", .{layer}));
        defer cb.free(ln1_w);
        const ln1_b = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.ln_1.bias", .{layer}));
        defer cb.free(ln1_b);
        if (!(try prepareLayerNormSlot(cb, allocator, gpt_config, layerNormSlot(layer, false), ln1_w, ln1_b))) {
            if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 ln1 failed layer={d}\n", .{layer});
            return false;
        }

        const c_attn_w = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.attn.c_attn.weight", .{layer}));
        defer cb.free(c_attn_w);
        const c_attn_b = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.attn.c_attn.bias", .{layer}));
        defer cb.free(c_attn_b);
        if (!(try prepareLinearSlot(cb, allocator, linearSlot(layer, .fused_attn), c_attn_w, c_attn_b, gpt_config.hidden_size, gpt_config.hidden_size * 3))) {
            if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 c_attn failed layer={d}\n", .{layer});
            return false;
        }

        const c_proj_w = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.attn.c_proj.weight", .{layer}));
        defer cb.free(c_proj_w);
        const c_proj_b = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.attn.c_proj.bias", .{layer}));
        defer cb.free(c_proj_b);
        if (!(try prepareLinearSlot(cb, allocator, linearSlot(layer, .attn_out_proj), c_proj_w, c_proj_b, gpt_config.hidden_size, gpt_config.hidden_size))) {
            if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 c_proj failed layer={d}\n", .{layer});
            return false;
        }

        const ln2_w = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.ln_2.weight", .{layer}));
        defer cb.free(ln2_w);
        const ln2_b = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.ln_2.bias", .{layer}));
        defer cb.free(ln2_b);
        if (!(try prepareLayerNormSlot(cb, allocator, gpt_config, layerNormSlot(layer, true), ln2_w, ln2_b))) {
            if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 ln2 failed layer={d}\n", .{layer});
            return false;
        }

        const mlp_fc1_w = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.mlp.c_fc.weight", .{layer}));
        defer cb.free(mlp_fc1_w);
        const mlp_fc1_b = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.mlp.c_fc.bias", .{layer}));
        defer cb.free(mlp_fc1_b);
        if (!(try prepareLinearSlot(cb, allocator, linearSlot(layer, .mlp_fc1), mlp_fc1_w, mlp_fc1_b, gpt_config.hidden_size, gpt_config.intermediate_size))) {
            if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 mlp_fc1 failed layer={d}\n", .{layer});
            return false;
        }

        const mlp_fc2_w = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.mlp.c_proj.weight", .{layer}));
        defer cb.free(mlp_fc2_w);
        const mlp_fc2_b = try cb.getWeight(try std.fmt.bufPrintZ(&name_buf, "h.{d}.mlp.c_proj.bias", .{layer}));
        defer cb.free(mlp_fc2_b);
        if (!(try prepareLinearSlot(cb, allocator, linearSlot(layer, .mlp_fc2), mlp_fc2_w, mlp_fc2_b, gpt_config.intermediate_size, gpt_config.hidden_size))) {
            if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 mlp_fc2 failed layer={d}\n", .{layer});
            return false;
        }
    }

    const final_norm_w = try cb.getWeight("ln_f.weight");
    defer cb.free(final_norm_w);
    const final_norm_b = try cb.getWeight("ln_f.bias");
    defer cb.free(final_norm_b);
    if (!(try prepareLayerNormSlot(cb, allocator, gpt_config, finalNormSlot(configured_layer_count), final_norm_w, final_norm_b))) {
        if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 final norm failed\n", .{});
        return false;
    }

    const lm_head_w = try decoder_tail_runtime.getLmHeadWeight(cb, gpt_config);
    defer cb.free(lm_head_w);
    if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(
        cb,
        allocator,
        finalLmHeadSlot(configured_layer_count),
        lm_head_w,
        gpt_config.hidden_size,
        gpt_config.vocab_size,
    ))) {
        if (prepareTraceRequested()) std.debug.print("prepare-trace: gpt2 lm head failed\n", .{});
        return false;
    }

    return true;
}

pub fn forwardGreedyToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?i64 {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;

    const hidden = (try cb.decoderRuntimeEmbedAbsolutePosition(&.{
        .token_id = @intCast(token_id),
        .position_id = seq_len - 1,
        .hidden_size = gpt_config.hidden_size,
    })) orelse return null;
    const overrides = buildOverrides(gpt_config, configured_layer_count);
    const final_hidden = try gpt_arch.forwardFinalHiddenLastRowFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        gpt_config,
        hidden,
        overrides,
        1,
        seq_len,
        decode_context,
        null,
    );
    defer cb.free(final_hidden);

    return forwardGreedyFromNormalizedHidden(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        final_hidden,
    );
}

pub fn forwardLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    const logits = (try forwardLastLogitsTensor(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        token_id,
        seq_len,
        decode_context,
    )) orelse return null;
    const logits_host = try cb.toFloat32(logits, allocator);
    cb.free(logits);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    return logits_host;
}

pub fn forwardPrefillLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    if (!supportsConfig(gpt_config)) return null;
    if (input_ids.len == 0 or decode_context.query_sequence_len != input_ids.len) return null;
    if (decode_context.attention_mode != .paged_prefill) return null;

    const hidden = try decoder_rms_runtime.embedTokens(cb, allocator, gpt_config, input_ids);
    const overrides = buildOverrides(gpt_config, configured_layer_count);
    const hidden_result = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        gpt_config,
        hidden,
        overrides,
        1,
        seq_len,
        decode_context,
        null,
    );
    defer cb.free(hidden_result.hidden);

    const last_hidden = if (hidden_result.total_rows == 1)
        hidden_result.hidden
    else
        try cb.sliceRows2D(allocator, hidden_result.hidden, hidden_result.total_rows - 1, 1, gpt_config.hidden_size);
    defer if (last_hidden != hidden_result.hidden) cb.free(last_hidden);

    const logits = (try forwardLogitsTensorFromNormalizedHidden(
        cb,
        gpt_config,
        configured_layer_count,
        last_hidden,
    )) orelse return null;
    defer cb.free(logits);

    const result = try cb.toFloat32(logits, allocator);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, result);
    return result;
}

pub fn forwardLastLogitsTensor(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?ops.CT {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode and decode_context.attention_mode != .paged_prefill) return null;

    const hidden = (try cb.decoderRuntimeEmbedAbsolutePosition(&.{
        .token_id = @intCast(token_id),
        .position_id = seq_len - 1,
        .hidden_size = gpt_config.hidden_size,
    })) orelse return null;
    const overrides = buildOverrides(gpt_config, configured_layer_count);
    const final_hidden = try gpt_arch.forwardFinalHiddenLastRowFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        gpt_config,
        hidden,
        overrides,
        1,
        seq_len,
        decode_context,
        null,
    );
    defer cb.free(final_hidden);

    return forwardLogitsTensorFromNormalizedHidden(
        cb,
        gpt_config,
        configured_layer_count,
        final_hidden,
    );
}

pub fn forwardSampledToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    sampling: model_runtime.SamplingConfig,
    token_history: []const i64,
) !?i64 {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;

    const hidden = (try cb.decoderRuntimeEmbedAbsolutePosition(&.{
        .token_id = @intCast(token_id),
        .position_id = seq_len - 1,
        .hidden_size = gpt_config.hidden_size,
    })) orelse return null;
    const overrides = buildOverrides(gpt_config, configured_layer_count);
    const final_hidden = try gpt_arch.forwardFinalHiddenLastRowFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        gpt_config,
        hidden,
        overrides,
        1,
        seq_len,
        decode_context,
        null,
    );
    defer cb.free(final_hidden);

    return forwardSampledFromNormalizedHidden(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        final_hidden,
        sampling,
        token_history,
    );
}
