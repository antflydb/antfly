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
const activations = @import("activations.zig");
const metal_runtime = @import("metal_runtime.zig");
const metal_tensor = @import("metal_tensor.zig");
const MetalTensor = metal_tensor.MetalTensor;
const mlx_metal_bridge = @import("mlx_metal_bridge.zig");
const mlx = @import("mlx.zig");
const c = mlx.c;
const runtime_root = @import("../runtime/root.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const turboquant = @import("../runtime/kv/turboquant.zig");
const ops = @import("../ops/ops.zig");

pub const QuantizedStorage = weight_source_mod.QuantizedStorage;

const RawMetalProvider = metal_runtime.RawMetalProvider;
const RawMetalDecodeRuntime = metal_runtime.RawMetalDecodeRuntime;
const decoder_runtime_layer_norm_slot_capacity = metal_runtime.decoder_runtime_layer_norm_slot_capacity;
const decoder_runtime_linear_slot_capacity = metal_runtime.decoder_runtime_linear_slot_capacity;
const RawQuantizedRuntimeLinearKind = metal_runtime.RawQuantizedRuntimeLinearKind;
const RawLinearSlotKind = metal_runtime.RawLinearSlotKind;
const GatheredSpanKey = metal_runtime.GatheredSpanKey;
const GatheredSpanEntry = metal_runtime.GatheredSpanEntry;
const termite_metal_provider_create = metal_runtime.termite_metal_provider_create;
const termite_metal_provider_destroy = metal_runtime.termite_metal_provider_destroy;
const termite_metal_decode_runtime_create = metal_runtime.termite_metal_decode_runtime_create;
const termite_metal_decode_runtime_destroy = metal_runtime.termite_metal_decode_runtime_destroy;
const termite_metal_decode_runtime_ready = metal_runtime.termite_metal_decode_runtime_ready;
const termite_metal_decode_runtime_reserve = metal_runtime.termite_metal_decode_runtime_reserve;
const termite_metal_decode_runtime_prepare_decoder_only_greedy = metal_runtime.termite_metal_decode_runtime_prepare_decoder_only_greedy;
const termite_metal_decode_runtime_reset_state = metal_runtime.termite_metal_decode_runtime_reset_state;
const termite_metal_decode_runtime_prepare_absolute_embeddings = metal_runtime.termite_metal_decode_runtime_prepare_absolute_embeddings;
const termite_metal_decode_runtime_embed_absolute_position = metal_runtime.termite_metal_decode_runtime_embed_absolute_position;
const termite_metal_decode_runtime_prepare_layer_norm = metal_runtime.termite_metal_decode_runtime_prepare_layer_norm;
const termite_metal_decode_runtime_apply_layer_norm = metal_runtime.termite_metal_decode_runtime_apply_layer_norm;
const termite_metal_decode_runtime_prepare_rms_norm = metal_runtime.termite_metal_decode_runtime_prepare_rms_norm;
const termite_metal_decode_runtime_apply_rms_norm = metal_runtime.termite_metal_decode_runtime_apply_rms_norm;
const termite_metal_decode_runtime_prepare_linear = metal_runtime.termite_metal_decode_runtime_prepare_linear;
const termite_metal_decode_runtime_apply_linear = metal_runtime.termite_metal_decode_runtime_apply_linear;
const termite_metal_decode_runtime_apply_i2_s_linear_slot = metal_runtime.termite_metal_decode_runtime_apply_i2_s_linear_slot;
const termite_metal_decode_runtime_apply_q4_k_linear_slot = metal_runtime.termite_metal_decode_runtime_apply_q4_k_linear_slot;
const termite_metal_decode_runtime_apply_q5_k_linear_slot = metal_runtime.termite_metal_decode_runtime_apply_q5_k_linear_slot;
const termite_metal_decode_runtime_apply_linear_argmax = metal_runtime.termite_metal_decode_runtime_apply_linear_argmax;
const termite_metal_decode_runtime_apply_layer_norm_linear_argmax = metal_runtime.termite_metal_decode_runtime_apply_layer_norm_linear_argmax;
const termite_metal_decode_runtime_apply_layer_norm_linear = metal_runtime.termite_metal_decode_runtime_apply_layer_norm_linear;
const termite_metal_decode_runtime_apply_layer_norm_linear_sample_device = metal_runtime.termite_metal_decode_runtime_apply_layer_norm_linear_sample_device;
const termite_metal_decode_runtime_apply_rms_norm_linear_argmax = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_linear_argmax;
const termite_metal_decode_runtime_apply_rms_norm_linear = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_linear;
const termite_metal_decode_runtime_apply_rms_norm_q4_k_linear_slot = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_q4_k_linear_slot;
const termite_metal_decode_runtime_apply_rms_norm_q5_k_linear_slot = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_q5_k_linear_slot;
const termite_metal_decode_runtime_apply_rms_norm_linear_sample_device = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_linear_sample_device;
const termite_metal_decode_runtime_sample_from_logits_device = metal_runtime.termite_metal_decode_runtime_sample_from_logits_device;
const termite_metal_decode_runtime_apply_activation = metal_runtime.termite_metal_decode_runtime_apply_activation;
const termite_metal_decode_runtime_apply_add = metal_runtime.termite_metal_decode_runtime_apply_add;
const termite_metal_decode_runtime_apply_multiply = metal_runtime.termite_metal_decode_runtime_apply_multiply;
const termite_metal_decode_runtime_apply_linear_activation_linear_residual = metal_runtime.termite_metal_decode_runtime_apply_linear_activation_linear_residual;
const termite_metal_decode_runtime_apply_linear_pair_activation_multiply_linear_residual = metal_runtime.termite_metal_decode_runtime_apply_linear_pair_activation_multiply_linear_residual;
const termite_metal_decode_runtime_update_attention_span = metal_runtime.termite_metal_decode_runtime_update_attention_span;
const termite_metal_decode_runtime_append_attention_span = metal_runtime.termite_metal_decode_runtime_append_attention_span;
const termite_metal_decode_runtime_attention_span = metal_runtime.termite_metal_decode_runtime_attention_span;
const termite_metal_decode_runtime_apply_attention_dense_block = metal_runtime.termite_metal_decode_runtime_apply_attention_dense_block;
const termite_metal_decode_runtime_apply_attention_gated_block = metal_runtime.termite_metal_decode_runtime_apply_attention_gated_block;
const termite_metal_decode_runtime_apply_attention_residual_tl1 = metal_runtime.termite_metal_decode_runtime_apply_attention_residual_tl1;
const termite_metal_decode_runtime_apply_attention_residual_tl2 = metal_runtime.termite_metal_decode_runtime_apply_attention_residual_tl2;
const termite_metal_decode_runtime_apply_attention_residual_i2_s_slot = metal_runtime.termite_metal_decode_runtime_apply_attention_residual_i2_s_slot;
const termite_metal_decode_runtime_apply_attention_residual_q4_k_slot = metal_runtime.termite_metal_decode_runtime_apply_attention_residual_q4_k_slot;
const termite_metal_decode_runtime_apply_attention_residual_q5_k_slot = metal_runtime.termite_metal_decode_runtime_apply_attention_residual_q5_k_slot;
const termite_metal_decode_runtime_apply_attention_span_residual_tl1 = metal_runtime.termite_metal_decode_runtime_apply_attention_span_residual_tl1;
const termite_metal_decode_runtime_apply_attention_span_residual_i2_s_slot = metal_runtime.termite_metal_decode_runtime_apply_attention_span_residual_i2_s_slot;
const termite_metal_decode_runtime_apply_attention_span_residual_tl2 = metal_runtime.termite_metal_decode_runtime_apply_attention_span_residual_tl2;
const termite_metal_decode_runtime_apply_gated_ffn_residual_tl1 = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_tl1;
const termite_metal_decode_runtime_apply_gated_ffn_residual_tl2 = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_tl2;
const termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s;
const termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s_slots = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_i2_s_slots;
const termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_slots = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_slots;
const termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q5_k_down_slots = metal_runtime.termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q5_k_down_slots;
const termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual;
const termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual_slot = metal_runtime.termite_metal_decode_runtime_apply_rms_norm_i2_s_linear_residual_slot;
const termite_metal_provider_linear_tl1 = metal_runtime.termite_metal_provider_linear_tl1;
const termite_metal_provider_linear_tl2 = metal_runtime.termite_metal_provider_linear_tl2;
const termite_metal_provider_compressed_key_scores_polar4 = metal_runtime.termite_metal_provider_compressed_key_scores_polar4;
const termite_metal_provider_compressed_key_scores_turbo3 = metal_runtime.termite_metal_provider_compressed_key_scores_turbo3;

pub const TimingStats = ops.NativeQuantTimingStats;

pub var timing_stats = TimingStats{};
pub var logged_quantized_gated_ffn_unsupported_type = false;
pub var logged_quantized_gated_ffn_backend_mixed_kind = false;
pub var logged_quantized_gated_ffn_backend_unsupported_kind = false;
pub const grouped_threadgroup_width: usize = 32;
pub const grouped_row_tile: usize = 4;

pub fn arrayFromQuantizedRawBytes(storage: *const QuantizedStorage, shape: []const i32) c.mlx_array {
    if (storage.raw_owned) {
        return mlx.arrayFromBytes(storage.raw_bytes, shape, c.MLX_UINT8);
    }
    return mlx.arrayFromBorrowedBytes(storage.raw_bytes, shape, c.MLX_UINT8);
}

pub fn resetTimingStats() void {
    timing_stats = .{};
}

pub fn getTimingStats() TimingStats {
    return timing_stats;
}

pub fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

pub fn enableRawMetalCompressedKeyScoresDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_RAW_METAL_COMPRESSED_KEY_SCORES") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

pub fn enableMetalDecoderRuntimeDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_DECODER_RUNTIME") orelse
        c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

pub fn disableMetalDecoderRuntimeAttentionSpanDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_DECODER_RUNTIME_DISABLE_ATTENTION_SPAN") orelse
        c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_DISABLE_ATTENTION_SPAN") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

pub fn notePreparedViewCacheHit() void {
    timing_stats.prepared_view_cache_hits += 1;
}

pub fn notePreparedViewCacheMiss() void {
    timing_stats.prepared_view_cache_misses += 1;
}

pub fn notePreparedViewOwnedMaterialization() void {
    timing_stats.prepared_view_owned_materializations += 1;
}

pub const LinearNoBiasRequest = struct {
    input: c.mlx_array,
    weight: c.mlx_array,
    quantized_storage: *QuantizedStorage,
    prepared_weight_bytes: ?[]const u8 = null,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stream: c.mlx_stream,
};

pub const LinearNoBiasPairRequest = struct {
    input: c.mlx_array,
    weight_a: c.mlx_array,
    weight_b: c.mlx_array,
    quantized_storage_a: *QuantizedStorage,
    quantized_storage_b: *QuantizedStorage,
    prepared_weight_bytes_a: ?[]const u8 = null,
    prepared_weight_bytes_b: ?[]const u8 = null,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stream: c.mlx_stream,
};

pub const MulMatIdRequest = struct {
    input: c.mlx_array,
    weight: c.mlx_array,
    quantized_storage: *QuantizedStorage,
    expert_ids: []const u32,
    /// Pre-built GPU tensor for expert IDs. When set, the kernel uses this
    /// directly instead of uploading `expert_ids` from CPU, avoiding a
    /// GPU↔CPU sync round-trip.
    expert_ids_arr: ?c.mlx_array = null,
    expert_tile_ids: ?[]const u32 = null,
    tile_row_starts: ?[]const u32 = null,
    tile_row_counts: ?[]const u32 = null,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stream: c.mlx_stream,
};

pub const MoeLinearNoBiasRequest = MulMatIdRequest;

pub const MoeLinearNoBiasPairRequest = struct {
    input: c.mlx_array,
    weight_a: c.mlx_array,
    weight_b: c.mlx_array,
    quantized_storage_a: *QuantizedStorage,
    quantized_storage_b: *QuantizedStorage,
    expert_ids: []const u32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stream: c.mlx_stream,
};

pub const MoeLinearNoBiasPairResult = struct {
    first: c.mlx_array,
    second: c.mlx_array,
};

pub const CompressedKeyFormat = metal_runtime.CompressedKeyFormat;

pub const DecoderRuntimePrepareLayerNormRequest = struct {
    weight: c.mlx_array,
    bias: c.mlx_array,
    slot: usize,
    hidden_size: usize,
};

pub const DecoderRuntimeApplyLayerNormRequest = struct {
    input: c.mlx_array,
    slot: usize,
    hidden_size: usize,
    eps: f32,
};

pub const DecoderRuntimePrepareRmsNormRequest = struct {
    weight: c.mlx_array,
    slot: usize,
    hidden_size: usize,
};

pub const DecoderRuntimeApplyRmsNormRequest = struct {
    input: c.mlx_array,
    slot: usize,
    hidden_size: usize,
    eps: f32,
};

pub const DecoderRuntimeApplyLayerNormLinearArgmaxRequest = struct {
    input: c.mlx_array,
    norm_slot: usize,
    linear_slot: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLayerNormLinearRequest = struct {
    input: c.mlx_array,
    norm_slot: usize,
    linear_slot: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyRmsNormLinearArgmaxRequest = struct {
    input: c.mlx_array,
    norm_slot: usize,
    linear_slot: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyRmsNormLinearRequest = struct {
    input: c.mlx_array,
    norm_slot: usize,
    linear_slot: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLayerNormLinearSampleRequest = struct {
    input: c.mlx_array,
    norm_slot: usize,
    linear_slot: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    token_history: []const i64,
};

pub const DecoderRuntimeApplyRmsNormLinearSampleRequest = struct {
    input: c.mlx_array,
    norm_slot: usize,
    linear_slot: usize,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    token_history: []const i64,
};

pub const SampleLogitsDeviceRequest = struct {
    input: c.mlx_array,
    out_dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    token_history: []const i64,
};

pub const DecoderRuntimePrepareLinearRequest = struct {
    weight: c.mlx_array,
    bias: c.mlx_array,
    quantized_storage: ?*const QuantizedStorage = null,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    retain_dense_fallback: bool = true,
};

pub const DecoderRuntimeApplyLinearRequest = struct {
    input: c.mlx_array,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearArgmaxRequest = struct {
    input: c.mlx_array,
    slot: usize,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearPairRequest = struct {
    input: c.mlx_array,
    slot_a: usize,
    slot_b: usize,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearQkvRequest = struct {
    input: c.mlx_array,
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
};

pub const DecoderRuntimeApplyActivationRequest = struct {
    input: c.mlx_array,
    kind: ops.DecoderRuntimeActivationKind,
    dim: usize,
};

pub const DecoderRuntimeApplyAddRequest = struct {
    lhs: c.mlx_array,
    rhs: c.mlx_array,
    dim: usize,
};

pub const RunDenseFfnResidualRequest = struct {
    input: c.mlx_array,
    residual: c.mlx_array,
    first_linear_slot: usize,
    second_linear_slot: usize,
    hidden_size: usize,
    intermediate_size: usize,
    activation: ops.DecoderRuntimeActivationKind,
};

pub const RunGatedFfnResidualRequest = struct {
    input: c.mlx_array,
    residual: c.mlx_array,
    gate_linear_slot: usize,
    up_linear_slot: usize,
    down_linear_slot: usize,
    post_gate_rms_norm_slot: ?usize = null,
    post_down_rms_norm_slot: ?usize = null,
    hidden_size: usize,
    intermediate_size: usize,
    eps: f32 = 0.0,
    activation: ops.DecoderRuntimeActivationKind,
};

pub const RunAttentionResidualPostLinearRequest = struct {
    attention_input: c.mlx_array,
    residual: c.mlx_array,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize = null,
    attention_post_linear_rms_norm_slot: ?usize = null,
};

pub const RunCompressedAttentionDenseDecoderBlockRequest = struct {
    q: ?c.mlx_array = null,
    k_suffix: ?c.mlx_array = null,
    v_suffix: ?c.mlx_array = null,
    attention_input: ?c.mlx_array = null,
    fused_qkv_linear_slot: ?usize = null,
    bootstrap_k_blocks: []const c.mlx_array = &.{},
    bootstrap_v_blocks: []const c.mlx_array = &.{},
    bootstrap_block_token_counts: []const usize = &.{},
    full_k: ?c.mlx_array = null,
    full_v: ?c.mlx_array = null,
    source_ptr_id: usize,
    sequence_id: runtime_root.kv.manager.SequenceId,
    layer_index: usize,
    query_sequence_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    format: CompressedKeyFormat,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize = null,
    attention_post_linear_rms_norm_slot: ?usize = null,
    residual: c.mlx_array,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: ?usize = null,
    ffn_rms_norm_slot: ?usize = null,
    first_ffn_linear_slot: usize,
    second_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation: ops.DecoderRuntimeActivationKind,
};

pub const RunCompressedAttentionGatedDecoderBlockRequest = struct {
    q: ?c.mlx_array = null,
    k_suffix: ?c.mlx_array = null,
    v_suffix: ?c.mlx_array = null,
    attention_input: ?c.mlx_array = null,
    q_linear_slot: ?usize = null,
    k_linear_slot: ?usize = null,
    v_linear_slot: ?usize = null,
    bootstrap_k_blocks: []const c.mlx_array = &.{},
    bootstrap_v_blocks: []const c.mlx_array = &.{},
    bootstrap_block_token_counts: []const usize = &.{},
    full_k: ?c.mlx_array = null,
    full_v: ?c.mlx_array = null,
    source_ptr_id: usize,
    sequence_id: runtime_root.kv.manager.SequenceId,
    layer_index: usize,
    query_sequence_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    format: CompressedKeyFormat,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize = null,
    attention_post_linear_rms_norm_slot: ?usize = null,
    residual: c.mlx_array,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: ?usize = null,
    ffn_rms_norm_slot: ?usize = null,
    ffn_post_gate_rms_norm_slot: ?usize = null,
    ffn_post_down_rms_norm_slot: ?usize = null,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation: ops.DecoderRuntimeActivationKind,
};

pub const CompressedAttentionResidualRequest = struct {
    q: MetalTensor,
    k_suffix: MetalTensor,
    v_suffix: MetalTensor,
    bootstrap_k_blocks: []const MetalTensor = &.{},
    bootstrap_v_blocks: []const MetalTensor = &.{},
    bootstrap_block_token_counts: []const usize = &.{},
    full_k: ?MetalTensor = null,
    full_v: ?MetalTensor = null,
    source_ptr_id: usize,
    sequence_id: runtime_root.kv.manager.SequenceId,
    layer_index: usize,
    query_sequence_len: usize,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    format: CompressedKeyFormat,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize = null,
    attention_post_linear_rms_norm_slot: ?usize = null,
    residual: MetalTensor,
    hidden_size: usize,
    attention_input_size: usize,
    eps: f32,
};

pub const CompressedKeyScoresRequest = struct {
    q: c.mlx_array,
    encoded_key: c.mlx_array,
    q_len: usize,
    block_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    format: CompressedKeyFormat,
    stream: c.mlx_stream,
};

pub const CompressedAttentionBlockRequest = struct {
    q: c.mlx_array,
    encoded_key: c.mlx_array,
    v: c.mlx_array,
    running_max: c.mlx_array,
    running_sum: c.mlx_array,
    running_acc: c.mlx_array,
    block_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    query_position: usize,
    block_position_offset: usize,
    sliding_window: usize,
    format: CompressedKeyFormat,
    stream: c.mlx_stream,
};

pub const CompressedAttentionBlockResult = struct {
    running_max: c.mlx_array,
    running_sum: c.mlx_array,
    running_acc: c.mlx_array,
};

pub const CompressedAttentionSpanRequest = struct {
    q: c.mlx_array,
    encoded_key: c.mlx_array,
    v: c.mlx_array,
    suffix_encoded_key: ?c.mlx_array = null,
    suffix_v: ?c.mlx_array = null,
    suffix_tokens: usize = 0,
    kv_tokens: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    key_row_bytes: usize,
    query_position: usize,
    kv_position_offset: usize,
    sliding_window: usize,
    format: CompressedKeyFormat,
    stream: c.mlx_stream,
};

pub const LmHeadArgmaxRequest = struct {
    hidden: c.mlx_array,
    weight: c.mlx_array,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    stream: c.mlx_stream,
};

pub const DecoderRuntimePrepareAbsoluteEmbeddingsRequest = struct {
    token_embedding: c.mlx_array,
    position_embedding: c.mlx_array,
    vocab_size: usize,
    max_position_embeddings: usize,
    hidden_size: usize,
};

pub const DecoderRuntimeEmbedAbsolutePositionRequest = struct {
    token_id: usize,
    position_id: usize,
    hidden_size: usize,
};

pub const PreparedWeight = struct {
    weight: c.mlx_array,
    quantized_storage: *QuantizedStorage,
    staged_backend_dense: bool,
    has_lazy_owner: bool,
    packed_expert: bool,
};

pub const ExecutionPlan = enum {
    unsupported,
    device_native,
    backend_dense,
    wrapper_direct_quant,
};

pub const LinearNoBiasPlanRequest = struct {
    prepared_weight: PreparedWeight,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

pub const LinearNoBiasPairPlanRequest = struct {
    prepared_weight_a: PreparedWeight,
    prepared_weight_b: PreparedWeight,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

pub const LinearNoBiasPairResult = struct {
    first: c.mlx_array,
    second: c.mlx_array,
};

pub const LinearNoBiasTripleResult = struct {
    first: c.mlx_array,
    second: c.mlx_array,
    third: c.mlx_array,
};

pub const Provider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        planLinearNoBias: *const fn (*anyopaque, request: *const LinearNoBiasPlanRequest) ExecutionPlan,
        linearNoBias: *const fn (*anyopaque, request: *const LinearNoBiasRequest) anyerror!?c.mlx_array,
        planLinearNoBiasPair: *const fn (*anyopaque, request: *const LinearNoBiasPairPlanRequest) ExecutionPlan,
        linearNoBiasPair: *const fn (*anyopaque, request: *const LinearNoBiasPairRequest) anyerror!?LinearNoBiasPairResult,
        mulMatId: *const fn (*anyopaque, request: *const MulMatIdRequest) anyerror!?c.mlx_array,
        moeLinearNoBias: *const fn (*anyopaque, request: *const MoeLinearNoBiasRequest) anyerror!?c.mlx_array,
        moeLinearNoBiasPair: *const fn (*anyopaque, request: *const MoeLinearNoBiasPairRequest) anyerror!?MoeLinearNoBiasPairResult,
        compressedKeyScores: *const fn (*anyopaque, request: *const CompressedKeyScoresRequest) anyerror!?c.mlx_array,
        compressedAttentionBlock: *const fn (*anyopaque, request: *const CompressedAttentionBlockRequest) anyerror!?CompressedAttentionBlockResult,
        compressedAttentionSpan: *const fn (*anyopaque, request: *const CompressedAttentionSpanRequest) anyerror!?c.mlx_array,
        lmHeadArgmax: *const fn (*anyopaque, request: *const LmHeadArgmaxRequest) anyerror!?c.mlx_array,
        decoderRuntimePrepareGreedy: *const fn (*anyopaque, request: *const ops.DecoderRuntimeGreedyRequest) anyerror!bool,
        decoderRuntimeResetState: *const fn (*anyopaque) anyerror!void,
        decoderRuntimePrepareAbsoluteEmbeddings: *const fn (*anyopaque, request: *const DecoderRuntimePrepareAbsoluteEmbeddingsRequest) anyerror!bool,
        decoderRuntimeEmbedAbsolutePosition: *const fn (*anyopaque, request: *const DecoderRuntimeEmbedAbsolutePositionRequest) anyerror!?c.mlx_array,
        decoderRuntimePrepareLayerNorm: *const fn (*anyopaque, request: *const DecoderRuntimePrepareLayerNormRequest) anyerror!bool,
        decoderRuntimeApplyLayerNorm: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLayerNormRequest) anyerror!?c.mlx_array,
        decoderRuntimePrepareRmsNorm: *const fn (*anyopaque, request: *const DecoderRuntimePrepareRmsNormRequest) anyerror!bool,
        decoderRuntimeApplyRmsNorm: *const fn (*anyopaque, request: *const DecoderRuntimeApplyRmsNormRequest) anyerror!?c.mlx_array,
        decoderRuntimeApplyLayerNormLinear: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearRequest) anyerror!?c.mlx_array,
        decoderRuntimeApplyLayerNormLinearArgmax: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearArgmaxRequest) anyerror!?usize,
        decoderRuntimeApplyLayerNormLinearSample: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearSampleRequest) anyerror!?usize,
        decoderRuntimeApplyRmsNormLinear: *const fn (*anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearRequest) anyerror!?c.mlx_array,
        decoderRuntimeApplyRmsNormLinearArgmax: *const fn (*anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearArgmaxRequest) anyerror!?usize,
        decoderRuntimeApplyRmsNormLinearSample: *const fn (*anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearSampleRequest) anyerror!?usize,
        sampleLogitsDevice: *const fn (*anyopaque, request: *const SampleLogitsDeviceRequest) anyerror!?usize,
        decoderRuntimePrepareLinear: *const fn (*anyopaque, request: *const DecoderRuntimePrepareLinearRequest) anyerror!bool,
        decoderRuntimeApplyLinear: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLinearRequest) anyerror!?c.mlx_array,
        decoderRuntimeApplyLinearArgmax: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLinearArgmaxRequest) anyerror!?usize,
        decoderRuntimeApplyLinearPair: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLinearPairRequest) anyerror!?LinearNoBiasPairResult,
        decoderRuntimeApplyLinearQkv: *const fn (*anyopaque, request: *const DecoderRuntimeApplyLinearQkvRequest) anyerror!?LinearNoBiasTripleResult,
        decoderRuntimeApplyActivation: *const fn (*anyopaque, request: *const DecoderRuntimeApplyActivationRequest) anyerror!?c.mlx_array,
        decoderRuntimeApplyAdd: *const fn (*anyopaque, request: *const DecoderRuntimeApplyAddRequest) anyerror!?c.mlx_array,
        runDenseFfnResidual: *const fn (*anyopaque, request: *const RunDenseFfnResidualRequest) anyerror!?c.mlx_array,
        runGatedFfnResidual: *const fn (*anyopaque, request: *const RunGatedFfnResidualRequest) anyerror!?c.mlx_array,
        runAttentionResidualPostLinear: *const fn (*anyopaque, request: *const RunAttentionResidualPostLinearRequest) anyerror!?c.mlx_array,
        runCompressedAttentionDenseDecoderBlock: *const fn (*anyopaque, request: *const RunCompressedAttentionDenseDecoderBlockRequest) anyerror!?c.mlx_array,
        runCompressedAttentionGatedDecoderBlock: *const fn (*anyopaque, request: *const RunCompressedAttentionGatedDecoderBlockRequest) anyerror!?c.mlx_array,
        hasGatheredSpanCache: *const fn (*anyopaque, source_ptr_id: usize, sequence_id: runtime_root.kv.manager.SequenceId, layer_index: usize) bool,
        deinit: ?*const fn (*anyopaque) void = null,
    };

    pub fn planLinearNoBias(self: Provider, request: *const LinearNoBiasPlanRequest) ExecutionPlan {
        return self.vtable.planLinearNoBias(self.ptr, request);
    }

    pub fn linearNoBias(self: Provider, request: *const LinearNoBiasRequest) !?c.mlx_array {
        return self.vtable.linearNoBias(self.ptr, request);
    }

    pub fn planLinearNoBiasPair(self: Provider, request: *const LinearNoBiasPairPlanRequest) ExecutionPlan {
        return self.vtable.planLinearNoBiasPair(self.ptr, request);
    }

    pub fn linearNoBiasPair(self: Provider, request: *const LinearNoBiasPairRequest) !?LinearNoBiasPairResult {
        return self.vtable.linearNoBiasPair(self.ptr, request);
    }

    pub fn mulMatId(self: Provider, request: *const MulMatIdRequest) !?c.mlx_array {
        return self.vtable.mulMatId(self.ptr, request);
    }

    pub fn moeLinearNoBias(self: Provider, request: *const MoeLinearNoBiasRequest) !?c.mlx_array {
        return self.mulMatId(request);
    }

    pub fn moeLinearNoBiasPair(self: Provider, request: *const MoeLinearNoBiasPairRequest) !?MoeLinearNoBiasPairResult {
        return self.vtable.moeLinearNoBiasPair(self.ptr, request);
    }

    pub fn compressedKeyScores(self: Provider, request: *const CompressedKeyScoresRequest) !?c.mlx_array {
        return self.vtable.compressedKeyScores(self.ptr, request);
    }

    pub fn compressedAttentionBlock(self: Provider, request: *const CompressedAttentionBlockRequest) !?CompressedAttentionBlockResult {
        return self.vtable.compressedAttentionBlock(self.ptr, request);
    }

    pub fn compressedAttentionSpan(self: Provider, request: *const CompressedAttentionSpanRequest) !?c.mlx_array {
        return self.vtable.compressedAttentionSpan(self.ptr, request);
    }

    pub fn lmHeadArgmax(self: Provider, request: *const LmHeadArgmaxRequest) !?c.mlx_array {
        return self.vtable.lmHeadArgmax(self.ptr, request);
    }

    pub fn decoderRuntimePrepareGreedy(self: Provider, request: *const ops.DecoderRuntimeGreedyRequest) !bool {
        return self.vtable.decoderRuntimePrepareGreedy(self.ptr, request);
    }

    pub fn decoderRuntimeResetState(self: Provider) !void {
        return self.vtable.decoderRuntimeResetState(self.ptr);
    }

    pub fn decoderRuntimePrepareAbsoluteEmbeddings(self: Provider, request: *const DecoderRuntimePrepareAbsoluteEmbeddingsRequest) !bool {
        return self.vtable.decoderRuntimePrepareAbsoluteEmbeddings(self.ptr, request);
    }

    pub fn decoderRuntimeEmbedAbsolutePosition(self: Provider, request: *const DecoderRuntimeEmbedAbsolutePositionRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeEmbedAbsolutePosition(self.ptr, request);
    }

    pub fn decoderRuntimePrepareLayerNorm(self: Provider, request: *const DecoderRuntimePrepareLayerNormRequest) !bool {
        return self.vtable.decoderRuntimePrepareLayerNorm(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLayerNorm(self: Provider, request: *const DecoderRuntimeApplyLayerNormRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyLayerNorm(self.ptr, request);
    }

    pub fn decoderRuntimePrepareRmsNorm(self: Provider, request: *const DecoderRuntimePrepareRmsNormRequest) !bool {
        return self.vtable.decoderRuntimePrepareRmsNorm(self.ptr, request);
    }

    pub fn decoderRuntimeApplyRmsNorm(self: Provider, request: *const DecoderRuntimeApplyRmsNormRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyRmsNorm(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLayerNormLinearArgmax(self: Provider, request: *const DecoderRuntimeApplyLayerNormLinearArgmaxRequest) !?usize {
        return self.vtable.decoderRuntimeApplyLayerNormLinearArgmax(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLayerNormLinear(self: Provider, request: *const DecoderRuntimeApplyLayerNormLinearRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyLayerNormLinear(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLayerNormLinearSample(self: Provider, request: *const DecoderRuntimeApplyLayerNormLinearSampleRequest) !?usize {
        return self.vtable.decoderRuntimeApplyLayerNormLinearSample(self.ptr, request);
    }

    pub fn decoderRuntimeApplyRmsNormLinearArgmax(self: Provider, request: *const DecoderRuntimeApplyRmsNormLinearArgmaxRequest) !?usize {
        return self.vtable.decoderRuntimeApplyRmsNormLinearArgmax(self.ptr, request);
    }

    pub fn decoderRuntimeApplyRmsNormLinear(self: Provider, request: *const DecoderRuntimeApplyRmsNormLinearRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyRmsNormLinear(self.ptr, request);
    }

    pub fn decoderRuntimeApplyRmsNormLinearSample(self: Provider, request: *const DecoderRuntimeApplyRmsNormLinearSampleRequest) !?usize {
        return self.vtable.decoderRuntimeApplyRmsNormLinearSample(self.ptr, request);
    }

    pub fn sampleLogitsDevice(self: Provider, request: *const SampleLogitsDeviceRequest) !?usize {
        return self.vtable.sampleLogitsDevice(self.ptr, request);
    }

    pub fn decoderRuntimePrepareLinear(self: Provider, request: *const DecoderRuntimePrepareLinearRequest) !bool {
        return self.vtable.decoderRuntimePrepareLinear(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLinear(self: Provider, request: *const DecoderRuntimeApplyLinearRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyLinear(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLinearArgmax(self: Provider, request: *const DecoderRuntimeApplyLinearArgmaxRequest) !?usize {
        return self.vtable.decoderRuntimeApplyLinearArgmax(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLinearPair(self: Provider, request: *const DecoderRuntimeApplyLinearPairRequest) !?LinearNoBiasPairResult {
        return self.vtable.decoderRuntimeApplyLinearPair(self.ptr, request);
    }

    pub fn decoderRuntimeApplyLinearQkv(self: Provider, request: *const DecoderRuntimeApplyLinearQkvRequest) !?LinearNoBiasTripleResult {
        return self.vtable.decoderRuntimeApplyLinearQkv(self.ptr, request);
    }

    pub fn decoderRuntimeApplyActivation(self: Provider, request: *const DecoderRuntimeApplyActivationRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyActivation(self.ptr, request);
    }

    pub fn decoderRuntimeApplyAdd(self: Provider, request: *const DecoderRuntimeApplyAddRequest) !?c.mlx_array {
        return self.vtable.decoderRuntimeApplyAdd(self.ptr, request);
    }

    pub fn runDenseFfnResidual(self: Provider, request: *const RunDenseFfnResidualRequest) !?c.mlx_array {
        return self.vtable.runDenseFfnResidual(self.ptr, request);
    }

    pub fn runGatedFfnResidual(self: Provider, request: *const RunGatedFfnResidualRequest) !?c.mlx_array {
        return self.vtable.runGatedFfnResidual(self.ptr, request);
    }

    pub fn runAttentionResidualPostLinear(self: Provider, request: *const RunAttentionResidualPostLinearRequest) !?c.mlx_array {
        return self.vtable.runAttentionResidualPostLinear(self.ptr, request);
    }

    pub fn runCompressedAttentionDenseDecoderBlock(self: Provider, request: *const RunCompressedAttentionDenseDecoderBlockRequest) !?c.mlx_array {
        return self.vtable.runCompressedAttentionDenseDecoderBlock(self.ptr, request);
    }

    pub fn runCompressedAttentionGatedDecoderBlock(self: Provider, request: *const RunCompressedAttentionGatedDecoderBlockRequest) !?c.mlx_array {
        return self.vtable.runCompressedAttentionGatedDecoderBlock(self.ptr, request);
    }

    pub fn hasGatheredSpanCache(self: Provider, source_ptr_id: usize, sequence_id: runtime_root.kv.manager.SequenceId, layer_index: usize) bool {
        return self.vtable.hasGatheredSpanCache(self.ptr, source_ptr_id, sequence_id, layer_index);
    }

    pub fn deinit(self: Provider) void {
        if (self.vtable.deinit) |deinit_fn| deinit_fn(self.ptr);
    }
};

const NullProvider = struct {
    fn planLinearNoBias(_: *anyopaque, _: *const LinearNoBiasPlanRequest) ExecutionPlan {
        return .unsupported;
    }

    fn linearNoBias(_: *anyopaque, _: *const LinearNoBiasRequest) !?c.mlx_array {
        return null;
    }

    fn planLinearNoBiasPair(_: *anyopaque, _: *const LinearNoBiasPairPlanRequest) ExecutionPlan {
        return .unsupported;
    }

    fn linearNoBiasPair(_: *anyopaque, _: *const LinearNoBiasPairRequest) !?LinearNoBiasPairResult {
        return null;
    }

    fn mulMatId(_: *anyopaque, _: *const MulMatIdRequest) !?c.mlx_array {
        return null;
    }

    fn moeLinearNoBias(ctx: *anyopaque, request: *const MoeLinearNoBiasRequest) !?c.mlx_array {
        return NullProvider.mulMatId(ctx, request);
    }

    fn moeLinearNoBiasPair(_: *anyopaque, _: *const MoeLinearNoBiasPairRequest) !?MoeLinearNoBiasPairResult {
        return null;
    }

    fn compressedKeyScores(_: *anyopaque, _: *const CompressedKeyScoresRequest) !?c.mlx_array {
        return null;
    }

    fn compressedAttentionBlock(_: *anyopaque, _: *const CompressedAttentionBlockRequest) !?CompressedAttentionBlockResult {
        return null;
    }

    fn compressedAttentionSpan(_: *anyopaque, _: *const CompressedAttentionSpanRequest) !?c.mlx_array {
        return null;
    }

    fn lmHeadArgmax(_: *anyopaque, _: *const LmHeadArgmaxRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimePrepareGreedy(_: *anyopaque, _: *const ops.DecoderRuntimeGreedyRequest) !bool {
        return false;
    }

    fn decoderRuntimeResetState(_: *anyopaque) !void {}

    fn decoderRuntimePrepareAbsoluteEmbeddings(_: *anyopaque, _: *const DecoderRuntimePrepareAbsoluteEmbeddingsRequest) !bool {
        return false;
    }

    fn decoderRuntimeEmbedAbsolutePosition(_: *anyopaque, _: *const DecoderRuntimeEmbedAbsolutePositionRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimePrepareLayerNorm(_: *anyopaque, _: *const DecoderRuntimePrepareLayerNormRequest) !bool {
        return false;
    }

    fn decoderRuntimeApplyLayerNorm(_: *anyopaque, _: *const DecoderRuntimeApplyLayerNormRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimePrepareRmsNorm(_: *anyopaque, _: *const DecoderRuntimePrepareRmsNormRequest) !bool {
        return false;
    }

    fn decoderRuntimeApplyRmsNorm(_: *anyopaque, _: *const DecoderRuntimeApplyRmsNormRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyLayerNormLinearArgmax(_: *anyopaque, _: *const DecoderRuntimeApplyLayerNormLinearArgmaxRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyLayerNormLinear(_: *anyopaque, _: *const DecoderRuntimeApplyLayerNormLinearRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyLayerNormLinearSample(_: *anyopaque, _: *const DecoderRuntimeApplyLayerNormLinearSampleRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyRmsNormLinearArgmax(_: *anyopaque, _: *const DecoderRuntimeApplyRmsNormLinearArgmaxRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyRmsNormLinear(_: *anyopaque, _: *const DecoderRuntimeApplyRmsNormLinearRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyRmsNormLinearSample(_: *anyopaque, _: *const DecoderRuntimeApplyRmsNormLinearSampleRequest) !?usize {
        return null;
    }

    fn sampleLogitsDevice(_: *anyopaque, _: *const SampleLogitsDeviceRequest) !?usize {
        return null;
    }

    fn decoderRuntimePrepareLinear(_: *anyopaque, _: *const DecoderRuntimePrepareLinearRequest) !bool {
        return false;
    }

    fn decoderRuntimeApplyLinear(_: *anyopaque, _: *const DecoderRuntimeApplyLinearRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyLinearArgmax(_: *anyopaque, _: *const DecoderRuntimeApplyLinearArgmaxRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyLinearPair(_: *anyopaque, _: *const DecoderRuntimeApplyLinearPairRequest) !?LinearNoBiasPairResult {
        return null;
    }

    fn decoderRuntimeApplyLinearQkv(_: *anyopaque, _: *const DecoderRuntimeApplyLinearQkvRequest) !?LinearNoBiasTripleResult {
        return null;
    }

    fn decoderRuntimeApplyActivation(_: *anyopaque, _: *const DecoderRuntimeApplyActivationRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyAdd(_: *anyopaque, _: *const DecoderRuntimeApplyAddRequest) !?c.mlx_array {
        return null;
    }

    fn runDenseFfnResidual(_: *anyopaque, _: *const RunDenseFfnResidualRequest) !?c.mlx_array {
        return null;
    }

    fn runGatedFfnResidual(_: *anyopaque, _: *const RunGatedFfnResidualRequest) !?c.mlx_array {
        return null;
    }

    fn runAttentionResidualPostLinear(_: *anyopaque, _: *const RunAttentionResidualPostLinearRequest) !?c.mlx_array {
        return null;
    }

    fn runCompressedAttentionDenseDecoderBlock(_: *anyopaque, _: *const RunCompressedAttentionDenseDecoderBlockRequest) !?c.mlx_array {
        return null;
    }

    fn runCompressedAttentionGatedDecoderBlock(_: *anyopaque, _: *const RunCompressedAttentionGatedDecoderBlockRequest) !?c.mlx_array {
        return null;
    }

    fn hasGatheredSpanCache(_: *anyopaque, _: usize, _: runtime_root.kv.manager.SequenceId, _: usize) bool {
        return false;
    }
};

const null_provider_vtable = Provider.VTable{
    .planLinearNoBias = &NullProvider.planLinearNoBias,
    .linearNoBias = &NullProvider.linearNoBias,
    .planLinearNoBiasPair = &NullProvider.planLinearNoBiasPair,
    .linearNoBiasPair = &NullProvider.linearNoBiasPair,
    .mulMatId = &NullProvider.mulMatId,
    .moeLinearNoBias = &NullProvider.moeLinearNoBias,
    .moeLinearNoBiasPair = &NullProvider.moeLinearNoBiasPair,
    .compressedKeyScores = &NullProvider.compressedKeyScores,
    .compressedAttentionBlock = &NullProvider.compressedAttentionBlock,
    .compressedAttentionSpan = &NullProvider.compressedAttentionSpan,
    .lmHeadArgmax = &NullProvider.lmHeadArgmax,
    .decoderRuntimePrepareGreedy = &NullProvider.decoderRuntimePrepareGreedy,
    .decoderRuntimeResetState = &NullProvider.decoderRuntimeResetState,
    .decoderRuntimePrepareAbsoluteEmbeddings = &NullProvider.decoderRuntimePrepareAbsoluteEmbeddings,
    .decoderRuntimeEmbedAbsolutePosition = &NullProvider.decoderRuntimeEmbedAbsolutePosition,
    .decoderRuntimePrepareLayerNorm = &NullProvider.decoderRuntimePrepareLayerNorm,
    .decoderRuntimeApplyLayerNorm = &NullProvider.decoderRuntimeApplyLayerNorm,
    .decoderRuntimePrepareRmsNorm = &NullProvider.decoderRuntimePrepareRmsNorm,
    .decoderRuntimeApplyRmsNorm = &NullProvider.decoderRuntimeApplyRmsNorm,
    .decoderRuntimeApplyLayerNormLinear = &NullProvider.decoderRuntimeApplyLayerNormLinear,
    .decoderRuntimeApplyLayerNormLinearArgmax = &NullProvider.decoderRuntimeApplyLayerNormLinearArgmax,
    .decoderRuntimeApplyLayerNormLinearSample = &NullProvider.decoderRuntimeApplyLayerNormLinearSample,
    .decoderRuntimeApplyRmsNormLinear = &NullProvider.decoderRuntimeApplyRmsNormLinear,
    .decoderRuntimeApplyRmsNormLinearArgmax = &NullProvider.decoderRuntimeApplyRmsNormLinearArgmax,
    .decoderRuntimeApplyRmsNormLinearSample = &NullProvider.decoderRuntimeApplyRmsNormLinearSample,
    .sampleLogitsDevice = &NullProvider.sampleLogitsDevice,
    .decoderRuntimePrepareLinear = &NullProvider.decoderRuntimePrepareLinear,
    .decoderRuntimeApplyLinear = &NullProvider.decoderRuntimeApplyLinear,
    .decoderRuntimeApplyLinearArgmax = &NullProvider.decoderRuntimeApplyLinearArgmax,
    .decoderRuntimeApplyLinearPair = &NullProvider.decoderRuntimeApplyLinearPair,
    .decoderRuntimeApplyLinearQkv = &NullProvider.decoderRuntimeApplyLinearQkv,
    .decoderRuntimeApplyActivation = &NullProvider.decoderRuntimeApplyActivation,
    .decoderRuntimeApplyAdd = &NullProvider.decoderRuntimeApplyAdd,
    .runDenseFfnResidual = &NullProvider.runDenseFfnResidual,
    .runGatedFfnResidual = &NullProvider.runGatedFfnResidual,
    .runAttentionResidualPostLinear = &NullProvider.runAttentionResidualPostLinear,
    .runCompressedAttentionDenseDecoderBlock = &NullProvider.runCompressedAttentionDenseDecoderBlock,
    .runCompressedAttentionGatedDecoderBlock = &NullProvider.runCompressedAttentionGatedDecoderBlock,
    .hasGatheredSpanCache = &NullProvider.hasGatheredSpanCache,
};

var null_provider_state: u8 = 0;

pub fn nullProvider() Provider {
    return defaultNullProvider();
}

pub fn defaultProvider() Provider {
    if (build_options.enable_mlx) {
        if (MetalProvider.create()) |provider| {
            return MetalProvider.provider(provider);
        } else |_| {}
    }
    return .{
        .ptr = &null_provider_state,
        .vtable = &null_provider_vtable,
    };
}

pub fn isNullProvider(provider: Provider) bool {
    return provider.vtable == &null_provider_vtable;
}

pub fn metalProvider(provider: Provider) ?*MetalProvider {
    if (!build_options.enable_mlx) return null;
    if (provider.vtable != &MetalProvider.vtable) return null;
    return @ptrCast(@alignCast(provider.ptr));
}

pub fn decoderRuntimeReady(provider: Provider) bool {
    if (provider.vtable == &null_provider_vtable) return false;
    if (metalProvider(provider)) |self| {
        return self.hasDecoderRuntime();
    }
    return false;
}

pub const MetalProvider = @import("metal_provider.zig").MetalProvider;

pub fn defaultNullProvider() Provider {
    return .{
        .ptr = &null_provider_state,
        .vtable = &null_provider_vtable,
    };
}

pub const PreparedWeightBytes = struct {
    bytes: []const u8,
    owned: bool,
};

pub fn prepareWeightBytesForLinear(
    storage: *QuantizedStorage,
    in_dim: usize,
    out_dim: usize,
    tensor_type: gguf_tensor_types.TensorType,
) !PreparedWeightBytes {
    timing_stats.prepare_calls += 1;
    if (storage.packed_expert != null) {
        if (storage.preparedBytes(.row_major_blocks)) |bytes| {
            timing_stats.prepare_cache_hits += 1;
            return .{ .bytes = bytes, .owned = false };
        }
        const prepared = try preparePackedExpertViewForLinear(storage, in_dim, out_dim, tensor_type);
        errdefer if (prepared.owned) std.heap.c_allocator.free(prepared.bytes);
        if (prepared.owned) {
            const owned = try storage.allocator.dupe(u8, prepared.bytes);
            std.heap.c_allocator.free(prepared.bytes);
            storage.setPreparedBytes(.row_major_blocks, owned, 0, 0);
            return .{ .bytes = storage.preparedBytes(.row_major_blocks).?, .owned = false };
        }
        return prepared;
    }
    if (storage.shape.len != 2) return error.InvalidQuantizedLinearShape;
    if (storage.shape[0] != @as(i64, @intCast(out_dim)) or storage.shape[1] != @as(i64, @intCast(in_dim))) {
        return error.InvalidQuantizedLinearShape;
    }
    return .{ .bytes = storage.raw_bytes, .owned = false };
}

pub fn preparePackedExpertViewForLinear(
    storage: *QuantizedStorage,
    in_dim: usize,
    out_dim: usize,
    tensor_type: gguf_tensor_types.TensorType,
) !PreparedWeightBytes {
    const packed_view = storage.packed_expert orelse return error.InvalidPackedExpertTensor;
    return slicePackedWeightBytes(storage, packed_view, in_dim, out_dim, tensor_type);
}

fn slicePackedWeightBytes(
    storage: *QuantizedStorage,
    packed_view: QuantizedStorage.PackedExpertView,
    in_dim: usize,
    out_dim: usize,
    tensor_type: gguf_tensor_types.TensorType,
) !PreparedWeightBytes {
    timing_stats.slice_calls += 1;
    const meta = try packedQuantizedMatrixMeta(storage.shape, packed_view, in_dim, out_dim, tensor_type);
    const total_bytes = out_dim * meta.row_blocks * meta.block_size;

    if (packed_view.expert_axis == 0) {
        const expert_base = try std.math.mul(usize, @intCast(packed_view.expert_index), total_bytes);
        return .{ .bytes = storage.raw_bytes[expert_base .. expert_base + total_bytes], .owned = false };
    }

    const out = try std.heap.c_allocator.alloc(u8, total_bytes);
    errdefer std.heap.c_allocator.free(out);

    for (0..out_dim) |row_index| {
        const row_block_base = try packedExpertRowBlockBase(meta, packed_view, row_index);
        const src_off = row_block_base * meta.block_size;
        const dst_off = row_index * meta.row_blocks * meta.block_size;
        @memcpy(out[dst_off .. dst_off + meta.row_blocks * meta.block_size], storage.raw_bytes[src_off .. src_off + meta.row_blocks * meta.block_size]);
    }
    return .{ .bytes = out, .owned = true };
}

const PackedQuantizedMatrixMeta = struct {
    layout: quant_codec.PackedExpertLinearMeta,
    row_blocks: usize,
    block_size: usize,
};

fn packedQuantizedMatrixMeta(
    shape: []const i64,
    packed_view: QuantizedStorage.PackedExpertView,
    in_dim: usize,
    out_dim: usize,
    tensor_type: gguf_tensor_types.TensorType,
) !PackedQuantizedMatrixMeta {
    const expert_axis: usize = @intCast(packed_view.expert_axis);
    const layout = try quant_codec.packedExpertLinearMeta(
        shape,
        expert_axis,
        packed_view.expert_index,
        in_dim,
        out_dim,
        tensor_type,
    );
    return .{
        .layout = layout,
        .row_blocks = layout.row_blocks,
        .block_size = layout.block_size,
    };
}

fn packedExpertRowBlockBase(
    meta: PackedQuantizedMatrixMeta,
    packed_view: QuantizedStorage.PackedExpertView,
    row_index: usize,
) !usize {
    return quant_codec.packedExpertLinearRowBlockBase(
        meta.layout,
        packed_view.expert_index,
        row_index,
        packed_view.row_offset,
    );
}

test "slice packed q8_0 weight bytes preserves selected expert rows" {
    var weight_raw: [136]u8 = [_]u8{0} ** 136;

    weight_raw[34] = 0x00;
    weight_raw[35] = 0x3C;
    for (0..32) |i| weight_raw[36 + i] = @bitCast(@as(i8, 3));

    weight_raw[102] = 0x00;
    weight_raw[103] = 0x3C;
    for (0..32) |i| weight_raw[104 + i] = @bitCast(@as(i8, 4));

    var storage = QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = &weight_raw,
        .shape = &.{ 2, 2, 32 },
        .packed_expert = .{
            .expert_index = 1,
            .expert_count = 2,
            .expert_axis = 1,
        },
        .allocator = std.testing.allocator,
    };

    const prepared = try slicePackedWeightBytes(&storage, storage.packed_expert.?, 32, 2, .{ .known = .Q8_0 });
    defer if (prepared.owned) std.heap.c_allocator.free(prepared.bytes);

    try std.testing.expectEqual(@as(usize, 68), prepared.bytes.len);
    try std.testing.expectEqual(@as(u8, @bitCast(@as(i8, 3))), prepared.bytes[2]);
    try std.testing.expectEqual(@as(u8, @bitCast(@as(i8, 4))), prepared.bytes[36]);
}

fn expectMetalLinearMatches(
    allocator: std.mem.Allocator,
    tensor_type: gguf_tensor_types.TensorType,
    input: []const f32,
    raw_weight: []u8,
    out_dim: usize,
    expected: []const f32,
) !void {
    try expectMetalLinearMatchesRows(allocator, tensor_type, input, raw_weight, 1, out_dim, expected);
}

fn expectMetalLinearMatchesRows(
    allocator: std.mem.Allocator,
    tensor_type: gguf_tensor_types.TensorType,
    input: []const f32,
    raw_weight: []u8,
    rows: usize,
    out_dim: usize,
    expected: []const f32,
) !void {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;
    if (rows == 0 or input.len % rows != 0) return error.InvalidTensorShape;
    const in_dim = input.len / rows;

    var shape_buf = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    var storage = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_weight,
        .shape = shape_buf[0..],
        .raw_owned = false,
        .allocator = allocator,
    };

    var provider = defaultProvider();
    defer provider.deinit();

    const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_arr = mlx.arrayFromFloat32(input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);
    const weight_shape = [_]i32{@intCast(raw_weight.len)};
    const weight_arr = mlx.arrayFromBytes(raw_weight, &weight_shape, c.MLX_UINT8);
    defer _ = c.mlx_array_free(weight_arr);

    try std.testing.expectEqual(ExecutionPlan.device_native, provider.planLinearNoBias(&.{
        .prepared_weight = .{
            .weight = weight_arr,
            .quantized_storage = &storage,
            .staged_backend_dense = false,
            .has_lazy_owner = false,
            .packed_expert = false,
        },
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
    }));

    const output_arr = (try provider.linearNoBias(&.{
        .input = input_arr,
        .weight = weight_arr,
        .quantized_storage = &storage,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .stream = mlx.gpuStream(),
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    const actual = try mlx.readFloat32(output_arr, allocator);
    defer allocator.free(actual);

    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-3);
    }
}

fn expectMetalLinearPairMatches(
    allocator: std.mem.Allocator,
    tensor_type: gguf_tensor_types.TensorType,
    input: []const f32,
    raw_weight_a: []u8,
    raw_weight_b: []u8,
    out_dim: usize,
    expected_a: []const f32,
    expected_b: []const f32,
) !void {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;
    const rows: usize = 1;
    if (input.len % rows != 0) return error.InvalidTensorShape;
    const in_dim = input.len / rows;

    var shape_buf = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    var storage_a = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_weight_a,
        .shape = shape_buf[0..],
        .raw_owned = false,
        .allocator = allocator,
    };
    var storage_b = QuantizedStorage{
        .tensor_type = tensor_type,
        .raw_bytes = raw_weight_b,
        .shape = shape_buf[0..],
        .raw_owned = false,
        .allocator = allocator,
    };

    var provider = defaultProvider();
    defer provider.deinit();

    const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_arr = mlx.arrayFromFloat32(input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);
    const weight_shape_a = [_]i32{@intCast(raw_weight_a.len)};
    const weight_arr_a = mlx.arrayFromBytes(raw_weight_a, &weight_shape_a, c.MLX_UINT8);
    defer _ = c.mlx_array_free(weight_arr_a);
    const weight_shape_b = [_]i32{@intCast(raw_weight_b.len)};
    const weight_arr_b = mlx.arrayFromBytes(raw_weight_b, &weight_shape_b, c.MLX_UINT8);
    defer _ = c.mlx_array_free(weight_arr_b);

    try std.testing.expectEqual(ExecutionPlan.device_native, provider.planLinearNoBiasPair(&.{
        .prepared_weight_a = .{
            .weight = weight_arr_a,
            .quantized_storage = &storage_a,
            .staged_backend_dense = false,
            .has_lazy_owner = false,
            .packed_expert = false,
        },
        .prepared_weight_b = .{
            .weight = weight_arr_b,
            .quantized_storage = &storage_b,
            .staged_backend_dense = false,
            .has_lazy_owner = false,
            .packed_expert = false,
        },
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
    }));

    const outputs = (try provider.linearNoBiasPair(&.{
        .input = input_arr,
        .weight_a = weight_arr_a,
        .weight_b = weight_arr_b,
        .quantized_storage_a = &storage_a,
        .quantized_storage_b = &storage_b,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .stream = mlx.gpuStream(),
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(outputs.first);
    defer _ = c.mlx_array_free(outputs.second);

    const actual_a = try mlx.readFloat32(outputs.first, allocator);
    defer allocator.free(actual_a);
    const actual_b = try mlx.readFloat32(outputs.second, allocator);
    defer allocator.free(actual_b);

    try std.testing.expectEqual(expected_a.len, actual_a.len);
    try std.testing.expectEqual(expected_b.len, actual_b.len);
    for (expected_a, actual_a) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-3);
    }
    for (expected_b, actual_b) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-3);
    }
}

fn bitnetActivationScaleReference(input_row: []const f32) f32 {
    var abs_max: f32 = 0.0;
    for (input_row) |value| abs_max = @max(abs_max, @abs(value));
    if (abs_max == 0.0) return 1.0;
    return abs_max / 127.0;
}

fn quantizeBitnetActivationReference(value: f32, scale: f32) f32 {
    if (scale == 0.0) return 0.0;
    const quantized = @min(@max(std.math.round(value / scale), -127.0), 127.0);
    return quantized * scale;
}

fn fillTL1ReferenceExpected(expected: []f32, rows: usize, input: []const f32, raw_weight: []const u8, out_dim: usize, in_dim: usize) !void {
    var shape_buf = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const view = try quant_codec.bitnetTL1View(shape_buf[0..], raw_weight);
    std.debug.assert(expected.len == rows * out_dim);
    std.debug.assert(input.len == rows * in_dim);

    for (0..rows) |r| {
        const input_row = input[r * in_dim ..][0..in_dim];
        const scale = bitnetActivationScaleReference(input_row);
        for (0..out_dim) |o| {
            var acc: f32 = 0.0;
            for (0..view.pairCount()) |pair_col| {
                const code = try view.pairCode(o, pair_col);
                const pair = quant_codec.bitnetTLPair(code);
                const col = pair_col * 2;
                const a0 = quantizeBitnetActivationReference(input_row[col], scale);
                const a1 = quantizeBitnetActivationReference(input_row[col + 1], scale);
                acc += (a0 * @as(f32, @floatFromInt(pair[0])) + a1 * @as(f32, @floatFromInt(pair[1]))) * view.weight_scale;
            }
            expected[r * out_dim + o] = acc;
        }
    }
}

fn fillTL2ReferenceExpected(expected: []f32, rows: usize, input: []const f32, raw_weight: []const u8, out_dim: usize, in_dim: usize) !void {
    var shape_buf = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
    const view = try quant_codec.bitnetTL2View(shape_buf[0..], raw_weight);
    std.debug.assert(expected.len == rows * out_dim);
    std.debug.assert(input.len == rows * in_dim);

    for (0..rows) |r| {
        const input_row = input[r * in_dim ..][0..in_dim];
        const scale = bitnetActivationScaleReference(input_row);
        for (0..out_dim) |o| {
            var acc: f32 = 0.0;
            for (0..view.threeCount()) |triple_col| {
                const code = try view.threeCode(o, triple_col);
                const negative = try view.threeNegative(o, triple_col);
                const triple = quant_codec.bitnetTL2Three(code, negative);
                const col = triple_col * 3;
                const a0 = quantizeBitnetActivationReference(input_row[col], scale);
                const a1 = quantizeBitnetActivationReference(input_row[col + 1], scale);
                const a2 = quantizeBitnetActivationReference(input_row[col + 2], scale);
                acc += (a0 * @as(f32, @floatFromInt(triple[0])) +
                    a1 * @as(f32, @floatFromInt(triple[1])) +
                    a2 * @as(f32, @floatFromInt(triple[2]))) * view.weight_scale;
            }
            for (0..view.twoPairCount()) |pair_col| {
                const code = try view.twoPairCode(o, pair_col);
                const pair = quant_codec.bitnetTLPair(code);
                const col = view.three_cols + pair_col * 2;
                const a0 = quantizeBitnetActivationReference(input_row[col], scale);
                const a1 = quantizeBitnetActivationReference(input_row[col + 1], scale);
                acc += (a0 * @as(f32, @floatFromInt(pair[0])) + a1 * @as(f32, @floatFromInt(pair[1]))) * view.weight_scale;
            }
            expected[r * out_dim + o] = acc;
        }
    }
}

fn setTL1PairCode(raw_bytes: []u8, rows: usize, cols: usize, row: usize, pair_col: usize, code: u4) !void {
    var shape_buf = [_]i64{ @intCast(rows), @intCast(cols) };
    const view = try quant_codec.bitnetTL1View(shape_buf[0..], raw_bytes);
    const config = view.config;
    const row_outer = row / config.bm;
    const row_in_bm = row % config.bm;
    const col_block = pair_col / (config.by / 2);
    const col_in_block = pair_col % (config.by / 2);
    const bm_block = row_in_bm / config.bmm;
    const row_in_bmm = row_in_bm % config.bmm;
    const by = 256 / config.bmm;
    const by_block = col_in_block / (by / 2);
    const pair_in_by = col_in_block % (by / 2);
    const row16 = row_in_bmm % 16;
    const bmm16 = row_in_bmm / 16;
    const by4 = pair_in_by / 2;
    const nibble = pair_in_by % 2;
    const index = ((((((row_outer * (config.cols / config.by) + col_block) * (config.bm / config.bmm) + bm_block) * (config.by / by) + by_block) * (config.bmm / 16) + bmm16) * (by / 4) + by4) * 16 + row16);
    if (index >= view.packed_bytes.len) return error.InvalidQuantizedDataSize;

    if (nibble == 0) {
        raw_bytes[index] = (raw_bytes[index] & 0x0F) | (@as(u8, code) << 4);
    } else {
        raw_bytes[index] = (raw_bytes[index] & 0xF0) | @as(u8, code);
    }
}

fn setTL2Three(raw_bytes: []u8, rows: usize, cols: usize, row: usize, triple_col: usize, code: u4, negative: bool) !void {
    var shape_buf = [_]i64{ @intCast(rows), @intCast(cols) };
    const view = try quant_codec.bitnetTL2View(shape_buf[0..], raw_bytes);
    const config = view.config;
    const by = 192 / config.bmm;
    const row_outer = row / config.bm;
    const row_in_bm = row % config.bm;
    const col_block = triple_col / (config.by / 3);
    const col_in_block = triple_col % (config.by / 3);
    const bm_block = row_in_bm / config.bmm;
    const row_in_bmm = row_in_bm % config.bmm;
    const by_block = col_in_block / (by / 3);
    const triple_in_by = col_in_block % (by / 3);
    const row_group = row_in_bmm / 8;
    const row_offset = row_in_bmm % 8;
    const final_group = if (row_group == 1) 2 else if (row_group == 2) 1 else row_group;
    const value_index = (((row_outer * (config.cols / config.by) + col_block) * (config.bm / config.bmm) + bm_block) * (config.by / by) + by_block) * config.bmm + final_group * 8 + row_offset;
    if (value_index >= view.three_values.len) return error.InvalidQuantizedDataSize;

    if (triple_in_by == 0) {
        raw_bytes[value_index] = (raw_bytes[value_index] & 0x0F) | (@as(u8, code) << 4);
    } else {
        raw_bytes[value_index] = (raw_bytes[value_index] & 0xF0) | @as(u8, code);
    }

    const sub_count = by / 3 * 4;
    const sign_block = col_in_block / sub_count;
    const sign_sub = col_in_block % sub_count;
    const line = sign_sub * config.bmm + row_in_bmm;
    const bit_lane = line / (config.bmm / 2);
    const half = line % (config.bmm / 2);
    const bit_index = 15 - bit_lane;
    const sign_index = (((row_outer * (config.cols / config.by) + col_block) * (config.bm / config.bmm) + bm_block) * (config.by / (by * 4)) + sign_block) * config.bmm + half * 2 + bit_index / 8;
    if (sign_index >= view.three_signs.len) return error.InvalidQuantizedDataSize;

    const sign_base = view.three_values.len;
    const mask: u8 = @as(u8, 1) << @intCast(bit_index % 8);
    if (negative) {
        raw_bytes[sign_base + sign_index] |= mask;
    } else {
        raw_bytes[sign_base + sign_index] &= ~mask;
    }
}

fn setTL2TwoPair(raw_bytes: []u8, rows: usize, cols: usize, row: usize, pair_col: usize, code: u4) !void {
    var shape_buf = [_]i64{ @intCast(rows), @intCast(cols) };
    const view = try quant_codec.bitnetTL2View(shape_buf[0..], raw_bytes);
    if (view.two_cols == 0) return error.InvalidTensorShape;

    const row_outer = row / 32;
    const row_in_bm = row % 32;
    const col_block = pair_col / 16;
    const col_in_block = pair_col % 16;
    const by_block = col_in_block / 2;
    const pair_in_by = col_in_block % 2;
    const row_group = row_in_bm / 8;
    const row_offset = row_in_bm % 8;
    const final_group = if (row_group == 1) 2 else if (row_group == 2) 1 else row_group;
    const tail_col_blocks = view.two_cols / 32;
    const index = (((row_outer * tail_col_blocks + col_block) * 8 + by_block) * 32 + final_group * 8 + row_offset);
    if (index >= view.two_values.len) return error.InvalidQuantizedDataSize;

    const pair_base = view.three_values.len + view.three_signs.len;
    if (pair_in_by == 0) {
        raw_bytes[pair_base + index] = (raw_bytes[pair_base + index] & 0x0F) | (@as(u8, code) << 4);
    } else {
        raw_bytes[pair_base + index] = (raw_bytes[pair_base + index] & 0xF0) | @as(u8, code);
    }
}

test "mlx metal q5_k kernel matches expected output" {
    var input: [256]f32 = [_]f32{1.0} ** 256;
    var weight_raw: [352]u8 = [_]u8{0} ** 352;

    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C;
    for (0..4) |i| weight_raw[4 + i] = 1;
    for (0..4) |i| weight_raw[12 + i] = 1;
    for (16..144) |i| weight_raw[i] = 0x11;

    const row1 = 176;
    weight_raw[row1 + 0] = 0x00;
    weight_raw[row1 + 1] = 0x3C;
    for (0..4) |i| weight_raw[row1 + 4 + i] = 1;
    for (0..4) |i| weight_raw[row1 + 12 + i] = 1;
    for (row1 + 16..row1 + 144) |i| weight_raw[i] = 0x22;

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .Q5_K }, &input, &weight_raw, 2, &.{ 1216.0, 1408.0 });
}

fn expectMetalCompressedKeyScoresMatch(
    allocator: std.mem.Allocator,
    comptime format: CompressedKeyFormat,
) !void {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    const q_len: usize = 2;
    const block_tokens: usize = 3;
    const num_heads: usize = 4;
    const num_kv_heads: usize = 2;
    const head_dim: usize = 64;
    const H_q = num_heads * head_dim;
    const H_kv = num_kv_heads * head_dim;
    const key_row_bytes = switch (format) {
        .polar4 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
        .turbo3 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim) + turboquant.turbo3ResidualBytes(num_kv_heads, head_dim),
    };

    var q: [q_len * H_q]f32 = undefined;
    var key: [block_tokens * H_kv]f32 = undefined;
    for (&q, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 23)) - 11)) / 11.0;
    }
    for (&key, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 31)) - 15)) / 15.0;
    }

    const encoded = try allocator.alloc(u8, block_tokens * key_row_bytes);
    defer allocator.free(encoded);
    for (0..block_tokens) |row| {
        const src = key[row * H_kv ..][0..H_kv];
        const dst = encoded[row * key_row_bytes ..][0..key_row_bytes];
        switch (format) {
            .polar4 => try turboquant.encodePolar4Key(src, dst, num_kv_heads, head_dim),
            .turbo3 => {
                const base_bytes = turboquant.turbo3KeyBytes(num_kv_heads, head_dim);
                const residual_bytes = turboquant.turbo3ResidualBytes(num_kv_heads, head_dim);
                try turboquant.encodeTurbo3Key(src, dst[0..base_bytes], num_kv_heads, head_dim);
                try turboquant.encodeTurbo3ResidualSketch(src, dst[0..base_bytes], dst[base_bytes..][0..residual_bytes], num_kv_heads, head_dim);
            },
        }
    }

    var provider = defaultProvider();
    defer provider.deinit();

    const q_shape = [_]i32{ @intCast(q_len), @intCast(H_q) };
    const q_arr = mlx.arrayFromFloat32(&q, &q_shape);
    defer _ = c.mlx_array_free(q_arr);
    const key_shape = [_]i32{@intCast(encoded.len)};
    const key_arr = mlx.arrayFromBytes(encoded, &key_shape, c.MLX_UINT8);
    defer _ = c.mlx_array_free(key_arr);

    const scores_arr = (try provider.compressedKeyScores(&.{
        .q = q_arr,
        .encoded_key = key_arr,
        .q_len = q_len,
        .block_tokens = block_tokens,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .key_row_bytes = key_row_bytes,
        .format = format,
        .stream = mlx.gpuStream(),
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(scores_arr);

    const actual = try mlx.readFloat32(scores_arr, allocator);
    defer allocator.free(actual);

    try std.testing.expectEqual(q_len * num_heads * block_tokens, actual.len);
    const heads_per_group = num_heads / num_kv_heads;
    for (0..num_heads) |h| {
        const kv_h = h / heads_per_group;
        for (0..q_len) |qi| {
            const query = q[qi * H_q + h * head_dim ..][0..head_dim];
            for (0..block_tokens) |ki| {
                const row_encoded = encoded[ki * key_row_bytes ..][0..key_row_bytes];
                const expected = switch (format) {
                    .polar4 => try turboquant.dotPolar4KeyFast(query, row_encoded, num_kv_heads, head_dim, kv_h),
                    .turbo3 => blk: {
                        const base_bytes = turboquant.turbo3KeyBytes(num_kv_heads, head_dim);
                        const residual_bytes = turboquant.turbo3ResidualBytes(num_kv_heads, head_dim);
                        const base_score = try turboquant.dotTurbo3KeyFast(query, row_encoded[0..base_bytes], num_kv_heads, head_dim, kv_h);
                        const residual_score = try turboquant.dotTurbo3ResidualSketch(query, row_encoded[base_bytes..][0..residual_bytes], num_kv_heads, head_dim, kv_h);
                        break :blk base_score + turboquant.turbo3_residual_default_scale * residual_score;
                    },
                };
                const idx = h * q_len * block_tokens + qi * block_tokens + ki;
                try std.testing.expectApproxEqAbs(expected, actual[idx], 1e-4);
            }
        }
    }
}

test "mlx metal polar4 compressed key scores match scalar reference" {
    try expectMetalCompressedKeyScoresMatch(std.testing.allocator, .polar4);
}

test "mlx metal turbo3 compressed key scores match scalar reference" {
    try expectMetalCompressedKeyScoresMatch(std.testing.allocator, .turbo3);
}

test "mlx metal lm head argmax matches scalar reference" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const rows: usize = 2;
    const in_dim: usize = 8;
    const out_dim: usize = 17;
    var hidden: [rows * in_dim]f32 = undefined;
    var weight: [out_dim * in_dim]f32 = undefined;
    for (&hidden, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 19)) - 9)) / 7.0;
    }
    for (&weight, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 23)) - 11)) / 9.0;
    }

    var expected: usize = 0;
    var best = -std.math.inf(f32);
    const last_hidden = hidden[(rows - 1) * in_dim ..][0..in_dim];
    for (0..out_dim) |vocab| {
        var acc: f32 = 0.0;
        for (0..in_dim) |d| acc += last_hidden[d] * weight[vocab * in_dim + d];
        if (acc > best) {
            best = acc;
            expected = vocab;
        }
    }

    var provider = defaultProvider();
    defer provider.deinit();

    const hidden_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const hidden_arr = mlx.arrayFromFloat32(&hidden, &hidden_shape);
    defer _ = c.mlx_array_free(hidden_arr);
    const weight_shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
    const weight_arr = mlx.arrayFromFloat32(&weight, &weight_shape);
    defer _ = c.mlx_array_free(weight_arr);

    const token_arr = (try provider.lmHeadArgmax(&.{
        .hidden = hidden_arr,
        .weight = weight_arr,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .stream = mlx.gpuStream(),
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(token_arr);

    const actual = try mlx.readFloat32(token_arr, allocator);
    defer allocator.free(actual);
    try std.testing.expectEqual(@as(usize, 1), actual.len);
    try std.testing.expectEqual(@as(f32, @floatFromInt(expected)), actual[0]);
}

test "mlx metal whole-token runtime reserves persistent buffers" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;
    try std.testing.expect(try provider.reserveDecoderRuntime(16 * 1024, @sizeOf(u32)));
}

test "mlx metal whole-token layer norm slot matches expected output" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const weight_shape = [_]i32{4};
    const input_shape = [_]i32{ 1, 4 };
    const ones = [_]f32{ 1.0, 1.0, 1.0, 1.0 };
    const zeros = [_]f32{ 0.0, 0.0, 0.0, 0.0 };
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    const weight = mlx.arrayFromFloat32(&ones, &weight_shape);
    defer _ = c.mlx_array_free(weight);
    const bias = mlx.arrayFromFloat32(&zeros, &weight_shape);
    defer _ = c.mlx_array_free(bias);
    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareLayerNorm(&.{
        .weight = weight,
        .bias = bias,
        .slot = 0,
        .hidden_size = 4,
    }));

    const output_arr = (try provider_api.decoderRuntimeApplyLayerNorm(&.{
        .input = input_arr,
        .slot = 0,
        .hidden_size = 4,
        .eps = 1e-5,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    const actual = try mlx.readFloat32(output_arr, std.testing.allocator);
    defer std.testing.allocator.free(actual);

    const expected = [_]f32{ -1.3416355, -0.44721183, 0.44721183, 1.3416355 };
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-4);
    }
}

test "mlx metal whole-token rms norm slot matches expected output" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const weight_shape = [_]i32{4};
    const input_shape = [_]i32{ 1, 4 };
    const weight_data = [_]f32{ 1.0, 1.0, 1.0, 1.0 };
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    const weight = mlx.arrayFromFloat32(&weight_data, &weight_shape);
    defer _ = c.mlx_array_free(weight);
    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareRmsNorm(&.{
        .weight = weight,
        .slot = 0,
        .hidden_size = 4,
    }));

    const output_arr = (try provider_api.decoderRuntimeApplyRmsNorm(&.{
        .input = input_arr,
        .slot = 0,
        .hidden_size = 4,
        .eps = 1e-5,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    const actual = try mlx.readFloat32(output_arr, std.testing.allocator);
    defer std.testing.allocator.free(actual);

    const inv_rms: f32 = 1.0 / @sqrt(7.5 + 1e-5);
    const expected = [_]f32{ 1.0 * inv_rms, 2.0 * inv_rms, 3.0 * inv_rms, 4.0 * inv_rms };
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-4);
    }
}

test "mlx metal whole-token layer norm plus linear argmax matches split path" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 4;
    const out_dim: usize = 3;
    const weight_shape = [_]i32{@intCast(hidden_size)};
    const linear_weight_shape = [_]i32{ @intCast(out_dim), @intCast(hidden_size) };
    const linear_bias_shape = [_]i32{@intCast(out_dim)};
    const input_shape = [_]i32{ 1, @intCast(hidden_size) };

    const norm_weight = [_]f32{ 1.0, 1.0, 1.0, 1.0 };
    const norm_bias = [_]f32{ 0.0, 0.0, 0.0, 0.0 };
    const linear_weight = [_]f32{
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 2.0, 0.0,
        0.0, 0.0, 0.0, 3.0,
    };
    const linear_bias = [_]f32{ 0.0, 0.25, -0.5 };
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    const norm_weight_arr = mlx.arrayFromFloat32(&norm_weight, &weight_shape);
    defer _ = c.mlx_array_free(norm_weight_arr);
    const norm_bias_arr = mlx.arrayFromFloat32(&norm_bias, &weight_shape);
    defer _ = c.mlx_array_free(norm_bias_arr);
    const linear_weight_arr = mlx.arrayFromFloat32(&linear_weight, &linear_weight_shape);
    defer _ = c.mlx_array_free(linear_weight_arr);
    const linear_bias_arr = mlx.arrayFromFloat32(&linear_bias, &linear_bias_shape);
    defer _ = c.mlx_array_free(linear_bias_arr);
    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareLayerNorm(&.{
        .weight = norm_weight_arr,
        .bias = norm_bias_arr,
        .slot = 0,
        .hidden_size = hidden_size,
    }));
    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = linear_weight_arr,
        .bias = linear_bias_arr,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    }));

    const fused = (try provider_api.decoderRuntimeApplyLayerNormLinearArgmax(&.{
        .input = input_arr,
        .norm_slot = 0,
        .linear_slot = 0,
        .hidden_size = hidden_size,
        .eps = 1e-5,
        .out_dim = out_dim,
    })) orelse return error.MlxProviderReturnedNull;

    const normed = (try provider_api.decoderRuntimeApplyLayerNorm(&.{
        .input = input_arr,
        .slot = 0,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(normed);
    const split = (try provider_api.decoderRuntimeApplyLinearArgmax(&.{
        .input = normed,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.MlxProviderReturnedNull;

    try std.testing.expectEqual(split, fused);
}

test "mlx metal whole-token rms norm plus linear argmax matches split path" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 4;
    const out_dim: usize = 3;
    const weight_shape = [_]i32{@intCast(hidden_size)};
    const linear_weight_shape = [_]i32{ @intCast(out_dim), @intCast(hidden_size) };
    const linear_bias_shape = [_]i32{@intCast(out_dim)};
    const input_shape = [_]i32{ 1, @intCast(hidden_size) };

    const norm_weight = [_]f32{ 1.0, 1.0, 1.0, 1.0 };
    const linear_weight = [_]f32{
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 2.0, 0.0,
        0.0, 0.0, 0.0, 3.0,
    };
    const linear_bias = [_]f32{ 0.0, 0.25, -0.5 };
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    const norm_weight_arr = mlx.arrayFromFloat32(&norm_weight, &weight_shape);
    defer _ = c.mlx_array_free(norm_weight_arr);
    const linear_weight_arr = mlx.arrayFromFloat32(&linear_weight, &linear_weight_shape);
    defer _ = c.mlx_array_free(linear_weight_arr);
    const linear_bias_arr = mlx.arrayFromFloat32(&linear_bias, &linear_bias_shape);
    defer _ = c.mlx_array_free(linear_bias_arr);
    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareRmsNorm(&.{
        .weight = norm_weight_arr,
        .slot = 0,
        .hidden_size = hidden_size,
    }));
    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = linear_weight_arr,
        .bias = linear_bias_arr,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    }));

    const fused = (try provider_api.decoderRuntimeApplyRmsNormLinearArgmax(&.{
        .input = input_arr,
        .norm_slot = 0,
        .linear_slot = 0,
        .hidden_size = hidden_size,
        .eps = 1e-5,
        .out_dim = out_dim,
    })) orelse return error.MlxProviderReturnedNull;

    const normed = (try provider_api.decoderRuntimeApplyRmsNorm(&.{
        .input = input_arr,
        .slot = 0,
        .hidden_size = hidden_size,
        .eps = 1e-5,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(normed);
    const split = (try provider_api.decoderRuntimeApplyLinearArgmax(&.{
        .input = normed,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = out_dim,
    })) orelse return error.MlxProviderReturnedNull;

    try std.testing.expectEqual(split, fused);
}

test "mlx metal whole-token linear slot matches expected output" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const weight_shape = [_]i32{ 2, 4 };
    const bias_shape = [_]i32{2};
    const input_shape = [_]i32{ 1, 4 };
    const weight = [_]f32{
        1.0,  2.0, 3.0,  4.0,
        -1.0, 0.5, 0.25, 2.0,
    };
    const bias = [_]f32{ 0.5, -1.0 };
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    const weight_arr = mlx.arrayFromFloat32(&weight, &weight_shape);
    defer _ = c.mlx_array_free(weight_arr);
    const bias_arr = mlx.arrayFromFloat32(&bias, &bias_shape);
    defer _ = c.mlx_array_free(bias_arr);
    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = weight_arr,
        .bias = bias_arr,
        .slot = 0,
        .in_dim = 4,
        .out_dim = 2,
    }));

    const output_arr = (try provider_api.decoderRuntimeApplyLinear(&.{
        .input = input_arr,
        .slot = 0,
        .in_dim = 4,
        .out_dim = 2,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    const actual = try mlx.readFloat32(output_arr, std.testing.allocator);
    defer std.testing.allocator.free(actual);

    const expected = [_]f32{ 30.5, 7.75 };
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
}

test "mlx metal whole-token linear slot accepts quantized i2_s" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const input_shape = [_]i32{ 1, 128 };
    const bias_shape = [_]i32{1};
    var input: [128]f32 = [_]f32{1.0} ** 128;
    var weight_raw: [32]u8 = [_]u8{0} ** 32;
    for (0..32) |i| weight_raw[i] = 0b10_10_10_10;
    const bias = [_]f32{0.0};

    var shape_buf = [_]i64{ 1, 128 };
    var storage = QuantizedStorage{
        .tensor_type = .{ .known = .I2_S },
        .raw_bytes = weight_raw[0..],
        .shape = shape_buf[0..],
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };

    const weight_shape = [_]i32{@intCast(weight_raw.len)};
    const weight_arr = mlx.arrayFromBytes(&weight_raw, &weight_shape, c.MLX_UINT8);
    defer _ = c.mlx_array_free(weight_arr);
    const bias_arr = mlx.arrayFromFloat32(&bias, &bias_shape);
    defer _ = c.mlx_array_free(bias_arr);
    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = weight_arr,
        .bias = bias_arr,
        .quantized_storage = &storage,
        .slot = 0,
        .in_dim = 128,
        .out_dim = 1,
    }));

    const output_arr = (try provider_api.decoderRuntimeApplyLinear(&.{
        .input = input_arr,
        .slot = 0,
        .in_dim = 128,
        .out_dim = 1,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    const actual = try mlx.readFloat32(output_arr, std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqual(@as(usize, 1), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, 128.0), actual[0], 1e-4);
}

test "mlx metal whole-token add matches expected output" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const input_shape = [_]i32{ 1, 4 };
    const lhs = [_]f32{ 1.0, 2.5, -3.0, 4.25 };
    const rhs = [_]f32{ -0.5, 1.5, 2.0, -1.25 };

    const lhs_arr = mlx.arrayFromFloat32(&lhs, &input_shape);
    defer _ = c.mlx_array_free(lhs_arr);
    const rhs_arr = mlx.arrayFromFloat32(&rhs, &input_shape);
    defer _ = c.mlx_array_free(rhs_arr);

    const output_arr = (try provider_api.decoderRuntimeApplyAdd(&.{
        .lhs = lhs_arr,
        .rhs = rhs_arr,
        .dim = 4,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    const actual = try mlx.readFloat32(output_arr, std.testing.allocator);
    defer std.testing.allocator.free(actual);

    const expected = [_]f32{ 0.5, 4.0, -1.0, 3.0 };
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
}

test "mlx metal whole-token gelu activation matches tanh reference" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const input_shape = [_]i32{ 1, 4 };
    const input = [_]f32{ -2.0, -0.5, 0.0, 1.5 };

    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);

    const output_arr = (try provider_api.decoderRuntimeApplyActivation(&.{
        .input = input_arr,
        .kind = .gelu,
        .dim = input.len,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(output_arr);

    var expected = input;
    activations.gelu(&expected);

    const actual = try mlx.readFloat32(output_arr, std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqual(input.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
}

test "mlx metal whole-token linear activation linear residual matches split path" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 3;
    const intermediate_size: usize = 4;
    const input_shape = [_]i32{ 1, @intCast(hidden_size) };
    const residual_shape = [_]i32{ 1, @intCast(hidden_size) };
    const first_weight_shape = [_]i32{ @intCast(intermediate_size), @intCast(hidden_size) };
    const first_bias_shape = [_]i32{@intCast(intermediate_size)};
    const second_weight_shape = [_]i32{ @intCast(hidden_size), @intCast(intermediate_size) };
    const second_bias_shape = [_]i32{@intCast(hidden_size)};

    const input = [_]f32{ 0.5, -1.25, 2.0 };
    const residual = [_]f32{ 1.0, 0.25, -0.75 };
    const first_weight = [_]f32{
        0.2,  -0.1, 0.4,
        -0.3, 0.5,  0.1,
        0.7,  0.2,  -0.6,
        0.1,  -0.4, 0.3,
    };
    const first_bias = [_]f32{ 0.05, -0.2, 0.15, 0.3 };
    const second_weight = [_]f32{
        0.4,  -0.3, 0.2,  0.1,
        -0.5, 0.6,  -0.1, 0.2,
        0.3,  0.25, 0.5,  -0.4,
    };
    const second_bias = [_]f32{ 0.1, -0.15, 0.05 };

    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);
    const residual_arr = mlx.arrayFromFloat32(&residual, &residual_shape);
    defer _ = c.mlx_array_free(residual_arr);
    const first_weight_arr = mlx.arrayFromFloat32(&first_weight, &first_weight_shape);
    defer _ = c.mlx_array_free(first_weight_arr);
    const first_bias_arr = mlx.arrayFromFloat32(&first_bias, &first_bias_shape);
    defer _ = c.mlx_array_free(first_bias_arr);
    const second_weight_arr = mlx.arrayFromFloat32(&second_weight, &second_weight_shape);
    defer _ = c.mlx_array_free(second_weight_arr);
    const second_bias_arr = mlx.arrayFromFloat32(&second_bias, &second_bias_shape);
    defer _ = c.mlx_array_free(second_bias_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = first_weight_arr,
        .bias = first_bias_arr,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    }));
    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = second_weight_arr,
        .bias = second_bias_arr,
        .slot = 1,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
    }));

    const fused = (try provider_api.runDenseFfnResidual(&.{
        .input = input_arr,
        .residual = residual_arr,
        .first_linear_slot = 0,
        .second_linear_slot = 1,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = .gelu_new,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(fused);

    const first = (try provider_api.decoderRuntimeApplyLinear(&.{
        .input = input_arr,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(first);
    const activated = (try provider_api.decoderRuntimeApplyActivation(&.{
        .input = first,
        .kind = .gelu_new,
        .dim = intermediate_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(activated);
    const second = (try provider_api.decoderRuntimeApplyLinear(&.{
        .input = activated,
        .slot = 1,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(second);
    const split = (try provider_api.decoderRuntimeApplyAdd(&.{
        .lhs = second,
        .rhs = residual_arr,
        .dim = hidden_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(split);

    const fused_host = try mlx.readFloat32(fused, std.testing.allocator);
    defer std.testing.allocator.free(fused_host);
    const split_host = try mlx.readFloat32(split, std.testing.allocator);
    defer std.testing.allocator.free(split_host);

    try std.testing.expectEqual(split_host.len, fused_host.len);
    for (split_host, fused_host) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 2e-4);
    }
}

test "mlx metal whole-token gated linear residual matches split path" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (!mlx.metalDeviceAvailable()) return error.SkipZigTest;

    var provider = try MetalProvider.create();
    defer provider.deinitOwned();
    const provider_api = Provider{
        .ptr = &provider,
        .vtable = &MetalProvider.vtable,
    };

    if (!provider.hasDecoderRuntime()) return error.SkipZigTest;

    const hidden_size: usize = 3;
    const intermediate_size: usize = 4;
    const input_shape = [_]i32{ 1, @intCast(hidden_size) };
    const residual_shape = [_]i32{ 1, @intCast(hidden_size) };
    const pair_weight_shape = [_]i32{ @intCast(intermediate_size), @intCast(hidden_size) };
    const pair_bias_shape = [_]i32{@intCast(intermediate_size)};
    const down_weight_shape = [_]i32{ @intCast(hidden_size), @intCast(intermediate_size) };
    const down_bias_shape = [_]i32{@intCast(hidden_size)};

    const input = [_]f32{ -0.75, 0.5, 1.25 };
    const residual = [_]f32{ 0.25, -1.0, 0.75 };
    const gate_weight = [_]f32{
        0.4,  -0.2, 0.1,
        -0.3, 0.7,  0.5,
        0.2,  0.1,  -0.6,
        0.5,  -0.4, 0.3,
    };
    const gate_bias = [_]f32{ 0.05, -0.1, 0.2, -0.15 };
    const up_weight = [_]f32{
        -0.2, 0.3,  0.6,
        0.5,  -0.4, 0.2,
        0.1,  0.8,  -0.3,
        -0.6, 0.25, 0.4,
    };
    const up_bias = [_]f32{ 0.1, 0.2, -0.05, 0.15 };
    const down_weight = [_]f32{
        0.3,  -0.2, 0.4,  0.1,
        -0.5, 0.6,  -0.1, 0.25,
        0.2,  0.15, 0.35, -0.45,
    };
    const down_bias = [_]f32{ 0.05, -0.2, 0.1 };

    const input_arr = mlx.arrayFromFloat32(&input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);
    const residual_arr = mlx.arrayFromFloat32(&residual, &residual_shape);
    defer _ = c.mlx_array_free(residual_arr);
    const gate_weight_arr = mlx.arrayFromFloat32(&gate_weight, &pair_weight_shape);
    defer _ = c.mlx_array_free(gate_weight_arr);
    const gate_bias_arr = mlx.arrayFromFloat32(&gate_bias, &pair_bias_shape);
    defer _ = c.mlx_array_free(gate_bias_arr);
    const up_weight_arr = mlx.arrayFromFloat32(&up_weight, &pair_weight_shape);
    defer _ = c.mlx_array_free(up_weight_arr);
    const up_bias_arr = mlx.arrayFromFloat32(&up_bias, &pair_bias_shape);
    defer _ = c.mlx_array_free(up_bias_arr);
    const down_weight_arr = mlx.arrayFromFloat32(&down_weight, &down_weight_shape);
    defer _ = c.mlx_array_free(down_weight_arr);
    const down_bias_arr = mlx.arrayFromFloat32(&down_bias, &down_bias_shape);
    defer _ = c.mlx_array_free(down_bias_arr);

    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = gate_weight_arr,
        .bias = gate_bias_arr,
        .slot = 0,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    }));
    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = up_weight_arr,
        .bias = up_bias_arr,
        .slot = 1,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    }));
    try std.testing.expect(try provider_api.decoderRuntimePrepareLinear(&.{
        .weight = down_weight_arr,
        .bias = down_bias_arr,
        .slot = 2,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
    }));

    const fused = (try provider_api.runGatedFfnResidual(&.{
        .input = input_arr,
        .residual = residual_arr,
        .gate_linear_slot = 0,
        .up_linear_slot = 1,
        .down_linear_slot = 2,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .activation = .silu,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(fused);

    const gate_up = (try provider_api.decoderRuntimeApplyLinearPair(&.{
        .input = input_arr,
        .slot_a = 0,
        .slot_b = 1,
        .in_dim = hidden_size,
        .out_dim = intermediate_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(gate_up.first);
    defer _ = c.mlx_array_free(gate_up.second);
    const activated = (try provider_api.decoderRuntimeApplyActivation(&.{
        .input = gate_up.first,
        .kind = .silu,
        .dim = intermediate_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(activated);

    var gated = c.mlx_array_new();
    defer _ = c.mlx_array_free(gated);
    try mlx.check(c.mlx_multiply(&gated, activated, gate_up.second, mlx.gpuStream()));

    const projected = (try provider_api.decoderRuntimeApplyLinear(&.{
        .input = gated,
        .slot = 2,
        .in_dim = intermediate_size,
        .out_dim = hidden_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(projected);
    const split = (try provider_api.decoderRuntimeApplyAdd(&.{
        .lhs = projected,
        .rhs = residual_arr,
        .dim = hidden_size,
    })) orelse return error.MlxProviderReturnedNull;
    defer _ = c.mlx_array_free(split);

    const fused_host = try mlx.readFloat32(fused, std.testing.allocator);
    defer std.testing.allocator.free(fused_host);
    const split_host = try mlx.readFloat32(split, std.testing.allocator);
    defer std.testing.allocator.free(split_host);

    try std.testing.expectEqual(split_host.len, fused_host.len);
    for (split_host, fused_host) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 2e-4);
    }
}

test "mlx metal q4_0 kernel uses low-half then high-half nibble order" {
    var input: [32]f32 = [_]f32{0.0} ** 32;
    input[0] = 1.0;
    input[1] = 1.0;
    input[16] = 1.0;
    input[17] = 1.0;

    var weight_raw: [18]u8 = [_]u8{0x88} ** 18;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C;
    weight_raw[2] = 0x10;
    weight_raw[3] = 0x32;

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .Q4_0 }, &input, &weight_raw, 1, &.{-26.0});
}

test "mlx metal q4_1 kernel uses low-half then high-half nibble order" {
    var input: [32]f32 = [_]f32{0.0} ** 32;
    input[0] = 1.0;
    input[1] = 1.0;
    input[16] = 1.0;
    input[17] = 1.0;

    var weight_raw: [20]u8 = [_]u8{0x11} ** 20;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C; // d = 1.0
    weight_raw[2] = 0x00;
    weight_raw[3] = 0x40; // m = 2.0
    weight_raw[4] = 0x30;
    weight_raw[5] = 0x52;

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .Q4_1 }, &input, &weight_raw, 1, &.{18.0});
}

test "mlx metal q5_1 kernel applies high bits scale and minimum" {
    var input: [32]f32 = [_]f32{0.0} ** 32;
    input[0] = 1.0;
    input[1] = 1.0;
    input[16] = 1.0;
    input[17] = 1.0;

    var weight_raw: [24]u8 = [_]u8{0} ** 24;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C; // d = 1.0
    weight_raw[2] = 0x00;
    weight_raw[3] = 0x40; // m = 2.0
    weight_raw[4] = 0x02;
    weight_raw[6] = 0x01; // high bits for values 1 and 16
    weight_raw[8] = 0x43;
    weight_raw[9] = 0x65;

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .Q5_1 }, &input, &weight_raw, 1, &.{58.0});
}

test "mlx metal q8_1 kernel uses signed values after block sum" {
    var input: [32]f32 = [_]f32{0.0} ** 32;
    input[0] = 1.0;
    input[1] = 1.0;
    input[2] = 1.0;
    input[3] = 1.0;

    var weight_raw: [36]u8 = [_]u8{0} ** 36;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C; // d = 1.0
    weight_raw[4] = 1;
    weight_raw[5] = 0xFE; // -2
    weight_raw[6] = 3;
    weight_raw[7] = 0xFC; // -4

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .Q8_1 }, &input, &weight_raw, 1, &.{-2.0});
}

test "mlx metal i8_s kernel uses signed int8 weights directly" {
    var input: [4]f32 = .{ 1.5, -2.0, 0.25, 3.0 };
    var weight_raw: [8]u8 = .{
        @bitCast(@as(i8, 2)),
        @bitCast(@as(i8, -3)),
        @bitCast(@as(i8, 4)),
        @bitCast(@as(i8, 1)),
        @bitCast(@as(i8, -1)),
        @bitCast(@as(i8, 5)),
        @bitCast(@as(i8, 2)),
        @bitCast(@as(i8, -2)),
    };

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .I8_S }, &input, &weight_raw, 2, &.{ 13.0, -17.0 });
}

test "mlx metal i2_s kernel uses per-row int8 activation quantization" {
    var input: [128]f32 = [_]f32{0.0} ** 128;
    input[0] = 0.51;
    input[1] = 1.0;

    var weight_raw: [32]u8 = [_]u8{0} ** 32;
    weight_raw[0] = 0b10_00_00_00;
    weight_raw[1] = 0b10_00_00_00;

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .I2_S }, &input, &weight_raw, 1, &.{65.0 / 127.0 + 1.0});
}

test "mlx metal i2_s pair kernel computes sibling matmuls" {
    var input: [128]f32 = [_]f32{1.0} ** 128;
    var weight_raw_a: [32]u8 = [_]u8{0} ** 32;
    var weight_raw_b: [32]u8 = [_]u8{0} ** 32;

    for (0..32) |i| {
        weight_raw_a[i] = 0b10_10_10_10;
        weight_raw_b[i] = 0b00_00_00_00;
    }

    try expectMetalLinearPairMatches(
        std.testing.allocator,
        .{ .known = .I2_S },
        &input,
        &weight_raw_a,
        &weight_raw_b,
        1,
        &.{128.0},
        &.{-128.0},
    );
}

test "mlx metal tl1 kernel matches activation-quantized packed LUT layout" {
    const allocator = std.testing.allocator;
    const rows: usize = 1536;
    const cols: usize = 1536;
    const packed_len = rows * cols / 4;

    const input = try allocator.alloc(f32, cols);
    defer allocator.free(input);
    @memset(input, 0.0);
    input[0] = 127.0;
    input[1] = 2.0;
    input[2] = -3.0;
    input[3] = 4.0;

    const weight_raw = try allocator.alloc(u8, packed_len + @sizeOf(f32));
    defer allocator.free(weight_raw);
    @memset(weight_raw, 0x44);
    std.mem.writeInt(u32, weight_raw[packed_len..][0..4], @bitCast(@as(f32, 0.25)), .little);

    try setTL1PairCode(weight_raw, rows, cols, 0, 0, 8);
    try setTL1PairCode(weight_raw, rows, cols, 0, 1, 2);
    try setTL1PairCode(weight_raw, rows, cols, 1, 0, 0);
    try setTL1PairCode(weight_raw, rows, cols, 1, 1, 6);

    const expected = try allocator.alloc(f32, rows);
    defer allocator.free(expected);
    @memset(expected, 0.0);
    expected[0] = 34.0;
    expected[1] = -34.0;

    try expectMetalLinearMatches(allocator, .{ .known = .TL1 }, input, weight_raw, rows, expected);
}

test "mlx metal tl2 kernel matches activation-quantized packed LUT layout" {
    const allocator = std.testing.allocator;
    const rows: usize = 1536;
    const cols: usize = 1536;
    const total_len_u64 = gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @as(i64, @intCast(cols)), @as(i64, @intCast(rows)) }) orelse return error.UnsupportedTensorShape;
    const total_len: usize = @intCast(total_len_u64);
    const scale_off = total_len - 32;

    const input = try allocator.alloc(f32, cols);
    defer allocator.free(input);
    @memset(input, 0.0);
    input[0] = 127.0;
    input[1] = 2.0;
    input[2] = -3.0;
    input[3] = 4.0;
    input[4] = -5.0;
    input[5] = 6.0;

    const weight_raw = try allocator.alloc(u8, total_len);
    defer allocator.free(weight_raw);
    @memset(weight_raw, 0x00);
    std.mem.writeInt(u32, weight_raw[scale_off..][0..4], @bitCast(@as(f32, 0.25)), .little);

    try setTL2Three(weight_raw, rows, cols, 0, 0, 13, false);
    try setTL2Three(weight_raw, rows, cols, 0, 1, 5, false);
    try setTL2Three(weight_raw, rows, cols, 1, 0, 1, false);
    try setTL2Three(weight_raw, rows, cols, 1, 1, 11, true);

    const expected = try allocator.alloc(f32, rows);
    defer allocator.free(expected);
    @memset(expected, 0.0);
    expected[0] = 32.25;
    expected[1] = 1.0;

    try expectMetalLinearMatches(allocator, .bitnet_tl2, input, weight_raw, rows, expected);
}

test "mlx metal tl1 kernel matches multi-row reference on preset shape" {
    const allocator = std.testing.allocator;
    const rows: usize = 2;
    const out_dim: usize = 1024;
    const in_dim: usize = 4096;
    const packed_len = out_dim * in_dim / 4;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    @memset(input, 0.0);
    input[0] = 7.5;
    input[1] = -3.0;
    input[2] = 2.0;
    input[3] = -1.0;
    input[in_dim + 0] = -12.0;
    input[in_dim + 1] = 5.5;
    input[in_dim + 2] = 0.75;
    input[in_dim + 3] = -8.0;

    const weight_raw = try allocator.alloc(u8, packed_len + 32);
    defer allocator.free(weight_raw);
    @memset(weight_raw, 0x44);
    std.mem.writeInt(u32, weight_raw[packed_len..][0..4], @bitCast(@as(f32, 0.5)), .little);

    try setTL1PairCode(weight_raw, out_dim, in_dim, 0, 0, 8);
    try setTL1PairCode(weight_raw, out_dim, in_dim, 0, 1, 2);
    try setTL1PairCode(weight_raw, out_dim, in_dim, 1, 0, 0);
    try setTL1PairCode(weight_raw, out_dim, in_dim, 1, 1, 6);

    const expected = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(expected);
    try fillTL1ReferenceExpected(expected, rows, input, weight_raw, out_dim, in_dim);

    try expectMetalLinearMatchesRows(allocator, .{ .known = .TL1 }, input, weight_raw, rows, out_dim, expected);
}

test "mlx metal tl2 kernel matches multi-row reference including tail pairs" {
    const allocator = std.testing.allocator;
    const rows: usize = 2;
    const out_dim: usize = 1024;
    const in_dim: usize = 4096;
    const total_len_u64 = gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @as(i64, @intCast(in_dim)), @as(i64, @intCast(out_dim)) }) orelse return error.UnsupportedTensorShape;
    const total_len: usize = @intCast(total_len_u64);
    const scale_off = total_len - 32;

    const input = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input);
    @memset(input, 0.0);
    input[0] = 6.0;
    input[1] = -2.5;
    input[2] = 3.25;
    input[3] = -1.0;
    input[4] = 5.0;
    input[5] = -4.5;
    input[4032] = 2.0;
    input[4033] = -7.0;
    input[in_dim + 0] = -10.0;
    input[in_dim + 1] = 2.0;
    input[in_dim + 2] = 1.0;
    input[in_dim + 3] = 4.0;
    input[in_dim + 4] = -3.0;
    input[in_dim + 5] = 8.0;
    input[in_dim + 4032] = -6.0;
    input[in_dim + 4033] = 1.5;

    const weight_raw = try allocator.alloc(u8, total_len);
    defer allocator.free(weight_raw);
    @memset(weight_raw, 0);
    std.mem.writeInt(u32, weight_raw[scale_off..][0..4], @bitCast(@as(f32, 0.25)), .little);

    var shape_buf = [_]i64{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) };
    const view = try quant_codec.bitnetTL2View(shape_buf[0..], weight_raw);
    @memset(weight_raw[0..view.three_values.len], 0);
    @memset(weight_raw[view.three_values.len .. view.three_values.len + view.three_signs.len], 0);
    @memset(weight_raw[view.three_values.len + view.three_signs.len ..][0..view.two_values.len], 0x44);

    try setTL2Three(weight_raw, out_dim, in_dim, 0, 0, 13, false);
    try setTL2Three(weight_raw, out_dim, in_dim, 0, 1, 5, false);
    try setTL2Three(weight_raw, out_dim, in_dim, 1, 0, 1, false);
    try setTL2Three(weight_raw, out_dim, in_dim, 1, 1, 11, true);
    try setTL2TwoPair(weight_raw, out_dim, in_dim, 0, 0, 8);
    try setTL2TwoPair(weight_raw, out_dim, in_dim, 1, 0, 0);

    const expected = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(expected);
    try fillTL2ReferenceExpected(expected, rows, input, weight_raw, out_dim, in_dim);

    try expectMetalLinearMatchesRows(allocator, .bitnet_tl2, input, weight_raw, rows, out_dim, expected);
}

test "mlx metal iq4_nl kernel uses nonlinear lookup table" {
    var input: [32]f32 = [_]f32{0.0} ** 32;
    input[0] = 1.0;
    input[1] = 1.0;
    input[16] = 1.0;
    input[17] = 1.0;

    var weight_raw: [18]u8 = [_]u8{0} ** 18;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C; // d = 1.0
    weight_raw[2] = 0x10; // low nibbles 0, 1
    weight_raw[3] = 0x32; // high nibbles 3, 2

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .IQ4_NL }, &input, &weight_raw, 1, &.{-379.0});
}

test "mlx metal iq4_xs kernel applies high and low packed scales" {
    var input: [256]f32 = [_]f32{0.0} ** 256;
    input[0] = 1.0;
    input[16] = 1.0;
    input[32] = 1.0;

    var weight_raw: [136]u8 = [_]u8{0} ** 136;
    weight_raw[0] = 0x00;
    weight_raw[1] = 0x3C; // d = 1.0
    weight_raw[2] = 0x01; // scale high bits: sub-block 0 => +32
    weight_raw[4] = 0x98; // scale lows: sub-block 0 => 8, sub-block 1 => 9
    weight_raw[8] = 0xF8; // sub-block 0: low nibble 8 => 1, high nibble 15 => 113
    weight_raw[24] = 0x0F; // sub-block 1: low nibble 15 => 113

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .IQ4_XS }, &input, &weight_raw, 1, &.{-3511.0});
}

test "mlx metal q6_k kernel matches expected output" {
    var input: [256]f32 = [_]f32{1.0} ** 256;
    var weight_raw: [420]u8 = [_]u8{0} ** 420;

    for (192..208) |i| weight_raw[i] = 1;
    weight_raw[208] = 0x00;
    weight_raw[209] = 0x3C;

    const row1 = 210;
    for (row1 + 192..row1 + 208) |i| weight_raw[i] = 2;
    weight_raw[row1 + 208] = 0x00;
    weight_raw[row1 + 209] = 0x3C;

    try expectMetalLinearMatches(std.testing.allocator, .{ .known = .Q6_K }, &input, &weight_raw, 2, &.{ -8192.0, -16384.0 });
}
