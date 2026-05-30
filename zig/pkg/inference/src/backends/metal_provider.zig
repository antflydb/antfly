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

// Metal provider for termite's decoder runtime.
//
// Extracted from mlx_quant.zig. Currently still gated on `enable_mlx` because
// the Provider vtable signatures use `c.mlx_array`. Once Task #15 lands (decoder
// runtime callers use MetalTensor-based vtable), the gate can move to
// `enable_metal`.

const std = @import("std");
const build_options = @import("build_options");
const activations = @import("activations.zig");
const metal_runtime = @import("metal_runtime.zig");
const metal_tensor = @import("metal_tensor.zig");
const mlx_quant = @import("mlx_quant.zig");
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

const QuantizedStorage = weight_source_mod.QuantizedStorage;

const RawMetalProvider = metal_runtime.RawMetalProvider;
const RawMetalDecodeRuntime = metal_runtime.RawMetalDecodeRuntime;
const decoder_runtime_layer_norm_slot_capacity = metal_runtime.decoder_runtime_layer_norm_slot_capacity;
const decoder_runtime_rms_norm_slot_capacity = metal_runtime.decoder_runtime_rms_norm_slot_capacity;
const decoder_runtime_linear_slot_capacity = metal_runtime.decoder_runtime_linear_slot_capacity;
const RawQuantizedRuntimeLinearKind = metal_runtime.RawQuantizedRuntimeLinearKind;
const RawQuantizedRuntimeLinearStorageMode = metal_runtime.RawQuantizedRuntimeLinearStorageMode;
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

// Types shared with mlx_quant.zig. Imported via alias to avoid duplicating
// definitions; mlx_quant.zig also imports from this file to get MetalProvider.
const mq = @import("mlx_quant.zig");
const prepareWeightBytesForLinear = mq.prepareWeightBytesForLinear;
const Provider = mq.Provider;
const ExecutionPlan = mq.ExecutionPlan;
const PreparedWeight = mq.PreparedWeight;
const LinearNoBiasRequest = mq.LinearNoBiasRequest;
const LinearNoBiasPlanRequest = mq.LinearNoBiasPlanRequest;
const LinearNoBiasPairRequest = mq.LinearNoBiasPairRequest;
const LinearNoBiasPairPlanRequest = mq.LinearNoBiasPairPlanRequest;
const LinearNoBiasPairResult = mq.LinearNoBiasPairResult;
const LinearNoBiasTripleResult = mq.LinearNoBiasTripleResult;
const MoeLinearNoBiasRequest = mq.MoeLinearNoBiasRequest;
const MoeLinearNoBiasPairRequest = mq.MoeLinearNoBiasPairRequest;
const MoeLinearNoBiasPairResult = mq.MoeLinearNoBiasPairResult;
const CompressedKeyFormat = mq.CompressedKeyFormat;
const CompressedKeyScoresRequest = mq.CompressedKeyScoresRequest;
const CompressedAttentionBlockRequest = mq.CompressedAttentionBlockRequest;
const CompressedAttentionBlockResult = mq.CompressedAttentionBlockResult;
const CompressedAttentionSpanRequest = mq.CompressedAttentionSpanRequest;
const CompressedAttentionResidualRequest = mq.CompressedAttentionResidualRequest;
const LmHeadArgmaxRequest = mq.LmHeadArgmaxRequest;
const SampleLogitsDeviceRequest = mq.SampleLogitsDeviceRequest;
const DecoderRuntimePrepareLayerNormRequest = mq.DecoderRuntimePrepareLayerNormRequest;
const DecoderRuntimeApplyLayerNormRequest = mq.DecoderRuntimeApplyLayerNormRequest;
const DecoderRuntimePrepareRmsNormRequest = mq.DecoderRuntimePrepareRmsNormRequest;
const DecoderRuntimeApplyRmsNormRequest = mq.DecoderRuntimeApplyRmsNormRequest;
const DecoderRuntimeApplyLayerNormLinearArgmaxRequest = mq.DecoderRuntimeApplyLayerNormLinearArgmaxRequest;
const DecoderRuntimeApplyLayerNormLinearRequest = mq.DecoderRuntimeApplyLayerNormLinearRequest;
const DecoderRuntimeApplyLayerNormLinearSampleRequest = mq.DecoderRuntimeApplyLayerNormLinearSampleRequest;
const DecoderRuntimeApplyRmsNormLinearArgmaxRequest = mq.DecoderRuntimeApplyRmsNormLinearArgmaxRequest;
const DecoderRuntimeApplyRmsNormLinearRequest = mq.DecoderRuntimeApplyRmsNormLinearRequest;
const DecoderRuntimeApplyRmsNormLinearSampleRequest = mq.DecoderRuntimeApplyRmsNormLinearSampleRequest;
const DecoderRuntimePrepareLinearRequest = mq.DecoderRuntimePrepareLinearRequest;
const DecoderRuntimeApplyLinearRequest = mq.DecoderRuntimeApplyLinearRequest;
const DecoderRuntimeApplyLinearArgmaxRequest = mq.DecoderRuntimeApplyLinearArgmaxRequest;
const DecoderRuntimeApplyLinearPairRequest = mq.DecoderRuntimeApplyLinearPairRequest;
const DecoderRuntimeApplyLinearQkvRequest = mq.DecoderRuntimeApplyLinearQkvRequest;
const DecoderRuntimeApplyActivationRequest = mq.DecoderRuntimeApplyActivationRequest;
const DecoderRuntimeApplyAddRequest = mq.DecoderRuntimeApplyAddRequest;
const DecoderRuntimePrepareAbsoluteEmbeddingsRequest = mq.DecoderRuntimePrepareAbsoluteEmbeddingsRequest;
const DecoderRuntimeEmbedAbsolutePositionRequest = mq.DecoderRuntimeEmbedAbsolutePositionRequest;
const RunDenseFfnResidualRequest = mq.RunDenseFfnResidualRequest;
const RunGatedFfnResidualRequest = mq.RunGatedFfnResidualRequest;
const RunAttentionResidualPostLinearRequest = mq.RunAttentionResidualPostLinearRequest;
const RunCompressedAttentionDenseDecoderBlockRequest = mq.RunCompressedAttentionDenseDecoderBlockRequest;
const RunCompressedAttentionGatedDecoderBlockRequest = mq.RunCompressedAttentionGatedDecoderBlockRequest;

pub const MetalProvider = if (build_options.enable_mlx) struct {
    raw_provider: ?*RawMetalProvider,
    raw_decode_runtime: ?*RawMetalDecodeRuntime,
    raw_decoder_family_prepared: bool = false,
    raw_decoder_prepared_kv_tokens: usize = 0,
    raw_absolute_embeddings_prepared: bool = false,
    raw_absolute_embeddings_vocab_size: usize = 0,
    raw_absolute_embeddings_position_count: usize = 0,
    raw_absolute_embeddings_hidden_size: usize = 0,
    raw_layer_norm_slots_prepared: [decoder_runtime_layer_norm_slot_capacity]bool = @as([decoder_runtime_layer_norm_slot_capacity]bool, @splat(false)),
    raw_layer_norm_slot_hidden_sizes: [decoder_runtime_layer_norm_slot_capacity]usize = @as([decoder_runtime_layer_norm_slot_capacity]usize, @splat(0)),
    raw_layer_norm_slot_weights: [decoder_runtime_layer_norm_slot_capacity]?MetalTensor = @as([decoder_runtime_layer_norm_slot_capacity]?MetalTensor, @splat(null)),
    raw_layer_norm_slot_biases: [decoder_runtime_layer_norm_slot_capacity]?MetalTensor = @as([decoder_runtime_layer_norm_slot_capacity]?MetalTensor, @splat(null)),
    raw_rms_norm_slots_prepared: [decoder_runtime_rms_norm_slot_capacity]bool = @as([decoder_runtime_rms_norm_slot_capacity]bool, @splat(false)),
    raw_rms_norm_slot_hidden_sizes: [decoder_runtime_rms_norm_slot_capacity]usize = @as([decoder_runtime_rms_norm_slot_capacity]usize, @splat(0)),
    raw_rms_norm_slot_weights: [decoder_runtime_rms_norm_slot_capacity]?MetalTensor = @as([decoder_runtime_rms_norm_slot_capacity]?MetalTensor, @splat(null)),
    raw_linear_slots_prepared: [decoder_runtime_linear_slot_capacity]bool = @as([decoder_runtime_linear_slot_capacity]bool, @splat(false)),
    raw_linear_slot_kinds: [decoder_runtime_linear_slot_capacity]RawLinearSlotKind = @as([decoder_runtime_linear_slot_capacity]RawLinearSlotKind, @splat(.none)),
    raw_linear_slot_in_dims: [decoder_runtime_linear_slot_capacity]usize = @as([decoder_runtime_linear_slot_capacity]usize, @splat(0)),
    raw_linear_slot_out_dims: [decoder_runtime_linear_slot_capacity]usize = @as([decoder_runtime_linear_slot_capacity]usize, @splat(0)),
    raw_linear_slot_quantized_storage: [decoder_runtime_linear_slot_capacity]?*QuantizedStorage = @as([decoder_runtime_linear_slot_capacity]?*QuantizedStorage, @splat(null)),
    raw_linear_slot_dense_weights: [decoder_runtime_linear_slot_capacity]?MetalTensor = @as([decoder_runtime_linear_slot_capacity]?MetalTensor, @splat(null)),
    raw_linear_slot_dense_biases: [decoder_runtime_linear_slot_capacity]?MetalTensor = @as([decoder_runtime_linear_slot_capacity]?MetalTensor, @splat(null)),
    raw_linear_slot_runtime_prepared_kind: [decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind, @splat(.none)),
    raw_linear_slot_runtime_prepared_modes: [decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode, @splat(.none)),
    raw_quant_runtime_private_prepare_nanos: u128 = 0,
    raw_quant_runtime_mapped_prepare_nanos: u128 = 0,
    raw_quant_runtime_mapped_attempts: u64 = 0,
    raw_quant_runtime_mapped_fallbacks: u64 = 0,
    raw_quant_runtime_mapped_failures: u64 = 0,
    gathered_spans: std.AutoHashMapUnmanaged(GatheredSpanKey, GatheredSpanEntry) = .empty,
    q4_0: c.mlx_fast_metal_kernel,
    q4_0_grouped: c.mlx_fast_metal_kernel,
    q4_1: c.mlx_fast_metal_kernel,
    q4_1_grouped: c.mlx_fast_metal_kernel,
    q5_0: c.mlx_fast_metal_kernel,
    q5_0_grouped: c.mlx_fast_metal_kernel,
    q5_1: c.mlx_fast_metal_kernel,
    q5_1_grouped: c.mlx_fast_metal_kernel,
    q8_0: c.mlx_fast_metal_kernel,
    q8_0_grouped: c.mlx_fast_metal_kernel,
    q8_1: c.mlx_fast_metal_kernel,
    q8_1_grouped: c.mlx_fast_metal_kernel,
    i8_s: c.mlx_fast_metal_kernel,
    iq4_nl: c.mlx_fast_metal_kernel,
    iq4_nl_grouped: c.mlx_fast_metal_kernel,
    iq4_xs: c.mlx_fast_metal_kernel,
    iq4_xs_grouped: c.mlx_fast_metal_kernel,
    q1_0: c.mlx_fast_metal_kernel,
    q2_k: c.mlx_fast_metal_kernel,
    q3_k: c.mlx_fast_metal_kernel,
    q4_k: c.mlx_fast_metal_kernel,
    q4_k_pair: c.mlx_fast_metal_kernel,
    q4_k_grouped: c.mlx_fast_metal_kernel,
    q5_k: c.mlx_fast_metal_kernel,
    q5_k_pair: c.mlx_fast_metal_kernel,
    q5_k_grouped: c.mlx_fast_metal_kernel,
    q5_k_grouped_tiled: c.mlx_fast_metal_kernel,
    q5_k_grouped_pair: c.mlx_fast_metal_kernel,
    q6_k: c.mlx_fast_metal_kernel,
    q6_k_grouped: c.mlx_fast_metal_kernel,
    q8_k: c.mlx_fast_metal_kernel,
    i2_s: c.mlx_fast_metal_kernel,
    i2_s_pair: c.mlx_fast_metal_kernel,
    tl1: c.mlx_fast_metal_kernel,
    tl2: c.mlx_fast_metal_kernel,
    polar4_key_scores: c.mlx_fast_metal_kernel,
    turbo3_key_scores: c.mlx_fast_metal_kernel,
    polar4_attention_block: c.mlx_fast_metal_kernel,
    turbo3_attention_block: c.mlx_fast_metal_kernel,
    polar4_attention_span: c.mlx_fast_metal_kernel,
    turbo3_attention_span: c.mlx_fast_metal_kernel,
    polar4_attention_span_partials: c.mlx_fast_metal_kernel,
    turbo3_attention_span_partials: c.mlx_fast_metal_kernel,
    attention_span_reduce: c.mlx_fast_metal_kernel,
    lm_head_argmax_partials: c.mlx_fast_metal_kernel,
    lm_head_argmax_reduce: c.mlx_fast_metal_kernel,

    const span_chunk_tokens = 32;

    const lm_head_argmax_partials_source =
        \\auto block = thread_position_in_grid.x;
        \\uint start = block * VocabBlock;
        \\uint end = start + VocabBlock;
        \\if (end > OutDim) {
        \\  end = OutDim;
        \\}
        \\float best = -3.402823466e+38f;
        \\uint best_id = start;
        \\uint hidden_base = (Rows - 1u) * InDim;
        \\for (uint vocab = start; vocab < end; ++vocab) {
        \\  float acc = 0.0f;
        \\  uint weight_base = vocab * InDim;
        \\  for (uint d = 0; d < InDim; ++d) {
        \\    acc += hidden[hidden_base + d] * weight[weight_base + d];
        \\  }
        \\  if (acc > best) {
        \\    best = acc;
        \\    best_id = vocab;
        \\  }
        \\}
        \\partial_scores[block] = best;
        \\partial_ids[block] = float(best_id);
    ;

    const lm_head_argmax_reduce_source =
        \\float best = partial_scores[0];
        \\float best_id = partial_ids[0];
        \\for (uint block = 1u; block < NumBlocks; ++block) {
        \\  float value = partial_scores[block];
        \\  float token = partial_ids[block];
        \\  if (value > best) {
        \\    best = value;
        \\    best_id = token;
        \\  }
        \\}
        \\token_id[0] = best_id;
    ;

    const q4_0_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 18u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 18u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  auto qs = weight + off + 2u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float lo = float(int(packed & 0x0Fu) - 8);
        \\    acc += input[in_off + i] * (d * lo);
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float hi = float(int((packed >> 4) & 0x0Fu) - 8);
        \\    acc += input[in_off + 16u + i] * (d * hi);
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const polar4_key_scores_source =
        \\auto ki = thread_position_in_grid.x;
        \\auto q_row = thread_position_in_grid.y;
        \\const uint h = q_row / QLen;
        \\const uint qi = q_row - h * QLen;
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = qi * NumHeads * HeadDim + h * HeadDim;
        \\const uint k_base = ki * KeyRowBytes;
        \\float acc = 0.0f;
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  uint value_index = kv_head_off + d;
        \\  uchar packed = encoded_key[k_base + (value_index >> 1)];
        \\  uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
        \\  float k = float(code) / 7.5f - 1.0f;
        \\  acc += q[q_base + d] * k;
        \\}
        \\scores[h * QLen * BlockTokens + qi * BlockTokens + ki] = acc;
    ;

    const turbo3_key_scores_source =
        \\auto ki = thread_position_in_grid.x;
        \\auto q_row = thread_position_in_grid.y;
        \\const uint h = q_row / QLen;
        \\const uint qi = q_row - h * QLen;
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = qi * NumHeads * HeadDim + h * HeadDim;
        \\const uint k_base = ki * KeyRowBytes;
        \\const uint residual_base = k_base + BaseKeyRowBytes;
        \\float residual_projection[32];
        \\for (uint projection = 0; projection < 32u; ++projection) {
        \\  float projected_query = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    ulong x = (ulong(kv_h + 1u) * 0x9e3779b97f4a7c15UL);
        \\    x ^= (ulong(projection + 1u) * 0xbf58476d1ce4e5b9UL);
        \\    x ^= (ulong(d + 1u) * 0x94d049bb133111ebUL);
        \\    x ^= x >> 30;
        \\    x *= 0xbf58476d1ce4e5b9UL;
        \\    x ^= x >> 27;
        \\    x *= 0x94d049bb133111ebUL;
        \\    x ^= x >> 31;
        \\    float sign = ((x & 1UL) == 0UL) ? 1.0f : -1.0f;
        \\    projected_query += sign * q[q_base + d];
        \\  }
        \\  residual_projection[projection] = projected_query;
        \\}
        \\float acc = 0.0f;
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  uint value_index = kv_head_off + d;
        \\  uint bit_offset = value_index * 3u;
        \\  uint byte_index = k_base + (bit_offset >> 3);
        \\  uint shift = bit_offset & 7u;
        \\  uint bits = uint(encoded_key[byte_index]) >> shift;
        \\  if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
        \\  float k = float(bits & 0x07u) / 3.5f - 1.0f;
        \\  acc += q[q_base + d] * k;
        \\}
        \\float residual_acc = 0.0f;
        \\for (uint projection = 0; projection < 32u; ++projection) {
        \\  uint residual_bit = kv_h * 32u + projection;
        \\  uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
        \\  float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        \\  residual_acc += residual_sign * residual_projection[projection];
        \\}
        \\acc += 0.125f * (residual_acc / 32.0f);
        \\scores[h * QLen * BlockTokens + qi * BlockTokens + ki] = acc;
    ;

    const polar4_attention_block_source =
        \\auto h = thread_position_in_grid.x;
        \\const uint query_position = uint(meta[0]);
        \\const uint block_position_offset = uint(meta[1]);
        \\const uint sliding_window = uint(meta[2]);
        \\const uint block_tokens = uint(meta[3]);
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = h * HeadDim;
        \\float block_max = -3.402823466e+38f;
        \\float scores[MaxBlockTokens];
        \\for (uint ki = 0; ki < block_tokens; ++ki) {
        \\  const uint key_pos = block_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  float acc = 0.0f;
        \\  if (allowed) {
        \\    const uint k_base = ki * KeyRowBytes;
        \\    for (uint d = 0; d < HeadDim; ++d) {
        \\      uint value_index = kv_head_off + d;
        \\      uchar packed = encoded_key[k_base + (value_index >> 1)];
        \\      uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
        \\      float k = float(code) / 7.5f - 1.0f;
        \\      acc += q[q_base + d] * k;
        \\    }
        \\    acc *= rsqrt(float(HeadDim));
        \\  } else {
        \\    acc = -3.402823466e+38f;
        \\  }
        \\  scores[ki] = acc;
        \\  if (acc > block_max) block_max = acc;
        \\}
        \\const uint hs = h;
        \\float old_max = running_max[hs];
        \\float old_sum = running_sum[hs];
        \\float new_max = old_max > block_max ? old_max : block_max;
        \\float prev_scale = exp(old_max - new_max);
        \\float block_sum = 0.0f;
        \\float exps[MaxBlockTokens];
        \\for (uint ki = 0; ki < block_tokens; ++ki) {
        \\  float e = exp(scores[ki] - new_max);
        \\  exps[ki] = e;
        \\  block_sum += e;
        \\}
        \\out_max[hs] = new_max;
        \\out_sum[hs] = old_sum * prev_scale + block_sum;
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  float weighted = 0.0f;
        \\  for (uint ki = 0; ki < block_tokens; ++ki) {
        \\    weighted += exps[ki] * v[ki * NumKvHeads * HeadDim + kv_head_off + d];
        \\  }
        \\  uint acc_idx = h * HeadDim + d;
        \\  out_acc[acc_idx] = running_acc[acc_idx] * prev_scale + weighted;
        \\}
    ;

    const turbo3_attention_block_source =
        \\auto h = thread_position_in_grid.x;
        \\const uint query_position = uint(meta[0]);
        \\const uint block_position_offset = uint(meta[1]);
        \\const uint sliding_window = uint(meta[2]);
        \\const uint block_tokens = uint(meta[3]);
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = h * HeadDim;
        \\float residual_projection[32];
        \\for (uint projection = 0; projection < 32u; ++projection) {
        \\  float projected_query = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    ulong x = (ulong(kv_h + 1u) * 0x9e3779b97f4a7c15UL);
        \\    x ^= (ulong(projection + 1u) * 0xbf58476d1ce4e5b9UL);
        \\    x ^= (ulong(d + 1u) * 0x94d049bb133111ebUL);
        \\    x ^= x >> 30;
        \\    x *= 0xbf58476d1ce4e5b9UL;
        \\    x ^= x >> 27;
        \\    x *= 0x94d049bb133111ebUL;
        \\    x ^= x >> 31;
        \\    float sign = ((x & 1UL) == 0UL) ? 1.0f : -1.0f;
        \\    projected_query += sign * q[q_base + d];
        \\  }
        \\  residual_projection[projection] = projected_query;
        \\}
        \\float block_max = -3.402823466e+38f;
        \\float scores[MaxBlockTokens];
        \\for (uint ki = 0; ki < block_tokens; ++ki) {
        \\  const uint key_pos = block_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  const uint k_base = ki * KeyRowBytes;
        \\  const uint residual_base = k_base + BaseKeyRowBytes;
        \\  float acc = 0.0f;
        \\  if (allowed) {
        \\    for (uint d = 0; d < HeadDim; ++d) {
        \\      uint value_index = kv_head_off + d;
        \\      uint bit_offset = value_index * 3u;
        \\      uint byte_index = k_base + (bit_offset >> 3);
        \\      uint shift = bit_offset & 7u;
        \\      uint bits = uint(encoded_key[byte_index]) >> shift;
        \\      if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
        \\      float k = float(bits & 0x07u) / 3.5f - 1.0f;
        \\      acc += q[q_base + d] * k;
        \\    }
        \\    float residual_acc = 0.0f;
        \\    for (uint projection = 0; projection < 32u; ++projection) {
        \\      uint residual_bit = kv_h * 32u + projection;
        \\      uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
        \\      float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        \\      residual_acc += residual_sign * residual_projection[projection];
        \\    }
        \\    acc += 0.125f * (residual_acc / 32.0f);
        \\    acc *= rsqrt(float(HeadDim));
        \\  } else {
        \\    acc = -3.402823466e+38f;
        \\  }
        \\  scores[ki] = acc;
        \\  if (acc > block_max) block_max = acc;
        \\}
        \\const uint hs = h;
        \\float old_max = running_max[hs];
        \\float old_sum = running_sum[hs];
        \\float new_max = old_max > block_max ? old_max : block_max;
        \\float prev_scale = exp(old_max - new_max);
        \\float block_sum = 0.0f;
        \\float exps[MaxBlockTokens];
        \\for (uint ki = 0; ki < block_tokens; ++ki) {
        \\  float e = exp(scores[ki] - new_max);
        \\  exps[ki] = e;
        \\  block_sum += e;
        \\}
        \\out_max[hs] = new_max;
        \\out_sum[hs] = old_sum * prev_scale + block_sum;
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  float weighted = 0.0f;
        \\  for (uint ki = 0; ki < block_tokens; ++ki) {
        \\    weighted += exps[ki] * v[ki * NumKvHeads * HeadDim + kv_head_off + d];
        \\  }
        \\  uint acc_idx = h * HeadDim + d;
        \\  out_acc[acc_idx] = running_acc[acc_idx] * prev_scale + weighted;
        \\}
    ;

    const polar4_attention_span_source =
        \\auto h = thread_position_in_grid.x;
        \\const uint query_position = uint(meta[0]);
        \\const uint kv_position_offset = uint(meta[1]);
        \\const uint sliding_window = uint(meta[2]);
        \\const uint kv_tokens = uint(meta[3]);
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = h * HeadDim;
        \\float best = -3.402823466e+38f;
        \\for (uint ki = 0; ki < kv_tokens; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  float score = -3.402823466e+38f;
        \\  if (allowed) {
        \\    const uint k_base = ki * KeyRowBytes;
        \\    float acc = 0.0f;
        \\    for (uint d = 0; d < HeadDim; ++d) {
        \\      uint value_index = kv_head_off + d;
        \\      uchar packed = encoded_key[k_base + (value_index >> 1)];
        \\      uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
        \\      float k = float(code) / 7.5f - 1.0f;
        \\      acc += q[q_base + d] * k;
        \\    }
        \\    score = acc * rsqrt(float(HeadDim));
        \\  }
        \\  if (score > best) best = score;
        \\}
        \\float sum = 0.0f;
        \\float acc_values[HeadDim];
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  acc_values[d] = 0.0f;
        \\}
        \\for (uint ki = 0; ki < kv_tokens; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  if (!allowed) continue;
        \\  const uint k_base = ki * KeyRowBytes;
        \\  float score = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    uint value_index = kv_head_off + d;
        \\    uchar packed = encoded_key[k_base + (value_index >> 1)];
        \\    uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
        \\    float k = float(code) / 7.5f - 1.0f;
        \\    score += q[q_base + d] * k;
        \\  }
        \\  float e = exp(score * rsqrt(float(HeadDim)) - best);
        \\  sum += e;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    acc_values[d] += e * v[ki * NumKvHeads * HeadDim + kv_head_off + d];
        \\  }
        \\}
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  output[h * HeadDim + d] = acc_values[d] / sum;
        \\}
    ;

    const turbo3_attention_span_source =
        \\auto h = thread_position_in_grid.x;
        \\const uint query_position = uint(meta[0]);
        \\const uint kv_position_offset = uint(meta[1]);
        \\const uint sliding_window = uint(meta[2]);
        \\const uint kv_tokens = uint(meta[3]);
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = h * HeadDim;
        \\float residual_projection[32];
        \\for (uint projection = 0; projection < 32u; ++projection) {
        \\  float projected_query = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    ulong x = (ulong(kv_h + 1u) * 0x9e3779b97f4a7c15UL);
        \\    x ^= (ulong(projection + 1u) * 0xbf58476d1ce4e5b9UL);
        \\    x ^= (ulong(d + 1u) * 0x94d049bb133111ebUL);
        \\    x ^= x >> 30;
        \\    x *= 0xbf58476d1ce4e5b9UL;
        \\    x ^= x >> 27;
        \\    x *= 0x94d049bb133111ebUL;
        \\    x ^= x >> 31;
        \\    float sign = ((x & 1UL) == 0UL) ? 1.0f : -1.0f;
        \\    projected_query += sign * q[q_base + d];
        \\  }
        \\  residual_projection[projection] = projected_query;
        \\}
        \\float best = -3.402823466e+38f;
        \\for (uint ki = 0; ki < kv_tokens; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  float score = -3.402823466e+38f;
        \\  if (allowed) {
        \\    const uint k_base = ki * KeyRowBytes;
        \\    const uint residual_base = k_base + BaseKeyRowBytes;
        \\    float acc = 0.0f;
        \\    for (uint d = 0; d < HeadDim; ++d) {
        \\      uint value_index = kv_head_off + d;
        \\      uint bit_offset = value_index * 3u;
        \\      uint byte_index = k_base + (bit_offset >> 3);
        \\      uint shift = bit_offset & 7u;
        \\      uint bits = uint(encoded_key[byte_index]) >> shift;
        \\      if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
        \\      float k = float(bits & 0x07u) / 3.5f - 1.0f;
        \\      acc += q[q_base + d] * k;
        \\    }
        \\    float residual_acc = 0.0f;
        \\    for (uint projection = 0; projection < 32u; ++projection) {
        \\      uint residual_bit = kv_h * 32u + projection;
        \\      uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
        \\      float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        \\      residual_acc += residual_sign * residual_projection[projection];
        \\    }
        \\    acc += 0.125f * (residual_acc / 32.0f);
        \\    score = acc * rsqrt(float(HeadDim));
        \\  }
        \\  if (score > best) best = score;
        \\}
        \\float sum = 0.0f;
        \\float acc_values[HeadDim];
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  acc_values[d] = 0.0f;
        \\}
        \\for (uint ki = 0; ki < kv_tokens; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  if (!allowed) continue;
        \\  const uint k_base = ki * KeyRowBytes;
        \\  const uint residual_base = k_base + BaseKeyRowBytes;
        \\  float score = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    uint value_index = kv_head_off + d;
        \\    uint bit_offset = value_index * 3u;
        \\    uint byte_index = k_base + (bit_offset >> 3);
        \\    uint shift = bit_offset & 7u;
        \\    uint bits = uint(encoded_key[byte_index]) >> shift;
        \\    if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
        \\    float k = float(bits & 0x07u) / 3.5f - 1.0f;
        \\    score += q[q_base + d] * k;
        \\  }
        \\  float residual_acc = 0.0f;
        \\  for (uint projection = 0; projection < 32u; ++projection) {
        \\    uint residual_bit = kv_h * 32u + projection;
        \\    uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
        \\    float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        \\    residual_acc += residual_sign * residual_projection[projection];
        \\  }
        \\  score += 0.125f * (residual_acc / 32.0f);
        \\  float e = exp(score * rsqrt(float(HeadDim)) - best);
        \\  sum += e;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    acc_values[d] += e * v[ki * NumKvHeads * HeadDim + kv_head_off + d];
        \\  }
        \\}
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  output[h * HeadDim + d] = acc_values[d] / sum;
        \\}
    ;

    const polar4_attention_span_partials_source =
        \\auto h = thread_position_in_grid.x;
        \\auto chunk = thread_position_in_grid.y;
        \\const uint query_position = uint(meta[0]);
        \\const uint kv_position_offset = uint(meta[1]);
        \\const uint sliding_window = uint(meta[2]);
        \\const uint kv_tokens = uint(meta[3]);
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = h * HeadDim;
        \\const uint chunk_start = chunk * SpanChunkTokens;
        \\uint chunk_end = chunk_start + SpanChunkTokens;
        \\if (chunk_end > kv_tokens) chunk_end = kv_tokens;
        \\float best = -3.402823466e+38f;
        \\for (uint ki = chunk_start; ki < chunk_end; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  float score = -3.402823466e+38f;
        \\  if (allowed) {
        \\    const uint k_base = ki * KeyRowBytes;
        \\    float acc = 0.0f;
        \\    for (uint d = 0; d < HeadDim; ++d) {
        \\      uint value_index = kv_head_off + d;
        \\      uchar packed = encoded_key[k_base + (value_index >> 1)];
        \\      uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
        \\      float k = float(code) / 7.5f - 1.0f;
        \\      acc += q[q_base + d] * k;
        \\    }
        \\    score = acc * rsqrt(float(HeadDim));
        \\  }
        \\  if (score > best) best = score;
        \\}
        \\const uint partial_idx = h * NumChunks + chunk;
        \\partial_max[partial_idx] = best;
        \\float sum = 0.0f;
        \\float acc_values[HeadDim];
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  acc_values[d] = 0.0f;
        \\}
        \\for (uint ki = chunk_start; ki < chunk_end; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  if (!allowed) continue;
        \\  const uint k_base = ki * KeyRowBytes;
        \\  float score = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    uint value_index = kv_head_off + d;
        \\    uchar packed = encoded_key[k_base + (value_index >> 1)];
        \\    uint code = ((value_index & 1u) == 0u) ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);
        \\    float k = float(code) / 7.5f - 1.0f;
        \\    score += q[q_base + d] * k;
        \\  }
        \\  float e = exp(score * rsqrt(float(HeadDim)) - best);
        \\  sum += e;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    acc_values[d] += e * v[ki * NumKvHeads * HeadDim + kv_head_off + d];
        \\  }
        \\}
        \\partial_sum[partial_idx] = sum;
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  partial_acc[partial_idx * HeadDim + d] = acc_values[d];
        \\}
    ;

    const turbo3_attention_span_partials_source =
        \\auto h = thread_position_in_grid.x;
        \\auto chunk = thread_position_in_grid.y;
        \\const uint query_position = uint(meta[0]);
        \\const uint kv_position_offset = uint(meta[1]);
        \\const uint sliding_window = uint(meta[2]);
        \\const uint kv_tokens = uint(meta[3]);
        \\const uint heads_per_group = NumHeads / NumKvHeads;
        \\const uint kv_h = h / heads_per_group;
        \\const uint kv_head_off = kv_h * HeadDim;
        \\const uint q_base = h * HeadDim;
        \\float residual_projection[32];
        \\for (uint projection = 0; projection < 32u; ++projection) {
        \\  float projected_query = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    ulong x = (ulong(kv_h + 1u) * 0x9e3779b97f4a7c15UL);
        \\    x ^= (ulong(projection + 1u) * 0xbf58476d1ce4e5b9UL);
        \\    x ^= (ulong(d + 1u) * 0x94d049bb133111ebUL);
        \\    x ^= x >> 30;
        \\    x *= 0xbf58476d1ce4e5b9UL;
        \\    x ^= x >> 27;
        \\    x *= 0x94d049bb133111ebUL;
        \\    x ^= x >> 31;
        \\    float sign = ((x & 1UL) == 0UL) ? 1.0f : -1.0f;
        \\    projected_query += sign * q[q_base + d];
        \\  }
        \\  residual_projection[projection] = projected_query;
        \\}
        \\const uint chunk_start = chunk * SpanChunkTokens;
        \\uint chunk_end = chunk_start + SpanChunkTokens;
        \\if (chunk_end > kv_tokens) chunk_end = kv_tokens;
        \\float best = -3.402823466e+38f;
        \\for (uint ki = chunk_start; ki < chunk_end; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  float score = -3.402823466e+38f;
        \\  if (allowed) {
        \\    const uint k_base = ki * KeyRowBytes;
        \\    const uint residual_base = k_base + BaseKeyRowBytes;
        \\    float acc = 0.0f;
        \\    for (uint d = 0; d < HeadDim; ++d) {
        \\      uint value_index = kv_head_off + d;
        \\      uint bit_offset = value_index * 3u;
        \\      uint byte_index = k_base + (bit_offset >> 3);
        \\      uint shift = bit_offset & 7u;
        \\      uint bits = uint(encoded_key[byte_index]) >> shift;
        \\      if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
        \\      float k = float(bits & 0x07u) / 3.5f - 1.0f;
        \\      acc += q[q_base + d] * k;
        \\    }
        \\    float residual_acc = 0.0f;
        \\    for (uint projection = 0; projection < 32u; ++projection) {
        \\      uint residual_bit = kv_h * 32u + projection;
        \\      uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
        \\      float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        \\      residual_acc += residual_sign * residual_projection[projection];
        \\    }
        \\    acc += 0.125f * (residual_acc / 32.0f);
        \\    score = acc * rsqrt(float(HeadDim));
        \\  }
        \\  if (score > best) best = score;
        \\}
        \\const uint partial_idx = h * NumChunks + chunk;
        \\partial_max[partial_idx] = best;
        \\float sum = 0.0f;
        \\float acc_values[HeadDim];
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  acc_values[d] = 0.0f;
        \\}
        \\for (uint ki = chunk_start; ki < chunk_end; ++ki) {
        \\  const uint key_pos = kv_position_offset + ki;
        \\  bool allowed = key_pos <= query_position;
        \\  if (sliding_window != 0u && allowed) {
        \\    allowed = (query_position - key_pos) < sliding_window;
        \\  }
        \\  if (!allowed) continue;
        \\  const uint k_base = ki * KeyRowBytes;
        \\  const uint residual_base = k_base + BaseKeyRowBytes;
        \\  float score = 0.0f;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    uint value_index = kv_head_off + d;
        \\    uint bit_offset = value_index * 3u;
        \\    uint byte_index = k_base + (bit_offset >> 3);
        \\    uint shift = bit_offset & 7u;
        \\    uint bits = uint(encoded_key[byte_index]) >> shift;
        \\    if (shift > 5u) bits |= uint(encoded_key[byte_index + 1u]) << (8u - shift);
        \\    float k = float(bits & 0x07u) / 3.5f - 1.0f;
        \\    score += q[q_base + d] * k;
        \\  }
        \\  float residual_acc = 0.0f;
        \\  for (uint projection = 0; projection < 32u; ++projection) {
        \\    uint residual_bit = kv_h * 32u + projection;
        \\    uchar residual_byte = encoded_key[residual_base + (residual_bit >> 3)];
        \\    float residual_sign = ((residual_byte >> (residual_bit & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        \\    residual_acc += residual_sign * residual_projection[projection];
        \\  }
        \\  score += 0.125f * (residual_acc / 32.0f);
        \\  float e = exp(score * rsqrt(float(HeadDim)) - best);
        \\  sum += e;
        \\  for (uint d = 0; d < HeadDim; ++d) {
        \\    acc_values[d] += e * v[ki * NumKvHeads * HeadDim + kv_head_off + d];
        \\  }
        \\}
        \\partial_sum[partial_idx] = sum;
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  partial_acc[partial_idx * HeadDim + d] = acc_values[d];
        \\}
    ;

    const attention_span_reduce_source =
        \\auto h = thread_position_in_grid.x;
        \\float best = partial_max[h * NumChunks];
        \\for (uint chunk = 1u; chunk < NumChunks; ++chunk) {
        \\  float value = partial_max[h * NumChunks + chunk];
        \\  if (value > best) best = value;
        \\}
        \\float sum = 0.0f;
        \\for (uint chunk = 0; chunk < NumChunks; ++chunk) {
        \\  uint partial_idx = h * NumChunks + chunk;
        \\  sum += partial_sum[partial_idx] * exp(partial_max[partial_idx] - best);
        \\}
        \\for (uint d = 0; d < HeadDim; ++d) {
        \\  float acc = 0.0f;
        \\  for (uint chunk = 0; chunk < NumChunks; ++chunk) {
        \\    uint partial_idx = h * NumChunks + chunk;
        \\    float scale = exp(partial_max[partial_idx] - best);
        \\    acc += partial_acc[partial_idx * HeadDim + d] * scale;
        \\  }
        \\  output[h * HeadDim + d] = sum == 0.0f ? 0.0f : acc / sum;
        \\}
    ;

    const q4_0_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 18u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 18u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  auto qs = weight + off + 2u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float lo = float(int(packed & 0x0Fu) - 8);
        \\    acc += input[in_off + i] * (d * lo);
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float hi = float(int((packed >> 4) & 0x0Fu) - 8);
        \\    acc += input[in_off + 16u + i] * (d * hi);
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q4_1_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 20u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 20u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort m_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float m = float(as_type<half>(m_bits));
        \\  auto qs = weight + off + 4u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float lo = float(int(packed & 0x0Fu));
        \\    acc += input[in_off + i] * (d * lo + m);
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float hi = float(int((packed >> 4) & 0x0Fu));
        \\    acc += input[in_off + 16u + i] * (d * hi + m);
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q4_1_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 20u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 20u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort m_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float m = float(as_type<half>(m_bits));
        \\  auto qs = weight + off + 4u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float lo = float(int(packed & 0x0Fu));
        \\    acc += input[in_off + i] * (d * lo + m);
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    float hi = float(int((packed >> 4) & 0x0Fu));
        \\    acc += input[in_off + 16u + i] * (d * hi + m);
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q5_0_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 22u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 22u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  uint qh = uint(weight[off + 2]) | (uint(weight[off + 3]) << 8) | (uint(weight[off + 4]) << 16) | (uint(weight[off + 5]) << 24);
        \\  auto qs = weight + off + 6u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] & 0x0Fu);
        \\    int hi1 = int((qh >> i) & 1u);
        \\    acc += input[in_off + i] * (d * float((lo4 | (hi1 << 4)) - 16));
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] >> 4);
        \\    int hi1 = int((qh >> (i + 16u)) & 1u);
        \\    acc += input[in_off + 16u + i] * (d * float((lo4 | (hi1 << 4)) - 16));
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q5_0_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 22u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 22u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  uint qh = uint(weight[off + 2]) | (uint(weight[off + 3]) << 8) | (uint(weight[off + 4]) << 16) | (uint(weight[off + 5]) << 24);
        \\  auto qs = weight + off + 6u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] & 0x0Fu);
        \\    int hi1 = int((qh >> i) & 1u);
        \\    acc += input[in_off + i] * (d * float((lo4 | (hi1 << 4)) - 16));
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] >> 4);
        \\    int hi1 = int((qh >> (i + 16u)) & 1u);
        \\    acc += input[in_off + 16u + i] * (d * float((lo4 | (hi1 << 4)) - 16));
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q5_1_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 24u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 24u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort m_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float m = float(as_type<half>(m_bits));
        \\  uint qh = uint(weight[off + 4]) | (uint(weight[off + 5]) << 8) | (uint(weight[off + 6]) << 16) | (uint(weight[off + 7]) << 24);
        \\  auto qs = weight + off + 8u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] & 0x0Fu);
        \\    int hi1 = int((qh >> i) & 1u);
        \\    acc += input[in_off + i] * (d * float(lo4 | (hi1 << 4)) + m);
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] >> 4);
        \\    int hi1 = int((qh >> (i + 16u)) & 1u);
        \\    acc += input[in_off + 16u + i] * (d * float(lo4 | (hi1 << 4)) + m);
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q5_1_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 24u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 24u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort m_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float m = float(as_type<half>(m_bits));
        \\  uint qh = uint(weight[off + 4]) | (uint(weight[off + 5]) << 8) | (uint(weight[off + 6]) << 16) | (uint(weight[off + 7]) << 24);
        \\  auto qs = weight + off + 8u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] & 0x0Fu);
        \\    int hi1 = int((qh >> i) & 1u);
        \\    acc += input[in_off + i] * (d * float(lo4 | (hi1 << 4)) + m);
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    int lo4 = int(qs[i] >> 4);
        \\    int hi1 = int((qh >> (i + 16u)) & 1u);
        \\    acc += input[in_off + 16u + i] * (d * float(lo4 | (hi1 << 4)) + m);
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    // Q1_0: 18 B per block, 128 values. Layout:
    //   block[0..2]   = fp16 per-block scale d
    //   block[2..18]  = 128 sign bits, LSB-first within each byte
    // Each weight is ±d (bit==1 → +d, bit==0 → −d). That means the matmul
    // inner loop needs zero multiplications by weight — just a signed
    // accumulate of the input values followed by one multiply by d at
    // block end. Processes 8 values per byte, 16 bytes per block.
    const q1_0_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 18u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 18u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  float signed_sum = 0.0f;
        \\  uint in_off = r * InDim + b * 128u;
        \\  for (uint by = 0; by < 16u; ++by) {
        \\    uchar sbits = weight[off + 2u + by];
        \\    uint base = in_off + by * 8u;
        \\    signed_sum += (sbits & 0x01u) != 0u ? input[base + 0u] : -input[base + 0u];
        \\    signed_sum += (sbits & 0x02u) != 0u ? input[base + 1u] : -input[base + 1u];
        \\    signed_sum += (sbits & 0x04u) != 0u ? input[base + 2u] : -input[base + 2u];
        \\    signed_sum += (sbits & 0x08u) != 0u ? input[base + 3u] : -input[base + 3u];
        \\    signed_sum += (sbits & 0x10u) != 0u ? input[base + 4u] : -input[base + 4u];
        \\    signed_sum += (sbits & 0x20u) != 0u ? input[base + 5u] : -input[base + 5u];
        \\    signed_sum += (sbits & 0x40u) != 0u ? input[base + 6u] : -input[base + 6u];
        \\    signed_sum += (sbits & 0x80u) != 0u ? input[base + 7u] : -input[base + 7u];
        \\  }
        \\  acc += d * signed_sum;
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q8_0_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 34u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 34u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  float block_acc = 0.0f;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 32; ++i) {
        \\    char q = as_type<char>(weight[off + 2u + i]);
        \\    block_acc += input[in_off + i] * float(q);
        \\  }
        \\  acc += d * block_acc;
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q8_k_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 292u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 292u;
        \\  uint bits = uint(weight[off]) | (uint(weight[off + 1]) << 8) | (uint(weight[off + 2]) << 16) | (uint(weight[off + 3]) << 24);
        \\  float d = as_type<float>(bits);
        \\  float block_acc = 0.0f;
        \\  uint in_off = r * InDim + b * 256u;
        \\  for (uint i = 0; i < 256; ++i) {
        \\    char q = as_type<char>(weight[off + 4u + i]);
        \\    block_acc += input[in_off + i] * float(q);
        \\  }
        \\  acc += d * block_acc;
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const i8_s_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  char q = as_type<char>(weight[row_offset + b]);
        \\  acc += input[r * InDim + b] * float(q);
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const i2_s_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float abs_max = 0.0f;
        \\for (uint i = 0; i < InDim; ++i) {
        \\  abs_max = fmax(abs_max, fabs(input[r * InDim + i]));
        \\}
        \\float act_scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 32u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 32u;
        \\  uint in_base = r * InDim + b * 128u;
        \\  for (uint group = 0; group < 32u; ++group) {
        \\    uchar packed = weight[off + group];
        \\    uint code0 = uint((packed >> 6) & 0x03u);
        \\    uint code1 = uint((packed >> 4) & 0x03u);
        \\    uint code2 = uint((packed >> 2) & 0x03u);
        \\    uint code3 = uint(packed & 0x03u);
        \\    float w0 = code0 == 0u ? -1.0f : (code0 == 2u ? 1.0f : 0.0f);
        \\    float w1 = code1 == 0u ? -1.0f : (code1 == 2u ? 1.0f : 0.0f);
        \\    float w2 = code2 == 0u ? -1.0f : (code2 == 2u ? 1.0f : 0.0f);
        \\    float w3 = code3 == 0u ? -1.0f : (code3 == 2u ? 1.0f : 0.0f);
        \\    float x0 = clamp(round(input[in_base + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float x1 = clamp(round(input[in_base + 32u + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float x2 = clamp(round(input[in_base + 64u + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float x3 = clamp(round(input[in_base + 96u + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    acc += x0 * w0 + x1 * w1 + x2 * w2 + x3 * w3;
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const i2_s_pair_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float abs_max = 0.0f;
        \\for (uint i = 0; i < InDim; ++i) {
        \\  abs_max = fmax(abs_max, fabs(input[r * InDim + i]));
        \\}
        \\float act_scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f;
        \\float acc_a = 0.0f;
        \\float acc_b = 0.0f;
        \\const uint row_offset = o * RowBlocks * 32u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 32u;
        \\  uint in_base = r * InDim + b * 128u;
        \\  for (uint group = 0; group < 32u; ++group) {
        \\    uchar packed_a = weight_a[off + group];
        \\    uchar packed_b = weight_b[off + group];
        \\    uint code_a0 = uint((packed_a >> 6) & 0x03u);
        \\    uint code_a1 = uint((packed_a >> 4) & 0x03u);
        \\    uint code_a2 = uint((packed_a >> 2) & 0x03u);
        \\    uint code_a3 = uint(packed_a & 0x03u);
        \\    uint code_b0 = uint((packed_b >> 6) & 0x03u);
        \\    uint code_b1 = uint((packed_b >> 4) & 0x03u);
        \\    uint code_b2 = uint((packed_b >> 2) & 0x03u);
        \\    uint code_b3 = uint(packed_b & 0x03u);
        \\    float wa0 = code_a0 == 0u ? -1.0f : (code_a0 == 2u ? 1.0f : 0.0f);
        \\    float wa1 = code_a1 == 0u ? -1.0f : (code_a1 == 2u ? 1.0f : 0.0f);
        \\    float wa2 = code_a2 == 0u ? -1.0f : (code_a2 == 2u ? 1.0f : 0.0f);
        \\    float wa3 = code_a3 == 0u ? -1.0f : (code_a3 == 2u ? 1.0f : 0.0f);
        \\    float wb0 = code_b0 == 0u ? -1.0f : (code_b0 == 2u ? 1.0f : 0.0f);
        \\    float wb1 = code_b1 == 0u ? -1.0f : (code_b1 == 2u ? 1.0f : 0.0f);
        \\    float wb2 = code_b2 == 0u ? -1.0f : (code_b2 == 2u ? 1.0f : 0.0f);
        \\    float wb3 = code_b3 == 0u ? -1.0f : (code_b3 == 2u ? 1.0f : 0.0f);
        \\    float x0 = clamp(round(input[in_base + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float x1 = clamp(round(input[in_base + 32u + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float x2 = clamp(round(input[in_base + 64u + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float x3 = clamp(round(input[in_base + 96u + group] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    acc_a += x0 * wa0 + x1 * wa1 + x2 * wa2 + x3 * wa3;
        \\    acc_b += x0 * wb0 + x1 * wb1 + x2 * wb2 + x3 * wb3;
        \\  }
        \\}
        \\output_a[r * OutDim + o] = acc_a;
        \\output_b[r * OutDim + o] = acc_b;
    ;

    const tl1_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint input_base = r * InDim;
        \\float abs_max = 0.0f;
        \\for (uint i = 0; i < InDim; ++i) {
        \\  abs_max = fmax(abs_max, fabs(input[input_base + i]));
        \\}
        \\float act_scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f;
        \\uint scale_off = PackedLen;
        \\uint scale_bits = uint(weight[scale_off]) |
        \\    (uint(weight[scale_off + 1u]) << 8) |
        \\    (uint(weight[scale_off + 2u]) << 16) |
        \\    (uint(weight[scale_off + 3u]) << 24);
        \\float weight_scale = as_type<float>(scale_bits);
        \\float acc = 0.0f;
        \\uint row_outer = o / Bm;
        \\uint row_in_bm = o % Bm;
        \\uint bm_block = row_in_bm / Bmm;
        \\uint row_in_bmm = row_in_bm % Bmm;
        \\uint tl_by = 256u / Bmm;
        \\uint row16 = row_in_bmm % 16u;
        \\uint bmm16 = row_in_bmm / 16u;
        \\uint col_blocks = InDim / CfgBy;
        \\uint pair_count = InDim / 2u;
        \\for (uint pair_col = 0; pair_col < pair_count; ++pair_col) {
        \\  uint col_block = pair_col / (CfgBy / 2u);
        \\  uint col_in_block = pair_col % (CfgBy / 2u);
        \\  uint by_block = col_in_block / (tl_by / 2u);
        \\  uint pair_in_by = col_in_block % (tl_by / 2u);
        \\  uint by4 = pair_in_by / 2u;
        \\  uint nibble = pair_in_by % 2u;
        \\  uint index = ((((((row_outer * col_blocks + col_block) * (Bm / Bmm) + bm_block) * (CfgBy / tl_by) + by_block) * (Bmm / 16u) + bmm16) * (tl_by / 4u) + by4) * 16u + row16);
        \\  uchar packed = weight[index];
        \\  uint code = nibble == 0u ? uint(packed >> 4) : uint(packed & 0x0Fu);
        \\  float w0 = floor(float(code) / 3.0f) - 1.0f;
        \\  float w1 = float(code % 3u) - 1.0f;
        \\  uint col = pair_col * 2u;
        \\  float a0 = clamp(round(input[input_base + col] / act_scale), -127.0f, 127.0f) * act_scale;
        \\  float a1 = clamp(round(input[input_base + col + 1u] / act_scale), -127.0f, 127.0f) * act_scale;
        \\  acc += a0 * w0 + a1 * w1;
        \\}
        \\output[r * OutDim + o] = acc * weight_scale;
    ;

    const tl2_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint input_base = r * InDim;
        \\float abs_max = 0.0f;
        \\for (uint i = 0; i < InDim; ++i) {
        \\  abs_max = fmax(abs_max, fabs(input[input_base + i]));
        \\}
        \\float act_scale = abs_max == 0.0f ? 1.0f : abs_max / 127.0f;
        \\uint scale_bits = uint(weight[ScaleOff]) |
        \\    (uint(weight[ScaleOff + 1u]) << 8) |
        \\    (uint(weight[ScaleOff + 2u]) << 16) |
        \\    (uint(weight[ScaleOff + 3u]) << 24);
        \\float weight_scale = as_type<float>(scale_bits);
        \\float acc = 0.0f;
        \\uint row_outer = o / Bm;
        \\uint row_in_bm = o % Bm;
        \\uint bm_block = row_in_bm / Bmm;
        \\uint row_in_bmm = row_in_bm % Bmm;
        \\uint row_group = row_in_bmm / 8u;
        \\uint row_offset = row_in_bmm % 8u;
        \\uint final_group = row_group == 1u ? 2u : (row_group == 2u ? 1u : row_group);
        \\uint three_by = 192u / Bmm;
        \\uint col_blocks = InDim / CfgBy;
        \\uint three_count = ThreeCols / 3u;
        \\for (uint triple_col = 0; triple_col < three_count; ++triple_col) {
        \\  uint col_block = triple_col / (CfgBy / 3u);
        \\  uint col_in_block = triple_col % (CfgBy / 3u);
        \\  uint by_block = col_in_block / (three_by / 3u);
        \\  uint triple_in_by = col_in_block % (three_by / 3u);
        \\  uint value_index = ((((row_outer * col_blocks + col_block) * (Bm / Bmm) + bm_block) * (CfgBy / three_by) + by_block) * Bmm + final_group * 8u + row_offset);
        \\  uchar packed = weight[value_index];
        \\  uint code = triple_in_by == 0u ? uint(packed >> 4) : uint(packed & 0x0Fu);
        \\  uint sub_count = (three_by / 3u) * 4u;
        \\  uint sign_block = col_in_block / sub_count;
        \\  uint sign_sub = col_in_block % sub_count;
        \\  uint line = sign_sub * Bmm + row_in_bmm;
        \\  uint bit_lane = line / (Bmm / 2u);
        \\  uint half_idx = line % (Bmm / 2u);
        \\  uint bit_index = 15u - bit_lane;
        \\  uint sign_index = ((((row_outer * col_blocks + col_block) * (Bm / Bmm) + bm_block) * (CfgBy / (three_by * 4u)) + sign_block) * Bmm + half_idx * 2u + bit_index / 8u);
        \\  uchar sign_byte = weight[ThreeValueLen + sign_index];
        \\  bool negative = ((sign_byte >> (bit_index % 8u)) & 1u) != 0u;
        \\  float w0 = 0.0f;
        \\  float w1 = 0.0f;
        \\  float w2 = 0.0f;
        \\  switch (code) {
        \\    case 1u: w2 = 1.0f; break;
        \\    case 2u: w1 = 1.0f; w2 = -1.0f; break;
        \\    case 3u: w1 = 1.0f; break;
        \\    case 4u: w1 = 1.0f; w2 = 1.0f; break;
        \\    case 5u: w0 = 1.0f; w1 = -1.0f; w2 = -1.0f; break;
        \\    case 6u: w0 = 1.0f; w1 = -1.0f; break;
        \\    case 7u: w0 = 1.0f; w1 = -1.0f; w2 = 1.0f; break;
        \\    case 8u: w0 = 1.0f; w2 = -1.0f; break;
        \\    case 9u: w0 = 1.0f; break;
        \\    case 10u: w0 = 1.0f; w2 = 1.0f; break;
        \\    case 11u: w0 = 1.0f; w1 = 1.0f; w2 = -1.0f; break;
        \\    case 12u: w0 = 1.0f; w1 = 1.0f; break;
        \\    case 13u: w0 = 1.0f; w1 = 1.0f; w2 = 1.0f; break;
        \\    default: break;
        \\  }
        \\  if (negative) {
        \\    w0 = -w0;
        \\    w1 = -w1;
        \\    w2 = -w2;
        \\  }
        \\  uint col = triple_col * 3u;
        \\  float a0 = clamp(round(input[input_base + col] / act_scale), -127.0f, 127.0f) * act_scale;
        \\  float a1 = clamp(round(input[input_base + col + 1u] / act_scale), -127.0f, 127.0f) * act_scale;
        \\  float a2 = clamp(round(input[input_base + col + 2u] / act_scale), -127.0f, 127.0f) * act_scale;
        \\  acc += a0 * w0 + a1 * w1 + a2 * w2;
        \\}
        \\if (TwoCols != 0u) {
        \\  uint tail_base = ThreeValueLen + ThreeSignLen;
        \\  uint tail_pair_count = TwoCols / 2u;
        \\  uint tail_row_outer = o / 32u;
        \\  uint tail_row_in_bm = o % 32u;
        \\  uint tail_row_group = tail_row_in_bm / 8u;
        \\  uint tail_row_offset = tail_row_in_bm % 8u;
        \\  uint tail_final_group = tail_row_group == 1u ? 2u : (tail_row_group == 2u ? 1u : tail_row_group);
        \\  for (uint pair_col = 0; pair_col < tail_pair_count; ++pair_col) {
        \\    uint col_block = pair_col / 16u;
        \\    uint col_in_block = pair_col % 16u;
        \\    uint by_block = col_in_block / 2u;
        \\    uint pair_in_by = col_in_block % 2u;
        \\    uint tail_col_blocks = TwoCols / 32u;
        \\    uint index = (((tail_row_outer * tail_col_blocks + col_block) * 8u + by_block) * 32u + tail_final_group * 8u + tail_row_offset);
        \\    uchar packed = weight[tail_base + index];
        \\    uint code = pair_in_by == 0u ? uint(packed >> 4) : uint(packed & 0x0Fu);
        \\    float w0 = floor(float(code) / 3.0f) - 1.0f;
        \\    float w1 = float(code % 3u) - 1.0f;
        \\    uint col = ThreeCols + pair_col * 2u;
        \\    float a0 = clamp(round(input[input_base + col] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    float a1 = clamp(round(input[input_base + col + 1u] / act_scale), -127.0f, 127.0f) * act_scale;
        \\    acc += a0 * w0 + a1 * w1;
        \\  }
        \\}
        \\output[r * OutDim + o] = acc * weight_scale;
    ;

    const q8_0_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 34u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 34u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  float block_acc = 0.0f;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 32; ++i) {
        \\    char q = as_type<char>(weight[off + 2u + i]);
        \\    block_acc += input[in_off + i] * float(q);
        \\  }
        \\  acc += d * block_acc;
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q8_1_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 36u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 36u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  float block_acc = 0.0f;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 32; ++i) {
        \\    char q = as_type<char>(weight[off + 4u + i]);
        \\    block_acc += input[in_off + i] * float(q);
        \\  }
        \\  acc += d * block_acc;
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q8_1_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 36u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 36u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  float block_acc = 0.0f;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 32; ++i) {
        \\    char q = as_type<char>(weight[off + 4u + i]);
        \\    block_acc += input[in_off + i] * float(q);
        \\  }
        \\  acc += d * block_acc;
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const iq4_nl_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\int iq4_values[16] = { -127, -104, -83, -65, -49, -35, -22, -10, 1, 13, 25, 38, 53, 69, 89, 113 };
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 18u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 18u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  auto qs = weight + off + 2u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    uint nib = uint(packed & 0x0Fu);
        \\    acc += input[in_off + i] * (d * float(iq4_values[nib]));
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    uint nib = uint((packed >> 4) & 0x0Fu);
        \\    acc += input[in_off + 16u + i] * (d * float(iq4_values[nib]));
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const iq4_nl_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\int iq4_values[16] = { -127, -104, -83, -65, -49, -35, -22, -10, 1, 13, 25, 38, 53, 69, 89, 113 };
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 18u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 18u;
        \\  ushort bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  float d = float(as_type<half>(bits));
        \\  auto qs = weight + off + 2u;
        \\  uint in_off = r * InDim + b * 32u;
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    uint nib = uint(packed & 0x0Fu);
        \\    acc += input[in_off + i] * (d * float(iq4_values[nib]));
        \\  }
        \\  for (uint i = 0; i < 16; ++i) {
        \\    uchar packed = qs[i];
        \\    uint nib = uint((packed >> 4) & 0x0Fu);
        \\    acc += input[in_off + 16u + i] * (d * float(iq4_values[nib]));
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const iq4_xs_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\int iq4_values[16] = { -127, -104, -83, -65, -49, -35, -22, -10, 1, 13, 25, 38, 53, 69, 89, 113 };
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 136u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 136u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort scales_h = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  auto scales_l = weight + off + 4u;
        \\  auto qs = weight + off + 8u;
        \\  for (uint ib = 0; ib < 8; ++ib) {
        \\    uint low = (uint(scales_l[ib / 2u]) >> (4u * (ib % 2u))) & 0x0Fu;
        \\    uint high = (uint(scales_h) >> (2u * ib)) & 0x03u;
        \\    float dl = d * float(int(low | (high << 4)) - 32);
        \\    uint q_base = ib * 16u;
        \\    uint in_off = r * InDim + b * 256u + ib * 32u;
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uchar packed = qs[q_base + i];
        \\      uint nib = uint(packed & 0x0Fu);
        \\      acc += input[in_off + i] * (dl * float(iq4_values[nib]));
        \\    }
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uchar packed = qs[q_base + i];
        \\      uint nib = uint((packed >> 4) & 0x0Fu);
        \\      acc += input[in_off + 16u + i] * (dl * float(iq4_values[nib]));
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const iq4_xs_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\int iq4_values[16] = { -127, -104, -83, -65, -49, -35, -22, -10, 1, 13, 25, 38, 53, 69, 89, 113 };
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 136u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 136u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort scales_h = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  auto scales_l = weight + off + 4u;
        \\  auto qs = weight + off + 8u;
        \\  for (uint ib = 0; ib < 8; ++ib) {
        \\    uint low = (uint(scales_l[ib / 2u]) >> (4u * (ib % 2u))) & 0x0Fu;
        \\    uint high = (uint(scales_h) >> (2u * ib)) & 0x03u;
        \\    float dl = d * float(int(low | (high << 4)) - 32);
        \\    uint q_base = ib * 16u;
        \\    uint in_off = r * InDim + b * 256u + ib * 32u;
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uchar packed = qs[q_base + i];
        \\      uint nib = uint(packed & 0x0Fu);
        \\      acc += input[in_off + i] * (dl * float(iq4_values[nib]));
        \\    }
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uchar packed = qs[q_base + i];
        \\      uint nib = uint((packed >> 4) & 0x0Fu);
        \\      acc += input[in_off + 16u + i] * (dl * float(iq4_values[nib]));
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q2_k_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 84u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 84u;
        \\  auto scales_raw = weight + off;
        \\  ushort d_bits = (ushort(weight[off + 17]) << 8) | ushort(weight[off + 16]);
        \\  ushort dmin_bits = (ushort(weight[off + 19]) << 8) | ushort(weight[off + 18]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float dmin = float(as_type<half>(dmin_bits));
        \\  auto qs = weight + off + 20u;
        \\  for (uint sub = 0; sub < 16; ++sub) {
        \\    float sc = float(scales_raw[sub] & 0x0Fu);
        \\    float m = float(scales_raw[sub] >> 4);
        \\    float dsc = d * sc;
        \\    float dmn = dmin * m;
        \\    uint chunk = sub / 8u;
        \\    uint group = (sub % 8u) / 2u;
        \\    uint l_base = (sub % 2u) * 16u;
        \\    uint q_base = chunk * 32u;
        \\    uint shift = group * 2u;
        \\    uint input_off = r * InDim + b * 256u + sub * 16u;
        \\    for (uint i = 0; i < 16; ++i) {
        \\      int q = int((qs[q_base + l_base + i] >> shift) & 0x03u);
        \\      acc += input[input_off + i] * (dsc * float(q) - dmn);
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q3_k_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 110u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 110u;
        \\  auto hmask = weight + off;
        \\  auto qs = weight + off + 32u;
        \\  auto scale_data = weight + off + 96u;
        \\  ushort d_bits = (ushort(weight[off + 109]) << 8) | ushort(weight[off + 108]);
        \\  float d = float(as_type<half>(d_bits));
        \\  int raw_scales[16];
        \\  for (uint i = 0; i < 4; ++i) {
        \\    raw_scales[i] = int(scale_data[i] & 0x0Fu) | (int(scale_data[8 + i] & 0x03u) << 4);
        \\    raw_scales[i + 4] = int(scale_data[4 + i] & 0x0Fu) | (int((scale_data[8 + i] >> 2) & 0x03u) << 4);
        \\    raw_scales[i + 8] = int((scale_data[i] >> 4) & 0x0Fu) | (int((scale_data[8 + i] >> 4) & 0x03u) << 4);
        \\    raw_scales[i + 12] = int((scale_data[4 + i] >> 4) & 0x0Fu) | (int((scale_data[8 + i] >> 6) & 0x03u) << 4);
        \\  }
        \\  for (uint sub = 0; sub < 16; ++sub) {
        \\    float scale = d * float(raw_scales[sub] - 32);
        \\    uint chunk = sub / 8u;
        \\    uint group = (sub % 8u) / 2u;
        \\    uint l_base = (sub % 2u) * 16u;
        \\    uint q_base = chunk * 32u;
        \\    uint shift = group * 2u;
        \\    uint hm_bit = chunk * 4u + group;
        \\    uint input_off = r * InDim + b * 256u + sub * 16u;
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uint l = l_base + i;
        \\      int low2 = int((qs[q_base + l] >> shift) & 0x03u);
        \\      int high1 = int((hmask[l] >> hm_bit) & 0x01u);
        \\      int q = low2 + high1 * 4 - 4;
        \\      acc += input[input_off + i] * (scale * float(q));
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q4_k_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 144u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 144u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort dmin_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float dmin = float(as_type<half>(dmin_bits));
        \\  auto scales = weight + off + 4u;
        \\  auto qs = weight + off + 16u;
        \\  float scs[8];
        \\  float mins[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs[j] = float(scales[j] & 63u);
        \\    mins[j] = float(scales[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs[j] = float((scales[j + 4] & 0x0Fu) | ((scales[j - 4] >> 6) << 4));
        \\    mins[j] = float((scales[j + 4] >> 4) | ((scales[j] >> 6) << 4));
        \\  }
        \\  uint q_off = 0u;
        \\  for (uint chunk = 0; chunk < 4; ++chunk) {
        \\    uint sub = chunk * 2u;
        \\    float dsc0 = d * scs[sub];
        \\    float dmn0 = dmin * mins[sub];
        \\    float dsc1 = d * scs[sub + 1u];
        \\    float dmn1 = dmin * mins[sub + 1u];
        \\    uint input_off = r * InDim + b * 256u + chunk * 64u;
        \\    for (uint i = 0; i < 32; ++i) acc += input[input_off + i] * (dsc0 * float(qs[q_off + i] & 0x0Fu) - dmn0);
        \\    for (uint i = 0; i < 32; ++i) acc += input[input_off + 32u + i] * (dsc1 * float(qs[q_off + i] >> 4) - dmn1);
        \\    q_off += 32u;
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q4_k_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 144u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 144u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort dmin_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float dmin = float(as_type<half>(dmin_bits));
        \\  auto scales = weight + off + 4u;
        \\  auto qs = weight + off + 16u;
        \\  float scs[8];
        \\  float mins[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs[j] = float(scales[j] & 63u);
        \\    mins[j] = float(scales[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs[j] = float((scales[j + 4] & 0x0Fu) | ((scales[j - 4] >> 6) << 4));
        \\    mins[j] = float((scales[j + 4] >> 4) | ((scales[j] >> 6) << 4));
        \\  }
        \\  uint q_off = 0u;
        \\  for (uint chunk = 0; chunk < 4; ++chunk) {
        \\    uint sub = chunk * 2u;
        \\    float dsc0 = d * scs[sub];
        \\    float dmn0 = dmin * mins[sub];
        \\    float dsc1 = d * scs[sub + 1u];
        \\    float dmn1 = dmin * mins[sub + 1u];
        \\    uint input_off = r * InDim + b * 256u + chunk * 64u;
        \\    for (uint i = 0; i < 32; ++i) acc += input[input_off + i] * (dsc0 * float(qs[q_off + i] & 0x0Fu) - dmn0);
        \\    for (uint i = 0; i < 32; ++i) acc += input[input_off + 32u + i] * (dsc1 * float(qs[q_off + i] >> 4) - dmn1);
        \\    q_off += 32u;
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q4_k_pair_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc_a = 0.0f;
        \\float acc_b = 0.0f;
        \\const uint row_offset = o * RowBlocks * 144u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off_a = row_offset + b * 144u;
        \\  uint off_b = row_offset + b * 144u;
        \\  ushort d_bits_a = (ushort(weight_a[off_a + 1]) << 8) | ushort(weight_a[off_a]);
        \\  ushort dmin_bits_a = (ushort(weight_a[off_a + 3]) << 8) | ushort(weight_a[off_a + 2]);
        \\  ushort d_bits_b = (ushort(weight_b[off_b + 1]) << 8) | ushort(weight_b[off_b]);
        \\  ushort dmin_bits_b = (ushort(weight_b[off_b + 3]) << 8) | ushort(weight_b[off_b + 2]);
        \\  float d_a = float(as_type<half>(d_bits_a));
        \\  float dmin_a = float(as_type<half>(dmin_bits_a));
        \\  float d_b = float(as_type<half>(d_bits_b));
        \\  float dmin_b = float(as_type<half>(dmin_bits_b));
        \\  auto scales_a = weight_a + off_a + 4u;
        \\  auto qs_a = weight_a + off_a + 16u;
        \\  auto scales_b = weight_b + off_b + 4u;
        \\  auto qs_b = weight_b + off_b + 16u;
        \\  float scs_a[8];
        \\  float mins_a[8];
        \\  float scs_b[8];
        \\  float mins_b[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs_a[j] = float(scales_a[j] & 63u);
        \\    mins_a[j] = float(scales_a[j + 4] & 63u);
        \\    scs_b[j] = float(scales_b[j] & 63u);
        \\    mins_b[j] = float(scales_b[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs_a[j] = float((scales_a[j + 4] & 0x0Fu) | ((scales_a[j - 4] >> 6) << 4));
        \\    mins_a[j] = float((scales_a[j + 4] >> 4) | ((scales_a[j] >> 6) << 4));
        \\    scs_b[j] = float((scales_b[j + 4] & 0x0Fu) | ((scales_b[j - 4] >> 6) << 4));
        \\    mins_b[j] = float((scales_b[j + 4] >> 4) | ((scales_b[j] >> 6) << 4));
        \\  }
        \\  uint q_off = 0u;
        \\  for (uint chunk = 0; chunk < 4; ++chunk) {
        \\    uint sub = chunk * 2u;
        \\    float dsc0_a = d_a * scs_a[sub];
        \\    float dmn0_a = dmin_a * mins_a[sub];
        \\    float dsc1_a = d_a * scs_a[sub + 1u];
        \\    float dmn1_a = dmin_a * mins_a[sub + 1u];
        \\    float dsc0_b = d_b * scs_b[sub];
        \\    float dmn0_b = dmin_b * mins_b[sub];
        \\    float dsc1_b = d_b * scs_b[sub + 1u];
        \\    float dmn1_b = dmin_b * mins_b[sub + 1u];
        \\    uint input_off = r * InDim + b * 256u + chunk * 64u;
        \\    for (uint i = 0; i < 32; ++i) {
        \\      uchar packed_a = qs_a[q_off + i];
        \\      uchar packed_b = qs_b[q_off + i];
        \\      float x0 = input[input_off + i];
        \\      float x1 = input[input_off + 32u + i];
        \\      acc_a += x0 * (dsc0_a * float(packed_a & 0x0Fu) - dmn0_a);
        \\      acc_a += x1 * (dsc1_a * float(packed_a >> 4) - dmn1_a);
        \\      acc_b += x0 * (dsc0_b * float(packed_b & 0x0Fu) - dmn0_b);
        \\      acc_b += x1 * (dsc1_b * float(packed_b >> 4) - dmn1_b);
        \\    }
        \\    q_off += 32u;
        \\  }
        \\}
        \\output_a[r * OutDim + o] = acc_a;
        \\output_b[r * OutDim + o] = acc_b;
    ;

    const q5_k_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 176u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 176u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort dmin_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float dmin = float(as_type<half>(dmin_bits));
        \\  auto scales = weight + off + 4u;
        \\  auto qh = weight + off + 16u;
        \\  auto ql = weight + off + 48u;
        \\  float scs[8];
        \\  float mins[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs[j] = float(scales[j] & 63u);
        \\    mins[j] = float(scales[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs[j] = float((scales[j + 4] & 0x0Fu) | ((scales[j - 4] >> 6) << 4));
        \\    mins[j] = float((scales[j + 4] >> 4) | ((scales[j] >> 6) << 4));
        \\  }
        \\  for (uint sub = 0; sub < 8; ++sub) {
        \\    float dsc = d * scs[sub];
        \\    float dmn = dmin * mins[sub];
        \\    uint chunk = sub / 2u;
        \\    bool is_high = (sub & 1u) != 0u;
        \\    uint ql_off = chunk * 32u;
        \\    uint hb_shift = sub;
        \\    uint input_off = r * InDim + b * 256u + sub * 32u;
        \\    for (uint i = 0; i < 32; ++i) {
        \\      uchar low = is_high ? (ql[ql_off + i] >> 4) : (ql[ql_off + i] & 0x0Fu);
        \\      int q = int(low) + int((qh[i] >> hb_shift) & 1u) * 16;
        \\      acc += input[input_off + i] * (dsc * float(q) - dmn);
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q5_k_pair_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc_a = 0.0f;
        \\float acc_b = 0.0f;
        \\const uint row_offset = o * RowBlocks * 176u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off_a = row_offset + b * 176u;
        \\  uint off_b = row_offset + b * 176u;
        \\  ushort d_bits_a = (ushort(weight_a[off_a + 1]) << 8) | ushort(weight_a[off_a]);
        \\  ushort dmin_bits_a = (ushort(weight_a[off_a + 3]) << 8) | ushort(weight_a[off_a + 2]);
        \\  ushort d_bits_b = (ushort(weight_b[off_b + 1]) << 8) | ushort(weight_b[off_b]);
        \\  ushort dmin_bits_b = (ushort(weight_b[off_b + 3]) << 8) | ushort(weight_b[off_b + 2]);
        \\  float d_a = float(as_type<half>(d_bits_a));
        \\  float dmin_a = float(as_type<half>(dmin_bits_a));
        \\  float d_b = float(as_type<half>(d_bits_b));
        \\  float dmin_b = float(as_type<half>(dmin_bits_b));
        \\  auto scales_a = weight_a + off_a + 4u;
        \\  auto qh_a = weight_a + off_a + 16u;
        \\  auto ql_a = weight_a + off_a + 48u;
        \\  auto scales_b = weight_b + off_b + 4u;
        \\  auto qh_b = weight_b + off_b + 16u;
        \\  auto ql_b = weight_b + off_b + 48u;
        \\  float scs_a[8];
        \\  float mins_a[8];
        \\  float scs_b[8];
        \\  float mins_b[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs_a[j] = float(scales_a[j] & 63u);
        \\    mins_a[j] = float(scales_a[j + 4] & 63u);
        \\    scs_b[j] = float(scales_b[j] & 63u);
        \\    mins_b[j] = float(scales_b[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs_a[j] = float((scales_a[j + 4] & 0x0Fu) | ((scales_a[j - 4] >> 6) << 4));
        \\    mins_a[j] = float((scales_a[j + 4] >> 4) | ((scales_a[j] >> 6) << 4));
        \\    scs_b[j] = float((scales_b[j + 4] & 0x0Fu) | ((scales_b[j - 4] >> 6) << 4));
        \\    mins_b[j] = float((scales_b[j + 4] >> 4) | ((scales_b[j] >> 6) << 4));
        \\  }
        \\  for (uint sub = 0; sub < 8; ++sub) {
        \\    float dsc_a = d_a * scs_a[sub];
        \\    float dmn_a = dmin_a * mins_a[sub];
        \\    float dsc_b = d_b * scs_b[sub];
        \\    float dmn_b = dmin_b * mins_b[sub];
        \\    uint chunk = sub / 2u;
        \\    bool is_high = (sub & 1u) != 0u;
        \\    uint ql_off = chunk * 32u;
        \\    uint hb_shift = sub;
        \\    uint input_off = r * InDim + b * 256u + sub * 32u;
        \\    for (uint i = 0; i < 32; ++i) {
        \\      uchar low_a = is_high ? (ql_a[ql_off + i] >> 4) : (ql_a[ql_off + i] & 0x0Fu);
        \\      uchar low_b = is_high ? (ql_b[ql_off + i] >> 4) : (ql_b[ql_off + i] & 0x0Fu);
        \\      int q_a = int(low_a) + int((qh_a[i] >> hb_shift) & 1u) * 16;
        \\      int q_b = int(low_b) + int((qh_b[i] >> hb_shift) & 1u) * 16;
        \\      float x = input[input_off + i];
        \\      acc_a += x * (dsc_a * float(q_a) - dmn_a);
        \\      acc_b += x * (dsc_b * float(q_b) - dmn_b);
        \\    }
        \\  }
        \\}
        \\output_a[r * OutDim + o] = acc_a;
        \\output_b[r * OutDim + o] = acc_b;
    ;

    const q5_k_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 176u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 176u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort dmin_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float dmin = float(as_type<half>(dmin_bits));
        \\  auto scales = weight + off + 4u;
        \\  auto qh = weight + off + 16u;
        \\  auto ql = weight + off + 48u;
        \\  float scs[8];
        \\  float mins[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs[j] = float(scales[j] & 63u);
        \\    mins[j] = float(scales[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs[j] = float((scales[j + 4] & 0x0Fu) | ((scales[j - 4] >> 6) << 4));
        \\    mins[j] = float((scales[j + 4] >> 4) | ((scales[j] >> 6) << 4));
        \\  }
        \\  for (uint sub = 0; sub < 8; ++sub) {
        \\    float dsc = d * scs[sub];
        \\    float dmn = dmin * mins[sub];
        \\    uint chunk = sub / 2u;
        \\    bool is_high = (sub & 1u) != 0u;
        \\    uint ql_off = chunk * 32u;
        \\    uint hb_shift = sub;
        \\    uint input_off = r * InDim + b * 256u + sub * 32u;
        \\    for (uint i = 0; i < 32; ++i) {
        \\      uchar low = is_high ? (ql[ql_off + i] >> 4) : (ql[ql_off + i] & 0x0Fu);
        \\      int q = int(low) + int((qh[i] >> hb_shift) & 1u) * 16;
        \\      acc += input[input_off + i] * (dsc * float(q) - dmn);
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q5_k_grouped_tiled_source =
        \\auto o = thread_position_in_grid.x;
        \\auto tile = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(tile_expert_ids[tile]);
        \\uint row_start = as_type<uint>(tile_row_starts[tile]);
        \\uint row_count = as_type<uint>(tile_row_counts[tile]);
        \\float acc0 = 0.0f;
        \\float acc1 = 0.0f;
        \\float acc2 = 0.0f;
        \\float acc3 = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 176u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 176u;
        \\  ushort d_bits = (ushort(weight[off + 1]) << 8) | ushort(weight[off]);
        \\  ushort dmin_bits = (ushort(weight[off + 3]) << 8) | ushort(weight[off + 2]);
        \\  float d = float(as_type<half>(d_bits));
        \\  float dmin = float(as_type<half>(dmin_bits));
        \\  auto scales = weight + off + 4u;
        \\  auto qh = weight + off + 16u;
        \\  auto ql = weight + off + 48u;
        \\  float scs[8];
        \\  float mins[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs[j] = float(scales[j] & 63u);
        \\    mins[j] = float(scales[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs[j] = float((scales[j + 4] & 0x0Fu) | ((scales[j - 4] >> 6) << 4));
        \\    mins[j] = float((scales[j + 4] >> 4) | ((scales[j] >> 6) << 4));
        \\  }
        \\  for (uint sub = 0; sub < 8; ++sub) {
        \\    float dsc = d * scs[sub];
        \\    float dmn = dmin * mins[sub];
        \\    uint chunk = sub / 2u;
        \\    bool is_high = (sub & 1u) != 0u;
        \\    uint ql_off = chunk * 32u;
        \\    uint hb_shift = sub;
        \\    uint input_base = b * 256u + sub * 32u;
        \\    for (uint i = 0; i < 32; ++i) {
        \\      uchar low = is_high ? (ql[ql_off + i] >> 4) : (ql[ql_off + i] & 0x0Fu);
        \\      int q = int(low) + int((qh[i] >> hb_shift) & 1u) * 16;
        \\      float w = dsc * float(q) - dmn;
        \\      if (row_count > 0u) acc0 += input[(row_start + 0u) * InDim + input_base + i] * w;
        \\      if (row_count > 1u) acc1 += input[(row_start + 1u) * InDim + input_base + i] * w;
        \\      if (row_count > 2u) acc2 += input[(row_start + 2u) * InDim + input_base + i] * w;
        \\      if (row_count > 3u) acc3 += input[(row_start + 3u) * InDim + input_base + i] * w;
        \\    }
        \\  }
        \\}
        \\if (row_count > 0u) output[(row_start + 0u) * OutDim + o] = acc0;
        \\if (row_count > 1u) output[(row_start + 1u) * OutDim + o] = acc1;
        \\if (row_count > 2u) output[(row_start + 2u) * OutDim + o] = acc2;
        \\if (row_count > 3u) output[(row_start + 3u) * OutDim + o] = acc3;
    ;

    const q5_k_grouped_pair_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc_a = 0.0f;
        \\float acc_b = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 176u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off_a = row_offset + b * 176u;
        \\  uint off_b = row_offset + b * 176u;
        \\  ushort d_bits_a = (ushort(weight_a[off_a + 1]) << 8) | ushort(weight_a[off_a]);
        \\  ushort dmin_bits_a = (ushort(weight_a[off_a + 3]) << 8) | ushort(weight_a[off_a + 2]);
        \\  ushort d_bits_b = (ushort(weight_b[off_b + 1]) << 8) | ushort(weight_b[off_b]);
        \\  ushort dmin_bits_b = (ushort(weight_b[off_b + 3]) << 8) | ushort(weight_b[off_b + 2]);
        \\  float d_a = float(as_type<half>(d_bits_a));
        \\  float dmin_a = float(as_type<half>(dmin_bits_a));
        \\  float d_b = float(as_type<half>(d_bits_b));
        \\  float dmin_b = float(as_type<half>(dmin_bits_b));
        \\  auto scales_a = weight_a + off_a + 4u;
        \\  auto qh_a = weight_a + off_a + 16u;
        \\  auto ql_a = weight_a + off_a + 48u;
        \\  auto scales_b = weight_b + off_b + 4u;
        \\  auto qh_b = weight_b + off_b + 16u;
        \\  auto ql_b = weight_b + off_b + 48u;
        \\  float scs_a[8];
        \\  float mins_a[8];
        \\  float scs_b[8];
        \\  float mins_b[8];
        \\  for (uint j = 0; j < 4; ++j) {
        \\    scs_a[j] = float(scales_a[j] & 63u);
        \\    mins_a[j] = float(scales_a[j + 4] & 63u);
        \\    scs_b[j] = float(scales_b[j] & 63u);
        \\    mins_b[j] = float(scales_b[j + 4] & 63u);
        \\  }
        \\  for (uint j = 4; j < 8; ++j) {
        \\    scs_a[j] = float((scales_a[j + 4] & 0x0Fu) | ((scales_a[j - 4] >> 6) << 4));
        \\    mins_a[j] = float((scales_a[j + 4] >> 4) | ((scales_a[j] >> 6) << 4));
        \\    scs_b[j] = float((scales_b[j + 4] & 0x0Fu) | ((scales_b[j - 4] >> 6) << 4));
        \\    mins_b[j] = float((scales_b[j + 4] >> 4) | ((scales_b[j] >> 6) << 4));
        \\  }
        \\  for (uint sub = 0; sub < 8; ++sub) {
        \\    float dsc_a = d_a * scs_a[sub];
        \\    float dmn_a = dmin_a * mins_a[sub];
        \\    float dsc_b = d_b * scs_b[sub];
        \\    float dmn_b = dmin_b * mins_b[sub];
        \\    uint chunk = sub / 2u;
        \\    bool is_high = (sub & 1u) != 0u;
        \\    uint ql_off = chunk * 32u;
        \\    uint hb_shift = sub;
        \\    uint input_off = r * InDim + b * 256u + sub * 32u;
        \\    for (uint i = 0; i < 32; ++i) {
        \\      uchar low_a = is_high ? (ql_a[ql_off + i] >> 4) : (ql_a[ql_off + i] & 0x0Fu);
        \\      uchar low_b = is_high ? (ql_b[ql_off + i] >> 4) : (ql_b[ql_off + i] & 0x0Fu);
        \\      int q_a = int(low_a) + int((qh_a[i] >> hb_shift) & 1u) * 16;
        \\      int q_b = int(low_b) + int((qh_b[i] >> hb_shift) & 1u) * 16;
        \\      float x = input[input_off + i];
        \\      acc_a += x * (dsc_a * float(q_a) - dmn_a);
        \\      acc_b += x * (dsc_b * float(q_b) - dmn_b);
        \\    }
        \\  }
        \\}
        \\output_a[r * OutDim + o] = acc_a;
        \\output_b[r * OutDim + o] = acc_b;
    ;

    const q6_k_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\float acc = 0.0f;
        \\const uint row_offset = o * RowBlocks * 210u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 210u;
        \\  auto ql = weight + off;
        \\  auto qh = weight + off + 128u;
        \\  ushort d_bits = (ushort(weight[off + 209]) << 8) | ushort(weight[off + 208]);
        \\  float d = float(as_type<half>(d_bits));
        \\  for (uint sub = 0; sub < 16; ++sub) {
        \\    float scale = d * float(as_type<char>(weight[off + 192u + sub]));
        \\    uint half_idx = sub / 8u;
        \\    uint group = (sub % 8u) / 2u;
        \\    uint l_base = (sub % 2u) * 16u;
        \\    uint ql_off = half_idx * 64u + (group & 1u) * 32u;
        \\    uint qh_off = half_idx * 32u;
        \\    uint qh_shift = group * 2u;
        \\    uint nibble_shift = (group / 2u) * 4u;
        \\    uint input_off = r * InDim + b * 256u + sub * 16u;
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uint l = l_base + i;
        \\      int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0Fu);
        \\      int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u);
        \\      int q = (low4 | (high2 << 4)) - 32;
        \\      acc += input[input_off + i] * (scale * float(q));
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    const q6_k_grouped_source =
        \\auto o = thread_position_in_grid.x;
        \\auto r = thread_position_in_grid.y;
        \\uint expert = as_type<uint>(expert_ids[r]);
        \\float acc = 0.0f;
        \\const uint row_offset = expert * ExpertStride + RowOffsetBytes + o * RowBlocks * 210u;
        \\for (uint b = 0; b < RowBlocks; ++b) {
        \\  uint off = row_offset + b * 210u;
        \\  auto ql = weight + off;
        \\  auto qh = weight + off + 128u;
        \\  ushort d_bits = (ushort(weight[off + 209]) << 8) | ushort(weight[off + 208]);
        \\  float d = float(as_type<half>(d_bits));
        \\  for (uint sub = 0; sub < 16; ++sub) {
        \\    float scale = d * float(as_type<char>(weight[off + 192u + sub]));
        \\    uint half_idx = sub / 8u;
        \\    uint group = (sub % 8u) / 2u;
        \\    uint l_base = (sub % 2u) * 16u;
        \\    uint ql_off = half_idx * 64u + (group & 1u) * 32u;
        \\    uint qh_off = half_idx * 32u;
        \\    uint qh_shift = group * 2u;
        \\    uint nibble_shift = (group / 2u) * 4u;
        \\    uint input_off = r * InDim + b * 256u + sub * 16u;
        \\    for (uint i = 0; i < 16; ++i) {
        \\      uint l = l_base + i;
        \\      int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0Fu);
        \\      int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u);
        \\      int q = (low4 | (high2 << 4)) - 32;
        \\      acc += input[input_off + i] * (scale * float(q));
        \\    }
        \\  }
        \\}
        \\output[r * OutDim + o] = acc;
    ;

    fn makeStringVector(values: []const []const u8) !c.mlx_vector_string {
        const vec = c.mlx_vector_string_new();
        errdefer _ = c.mlx_vector_string_free(vec);
        for (values) |value| {
            const value_z = try std.heap.c_allocator.dupeZ(u8, value);
            defer std.heap.c_allocator.free(value_z);
            try mlx.check(c.mlx_vector_string_append_value(vec, value_z.ptr));
        }
        return vec;
    }

    fn makeKernelWithLayout(
        name: []const u8,
        source: []const u8,
        inputs_layout: []const []const u8,
        outputs_layout: []const []const u8,
    ) !c.mlx_fast_metal_kernel {
        const inputs = try makeStringVector(inputs_layout);
        defer _ = c.mlx_vector_string_free(inputs);
        const outputs = try makeStringVector(outputs_layout);
        defer _ = c.mlx_vector_string_free(outputs);
        const name_z = try std.heap.c_allocator.dupeZ(u8, name);
        defer std.heap.c_allocator.free(name_z);
        const source_z = try std.heap.c_allocator.dupeZ(u8, source);
        defer std.heap.c_allocator.free(source_z);
        const header_z = try std.heap.c_allocator.dupeZ(u8, "");
        defer std.heap.c_allocator.free(header_z);

        const kernel = c.mlx_fast_metal_kernel_new(
            name_z.ptr,
            inputs,
            outputs,
            source_z.ptr,
            header_z.ptr,
            true,
            false,
        );
        if (kernel.ctx == null) return error.MetalProviderUnavailable;
        return kernel;
    }

    fn makeKernel(name: []const u8, source: []const u8) !c.mlx_fast_metal_kernel {
        return makeKernelWithLayout(name, source, &.{ "input", "weight" }, &.{"output"});
    }

    fn addTemplateInt(cfg: c.mlx_fast_metal_kernel_config, name: []const u8, value: usize) !void {
        const name_z = try std.heap.c_allocator.dupeZ(u8, name);
        defer std.heap.c_allocator.free(name_z);
        try mlx.check(c.mlx_fast_metal_kernel_config_add_template_arg_int(cfg, name_z.ptr, @intCast(value)));
    }

    pub fn create() !MetalProvider {
        var kernels: MetalProvider = undefined;
        kernels.raw_provider = termite_metal_provider_create();
        errdefer termite_metal_provider_destroy(kernels.raw_provider);
        kernels.raw_decode_runtime = termite_metal_decode_runtime_create();
        errdefer termite_metal_decode_runtime_destroy(kernels.raw_decode_runtime);
        kernels.raw_decoder_family_prepared = false;
        kernels.raw_decoder_prepared_kv_tokens = 0;
        kernels.raw_absolute_embeddings_prepared = false;
        kernels.raw_absolute_embeddings_vocab_size = 0;
        kernels.raw_absolute_embeddings_position_count = 0;
        kernels.raw_absolute_embeddings_hidden_size = 0;
        kernels.raw_layer_norm_slots_prepared = @as([decoder_runtime_layer_norm_slot_capacity]bool, @splat(false));
        kernels.raw_layer_norm_slot_hidden_sizes = @as([decoder_runtime_layer_norm_slot_capacity]usize, @splat(0));
        kernels.raw_layer_norm_slot_weights = @as([decoder_runtime_layer_norm_slot_capacity]?MetalTensor, @splat(null));
        kernels.raw_layer_norm_slot_biases = @as([decoder_runtime_layer_norm_slot_capacity]?MetalTensor, @splat(null));
        kernels.raw_rms_norm_slots_prepared = @as([decoder_runtime_rms_norm_slot_capacity]bool, @splat(false));
        kernels.raw_rms_norm_slot_hidden_sizes = @as([decoder_runtime_rms_norm_slot_capacity]usize, @splat(0));
        kernels.raw_rms_norm_slot_weights = @as([decoder_runtime_rms_norm_slot_capacity]?MetalTensor, @splat(null));
        kernels.raw_linear_slots_prepared = @as([decoder_runtime_linear_slot_capacity]bool, @splat(false));
        kernels.raw_linear_slot_kinds = @as([decoder_runtime_linear_slot_capacity]RawLinearSlotKind, @splat(.none));
        kernels.raw_linear_slot_in_dims = @as([decoder_runtime_linear_slot_capacity]usize, @splat(0));
        kernels.raw_linear_slot_out_dims = @as([decoder_runtime_linear_slot_capacity]usize, @splat(0));
        kernels.raw_linear_slot_quantized_storage = @as([decoder_runtime_linear_slot_capacity]?*QuantizedStorage, @splat(null));
        kernels.raw_linear_slot_dense_weights = @as([decoder_runtime_linear_slot_capacity]?MetalTensor, @splat(null));
        kernels.raw_linear_slot_dense_biases = @as([decoder_runtime_linear_slot_capacity]?MetalTensor, @splat(null));
        kernels.raw_linear_slot_runtime_prepared_kind = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind, @splat(.none));
        kernels.raw_linear_slot_runtime_prepared_modes = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode, @splat(.none));
        kernels.raw_quant_runtime_private_prepare_nanos = 0;
        kernels.raw_quant_runtime_mapped_prepare_nanos = 0;
        kernels.raw_quant_runtime_mapped_attempts = 0;
        kernels.raw_quant_runtime_mapped_fallbacks = 0;
        kernels.raw_quant_runtime_mapped_failures = 0;
        kernels.gathered_spans = .empty;
        if (mq.enableMetalDecoderRuntimeDebug()) {
            if (kernels.hasDecoderRuntime()) {
                _ = kernels.reserveDecoderRuntime(4096, @sizeOf(u32)) catch false;
            }
        }
        kernels.q4_0 = try makeKernel("termite_q4_0_linear", q4_0_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_0);
        kernels.q4_0_grouped = try makeKernelWithLayout(
            "termite_q4_0_grouped_linear",
            q4_0_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_0_grouped);
        kernels.q4_1 = try makeKernel("termite_q4_1_linear", q4_1_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_1);
        kernels.q4_1_grouped = try makeKernelWithLayout(
            "termite_q4_1_grouped_linear",
            q4_1_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_1_grouped);
        kernels.q5_0 = try makeKernel("termite_q5_0_linear", q5_0_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_0);
        kernels.q5_0_grouped = try makeKernelWithLayout(
            "termite_q5_0_grouped_linear",
            q5_0_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_0_grouped);
        kernels.q5_1 = try makeKernel("termite_q5_1_linear", q5_1_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_1);
        kernels.q5_1_grouped = try makeKernelWithLayout(
            "termite_q5_1_grouped_linear",
            q5_1_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_1_grouped);
        kernels.q8_0 = try makeKernel("termite_q8_0_linear", q8_0_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q8_0);
        kernels.q8_0_grouped = try makeKernelWithLayout(
            "termite_q8_0_grouped_linear",
            q8_0_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q8_0_grouped);
        kernels.q8_1 = try makeKernel("termite_q8_1_linear", q8_1_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q8_1);
        kernels.q8_1_grouped = try makeKernelWithLayout(
            "termite_q8_1_grouped_linear",
            q8_1_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q8_1_grouped);
        kernels.iq4_nl = try makeKernel("termite_iq4_nl_linear", iq4_nl_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.iq4_nl);
        kernels.iq4_nl_grouped = try makeKernelWithLayout(
            "termite_iq4_nl_grouped_linear",
            iq4_nl_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.iq4_nl_grouped);
        kernels.iq4_xs = try makeKernel("termite_iq4_xs_linear", iq4_xs_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.iq4_xs);
        kernels.iq4_xs_grouped = try makeKernelWithLayout(
            "termite_iq4_xs_grouped_linear",
            iq4_xs_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.iq4_xs_grouped);
        kernels.q1_0 = try makeKernel("termite_q1_0_linear", q1_0_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q1_0);
        kernels.q2_k = try makeKernel("termite_q2_k_linear", q2_k_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q2_k);
        kernels.q3_k = try makeKernel("termite_q3_k_linear", q3_k_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q3_k);
        kernels.q4_k = try makeKernel("termite_q4_k_linear", q4_k_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_k);
        kernels.q4_k_pair = try makeKernelWithLayout(
            "termite_q4_k_pair_linear",
            q4_k_pair_source,
            &.{ "input", "weight_a", "weight_b" },
            &.{ "output_a", "output_b" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_k_pair);
        kernels.q4_k_grouped = try makeKernelWithLayout(
            "termite_q4_k_grouped_linear",
            q4_k_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q4_k_grouped);
        kernels.q5_k = try makeKernel("termite_q5_k_linear", q5_k_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_k);
        kernels.q5_k_pair = try makeKernelWithLayout(
            "termite_q5_k_pair_linear",
            q5_k_pair_source,
            &.{ "input", "weight_a", "weight_b" },
            &.{ "output_a", "output_b" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_k_pair);
        kernels.q5_k_grouped = try makeKernelWithLayout(
            "termite_q5_k_grouped_linear",
            q5_k_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_k_grouped);
        kernels.q5_k_grouped_tiled = try makeKernelWithLayout(
            "termite_q5_k_grouped_tiled_linear",
            q5_k_grouped_tiled_source,
            &.{ "input", "weight", "tile_expert_ids", "tile_row_starts", "tile_row_counts" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_k_grouped_tiled);
        kernels.q5_k_grouped_pair = try makeKernelWithLayout(
            "termite_q5_k_grouped_pair_linear",
            q5_k_grouped_pair_source,
            &.{ "input", "weight_a", "weight_b", "expert_ids" },
            &.{ "output_a", "output_b" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q5_k_grouped_pair);
        kernels.q6_k = try makeKernel("termite_q6_k_linear", q6_k_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q6_k);
        kernels.q6_k_grouped = try makeKernelWithLayout(
            "termite_q6_k_grouped_linear",
            q6_k_grouped_source,
            &.{ "input", "weight", "expert_ids" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.q6_k_grouped);
        kernels.q8_k = try makeKernel("termite_q8_k_linear", q8_k_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.q8_k);
        kernels.i8_s = try makeKernel("termite_i8_s_linear", i8_s_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.i8_s);
        kernels.i2_s = try makeKernel("termite_i2_s_linear", i2_s_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.i2_s);
        kernels.i2_s_pair = try makeKernelWithLayout(
            "termite_i2_s_pair_linear",
            i2_s_pair_source,
            &.{ "input", "weight_a", "weight_b" },
            &.{ "output_a", "output_b" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.i2_s_pair);
        kernels.tl1 = try makeKernel("termite_tl1_linear", tl1_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.tl1);
        kernels.tl2 = try makeKernel("termite_tl2_linear", tl2_source);
        errdefer c.mlx_fast_metal_kernel_free(kernels.tl2);
        kernels.polar4_key_scores = try makeKernelWithLayout(
            "termite_polar4_key_scores",
            polar4_key_scores_source,
            &.{ "q", "encoded_key" },
            &.{"scores"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.polar4_key_scores);
        kernels.turbo3_key_scores = try makeKernelWithLayout(
            "termite_turbo3_key_scores",
            turbo3_key_scores_source,
            &.{ "q", "encoded_key" },
            &.{"scores"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.turbo3_key_scores);
        kernels.polar4_attention_block = try makeKernelWithLayout(
            "termite_polar4_attention_block",
            polar4_attention_block_source,
            &.{ "q", "encoded_key", "v", "running_max", "running_sum", "running_acc", "meta" },
            &.{ "out_max", "out_sum", "out_acc" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.polar4_attention_block);
        kernels.turbo3_attention_block = try makeKernelWithLayout(
            "termite_turbo3_attention_block",
            turbo3_attention_block_source,
            &.{ "q", "encoded_key", "v", "running_max", "running_sum", "running_acc", "meta" },
            &.{ "out_max", "out_sum", "out_acc" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.turbo3_attention_block);
        kernels.polar4_attention_span = try makeKernelWithLayout(
            "termite_polar4_attention_span",
            polar4_attention_span_source,
            &.{ "q", "encoded_key", "v", "meta" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.polar4_attention_span);
        kernels.turbo3_attention_span = try makeKernelWithLayout(
            "termite_turbo3_attention_span",
            turbo3_attention_span_source,
            &.{ "q", "encoded_key", "v", "meta" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.turbo3_attention_span);
        kernels.polar4_attention_span_partials = try makeKernelWithLayout(
            "termite_polar4_attention_span_partials",
            polar4_attention_span_partials_source,
            &.{ "q", "encoded_key", "v", "meta" },
            &.{ "partial_max", "partial_sum", "partial_acc" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.polar4_attention_span_partials);
        kernels.turbo3_attention_span_partials = try makeKernelWithLayout(
            "termite_turbo3_attention_span_partials",
            turbo3_attention_span_partials_source,
            &.{ "q", "encoded_key", "v", "meta" },
            &.{ "partial_max", "partial_sum", "partial_acc" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.turbo3_attention_span_partials);
        kernels.attention_span_reduce = try makeKernelWithLayout(
            "termite_attention_span_reduce",
            attention_span_reduce_source,
            &.{ "partial_max", "partial_sum", "partial_acc" },
            &.{"output"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.attention_span_reduce);
        kernels.lm_head_argmax_partials = try makeKernelWithLayout(
            "termite_lm_head_argmax_partials",
            lm_head_argmax_partials_source,
            &.{ "hidden", "weight" },
            &.{ "partial_scores", "partial_ids" },
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.lm_head_argmax_partials);
        kernels.lm_head_argmax_reduce = try makeKernelWithLayout(
            "termite_lm_head_argmax_reduce",
            lm_head_argmax_reduce_source,
            &.{ "partial_scores", "partial_ids" },
            &.{"token_id"},
        );
        errdefer c.mlx_fast_metal_kernel_free(kernels.lm_head_argmax_reduce);
        return kernels;
    }

    pub fn provider(self: MetalProvider) Provider {
        const boxed = std.heap.c_allocator.create(MetalProvider) catch return mq.defaultNullProvider();
        boxed.* = self;
        return .{
            .ptr = boxed,
            .vtable = &vtable,
        };
    }

    fn planLinearNoBias(ctx: *anyopaque, request: *const LinearNoBiasPlanRequest) ExecutionPlan {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        _ = self;
        if (request.in_dim == 0 or request.out_dim == 0) return .unsupported;
        return switch (request.prepared_weight.quantized_storage.tensor_type) {
            .known => |known| switch (known) {
                .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q8_0, .Q8_1, .IQ4_NL => if (request.in_dim % 32 == 0) .device_native else .unsupported,
                .Q1_0 => if (request.in_dim % 128 == 0) .device_native else .unsupported,
                .I8_S => .device_native,
                .I2_S => if (request.in_dim % 128 == 0) .device_native else .unsupported,
                .TL1 => blk: {
                    const storage = request.prepared_weight.quantized_storage;
                    const view = quant_codec.bitnetTL1View(storage.shape, storage.raw_bytes) catch break :blk .unsupported;
                    if (view.cols != request.in_dim or view.rows != request.out_dim) break :blk .unsupported;
                    break :blk .device_native;
                },
                .Q2_K, .Q3_K, .Q4_K, .Q5_K, .Q6_K, .Q8_K, .IQ4_XS => if (request.in_dim % 256 == 0) .device_native else .unsupported,
                else => .unsupported,
            },
            .bitnet_tl2 => blk: {
                const storage = request.prepared_weight.quantized_storage;
                const view = quant_codec.bitnetTL2View(storage.shape, storage.raw_bytes) catch break :blk .unsupported;
                if (view.cols != request.in_dim or view.rows != request.out_dim) break :blk .unsupported;
                break :blk .device_native;
            },
            .unknown => .unsupported,
        };
    }

    fn planLinearNoBiasPair(ctx: *anyopaque, request: *const LinearNoBiasPairPlanRequest) ExecutionPlan {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        _ = self;
        if (request.in_dim == 0 or request.out_dim == 0) return .unsupported;
        if (!std.meta.eql(request.prepared_weight_a.quantized_storage.tensor_type, request.prepared_weight_b.quantized_storage.tensor_type)) return .unsupported;
        return switch (request.prepared_weight_a.quantized_storage.tensor_type) {
            .known => |known| switch (known) {
                .I2_S => if (request.in_dim % 128 == 0) .device_native else .unsupported,
                .Q4_K => if (request.in_dim % 256 == 0) .device_native else .unsupported,
                .Q5_K => if (request.in_dim % 256 == 0) .device_native else .unsupported,
                else => .unsupported,
            },
            .bitnet_tl2 => .unsupported,
            .unknown => .unsupported,
        };
    }

    const WeightArray = struct {
        arr: c.mlx_array,
        owned: bool,
    };

    fn resolveWeightArray(request: *const LinearNoBiasRequest, tensor_type: gguf_tensor_types.TensorType) !WeightArray {
        if (request.prepared_weight_bytes) |prepared_bytes| {
            const shape = [_]i32{@intCast(prepared_bytes.len)};
            return .{
                .arr = mlx.arrayFromBytes(prepared_bytes, &shape, c.MLX_UINT8),
                .owned = true,
            };
        }
        // Only use the on-device array if it is raw quantized bytes (UINT8).
        // Weights that were dequantized to F16/F32 at load time (e.g. tied
        // embed_tokens) land here with a non-UINT8 dtype and must be
        // re-prepared from the host quantized storage instead.
        if (request.weight.ctx != null and c.mlx_array_dtype(request.weight) == c.MLX_UINT8) {
            return .{ .arr = request.weight, .owned = false };
        }
        const prepared = try mlx_quant.prepareWeightBytesForLinear(request.quantized_storage, request.in_dim, request.out_dim, tensor_type);
        const shape = [_]i32{@intCast(prepared.bytes.len)};
        return .{
            .arr = mlx.arrayFromBytes(prepared.bytes, &shape, c.MLX_UINT8),
            .owned = true,
        };
    }

    fn applyKernel(
        self: *MetalProvider,
        kernel: c.mlx_fast_metal_kernel,
        request: *const LinearNoBiasRequest,
        row_blocks: usize,
        tensor_type: gguf_tensor_types.TensorType,
    ) !?c.mlx_array {
        _ = self;
        const weight = try resolveWeightArray(request, tensor_type);
        defer {
            if (weight.owned) _ = c.mlx_array_free(weight.arr);
        }

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "RowBlocks", row_blocks);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), request.rows)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, mq.grouped_threadgroup_width))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weight.arr };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    fn applyTL1Kernel(self: *MetalProvider, request: *const LinearNoBiasRequest) !?c.mlx_array {
        const source_bytes = request.prepared_weight_bytes orelse request.quantized_storage.raw_bytes;
        const view = try quant_codec.bitnetTL1View(request.quantized_storage.shape, source_bytes);
        if (view.cols != request.in_dim or view.rows != request.out_dim) return null;

        const weight = try resolveWeightArray(request, .{ .known = .TL1 });
        defer {
            if (weight.owned) _ = c.mlx_array_free(weight.arr);
        }

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "PackedLen", view.packed_bytes.len);
        try addTemplateInt(cfg, "Bm", view.config.bm);
        try addTemplateInt(cfg, "CfgBy", view.config.by);
        try addTemplateInt(cfg, "Bmm", view.config.bmm);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), request.rows)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, mq.grouped_threadgroup_width))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weight.arr };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, self.tl1, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    fn applyTL2Kernel(self: *MetalProvider, request: *const LinearNoBiasRequest) !?c.mlx_array {
        const source_bytes = request.prepared_weight_bytes orelse request.quantized_storage.raw_bytes;
        const view = try quant_codec.bitnetTL2View(request.quantized_storage.shape, source_bytes);
        if (view.cols != request.in_dim or view.rows != request.out_dim) return null;

        const weight = try resolveWeightArray(request, .bitnet_tl2);
        defer {
            if (weight.owned) _ = c.mlx_array_free(weight.arr);
        }

        const scale_off_u64 = (gguf_tensor_types.byteLen(.bitnet_tl2, &.{ @intCast(view.cols), @intCast(view.rows) }) orelse return error.UnsupportedTensorShape) - 32;
        const scale_off: usize = @intCast(scale_off_u64);
        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "ScaleOff", scale_off);
        try addTemplateInt(cfg, "ThreeValueLen", view.three_values.len);
        try addTemplateInt(cfg, "ThreeSignLen", view.three_signs.len);
        try addTemplateInt(cfg, "Bm", view.config.bm);
        try addTemplateInt(cfg, "CfgBy", view.config.by);
        try addTemplateInt(cfg, "Bmm", view.config.bmm);
        try addTemplateInt(cfg, "ThreeCols", view.three_cols);
        try addTemplateInt(cfg, "TwoCols", view.two_cols);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), request.rows)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, mq.grouped_threadgroup_width))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weight.arr };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, self.tl2, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    const PairWeightArrays = struct {
        first: WeightArray,
        second: WeightArray,
    };

    fn resolvePairWeightArrays(
        request: *const LinearNoBiasPairRequest,
        tensor_type: gguf_tensor_types.TensorType,
    ) !PairWeightArrays {
        return .{
            .first = try resolveWeightArray(&.{
                .input = request.input,
                .weight = request.weight_a,
                .quantized_storage = request.quantized_storage_a,
                .prepared_weight_bytes = request.prepared_weight_bytes_a,
                .rows = request.rows,
                .in_dim = request.in_dim,
                .out_dim = request.out_dim,
                .stream = request.stream,
            }, tensor_type),
            .second = try resolveWeightArray(&.{
                .input = request.input,
                .weight = request.weight_b,
                .quantized_storage = request.quantized_storage_b,
                .prepared_weight_bytes = request.prepared_weight_bytes_b,
                .rows = request.rows,
                .in_dim = request.in_dim,
                .out_dim = request.out_dim,
                .stream = request.stream,
            }, tensor_type),
        };
    }

    fn applyPairKernel(
        self: *MetalProvider,
        kernel: c.mlx_fast_metal_kernel,
        request: *const LinearNoBiasPairRequest,
        row_blocks: usize,
        tensor_type: gguf_tensor_types.TensorType,
    ) !?LinearNoBiasPairResult {
        _ = self;
        const weights = try resolvePairWeightArrays(request, tensor_type);
        defer {
            if (weights.first.owned) _ = c.mlx_array_free(weights.first.arr);
            if (weights.second.owned) _ = c.mlx_array_free(weights.second.arr);
        }

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "RowBlocks", row_blocks);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), request.rows)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, mq.grouped_threadgroup_width))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weights.first.arr, weights.second.arr };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 2) return null;

        var first = c.mlx_array_new();
        var second = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&first, out_vec, 0));
        errdefer _ = c.mlx_array_free(first);
        try mlx.check(c.mlx_vector_array_get(&second, out_vec, 1));
        return .{ .first = first, .second = second };
    }

    fn applyGroupedKernel(
        self: *MetalProvider,
        kernel: c.mlx_fast_metal_kernel,
        request: *const MoeLinearNoBiasRequest,
        row_blocks: usize,
        bytes_per_block: usize,
    ) !?c.mlx_array {
        _ = self;
        const packed_view = request.quantized_storage.packed_expert orelse return null;
        if (packed_view.expert_axis != 0 and packed_view.expert_axis != 2) return null;
        if (request.expert_ids_arr == null and request.expert_ids.len != request.rows) return error.InvalidPackedExpertTensor;

        // Compute expert stride and row offset for fused gate+up tensors.
        // For non-fused tensors: expert_stride = out_dim * row_blocks * bytes_per_block, row_offset_bytes = 0.
        // For fused gate+up: expert_stride covers the FULL fused dimension, row_offset_bytes skips to the correct half.
        const expert_count: usize = packed_view.expert_count;
        const expert_stride = if (expert_count > 0) request.quantized_storage.raw_bytes.len / expert_count else request.out_dim * row_blocks * bytes_per_block;
        const row_offset_bytes: usize = @as(usize, packed_view.row_offset) * row_blocks * bytes_per_block;

        const weight_started_at = mq.monotonicNowNs();
        const weight_shape = [_]i32{@intCast(request.quantized_storage.raw_bytes.len)};
        const weight = mq.arrayFromQuantizedRawBytes(request.quantized_storage, &weight_shape);
        defer _ = c.mlx_array_free(weight);
        mq.timing_stats.grouped_weight_setup_nanos += @intCast(mq.monotonicNowNs() - weight_started_at);

        const ids_started_at = mq.monotonicNowNs();
        // Use pre-built GPU tensor if available, otherwise upload from CPU.
        const expert_ids_from_cpu = if (request.expert_ids_arr != null) null else blk: {
            const ids_owned = try std.heap.c_allocator.alloc(i32, request.expert_ids.len);
            for (request.expert_ids, 0..) |expert_id, idx| ids_owned[idx] = @intCast(expert_id);
            const ids_shape = [_]i32{@intCast(ids_owned.len)};
            break :blk mlx.arrayFromOwnedInt32(ids_owned, &ids_shape);
        };
        const expert_ids = request.expert_ids_arr orelse expert_ids_from_cpu.?;
        defer if (expert_ids_from_cpu) |arr| {
            _ = c.mlx_array_free(arr);
        };
        mq.timing_stats.grouped_ids_setup_nanos += @intCast(mq.monotonicNowNs() - ids_started_at);

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "RowBlocks", row_blocks);
        try addTemplateInt(cfg, "ExpertStride", expert_stride);
        try addTemplateInt(cfg, "RowOffsetBytes", row_offset_bytes);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), request.rows)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, mq.grouped_threadgroup_width))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weight, expert_ids };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        const apply_started_at = mq.monotonicNowNs();
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        mq.timing_stats.grouped_apply_nanos += @intCast(mq.monotonicNowNs() - apply_started_at);
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    fn applyGroupedTiledKernel(
        self: *MetalProvider,
        kernel: c.mlx_fast_metal_kernel,
        request: *const MoeLinearNoBiasRequest,
        row_blocks: usize,
    ) !?c.mlx_array {
        _ = self;
        const packed_view = request.quantized_storage.packed_expert orelse return null;
        const tile_expert_ids = request.expert_tile_ids orelse return null;
        const tile_row_starts = request.tile_row_starts orelse return null;
        const tile_row_counts = request.tile_row_counts orelse return null;
        if (packed_view.expert_axis != 0 and packed_view.expert_axis != 2) return null;
        if (tile_expert_ids.len == 0 or tile_expert_ids.len != tile_row_starts.len or tile_expert_ids.len != tile_row_counts.len) {
            return error.InvalidPackedExpertTensor;
        }

        const expert_count: usize = packed_view.expert_count;
        const bytes_per_block: usize = 176;
        const expert_stride = if (expert_count > 0)
            request.quantized_storage.raw_bytes.len / expert_count
        else
            request.out_dim * row_blocks * bytes_per_block;
        const row_offset_bytes: usize = @as(usize, packed_view.row_offset) * row_blocks * bytes_per_block;

        const weight_started_at = mq.monotonicNowNs();
        const weight_shape = [_]i32{@intCast(request.quantized_storage.raw_bytes.len)};
        const weight = mq.arrayFromQuantizedRawBytes(request.quantized_storage, &weight_shape);
        defer _ = c.mlx_array_free(weight);
        mq.timing_stats.grouped_weight_setup_nanos += @intCast(mq.monotonicNowNs() - weight_started_at);

        const ids_started_at = mq.monotonicNowNs();
        const tile_expert_owned = try std.heap.c_allocator.alloc(i32, tile_expert_ids.len);
        const tile_start_owned = try std.heap.c_allocator.alloc(i32, tile_row_starts.len);
        const tile_count_owned = try std.heap.c_allocator.alloc(i32, tile_row_counts.len);
        for (tile_expert_ids, 0..) |expert_id, idx| tile_expert_owned[idx] = @intCast(expert_id);
        for (tile_row_starts, 0..) |row_start, idx| tile_start_owned[idx] = @intCast(row_start);
        for (tile_row_counts, 0..) |row_count, idx| tile_count_owned[idx] = @intCast(row_count);
        const ids_shape = [_]i32{@intCast(tile_expert_owned.len)};
        const starts_shape = [_]i32{@intCast(tile_start_owned.len)};
        const counts_shape = [_]i32{@intCast(tile_count_owned.len)};
        const tile_expert_arr = mlx.arrayFromOwnedInt32(tile_expert_owned, &ids_shape);
        defer _ = c.mlx_array_free(tile_expert_arr);
        const tile_start_arr = mlx.arrayFromOwnedInt32(tile_start_owned, &starts_shape);
        defer _ = c.mlx_array_free(tile_start_arr);
        const tile_count_arr = mlx.arrayFromOwnedInt32(tile_count_owned, &counts_shape);
        defer _ = c.mlx_array_free(tile_count_arr);
        mq.timing_stats.grouped_ids_setup_nanos += @intCast(mq.monotonicNowNs() - ids_started_at);

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "RowBlocks", row_blocks);
        try addTemplateInt(cfg, "ExpertStride", expert_stride);
        try addTemplateInt(cfg, "RowOffsetBytes", row_offset_bytes);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), tile_expert_ids.len)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, mq.grouped_threadgroup_width))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weight, tile_expert_arr, tile_start_arr, tile_count_arr };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        const apply_started_at = mq.monotonicNowNs();
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        mq.timing_stats.grouped_apply_nanos += @intCast(mq.monotonicNowNs() - apply_started_at);
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    fn applyGroupedPairKernel(
        self: *MetalProvider,
        kernel: c.mlx_fast_metal_kernel,
        request: *const MoeLinearNoBiasPairRequest,
        row_blocks: usize,
        tensor_type: gguf_tensor_types.TensorType,
    ) !?MoeLinearNoBiasPairResult {
        _ = self;
        const packed_view_a = request.quantized_storage_a.packed_expert orelse return null;
        const packed_view_b = request.quantized_storage_b.packed_expert orelse return null;
        if (packed_view_a.expert_axis != 0 or packed_view_b.expert_axis != 0) return null;
        if (request.expert_ids.len != request.rows) return error.InvalidPackedExpertTensor;
        if (!std.meta.eql(request.quantized_storage_a.tensor_type, tensor_type) or !std.meta.eql(request.quantized_storage_b.tensor_type, tensor_type)) return null;

        const weight_shape_a = [_]i32{@intCast(request.quantized_storage_a.raw_bytes.len)};
        const weight_a = mlx.arrayFromBorrowedBytes(request.quantized_storage_a.raw_bytes, &weight_shape_a, c.MLX_UINT8);
        defer _ = c.mlx_array_free(weight_a);
        const weight_shape_b = [_]i32{@intCast(request.quantized_storage_b.raw_bytes.len)};
        const weight_b = mlx.arrayFromBorrowedBytes(request.quantized_storage_b.raw_bytes, &weight_shape_b, c.MLX_UINT8);
        defer _ = c.mlx_array_free(weight_b);

        const ids_owned = try std.heap.c_allocator.alloc(i32, request.expert_ids.len);
        for (request.expert_ids, 0..) |expert_id, idx| ids_owned[idx] = @intCast(expert_id);
        const ids_shape = [_]i32{@intCast(ids_owned.len)};
        const expert_ids = mlx.arrayFromOwnedInt32(ids_owned, &ids_shape);
        defer _ = c.mlx_array_free(expert_ids);

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ @intCast(request.rows), @intCast(request.out_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "InDim", request.in_dim);
        try addTemplateInt(cfg, "OutDim", request.out_dim);
        try addTemplateInt(cfg, "RowBlocks", row_blocks);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(@max(@as(usize, 1), request.out_dim)), @intCast(@max(@as(usize, 1), request.rows)), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@max(@as(usize, 1), @min(request.out_dim, @as(usize, 32)))), 1, 1));

        const inputs = [_]c.mlx_array{ request.input, weight_a, weight_b, expert_ids };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 2) return null;

        var first = c.mlx_array_new();
        var second = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&first, out_vec, 0));
        errdefer _ = c.mlx_array_free(first);
        try mlx.check(c.mlx_vector_array_get(&second, out_vec, 1));
        return .{ .first = first, .second = second };
    }

    fn linearNoBias(ctx: *anyopaque, request: *const LinearNoBiasRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        mq.timing_stats.calls += 1;
        return switch (request.quantized_storage.tensor_type) {
            .known => |known| switch (known) {
                .Q4_0 => try self.applyKernel(self.q4_0, request, request.in_dim / 32, .{ .known = .Q4_0 }),
                .Q4_1 => try self.applyKernel(self.q4_1, request, request.in_dim / 32, .{ .known = .Q4_1 }),
                .Q5_0 => try self.applyKernel(self.q5_0, request, request.in_dim / 32, .{ .known = .Q5_0 }),
                .Q5_1 => try self.applyKernel(self.q5_1, request, request.in_dim / 32, .{ .known = .Q5_1 }),
                .Q8_0 => try self.applyKernel(self.q8_0, request, request.in_dim / 32, .{ .known = .Q8_0 }),
                .Q8_1 => try self.applyKernel(self.q8_1, request, request.in_dim / 32, .{ .known = .Q8_1 }),
                .IQ4_NL => try self.applyKernel(self.iq4_nl, request, request.in_dim / 32, .{ .known = .IQ4_NL }),
                .Q2_K => try self.applyKernel(self.q2_k, request, request.in_dim / 256, .{ .known = .Q2_K }),
                .Q3_K => try self.applyKernel(self.q3_k, request, request.in_dim / 256, .{ .known = .Q3_K }),
                .Q4_K => try self.applyKernel(self.q4_k, request, request.in_dim / 256, .{ .known = .Q4_K }),
                .Q5_K => try self.applyKernel(self.q5_k, request, request.in_dim / 256, .{ .known = .Q5_K }),
                .Q6_K => try self.applyKernel(self.q6_k, request, request.in_dim / 256, .{ .known = .Q6_K }),
                .Q8_K => try self.applyKernel(self.q8_k, request, request.in_dim / 256, .{ .known = .Q8_K }),
                .I8_S => try self.applyKernel(self.i8_s, request, request.in_dim, .{ .known = .I8_S }),
                .IQ4_XS => try self.applyKernel(self.iq4_xs, request, request.in_dim / 256, .{ .known = .IQ4_XS }),
                .Q1_0 => try self.applyKernel(self.q1_0, request, request.in_dim / 128, .{ .known = .Q1_0 }),
                .I2_S => try self.applyKernel(self.i2_s, request, request.in_dim / 128, .{ .known = .I2_S }),
                .TL1 => try self.applyTL1Kernel(request),
                else => null,
            },
            .bitnet_tl2 => try self.applyTL2Kernel(request),
            .unknown => null,
        };
    }

    fn linearNoBiasPair(ctx: *anyopaque, request: *const LinearNoBiasPairRequest) !?LinearNoBiasPairResult {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        mq.timing_stats.pair_calls += 1;
        return switch (request.quantized_storage_a.tensor_type) {
            .known => |known| switch (known) {
                .I2_S => try self.applyPairKernel(self.i2_s_pair, request, request.in_dim / 128, .{ .known = .I2_S }),
                .Q4_K => try self.applyPairKernel(self.q4_k_pair, request, request.in_dim / 256, .{ .known = .Q4_K }),
                .Q5_K => try self.applyPairKernel(self.q5_k_pair, request, request.in_dim / 256, .{ .known = .Q5_K }),
                else => null,
            },
            .bitnet_tl2 => null,
            .unknown => null,
        };
    }

    fn mulMatId(ctx: *anyopaque, request: *const MoeLinearNoBiasRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        mq.timing_stats.grouped_calls += 1;
        if (request.quantized_storage.packed_expert == null) return null;
        return switch (request.quantized_storage.tensor_type) {
            .known => |known| switch (known) {
                .Q5_K => blk: {
                    mq.timing_stats.grouped_q5_k_calls += 1;
                    if (request.out_dim > request.in_dim) {
                        mq.timing_stats.grouped_q5_k_expand_calls += 1;
                    } else {
                        mq.timing_stats.grouped_q5_k_down_calls += 1;
                    }
                    if (request.expert_tile_ids != null and request.tile_row_starts != null and request.tile_row_counts != null) {
                        break :blk try self.applyGroupedTiledKernel(self.q5_k_grouped_tiled, request, request.in_dim / 256);
                    }
                    break :blk try self.applyGroupedKernel(self.q5_k_grouped, request, request.in_dim / 256, 176);
                },
                .Q4_0 => try self.applyGroupedKernel(self.q4_0_grouped, request, request.in_dim / 32, 18),
                .Q4_1 => try self.applyGroupedKernel(self.q4_1_grouped, request, request.in_dim / 32, 20),
                .Q5_0 => try self.applyGroupedKernel(self.q5_0_grouped, request, request.in_dim / 32, 22),
                .Q5_1 => try self.applyGroupedKernel(self.q5_1_grouped, request, request.in_dim / 32, 24),
                .Q4_K => try self.applyGroupedKernel(self.q4_k_grouped, request, request.in_dim / 256, 144),
                .Q8_0 => try self.applyGroupedKernel(self.q8_0_grouped, request, request.in_dim / 32, 34),
                .Q8_1 => try self.applyGroupedKernel(self.q8_1_grouped, request, request.in_dim / 32, 36),
                .IQ4_NL => try self.applyGroupedKernel(self.iq4_nl_grouped, request, request.in_dim / 32, 18),
                .IQ4_XS => try self.applyGroupedKernel(self.iq4_xs_grouped, request, request.in_dim / 256, 136),
                .Q6_K => blk: {
                    mq.timing_stats.grouped_q6_k_calls += 1;
                    if (request.out_dim > request.in_dim) {
                        mq.timing_stats.grouped_q6_k_expand_calls += 1;
                    } else {
                        mq.timing_stats.grouped_q6_k_down_calls += 1;
                    }
                    break :blk try self.applyGroupedKernel(self.q6_k_grouped, request, request.in_dim / 256, 210);
                },
                else => null,
            },
            .bitnet_tl2 => null,
            .unknown => null,
        };
    }

    fn moeLinearNoBias(ctx: *anyopaque, request: *const MoeLinearNoBiasRequest) !?c.mlx_array {
        return mulMatId(ctx, request);
    }

    fn moeLinearNoBiasPair(ctx: *anyopaque, request: *const MoeLinearNoBiasPairRequest) !?MoeLinearNoBiasPairResult {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.quantized_storage_a.packed_expert == null or request.quantized_storage_b.packed_expert == null) return null;
        return switch (request.quantized_storage_a.tensor_type) {
            .known => |known| switch (known) {
                .Q5_K => try self.applyGroupedPairKernel(self.q5_k_grouped_pair, request, request.in_dim / 256, .{ .known = .Q5_K }),
                else => null,
            },
            .bitnet_tl2 => null,
            .unknown => null,
        };
    }

    fn compressedKeyScoresRawMetal(self: *MetalProvider, request: *const CompressedKeyScoresRequest) !?c.mlx_array {
        if (!mq.enableRawMetalCompressedKeyScoresDebug()) return null;
        const raw_provider = self.raw_provider orelse return null;
        if (c.mlx_array_dtype(request.q) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_dtype(request.encoded_key) != c.MLX_UINT8) return null;
        if (c.mlx_array_ndim(request.q) != 2 or c.mlx_array_ndim(request.encoded_key) != 1) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.q, 0))) != request.q_len) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.q, 1))) != request.num_heads * request.head_dim) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.encoded_key, 0))) != request.block_tokens * request.key_row_bytes) return null;

        try mlx.evalArray(request.q);
        try mlx.evalArray(request.encoded_key);

        const q_ptr = c.mlx_array_data_float32(request.q);
        const encoded_ptr = c.mlx_array_data_uint8(request.encoded_key);
        if (q_ptr == null or encoded_ptr == null) return error.MlxDataNull;

        const output_len = request.num_heads * request.q_len * request.block_tokens;
        const output = try std.heap.c_allocator.alloc(f32, output_len);
        errdefer std.heap.c_allocator.free(output);

        const rc = switch (request.format) {
            .polar4 => termite_metal_provider_compressed_key_scores_polar4(
                raw_provider,
                q_ptr,
                request.q_len,
                encoded_ptr,
                request.block_tokens,
                request.num_heads,
                request.num_kv_heads,
                request.head_dim,
                request.key_row_bytes,
                output.ptr,
            ),
            .turbo3 => termite_metal_provider_compressed_key_scores_turbo3(
                raw_provider,
                q_ptr,
                request.q_len,
                encoded_ptr,
                request.block_tokens,
                request.num_heads,
                request.num_kv_heads,
                request.head_dim,
                request.key_row_bytes,
                output.ptr,
            ),
        };
        if (rc != 0) return null;

        const output_shape = [_]i32{ 1, @intCast(request.num_heads), @intCast(request.q_len), @intCast(request.block_tokens) };
        return mlx.arrayFromOwnedFloat32(output, &output_shape);
    }

    pub fn hasDecoderRuntime(self: *const MetalProvider) bool {
        const runtime = self.raw_decode_runtime orelse return false;
        return termite_metal_decode_runtime_ready(runtime) != 0;
    }

    pub fn reserveDecoderRuntime(self: *MetalProvider, scratch_bytes: usize, token_bytes: usize) !bool {
        const runtime = self.raw_decode_runtime orelse return false;
        return termite_metal_decode_runtime_reserve(runtime, scratch_bytes, token_bytes) == 0;
    }

    fn decoderRuntimePrepareGreedy(ctx: *anyopaque, request: *const ops.DecoderRuntimeGreedyRequest) !bool {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (self.raw_decode_runtime == null) return false;
        return metal_runtime.decoderRuntimePrepareGreedy(self, request.*, &mq.timing_stats);
    }

    fn decoderRuntimeResetState(ctx: *anyopaque) !void {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        metal_runtime.decoderRuntimeResetState(self);
    }

    fn decoderRuntimePrepareAbsoluteEmbeddings(ctx: *anyopaque, request: *const DecoderRuntimePrepareAbsoluteEmbeddingsRequest) !bool {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        const started_at = mq.monotonicNowNs();
        defer mq.timing_stats.decoder_runtime_prepare_embed_nanos += @intCast(mq.monotonicNowNs() - started_at);
        var token_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        var position_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const token_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.token_embedding, &token_shape_buf);
        const position_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.position_embedding, &position_shape_buf);
        return metal_runtime.decoderRuntimePrepareAbsoluteEmbeddings(self, .{
            .token_embedding = token_tensor,
            .position_embedding = position_tensor,
            .vocab_size = request.vocab_size,
            .max_position_embeddings = request.max_position_embeddings,
            .hidden_size = request.hidden_size,
        });
    }

    fn decoderRuntimeEmbedAbsolutePosition(ctx: *anyopaque, request: *const DecoderRuntimeEmbedAbsolutePositionRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        const tensor = (try metal_runtime.decoderRuntimeEmbedAbsolutePosition(self, request.*, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimePrepareLayerNorm(ctx: *anyopaque, request: *const DecoderRuntimePrepareLayerNormRequest) !bool {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        const started_at = mq.monotonicNowNs();
        defer mq.timing_stats.decoder_runtime_prepare_layer_norm_nanos += @intCast(mq.monotonicNowNs() - started_at);
        mq.timing_stats.decoder_runtime_prepare_layer_norm_calls += 1;
        var weight_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        var bias_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const weight_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.weight, &weight_shape_buf);
        const bias_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.bias, &bias_shape_buf);
        return metal_runtime.decoderRuntimePrepareLayerNorm(self, .{
            .weight = weight_tensor,
            .bias = bias_tensor,
            .slot = request.slot,
            .hidden_size = request.hidden_size,
        });
    }

    fn decoderRuntimeApplyLayerNorm(ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyLayerNorm(self, .{
            .input = input_tensor,
            .slot = request.slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimePrepareRmsNorm(ctx: *anyopaque, request: *const DecoderRuntimePrepareRmsNormRequest) !bool {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        const started_at = mq.monotonicNowNs();
        defer mq.timing_stats.decoder_runtime_prepare_layer_norm_nanos += @intCast(mq.monotonicNowNs() - started_at);
        mq.timing_stats.decoder_runtime_prepare_layer_norm_calls += 1;
        var weight_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const weight_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.weight, &weight_shape_buf);
        return metal_runtime.decoderRuntimePrepareRmsNorm(self, .{
            .weight = weight_tensor,
            .slot = request.slot,
            .hidden_size = request.hidden_size,
        });
    }

    fn decoderRuntimeApplyRmsNorm(ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyRmsNorm(self, .{
            .input = input_tensor,
            .slot = request.slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        }, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimeApplyLayerNormLinearArgmax(ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearArgmaxRequest) !?usize {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        return metal_runtime.decoderRuntimeApplyLayerNormLinearArgmax(self, .{
            .input = input_tensor,
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        });
    }

    fn decoderRuntimeApplyLayerNormLinear(ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyLayerNormLinear(self, .{
            .input = input_tensor,
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        })) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimeApplyLayerNormLinearSample(ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearSampleRequest) !?usize {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        return metal_runtime.decoderRuntimeApplyLayerNormLinearSample(self, .{
            .input = input_tensor,
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
            .temperature = request.temperature,
            .top_k = request.top_k,
            .top_p = request.top_p,
            .min_p = request.min_p,
            .repetition_penalty = request.repetition_penalty,
            .frequency_penalty = request.frequency_penalty,
            .presence_penalty = request.presence_penalty,
            .token_history = request.token_history,
        });
    }

    fn decoderRuntimeApplyRmsNormLinearArgmax(ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearArgmaxRequest) !?usize {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        return metal_runtime.decoderRuntimeApplyRmsNormLinearArgmax(self, .{
            .input = input_tensor,
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        });
    }

    fn decoderRuntimeApplyRmsNormLinear(ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyRmsNormLinear(self, .{
            .input = input_tensor,
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        })) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimeApplyRmsNormLinearSample(ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearSampleRequest) !?usize {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        return metal_runtime.decoderRuntimeApplyRmsNormLinearSample(self, .{
            .input = input_tensor,
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
            .temperature = request.temperature,
            .top_k = request.top_k,
            .top_p = request.top_p,
            .min_p = request.min_p,
            .repetition_penalty = request.repetition_penalty,
            .frequency_penalty = request.frequency_penalty,
            .presence_penalty = request.presence_penalty,
            .token_history = request.token_history,
        });
    }

    fn sampleLogitsDevice(ctx: *anyopaque, request: *const SampleLogitsDeviceRequest) !?usize {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        return metal_runtime.sampleLogitsDevice(self, .{
            .input = input_tensor,
            .out_dim = request.out_dim,
            .temperature = request.temperature,
            .top_k = request.top_k,
            .top_p = request.top_p,
            .min_p = request.min_p,
            .repetition_penalty = request.repetition_penalty,
            .frequency_penalty = request.frequency_penalty,
            .presence_penalty = request.presence_penalty,
            .token_history = request.token_history,
        });
    }

    fn dupQuantizedStorage(storage: *const QuantizedStorage) !*QuantizedStorage {
        return metal_runtime.dupQuantizedStorage(storage);
    }

    fn clearRawLinearSlot(self: *MetalProvider, slot: usize) void {
        metal_runtime.clearRawLinearSlot(self, slot);
    }

    fn makeQuantizedWeightArray(storage: *const QuantizedStorage) !c.mlx_array {
        return metal_runtime.makeQuantizedWeightArray(storage);
    }

    fn decoderRuntimePrepareLinear(ctx: *anyopaque, request: *const DecoderRuntimePrepareLinearRequest) !bool {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        const started_at = mq.monotonicNowNs();
        defer mq.timing_stats.decoder_runtime_prepare_linear_nanos += @intCast(mq.monotonicNowNs() - started_at);
        if (mq.timing_stats.decoder_runtime_prepare_linear_first_provider_ptr == 0) {
            mq.timing_stats.decoder_runtime_prepare_linear_first_provider_ptr = @intFromPtr(self);
        }

        // Validate weight/bias are f32 before bridging (quantized path ignores
        // the MLX weight/bias and uses request.quantized_storage instead).
        if (request.quantized_storage == null) {
            if (c.mlx_array_dtype(request.weight) != c.MLX_FLOAT32 or
                c.mlx_array_dtype(request.bias) != c.MLX_FLOAT32)
            {
                mq.timing_stats.decoder_runtime_prepare_linear_weight_dtype_failures += 1;
                return false;
            }
        }
        if (c.mlx_array_dtype(request.bias) != c.MLX_FLOAT32) {
            mq.timing_stats.decoder_runtime_prepare_linear_bias_shape_failures += 1;
            return false;
        }

        var weight_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        var bias_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const weight_tensor = if (request.quantized_storage == null)
            try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.weight, &weight_shape_buf)
        else
            metal_tensor.MetalTensor.borrowed(@ptrFromInt(@alignOf(f32)), 0, &.{});
        const bias_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.bias, &bias_shape_buf);

        const ok = try metal_runtime.decoderRuntimePrepareLinear(self, .{
            .weight = weight_tensor,
            .bias = bias_tensor,
            .quantized_storage = request.quantized_storage,
            .slot = request.slot,
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        }, &mq.timing_stats);
        if (!ok) return false;

        // Dense fallback cache: retain provider-owned float32 tensors so the
        // multi-row path in decoderRuntimeApplyLinear can still invoke mlx_addmm
        // when Metal's direct runtime path doesn't apply.
        if (request.retain_dense_fallback and request.quantized_storage == null) {
            self.raw_linear_slot_dense_weights[request.slot] = try self.cloneFloat32Tensor(request.weight);
            errdefer {
                if (self.raw_linear_slot_dense_weights[request.slot]) |*tensor| {
                    tensor.deinit();
                    self.raw_linear_slot_dense_weights[request.slot] = null;
                }
            }
            self.raw_linear_slot_dense_biases[request.slot] = try self.cloneFloat32Tensor(request.bias);
            errdefer {
                if (self.raw_linear_slot_dense_biases[request.slot]) |*tensor| {
                    tensor.deinit();
                    self.raw_linear_slot_dense_biases[request.slot] = null;
                }
            }
        }
        return true;
    }

    fn decoderRuntimeApplyLinear(ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (mq.timing_stats.decoder_runtime_apply_linear_first_provider_ptr == 0) {
            mq.timing_stats.decoder_runtime_apply_linear_first_provider_ptr = @intFromPtr(self);
        }
        if (request.in_dim == 0 or request.out_dim == 0 or request.slot >= decoder_runtime_linear_slot_capacity) return null;
        if (!self.raw_linear_slots_prepared[request.slot]) {
            mq.timing_stats.decoder_runtime_apply_linear_not_prepared += 1;
            if (mq.timing_stats.decoder_runtime_apply_linear_first_not_prepared_slot == std.math.maxInt(usize)) {
                mq.timing_stats.decoder_runtime_apply_linear_first_not_prepared_slot = request.slot;
            }
            return null;
        }
        if (self.raw_linear_slot_in_dims[request.slot] != request.in_dim or self.raw_linear_slot_out_dims[request.slot] != request.out_dim) {
            mq.timing_stats.decoder_runtime_apply_linear_dim_mismatch += 1;
            return null;
        }
        if (c.mlx_array_dtype(request.input) != c.MLX_FLOAT32 or c.mlx_array_ndim(request.input) != 2) {
            mq.timing_stats.decoder_runtime_apply_linear_input_shape_failures += 1;
            return null;
        }
        const rows = @as(usize, @intCast(c.mlx_array_dim(request.input, 0)));
        if (rows == 0 or @as(usize, @intCast(c.mlx_array_dim(request.input, 1))) != request.in_dim) {
            mq.timing_stats.decoder_runtime_apply_linear_input_shape_failures += 1;
            return null;
        }

        if (self.raw_linear_slot_kinds[request.slot] == .quantized) {
            const storage = self.raw_linear_slot_quantized_storage[request.slot] orelse {
                mq.timing_stats.decoder_runtime_apply_linear_quantized_storage_nulls += 1;
                return null;
            };
            if (try self.tryApplyQuantizedRuntimeLinear(
                request.slot,
                request.input,
                rows,
                request.in_dim,
                request.out_dim,
            )) |output_arr| {
                return output_arr;
            }
            const weight = makeQuantizedWeightArray(storage) catch {
                mq.timing_stats.decoder_runtime_apply_linear_quantized_weight_nulls += 1;
                return null;
            };
            defer _ = c.mlx_array_free(weight);
            mq.timing_stats.decoder_runtime_apply_linear_calls += 1;
            return linearNoBias(ctx, &.{
                .input = request.input,
                .weight = weight,
                .quantized_storage = storage,
                .prepared_weight_bytes = storage.preparedBytes(.row_major_blocks),
                .rows = rows,
                .in_dim = request.in_dim,
                .out_dim = request.out_dim,
                .stream = mlx.gpuStream(),
            });
        }

        if (rows != 1) {
            try mlx.evalArray(request.input);
            var dense_multi_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            const dense_multi_input_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, dense_multi_shape_buf[0..]);
            if (try metal_runtime.tryApplyDenseRuntimeLinear(
                self,
                request.slot,
                dense_multi_input_mt,
                rows,
                request.in_dim,
                request.out_dim,
            )) |result_mt| {
                mq.timing_stats.decoder_runtime_apply_linear_calls += 1;
                return mlx_metal_bridge.adoptMetalTensorAsMlxArray(result_mt);
            }

            // Metal path unavailable — fall back to mlx_addmm using retained
            // dense weight copies (only present when retain_dense_fallback=true).
            const weight = self.raw_linear_slot_dense_weights[request.slot] orelse {
                mq.timing_stats.decoder_runtime_apply_linear_dense_cache_nulls += 1;
                return null;
            };
            const bias = self.raw_linear_slot_dense_biases[request.slot] orelse {
                mq.timing_stats.decoder_runtime_apply_linear_dense_cache_nulls += 1;
                return null;
            };
            const weight_arr = mlx_metal_bridge.borrowMetalTensorAsMlxArray(weight);
            defer _ = c.mlx_array_free(weight_arr);
            const bias_arr = mlx_metal_bridge.borrowMetalTensorAsMlxArray(bias);
            defer _ = c.mlx_array_free(bias_arr);
            const stream = mlx.gpuStream();
            const weight_dim0: usize = @intCast(c.mlx_array_dim(weight_arr, 0));
            const weight_dim1: usize = @intCast(c.mlx_array_dim(weight_arr, 1));
            var rhs_owned = false;
            const rhs = if (weight_dim0 == request.out_dim and weight_dim1 == request.in_dim) blk: {
                var w_t = c.mlx_array_new();
                errdefer _ = c.mlx_array_free(w_t);
                try mlx.check(c.mlx_transpose(&w_t, weight_arr, stream));
                rhs_owned = true;
                break :blk w_t;
            } else if (weight_dim0 == request.in_dim and weight_dim1 == request.out_dim)
                weight_arr
            else
                return null;
            defer if (rhs_owned) {
                _ = c.mlx_array_free(rhs);
            };

            var result = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(result);
            mq.timing_stats.decoder_runtime_apply_linear_calls += 1;
            try mlx.check(c.mlx_addmm(&result, bias_arr, request.input, rhs, 1.0, 1.0, stream));
            return result;
        }

        const runtime = self.raw_decode_runtime orelse return null;
        if (termite_metal_decode_runtime_ready(runtime) == 0) return null;

        try mlx.evalArray(request.input);
        const input_base = c.mlx_array_data_float32(request.input);
        if (input_base == null) return null;

        const output = try std.heap.c_allocator.alloc(f32, request.out_dim);
        errdefer std.heap.c_allocator.free(output);
        mq.timing_stats.decoder_runtime_apply_linear_calls += 1;
        const rc = termite_metal_decode_runtime_apply_linear(
            runtime,
            request.slot,
            input_base.?,
            request.in_dim,
            request.out_dim,
            output.ptr,
        );
        if (rc != 0) return null;
        const shape = [_]i32{ 1, @intCast(request.out_dim) };
        return mlx.arrayFromOwnedFloat32(output, &shape);
    }

    fn decoderRuntimeApplyLinearArgmax(ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearArgmaxRequest) !?usize {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.in_dim == 0 or request.out_dim == 0 or request.slot >= decoder_runtime_linear_slot_capacity) return null;
        if (!self.raw_linear_slots_prepared[request.slot]) return null;
        if (self.raw_linear_slot_in_dims[request.slot] != request.in_dim) return null;
        if (self.raw_linear_slot_out_dims[request.slot] != request.out_dim) return null;
        if (self.raw_linear_slot_kinds[request.slot] != .dense) return null;
        if (c.mlx_array_dtype(request.input) != c.MLX_FLOAT32 or c.mlx_array_ndim(request.input) != 2) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.input, 0))) != 1) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.input, 1))) != request.in_dim) return null;

        const runtime = self.raw_decode_runtime orelse return null;
        if (termite_metal_decode_runtime_ready(runtime) == 0) return null;

        try mlx.evalArray(request.input);
        const input_base = c.mlx_array_data_float32(request.input);
        if (input_base == null) return null;

        var token_id: u32 = 0;
        const rc = termite_metal_decode_runtime_apply_linear_argmax(
            runtime,
            request.slot,
            input_base.?,
            request.in_dim,
            request.out_dim,
            &token_id,
        );
        if (rc != 0) return null;
        return token_id;
    }

    fn decoderRuntimeApplyLinearPair(ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearPairRequest) !?LinearNoBiasPairResult {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.in_dim == 0 or request.out_dim == 0) return null;
        if (request.slot_a >= decoder_runtime_linear_slot_capacity or request.slot_b >= decoder_runtime_linear_slot_capacity) return null;
        if (request.slot_a == request.slot_b) return null;
        if (!self.raw_linear_slots_prepared[request.slot_a] or !self.raw_linear_slots_prepared[request.slot_b]) {
            mq.timing_stats.decoder_runtime_apply_linear_pair_not_prepared += 1;
            if (mq.timing_stats.decoder_runtime_apply_linear_pair_first_not_prepared_slot_a == std.math.maxInt(usize)) {
                mq.timing_stats.decoder_runtime_apply_linear_pair_first_not_prepared_slot_a = request.slot_a;
                mq.timing_stats.decoder_runtime_apply_linear_pair_first_not_prepared_slot_b = request.slot_b;
            }
            return null;
        }
        if (self.raw_linear_slot_in_dims[request.slot_a] != request.in_dim or self.raw_linear_slot_in_dims[request.slot_b] != request.in_dim or
            self.raw_linear_slot_out_dims[request.slot_a] != request.out_dim or self.raw_linear_slot_out_dims[request.slot_b] != request.out_dim)
        {
            mq.timing_stats.decoder_runtime_apply_linear_pair_dim_mismatch += 1;
            return null;
        }
        if (c.mlx_array_dtype(request.input) != c.MLX_FLOAT32 or c.mlx_array_ndim(request.input) != 2) {
            mq.timing_stats.decoder_runtime_apply_linear_pair_input_shape_failures += 1;
            return null;
        }
        const rows = @as(usize, @intCast(c.mlx_array_dim(request.input, 0)));
        if (rows == 0 or @as(usize, @intCast(c.mlx_array_dim(request.input, 1))) != request.in_dim) {
            mq.timing_stats.decoder_runtime_apply_linear_pair_input_shape_failures += 1;
            return null;
        }

        if (self.raw_linear_slot_kinds[request.slot_a] == .quantized and self.raw_linear_slot_kinds[request.slot_b] == .quantized) {
            const storage_a = self.raw_linear_slot_quantized_storage[request.slot_a] orelse return null;
            const storage_b = self.raw_linear_slot_quantized_storage[request.slot_b] orelse return null;
            const kind_a = quantizedRuntimeLinearKind(storage_a);
            const kind_b = quantizedRuntimeLinearKind(storage_b);
            if (kind_a == .none or kind_a != kind_b) {
                mq.timing_stats.decoder_runtime_pair_non_i2s += 1;
            }
            if (kind_a != .none and kind_a == kind_b) {
                if (try self.tryApplyQuantizedRuntimeLinearPair(
                    request.slot_a,
                    request.slot_b,
                    request.input,
                    rows,
                    request.in_dim,
                    request.out_dim,
                )) |pair_result| {
                    return pair_result;
                }
                mq.timing_stats.decoder_runtime_pair_direct_failures += 1;
            }
            const weight_a = makeQuantizedWeightArray(storage_a) catch return null;
            defer _ = c.mlx_array_free(weight_a);
            const weight_b = makeQuantizedWeightArray(storage_b) catch return null;
            defer _ = c.mlx_array_free(weight_b);
            const pair_plan = planLinearNoBiasPair(ctx, &.{
                .prepared_weight_a = .{
                    .weight = weight_a,
                    .quantized_storage = storage_a,
                    .staged_backend_dense = false,
                    .has_lazy_owner = false,
                    .packed_expert = false,
                },
                .prepared_weight_b = .{
                    .weight = weight_b,
                    .quantized_storage = storage_b,
                    .staged_backend_dense = false,
                    .has_lazy_owner = false,
                    .packed_expert = false,
                },
                .rows = rows,
                .in_dim = request.in_dim,
                .out_dim = request.out_dim,
            });
            if (pair_plan == .device_native) {
                mq.timing_stats.decoder_runtime_pair_backend_fallbacks += 1;
                if (try linearNoBiasPair(ctx, &.{
                    .input = request.input,
                    .weight_a = weight_a,
                    .weight_b = weight_b,
                    .quantized_storage_a = storage_a,
                    .quantized_storage_b = storage_b,
                    .prepared_weight_bytes_a = storage_a.preparedBytes(.row_major_blocks),
                    .prepared_weight_bytes_b = storage_b.preparedBytes(.row_major_blocks),
                    .rows = rows,
                    .in_dim = request.in_dim,
                    .out_dim = request.out_dim,
                    .stream = mlx.gpuStream(),
                })) |pair_result| {
                    return pair_result;
                }
            }
        }
        if (self.raw_linear_slot_kinds[request.slot_a] == .dense and self.raw_linear_slot_kinds[request.slot_b] == .dense) {
            try mlx.evalArray(request.input);
            var dense_pair_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            const dense_pair_input_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, dense_pair_shape_buf[0..]);
            if (try metal_runtime.tryApplyDenseRuntimeLinearPair(
                self,
                request.slot_a,
                request.slot_b,
                dense_pair_input_mt,
                rows,
                request.in_dim,
                request.out_dim,
            )) |pair_result| {
                mq.timing_stats.decoder_runtime_apply_linear_pair_dense_direct_successes += 1;
                return .{
                    .first = mlx_metal_bridge.adoptMetalTensorAsMlxArray(pair_result.first),
                    .second = mlx_metal_bridge.adoptMetalTensorAsMlxArray(pair_result.second),
                };
            }
            mq.timing_stats.decoder_runtime_apply_linear_pair_dense_direct_failures += 1;
        }

        const first = (try decoderRuntimeApplyLinear(ctx, &.{
            .input = request.input,
            .slot = request.slot_a,
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        })) orelse {
            mq.timing_stats.decoder_runtime_apply_linear_pair_dense_delegate_failures += 1;
            return null;
        };
        errdefer _ = c.mlx_array_free(first);
        const second = (try decoderRuntimeApplyLinear(ctx, &.{
            .input = request.input,
            .slot = request.slot_b,
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        })) orelse {
            mq.timing_stats.decoder_runtime_apply_linear_pair_dense_delegate_failures += 1;
            _ = c.mlx_array_free(first);
            return null;
        };
        return .{ .first = first, .second = second };
    }

    fn decoderRuntimeApplyLinearQkv(ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearQkvRequest) !?LinearNoBiasTripleResult {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.in_dim == 0 or request.q_out_dim == 0 or request.kv_out_dim == 0) return null;
        if (request.q_slot >= decoder_runtime_linear_slot_capacity or request.k_slot >= decoder_runtime_linear_slot_capacity or request.v_slot >= decoder_runtime_linear_slot_capacity) return null;
        if (request.q_slot == request.k_slot or request.q_slot == request.v_slot or request.k_slot == request.v_slot) return null;
        if (!self.raw_linear_slots_prepared[request.q_slot] or !self.raw_linear_slots_prepared[request.k_slot] or !self.raw_linear_slots_prepared[request.v_slot]) {
            return null;
        }
        if (self.raw_linear_slot_in_dims[request.q_slot] != request.in_dim or
            self.raw_linear_slot_in_dims[request.k_slot] != request.in_dim or
            self.raw_linear_slot_in_dims[request.v_slot] != request.in_dim or
            self.raw_linear_slot_out_dims[request.q_slot] != request.q_out_dim or
            self.raw_linear_slot_out_dims[request.k_slot] != request.kv_out_dim or
            self.raw_linear_slot_out_dims[request.v_slot] != request.kv_out_dim)
        {
            return null;
        }
        if (c.mlx_array_dtype(request.input) != c.MLX_FLOAT32 or c.mlx_array_ndim(request.input) != 2) return null;
        const rows = @as(usize, @intCast(c.mlx_array_dim(request.input, 0)));
        if (rows == 0 or @as(usize, @intCast(c.mlx_array_dim(request.input, 1))) != request.in_dim) return null;

        if (self.raw_linear_slot_kinds[request.q_slot] == .quantized and
            self.raw_linear_slot_kinds[request.k_slot] == .quantized and
            self.raw_linear_slot_kinds[request.v_slot] == .quantized)
        {
            if (try self.tryApplyQuantizedRuntimeLinearQkv(
                request.q_slot,
                request.k_slot,
                request.v_slot,
                request.input,
                rows,
                request.in_dim,
                request.q_out_dim,
                request.kv_out_dim,
            )) |result| {
                return result;
            }
        }

        const q = (try decoderRuntimeApplyLinear(ctx, &.{
            .input = request.input,
            .slot = request.q_slot,
            .in_dim = request.in_dim,
            .out_dim = request.q_out_dim,
        })) orelse return null;
        errdefer _ = c.mlx_array_free(q);
        const kv = (try decoderRuntimeApplyLinearPair(ctx, &.{
            .input = request.input,
            .slot_a = request.k_slot,
            .slot_b = request.v_slot,
            .in_dim = request.in_dim,
            .out_dim = request.kv_out_dim,
        })) orelse {
            _ = c.mlx_array_free(q);
            return null;
        };
        return .{
            .first = q,
            .second = kv.first,
            .third = kv.second,
        };
    }

    fn decoderRuntimeApplyActivation(ctx: *anyopaque, request: *const DecoderRuntimeApplyActivationRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.input, &input_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyActivation(self, .{
            .input = input_tensor,
            .kind = request.kind,
            .dim = request.dim,
        }, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimeApplyAdd(ctx: *anyopaque, request: *const DecoderRuntimeApplyAddRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var lhs_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        var rhs_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const lhs_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.lhs, &lhs_shape_buf);
        const rhs_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.rhs, &rhs_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyAdd(self, .{
            .lhs = lhs_tensor,
            .rhs = rhs_tensor,
            .dim = request.dim,
        }, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn decoderRuntimeApplyMultiply(
        ctx: *anyopaque,
        lhs: c.mlx_array,
        rhs: c.mlx_array,
        dim: usize,
    ) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var lhs_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        var rhs_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const lhs_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(lhs, &lhs_shape_buf);
        const rhs_tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(rhs, &rhs_shape_buf);
        const tensor = (try metal_runtime.decoderRuntimeApplyMultiply(self, lhs_tensor, rhs_tensor, dim)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(tensor);
    }

    fn runDenseFfnResidual(
        ctx: *anyopaque,
        request: *const RunDenseFfnResidualRequest,
    ) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.hidden_size == 0 or request.intermediate_size == 0) return null;
        if (request.first_linear_slot >= decoder_runtime_linear_slot_capacity or request.second_linear_slot >= decoder_runtime_linear_slot_capacity) return null;
        if (!self.raw_linear_slots_prepared[request.first_linear_slot]) return null;
        if (!self.raw_linear_slots_prepared[request.second_linear_slot]) return null;
        if (self.raw_linear_slot_in_dims[request.first_linear_slot] != request.hidden_size or self.raw_linear_slot_out_dims[request.first_linear_slot] != request.intermediate_size) return null;
        if (self.raw_linear_slot_in_dims[request.second_linear_slot] != request.intermediate_size or self.raw_linear_slot_out_dims[request.second_linear_slot] != request.hidden_size) return null;

        if (self.raw_linear_slot_kinds[request.first_linear_slot] == .quantized and self.raw_linear_slot_kinds[request.second_linear_slot] == .quantized) {
            const first = (try decoderRuntimeApplyLinear(ctx, &.{
                .slot = request.first_linear_slot,
                .input = request.input,
                .in_dim = request.hidden_size,
                .out_dim = request.intermediate_size,
            })) orelse return null;
            defer _ = c.mlx_array_free(first);
            const activated = (try decoderRuntimeApplyActivation(ctx, &.{
                .input = first,
                .kind = request.activation,
                .dim = request.intermediate_size,
            })) orelse return null;
            defer _ = c.mlx_array_free(activated);
            const projected = (try decoderRuntimeApplyLinear(ctx, &.{
                .slot = request.second_linear_slot,
                .input = activated,
                .in_dim = request.intermediate_size,
                .out_dim = request.hidden_size,
            })) orelse return null;
            errdefer _ = c.mlx_array_free(projected);
            var result = c.mlx_array_new();
            try mlx.check(c.mlx_add(&result, projected, request.residual, mlx.gpuStream()));
            _ = c.mlx_array_free(projected);
            return result;
        }

        const runtime = self.raw_decode_runtime orelse return null;
        if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
        if (self.raw_linear_slot_kinds[request.first_linear_slot] != .dense or self.raw_linear_slot_kinds[request.second_linear_slot] != .dense) return null;
        if (c.mlx_array_dtype(request.input) != c.MLX_FLOAT32 or c.mlx_array_dtype(request.residual) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_ndim(request.input) != 2 or c.mlx_array_ndim(request.residual) != 2) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.input, 0))) != 1 or @as(usize, @intCast(c.mlx_array_dim(request.residual, 0))) != 1) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.input, 1))) != request.hidden_size or @as(usize, @intCast(c.mlx_array_dim(request.residual, 1))) != request.hidden_size) return null;

        try mlx.evalArray(request.input);
        try mlx.evalArray(request.residual);
        const input_base = c.mlx_array_data_float32(request.input);
        const residual_base = c.mlx_array_data_float32(request.residual);
        if (input_base == null or residual_base == null) return null;

        const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
        errdefer std.heap.c_allocator.free(output);
        const rc = termite_metal_decode_runtime_apply_linear_activation_linear_residual(
            runtime,
            request.first_linear_slot,
            request.second_linear_slot,
            input_base.?,
            residual_base.?,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
            output.ptr,
        );
        if (rc != 0) return null;
        const shape = [_]i32{ 1, @intCast(request.hidden_size) };
        return mlx.arrayFromOwnedFloat32(output, &shape);
    }

    fn tryRawProviderQuantizedLinearHost(
        self: *MetalProvider,
        storage: *const QuantizedStorage,
        input: [*c]const f32,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        output: [*c]f32,
    ) !bool {
        return metal_runtime.tryRawProviderQuantizedLinearHost(self, storage, input, rows, in_dim, out_dim, output);
    }

    fn tryRawLinearHost(
        self: *MetalProvider,
        slot: usize,
        input: [*c]const f32,
        in_dim: usize,
        out_dim: usize,
        output: [*c]f32,
    ) !bool {
        return metal_runtime.tryRawLinearHost(self, slot, input, in_dim, out_dim, output);
    }

    fn tryRawAttentionResidualHost(
        self: *MetalProvider,
        attention_input: [*c]const f32,
        residual: [*c]const f32,
        attention_input_size: usize,
        hidden_size: usize,
        eps: f32,
        attention_linear_slot: usize,
        attention_pre_linear_rms_norm_slot: ?usize,
        attention_post_linear_rms_norm_slot: ?usize,
        output: [*c]f32,
    ) !bool {
        return metal_runtime.tryRawAttentionResidualHost(
            self,
            attention_input,
            residual,
            attention_input_size,
            hidden_size,
            eps,
            attention_linear_slot,
            attention_pre_linear_rms_norm_slot,
            attention_post_linear_rms_norm_slot,
            output,
        );
    }

    fn tryRawCompressedAttentionResidualHost(
        self: *MetalProvider,
        format: CompressedKeyFormat,
        q: [*c]const f32,
        encoded_key: [*c]const u8,
        v: [*c]const f32,
        kv_tokens: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        key_row_bytes: usize,
        v_row_stride: usize,
        base_key_row_bytes: usize,
        query_position: usize,
        kv_position_offset: usize,
        sliding_window: usize,
        residual: [*c]const f32,
        attention_input_size: usize,
        hidden_size: usize,
        eps: f32,
        attention_linear_slot: usize,
        attention_pre_linear_rms_norm_slot: ?usize,
        attention_post_linear_rms_norm_slot: ?usize,
        output: [*c]f32,
    ) !bool {
        return metal_runtime.tryRawCompressedAttentionResidualHost(
            self,
            format,
            q,
            encoded_key,
            v,
            kv_tokens,
            num_heads,
            num_kv_heads,
            head_dim,
            key_row_bytes,
            v_row_stride,
            base_key_row_bytes,
            query_position,
            kv_position_offset,
            sliding_window,
            residual,
            attention_input_size,
            hidden_size,
            eps,
            attention_linear_slot,
            attention_pre_linear_rms_norm_slot,
            attention_post_linear_rms_norm_slot,
            output,
        );
    }

    fn tryRawQuantizedGatedFfnResidualHost(
        self: *MetalProvider,
        input: [*c]const f32,
        residual: [*c]const f32,
        rows: usize,
        hidden_size: usize,
        intermediate_size: usize,
        activation: ops.DecoderRuntimeActivationKind,
        gate_linear_slot: usize,
        up_linear_slot: usize,
        down_linear_slot: usize,
        post_gate_rms_norm_slot: ?usize,
        post_down_rms_norm_slot: ?usize,
        output: [*c]f32,
    ) !bool {
        return metal_runtime.tryRawQuantizedGatedFfnResidualHost(
            self,
            .{
                .input = input,
                .residual = residual,
                .rows = rows,
                .hidden_size = hidden_size,
                .intermediate_size = intermediate_size,
                .activation = activation,
                .gate_linear_slot = gate_linear_slot,
                .up_linear_slot = up_linear_slot,
                .down_linear_slot = down_linear_slot,
                .post_gate_rms_norm_slot = post_gate_rms_norm_slot,
                .post_down_rms_norm_slot = post_down_rms_norm_slot,
                .output = output,
            },
            &mq.timing_stats,
            &mq.logged_quantized_gated_ffn_unsupported_type,
        );
    }

    fn ensureI2SRuntimeLinearSlotPrepared(
        self: *MetalProvider,
        slot: usize,
        in_dim: usize,
        out_dim: usize,
    ) bool {
        return metal_runtime.ensureI2SRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
    }

    fn ensureQ4KRuntimeLinearSlotPrepared(
        self: *MetalProvider,
        slot: usize,
        in_dim: usize,
        out_dim: usize,
    ) bool {
        return metal_runtime.ensureQ4KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
    }

    fn ensureQ5KRuntimeLinearSlotPrepared(
        self: *MetalProvider,
        slot: usize,
        in_dim: usize,
        out_dim: usize,
    ) bool {
        return metal_runtime.ensureQ5KRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
    }

    fn quantizedRuntimeLinearKind(storage: *const QuantizedStorage) RawQuantizedRuntimeLinearKind {
        return metal_runtime.quantizedRuntimeLinearKind(storage);
    }

    fn ensureQuantizedRuntimeLinearSlotPrepared(
        self: *MetalProvider,
        slot: usize,
        in_dim: usize,
        out_dim: usize,
    ) RawQuantizedRuntimeLinearKind {
        return metal_runtime.ensureQuantizedRuntimeLinearSlotPrepared(self, slot, in_dim, out_dim);
    }

    fn tryApplyQuantizedRuntimeLinear(
        self: *MetalProvider,
        slot: usize,
        input: c.mlx_array,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?c.mlx_array {
        mq.timing_stats.decoder_runtime_apply_linear_calls += 1;
        try mlx.evalArray(input);
        var shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(input, shape_buf[0..]);
        const result_mt = (try metal_runtime.tryApplyQuantizedRuntimeLinear(self, slot, input_mt, rows, in_dim, out_dim)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(result_mt);
    }

    fn tryApplyQuantizedRuntimeLinearPair(
        self: *MetalProvider,
        slot_a: usize,
        slot_b: usize,
        input: c.mlx_array,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?LinearNoBiasPairResult {
        try mlx.evalArray(input);
        var shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(input, shape_buf[0..]);
        const pair_result = (try metal_runtime.tryApplyQuantizedRuntimeLinearPair(
            self,
            slot_a,
            slot_b,
            input_mt,
            rows,
            in_dim,
            out_dim,
        )) orelse return null;
        mq.timing_stats.decoder_runtime_pair_direct_successes += 1;
        return .{
            .first = mlx_metal_bridge.adoptMetalTensorAsMlxArray(pair_result.first),
            .second = mlx_metal_bridge.adoptMetalTensorAsMlxArray(pair_result.second),
        };
    }

    fn tryApplyQuantizedRuntimeLinearQkv(
        self: *MetalProvider,
        q_slot: usize,
        k_slot: usize,
        v_slot: usize,
        input: c.mlx_array,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) !?LinearNoBiasTripleResult {
        try mlx.evalArray(input);
        var shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(input, shape_buf[0..]);
        const triple_result = (try metal_runtime.tryApplyQuantizedRuntimeLinearQkv(
            self,
            q_slot,
            k_slot,
            v_slot,
            input_mt,
            rows,
            in_dim,
            q_out_dim,
            kv_out_dim,
        )) orelse return null;
        return .{
            .first = mlx_metal_bridge.adoptMetalTensorAsMlxArray(triple_result.first),
            .second = mlx_metal_bridge.adoptMetalTensorAsMlxArray(triple_result.second),
            .third = mlx_metal_bridge.adoptMetalTensorAsMlxArray(triple_result.third),
        };
    }

    fn tryRawPostGateI2SResidualHost(
        self: *MetalProvider,
        input: [*c]const f32,
        residual: [*c]const f32,
        intermediate_size: usize,
        hidden_size: usize,
        post_gate_rms_norm_slot: usize,
        down_linear_slot: usize,
        output: [*c]f32,
    ) !bool {
        return metal_runtime.tryRawPostGateI2SResidualHost(
            self,
            input,
            residual,
            intermediate_size,
            hidden_size,
            post_gate_rms_norm_slot,
            down_linear_slot,
            output,
        );
    }

    fn runCompressedAttentionResidual(
        self: *MetalProvider,
        request: *const CompressedAttentionResidualRequest,
    ) !?c.mlx_array {
        const result_mt = (try metal_runtime.runCompressedAttentionResidual(self, request.*, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(result_mt);
    }

    fn runGatedFfnResidual(
        ctx: *anyopaque,
        request: *const RunGatedFfnResidualRequest,
    ) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.hidden_size == 0 or request.intermediate_size == 0) return null;
        if (request.gate_linear_slot >= decoder_runtime_linear_slot_capacity or request.up_linear_slot >= decoder_runtime_linear_slot_capacity or request.down_linear_slot >= decoder_runtime_linear_slot_capacity) return null;
        if (!self.raw_linear_slots_prepared[request.gate_linear_slot]) return null;
        if (!self.raw_linear_slots_prepared[request.up_linear_slot]) return null;
        if (!self.raw_linear_slots_prepared[request.down_linear_slot]) return null;
        if (self.raw_linear_slot_in_dims[request.gate_linear_slot] != request.hidden_size or self.raw_linear_slot_out_dims[request.gate_linear_slot] != request.intermediate_size) return null;
        if (self.raw_linear_slot_in_dims[request.up_linear_slot] != request.hidden_size or self.raw_linear_slot_out_dims[request.up_linear_slot] != request.intermediate_size) return null;
        if (self.raw_linear_slot_in_dims[request.down_linear_slot] != request.intermediate_size or self.raw_linear_slot_out_dims[request.down_linear_slot] != request.hidden_size) return null;

        if (self.raw_linear_slot_kinds[request.gate_linear_slot] == .quantized and
            self.raw_linear_slot_kinds[request.up_linear_slot] == .quantized and
            self.raw_linear_slot_kinds[request.down_linear_slot] == .quantized)
        {
            const input_rows = @as(usize, @intCast(c.mlx_array_dim(request.input, 0)));
            const residual_rows = @as(usize, @intCast(c.mlx_array_dim(request.residual, 0)));
            if (input_rows == 0 or residual_rows != input_rows) return null;
            try mlx.evalArray(request.input);
            try mlx.evalArray(request.residual);
            const input_base = c.mlx_array_data_float32(request.input);
            const residual_base = c.mlx_array_data_float32(request.residual);
            if (input_base == null or residual_base == null) return null;
            if (metal_runtime.shouldAttemptDirectQuantizedGatedFfn(
                self,
                request.gate_linear_slot,
                request.up_linear_slot,
                request.down_linear_slot,
                &mq.timing_stats,
                &mq.logged_quantized_gated_ffn_backend_mixed_kind,
                &mq.logged_quantized_gated_ffn_backend_unsupported_kind,
            )) {
                const direct = try std.heap.c_allocator.alloc(f32, input_rows * request.hidden_size);
                errdefer std.heap.c_allocator.free(direct);
                const direct_ok = try self.tryRawQuantizedGatedFfnResidualHost(
                    input_base.?,
                    residual_base.?,
                    input_rows,
                    request.hidden_size,
                    request.intermediate_size,
                    request.activation,
                    request.gate_linear_slot,
                    request.up_linear_slot,
                    request.down_linear_slot,
                    request.post_gate_rms_norm_slot,
                    request.post_down_rms_norm_slot,
                    direct.ptr,
                );
                if (direct_ok) {
                    mq.timing_stats.quantized_gated_ffn_direct_successes += 1;
                    const shape = [_]i32{ @intCast(input_rows), @intCast(request.hidden_size) };
                    return mlx.arrayFromOwnedFloat32(direct, &shape);
                }
                mq.timing_stats.quantized_gated_ffn_direct_fallbacks += 1;
                std.heap.c_allocator.free(direct);
            } else {
                mq.timing_stats.quantized_gated_ffn_backend_fallbacks += 1;
            }

            const pair = (try decoderRuntimeApplyLinearPair(ctx, &.{
                .slot_a = request.gate_linear_slot,
                .slot_b = request.up_linear_slot,
                .input = request.input,
                .in_dim = request.hidden_size,
                .out_dim = request.intermediate_size,
            })) orelse return null;
            defer _ = c.mlx_array_free(pair.first);
            defer _ = c.mlx_array_free(pair.second);
            const activated = (try decoderRuntimeApplyActivation(ctx, &.{
                .input = pair.first,
                .kind = request.activation,
                .dim = request.intermediate_size,
            })) orelse return null;
            defer _ = c.mlx_array_free(activated);
            const gated = (try decoderRuntimeApplyMultiply(
                ctx,
                activated,
                pair.second,
                request.intermediate_size,
            )) orelse return null;
            defer _ = c.mlx_array_free(gated);
            const projected = (try decoderRuntimeApplyLinear(ctx, &.{
                .slot = request.down_linear_slot,
                .input = gated,
                .in_dim = request.intermediate_size,
                .out_dim = request.hidden_size,
            })) orelse return null;
            defer _ = c.mlx_array_free(projected);
            const post_down_owns = request.post_down_rms_norm_slot != null;
            const post_down = if (request.post_down_rms_norm_slot) |slot|
                (try decoderRuntimeApplyRmsNorm(ctx, &.{
                    .slot = slot,
                    .input = projected,
                    .hidden_size = request.hidden_size,
                    .eps = 0.0,
                })) orelse return null
            else
                projected;
            defer {
                if (post_down_owns) _ = c.mlx_array_free(post_down);
            }
            return decoderRuntimeApplyAdd(ctx, &.{
                .lhs = post_down,
                .rhs = request.residual,
                .dim = request.hidden_size,
            });
        }

        const runtime = self.raw_decode_runtime orelse return null;
        if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
        if (self.raw_linear_slot_kinds[request.gate_linear_slot] != .dense or self.raw_linear_slot_kinds[request.up_linear_slot] != .dense or self.raw_linear_slot_kinds[request.down_linear_slot] != .dense) return null;
        if (c.mlx_array_dtype(request.input) != c.MLX_FLOAT32 or c.mlx_array_dtype(request.residual) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_ndim(request.input) != 2 or c.mlx_array_ndim(request.residual) != 2) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.input, 0))) != 1 or @as(usize, @intCast(c.mlx_array_dim(request.residual, 0))) != 1) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.input, 1))) != request.hidden_size or @as(usize, @intCast(c.mlx_array_dim(request.residual, 1))) != request.hidden_size) return null;
        try mlx.evalArray(request.input);
        try mlx.evalArray(request.residual);
        const input_base = c.mlx_array_data_float32(request.input);
        const residual_base = c.mlx_array_data_float32(request.residual);
        if (input_base == null or residual_base == null) return null;

        if (request.post_down_rms_norm_slot == null) {
            const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
            errdefer std.heap.c_allocator.free(output);
            const rc = termite_metal_decode_runtime_apply_linear_pair_activation_multiply_linear_residual(
                runtime,
                request.gate_linear_slot,
                request.up_linear_slot,
                request.down_linear_slot,
                std.math.maxInt(usize),
                input_base.?,
                residual_base.?,
                request.hidden_size,
                request.intermediate_size,
                @intFromEnum(request.activation),
                output.ptr,
            );
            if (rc != 0) return null;
            const shape = [_]i32{ 1, @intCast(request.hidden_size) };
            return mlx.arrayFromOwnedFloat32(output, &shape);
        }
        const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
        errdefer std.heap.c_allocator.free(output);
        const rc = termite_metal_decode_runtime_apply_linear_pair_activation_multiply_linear_residual(
            runtime,
            request.gate_linear_slot,
            request.up_linear_slot,
            request.down_linear_slot,
            request.post_down_rms_norm_slot.?,
            input_base.?,
            residual_base.?,
            request.hidden_size,
            request.intermediate_size,
            @intFromEnum(request.activation),
            output.ptr,
        );
        if (rc != 0) return null;
        const shape = [_]i32{ 1, @intCast(request.hidden_size) };
        return mlx.arrayFromOwnedFloat32(output, &shape);
    }

    fn runAttentionResidualPostLinear(
        ctx: *anyopaque,
        request: *const RunAttentionResidualPostLinearRequest,
    ) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.attention_linear_slot >= decoder_runtime_linear_slot_capacity) return null;
        if (!self.raw_linear_slots_prepared[request.attention_linear_slot]) return null;
        if (self.raw_linear_slot_in_dims[request.attention_linear_slot] != request.attention_input_size or
            self.raw_linear_slot_out_dims[request.attention_linear_slot] != request.hidden_size) return null;
        if (!metal_runtime.canTryRawAttentionResidualHost(
            self,
            request.attention_linear_slot,
            request.attention_input_size,
            request.hidden_size,
        )) return null;

        const input_eval_started_at = mq.monotonicNowNs();
        try mlx.evalArray(request.attention_input);
        try mlx.evalArray(request.residual);
        const attention_input_base = c.mlx_array_data_float32(request.attention_input) orelse return null;
        const residual_base = c.mlx_array_data_float32(request.residual) orelse return null;
        const input_eval_finished_at = mq.monotonicNowNs();
        if (input_eval_finished_at > input_eval_started_at) {
            mq.timing_stats.compressed_attention_residual_input_eval_nanos += input_eval_finished_at - input_eval_started_at;
        }

        const output = try std.heap.c_allocator.alloc(f32, request.hidden_size);
        errdefer std.heap.c_allocator.free(output);
        const raw_runtime_started_at = mq.monotonicNowNs();
        if (try self.tryRawAttentionResidualHost(
            attention_input_base,
            residual_base,
            request.attention_input_size,
            request.hidden_size,
            request.eps,
            request.attention_linear_slot,
            request.attention_pre_linear_rms_norm_slot,
            request.attention_post_linear_rms_norm_slot,
            output.ptr,
        )) {
            const raw_runtime_finished_at = mq.monotonicNowNs();
            if (raw_runtime_finished_at > raw_runtime_started_at) {
                mq.timing_stats.compressed_attention_residual_raw_runtime_nanos += raw_runtime_finished_at - raw_runtime_started_at;
            }
            mq.timing_stats.compressed_attention_residual_post_linear_successes += 1;
            const shape = [_]i32{ 1, @intCast(request.hidden_size) };
            return mlx.arrayFromOwnedFloat32(output, &shape);
        }
        const raw_runtime_finished_at = mq.monotonicNowNs();
        if (raw_runtime_finished_at > raw_runtime_started_at) {
            mq.timing_stats.compressed_attention_residual_raw_runtime_nanos += raw_runtime_finished_at - raw_runtime_started_at;
        }
        mq.timing_stats.compressed_attention_residual_post_linear_failures += 1;
        return null;
    }

    const DecoderRuntimeNormResult = struct {
        arr: c.mlx_array,
        owns: bool,
    };

    fn decoderRuntimeApplyFfnNormInternal(
        self: *MetalProvider,
        input: c.mlx_array,
        layer_norm_slot: ?usize,
        rms_norm_slot: ?usize,
        hidden_size: usize,
        eps: f32,
    ) !?DecoderRuntimeNormResult {
        var input_shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const input_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(input, input_shape_buf[0..]);
        const result_mt = (try metal_runtime.decoderRuntimeApplyFfnNormInternal(
            self,
            input_mt,
            layer_norm_slot,
            rms_norm_slot,
            hidden_size,
            eps,
        )) orelse return null;
        if (result_mt.owned_by_c_allocator) {
            return DecoderRuntimeNormResult{
                .arr = mlx_metal_bridge.adoptMetalTensorAsMlxArray(result_mt),
                .owns = true,
            };
        }
        return DecoderRuntimeNormResult{ .arr = input, .owns = false };
    }

    pub fn cloneFloat32Tensor(self: *MetalProvider, arr: c.mlx_array) !MetalTensor {
        _ = self;
        const stream = mlx.gpuStream();
        var casted = c.mlx_array_new();
        const used_casted = c.mlx_array_dtype(arr) != c.MLX_FLOAT32;
        const source = if (!used_casted)
            arr
        else blk: {
            try mlx.check(c.mlx_astype(&casted, arr, c.MLX_FLOAT32, stream));
            break :blk casted;
        };
        defer {
            if (used_casted) {
                _ = c.mlx_array_free(casted);
            }
        }

        var cloned = c.mlx_array_new();
        try mlx.check(c.mlx_contiguous(&cloned, source, false, stream));
        try mlx.evalArray(cloned);
        defer _ = c.mlx_array_free(cloned);

        var shape_buf: [metal_tensor.max_dims]i32 = undefined;
        const tensor = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(cloned, shape_buf[0..]);
        return MetalTensor.ownedCloneFrom(tensor.slice(), tensor.shape());
    }

    fn applyActivationHost(values: []f32, kind: ops.DecoderRuntimeActivationKind) void {
        switch (kind) {
            .gelu => activations.gelu(values),
            .gelu_new => for (values) |*v| {
                const x = v.*;
                const inner = 0.7978845608 * (x + 0.044715 * x * x * x);
                v.* = 0.5 * x * (1.0 + std.math.tanh(inner));
            },
            .silu => activations.silu(values),
            .relu => activations.relu(values),
            .quick_gelu => activations.quickGelu(values),
            .relu_squared => {
                activations.relu(values);
                for (values) |*v| v.* *= v.*;
            },
        }
    }

    fn sliceRows(self: *MetalProvider, arr: c.mlx_array, start_row: usize, row_count: usize) !c.mlx_array {
        return metal_runtime.sliceRows(self, arr, start_row, row_count);
    }

    fn concatenateRows(self: *MetalProvider, lhs: c.mlx_array, rhs: c.mlx_array) !c.mlx_array {
        return metal_runtime.concatenateRows(self, lhs, rhs);
    }

    fn cloneRowsFromBootstrapBlocks(
        self: *MetalProvider,
        blocks: []const c.mlx_array,
        token_counts: []const usize,
    ) !?c.mlx_array {
        return metal_runtime.cloneRowsFromBootstrapBlocks(self, blocks, token_counts);
    }

    fn replaceGatheredSpanEntry(
        self: *MetalProvider,
        entry: *GatheredSpanEntry,
        new_k: c.mlx_array,
        new_v: c.mlx_array,
        token_count: usize,
        position_offset: usize,
    ) !void {
        return metal_runtime.replaceGatheredSpanEntry(self, entry, new_k, new_v, token_count, position_offset);
    }

    fn resetGatheredSpans(self: *MetalProvider) void {
        metal_runtime.resetGatheredSpans(self);
    }

    fn hasGatheredSpanCache(ctx: *anyopaque, source_ptr_id: usize, sequence_id: runtime_root.kv.manager.SequenceId, layer_index: usize) bool {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        return metal_runtime.hasGatheredSpanCache(self, source_ptr_id, sequence_id, layer_index);
    }

    fn updateGatheredSpan(
        self: *MetalProvider,
        key: GatheredSpanKey,
        k_suffix: c.mlx_array,
        v_suffix: c.mlx_array,
        bootstrap_k_blocks: []const c.mlx_array,
        bootstrap_v_blocks: []const c.mlx_array,
        bootstrap_block_token_counts: []const usize,
        full_k: ?c.mlx_array,
        full_v: ?c.mlx_array,
        query_sequence_len: usize,
        kv_tokens: usize,
        kv_position_offset: usize,
    ) !?*GatheredSpanEntry {
        return metal_runtime.updateGatheredSpan(
            self,
            key,
            k_suffix,
            v_suffix,
            bootstrap_k_blocks,
            bootstrap_v_blocks,
            bootstrap_block_token_counts,
            full_k,
            full_v,
            query_sequence_len,
            kv_tokens,
            kv_position_offset,
            &mq.timing_stats,
        );
    }

    fn encodeCompressedKeyRowsForRuntime(
        self: *MetalProvider,
        k: c.mlx_array,
        kv_tokens: usize,
        num_kv_heads: usize,
        head_dim: usize,
        format: CompressedKeyFormat,
    ) ![]u8 {
        return metal_runtime.encodeCompressedKeyRowsForRuntime(self, k, kv_tokens, num_kv_heads, head_dim, format);
    }

    fn runCompressedAttentionDenseDecoderBlock(
        ctx: *anyopaque,
        request: *const RunCompressedAttentionDenseDecoderBlockRequest,
    ) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        const runtime = self.raw_decode_runtime orelse return null;
        if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
        if (request.attention_linear_slot >= decoder_runtime_linear_slot_capacity or
            request.first_ffn_linear_slot >= decoder_runtime_linear_slot_capacity or
            request.second_ffn_linear_slot >= decoder_runtime_linear_slot_capacity) return null;
        mq.timing_stats.compressed_block_dense_calls += 1;
        if (self.raw_linear_slot_kinds[request.attention_linear_slot] != .dense or
            self.raw_linear_slot_kinds[request.first_ffn_linear_slot] != .dense or
            self.raw_linear_slot_kinds[request.second_ffn_linear_slot] != .dense)
        {
            const q_arr = request.q orelse return null;
            const ks_arr = request.k_suffix orelse return null;
            const vs_arr = request.v_suffix orelse return null;
            const attn_started_at = mq.monotonicNowNs();
            var q_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            var ks_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            var vs_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            var fk_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            var fv_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            var res_shape_buf: [metal_tensor.max_dims]i32 = undefined;
            const q_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(q_arr, q_shape_buf[0..]);
            const ks_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(ks_arr, ks_shape_buf[0..]);
            const vs_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(vs_arr, vs_shape_buf[0..]);
            const fk_mt = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.full_k, fk_shape_buf[0..]);
            const fv_mt = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.full_v, fv_shape_buf[0..]);
            const res_mt = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.residual, res_shape_buf[0..]);
            const bk_bridged = try mlx_metal_bridge.borrowMlxArraysAsMetalTensors(std.heap.c_allocator, request.bootstrap_k_blocks);
            defer std.heap.c_allocator.free(bk_bridged.tensors);
            defer std.heap.c_allocator.free(bk_bridged.shapes);
            const bv_bridged = try mlx_metal_bridge.borrowMlxArraysAsMetalTensors(std.heap.c_allocator, request.bootstrap_v_blocks);
            defer std.heap.c_allocator.free(bv_bridged.tensors);
            defer std.heap.c_allocator.free(bv_bridged.shapes);
            const attn_res = (try self.runCompressedAttentionResidual(&.{
                .q = q_mt,
                .k_suffix = ks_mt,
                .v_suffix = vs_mt,
                .bootstrap_k_blocks = bk_bridged.tensors,
                .bootstrap_v_blocks = bv_bridged.tensors,
                .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
                .full_k = fk_mt,
                .full_v = fv_mt,
                .source_ptr_id = request.source_ptr_id,
                .sequence_id = request.sequence_id,
                .layer_index = request.layer_index,
                .query_sequence_len = request.query_sequence_len,
                .kv_tokens = request.kv_tokens,
                .num_heads = request.num_heads,
                .num_kv_heads = request.num_kv_heads,
                .head_dim = request.head_dim,
                .key_row_bytes = request.key_row_bytes,
                .query_position = request.query_position,
                .kv_position_offset = request.kv_position_offset,
                .sliding_window = request.sliding_window,
                .format = request.format,
                .attention_linear_slot = request.attention_linear_slot,
                .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
                .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
                .residual = res_mt,
                .hidden_size = request.hidden_size,
                .attention_input_size = request.num_heads * request.head_dim,
                .eps = request.eps,
            })) orelse return null;
            mq.timing_stats.compressed_block_quantized_attention_calls += 1;
            mq.timing_stats.compressed_block_quantized_attention_nanos += @intCast(mq.monotonicNowNs() - attn_started_at);
            defer _ = c.mlx_array_free(attn_res);
            const ffn_normed = (try self.decoderRuntimeApplyFfnNormInternal(
                attn_res,
                request.ffn_layer_norm_slot,
                request.ffn_rms_norm_slot,
                request.hidden_size,
                request.eps,
            )) orelse return null;
            defer {
                if (ffn_normed.owns) _ = c.mlx_array_free(ffn_normed.arr);
            }
            const apply_started_at = mq.monotonicNowNs();
            const hidden = try runDenseFfnResidual(ctx, &.{
                .input = ffn_normed.arr,
                .residual = attn_res,
                .first_linear_slot = request.first_ffn_linear_slot,
                .second_linear_slot = request.second_ffn_linear_slot,
                .hidden_size = request.hidden_size,
                .intermediate_size = request.intermediate_size,
                .activation = request.activation,
            });
            const apply_elapsed = mq.monotonicNowNs() - apply_started_at;
            mq.timing_stats.compressed_block_apply_nanos += @intCast(apply_elapsed);
            mq.timing_stats.compressed_block_quantized_ffn_nanos += @intCast(apply_elapsed);
            return hidden;
        }
        var q_shape_d: [metal_tensor.max_dims]i32 = undefined;
        var ks_shape_d: [metal_tensor.max_dims]i32 = undefined;
        var vs_shape_d: [metal_tensor.max_dims]i32 = undefined;
        var fk_shape_d: [metal_tensor.max_dims]i32 = undefined;
        var fv_shape_d: [metal_tensor.max_dims]i32 = undefined;
        var res_shape_d: [metal_tensor.max_dims]i32 = undefined;
        var ai_shape_d: [metal_tensor.max_dims]i32 = undefined;
        const q_mt_d = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.q, q_shape_d[0..]);
        const ks_mt_d = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.k_suffix, ks_shape_d[0..]);
        const vs_mt_d = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.v_suffix, vs_shape_d[0..]);
        const ai_mt_d = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.attention_input, ai_shape_d[0..]);
        const fk_mt_d = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.full_k, fk_shape_d[0..]);
        const fv_mt_d = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.full_v, fv_shape_d[0..]);
        const res_mt_d = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.residual, res_shape_d[0..]);
        const bk_bridged_d = try mlx_metal_bridge.borrowMlxArraysAsMetalTensors(std.heap.c_allocator, request.bootstrap_k_blocks);
        defer std.heap.c_allocator.free(bk_bridged_d.tensors);
        defer std.heap.c_allocator.free(bk_bridged_d.shapes);
        const bv_bridged_d = try mlx_metal_bridge.borrowMlxArraysAsMetalTensors(std.heap.c_allocator, request.bootstrap_v_blocks);
        defer std.heap.c_allocator.free(bv_bridged_d.tensors);
        defer std.heap.c_allocator.free(bv_bridged_d.shapes);
        const result_mt = (try metal_runtime.runCompressedAttentionDenseDecoderBlockDirect(self, .{
            .q = q_mt_d,
            .k_suffix = ks_mt_d,
            .v_suffix = vs_mt_d,
            .attention_input = ai_mt_d,
            .fused_qkv_linear_slot = request.fused_qkv_linear_slot,
            .bootstrap_k_blocks = bk_bridged_d.tensors,
            .bootstrap_v_blocks = bv_bridged_d.tensors,
            .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
            .full_k = fk_mt_d,
            .full_v = fv_mt_d,
            .source_ptr_id = request.source_ptr_id,
            .sequence_id = request.sequence_id,
            .layer_index = request.layer_index,
            .query_sequence_len = request.query_sequence_len,
            .kv_tokens = request.kv_tokens,
            .num_heads = request.num_heads,
            .num_kv_heads = request.num_kv_heads,
            .head_dim = request.head_dim,
            .key_row_bytes = request.key_row_bytes,
            .query_position = request.query_position,
            .kv_position_offset = request.kv_position_offset,
            .sliding_window = request.sliding_window,
            .format = request.format,
            .attention_linear_slot = request.attention_linear_slot,
            .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
            .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
            .residual = res_mt_d,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .ffn_layer_norm_slot = request.ffn_layer_norm_slot,
            .ffn_rms_norm_slot = request.ffn_rms_norm_slot,
            .first_ffn_linear_slot = request.first_ffn_linear_slot,
            .second_ffn_linear_slot = request.second_ffn_linear_slot,
            .intermediate_size = request.intermediate_size,
            .activation = request.activation,
        }, &mq.timing_stats)) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(result_mt);
    }

    fn runCompressedAttentionGatedDecoderBlock(
        ctx: *anyopaque,
        request: *const RunCompressedAttentionGatedDecoderBlockRequest,
    ) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        var res_shape_g: [metal_tensor.max_dims]i32 = undefined;
        var fk_shape_g: [metal_tensor.max_dims]i32 = undefined;
        var fv_shape_g: [metal_tensor.max_dims]i32 = undefined;
        var q_shape_g: [metal_tensor.max_dims]i32 = undefined;
        var ks_shape_g: [metal_tensor.max_dims]i32 = undefined;
        var vs_shape_g: [metal_tensor.max_dims]i32 = undefined;
        var ai_shape_g: [metal_tensor.max_dims]i32 = undefined;
        const res_mt_g = try mlx_metal_bridge.borrowMlxArrayAsMetalTensor(request.residual, res_shape_g[0..]);
        const fk_mt_g = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.full_k, fk_shape_g[0..]);
        const fv_mt_g = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.full_v, fv_shape_g[0..]);
        const q_mt_g = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.q, q_shape_g[0..]);
        const ks_mt_g = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.k_suffix, ks_shape_g[0..]);
        const vs_mt_g = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.v_suffix, vs_shape_g[0..]);
        const ai_mt_g = try mlx_metal_bridge.borrowOptionalMlxArrayAsMetalTensor(request.attention_input, ai_shape_g[0..]);
        const bk_bridged_g = try mlx_metal_bridge.borrowMlxArraysAsMetalTensors(std.heap.c_allocator, request.bootstrap_k_blocks);
        defer std.heap.c_allocator.free(bk_bridged_g.tensors);
        defer std.heap.c_allocator.free(bk_bridged_g.shapes);
        const bv_bridged_g = try mlx_metal_bridge.borrowMlxArraysAsMetalTensors(std.heap.c_allocator, request.bootstrap_v_blocks);
        defer std.heap.c_allocator.free(bv_bridged_g.tensors);
        defer std.heap.c_allocator.free(bv_bridged_g.shapes);
        const metal_request = .{
            .q = q_mt_g,
            .k_suffix = ks_mt_g,
            .v_suffix = vs_mt_g,
            .attention_input = ai_mt_g,
            .q_linear_slot = request.q_linear_slot,
            .k_linear_slot = request.k_linear_slot,
            .v_linear_slot = request.v_linear_slot,
            .bootstrap_k_blocks = bk_bridged_g.tensors,
            .bootstrap_v_blocks = bv_bridged_g.tensors,
            .bootstrap_block_token_counts = request.bootstrap_block_token_counts,
            .full_k = fk_mt_g,
            .full_v = fv_mt_g,
            .source_ptr_id = request.source_ptr_id,
            .sequence_id = request.sequence_id,
            .layer_index = request.layer_index,
            .query_sequence_len = request.query_sequence_len,
            .kv_tokens = request.kv_tokens,
            .num_heads = request.num_heads,
            .num_kv_heads = request.num_kv_heads,
            .head_dim = request.head_dim,
            .key_row_bytes = request.key_row_bytes,
            .query_position = request.query_position,
            .kv_position_offset = request.kv_position_offset,
            .sliding_window = request.sliding_window,
            .format = request.format,
            .attention_linear_slot = request.attention_linear_slot,
            .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
            .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
            .residual = res_mt_g,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .ffn_layer_norm_slot = request.ffn_layer_norm_slot,
            .ffn_rms_norm_slot = request.ffn_rms_norm_slot,
            .ffn_post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
            .ffn_post_down_rms_norm_slot = request.ffn_post_down_rms_norm_slot,
            .gate_ffn_linear_slot = request.gate_ffn_linear_slot,
            .up_ffn_linear_slot = request.up_ffn_linear_slot,
            .down_ffn_linear_slot = request.down_ffn_linear_slot,
            .intermediate_size = request.intermediate_size,
            .activation = request.activation,
        };
        const result_mt = (try metal_runtime.runCompressedAttentionGatedDecoderBlockBackend(
            self,
            ctx,
            metal_request,
            &mq.timing_stats,
            &mq.logged_quantized_gated_ffn_unsupported_type,
            &mq.logged_quantized_gated_ffn_backend_mixed_kind,
            &mq.logged_quantized_gated_ffn_backend_unsupported_kind,
            runGatedFfnResidual,
            decoderRuntimeApplyLinear,
            decoderRuntimeApplyLinearPair,
        )) orelse return null;
        return mlx_metal_bridge.adoptMetalTensorAsMlxArray(result_mt);
    }

    fn compressedKeyScores(ctx: *anyopaque, request: *const CompressedKeyScoresRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.q_len == 0 or request.block_tokens == 0 or request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return null;
        if (request.num_heads % request.num_kv_heads != 0) return null;
        const base_key_row_bytes = switch (request.format) {
            .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
            .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
        };
        const residual_key_row_bytes = switch (request.format) {
            .polar4 => 0,
            .turbo3 => (@as(usize, request.num_kv_heads) * turboquant.turbo3_residual_bits_per_head + 7) / 8,
        };
        const expected_key_row_bytes = base_key_row_bytes + residual_key_row_bytes;
        if (request.key_row_bytes != expected_key_row_bytes) return null;
        if (try self.compressedKeyScoresRawMetal(request)) |output| return output;

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ 1, @intCast(request.num_heads), @intCast(request.q_len), @intCast(request.block_tokens) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "QLen", request.q_len);
        try addTemplateInt(cfg, "BlockTokens", request.block_tokens);
        try addTemplateInt(cfg, "NumHeads", request.num_heads);
        try addTemplateInt(cfg, "NumKvHeads", request.num_kv_heads);
        try addTemplateInt(cfg, "HeadDim", request.head_dim);
        try addTemplateInt(cfg, "KeyRowBytes", request.key_row_bytes);
        try addTemplateInt(cfg, "BaseKeyRowBytes", base_key_row_bytes);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(request.block_tokens), @intCast(request.num_heads * request.q_len), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@min(request.block_tokens, mq.grouped_threadgroup_width)), 1, 1));

        const inputs = [_]c.mlx_array{ request.q, request.encoded_key };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        const kernel = switch (request.format) {
            .polar4 => self.polar4_key_scores,
            .turbo3 => self.turbo3_key_scores,
        };
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    fn compressedAttentionBlock(ctx: *anyopaque, request: *const CompressedAttentionBlockRequest) !?CompressedAttentionBlockResult {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.block_tokens == 0 or request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return null;
        if (request.num_heads % request.num_kv_heads != 0) return null;
        if (request.block_tokens > 256) return null;
        if (c.mlx_array_dtype(request.q) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_dtype(request.v) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_ndim(request.q) != 2 or c.mlx_array_ndim(request.v) != 2) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.q, 0))) != 1) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.q, 1))) != request.num_heads * request.head_dim) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.v, 0))) != request.block_tokens) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.v, 1))) < request.num_kv_heads * request.head_dim) return null;

        const base_key_row_bytes = switch (request.format) {
            .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
            .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
        };
        const residual_key_row_bytes = switch (request.format) {
            .polar4 => 0,
            .turbo3 => (@as(usize, request.num_kv_heads) * turboquant.turbo3_residual_bits_per_head + 7) / 8,
        };
        const expected_key_row_bytes = base_key_row_bytes + residual_key_row_bytes;
        if (request.key_row_bytes != expected_key_row_bytes) return null;

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const max_shape = [_]c_int{ 1, @intCast(request.num_heads), 1, 1 };
        const acc_shape = [_]c_int{ 1, @intCast(request.num_heads), 1, @intCast(request.head_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &max_shape, max_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &max_shape, max_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &acc_shape, acc_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "MaxBlockTokens", 256);
        try addTemplateInt(cfg, "NumHeads", request.num_heads);
        try addTemplateInt(cfg, "NumKvHeads", request.num_kv_heads);
        try addTemplateInt(cfg, "HeadDim", request.head_dim);
        try addTemplateInt(cfg, "KeyRowBytes", request.key_row_bytes);
        try addTemplateInt(cfg, "BaseKeyRowBytes", base_key_row_bytes);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(request.num_heads), 1, 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@min(request.num_heads, mq.grouped_threadgroup_width)), 1, 1));

        const meta_values = [_]i32{
            @intCast(request.query_position),
            @intCast(request.block_position_offset),
            @intCast(request.sliding_window),
            @intCast(request.block_tokens),
        };
        const meta_shape = [_]i32{meta_values.len};
        const meta = mlx.arrayFromInt32(&meta_values, &meta_shape);
        defer _ = c.mlx_array_free(meta);

        const inputs = [_]c.mlx_array{
            request.q,
            request.encoded_key,
            request.v,
            request.running_max,
            request.running_sum,
            request.running_acc,
            meta,
        };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        const kernel = switch (request.format) {
            .polar4 => self.polar4_attention_block,
            .turbo3 => self.turbo3_attention_block,
        };
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 3) return null;

        var out_max = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(out_max);
        var out_sum = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(out_sum);
        var out_acc = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(out_acc);
        try mlx.check(c.mlx_vector_array_get(&out_max, out_vec, 0));
        try mlx.check(c.mlx_vector_array_get(&out_sum, out_vec, 1));
        try mlx.check(c.mlx_vector_array_get(&out_acc, out_vec, 2));
        return .{
            .running_max = out_max,
            .running_sum = out_sum,
            .running_acc = out_acc,
        };
    }

    fn compressedAttentionSpanChunked(self: *MetalProvider, request: *const CompressedAttentionSpanRequest, base_key_row_bytes: usize) !?c.mlx_array {
        const num_chunks = (request.kv_tokens + span_chunk_tokens - 1) / span_chunk_tokens;
        if (num_chunks <= 1) return null;

        const partial_cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(partial_cfg);

        const partial_shape = [_]c_int{ @intCast(request.num_heads), @intCast(num_chunks) };
        const partial_acc_shape = [_]c_int{ @intCast(request.num_heads), @intCast(num_chunks), @intCast(request.head_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(partial_cfg, &partial_shape, partial_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(partial_cfg, &partial_shape, partial_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(partial_cfg, &partial_acc_shape, partial_acc_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(partial_cfg, "SpanChunkTokens", span_chunk_tokens);
        try addTemplateInt(partial_cfg, "NumChunks", num_chunks);
        try addTemplateInt(partial_cfg, "NumHeads", request.num_heads);
        try addTemplateInt(partial_cfg, "NumKvHeads", request.num_kv_heads);
        try addTemplateInt(partial_cfg, "HeadDim", request.head_dim);
        try addTemplateInt(partial_cfg, "KeyRowBytes", request.key_row_bytes);
        try addTemplateInt(partial_cfg, "BaseKeyRowBytes", base_key_row_bytes);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(partial_cfg, @intCast(request.num_heads), @intCast(num_chunks), 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(partial_cfg, @intCast(@min(request.num_heads, mq.grouped_threadgroup_width)), 1, 1));

        const meta_values = [_]i32{
            @intCast(request.query_position),
            @intCast(request.kv_position_offset),
            @intCast(request.sliding_window),
            @intCast(request.kv_tokens),
        };
        const meta_shape = [_]i32{meta_values.len};
        const meta = mlx.arrayFromInt32(&meta_values, &meta_shape);
        defer _ = c.mlx_array_free(meta);

        const partial_inputs = [_]c.mlx_array{ request.q, request.encoded_key, request.v, meta };
        const partial_in_vec = c.mlx_vector_array_new_data(&partial_inputs, partial_inputs.len);
        defer _ = c.mlx_vector_array_free(partial_in_vec);

        var partial_out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(partial_out_vec);
        const partial_kernel = switch (request.format) {
            .polar4 => self.polar4_attention_span_partials,
            .turbo3 => self.turbo3_attention_span_partials,
        };
        try mlx.check(c.mlx_fast_metal_kernel_apply(&partial_out_vec, partial_kernel, partial_in_vec, partial_cfg, request.stream));
        if (c.mlx_vector_array_size(partial_out_vec) < 3) return null;

        var partial_max = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(partial_max);
        var partial_sum = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(partial_sum);
        var partial_acc = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(partial_acc);
        try mlx.check(c.mlx_vector_array_get(&partial_max, partial_out_vec, 0));
        try mlx.check(c.mlx_vector_array_get(&partial_sum, partial_out_vec, 1));
        try mlx.check(c.mlx_vector_array_get(&partial_acc, partial_out_vec, 2));
        defer _ = c.mlx_array_free(partial_max);
        defer _ = c.mlx_array_free(partial_sum);
        defer _ = c.mlx_array_free(partial_acc);

        const reduce_cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(reduce_cfg);

        const output_shape = [_]c_int{ 1, @intCast(request.num_heads * request.head_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(reduce_cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(reduce_cfg, "NumChunks", num_chunks);
        try addTemplateInt(reduce_cfg, "HeadDim", request.head_dim);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(reduce_cfg, @intCast(request.num_heads), 1, 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(reduce_cfg, @intCast(@min(request.num_heads, mq.grouped_threadgroup_width)), 1, 1));

        const reduce_inputs = [_]c.mlx_array{ partial_max, partial_sum, partial_acc };
        const reduce_in_vec = c.mlx_vector_array_new_data(&reduce_inputs, reduce_inputs.len);
        defer _ = c.mlx_vector_array_free(reduce_in_vec);

        var reduce_out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(reduce_out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&reduce_out_vec, self.attention_span_reduce, reduce_in_vec, reduce_cfg, request.stream));
        if (c.mlx_vector_array_size(reduce_out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, reduce_out_vec, 0));
        return output;
    }

    fn compressedAttentionSpanDecoderRuntime(self: *MetalProvider, request: *const CompressedAttentionSpanRequest, base_key_row_bytes: usize) !?c.mlx_array {
        if (!mq.enableMetalDecoderRuntimeDebug()) return null;
        if (mq.disableMetalDecoderRuntimeAttentionSpanDebug()) return null;
        const runtime = self.raw_decode_runtime orelse return null;
        if (termite_metal_decode_runtime_ready(runtime) == 0) return null;
        if (c.mlx_array_dtype(request.q) != c.MLX_FLOAT32 or c.mlx_array_dtype(request.v) != c.MLX_FLOAT32 or c.mlx_array_dtype(request.encoded_key) != c.MLX_UINT8) return null;
        if (c.mlx_array_ndim(request.q) != 2 or c.mlx_array_ndim(request.v) != 2 or c.mlx_array_ndim(request.encoded_key) != 1) return null;
        const v_row_stride: usize = @intCast(c.mlx_array_dim(request.v, 1));
        if (v_row_stride < request.num_kv_heads * request.head_dim) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.encoded_key, 0))) != request.kv_tokens * request.key_row_bytes) return null;

        try mlx.evalArray(request.q);
        const q_ptr = c.mlx_array_data_float32(request.q);
        if (q_ptr == null) return error.MlxDataNull;

        const output_len = request.num_heads * request.head_dim;
        const output = try std.heap.c_allocator.alloc(f32, output_len);
        errdefer std.heap.c_allocator.free(output);

        var span_ready = false;
        if (request.suffix_tokens > 0 and request.suffix_encoded_key != null and request.suffix_v != null) {
            const suffix_encoded_key = request.suffix_encoded_key.?;
            const suffix_v = request.suffix_v.?;
            if (c.mlx_array_dtype(suffix_encoded_key) == c.MLX_UINT8 and
                c.mlx_array_dtype(suffix_v) == c.MLX_FLOAT32 and
                c.mlx_array_ndim(suffix_encoded_key) == 1 and
                c.mlx_array_ndim(suffix_v) == 2 and
                @as(usize, @intCast(c.mlx_array_dim(suffix_encoded_key, 0))) == request.suffix_tokens * request.key_row_bytes and
                @as(usize, @intCast(c.mlx_array_dim(suffix_v, 1))) == v_row_stride)
            {
                try mlx.evalArray(suffix_encoded_key);
                try mlx.evalArray(suffix_v);
                const suffix_encoded_ptr = c.mlx_array_data_uint8(suffix_encoded_key);
                const suffix_v_ptr = c.mlx_array_data_float32(suffix_v);
                if (suffix_encoded_ptr == null or suffix_v_ptr == null) return error.MlxDataNull;
                span_ready = termite_metal_decode_runtime_append_attention_span(
                    runtime,
                    suffix_encoded_ptr,
                    suffix_v_ptr,
                    request.kv_tokens,
                    request.suffix_tokens,
                    request.key_row_bytes,
                    v_row_stride,
                    request.kv_position_offset,
                ) == 0;
            }
        }

        if (!span_ready) {
            try mlx.evalArray(request.encoded_key);
            try mlx.evalArray(request.v);
            const encoded_ptr = c.mlx_array_data_uint8(request.encoded_key);
            const v_ptr = c.mlx_array_data_float32(request.v);
            if (encoded_ptr == null or v_ptr == null) return error.MlxDataNull;
            if (termite_metal_decode_runtime_update_attention_span(
                runtime,
                encoded_ptr,
                v_ptr,
                request.kv_tokens,
                request.key_row_bytes,
                v_row_stride,
                request.kv_position_offset,
            ) != 0) return null;
        }

        mq.timing_stats.decoder_runtime_attention_span_calls += 1;
        const rc = termite_metal_decode_runtime_attention_span(
            runtime,
            switch (request.format) {
                .polar4 => 0,
                .turbo3 => 1,
            },
            q_ptr,
            request.kv_tokens,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
            request.key_row_bytes,
            base_key_row_bytes,
            request.query_position,
            request.kv_position_offset,
            request.sliding_window,
            output.ptr,
        );
        if (rc != 0) return null;
        const shape = [_]i32{ 1, @intCast(output_len) };
        return mlx.arrayFromOwnedFloat32(output, &shape);
    }

    fn compressedAttentionSpan(ctx: *anyopaque, request: *const CompressedAttentionSpanRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.kv_tokens == 0 or request.num_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return null;
        if (request.num_heads % request.num_kv_heads != 0) return null;
        if (c.mlx_array_dtype(request.q) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_dtype(request.v) != c.MLX_FLOAT32) return null;
        if (c.mlx_array_ndim(request.q) != 2 or c.mlx_array_ndim(request.v) != 2) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.q, 0))) != 1) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.q, 1))) != request.num_heads * request.head_dim) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.v, 0))) != request.kv_tokens) return null;
        if (@as(usize, @intCast(c.mlx_array_dim(request.v, 1))) < request.num_kv_heads * request.head_dim) return null;

        const base_key_row_bytes = switch (request.format) {
            .polar4 => (@as(usize, request.num_kv_heads) * request.head_dim + 1) / 2,
            .turbo3 => (@as(usize, request.num_kv_heads) * request.head_dim * 3 + 7) / 8,
        };
        const residual_key_row_bytes = switch (request.format) {
            .polar4 => 0,
            .turbo3 => (@as(usize, request.num_kv_heads) * turboquant.turbo3_residual_bits_per_head + 7) / 8,
        };
        const expected_key_row_bytes = base_key_row_bytes + residual_key_row_bytes;
        if (request.key_row_bytes != expected_key_row_bytes) return null;
        if (try self.compressedAttentionSpanDecoderRuntime(request, base_key_row_bytes)) |output| return output;
        if (request.kv_tokens > span_chunk_tokens) {
            if (try self.compressedAttentionSpanChunked(request, base_key_row_bytes)) |output| return output;
        }

        const cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(cfg);

        const output_shape = [_]c_int{ 1, @intCast(request.num_heads * request.head_dim) };
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(cfg, &output_shape, output_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(cfg, "NumHeads", request.num_heads);
        try addTemplateInt(cfg, "NumKvHeads", request.num_kv_heads);
        try addTemplateInt(cfg, "HeadDim", request.head_dim);
        try addTemplateInt(cfg, "KeyRowBytes", request.key_row_bytes);
        try addTemplateInt(cfg, "BaseKeyRowBytes", base_key_row_bytes);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(cfg, @intCast(request.num_heads), 1, 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(cfg, @intCast(@min(request.num_heads, mq.grouped_threadgroup_width)), 1, 1));

        const meta_values = [_]i32{
            @intCast(request.query_position),
            @intCast(request.kv_position_offset),
            @intCast(request.sliding_window),
            @intCast(request.kv_tokens),
        };
        const meta_shape = [_]i32{meta_values.len};
        const meta = mlx.arrayFromInt32(&meta_values, &meta_shape);
        defer _ = c.mlx_array_free(meta);

        const inputs = [_]c.mlx_array{ request.q, request.encoded_key, request.v, meta };
        const in_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
        defer _ = c.mlx_vector_array_free(in_vec);

        var out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(out_vec);
        const kernel = switch (request.format) {
            .polar4 => self.polar4_attention_span,
            .turbo3 => self.turbo3_attention_span,
        };
        try mlx.check(c.mlx_fast_metal_kernel_apply(&out_vec, kernel, in_vec, cfg, request.stream));
        if (c.mlx_vector_array_size(out_vec) < 1) return null;

        var output = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&output, out_vec, 0));
        return output;
    }

    fn lmHeadArgmax(ctx: *anyopaque, request: *const LmHeadArgmaxRequest) !?c.mlx_array {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        if (request.rows == 0 or request.in_dim == 0 or request.out_dim == 0) return null;

        const vocab_block: usize = 128;
        const partial_blocks = (request.out_dim + vocab_block - 1) / vocab_block;

        const partial_cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(partial_cfg);
        const partial_shape = [_]c_int{@intCast(partial_blocks)};
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(partial_cfg, &partial_shape, partial_shape.len, c.MLX_FLOAT32));
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(partial_cfg, &partial_shape, partial_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(partial_cfg, "Rows", request.rows);
        try addTemplateInt(partial_cfg, "InDim", request.in_dim);
        try addTemplateInt(partial_cfg, "OutDim", request.out_dim);
        try addTemplateInt(partial_cfg, "VocabBlock", vocab_block);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(partial_cfg, @intCast(partial_blocks), 1, 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(partial_cfg, 1, 1, 1));

        const partial_inputs = [_]c.mlx_array{ request.hidden, request.weight };
        const partial_in_vec = c.mlx_vector_array_new_data(&partial_inputs, partial_inputs.len);
        defer _ = c.mlx_vector_array_free(partial_in_vec);

        var partial_out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(partial_out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&partial_out_vec, self.lm_head_argmax_partials, partial_in_vec, partial_cfg, request.stream));
        if (c.mlx_vector_array_size(partial_out_vec) < 2) return null;

        var partial_scores = c.mlx_array_new();
        defer _ = c.mlx_array_free(partial_scores);
        var partial_ids = c.mlx_array_new();
        defer _ = c.mlx_array_free(partial_ids);
        try mlx.check(c.mlx_vector_array_get(&partial_scores, partial_out_vec, 0));
        try mlx.check(c.mlx_vector_array_get(&partial_ids, partial_out_vec, 1));

        const reduce_cfg = c.mlx_fast_metal_kernel_config_new();
        defer c.mlx_fast_metal_kernel_config_free(reduce_cfg);
        const token_shape = [_]c_int{1};
        try mlx.check(c.mlx_fast_metal_kernel_config_add_output_arg(reduce_cfg, &token_shape, token_shape.len, c.MLX_FLOAT32));
        try addTemplateInt(reduce_cfg, "NumBlocks", partial_blocks);
        try mlx.check(c.mlx_fast_metal_kernel_config_set_grid(reduce_cfg, 1, 1, 1));
        try mlx.check(c.mlx_fast_metal_kernel_config_set_thread_group(reduce_cfg, 1, 1, 1));

        const reduce_inputs = [_]c.mlx_array{ partial_scores, partial_ids };
        const reduce_in_vec = c.mlx_vector_array_new_data(&reduce_inputs, reduce_inputs.len);
        defer _ = c.mlx_vector_array_free(reduce_in_vec);

        var reduce_out_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(reduce_out_vec);
        try mlx.check(c.mlx_fast_metal_kernel_apply(&reduce_out_vec, self.lm_head_argmax_reduce, reduce_in_vec, reduce_cfg, request.stream));
        if (c.mlx_vector_array_size(reduce_out_vec) < 1) return null;

        var token_id = c.mlx_array_new();
        try mlx.check(c.mlx_vector_array_get(&token_id, reduce_out_vec, 0));
        return token_id;
    }

    pub fn deinitOwned(self: *MetalProvider) void {
        self.resetGatheredSpans();
        for (0..decoder_runtime_linear_slot_capacity) |slot| self.clearRawLinearSlot(slot);
        for (0..decoder_runtime_layer_norm_slot_capacity) |slot| {
            if (self.raw_layer_norm_slot_weights[slot]) |*t| t.deinit();
            self.raw_layer_norm_slot_weights[slot] = null;
            if (self.raw_layer_norm_slot_biases[slot]) |*t| t.deinit();
            self.raw_layer_norm_slot_biases[slot] = null;
        }
        for (0..decoder_runtime_rms_norm_slot_capacity) |slot| {
            if (self.raw_rms_norm_slot_weights[slot]) |*t| t.deinit();
            self.raw_rms_norm_slot_weights[slot] = null;
        }
        termite_metal_provider_destroy(self.raw_provider);
        termite_metal_decode_runtime_destroy(self.raw_decode_runtime);
        c.mlx_fast_metal_kernel_free(self.q4_0);
        c.mlx_fast_metal_kernel_free(self.q4_0_grouped);
        c.mlx_fast_metal_kernel_free(self.q4_1);
        c.mlx_fast_metal_kernel_free(self.q4_1_grouped);
        c.mlx_fast_metal_kernel_free(self.q5_0);
        c.mlx_fast_metal_kernel_free(self.q5_0_grouped);
        c.mlx_fast_metal_kernel_free(self.q5_1);
        c.mlx_fast_metal_kernel_free(self.q5_1_grouped);
        c.mlx_fast_metal_kernel_free(self.q8_0);
        c.mlx_fast_metal_kernel_free(self.q8_0_grouped);
        c.mlx_fast_metal_kernel_free(self.q8_1);
        c.mlx_fast_metal_kernel_free(self.q8_1_grouped);
        c.mlx_fast_metal_kernel_free(self.iq4_nl);
        c.mlx_fast_metal_kernel_free(self.iq4_nl_grouped);
        c.mlx_fast_metal_kernel_free(self.iq4_xs);
        c.mlx_fast_metal_kernel_free(self.iq4_xs_grouped);
        c.mlx_fast_metal_kernel_free(self.q2_k);
        c.mlx_fast_metal_kernel_free(self.q3_k);
        c.mlx_fast_metal_kernel_free(self.q4_k);
        c.mlx_fast_metal_kernel_free(self.q4_k_pair);
        c.mlx_fast_metal_kernel_free(self.q4_k_grouped);
        c.mlx_fast_metal_kernel_free(self.q5_k);
        c.mlx_fast_metal_kernel_free(self.q5_k_pair);
        c.mlx_fast_metal_kernel_free(self.q5_k_grouped);
        c.mlx_fast_metal_kernel_free(self.q5_k_grouped_tiled);
        c.mlx_fast_metal_kernel_free(self.q5_k_grouped_pair);
        c.mlx_fast_metal_kernel_free(self.q6_k);
        c.mlx_fast_metal_kernel_free(self.q6_k_grouped);
        c.mlx_fast_metal_kernel_free(self.q8_k);
        c.mlx_fast_metal_kernel_free(self.i8_s);
        c.mlx_fast_metal_kernel_free(self.i2_s);
        c.mlx_fast_metal_kernel_free(self.i2_s_pair);
        c.mlx_fast_metal_kernel_free(self.tl1);
        c.mlx_fast_metal_kernel_free(self.tl2);
        c.mlx_fast_metal_kernel_free(self.polar4_key_scores);
        c.mlx_fast_metal_kernel_free(self.turbo3_key_scores);
        c.mlx_fast_metal_kernel_free(self.polar4_attention_block);
        c.mlx_fast_metal_kernel_free(self.turbo3_attention_block);
        c.mlx_fast_metal_kernel_free(self.polar4_attention_span);
        c.mlx_fast_metal_kernel_free(self.turbo3_attention_span);
        c.mlx_fast_metal_kernel_free(self.polar4_attention_span_partials);
        c.mlx_fast_metal_kernel_free(self.turbo3_attention_span_partials);
        c.mlx_fast_metal_kernel_free(self.attention_span_reduce);
        c.mlx_fast_metal_kernel_free(self.lm_head_argmax_partials);
        c.mlx_fast_metal_kernel_free(self.lm_head_argmax_reduce);
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *MetalProvider = @ptrCast(@alignCast(ctx));
        self.deinitOwned();
        std.heap.c_allocator.destroy(self);
    }

    pub const vtable = Provider.VTable{
        .planLinearNoBias = &planLinearNoBias,
        .linearNoBias = &linearNoBias,
        .planLinearNoBiasPair = &planLinearNoBiasPair,
        .linearNoBiasPair = &linearNoBiasPair,
        .mulMatId = &mulMatId,
        .moeLinearNoBias = &moeLinearNoBias,
        .moeLinearNoBiasPair = &moeLinearNoBiasPair,
        .compressedKeyScores = &compressedKeyScores,
        .compressedAttentionBlock = &compressedAttentionBlock,
        .compressedAttentionSpan = &compressedAttentionSpan,
        .lmHeadArgmax = &lmHeadArgmax,
        .decoderRuntimePrepareGreedy = &decoderRuntimePrepareGreedy,
        .decoderRuntimeResetState = &decoderRuntimeResetState,
        .decoderRuntimePrepareAbsoluteEmbeddings = &decoderRuntimePrepareAbsoluteEmbeddings,
        .decoderRuntimeEmbedAbsolutePosition = &decoderRuntimeEmbedAbsolutePosition,
        .decoderRuntimePrepareLayerNorm = &decoderRuntimePrepareLayerNorm,
        .decoderRuntimeApplyLayerNorm = &decoderRuntimeApplyLayerNorm,
        .decoderRuntimePrepareRmsNorm = &decoderRuntimePrepareRmsNorm,
        .decoderRuntimeApplyRmsNorm = &decoderRuntimeApplyRmsNorm,
        .decoderRuntimeApplyLayerNormLinear = &decoderRuntimeApplyLayerNormLinear,
        .decoderRuntimeApplyLayerNormLinearArgmax = &decoderRuntimeApplyLayerNormLinearArgmax,
        .decoderRuntimeApplyLayerNormLinearSample = &decoderRuntimeApplyLayerNormLinearSample,
        .decoderRuntimeApplyRmsNormLinear = &decoderRuntimeApplyRmsNormLinear,
        .decoderRuntimeApplyRmsNormLinearArgmax = &decoderRuntimeApplyRmsNormLinearArgmax,
        .decoderRuntimeApplyRmsNormLinearSample = &decoderRuntimeApplyRmsNormLinearSample,
        .sampleLogitsDevice = &sampleLogitsDevice,
        .decoderRuntimePrepareLinear = &decoderRuntimePrepareLinear,
        .decoderRuntimeApplyLinear = &decoderRuntimeApplyLinear,
        .decoderRuntimeApplyLinearArgmax = &decoderRuntimeApplyLinearArgmax,
        .decoderRuntimeApplyLinearPair = &decoderRuntimeApplyLinearPair,
        .decoderRuntimeApplyLinearQkv = &decoderRuntimeApplyLinearQkv,
        .decoderRuntimeApplyActivation = &decoderRuntimeApplyActivation,
        .decoderRuntimeApplyAdd = &decoderRuntimeApplyAdd,
        .runDenseFfnResidual = &runDenseFfnResidual,
        .runGatedFfnResidual = &runGatedFfnResidual,
        .runAttentionResidualPostLinear = &runAttentionResidualPostLinear,
        .runCompressedAttentionDenseDecoderBlock = &runCompressedAttentionDenseDecoderBlock,
        .runCompressedAttentionGatedDecoderBlock = &runCompressedAttentionGatedDecoderBlock,
        .hasGatheredSpanCache = &hasGatheredSpanCache,
        .deinit = &deinit,
    };
} else struct {};
