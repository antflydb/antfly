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

// Abstract compute operations for neural network inference.
//
// Provides a backend-agnostic interface over tensor operations. Model
// architectures (BERT, T5, GPT) call these ops without knowing whether
// the underlying execution uses native CPU math, MLX, or anything else.
//
// Inspired by llama.cpp's GGML: models build computation, backends execute.

const std = @import("std");
const runtime = @import("../runtime/root.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const operator_plan = @import("../graph/operator_plan.zig");
const tensor_mod = @import("../backends/tensor.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const gpt_model = @import("../models/gpt.zig");
const ml = @import("ml");

/// Opaque tensor handle. The concrete type depends on the compute backend
/// (native CPU uses f32 slices, MLX uses mlx_array handles). Tensors are always
/// freed via the ComputeBackend that created them.
pub const CT = backend_contracts.CT;

pub const UnaryConsumeOp = enum {
    gelu,
    relu,
    silu,
    quick_gelu,
    sigmoid,
    tanh_act,
    negate,
    sqrt,
    rsqrt,
    exp,
    log,
    sin,
    cos,
    tanh_prim,
    erf,
    abs,
};

pub const BackendKind = backend_contracts.BackendKind;
pub const GraphDType = ml.graph.DType;
pub const OperatorPlan = operator_plan.OperatorPlan;

pub const LinearNoBiasPairResult = struct {
    first: CT,
    second: CT,
};

pub const LinearPairResult = struct {
    first: CT,
    second: CT,
};

pub const LinearTripleResult = struct {
    first: CT,
    second: CT,
    third: CT,
};

pub const LinearNoBiasTripleResult = struct {
    first: CT,
    second: CT,
    third: CT,
};

pub const LinearPlannedRequest = struct {
    input: CT,
    weight: CT,
    bias: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    operator_plan: OperatorPlan,
};

pub const LinearNoBiasPlannedRequest = struct {
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    operator_plan: OperatorPlan,
};

pub const SplitLastDim3Result = struct {
    first: CT,
    second: CT,
    third: CT,
};

pub const MulMatIdRequest = struct {
    input: CT,
    expert_ids: []const u32,
    expert_tile_ids: ?[]const u32 = null,
    tile_row_starts: ?[]const u32 = null,
    tile_row_counts: ?[]const u32 = null,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

pub const MoeLinearNoBiasRequest = MulMatIdRequest;

pub const MoeScatterAddRequest = struct {
    base: CT,
    row_ids: []const u32,
    row_weights: []const f32,
    updates: CT,
    rows: usize,
    dim: usize,
};

pub const MoeRouteSelection = struct {
    expert_ids: []u32,
    route_weights: []f32,
    rows: usize,
    top_k: usize,
};

pub const MoeLinearNoBiasPairResult = struct {
    first: CT,
    second: CT,
};

pub const ExportTensorData = struct {
    dtype: tensor_mod.DType,
    payload: union(enum) {
        bytes: []u8,
        quantized_f32: struct {
            tensor_type: gguf_tensor_types.TensorType,
            raw_bytes: []u8,
            shape: []i64,
        },
    },
};

pub const SampleLastRowRequest = struct {
    tensor: CT,
    rows: usize,
    dim: usize,
    temperature: f32,
    top_k: usize,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32 = 1.0,
    frequency_penalty: f32 = 0.0,
    presence_penalty: f32 = 0.0,
    token_history: []const i64 = &.{},
};

pub const TakeRowsRequest = struct {
    input: CT,
    row_ids: []const u32,
    rows: usize,
    dim: usize,
};

pub const GlinerWordEmbeddingsRequest = struct {
    hidden: CT,
    words_mask: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
    num_words: usize,
};

pub const GlinerLabelGruCombinedRequest = struct {
    label_embeddings: CT,
    num_labels: usize,
    hidden_size: usize,
};

pub const DenseMlp2Request = struct {
    input: CT,
    first_weight: CT,
    first_bias: CT,
    second_weight: CT,
    second_bias: CT,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    out_dim: usize,
    activation: DecoderRuntimeActivationKind = .relu,
};

pub const DenseFfnLayerNormRequest = struct {
    input: CT,
    residual: CT,
    first_weight: CT,
    first_bias: CT,
    second_weight: CT,
    second_bias: CT,
    layer_norm_weight: CT,
    layer_norm_bias: CT,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    eps: f32,
    activation: DecoderRuntimeActivationKind = .gelu,
};

pub const DenseLinearLayerNormRequest = struct {
    input: CT,
    residual: CT,
    weight: CT,
    bias: CT,
    layer_norm_weight: CT,
    layer_norm_bias: CT,
    rows: usize,
    in_dim: usize,
    hidden_size: usize,
    eps: f32,
};

pub const DebertaEmbeddingsRequest = struct {
    word_embeddings: CT,
    layer_norm_weight: CT,
    layer_norm_bias: CT,
    input_ids: []const i64,
    attention_mask: []const i64,
    total: usize,
    hidden_size: usize,
    eps: f32,
};

/// Fused MoE forward: route selection + expert compute + scatter-add,
/// entirely on GPU with no CPU round-trips for routing.
pub const MoeForwardFusedRequest = struct {
    input: CT, // [total, hidden_size]
    router_logits: CT, // [total, num_experts]
    w1: CT, // gate weight (packed experts)
    w3: CT, // up weight (packed experts)
    w2: CT, // down weight (packed experts)
    expert_scale: ?CT, // per-expert output scale [num_experts], or null
    total: usize,
    hidden_size: usize,
    inter_size: usize,
    num_experts: usize,
    top_k: usize,
};

pub const RunMoeBlockRequest = MoeForwardFusedRequest;

pub const DecoderRuntimeGreedyRequest = backend_contracts.DecoderRuntimeGreedyRequest;
pub const DecoderRuntimePrepareAbsoluteEmbeddingsRequest = backend_contracts.DecoderRuntimePrepareAbsoluteEmbeddingsRequest;
pub const DecoderRuntimeEmbedAbsolutePositionRequest = backend_contracts.DecoderRuntimeEmbedAbsolutePositionRequest;
pub const DecoderRuntimePrepareLayerNormRequest = backend_contracts.DecoderRuntimePrepareLayerNormRequest;
pub const DecoderRuntimeEnsureLayerNormSlotRequest = backend_contracts.DecoderRuntimeEnsureLayerNormSlotRequest;
pub const DecoderRuntimeApplyLayerNormRequest = backend_contracts.DecoderRuntimeApplyLayerNormRequest;
pub const DecoderRuntimePrepareRmsNormRequest = backend_contracts.DecoderRuntimePrepareRmsNormRequest;
pub const DecoderRuntimeEnsureRmsNormSlotRequest = backend_contracts.DecoderRuntimeEnsureRmsNormSlotRequest;
pub const DecoderRuntimeApplyRmsNormRequest = backend_contracts.DecoderRuntimeApplyRmsNormRequest;
pub const DecoderRuntimeApplyLayerNormLinearArgmaxRequest = backend_contracts.DecoderRuntimeApplyLayerNormLinearArgmaxRequest;
pub const DecoderRuntimeApplyLayerNormLinearRequest = backend_contracts.DecoderRuntimeApplyLayerNormLinearRequest;
pub const DecoderRuntimeApplyRmsNormLinearArgmaxRequest = backend_contracts.DecoderRuntimeApplyRmsNormLinearArgmaxRequest;
pub const DecoderRuntimeApplyRmsNormLinearRequest = backend_contracts.DecoderRuntimeApplyRmsNormLinearRequest;
pub const DecoderRuntimeApplyLayerNormLinearSampleRequest = backend_contracts.DecoderRuntimeApplyLayerNormLinearSampleRequest;
pub const DecoderRuntimeApplyRmsNormLinearSampleRequest = backend_contracts.DecoderRuntimeApplyRmsNormLinearSampleRequest;
pub const DecoderRuntimePrepareLinearRequest = backend_contracts.DecoderRuntimePrepareLinearRequest;
pub const DecoderRuntimeEnsureLinearSlotRequest = backend_contracts.DecoderRuntimeEnsureLinearSlotRequest;
pub const DecoderRuntimeApplyLinearRequest = backend_contracts.DecoderRuntimeApplyLinearRequest;
pub const DecoderRuntimeApplyLinearArgmaxRequest = backend_contracts.DecoderRuntimeApplyLinearArgmaxRequest;
pub const DecoderRuntimeApplyLinearPairRequest = backend_contracts.DecoderRuntimeApplyLinearPairRequest;
pub const DecoderRuntimeApplyLinearQkvRequest = backend_contracts.DecoderRuntimeApplyLinearQkvRequest;
pub const DecoderRuntimeActivationKind = backend_contracts.DecoderRuntimeActivationKind;
pub const DecoderRuntimeApplyActivationRequest = backend_contracts.DecoderRuntimeApplyActivationRequest;
pub const DecoderRuntimeApplyAddRequest = backend_contracts.DecoderRuntimeApplyAddRequest;
pub const DecoderRuntimeApplyAddScaleRequest = backend_contracts.DecoderRuntimeApplyAddScaleRequest;
pub const DecoderRuntimeApplyScaledAddScaleRequest = backend_contracts.DecoderRuntimeApplyScaledAddScaleRequest;
pub const RunDenseFfnResidualRequest = backend_contracts.RunDenseFfnResidualRequest;
pub const RunGatedFfnResidualRequest = backend_contracts.RunGatedFfnResidualRequest;
pub const RunAttentionRequest = backend_contracts.RunAttentionRequest;
pub const DeepSeekV4CompressedAttentionPath = backend_contracts.DeepSeekV4CompressedAttentionPath;
pub const DeepSeekV4CompressedComponentRequest = backend_contracts.DeepSeekV4CompressedComponentRequest;
pub const DeepSeekV4CompressedAttentionRequest = backend_contracts.DeepSeekV4CompressedAttentionRequest;
pub const SeedPagedAttentionSpanRequest = backend_contracts.SeedPagedAttentionSpanRequest;
pub const RunAttentionResidualRequest = backend_contracts.RunAttentionResidualRequest;
pub const RunAttentionOutputResidualRequest = backend_contracts.RunAttentionOutputResidualRequest;
pub const RunDenseDecoderBlockRequest = backend_contracts.RunDenseDecoderBlockRequest;
pub const RunGatedDecoderBlockRequest = backend_contracts.RunGatedDecoderBlockRequest;
pub const PlannedLayerContract = backend_contracts.PlannedLayerContract;
pub const PlannedFrameLayerWindow = backend_contracts.PlannedFrameLayerWindow;
pub const PlannedCommandOp = backend_contracts.PlannedCommandOp;

pub const AttentionMode = backend_contracts.AttentionMode;
pub const AttentionSinkMetadata = backend_contracts.AttentionSinkMetadata;
pub const KvCacheView = backend_contracts.KvCacheView;
pub const KvBatchView = backend_contracts.KvBatchView;
pub const PagedKvLayerCacheRows = backend_contracts.PagedKvLayerCacheRows;
pub const DirectFamilyTimingSnapshot = backend_contracts.DirectFamilyTimingSnapshot;

pub const NativeQuantTimingStats = struct {
    calls: u64 = 0,
    pair_calls: u64 = 0,
    grouped_calls: u64 = 0,
    grouped_q5_k_calls: u64 = 0,
    grouped_q6_k_calls: u64 = 0,
    grouped_q5_k_expand_calls: u64 = 0,
    grouped_q5_k_down_calls: u64 = 0,
    grouped_q6_k_expand_calls: u64 = 0,
    grouped_q6_k_down_calls: u64 = 0,
    prepare_calls: u64 = 0,
    prepare_cache_hits: u64 = 0,
    decoder_runtime_prepare_calls: u64 = 0,
    decoder_runtime_embed_calls: u64 = 0,
    decoder_runtime_prepare_layer_norm_calls: u64 = 0,
    decoder_runtime_apply_layer_norm_calls: u64 = 0,
    decoder_runtime_prepare_linear_calls: u64 = 0,
    decoder_runtime_prepare_linear_first_provider_ptr: usize = 0,
    decoder_runtime_prepare_linear_runtime_not_ready: u64 = 0,
    decoder_runtime_prepare_linear_bias_shape_failures: u64 = 0,
    decoder_runtime_prepare_linear_packed_expert_failures: u64 = 0,
    decoder_runtime_prepare_linear_weight_dtype_failures: u64 = 0,
    decoder_runtime_prepare_linear_weight_ndim_failures: u64 = 0,
    decoder_runtime_prepare_linear_weight_shape_failures: u64 = 0,
    decoder_runtime_apply_linear_calls: u64 = 0,
    decoder_runtime_apply_linear_first_provider_ptr: usize = 0,
    decoder_runtime_apply_linear_not_prepared: u64 = 0,
    decoder_runtime_apply_linear_first_not_prepared_slot: usize = std.math.maxInt(usize),
    decoder_runtime_apply_linear_dim_mismatch: u64 = 0,
    decoder_runtime_apply_linear_input_shape_failures: u64 = 0,
    decoder_runtime_apply_linear_quantized_storage_nulls: u64 = 0,
    decoder_runtime_apply_linear_quantized_weight_nulls: u64 = 0,
    decoder_runtime_apply_linear_dense_cache_nulls: u64 = 0,
    decoder_runtime_frame_begins: u64 = 0,
    decoder_runtime_frame_submits: u64 = 0,
    decoder_runtime_frame_wait_nanos: u128 = 0,
    decoder_runtime_frame_gpu_nanos: u128 = 0,
    metal_tensor_device_owned_buffers_created: u64 = 0,
    metal_tensor_device_owned_buffers_released: u64 = 0,
    metal_tensor_device_owned_live_bytes: u64 = 0,
    metal_tensor_device_owned_peak_live_bytes: u64 = 0,
    metal_tensor_host_mirror_allocations: u64 = 0,
    metal_tensor_host_mirror_frees: u64 = 0,
    metal_tensor_host_mirror_live_bytes: u64 = 0,
    metal_tensor_host_mirror_peak_live_bytes: u64 = 0,
    metal_tensor_host_mirror_download_bytes: u64 = 0,
    metal_tensor_to_host_calls: u64 = 0,
    metal_tensor_to_host_device_calls: u64 = 0,
    metal_runtime_buffer_count: u64 = 0,
    metal_runtime_total_bytes: u64 = 0,
    metal_runtime_embedding_bytes: u64 = 0,
    metal_runtime_norm_bytes: u64 = 0,
    metal_runtime_dense_linear_bytes: u64 = 0,
    metal_runtime_dense_linear_buffer_count: u64 = 0,
    metal_runtime_dense_linear_largest_slot: u64 = 0,
    metal_runtime_dense_linear_largest_bytes: u64 = 0,
    metal_runtime_dense_linear_largest_in_dim: u64 = 0,
    metal_runtime_dense_linear_largest_out_dim: u64 = 0,
    metal_runtime_dense_linear_weight_bytes: u64 = 0,
    metal_runtime_dense_linear_f32_weight_bytes: u64 = 0,
    metal_runtime_dense_linear_bf16_weight_bytes: u64 = 0,
    metal_runtime_dense_linear_f32_slots: u64 = 0,
    metal_runtime_dense_linear_bf16_slots: u64 = 0,
    metal_runtime_quant_linear_bytes: u64 = 0,
    metal_runtime_scratch_bytes: u64 = 0,
    metal_runtime_scratch_pool_bytes: u64 = 0,
    metal_runtime_scratch_pool_slots: u64 = 0,
    metal_runtime_scratch_pool_in_use_slots: u64 = 0,
    metal_runtime_scratch_pool_pending_slots: u64 = 0,
    metal_runtime_attention_span_bytes: u64 = 0,
    metal_runtime_hidden_state_bytes: u64 = 0,
    metal_runtime_frame_retained_bytes: u64 = 0,
    metal_runtime_graph_plan_bytes: u64 = 0,
    metal_runtime_graph_plan_slots: u64 = 0,
    metal_runtime_graph_plan_active: u64 = 0,
    metal_runtime_graph_plan_count: u64 = 0,
    metal_runtime_graph_plan_allocations: u64 = 0,
    metal_runtime_graph_plan_reuses: u64 = 0,
    metal_runtime_deberta_encoder_frame_plan_attempts: u64 = 0,
    metal_runtime_deberta_encoder_frame_plan_successes: u64 = 0,
    metal_runtime_deberta_encoder_frame_plan_reuses: u64 = 0,
    metal_runtime_deberta_encoder_frame_plan_failures: u64 = 0,
    metal_runtime_deberta_embeddings_attempts: u64 = 0,
    metal_runtime_deberta_embeddings_successes: u64 = 0,
    metal_runtime_deberta_embeddings_fallbacks: u64 = 0,
    metal_runtime_deberta_encoder_layer_attempts: u64 = 0,
    metal_runtime_deberta_encoder_layer_successes: u64 = 0,
    metal_runtime_deberta_encoder_layer_fallbacks: u64 = 0,
    metal_runtime_deberta_relative_qk_pair_calls: u64 = 0,
    metal_runtime_deberta_relative_qk_pair_fallbacks: u64 = 0,
    metal_runtime_dense_qkv_packed_calls: u64 = 0,
    metal_runtime_dense_qkv_packed_fallbacks: u64 = 0,
    metal_runtime_dense_pair_packed_calls: u64 = 0,
    metal_runtime_dense_pair_packed_fallbacks: u64 = 0,
    metal_runtime_mps_dense_linear_standalone_calls: u64 = 0,
    metal_runtime_mps_dense_linear_active_frame_calls: u64 = 0,
    metal_runtime_mps_dense_linear_standalone_wait_nanos: u64 = 0,
    metal_runtime_mps_dense_linear_standalone_gpu_nanos: u64 = 0,
    metal_runtime_last_frame_mps_dense_linear_count: u64 = 0,
    metal_runtime_deberta_ffn_fused_calls: u64 = 0,
    metal_runtime_deberta_ffn_fused_mps_matmuls: u64 = 0,
    metal_runtime_deberta_ffn_fused_fallbacks: u64 = 0,
    metal_runtime_deberta_attention_flash_calls: u64 = 0,
    metal_runtime_deberta_attention_legacy_calls: u64 = 0,
    metal_runtime_deberta_attention_gemm_calls: u64 = 0,
    metal_runtime_deberta_attention_gemm_fallbacks: u64 = 0,
    metal_runtime_mpsgraph_ffn_calls: u64 = 0,
    metal_runtime_mpsgraph_ffn_fallbacks: u64 = 0,
    metal_runtime_mpsgraph_ffn_compiles: u64 = 0,
    metal_runtime_mpsgraph_ffn_cache_hits: u64 = 0,
    metal_runtime_compute_encoder_count: u64 = 0,
    metal_runtime_blit_encoder_count: u64 = 0,
    metal_runtime_last_frame_compute_encoder_count: u64 = 0,
    metal_runtime_last_frame_blit_encoder_count: u64 = 0,
    metal_runtime_last_frame_planned_compute_scope_count: u64 = 0,
    metal_runtime_last_frame_planned_barrier_count: u64 = 0,
    metal_runtime_last_frame_compute_quant_linear_count: u64 = 0,
    metal_runtime_last_frame_compute_quant_qkv_count: u64 = 0,
    metal_runtime_last_frame_compute_quant_pair_act_count: u64 = 0,
    metal_runtime_last_frame_compute_attention_count: u64 = 0,
    metal_runtime_last_frame_compute_rms_norm_count: u64 = 0,
    metal_runtime_last_frame_compute_head_rope_count: u64 = 0,
    metal_runtime_last_frame_compute_ffn_count: u64 = 0,
    metal_runtime_last_frame_compute_ple_count: u64 = 0,
    metal_runtime_last_frame_compute_tail_count: u64 = 0,
    metal_runtime_last_frame_compute_embedding_count: u64 = 0,
    metal_runtime_last_frame_compute_dense_linear_count: u64 = 0,
    metal_runtime_last_frame_compute_layer_count: u64 = 0,
    metal_runtime_last_frame_compute_other_count: u64 = 0,
    metal_runtime_last_frame_compute_region_attention_count: u64 = 0,
    metal_runtime_last_frame_compute_region_attention_project_count: u64 = 0,
    metal_runtime_last_frame_compute_region_ffn_norm_count: u64 = 0,
    metal_runtime_last_frame_compute_region_ffn_count: u64 = 0,
    metal_runtime_last_frame_compute_region_ple_count: u64 = 0,
    metal_runtime_last_frame_compute_region_tail_count: u64 = 0,
    metal_runtime_last_frame_compute_region_embedding_count: u64 = 0,
    metal_runtime_last_frame_compute_region_layer_count: u64 = 0,
    metal_runtime_last_frame_compute_region_other_count: u64 = 0,
    metal_runtime_last_frame_planned_command_op_count: u64 = 0,
    metal_runtime_last_frame_planned_command_op_kind_counts: [32]u64 = [_]u64{0} ** 32,
    metal_runtime_last_frame_planned_command_operator_counts: [16]u64 = [_]u64{0} ** 16,
    metal_runtime_last_frame_planned_command_quant_dispatch_counts: [4]u64 = [_]u64{0} ** 4,
    metal_runtime_last_frame_blit_buffer_upload_count: u64 = 0,
    metal_runtime_last_frame_blit_buffer_copy_count: u64 = 0,
    metal_runtime_last_frame_blit_buffer_slice_count: u64 = 0,
    metal_runtime_last_frame_blit_attention_span_count: u64 = 0,
    metal_runtime_last_frame_blit_ffn_copy_count: u64 = 0,
    metal_runtime_last_frame_blit_embedding_count: u64 = 0,
    metal_runtime_last_frame_blit_other_count: u64 = 0,
    metal_runtime_q8_0_linear_dispatch_scalar: u64 = 0,
    metal_runtime_q8_0_linear_dispatch_mmv: u64 = 0,
    metal_runtime_q8_0_linear_dispatch_small_batch: u64 = 0,
    metal_runtime_q8_0_linear_dispatch_mm: u64 = 0,
    metal_runtime_q8_0_linear_rows_1: u64 = 0,
    metal_runtime_q8_0_linear_rows_2_8: u64 = 0,
    metal_runtime_q8_0_linear_rows_9_64: u64 = 0,
    metal_runtime_q8_0_linear_rows_65_plus: u64 = 0,
    metal_runtime_q8_0_pair_activation_mm_f16_output: u64 = 0,
    metal_runtime_q8_0_linear_mm_f16_input: u64 = 0,
    metal_runtime_q8_0_pair_activation_rms_scale_mmv_f16_output: u64 = 0,
    metal_runtime_q8_0_linear_mmv_f16_input: u64 = 0,
    metal_runtime_q8_0_linear_family_dispatch_counts: [12][4]u64 = [_][4]u64{[_]u64{0} ** 4} ** 12,
    metal_provider_quantized_slots: u64 = 0,
    metal_provider_quantized_raw_bytes: u64 = 0,
    metal_provider_quantized_raw_owned_bytes: u64 = 0,
    metal_provider_quantized_prepared_bytes: u64 = 0,
    metal_provider_quantized_runtime_prepared_slots: u64 = 0,
    metal_provider_quantized_runtime_prepared_bytes: u64 = 0,
    metal_provider_quantized_runtime_private_slots: u64 = 0,
    metal_provider_quantized_runtime_private_bytes: u64 = 0,
    metal_provider_quantized_runtime_mapped_slots: u64 = 0,
    metal_provider_quantized_runtime_mapped_bytes: u64 = 0,
    metal_provider_quantized_runtime_mapped_attempts: u64 = 0,
    metal_provider_quantized_runtime_mapped_fallbacks: u64 = 0,
    metal_provider_quantized_runtime_mapped_failures: u64 = 0,
    metal_provider_quantized_runtime_private_nanos: u128 = 0,
    metal_provider_quantized_runtime_mapped_nanos: u128 = 0,
    metal_provider_dense_slot_host_bytes: u64 = 0,
    metal_provider_norm_slot_host_bytes: u64 = 0,
    metal_provider_gathered_span_entries: u64 = 0,
    metal_provider_gathered_span_device_bytes: u64 = 0,
    metal_provider_gathered_span_encoded_host_bytes: u64 = 0,
    decoder_runtime_apply_linear_pair_not_prepared: u64 = 0,
    decoder_runtime_apply_linear_pair_first_not_prepared_slot_a: usize = std.math.maxInt(usize),
    decoder_runtime_apply_linear_pair_first_not_prepared_slot_b: usize = std.math.maxInt(usize),
    decoder_runtime_apply_linear_pair_dim_mismatch: u64 = 0,
    decoder_runtime_apply_linear_pair_input_shape_failures: u64 = 0,
    decoder_runtime_apply_linear_pair_dense_direct_successes: u64 = 0,
    decoder_runtime_apply_linear_pair_dense_direct_failures: u64 = 0,
    decoder_runtime_apply_linear_pair_dense_delegate_failures: u64 = 0,
    decoder_runtime_apply_activation_calls: u64 = 0,
    decoder_runtime_apply_add_calls: u64 = 0,
    decoder_runtime_attention_span_calls: u64 = 0,
    decoder_runtime_prepare_embed_nanos: u128 = 0,
    decoder_runtime_prepare_layer_norm_nanos: u128 = 0,
    decoder_runtime_prepare_linear_nanos: u128 = 0,
    slice_calls: u64 = 0,
    prepared_view_cache_hits: u64 = 0,
    prepared_view_cache_misses: u64 = 0,
    prepared_view_owned_materializations: u64 = 0,
    decoder_runtime_pair_direct_successes: u64 = 0,
    decoder_runtime_pair_direct_failures: u64 = 0,
    decoder_runtime_pair_backend_fallbacks: u64 = 0,
    decoder_runtime_pair_non_i2s: u64 = 0,
    grouped_weight_setup_nanos: u128 = 0,
    grouped_ids_setup_nanos: u128 = 0,
    grouped_apply_nanos: u128 = 0,
    compressed_block_dense_calls: u64 = 0,
    compressed_block_gated_calls: u64 = 0,
    f32_kv_gated_block_calls: u64 = 0,
    f32_kv_gated_block_successes: u64 = 0,
    f32_kv_gated_block_nulls: u64 = 0,
    f32_kv_quant_direct_block_successes: u64 = 0,
    f32_kv_quant_direct_block_failures: u64 = 0,
    compressed_block_active_frame_f32_reroutes: u64 = 0,
    compressed_block_active_frame_bootstrap_misses: u64 = 0,
    compressed_block_project_nanos: u128 = 0,
    compressed_block_span_prep_nanos: u128 = 0,
    compressed_block_encode_nanos: u128 = 0,
    compressed_block_apply_nanos: u128 = 0,
    compressed_block_replace_span_nanos: u128 = 0,
    compressed_block_attention_span_nanos: u128 = 0,
    compressed_block_attention_prefix_nanos: u128 = 0,
    compressed_block_gated_ffn_residual_nanos: u128 = 0,
    compressed_block_command_wait_nanos: u128 = 0,
    compressed_block_gpu_nanos: u128 = 0,
    active_decode_attention_f32_kernels: u64 = 0,
    active_decode_q8_0_linear_kernels: u64 = 0,
    active_decode_q8_0_attention_linear_kernels: u64 = 0,
    active_decode_q8_0_ffn_down_linear_kernels: u64 = 0,
    active_decode_q8_0_ple_linear_kernels: u64 = 0,
    active_decode_q8_0_pair_activation_kernels: u64 = 0,
    active_decode_rms_norm_kernels: u64 = 0,
    active_decode_rms_norm_add_kernels: u64 = 0,
    active_decode_layer_norm_kernels: u64 = 0,
    active_decode_add_kernels: u64 = 0,
    active_decode_head_norm_rope_fused_kernels: u64 = 0,
    active_decode_blit_copies: u64 = 0,
    active_decode_layers: u64 = 0,
    active_decode_layer_input_direct_attempts: u64 = 0,
    active_decode_layer_input_direct_hits: u64 = 0,
    active_decode_attn_norm_ops: u64 = 0,
    active_decode_q_linear_ops: u64 = 0,
    active_decode_qkv_ops: u64 = 0,
    active_decode_head_norm_ops: u64 = 0,
    active_decode_rope_ops: u64 = 0,
    active_decode_head_norm_rope_fused_ops: u64 = 0,
    active_decode_ple_ops: u64 = 0,
    active_decode_final_fused_argmax_ops: u64 = 0,
    active_decode_final_split_argmax_ops: u64 = 0,
    active_decode_frame_attempts: u64 = 0,
    active_decode_frame_successes: u64 = 0,
    active_decode_frame_disabled: u64 = 0,
    active_decode_frame_scratch_failures: u64 = 0,
    active_decode_frame_fallbacks: u64 = 0,
    active_decode_frame_batch_fallbacks: u64 = 0,
    active_decode_frame_initial_tensor_fallbacks: u64 = 0,
    active_decode_frame_layer_fallbacks: u64 = 0,
    active_decode_frame_tail_fallbacks: u64 = 0,
    prefill_frame_plan_attempts: u64 = 0,
    prefill_frame_plan_successes: u64 = 0,
    prefill_frame_plan_failures: u64 = 0,
    prefill_frame_execute_attempts: u64 = 0,
    prefill_frame_execute_successes: u64 = 0,
    prefill_frame_execute_failures: u64 = 0,
    prefill_frame_execute_missing_ple: u64 = 0,
    prefill_frame_contract_ops: u64 = 0,
    prefill_frame_contract_scopes: u64 = 0,
    prefill_frame_contract_barriers: u64 = 0,
    prefill_frame_contract_windows: u64 = 0,
    prefill_frame_contract_full_frames: u64 = 0,
    prefill_frame_executor_layer_contracts: u64 = 0,
    prefill_frame_executor_tail_contracts: u64 = 0,
    prefill_frame_executor_local_plan_bypasses: u64 = 0,
    prefill_frame_executor_layer_runtime_calls: u64 = 0,
    prefill_frame_executor_layer_runtime_successes: u64 = 0,
    prefill_frame_executor_layer_runtime_failures: u64 = 0,
    prefill_frame_executor_layer_staged_paths: u64 = 0,
    prefill_frame_executor_layer_runtime_nanos: u128 = 0,
    prefill_frame_executor_layer_setup_nanos: u128 = 0,
    prefill_frame_executor_layer_block_nanos: u128 = 0,
    prefill_frame_executor_layer_staged_nanos: u128 = 0,
    prefill_frame_executor_scope_links: u64 = 0,
    prefill_frame_tail_contract_hits: u64 = 0,
    prefill_frame_tail_contract_misses: u64 = 0,
    prefill_frame_execute_no_runtime: u64 = 0,
    prefill_frame_execute_no_active_frame: u64 = 0,
    prefill_frame_execute_invalid_contract: u64 = 0,
    prefill_frame_execute_invalid_shape: u64 = 0,
    prefill_frame_execute_missing_plan: u64 = 0,
    prefill_frame_execute_plan_mismatch: u64 = 0,
    prefill_frame_execute_output_hidden_set: u64 = 0,
    compressed_block_quantized_attention_calls: u64 = 0,
    compressed_block_quantized_attention_nanos: u128 = 0,
    compressed_block_quantized_ffn_nanos: u128 = 0,
    quantized_gated_pair_nanos: u128 = 0,
    quantized_gated_activation_multiply_nanos: u128 = 0,
    quantized_gated_post_gate_norm_nanos: u128 = 0,
    quantized_gated_down_nanos: u128 = 0,
    quantized_gated_add_nanos: u128 = 0,
    compressed_block_gated_quantized_branch_calls: u64 = 0,
    compressed_block_gated_quantized_attention_nulls: u64 = 0,
    compressed_block_gated_quantized_attention_prefill_nulls: u64 = 0,
    compressed_block_gated_quantized_attention_decode_nulls: u64 = 0,
    compressed_block_gated_quantized_norm_nulls: u64 = 0,
    compressed_block_gated_direct_successes: u64 = 0,
    compressed_block_gated_direct_runtime_failures: u64 = 0,
    compressed_block_gated_direct_fail_replace_span: u64 = 0,
    compressed_block_gated_direct_fail_attention_span: u64 = 0,
    compressed_block_gated_direct_fail_attention_prefix: u64 = 0,
    compressed_block_gated_direct_fail_gated_ffn: u64 = 0,
    compressed_block_gated_direct_first_failure_code: i64 = 0,
    compressed_attention_residual_multi_row_calls: u64 = 0,
    compressed_attention_residual_multi_row_successes: u64 = 0,
    gathered_span_cold_miss_nulls: u64 = 0,
    gathered_span_seed_calls: u64 = 0,
    gathered_span_seed_successes: u64 = 0,
    gathered_span_decode_cache_hits: u64 = 0,
    gathered_span_decode_cache_misses: u64 = 0,
    gathered_span_same_span_hits: u64 = 0,
    gathered_span_append_hits: u64 = 0,
    gathered_span_offset_regressions: u64 = 0,
    gathered_span_prefix_token_mismatches: u64 = 0,
    gathered_span_prefix_mismatch_resets: u64 = 0,
    gathered_span_reset_rebuilds: u64 = 0,
    gathered_span_first_prefill_source_ptr: usize = 0,
    gathered_span_first_decode_source_ptr: usize = 0,
    quantized_gated_ffn_direct_successes: u64 = 0,
    quantized_gated_ffn_direct_fallbacks: u64 = 0,
    quantized_gated_ffn_backend_fallbacks: u64 = 0,
    quantized_gated_ffn_backend_mixed_kind_fallbacks: u64 = 0,
    quantized_gated_ffn_backend_unsupported_kind_fallbacks: u64 = 0,
    quantized_gated_ffn_type_mismatches: u64 = 0,
    quantized_gated_ffn_unsupported_types: u64 = 0,
    quantized_gated_ffn_runtime_failures: u64 = 0,
    compressed_attention_residual_fused_successes: u64 = 0,
    compressed_attention_residual_update_span_failures: u64 = 0,
    compressed_attention_residual_attention_span_failures: u64 = 0,
    compressed_attention_residual_post_linear_successes: u64 = 0,
    compressed_attention_residual_post_linear_failures: u64 = 0,
    compressed_attention_residual_input_eval_nanos: u128 = 0,
    compressed_attention_residual_raw_runtime_nanos: u128 = 0,
};

pub const QuantExecutionTimingStats = struct {
    backend_dense_calls: u64 = 0,
    wrapper_calls: u64 = 0,
    wrapper_packed_calls: u64 = 0,
    device_native_calls: u64 = 0,
    device_native_packed_calls: u64 = 0,
    device_native_pair_calls: u64 = 0,
    device_native_pair_packed_calls: u64 = 0,
    device_native_moe_grouped_calls: u64 = 0,
    device_native_packed_backend_weight_calls: u64 = 0,
    device_native_packed_prepared_view_calls: u64 = 0,
    packed_backend_prefetch_attempts: u64 = 0,
    packed_backend_prefetch_successes: u64 = 0,
    packed_backend_prefetch_denials: u64 = 0,
    paged_decode_calls: u64 = 0,
    paged_decode_blocks: u64 = 0,
    paged_update_kv_nanos: u128 = 0,
    paged_cache_lookup_nanos: u128 = 0,
    paged_block_setup_nanos: u128 = 0,
    paged_mask_nanos: u128 = 0,
    paged_block_apply_nanos: u128 = 0,
    paged_decode_total_nanos: u128 = 0,
    moe_grouped_recovered_packed_metadata: u64 = 0,
    moe_grouped_stage_calls: u64 = 0,
    moe_grouped_stage_bytes: u64 = 0,
    moe_grouped_stage_experts: u64 = 0,
    moe_grouped_stage_nanos: u128 = 0,
    moe_grouped_fail_not_device_native: u64 = 0,
    moe_grouped_fail_missing_quant_storage: u64 = 0,
    moe_grouped_fail_not_packed: u64 = 0,
    moe_grouped_fail_provider_null: u64 = 0,
    attn_quant_device_native_calls: u64 = 0,
    attn_quant_backend_dense_calls: u64 = 0,
    attn_quant_wrapper_calls: u64 = 0,
    attn_dense_lazy_calls: u64 = 0,
    router_quant_device_native_calls: u64 = 0,
    router_quant_backend_dense_calls: u64 = 0,
    router_quant_wrapper_calls: u64 = 0,
    lm_head_quant_device_native_calls: u64 = 0,
    lm_head_quant_backend_dense_calls: u64 = 0,
    lm_head_quant_wrapper_calls: u64 = 0,
    dense_block_fast_attempts: u64 = 0,
    dense_block_fast_no_blocks: u64 = 0,
    dense_block_fast_unsupported_format: u64 = 0,
    gated_block_fast_attempts: u64 = 0,
    gated_block_fast_no_blocks: u64 = 0,
    gated_block_fast_unsupported_format: u64 = 0,
    attention_residual_core_nanos: u128 = 0,
    attention_residual_post_linear_fast_nanos: u128 = 0,
    attention_residual_post_linear_fallback_nanos: u128 = 0,
    gated_input_projection_nanos: u128 = 0,
    gated_input_attention_nanos: u128 = 0,
    gated_input_ffn_norm_nanos: u128 = 0,
    gated_input_ffn_nanos: u128 = 0,
    gated_qkv_attention_nanos: u128 = 0,
    gated_qkv_ffn_norm_nanos: u128 = 0,
    gated_qkv_ffn_nanos: u128 = 0,
    gated_input_project_failures: u64 = 0,
    gated_input_attention_failures: u64 = 0,
    gated_input_ffn_norm_failures: u64 = 0,
    gated_input_ffn_failures: u64 = 0,
    gated_qkv_attention_failures: u64 = 0,
    gated_qkv_ffn_norm_failures: u64 = 0,
    gated_qkv_ffn_failures: u64 = 0,
};

pub const BackendDebugTimingSnapshot = struct {
    native_quant_null: bool = true,
    provider: NativeQuantTimingStats = .{},
    quant: QuantExecutionTimingStats = .{},
};

pub const DecoderRuntimePrepareReuseResult = backend_contracts.DecoderRuntimePrepareReuseResult;
pub const AttentionContext = backend_contracts.AttentionContext;
pub const DecoderRuntimeDecodeContract = backend_contracts.DecoderRuntimeDecodeContract;
pub const DecoderRuntimeDecodeMode = backend_contracts.DecoderRuntimeDecodeMode;
pub const DecoderRuntimeLayerSpec = backend_contracts.DecoderRuntimeLayerSpec;
pub const DecoderRuntimeDecodeItem = backend_contracts.DecoderRuntimeDecodeItem;
pub const DecoderRuntimeDecodeRequest = backend_contracts.DecoderRuntimeDecodeRequest;
pub const DecoderRuntimePrefillFramePlanRequest = backend_contracts.DecoderRuntimePrefillFramePlanRequest;
pub const DecoderRuntimeGraphCommandPlanFrameRequest = backend_contracts.DecoderRuntimeGraphCommandPlanFrameRequest;
pub const DebertaEncoderLayerSpec = backend_contracts.DebertaEncoderLayerSpec;
pub const DebertaEncoderFramePlanRequest = backend_contracts.DebertaEncoderFramePlanRequest;
pub const DebertaEncoderLayerRequest = backend_contracts.DebertaEncoderLayerRequest;

pub const GraphPlanSlot = struct {
    slot: usize,
    bytes: usize,
};

/// Abstract compute backend for tensor operations.
pub const ComputeBackend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub fn kind(self: *const ComputeBackend) BackendKind {
        return self.vtable.backendKind(self.ptr);
    }

    /// Install a backend-provided device-write hook on `storage` if this
    /// backend supports one (today: Metal + compressed KV dtypes). When the
    /// backend can't accelerate device writes for the storage's geometry, the
    /// call is a no-op and the storage continues using its host write path.
    pub fn provisionKvDeviceWriteHook(
        self: *const ComputeBackend,
        storage: *runtime.kv.storage_runtime.KvStorageRuntime,
    ) anyerror!void {
        const hook = self.vtable.provisionKvDeviceWriteHook orelse return;
        return hook(self.ptr, storage);
    }

    /// Returns the backend's Io if it was constructed with one.  Free
    /// functions that take `*const ComputeBackend` (web/rerank_api,
    /// runtime/kv/compaction) can use this to dispatch parallel work through
    /// the runtime's thread pool instead of the process-wide *Sync futex pool.
    pub fn getIo(self: *const ComputeBackend) ?std.Io {
        const accessor = self.vtable.getIo orelse return null;
        return accessor(self.ptr);
    }

    pub fn reserveGraphPlanSlots(self: *const ComputeBackend, slots: []const GraphPlanSlot) !bool {
        const op = self.vtable.reserveGraphPlanSlots orelse return false;
        return op(self.ptr, slots);
    }

    pub fn decoderRuntimePushPlannedComputeBarrierSuppression(self: *const ComputeBackend) !bool {
        const op = self.vtable.decoderRuntimePushPlannedComputeBarrierSuppression orelse return false;
        return op(self.ptr);
    }

    pub fn decoderRuntimePopPlannedComputeBarrierSuppression(self: *const ComputeBackend) !void {
        const op = self.vtable.decoderRuntimePopPlannedComputeBarrierSuppression orelse return;
        return op(self.ptr);
    }

    pub fn tryConvertDType(self: *const ComputeBackend, tensor: CT, target: GraphDType) !?CT {
        const op = self.vtable.convertDType orelse return null;
        return op(self.ptr, tensor, target);
    }

    pub const VTable = struct {
        backendKind: *const fn (ctx: *anyopaque) BackendKind,
        deinitBackend: *const fn (ctx: *anyopaque) void,
        freeTensor: *const fn (ctx: *anyopaque, tensor: CT) void,
        /// Optional backend-provided device-write hook installer. Metal sets
        /// this to construct a MetalKvStorage when the storage config matches
        /// its fast-path (polar4/turbo3 keys). Backends without a device KV
        /// impl leave this null.
        provisionKvDeviceWriteHook: ?*const fn (ctx: *anyopaque, storage: *runtime.kv.storage_runtime.KvStorageRuntime) anyerror!void = null,

        /// Optional accessor for the backend's stored Io.  Free functions that
        /// receive a `*const ComputeBackend` (rerank_api, kv compaction, etc.)
        /// can call `cb.getIo()` to fetch the runtime's Io and dispatch
        /// parallel work through it instead of the void *Sync escape hatches.
        /// Backends constructed without an Io return null; `getIo` itself may
        /// also be null for backends that have no notion of an Io.
        getIo: ?*const fn (ctx: *anyopaque) ?std.Io = null,

        /// Reserve backend-owned graph-plan scratch/storage slots before a
        /// partition executes. Metal maps these to persistent MTLBuffer slots.
        reserveGraphPlanSlots: ?*const fn (ctx: *anyopaque, slots: []const GraphPlanSlot) anyerror!bool = null,

        /// Temporarily suppress planned compute barriers for a backend-owned
        /// frame whose caller has stronger model-specific ordering knowledge.
        decoderRuntimePushPlannedComputeBarrierSuppression: ?*const fn (ctx: *anyopaque) anyerror!bool = null,
        decoderRuntimePopPlannedComputeBarrierSuppression: ?*const fn (ctx: *anyopaque) anyerror!void = null,

        convertDType: ?*const fn (ctx: *anyopaque, tensor: CT, target: GraphDType) anyerror!?CT = null,

        /// Look up a named weight tensor. Returned tensor is borrowed (do NOT free).
        getWeight: *const fn (ctx: *anyopaque, name: []const u8) anyerror!CT,
        prefetchWeightHint: *const fn (ctx: *anyopaque, name: []const u8, hint: u32) void,
        drainPrefetchBudget: *const fn (ctx: *anyopaque, max_items: usize) void,

        /// Embedding table lookup: weight[ids[i]] for each i. Returns [total, dim].
        embeddingLookup: *const fn (ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT,

        /// Embedding table lookup where the ids already live in a backend tensor.
        /// Backends may return null to use the host-id embedding path.
        embeddingLookupTensor: ?*const fn (ctx: *anyopaque, weight: CT, ids: CT, total: usize, dim: usize) anyerror!?CT = null,

        /// DeBERTa-v3 embedding block: word lookup + row LayerNorm + attention-mask multiply.
        /// Backends may return null to use the generic embedding/layernorm/multiply path.
        debertaEmbeddings: ?*const fn (ctx: *anyopaque, request: *const DebertaEmbeddingsRequest) anyerror!?CT = null,

        /// Gather rows from a [total, dim] tensor along axis 0.
        takeRows: ?*const fn (ctx: *anyopaque, request: *const TakeRowsRequest) anyerror!?CT = null,

        /// GLiNER-specific word embedding aggregation:
        /// averages encoder hidden rows by a host words_mask into
        /// [batch * num_words, hidden_size].
        glinerWordEmbeddings: ?*const fn (ctx: *anyopaque, request: *const GlinerWordEmbeddingsRequest) anyerror!?CT = null,

        /// GLiNER-specific CountLSTM GRU step plus skip connection:
        /// returns `gru(label_embeddings, pos0) + label_embeddings` as
        /// [num_labels, hidden_size].
        glinerLabelGruCombined: ?*const fn (ctx: *anyopaque, request: *const GlinerLabelGruCombinedRequest) anyerror!?CT = null,

        /// Y = X @ W^T + bias. X:[rows, in_dim], W:[out_dim, in_dim], bias:[out_dim] → Y:[rows, out_dim]
        linear: *const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT,

        /// Y = quick_gelu(X @ W^T + bias). Backends may fuse projection,
        /// bias, and activation; callers fall back to linear + quickGelu.
        linearQuickGelu: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT = null,

        /// Y = relu(X @ W^T + bias). Backends may fuse projection,
        /// bias, and activation; callers fall back to linear + relu.
        linearRelu: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT = null,

        /// Two-layer dense MLP: activation(X @ W1^T + b1) @ W2^T + b2.
        /// Backends may fuse scheduling and residency; callers fall back to
        /// linear + activation + linear.
        denseMlp2: ?*const fn (ctx: *anyopaque, request: *const DenseMlp2Request) anyerror!?CT = null,

        /// DeBERTa FFN block: activation(X @ W1^T + b1) @ W2^T + b2,
        /// residual add, then LayerNorm. Backends may keep the entire strip
        /// resident; callers fall back to the generic ops sequence.
        denseFfnLayerNorm: ?*const fn (ctx: *anyopaque, request: *const DenseFfnLayerNormRequest) anyerror!?CT = null,

        /// Dense projection, residual add, then LayerNorm. Used by DeBERTa
        /// attention output blocks to keep the dense+norm strip resident.
        denseLinearLayerNorm: ?*const fn (ctx: *anyopaque, request: *const DenseLinearLayerNormRequest) anyerror!?CT = null,

        /// Y = gelu(X @ W^T + bias). Backends may fuse projection,
        /// bias, and activation; callers fall back to linear + gelu.
        linearGelu: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT = null,

        /// Y = X @ W^T + bias + residual. Backends may fuse projection,
        /// bias, and residual add; callers fall back to linear + add.
        linearAdd: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, residual: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT = null,

        /// Y = layer_norm(A + B). Backends may fuse residual add and layer norm;
        /// callers fall back to add + layerNorm.
        addLayerNorm: ?*const fn (ctx: *anyopaque, a: CT, b: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!?CT = null,

        /// Planned variant for graph executors that already selected a
        /// backend-specific operator. Backends that leave this null use
        /// `linear`; callers still get the normal correctness path.
        linearPlanned: ?*const fn (ctx: *anyopaque, request: *const LinearPlannedRequest) anyerror!CT = null,

        /// Y = X @ W^T (no bias). X:[rows, in_dim], W:[out_dim, in_dim] → Y:[rows, out_dim]
        linearNoBias: *const fn (ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT,

        /// Planned no-bias linear variant. This is the hot path for quantized
        /// graph command execution because the partitioner can carry row-bucket
        /// and packed-format choices into the backend.
        linearNoBiasPlanned: ?*const fn (ctx: *anyopaque, request: *const LinearNoBiasPlannedRequest) anyerror!CT = null,

        /// Grouped no-bias linear where `weight` is already the concatenation
        /// of `num_projections` projection matrices along its row/output axis.
        /// Backends may use `projection_out_dims` to choose a grouped kernel;
        /// otherwise the wrapper falls back to `linearNoBias`.
        linearNoBiasGrouped: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize, projection_out_dims: []const u32, num_projections: usize) anyerror!CT = null,

        /// Create a float32 zero tensor with shape [rows, dim].
        zeroTensor: ?*const fn (ctx: *anyopaque, rows: usize, dim: usize) anyerror!?CT = null,

        /// Compute two no-bias linears that share the same input shape.
        /// Backends may fuse this path; otherwise the wrapper falls back to
        /// two independent `linearNoBias` calls.
        linearNoBiasPair: ?*const fn (ctx: *anyopaque, input: CT, weight_a: CT, weight_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!LinearNoBiasPairResult = null,

        /// Compute two biased linears that share the same input shape.
        /// Backends can apply bias inside their native quantized path;
        /// otherwise the wrapper falls back to no-bias pair + add or two
        /// independent `linear` calls.
        linearPair: ?*const fn (ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!LinearPairResult = null,

        /// Compute three biased linears that share the same input shape.
        /// This is the Q/K/V fast path for encoders with separate projection
        /// weights. Backends may fuse quantized kernels and bias application;
        /// otherwise the wrapper falls back to three independent `linear` calls.
        linearTriple: ?*const fn (ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, weight_c: CT, bias_c: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!LinearTripleResult = null,

        /// Y = X @ (W_base + LoRA_delta)^T + bias
        /// where LoRA_delta = (adapter_a^T * adapter_b) * (alpha / rank)
        ///
        /// X:         [rows, in_dim]
        /// W_base:    [out_dim, in_dim]  (same layout as linear's weight)
        /// bias:      [out_dim]
        /// adapter_a: [rank, in_dim]    — LoRA A matrix
        /// adapter_b: [out_dim, rank]   — LoRA B matrix  (NOTE: rows=out_dim, cols=rank)
        /// alpha:     scaling factor
        ///
        /// Returns Y: [rows, out_dim]
        ///
        /// Backends that don't implement this field get a fallback that returns
        /// error.LinearLoRANotImplemented.
        linearLoRA: ?*const fn (
            ctx: *anyopaque,
            input: CT,
            base_weight: CT,
            bias: CT,
            lora_a: CT,
            lora_b: CT,
            alpha: f32,
            rank: usize,
            rows: usize,
            in_dim: usize,
            out_dim: usize,
        ) anyerror!CT = null,

        /// Split a rank-2 [rows, 3*dim] tensor into three [rows, dim] tensors
        /// along the last dimension. Backends may implement this without a host
        /// round-trip; otherwise the wrapper falls back to toFloat32+copy.
        splitLastDim3: ?*const fn (ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!SplitLastDim3Result = null,

        /// Reshape a rank-2 tensor view from [old_rows, old_cols] to
        /// [new_rows, new_cols]. Backends may implement this as a metadata-only
        /// view; otherwise the wrapper falls back to copying through host f32.
        reshape2D: ?*const fn (ctx: *anyopaque, input: CT, old_rows: usize, old_cols: usize, new_rows: usize, new_cols: usize) anyerror!CT = null,

        /// Concatenate two rank-2 tensors along rows:
        /// a:[rows_a, cols], b:[rows_b, cols] -> [rows_a + rows_b, cols]
        concatRows2D: ?*const fn (ctx: *anyopaque, a: CT, b: CT, rows_a: usize, rows_b: usize, cols: usize) anyerror!CT = null,

        /// Slice contiguous rows from a rank-2 tensor:
        /// input:[rows, cols] -> [row_count, cols]
        sliceRows2D: ?*const fn (ctx: *anyopaque, input: CT, start_row: usize, row_count: usize, cols: usize) anyerror!CT = null,

        /// ggml_mul_mat_id-style routed matrix multiply. `input` is already
        /// grouped row-wise, `weight` is the full packed expert tensor or an
        /// equivalent backend handle, and `expert_ids[i]` selects which expert
        /// family to use for input row `i`.
        mulMatId: ?*const fn (ctx: *anyopaque, request: *const MulMatIdRequest) anyerror!?CT = null,

        /// Compatibility name for grouped MoE linear. New backend
        /// implementations should prefer `mulMatId`.
        moeLinearNoBias: ?*const fn (ctx: *anyopaque, request: *const MoeLinearNoBiasRequest) anyerror!?CT = null,
        moeLinearNoBiasPair: ?*const fn (
            ctx: *anyopaque,
            input: CT,
            expert_ids: []const u32,
            weight_a: CT,
            weight_b: CT,
            rows: usize,
            in_dim: usize,
            out_dim: usize,
        ) anyerror!?MoeLinearNoBiasPairResult = null,

        /// Weighted scatter-add for grouped MoE outputs. `updates` is [rows, dim]
        /// and is added into `base` along axis 0 using `row_ids`, after scaling
        /// each row by `row_weights[i]`.
        moeScatterAdd: ?*const fn (ctx: *anyopaque, request: *const MoeScatterAddRequest) anyerror!?CT = null,

        /// Select top-k expert ids and normalized routing weights from router
        /// logits. Returns flat row-major arrays of length rows * top_k.
        moeSelectRoutes: ?*const fn (ctx: *anyopaque, logits: CT, rows: usize, num_experts: usize, top_k: usize, allocator: std.mem.Allocator) anyerror!?MoeRouteSelection = null,

        /// Fused MoE forward: route selection + expert compute + scatter-add,
        /// entirely on GPU with no CPU round-trips for routing. Backends that
        /// don't support this return null, and the caller falls back to the
        /// existing CPU-orchestrated path.
        moeForwardFused: ?*const fn (ctx: *anyopaque, request: *const MoeForwardFusedRequest) anyerror!?CT = null,

        /// Execute a routed-expert decoder block. This is the MoE analogue of
        /// the dense/gated decoder-block contracts and should only exist where
        /// routed-expert execution is structurally distinct.
        runMoeBlock: ?*const fn (ctx: *anyopaque, request: *const RunMoeBlockRequest) anyerror!?CT = null,

        /// Store a per-expert output scale tensor for the current MoE layer.
        /// The graph backend threads this into fused_moe_scatter_add so the
        /// interpreter can apply it to route weights at execution time.
        /// Other backends ignore this (scaling is applied inline).
        setMoeExpertScale: ?*const fn (ctx: *anyopaque, scale: CT) void = null,

        /// Layer normalization over last `dim` elements.
        layerNorm: *const fn (ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT,

        /// Optional destructive layer norm that may reuse the input tensor's
        /// storage when it is uniquely owned. Callers must only use this when
        /// the input is at last use.
        layerNormConsumeInput: ?*const fn (ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!?CT = null,

        /// RMS normalization: x * rsqrt(mean(x^2) + eps) * weight. No bias, no mean subtraction.
        rmsNorm: *const fn (ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT,

        /// Optional destructive RMS norm that may reuse the input tensor's
        /// storage when it is uniquely owned. Callers must only use this when
        /// the input is at last use.
        rmsNormConsumeInput: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!?CT = null,

        /// GELU activation (element-wise).
        gelu: *const fn (ctx: *anyopaque, input: CT) anyerror!CT,

        /// Tanh-approximate GELU activation used by GGUF `gelu_pytorch_tanh`.
        geluNew: ?*const fn (ctx: *anyopaque, input: CT) anyerror!CT = null,

        /// ReLU activation (element-wise).
        relu: *const fn (ctx: *anyopaque, input: CT) anyerror!CT,

        /// SiLU/Swish activation (element-wise): x * sigmoid(x).
        silu: *const fn (ctx: *anyopaque, input: CT) anyerror!CT,

        /// Quick GELU activation (element-wise): x * sigmoid(1.702 * x).
        /// Used by CLIP and some other models as a faster GELU approximation.
        quickGelu: *const fn (ctx: *anyopaque, input: CT) anyerror!CT,

        /// Sigmoid activation (element-wise): 1 / (1 + exp(-x)).
        sigmoid: *const fn (ctx: *anyopaque, input: CT) anyerror!CT,

        /// Hyperbolic tangent activation (element-wise).
        tanh_act: *const fn (ctx: *anyopaque, input: CT) anyerror!CT,

        /// Optional destructive unary op that may reuse the input tensor's
        /// storage when it is uniquely owned. Callers must only use this when
        /// the input is at last use.
        unaryConsume: ?*const fn (ctx: *anyopaque, op: UnaryConsumeOp, input: CT) anyerror!?CT = null,

        /// Concatenate two tensors along the last dimension.
        /// a: [total, dim_a], b: [total, dim_b] → [total, dim_a + dim_b]
        concat: *const fn (ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT,

        /// Element-wise addition. Result is a new tensor; inputs are NOT freed.
        add: *const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!CT,

        /// Optional destructive addition that may reuse the left-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `a` is at last use.
        addConsumeLeft: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive addition that may reuse the right-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `b` is at last use.
        addConsumeRight: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive multiply that may reuse the left-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `a` is at last use.
        multiplyConsumeLeft: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive multiply that may reuse the right-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `b` is at last use.
        multiplyConsumeRight: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive subtraction that may reuse the left-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `a` is at last use.
        subtractConsumeLeft: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive division that may reuse the left-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `a` is at last use.
        divideConsumeLeft: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive less-than that may reuse the left-hand tensor's
        /// storage when it is uniquely owned and already matches the output
        /// size. Callers must only use this when `a` is at last use.
        lessThanConsumeLeft: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!?CT = null,

        /// Optional destructive select that may reuse the true branch storage
        /// when it is uniquely owned and already matches the output shape.
        /// Callers must only use this when `on_true` is at last use.
        whereSelectConsumeTrue: ?*const fn (ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!?CT = null,

        /// Optional destructive select that may reuse the false branch storage
        /// when it is uniquely owned and already matches the output shape.
        /// Callers must only use this when `on_false` is at last use.
        whereSelectConsumeFalse: ?*const fn (ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!?CT = null,

        /// Multi-head scaled dot-product attention (bidirectional, for encoders).
        /// Q,K,V: [batch*seq_len, num_heads*head_dim] (interleaved heads).
        /// mask: [batch, seq_len] with 0=masked, 1=attend.
        /// attn_bias: optional [num_heads, seq_len, seq_len] additive bias (e.g. T5 relative position bias), or null.
        /// Returns: [batch*seq_len, num_heads*head_dim].
        scaledDotProductAttention: *const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, mask: []const i64, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT,

        /// Optional bidirectional attention with no mask. This is equivalent
        /// to scaledDotProductAttention with an all-ones mask, but lets
        /// backends avoid a host mask allocation/upload.
        scaledDotProductAttentionFull: ?*const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!?CT = null,

        /// Causal self-attention for decoder layers.
        /// Q,K,V: [batch*seq_len, num_heads*head_dim].
        /// attn_bias: optional [num_heads, seq_len, seq_len] additive bias (e.g. relative position bias), or null.
        /// Returns: [batch*seq_len, num_heads*head_dim].
        causalSelfAttention: *const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT,

        /// Cross-attention: Q from decoder, K/V from encoder.
        /// Q: [batch*dec_seq, num_heads*head_dim], K/V: [batch*enc_seq, num_heads*head_dim].
        /// enc_mask: [batch, enc_seq] with 0=masked, 1=attend.
        /// Returns: [batch*dec_seq, num_heads*head_dim].
        crossAttention: *const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, enc_mask: []const i64, batch: usize, dec_seq: usize, enc_seq: usize, num_heads: usize, head_dim: usize) anyerror!CT,

        /// Compute T5-style relative position bias.
        /// weight: [num_heads, num_buckets] learned bias table.
        /// Returns: [num_heads, q_len, k_len] additive bias for attention scores.
        relativePositionBias: *const fn (ctx: *anyopaque, weight: CT, q_len: usize, k_len: usize, num_heads: usize, num_buckets: usize, max_distance: usize, bidirectional: bool) anyerror!CT,

        /// Disentangled self-attention with projected relative position
        /// embeddings. Q,K,V are [batch*seq_len, num_heads*head_dim], while
        /// Q_r/K_r are [2*seq_len-1, num_heads*head_dim].
        /// Returns [batch*seq_len, num_heads*head_dim].
        disentangledRelativeAttention: *const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, Q_r: CT, K_r: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT,

        /// Optional destructive softmax over the last dimension. When this
        /// returns a tensor, the backend may have reused `input`'s storage, so
        /// callers must only use it when `input` is at last use.
        softmaxConsume: ?*const fn (ctx: *anyopaque, input: CT, dim: u32) anyerror!?CT = null,

        /// Optional destructive log-softmax over the last dimension. Same
        /// ownership contract as `softmaxConsume`.
        logSoftmaxConsume: ?*const fn (ctx: *anyopaque, input: CT, dim: u32) anyerror!?CT = null,

        /// Windowed self-attention over image tokens laid out as
        /// [batch*height*width, dim]. The backend handles padding into
        /// non-overlapping windows, QKV projection, attention, projection,
        /// and unpadding back to the original token layout.
        windowedSelfAttention: *const fn (
            ctx: *anyopaque,
            input: CT,
            norm_weight: CT,
            norm_bias: CT,
            qkv_weight: CT,
            qkv_bias: CT,
            proj_weight: CT,
            proj_bias: CT,
            batch: usize,
            height: usize,
            width: usize,
            dim: usize,
            num_heads: usize,
            window_size: usize,
        ) anyerror!CT,

        /// Grouped channel self-attention over [batch*seq_len, dim] tokens.
        /// The backend handles layer norm, QKV projection, grouped channel
        /// attention, and output projection.
        channelSelfAttention: *const fn (
            ctx: *anyopaque,
            input: CT,
            norm_weight: CT,
            norm_bias: CT,
            qkv_weight: CT,
            qkv_bias: CT,
            proj_weight: CT,
            proj_bias: CT,
            batch: usize,
            seq_len: usize,
            dim: usize,
            groups: usize,
        ) anyerror!CT,

        /// 2D convolution over image tokens stored as [batch*height*width, channels].
        /// The backend reshapes to image layout, runs conv2d, then reshapes back
        /// to token layout [batch*out_h*out_w, out_channels].
        tokenGridConv2d: *const fn (
            ctx: *anyopaque,
            input: CT,
            weight: CT,
            bias: CT,
            batch: usize,
            in_channels: usize,
            out_channels: usize,
            height: usize,
            width: usize,
            kernel_h: usize,
            kernel_w: usize,
            stride_h: usize,
            stride_w: usize,
            padding_h: usize,
            padding_w: usize,
            groups: usize,
        ) anyerror!CT,

        /// Element-wise multiplication. Result is a new tensor; inputs are NOT freed.
        multiply: *const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!CT,

        /// 1D convolution: input:[batch, in_ch, time], weight:[out_ch, in_ch, kernel], bias:[out_ch]
        /// Returns [batch, out_ch, out_time] where out_time = (time + 2*padding - kernel) / stride + 1.
        conv1d: *const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, time_steps: usize, kernel_size: usize, stride: usize, padding: usize) anyerror!CT,

        /// 2D convolution: input:[batch, in_ch, height, width],
        /// weight:[out_ch, in_ch/groups, kernel_h, kernel_w], bias:[out_ch].
        /// Returns [batch, out_ch, out_h, out_w].
        conv2d: *const fn (ctx: *anyopaque, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, height: usize, width: usize, kernel_h: usize, kernel_w: usize, stride_h: usize, stride_w: usize, padding_h: usize, padding_w: usize, groups: usize) anyerror!CT,

        /// Apply rotary position embeddings (RoPE) in-place.
        /// input: [total, dim] where total = batch*seq_len.
        /// Rotates pairs of dimensions using sin/cos of position-dependent angles.
        rope: *const fn (ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT,

        /// Apply rotary position embeddings with per-item query lengths and
        /// position offsets for mixed prefill+decode batches.
        ropePerItem: *const fn (ctx: *anyopaque, input: CT, batch: usize, max_seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, query_lengths: []const usize, position_offsets: []const usize, consecutive_pairs: bool) anyerror!CT,

        /// Causal self-attention with grouped-query attention (GQA) support.
        /// Q: [batch*seq, num_heads*head_dim], K/V: [batch*seq, num_kv_heads*head_dim].
        /// num_kv_heads <= num_heads; heads are repeated as needed.
        /// attn_bias: optional additive bias, or null.
        /// Returns: [batch*seq, num_heads*head_dim].
        gqaCausalAttention: *const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT,

        /// Backend entry point for paged or streaming GQA attention.
        /// Current dense backends may fall back to gqaCausalAttention until
        /// they grow true paged KV implementations.
        gqaPagedAttention: *const fn (ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, attention: AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT,

        /// Create a tensor from raw f32 data. The data is copied (caller retains ownership).
        fromFloat32: *const fn (ctx: *anyopaque, data: []const f32) anyerror!CT,

        /// Create a tensor from raw f32 data with an explicit logical shape.
        /// Backends that are shape-agnostic may ignore `shape`.
        fromFloat32Shape: *const fn (ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT,

        /// Create a tensor from raw i32 data with an explicit logical shape.
        /// Backends may leave this null when they do not support integer tensors.
        fromInt32Shape: ?*const fn (ctx: *anyopaque, data: []const i32, shape: []const i32) anyerror!?CT = null,

        /// Copy tensor data to a caller-owned f32 slice.
        toFloat32: *const fn (ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32,

        /// Return caller-owned raw tensor bytes when the backend can export a
        /// tensor in its native logical dtype without widening through f32.
        exportTensorData: ?*const fn (ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror!?ExportTensorData = null,

        /// Clone a tensor into a new backend handle with the requested logical
        /// shape without requiring host materialization. Backends may alias
        /// immutable device storage when lifetime/refcounting makes that safe.
        cloneTensorShape: ?*const fn (ctx: *anyopaque, tensor: CT, shape: []const i32) anyerror!?CT = null,

        /// Return the logical tensor dtype when the backend can report it.
        tensorDType: ?*const fn (ctx: *anyopaque, tensor: CT) anyerror!tensor_mod.DType = null,

        /// Return a caller-owned logical tensor shape when the backend can report one.
        tensorShape: ?*const fn (ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 = null,

        /// Force backend completion for work producing this tensor.
        /// Backends may leave this null if they do not support explicit sync.
        evalTensor: ?*const fn (ctx: *anyopaque, tensor: CT) anyerror!void = null,

        /// Return the argmax token id from the last row of a [rows, dim] tensor.
        /// Backends may return null when they do not provide a specialized path.
        argmaxLastRow: ?*const fn (ctx: *anyopaque, tensor: CT, rows: usize, dim: usize) anyerror!?u32 = null,

        /// Sample a token id from the last row of a [rows, dim] tensor.
        /// Backends may return null to fall back to host materialization.
        sampleLastRow: ?*const fn (ctx: *anyopaque, request: *const SampleLastRowRequest) anyerror!?u32 = null,

        /// Compute argmax(input @ weight^T) for the last input row without
        /// materializing the full logits matrix. Backends may return null to
        /// use the ordinary linear + argmax path.
        linearNoBiasArgmaxLastRow: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?u32 = null,

        /// Compute argmax(input @ weight^T) for the last input row and return
        /// the token id as a backend tensor. Used by decode paths that want to
        /// feed the next token back into the backend without a host upload.
        linearNoBiasArgmaxLastRowTensor: ?*const fn (ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT = null,

        /// Prepare a backend-owned whole-token greedy decode runtime for a
        /// decoder-only model. Backends return false when unsupported so the
        /// caller can fall back to the existing decode path.
        decoderRuntimePrepareGreedy: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeGreedyRequest) anyerror!bool = null,

        /// Reset runtime-owned mutable state for the backend whole-token
        /// decode path while keeping resident model weights/slots intact.
        decoderRuntimeResetState: ?*const fn (ctx: *anyopaque) anyerror!void = null,

        /// Backend-owned greedy decode entry point. The request describes
        /// a batch of qLen=1 decode items plus the decoder contract without
        /// exposing model-specific frontend types. Backends return false
        /// when unsupported so callers can fall back.
        decoderRuntimeDecode: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeDecodeRequest) anyerror!bool = null,

        /// Build and reserve a backend-owned qLen>1 prefill frame plan before
        /// frontend layer orchestration starts encoding runtime operations.
        /// Backends return false when the model/shape cannot use the planned
        /// frame path.
        decoderRuntimePlanPrefillFrame: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimePrefillFramePlanRequest) anyerror!bool = null,

        /// Execute a backend-owned graph command plan frame as one planned
        /// sequence. Backends return false before encoding when unsupported;
        /// after accepting, errors should abort the active frame.
        decoderRuntimeExecuteGraphCommandPlanFrame: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeGraphCommandPlanFrameRequest) anyerror!bool = null,

        /// Build/cache a backend-owned GLiNER DeBERTa encoder frame plan.
        /// This prepares model-specific encoder block metadata without
        /// executing the frame.
        debertaEncoderPlanFrame: ?*const fn (ctx: *anyopaque, request: *const DebertaEncoderFramePlanRequest) anyerror!bool = null,

        /// Execute one GLiNER DeBERTa encoder layer from a previously
        /// prepared backend-owned slot layout. Backends return null when the
        /// shape/path should use the ordinary eager ops.
        debertaEncoderLayer: ?*const fn (ctx: *anyopaque, request: *const DebertaEncoderLayerRequest) anyerror!?CT = null,

        /// Begin a backend-owned decoder frame. Backends that support this
        /// encode subsequent runtime operations into one command submission
        /// until `decoderRuntimeSubmitAndWaitFrame` is called.
        decoderRuntimeBeginFrame: ?*const fn (ctx: *anyopaque) anyerror!bool = null,

        /// Return true when a backend-owned decoder frame is already active.
        decoderRuntimeHasActiveFrame: ?*const fn (ctx: *anyopaque) bool = null,

        /// Submit and wait for the active backend-owned decoder frame.
        decoderRuntimeSubmitAndWaitFrame: ?*const fn (ctx: *anyopaque) anyerror!void = null,

        /// Submit and wait for the active backend-owned decoder frame, then
        /// reopen a frame for subsequent runtime operations.
        decoderRuntimeFlushActiveFrame: ?*const fn (ctx: *anyopaque) anyerror!void = null,

        /// Drop the active backend-owned decoder frame without submitting it.
        decoderRuntimeCancelFrame: ?*const fn (ctx: *anyopaque) anyerror!void = null,

        /// Gather one paged-KV layer from backend-owned cache state into
        /// host-owned row-major `f32` buffers. Returns null when unsupported.
        gatherPagedKvLayerCache: ?*const fn (ctx: *anyopaque, allocator: std.mem.Allocator, kv: KvCacheView, token_count: usize, layer_index: usize) anyerror!?PagedKvLayerCacheRows = null,

        /// Seed one paged-KV layer back into backend-owned cache state from
        /// host-owned row-major `f32` buffers. Returns false when unsupported.
        seedPagedKvLayerCache: ?*const fn (ctx: *anyopaque, kv: KvCacheView, token_count: usize, layer_index: usize, k_rows_host: []const f32, v_rows_host: []const f32) anyerror!bool = null,

        /// Snapshot direct-family timing counters that the whole-model Metal
        /// executor uses for attribution. Backends that do not support this
        /// can leave it null and the snapshot will be zeroed.
        directFamilyTimingSnapshot: ?*const fn (ctx: *anyopaque) DirectFamilyTimingSnapshot = null,

        /// Snapshot backend-owned debug timing counters for diagnostic
        /// printing. Backends that do not support this can leave it null.
        debugTimingSnapshot: ?*const fn (ctx: *anyopaque) BackendDebugTimingSnapshot = null,

        /// Reset backend-owned debug timing counters for diagnostic
        /// printing. Backends that do not support this can leave it null.
        resetDebugTimingStats: ?*const fn (ctx: *anyopaque) void = null,

        /// Reuse or prepare decoder-runtime family state for the current
        /// sequence shape. Backends that do not support this can leave it
        /// null and the caller will see a zero-valued result.
        decoderRuntimePrepareOrReuseFamily: ?*const fn (ctx: *anyopaque, allocator: std.mem.Allocator, gpt_config: gpt_model.Config, current_kv_tokens: usize, configured_layer_count: usize) anyerror!DecoderRuntimePrepareReuseResult = null,

        /// Report whether the backend-owned decoder runtime exists and is
        /// currently available.
        decoderRuntimeReady: ?*const fn (ctx: *anyopaque) bool = null,

        /// Report whether absolute embedding tables are resident in the
        /// backend-owned decoder runtime.
        decoderRuntimeAbsoluteEmbeddingsPrepared: ?*const fn (ctx: *anyopaque) bool = null,

        /// Upload absolute token/position embedding tables into a backend-
        /// owned whole-token decode runtime. Backends return false when the
        /// model or runtime is unsupported.
        decoderRuntimePrepareAbsoluteEmbeddings: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimePrepareAbsoluteEmbeddingsRequest) anyerror!bool = null,

        /// Execute the decoder input embedding step for absolute-position
        /// models from backend-owned embedding tables: gather token and
        /// position rows by id and return their sum as a backend tensor with
        /// shape [1, hidden_size].
        decoderRuntimeEmbedAbsolutePosition: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeEmbedAbsolutePositionRequest) anyerror!?CT = null,

        /// Upload layer-norm parameters into a backend-owned whole-token
        /// decode runtime. `slot` is backend-defined but stable for the life
        /// of the prepared runtime.
        decoderRuntimePrepareLayerNorm: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimePrepareLayerNormRequest) anyerror!bool = null,

        /// Reuse or prepare a backend-owned graph/runtime layer-norm slot
        /// without exposing backend slot allocation policy to graph executors.
        decoderRuntimeEnsureLayerNormSlot: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeEnsureLayerNormSlotRequest) anyerror!?usize = null,

        /// Apply a previously prepared layer norm from the backend-owned
        /// whole-token runtime and return a [1, hidden_size] tensor.
        decoderRuntimeApplyLayerNorm: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormRequest) anyerror!?CT = null,

        /// Upload RMS-norm parameters into a backend-owned whole-token
        /// decode runtime. `slot` is backend-defined but stable for the life
        /// of the prepared runtime.
        decoderRuntimePrepareRmsNorm: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimePrepareRmsNormRequest) anyerror!bool = null,

        /// Reuse or prepare a backend-owned graph/runtime RMS-norm slot
        /// without exposing backend slot allocation policy to graph executors.
        decoderRuntimeEnsureRmsNormSlot: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeEnsureRmsNormSlotRequest) anyerror!?usize = null,

        /// Apply a previously prepared RMS norm from the backend-owned
        /// whole-token runtime and return a [1, hidden_size] tensor.
        decoderRuntimeApplyRmsNorm: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormRequest) anyerror!?CT = null,

        /// Apply a prepared layer norm slot followed by a prepared linear
        /// slot inside one backend-owned whole-token submission and return a
        /// [1, out_dim] tensor.
        decoderRuntimeApplyLayerNormLinear: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearRequest) anyerror!?CT = null,

        /// Apply a prepared layer norm slot followed by a prepared linear
        /// argmax slot inside one backend-owned whole-token submission.
        decoderRuntimeApplyLayerNormLinearArgmax: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearArgmaxRequest) anyerror!?usize = null,

        /// Apply a prepared layer norm slot followed by a prepared linear
        /// slot inside one backend-owned whole-token submission and sample a
        /// token from the resulting last-row logits using a penalty-free
        /// sampling config.
        decoderRuntimeApplyLayerNormLinearSample: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLayerNormLinearSampleRequest) anyerror!?usize = null,

        /// Apply a prepared RMS norm slot followed by a prepared linear
        /// slot inside one backend-owned whole-token submission and return a
        /// [1, out_dim] tensor.
        decoderRuntimeApplyRmsNormLinear: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearRequest) anyerror!?CT = null,

        /// Apply a prepared RMS norm slot followed by a prepared linear
        /// argmax slot inside one backend-owned whole-token submission.
        decoderRuntimeApplyRmsNormLinearArgmax: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearArgmaxRequest) anyerror!?usize = null,

        /// Apply a prepared RMS norm slot followed by a prepared linear
        /// slot inside one backend-owned whole-token submission and sample a
        /// token from the resulting last-row logits using a penalty-free
        /// sampling config.
        decoderRuntimeApplyRmsNormLinearSample: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyRmsNormLinearSampleRequest) anyerror!?usize = null,

        /// Upload dense linear parameters into a backend-owned whole-token
        /// runtime slot. Weight shape is [out_dim, in_dim], bias shape is
        /// [out_dim].
        decoderRuntimePrepareLinear: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimePrepareLinearRequest) anyerror!bool = null,

        /// Reuse or prepare a backend-owned graph/runtime dense linear slot
        /// without exposing backend slot allocation policy to graph executors.
        decoderRuntimeEnsureLinearSlot: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeEnsureLinearSlotRequest) anyerror!?usize = null,

        /// Apply a previously prepared dense linear slot to a [1, in_dim]
        /// input and return a [1, out_dim] tensor.
        decoderRuntimeApplyLinear: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearRequest) anyerror!?CT = null,

        /// Apply a previously prepared dense linear slot to a [1, in_dim]
        /// input and return the argmax token id without materializing full
        /// logits back through the generic path.
        decoderRuntimeApplyLinearArgmax: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearArgmaxRequest) anyerror!?usize = null,

        /// Apply two previously prepared linear slots to the same [1, in_dim]
        /// input and return both [1, out_dim] outputs.
        decoderRuntimeApplyLinearPair: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearPairRequest) anyerror!?LinearNoBiasPairResult = null,

        /// Apply three previously prepared q/k/v linear slots to the same
        /// input and return all projected outputs.
        decoderRuntimeApplyLinearQkv: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyLinearQkvRequest) anyerror!?LinearNoBiasTripleResult = null,

        /// Apply an activation inside the backend-owned decoder runtime.
        decoderRuntimeApplyActivation: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyActivationRequest) anyerror!?CT = null,

        /// Apply elementwise add inside the backend-owned decoder runtime.
        decoderRuntimeApplyAdd: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyAddRequest) anyerror!?CT = null,

        /// Apply scaled elementwise add inside the backend-owned decoder runtime.
        decoderRuntimeApplyAddScale: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyAddScaleRequest) anyerror!?CT = null,

        /// Apply `(lhs * lhs_scale + rhs) * output_scale` inside the backend runtime.
        decoderRuntimeApplyScaledAddScale: ?*const fn (ctx: *anyopaque, request: *const DecoderRuntimeApplyScaledAddScaleRequest) anyerror!?CT = null,

        /// Apply a two-linear FFN strip (`linear -> activation -> linear ->
        /// residual add`) inside one backend-owned whole-token submission and
        /// return the post-residual [1, hidden_size] tensor.
        runDenseFfnResidual: ?*const fn (ctx: *anyopaque, request: *const RunDenseFfnResidualRequest) anyerror!?CT = null,

        /// Apply a gated FFN strip (`linear_pair -> activation(first) ->
        /// multiply(second) -> linear -> residual add`) inside one backend-
        /// owned whole-token submission and return the post-residual
        /// [1, hidden_size] tensor.
        runGatedFfnResidual: ?*const fn (ctx: *anyopaque, request: *const RunGatedFfnResidualRequest) anyerror!?CT = null,

        /// Execute a qLen=1 decoder attention step for the backend-owned
        /// path. Backends may return null to fall back to the ordinary
        /// attention path.
        runAttention: ?*const fn (ctx: *anyopaque, request: *const RunAttentionRequest) anyerror!?CT = null,

        /// Execute DeepSeek V4 compressed/hybrid attention with backend-owned
        /// cache state. Backends may return null to use the host reference
        /// compressed cache.
        runDeepSeekV4CompressedAttention: ?*const fn (ctx: *anyopaque, request: *const DeepSeekV4CompressedAttentionRequest) anyerror!?CT = null,

        /// Seed or extend backend-owned paged-attention span state during
        /// prefill using the current K/V suffix. Backends may return false to
        /// indicate unsupported or ignored.
        seedPagedAttentionSpan: ?*const fn (ctx: *anyopaque, request: *const SeedPagedAttentionSpanRequest) anyerror!bool = null,

        /// Execute a qLen=1 decoder attention readout followed by a prepared
        /// output projection slot and residual add inside one backend-owned
        /// path. Backends may return null to fall back to split ops.
        runAttentionResidual: ?*const fn (ctx: *anyopaque, request: *const RunAttentionResidualRequest) anyerror!?CT = null,

        /// Apply a previously computed attention output through the prepared
        /// output projection slot, optional RMS norms, and residual add inside
        /// one backend-owned device path. Backends may return null to fall
        /// back to split ops.
        runAttentionOutputResidual: ?*const fn (ctx: *anyopaque, request: *const RunAttentionOutputResidualRequest) anyerror!?CT = null,

        /// Execute a qLen=1 paged-attention step, prepared output projection,
        /// residual add, FFN norm, and dense FFN strip inside one backend-
        /// owned whole-token block path. Backends may return null to fall
        /// back to the split attention + FFN path.
        runDenseDecoderBlock: ?*const fn (ctx: *anyopaque, request: *const RunDenseDecoderBlockRequest) anyerror!?CT = null,

        /// Execute a qLen=1 paged-attention step, prepared output projection,
        /// residual add, FFN norm, and gated FFN strip inside one backend-
        /// owned whole-token block path. Backends may return null to fall
        /// back to the split attention + FFN path.
        runGatedDecoderBlock: ?*const fn (ctx: *anyopaque, request: *const RunGatedDecoderBlockRequest) anyerror!?CT = null,

        /// Reshape a 2D tensor to a new 2D shape (same total elements).
        /// Backends that don't track tensor shapes may leave this null.
        reshape2d: ?*const fn (ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT = null,

        /// Slice the last dimension of a 2D tensor: [rows, D] → [rows, stop-start].
        sliceLastDim: ?*const fn (ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT = null,

        // ── Primitive ops for training (all optional) ────────────────
        // These enable execution of lowered/gradient graphs through real
        // backends. Backends that don't support training leave them null.

        /// Element-wise subtraction: a - b.
        subtract: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!CT = null,
        /// Element-wise division: a / b.
        divide: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!CT = null,
        /// Element-wise negation: -a.
        negate: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise square root.
        sqrtOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise reciprocal square root: 1/sqrt(a).
        rsqrtOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise exp.
        expOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise natural log.
        logOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise sin.
        sinOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise cos.
        cosOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise tanh (primitive, distinct from fused tanh_act).
        tanhOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise erf (Gauss error function).
        erfOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise absolute value.
        absOp: ?*const fn (ctx: *anyopaque, a: CT) anyerror!CT = null,
        /// Element-wise less-than comparison: result[i] = a[i] < b[i] ? 1.0 : 0.0.
        lessThan: ?*const fn (ctx: *anyopaque, a: CT, b: CT) anyerror!CT = null,
        /// Element-wise select: result[i] = cond[i] != 0 ? on_true[i] : on_false[i].
        whereSelect: ?*const fn (ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!CT = null,
        /// Reduce sum along given axes. Shape metadata passed separately.
        reduceSumOp: ?*const fn (ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT = null,
        /// Reduce max along given axes.
        reduceMaxOp: ?*const fn (ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT = null,
        /// Reduce mean along given axes.
        reduceMeanOp: ?*const fn (ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT = null,
        /// Argmax along an axis, returning index values.
        argmaxOp: ?*const fn (ctx: *anyopaque, input: CT, axis: u8, keepdims: bool, input_shape: []const i64) anyerror!CT = null,
        /// Reshape tensor to new shape (same total elements).
        reshapeOp: ?*const fn (ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT = null,
        /// Transpose tensor according to permutation.
        transposeOp: ?*const fn (ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT = null,
        /// Broadcast input to target shape along given axes.
        broadcastInDimOp: ?*const fn (ctx: *anyopaque, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!CT = null,
        /// Generalized dot product (matmul with contracting/batch dims).
        dotGeneralOp: ?*const fn (ctx: *anyopaque, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) anyerror!CT = null,
        /// Scatter-add: accumulate updates into a zeroed output using indices.
        scatterAddOp: ?*const fn (ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!CT = null,
        /// Gather elements along an axis using indices.
        gatherOp: ?*const fn (ctx: *anyopaque, input: CT, indices: CT, axis: u8, input_shape: []const i64) anyerror!CT = null,
        /// Slice a tensor with starts/limits/strides per axis.
        sliceOp: ?*const fn (ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) anyerror!CT = null,
        /// Concatenate tensors along an axis (primitive version).
        concatPrimOp: ?*const fn (ctx: *anyopaque, a: CT, b: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) anyerror!CT = null,
        /// Softmax along last dimension: exp(x-max)/sum(exp(x-max)).
        /// `last_dim_size` is the size of the last dimension (not an axis index).
        softmaxOp: ?*const fn (ctx: *anyopaque, input: CT, last_dim_size: u32) anyerror!CT = null,
        /// Log-softmax along last dimension: x - max - log(sum(exp(x-max))).
        /// `last_dim_size` is the size of the last dimension (not an axis index).
        logSoftmaxOp: ?*const fn (ctx: *anyopaque, input: CT, last_dim_size: u32) anyerror!CT = null,

        /// Download multiple tensors to f32 in a single backend round-trip.
        /// Backends that support batched eval (e.g. MLX mlx_eval with a vector of
        /// arrays) should implement this to reduce GPU sync overhead.
        /// Default (null): sequential toFloat32 calls used by the wrapper below.
        toFloat32Batch: ?*const fn (ctx: *anyopaque, cts: []const CT, allocator: std.mem.Allocator) anyerror![][]f32 = null,

        /// GPU-accelerated LoRA gradient accumulation. Writes into grad_a/grad_b (overwrites, not +=).
        /// If null, caller falls back to CPU accumulateLinearLoRAGrads.
        ///
        /// Shapes (termite-zig convention):
        ///   lora_a: [rank, in_features]
        ///   lora_b: [out_features, rank]
        ///   grad_a: [rank, in_features]  — overwritten on return
        ///   grad_b: [out_features, rank] — overwritten on return
        ///   inputs: [rows, in_features]
        ///   output_grads: [rows, out_features]
        accumulateLoRAGrads: ?*const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            grad_a: []f32,
            grad_b: []f32,
            inputs: []const f32,
            output_grads: []const f32,
            lora_a: []const f32,
            lora_b: []const f32,
            rows: usize,
            in_features: usize,
            out_features: usize,
            rank: usize,
            scale: f32,
        ) anyerror!void = null,
    };

    // Convenience wrappers
    pub fn deinit(self: *const ComputeBackend) void {
        self.vtable.deinitBackend(self.ptr);
    }

    pub fn free(self: *const ComputeBackend, tensor: CT) void {
        self.vtable.freeTensor(self.ptr, tensor);
    }

    pub fn getWeight(self: *const ComputeBackend, name: []const u8) !CT {
        return self.vtable.getWeight(self.ptr, name);
    }

    pub fn prefetchWeight(self: *const ComputeBackend, name: []const u8) void {
        self.vtable.prefetchWeightHint(self.ptr, name, 1);
    }

    pub fn prefetchWeightHint(self: *const ComputeBackend, name: []const u8, hint: u32) void {
        self.vtable.prefetchWeightHint(self.ptr, name, hint);
    }

    pub fn drainPrefetch(self: *const ComputeBackend) void {
        self.vtable.drainPrefetchBudget(self.ptr, std.math.maxInt(usize));
    }

    pub fn drainPrefetchBudget(self: *const ComputeBackend, max_items: usize) void {
        self.vtable.drainPrefetchBudget(self.ptr, max_items);
    }

    pub fn embeddingLookup(self: *const ComputeBackend, weight: CT, ids: []const i64, total: usize, dim: usize) !CT {
        return self.vtable.embeddingLookup(self.ptr, weight, ids, total, dim);
    }

    pub fn embeddingLookupTensor(self: *const ComputeBackend, weight: CT, ids: CT, total: usize, dim: usize) !?CT {
        if (self.vtable.embeddingLookupTensor) |op| {
            return op(self.ptr, weight, ids, total, dim);
        }
        return null;
    }

    pub fn debertaEmbeddings(self: *const ComputeBackend, request: DebertaEmbeddingsRequest) !?CT {
        if (self.vtable.debertaEmbeddings) |op| {
            return op(self.ptr, &request);
        }
        return null;
    }

    pub fn takeRows(self: *const ComputeBackend, input: CT, row_ids: []const u32, rows: usize, dim: usize) !?CT {
        if (self.vtable.takeRows) |take_rows| {
            return take_rows(self.ptr, &.{
                .input = input,
                .row_ids = row_ids,
                .rows = rows,
                .dim = dim,
            });
        }
        return null;
    }

    pub fn glinerWordEmbeddings(self: *const ComputeBackend, hidden: CT, words_mask: []const i64, batch: usize, seq_len: usize, hidden_size: usize, num_words: usize) !?CT {
        if (self.vtable.glinerWordEmbeddings) |op| {
            return op(self.ptr, &.{
                .hidden = hidden,
                .words_mask = words_mask,
                .batch = batch,
                .seq_len = seq_len,
                .hidden_size = hidden_size,
                .num_words = num_words,
            });
        }
        return null;
    }

    pub fn glinerLabelGruCombined(self: *const ComputeBackend, label_embeddings: CT, num_labels: usize, hidden_size: usize) !?CT {
        if (self.vtable.glinerLabelGruCombined) |op| {
            return op(self.ptr, &.{
                .label_embeddings = label_embeddings,
                .num_labels = num_labels,
                .hidden_size = hidden_size,
            });
        }
        return null;
    }

    pub fn linear(self: *const ComputeBackend, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) !CT {
        return self.vtable.linear(self.ptr, input, weight, bias, rows, in_dim, out_dim);
    }

    pub fn linearWithPlan(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        bias: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        op_plan: ?OperatorPlan,
    ) !CT {
        if (op_plan) |plan| {
            if (self.vtable.linearPlanned) |op| {
                return op(self.ptr, &.{
                    .input = input,
                    .weight = weight,
                    .bias = bias,
                    .rows = rows,
                    .in_dim = in_dim,
                    .out_dim = out_dim,
                    .operator_plan = plan,
                });
            }
        }
        return self.linear(input, weight, bias, rows, in_dim, out_dim);
    }

    pub fn linearNoBias(self: *const ComputeBackend, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) !CT {
        return self.vtable.linearNoBias(self.ptr, input, weight, rows, in_dim, out_dim);
    }

    pub fn linearNoBiasWithPlan(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        op_plan: ?OperatorPlan,
    ) !CT {
        if (op_plan) |plan| {
            if (self.vtable.linearNoBiasPlanned) |op| {
                return op(self.ptr, &.{
                    .input = input,
                    .weight = weight,
                    .rows = rows,
                    .in_dim = in_dim,
                    .out_dim = out_dim,
                    .operator_plan = plan,
                });
            }
        }
        return self.linearNoBias(input, weight, rows, in_dim, out_dim);
    }

    pub fn linearNoBiasGrouped(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        projection_out_dims: []const u32,
        num_projections: usize,
    ) !CT {
        if (self.vtable.linearNoBiasGrouped) |op| {
            return op(self.ptr, input, weight, rows, in_dim, out_dim, projection_out_dims, num_projections);
        }
        return self.linearNoBias(input, weight, rows, in_dim, out_dim);
    }

    pub fn linearLoRA(
        self: *const ComputeBackend,
        input: CT,
        base_weight: CT,
        bias: CT,
        lora_a: CT,
        lora_b: CT,
        alpha: f32,
        rank: usize,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !CT {
        if (self.vtable.linearLoRA) |op| {
            return op(self.ptr, input, base_weight, bias, lora_a, lora_b, alpha, rank, rows, in_dim, out_dim);
        }
        return fallbackLinearLoRA(self, input, base_weight, bias, lora_a, lora_b, alpha, rank, rows, in_dim, out_dim);
    }

    pub fn zeroTensor(self: *const ComputeBackend, rows: usize, dim: usize) !?CT {
        if (self.vtable.zeroTensor) |zero_tensor| {
            return zero_tensor(self.ptr, rows, dim);
        }
        return null;
    }

    pub fn linearNoBiasPair(
        self: *const ComputeBackend,
        input: CT,
        weight_a: CT,
        weight_b: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !LinearNoBiasPairResult {
        if (self.vtable.linearNoBiasPair) |linear_no_bias_pair| {
            return linear_no_bias_pair(self.ptr, input, weight_a, weight_b, rows, in_dim, out_dim);
        }
        return .{
            .first = try self.linearNoBias(input, weight_a, rows, in_dim, out_dim),
            .second = try self.linearNoBias(input, weight_b, rows, in_dim, out_dim),
        };
    }

    pub fn linearPair(
        self: *const ComputeBackend,
        input: CT,
        weight_a: CT,
        bias_a: CT,
        weight_b: CT,
        bias_b: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !LinearPairResult {
        if (self.vtable.linearPair) |linear_pair| {
            return linear_pair(self.ptr, input, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
        }
        if (self.vtable.linearNoBiasPair) |linear_no_bias_pair| {
            const no_bias = try linear_no_bias_pair(self.ptr, input, weight_a, weight_b, rows, in_dim, out_dim);
            var raw_first_live = true;
            var raw_second_live = true;
            errdefer if (raw_first_live) self.free(no_bias.first);
            errdefer if (raw_second_live) self.free(no_bias.second);

            const first = try self.add(no_bias.first, bias_a);
            self.free(no_bias.first);
            raw_first_live = false;
            var first_live = true;
            errdefer if (first_live) self.free(first);

            const second = try self.add(no_bias.second, bias_b);
            self.free(no_bias.second);
            raw_second_live = false;
            first_live = false;

            return .{ .first = first, .second = second };
        }

        const first = try self.linear(input, weight_a, bias_a, rows, in_dim, out_dim);
        errdefer self.free(first);
        const second = try self.linear(input, weight_b, bias_b, rows, in_dim, out_dim);
        return .{ .first = first, .second = second };
    }

    pub fn linearTriple(
        self: *const ComputeBackend,
        input: CT,
        weight_a: CT,
        bias_a: CT,
        weight_b: CT,
        bias_b: CT,
        weight_c: CT,
        bias_c: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !LinearTripleResult {
        if (self.vtable.linearTriple) |linear_triple| {
            return linear_triple(self.ptr, input, weight_a, bias_a, weight_b, bias_b, weight_c, bias_c, rows, in_dim, out_dim);
        }

        const first = try self.linear(input, weight_a, bias_a, rows, in_dim, out_dim);
        errdefer self.free(first);
        const second = try self.linear(input, weight_b, bias_b, rows, in_dim, out_dim);
        errdefer self.free(second);
        const third = try self.linear(input, weight_c, bias_c, rows, in_dim, out_dim);
        return .{ .first = first, .second = second, .third = third };
    }

    pub fn splitLastDim3(
        self: *const ComputeBackend,
        allocator: std.mem.Allocator,
        input: CT,
        rows: usize,
        dim: usize,
    ) !SplitLastDim3Result {
        if (self.vtable.splitLastDim3) |split_last_dim_3| {
            return split_last_dim_3(self.ptr, input, rows, dim);
        }

        const data = try self.toFloat32(input, allocator);
        defer allocator.free(data);
        if (data.len != rows * dim * 3) return error.UnexpectedOutputShape;

        const total = rows * dim;
        const first = try allocator.alloc(f32, total);
        errdefer allocator.free(first);
        const second = try allocator.alloc(f32, total);
        errdefer allocator.free(second);
        const third = try allocator.alloc(f32, total);
        errdefer allocator.free(third);

        for (0..rows) |row| {
            const src = row * dim * 3;
            const dst = row * dim;
            @memcpy(first[dst..][0..dim], data[src..][0..dim]);
            @memcpy(second[dst..][0..dim], data[src + dim ..][0..dim]);
            @memcpy(third[dst..][0..dim], data[src + dim * 2 ..][0..dim]);
        }

        const shape = [_]i32{ @intCast(rows), @intCast(dim) };
        return .{
            .first = try self.fromFloat32Shape(first, &shape),
            .second = try self.fromFloat32Shape(second, &shape),
            .third = try self.fromFloat32Shape(third, &shape),
        };
    }

    pub fn reshape2D(
        self: *const ComputeBackend,
        allocator: std.mem.Allocator,
        input: CT,
        old_rows: usize,
        old_cols: usize,
        new_rows: usize,
        new_cols: usize,
    ) !CT {
        if (old_rows * old_cols != new_rows * new_cols) return error.UnexpectedOutputShape;
        if (self.vtable.reshape2D) |reshape_2d| {
            return reshape_2d(self.ptr, input, old_rows, old_cols, new_rows, new_cols);
        }
        const data = try self.toFloat32(input, allocator);
        defer allocator.free(data);
        if (data.len != old_rows * old_cols) return error.UnexpectedOutputShape;
        const shape = [_]i32{ @intCast(new_rows), @intCast(new_cols) };
        return self.fromFloat32Shape(data, &shape);
    }

    pub fn concatRows2D(
        self: *const ComputeBackend,
        allocator: std.mem.Allocator,
        a: CT,
        b: CT,
        rows_a: usize,
        rows_b: usize,
        cols: usize,
    ) !CT {
        if (self.vtable.concatRows2D) |concat_rows_2d| {
            return concat_rows_2d(self.ptr, a, b, rows_a, rows_b, cols);
        }
        const a_data = try self.toFloat32(a, allocator);
        defer allocator.free(a_data);
        const b_data = try self.toFloat32(b, allocator);
        defer allocator.free(b_data);
        if (a_data.len != rows_a * cols or b_data.len != rows_b * cols) return error.UnexpectedOutputShape;
        const out = try allocator.alloc(f32, (rows_a + rows_b) * cols);
        defer allocator.free(out);
        @memcpy(out[0..a_data.len], a_data);
        @memcpy(out[a_data.len..][0..b_data.len], b_data);
        const shape = [_]i32{ @intCast(rows_a + rows_b), @intCast(cols) };
        return self.fromFloat32Shape(out, &shape);
    }

    pub fn sliceRows2D(
        self: *const ComputeBackend,
        allocator: std.mem.Allocator,
        input: CT,
        start_row: usize,
        row_count: usize,
        cols: usize,
    ) !CT {
        if (self.vtable.sliceRows2D) |slice_rows_2d| {
            return slice_rows_2d(self.ptr, input, start_row, row_count, cols);
        }
        const data = try self.toFloat32(input, allocator);
        defer allocator.free(data);
        const total_rows = @divExact(data.len, cols);
        if (total_rows * cols != data.len) return error.UnexpectedOutputShape;
        if (start_row + row_count > total_rows) return error.UnexpectedOutputShape;
        const out = data[start_row * cols ..][0 .. row_count * cols];
        const shape = [_]i32{ @intCast(row_count), @intCast(cols) };
        return self.fromFloat32Shape(out, &shape);
    }

    pub fn mulMatId(
        self: *const ComputeBackend,
        request: *const MulMatIdRequest,
    ) !?CT {
        if (self.vtable.mulMatId) |mul_mat_id| {
            return mul_mat_id(self.ptr, request);
        }
        if (self.vtable.moeLinearNoBias) |moe_linear_no_bias| {
            return moe_linear_no_bias(self.ptr, request);
        }
        return null;
    }

    pub fn moeLinearNoBias(
        self: *const ComputeBackend,
        input: CT,
        expert_ids: []const u32,
        expert_tile_ids: ?[]const u32,
        tile_row_starts: ?[]const u32,
        tile_row_counts: ?[]const u32,
        weight: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?CT {
        return self.mulMatId(&.{
            .input = input,
            .expert_ids = expert_ids,
            .expert_tile_ids = expert_tile_ids,
            .tile_row_starts = tile_row_starts,
            .tile_row_counts = tile_row_counts,
            .weight = weight,
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
        });
    }

    pub fn moeLinearNoBiasPair(
        self: *const ComputeBackend,
        input: CT,
        expert_ids: []const u32,
        weight_a: CT,
        weight_b: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?MoeLinearNoBiasPairResult {
        if (self.vtable.moeLinearNoBiasPair) |moe_linear_no_bias_pair| {
            return moe_linear_no_bias_pair(self.ptr, input, expert_ids, weight_a, weight_b, rows, in_dim, out_dim);
        }
        return null;
    }

    pub fn moeScatterAdd(
        self: *const ComputeBackend,
        base: CT,
        row_ids: []const u32,
        row_weights: []const f32,
        updates: CT,
        rows: usize,
        dim: usize,
    ) !?CT {
        if (self.vtable.moeScatterAdd) |moe_scatter_add| {
            return moe_scatter_add(self.ptr, &.{
                .base = base,
                .row_ids = row_ids,
                .row_weights = row_weights,
                .updates = updates,
                .rows = rows,
                .dim = dim,
            });
        }
        return null;
    }

    pub fn moeForwardFused(self: *const ComputeBackend, request: *const MoeForwardFusedRequest) !?CT {
        if (self.vtable.moeForwardFused) |fused| {
            return fused(self.ptr, request);
        }
        return null;
    }

    pub fn moeSelectRoutes(
        self: *const ComputeBackend,
        logits: CT,
        rows: usize,
        num_experts: usize,
        top_k: usize,
        allocator: std.mem.Allocator,
    ) !?MoeRouteSelection {
        if (self.vtable.moeSelectRoutes) |moe_select_routes| {
            return moe_select_routes(self.ptr, logits, rows, num_experts, top_k, allocator);
        }
        return null;
    }

    pub fn setMoeExpertScale(self: *const ComputeBackend, scale: CT) void {
        if (self.vtable.setMoeExpertScale) |set_scale| {
            set_scale(self.ptr, scale);
        }
    }

    pub fn layerNorm(self: *const ComputeBackend, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) !CT {
        return self.vtable.layerNorm(self.ptr, input, gamma, beta, dim, eps);
    }

    pub fn layerNormConsumeInput(self: *const ComputeBackend, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) !?CT {
        if (self.vtable.layerNormConsumeInput) |f| return f(self.ptr, input, gamma, beta, dim, eps);
        return null;
    }

    pub fn addLayerNorm(self: *const ComputeBackend, a: CT, b: CT, gamma: CT, beta: CT, dim: usize, eps: f32) !?CT {
        if (self.vtable.addLayerNorm) |f| return f(self.ptr, a, b, gamma, beta, dim, eps);
        return null;
    }

    pub fn rmsNorm(self: *const ComputeBackend, input: CT, weight: CT, dim: usize, eps: f32) !CT {
        return self.vtable.rmsNorm(self.ptr, input, weight, dim, eps);
    }

    pub fn rmsNormConsumeInput(self: *const ComputeBackend, input: CT, weight: CT, dim: usize, eps: f32) !?CT {
        if (self.vtable.rmsNormConsumeInput) |f| return f(self.ptr, input, weight, dim, eps);
        return null;
    }

    pub fn gelu(self: *const ComputeBackend, input: CT) !CT {
        return self.vtable.gelu(self.ptr, input);
    }

    pub fn geluNew(self: *const ComputeBackend, input: CT) !CT {
        if (self.vtable.geluNew) |f| return f(self.ptr, input);
        return self.vtable.gelu(self.ptr, input);
    }

    pub fn relu(self: *const ComputeBackend, input: CT) !CT {
        return self.vtable.relu(self.ptr, input);
    }

    pub fn silu(self: *const ComputeBackend, input: CT) !CT {
        return self.vtable.silu(self.ptr, input);
    }

    pub fn quickGelu(self: *const ComputeBackend, input: CT) !CT {
        return self.vtable.quickGelu(self.ptr, input);
    }

    pub fn linearQuickGelu(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        bias: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?CT {
        if (self.vtable.linearQuickGelu) |f| return f(self.ptr, input, weight, bias, rows, in_dim, out_dim);
        return null;
    }

    pub fn linearRelu(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        bias: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?CT {
        if (self.vtable.linearRelu) |f| return f(self.ptr, input, weight, bias, rows, in_dim, out_dim);
        return null;
    }

    pub fn denseMlp2(self: *const ComputeBackend, request: *const DenseMlp2Request) !?CT {
        if (self.vtable.denseMlp2) |f| return f(self.ptr, request);
        return null;
    }

    pub fn denseFfnLayerNorm(self: *const ComputeBackend, request: *const DenseFfnLayerNormRequest) !?CT {
        if (self.vtable.denseFfnLayerNorm) |f| return f(self.ptr, request);
        return null;
    }

    pub fn denseLinearLayerNorm(self: *const ComputeBackend, request: *const DenseLinearLayerNormRequest) !?CT {
        if (self.vtable.denseLinearLayerNorm) |f| return f(self.ptr, request);
        return null;
    }

    pub fn linearGelu(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        bias: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?CT {
        if (self.vtable.linearGelu) |f| return f(self.ptr, input, weight, bias, rows, in_dim, out_dim);
        return null;
    }

    pub fn linearAdd(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        bias: CT,
        residual: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !?CT {
        if (self.vtable.linearAdd) |f| return f(self.ptr, input, weight, bias, residual, rows, in_dim, out_dim);
        return null;
    }

    pub fn sigmoid(self: *const ComputeBackend, input: CT) !CT {
        return self.vtable.sigmoid(self.ptr, input);
    }

    pub fn tanh_act(self: *const ComputeBackend, input: CT) !CT {
        return self.vtable.tanh_act(self.ptr, input);
    }

    pub fn unaryConsume(self: *const ComputeBackend, op: UnaryConsumeOp, input: CT) !?CT {
        if (self.vtable.unaryConsume) |f| return f(self.ptr, op, input);
        return null;
    }

    pub fn concat(self: *const ComputeBackend, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) !CT {
        return self.vtable.concat(self.ptr, a, b, total, dim_a, dim_b);
    }

    pub fn add(self: *const ComputeBackend, a: CT, b: CT) !CT {
        return self.vtable.add(self.ptr, a, b);
    }

    pub fn addConsumeLeft(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.addConsumeLeft) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn addConsumeRight(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.addConsumeRight) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn multiplyConsumeLeft(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.multiplyConsumeLeft) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn multiplyConsumeRight(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.multiplyConsumeRight) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn subtractConsumeLeft(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.subtractConsumeLeft) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn divideConsumeLeft(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.divideConsumeLeft) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn lessThanConsumeLeft(self: *const ComputeBackend, a: CT, b: CT) !?CT {
        if (self.vtable.lessThanConsumeLeft) |f| return f(self.ptr, a, b);
        return null;
    }

    pub fn whereSelectConsumeTrue(self: *const ComputeBackend, cond: CT, on_true: CT, on_false: CT) !?CT {
        if (self.vtable.whereSelectConsumeTrue) |f| return f(self.ptr, cond, on_true, on_false);
        return null;
    }

    pub fn whereSelectConsumeFalse(self: *const ComputeBackend, cond: CT, on_true: CT, on_false: CT) !?CT {
        if (self.vtable.whereSelectConsumeFalse) |f| return f(self.ptr, cond, on_true, on_false);
        return null;
    }

    pub fn scaledDotProductAttention(self: *const ComputeBackend, Q: CT, K: CT, V: CT, mask: []const i64, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) !CT {
        return self.vtable.scaledDotProductAttention(self.ptr, Q, K, V, mask, attn_bias, batch, seq_len, num_heads, head_dim);
    }

    pub fn scaledDotProductAttentionFull(self: *const ComputeBackend, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) !?CT {
        if (self.vtable.scaledDotProductAttentionFull) |f| {
            return f(self.ptr, Q, K, V, attn_bias, batch, seq_len, num_heads, head_dim);
        }
        return null;
    }

    pub fn causalSelfAttention(self: *const ComputeBackend, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) !CT {
        return self.vtable.causalSelfAttention(self.ptr, Q, K, V, attn_bias, batch, seq_len, num_heads, head_dim);
    }

    pub fn crossAttention(self: *const ComputeBackend, Q: CT, K: CT, V: CT, enc_mask: []const i64, batch: usize, dec_seq: usize, enc_seq: usize, num_heads: usize, head_dim: usize) !CT {
        return self.vtable.crossAttention(self.ptr, Q, K, V, enc_mask, batch, dec_seq, enc_seq, num_heads, head_dim);
    }

    pub fn relativePositionBias(self: *const ComputeBackend, weight: CT, q_len: usize, k_len: usize, num_heads: usize, num_buckets: usize, max_distance: usize, bidirectional: bool) !CT {
        return self.vtable.relativePositionBias(self.ptr, weight, q_len, k_len, num_heads, num_buckets, max_distance, bidirectional);
    }

    pub fn disentangledRelativeAttention(self: *const ComputeBackend, Q: CT, K: CT, V: CT, Q_r: CT, K_r: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) !CT {
        return self.vtable.disentangledRelativeAttention(self.ptr, Q, K, V, Q_r, K_r, mask, batch, seq_len, num_heads, head_dim);
    }

    pub fn softmaxConsume(self: *const ComputeBackend, input: CT, dim: u32) !?CT {
        if (self.vtable.softmaxConsume) |f| return f(self.ptr, input, dim);
        return null;
    }

    pub fn logSoftmaxConsume(self: *const ComputeBackend, input: CT, dim: u32) !?CT {
        if (self.vtable.logSoftmaxConsume) |f| return f(self.ptr, input, dim);
        return null;
    }

    pub fn windowedSelfAttention(
        self: *const ComputeBackend,
        input: CT,
        norm_weight: CT,
        norm_bias: CT,
        qkv_weight: CT,
        qkv_bias: CT,
        proj_weight: CT,
        proj_bias: CT,
        batch: usize,
        height: usize,
        width: usize,
        dim: usize,
        num_heads: usize,
        window_size: usize,
    ) !CT {
        return self.vtable.windowedSelfAttention(
            self.ptr,
            input,
            norm_weight,
            norm_bias,
            qkv_weight,
            qkv_bias,
            proj_weight,
            proj_bias,
            batch,
            height,
            width,
            dim,
            num_heads,
            window_size,
        );
    }

    pub fn channelSelfAttention(
        self: *const ComputeBackend,
        input: CT,
        norm_weight: CT,
        norm_bias: CT,
        qkv_weight: CT,
        qkv_bias: CT,
        proj_weight: CT,
        proj_bias: CT,
        batch: usize,
        seq_len: usize,
        dim: usize,
        groups: usize,
    ) !CT {
        return self.vtable.channelSelfAttention(
            self.ptr,
            input,
            norm_weight,
            norm_bias,
            qkv_weight,
            qkv_bias,
            proj_weight,
            proj_bias,
            batch,
            seq_len,
            dim,
            groups,
        );
    }

    pub fn tokenGridConv2d(
        self: *const ComputeBackend,
        input: CT,
        weight: CT,
        bias: CT,
        batch: usize,
        in_channels: usize,
        out_channels: usize,
        height: usize,
        width: usize,
        kernel_h: usize,
        kernel_w: usize,
        stride_h: usize,
        stride_w: usize,
        padding_h: usize,
        padding_w: usize,
        groups: usize,
    ) !CT {
        return self.vtable.tokenGridConv2d(
            self.ptr,
            input,
            weight,
            bias,
            batch,
            in_channels,
            out_channels,
            height,
            width,
            kernel_h,
            kernel_w,
            stride_h,
            stride_w,
            padding_h,
            padding_w,
            groups,
        );
    }

    pub fn conv1d(self: *const ComputeBackend, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, time_steps: usize, kernel_size: usize, stride: usize, padding: usize) !CT {
        return self.vtable.conv1d(self.ptr, input, weight, bias, batch, in_channels, out_channels, time_steps, kernel_size, stride, padding);
    }

    pub fn conv2d(self: *const ComputeBackend, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, height: usize, width: usize, kernel_h: usize, kernel_w: usize, stride_h: usize, stride_w: usize, padding_h: usize, padding_w: usize, groups: usize) !CT {
        return self.vtable.conv2d(self.ptr, input, weight, bias, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups);
    }

    pub fn multiply(self: *const ComputeBackend, a: CT, b: CT) !CT {
        return self.vtable.multiply(self.ptr, a, b);
    }

    pub fn rope(self: *const ComputeBackend, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) !CT {
        return self.vtable.rope(self.ptr, input, seq_len, head_dim, rope_dim, theta, freq_scale, position_offset, consecutive_pairs);
    }

    pub fn ropePerItem(
        self: *const ComputeBackend,
        input: CT,
        batch: usize,
        max_seq_len: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        query_lengths: []const usize,
        position_offsets: []const usize,
        consecutive_pairs: bool,
    ) !CT {
        return self.vtable.ropePerItem(self.ptr, input, batch, max_seq_len, head_dim, rope_dim, theta, freq_scale, query_lengths, position_offsets, consecutive_pairs);
    }

    pub fn gqaCausalAttention(self: *const ComputeBackend, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) !CT {
        return self.vtable.gqaCausalAttention(self.ptr, Q, K, V, attn_bias, batch, seq_len, num_heads, num_kv_heads, head_dim);
    }

    pub fn gqaPagedAttention(self: *const ComputeBackend, Q: CT, K: CT, V: CT, attn_bias: ?CT, attention: AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) !CT {
        return self.vtable.gqaPagedAttention(self.ptr, Q, K, V, attn_bias, attention, batch, num_heads, num_kv_heads, head_dim);
    }

    pub fn fromFloat32(self: *const ComputeBackend, data: []const f32) !CT {
        return self.vtable.fromFloat32(self.ptr, data);
    }

    pub fn fromFloat32Shape(self: *const ComputeBackend, data: []const f32, shape: []const i32) !CT {
        return self.vtable.fromFloat32Shape(self.ptr, data, shape);
    }

    pub fn fromInt32Shape(self: *const ComputeBackend, data: []const i32, shape: []const i32) !?CT {
        if (self.vtable.fromInt32Shape) |op| {
            return op(self.ptr, data, shape);
        }
        return null;
    }

    pub fn toFloat32(self: *const ComputeBackend, tensor: CT, allocator: std.mem.Allocator) ![]f32 {
        return self.vtable.toFloat32(self.ptr, tensor, allocator);
    }

    pub fn exportTensorData(self: *const ComputeBackend, tensor: CT, allocator: std.mem.Allocator) !?ExportTensorData {
        if (self.vtable.exportTensorData) |export_tensor_data| {
            return export_tensor_data(self.ptr, tensor, allocator);
        }
        return null;
    }

    pub fn cloneTensorShape(self: *const ComputeBackend, tensor: CT, shape: []const i32) !?CT {
        if (self.vtable.cloneTensorShape) |clone_tensor_shape| {
            return clone_tensor_shape(self.ptr, tensor, shape);
        }
        return null;
    }

    pub fn tensorDType(self: *const ComputeBackend, tensor: CT) !tensor_mod.DType {
        if (self.vtable.tensorDType) |tensor_dtype| {
            return tensor_dtype(self.ptr, tensor);
        }
        return error.UnsupportedTensorType;
    }

    pub fn tensorShape(self: *const ComputeBackend, tensor: CT, allocator: std.mem.Allocator) ![]i64 {
        if (self.vtable.tensorShape) |tensor_shape| {
            return tensor_shape(self.ptr, tensor, allocator);
        }
        return error.UnsupportedShape;
    }

    pub fn evalTensor(self: *const ComputeBackend, tensor: CT) !void {
        if (self.vtable.evalTensor) |eval_tensor| {
            return eval_tensor(self.ptr, tensor);
        }
    }

    /// Download multiple tensors to f32 in a single backend round-trip.
    /// When the backend provides a batched implementation (e.g. MLX single eval
    /// of all arrays at once), this reduces GPU sync overhead significantly.
    /// Falls back to sequential toFloat32 calls when no batched path exists.
    /// Caller owns each element of the returned slice and must free them, along
    /// with the outer slice itself, using the same allocator.
    pub fn toFloat32Batch(self: *const ComputeBackend, cts: []const CT, allocator: std.mem.Allocator) ![][]f32 {
        if (self.vtable.toFloat32Batch) |op| return op(self.ptr, cts, allocator);
        const results = try allocator.alloc([]f32, cts.len);
        var done: usize = 0;
        errdefer {
            for (results[0..done]) |r| allocator.free(r);
            allocator.free(results);
        }
        for (cts, results) |ct, *r| {
            r.* = try self.toFloat32(ct, allocator);
            done += 1;
        }
        return results;
    }

    pub fn argmaxLastRow(self: *const ComputeBackend, tensor: CT, rows: usize, dim: usize) !?u32 {
        if (self.vtable.argmaxLastRow) |argmax_last_row| {
            return argmax_last_row(self.ptr, tensor, rows, dim);
        }
        return null;
    }

    pub fn sampleLastRow(self: *const ComputeBackend, request: *const SampleLastRowRequest) !?u32 {
        if (self.vtable.sampleLastRow) |sample_last_row| {
            return sample_last_row(self.ptr, request);
        }
        return null;
    }

    pub fn linearNoBiasArgmaxLastRow(self: *const ComputeBackend, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) !?u32 {
        if (self.vtable.linearNoBiasArgmaxLastRow) |op| {
            return op(self.ptr, input, weight, rows, in_dim, out_dim);
        }
        return null;
    }

    pub fn linearNoBiasArgmaxLastRowTensor(self: *const ComputeBackend, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) !?CT {
        if (self.vtable.linearNoBiasArgmaxLastRowTensor) |op| {
            return op(self.ptr, input, weight, rows, in_dim, out_dim);
        }
        return null;
    }

    pub fn decoderRuntimePrepareGreedy(self: *const ComputeBackend, request: *const DecoderRuntimeGreedyRequest) !bool {
        if (self.vtable.decoderRuntimePrepareGreedy) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeResetState(self: *const ComputeBackend) !void {
        if (self.vtable.decoderRuntimeResetState) |op| {
            return op(self.ptr);
        }
    }

    pub fn decoderRuntimeDecode(self: *const ComputeBackend, request: *const DecoderRuntimeDecodeRequest) !bool {
        if (self.vtable.decoderRuntimeDecode) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeDecodeBatch(self: *const ComputeBackend, request: *const DecoderRuntimeDecodeRequest) !bool {
        return self.decoderRuntimeDecode(request);
    }

    pub fn decoderRuntimePlanPrefillFrame(self: *const ComputeBackend, request: *const DecoderRuntimePrefillFramePlanRequest) !bool {
        if (self.vtable.decoderRuntimePlanPrefillFrame) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeExecuteGraphCommandPlanFrame(self: *const ComputeBackend, request: *const DecoderRuntimeGraphCommandPlanFrameRequest) !bool {
        if (self.vtable.decoderRuntimeExecuteGraphCommandPlanFrame) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn debertaEncoderPlanFrame(self: *const ComputeBackend, request: *const DebertaEncoderFramePlanRequest) !bool {
        if (self.vtable.debertaEncoderPlanFrame) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn debertaEncoderLayer(self: *const ComputeBackend, request: *const DebertaEncoderLayerRequest) !?CT {
        if (self.vtable.debertaEncoderLayer) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeBeginFrame(self: *const ComputeBackend) !bool {
        if (self.vtable.decoderRuntimeBeginFrame) |op| {
            return op(self.ptr);
        }
        return false;
    }

    pub fn decoderRuntimeHasActiveFrame(self: *const ComputeBackend) bool {
        if (self.vtable.decoderRuntimeHasActiveFrame) |op| {
            return op(self.ptr);
        }
        return false;
    }

    pub fn decoderRuntimeSubmitAndWaitFrame(self: *const ComputeBackend) !void {
        if (self.vtable.decoderRuntimeSubmitAndWaitFrame) |op| {
            return op(self.ptr);
        }
    }

    pub fn decoderRuntimeFlushActiveFrame(self: *const ComputeBackend) !void {
        if (self.vtable.decoderRuntimeFlushActiveFrame) |op| {
            return op(self.ptr);
        }
    }

    pub fn decoderRuntimeCancelFrame(self: *const ComputeBackend) !void {
        if (self.vtable.decoderRuntimeCancelFrame) |op| {
            return op(self.ptr);
        }
    }

    pub fn gatherPagedKvLayerCache(
        self: *const ComputeBackend,
        allocator: std.mem.Allocator,
        kv: KvCacheView,
        token_count: usize,
        layer_index: usize,
    ) !?PagedKvLayerCacheRows {
        if (self.vtable.gatherPagedKvLayerCache) |op| {
            return op(self.ptr, allocator, kv, token_count, layer_index);
        }
        return null;
    }

    pub fn seedPagedKvLayerCache(
        self: *const ComputeBackend,
        kv: KvCacheView,
        token_count: usize,
        layer_index: usize,
        k_rows_host: []const f32,
        v_rows_host: []const f32,
    ) !bool {
        if (self.vtable.seedPagedKvLayerCache) |op| {
            return op(self.ptr, kv, token_count, layer_index, k_rows_host, v_rows_host);
        }
        return false;
    }

    pub fn directFamilyTimingSnapshot(self: *const ComputeBackend) DirectFamilyTimingSnapshot {
        if (self.vtable.directFamilyTimingSnapshot) |op| {
            return op(self.ptr);
        }
        return .{};
    }

    pub fn decoderRuntimePrepareOrReuseFamily(
        self: *const ComputeBackend,
        allocator: std.mem.Allocator,
        gpt_config: gpt_model.Config,
        current_kv_tokens: usize,
        configured_layer_count: usize,
    ) !DecoderRuntimePrepareReuseResult {
        if (self.vtable.decoderRuntimePrepareOrReuseFamily) |op| {
            return op(self.ptr, allocator, gpt_config, current_kv_tokens, configured_layer_count);
        }
        return .{};
    }

    pub fn debugTimingSnapshot(self: *const ComputeBackend) BackendDebugTimingSnapshot {
        if (self.vtable.debugTimingSnapshot) |op| {
            return op(self.ptr);
        }
        return .{};
    }

    pub fn resetDebugTimingStats(self: *const ComputeBackend) void {
        if (self.vtable.resetDebugTimingStats) |op| {
            op(self.ptr);
        }
    }

    pub fn decoderRuntimeReady(self: *const ComputeBackend) bool {
        if (self.vtable.decoderRuntimeReady) |op| {
            return op(self.ptr);
        }
        return false;
    }

    pub fn decoderRuntimeAbsoluteEmbeddingsPrepared(self: *const ComputeBackend) bool {
        if (self.vtable.decoderRuntimeAbsoluteEmbeddingsPrepared) |op| {
            return op(self.ptr);
        }
        return false;
    }

    pub fn decoderRuntimePrepareAbsoluteEmbeddings(self: *const ComputeBackend, request: *const DecoderRuntimePrepareAbsoluteEmbeddingsRequest) !bool {
        if (self.vtable.decoderRuntimePrepareAbsoluteEmbeddings) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeEmbedAbsolutePosition(self: *const ComputeBackend, request: *const DecoderRuntimeEmbedAbsolutePositionRequest) !?CT {
        if (self.vtable.decoderRuntimeEmbedAbsolutePosition) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimePrepareLayerNorm(self: *const ComputeBackend, request: *const DecoderRuntimePrepareLayerNormRequest) !bool {
        if (self.vtable.decoderRuntimePrepareLayerNorm) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeEnsureLayerNormSlot(self: *const ComputeBackend, request: *const DecoderRuntimeEnsureLayerNormSlotRequest) !?usize {
        if (self.vtable.decoderRuntimeEnsureLayerNormSlot) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLayerNorm(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLayerNormRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyLayerNorm) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimePrepareRmsNorm(self: *const ComputeBackend, request: *const DecoderRuntimePrepareRmsNormRequest) !bool {
        if (self.vtable.decoderRuntimePrepareRmsNorm) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeEnsureRmsNormSlot(self: *const ComputeBackend, request: *const DecoderRuntimeEnsureRmsNormSlotRequest) !?usize {
        if (self.vtable.decoderRuntimeEnsureRmsNormSlot) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyRmsNorm(self: *const ComputeBackend, request: *const DecoderRuntimeApplyRmsNormRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyRmsNorm) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLayerNormLinearArgmax(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLayerNormLinearArgmaxRequest) !?usize {
        if (self.vtable.decoderRuntimeApplyLayerNormLinearArgmax) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLayerNormLinear(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLayerNormLinearRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyLayerNormLinear) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLayerNormLinearSample(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLayerNormLinearSampleRequest) !?usize {
        if (self.vtable.decoderRuntimeApplyLayerNormLinearSample) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyRmsNormLinearArgmax(self: *const ComputeBackend, request: *const DecoderRuntimeApplyRmsNormLinearArgmaxRequest) !?usize {
        if (self.vtable.decoderRuntimeApplyRmsNormLinearArgmax) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyRmsNormLinear(self: *const ComputeBackend, request: *const DecoderRuntimeApplyRmsNormLinearRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyRmsNormLinear) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyRmsNormLinearSample(self: *const ComputeBackend, request: *const DecoderRuntimeApplyRmsNormLinearSampleRequest) !?usize {
        if (self.vtable.decoderRuntimeApplyRmsNormLinearSample) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimePrepareLinear(self: *const ComputeBackend, request: *const DecoderRuntimePrepareLinearRequest) !bool {
        if (self.vtable.decoderRuntimePrepareLinear) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn decoderRuntimeEnsureLinearSlot(self: *const ComputeBackend, request: *const DecoderRuntimeEnsureLinearSlotRequest) !?usize {
        if (self.vtable.decoderRuntimeEnsureLinearSlot) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLinear(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLinearRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyLinear) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLinearArgmax(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLinearArgmaxRequest) !?usize {
        if (self.vtable.decoderRuntimeApplyLinearArgmax) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLinearPair(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLinearPairRequest) !?LinearNoBiasPairResult {
        if (self.vtable.decoderRuntimeApplyLinearPair) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyLinearQkv(self: *const ComputeBackend, request: *const DecoderRuntimeApplyLinearQkvRequest) !?LinearNoBiasTripleResult {
        if (self.vtable.decoderRuntimeApplyLinearQkv) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyActivation(self: *const ComputeBackend, request: *const DecoderRuntimeApplyActivationRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyActivation) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyAdd(self: *const ComputeBackend, request: *const DecoderRuntimeApplyAddRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyAdd) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyAddScale(self: *const ComputeBackend, request: *const DecoderRuntimeApplyAddScaleRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyAddScale) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn decoderRuntimeApplyScaledAddScale(self: *const ComputeBackend, request: *const DecoderRuntimeApplyScaledAddScaleRequest) !?CT {
        if (self.vtable.decoderRuntimeApplyScaledAddScale) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runDenseFfnResidual(self: *const ComputeBackend, request: *const RunDenseFfnResidualRequest) !?CT {
        if (self.vtable.runDenseFfnResidual) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runGatedFfnResidual(self: *const ComputeBackend, request: *const RunGatedFfnResidualRequest) !?CT {
        if (self.vtable.runGatedFfnResidual) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runAttention(self: *const ComputeBackend, request: *const RunAttentionRequest) !?CT {
        if (self.vtable.runAttention) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runDeepSeekV4CompressedAttention(self: *const ComputeBackend, request: *const DeepSeekV4CompressedAttentionRequest) !?CT {
        if (self.vtable.runDeepSeekV4CompressedAttention) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn seedPagedAttentionSpan(self: *const ComputeBackend, request: *const SeedPagedAttentionSpanRequest) !bool {
        if (self.vtable.seedPagedAttentionSpan) |op| {
            return op(self.ptr, request);
        }
        return false;
    }

    pub fn runAttentionResidual(self: *const ComputeBackend, request: *const RunAttentionResidualRequest) !?CT {
        if (self.vtable.runAttentionResidual) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runAttentionOutputResidual(self: *const ComputeBackend, request: *const RunAttentionOutputResidualRequest) !?CT {
        if (self.vtable.runAttentionOutputResidual) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runDenseDecoderBlock(self: *const ComputeBackend, request: *const RunDenseDecoderBlockRequest) !?CT {
        if (self.vtable.runDenseDecoderBlock) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runGatedDecoderBlock(self: *const ComputeBackend, request: *const RunGatedDecoderBlockRequest) !?CT {
        if (self.vtable.runGatedDecoderBlock) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    pub fn runMoeBlock(self: *const ComputeBackend, request: *const RunMoeBlockRequest) !?CT {
        if (self.vtable.runMoeBlock) |op| {
            return op(self.ptr, request);
        }
        return null;
    }

    /// GPU-accelerated LoRA gradient accumulation.
    /// Returns true if the GPU path ran, false if the backend does not support it
    /// (so the caller can fall back to the CPU implementation).
    pub fn accumulateLoRAGrads(
        self: ComputeBackend,
        allocator: std.mem.Allocator,
        grad_a: []f32,
        grad_b: []f32,
        inputs: []const f32,
        output_grads: []const f32,
        lora_a: []const f32,
        lora_b: []const f32,
        rows: usize,
        in_features: usize,
        out_features: usize,
        rank: usize,
        scale: f32,
    ) anyerror!bool {
        if (self.vtable.accumulateLoRAGrads) |f| {
            try f(self.ptr, allocator, grad_a, grad_b, inputs, output_grads, lora_a, lora_b, rows, in_features, out_features, rank, scale);
            return true;
        }
        return false;
    }

    /// Reshape a 2D tensor to new dimensions (same total elements).
    /// Returns null if the backend doesn't support reshape.
    pub fn reshape2d(self: *const ComputeBackend, input: CT, rows: usize, cols: usize) !?CT {
        if (self.vtable.reshape2d) |reshape| {
            return reshape(self.ptr, input, rows, cols);
        }
        return null;
    }

    /// Slice the last dimension: [rows, D] → [rows, stop-start].
    /// Falls back to CPU download+slice+upload if not supported.
    pub fn sliceLastDim(self: *const ComputeBackend, input: CT, start: usize, stop: usize) !CT {
        if (self.vtable.sliceLastDim) |slice_fn| {
            return slice_fn(self.ptr, input, start, stop);
        }
        return error.UnsupportedOperation;
    }

    // ── Primitive op wrappers (for training / lowered graph execution) ──

    pub fn primSubtract(self: *const ComputeBackend, a: CT, b: CT) !CT {
        if (self.vtable.subtract) |f| return f(self.ptr, a, b);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primDivide(self: *const ComputeBackend, a: CT, b: CT) !CT {
        if (self.vtable.divide) |f| return f(self.ptr, a, b);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primNegate(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.negate) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primSqrt(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.sqrtOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primRsqrt(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.rsqrtOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primExp(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.expOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primLog(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.logOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primSin(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.sinOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primCos(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.cosOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primTanh(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.tanhOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primErf(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.erfOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primAbs(self: *const ComputeBackend, a: CT) !CT {
        if (self.vtable.absOp) |f| return f(self.ptr, a);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primLessThan(self: *const ComputeBackend, a: CT, b: CT) !CT {
        if (self.vtable.lessThan) |f| return f(self.ptr, a, b);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primWhereSelect(self: *const ComputeBackend, cond: CT, on_true: CT, on_false: CT) !CT {
        if (self.vtable.whereSelect) |f| return f(self.ptr, cond, on_true, on_false);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primReduceSum(self: *const ComputeBackend, input: CT, axes: []const u8, input_shape: []const i64) !CT {
        if (self.vtable.reduceSumOp) |f| return f(self.ptr, input, axes, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primReduceMax(self: *const ComputeBackend, input: CT, axes: []const u8, input_shape: []const i64) !CT {
        if (self.vtable.reduceMaxOp) |f| return f(self.ptr, input, axes, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primReduceMean(self: *const ComputeBackend, input: CT, axes: []const u8, input_shape: []const i64) !CT {
        if (self.vtable.reduceMeanOp) |f| return f(self.ptr, input, axes, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primArgMax(self: *const ComputeBackend, input: CT, axis: u8, keepdims: bool, input_shape: []const i64) !CT {
        if (self.vtable.argmaxOp) |f| return f(self.ptr, input, axis, keepdims, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primReshape(self: *const ComputeBackend, input: CT, new_shape: []const i64) !CT {
        if (self.vtable.reshapeOp) |f| return f(self.ptr, input, new_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primTranspose(self: *const ComputeBackend, input: CT, perm: []const u8, input_shape: []const i64) !CT {
        if (self.vtable.transposeOp) |f| return f(self.ptr, input, perm, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primBroadcastInDim(self: *const ComputeBackend, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) !CT {
        if (self.vtable.broadcastInDimOp) |f| return f(self.ptr, input, target_shape, broadcast_axes, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primDotGeneral(self: *const ComputeBackend, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) !CT {
        if (self.vtable.dotGeneralOp) |f| return f(self.ptr, lhs, rhs, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primScatterAdd(self: *const ComputeBackend, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) !CT {
        if (self.vtable.scatterAddOp) |f| return f(self.ptr, input, indices, input_shape, indices_shape, axis);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primGather(self: *const ComputeBackend, input: CT, indices: CT, axis: u8, input_shape: []const i64) !CT {
        if (self.vtable.gatherOp) |f| return f(self.ptr, input, indices, axis, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primSlice(self: *const ComputeBackend, input: CT, starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) !CT {
        if (self.vtable.sliceOp) |f| return f(self.ptr, input, starts, limits, strides, input_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primConcatPrim(self: *const ComputeBackend, a: CT, b: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) !CT {
        if (self.vtable.concatPrimOp) |f| return f(self.ptr, a, b, axis, a_shape, b_shape);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primSoftmax(self: *const ComputeBackend, input: CT, dim: u32) !CT {
        if (self.vtable.softmaxOp) |f| return f(self.ptr, input, dim);
        return error.UnsupportedPrimitiveOp;
    }
    pub fn primLogSoftmax(self: *const ComputeBackend, input: CT, dim: u32) !CT {
        if (self.vtable.logSoftmaxOp) |f| return f(self.ptr, input, dim);
        return error.UnsupportedPrimitiveOp;
    }
};

fn fallbackLinearLoRA(
    self: *const ComputeBackend,
    input: CT,
    base_weight: CT,
    bias: CT,
    lora_a: CT,
    lora_b: CT,
    alpha: f32,
    rank: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    _ = rank;
    _ = .{ self, input, base_weight, bias, lora_a, lora_b, alpha, rows, in_dim, out_dim };
    return error.LinearLoRANotImplemented;
}
