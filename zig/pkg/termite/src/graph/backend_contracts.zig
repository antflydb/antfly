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

// Backend-neutral runtime contracts shared by graph/model runtimes and
// concrete backend implementations.
//
// Keep device-specific storage, kernels, and command submission out of this
// file. Those stay in backend modules such as backends/metal_runtime.zig.

const std = @import("std");
const ml = @import("ml");
const runtime = @import("../runtime/root.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const quant_matmul = @import("quant_matmul.zig");

/// Opaque tensor handle. The concrete type depends on the compute backend.
/// Tensors are freed by the backend that created them.
pub const CT = *anyopaque;

pub const BackendKind = enum {
    native,
    metal,
    mlx,
    onnx,
    pjrt,
    cuda,
    wasm,
    webgpu,
    graph,
};

pub const TensorStorageClass = enum {
    unknown,
    host_f32,
    host_dense,
    host_packed_quant,
    metal_buffer,
    webgpu_buffer,
    mlx_array,
    onnx_tensor,
    pjrt_buffer,
    cuda_buffer,
    runtime_input,
    constant,
    metadata_view,

    pub fn isHost(self: TensorStorageClass) bool {
        return switch (self) {
            .host_f32, .host_dense, .host_packed_quant, .runtime_input, .constant => true,
            else => false,
        };
    }

    pub fn isDevice(self: TensorStorageClass) bool {
        return switch (self) {
            .metal_buffer, .webgpu_buffer, .mlx_array, .onnx_tensor, .pjrt_buffer, .cuda_buffer => true,
            else => false,
        };
    }

    pub fn isViewMetadata(self: TensorStorageClass) bool {
        return self == .metadata_view;
    }
};

pub const TensorStrides = struct {
    values: [ml.graph.shape.max_rank]i64 = .{0} ** ml.graph.shape.max_rank,
    rank: u8 = 0,

    pub fn init(strides: []const i64) TensorStrides {
        std.debug.assert(strides.len <= ml.graph.shape.max_rank);
        var result = TensorStrides{ .rank = @intCast(strides.len) };
        @memcpy(result.values[0..strides.len], strides);
        return result;
    }

    pub fn none() TensorStrides {
        return .{};
    }

    pub fn dense(shape: ml.graph.Shape) TensorStrides {
        var result = TensorStrides{ .rank = shape.rank() };
        var stride: i64 = 1;
        var axis = shape.rank();
        while (axis > 0) {
            axis -= 1;
            result.values[axis] = stride;
            const dim = shape.dims[axis];
            if (dim > 0) {
                stride = std.math.mul(i64, stride, dim) catch 0;
            } else {
                stride = 0;
            }
        }
        return result;
    }

    pub fn asSlice(self: *const TensorStrides) []const i64 {
        return self.values[0..self.rank];
    }
};

pub const TensorDesc = struct {
    shape: ml.graph.Shape,
    storage: TensorStorageClass = .unknown,
    strides: TensorStrides = .{},
    quant_format: ?quant_matmul.Format = null,
    attention_kv_format: ?quant_matmul.AttentionKvFormat = null,
    attention_storage: ?quant_matmul.AttentionStorage = null,
    view_source: ?ml.graph.NodeId = null,
    resident_backend: ?BackendKind = null,
    device_id: u32 = 0,

    pub fn init(shape: ml.graph.Shape, storage: TensorStorageClass) TensorDesc {
        return .{
            .shape = shape,
            .storage = storage,
            .strides = TensorStrides.dense(shape),
        };
    }

    pub fn view(
        shape: ml.graph.Shape,
        source: ml.graph.NodeId,
        strides: TensorStrides,
        resident_backend: ?BackendKind,
        device_id: u32,
    ) TensorDesc {
        return .{
            .shape = shape,
            .storage = .metadata_view,
            .strides = strides,
            .view_source = source,
            .resident_backend = resident_backend,
            .device_id = device_id,
        };
    }

    pub fn packedQuant(shape: ml.graph.Shape, format: quant_matmul.Format) TensorDesc {
        return .{
            .shape = shape,
            .storage = .host_packed_quant,
            .strides = TensorStrides.dense(shape),
            .quant_format = format,
        };
    }

    pub fn attentionKv(
        shape: ml.graph.Shape,
        storage_class: TensorStorageClass,
        kv_format: quant_matmul.AttentionKvFormat,
        attention_storage: quant_matmul.AttentionStorage,
        resident_backend: ?BackendKind,
        device_id: u32,
    ) TensorDesc {
        return .{
            .shape = shape,
            .storage = storage_class,
            .strides = TensorStrides.dense(shape),
            .attention_kv_format = kv_format,
            .attention_storage = attention_storage,
            .resident_backend = resident_backend,
            .device_id = device_id,
        };
    }

    pub fn isView(self: TensorDesc) bool {
        return self.storage.isViewMetadata() or self.view_source != null;
    }

    pub fn isHostResident(self: TensorDesc) bool {
        return self.storage.isHost();
    }

    pub fn isDeviceResident(self: TensorDesc) bool {
        return self.storage.isDevice();
    }

    pub fn isPackedQuant(self: TensorDesc) bool {
        return self.quant_format != null or self.storage == .host_packed_quant;
    }

    pub fn isAttentionKv(self: TensorDesc) bool {
        return self.attention_kv_format != null;
    }
};

pub fn quantFormatFromGgufTensorType(tensor_type: gguf_tensor_types.TensorType) ?quant_matmul.Format {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .Q4_0 => .q4_0,
            .Q4_1 => .q4_1,
            .Q5_0 => .q5_0,
            .Q5_1 => .q5_1,
            .Q8_0 => .q8_0,
            .Q8_1 => .q8_1,
            .Q2_K => .q2_k,
            .Q3_K => .q3_k,
            .Q4_K => .q4_k,
            .Q5_K => .q5_k,
            .Q6_K => .q6_k,
            .Q8_K => .q8_k,
            .IQ1_S => .iq1_s,
            .IQ1_M => .iq1_m,
            .IQ2_XXS => .iq2_xxs,
            .IQ2_XS => .iq2_xs,
            .IQ2_S => .iq2_s,
            .IQ3_XXS => .iq3_xxs,
            .IQ3_S => .iq3_s,
            .IQ4_NL => .iq4_nl,
            .IQ4_XS => .iq4_xs,
            .TQ1_0 => .tq1_0,
            .TQ2_0 => .tq2_0,
            .I2_S => .i2_s,
            .I8_S => .i8_s,
            .TL1 => .tl1,
            .MXFP4 => .mxfp4,
            .NVFP4 => .nvfp4,
            .Q1_0 => .q1_0,
            else => null,
        },
        .bitnet_tl2 => .tl2,
        else => null,
    };
}

pub const DecoderRuntimeGreedyRequest = struct {
    hidden_size: usize,
    intermediate_size: usize,
    num_layers: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    vocab_size: usize,
    kv_tokens: usize,
};

pub const DecoderRuntimePrepareAbsoluteEmbeddingsRequest = struct {
    token_embedding: CT,
    position_embedding: CT,
    vocab_size: usize,
    max_position_embeddings: usize,
    hidden_size: usize,
};

pub const DecoderRuntimeEmbedAbsolutePositionRequest = struct {
    token_id: usize,
    position_id: usize,
    hidden_size: usize,
};

pub const DecoderRuntimePrepareLayerNormRequest = struct {
    slot: usize,
    weight: CT,
    bias: CT,
    hidden_size: usize,
};

pub const DecoderRuntimeEnsureLayerNormSlotRequest = struct {
    weight: CT,
    bias: CT,
    hidden_size: usize,
};

pub const DecoderRuntimeApplyLayerNormRequest = struct {
    slot: usize,
    input: CT,
    hidden_size: usize,
    eps: f32,
};

pub const DecoderRuntimePrepareRmsNormRequest = struct {
    slot: usize,
    weight: CT,
    hidden_size: usize,
};

pub const DecoderRuntimeEnsureRmsNormSlotRequest = struct {
    weight: CT,
    hidden_size: usize,
};

pub const DecoderRuntimeApplyRmsNormRequest = struct {
    slot: usize,
    input: CT,
    hidden_size: usize,
    eps: f32,
};

pub const DecoderRuntimeApplyLayerNormLinearArgmaxRequest = struct {
    norm_slot: usize,
    linear_slot: usize,
    input: CT,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLayerNormLinearRequest = struct {
    norm_slot: usize,
    linear_slot: usize,
    input: CT,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyRmsNormLinearArgmaxRequest = struct {
    norm_slot: usize,
    linear_slot: usize,
    input: CT,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyRmsNormLinearRequest = struct {
    norm_slot: usize,
    linear_slot: usize,
    input: CT,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLayerNormLinearSampleRequest = struct {
    norm_slot: usize,
    linear_slot: usize,
    input: CT,
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
    norm_slot: usize,
    linear_slot: usize,
    input: CT,
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

pub const DecoderRuntimePrepareLinearRequest = struct {
    slot: usize,
    weight: CT,
    bias: CT,
    in_dim: usize,
    out_dim: usize,
    retain_dense_fallback: bool = true,
};

pub const DecoderRuntimeEnsureLinearSlotRequest = struct {
    weight: CT,
    bias: ?CT,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearRequest = struct {
    slot: usize,
    input: CT,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearArgmaxRequest = struct {
    slot: usize,
    input: CT,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearPairRequest = struct {
    slot_a: usize,
    slot_b: usize,
    input: CT,
    in_dim: usize,
    out_dim: usize,
};

pub const DecoderRuntimeApplyLinearQkvRequest = struct {
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
    input: CT,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
};

pub const DecoderRuntimeActivationKind = enum(u8) {
    gelu,
    gelu_new,
    silu,
    relu,
    quick_gelu,
    relu_squared,
};

pub const PlannedLayerContract = struct {
    ops: []const u16 = &.{},
    barriers: []const u8 = &.{},
    quant_dispatches: []const u8 = &.{},
    command_ops: []const PlannedCommandOp = &.{},
    start_index: usize = 0,
};

pub const PlannedCommandOp = extern struct {
    kind: u16 = 0,
    barrier_before: u8 = 0,
    quant_dispatch: u8 = 255,
    operator: u8 = 255,
    format: u8 = 255,
    input_dtype: u8 = 0,
    output_dtype: u8 = 0,
    source: u32 = 0,
    region: u32 = 0,
    scope_index: u32 = 0,
    resource_start: usize = 0,
    resource_count: usize = 0,
};

pub const DecoderRuntimeApplyActivationRequest = struct {
    input: CT,
    kind: DecoderRuntimeActivationKind,
    dim: usize,
};

pub const DecoderRuntimeApplyAddRequest = struct {
    lhs: CT,
    rhs: CT,
    dim: usize,
};

pub const DecoderRuntimeApplyAddScaleRequest = struct {
    lhs: CT,
    rhs: CT,
    dim: usize,
    scale: f32,
};

pub const DecoderRuntimeApplyScaledAddScaleRequest = struct {
    lhs: CT,
    rhs: CT,
    dim: usize,
    lhs_scale: f32,
    output_scale: f32,
};

pub const AttentionMode = enum {
    dense_causal,
    paged_prefill,
    paged_decode,
};

pub const KvCacheView = struct {
    sequence_id: runtime.kv.manager.SequenceId,
    pool_id: runtime.kv.block.KvPoolId,
    logical_block_count: usize,
    tail_tokens: u16,
    position_offset: usize = 0,
    logical_blocks: ?[]const runtime.kv.block.KvBlockId = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
};

pub const KvBatchView = struct {
    kv_cache: KvCacheView,
    kv_manager: *runtime.kv.manager.KvManager,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
    /// Per-item overrides for mixed prefill+decode batches.
    /// When set, these override the shared AttentionContext fields for this item.
    per_item_query_len: ?usize = null,
    per_item_total_len: ?usize = null,
    per_item_kv_len: ?usize = null,
    per_item_kv_position_offset: ?usize = null,
    per_item_mode: ?AttentionMode = null,
};

pub const AttentionSinkMetadata = struct {
    /// Backend-owned tensor containing one sink value per attention head.
    per_head_tensor: ?CT = null,
    /// Backend-owned slot handle for a prepared per-head sink tensor.
    slot: ?usize = null,

    pub fn hasMetadata(self: AttentionSinkMetadata) bool {
        return self.per_head_tensor != null or self.slot != null;
    }
};

pub const AttentionContext = struct {
    mode: AttentionMode = .dense_causal,
    total_sequence_len: usize,
    query_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize = 0,
    decoder_runtime_resident_kv_sequence_len: ?usize = null,
    decoder_runtime_resident_kv_position_offset: ?usize = null,
    sliding_window: usize = 0,
    /// Optional square [total_sequence_len, total_sequence_len] mask where a
    /// non-zero entry allows attention to bypass the causal future-token mask.
    /// Used by Gemma 3 multimodal prefill to give image soft tokens
    /// bidirectional attention within the same image block.
    attn_or_mask: ?[]const u8 = null,
    kv_cache: ?KvCacheView = null,
    kv_manager: ?*runtime.kv.manager.KvManager = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
    kv_batch: ?[]const KvBatchView = null,
    layer_index: usize = 0,
    /// When true, skip writing K/V to the cache (shared KV layers read from a donor layer).
    skip_kv_write: bool = false,
    /// Optional attention-sink metadata for models that prepend per-head sinks
    /// to the attention score stream. Backends that do not implement sinks
    /// should leave this empty.
    attention_sink: AttentionSinkMetadata = .{},
};

pub const RunDenseFfnResidualRequest = struct {
    first_linear_slot: usize,
    second_linear_slot: usize,
    input: CT,
    residual: CT,
    hidden_size: usize,
    intermediate_size: usize,
    activation: DecoderRuntimeActivationKind,
};

pub const RunGatedFfnResidualRequest = struct {
    gate_linear_slot: usize,
    up_linear_slot: usize,
    down_linear_slot: usize,
    input: CT,
    residual: CT,
    post_gate_rms_norm_slot: ?usize = null,
    post_gate_rms_norm_weight: ?CT = null,
    post_down_rms_norm_slot: ?usize = null,
    post_down_rms_norm_weight: ?CT = null,
    hidden_size: usize,
    intermediate_size: usize,
    eps: f32 = 0.0,
    activation: DecoderRuntimeActivationKind,
    planned_layer_contract: PlannedLayerContract = .{},
};

pub const RunAttentionRequest = struct {
    q: CT,
    k: CT,
    v: CT,
    attention: AttentionContext,
    attention_sink: AttentionSinkMetadata = .{},
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
};

pub const DeepSeekV4CompressedAttentionPath = enum(u8) {
    heavily_compressed,
    compressed_sparse,
};

/// DeepSeek V4's model contract for compressed attention. Backend
/// implementations should keep their lower-level kernels generic where the
/// operation is reusable compressed-attention/cache maintenance rather than a
/// DeepSeek-only primitive.
pub const DeepSeekV4CompressedComponentRequest = struct {
    projected: CT,
    gate: CT,
    bias: CT,
    norm: CT,
    row_dim: usize,
    gate_width: usize,
};

pub const DeepSeekV4CompressedAttentionRequest = struct {
    cache_key: usize,
    layer_index: usize,
    path: DeepSeekV4CompressedAttentionPath,
    q: CT,
    local_kv: CT,
    sinks: CT,
    compressor: DeepSeekV4CompressedComponentRequest,
    indexer: ?DeepSeekV4CompressedComponentRequest = null,
    index_query: ?CT = null,
    index_head_weights: ?CT = null,
    query_abs_start: usize,
    query_rows: usize,
    total_tokens: usize,
    num_heads: usize,
    head_dim: usize,
    sliding_window: usize,
    compress_rate: usize,
    top_k: usize,
    index_heads: usize = 0,
    index_head_dim: usize = 0,
    rope_dim: usize,
    rope_theta: f32,
    rope_freq_scale: f32 = 1.0,
    rope_consecutive_pairs: bool = false,
    eps: f32,
};

pub const SeedPagedAttentionSpanRequest = struct {
    k: CT,
    v: CT,
    attention: AttentionContext,
    num_kv_heads: usize,
    head_dim: usize,
};

pub const RunAttentionResidualRequest = struct {
    q: CT,
    k: CT,
    v: CT,
    residual: CT,
    attention: AttentionContext,
    attention_sink: AttentionSinkMetadata = .{},
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    linear_slot: usize,
    pre_linear_rms_norm_slot: ?usize = null,
    post_linear_rms_norm_slot: ?usize = null,
    hidden_size: usize,
    eps: f32,
};

pub const RunAttentionOutputResidualRequest = struct {
    attention_output: CT,
    residual: CT,
    rows: usize,
    attention_input_size: usize,
    hidden_size: usize,
    linear_slot: usize,
    pre_linear_rms_norm_slot: ?usize = null,
    post_linear_rms_norm_slot: ?usize = null,
    eps: f32,
};

pub const RunDenseDecoderBlockRequest = struct {
    q: ?CT = null,
    k: ?CT = null,
    v: ?CT = null,
    attention_input: ?CT = null,
    residual: CT,
    attention: AttentionContext,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    fused_qkv_linear_slot: ?usize = null,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize = null,
    attention_post_linear_rms_norm_slot: ?usize = null,
    hidden_size: usize,
    eps: f32,
    ffn_layer_norm_slot: ?usize = null,
    ffn_rms_norm_slot: ?usize = null,
    first_ffn_linear_slot: usize,
    second_ffn_linear_slot: usize,
    intermediate_size: usize,
    activation: DecoderRuntimeActivationKind,
};

pub const RunGatedDecoderBlockRequest = struct {
    q: ?CT = null,
    k: ?CT = null,
    v: ?CT = null,
    attention_input: ?CT = null,
    residual: CT,
    attention: AttentionContext,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    q_linear_slot: ?usize = null,
    k_linear_slot: ?usize = null,
    v_linear_slot: ?usize = null,
    q_head_norm_slot: ?usize = null,
    k_head_norm_slot: ?usize = null,
    rope_active_dim: usize = 0,
    rope_theta: f32 = 10000.0,
    rope_freq_scale: f32 = 1.0,
    rope_consecutive_pairs: bool = false,
    global_head_dim: usize = 0,
    attention_linear_slot: usize,
    attention_pre_linear_rms_norm_slot: ?usize = null,
    attention_post_linear_rms_norm_slot: ?usize = null,
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
    activation: DecoderRuntimeActivationKind,
    ple: ?CT = null,
    ple_gate_linear_slot: ?usize = null,
    ple_proj_linear_slot: ?usize = null,
    ple_post_norm_slot: ?usize = null,
    ple_hidden_size: usize = 0,
    output_scale: ?CT = null,
    output_scale_value: ?f32 = null,
    planned_setup_contract: PlannedLayerContract = .{},
    planned_layer_contract: PlannedLayerContract = .{},
    planned_frame_contract: PlannedLayerContract = .{},
    planned_frame_layer_window: ?PlannedFrameLayerWindow = null,
    graph_plan_tail_vocab_size: usize = 0,
};

pub const PlannedFrameLayerWindow = struct {
    layer_start: usize = 0,
    setup_start: usize = 0,
    block_start: usize = 0,
    layer_end: usize = 0,
};

pub const PagedKvLayerCacheRows = struct {
    k: []f32,
    v: []f32,
};

pub const DirectFamilyTimingSnapshot = struct {
    project_nanos: u128 = 0,
    span_prep_nanos: u128 = 0,
    quant_attn_nanos: u128 = 0,
    block_apply_nanos: u128 = 0,
    frame_wait_nanos: u128 = 0,
    frame_gpu_nanos: u128 = 0,

    pub fn delta(after: DirectFamilyTimingSnapshot, before: DirectFamilyTimingSnapshot) DirectFamilyTimingSnapshot {
        return .{
            .project_nanos = after.project_nanos - before.project_nanos,
            .span_prep_nanos = after.span_prep_nanos - before.span_prep_nanos,
            .quant_attn_nanos = after.quant_attn_nanos - before.quant_attn_nanos,
            .block_apply_nanos = after.block_apply_nanos - before.block_apply_nanos,
            .frame_wait_nanos = after.frame_wait_nanos - before.frame_wait_nanos,
            .frame_gpu_nanos = after.frame_gpu_nanos - before.frame_gpu_nanos,
        };
    }
};

pub const DecoderRuntimePrepareReuseResult = struct {
    prepared: bool = false,
    reserve_kv_tokens: usize = 0,
    fast_hit: bool = false,
    used_greedy: bool = false,
};

pub const DecoderRuntimeDecodeContract = enum(u8) {
    gemma4_gated_ple_shared_kv,
    gliner_deberta_encoder,
    qwen3_dense_text_embedding,
};

pub const DecoderRuntimeDecodeMode = enum(u8) {
    greedy_argmax,
};

pub const DecoderRuntimeLayerSpec = struct {
    kv_heads: usize,
    head_dim: usize,
    intermediate_size: usize,
    kv_layer_index: usize,
    shares_kv: bool,
    sliding_window: usize,
    rope_dim: usize,
    rope_active_dim: usize,
    rope_theta: f32,
    attn_pre_norm_slot: usize,
    attn_post_norm_slot: usize,
    ffn_pre_norm_slot: usize,
    ffn_post_norm_slot: usize,
    q_head_norm_slot: ?usize = null,
    k_head_norm_slot: ?usize = null,
    q_linear_slot: usize,
    k_linear_slot: usize,
    v_linear_slot: usize,
    attention_linear_slot: usize,
    gate_ffn_linear_slot: usize,
    up_ffn_linear_slot: usize,
    down_ffn_linear_slot: usize,
    ple_gate_linear_slot: ?usize = null,
    ple_proj_linear_slot: ?usize = null,
    ple_post_norm_slot: ?usize = null,
    output_scale_value: ?f32 = null,
};

pub const DecoderRuntimeDecodeItem = struct {
    token_id: i64,
    position: usize,
    seq_len: usize,
    attention: AttentionContext,
};

pub const DecoderRuntimeDecodeRequest = struct {
    contract: DecoderRuntimeDecodeContract,
    mode: DecoderRuntimeDecodeMode = .greedy_argmax,
    configured_layer_count: usize,
    layer_count: usize,
    hidden_size: usize,
    vocab_size: usize,
    num_attention_heads: usize,
    norm_eps: f32,
    ple_hidden_size: usize,
    token_embedding_scale: f32,
    global_head_dim: usize,
    rope_freq_scale: f32,
    rope_consecutive_pairs: bool,
    activation: DecoderRuntimeActivationKind,
    final_norm_slot: usize,
    final_lm_head_slot: usize,
    ple_model_proj_slot: ?usize = null,
    ple_proj_norm_slot: ?usize = null,
    layers: []const DecoderRuntimeLayerSpec,
    items: []const DecoderRuntimeDecodeItem,
    token_embedding_weight: ?CT = null,
    ple_token_embedding_weight: ?CT = null,
    /// Optional backend tensor containing one token id per item. When null,
    /// `items[*].token_id` is authoritative.
    input_token_ids: ?CT = null,
    /// Optional backend tensor containing one position per item. When null,
    /// `items[*].position` is authoritative.
    input_positions: ?CT = null,
    /// Optional backend-owned token-id output tensor for the next decode step.
    /// Backends may leave this null and use host writeback only.
    output_token_ids_tensor: ?*CT = null,
    /// Host-visible token writeback for each item. Backends may keep token ids
    /// device-owned internally, but successful calls must populate this slice.
    output_token_ids: []i64,
};

pub const DecoderRuntimePrefillFramePlanRequest = struct {
    contract: DecoderRuntimeDecodeContract,
    layer_count: usize,
    rows: usize,
    batch: usize = 1,
    seq_len: usize = 0,
    hidden_size: usize,
    vocab_size: usize,
    num_attention_heads: usize,
    global_head_dim: usize,
    ple_hidden_size: usize,
    final_norm_slot: usize,
    final_lm_head_slot: usize,
    include_tail: bool = true,
    layers: []const DecoderRuntimeLayerSpec,
};

pub const DecoderRuntimeGraphCommandPlanFrameRequest = struct {
    contract: DecoderRuntimeDecodeContract,
    layer_count: usize,
    rows: usize,
    batch: usize = 1,
    seq_len: usize = 0,
    hidden_size: usize,
    vocab_size: usize,
    num_attention_heads: usize,
    global_head_dim: usize,
    ple_hidden_size: usize,
    final_norm_slot: usize = 0,
    norm_eps: f32,
    rope_freq_scale: f32,
    rope_consecutive_pairs: bool,
    activation: DecoderRuntimeActivationKind,
    attention: AttentionContext,
    hidden: CT,
    ple_vectors: ?CT = null,
    layers: []const DecoderRuntimeLayerSpec,
    output_hidden: *?CT,
};

pub const DebertaEncoderLayerSpec = struct {
    q_linear_slot: usize,
    k_linear_slot: usize,
    v_linear_slot: usize,
    attention_output_linear_slot: usize,
    intermediate_linear_slot: usize,
    output_linear_slot: usize,
    attention_layer_norm_slot: usize,
    output_layer_norm_slot: usize,
};

pub const DebertaEncoderFramePlanRequest = struct {
    contract: DecoderRuntimeDecodeContract = .gliner_deberta_encoder,
    layer_count: usize,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
    intermediate_size: usize,
    num_attention_heads: usize,
    position_buckets: usize,
    max_position_embeddings: usize,
    norm_eps: f32,
    layers: []const DebertaEncoderLayerSpec,
};

pub const DebertaEncoderLayerRequest = struct {
    contract: DecoderRuntimeDecodeContract = .gliner_deberta_encoder,
    layer: DebertaEncoderLayerSpec,
    hidden: CT,
    relative_embeddings: CT,
    relative_full_to_unique: ?[]const i64 = null,
    relative_unique_count: usize,
    relative_full_count: usize,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
    intermediate_size: usize,
    num_attention_heads: usize,
    head_dim: usize,
    norm_eps: f32,
};

test "tensor storage classes classify host and device residency" {
    try std.testing.expect(TensorStorageClass.host_f32.isHost());
    try std.testing.expect(TensorStorageClass.host_packed_quant.isHost());
    try std.testing.expect(!TensorStorageClass.host_f32.isDevice());
    try std.testing.expect(TensorStorageClass.metal_buffer.isDevice());
    try std.testing.expect(TensorStorageClass.webgpu_buffer.isDevice());
    try std.testing.expect(TensorStorageClass.cuda_buffer.isDevice());
    try std.testing.expect(!TensorStorageClass.metadata_view.isHost());
    try std.testing.expect(TensorStorageClass.metadata_view.isViewMetadata());
}

test "tensor descriptors preserve view and quant metadata" {
    const dense = TensorDesc.init(ml.graph.Shape.init(.f32, &.{ 2, 4 }), .host_f32);
    try std.testing.expect(dense.isHostResident());
    try std.testing.expect(!dense.isView());
    try std.testing.expect(!dense.isPackedQuant());

    const strides = TensorStrides.init(&.{ 4, 1 });
    const view_desc = TensorDesc.view(ml.graph.Shape.init(.f32, &.{ 2, 4 }), 7, strides, .metal, 2);
    try std.testing.expect(view_desc.isView());
    try std.testing.expectEqual(@as(?ml.graph.NodeId, 7), view_desc.view_source);
    try std.testing.expectEqualSlices(i64, &.{ 4, 1 }, view_desc.strides.asSlice());
    try std.testing.expectEqual(@as(?BackendKind, .metal), view_desc.resident_backend);
    try std.testing.expectEqual(@as(u32, 2), view_desc.device_id);

    const quant = TensorDesc.packedQuant(ml.graph.Shape.init(.f32, &.{ 8, 32 }), .q4_0);
    try std.testing.expect(quant.isHostResident());
    try std.testing.expect(quant.isPackedQuant());
    try std.testing.expectEqual(@as(?quant_matmul.Format, .q4_0), quant.quant_format);

    const kv = TensorDesc.attentionKv(
        ml.graph.Shape.init(.f32, &.{ 1, 2, 4 }),
        .metal_buffer,
        .polar4,
        .paged,
        .metal,
        1,
    );
    try std.testing.expect(kv.isAttentionKv());
    try std.testing.expect(kv.isDeviceResident());
    try std.testing.expectEqual(@as(?quant_matmul.AttentionKvFormat, .polar4), kv.attention_kv_format);
    try std.testing.expectEqual(@as(?quant_matmul.AttentionStorage, .paged), kv.attention_storage);
}

test "gguf quant tensor types map to graph quant formats" {
    try std.testing.expectEqual(@as(?quant_matmul.Format, .q4_0), quantFormatFromGgufTensorType(.{ .known = .Q4_0 }));
    try std.testing.expectEqual(@as(?quant_matmul.Format, .q8_k), quantFormatFromGgufTensorType(.{ .known = .Q8_K }));
    try std.testing.expectEqual(@as(?quant_matmul.Format, .mxfp4), quantFormatFromGgufTensorType(.{ .known = .MXFP4 }));
    try std.testing.expectEqual(@as(?quant_matmul.Format, .tl2), quantFormatFromGgufTensorType(.bitnet_tl2));
    try std.testing.expectEqual(@as(?quant_matmul.Format, null), quantFormatFromGgufTensorType(.{ .known = .F32 }));
}
