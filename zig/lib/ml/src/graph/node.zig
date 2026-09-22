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
const shape_mod = @import("shape.zig");
const Shape = shape_mod.Shape;
const DType = shape_mod.DType;
const max_rank = shape_mod.max_rank;

pub const NodeId = u32;
pub const null_node: NodeId = std.math.maxInt(NodeId);

// ── Primitive Op Types ─────────────────────────────────────────────────

pub const PrimitiveOp = enum(u8) {
    // Constants & parameters
    parameter,
    constant,

    // Elementwise unary
    neg,
    sqrt,
    rsqrt,
    exp,
    log,
    sin,
    cos,
    tanh,
    erf,
    abs,

    // Elementwise binary
    add,
    mul,
    sub,
    div,

    // Comparison
    less_than,
    where_select,

    // Reduction
    reduce_sum,
    reduce_max,
    reduce_mean,
    argmax,

    // Shape manipulation
    reshape,
    transpose,
    broadcast_in_dim,
    slice,
    concat,
    range,
    shape_of,
    size_of,

    // Data movement
    gather,
    scatter_add,

    // Contraction
    dot_general,

    // Convolution
    conv_general,

    // Type conversion
    convert_dtype,
};

// ── Fused Op Types (matching ComputeBackend VTable) ────────────────────

pub const FusedOp = enum(u8) {
    linear,
    linear_no_bias,
    linear_no_bias_pair,
    embedding_lookup,
    layer_norm,
    rms_norm,
    gelu,
    relu,
    silu,
    quick_gelu,
    sigmoid,
    tanh_act,
    concat,
    elem_add,
    elem_multiply,
    scaled_dot_product_attention,
    causal_self_attention,
    cross_attention,
    gqa_causal_attention,
    gqa_paged_attention,
    relative_position_bias,
    disentangled_relative_attention,
    windowed_self_attention,
    channel_self_attention,
    rope,
    rope_per_item,
    conv1d,
    conv2d,
    token_grid_conv2d,
    from_float32,
    from_float32_shape,
    to_float32,
    moe_linear_no_bias,
    moe_linear_no_bias_pair,
    moe_scatter_add,
    moe_select_routes,
    take_rows,
    zero_tensor,
};

// ── Op Attributes ──────────────────────────────────────────────────────

pub const ReduceAttrs = struct {
    axes: [max_rank]u8 = .{0} ** max_rank,
    num_axes: u8 = 0,
};

pub const ArgReduceAttrs = struct {
    axis: u8 = 0,
    keepdims: bool = true,
};

pub const ReshapeAttrs = struct {
    new_shape: Shape,
    /// ONNX Reshape may compute its target from request-dependent shape
    /// arithmetic. In that case input 1 carries the exact target dimensions
    /// and the static new_shape is only an import/planning approximation.
    runtime_shape: bool = false,
    allow_zero: bool = false,
};

pub const TransposeAttrs = struct {
    perm: [max_rank]u8 = .{0} ** max_rank,
    num_axes: u8 = 0,
};

pub const BroadcastAttrs = struct {
    target_shape: Shape,
    broadcast_axes: [max_rank]u8 = .{0} ** max_rank,
    num_axes: u8 = 0,
};

pub const SliceAttrs = struct {
    starts: [max_rank]i64 = .{0} ** max_rank,
    limits: [max_rank]i64 = .{0} ** max_rank,
    strides: [max_rank]i64 = .{1} ** max_rank,
    num_axes: u8 = 0,
    /// ONNX Slice can derive starts/limits from runtime shape subgraphs.
    /// When either flag is set, node inputs are [data, starts, limits] and
    /// bound_axes maps those compact input tensors onto the full-rank attrs.
    bound_axes: [max_rank]u8 = .{0} ** max_rank,
    num_bound_axes: u8 = 0,
    runtime_starts: bool = false,
    runtime_limits: bool = false,
};

pub const ConcatAttrs = struct {
    axis: u8 = 0,
};

pub const ShapeOfAttrs = struct {
    start: u8 = 0,
    end: u8 = 0,
};

pub const TakeRowsAttrs = struct {
    axis: u8 = 0,
};

/// Preserve the source indexing operation's deterministic CUDA reduction.
/// Tensor.gather and advanced row indexing have distinct backward arithmetic.
pub const ScatterReduction = enum { serial_v1, pytorch_gather_v1, pytorch_embedding_v1 };

pub const GatherAttrs = struct {
    axis: u8 = 0,
    elements: bool = false,
    backward_reduction: ScatterReduction = .serial_v1,
    /// Embedding lookup leaves the padding row readable in forward, but
    /// suppresses its weight gradient. Only the embedding profile consumes it.
    backward_padding_index: ?u32 = null,
};

pub const ScatterAddAttrs = struct {
    axis: u8 = 0,
    reduction: ScatterReduction = .serial_v1,
    padding_index: ?u32 = null,
};

pub const DotGeneralAttrs = struct {
    lhs_contracting: [max_rank]u8 = .{0} ** max_rank,
    rhs_contracting: [max_rank]u8 = .{0} ** max_rank,
    lhs_batch: [max_rank]u8 = .{0} ** max_rank,
    rhs_batch: [max_rank]u8 = .{0} ** max_rank,
    num_contracting: u8 = 0,
    num_batch: u8 = 0,
    /// Explicit target profile: differentiate supported matrix contractions
    /// using storage orientations instead of materializing transposes.
    retain_backward_storage: bool = false,
};

pub const ConvAttrs = struct {
    strides: [4]u32 = .{1} ** 4,
    padding: [4][2]i32 = .{.{0} ** 2} ** 4,
    num_spatial: u8 = 0,
    groups: u32 = 1,
    /// Per-spatial-axis tap spacing (ONNX `dilations`). Executors that
    /// only implement dense kernels expand the weight with inserted zeros,
    /// which is exactly equivalent.
    dilations: [4]u32 = .{1} ** 4,

    pub fn hasDilation(self: ConvAttrs) bool {
        for (self.dilations[0..self.num_spatial]) |d| if (d > 1) return true;
        return false;
    }
};

pub const ConvertDTypeAttrs = struct {
    target: DType,
};

pub const ParameterAttrs = struct {
    name_offset: u32,
    name_len: u16,
};

pub const ConstantAttrs = struct {
    data_offset: u32,
    data_len: u32,
};

// Fused op attribute structs
pub const LinearAttrs = struct {
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    /// Keep matrix storage in the explicit fused VJP. Opt in only when the
    /// target supports both dense contraction orientations.
    retain_backward_storage: bool = false,
    /// Optional grouped/GQA hint. When `num_projections > 0`, the
    /// matmul output is the concatenation of `num_projections`
    /// per-projection results along axis 1; their sizes live in
    /// `projection_out_dims[0..num_projections]` and sum to
    /// `out_dim`. Set by `fuseLinearPairs` when it folds Q/K/V (or
    /// any other grouped projection set) into a single matmul on a
    /// concatenated weight. Backends that don't dispatch a grouped
    /// kernel can ignore these — the op semantics are unchanged
    /// (still a regular matmul of `(input × combined_weight)`).
    projection_out_dims: [4]u32 = .{0} ** 4,
    num_projections: u8 = 0,
};

pub const NormAttrs = struct {
    dim: u32,
    eps: f32,
};

/// Inclusive FP32 scan of physical [batch*width,channels]. Reference layout
/// records the original scan axis: reshaping an innermost scan to channels=1
/// must not silently select the outer-axis arithmetic. A single vector uses
/// the pinned deterministic block scan regardless of reference layout.
pub const PrefixScanAttrs = struct {
    batch: u32,
    width: u32,
    channels: u32,
    reference: enum { outer, inner } = .outer,
    reverse: bool = false,

    pub fn elements(self: @This()) !usize {
        if (self.batch == 0 or self.width == 0 or self.channels == 0 or
            (self.reference == .inner and self.channels != 1)) return error.InvalidPrefixScanShape;
        const rows = try std.math.mul(u64, self.batch, self.width);
        const count = try std.math.mul(u64, rows, self.channels);
        if (count > std.math.maxInt(i32)) return error.InvalidPrefixScanShape;
        return @intCast(count);
    }
    pub fn shape(self: @This()) !Shape {
        _ = try self.elements();
        return Shape.init(.f32, &.{ @as(i64, self.batch) * self.width, self.channels });
    }
    pub fn singleVector(self: @This()) bool {
        return self.batch == 1 and self.channels == 1;
    }
    pub fn scratchBytes(self: @This()) !usize {
        _ = try self.elements();
        // PyTorch's deterministic scan admits at most 1024 blocks. Actual
        // launch count also depends on the device's multiprocessor count.
        return if (self.singleVector()) @min((@as(usize, self.width) + 8191) / 8192, 1024) * 4 else 0;
    }
};

/// Detached features of immutable span metadata. Inputs are clamped lengths
/// [batch*capacity,1] and token counts [batch,1]; output columns are log1p,
/// normalized length and rsqrt. This op intentionally has no input gradients.
pub const FrozenSpanFeaturesAttrs = struct {
    batch: u32,
    capacity: u32,

    pub fn rows(self: @This()) !u32 {
        const count = @as(u64, self.batch) * self.capacity;
        if (self.batch == 0 or self.capacity == 0 or count > std.math.maxInt(i32) / 3)
            return error.InvalidFrozenSpanFeaturesShape;
        return @intCast(count);
    }
    pub fn shape(self: @This()) !Shape {
        return Shape.init(.f32, &.{ try self.rows(), 3 });
    }
    pub fn validate(self: @This(), output: Shape, lengths: Shape, counts: Shape) !void {
        const count = try self.rows();
        if (!output.eq(try self.shape()) or !lengths.eq(Shape.init(.f32, &.{ count, 1 })) or
            !counts.eq(Shape.init(.f32, &.{ self.batch, 1 }))) return error.InvalidFrozenSpanFeaturesShape;
    }
};

/// CUDA-compatible boundary attention, FP32 D32 and zero probability dropout.
/// QKV is [B*N,3*H*32], mask is [B,N]. The output packs contiguous
/// [B*N,H*32] followed by saved [B,H,ceil(N/32)*32] log-sum-exp. The saved
/// suffix is auxiliary, nondifferentiable state owned by the ordinary tape.
/// Allowed keys are (valid key AND within window) OR diagonal; window 0 is global.
pub const BoundaryTrainingAttentionAttrs = struct {
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    window: u32 = 0,

    pub const Layout = struct {
        rows: i64,
        hidden: i64,
        output_elements: i64,
        lse_elements: i64,
        bias_columns: i64,
        bias_elements: i64,
        delta_elements: i64,
        workspace_bytes: usize,

        pub fn qkvShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ self.rows, 3 * self.hidden });
        }
        pub fn savedShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ 1, self.output_elements + self.lse_elements });
        }
        pub fn attendedShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ self.rows, self.hidden });
        }
        pub fn scratchBytes(self: Layout, backward: bool) !usize {
            const bias = try std.math.mul(usize, @intCast(self.bias_elements), 4);
            if (!backward) return bias;
            return std.math.add(usize, bias, try std.math.add(usize, self.workspace_bytes, std.mem.alignForward(usize, try std.math.mul(usize, @intCast(self.delta_elements), 4), 16)));
        }
    };

    pub fn maskShape(self: @This()) Shape {
        return Shape.init(.f32, &.{ self.batch, self.seq_len });
    }

    pub fn layout(self: @This()) !Layout {
        if (self.batch == 0 or self.seq_len == 0 or self.num_heads == 0 or
            self.batch > 65535 or self.num_heads > 65535 or self.window > std.math.maxInt(i32))
            return error.InvalidBoundaryTrainingAttentionShape;
        const rows = try std.math.mul(i64, self.batch, self.seq_len);
        const hidden = try std.math.mul(i64, self.num_heads, 32);
        const elements = try std.math.mul(i64, rows, hidden);
        const bh = try std.math.mul(i64, self.batch, self.num_heads);
        const lse = try std.math.mul(i64, bh, @divTrunc(@as(i64, self.seq_len) + 31, 32) * 32);
        const bias_columns = @divTrunc(@as(i64, self.seq_len) + 7, 8) * 8;
        const bias = try std.math.mul(i64, rows, bias_columns);
        // Device helpers use signed 32-bit indexing. Check whole physical
        // buffers, including padding, before casting any launch dimensions.
        if (try std.math.mul(i64, elements, 3) > std.math.maxInt(i32) or
            try std.math.add(i64, elements, lse) > std.math.maxInt(i32) or bias > std.math.maxInt(i32))
            return error.InvalidBoundaryTrainingAttentionShape;
        // Pinned CUTLASS D32: one 16-byte lock/counter header and 64*64
        // FP32 values per query tile; no dK/dV accumulation workspace.
        const blocks = @divTrunc(@as(i64, self.seq_len) + 63, 64);
        const workspace = try std.math.mul(i64, try std.math.mul(i64, bh, blocks), 16400);
        return .{ .rows = rows, .hidden = hidden, .output_elements = elements, .lse_elements = lse, .bias_columns = bias_columns, .bias_elements = bias, .delta_elements = try std.math.mul(i64, bh, self.seq_len), .workspace_bytes = std.math.cast(usize, workspace) orelse return error.InvalidBoundaryTrainingAttentionShape };
    }
};

pub const AttentionAttrs = struct {
    batch: u32,
    seq_len: u32,
    kv_seq_len: u32 = 0,
    num_heads: u32,
    num_kv_heads: u32 = 0,
    head_dim: u32,
    layer_index: u32 = std.math.maxInt(u32),
    skip_kv_write: bool = false,
};

/// Version-1 training attention with replayable probability dropout. This is
/// distinct from inference attention: the query AND key mask use finite
/// -max(f32), so fully masked queries retain uniform softmax probabilities.
///
/// Forward leaves are packed [Q;K;V], packed [Qr;Kr], and physical i32 control.
/// Control contains seed/microbatch/replica low/high u32 bit limbs, B*S token
/// validity entries, and 2*S-1 relative bucket indices. Both relative terms
/// use bucket[q-k+S-1]. Dropout addresses ((b*heads+h)*S+q)*S+k, independently
/// of execution tiling; it affects the PV numerator, not normalization.
pub const DebertaTrainingAttentionAttrs = struct {
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
    relative_rows: u32,
    dropout_probability: f32,
    dropout_stream_id: u64,

    pub const Layout = struct {
        batch_tokens: i64,
        hidden: i64,
        qkv_rows: i64,
        relative_packed_rows: i64,
        gradient_rows: i64,
        control_elements: i64,

        pub fn qkvShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ self.qkv_rows, self.hidden });
        }
        pub fn relativeShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ self.relative_packed_rows, self.hidden });
        }
        pub fn controlShape(self: Layout) Shape {
            return Shape.init(.i32, &.{self.control_elements});
        }
        pub fn outputShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ self.batch_tokens, self.hidden });
        }
        pub fn gradientShape(self: Layout) Shape {
            return Shape.init(.f32, &.{ self.gradient_rows, self.hidden });
        }
    };

    /// Shape validation only. Each backend must separately admit physical
    /// bytes, scratch, dispatch work and the contents of the control leaf.
    pub fn layout(self: DebertaTrainingAttentionAttrs) !Layout {
        for ([_]u32{ self.batch, self.seq_len, self.num_heads, self.head_dim, self.relative_rows }) |dim|
            if (dim == 0 or dim > std.math.maxInt(i32)) return error.InvalidDebertaTrainingAttentionShape;
        if (!std.math.isFinite(self.dropout_probability) or self.dropout_probability < 0 or self.dropout_probability >= 1)
            return error.InvalidDebertaTrainingAttentionShape;
        const bs = try std.math.mul(i64, self.batch, self.seq_len);
        const hidden = try std.math.mul(i64, self.num_heads, self.head_dim);
        const qkv_rows = try std.math.mul(i64, 3, bs);
        const relative_rows = try std.math.mul(i64, 2, self.relative_rows);
        const gradient_rows = try std.math.add(i64, qkv_rows, relative_rows);
        const relative_indices = try std.math.sub(i64, try std.math.mul(i64, 2, self.seq_len), 1);
        const control_elements = try std.math.add(i64, 6, try std.math.add(i64, bs, relative_indices));
        // Reject element-count overflow before Shape.numElements or VJP
        // slicing can encounter a malformed manually assembled graph.
        _ = try std.math.mul(i64, gradient_rows, hidden);
        return .{ .batch_tokens = bs, .hidden = hidden, .qkv_rows = qkv_rows, .relative_packed_rows = relative_rows, .gradient_rows = gradient_rows, .control_elements = control_elements };
    }
};

pub const RopeAttrs = struct {
    seq_len: u32,
    head_dim: u32,
    rope_dim: u32 = 0, // 0 means same as head_dim
    theta: f32,
    freq_scale: f32,
    position_offset: u32 = 0,
    consecutive_pairs: bool = false,
};

pub const Conv1dAttrs = struct {
    batch: u32,
    in_channels: u32,
    out_channels: u32,
    time_steps: u32,
    kernel_size: u32,
    stride: u32,
    padding: u32,
};

pub const Conv2dAttrs = struct {
    batch: u32,
    in_channels: u32,
    out_channels: u32,
    height: u32,
    width: u32,
    kernel_h: u32,
    kernel_w: u32,
    stride_h: u32,
    stride_w: u32,
    padding_h: u32,
    padding_w: u32,
    groups: u32,
};

pub const RelativePositionBiasAttrs = struct {
    q_len: u32,
    k_len: u32,
    num_heads: u32,
    num_buckets: u32,
    max_distance: u32,
    bidirectional: bool,
};

pub const CrossAttentionAttrs = struct {
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    num_heads: u32,
    head_dim: u32,
};

pub const WindowedAttentionAttrs = struct {
    batch: u32,
    height: u32,
    width: u32,
    dim: u32,
    num_heads: u32,
    window_size: u32,
};

pub const ChannelAttentionAttrs = struct {
    batch: u32,
    seq_len: u32,
    dim: u32,
    groups: u32,
};

pub const MoeLinearAttrs = struct {
    rows: u32,
    in_dim: u32,
    out_dim: u32,
};

pub const MoeScatterAddAttrs = struct {
    rows: u32,
    dim: u32,
};

pub const MoeSelectRoutesAttrs = struct {
    layer_index: u32 = 0,
    rows: u32,
    num_experts: u32,
    top_k: u32,
};

pub const EmbeddingAttrs = struct {
    total: u32,
    dim: u32,
};

pub const ConcatFusedAttrs = struct {
    total: u32,
    dim_a: u32,
    dim_b: u32,
};

pub const FusedTakeRowsAttrs = struct {
    rows: u32,
    dim: u32,
};

pub const ArgmaxAttrs = struct {
    rows: u32,
    dim: u32,
};

pub const SoftmaxAttrs = struct {
    dim: u32, // size of last dimension (softmax axis)
    /// Opt in only when the executor supports fused_softmax_backward.
    /// Defaults preserve existing backend differentiation behavior.
    fuse_backward: bool = false,
};

pub const MaskedBceReduction = enum(u8) {
    sum,
    mean,
};

pub const MaskedBceWithLogitsAttrs = struct {
    positive_weight: f32 = 1.0,
    negative_weight: f32 = 1.0,
    eps: f32 = 1e-6,
    reduction: MaskedBceReduction = .mean,
};

// ── Node ───────────────────────────────────────────────────────────────

/// Discriminated union of all ops in the graph IR.
pub const OpCode = union(enum) {
    // Primitives
    parameter: ParameterAttrs,
    constant: ConstantAttrs,
    neg: void,
    sqrt: void,
    rsqrt: void,
    exp: void,
    log: void,
    sin: void,
    cos: void,
    tanh: void,
    erf: void,
    abs: void,
    add: void,
    mul: void,
    sub: void,
    div: void,
    less_than: void,
    where_select: void,
    reduce_sum: ReduceAttrs,
    reduce_max: ReduceAttrs,
    reduce_mean: ReduceAttrs,
    cumulative_sum: struct { axis: u8, exclusive: bool = false, reverse: bool = false },
    argmax: ArgReduceAttrs,
    reshape: ReshapeAttrs,
    transpose: TransposeAttrs,
    broadcast_in_dim: BroadcastAttrs,
    slice: SliceAttrs,
    concat_prim: ConcatAttrs,
    range: void,
    shape_of: ShapeOfAttrs,
    size_of: void,
    gather: GatherAttrs,
    scatter_add: ScatterAddAttrs,
    dot_general: DotGeneralAttrs,
    conv_general: ConvAttrs,
    convert_dtype: ConvertDTypeAttrs,

    // Fused ops (matching ComputeBackend VTable)
    fused_linear: LinearAttrs,
    fused_linear_no_bias: LinearAttrs,
    fused_embedding_lookup: EmbeddingAttrs,
    fused_layer_norm: NormAttrs,
    fused_layer_norm_backward: NormAttrs,
    fused_rms_norm: NormAttrs,
    fused_gelu: void,
    fused_gelu_exact: void,
    fused_gelu_backward: void,
    fused_gelu_exact_backward: void,
    fused_relu: void,
    fused_silu: void,
    fused_silu_backward: void,
    fused_prefix_scan_v1: PrefixScanAttrs,
    frozen_span_features_v1: FrozenSpanFeaturesAttrs,
    fused_quick_gelu: void,
    fused_sigmoid: void,
    /// Inputs: saved sigmoid output and upstream cotangent (FP32).
    fused_sigmoid_backward: void,
    fused_tanh_act: void,
    fused_concat: ConcatFusedAttrs,
    fused_elem_add: void,
    fused_elem_multiply: void,
    fused_add_mul_scalar: void,
    fused_boundary_training_attention_v1: BoundaryTrainingAttentionAttrs,
    fused_boundary_training_attention_backward_v1: BoundaryTrainingAttentionAttrs,
    fused_sdpa: AttentionAttrs,
    fused_causal_self_attention: AttentionAttrs,
    fused_cross_attention: CrossAttentionAttrs,
    fused_gqa_causal_attention: AttentionAttrs,
    fused_disentangled_attention: AttentionAttrs,
    fused_disentangled_attention_backward: AttentionAttrs,
    fused_deberta_training_attention_v1: DebertaTrainingAttentionAttrs,
    fused_deberta_training_attention_backward_v1: DebertaTrainingAttentionAttrs,
    fused_relative_position_bias: RelativePositionBiasAttrs,
    fused_rope: RopeAttrs,
    fused_conv1d: Conv1dAttrs,
    fused_conv2d: Conv2dAttrs,
    fused_windowed_self_attention: WindowedAttentionAttrs,
    fused_channel_self_attention: ChannelAttentionAttrs,
    fused_linear_no_bias_pair: LinearAttrs,
    fused_moe_linear_no_bias: MoeLinearAttrs,
    fused_moe_linear_no_bias_pair: MoeLinearAttrs,
    fused_moe_scatter_add: MoeScatterAddAttrs,
    fused_moe_select_routes: MoeSelectRoutesAttrs,
    fused_take_rows: FusedTakeRowsAttrs,
    fused_from_float32: void,
    fused_to_float32: void,
    fused_zero_tensor: LinearAttrs,
    fused_eval_tensor: void,
    fused_argmax_last_row: ArgmaxAttrs,
    fused_softmax: SoftmaxAttrs,
    fused_log_softmax: SoftmaxAttrs,
    fused_masked_bce_with_logits_loss: MaskedBceWithLogitsAttrs,
    fused_masked_bce_with_logits_backward: MaskedBceWithLogitsAttrs,
    /// Identity in forward execution, with no gradient through its input.
    /// Kept as an intrinsic so lowering cannot lose a PEFT detach boundary.
    stop_gradient: void,

    /// Inputs are the precomputed cotangent * probability and probability.
    /// Output is weighted_cotangent - probability * row_sum(weighted_cotangent).
    fused_softmax_backward: SoftmaxAttrs,

    pub fn isFused(self: OpCode) bool {
        return switch (self) {
            inline else => |_, tag| {
                const name = @tagName(tag);
                return name.len >= 6 and std.mem.eql(u8, name[0..6], "fused_");
            },
        };
    }

    pub fn isPrimitive(self: OpCode) bool {
        return !self.isFused();
    }
};

/// A single node in the computation graph.
pub const Node = struct {
    op: OpCode,
    output_shape: Shape,

    /// Up to 4 inputs stored inline. Most ML ops take 1-3 inputs.
    inputs: [4]NodeId = .{null_node} ** 4,
    num_inputs: u8 = 0,

    /// Points to the root of a decomposed primitive subgraph that computes
    /// the same result. Used by autograd to differentiate fused ops without
    /// hand-written VJPs (GoMLX's vjpAlternateOutputs pattern).
    vjp_alternate: NodeId = null_node,

    pub fn getInputs(self: *const Node) []const NodeId {
        return self.inputs[0..self.num_inputs];
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "OpCode.isFused" {
    const fused = OpCode{ .fused_gelu = {} };
    try std.testing.expect(fused.isFused());
    try std.testing.expect(!fused.isPrimitive());

    const prim = OpCode{ .add = {} };
    try std.testing.expect(!prim.isFused());
    try std.testing.expect(prim.isPrimitive());
}

test "Node inline inputs" {
    const n = Node{
        .op = .{ .add = {} },
        .output_shape = Shape.init(.f32, &.{ 2, 3 }),
        .inputs = .{ 0, 1, null_node, null_node },
        .num_inputs = 2,
    };
    try std.testing.expectEqual(@as(usize, 2), n.getInputs().len);
    try std.testing.expectEqual(@as(NodeId, 0), n.getInputs()[0]);
    try std.testing.expectEqual(@as(NodeId, 1), n.getInputs()[1]);
}
