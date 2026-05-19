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

pub const GatherAttrs = struct {
    axis: u8 = 0,
};

pub const ScatterAddAttrs = struct {
    axis: u8 = 0,
};

pub const DotGeneralAttrs = struct {
    lhs_contracting: [max_rank]u8 = .{0} ** max_rank,
    rhs_contracting: [max_rank]u8 = .{0} ** max_rank,
    lhs_batch: [max_rank]u8 = .{0} ** max_rank,
    rhs_batch: [max_rank]u8 = .{0} ** max_rank,
    num_contracting: u8 = 0,
    num_batch: u8 = 0,
};

pub const ConvAttrs = struct {
    strides: [4]u32 = .{1} ** 4,
    padding: [4][2]i32 = .{.{0} ** 2} ** 4,
    num_spatial: u8 = 0,
    groups: u32 = 1,
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
    argmax: ArgReduceAttrs,
    reshape: ReshapeAttrs,
    transpose: TransposeAttrs,
    broadcast_in_dim: BroadcastAttrs,
    slice: SliceAttrs,
    concat_prim: ConcatAttrs,
    range: void,
    shape_of: ShapeOfAttrs,
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
    fused_rms_norm: NormAttrs,
    fused_gelu: void,
    fused_relu: void,
    fused_silu: void,
    fused_quick_gelu: void,
    fused_sigmoid: void,
    fused_tanh_act: void,
    fused_concat: ConcatFusedAttrs,
    fused_elem_add: void,
    fused_elem_multiply: void,
    fused_sdpa: AttentionAttrs,
    fused_causal_self_attention: AttentionAttrs,
    fused_cross_attention: CrossAttentionAttrs,
    fused_gqa_causal_attention: AttentionAttrs,
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
