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

//! Metal-specific graph partition capability policy.
//!
//! The generic partitioner owns graph assignment mechanics. This module owns
//! Metal's op support, shape/attrs eligibility, quant kernel planning, and
//! host-assisted diagnostics.

const std = @import("std");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const partition = @import("partition.zig");
const quant_matmul = @import("quant_matmul.zig");
const operator_plan = @import("operator_plan.zig");
const transpose_utils = @import("transpose_utils.zig");
const gemma_graph = @import("../architectures/gemma_graph.zig");

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.OpCode;

const CapabilityDecision = partition.CapabilityDecision;
const CapabilityQuery = partition.CapabilityQuery;
const CapabilityReason = partition.CapabilityReason;

/// Conservative capability filter for eager Metal graph partitions.
///
/// This is intentionally narrower than `supportsAll`: unsupported or
/// shape-fragile ops should form explicit native fallback islands instead of
/// failing inside a Metal partition.
pub fn supportsMetalEagerGraph(op: OpCode) bool {
    return switch (op) {
        .parameter,
        .constant,
        .neg,
        .sqrt,
        .rsqrt,
        .exp,
        .log,
        .sin,
        .cos,
        .tanh,
        .erf,
        .abs,
        .convert_dtype,
        .add,
        .mul,
        .sub,
        .div,
        .less_than,
        .where_select,
        .reduce_sum,
        .reduce_max,
        .reduce_mean,
        .argmax,
        .fused_linear,
        .fused_linear_no_bias,
        .fused_linear_no_bias_pair,
        .fused_layer_norm,
        .fused_rms_norm,
        .fused_gelu,
        .fused_relu,
        .fused_silu,
        .fused_quick_gelu,
        .fused_sigmoid,
        .fused_tanh_act,
        .fused_elem_add,
        .fused_elem_multiply,
        .fused_embedding_lookup,
        .fused_from_float32,
        .fused_to_float32,
        .fused_zero_tensor,
        .fused_rope,
        .fused_gqa_causal_attention,
        .reshape,
        .transpose,
        .broadcast_in_dim,
        .gather,
        .scatter_add,
        .slice,
        .concat_prim,
        .dot_general,
        .conv_general,
        .fused_sdpa,
        .fused_softmax,
        .fused_log_softmax,
        .fused_conv1d,
        .fused_conv2d,
        .fused_take_rows,
        => true,
        else => false,
    };
}

pub fn decideMetalEagerGraph(query: CapabilityQuery) CapabilityDecision {
    if (!supportsMetalEagerGraph(query.op)) return CapabilityDecision.reject(.unsupported_op);
    if (!metalEagerGraphNodeHasSupportedResidentShape(query)) return CapabilityDecision.reject(.unsupported_op);
    if (metalQuantizedRowPlan(query)) |row_plan| {
        if (row_plan.operator == .fallback) return CapabilityDecision.reject(.missing_quant_kernel);
        return CapabilityDecision.acceptCostWithOperator(metalEstimatedCost(query), .{ .quant_row = row_plan });
    } else if (std.meta.activeTag(query.op) == .fused_take_rows) {
        return CapabilityDecision.reject(.missing_quant_kernel);
    }
    const packed_quant_metadata_view = metalPackedQuantMetadataView(query);
    const quant_plan = if (nodeHasPackedQuantInput(query) and !packed_quant_metadata_view) blk: {
        const plan = metalQuantizedLinearPlan(query) orelse
            metalQuantizedDotGeneralPlan(query) orelse
            return CapabilityDecision.reject(.missing_quant_kernel);
        if (plan.operator == .fallback) return CapabilityDecision.reject(.missing_quant_kernel);
        break :blk plan;
    } else null;
    if (!nodeInputsAreMetalCompatible(query)) return CapabilityDecision.reject(.wrong_storage);
    if (metalAttentionPlan(query)) |attention_plan| {
        if (attention_plan.operator == .fallback) return CapabilityDecision.reject(.missing_quant_kernel);
        return CapabilityDecision.acceptCostWithOperator(metalEstimatedCost(query), .{ .attention = attention_plan });
    } else if (std.meta.activeTag(query.op) == .fused_sdpa) {
        return CapabilityDecision.reject(.missing_quant_kernel);
    }

    const cost = nodeComputeCost(query.graph, query.node_id);
    switch (query.op) {
        .parameter, .constant => {
            if (!nodeOutputIsMetalResident(query) and !nodeOutputIsPackedQuant(query)) return CapabilityDecision.reject(.wrong_storage);
            return CapabilityDecision.acceptCost(cost);
        },
        .fused_from_float32, .fused_zero_tensor => return CapabilityDecision.acceptCost(cost),
        .reshape, .transpose, .broadcast_in_dim, .slice, .concat_prim => {
            if (!nodeHasMetalResidentInput(query) and !packed_quant_metadata_view) return CapabilityDecision.reject(.wrong_storage);
            return CapabilityDecision.acceptCost(cost);
        },
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair, .dot_general => {
            if (!nodeHasMetalResidentInput(query) and cost < metalHostMatmulMinComputeCost) {
                return CapabilityDecision.unprofitable();
            }
        },
        else => {
            if (!nodeHasMetalResidentInput(query) and metalHostTransferCost(query) < metalHostElementwiseMinTransferBytes) {
                return CapabilityDecision.unprofitable();
            }
        },
    }
    return acceptMetalCost(metalEstimatedCost(query), quant_plan);
}

/// Conservative classification for Metal eager graph ops that are accepted for
/// correctness today but are not yet a device-resident graph implementation.
///
/// This is diagnostic-only. The stricter partition policy should move ops out
/// of this set as their Metal implementations stop materializing through host
/// f32 buffers.
pub fn metalEagerGraphOpIsHostAssisted(op: OpCode) bool {
    _ = op;
    return false;
}

/// Node-aware host-assist classifier for Metal graph reports.
///
/// Some op tags have both resident and host-assisted paths depending on attrs
/// and static shape. Keep this query in sync with `decideMetalEagerGraph` so
/// reports describe the implementation path that the node would actually take.
pub fn metalEagerGraphNodeIsHostAssisted(query: CapabilityQuery) bool {
    if (!supportsMetalEagerGraph(query.op)) return false;
    if (!metalEagerGraphNodeHasSupportedResidentShape(query)) return false;
    return metalEagerGraphOpIsHostAssisted(query.op);
}

fn metalQuantizedLinearPlan(query: CapabilityQuery) ?quant_matmul.Plan {
    if (metalQuantizedDotGeneralPlan(query)) |plan| return plan;
    const descs = query.tensor_descs orelse return null;
    const n = query.graph.node(query.node_id);
    const inputs = n.getInputs();
    const attrs = switch (query.op) {
        .fused_linear => |attrs| attrs,
        .fused_linear_no_bias => |attrs| attrs,
        .fused_linear_no_bias_pair => |attrs| attrs,
        else => return null,
    };
    if (attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0) return null;
    if (inputs.len < 2) return null;

    if (inputDesc(descs, inputs[0])) |activation_desc| {
        if (activation_desc.isPackedQuant()) return null;
    }

    const first_format = packedQuantInputFormat(descs, inputs[1]) orelse return null;
    if (!quant_matmul.packedFormatDescriptor(first_format).supported()) return null;

    if (std.meta.activeTag(query.op) == .fused_linear_no_bias_pair) {
        if (inputs.len < 3) return null;
        const second_format = packedQuantInputFormat(descs, inputs[2]) orelse return null;
        if (second_format != first_format) return null;
    }

    return quant_matmul.plan(.{
        .rows = attrs.rows,
        .in_dim = attrs.in_dim,
        .out_dim = attrs.out_dim,
        .format = first_format,
    });
}

fn metalQuantizedDotGeneralPlan(query: CapabilityQuery) ?quant_matmul.Plan {
    const attrs = switch (query.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (attrs.num_contracting != 1 or attrs.num_batch != 0) return null;
    if (attrs.lhs_contracting[0] != 1) return null;
    if (attrs.rhs_contracting[0] != 0 and attrs.rhs_contracting[0] != 1) return null;
    const lhs_shape = nodeInputShape(query, 0) orelse return null;
    const rhs_shape = nodeInputShape(query, 1) orelse return null;
    if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2) return null;
    if (lhs_shape.dtype != .f32) return null;
    const rows = positiveShapeDim(lhs_shape, 0) orelse return null;
    const in_dim = positiveShapeDim(lhs_shape, 1) orelse return null;
    const rhs_axis: usize = @intCast(attrs.rhs_contracting[0]);
    const out_axis: usize = 1 - rhs_axis;
    const rhs_in_dim = positiveShapeDim(rhs_shape, rhs_axis) orelse return null;
    const out_dim = positiveShapeDim(rhs_shape, out_axis) orelse return null;
    if (rhs_in_dim != in_dim) return null;
    const descs = query.tensor_descs orelse return null;
    if (inputDesc(descs, query.graph.node(query.node_id).inputs[0])) |activation_desc| {
        if (activation_desc.isPackedQuant()) return null;
    }
    const format = packedQuantInputFormat(descs, query.graph.node(query.node_id).inputs[1]) orelse return null;
    if (!quant_matmul.packedFormatDescriptor(format).supported()) return null;
    return quant_matmul.plan(.{
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .format = format,
    });
}

fn metalQuantizedRowPlan(query: CapabilityQuery) ?quant_matmul.RowOpPlan {
    const descs = query.tensor_descs orelse return null;
    const n = query.graph.node(query.node_id);
    const attrs = switch (query.op) {
        .fused_take_rows => |attrs| attrs,
        else => return null,
    };
    if (n.num_inputs < 1 or attrs.rows == 0 or attrs.dim == 0) return null;
    const format = packedQuantInputFormat(descs, n.inputs[0]) orelse return null;
    if (!quant_matmul.packedFormatDescriptor(format).supported()) return null;
    return quant_matmul.rowOpPlan(format, .get_rows, attrs.rows, attrs.dim);
}

fn metalAttentionPlan(query: CapabilityQuery) ?quant_matmul.AttentionPlan {
    const attrs = switch (query.op) {
        .fused_sdpa => |attrs| attrs,
        else => return null,
    };
    if (attrs.batch == 0 or attrs.seq_len == 0 or attrs.num_heads == 0 or attrs.head_dim == 0) return null;
    const kv_len = if (attrs.kv_seq_len != 0) attrs.kv_seq_len else attrs.seq_len;
    const inputs = query.graph.node(query.node_id).getInputs();
    const kv_format, const storage = attentionKvMetadataForSdpa(query, inputs) orelse .{ .f32, .dense };
    if (kv_format == .f32 and storage == .dense) {
        if (attrs.num_kv_heads != 0 and attrs.num_kv_heads != attrs.num_heads) return null;
        if (kv_len != attrs.seq_len) return null;
    }
    return quant_matmul.attentionPlanWithStorage(attrs.seq_len, kv_len, attrs.head_dim, kv_format, storage);
}

fn attentionKvMetadataForSdpa(
    query: CapabilityQuery,
    inputs: []const NodeId,
) ?struct { quant_matmul.AttentionKvFormat, quant_matmul.AttentionStorage } {
    if (inputs.len < 3) return null;
    const descs = query.tensor_descs orelse return null;
    const k_desc = inputDesc(descs, inputs[1]) orelse return null;
    const v_desc = inputDesc(descs, inputs[2]) orelse return null;
    const k_format = k_desc.attention_kv_format orelse return null;
    const v_format = v_desc.attention_kv_format orelse return null;
    if (k_format != v_format) return null;
    const k_storage = k_desc.attention_storage orelse .dense;
    const v_storage = v_desc.attention_storage orelse .dense;
    if (k_storage != v_storage) return null;
    return .{ k_format, k_storage };
}

fn packedQuantInputFormat(descs: []const ?contracts.TensorDesc, input_id: NodeId) ?quant_matmul.Format {
    const desc = inputDesc(descs, input_id) orelse return null;
    if (!desc.isPackedQuant()) return null;
    return desc.quant_format;
}

fn acceptMetalCost(estimated_cost: u64, quant_plan: ?quant_matmul.Plan) CapabilityDecision {
    if (quant_plan) |plan| {
        return CapabilityDecision.acceptCostWithOperator(estimated_cost, .{ .quant_matmul = plan });
    }
    return CapabilityDecision.acceptCost(estimated_cost);
}

fn metalEagerGraphNodeHasSupportedResidentShape(query: CapabilityQuery) bool {
    return switch (query.op) {
        .transpose => metalTransposeHasResidentShape(query),
        .dot_general => metalDotGeneralHasResidentShape(query),
        .conv_general => metalConvGeneralHasResidentShape(query),
        .fused_conv1d => metalFusedConv1dHasResidentShape(query),
        .fused_conv2d => metalFusedConv2dHasResidentShape(query),
        .reduce_sum, .reduce_max, .reduce_mean => metalReduceLastDimHasResidentShape(query),
        .broadcast_in_dim => metalBroadcastHasResidentShape(query),
        .gather => metalGatherHasResidentShape(query),
        .scatter_add => metalScatterAddHasResidentShape(query),
        .slice => metalSliceHasResidentShape(query),
        .argmax => metalArgmaxHasResidentShape(query),
        .fused_sdpa => metalSdpaHasResidentShape(query),
        .fused_gqa_causal_attention => metalGqaCausalAttentionHasResidentShape(query),
        .fused_softmax, .fused_log_softmax => metalSoftmaxHasResidentShape(query),
        .convert_dtype => metalConvertDTypeHasResidentShape(query),
        else => true,
    };
}

fn metalConvertDTypeHasResidentShape(query: CapabilityQuery) bool {
    const input_shape = nodeInputShape(query, 0) orelse return false;
    return shapeHasConcreteElements(input_shape);
}

fn metalScatterAddHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .scatter_add => |attrs| attrs,
        else => return false,
    };
    if (attrs.axis != 0) return false;
    const n = query.graph.node(query.node_id);
    const inputs = n.getInputs();
    if (inputs.len != 3) return false;

    const dest_shape = nodeInputShape(query, 0) orelse return false;
    const values_shape = nodeInputShape(query, 1) orelse return false;
    const indices_shape = nodeInputShape(query, 2) orelse return false;
    const output_shape = n.output_shape;
    if (dest_shape.dtype != .f32 or values_shape.dtype != .f32 or output_shape.dtype != .f32) return false;
    if (dest_shape.rank() != 2 or values_shape.rank() != 2 or indices_shape.rank() != 1 or output_shape.rank() != 2) return false;
    if (!shapeHasConcreteElements(dest_shape) or !shapeHasConcreteElements(values_shape) or !shapeHasConcreteElements(indices_shape) or !shapeHasConcreteElements(output_shape)) return false;
    if (dest_shape.dim(1) != values_shape.dim(1) or output_shape.dim(0) != dest_shape.dim(0) or output_shape.dim(1) != dest_shape.dim(1)) return false;
    if (values_shape.dim(0) != indices_shape.dim(0)) return false;
    return true;
}

fn metalTransposeHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .transpose => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const output_shape = query.graph.node(query.node_id).output_shape;
    if (input_shape.dtype != .f32 or output_shape.dtype != .f32) return false;
    const rank = input_shape.rank();
    if (rank == 0 or rank > 8 or output_shape.rank() != rank) return false;
    var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
    const perm = transpose_utils.effectivePerm(attrs, rank, &perm_buf);
    if (!transpose_utils.isValidPermutation(perm, rank)) return false;
    for (perm, 0..) |p, axis| {
        const input_axis: u8 = @intCast(p);
        const output_axis: u8 = @intCast(axis);
        if (input_shape.dim(input_axis) <= 0 or output_shape.dim(output_axis) != input_shape.dim(input_axis)) return false;
    }
    return shapeHasConcreteElements(input_shape) and shapeHasConcreteElements(output_shape);
}

fn metalReshapeHasResidentShape(query: CapabilityQuery) bool {
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const output_shape = query.graph.node(query.node_id).output_shape;
    return input_shape.dtype == output_shape.dtype and
        shapeHasConcreteElements(input_shape) and
        shapeHasConcreteElements(output_shape);
}

fn metalDotGeneralHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    if (attrs.num_contracting != 1) return false;
    const lhs_shape = nodeInputShape(query, 0) orelse return false;
    const rhs_shape = nodeInputShape(query, 1) orelse return false;
    const output_shape = query.graph.node(query.node_id).output_shape;
    if (attrs.num_batch != 0) return metalBatchedDotGeneralHasResidentShape(attrs, lhs_shape, rhs_shape, output_shape);
    if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2 or output_shape.rank() != 2) return false;
    if (lhs_shape.dtype != .f32 or output_shape.dtype != .f32) return false;
    if (attrs.lhs_contracting[0] != 1) return false;
    if (attrs.rhs_contracting[0] != 0 and attrs.rhs_contracting[0] != 1) return false;
    const rhs_dense_f32 = rhs_shape.dtype == .f32;
    const rhs_quant = blk: {
        const descs = query.tensor_descs orelse break :blk false;
        const n = query.graph.node(query.node_id);
        const rhs_desc = inputDesc(descs, n.inputs[1]) orelse break :blk false;
        break :blk rhs_desc.isPackedQuant() and
            quant_matmul.packedFormatDescriptor(rhs_desc.quant_format orelse return false).supported();
    };
    if (!rhs_dense_f32 and !rhs_quant) return false;
    const m = positiveShapeDim(lhs_shape, 0) orelse return false;
    const k = positiveShapeDim(lhs_shape, 1) orelse return false;
    const rhs_axis: usize = @intCast(attrs.rhs_contracting[0]);
    const rhs_k = positiveShapeDim(rhs_shape, rhs_axis) orelse return false;
    const n = positiveShapeDim(rhs_shape, 1 - rhs_axis) orelse return false;
    if (k != rhs_k) return false;
    return output_shape.dim(0) == @as(i64, @intCast(m)) and output_shape.dim(1) == @as(i64, @intCast(n));
}

fn metalBatchedDotGeneralHasResidentShape(
    attrs: ml.graph.node.DotGeneralAttrs,
    lhs_shape: ml.graph.Shape,
    rhs_shape: ml.graph.Shape,
    output_shape: ml.graph.Shape,
) bool {
    if (lhs_shape.dtype != .f32 or rhs_shape.dtype != .f32 or output_shape.dtype != .f32) return false;
    const rank = lhs_shape.rank();
    if (rank == 0 or rank != rhs_shape.rank() or rank != output_shape.rank()) return false;
    if (attrs.num_batch == 0 or rank != attrs.num_batch + 2) return false;
    const m_axis: u8 = @intCast(rank - 2);
    const k_axis: u8 = @intCast(rank - 1);
    if (attrs.lhs_contracting[0] != k_axis) return false;
    if (attrs.rhs_contracting[0] != k_axis and attrs.rhs_contracting[0] != m_axis) return false;

    for (0..attrs.num_batch) |idx| {
        if (attrs.lhs_batch[idx] != idx or attrs.rhs_batch[idx] != idx) return false;
        const dim = lhs_shape.dim(@intCast(idx));
        if (dim <= 0 or rhs_shape.dim(@intCast(idx)) != dim or output_shape.dim(@intCast(idx)) != dim) return false;
    }

    const m = lhs_shape.dim(m_axis);
    const k = lhs_shape.dim(k_axis);
    const rhs_contract_axis = attrs.rhs_contracting[0];
    const n_axis: u8 = if (rhs_contract_axis == k_axis) m_axis else k_axis;
    const rhs_k = rhs_shape.dim(rhs_contract_axis);
    const n = rhs_shape.dim(n_axis);
    if (m <= 0 or k <= 0 or rhs_k <= 0 or n <= 0 or k != rhs_k) return false;
    return output_shape.dim(m_axis) == m and output_shape.dim(k_axis) == n and
        shapeHasConcreteElements(lhs_shape) and shapeHasConcreteElements(rhs_shape) and shapeHasConcreteElements(output_shape);
}

fn metalConvGeneralHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .conv_general => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const weight_shape = nodeInputShape(query, 1) orelse return false;
    if (input_shape.dtype != .f32 or weight_shape.dtype != .f32 or query.graph.node(query.node_id).output_shape.dtype != .f32) return false;
    if (attrs.num_spatial == 1 and attrs.groups == 1 and input_shape.rank() == 3 and weight_shape.rank() == 3 and attrs.padding[0][0] == attrs.padding[0][1]) {
        if ((positiveShapeDim(input_shape, 1) orelse return false) != (positiveShapeDim(weight_shape, 1) orelse return false)) return false;
        return conv1dOutputMatches(query.graph.node(query.node_id).output_shape, input_shape, weight_shape, attrs.strides[0], attrs.padding[0][0], null);
    }
    if (attrs.num_spatial == 2 and input_shape.rank() == 4 and weight_shape.rank() == 4 and attrs.padding[0][0] == attrs.padding[0][1] and attrs.padding[1][0] == attrs.padding[1][1]) {
        return conv2dOutputMatches(query.graph.node(query.node_id).output_shape, input_shape, weight_shape, attrs.strides[0], attrs.strides[1], attrs.padding[0][0], attrs.padding[1][0], std.math.cast(usize, attrs.groups) orelse return false);
    }
    return false;
}

fn metalFusedConv1dHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .fused_conv1d => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const weight_shape = nodeInputShape(query, 1) orelse return false;
    const bias_shape = nodeInputShape(query, 2) orelse return false;
    if (input_shape.dtype != .f32 or weight_shape.dtype != .f32 or bias_shape.dtype != .f32 or query.graph.node(query.node_id).output_shape.dtype != .f32) return false;
    if (input_shape.rank() != 3 or weight_shape.rank() != 3 or bias_shape.rank() != 1) return false;
    if ((positiveShapeDim(input_shape, 1) orelse return false) != attrs.in_channels) return false;
    if ((positiveShapeDim(weight_shape, 0) orelse return false) != attrs.out_channels) return false;
    if ((positiveShapeDim(weight_shape, 1) orelse return false) != attrs.in_channels) return false;
    if ((positiveShapeDim(weight_shape, 2) orelse return false) != attrs.kernel_size) return false;
    if ((positiveShapeDim(bias_shape, 0) orelse return false) != attrs.out_channels) return false;
    return conv1dOutputMatches(query.graph.node(query.node_id).output_shape, input_shape, weight_shape, @intCast(attrs.stride), @intCast(attrs.padding), attrs.out_channels);
}

fn metalFusedConv2dHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .fused_conv2d => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const weight_shape = nodeInputShape(query, 1) orelse return false;
    const bias_shape = nodeInputShape(query, 2) orelse return false;
    if (input_shape.dtype != .f32 or weight_shape.dtype != .f32 or bias_shape.dtype != .f32 or query.graph.node(query.node_id).output_shape.dtype != .f32) return false;
    if (input_shape.rank() != 4 or weight_shape.rank() != 4 or bias_shape.rank() != 1) return false;
    if ((positiveShapeDim(input_shape, 1) orelse return false) != attrs.in_channels) return false;
    if ((positiveShapeDim(weight_shape, 0) orelse return false) != attrs.out_channels) return false;
    if ((positiveShapeDim(weight_shape, 2) orelse return false) != attrs.kernel_h) return false;
    if ((positiveShapeDim(weight_shape, 3) orelse return false) != attrs.kernel_w) return false;
    if ((positiveShapeDim(bias_shape, 0) orelse return false) != attrs.out_channels) return false;
    return conv2dOutputMatches(query.graph.node(query.node_id).output_shape, input_shape, weight_shape, @intCast(attrs.stride_h), @intCast(attrs.stride_w), @intCast(attrs.padding_h), @intCast(attrs.padding_w), attrs.groups);
}

fn metalArgmaxHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .argmax => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    if (input_shape.dtype != .f32) return false;
    if (attrs.axis >= input_shape.rank()) return false;
    if (input_shape.dim(attrs.axis) <= 0) return false;
    return shapeHasConcreteElements(input_shape);
}

fn metalGatherHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .gather => |attrs| attrs,
        else => return false,
    };
    if (attrs.axis != 0) return false;
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const indices_shape = nodeInputShape(query, 1) orelse return false;
    if (input_shape.dtype != .f32) return false;
    if (input_shape.rank() != 2) return false;
    if (indices_shape.rank() + 1 > ml.graph.shape.max_rank) return false;
    return shapeHasConcreteElements(input_shape) and shapeHasConcreteElements(indices_shape);
}

fn metalReduceLastDimHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .reduce_sum, .reduce_max, .reduce_mean => |attrs| attrs,
        else => return false,
    };
    if (attrs.num_axes != 1) return false;
    const input_shape = nodeInputShape(query, 0) orelse return false;
    if (input_shape.dtype != .f32) return false;
    const rank = input_shape.rank();
    if (rank == 0 or attrs.axes[0] != rank - 1) return false;
    if (input_shape.dim(rank - 1) <= 0) return false;
    return shapeHasConcreteElements(input_shape);
}

fn metalSoftmaxHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .fused_softmax, .fused_log_softmax => |attrs| attrs,
        else => return false,
    };
    if (attrs.dim == 0) return false;
    const input_shape = nodeInputShape(query, 0) orelse return false;
    if (input_shape.dtype != .f32) return false;
    const rank = input_shape.rank();
    if (rank == 0) return false;
    const last_dim = input_shape.dim(rank - 1);
    if (last_dim <= 0 or last_dim != attrs.dim) return false;
    const elems = shapeElementCount(input_shape) orelse return false;
    return elems % attrs.dim == 0;
}

fn metalBroadcastHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .broadcast_in_dim => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const output_shape = query.graph.node(query.node_id).output_shape;
    if (input_shape.dtype != .f32 or output_shape.dtype != .f32) return false;
    const rank = input_shape.rank();
    const output_rank = output_shape.rank();
    if (rank == 0 or output_rank == 0 or attrs.num_axes != rank) return false;
    const input_elems = shapeElementCount(input_shape) orelse return false;
    const output_elems = shapeElementCount(output_shape) orelse return false;
    if (input_elems > std.math.maxInt(u32) or output_elems > std.math.maxInt(u32)) return false;

    var mapped_output_axes = [_]bool{false} ** ml.graph.shape.max_rank;
    for (0..rank) |input_axis| {
        const output_axis: usize = attrs.broadcast_axes[input_axis];
        if (output_axis >= output_rank or mapped_output_axes[output_axis]) return false;
        mapped_output_axes[output_axis] = true;

        const in_dim = input_shape.dim(@intCast(input_axis));
        const out_dim = output_shape.dim(@intCast(output_axis));
        if (in_dim <= 0 or out_dim <= 0) return false;
        if (in_dim != 1 and in_dim != out_dim) return false;
    }
    return true;
}

fn metalSliceHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .slice => |attrs| attrs,
        else => return false,
    };
    const input_shape = nodeInputShape(query, 0) orelse return false;
    const output_shape = query.graph.node(query.node_id).output_shape;
    if (input_shape.dtype != .f32 or output_shape.dtype != .f32) return false;
    const rank = input_shape.rank();
    if (rank == 0 or output_shape.rank() != rank or attrs.num_axes != rank) return false;

    var starts: [ml.graph.shape.max_rank]usize = undefined;
    var strides: [ml.graph.shape.max_rank]usize = undefined;
    for (0..rank) |axis| {
        const dim = input_shape.dims[axis];
        const out_dim = output_shape.dims[axis];
        if (dim <= 0 or out_dim <= 0) return false;
        const stride = attrs.strides[axis];
        if (stride <= 0) return false;
        var start = attrs.starts[axis];
        var limit = attrs.limits[axis];
        if (start < 0) start += dim;
        if (limit < 0) limit += dim;
        if (start < 0) start = 0;
        if (limit > dim) limit = dim;
        if (start > limit) limit = start;
        const span = limit - start;
        const computed_out_dim: i64 = if (span <= 0) 0 else @divFloor(span + stride - 1, stride);
        if (computed_out_dim != out_dim) return false;
        starts[axis] = @intCast(start);
        strides[axis] = @intCast(stride);
    }
    if (rank == 2 and
        starts[0] == 0 and
        output_shape.dims[0] == input_shape.dims[0] and
        strides[0] == 1 and
        strides[1] == 1)
    {
        return true;
    }
    return metalSliceIsContiguousView(input_shape, output_shape, starts[0..rank], strides[0..rank]);
}

fn metalSliceIsContiguousView(
    input_shape: ml.graph.Shape,
    output_shape: ml.graph.Shape,
    starts: []const usize,
    strides: []const usize,
) bool {
    const rank = input_shape.rank();
    if (rank != output_shape.rank() or starts.len != rank or strides.len != rank) return false;

    var out_numel: u64 = 1;
    var last_changed: ?usize = null;
    for (0..rank) |axis| {
        const in_dim = input_shape.dims[axis];
        const out_dim = output_shape.dims[axis];
        if (in_dim <= 0 or out_dim <= 0 or strides[axis] != 1) return false;
        const in_dim_usize: usize = @intCast(in_dim);
        const out_dim_usize: usize = @intCast(out_dim);
        if (starts[axis] > in_dim_usize) return false;
        if (out_dim_usize > in_dim_usize - starts[axis]) return false;
        if (starts[axis] != 0 or out_dim != in_dim) last_changed = axis;
        out_numel = checkedMul(out_numel, @intCast(out_dim_usize));
        if (out_numel == std.math.maxInt(u64)) return false;
    }

    const cut = last_changed orelse return true;
    var axis: usize = 0;
    while (axis < cut) : (axis += 1) {
        if (output_shape.dims[axis] != 1) return false;
    }
    axis = cut + 1;
    while (axis < rank) : (axis += 1) {
        if (starts[axis] != 0 or output_shape.dims[axis] != input_shape.dims[axis]) return false;
    }
    return true;
}

fn metalSdpaHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .fused_sdpa => |attrs| attrs,
        else => return false,
    };
    if (attrs.batch == 0 or attrs.seq_len == 0 or attrs.num_heads == 0 or attrs.head_dim == 0) return false;
    const kv_heads = if (attrs.num_kv_heads != 0) attrs.num_kv_heads else attrs.num_heads;
    const kv_len = if (attrs.kv_seq_len != 0) attrs.kv_seq_len else attrs.seq_len;

    const q_expected = checkedMul(
        checkedMul(@as(u64, attrs.batch), @as(u64, attrs.num_heads)),
        checkedMul(@as(u64, attrs.seq_len), @as(u64, attrs.head_dim)),
    );
    const kv_expected = checkedMul(
        checkedMul(@as(u64, attrs.batch), @as(u64, kv_heads)),
        checkedMul(@as(u64, kv_len), @as(u64, attrs.head_dim)),
    );
    if (q_expected == 0 or q_expected == std.math.maxInt(u64) or
        kv_expected == 0 or kv_expected == std.math.maxInt(u64)) return false;

    for (0..3) |input_index| {
        const input_shape = nodeInputShape(query, input_index) orelse return false;
        if (input_shape.dtype != .f32) return false;
        const expected = if (input_index == 0) q_expected else kv_expected;
        if (shapeElementCount(input_shape) != expected) return false;
    }

    if (nodeInputShape(query, 3)) |bias_shape| {
        if (bias_shape.dtype != .f32) return false;
        const shared_bias_len = checkedMul(
            @as(u64, attrs.num_heads),
            checkedMul(@as(u64, attrs.seq_len), @as(u64, kv_len)),
        );
        const batched_bias_len = checkedMul(@as(u64, attrs.batch), shared_bias_len);
        const broadcast_head_bias_len = checkedMul(
            @as(u64, attrs.batch),
            checkedMul(@as(u64, attrs.seq_len), @as(u64, kv_len)),
        );
        const bias_elems = shapeElementCount(bias_shape) orelse return false;
        if (bias_elems != shared_bias_len and bias_elems != batched_bias_len and bias_elems != broadcast_head_bias_len) return false;
    }
    return true;
}

fn metalGqaCausalAttentionHasResidentShape(query: CapabilityQuery) bool {
    const attrs = switch (query.op) {
        .fused_gqa_causal_attention => |attrs| attrs,
        else => return false,
    };
    if (attrs.batch == 0 or attrs.seq_len == 0 or attrs.num_heads == 0 or attrs.head_dim == 0) return false;
    const kv_heads = if (attrs.num_kv_heads != 0) attrs.num_kv_heads else attrs.num_heads;
    const kv_len = if (attrs.kv_seq_len != 0) attrs.kv_seq_len else attrs.seq_len;

    const q_expected = checkedMul(
        checkedMul(@as(u64, attrs.batch), @as(u64, attrs.num_heads)),
        checkedMul(@as(u64, attrs.seq_len), @as(u64, attrs.head_dim)),
    );
    // Paged decode graph inputs carry only the current suffix rows. The full
    // kv_seq_len lives in the runtime KV cache and is selected by attrs.
    const kv_expected = checkedMul(
        checkedMul(@as(u64, attrs.batch), @as(u64, kv_heads)),
        checkedMul(@as(u64, attrs.seq_len), @as(u64, attrs.head_dim)),
    );
    if (q_expected == 0 or q_expected == std.math.maxInt(u64) or
        kv_expected == 0 or kv_expected == std.math.maxInt(u64)) return false;

    for (0..3) |input_index| {
        const input_shape = nodeInputShape(query, input_index) orelse return false;
        if (input_shape.dtype != .f32) return false;
        const expected = if (input_index == 0) q_expected else kv_expected;
        if (shapeElementCount(input_shape) != expected) return false;
    }

    if (nodeInputShape(query, 3)) |bias_shape| {
        if (bias_shape.dtype != .f32) return false;
        const shared_bias_len = checkedMul(
            @as(u64, attrs.num_heads),
            checkedMul(@as(u64, attrs.seq_len), @as(u64, kv_len)),
        );
        const batched_bias_len = checkedMul(@as(u64, attrs.batch), shared_bias_len);
        const broadcast_head_bias_len = checkedMul(
            @as(u64, attrs.batch),
            checkedMul(@as(u64, attrs.seq_len), @as(u64, kv_len)),
        );
        const bias_elems = shapeElementCount(bias_shape) orelse return false;
        if (bias_elems != shared_bias_len and bias_elems != batched_bias_len and bias_elems != broadcast_head_bias_len) return false;
    }
    return true;
}

fn nodeInputShape(query: CapabilityQuery, index: usize) ?ml.graph.Shape {
    const n = query.graph.node(query.node_id);
    const inputs = n.getInputs();
    if (index >= inputs.len) return null;
    const input_id = inputs[index];
    if (input_id == null_node) return null;
    return query.graph.node(input_id).output_shape;
}

fn shapeHasConcreteElements(shape: ml.graph.Shape) bool {
    return shapeElementCount(shape) != null;
}

fn shapeElementCount(shape: ml.graph.Shape) ?u64 {
    const elems = shape.numElements() orelse return null;
    if (elems <= 0) return null;
    return @intCast(elems);
}

fn positiveShapeDim(shape: ml.graph.Shape, axis: usize) ?usize {
    if (axis >= shape.rank()) return null;
    const dim = shape.dim(@intCast(axis));
    if (dim <= 0) return null;
    return std.math.cast(usize, dim);
}

fn conv1dOutputMatches(
    output_shape: ml.graph.Shape,
    input_shape: ml.graph.Shape,
    weight_shape: ml.graph.Shape,
    stride_i64: i64,
    padding_i64: i64,
    out_channels_override: ?usize,
) bool {
    if (output_shape.rank() != 3 or stride_i64 <= 0 or padding_i64 < 0) return false;
    const batch = positiveShapeDim(input_shape, 0) orelse return false;
    const time_steps = positiveShapeDim(input_shape, 2) orelse return false;
    const out_channels = out_channels_override orelse (positiveShapeDim(weight_shape, 0) orelse return false);
    const kernel_size = positiveShapeDim(weight_shape, 2) orelse return false;
    const stride: usize = @intCast(stride_i64);
    const padding: usize = @intCast(padding_i64);
    if (time_steps + 2 * padding < kernel_size) return false;
    const out_time = (time_steps + 2 * padding - kernel_size) / stride + 1;
    return out_time > 0 and
        output_shape.dim(0) == @as(i64, @intCast(batch)) and
        output_shape.dim(1) == @as(i64, @intCast(out_channels)) and
        output_shape.dim(2) == @as(i64, @intCast(out_time));
}

fn conv2dOutputMatches(
    output_shape: ml.graph.Shape,
    input_shape: ml.graph.Shape,
    weight_shape: ml.graph.Shape,
    stride_h_i64: i64,
    stride_w_i64: i64,
    padding_h_i64: i64,
    padding_w_i64: i64,
    groups: usize,
) bool {
    if (output_shape.rank() != 4 or stride_h_i64 <= 0 or stride_w_i64 <= 0 or padding_h_i64 < 0 or padding_w_i64 < 0 or groups == 0) return false;
    const batch = positiveShapeDim(input_shape, 0) orelse return false;
    const in_channels = positiveShapeDim(input_shape, 1) orelse return false;
    const height = positiveShapeDim(input_shape, 2) orelse return false;
    const width = positiveShapeDim(input_shape, 3) orelse return false;
    const out_channels = positiveShapeDim(weight_shape, 0) orelse return false;
    const weight_in_channels = positiveShapeDim(weight_shape, 1) orelse return false;
    const kernel_h = positiveShapeDim(weight_shape, 2) orelse return false;
    const kernel_w = positiveShapeDim(weight_shape, 3) orelse return false;
    if (in_channels % groups != 0 or out_channels % groups != 0 or weight_in_channels != in_channels / groups) return false;
    const stride_h: usize = @intCast(stride_h_i64);
    const stride_w: usize = @intCast(stride_w_i64);
    const padding_h: usize = @intCast(padding_h_i64);
    const padding_w: usize = @intCast(padding_w_i64);
    if (height + 2 * padding_h < kernel_h or width + 2 * padding_w < kernel_w) return false;
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    return out_h > 0 and out_w > 0 and
        output_shape.dim(0) == @as(i64, @intCast(batch)) and
        output_shape.dim(1) == @as(i64, @intCast(out_channels)) and
        output_shape.dim(2) == @as(i64, @intCast(out_h)) and
        output_shape.dim(3) == @as(i64, @intCast(out_w));
}

fn inputDesc(descs: []const ?contracts.TensorDesc, input_id: NodeId) ?contracts.TensorDesc {
    if (input_id == null_node) return null;
    const index: usize = @intCast(input_id);
    if (index >= descs.len) return null;
    return descs[index];
}

fn nodeComputeCost(graph: *const Graph, node_id: NodeId) u64 {
    const n = graph.node(node_id);
    return switch (n.op) {
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair => |attrs| mul3Cost(attrs.rows, attrs.in_dim, attrs.out_dim),
        .dot_general => |attrs| dotGeneralCost(graph, node_id, attrs) orelse nodeElementCost(graph, node_id),
        else => nodeElementCost(graph, node_id),
    };
}

fn dotGeneralCost(graph: *const Graph, node_id: NodeId, attrs: ml.graph.node.DotGeneralAttrs) ?u64 {
    if (attrs.num_contracting != 1) return null;
    const n = graph.node(node_id);
    if (n.num_inputs < 2) return null;
    const lhs = graph.node(n.inputs[0]).output_shape;
    const lhs_axis = attrs.lhs_contracting[0];
    if (lhs_axis >= lhs.rank()) return null;
    const k = lhs.dims[lhs_axis];
    if (k <= 0) return null;
    return checkedMul(nodeElementCost(graph, node_id), @intCast(k));
}

fn nodeElementCost(graph: *const Graph, node_id: NodeId) u64 {
    const shape = graph.node(node_id).output_shape;
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return 1;
    return @intCast(elems);
}

fn tensorByteSize(shape: ml.graph.Shape) u64 {
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return @intCast(shape.dtype.byteSize());
    return checkedMul(@intCast(elems), @intCast(shape.dtype.byteSize()));
}

fn descByteSize(desc: contracts.TensorDesc) u64 {
    return tensorByteSize(desc.shape);
}

fn nodeInputBytes(query: CapabilityQuery) u64 {
    const descs = query.tensor_descs orelse return 0;
    const n = query.graph.node(query.node_id);
    var total: u64 = 0;
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| total = checkedAdd(total, descByteSize(desc));
    }
    return total;
}

fn metalHostTransferCost(query: CapabilityQuery) u64 {
    const descs = query.tensor_descs orelse return tensorByteSize(query.graph.node(query.node_id).output_shape);
    const n = query.graph.node(query.node_id);
    var total: u64 = 0;
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (!descIsMetalResident(desc) and desc.storage.isHost()) {
                total = checkedAdd(total, descByteSize(desc));
            }
        }
    }
    return checkedAdd(total, tensorByteSize(n.output_shape));
}

fn nodeHasPackedQuantInput(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (desc.isPackedQuant()) return true;
        }
    }
    return false;
}

fn metalPackedQuantMetadataView(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const n = query.graph.node(query.node_id);
    const inputs = n.getInputs();
    if (inputs.len == 0) return false;
    if (std.meta.activeTag(query.op) == .concat_prim) return metalConcatPackedQuantMetadataView(descs, inputs);
    if (std.meta.activeTag(query.op) == .fused_embedding_lookup) {
        const weight_desc = inputDesc(descs, inputs[0]) orelse return false;
        return weight_desc.isPackedQuant() and weight_desc.quant_format != null;
    }
    const source_desc = inputDesc(descs, inputs[0]) orelse return false;
    if (!source_desc.isPackedQuant()) return false;
    return switch (query.op) {
        .transpose => metalTransposeHasResidentShape(query),
        .reshape => metalReshapeHasResidentShape(query),
        else => false,
    };
}

fn metalConcatPackedQuantMetadataView(descs: []const ?contracts.TensorDesc, inputs: []const NodeId) bool {
    var format: ?quant_matmul.Format = null;
    for (inputs) |input_id| {
        const desc = inputDesc(descs, input_id) orelse return false;
        if (!desc.isPackedQuant()) return false;
        const input_format = desc.quant_format orelse return false;
        if (format) |existing| {
            if (existing != input_format) return false;
        } else {
            format = input_format;
        }
    }
    return format != null;
}

fn nodeInputsAreMetalCompatible(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return true;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (desc.resident_backend) |backend| {
                if (backend != .metal) return false;
            }
            switch (desc.storage) {
                .unknown, .metadata_view, .runtime_input, .constant, .host_f32, .host_dense, .host_packed_quant, .metal_buffer => {},
                else => return false,
            }
        }
    }
    return true;
}

fn nodeOutputIsMetalResident(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const desc = inputDesc(descs, query.node_id) orelse return false;
    return descIsMetalResident(desc);
}

fn nodeOutputIsPackedQuant(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const desc = inputDesc(descs, query.node_id) orelse return false;
    return desc.isPackedQuant();
}

fn nodeHasMetalResidentInput(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (descIsMetalResident(desc)) return true;
        }
    }
    return false;
}

fn descIsMetalResident(desc: contracts.TensorDesc) bool {
    return desc.storage == .metal_buffer or desc.resident_backend == .metal;
}

fn metalEstimatedCost(query: CapabilityQuery) u64 {
    return checkedAdd(nodeComputeCost(query.graph, query.node_id) / 16, metalHostTransferCost(query));
}

fn mul3Cost(a: u32, b: u32, c: u32) u64 {
    return checkedMul(checkedMul(a, b), c);
}

fn checkedMul(a: u64, b: u64) u64 {
    return std.math.mul(u64, a, b) catch std.math.maxInt(u64);
}

fn checkedAdd(a: u64, b: u64) u64 {
    return std.math.add(u64, a, b) catch std.math.maxInt(u64);
}

const metalHostMatmulMinComputeCost: u64 = 128 * 1024;
const metalHostElementwiseMinTransferBytes: u64 = 256 * 1024;

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;

test "metal eager graph capability keeps GLiNER scatter_add resident" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const dest = try builder.parameter("dest", ml.graph.Shape.init(.f32, &.{ 4, 2 }));
    const values = try builder.parameter("values", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const indices = try builder.parameter("indices", ml.graph.Shape.init(.i64, &.{2}));
    const scattered = try builder.scatterAdd(dest, values, indices, 0);
    try graph.markOutput(scattered);

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &supportsMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll },
    };
    var plan = try partition.partition(allocator, &graph, &caps);
    defer plan.deinit();

    var saw_metal = false;
    for (plan.partitions) |part| {
        if (part.backend == .metal) saw_metal = true;
    }
    try std.testing.expect(saw_metal);
    try std.testing.expect(supportsMetalEagerGraph(graph.node(scattered).op));
}

test "metal eager graph diagnostics distinguish host-assisted accepted ops" {
    const linear: OpCode = .{ .fused_linear_no_bias = .{
        .rows = 1,
        .in_dim = 8,
        .out_dim = 8,
    } };
    const rms_norm: OpCode = .{ .fused_rms_norm = .{
        .dim = 8,
        .eps = 1e-5,
    } };
    const parameter: OpCode = .{ .parameter = .{ .name_offset = 0, .name_len = 0 } };
    const gather: OpCode = .{ .gather = .{
        .axis = 0,
    } };
    const convert: OpCode = .{ .convert_dtype = .{ .target = .i64 } };
    const argmax: OpCode = .{ .argmax = .{ .axis = 1, .keepdims = false } };
    const sdpa: OpCode = .{ .fused_sdpa = .{
        .batch = 1,
        .seq_len = 2,
        .num_heads = 1,
        .head_dim = 8,
    } };
    const add: OpCode = .{ .add = {} };
    const rsqrt: OpCode = .{ .rsqrt = {} };
    const where_select: OpCode = .{ .where_select = {} };
    const reduce_mean: OpCode = .{ .reduce_mean = .{} };
    const transpose: OpCode = .{ .transpose = .{
        .perm = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .num_axes = 2,
    } };
    const dot: OpCode = .{ .dot_general = .{
        .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
        .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
        .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
        .num_contracting = 1,
        .num_batch = 0,
    } };
    const conv: OpCode = .{ .conv_general = .{} };
    const conv1d: OpCode = .{ .fused_conv1d = .{
        .batch = 1,
        .in_channels = 1,
        .out_channels = 1,
        .time_steps = 8,
        .kernel_size = 3,
        .stride = 1,
        .padding = 0,
    } };
    const conv2d: OpCode = .{ .fused_conv2d = .{
        .batch = 1,
        .in_channels = 1,
        .out_channels = 1,
        .height = 8,
        .width = 8,
        .kernel_h = 3,
        .kernel_w = 3,
        .stride_h = 1,
        .stride_w = 1,
        .padding_h = 0,
        .padding_w = 0,
        .groups = 1,
    } };

    try std.testing.expect(supportsMetalEagerGraph(linear));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(linear));
    try std.testing.expect(supportsMetalEagerGraph(rms_norm));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(rms_norm));
    try std.testing.expect(supportsMetalEagerGraph(parameter));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(parameter));
    try std.testing.expect(supportsMetalEagerGraph(add));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(add));
    try std.testing.expect(supportsMetalEagerGraph(rsqrt));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(rsqrt));
    try std.testing.expect(supportsMetalEagerGraph(where_select));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(where_select));
    try std.testing.expect(supportsMetalEagerGraph(reduce_mean));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(reduce_mean));
    try std.testing.expect(supportsMetalEagerGraph(gather));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(gather));
    try std.testing.expect(supportsMetalEagerGraph(convert));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(convert));
    try std.testing.expect(supportsMetalEagerGraph(argmax));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(argmax));
    try std.testing.expect(supportsMetalEagerGraph(sdpa));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(sdpa));
    try std.testing.expect(supportsMetalEagerGraph(transpose));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(transpose));
    try std.testing.expect(supportsMetalEagerGraph(dot));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(dot));
    try std.testing.expect(supportsMetalEagerGraph(conv));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(conv));
    try std.testing.expect(supportsMetalEagerGraph(conv1d));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(conv1d));
    try std.testing.expect(supportsMetalEagerGraph(conv2d));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(conv2d));
    try std.testing.expect(supportsMetalEagerGraph(.{ .fused_zero_tensor = .{ .rows = 1, .in_dim = 0, .out_dim = 4 } }));
    try std.testing.expect(!metalEagerGraphOpIsHostAssisted(.{ .fused_zero_tensor = .{ .rows = 1, .in_dim = 0, .out_dim = 4 } }));
}

fn expectMetalNodeAcceptedResident(
    graph: *const Graph,
    tensor_descs: []const ?contracts.TensorDesc,
    node_id: NodeId,
) !void {
    const query = CapabilityQuery{
        .graph = graph,
        .node_id = node_id,
        .op = graph.node(node_id).op,
        .tensor_descs = tensor_descs,
    };
    const decision = decideMetalEagerGraph(query);
    try std.testing.expect(decision.can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(query));
}

test "metal diagnostics keep formerly host-assisted transpose dot and conv resident" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const transpose_input = try b.parameter("transpose_input", Shape.init(.f32, &.{ 2, 3 }));
    const transposed = try b.transpose(transpose_input, &.{ 1, 0 });

    const lhs = try b.parameter("lhs", Shape.init(.f32, &.{ 2, 2 }));
    const rhs = try b.parameter("rhs", Shape.init(.f32, &.{ 2, 3 }));
    const dot = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 3 }),
        .inputs = .{ lhs, rhs, null_node, null_node },
        .num_inputs = 2,
    });

    const conv1_input = try b.parameter("conv1_input", Shape.init(.f32, &.{ 1, 1, 4 }));
    const conv1_weight = try b.parameter("conv1_weight", Shape.init(.f32, &.{ 1, 1, 2 }));
    const conv1_general = try g.addNode(.{
        .op = .{ .conv_general = .{
            .strides = .{ 1, 1, 1, 1 },
            .padding = .{ .{ 0, 0 }, .{ 0, 0 }, .{ 0, 0 }, .{ 0, 0 } },
            .num_spatial = 1,
            .groups = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 3 }),
        .inputs = .{ conv1_input, conv1_weight, null_node, null_node },
        .num_inputs = 2,
    });

    const fused_conv1_bias = try b.parameter("fused_conv1_bias", Shape.init(.f32, &.{1}));
    const fused_conv1 = try g.addNode(.{
        .op = .{ .fused_conv1d = .{
            .batch = 1,
            .in_channels = 1,
            .out_channels = 1,
            .time_steps = 4,
            .kernel_size = 2,
            .stride = 1,
            .padding = 0,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 3 }),
        .inputs = .{ conv1_input, conv1_weight, fused_conv1_bias, null_node },
        .num_inputs = 3,
    });

    const conv2_input = try b.parameter("conv2_input", Shape.init(.f32, &.{ 1, 1, 3, 3 }));
    const conv2_weight = try b.parameter("conv2_weight", Shape.init(.f32, &.{ 1, 1, 2, 2 }));
    const conv2_general = try g.addNode(.{
        .op = .{ .conv_general = .{
            .strides = .{ 1, 1, 1, 1 },
            .padding = .{ .{ 0, 0 }, .{ 0, 0 }, .{ 0, 0 }, .{ 0, 0 } },
            .num_spatial = 2,
            .groups = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 2, 2 }),
        .inputs = .{ conv2_input, conv2_weight, null_node, null_node },
        .num_inputs = 2,
    });

    const fused_conv2_bias = try b.parameter("fused_conv2_bias", Shape.init(.f32, &.{1}));
    const fused_conv2 = try g.addNode(.{
        .op = .{ .fused_conv2d = .{
            .batch = 1,
            .in_channels = 1,
            .out_channels = 1,
            .height = 3,
            .width = 3,
            .kernel_h = 2,
            .kernel_w = 2,
            .stride_h = 1,
            .stride_w = 1,
            .padding_h = 0,
            .padding_w = 0,
            .groups = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 2, 2 }),
        .inputs = .{ conv2_input, conv2_weight, fused_conv2_bias, null_node },
        .num_inputs = 3,
    });

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);

    try expectMetalNodeAcceptedResident(&g, seeds, transposed);
    try expectMetalNodeAcceptedResident(&g, seeds, dot);
    try expectMetalNodeAcceptedResident(&g, seeds, conv1_general);
    try expectMetalNodeAcceptedResident(&g, seeds, fused_conv1);
    try expectMetalNodeAcceptedResident(&g, seeds, conv2_general);
    try expectMetalNodeAcceptedResident(&g, seeds, fused_conv2);
}

test "metal planner keeps clipclap l2 normalize tail resident" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const batch: i64 = 4;
    const dim: i64 = 768;
    const x = try b.parameter("projected", Shape.init(.f32, &.{ batch, dim }));
    const eps = try b.parameter("l2_eps", Shape.init(.f32, &.{ batch, 1 }));
    const one = try b.parameter("l2_one", Shape.init(.f32, &.{ batch, 1 }));
    const squared = try b.mul(x, x);
    const sum_sq = try b.reduceSum(squared, &.{1});
    const zeroish = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = Shape.init(.f32, &.{ batch, 1 }),
        .inputs = .{ sum_sq, eps, null_node, null_node },
        .num_inputs = 2,
    });
    const inv_norm_raw = try b.rsqrt(sum_sq);
    const inv_norm = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = Shape.init(.f32, &.{ batch, 1 }),
        .inputs = .{ zeroish, one, inv_norm_raw, null_node },
        .num_inputs = 3,
    });
    const scale = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ batch, dim }),
            .broadcast_axes = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_axes = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ batch, dim }),
        .inputs = .{ inv_norm, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const normalized = try b.mul(x, scale);
    try g.markOutput(normalized);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    for (&[_]NodeId{ squared, sum_sq, zeroish, inv_norm_raw, inv_norm, scale, normalized }) |node_id| {
        try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[node_id]].backend);
    }
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.wrong_storage));
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

test "metal planner accepts default reverse transpose with resident input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const transposed = try g.addNode(.{
        .op = .{ .transpose = .{} },
        .output_shape = Shape.init(.f32, &.{ 4, 3, 2 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(transposed);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedTensorDescriptor(seeds, &g, x, contracts.TensorDesc.init(g.node(x).output_shape, .metal_buffer));
    const descs = try partition.buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = transposed,
        .op = g.node(transposed).op,
        .tensor_descs = descs,
    });
    try std.testing.expect(decision.can_execute);
}

test "metal planner accepts clip attention-layout transpose with resident input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 1, 8, 64 }));
    const transposed = try b.transpose(x, &.{ 0, 2, 1, 3 });
    try g.markOutput(transposed);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedTensorDescriptor(seeds, &g, x, contracts.TensorDesc.init(g.node(x).output_shape, .metal_buffer));
    const descs = try partition.buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = transposed,
        .op = g.node(transposed).op,
        .tensor_descs = descs,
    });
    try std.testing.expect(decision.can_execute);
}

test "metal eager graph capability decisions are attrs and shape aware" {
    const allocator = std.testing.allocator;

    var broadcast_graph = Graph.init(allocator);
    defer broadcast_graph.deinit();
    var broadcast_builder = Builder.init(&broadcast_graph);
    const broadcast_input = try broadcast_builder.parameter("broadcast_input", Shape.init(.f32, &.{ 2, 4 }));
    const rank_expanding_broadcast = try broadcast_graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
            .broadcast_axes = .{ 0, 2, 0, 0, 0, 0, 0, 0 },
            .num_axes = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
        .inputs = .{ broadcast_input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const duplicate_axis_broadcast = try broadcast_graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
            .broadcast_axes = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_axes = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
        .inputs = .{ broadcast_input, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    const broadcast_seeds = try partition.allocTensorDescriptorSeeds(allocator, &broadcast_graph);
    defer allocator.free(broadcast_seeds);
    try partition.seedAllParameterResidency(broadcast_seeds, &broadcast_graph, .metal, 0);

    const rank_expanding_broadcast_query = CapabilityQuery{
        .graph = &broadcast_graph,
        .node_id = rank_expanding_broadcast,
        .op = broadcast_graph.node(rank_expanding_broadcast).op,
        .tensor_descs = broadcast_seeds,
    };
    try std.testing.expect(decideMetalEagerGraph(rank_expanding_broadcast_query).can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(rank_expanding_broadcast_query));

    const duplicate_axis_broadcast_query = CapabilityQuery{
        .graph = &broadcast_graph,
        .node_id = duplicate_axis_broadcast,
        .op = broadcast_graph.node(duplicate_axis_broadcast).op,
        .tensor_descs = broadcast_seeds,
    };
    try std.testing.expect(!decideMetalEagerGraph(duplicate_axis_broadcast_query).can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(duplicate_axis_broadcast_query));

    var gather_graph = Graph.init(allocator);
    defer gather_graph.deinit();
    var gather_builder = Builder.init(&gather_graph);
    const table = try gather_builder.parameter("table", Shape.init(.f32, &.{ 4, 3 }));
    const indices = try gather_builder.parameter("indices", Shape.init(.i64, &.{2}));
    const gathered = try gather_builder.gather(table, indices, Shape.init(.f32, &.{ 2, 3 }));
    const gather_axis1 = try gather_graph.addNode(.{
        .op = .{ .gather = .{ .axis = 1 } },
        .output_shape = Shape.init(.f32, &.{ 4, 2 }),
        .inputs = .{ table, indices, null_node, null_node },
        .num_inputs = 2,
    });

    const gather_seeds = try partition.allocTensorDescriptorSeeds(allocator, &gather_graph);
    defer allocator.free(gather_seeds);
    try partition.seedAllParameterResidency(gather_seeds, &gather_graph, .metal, 0);

    const valid_gather_query = CapabilityQuery{
        .graph = &gather_graph,
        .node_id = gathered,
        .op = gather_graph.node(gathered).op,
        .tensor_descs = gather_seeds,
    };
    const valid_gather_decision = decideMetalEagerGraph(valid_gather_query);
    try std.testing.expect(valid_gather_decision.can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(valid_gather_query));

    const invalid_gather_query = CapabilityQuery{
        .graph = &gather_graph,
        .node_id = gather_axis1,
        .op = gather_graph.node(gather_axis1).op,
        .tensor_descs = gather_seeds,
    };
    const invalid_gather_decision = decideMetalEagerGraph(invalid_gather_query);
    try std.testing.expect(!invalid_gather_decision.can_execute);
    try std.testing.expectEqual(CapabilityReason.unsupported_op, invalid_gather_decision.reason);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(invalid_gather_query));

    var argmax_graph = Graph.init(allocator);
    defer argmax_graph.deinit();
    var argmax_builder = Builder.init(&argmax_graph);
    const argmax_input = try argmax_builder.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const argmax = try argmax_builder.argMax(argmax_input, 1, false);
    const dynamic_input = try argmax_builder.parameter("dynamic", Shape.init(.f32, &.{ 2, -1 }));
    const dynamic_argmax = try argmax_graph.addNode(.{
        .op = .{ .argmax = .{ .axis = 1, .keepdims = false } },
        .output_shape = Shape.init(.i64, &.{2}),
        .inputs = .{ dynamic_input, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    const argmax_seeds = try partition.allocTensorDescriptorSeeds(allocator, &argmax_graph);
    defer allocator.free(argmax_seeds);
    try partition.seedAllParameterResidency(argmax_seeds, &argmax_graph, .metal, 0);

    const argmax_query = CapabilityQuery{
        .graph = &argmax_graph,
        .node_id = argmax,
        .op = argmax_graph.node(argmax).op,
        .tensor_descs = argmax_seeds,
    };
    try std.testing.expect(decideMetalEagerGraph(argmax_query).can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(argmax_query));

    const dynamic_argmax_query = CapabilityQuery{
        .graph = &argmax_graph,
        .node_id = dynamic_argmax,
        .op = argmax_graph.node(dynamic_argmax).op,
        .tensor_descs = argmax_seeds,
    };
    try std.testing.expect(!decideMetalEagerGraph(dynamic_argmax_query).can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(dynamic_argmax_query));

    var sdpa_graph = Graph.init(allocator);
    defer sdpa_graph.deinit();
    var sdpa_builder = Builder.init(&sdpa_graph);
    const q = try sdpa_builder.parameter("q", Shape.init(.f32, &.{ 2, 3, 4 }));
    const k = try sdpa_builder.parameter("k", Shape.init(.f32, &.{ 2, 3, 4 }));
    const v = try sdpa_builder.parameter("v", Shape.init(.f32, &.{ 2, 3, 4 }));
    const sdpa = try sdpa_builder.sdpa(q, k, v, 1, 3, 2, 4);
    const mismatched_sdpa = try sdpa_graph.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = 1,
            .seq_len = 3,
            .kv_seq_len = 4,
            .num_heads = 2,
            .head_dim = 4,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });

    const sdpa_seeds = try partition.allocTensorDescriptorSeeds(allocator, &sdpa_graph);
    defer allocator.free(sdpa_seeds);
    try partition.seedAllParameterResidency(sdpa_seeds, &sdpa_graph, .metal, 0);

    const sdpa_query = CapabilityQuery{
        .graph = &sdpa_graph,
        .node_id = sdpa,
        .op = sdpa_graph.node(sdpa).op,
        .tensor_descs = sdpa_seeds,
    };
    const sdpa_decision = decideMetalEagerGraph(sdpa_query);
    try std.testing.expect(sdpa_decision.can_execute);
    const sdpa_operator = sdpa_decision.operator_plan orelse return error.ExpectedOperatorPlan;
    try std.testing.expectEqual(operator_plan.Operator.attention_flash, sdpa_operator.operator());
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(sdpa_query));

    const mismatched_sdpa_query = CapabilityQuery{
        .graph = &sdpa_graph,
        .node_id = mismatched_sdpa,
        .op = sdpa_graph.node(mismatched_sdpa).op,
        .tensor_descs = sdpa_seeds,
    };
    try std.testing.expect(!decideMetalEagerGraph(mismatched_sdpa_query).can_execute);
    try std.testing.expect(!metalEagerGraphNodeIsHostAssisted(mismatched_sdpa_query));

    try expectSdpaBiasShapePlansToMetal(&.{ 2, 3, 3 });
    try expectSdpaBiasShapePlansToMetal(&.{ 1, 2, 3, 3 });
    try expectSdpaBiasShapePlansToMetal(&.{ 1, 1, 3, 3 });
    try expectSdpaBiasShapeRejected(&.{ 2, 2, 3, 3 });
    try expectSdpaBiasShapeRejected(&.{ 2, 3 });

    var slice_graph = Graph.init(allocator);
    defer slice_graph.deinit();
    var slice_builder = Builder.init(&slice_graph);
    const slice_input = try slice_builder.parameter("qkv", Shape.init(.f32, &.{ 2, 6 }));
    const slice = try slice_builder.sliceLastDim(slice_input, 1, 4);
    const slice_seeds = try partition.allocTensorDescriptorSeeds(allocator, &slice_graph);
    defer allocator.free(slice_seeds);
    try partition.seedAllParameterResidency(slice_seeds, &slice_graph, .metal, 0);
    try std.testing.expect(decideMetalEagerGraph(.{
        .graph = &slice_graph,
        .node_id = slice,
        .op = slice_graph.node(slice).op,
        .tensor_descs = slice_seeds,
    }).can_execute);

    var gqa_graph = Graph.init(allocator);
    defer gqa_graph.deinit();
    var gqa_builder = Builder.init(&gqa_graph);
    const gqa_q = try gqa_builder.parameter("q", Shape.init(.f32, &.{ 1, 4 }));
    const gqa_k = try gqa_builder.parameter("k", Shape.init(.f32, &.{ 1, 4 }));
    const gqa_v = try gqa_builder.parameter("v", Shape.init(.f32, &.{ 1, 4 }));
    const gqa_dense = try gqa_graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .kv_seq_len = 1,
            .num_heads = 1,
            .num_kv_heads = 1,
            .head_dim = 4,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ gqa_q, gqa_k, gqa_v, null_node },
        .num_inputs = 3,
    });
    const gqa_skip_kv = try gqa_graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .kv_seq_len = 1,
            .num_heads = 1,
            .num_kv_heads = 1,
            .head_dim = 4,
            .skip_kv_write = true,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ gqa_q, gqa_k, gqa_v, null_node },
        .num_inputs = 3,
    });
    const gqa_skip_kv_decode = try gqa_graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .kv_seq_len = 3,
            .num_heads = 1,
            .num_kv_heads = 1,
            .head_dim = 4,
            .skip_kv_write = true,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ gqa_q, gqa_k, gqa_v, null_node },
        .num_inputs = 3,
    });
    const gqa_paged_decode = try gqa_graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .kv_seq_len = 3,
            .num_heads = 1,
            .num_kv_heads = 1,
            .head_dim = 4,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ gqa_q, gqa_k, gqa_v, null_node },
        .num_inputs = 3,
    });
    const gqa_seeds = try partition.allocTensorDescriptorSeeds(allocator, &gqa_graph);
    defer allocator.free(gqa_seeds);
    try partition.seedAllParameterResidency(gqa_seeds, &gqa_graph, .metal, 0);
    try std.testing.expect(decideMetalEagerGraph(.{
        .graph = &gqa_graph,
        .node_id = gqa_dense,
        .op = gqa_graph.node(gqa_dense).op,
        .tensor_descs = gqa_seeds,
    }).can_execute);
    try std.testing.expect(decideMetalEagerGraph(.{
        .graph = &gqa_graph,
        .node_id = gqa_skip_kv,
        .op = gqa_graph.node(gqa_skip_kv).op,
        .tensor_descs = gqa_seeds,
    }).can_execute);
    try std.testing.expect(decideMetalEagerGraph(.{
        .graph = &gqa_graph,
        .node_id = gqa_skip_kv_decode,
        .op = gqa_graph.node(gqa_skip_kv_decode).op,
        .tensor_descs = gqa_seeds,
    }).can_execute);
    try std.testing.expect(decideMetalEagerGraph(.{
        .graph = &gqa_graph,
        .node_id = gqa_paged_decode,
        .op = gqa_graph.node(gqa_paged_decode).op,
        .tensor_descs = gqa_seeds,
    }).can_execute);
}

test "metal fused sdpa plans compressed kv descriptors as quantized attention" {
    try expectSdpaKvDescriptorPlans(.polar4, .paged, .attention_quantized_kv);
    try expectSdpaKvDescriptorPlans(.turbo3, .paged, .attention_quantized_kv);
}

fn expectSdpaKvDescriptorPlans(
    kv_format: quant_matmul.AttentionKvFormat,
    storage: quant_matmul.AttentionStorage,
    expected_operator: operator_plan.Operator,
) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 4, 2, 8 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 3, 8 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 3, 8 }));
    const sdpa = try g.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = 1,
            .seq_len = 2,
            .kv_seq_len = 3,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 8,
        } },
        .output_shape = Shape.init(.f32, &.{ 4, 2, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedTensorDescriptor(seeds, &g, q, blk: {
        var desc = contracts.TensorDesc.init(g.node(q).output_shape, .metal_buffer);
        desc.resident_backend = .metal;
        break :blk desc;
    });
    const k_desc = contracts.TensorDesc.attentionKv(g.node(k).output_shape, .metal_buffer, kv_format, storage, .metal, 0);
    const v_desc = contracts.TensorDesc.attentionKv(g.node(v).output_shape, .metal_buffer, kv_format, storage, .metal, 0);
    try partition.seedTensorDescriptor(seeds, &g, k, k_desc);
    try partition.seedTensorDescriptor(seeds, &g, v, v_desc);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = sdpa,
        .op = g.node(sdpa).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(decision.can_execute);
    const plan = decision.operator_plan orelse return error.ExpectedOperatorPlan;
    try std.testing.expectEqual(expected_operator, plan.operator());
    const attention = switch (plan) {
        .attention => |attention| attention,
        else => return error.ExpectedOperatorPlan,
    };
    try std.testing.expectEqual(kv_format, attention.kv_format);
    try std.testing.expectEqual(storage, attention.storage);
    try std.testing.expectEqual(@as(usize, 2), attention.q_len);
    try std.testing.expectEqual(@as(usize, 3), attention.kv_len);
}

fn expectSdpaBiasShapePlansToMetal(bias_dims: []const i64) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 2, 3, 4 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 3, 4 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 3, 4 }));
    const bias = try b.parameter("bias", Shape.init(.f32, bias_dims));
    const sdpa = try g.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = 1,
            .seq_len = 3,
            .num_heads = 2,
            .head_dim = 4,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
        .inputs = .{ q, k, v, bias },
        .num_inputs = 4,
    });

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = sdpa,
        .op = g.node(sdpa).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(decision.can_execute);
    const sdpa_operator = decision.operator_plan orelse return error.ExpectedOperatorPlan;
    try std.testing.expectEqual(operator_plan.Operator.attention_flash, sdpa_operator.operator());
}

fn expectSdpaBiasShapeRejected(bias_dims: []const i64) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 2, 3, 4 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 3, 4 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 3, 4 }));
    const bias = try b.parameter("bias", Shape.init(.f32, bias_dims));
    const sdpa = try g.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = 1,
            .seq_len = 3,
            .num_heads = 2,
            .head_dim = 4,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 3, 4 }),
        .inputs = .{ q, k, v, bias },
        .num_inputs = 4,
    });

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = sdpa,
        .op = g.node(sdpa).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(!decision.can_execute);
    try std.testing.expectEqual(CapabilityReason.unsupported_op, decision.reason);
}

test "seeded uploadable constants keep tiny device chains on metal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ 1, 2, 3, 4 }, Shape.init(.f32, &.{ 2, 2 }));
    const out = try b.neg(c);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllUploadableResidency(seeds, &g, .metal, 0);

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[c]].backend);
    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.wrong_storage));
}

test "metal partitions generated runtime placeholders and rope as resident" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    const runtime_input = try g.addNode(.{
        .op = .{ .fused_from_float32 = {} },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ null_node, null_node, null_node, null_node },
        .num_inputs = 0,
    });
    const runtime_rope = try g.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = 1,
            .head_dim = 4,
            .rope_dim = 4,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ runtime_input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const zero = try g.addNode(.{
        .op = .{ .fused_zero_tensor = .{ .rows = 1, .in_dim = 0, .out_dim = 4 } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ null_node, null_node, null_node, null_node },
        .num_inputs = 0,
    });
    const zero_rope = try g.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = 1,
            .head_dim = 4,
            .rope_dim = 4,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ zero, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(runtime_rope);
    try g.markOutput(zero_rope);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllUploadableResidency(seeds, &g, .metal, 0);

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{ .tensor_descs = seeds });
    defer plan.deinit();

    for (plan.partitions) |part| {
        try std.testing.expectEqual(contracts.BackendKind.metal, part.backend);
    }
}

test "metal decision rejects tiny compute as unprofitable and packed quant inputs as missing kernel" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const tiny = try b.gelu(x);
    try g.markOutput(tiny);

    var tiny_descs = try partition.buildTensorDescriptors(allocator, &g, null);
    defer allocator.free(tiny_descs);
    const tiny_decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = tiny,
        .op = g.node(tiny).op,
        .tensor_descs = tiny_descs,
    });
    try std.testing.expect(tiny_decision.can_execute);
    try std.testing.expect(!tiny_decision.should_execute);
    try std.testing.expectEqual(CapabilityReason.unprofitable_shape, tiny_decision.reason);

    tiny_descs[@intCast(x)] = contracts.TensorDesc.packedQuant(g.node(x).output_shape, .q4_0);
    const quant_decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = tiny,
        .op = g.node(tiny).op,
        .tensor_descs = tiny_descs,
    });
    try std.testing.expect(!quant_decision.can_execute);
    try std.testing.expectEqual(CapabilityReason.missing_quant_kernel, quant_decision.reason);
}

test "packed quant metadata views stay packed and reject generic primitive metal kernels" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("quant.weight", Shape.init(.f32, &.{ 2, 4 }));
    const reshaped = try b.reshape(x, Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(reshaped);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "quant.weight", .q4_0));

    const descs = try partition.buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);
    try std.testing.expect(descs[@intCast(x)].?.isPackedQuant());
    try std.testing.expect(descs[@intCast(reshaped)].?.isPackedQuant());
    try std.testing.expectEqual(@as(?quant_matmul.Format, .q4_0), descs[@intCast(reshaped)].?.quant_format);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = descs,
    });
    try std.testing.expect(!decision.can_execute);
    try std.testing.expectEqual(CapabilityReason.missing_quant_kernel, decision.reason);
}

test "device resident elementwise chain stays on metal despite tiny shapes" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const activated = try b.gelu(x);
    const out = try b.neg(activated);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedParameterResidency(seeds, &g, x, .metal, 0);

    var diagnostics = partition.CapabilityDiagnostics{};
    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[x]].backend);
    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[activated]].backend);
    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[out]].backend);
}

test "host to metal single small op is rejected as unprofitable" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var diagnostics = partition.CapabilityDiagnostics{};
    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{ .diagnostics = &diagnostics });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.native, plan.partitions[plan.node_assignment[x]].backend);
    try std.testing.expectEqual(contracts.BackendKind.native, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expect(diagnostics.count(.unprofitable_shape) >= 1);
}

test "seeded iq2_s quant parameter descriptors keep metal partitions on native fallback" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 64, 64 }));
    const w = try b.parameter("linear.weight", Shape.init(.f32, &.{ 64, 64 }));
    const out = try b.linearNoBias(x, w, 64, 64, 64);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "linear.weight", .iq2_s));

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &decideMetalEagerGraph },
        .{ .backend = .graph, .priority = 10, .decide = &partition.decideBlasAccelerate },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    const out_partition = plan.node_assignment[out];
    try std.testing.expectEqual(contracts.BackendKind.native, plan.partitions[out_partition].backend);
    try std.testing.expect(diagnostics.count(.missing_quant_kernel) >= 2);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.operatorCount(.mul_mm));
    try std.testing.expect(plan.operatorPlanForNode(out) == null);
}

test "seeded q8 quant linear descriptor can route supported matmul to metal" {
    try expectSeededQ8LinearRoutesToMetal(64, .mul_mm);
}

test "seeded q8 quant linear descriptor can route decode matvec to metal" {
    try expectSeededQ8LinearRoutesToMetal(1, .mul_mv);
}

test "seeded q8 quant linear descriptor can route small prompt matvec ext to metal" {
    try expectSeededQ8LinearRoutesToMetal(4, .mul_mv_ext);
}

test "seeded non-q8 quant linear descriptors expose supported metal kernels" {
    try expectSeededQuantLinearRoutesToMetal(.q4_0, 1, .mul_mv);
    try expectSeededQuantLinearRoutesToMetal(.q4_1, 1, .mul_mv);
    try expectSeededQuantLinearRoutesToMetal(.q5_0, 1, .mul_mv);
    try expectSeededQuantLinearRoutesToMetal(.q5_1, 4, .mul_mv_ext);
    try expectSeededQuantLinearRoutesToMetal(.q8_1, 9, .mul_mm);
    try expectSeededQuantLinearRoutesToMetalDims(.q1_0, 1, 128, 64, .mul_mv);
    try expectSeededQuantLinearRoutesToMetalDims(.i2_s, 4, 128, 64, .mul_mv_ext);
    try expectSeededQuantLinearRoutesToMetal(.i8_s, 9, .mul_mm);
    try expectSeededQuantLinearRoutesToMetalDims(.q2_k, 1, 256, 64, .mul_mv);
    try expectSeededQuantLinearRoutesToMetalDims(.q3_k, 4, 256, 64, .mul_mv_ext);
    try expectSeededQuantLinearRoutesToMetalDims(.q4_k, 9, 256, 64, .mul_mm);
    try expectSeededQuantLinearRoutesToMetalDims(.q5_k, 1, 256, 64, .mul_mv);
    try expectSeededQuantLinearRoutesToMetalDims(.q6_k, 4, 256, 64, .mul_mv_ext);
    try expectSeededQuantLinearRoutesToMetalDims(.q8_k, 9, 256, 64, .mul_mm);
    try expectSeededQuantLinearRoutesToMetal(.iq4_nl, 1, .mul_mv);
    try expectSeededQuantLinearRoutesToMetalDims(.iq4_xs, 4, 256, 64, .mul_mv_ext);
    try expectSeededQuantLinearRoutesToMetal(.mxfp4, 9, .mul_mm);
    try expectSeededQuantLinearRoutesToMetal(.q4_0, 4, .mul_mv_ext);
    try expectSeededQuantLinearRoutesToMetal(.q4_1, 9, .mul_mm);
    try expectSeededQuantLinearRoutesToMetalDims(.q5_k, 9, 256, 64, .mul_mm);
}

test "seeded quant qkv projections keep activation transposes on metal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const rows: usize = 1;
    const dim: usize = 32;
    const x = try b.parameter("x", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(dim)) }));
    const q_weight = try b.parameter("q.weight", Shape.init(.f32, &.{ @as(i64, @intCast(dim)), @as(i64, @intCast(dim)) }));
    const k_weight = try b.parameter("k.weight", Shape.init(.f32, &.{ @as(i64, @intCast(dim)), @as(i64, @intCast(dim)) }));
    const v_weight = try b.parameter("v.weight", Shape.init(.f32, &.{ @as(i64, @intCast(dim)), @as(i64, @intCast(dim)) }));
    const out_weight = try b.parameter("out.weight", Shape.init(.f32, &.{ @as(i64, @intCast(dim)), @as(i64, @intCast(dim)) }));

    const q = try b.linearNoBias(x, q_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const k = try b.linearNoBias(x, k_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const v = try b.linearNoBias(x, v_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const q_3d = try b.reshape(q, Shape.init(.f32, &.{ 1, @as(i64, @intCast(rows)), @as(i64, @intCast(dim)) }));
    const q_t = try b.transpose(q_3d, &.{ 0, 2, 1 });
    const q_tt = try b.transpose(q_t, &.{ 0, 2, 1 });
    const q_rope = try g.addNode(Node{
        .op = .{ .fused_rope = .{
            .seq_len = rows,
            .head_dim = dim,
            .rope_dim = dim,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = g.node(q_tt).output_shape,
        .inputs = .{ q_tt, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_back = try b.reshape(q_rope, Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(dim)) }));
    const logits = try b.add(q_back, k);
    const probs = try b.softmax(logits);
    const mixed = try b.add(probs, v);
    const out = try b.linearNoBias(mixed, out_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedParameterResidency(seeds, &g, x, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "q.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "k.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "v.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "out.weight", .q8_0));

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    const metal_nodes = [_]NodeId{ q, k, v, q_3d, q_t, q_tt, q_rope, q_back, logits, probs, mixed, out };
    for (metal_nodes) |node_id| {
        try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[node_id]].backend);
    }
    try std.testing.expectEqual(operator_plan.Operator.mul_mv, plan.operatorPlanForNode(q).?.operator());
    try std.testing.expectEqual(operator_plan.Operator.mul_mv, plan.operatorPlanForNode(k).?.operator());
    try std.testing.expectEqual(operator_plan.Operator.mul_mv, plan.operatorPlanForNode(v).?.operator());
    try std.testing.expectEqual(operator_plan.Operator.mul_mv, plan.operatorPlanForNode(out).?.operator());
    try std.testing.expect(diagnostics.operatorCount(.mul_mv) >= 4);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

test "gemma4 graph planner keeps real qkv projection layout on metal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const cfg = gemma_graph.Config{
        .family = .gemma,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 8,
        .intermediate_size = 64,
        .vocab_size = 96,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    };

    const input_ids = try b.parameter("input_ids", Shape.init(.f32, &.{ 1, 4 }));
    const rope_cos = try b.parameter("rope_cos", Shape.init(.f32, &.{ 4, 8 }));
    const rope_sin = try b.parameter("rope_sin", Shape.init(.f32, &.{ 4, 8 }));
    const graph = try gemma_graph.buildForwardGraph(&b, cfg, 1, 4, .{
        .input_ids = input_ids,
        .rope_cos = rope_cos,
        .rope_sin = rope_sin,
    });
    try g.markOutput(graph.output_node);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.self_attn.q_proj.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.self_attn.k_proj.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.self_attn.v_proj.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.self_attn.o_proj.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.mlp.gate_proj.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.mlp.up_proj.weight", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "model.layers.0.mlp.down_proj.weight", .q8_0));

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    const reachable = try allocReachableFromOutputs(allocator, &g);
    defer allocator.free(reachable);

    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.self_attn.q_proj.weight", .mul_mv_ext);
    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.self_attn.k_proj.weight", .mul_mv_ext);
    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.self_attn.v_proj.weight", .mul_mv_ext);
    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.self_attn.o_proj.weight", .mul_mv_ext);
    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.mlp.gate_proj.weight", .mul_mv_ext);
    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.mlp.up_proj.weight", .mul_mv_ext);
    try expectReachableNamedWeightLinearConsumersOnMetal(&g, &plan, reachable, "model.layers.0.mlp.down_proj.weight", .mul_mv_ext);

    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .dot_general, 2);
    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .fused_softmax, 1);
    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .transpose, 4);
    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .fused_rms_norm, 5);
    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .fused_gelu, 1);
    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .add, 3);
    try expectReachableNodesWithOpTagOnMetal(&g, &plan, reachable, .mul, 2);

    try std.testing.expect(diagnostics.operatorCount(.mul_mv_ext) >= 7);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

test "seeded quant dot_general descriptors expose supported metal kernels" {
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q8_0, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q4_0, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q4_1, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q5_0, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q5_1, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q8_1, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q1_0, 128);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.i2_s, 128);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.i8_s, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q2_k, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q3_k, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q4_k, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q5_k, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q6_k, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.q8_k, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.iq4_nl, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.iq4_xs, 256);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.mxfp4, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.nvfp4, 64);
    try expectAllSeededQuantDotGeneralRoutesToMetal(.iq2_xs, 256);
    try expectSeededQuantDotGeneralRoutesToMetalDims(.tl1, 1, 1536, 1536, .mul_mv);
    try expectSeededQuantDotGeneralRoutesToMetalDims(.tl1, 4, 1536, 1536, .mul_mv_ext);
    try expectSeededQuantDotGeneralRoutesToMetalDims(.tl1, 9, 1536, 1536, .mul_mm);
    try expectSeededQuantDotGeneralRoutesToMetalDims(.tl2, 1, 1536, 1536, .mul_mv);
    try expectSeededQuantDotGeneralRoutesToMetalDims(.tl2, 4, 1536, 1536, .mul_mv_ext);
    try expectSeededQuantDotGeneralRoutesToMetalDims(.tl2, 9, 1536, 1536, .mul_mm);
}

test "quant concat feeding linear selects metal quant operator plan" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 64 }));
    const w_a = try b.parameter("w_a", Shape.init(.f32, &.{ 32, 64 }));
    const w_b = try b.parameter("w_b", Shape.init(.f32, &.{ 32, 64 }));
    const w = try b.concat(w_a, w_b, 0);
    const out = try b.linearNoBias(x, w, 4, 64, 64);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedParameterResidency(seeds, &g, x, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "w_a", .q8_0));
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "w_b", .q8_0));

    const descs = try partition.buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);
    try std.testing.expect(descs[@intCast(w)].?.isPackedQuant());

    const concat_decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = w,
        .op = g.node(w).op,
        .tensor_descs = descs,
    });
    try std.testing.expect(concat_decision.can_execute);
    try std.testing.expect(concat_decision.should_execute);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = descs,
    });
    try std.testing.expect(decision.can_execute);
    try std.testing.expect(decision.should_execute);
    try std.testing.expectEqual(operator_plan.Operator.mul_mv_ext, decision.operator_plan.?.operator());
}

test "quant embedding lookup stays on metal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const weight = try b.parameter("embed.weight", Shape.init(.f32, &.{ 128, 64 }));
    const ids = try b.parameter("ids", Shape.init(.i64, &.{4}));
    const out = try b.embeddingLookup(weight, ids, 4, 64);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "embed.weight", .q8_0));
    try partition.seedParameterResidency(seeds, &g, ids, .metal, 0);

    const descs = try partition.buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = descs,
    });
    try std.testing.expect(decision.can_execute);
    try std.testing.expect(decision.should_execute);
}

test "seeded unsupported quant dot_general descriptors reject metal" {
    try expectSeededQuantDotGeneralRejects(.iq2_s, .missing_quant_kernel);
    try expectSeededQuantDotGeneralRejects(.iq3_s, .missing_quant_kernel);
    try expectSeededQuantDotGeneralRejects(.tq1_0, .missing_quant_kernel);
}

test "seeded quant take rows descriptor routes get_rows to metal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);
    const source_rows: usize = 16;
    const rows: usize = 4;
    const dim: usize = 64;

    const table = try b.parameter("expert.table", Shape.init(.f32, &.{ @as(i64, @intCast(source_rows)), @as(i64, @intCast(dim)) }));
    const out = try g.addNode(Node{
        .op = .{ .fused_take_rows = .{
            .rows = @intCast(rows),
            .dim = @intCast(dim),
        } },
        .output_shape = Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(dim)) }),
        .inputs = .{ table, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "expert.table", .q5_k));

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(decision.can_execute);
    try std.testing.expect(decision.should_execute);
    try std.testing.expectEqual(operator_plan.Operator.get_rows, decision.operator_plan.?.operator());

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expectEqual(@as(usize, 1), diagnostics.operatorCount(.get_rows));
    try std.testing.expectEqual(operator_plan.Operator.get_rows, plan.operatorPlanForNode(out).?.operator());
}

test "seeded resident backend descriptors keep tiny device chains on metal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedParameterResidency(seeds, &g, x, .metal, 0);

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{ .tensor_descs = seeds });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[x]].backend);
    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[out]].backend);
}

fn expectNamedWeightLinearConsumersOnMetal(
    graph: *const Graph,
    plan: *const partition.PartitionPlan,
    weight_name: []const u8,
    expected_operator: operator_plan.Operator,
) !void {
    var found = false;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_linear, .fused_linear_no_bias => {},
            else => continue,
        }
        const inputs = node.getInputs();
        if (inputs.len < 2 or inputs[1] == null_node) continue;
        const weight = graph.node(inputs[1]);
        if (std.meta.activeTag(weight.op) != .parameter) continue;
        if (!std.mem.eql(u8, graph.parameterName(weight), weight_name)) continue;

        found = true;
        try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[plan.node_assignment[node_id]].backend);
        try std.testing.expectEqual(expected_operator, plan.operatorPlanForNode(node_id).?.operator());
    }
    try std.testing.expect(found);
}

fn expectReachableNamedWeightLinearConsumersOnMetal(
    graph: *const Graph,
    plan: *const partition.PartitionPlan,
    reachable: []const bool,
    weight_name: []const u8,
    expected_operator: operator_plan.Operator,
) !void {
    var found = false;
    for (0..graph.nodeCount()) |idx| {
        if (!reachable[idx]) continue;
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_linear, .fused_linear_no_bias => {},
            else => continue,
        }
        const inputs = node.getInputs();
        if (inputs.len < 2 or inputs[1] == null_node) continue;
        const weight = graph.node(inputs[1]);
        if (std.meta.activeTag(weight.op) != .parameter) continue;
        if (!std.mem.eql(u8, graph.parameterName(weight), weight_name)) continue;

        found = true;
        try expectNodePlannedOnMetal(graph, plan, node_id);
        try std.testing.expectEqual(expected_operator, plan.operatorPlanForNode(node_id).?.operator());
    }
    try std.testing.expect(found);
}

fn expectReachableNodesWithOpTagOnMetal(
    graph: *const Graph,
    plan: *const partition.PartitionPlan,
    reachable: []const bool,
    tag: std.meta.Tag(OpCode),
    min_count: usize,
) !void {
    var count: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        if (!reachable[idx]) continue;
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        if (std.meta.activeTag(node.op) != tag) continue;
        count += 1;
        try expectNodePlannedOnMetal(graph, plan, node_id);
    }
    try std.testing.expect(count >= min_count);
}

fn expectNodePlannedOnMetal(
    graph: *const Graph,
    plan: *const partition.PartitionPlan,
    node_id: NodeId,
) !void {
    const partition_index = plan.node_assignment[node_id];
    const backend = plan.partitions[partition_index].backend;
    if (backend != .metal) {
        const node = graph.node(node_id);
        std.debug.print("expected node {d} op={s} shape={any} on metal, got {s}\n", .{
            node_id,
            @tagName(std.meta.activeTag(node.op)),
            node.output_shape,
            @tagName(backend),
        });
    }
    try std.testing.expectEqual(contracts.BackendKind.metal, backend);
}

fn allocReachableFromOutputs(allocator: std.mem.Allocator, graph: *const Graph) ![]bool {
    const reachable = try allocator.alloc(bool, graph.nodeCount());
    @memset(reachable, false);
    for (graph.outputs.items) |output| {
        markReachable(graph, reachable, output);
    }
    return reachable;
}

fn markReachable(graph: *const Graph, reachable: []bool, node_id: NodeId) void {
    if (node_id == null_node) return;
    const idx: usize = @intCast(node_id);
    if (idx >= reachable.len or reachable[idx]) return;
    reachable[idx] = true;
    const node = graph.node(node_id);
    for (node.getInputs()) |input_id| {
        markReachable(graph, reachable, input_id);
    }
}

fn expectSeededQ8LinearRoutesToMetal(rows: usize, expected_operator: operator_plan.Operator) !void {
    try expectSeededQuantLinearRoutesToMetal(.q8_0, rows, expected_operator);
}

fn expectSeededQuantLinearRoutesToMetal(format: quant_matmul.Format, rows: usize, expected_operator: operator_plan.Operator) !void {
    try expectSeededQuantLinearRoutesToMetalDims(format, rows, 64, 64, expected_operator);
}

fn expectSeededQuantLinearRoutesToMetalDims(
    format: quant_matmul.Format,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    expected_operator: operator_plan.Operator,
) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(in_dim)) }));
    const w = try b.parameter("linear.weight", Shape.init(.f32, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) }));
    const out = try b.linearNoBias(x, w, @intCast(rows), @intCast(in_dim), @intCast(out_dim));
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "linear.weight", format));

    const out_decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(out_decision.can_execute);
    try std.testing.expect(out_decision.should_execute);
    try std.testing.expectEqual(expected_operator, out_decision.operator_plan.?.operator());

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    const out_partition = plan.node_assignment[out];
    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[out_partition].backend);
    try std.testing.expect(diagnostics.operatorCount(expected_operator) >= 1);
    try std.testing.expectEqual(expected_operator, plan.operatorPlanForNode(out).?.operator());
}

fn expectAllSeededQuantDotGeneralRoutesToMetal(format: quant_matmul.Format, in_dim: usize) !void {
    try expectSeededQuantDotGeneralRoutesToMetalDims(format, 1, in_dim, 64, .mul_mv);
    try expectSeededQuantDotGeneralRoutesToMetalDims(format, 4, in_dim, 64, .mul_mv_ext);
    try expectSeededQuantDotGeneralRoutesToMetalDims(format, 9, in_dim, 64, .mul_mm);
}

fn expectSeededQuantDotGeneralRoutesToMetalDims(
    format: quant_matmul.Format,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    expected_operator: operator_plan.Operator,
) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(in_dim)) }));
    const w = try b.parameter("dot.weight", Shape.init(.f32, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) }));
    const out = try g.addNode(Node{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(out_dim)) }),
        .inputs = .{ x, w, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "dot.weight", format));

    const out_decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(out_decision.can_execute);
    try std.testing.expect(out_decision.should_execute);
    try std.testing.expectEqual(expected_operator, out_decision.operator_plan.?.operator());

    const caps = [_]partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    const out_partition = plan.node_assignment[out];
    try std.testing.expectEqual(contracts.BackendKind.metal, plan.partitions[out_partition].backend);
    try std.testing.expectEqual(@as(usize, 1), diagnostics.operatorCount(expected_operator));
    try std.testing.expectEqual(expected_operator, plan.operatorPlanForNode(out).?.operator());
}

fn expectSeededQuantDotGeneralRejects(format: quant_matmul.Format, expected_reason: CapabilityReason) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const rows: usize = 1;
    const in_dim: usize = 256;
    const out_dim: usize = 64;
    const x = try b.parameter("x", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(in_dim)) }));
    const w = try b.parameter("dot.weight", Shape.init(.f32, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) }));
    const out = try g.addNode(Node{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(out_dim)) }),
        .inputs = .{ x, w, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    const seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition.seedAllParameterResidency(seeds, &g, .metal, 0);
    try std.testing.expect(try partition.seedParameterQuantFormatByName(seeds, &g, "dot.weight", format));

    const decision = decideMetalEagerGraph(.{
        .graph = &g,
        .node_id = out,
        .op = g.node(out).op,
        .tensor_descs = seeds,
    });
    try std.testing.expect(!decision.can_execute);
    try std.testing.expectEqual(expected_reason, decision.reason);
    try std.testing.expect(decision.operator_plan == null);
}
