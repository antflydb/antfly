// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! PJRT/HLO partition compiler.
//!
//! Translates a graph IR partition into an HLO program, extracts weights
//! from the compute backend as float32, and produces serialized
//! HloModuleProto bytes ready for PJRT compilation.
//!
//! Supported ops (matching partition.supportsLinearNormActivation +
//! embedding + concat):
//!   fused_linear, fused_linear_no_bias, fused_linear_no_bias_pair,
//!   fused_rms_norm, fused_layer_norm,
//!   fused_gelu, fused_relu, fused_silu, fused_quick_gelu, fused_sigmoid, fused_tanh_act,
//!   fused_elem_add, fused_elem_multiply, fused_add_mul_scalar,
//!   fused_from_float32, fused_to_float32, fused_rope,
//!   fused_gqa_causal_attention (static batch-1 full-recompute),
//!   fused_embedding_lookup, fused_concat

const std = @import("std");
const ml = @import("ml");
const pjrt = @import("pjrt");
const hlo = pjrt.hlo;
const Allocator = std.mem.Allocator;

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.OpCode;

const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;

const partition_mod = @import("partition.zig");
const Partition = partition_mod.Partition;
const partition_export = @import("partition_export.zig");

// ── Public types ────────────────────────────────────────────────────

/// Result of compiling a graph partition to HLO.
pub const CompileResult = struct {
    /// Serialized HloModuleProto bytes.
    hlo_bytes: []u8,
    /// Full input bindings, ordered to match HLO parameters.
    input_bindings: []InputBinding,
    /// External input node IDs, ordered to match HLO parameters.
    input_node_ids: []NodeId,
    /// Output node IDs, ordered to match HLO outputs.
    output_node_ids: []NodeId,
    /// Actual HLO parameter shapes, ordered to match input_bindings.
    input_shapes: [][]i64,
    /// Actual HLO output shapes, ordered to match output_node_ids.
    output_shapes: [][]i64,
    allocator: Allocator,

    pub fn deinit(self: *CompileResult) void {
        self.allocator.free(self.hlo_bytes);
        self.allocator.free(self.input_bindings);
        self.allocator.free(self.input_node_ids);
        self.allocator.free(self.output_node_ids);
        freeShapeSlices(self.allocator, self.input_shapes);
        freeShapeSlices(self.allocator, self.output_shapes);
    }
};

pub const InputBinding = union(enum) {
    graph_node: NodeId,
    embedding_ids: NodeId,
    semantic_past_graph_node: NodeId,
};

pub const CompileOptions = struct {
    /// Emit graph parameter nodes as HLO parameters instead of embedding their
    /// current f32 values as HLO constants. This is host-assisted and mainly
    /// useful for validation or plugins that cannot compile huge constant HLO.
    parameter_inputs: bool = false,
    /// Request/state parameters remain dynamic; immutable model weights can
    /// still be embedded once in the compiled executable.
    mutable_parameters: ?[]const bool = null,
    /// Offline whole-model phase artifacts need semantic KV cache values to
    /// cross the ModelRuntime prefill/decode boundary. Inline partitions keep
    /// the old node-oriented ABI.
    semantic_kv_bindings: bool = false,
    /// Decode phase artifacts consume retained past K/V buffers as explicit
    /// semantic inputs, then return updated present K/V buffers.
    semantic_kv_inputs: bool = false,
};

// ── Compiler ────────────────────────────────────────────────────────

/// Compile a graph partition into serialized HloModuleProto bytes.
///
/// Walks the partition's nodes in topological order, extracts weights from
/// the compute backend as float32, maps fused ops to HLO operations, and
/// serializes the result. External inputs become HLO parameters; nodes
/// consumed by downstream partitions or marked as graph outputs become
/// the computation's outputs.
pub fn compilePartition(
    allocator: Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
) !CompileResult {
    return compilePartitionWithOptions(allocator, graph, part, cb, .{});
}

pub fn compilePartitionWithOptions(
    allocator: Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
    options: CompileOptions,
) !CompileResult {
    const count = graph.nodeCount();

    // Map graph NodeId → HLO instruction Id (dense array, null = unmapped).
    const node_map = try allocator.alloc(?hlo.Id, count);
    defer allocator.free(node_map);
    @memset(node_map, null);

    // Fast partition membership lookup.
    var part_set = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer part_set.deinit(allocator);
    for (part.node_ids) |nid| try part_set.put(allocator, nid, {});

    // ── 0. Build add-reducer sub-computation for reductions ─────
    //
    // This is a tiny computation: add(lhs, rhs) used as the combiner
    // for sum-reduce ops in RMS norm, layer norm, etc.
    // We assign it a fixed ID that won't collide with the main computation.
    // The main computation starts IDs at 1, and the reducer uses ID
    // space starting at a high offset.
    const reducer_comp_id: hlo.Id = 1_000_000;
    var add_reducer = hlo.Builder.init(allocator, "add_reducer");
    defer add_reducer.deinit();
    add_reducer.next_id = reducer_comp_id + 1; // instructions start after comp ID
    add_reducer.base_id = reducer_comp_id + 1;
    const r_lhs = try add_reducer.parameter(0, hlo.Shape.scalar(.f32), "lhs");
    const r_rhs = try add_reducer.parameter(1, hlo.Shape.scalar(.f32), "rhs");
    _ = try add_reducer.add(r_lhs, r_rhs);
    var reducer_comp = add_reducer.build();
    reducer_comp.id = reducer_comp_id;

    const max_reducer_comp_id: hlo.Id = 2_000_000;
    var max_reducer = hlo.Builder.init(allocator, "max_reducer");
    defer max_reducer.deinit();
    max_reducer.next_id = max_reducer_comp_id + 1;
    max_reducer.base_id = max_reducer_comp_id + 1;
    const mr_lhs = try max_reducer.parameter(0, hlo.Shape.scalar(.f32), "lhs");
    const mr_rhs = try max_reducer.parameter(1, hlo.Shape.scalar(.f32), "rhs");
    _ = try max_reducer.maximum(mr_lhs, mr_rhs);
    var max_reducer_comp = max_reducer.build();
    max_reducer_comp.id = max_reducer_comp_id;

    var b = hlo.Builder.init(allocator, "main");
    defer b.deinit();

    var next_param: u32 = 0;

    // ── 1. Create HLO parameters for external inputs ────────────

    var input_bindings_list = std.ArrayListUnmanaged(InputBinding).empty;
    errdefer input_bindings_list.deinit(allocator);
    var input_node_ids_list = std.ArrayListUnmanaged(NodeId).empty;
    errdefer input_node_ids_list.deinit(allocator);
    var semantic_past_inputs = std.AutoHashMapUnmanaged(NodeId, hlo.Id).empty;
    defer semantic_past_inputs.deinit(allocator);

    for (part.external_inputs) |ext_in| {
        const n = graph.node(ext_in.node_id);
        if (try emitEmbeddingIdsInputBinding(
            allocator,
            &b,
            graph,
            ext_in.node_id,
            n,
            &part_set,
            &input_bindings_list,
            &next_param,
        )) |input_id| {
            node_map[@intCast(ext_in.node_id)] = input_id;
            continue;
        }
        const shape = graphShapeToHlo(n);
        const name_buf = try std.fmt.allocPrint(allocator, "input_{d}", .{next_param});
        defer allocator.free(name_buf);
        const input_id = try b.parameter(next_param, shape, name_buf);
        node_map[@intCast(ext_in.node_id)] = input_id;
        next_param += 1;
        try input_bindings_list.append(allocator, .{ .graph_node = ext_in.node_id });
        try input_node_ids_list.append(allocator, ext_in.node_id);
    }

    if (options.semantic_kv_bindings) {
        for (part.node_ids) |node_id| {
            const n = graph.node(node_id);
            switch (n.op) {
                .fused_gqa_causal_attention => |attrs| {
                    if (!options.semantic_kv_inputs and !attrs.skip_kv_write) continue;
                    const ins = n.getInputs();
                    if (ins.len < 3) return error.UnsupportedShape;
                    try appendSemanticPastInput(
                        allocator,
                        &b,
                        graph,
                        ins[1],
                        &semantic_past_inputs,
                        &input_bindings_list,
                        &input_node_ids_list,
                        &next_param,
                        attrs,
                    );
                    try appendSemanticPastInput(
                        allocator,
                        &b,
                        graph,
                        ins[2],
                        &semantic_past_inputs,
                        &input_bindings_list,
                        &input_node_ids_list,
                        &next_param,
                        attrs,
                    );
                },
                else => {},
            }
        }
    }

    // ── 2. Walk partition nodes, emit HLO ops ───────────────────

    // Secondary results belong to their producer. Topological scheduling may
    // interleave independent pairs before visiting either secondary consumer.
    var pair_seconds: std.AutoHashMapUnmanaged(NodeId, hlo.Id) = .empty;
    defer pair_seconds.deinit(allocator);
    // Track whether we need the reducer computation.
    var needs_reducer = false;
    var needs_max_reducer = false;

    for (part.node_ids) |node_id| {
        const nid: usize = @intCast(node_id);
        const n = graph.node(node_id);
        const ins = n.getInputs();
        if (try emitPrimitiveNode(allocator, &b, n, node_map, nid, &reducer_comp, &max_reducer_comp, &needs_reducer, &needs_max_reducer)) continue;

        switch (n.op) {
            .parameter => {
                const mutable = if (options.mutable_parameters) |flags| blk: {
                    if (flags.len != graph.nodeCount()) return error.InvalidMutableParameterTable;
                    break :blk flags[node_id];
                } else false;
                if (options.parameter_inputs or mutable) {
                    const shape = graphShapeToHlo(n);
                    const name_buf = try std.fmt.allocPrint(allocator, "input_{d}", .{next_param});
                    defer allocator.free(name_buf);
                    const input_id = try b.parameter(next_param, shape, name_buf);
                    node_map[nid] = input_id;
                    next_param += 1;
                    try input_bindings_list.append(allocator, .{ .graph_node = node_id });
                    try input_node_ids_list.append(allocator, node_id);
                    continue;
                }
                const wname = graph.parameterName(n);
                const ct = cb.getWeight(wname) catch |err| {
                    std.log.err(
                        "PJRT HLO compile missing parameter weight: node={d} name={s} shape={any} err={s}",
                        .{ node_id, wname, n.output_shape.dims[0..n.output_shape.rank()], @errorName(err) },
                    );
                    return err;
                };
                defer cb.free(ct);
                const f32_data = try cb.toFloat32(ct, allocator);
                defer allocator.free(f32_data);

                const shape = graphShapeToHlo(n);
                node_map[nid] = try b.constantF32(shape, f32_data);
            },

            .constant => {
                node_map[nid] = try emitGraphConstant(&b, graph, n);
            },

            .fused_from_float32, .fused_to_float32 => {
                if (try emitEmbeddingIdsInputBinding(
                    allocator,
                    &b,
                    graph,
                    node_id,
                    n,
                    &part_set,
                    &input_bindings_list,
                    &next_param,
                )) |input_id| {
                    node_map[nid] = input_id;
                } else if (n.op == .fused_to_float32 and ins.len > 0 and ins[0] != null_node and graph.node(ins[0]).op == .fused_linear_no_bias_pair) {
                    node_map[nid] = pair_seconds.get(ins[0]) orelse return error.MissingPairedGraphOutput;
                } else if (ins.len > 0 and ins[0] != null_node) {
                    node_map[nid] = node_map[@intCast(ins[0])];
                }
            },

            .fused_segment_attention => |attrs| {
                node_map[nid] = try emitSegmentAttention(allocator, &b, n, try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1]), try getHloId(node_map, ins[2]), try getHloId(node_map, ins[3]), attrs, &reducer_comp, &max_reducer_comp);
                needs_reducer = true;
                needs_max_reducer = true;
            },

            .fused_linear => {
                const x = try getHloId(node_map, ins[0]);
                const w = try getHloId(node_map, ins[1]);
                const bias = try getHloId(node_map, ins[2]);
                const wt = try transposeLastTwo(&b, w);
                const mm = try b.matmul(x, wt);
                node_map[nid] = try b.add(mm, bias);
            },

            .fused_linear_no_bias => {
                const x = try getHloId(node_map, ins[0]);
                const w = try getHloId(node_map, ins[1]);
                const wt = try transposeLastTwo(&b, w);
                node_map[nid] = try b.matmul(x, wt);
            },

            .fused_linear_no_bias_pair => {
                const x = try getHloId(node_map, ins[0]);
                const w1 = try getHloId(node_map, ins[1]);
                const w2 = try getHloId(node_map, ins[2]);
                const wt1 = try transposeLastTwo(&b, w1);
                const wt2 = try transposeLastTwo(&b, w2);
                node_map[nid] = try b.matmul(x, wt1);
                try pair_seconds.put(allocator, node_id, try b.matmul(x, wt2));
            },

            .fused_rms_norm => |attrs| {
                const x = try getHloId(node_map, ins[0]);
                const gamma = try getHloId(node_map, ins[1]);
                node_map[nid] = try emitRmsNorm(&b, x, gamma, attrs.eps, &reducer_comp);
                needs_reducer = true;
            },

            .fused_layer_norm => |attrs| {
                const x = try getHloId(node_map, ins[0]);
                const gamma = try getHloId(node_map, ins[1]);
                const beta = try getHloId(node_map, ins[2]);
                node_map[nid] = try emitLayerNorm(&b, x, gamma, beta, attrs.eps, &reducer_comp);
                needs_reducer = true;
            },

            .fused_conv1d => |attrs| {
                const input = try b.reshape(try getHloId(node_map, ins[0]), hlo.Shape.init(.f32, &.{ attrs.batch, attrs.in_channels, attrs.time_steps }));
                const weight = try b.reshape(try getHloId(node_map, ins[1]), hlo.Shape.init(.f32, &.{ attrs.out_channels, attrs.in_channels, attrs.kernel_size }));
                var conv = hlo.Convolution{ .spatial_rank = 1 };
                conv.strides[0] = attrs.stride;
                conv.padding_low[0] = attrs.padding;
                conv.padding_high[0] = attrs.padding;
                const shape = graphShapeToHlo(n);
                const result = try b.convolution(input, weight, shape, conv);
                node_map[nid] = try addChannelBias(&b, result, try getHloId(node_map, ins[2]), shape);
            },
            .fused_conv2d => |attrs| {
                if (attrs.in_channels == 0 or attrs.groups == 0 or attrs.in_channels % attrs.groups != 0 or attrs.out_channels % attrs.groups != 0) return error.UnsupportedConvolutionLayout;
                const input = try b.reshape(try getHloId(node_map, ins[0]), hlo.Shape.init(.f32, &.{ attrs.batch, attrs.in_channels, attrs.height, attrs.width }));
                const weight = try b.reshape(try getHloId(node_map, ins[1]), hlo.Shape.init(.f32, &.{ attrs.out_channels, @divExact(attrs.in_channels, attrs.groups), attrs.kernel_h, attrs.kernel_w }));
                var conv = hlo.Convolution{ .spatial_rank = 2, .groups = attrs.groups };
                conv.strides[0..2].* = .{ attrs.stride_h, attrs.stride_w };
                conv.padding_low[0..2].* = .{ attrs.padding_h, attrs.padding_w };
                conv.padding_high[0..2].* = .{ attrs.padding_h, attrs.padding_w };
                const shape = graphShapeToHlo(n);
                const result = try b.convolution(input, weight, shape, conv);
                node_map[nid] = try addChannelBias(&b, result, try getHloId(node_map, ins[2]), shape);
            },

            .fused_gelu_exact => {
                const x = try getHloId(node_map, ins[0]);
                const half_x = try b.multiply(x, try b.constantScalarF32(0.5));
                const scaled = try b.multiply(x, try b.constantScalarF32(0.7071067811865476));
                node_map[nid] = try b.add(half_x, try b.multiply(half_x, try b.erf(scaled)));
            },

            .fused_gelu => {
                node_map[nid] = try b.gelu(try getHloId(node_map, ins[0]));
            },

            .fused_relu => {
                node_map[nid] = try b.relu(try getHloId(node_map, ins[0]));
            },

            .fused_silu => {
                node_map[nid] = try b.silu(try getHloId(node_map, ins[0]));
            },

            .fused_quick_gelu => {
                const x = try getHloId(node_map, ins[0]);
                const scale = try b.constantScalarF32(1.702);
                const scaled = try b.multiply(x, scale);
                const sig = try b.logistic(scaled);
                node_map[nid] = try b.multiply(x, sig);
            },

            .fused_sigmoid => {
                node_map[nid] = try b.logistic(try getHloId(node_map, ins[0]));
            },

            .fused_tanh_act => {
                node_map[nid] = try b.tanh(try getHloId(node_map, ins[0]));
            },

            .fused_elem_add => {
                const x = try getHloId(node_map, ins[0]);
                const y = try getHloId(node_map, ins[1]);
                node_map[nid] = try b.add(x, y);
            },

            .fused_elem_multiply => {
                const x = try getHloId(node_map, ins[0]);
                const y = try getHloId(node_map, ins[1]);
                node_map[nid] = try b.multiply(x, y);
            },

            .fused_add_mul_scalar => {
                const x = try getHloId(node_map, ins[0]);
                const y = try getHloId(node_map, ins[1]);
                const scalar = try getHloId(node_map, ins[2]);
                const sum = try b.add(x, y);
                node_map[nid] = try b.multiply(sum, scalar);
            },

            .fused_embedding_lookup => {
                const table = try getHloId(node_map, ins[0]);
                const ids_ct = try getHloId(node_map, ins[1]);
                const result_shape = graphShapeToHlo(n);
                node_map[nid] = try b.gather(table, ids_ct, result_shape);
            },

            .fused_rope => |attrs| {
                node_map[nid] = try emitStaticRope(
                    allocator,
                    &b,
                    n,
                    try getHloId(node_map, ins[0]),
                    attrs,
                );
            },

            .fused_gqa_causal_attention => |attrs| {
                const q = try getHloId(node_map, ins[0]);
                const k = try getHloId(node_map, ins[1]);
                const v = try getHloId(node_map, ins[2]);
                const bias = if (n.num_inputs > 3 and ins[3] != null_node) try getHloId(node_map, ins[3]) else null;
                const past_k = if (options.semantic_kv_bindings) semantic_past_inputs.get(ins[1]) else null;
                const past_v = if (options.semantic_kv_bindings) semantic_past_inputs.get(ins[2]) else null;
                const emitted = try emitStaticGqaAttention(
                    allocator,
                    &b,
                    n,
                    q,
                    k,
                    v,
                    bias,
                    attrs,
                    &reducer_comp,
                    &max_reducer_comp,
                    past_k,
                    past_v,
                );
                node_map[nid] = emitted.output;
                if (emitted.present_k) |present_k| node_map[@intCast(ins[1])] = present_k;
                if (emitted.present_v) |present_v| node_map[@intCast(ins[2])] = present_v;
                needs_reducer = true;
                needs_max_reducer = true;
            },

            .fused_concat => {
                const x = try getHloId(node_map, ins[0]);
                const y = try getHloId(node_map, ins[1]);
                const rank = graph.node(ins[0]).output_shape.rank();
                const result_shape = graphShapeToHlo(n);
                node_map[nid] = try b.concatenate(&.{ x, y }, @as(i64, @intCast(rank)) - 1, result_shape);
            },

            else => {
                std.log.err(
                    "PJRT HLO compile unsupported op: node={d} op={s} shape={any}",
                    .{ node_id, @tagName(n.op), n.output_shape.dims[0..n.output_shape.rank()] },
                );
                return error.UnsupportedOp;
            },
        }
    }

    // ── 3. Determine partition outputs ──────────────────────────

    const output_node_ids = try computeOutputsWithOptions(allocator, graph, &part_set, part.node_ids, options);
    errdefer allocator.free(output_node_ids);

    // Make the output(s) the root of the computation.
    if (output_node_ids.len == 0) {
        return error.NoOutputs;
    } else if (output_node_ids.len == 1) {
        // Single output: ensure it is the last (root) instruction.
        const out_hlo_id = node_map[@intCast(output_node_ids[0])] orelse return error.MissingOutput;
        const last_id = b.instructions.items[b.instructions.items.len - 1].id;
        if (out_hlo_id != last_id) {
            // Add a reshape-to-same-shape as a no-op to make it the root.
            const out_shape = graphShapeToHlo(graph.node(output_node_ids[0]));
            _ = try b.reshape(out_hlo_id, out_shape);
        }
    } else {
        // Multiple outputs: pack into an HLO tuple as the root.
        var hlo_output_ids = try allocator.alloc(hlo.Id, output_node_ids.len);
        defer allocator.free(hlo_output_ids);
        for (output_node_ids, 0..) |nid, i| {
            hlo_output_ids[i] = node_map[@intCast(nid)] orelse return error.MissingOutput;
        }
        _ = try b.tuple(hlo_output_ids);
    }

    // ── 4. Build & serialize HloModuleProto ─────────────────────

    const comp = b.build();

    var aux_list = std.ArrayListUnmanaged(hlo.Computation).empty;
    defer aux_list.deinit(allocator);
    if (needs_reducer) try aux_list.append(allocator, reducer_comp);
    if (needs_max_reducer) try aux_list.append(allocator, max_reducer_comp);
    const module = hlo.Module.initWithAux("partition", comp, aux_list.items);
    const hlo_bytes = try module.serialize(allocator);
    errdefer allocator.free(hlo_bytes);
    const input_bindings = try input_bindings_list.toOwnedSlice(allocator);
    errdefer allocator.free(input_bindings);
    const input_node_ids = try input_node_ids_list.toOwnedSlice(allocator);
    errdefer allocator.free(input_node_ids);
    const input_shapes = try buildInputShapesForBindings(allocator, graph, input_bindings);
    errdefer freeShapeSlices(allocator, input_shapes);
    const output_shapes = try cloneHloShapesForOutputNodeIds(allocator, &b, node_map, output_node_ids);
    errdefer freeShapeSlices(allocator, output_shapes);

    return .{
        .hlo_bytes = hlo_bytes,
        .input_bindings = input_bindings,
        .input_node_ids = input_node_ids,
        .output_node_ids = output_node_ids,
        .input_shapes = input_shapes,
        .output_shapes = output_shapes,
        .allocator = allocator,
    };
}

// ── HLO composite helpers ──────────────────────────────────────────

fn addChannelBias(b: *hlo.Builder, value: hlo.Id, bias: hlo.Id, shape: hlo.Shape) !hlo.Id {
    const channel = try b.reshape(bias, hlo.Shape.init(shape.element_type, &.{shape.dimensions[1]}));
    return b.add(value, try b.broadcast(channel, &.{1}, shape));
}
fn convolutionAttrs(attrs: ml.graph.node.ConvAttrs) !hlo.Convolution {
    if (attrs.transposed or attrs.num_spatial == 0 or attrs.num_spatial > 4 or attrs.groups == 0) return error.UnsupportedConvolutionLayout;
    var result = hlo.Convolution{ .spatial_rank = attrs.num_spatial, .groups = attrs.groups };
    for (0..attrs.num_spatial) |axis| {
        if (attrs.output_padding[axis] != 0) return error.UnsupportedConvolutionLayout;
        result.strides[axis] = attrs.strides[axis];
        result.dilations[axis] = attrs.dilations[axis];
        result.padding_low[axis] = attrs.padding[axis][0];
        result.padding_high[axis] = attrs.padding[axis][1];
    }
    return result;
}

/// RMS norm: x * rsqrt(mean(x^2) + eps) * gamma
fn emitRmsNorm(b: *hlo.Builder, x: hlo.Id, gamma: hlo.Id, eps: f32, reducer: *const hlo.Computation) !hlo.Id {
    const x_shape = b.getInst(x).shape;
    const rank = x_shape.dimensions.len;
    const last_dim_size = x_shape.dimensions[rank - 1];

    // x^2
    const x_sq = try b.multiply(x, x);

    // sum(x^2) along last dim
    const reduced_shape = reducedShape(x_shape, rank);
    const zero = try b.constantScalarF32(0.0);
    const sum_sq = try b.reduce(x_sq, zero, &.{@as(i64, @intCast(rank - 1))}, reduced_shape, reducer.id);

    // mean = sum / N
    const dim_size_f = try b.constantScalarF32(@floatFromInt(last_dim_size));
    const mean_sq = try b.divide(sum_sq, dim_size_f);

    // mean + eps
    const eps_const = try b.constantScalarF32(eps);
    const variance = try b.add(mean_sq, eps_const);

    // rsqrt → broadcast back to original shape
    const inv_std = try b.rsqrt(variance);
    const inv_std_bc = try b.broadcast(inv_std, broadcastDimsExceptLast(rank), x_shape);

    // x * rsqrt(mean(x^2) + eps) * gamma
    const normed = try b.multiply(x, inv_std_bc);
    return b.multiply(normed, gamma);
}

/// Layer norm: (x - mean) * rsqrt(var + eps) * gamma + beta
fn emitLayerNorm(b: *hlo.Builder, x: hlo.Id, gamma: hlo.Id, beta: hlo.Id, eps: f32, reducer: *const hlo.Computation) !hlo.Id {
    const x_shape = b.getInst(x).shape;
    const rank = x_shape.dimensions.len;
    const last_dim_size = x_shape.dimensions[rank - 1];
    const dim_size_f = try b.constantScalarF32(@floatFromInt(last_dim_size));
    const eps_const = try b.constantScalarF32(eps);

    const reduced_shape = reducedShape(x_shape, rank);
    const zero = try b.constantScalarF32(0.0);
    const reduce_dims: []const i64 = &.{@as(i64, @intCast(rank - 1))};

    // mean(x)
    const sum_x = try b.reduce(x, zero, reduce_dims, reduced_shape, reducer.id);
    const mean_x = try b.divide(sum_x, dim_size_f);
    const mean_bc = try b.broadcast(mean_x, broadcastDimsExceptLast(rank), x_shape);

    // x - mean
    const centered = try b.subtract(x, mean_bc);

    // var = mean((x - mean)^2)
    const centered_sq = try b.multiply(centered, centered);
    const sum_sq = try b.reduce(centered_sq, zero, reduce_dims, reduced_shape, reducer.id);
    const var_x = try b.divide(sum_sq, dim_size_f);

    // rsqrt(var + eps)
    const variance = try b.add(var_x, eps_const);
    const inv_std = try b.rsqrt(variance);
    const inv_std_bc = try b.broadcast(inv_std, broadcastDimsExceptLast(rank), x_shape);

    // (x - mean) * rsqrt(var + eps) * gamma + beta
    const normed = try b.multiply(centered, inv_std_bc);
    const scaled = try b.multiply(normed, gamma);
    return b.add(scaled, beta);
}

// ── Helpers ─────────────────────────────────────────────────────────

fn getHloId(node_map: []const ?hlo.Id, node_id: NodeId) !hlo.Id {
    if (node_id == null_node) return error.MissingInput;
    return node_map[@intCast(node_id)] orelse return error.MissingInput;
}

fn emitEmbeddingIdsInputBinding(
    allocator: Allocator,
    b: *hlo.Builder,
    graph: *const Graph,
    node_id: NodeId,
    n: *const Node,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
    input_bindings_list: *std.ArrayListUnmanaged(InputBinding),
    next_param: *u32,
) !?hlo.Id {
    if (!isEmbeddingIdsPlaceholder(graph, node_id, part_set)) return null;
    const name_buf = try std.fmt.allocPrint(allocator, "input_{d}", .{next_param.*});
    defer allocator.free(name_buf);
    const input_id = try b.parameter(next_param.*, graphShapeToHlo(n), name_buf);
    next_param.* += 1;
    try input_bindings_list.append(allocator, .{ .embedding_ids = node_id });
    return input_id;
}

fn appendSemanticPastInput(
    allocator: Allocator,
    b: *hlo.Builder,
    graph: *const Graph,
    node_id: NodeId,
    semantic_past_inputs: *std.AutoHashMapUnmanaged(NodeId, hlo.Id),
    input_bindings_list: *std.ArrayListUnmanaged(InputBinding),
    input_node_ids_list: *std.ArrayListUnmanaged(NodeId),
    next_param: *u32,
    attrs: ml.graph.node.AttentionAttrs,
) !void {
    if (semantic_past_inputs.contains(node_id)) return;
    const past_shape = try semanticPastShapeForNode(graph, node_id, attrs);
    const name_buf = try std.fmt.allocPrint(allocator, "input_{d}", .{next_param.*});
    defer allocator.free(name_buf);
    const input_id = try b.parameter(next_param.*, hlo.Shape.init(past_shape.element_type, past_shape.dims[0..]), name_buf);
    next_param.* += 1;
    try semantic_past_inputs.put(allocator, node_id, input_id);
    try input_bindings_list.append(allocator, .{ .semantic_past_graph_node = node_id });
    try input_node_ids_list.append(allocator, node_id);
}

const SemanticPastShape = struct {
    element_type: hlo.ElementType,
    dims: [2]i64,
};

fn semanticPastShapeForNode(
    graph: *const Graph,
    node_id: NodeId,
    attrs: ml.graph.node.AttentionAttrs,
) !SemanticPastShape {
    const n = graph.node(node_id);
    if (n.output_shape.rank() != 2) return error.UnsupportedShape;
    const current_rows = n.output_shape.dims[0];
    const hidden = n.output_shape.dims[1];
    const query_rows: i64 = @intCast(attrs.seq_len);
    if (current_rows != query_rows) return error.UnsupportedShape;
    const kv_rows: i64 = if (attrs.kv_seq_len != 0) @intCast(attrs.kv_seq_len) else query_rows;
    if (kv_rows < query_rows) return error.UnsupportedShape;
    return .{
        .element_type = graphDTypeToHlo(n.output_shape.dtype),
        .dims = .{ kv_rows - query_rows, hidden },
    };
}

fn cloneHloShapesForOutputNodeIds(
    allocator: Allocator,
    b: *const hlo.Builder,
    node_map: []const ?hlo.Id,
    node_ids: []const NodeId,
) ![][]i64 {
    const out = try allocator.alloc([]i64, node_ids.len);
    errdefer allocator.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |shape| allocator.free(shape);
    }
    for (node_ids, 0..) |node_id, i| {
        const id = node_map[@intCast(node_id)] orelse return error.MissingOutput;
        out[i] = try allocator.dupe(i64, b.getInst(id).shape.dimensions);
        initialized += 1;
    }
    return out;
}

fn buildInputShapesForBindings(
    allocator: Allocator,
    graph: *const Graph,
    bindings: []const InputBinding,
) ![][]i64 {
    const out = try allocator.alloc([]i64, bindings.len);
    errdefer allocator.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |shape| allocator.free(shape);
    }
    for (bindings, 0..) |binding, i| {
        out[i] = switch (binding) {
            .graph_node => |node_id| blk: {
                const node = graph.node(node_id);
                break :blk try allocator.dupe(i64, node.output_shape.dims[0..node.output_shape.rank()]);
            },
            .embedding_ids => |node_id| blk: {
                const node = graph.node(node_id);
                break :blk try allocator.dupe(i64, node.output_shape.dims[0..node.output_shape.rank()]);
            },
            .semantic_past_graph_node => |node_id| blk: {
                const attrs = findAttentionAttrsForKvNode(graph, node_id) orelse return error.UnsupportedShape;
                const past_shape = try semanticPastShapeForNode(graph, node_id, attrs);
                break :blk try allocator.dupe(i64, past_shape.dims[0..]);
            },
        };
        initialized += 1;
    }
    return out;
}

fn findAttentionAttrsForKvNode(graph: *const Graph, node_id: NodeId) ?ml.graph.node.AttentionAttrs {
    for (0..graph.nodeCount()) |i| {
        const candidate_id: NodeId = @intCast(i);
        const node = graph.node(candidate_id);
        switch (node.op) {
            .fused_gqa_causal_attention => |attrs| {
                const inputs = node.getInputs();
                if (inputs.len >= 3 and (inputs[1] == node_id or inputs[2] == node_id)) return attrs;
            },
            else => {},
        }
    }
    return null;
}

fn freeShapeSlices(allocator: Allocator, shapes: [][]i64) void {
    for (shapes) |shape| allocator.free(shape);
    if (shapes.len > 0) allocator.free(shapes);
}

fn isEmbeddingIdsPlaceholder(
    graph: *const Graph,
    node_id: NodeId,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
) bool {
    const node = graph.node(node_id);
    if (node.op != .fused_from_float32 or node.output_shape.dtype != .i64) return false;
    for (0..graph.nodeCount()) |i| {
        const consumer_id: NodeId = @intCast(i);
        if (!part_set.contains(consumer_id)) continue;
        const consumer = graph.node(consumer_id);
        if (consumer.op != .fused_embedding_lookup) continue;
        for (consumer.getInputs()) |input_id| {
            if (input_id == node_id) return true;
        }
    }
    return false;
}

fn emitStaticRope(
    allocator: Allocator,
    b: *hlo.Builder,
    node: *const Node,
    x: hlo.Id,
    attrs: ml.graph.node.RopeAttrs,
) !hlo.Id {
    const shape = node.output_shape;
    if (shape.rank() != 2) return error.UnsupportedShape;
    const rows: usize = @intCast(shape.dims[0]);
    const hidden: usize = @intCast(shape.dims[1]);
    const seq_len: usize = @intCast(attrs.seq_len);
    const head_dim: usize = @intCast(attrs.head_dim);
    const rope_dim: usize = if (attrs.rope_dim > 0) @intCast(attrs.rope_dim) else head_dim;
    if (rows != seq_len or seq_len == 0) return error.UnsupportedShape;
    if (head_dim == 0 or hidden == 0 or hidden % head_dim != 0) return error.UnsupportedShape;
    if (rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.UnsupportedShape;

    const cos_mask = try buildRopeScaleMask(
        allocator,
        rows,
        hidden,
        head_dim,
        rope_dim,
        attrs.theta,
        attrs.freq_scale,
        @intCast(attrs.position_offset),
        attrs.consecutive_pairs,
        .cos,
    );
    defer allocator.free(cos_mask);

    const sin_mask = try buildRopeScaleMask(
        allocator,
        rows,
        hidden,
        head_dim,
        rope_dim,
        attrs.theta,
        attrs.freq_scale,
        @intCast(attrs.position_offset),
        attrs.consecutive_pairs,
        .sin,
    );
    defer allocator.free(sin_mask);

    const cos_id = try b.constantF32(hlo.Shape.init(.f32, &.{ @intCast(rows), @intCast(hidden) }), cos_mask);
    const sin_id = try b.constantF32(hlo.Shape.init(.f32, &.{ @intCast(rows), @intCast(hidden) }), sin_mask);
    const rotated = try emitRopePermutation(b, x, rows, hidden, head_dim, rope_dim, attrs.consecutive_pairs);
    const x_scaled = try b.multiply(x, cos_id);
    const rotated_scaled = try b.multiply(rotated, sin_id);
    return b.add(x_scaled, rotated_scaled);
}

const RopeScaleKind = enum { cos, sin };

fn buildRopeScaleMask(
    allocator: Allocator,
    rows: usize,
    hidden: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    consecutive_pairs: bool,
    kind: RopeScaleKind,
) ![]f32 {
    const data = try allocator.alloc(f32, rows * hidden);
    @memset(data, switch (kind) {
        .cos => 1.0,
        .sin => 0.0,
    });
    const num_heads = hidden / head_dim;
    const half = rope_dim / 2;
    const head_half = head_dim / 2;
    for (0..rows) |row| {
        const pos = position_offset + row;
        for (0..num_heads) |head| {
            const head_base = row * hidden + head * head_dim;
            for (0..half) |j| {
                const freq = 1.0 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(rope_dim)));
                const angle = @as(f32, @floatFromInt(pos)) * freq_scale * freq;
                const value = switch (kind) {
                    .cos => @cos(angle),
                    .sin => @sin(angle),
                };
                const idx0 = if (consecutive_pairs) 2 * j else j;
                const idx1 = if (consecutive_pairs) 2 * j + 1 else j + head_half;
                data[head_base + idx0] = value;
                data[head_base + idx1] = value;
            }
        }
    }
    return data;
}

/// Pair-local coordinate rotation. No dense permutation operand or matmul.
fn emitRopePermutation(b: *hlo.Builder, x: hlo.Id, rows: usize, hidden: usize, head_dim: usize, rope_dim: usize, consecutive: bool) !hlo.Id {
    const r: i64 = @intCast(rows);
    const heads: i64 = @intCast(hidden / head_dim);
    const d: i64 = @intCast(head_dim);
    const rotary: i64 = @intCast(rope_dim);
    const shape = hlo.Shape.init(.f32, &.{ r, heads, d });
    const source = try b.reshape(x, shape);
    const half = @divExact(rotary, 2);
    var rotated: hlo.Id = undefined;
    if (consecutive) {
        const lane_shape = hlo.Shape.init(.f32, &.{ r, heads, half });
        const even = try b.slice(source, &.{ 0, 0, 0 }, &.{ r, heads, rotary }, &.{ 1, 1, 2 }, lane_shape);
        const odd = try b.slice(source, &.{ 0, 0, 1 }, &.{ r, heads, rotary }, &.{ 1, 1, 2 }, lane_shape);
        const pair_shape = hlo.Shape.init(.f32, &.{ r, heads, half, 1 });
        const pairs = try b.concatenate(&.{ try b.reshape(try b.negate(odd), pair_shape), try b.reshape(even, pair_shape) }, 3, hlo.Shape.init(.f32, &.{ r, heads, half, 2 }));
        rotated = try b.reshape(pairs, hlo.Shape.init(.f32, &.{ r, heads, rotary }));
    } else {
        // Partial half-split RoPE pairs across head_dim/2, not rope_dim/2.
        const head_half = @divExact(d, 2);
        const lane_shape = hlo.Shape.init(.f32, &.{ r, heads, half });
        var first = try b.slice(source, &.{ 0, 0, 0 }, &.{ r, heads, half }, &.{ 1, 1, 1 }, lane_shape);
        const second = try b.slice(source, &.{ 0, 0, head_half }, &.{ r, heads, head_half + half }, &.{ 1, 1, 1 }, lane_shape);
        var negative = try b.negate(second);
        if (half != head_half) {
            const gap = try b.broadcast(try b.constantScalarF32(0), &.{}, hlo.Shape.init(.f32, &.{ r, heads, head_half - half }));
            const half_shape = hlo.Shape.init(.f32, &.{ r, heads, head_half });
            first = try b.concatenate(&.{ first, gap }, 2, half_shape);
            negative = try b.concatenate(&.{ negative, gap }, 2, half_shape);
        }
        rotated = try b.concatenate(&.{ negative, first }, 2, shape);
    }
    if (consecutive and rotary != d) {
        const tail_shape = hlo.Shape.init(.f32, &.{ r, heads, d - rotary });
        const tail = try b.broadcast(try b.constantScalarF32(0), &.{}, tail_shape);
        rotated = try b.concatenate(&.{ rotated, tail }, 2, shape);
    }
    return b.reshape(rotated, hlo.Shape.init(.f32, &.{ r, @intCast(hidden) }));
}

fn buildRopePermutationMatrix(
    allocator: Allocator,
    hidden: usize,
    head_dim: usize,
    rope_dim: usize,
    consecutive_pairs: bool,
) ![]f32 {
    const data = try allocator.alloc(f32, hidden * hidden);
    @memset(data, 0.0);
    const num_heads = hidden / head_dim;
    const half = rope_dim / 2;
    const head_half = head_dim / 2;
    for (0..num_heads) |head| {
        const head_base = head * head_dim;
        for (0..half) |j| {
            const idx0 = head_base + if (consecutive_pairs) 2 * j else j;
            const idx1 = head_base + if (consecutive_pairs) 2 * j + 1 else j + head_half;
            data[idx1 * hidden + idx0] = -1.0;
            data[idx0 * hidden + idx1] = 1.0;
        }
    }
    return data;
}

const StaticGqaAttentionResult = struct {
    output: hlo.Id,
    present_k: ?hlo.Id = null,
    present_v: ?hlo.Id = null,
};

/// One portable primitive lowering for inference and gradient programs.
fn emitPrimitiveNode(
    allocator: Allocator,
    b: *hlo.Builder,
    n: *const Node,
    node_map: []?hlo.Id,
    nid: usize,
    reducer_comp: *const hlo.Computation,
    max_reducer_comp: *const hlo.Computation,
    needs_add_reducer: *bool,
    needs_max_reducer: *bool,
) !bool {
    const ins = n.getInputs();
    switch (n.op) {
        .gather => |attrs| {
            if (attrs.axis != 0) return error.UnsupportedShape;
            node_map[nid] = try b.gather(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1]), graphShapeToHlo(n));
        },
        .fused_disentangled_attention => |attrs| {
            if (ins.len != 3) return error.UnsupportedShape;
            node_map[nid] = try @import("pjrt_relative_attention.zig").emit(allocator, b, try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1]), try getHloId(node_map, ins[2]), attrs, reducer_comp.id, max_reducer_comp.id);
            needs_add_reducer.* = true;
            needs_max_reducer.* = true;
        },
        .stop_gradient => node_map[nid] = try getHloId(node_map, ins[0]),
        .fused_softmax => {
            const source = try getHloId(node_map, ins[0]);
            const shape = b.getInst(source).shape;
            if (shape.dimensions.len == 0 or shape.dimensions.len > 8 or shape.element_type != .f32) return error.UnsupportedShape;
            const axis: i64 = @intCast(shape.dimensions.len - 1);
            const reduced = hlo.Shape.init(.f32, shape.dimensions[0 .. shape.dimensions.len - 1]);
            const negative = try b.constantScalarF32(-std.math.inf(f32));
            const maximum = try b.reduce(source, negative, &.{axis}, reduced, max_reducer_comp.id);
            var axes: [8]i64 = undefined;
            for (axes[0 .. shape.dimensions.len - 1], 0..) |*dimension, index| dimension.* = @intCast(index);
            const exp = try b.exponential(try b.subtract(source, try b.broadcast(maximum, axes[0 .. shape.dimensions.len - 1], shape)));
            const sums = try b.reduce(exp, try b.constantScalarF32(0), &.{axis}, reduced, reducer_comp.id);
            node_map[nid] = try b.divide(exp, try b.broadcast(sums, axes[0 .. shape.dimensions.len - 1], shape));
            needs_add_reducer.* = true;
            needs_max_reducer.* = true;
        },
        .conv_general => |attrs| node_map[nid] = try b.convolution(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1]), graphShapeToHlo(n), try convolutionAttrs(attrs)),
        // Unary elementwise ops
        .neg => node_map[nid] = try b.negate(try getHloId(node_map, ins[0])),
        .sqrt => node_map[nid] = try b.sqrt(try getHloId(node_map, ins[0])),
        .rsqrt => node_map[nid] = try b.rsqrt(try getHloId(node_map, ins[0])),
        .exp => node_map[nid] = try b.exponential(try getHloId(node_map, ins[0])),
        .log => node_map[nid] = try b.log(try getHloId(node_map, ins[0])),
        .sin => node_map[nid] = try b.sine(try getHloId(node_map, ins[0])),
        .cos => node_map[nid] = try b.cosine(try getHloId(node_map, ins[0])),
        .tanh => node_map[nid] = try b.tanh(try getHloId(node_map, ins[0])),
        .erf => node_map[nid] = try b.erf(try getHloId(node_map, ins[0])),
        .abs => node_map[nid] = try b.abs(try getHloId(node_map, ins[0])),

        // Binary elementwise ops
        .add => node_map[nid] = try b.add(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1])),
        .mul => node_map[nid] = try b.multiply(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1])),
        .sub => node_map[nid] = try b.subtract(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1])),
        .div => node_map[nid] = try b.divide(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1])),
        .less_than => node_map[nid] = try b.lessThan(try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1])),

        // Select (where)
        .where_select => node_map[nid] = try b.select(
            try getHloId(node_map, ins[0]),
            try getHloId(node_map, ins[1]),
            try getHloId(node_map, ins[2]),
        ),

        .reduce_sum, .reduce_mean, .reduce_max => |attrs| {
            const source = try getHloId(node_map, ins[0]);
            const source_shape = b.getInst(source).shape;
            if (source_shape.element_type != .f32) return error.UnsupportedPrimitiveType;
            var axes: [8]i64 = undefined;
            var reduced: [8]i64 = undefined;
            var rank: usize = 0;
            var elements: usize = 1;
            for (attrs.axes[0..attrs.num_axes], 0..) |axis, index| {
                if (axis >= source_shape.dimensions.len) return error.UnsupportedShape;
                axes[index] = axis;
                elements = try std.math.mul(usize, elements, std.math.cast(usize, source_shape.dimensions[axis]) orelse return error.UnsupportedShape);
            }
            for (source_shape.dimensions, 0..) |dimension, axis| {
                if (std.mem.indexOfScalar(u8, attrs.axes[0..attrs.num_axes], @intCast(axis)) != null) continue;
                reduced[rank] = dimension;
                rank += 1;
            }
            const maximum = n.op == .reduce_max;
            if (maximum) needs_max_reducer.* = true else needs_add_reducer.* = true;
            const initial = try b.constantScalarF32(if (maximum) -std.math.inf(f32) else 0);
            var value = try b.reduce(source, initial, axes[0..attrs.num_axes], hlo.Shape.init(.f32, reduced[0..rank]), if (maximum) max_reducer_comp.id else reducer_comp.id);
            if (n.op == .reduce_mean) {
                if (elements == 0) return error.UnsupportedShape;
                value = try b.divide(value, try b.constantScalarF32(@floatFromInt(elements)));
            }
            node_map[nid] = try b.reshape(value, graphShapeToHlo(n));
        },

        // Shape ops
        .reshape => |attrs| {
            if (attrs.runtime_shape or !hloShapeIsConcretePositive(attrs.new_shape)) return error.UnsupportedShape;
            node_map[nid] = try b.reshape(
                try getHloId(node_map, ins[0]),
                hlo.Shape.init(graphDTypeToHlo(attrs.new_shape.dtype), attrs.new_shape.dims[0..attrs.new_shape.rank()]),
            );
        },

        .transpose => |attrs| {
            const src = try getHloId(node_map, ins[0]);
            const src_shape = b.getInst(src).shape;
            var perm = try allocator.alloc(i64, attrs.num_axes);
            defer allocator.free(perm);
            var result_dims = try allocator.alloc(i64, attrs.num_axes);
            defer allocator.free(result_dims);
            for (attrs.perm[0..attrs.num_axes], 0..) |p, j| {
                perm[j] = @intCast(p);
                result_dims[j] = src_shape.dimensions[@intCast(p)];
            }
            node_map[nid] = try b.transpose(
                src,
                perm,
                hlo.Shape.init(src_shape.element_type, result_dims),
            );
        },

        .broadcast_in_dim => |attrs| {
            const src = try getHloId(node_map, ins[0]);
            var bcast_dims = try allocator.alloc(i64, attrs.num_axes);
            defer allocator.free(bcast_dims);
            for (attrs.broadcast_axes[0..attrs.num_axes], 0..) |ax, j| bcast_dims[j] = @intCast(ax);
            const target_shape = hlo.Shape.init(
                graphDTypeToHlo(attrs.target_shape.dtype),
                attrs.target_shape.dims[0..attrs.target_shape.rank()],
            );
            node_map[nid] = try b.broadcast(src, bcast_dims, target_shape);
        },

        // Contraction
        .dot_general => |attrs| {
            const lhs = try getHloId(node_map, ins[0]);
            const rhs = try getHloId(node_map, ins[1]);
            var lhs_cont = try allocator.alloc(i64, attrs.num_contracting);
            defer allocator.free(lhs_cont);
            var rhs_cont = try allocator.alloc(i64, attrs.num_contracting);
            defer allocator.free(rhs_cont);
            var lhs_batch = try allocator.alloc(i64, attrs.num_batch);
            defer allocator.free(lhs_batch);
            var rhs_batch = try allocator.alloc(i64, attrs.num_batch);
            defer allocator.free(rhs_batch);
            for (attrs.lhs_contracting[0..attrs.num_contracting], 0..) |d, j| lhs_cont[j] = @intCast(d);
            for (attrs.rhs_contracting[0..attrs.num_contracting], 0..) |d, j| rhs_cont[j] = @intCast(d);
            for (attrs.lhs_batch[0..attrs.num_batch], 0..) |d, j| lhs_batch[j] = @intCast(d);
            for (attrs.rhs_batch[0..attrs.num_batch], 0..) |d, j| rhs_batch[j] = @intCast(d);
            const result_shape = graphShapeToHlo(n);
            node_map[nid] = try b.dot(lhs, rhs, result_shape, .{
                .lhs_contracting_dimensions = lhs_cont,
                .rhs_contracting_dimensions = rhs_cont,
                .lhs_batch_dimensions = lhs_batch,
                .rhs_batch_dimensions = rhs_batch,
            });
        },

        // Type conversion
        .convert_dtype => |attrs| {
            node_map[nid] = try b.convertType(
                try getHloId(node_map, ins[0]),
                graphDTypeToHlo(attrs.target),
            );
        },

        .slice => |attrs| {
            if (attrs.runtime_starts or attrs.runtime_limits) return error.UnsupportedShape;
            const rank: usize = attrs.num_axes;
            if (rank == 0 or rank > 8) return error.UnsupportedShape;
            var starts: [8]i64 = undefined;
            var limits: [8]i64 = undefined;
            var strides: [8]i64 = undefined;
            for (0..rank) |axis| {
                starts[axis] = attrs.starts[axis];
                limits[axis] = attrs.limits[axis];
                strides[axis] = attrs.strides[axis];
            }
            node_map[nid] = try b.slice(
                try getHloId(node_map, ins[0]),
                starts[0..rank],
                limits[0..rank],
                strides[0..rank],
                graphShapeToHlo(n),
            );
        },

        .concat_prim => |attrs| {
            const x = try getHloId(node_map, ins[0]);
            const y = try getHloId(node_map, ins[1]);
            node_map[nid] = try b.concatenate(&.{ x, y }, @intCast(attrs.axis), graphShapeToHlo(n));
        },

        else => return false,
    }
    return true;
}

fn segmentMetadataLane(b: *hlo.Builder, metadata: hlo.Id, start: usize, limit: usize, stride: usize, count: usize, axis: i64, rows: i64, columns: i64) !hlo.Id {
    const lane = try b.slice(metadata, &.{@intCast(start)}, &.{@intCast(limit)}, &.{@intCast(stride)}, hlo.Shape.init(.s32, &.{@intCast(count)}));
    return b.broadcast(lane, &.{axis}, hlo.Shape.init(.s32, &.{ rows, columns }));
}

fn emitSegmentAttention(
    allocator: Allocator,
    b: *hlo.Builder,
    node: *const Node,
    q: hlo.Id,
    k: hlo.Id,
    v: hlo.Id,
    metadata: hlo.Id,
    attrs: ml.graph.node.SegmentAttentionAttrs,
    add_reducer: *const hlo.Computation,
    max_reducer: *const hlo.Computation,
) !hlo.Id {
    const zero = try b.constantScalarF32(0);
    if (attrs.queries == 0 or attrs.keys == 0) return b.broadcast(zero, &.{}, graphShapeToHlo(node));
    if (attrs.num_heads == 0 or attrs.head_dim == 0) return error.UnsupportedShape;
    const rows: i64 = attrs.queries;
    const columns: i64 = attrs.keys;
    const heads: i64 = attrs.num_heads;
    const dim: i64 = attrs.head_dim;
    const ranges_end = try std.math.mul(usize, attrs.queries, 6);
    const metadata_count = try std.math.add(usize, ranges_end, try std.math.add(usize, attrs.queries, attrs.keys));
    const metadata_shape = b.getInst(metadata).shape;
    if (metadata_shape.element_type != .s32 or metadata_shape.dimensions.len != 1 or metadata_shape.dimensions[0] != metadata_count) return error.InvalidAttentionMetadata;
    const indices = try allocator.alloc(i32, @as(usize, attrs.keys) + 1);
    defer allocator.free(indices);
    for (indices[0..attrs.keys], 0..) |*value, index| value.* = std.math.cast(i32, index) orelse return error.UnsupportedShape;
    indices[attrs.keys] = @bitCast(attrs.window);
    const index = try b.constantS32(hlo.Shape.init(.s32, &.{columns}), indices[0..attrs.keys]);
    const keys = try b.broadcast(index, &.{1}, hlo.Shape.init(.s32, &.{ rows, columns }));
    var visible: ?hlo.Id = null;
    for (0..3) |range| {
        const start = try segmentMetadataLane(b, metadata, range * 2, ranges_end, 6, attrs.queries, 0, rows, columns);
        const stop = try segmentMetadataLane(b, metadata, range * 2 + 1, ranges_end, 6, attrs.queries, 0, rows, columns);
        const allowed = try b.logicalAnd(try b.compare(keys, start, "GE"), try b.compare(keys, stop, "LT"));
        visible = if (visible) |previous| try b.logicalOr(previous, allowed) else allowed;
    }
    if (attrs.window != std.math.maxInt(u32)) {
        const qp = try segmentMetadataLane(b, metadata, ranges_end, ranges_end + attrs.queries, 1, attrs.queries, 0, rows, columns);
        const kp = try segmentMetadataLane(b, metadata, ranges_end + attrs.queries, metadata_count, 1, attrs.keys, 1, rows, columns);
        // Widen before subtracting: signed positions may span the entire i32 domain.
        const distance = try b.abs(try b.subtract(try b.convertType(qp, .s64), try b.convertType(kp, .s64)));
        const window = try b.constantS32(hlo.Shape.scalar(.s32), indices[attrs.keys..]);
        var wide_window = try b.convertType(window, .s64);
        if (attrs.window > std.math.maxInt(i32)) wide_window = try b.add(wide_window, try b.convertType(try b.constantScalarF32(4294967296), .s64));
        const window_grid = try b.broadcast(wide_window, &.{}, hlo.Shape.init(.s64, &.{ rows, columns }));
        visible = try b.logicalAnd(visible.?, try b.compare(distance, window_grid, "LE"));
    }
    const query = try reshapeAttentionInputToHeads(b, q, rows, heads, dim);
    const key = try reshapeAttentionInputToHeads(b, k, columns, heads, dim);
    const value = try reshapeAttentionInputToHeads(b, v, columns, heads, dim);
    const key_t = try b.transpose(key, &.{ 0, 2, 1 }, hlo.Shape.init(.f32, &.{ heads, dim, columns }));
    const score_shape = hlo.Shape.init(.f32, &.{ heads, rows, columns });
    var scores = try b.dot(query, key_t, score_shape, .{ .lhs_contracting_dimensions = &.{2}, .rhs_contracting_dimensions = &.{1}, .lhs_batch_dimensions = &.{0}, .rhs_batch_dimensions = &.{0} });
    scores = try b.multiply(scores, try b.constantScalarF32(1 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim)))));
    const mask = try b.broadcast(visible.?, &.{ 1, 2 }, hlo.Shape.init(.pred, &.{ heads, rows, columns }));
    const negative_infinity = try b.constantScalarF32(-std.math.inf(f32));
    scores = try b.select(mask, scores, try b.broadcast(negative_infinity, &.{}, score_shape));
    const row_shape = hlo.Shape.init(.f32, &.{ heads, rows });
    const maximum = try b.reduce(scores, negative_infinity, &.{2}, row_shape, max_reducer.id);
    const safe_maximum = try b.maximum(maximum, try b.constantScalarF32(-std.math.floatMax(f32)));
    const exp = try b.exponential(try b.subtract(scores, try b.broadcast(safe_maximum, &.{ 0, 1 }, score_shape)));
    const sums = try b.reduce(exp, zero, &.{2}, row_shape, add_reducer.id);
    const one = try b.constantScalarF32(1);
    const probabilities = try b.divide(exp, try b.broadcast(try b.maximum(sums, one), &.{ 0, 1 }, score_shape));
    const output_shape = hlo.Shape.init(.f32, &.{ heads, rows, dim });
    var attended = try b.dot(probabilities, value, output_shape, .{ .lhs_contracting_dimensions = &.{2}, .rhs_contracting_dimensions = &.{1}, .lhs_batch_dimensions = &.{0}, .rhs_batch_dimensions = &.{0} });
    const active = try b.compare(sums, try b.broadcast(zero, &.{}, row_shape), "NE");
    attended = try b.select(try b.broadcast(active, &.{ 0, 1 }, hlo.Shape.init(.pred, &.{ heads, rows, dim })), attended, try b.broadcast(zero, &.{}, output_shape));
    const transposed = try b.transpose(attended, &.{ 1, 0, 2 }, hlo.Shape.init(.f32, &.{ rows, heads, dim }));
    return b.reshape(transposed, graphShapeToHlo(node));
}

fn emitStaticGqaAttention(
    allocator: Allocator,
    b: *hlo.Builder,
    node: *const Node,
    q: hlo.Id,
    k: hlo.Id,
    v: hlo.Id,
    bias: ?hlo.Id,
    attrs: ml.graph.node.AttentionAttrs,
    add_reducer: *const hlo.Computation,
    max_reducer: *const hlo.Computation,
    past_k: ?hlo.Id,
    past_v: ?hlo.Id,
) !StaticGqaAttentionResult {
    if (attrs.batch != 1 or attrs.seq_len == 0 or attrs.num_heads == 0 or attrs.head_dim == 0) return error.UnsupportedShape;
    const has_past = past_k != null or past_v != null;
    if ((attrs.skip_kv_write or has_past) and (past_k == null or past_v == null or attrs.seq_len != 1)) return error.UnsupportedShape;
    const q_len: i64 = @intCast(attrs.seq_len);
    const num_heads: i64 = @intCast(attrs.num_heads);
    const num_kv_heads: i64 = if (attrs.num_kv_heads > 0) @intCast(attrs.num_kv_heads) else num_heads;
    const head_dim: i64 = @intCast(attrs.head_dim);
    if (@rem(num_heads, num_kv_heads) != 0) return error.UnsupportedShape;

    const present_k = if (past_k) |past| try concatenateKvCache(b, past, k, num_kv_heads, head_dim) else null;
    const present_v = if (past_v) |past| try concatenateKvCache(b, past, v, num_kv_heads, head_dim) else null;
    const k_for_attention = present_k orelse k;
    const v_for_attention = present_v orelse v;
    const kv_len = attentionRowsFromShape(b.getInst(k_for_attention).shape, num_kv_heads, head_dim) orelse return error.UnsupportedShape;

    const q_heads = try reshapeAttentionInputToHeads(b, q, q_len, num_heads, head_dim);
    const k_heads = try reshapeAttentionInputToHeads(
        b,
        try repeatGqaKvHeads(b, k_for_attention, kv_len, num_kv_heads, num_heads, head_dim),
        kv_len,
        num_heads,
        head_dim,
    );
    const v_heads = try reshapeAttentionInputToHeads(
        b,
        try repeatGqaKvHeads(b, v_for_attention, kv_len, num_kv_heads, num_heads, head_dim),
        kv_len,
        num_heads,
        head_dim,
    );
    const k_t = try b.transpose(k_heads, &.{ 0, 2, 1 }, hlo.Shape.init(.f32, &.{ num_heads, head_dim, kv_len }));
    var scores = try b.dot(q_heads, k_t, hlo.Shape.init(.f32, &.{ num_heads, q_len, kv_len }), .{
        .lhs_contracting_dimensions = &.{2},
        .rhs_contracting_dimensions = &.{1},
        .lhs_batch_dimensions = &.{0},
        .rhs_batch_dimensions = &.{0},
    });
    const scale = try b.constantScalarF32(1.0 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim))));
    scores = try b.multiply(scores, scale);
    if (past_k == null) {
        scores = try b.add(scores, try emitCausalMask(allocator, b, num_heads, q_len));
    }
    if (bias) |bias_id| scores = try b.add(scores, bias_id);

    const probs = try emitSoftmaxLastDim(b, scores, add_reducer, max_reducer);
    const attended = try b.dot(probs, v_heads, hlo.Shape.init(.f32, &.{ num_heads, q_len, head_dim }), .{
        .lhs_contracting_dimensions = &.{2},
        .rhs_contracting_dimensions = &.{1},
        .lhs_batch_dimensions = &.{0},
        .rhs_batch_dimensions = &.{0},
    });
    const attended_t = try b.transpose(attended, &.{ 1, 0, 2 }, hlo.Shape.init(.f32, &.{ q_len, num_heads, head_dim }));
    return .{
        .output = try b.reshape(attended_t, graphShapeToHlo(node)),
        .present_k = present_k,
        .present_v = present_v,
    };
}

fn concatenateKvCache(b: *hlo.Builder, past: hlo.Id, current: hlo.Id, num_kv_heads: i64, head_dim: i64) !hlo.Id {
    const past_shape = b.getInst(past).shape;
    const current_shape = b.getInst(current).shape;
    const past_rows = attentionRowsFromShape(past_shape, num_kv_heads, head_dim) orelse return error.UnsupportedShape;
    const current_rows = attentionRowsFromShape(current_shape, num_kv_heads, head_dim) orelse return error.UnsupportedShape;
    return b.concatenate(
        &.{ past, current },
        0,
        hlo.Shape.init(.f32, &.{ past_rows + current_rows, num_kv_heads * head_dim }),
    );
}

fn attentionRowsFromShape(shape: hlo.Shape, heads: i64, head_dim: i64) ?i64 {
    if (shape.element_type != .f32 or shape.dimensions.len != 2) return null;
    if (shape.dimensions[1] != heads * head_dim) return null;
    return shape.dimensions[0];
}

fn reshapeAttentionInputToHeads(
    b: *hlo.Builder,
    value: hlo.Id,
    seq_len: i64,
    num_heads: i64,
    head_dim: i64,
) !hlo.Id {
    const value4 = try b.reshape(value, hlo.Shape.init(.f32, &.{ seq_len, num_heads, head_dim }));
    return b.transpose(value4, &.{ 1, 0, 2 }, hlo.Shape.init(.f32, &.{ num_heads, seq_len, head_dim }));
}

fn repeatGqaKvHeads(b: *hlo.Builder, value: hlo.Id, seq_len: i64, num_kv_heads: i64, num_heads: i64, head_dim: i64) !hlo.Id {
    if (num_kv_heads == num_heads) return value;
    const repeat_factor = std.math.divExact(i64, num_heads, num_kv_heads) catch return error.UnsupportedShape;
    const grouped = try b.reshape(value, hlo.Shape.init(.f32, &.{ seq_len, num_kv_heads, 1, head_dim }));
    const expanded = try b.broadcast(grouped, &.{ 0, 1, 2, 3 }, hlo.Shape.init(.f32, &.{ seq_len, num_kv_heads, repeat_factor, head_dim }));
    return b.reshape(expanded, hlo.Shape.init(.f32, &.{ seq_len, num_heads, head_dim }));
}

fn emitCausalMask(allocator: Allocator, b: *hlo.Builder, heads: i64, rows: i64) !hlo.Id {
    const positions = try allocator.alloc(i32, std.math.cast(usize, rows) orelse return error.UnsupportedShape);
    defer allocator.free(positions);
    for (positions, 0..) |*value, index| value.* = std.math.cast(i32, index) orelse return error.UnsupportedShape;
    const index = try b.constantS32(hlo.Shape.init(.s32, &.{rows}), positions);
    const grid = hlo.Shape.init(.s32, &.{ rows, rows });
    const allowed = try b.compare(try b.broadcast(index, &.{0}, grid), try b.broadcast(index, &.{1}, grid), "GE");
    const floats = hlo.Shape.init(.f32, &.{ rows, rows });
    // Preserve the existing finite exclusion value while computing the mask
    // from O(S) host metadata rather than serializing H*S*S float constants.
    const mask = try b.select(allowed, try b.broadcast(try b.constantScalarF32(0), &.{}, floats), try b.broadcast(try b.constantScalarF32(-1.0e9), &.{}, floats));
    return b.broadcast(mask, &.{ 1, 2 }, hlo.Shape.init(.f32, &.{ heads, rows, rows }));
}

fn emitSoftmaxLastDim(
    b: *hlo.Builder,
    x: hlo.Id,
    add_reducer: *const hlo.Computation,
    max_reducer: *const hlo.Computation,
) !hlo.Id {
    const x_shape = b.getInst(x).shape;
    const rank = x_shape.dimensions.len;
    const reduced_shape = reducedShape(x_shape, rank);
    const reduce_dims: []const i64 = &.{@as(i64, @intCast(rank - 1))};

    const neg_inf = try b.constantScalarF32(-std.math.inf(f32));
    const max_val = try b.reduce(x, neg_inf, reduce_dims, reduced_shape, max_reducer.id);
    const max_bc = try b.broadcast(max_val, broadcastDimsExceptLast(rank), x_shape);
    const shifted = try b.subtract(x, max_bc);
    const exp = try b.exponential(shifted);
    const zero = try b.constantScalarF32(0.0);
    const sum = try b.reduce(exp, zero, reduce_dims, reduced_shape, add_reducer.id);
    const sum_bc = try b.broadcast(sum, broadcastDimsExceptLast(rank), x_shape);
    return b.divide(exp, sum_bc);
}

fn graphShapeToHlo(n: *const Node) hlo.Shape {
    return hlo.Shape.init(
        graphDTypeToHlo(n.output_shape.dtype),
        n.output_shape.dims[0..n.output_shape.rank()],
    );
}

fn hloShapeIsConcretePositive(shape: ml.graph.Shape) bool {
    for (0..shape.rank()) |axis| {
        if (shape.dim(@intCast(axis)) <= 0) return false;
    }
    return true;
}

fn emitGraphConstant(b: *hlo.Builder, graph: *const Graph, n: *const Node) !hlo.Id {
    const attrs = n.op.constant;
    const shape = graphShapeToHlo(n);
    return switch (n.output_shape.dtype) {
        .f32 => b.constantF32(shape, graph.constantData(attrs.data_offset, attrs.data_len)),
        .i32 => b.constantS32(shape, graph.constantDataAs(i32, attrs.data_offset, attrs.data_len)),
        .i64 => b.constantS64(shape, graph.constantDataAs(i64, attrs.data_offset, attrs.data_len)),
        .bool_ => b.constantPred(shape, graph.constantDataAs(u8, attrs.data_offset, attrs.data_len)),
        else => error.UnsupportedTensorType,
    };
}

fn graphDTypeToHlo(dtype: ml.graph.DType) hlo.ElementType {
    return switch (dtype) {
        .f32, .f16, .bf16 => .f32,
        .f64 => .f64,
        .i8 => .s8,
        .i16 => .s16,
        .i32 => .s32,
        .i64 => .s64,
        .u8 => .u8,
        .bool_ => .pred,
    };
}

/// Transpose the last two dimensions of a 2D+ tensor.
fn transposeLastTwo(b: *hlo.Builder, id: hlo.Id) !hlo.Id {
    const shape = b.getInst(id).shape;
    const rank = shape.dimensions.len;
    if (rank < 2) return id;

    var perm: [8]i64 = undefined;
    for (0..rank) |i| perm[i] = @intCast(i);
    perm[rank - 2] = @intCast(rank - 1);
    perm[rank - 1] = @intCast(rank - 2);

    var result_dims: [8]i64 = undefined;
    for (0..rank) |i| result_dims[i] = shape.dimensions[i];
    result_dims[rank - 2] = shape.dimensions[rank - 1];
    result_dims[rank - 1] = shape.dimensions[rank - 2];

    return b.transpose(id, perm[0..rank], hlo.Shape.init(shape.element_type, result_dims[0..rank]));
}

/// Shape with last dimension removed (for reduce result).
fn reducedShape(shape: hlo.Shape, rank: usize) hlo.Shape {
    return hlo.Shape.init(shape.element_type, shape.dimensions[0 .. rank - 1]);
}

/// Broadcast dimensions for broadcasting a reduced tensor back to full shape.
/// For rank=2: {0}, for rank=3: {0, 1}, etc.
fn broadcastDimsExceptLast(rank: usize) []const i64 {
    const dims = comptime blk: {
        var d: [8]i64 = undefined;
        for (0..8) |i| d[i] = @intCast(i);
        break :blk d;
    };
    return dims[0 .. rank - 1];
}

/// Find nodes in the partition that are consumed by downstream partitions
/// or are graph outputs.
fn computeOutputs(
    allocator: Allocator,
    graph: *const Graph,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
) ![]NodeId {
    return partition_export.computeOutputs(allocator, graph, part_set);
}

fn computeOutputsWithOptions(
    allocator: Allocator,
    graph: *const Graph,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
    part_node_ids: []const NodeId,
    options: CompileOptions,
) ![]NodeId {
    const base = try computeOutputs(allocator, graph, part_set);
    errdefer allocator.free(base);
    if (!options.semantic_kv_bindings) return base;

    var out = std.ArrayListUnmanaged(NodeId).empty;
    errdefer out.deinit(allocator);
    if (semanticPrimaryOutput(graph, part_set)) |primary_output| {
        try out.append(allocator, primary_output);
    } else {
        try out.appendSlice(allocator, base);
    }
    for (part_node_ids) |node_id| {
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_gqa_causal_attention => {
                const inputs = node.getInputs();
                if (inputs.len < 3) return error.UnsupportedShape;
                try appendUniqueNodeId(allocator, &out, inputs[1]);
                try appendUniqueNodeId(allocator, &out, inputs[2]);
            },
            else => {},
        }
    }
    allocator.free(base);
    return out.toOwnedSlice(allocator);
}

fn semanticPrimaryOutput(
    graph: *const Graph,
    part_set: *const std.AutoHashMapUnmanaged(NodeId, void),
) ?NodeId {
    var idx = graph.outputs.items.len;
    while (idx > 0) {
        idx -= 1;
        const output_id = graph.outputs.items[idx];
        if (part_set.contains(output_id)) return output_id;
    }
    return null;
}

fn appendUniqueNodeId(
    allocator: Allocator,
    list: *std.ArrayListUnmanaged(NodeId),
    node_id: NodeId,
) !void {
    for (list.items) |existing| {
        if (existing == node_id) return;
    }
    try list.append(allocator, node_id);
}

// ── Gradient graph compiler ──────────────────────────────────────────

/// Compile a primitive-op graph (post-autodiff lowering) to HLO.
///
/// `input_ids`: ordered NodeIds that become HLO parameter inputs (runtime values).
/// `output_ids`: ordered NodeIds that become the HLO computation outputs.
///
/// All other `parameter` nodes in the graph (not in input_ids) are treated as
/// constants and must have data accessible via the graph's constant_pool.
/// (Post-autodiff, parameter nodes ARE the runtime inputs — there are no
/// "weight parameters" embedded in the graph itself.)
pub fn compileGradientGraph(
    allocator: Allocator,
    graph: *const Graph,
    input_ids: []const NodeId,
    output_ids: []const NodeId,
) !CompileResult {
    const count = graph.nodeCount();
    const node_map = try allocator.alloc(?hlo.Id, count);
    defer allocator.free(node_map);
    @memset(node_map, null);

    // Build set of input node IDs for O(1) lookup.
    var input_set = std.AutoHashMapUnmanaged(NodeId, u32).empty; // NodeId → param index
    defer input_set.deinit(allocator);
    for (input_ids, 0..) |nid, i| {
        try input_set.put(allocator, nid, @intCast(i));
    }

    // Build add-reducer for reduce_sum / reduce_mean ops.
    const reducer_comp_id: hlo.Id = 1_000_000;
    var add_reducer = hlo.Builder.init(allocator, "add_reducer");
    defer add_reducer.deinit();
    add_reducer.next_id = reducer_comp_id + 1;
    add_reducer.base_id = reducer_comp_id + 1;
    const r_lhs = try add_reducer.parameter(0, hlo.Shape.scalar(.f32), "lhs");
    const r_rhs = try add_reducer.parameter(1, hlo.Shape.scalar(.f32), "rhs");
    _ = try add_reducer.add(r_lhs, r_rhs);
    var reducer_comp = add_reducer.build();
    reducer_comp.id = reducer_comp_id;

    // Build max-reducer for reduce_max ops.
    const max_reducer_comp_id: hlo.Id = 2_000_000;
    var max_reducer = hlo.Builder.init(allocator, "max_reducer");
    defer max_reducer.deinit();
    max_reducer.next_id = max_reducer_comp_id + 1;
    max_reducer.base_id = max_reducer_comp_id + 1;
    const mr_lhs = try max_reducer.parameter(0, hlo.Shape.scalar(.f32), "lhs");
    const mr_rhs = try max_reducer.parameter(1, hlo.Shape.scalar(.f32), "rhs");
    _ = try max_reducer.maximum(mr_lhs, mr_rhs);
    var max_reducer_comp = max_reducer.build();
    max_reducer_comp.id = max_reducer_comp_id;

    var b = hlo.Builder.init(allocator, "main");
    defer b.deinit();

    var needs_add_reducer = false;
    var needs_max_reducer = false;

    // Walk all nodes in topological order (graph stores them topologically).
    for (0..count) |i| {
        const node_id: NodeId = @intCast(i);
        const n = graph.node(node_id);
        const nid: usize = @intCast(node_id);

        // Runtime input: emit as HLO parameter.
        if (input_set.get(node_id)) |param_idx| {
            const shape = graphShapeToHlo(n);
            const name_buf = try std.fmt.allocPrint(allocator, "input_{d}", .{param_idx});
            defer allocator.free(name_buf);
            node_map[nid] = try b.parameter(param_idx, shape, name_buf);
            continue;
        }

        if (try emitPrimitiveNode(allocator, &b, n, node_map, nid, &reducer_comp, &max_reducer_comp, &needs_add_reducer, &needs_max_reducer)) continue;
        switch (n.op) {
            .parameter => {
                return error.MissingRuntimeInputParameter;
            },

            .constant => {
                node_map[nid] = try emitGraphConstant(&b, graph, n);
            },

            // Ops not supported in the gradient path
            .gather, .scatter_add, .conv_general, .average_pool => {
                return error.UnsupportedPrimitiveOp;
            },

            // Fused ops should never appear in a lowered gradient graph
            else => return error.FusedOpInGradientGraph,
        }
    }

    // Build root: single output or tuple.
    const output_node_ids = try allocator.dupe(NodeId, output_ids);
    errdefer allocator.free(output_node_ids);

    const input_node_ids_copy = try allocator.dupe(NodeId, input_ids);
    errdefer allocator.free(input_node_ids_copy);
    const input_bindings = try allocator.alloc(InputBinding, input_ids.len);
    errdefer allocator.free(input_bindings);
    for (input_ids, 0..) |input_id, i| {
        input_bindings[i] = .{ .graph_node = input_id };
    }

    if (output_ids.len == 0) {
        return error.NoOutputs;
    } else if (output_ids.len == 1) {
        const out_hlo = node_map[@intCast(output_ids[0])] orelse return error.MissingOutput;
        const last_id = b.instructions.items[b.instructions.items.len - 1].id;
        if (out_hlo != last_id) {
            _ = try b.reshape(out_hlo, graphShapeToHlo(graph.node(output_ids[0])));
        }
    } else {
        var hlo_outputs = try allocator.alloc(hlo.Id, output_ids.len);
        defer allocator.free(hlo_outputs);
        for (output_ids, 0..) |out_nid, oi| {
            hlo_outputs[oi] = node_map[@intCast(out_nid)] orelse return error.MissingOutput;
        }
        _ = try b.tuple(hlo_outputs);
    }

    const comp = b.build();
    var aux_list = std.ArrayListUnmanaged(hlo.Computation).empty;
    defer aux_list.deinit(allocator);
    if (needs_add_reducer) try aux_list.append(allocator, reducer_comp);
    if (needs_max_reducer) try aux_list.append(allocator, max_reducer_comp);
    const module = hlo.Module.initWithAux("gradient", comp, aux_list.items);
    const hlo_bytes = try module.serialize(allocator);
    errdefer allocator.free(hlo_bytes);
    const input_shapes = try buildInputShapesForBindings(allocator, graph, input_bindings);
    errdefer freeShapeSlices(allocator, input_shapes);
    const output_shapes = try cloneHloShapesForOutputNodeIds(allocator, &b, node_map, output_node_ids);
    errdefer freeShapeSlices(allocator, output_shapes);

    return .{
        .hlo_bytes = hlo_bytes,
        .input_bindings = input_bindings,
        .input_node_ids = input_node_ids_copy,
        .output_node_ids = output_node_ids,
        .input_shapes = input_shapes,
        .output_shapes = output_shapes,
        .allocator = allocator,
    };
}

/// Shape after reducing along the given axes (removed dimensions are dropped).
// ── Tests ───────────────────────────────────────────────────────────

const Shape = ml.graph.Shape;
const GraphBuilder = ml.graph.Builder;
const Capability = partition_mod.Capability;
const partition_fn = partition_mod.partition;

test "computeOutputs finds graph outputs in partition" {
    const alloc = std.testing.allocator;
    var g = Graph.init(alloc);
    defer g.deinit();
    var gb = GraphBuilder.init(&g);

    const x = try gb.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try gb.gelu(x);
    try g.markOutput(out);

    const caps = [_]Capability{
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var plan = try partition_fn(alloc, &g, &caps);
    defer plan.deinit();

    var part_set = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer part_set.deinit(alloc);
    for (plan.partitions[0].node_ids) |nid| try part_set.put(alloc, nid, {});

    const outputs = try computeOutputs(alloc, &g, &part_set);
    defer alloc.free(outputs);

    try std.testing.expect(outputs.len >= 1);
    var found = false;
    for (outputs) |o| {
        if (o == out) found = true;
    }
    try std.testing.expect(found);
}

test "graphDTypeToHlo mapping" {
    try std.testing.expectEqual(hlo.ElementType.f32, graphDTypeToHlo(.f32));
    try std.testing.expectEqual(hlo.ElementType.f32, graphDTypeToHlo(.f16));
    try std.testing.expectEqual(hlo.ElementType.f32, graphDTypeToHlo(.bf16));
    try std.testing.expectEqual(hlo.ElementType.s32, graphDTypeToHlo(.i32));
    try std.testing.expectEqual(hlo.ElementType.s64, graphDTypeToHlo(.i64));
    try std.testing.expectEqual(hlo.ElementType.u8, graphDTypeToHlo(.u8));
    try std.testing.expectEqual(hlo.ElementType.pred, graphDTypeToHlo(.bool_));
}

test "PJRT RoPE constants match half-split rotation layout" {
    const perm = try buildRopePermutationMatrix(std.testing.allocator, 4, 4, 4, false);
    defer std.testing.allocator.free(perm);

    try std.testing.expectEqual(@as(usize, 16), perm.len);
    try std.testing.expectEqual(@as(f32, -1.0), perm[2 * 4 + 0]);
    try std.testing.expectEqual(@as(f32, 1.0), perm[0 * 4 + 2]);
    try std.testing.expectEqual(@as(f32, -1.0), perm[3 * 4 + 1]);
    try std.testing.expectEqual(@as(f32, 1.0), perm[1 * 4 + 3]);

    const sin_mask = try buildRopeScaleMask(std.testing.allocator, 2, 4, 4, 4, 10000.0, 1.0, 0, false, .sin);
    defer std.testing.allocator.free(sin_mask);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), sin_mask[0], 1e-6);
    try std.testing.expectApproxEqAbs(@sin(@as(f32, 1.0)), sin_mask[4], 1e-6);
    try std.testing.expectApproxEqAbs(@sin(@as(f32, 1.0)), sin_mask[6], 1e-6);
}

test "PJRT capability includes static RoPE lowering" {
    try std.testing.expect(partition_mod.supportsPjrt(.{ .fused_rope = .{
        .seq_len = 1,
        .head_dim = 4,
        .rope_dim = 4,
        .theta = 10000.0,
        .freq_scale = 1.0,
    } }));
}

test "PJRT compiler lowers static batch-one GQA attention to HLO" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 3, 8 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 3, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 3, 4 }));
    const attn = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 3,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 3, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(attn);

    const node_ids = [_]NodeId{attn};
    const external_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = q, .source_partition = 0 },
        .{ .node_id = k, .source_partition = 0 },
        .{ .node_id = v, .source_partition = 0 },
    };
    const part: Partition = .{
        .backend = .pjrt,
        .node_ids = &node_ids,
        .external_inputs = &external_inputs,
    };
    const fake_cb: ComputeBackend = undefined;
    var result = try compilePartition(allocator, &graph, &part, &fake_cb);
    defer result.deinit();

    try std.testing.expect(result.hlo_bytes.len > 0);
    try std.testing.expectEqual(@as(usize, 3), result.input_bindings.len);
    try std.testing.expectEqual(@as(usize, 1), result.output_node_ids.len);
    try std.testing.expectEqual(@as(usize, 3), result.input_shapes.len);
    try std.testing.expectEqual(@as(usize, 1), result.output_shapes.len);
    try std.testing.expectEqualSlices(i64, &.{ 3, 8 }, result.input_shapes[0]);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, result.input_shapes[1]);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, result.input_shapes[2]);
    try std.testing.expectEqualSlices(i64, &.{ 3, 8 }, result.output_shapes[0]);
    try std.testing.expectEqual(attn, result.output_node_ids[0]);
}

test "PJRT compiler lowers graph constants" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 1, 2 }));
    const loc = try graph.internConstant(&.{ 1.0, 2.0 });
    const c = try graph.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = Shape.init(.f32, &.{ 1, 2 }),
    });
    const y = try graph.addNode(.{
        .op = .{ .fused_elem_add = {} },
        .output_shape = Shape.init(.f32, &.{ 1, 2 }),
        .inputs = .{ x, c, null_node, null_node },
        .num_inputs = 2,
    });
    try graph.markOutput(y);

    const node_ids = [_]NodeId{ c, y };
    const external_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = x, .source_partition = 0 },
    };
    const part: Partition = .{
        .backend = .pjrt,
        .node_ids = &node_ids,
        .external_inputs = &external_inputs,
    };
    const fake_cb: ComputeBackend = undefined;
    var result = try compilePartition(allocator, &graph, &part, &fake_cb);
    defer result.deinit();

    try std.testing.expect(result.hlo_bytes.len > 0);
    try std.testing.expectEqual(@as(usize, 1), result.input_bindings.len);
    try std.testing.expectEqual(@as(usize, 1), result.output_node_ids.len);
    try std.testing.expectEqual(y, result.output_node_ids[0]);
}

test "PJRT compiler can externalize graph parameters as inputs" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 1, 2 }));
    const w = try builder.parameter("w", Shape.init(.f32, &.{ 2, 2 }));
    const y = try graph.addNode(.{
        .op = .{ .fused_linear_no_bias = .{ .rows = 1, .in_dim = 2, .out_dim = 2 } },
        .output_shape = Shape.init(.f32, &.{ 1, 2 }),
        .inputs = .{ x, w, null_node, null_node },
        .num_inputs = 2,
    });
    try graph.markOutput(y);

    const node_ids = [_]NodeId{ w, y };
    const external_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = x, .source_partition = 0 },
    };
    const part: Partition = .{
        .backend = .pjrt,
        .node_ids = &node_ids,
        .external_inputs = &external_inputs,
    };
    const fake_cb: ComputeBackend = undefined;
    var result = try compilePartitionWithOptions(allocator, &graph, &part, &fake_cb, .{ .parameter_inputs = true });
    defer result.deinit();

    try std.testing.expect(result.hlo_bytes.len > 0);
    try std.testing.expectEqual(@as(usize, 2), result.input_bindings.len);
    try std.testing.expectEqual(@as(usize, 2), result.input_node_ids.len);
    try std.testing.expectEqual(x, result.input_node_ids[0]);
    try std.testing.expectEqual(w, result.input_node_ids[1]);
}

test "PJRT compiler semantic KV option adds past inputs and present outputs" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 1, 8 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 1, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 1, 4 }));
    const attn = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 0,
            .skip_kv_write = true,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(attn);

    const node_ids = [_]NodeId{attn};
    const external_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = q, .source_partition = 0 },
        .{ .node_id = k, .source_partition = 0 },
        .{ .node_id = v, .source_partition = 0 },
    };
    const part: Partition = .{
        .backend = .pjrt,
        .node_ids = &node_ids,
        .external_inputs = &external_inputs,
    };
    const fake_cb: ComputeBackend = undefined;
    var result = try compilePartitionWithOptions(allocator, &graph, &part, &fake_cb, .{ .semantic_kv_bindings = true });
    defer result.deinit();

    try std.testing.expect(result.hlo_bytes.len > 0);
    try std.testing.expectEqual(@as(usize, 5), result.input_bindings.len);
    try std.testing.expectEqual(@as(usize, 3), result.output_node_ids.len);
    try std.testing.expectEqual(attn, result.output_node_ids[0]);
    try std.testing.expectEqual(k, result.output_node_ids[1]);
    try std.testing.expectEqual(v, result.output_node_ids[2]);
}

test "PJRT semantic KV output selection keeps only the final graph output" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 1, 8 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 1, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 1, 4 }));
    const junk = try graph.addNode(.{
        .op = .{ .fused_elem_add = {} },
        .output_shape = Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q, q, null_node, null_node },
        .num_inputs = 2,
    });
    const attn = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 0,
            .skip_kv_write = false,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(junk);
    try graph.markOutput(attn);

    const node_ids = [_]NodeId{ junk, attn };
    var part_set = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer part_set.deinit(allocator);
    for (node_ids) |node_id| try part_set.put(allocator, node_id, {});

    const outputs = try computeOutputsWithOptions(allocator, &graph, &part_set, &node_ids, .{ .semantic_kv_bindings = true });
    defer allocator.free(outputs);
    try std.testing.expectEqual(@as(usize, 3), outputs.len);
    try std.testing.expectEqual(attn, outputs[0]);
    try std.testing.expectEqual(k, outputs[1]);
    try std.testing.expectEqual(v, outputs[2]);
}

test "PJRT compiler semantic decode inputs cover normal cache-writing attention" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 1, 8 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 1, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 1, 4 }));
    const attn = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .kv_seq_len = 3,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 0,
            .skip_kv_write = false,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(attn);

    const node_ids = [_]NodeId{attn};
    const external_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = q, .source_partition = 0 },
        .{ .node_id = k, .source_partition = 0 },
        .{ .node_id = v, .source_partition = 0 },
    };
    const part: Partition = .{
        .backend = .pjrt,
        .node_ids = &node_ids,
        .external_inputs = &external_inputs,
    };
    const fake_cb: ComputeBackend = undefined;

    var prefill_result = try compilePartitionWithOptions(allocator, &graph, &part, &fake_cb, .{ .semantic_kv_bindings = true });
    defer prefill_result.deinit();
    try std.testing.expectEqual(@as(usize, 3), prefill_result.input_bindings.len);
    try std.testing.expectEqual(@as(usize, 3), prefill_result.output_node_ids.len);

    var decode_result = try compilePartitionWithOptions(allocator, &graph, &part, &fake_cb, .{
        .semantic_kv_bindings = true,
        .semantic_kv_inputs = true,
    });
    defer decode_result.deinit();
    try std.testing.expectEqual(@as(usize, 5), decode_result.input_bindings.len);
    try std.testing.expectEqual(@as(usize, 3), decode_result.output_node_ids.len);
    try std.testing.expectEqual(@as(usize, 5), decode_result.input_shapes.len);
    try std.testing.expectEqual(@as(usize, 3), decode_result.output_shapes.len);
    try std.testing.expectEqualSlices(i64, &.{ 1, 8 }, decode_result.input_shapes[0]);
    try std.testing.expectEqualSlices(i64, &.{ 1, 4 }, decode_result.input_shapes[1]);
    try std.testing.expectEqualSlices(i64, &.{ 1, 4 }, decode_result.input_shapes[2]);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, decode_result.input_shapes[3]);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, decode_result.input_shapes[4]);
    try std.testing.expectEqualSlices(i64, &.{ 1, 8 }, decode_result.output_shapes[0]);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, decode_result.output_shapes[1]);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, decode_result.output_shapes[2]);
}
