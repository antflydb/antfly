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
    entry_layout_options: ?[]u8 = null,
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
    resident_output_start: ?usize = null,
    resident_query_rows: usize = 0,
    resident_dynamic_rows: bool = false,
    allocator: Allocator,

    pub fn deinit(self: *CompileResult) void {
        self.allocator.free(self.hlo_bytes);
        if (self.entry_layout_options) |bytes| self.allocator.free(bytes);
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
    /// Stable CUDA decode reductions; activations and affine math remain F32.
    wide_single_row_norm: bool = false,
    /// Evaluate compact rotary coefficients before their BF16 publication.
    wide_rotary_trig: bool = false,
    /// Fuse single-row projections as F32 products/reduction, without changing input precision.
    reduce_single_row_linear: bool = false,
    precision: @import("../execution/precision.zig").Policy = .f32,
    /// Request-private fixed-capacity state. Position is a runtime scalar;
    /// no sequence-length-specialized decode executables or host KV readbacks.
    resident_capacity: usize = 0,
    resident_initial: bool = false,
    resident_visibility: ?[]const u8 = null,
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

    const wide_reducer_id: hlo.Id = 3_000_000;
    var wide_add = hlo.Builder.init(allocator, "wide_norm_sum");
    defer wide_add.deinit();
    var wide_reducer: ?hlo.Computation = null;
    if (options.wide_single_row_norm) {
        wide_add.next_id = wide_reducer_id + 1;
        wide_add.base_id = wide_add.next_id;
        const wide_left = try wide_add.parameter(0, hlo.Shape.scalar(.f64), "lhs");
        const wide_right = try wide_add.parameter(1, hlo.Shape.scalar(.f64), "rhs");
        _ = try wide_add.add(wide_left, wide_right);
        wide_reducer = wide_add.build();
        wide_reducer.?.id = wide_reducer_id;
    }
    var needs_wide_norm = false;

    var b = hlo.Builder.init(allocator, "main");
    if (options.precision == .bf16) {
        b.matrix_operand_type = .bf16;
        b.gelu_schedule = .coefficient_first_stable_tail;
    }
    defer b.deinit();

    var recurrent_programs: std.ArrayListUnmanaged(*@import("pjrt_gated_delta.zig").Program) = .empty;
    defer {
        for (recurrent_programs.items) |program| {
            program.deinit();
            allocator.destroy(program);
        }
        recurrent_programs.deinit(allocator);
    }

    var next_param: u32 = 0;

    const ordinary_outputs = try computeOutputsWithOptions(allocator, graph, &part_set, part.node_ids, options);
    defer allocator.free(ordinary_outputs);
    // On resident decode, immutable cross K/V are boundary operands. Stop
    // reachability there before emitting/uploading their encoder ancestors.
    const prune_cross_state = cross: {
        if (options.resident_capacity == 0 or options.resident_initial) break :cross false;
        for (part.node_ids) |id| if (graph.node(id).op == .fused_cross_attention) break :cross true;
        break :cross false;
    };
    const needed = if (prune_cross_state) try allocator.alloc(bool, count) else null;
    defer if (needed) |reachable| allocator.free(reachable);
    if (needed) |reachable| {
        @memset(reachable, false);
        var pending: std.ArrayListUnmanaged(NodeId) = .empty;
        defer pending.deinit(allocator);
        for (ordinary_outputs) |output| {
            if (!reachable[output]) {
                reachable[output] = true;
                try pending.append(allocator, output);
            }
        }
        while (pending.pop()) |id| {
            const node = graph.node(id);
            for (node.getInputs(), 0..) |input, edge| {
                if (input == null_node or (node.op == .fused_cross_attention and (edge == 1 or edge == 2))) continue;
                if (input >= count) return error.UnsupportedShape;
                if (!reachable[input]) {
                    reachable[input] = true;
                    try pending.append(allocator, input);
                }
            }
        }
    }

    // ── 1. Create HLO parameters for external inputs ────────────

    var input_bindings_list = std.ArrayListUnmanaged(InputBinding).empty;
    errdefer input_bindings_list.deinit(allocator);
    var input_node_ids_list = std.ArrayListUnmanaged(NodeId).empty;
    errdefer input_node_ids_list.deinit(allocator);
    var semantic_past_inputs = std.AutoHashMapUnmanaged(NodeId, hlo.Id).empty;
    defer semantic_past_inputs.deinit(allocator);

    for (part.external_inputs) |ext_in| {
        if (needed) |reachable| if (!reachable[ext_in.node_id]) continue;
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

    if (options.resident_capacity != 0 and (options.semantic_kv_bindings or options.resident_capacity > 16_777_216)) return error.UnsupportedResidentDecode;
    var resident = ResidentLowering{ .capacity = options.resident_capacity };
    defer resident.outputs.deinit(allocator);
    defer resident.nodes.deinit(allocator);
    defer resident.kv.deinit(allocator);
    if (resident.capacity != 0) resident.position = if (options.resident_initial) try b.constantScalarF32(0) else try resident.input(allocator, &b, &input_bindings_list, &next_param, null_node, hlo.Shape.scalar(.f32));
    // Prebind this control independently of graph scheduling. It also selects
    // the public logits row; changing prompt length cannot specialize its value.
    if (resident.capacity != 0) for (part.node_ids) |node_id| {
        const node = graph.node(node_id);
        if (node.op != .parameter or !std.mem.eql(u8, graph.parameterName(node), "logits_row")) continue;
        if (node.output_shape.dtype != .i64 or node.output_shape.rank() != 1 or node.output_shape.dims[0] != 1) return error.UnsupportedResidentDecode;
        const input = node_map[node_id] orelse input: {
            const id = try b.parameter(next_param, graphShapeToHlo(node), "logits_row");
            next_param += 1;
            try input_bindings_list.append(allocator, .{ .graph_node = node_id });
            try input_node_ids_list.append(allocator, node_id);
            node_map[node_id] = id;
            break :input id;
        };
        resident.valid_rows = try b.add(try b.convertType(try b.reshape(input, hlo.Shape.scalar(.s64)), .s32), try b.constantS32(hlo.Shape.scalar(.s32), &.{1}));
    };

    // ── 2. Walk partition nodes, emit HLO ops ───────────────────

    // Secondary results belong to their producer. Topological scheduling may
    // interleave independent pairs before visiting either secondary consumer.
    var pair_seconds: std.AutoHashMapUnmanaged(NodeId, hlo.Id) = .empty;
    defer pair_seconds.deinit(allocator);
    // Track whether we need the reducer computation.
    var needs_reducer = false;
    var needs_max_reducer = false;

    const log_lowering_errors = @import("antfly_platform").env.getenvBool("TERMITE_PJRT_EXEC_DEBUG");
    for (part.node_ids) |node_id| {
        const nid: usize = @intCast(node_id);
        if (needed) |reachable| if (!reachable[nid]) continue;
        const n = graph.node(node_id);
        errdefer if (log_lowering_errors) std.log.warn("PJRT HLO lowering failed: node={d} op={s} shape={any}", .{ node_id, @tagName(n.op), n.output_shape.dims[0..n.output_shape.rank()] });
        const ins = n.getInputs();
        if (n.op == .fused_gated_delta) {
            const program = try allocator.create(@import("pjrt_gated_delta.zig").Program);
            program.* = .{ .condition = hlo.Builder.init(allocator, "delta_condition"), .body = hlo.Builder.init(allocator, "delta_body"), .condition_id = 0, .body_id = 0 };
            recurrent_programs.append(allocator, program) catch |err| {
                program.deinit();
                allocator.destroy(program);
                return err;
            };
            if (ins.len != 3) return error.UnsupportedShape;
            const attrs = n.op.fused_gated_delta;
            if (resident.capacity != 0) try resident.queryRows(attrs.rows);
            const initial = if (resident.capacity != 0 and !options.resident_initial) try resident.input(allocator, &b, &input_bindings_list, &next_param, node_id, hlo.Shape.init(.f32, &.{ attrs.value_heads, attrs.key_dim, attrs.value_dim })) else null;
            const emitted = try program.emitWithState(allocator, &b, try getHloId(node_map, ins[0]), try getHloId(node_map, ins[1]), try getHloId(node_map, ins[2]), attrs, reducer_comp.id, recurrent_programs.items.len - 1, initial, resident.valid_rows);
            node_map[nid] = emitted.output;
            if (resident.capacity != 0) try resident.output(allocator, node_id, emitted.state);
            needs_reducer = true;
            continue;
        }

        if (resident.capacity != 0 and n.op == .conv_general) {
            const attrs = n.op.conv_general;
            // Only the canonical causal depthwise temporal layout owns history.
            // Image/encoder convolutions must never become decoder state.
            if (attrs.num_spatial == 1 and attrs.padding[0][0] > 0 and attrs.padding[0][1] == 0 and attrs.groups > 0) {
                const x = try getHloId(node_map, ins[0]);
                const w = try getHloId(node_map, ins[1]);
                const xs = b.getInst(x).shape;
                const ws = b.getInst(w).shape;
                const history = attrs.padding[0][0];
                if (xs.dimensions.len == 3) try resident.queryRows(@intCast(xs.dimensions[2]));
                if (xs.dimensions.len != 3 or ws.dimensions.len != 3 or xs.dimensions[0] != 1 or xs.dimensions[1] != attrs.groups or ws.dimensions[1] != 1 or ws.dimensions[2] != history + 1 or attrs.strides[0] != 1 or attrs.dilations[0] != 1) return error.UnsupportedResidentDecode;
                const shape = hlo.Shape.init(.f32, &.{ 1, xs.dimensions[1], history });
                var past = if (options.resident_initial) try b.broadcast(try b.constantScalarF32(0), &.{}, shape) else try resident.input(allocator, &b, &input_bindings_list, &next_param, node_id, shape);
                if (options.precision == .bf16 and (try b.provenBf16(x)) != null) past = try roundBf16(&b, past, true);
                const combined_shape = hlo.Shape.init(.f32, &.{ 1, xs.dimensions[1], history + xs.dimensions[2] });
                const combined = try b.concatenate(&.{ past, x }, 2, combined_shape);
                var conv = try convolutionAttrs(attrs);
                conv.padding_low[0] = 0;
                node_map[nid] = try b.convolution(combined, w, graphShapeToHlo(n), conv);
                const tail = if (resident.valid_rows) |limit| tail: {
                    const zero = try b.constantS32(hlo.Shape.scalar(.s32), &.{0});
                    break :tail try b.dynamicSlice(combined, &.{ zero, zero, limit }, shape.dimensions);
                } else try b.slice(combined, &.{ 0, 0, xs.dimensions[2] }, combined_shape.dimensions, &.{ 1, 1, 1 }, shape);
                try resident.output(allocator, node_id, tail);
                continue;
            }
        }
        if (try emitPrimitiveNode(allocator, &b, n, node_map, nid, &reducer_comp, &max_reducer_comp, &needs_reducer, &needs_max_reducer)) continue;

        switch (n.op) {
            .fused_zero_tensor => {
                node_map[nid] = try b.broadcast(try b.constantScalarF32(0), &.{}, graphShapeToHlo(n));
            },
            .parameter => {
                if (node_map[nid] != null) continue;
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
                node_map[nid] = try floatingConstant(&b, shape, f32_data);
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
                const mm = try emitLinear(&b, x, w, wt, options.reduce_single_row_linear, &reducer_comp, &needs_reducer);
                node_map[nid] = try b.add(mm, bias);
            },

            .fused_linear_no_bias => {
                const x = try getHloId(node_map, ins[0]);
                const w = try getHloId(node_map, ins[1]);
                const wt = try transposeLastTwo(&b, w);
                node_map[nid] = try emitLinear(&b, x, w, wt, options.reduce_single_row_linear, &reducer_comp, &needs_reducer);
            },

            .fused_linear_no_bias_pair => {
                const x = try getHloId(node_map, ins[0]);
                const w1 = try getHloId(node_map, ins[1]);
                const w2 = try getHloId(node_map, ins[2]);
                const wt1 = try transposeLastTwo(&b, w1);
                const wt2 = try transposeLastTwo(&b, w2);
                node_map[nid] = try emitLinear(&b, x, w1, wt1, options.reduce_single_row_linear, &reducer_comp, &needs_reducer);
                try pair_seconds.put(allocator, node_id, try emitLinear(&b, x, w2, wt2, options.reduce_single_row_linear, &reducer_comp, &needs_reducer));
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
                const norm_elements = n.output_shape.numElements() orelse return error.UnsupportedShape;
                const wide = if (options.wide_single_row_norm and norm_elements == attrs.dim) &wide_reducer.? else null;
                node_map[nid] = try emitLayerNorm(&b, x, gamma, beta, attrs.eps, &reducer_comp, wide);
                needs_wide_norm = needs_wide_norm or wide != null;
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

            .fused_add_mul_scalar => |attrs| {
                const x = try getHloId(node_map, ins[0]);
                const y = try getHloId(node_map, ins[1]);
                const scalar = try getHloId(node_map, ins[2]);
                node_map[nid] = if (attrs.scale_before_add)
                    try b.add(try b.multiply(x, scalar), y)
                else
                    try b.multiply(try b.add(x, y), scalar);
            },

            .fused_embedding_lookup => {
                const table = try getHloId(node_map, ins[0]);
                const ids_ct = try getHloId(node_map, ins[1]);
                const result_shape = graphShapeToHlo(n);
                node_map[nid] = try b.gather(table, ids_ct, result_shape);
            },

            .fused_rope => |attrs| {
                if (resident.capacity != 0) {
                    node_map[nid] = try emitResidentRope(allocator, &b, n, try getHloId(node_map, ins[0]), attrs, resident.position.?, resident.capacity, options.precision == .bf16, options.wide_rotary_trig);
                    continue;
                }
                node_map[nid] = try emitStaticRope(
                    allocator,
                    &b,
                    n,
                    try getHloId(node_map, ins[0]),
                    attrs,
                );
            },

            .fused_cross_attention => |attrs| {
                if (resident.capacity == 0 or attrs.batch != 1 or ins.len != 4) return error.UnsupportedResidentDecode;
                try resident.queryRows(attrs.dec_seq);
                const shape = hlo.Shape.init(.f32, &.{ attrs.enc_seq, @as(i64, attrs.num_heads) * attrs.head_dim });
                const key = if (options.resident_initial) try getHloId(node_map, ins[1]) else try resident.input(allocator, &b, &input_bindings_list, &next_param, ins[1], shape);
                const value = if (options.resident_initial) try getHloId(node_map, ins[2]) else try resident.input(allocator, &b, &input_bindings_list, &next_param, ins[2], shape);
                try resident.output(allocator, ins[1], key);
                try resident.output(allocator, ins[2], value);
                node_map[nid] = try emitResidentCrossAttention(&b, n, try getHloId(node_map, ins[0]), key, value, try getHloId(node_map, ins[3]), attrs, &reducer_comp, &max_reducer_comp, options.precision == .bf16);
                needs_reducer = true;
                needs_max_reducer = true;
            },

            .fused_gqa_causal_attention => |attrs| {
                const q = try getHloId(node_map, ins[0]);
                const k = try getHloId(node_map, ins[1]);
                const v = try getHloId(node_map, ins[2]);
                const bias = if (n.num_inputs > 3 and ins[3] != null_node) try getHloId(node_map, ins[3]) else null;
                if (resident.capacity != 0) {
                    if (attrs.batch != 1 or attrs.seq_len > resident.capacity or bias != null) return error.UnsupportedResidentDecode;
                    try resident.queryRows(attrs.seq_len);
                    const width = @as(i64, if (attrs.num_kv_heads == 0) attrs.num_heads else attrs.num_kv_heads) * attrs.head_dim;
                    const shape = hlo.Shape.init(.f32, &.{ @intCast(resident.capacity), width });
                    var present_k = resident.kv.get(ins[1]);
                    var present_v = resident.kv.get(ins[2]);
                    if ((present_k == null) != (present_v == null)) return error.UnsupportedResidentDecode;
                    if (present_k == null) {
                        if (attrs.skip_kv_write) return error.UnsupportedResidentDecode;
                        const old_k = if (options.resident_initial) try b.broadcast(try b.constantScalarF32(0), &.{}, shape) else try resident.input(allocator, &b, &input_bindings_list, &next_param, ins[1], shape);
                        const old_v = if (options.resident_initial) old_k else try resident.input(allocator, &b, &input_bindings_list, &next_param, ins[2], shape);
                        const zero = try b.constantS32(hlo.Shape.scalar(.s32), &.{0});
                        const position = try b.convertType(resident.position.?, .s32);
                        present_k = try b.dynamicUpdateSlice(old_k, k, &.{ position, zero });
                        present_v = try b.dynamicUpdateSlice(old_v, v, &.{ position, zero });
                        try resident.output(allocator, ins[1], present_k.?);
                        try resident.output(allocator, ins[2], present_v.?);
                        try resident.kv.put(allocator, ins[1], present_k.?);
                        try resident.kv.put(allocator, ins[2], present_v.?);
                    }
                    var attention_attrs = attrs;
                    attention_attrs.skip_kv_write = false;
                    // Prefill produces banks; decode consumes them. Attend
                    // directly to the initial prompt, not unused future slots.
                    // A widened BF16 carrier retains its exact storage proof.
                    const attention_k = if (options.resident_initial) k else if (options.precision == .bf16 and (try b.provenBf16(k)) != null) try roundBf16(&b, present_k.?, true) else present_k.?;
                    const attention_v = if (options.resident_initial) v else present_v.?;
                    const emitted = try emitStaticGqaAttention(allocator, &b, n, q, attention_k, attention_v, null, attention_attrs, &reducer_comp, &max_reducer_comp, null, null, resident.position, options.resident_visibility);
                    node_map[nid] = emitted.output;
                    needs_reducer = true;
                    needs_max_reducer = true;
                    continue;
                }
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
                    null,
                    null,
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

    const output_node_ids = try allocator.alloc(NodeId, ordinary_outputs.len + resident.outputs.items.len);
    errdefer allocator.free(output_node_ids);
    @memcpy(output_node_ids[0..ordinary_outputs.len], ordinary_outputs);
    @memcpy(output_node_ids[ordinary_outputs.len..], resident.nodes.items);
    if (output_node_ids.len == 0) return error.NoOutputs;
    const hlo_output_ids = try allocator.alloc(hlo.Id, output_node_ids.len);
    defer allocator.free(hlo_output_ids);
    for (ordinary_outputs, 0..) |nid, i| hlo_output_ids[i] = node_map[@intCast(nid)] orelse return error.MissingOutput;
    @memcpy(hlo_output_ids[ordinary_outputs.len..], resident.outputs.items);
    if (hlo_output_ids.len == 1) {
        const root = hlo_output_ids[0];
        _ = try b.reshape(root, b.getInst(root).shape);
    } else _ = try b.tuple(hlo_output_ids);

    // ── 4. Build & serialize HloModuleProto ─────────────────────

    const comp = b.build();

    var aux_list = std.ArrayListUnmanaged(hlo.Computation).empty;
    defer aux_list.deinit(allocator);
    if (needs_reducer) try aux_list.append(allocator, reducer_comp);
    if (needs_max_reducer) try aux_list.append(allocator, max_reducer_comp);
    if (needs_wide_norm) try aux_list.append(allocator, wide_reducer.?);
    for (recurrent_programs.items) |program| try aux_list.appendSlice(allocator, &program.computations());
    const module = hlo.Module.initWithAux("partition", comp, aux_list.items);
    const entry_layout_options = if (resident.capacity != 0) try module.serializeEntryLayoutOptions(allocator) else null;
    errdefer if (entry_layout_options) |bytes| allocator.free(bytes);
    const hlo_bytes = try module.serialize(allocator);
    errdefer allocator.free(hlo_bytes);
    const input_bindings = try input_bindings_list.toOwnedSlice(allocator);
    errdefer allocator.free(input_bindings);
    const input_node_ids = try input_node_ids_list.toOwnedSlice(allocator);
    errdefer allocator.free(input_node_ids);
    const input_shapes = if (resident.capacity != 0) try cloneParameterShapes(allocator, &b, input_bindings.len) else try buildInputShapesForBindings(allocator, graph, input_bindings);
    errdefer freeShapeSlices(allocator, input_shapes);
    const output_shapes = try allocator.alloc([]i64, hlo_output_ids.len);
    var shapes_initialized: usize = 0;
    errdefer {
        for (output_shapes[0..shapes_initialized]) |shape| allocator.free(shape);
        allocator.free(output_shapes);
    }
    for (hlo_output_ids, output_shapes) |id, *shape| {
        shape.* = try allocator.dupe(i64, b.getInst(id).shape.dimensions);
        shapes_initialized += 1;
    }
    errdefer freeShapeSlices(allocator, output_shapes);

    return .{
        .hlo_bytes = hlo_bytes,
        .entry_layout_options = entry_layout_options,
        .input_bindings = input_bindings,
        .input_node_ids = input_node_ids,
        .output_node_ids = output_node_ids,
        .input_shapes = input_shapes,
        .output_shapes = output_shapes,
        .allocator = allocator,
        .resident_output_start = if (resident.capacity != 0) ordinary_outputs.len else null,
        .resident_query_rows = resident.rows,
        .resident_dynamic_rows = resident.valid_rows != null,
    };
}

/// Fixed immutable cross banks share the existing head layout and reduction
/// programs. Visibility remains dynamic, including masked NaNs and empty rows.
fn emitResidentCrossAttention(b: *hlo.Builder, node: *const Node, q: hlo.Id, k: hlo.Id, v: hlo.Id, visibility: hlo.Id, attrs: ml.graph.node.CrossAttentionAttrs, add: *const hlo.Computation, maximum: *const hlo.Computation, bf16: bool) !hlo.Id {
    const heads: i64 = attrs.num_heads;
    const rows: i64 = attrs.dec_seq;
    const keys: i64 = attrs.enc_seq;
    const dim: i64 = attrs.head_dim;
    const qh = try reshapeAttentionInputToHeads(b, q, rows, heads, dim);
    const kh = try reshapeAttentionInputToHeads(b, k, keys, heads, dim);
    var vh = try reshapeAttentionInputToHeads(b, v, keys, heads, dim);
    const scores_shape = hlo.Shape.init(.f32, &.{ heads, rows, keys });
    var scores = try b.dot(qh, try b.transpose(kh, &.{ 0, 2, 1 }, hlo.Shape.init(.f32, &.{ heads, dim, keys })), scores_shape, .{ .lhs_contracting_dimensions = &.{2}, .rhs_contracting_dimensions = &.{1}, .lhs_batch_dimensions = &.{0}, .rhs_batch_dimensions = &.{0} });
    scores = try b.multiply(scores, try b.constantScalarF32(1 / @sqrt(@as(f32, @floatFromInt(dim)))));
    const mask = try b.reshape(visibility, hlo.Shape.init(.pred, &.{keys}));
    const visible = try b.broadcast(mask, &.{2}, hlo.Shape.init(.pred, scores_shape.dimensions));
    scores = try b.select(visible, scores, try b.broadcast(try b.constantScalarF32(-std.math.inf(f32)), &.{}, scores_shape));
    const value_shape = hlo.Shape.init(.f32, &.{ heads, keys, dim });
    vh = try b.select(try b.broadcast(mask, &.{1}, hlo.Shape.init(.pred, value_shape.dimensions)), vh, try b.broadcast(try b.constantScalarF32(0), &.{}, value_shape));
    const reduced = hlo.Shape.init(.f32, &.{ heads, rows });
    const max_value = try b.maximum(try b.reduce(scores, try b.constantScalarF32(-std.math.inf(f32)), &.{2}, reduced, maximum.id), try b.constantScalarF32(-std.math.floatMax(f32)));
    const exp = try b.exponential(try b.subtract(scores, try b.broadcast(max_value, &.{ 0, 1 }, scores_shape)));
    const sums = try b.maximum(try b.reduce(exp, try b.constantScalarF32(0), &.{2}, reduced, add.id), try b.constantScalarF32(1));
    var probabilities = try b.divide(exp, try b.broadcast(sums, &.{ 0, 1 }, scores_shape));
    if (bf16) probabilities = try roundBf16(b, probabilities, true);
    const attended = try b.dot(probabilities, vh, hlo.Shape.init(.f32, &.{ heads, rows, dim }), .{ .lhs_contracting_dimensions = &.{2}, .rhs_contracting_dimensions = &.{1}, .lhs_batch_dimensions = &.{0}, .rhs_batch_dimensions = &.{0} });
    return b.reshape(try b.transpose(attended, &.{ 1, 0, 2 }, hlo.Shape.init(.f32, &.{ rows, heads, dim })), graphShapeToHlo(node));
}

const ResidentLowering = struct {
    capacity: usize,
    position: ?hlo.Id = null,
    valid_rows: ?hlo.Id = null,
    rows: usize = 0,
    outputs: std.ArrayListUnmanaged(hlo.Id) = .empty,
    nodes: std.ArrayListUnmanaged(NodeId) = .empty,
    kv: std.AutoHashMapUnmanaged(NodeId, hlo.Id) = .empty,

    fn queryRows(self: *@This(), rows: usize) !void {
        if (rows == 0 or (self.rows != 0 and self.rows != rows)) return error.UnsupportedResidentDecode;
        self.rows = rows;
    }
    fn input(_: *@This(), a: Allocator, b: *hlo.Builder, bindings: *std.ArrayListUnmanaged(InputBinding), next: *u32, node: NodeId, shape: hlo.Shape) !hlo.Id {
        const id = try b.parameter(next.*, shape, "resident_state");
        next.* += 1;
        try bindings.append(a, .{ .semantic_past_graph_node = node });
        return id;
    }
    fn output(self: *@This(), a: Allocator, node: NodeId, id: hlo.Id) !void {
        try self.outputs.append(a, id);
        try self.nodes.append(a, node);
    }
};

fn cloneParameterShapes(a: Allocator, b: *const hlo.Builder, count: usize) ![][]i64 {
    const shapes = try a.alloc([]i64, count);
    @memset(shapes, &.{});
    errdefer {
        for (shapes) |shape| a.free(shape);
        a.free(shapes);
    }
    for (b.instructions.items) |inst| if (inst.parameter_number) |index| {
        if (index >= count) return error.InvalidParameterIndex;
        shapes[index] = try a.dupe(i64, inst.shape.dimensions);
    };
    return shapes;
}

fn residentRange(a: Allocator, b: *hlo.Builder, count: i64) !hlo.Id {
    const values = try a.alloc(i32, @intCast(count));
    defer a.free(values);
    for (values, 0..) |*value, i| value.* = @intCast(i);
    return b.constantS32(hlo.Shape.init(.s32, &.{count}), values);
}

fn emitCausalVisibility(a: Allocator, b: *hlo.Builder, heads: i64, rows: i64, keys: i64, window: u32, position: hlo.Id, visibility: ?[]const u8) !hlo.Id {
    const grid = hlo.Shape.init(.s32, &.{ rows, keys });
    const query = try b.broadcast(try b.add(try residentRange(a, b, rows), try b.convertType(position, .s32)), &.{0}, grid);
    const key = try b.broadcast(try residentRange(a, b, keys), &.{1}, grid);
    var allowed = try b.compare(query, key, "GE");
    if (visibility) |mask| {
        if (rows > keys or mask.len != try std.math.mul(usize, @intCast(rows), @intCast(rows))) return error.InvalidAttentionMask;
        var blocks = try b.constantPred(hlo.Shape.init(.pred, &.{ rows, rows }), mask);
        if (keys != rows) {
            const tail_shape = hlo.Shape.init(.pred, &.{ rows, keys - rows });
            const tail = try b.broadcast(try b.constantPred(hlo.Shape.scalar(.pred), &.{0}), &.{}, tail_shape);
            blocks = try b.concatenate(&.{ blocks, tail }, 1, hlo.Shape.init(.pred, &.{ rows, keys }));
        }
        allowed = try b.logicalOr(allowed, blocks);
    }
    if (window != 0) {
        const window_span = std.math.cast(i32, window - 1) orelse return error.UnsupportedAttentionShape;
        const span = try b.broadcast(try b.constantS32(hlo.Shape.scalar(.s32), &.{window_span}), &.{}, grid);
        allowed = try b.logicalAnd(allowed, try b.compare(key, try b.subtract(query, span), "GE"));
    }
    return b.broadcast(allowed, &.{ 1, 2 }, hlo.Shape.init(.pred, &.{ heads, rows, keys }));
}

/// A row-vector projection is a bandwidth-bound reduction, rather than a matrix
/// multiplication. Keep the graph's exact F32 operands (including prior BF16
/// publication) and let XLA fuse product/reduction without a GEMM custom call.
/// Multi-row projections retain the provider's tuned matrix multiplication.
fn emitLinear(b: *hlo.Builder, x: hlo.Id, weight: hlo.Id, transposed: hlo.Id, enabled: bool, reducer: *const hlo.Computation, needs_reducer: *bool) !hlo.Id {
    const xs = b.getInst(x).shape;
    const ws = b.getInst(weight).shape;
    if (!enabled or xs.element_type != .f32 or ws.element_type != .f32 or xs.dimensions.len != 2 or ws.dimensions.len != 2 or xs.dimensions[0] != 1)
        return b.matmul(x, transposed);
    if (xs.dimensions[1] != ws.dimensions[1]) return error.UnsupportedShape;
    const vector = try b.reshape(x, hlo.Shape.init(.f32, &.{xs.dimensions[1]}));
    const products = try b.multiply(weight, try b.broadcast(vector, &.{1}, ws));
    const sums = try b.reduce(products, try b.constantScalarF32(0), &.{1}, hlo.Shape.init(.f32, &.{ws.dimensions[0]}), reducer.id);
    needs_reducer.* = true;
    return b.reshape(sums, hlo.Shape.init(.f32, &.{ 1, ws.dimensions[0] }));
}

fn roundBf16(b: *hlo.Builder, id: hlo.Id, enabled: bool) !hlo.Id {
    return if (enabled) b.convertType(try b.convertType(id, .bf16), .f32) else id;
}

fn emitResidentRope(a: Allocator, b: *hlo.Builder, node: *const Node, x: hlo.Id, attrs: ml.graph.node.RopeAttrs, position: hlo.Id, capacity: usize, bf16: bool, wide_trig: bool) !hlo.Id {
    const shape = node.output_shape;
    if (shape.rank() != 2 or shape.dims[0] != attrs.seq_len or attrs.seq_len > capacity) return error.UnsupportedResidentDecode;
    const rows: usize = @intCast(shape.dims[0]);
    const width: usize = @intCast(shape.dims[1]);
    const dim: usize = attrs.head_dim;
    const rotary: usize = if (attrs.rope_dim == 0) dim else attrs.rope_dim;
    if (dim == 0 or width % dim != 0 or rotary == 0 or rotary > dim or rotary % 2 != 0) return error.UnsupportedResidentDecode;
    const frequencies = try a.alloc(f32, dim);
    defer a.free(frequencies);
    const active = try a.alloc(u8, width);
    defer a.free(active);
    for (frequencies, 0..) |*frequency, lane| {
        const pair = if (attrs.consecutive_pairs) lane / 2 else lane % (dim / 2);
        const enabled = pair < rotary / 2 and (if (attrs.consecutive_pairs) lane < rotary else true);
        frequency.* = if (!enabled) 0 else 1 / std.math.pow(f32, attrs.theta, @as(f32, @floatFromInt(pair * 2)) / @as(f32, @floatFromInt(rotary)));
    }
    for (active, 0..) |*flag, col| {
        const lane = col % dim;
        const pair = if (attrs.consecutive_pairs) lane / 2 else lane % (dim / 2);
        flag.* = @intFromBool(pair < rotary / 2 and (if (attrs.consecutive_pairs) lane < rotary else true));
    }
    // Coefficients belong to a rotary lane, not an attention head. Sharing
    // this compact grid also lets Q and K reuse the same trigonometry.
    const compact = hlo.Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) });
    const hs = hlo.Shape.init(.f32, &.{ @intCast(rows), @intCast(width) });
    const positions = try b.add(try b.convertType(try residentRange(a, b, @intCast(rows)), .f32), position);
    const scaled = try b.multiply(positions, try b.constantScalarF32(attrs.freq_scale));
    const angles = try b.multiply(try b.broadcast(scaled, &.{0}, compact), try b.broadcast(try b.constantF32(hlo.Shape.init(.f32, &.{@intCast(dim)}), frequencies), &.{1}, compact));
    const trig_angles = if (wide_trig) try b.convertType(angles, .f64) else angles;
    var cosine = try b.cosine(trig_angles);
    var sine = try b.sine(trig_angles);
    if (wide_trig) {
        cosine = try b.convertType(cosine, .f32);
        sine = try b.convertType(sine, .f32);
    }
    const head_shape = hlo.Shape.init(.f32, &.{ @intCast(rows), @intCast(width / dim), @intCast(dim) });
    const cosines = try b.reshape(try b.broadcast(try roundBf16(b, cosine, bf16), &.{ 0, 2 }, head_shape), hs);
    const sines = try b.reshape(try b.broadcast(try roundBf16(b, sine, bf16), &.{ 0, 2 }, head_shape), hs);
    const rotated = try emitRopePermutation(b, x, rows, width, dim, rotary, attrs.consecutive_pairs);
    const result = try roundBf16(b, try b.add(try roundBf16(b, try b.multiply(x, cosines), bf16), try roundBf16(b, try b.multiply(rotated, sines), bf16)), bf16);
    if (!bf16 or rotary == dim) return result;
    const predicate = try b.broadcast(try b.constantPred(hlo.Shape.init(.pred, &.{@intCast(width)}), active), &.{1}, hlo.Shape.init(.pred, hs.dimensions));
    return b.select(predicate, result, x);
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
fn emitLayerNorm(b: *hlo.Builder, x: hlo.Id, gamma: hlo.Id, beta: hlo.Id, eps: f32, reducer: *const hlo.Computation, wide: ?*const hlo.Computation) !hlo.Id {
    const input = if (wide != null) try b.convertType(x, .f64) else x;
    const x_shape = b.getInst(input).shape;
    const reduction = wide orelse reducer;
    const rank = x_shape.dimensions.len;
    const last_dim_size = x_shape.dimensions[rank - 1];
    const size = try b.constantScalarF32(@floatFromInt(last_dim_size));
    const dim_size_f = if (wide != null) try b.convertType(size, .f64) else size;
    const epsilon = try b.constantScalarF32(eps);
    const eps_const = if (wide != null) try b.convertType(epsilon, .f64) else epsilon;

    const reduced_shape = reducedShape(x_shape, rank);
    const zero_f32 = try b.constantScalarF32(0.0);
    const zero = if (wide != null) try b.convertType(zero_f32, .f64) else zero_f32;
    const reduce_dims: []const i64 = &.{@as(i64, @intCast(rank - 1))};

    // mean(x)
    const sum_x = try b.reduce(input, zero, reduce_dims, reduced_shape, reduction.id);
    const mean_x = try b.divide(sum_x, dim_size_f);
    const mean_bc = try b.broadcast(mean_x, broadcastDimsExceptLast(rank), x_shape);

    // x - mean
    const centered = try b.subtract(input, mean_bc);

    // var = mean((x - mean)^2)
    const centered_sq = try b.multiply(centered, centered);
    const sum_sq = try b.reduce(centered_sq, zero, reduce_dims, reduced_shape, reduction.id);
    const var_x = try b.divide(sum_sq, dim_size_f);

    // rsqrt(var + eps)
    const variance = try b.add(var_x, eps_const);
    const inv_std = try b.rsqrt(variance);
    const inv_std_bc = try b.broadcast(inv_std, broadcastDimsExceptLast(rank), x_shape);

    // (x - mean) * rsqrt(var + eps) * gamma + beta
    const normed = try b.multiply(centered, inv_std_bc);
    const published = if (wide != null) try b.convertType(normed, .f32) else normed;
    const scaled = try b.multiply(published, gamma);
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

const buildRopeScaleMask = @import("rotary_program.zig").coefficients;

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
        .where_select => {
            const shape = graphShapeToHlo(n);
            var predicate = try getHloId(node_map, ins[0]);
            if (b.getInst(predicate).shape.element_type != .pred) predicate = try b.convertType(predicate, .pred);
            predicate = try broadcastSelectOperand(b, predicate, hlo.Shape.init(.pred, shape.dimensions));
            const yes = try broadcastSelectOperand(b, try getHloId(node_map, ins[1]), shape);
            const no = try broadcastSelectOperand(b, try getHloId(node_map, ins[2]), shape);
            node_map[nid] = try b.select(predicate, yes, no);
        },

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

/// HLO select has no implicit broadcasting and requires a predicate operand.
/// Preserve the semantic IR's trailing-axis broadcasting explicitly.
fn broadcastSelectOperand(b: *hlo.Builder, input: hlo.Id, target: hlo.Shape) !hlo.Id {
    const source = b.getInst(input).shape;
    if (source.element_type != target.element_type or source.dimensions.len > target.dimensions.len or target.dimensions.len > 8) return error.UnsupportedShape;
    if (std.mem.eql(i64, source.dimensions, target.dimensions)) return input;
    var axes: [8]i64 = undefined;
    const offset = target.dimensions.len - source.dimensions.len;
    for (source.dimensions, 0..) |size, axis| {
        if (size != 1 and size != target.dimensions[offset + axis]) return error.UnsupportedShape;
        axes[axis] = @intCast(offset + axis);
    }
    return b.broadcast(input, axes[0..source.dimensions.len], target);
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
    const kv: i64 = if (attrs.num_kv_heads == 0) heads else attrs.num_kv_heads;
    if (kv == 0 or @mod(heads, kv) != 0) return error.UnsupportedAttentionShape;
    const groups = @divExact(heads, kv);
    const query_flat = try b.reshape(q, hlo.Shape.init(.f32, &.{ rows, kv, groups, dim }));
    const query = try b.transpose(query_flat, &.{ 1, 2, 0, 3 }, hlo.Shape.init(.f32, &.{ kv, groups, rows, dim }));
    const key = try reshapeAttentionInputToHeads(b, k, columns, kv, dim);
    const value = try reshapeAttentionInputToHeads(b, v, columns, kv, dim);
    const key_t = try b.transpose(key, &.{ 0, 2, 1 }, hlo.Shape.init(.f32, &.{ kv, dim, columns }));
    const score_shape = hlo.Shape.init(.f32, &.{ kv, groups, rows, columns });
    var scores = try b.dot(query, key_t, score_shape, .{ .lhs_contracting_dimensions = &.{3}, .rhs_contracting_dimensions = &.{1}, .lhs_batch_dimensions = &.{0}, .rhs_batch_dimensions = &.{0} });
    scores = try b.multiply(scores, try b.constantScalarF32(attrs.score_scale orelse (1 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim))))));
    const mask = try b.broadcast(visible.?, &.{ 2, 3 }, hlo.Shape.init(.pred, score_shape.dimensions));
    const negative_infinity = try b.constantScalarF32(-std.math.inf(f32));
    scores = try b.select(mask, scores, try b.broadcast(negative_infinity, &.{}, score_shape));
    const row_shape = hlo.Shape.init(.f32, &.{ kv, groups, rows });
    const maximum = try b.reduce(scores, negative_infinity, &.{3}, row_shape, max_reducer.id);
    const safe_maximum = try b.maximum(maximum, try b.constantScalarF32(-std.math.floatMax(f32)));
    const exp = try b.exponential(try b.subtract(scores, try b.broadcast(safe_maximum, &.{ 0, 1, 2 }, score_shape)));
    const sums = try b.reduce(exp, zero, &.{3}, row_shape, add_reducer.id);
    const one = try b.constantScalarF32(1);
    const denominator = try b.maximum(sums, one);
    const probabilities = if (attrs.bf16_probability)
        try b.convertType(try b.convertType(exp, .bf16), .f32)
    else
        try b.divide(exp, try b.broadcast(denominator, &.{ 0, 1, 2 }, score_shape));
    const output_shape = hlo.Shape.init(.f32, &.{ kv, groups, rows, dim });
    var attended = try b.dot(probabilities, value, output_shape, .{ .lhs_contracting_dimensions = &.{3}, .rhs_contracting_dimensions = &.{1}, .lhs_batch_dimensions = &.{0}, .rhs_batch_dimensions = &.{0} });
    if (attrs.bf16_probability) attended = try b.divide(attended, try b.broadcast(denominator, &.{ 0, 1, 2 }, output_shape));
    const active = try b.compare(sums, try b.broadcast(zero, &.{}, row_shape), "NE");
    attended = try b.select(try b.broadcast(active, &.{ 0, 1, 2 }, hlo.Shape.init(.pred, output_shape.dimensions)), attended, try b.broadcast(zero, &.{}, output_shape));
    const transposed = try b.transpose(attended, &.{ 2, 0, 1, 3 }, hlo.Shape.init(.f32, &.{ rows, kv, groups, dim }));
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
    runtime_position: ?hlo.Id,
    visibility: ?[]const u8,
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
    if (runtime_position != null or past_k == null or (attrs.sliding_window orelse 0) != 0) {
        if (kv_len < q_len) return error.UnsupportedAttentionShape;
        // Ordinary and resident programs share exact visibility. A finite
        // additive exclusion can outrank valid scores and needlessly changes
        // the arithmetic graph at the cache boundary.
        const position = runtime_position orelse try b.constantScalarF32(@floatFromInt(kv_len - q_len));
        const allowed = try emitCausalVisibility(allocator, b, num_heads, q_len, kv_len, attrs.sliding_window orelse 0, position, visibility);
        scores = try b.select(allowed, scores, try b.broadcast(try b.constantScalarF32(-std.math.inf(f32)), &.{}, b.getInst(scores).shape));
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

fn floatingConstant(b: *hlo.Builder, shape: hlo.Shape, values: []const f32) !hlo.Id {
    // Uniform tensors retain a scalar literal plus a broadcast, including
    // large masks/constants originating before semantic optimization.
    var uniform = values.len > 16;
    if (uniform) for (values[1..]) |value| {
        if (@as(u32, @bitCast(value)) != @as(u32, @bitCast(values[0]))) {
            uniform = false;
            break;
        }
    };
    const value = if (uniform) try b.broadcast(try b.constantScalarF32(values[0]), &.{}, hlo.Shape.init(.f32, shape.dimensions)) else try b.constantF32(hlo.Shape.init(.f32, shape.dimensions), values);
    return if (shape.element_type == .f32) value else b.convertType(value, shape.element_type);
}

fn emitGraphConstant(b: *hlo.Builder, graph: *const Graph, n: *const Node) !hlo.Id {
    const attrs = n.op.constant;
    const shape = graphShapeToHlo(n);
    return switch (n.output_shape.dtype) {
        .f32 => floatingConstant(b, shape, graph.constantData(attrs.data_offset, attrs.data_len)),
        .f16, .bf16 => floating: {
            const raw = graph.constantDataAs(u16, attrs.data_offset, attrs.data_len);
            const values = try b.allocator.alloc(f32, raw.len);
            defer b.allocator.free(values);
            for (raw, values) |bits, *value| value.* = if (n.output_shape.dtype == .bf16) @bitCast(@as(u32, bits) << 16) else @floatCast(@as(f16, @bitCast(bits)));
            break :floating try floatingConstant(b, shape, values);
        },
        .i32 => b.constantS32(shape, graph.constantDataAs(i32, attrs.data_offset, attrs.data_len)),
        .i64 => b.constantS64(shape, graph.constantDataAs(i64, attrs.data_offset, attrs.data_len)),
        .bool_ => b.constantPred(shape, graph.constantDataAs(u8, attrs.data_offset, attrs.data_len)),
        else => error.UnsupportedTensorType,
    };
}

fn graphDTypeToHlo(dtype: ml.graph.DType) hlo.ElementType {
    return switch (dtype) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
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
    try std.testing.expectEqual(hlo.ElementType.f16, graphDTypeToHlo(.f16));
    try std.testing.expectEqual(hlo.ElementType.bf16, graphDTypeToHlo(.bf16));
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

test "PJRT compiler keeps uniform activation constants compact" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);
    const data = try a.alloc(f32, 512 * 512);
    defer a.free(data);
    @memset(data, -1e9);
    const value = try builder.tensorConst(data, Shape.init(.f32, &.{ 512, 512 }));
    try graph.outputs.append(a, value);
    const partition = Partition{ .backend = .pjrt, .node_ids = &.{value}, .external_inputs = &.{} };
    const backend: ComputeBackend = undefined;
    var compiled = try compilePartition(a, &graph, &partition, &backend);
    defer compiled.deinit();
    try std.testing.expect(compiled.hlo_bytes.len < 4096);
}

test "PJRT resident causal mask excludes padding below finite score sentinels" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);
    const shape = Shape.init(.f32, &.{ 1, 2 });
    const q = try builder.parameter("q", shape);
    const k = try builder.parameter("k", shape);
    const v = try builder.parameter("v", shape);
    const attention = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{ .batch = 1, .seq_len = 1, .num_heads = 1, .num_kv_heads = 1, .head_dim = 2 } },
        .output_shape = shape,
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(attention);
    const external = [_]partition_mod.ExternalInput{ .{ .node_id = q, .source_partition = 0 }, .{ .node_id = k, .source_partition = 0 }, .{ .node_id = v, .source_partition = 0 } };
    const part = Partition{ .backend = .pjrt, .node_ids = &.{attention}, .external_inputs = &external };
    const fake: ComputeBackend = undefined;
    var result = try compilePartitionWithOptions(a, &graph, &part, &fake, .{ .resident_capacity = 4 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 6), result.input_bindings.len);
    var client = pjrt.pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    var executable = try client.compileWithEntryLayouts(result.hlo_bytes, result.output_shapes.len, client.device, result.entry_layout_options.?, a);
    defer executable.deinit();
    var query = try client.bufferFromHostFloat32(&.{ 1, 1 }, &.{ 1, 2 });
    defer query.deinit();
    var key = try client.bufferFromHostFloat32(&.{ -2e9, -2e9 }, &.{ 1, 2 });
    defer key.deinit();
    var value = try client.bufferFromHostFloat32(&.{ 3, 7 }, &.{ 1, 2 });
    defer value.deinit();
    var position = try client.bufferFromHostFloat32(&.{1}, &.{});
    defer position.deinit();
    var past_key = try client.bufferFromHostFloat32(&.{ -2e9, -2e9, 0, 0, 0, 0, 0, 0 }, &.{ 4, 2 });
    defer past_key.deinit();
    var past_value = try client.bufferFromHostFloat32(&.{ 3, 7, 0, 0, 0, 0, 0, 0 }, &.{ 4, 2 });
    defer past_value.deinit();
    const outputs = try executable.execute(&.{ query, key, value, position, past_key, past_value }, a);
    defer a.free(outputs);
    defer for (outputs) |*output| output.deinit();
    const actual = try outputs[0].toFloat32WithShape(a, &.{ 1, 2 });
    defer a.free(actual);
    try std.testing.expectApproxEqAbs(@as(f32, 3), actual[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 7), actual[1], 1e-6);
}

test "PJRT stable single query normalization preserves small variance at large offsets" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);
    const x = try builder.parameter("x", Shape.init(.f32, &.{ 1, 4 }));
    const gamma = try builder.parameter("gamma", Shape.init(.f32, &.{4}));
    const beta = try builder.parameter("beta", Shape.init(.f32, &.{4}));
    const norm = try graph.addNode(.{ .op = .{ .fused_layer_norm = .{ .dim = 4, .eps = 1e-5 } }, .output_shape = Shape.init(.f32, &.{ 1, 4 }), .inputs = .{ x, gamma, beta, null_node }, .num_inputs = 3 });
    try graph.markOutput(norm);
    const external = [_]partition_mod.ExternalInput{ .{ .node_id = x, .source_partition = 0 }, .{ .node_id = gamma, .source_partition = 0 }, .{ .node_id = beta, .source_partition = 0 } };
    const part = Partition{ .backend = .pjrt, .node_ids = &.{norm}, .external_inputs = &external };
    const fake: ComputeBackend = undefined;
    var result = try compilePartitionWithOptions(a, &graph, &part, &fake, .{ .wide_single_row_norm = true });
    defer result.deinit();
    var client = pjrt.pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    var executable = try client.compile(result.hlo_bytes, 1);
    defer executable.deinit();
    const values = [_]f32{ 65536, 65536.0078125, 65536.015625, 65536.0234375 };
    var input = try client.bufferFromHostFloat32(&values, &.{ 1, 4 });
    defer input.deinit();
    var scale = try client.bufferFromHostFloat32(&.{ 1, 1, 1, 1 }, &.{4});
    defer scale.deinit();
    var bias = try client.bufferFromHostFloat32(&.{ 0, 0, 0, 0 }, &.{4});
    defer bias.deinit();
    const outputs = try executable.execute(&.{ input, scale, bias }, a);
    defer a.free(outputs);
    defer for (outputs) |*output| output.deinit();
    const actual = try outputs[0].toFloat32WithShape(a, &.{ 1, 4 });
    defer a.free(actual);
    var mean: f64 = 0;
    for (values) |value| mean += value;
    mean /= values.len;
    var variance: f64 = 0;
    for (values) |value| variance += (@as(f64, value) - mean) * (@as(f64, value) - mean);
    variance /= values.len;
    const inverse = 1 / @sqrt(variance + @as(f64, @as(f32, 1e-5)));
    for (values, actual) |value, got| try std.testing.expectApproxEqAbs(@as(f32, @floatCast((@as(f64, value) - mean) * inverse)), got, 1e-6);
}

test "PJRT compact resident rotary coefficients preserve BF16 host publication" {
    const a = std.testing.allocator;
    const Reference = struct {
        fn bf16(value: f32) f32 {
            const bits: u32 = @bitCast(value);
            return @bitCast((bits + 0x7fff + ((bits >> 16) & 1)) & 0xffff0000);
        }
    };
    var client = pjrt.pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    const rows = 4;
    const heads = 3;
    const dim = 128;
    const width = heads * dim;
    for ([_]bool{ false, true }) |consecutive| {
        var graph = Graph.init(a);
        defer graph.deinit();
        var builder = GraphBuilder.init(&graph);
        const shape = Shape.init(.f32, &.{ rows, width });
        const x = try builder.parameter("x", shape);
        const rope = try graph.addNode(.{ .op = .{ .fused_rope = .{ .seq_len = rows, .head_dim = dim, .theta = 1000000, .freq_scale = 1, .consecutive_pairs = consecutive } }, .output_shape = shape, .inputs = .{ x, null_node, null_node, null_node }, .num_inputs = 1 });
        try graph.markOutput(rope);
        const external = [_]partition_mod.ExternalInput{.{ .node_id = x, .source_partition = 0 }};
        const part = Partition{ .backend = .pjrt, .node_ids = &.{rope}, .external_inputs = &external };
        const fake: ComputeBackend = undefined;
        var result = try compilePartitionWithOptions(a, &graph, &part, &fake, .{ .precision = .bf16, .resident_capacity = 65536, .wide_rotary_trig = true });
        defer result.deinit();
        var executable = try client.compileWithEntryLayouts(result.hlo_bytes, 1, client.device, result.entry_layout_options.?, a);
        defer executable.deinit();
        var values: [rows * width]f32 = undefined;
        for (&values, 0..) |*value, index| {
            const lane = index % dim;
            value.* = if (if (consecutive) lane % 2 == 0 else lane < dim / 2) 1 else 0;
        }
        var input = try client.bufferFromHostFloat32(&values, &.{ rows, width });
        defer input.deinit();
        for ([_]usize{ 0, 63, 65, 1023, 32765, 40956 }) |start| {
            var position = try client.bufferFromHostFloat32(&.{@floatFromInt(start)}, &.{});
            defer position.deinit();
            const outputs = try executable.execute(&.{ input, position }, a);
            defer a.free(outputs);
            defer for (outputs) |*output| output.deinit();
            const actual = try outputs[0].toFloat32WithShape(a, &.{ rows, width });
            defer a.free(actual);
            for (actual, 0..) |got, index| {
                const lane = index % dim;
                const pair = if (consecutive) lane / 2 else lane % (dim / 2);
                const frequency = 1 / std.math.pow(f32, 1000000, @as(f32, @floatFromInt(2 * pair)) / dim);
                const angle = @as(f32, @floatFromInt(start + index / width)) * frequency;
                const want = Reference.bf16(if (if (consecutive) lane % 2 == 0 else lane < dim / 2) @cos(angle) else @sin(angle));
                try std.testing.expectEqual(want, got);
            }
        }
    }
}

test "PJRT full prefix causal mask excludes future keys below finite sentinels" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var builder = GraphBuilder.init(&graph);
    const shape = Shape.init(.f32, &.{ 2, 2 });
    const q = try builder.parameter("q", shape);
    const k = try builder.parameter("k", shape);
    const v = try builder.parameter("v", shape);
    const attention = try graph.addNode(.{ .op = .{ .fused_gqa_causal_attention = .{ .batch = 1, .seq_len = 2, .num_heads = 1, .num_kv_heads = 1, .head_dim = 2 } }, .output_shape = shape, .inputs = .{ q, k, v, null_node }, .num_inputs = 3 });
    try graph.markOutput(attention);
    const external = [_]partition_mod.ExternalInput{ .{ .node_id = q, .source_partition = 0 }, .{ .node_id = k, .source_partition = 0 }, .{ .node_id = v, .source_partition = 0 } };
    const part = Partition{ .backend = .pjrt, .node_ids = &.{attention}, .external_inputs = &external };
    const fake: ComputeBackend = undefined;
    var result = try compilePartition(a, &graph, &part, &fake);
    defer result.deinit();
    var client = pjrt.pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    var executable = try client.compile(result.hlo_bytes, 1);
    defer executable.deinit();
    var query = try client.bufferFromHostFloat32(&.{ 1, 1, 1, 1 }, &.{ 2, 2 });
    defer query.deinit();
    var key = try client.bufferFromHostFloat32(&.{ -2e9, -2e9, 0, 0 }, &.{ 2, 2 });
    defer key.deinit();
    var value = try client.bufferFromHostFloat32(&.{ 3, 7, 99, 101 }, &.{ 2, 2 });
    defer value.deinit();
    const outputs = try executable.execute(&.{ query, key, value }, a);
    defer a.free(outputs);
    defer for (outputs) |*output| output.deinit();
    const actual = try outputs[0].toFloat32WithShape(a, &.{ 2, 2 });
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 3, 7, 99, 101 }, actual);
}

test "PJRT single row linear reduction preserves F32 operands and multi row fallback" {
    const a = std.testing.allocator;
    var client = pjrt.pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    const width = 1024;
    const outputs_count = 3;
    for ([_]usize{ 1, 2 }) |rows| {
        var sum_builder = hlo.Builder.init(a, "linear_sum");
        defer sum_builder.deinit();
        sum_builder.next_id = 1_000_001;
        sum_builder.base_id = sum_builder.next_id;
        const left = try sum_builder.parameter(0, hlo.Shape.scalar(.f32), "left");
        const right = try sum_builder.parameter(1, hlo.Shape.scalar(.f32), "right");
        _ = try sum_builder.add(left, right);
        var sum = sum_builder.build();
        sum.id = 1_000_000;
        var b = hlo.Builder.init(a, "main");
        defer b.deinit();
        const x = try b.parameter(0, hlo.Shape.init(.f32, &.{ @intCast(rows), width }), "x");
        const weight = try b.parameter(1, hlo.Shape.init(.f32, &.{ outputs_count, width }), "weight");
        var needs_sum = false;
        _ = try emitLinear(&b, x, weight, try transposeLastTwo(&b, weight), true, &sum, &needs_sum);
        try std.testing.expectEqual(rows == 1, needs_sum);
        const module = hlo.Module.initWithAux("linear_reduction", b.build(), if (needs_sum) &.{sum} else &.{});
        const bytes = try module.serialize(a);
        defer a.free(bytes);
        var executable = try client.compile(bytes, 1);
        defer executable.deinit();
        var values: [2 * width]f32 = undefined;
        var weights: [outputs_count * width]f32 = undefined;
        // Values deliberately are not BF16 representable: this optimization
        // must not introduce a new rounding boundary to obtain a fast GEMV.
        for (&values, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 31)) - 15)) / 1024 + 0.000003;
        for (&weights, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 23)) - 11)) / 128 + 0.000001;
        var xb = try client.bufferFromHostFloat32(values[0 .. rows * width], &.{ @intCast(rows), width });
        defer xb.deinit();
        var wb = try client.bufferFromHostFloat32(&weights, &.{ outputs_count, width });
        defer wb.deinit();
        const results = try executable.execute(&.{ xb, wb }, a);
        defer a.free(results);
        defer for (results) |*v| v.deinit();
        const actual = try results[0].toFloat32WithShape(a, &.{ @intCast(rows), outputs_count });
        defer a.free(actual);
        for (actual, 0..) |got, i| {
            var expected: f64 = 0;
            for (0..width) |k| expected += @as(f64, values[(i / outputs_count) * width + k] * weights[(i % outputs_count) * width + k]);
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), got, 0.0000001);
        }
    }
}
