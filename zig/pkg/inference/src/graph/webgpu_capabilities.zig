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

//! WebGPU-specific graph partition capability policy.
//!
//! The generic partitioner owns graph assignment mechanics. This module owns
//! WebGPU op support, shape/storage eligibility, and placement cost decisions.

const std = @import("std");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const partition = @import("partition.zig");
const quant_matmul = @import("quant_matmul.zig");

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const OpCode = ml.graph.OpCode;

const CapabilityDecision = partition.CapabilityDecision;
const CapabilityQuery = partition.CapabilityQuery;

pub fn decideWebGpuGraph(query: CapabilityQuery) CapabilityDecision {
    if (!supportsWebGpuGraph(query.op)) return CapabilityDecision.reject(.unsupported_op);
    if (query.graph.node(query.node_id).output_shape.dtype != .f32) return CapabilityDecision.reject(.unsupported_op);
    if (!webGpuNodeHasSupportedShape(query)) return CapabilityDecision.reject(.unsupported_op);
    const quant_plan = if (nodeHasPackedQuantInput(query)) blk: {
        const plan = webGpuQuantizedLinearPlan(query) orelse return CapabilityDecision.reject(.missing_quant_kernel);
        if (plan.operator == .fallback) return CapabilityDecision.reject(.missing_quant_kernel);
        if (!webGpuSupportsQuantFormat(plan.format)) return CapabilityDecision.reject(.missing_quant_kernel);
        break :blk plan;
    } else null;
    if (!nodeInputsAreWebGpuCompatible(query, quant_plan != null)) return CapabilityDecision.reject(.wrong_storage);

    const cost = nodeComputeCost(query.graph, query.node_id);
    switch (query.op) {
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair => {
            if (!nodeHasWebGpuResidentInput(query) and cost < webGpuHostMatmulMinComputeCost) {
                return CapabilityDecision.unprofitable();
            }
        },
        else => {
            if (!nodeHasWebGpuResidentInput(query) and webGpuHostTransferCost(query) < webGpuHostElementwiseMinTransferBytes) {
                return CapabilityDecision.unprofitable();
            }
        },
    }
    const estimated_cost = webGpuEstimatedCost(query);
    if (quant_plan) |plan| {
        return CapabilityDecision.acceptCostWithOperator(estimated_cost, .{ .quant_matmul = plan });
    }
    return CapabilityDecision.acceptCost(estimated_cost);
}

pub fn supportsWebGpuGraph(op: OpCode) bool {
    return switch (op) {
        .fused_from_float32,
        .add,
        .fused_elem_add,
        .mul,
        .fused_elem_multiply,
        .sub,
        .div,
        .less_than,
        .where_select,
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
        .reshape,
        .slice,
        .concat_prim,
        .broadcast_in_dim,
        .reduce_sum,
        .reduce_max,
        .reduce_mean,
        .fused_softmax,
        .fused_log_softmax,
        .fused_linear,
        .fused_linear_no_bias,
        .fused_linear_no_bias_pair,
        .fused_to_float32,
        .fused_layer_norm,
        .fused_rms_norm,
        .fused_gelu,
        => true,
        else => false,
    };
}

const webGpuHostMatmulMinComputeCost: u64 = 128 * 1024;
const webGpuHostElementwiseMinTransferBytes: u64 = 256 * 1024;

fn webGpuEstimatedCost(query: CapabilityQuery) u64 {
    return checkedAdd(nodeComputeCost(query.graph, query.node_id) / 16, webGpuHostTransferCost(query));
}

fn webGpuHostTransferCost(query: CapabilityQuery) u64 {
    const descs = query.tensor_descs orelse return tensorByteSize(query.graph.node(query.node_id).output_shape);
    const n = query.graph.node(query.node_id);
    var total: u64 = 0;
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (!descIsWebGpuResident(desc) and desc.storage.isHost()) {
                total = checkedAdd(total, descByteSize(desc));
            }
        }
    }
    return checkedAdd(total, tensorByteSize(n.output_shape));
}

fn nodeComputeCost(graph: *const Graph, node_id: NodeId) u64 {
    const n = graph.node(node_id);
    return switch (n.op) {
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair => |attrs| mul3Cost(attrs.rows, attrs.in_dim, attrs.out_dim),
        .dot_general => |attrs| dotGeneralCost(graph, node_id, attrs) orelse nodeElementCost(graph, node_id),
        else => nodeElementCost(graph, node_id),
    };
}

fn nodeElementCost(graph: *const Graph, node_id: NodeId) u64 {
    const shape = graph.node(node_id).output_shape;
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return 1;
    return @intCast(elems);
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

fn tensorByteSize(shape: ml.graph.Shape) u64 {
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return @intCast(shape.dtype.byteSize());
    return checkedMul(@intCast(elems), @intCast(shape.dtype.byteSize()));
}

fn descByteSize(desc: contracts.TensorDesc) u64 {
    return tensorByteSize(desc.shape);
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

fn nodeInputsAreWebGpuCompatible(query: CapabilityQuery, allow_one_packed_quant_weight: bool) bool {
    const descs = query.tensor_descs orelse return true;
    const n = query.graph.node(query.node_id);
    var packed_quant_inputs: usize = 0;
    const allowed_packed_quant_inputs = if (allow_one_packed_quant_weight) webGpuAllowedPackedQuantWeights(query) else 0;
    for (n.getInputs(), 0..) |input_id, input_index| {
        if (inputDesc(descs, input_id)) |desc| {
            if (desc.isPackedQuant()) {
                packed_quant_inputs += 1;
                if (packed_quant_inputs > allowed_packed_quant_inputs or !webGpuPackedQuantInputIndexAllowed(query, input_index)) return false;
                continue;
            }
            if (desc.shape.dtype != .f32) return false;
            if (desc.resident_backend) |backend| {
                if (backend != .webgpu) return false;
            }
            switch (desc.storage) {
                .unknown, .metadata_view, .runtime_input, .constant, .host_f32, .host_dense, .webgpu_buffer => {},
                else => return false,
            }
        }
    }
    if (packed_quant_inputs > 0 and packed_quant_inputs != allowed_packed_quant_inputs) return false;
    return true;
}

fn webGpuAllowedPackedQuantWeights(query: CapabilityQuery) usize {
    return switch (query.op) {
        .fused_linear_no_bias => 1,
        .fused_linear_no_bias_pair => 2,
        else => 0,
    };
}

fn webGpuPackedQuantInputIndexAllowed(query: CapabilityQuery, input_index: usize) bool {
    return switch (query.op) {
        .fused_linear_no_bias => input_index == 1,
        .fused_linear_no_bias_pair => input_index == 1 or input_index == 2,
        else => false,
    };
}

fn webGpuQuantizedLinearPlan(query: CapabilityQuery) ?quant_matmul.Plan {
    const descs = query.tensor_descs orelse return null;
    const n = query.graph.node(query.node_id);
    const attrs = switch (query.op) {
        .fused_linear_no_bias => |attrs| attrs,
        .fused_linear_no_bias_pair => |attrs| attrs,
        else => return null,
    };
    if (!webGpuLinearAttrsHaveSupportedShape(attrs)) return null;
    if (n.num_inputs < 2) return null;
    const input_desc = inputDesc(descs, n.inputs[0]) orelse return null;
    const weight_desc = inputDesc(descs, n.inputs[1]) orelse return null;
    if (input_desc.isPackedQuant() or input_desc.shape.dtype != .f32) return null;
    const format = weight_desc.quant_format orelse return null;
    if (!webGpuSupportsQuantFormat(format)) return null;
    if (std.meta.activeTag(query.op) == .fused_linear_no_bias_pair) {
        if (n.num_inputs < 3) return null;
        const weight_b_desc = inputDesc(descs, n.inputs[2]) orelse return null;
        if (weight_b_desc.quant_format != format) return null;
    }
    return quant_matmul.plan(.{
        .rows = attrs.rows,
        .in_dim = attrs.in_dim,
        .out_dim = attrs.out_dim,
        .format = format,
    });
}

fn webGpuSupportsQuantFormat(format: quant_matmul.Format) bool {
    return switch (format) {
        .q4_0,
        .q4_1,
        .q5_0,
        .q5_1,
        .q8_0,
        .q8_1,
        .q2_k,
        .q3_k,
        .q4_k,
        .q5_k,
        .q6_k,
        .q8_k,
        .iq4_nl,
        .iq4_xs,
        .i2_s,
        => true,
        else => false,
    };
}

fn nodeHasWebGpuResidentInput(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (descIsWebGpuResident(desc)) return true;
        }
    }
    return false;
}

fn webGpuNodeHasSupportedShape(query: CapabilityQuery) bool {
    const node = query.graph.node(query.node_id);
    if (node.output_shape.numElements() == null and node.output_shape.maxElements() == null) return false;
    return switch (query.op) {
        .fused_linear_no_bias, .fused_linear_no_bias_pair => |attrs| webGpuLinearAttrsHaveSupportedShape(attrs),
        .fused_layer_norm, .fused_rms_norm => |attrs| blk: {
            if (attrs.dim == 0) break :blk false;
            const output_elems = tensorElementCount(node.output_shape) orelse break :blk false;
            break :blk output_elems % attrs.dim == 0;
        },
        else => true,
    };
}

fn webGpuLinearAttrsHaveSupportedShape(attrs: ml.graph.node.LinearAttrs) bool {
    if (attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0) return false;
    if (attrs.num_projections == 0) return true;
    if (attrs.num_projections > attrs.projection_out_dims.len) return false;
    var total: u64 = 0;
    for (attrs.projection_out_dims[0..attrs.num_projections]) |dim| {
        if (dim == 0) return false;
        total = checkedAdd(total, dim);
    }
    return total == attrs.out_dim;
}

fn tensorElementCount(shape: ml.graph.Shape) ?usize {
    const rank = shape.rank();
    var total: usize = 1;
    for (0..rank) |axis| {
        const dim = shape.dim(@intCast(axis));
        if (dim < 0) return null;
        total *= @intCast(dim);
    }
    return total;
}

fn descIsWebGpuResident(desc: contracts.TensorDesc) bool {
    return desc.storage == .webgpu_buffer or desc.resident_backend == .webgpu;
}

fn inputDesc(descs: []const ?contracts.TensorDesc, input_id: NodeId) ?contracts.TensorDesc {
    if (input_id == ml.graph.null_node) return null;
    const index: usize = @intCast(input_id);
    if (index >= descs.len) return null;
    return descs[index];
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

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;

test "webgpu graph capability admits resident dense transformer chain" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const rows: usize = 64;
    const hidden: usize = 64;
    const x = try b.parameter("x", Shape.init(.f32, &.{ rows, hidden }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ hidden, hidden }));
    const rms_weight = try b.parameter("rms_weight", Shape.init(.f32, &.{hidden}));
    const gamma = try b.parameter("gamma", Shape.init(.f32, &.{hidden}));
    const beta = try b.parameter("beta", Shape.init(.f32, &.{hidden}));
    const lin = try b.linearNoBias(x, w, rows, hidden, hidden);
    const rms = try b.rmsNorm(lin, rms_weight, hidden, 1e-5);
    const gelu = try b.gelu(rms);
    const out = try b.layerNorm(gelu, gamma, beta, hidden, 1e-5);
    try g.markOutput(out);

    const caps = [_]partition.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &supportsWebGpuGraph, .decide = &decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll, .decide = &partition.decideNative },
    };
    const descriptor_seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = descriptor_seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[lin]].backend);
    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[rms]].backend);
    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[gelu]].backend);
    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expect(diagnostics.count(.supported) >= 4);
}

test "webgpu graph capability leaves tiny host-only ops on native" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const caps = [_]partition.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &supportsWebGpuGraph, .decide = &decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll, .decide = &partition.decideNative },
    };
    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{ .diagnostics = &diagnostics });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.native, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expect(diagnostics.count(.unprofitable_shape) > 0);
}

test "webgpu graph capability admits planned packed quant linear no bias" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const rows: usize = 9;
    const in_dim: usize = 32;
    const out_dim: usize = 16;
    const x = try b.parameter("x", Shape.init(.f32, &.{ rows, in_dim }));
    const w = try b.parameter("w_q4", Shape.init(.f32, &.{ out_dim, in_dim }));
    const out = try b.linearNoBias(x, w, rows, in_dim, out_dim);
    try g.markOutput(out);

    const caps = [_]partition.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &supportsWebGpuGraph, .decide = &decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll, .decide = &partition.decideNative },
    };
    const descriptor_seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    descriptor_seeds[@intCast(w)] = contracts.TensorDesc.packedQuant(g.node(w).output_shape, .q4_0);

    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = descriptor_seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[out]].backend);
    const op_plan = plan.operatorPlanForNode(out) orelse return error.MissingOperatorPlan;
    try std.testing.expectEqual(quant_matmul.Operator.mul_mm, op_plan.operator());
    try std.testing.expect(diagnostics.operator_stats.count(.mul_mm) > 0);
}

test "webgpu graph capability admits planned packed grouped quant linear no bias" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const rows: usize = 9;
    const in_dim: usize = 32;
    const out_dim: usize = 64;
    const x = try b.parameter("x", Shape.init(.f32, &.{ rows, in_dim }));
    const w = try b.parameter("w_qkv_q4", Shape.init(.f32, &.{ out_dim, in_dim }));
    const out = try g.addNode(.{
        .op = .{ .fused_linear_no_bias = .{
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
            .projection_out_dims = .{ 32, 16, 16, 0 },
            .num_projections = 3,
        } },
        .output_shape = Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, w, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    const caps = [_]partition.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &supportsWebGpuGraph, .decide = &decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll, .decide = &partition.decideNative },
    };
    const descriptor_seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    descriptor_seeds[@intCast(w)] = contracts.TensorDesc.packedQuant(g.node(w).output_shape, .q4_0);

    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = descriptor_seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[out]].backend);
    const op_plan = plan.operatorPlanForNode(out) orelse return error.MissingOperatorPlan;
    try std.testing.expectEqual(quant_matmul.Operator.mul_mm, op_plan.operator());
    try std.testing.expect(diagnostics.operator_stats.count(.mul_mm) > 0);
}

test "webgpu graph capability admits planned packed quant linear no bias pair" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const rows: usize = 9;
    const in_dim: usize = 32;
    const out_dim: usize = 16;
    const x = try b.parameter("x", Shape.init(.f32, &.{ rows, in_dim }));
    const w_a = try b.parameter("w_a_q4", Shape.init(.f32, &.{ out_dim, in_dim }));
    const w_b = try b.parameter("w_b_q4", Shape.init(.f32, &.{ out_dim, in_dim }));
    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
        } },
        .output_shape = Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, w_a, w_b, ml.graph.null_node },
        .num_inputs = 3,
    });
    const second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ pair, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    const out = try b.add(pair, second);
    try g.markOutput(out);

    const caps = [_]partition.Capability{
        .{ .backend = .webgpu, .priority = 10, .supports = &supportsWebGpuGraph, .decide = &decideWebGpuGraph },
        .{ .backend = .native, .priority = 0, .supports = &partition.supportsAll, .decide = &partition.decideNative },
    };
    const descriptor_seeds = try partition.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(descriptor_seeds);
    try partition.seedAllUploadableResidency(descriptor_seeds, &g, .webgpu, 0);
    descriptor_seeds[@intCast(w_a)] = contracts.TensorDesc.packedQuant(g.node(w_a).output_shape, .q4_0);
    descriptor_seeds[@intCast(w_b)] = contracts.TensorDesc.packedQuant(g.node(w_b).output_shape, .q4_0);

    var diagnostics = partition.CapabilityDiagnostics{};
    var plan = try partition.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = descriptor_seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[pair]].backend);
    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[second]].backend);
    try std.testing.expectEqual(contracts.BackendKind.webgpu, plan.partitions[plan.node_assignment[out]].backend);
    const op_plan = plan.operatorPlanForNode(pair) orelse return error.MissingOperatorPlan;
    try std.testing.expectEqual(quant_matmul.Operator.mul_mm, op_plan.operator());
    try std.testing.expect(diagnostics.operator_stats.count(.mul_mm) > 0);
}
