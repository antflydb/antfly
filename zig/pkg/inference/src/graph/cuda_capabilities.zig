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

//! Legality of the resident general CUDA graph path. Specialized model
//! implementations retain their own, broader kernel contracts.
const std = @import("std");
const ml = @import("ml");
const partition = @import("partition.zig");
const contracts = @import("backend_contracts.zig");
const primitive_plans = @import("../ops/kernel_geometry.zig");

pub fn supports(op: ml.graph.OpCode) bool {
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
        .add,
        .sub,
        .mul,
        .div,
        .reduce_sum,
        .reduce_max,
        .reduce_mean,
        .reshape,
        .transpose,
        .broadcast_in_dim,
        .dot_general,
        .fused_layer_norm,
        .fused_rms_norm,
        .fused_softmax,
        .fused_log_softmax,
        .fused_linear,
        .fused_linear_no_bias,
        .fused_gelu,
        .fused_gelu_exact,
        .fused_relu,
        .fused_silu,
        => true,
        else => false,
    };
}

fn concrete(shape: ml.graph.Shape) bool {
    const count = shape.numElements() orelse return false;
    if (count == 0 or count > std.math.maxInt(u32)) return false;
    for (shape.dims[0..shape.rank()]) |dim| if (dim <= 0 or dim > std.math.maxInt(i32)) return false;
    return true;
}

fn dense(query: partition.CapabilityQuery, node_id: ml.graph.NodeId) bool {
    const descs = query.tensor_descs orelse return true;
    var id = node_id;
    var remaining = query.graph.nodeCount();
    while (remaining != 0) : (remaining -= 1) {
        if (id >= descs.len) return false;
        const desc = descs[id] orelse return true;
        if (desc.quant_format != null) return false;
        // CUDA materializes these layouts. A CPU producer crossing into CUDA
        // is likewise normalized by transfer; explicit strided inputs retain
        // the stride check below. Walk reshape chains without recursive stack.
        switch (query.graph.node(id).op) {
            .transpose, .broadcast_in_dim => return true,
            .reshape => {
                id = query.graph.node(id).inputs[0];
                continue;
            },
            else => {},
        }
        if (desc.strides.rank == 0) return desc.storage != .metadata_view;
        const expected = contracts.TensorStrides.dense(desc.shape);
        return std.mem.eql(i64, desc.strides.asSlice(), expected.asSlice());
    }
    return false;
}

pub fn decide(query: partition.CapabilityQuery) partition.CapabilityDecision {
    const reject = partition.CapabilityDecision.reject;
    if (!supports(query.op)) return reject(.unsupported_op);
    const node = query.graph.node(query.node_id);
    if (!concrete(node.output_shape)) return reject(.unsupported_op);
    if (!dense(query, query.node_id)) return reject(.wrong_storage);
    switch (query.op) {
        .parameter, .constant => return partition.CapabilityDecision.accept(),
        else => {},
    }
    // These general kernels consume physical f32. Integer arithmetic and
    // packed/strided operands must go to a separately qualified implementation.
    if (node.output_shape.dtype != .f32) return reject(.unsupported_op);
    for (node.getInputs()) |id| {
        if (id == ml.graph.null_node) return reject(.unsupported_op);
        const shape = query.graph.node(id).output_shape;
        if (shape.dtype != .f32 or !concrete(shape)) return reject(.unsupported_op);
        if (!dense(query, id)) return reject(.wrong_storage);
    }
    switch (query.op) {
        .neg, .sqrt, .rsqrt, .exp, .log, .sin, .cos, .tanh, .erf, .abs, .fused_gelu, .fused_gelu_exact, .fused_relu, .fused_silu => {
            if (node.num_inputs != 1 or !sameShape(node.output_shape, query.graph.node(node.inputs[0]).output_shape)) return reject(.unsupported_op);
        },
        .add, .sub, .mul, .div => {
            if (node.num_inputs != 2) return reject(.unsupported_op);
            const lhs = query.graph.node(node.inputs[0]).output_shape;
            const rhs = query.graph.node(node.inputs[1]).output_shape;
            const rank = @max(lhs.rank(), rhs.rank());
            if (node.output_shape.rank() != rank) return reject(.unsupported_op);
            for (0..rank) |axis| {
                const l = if (axis + lhs.rank() >= rank) lhs.dims[axis + lhs.rank() - rank] else 1;
                const r = if (axis + rhs.rank() >= rank) rhs.dims[axis + rhs.rank() - rank] else 1;
                if ((l != r and l != 1 and r != 1) or node.output_shape.dims[axis] != @max(l, r)) return reject(.unsupported_op);
            }
        },
        .transpose, .broadcast_in_dim => {
            if (@import("../ops/kernel_descriptor.zig").lower(query.graph, query.node_id) == null) return reject(.unsupported_op);
        },
        .dot_general => |attrs| {
            if (node.num_inputs != 2 or attrs.num_contracting != 1) return reject(.unsupported_op);
            const lhs = query.graph.node(node.inputs[0]).output_shape;
            const rhs = query.graph.node(node.inputs[1]).output_shape;
            const lc = attrs.lhs_contracting[0];
            const rc = attrs.rhs_contracting[0];
            if (attrs.num_batch > 0) {
                _ = primitive_plans.planBatchedDotGeneral(lhs.dims[0..lhs.rank()], rhs.dims[0..rhs.rank()], attrs.lhs_contracting[0..1], attrs.rhs_contracting[0..1], attrs.lhs_batch[0..attrs.num_batch], attrs.rhs_batch[0..attrs.num_batch]) catch return reject(.unsupported_op);
            } else if (lhs.rank() < 2 or rhs.rank() != 2 or lc != lhs.rank() - 1 or rc > 1 or lhs.dim(lc) != rhs.dim(rc)) return reject(.unsupported_op);
        },
        .fused_linear, .fused_linear_no_bias => |attrs| {
            const expected_inputs: u8 = if (query.op == .fused_linear) 3 else 2;
            if (node.num_inputs != expected_inputs or attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0 or attrs.num_projections != 0) return reject(.unsupported_op);
            const lhs = query.graph.node(node.inputs[0]).output_shape;
            const rhs = query.graph.node(node.inputs[1]).output_shape;
            if (node.output_shape.rank() != 2 or node.output_shape.dim(0) != attrs.rows or node.output_shape.dim(1) != attrs.out_dim) return reject(.unsupported_op);
            if (lhs.numElements().? != @as(u64, attrs.rows) * attrs.in_dim or rhs.rank() != 2 or rhs.dim(0) != attrs.out_dim or rhs.dim(1) != attrs.in_dim) return reject(.unsupported_op);
            if (node.num_inputs == 3 and query.graph.node(node.inputs[2]).output_shape.numElements().? != attrs.out_dim) return reject(.unsupported_op);
        },
        .reduce_sum, .reduce_max, .reduce_mean => {
            const descriptor = @import("../ops/kernel_descriptor.zig").lower(query.graph, query.node_id) orelse return reject(.unsupported_op);
            if (descriptor.operation.reduction.geometry.rank == 0) return reject(.unsupported_op);
        },
        .fused_layer_norm, .fused_rms_norm => {
            if (@import("../ops/kernel_descriptor.zig").lower(query.graph, query.node_id) == null) return reject(.unsupported_op);
        },
        .fused_softmax, .fused_log_softmax => |attrs| {
            const shape = node.output_shape;
            if (node.num_inputs != 1 or !sameShape(shape, query.graph.node(node.inputs[0]).output_shape)) return reject(.unsupported_op);
            if (shape.rank() == 0 or attrs.dim == 0 or attrs.dim != shape.dim(shape.rank() - 1)) return reject(.unsupported_op);
        },
        .reshape => {
            if (node.num_inputs != 1 or node.output_shape.numElements() != query.graph.node(node.inputs[0]).output_shape.numElements()) return reject(.unsupported_op);
        },
        else => {},
    }
    return partition.CapabilityDecision.accept();
}

fn sameShape(lhs: ml.graph.Shape, rhs: ml.graph.Shape) bool {
    return lhs.dtype == rhs.dtype and std.mem.eql(i64, lhs.dims[0..lhs.rank()], rhs.dims[0..rhs.rank()]);
}

test "CUDA graph legality rejects integer arithmetic and strided operands" {
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.add(x, x);
    const query = partition.CapabilityQuery{ .graph = &graph, .node_id = y, .op = graph.node(y).op };
    try std.testing.expect(decide(query).can_execute);
    graph.nodeMut(x).output_shape.dtype = .i64;
    try std.testing.expect(!decide(query).can_execute);
    graph.nodeMut(x).output_shape.dtype = .f32;
    var descriptors = [_]?contracts.TensorDesc{ contracts.TensorDesc.view(graph.node(x).output_shape, x, contracts.TensorStrides.init(&.{ 1, 2 }), .cuda, 0), null };
    var strided_query = query;
    strided_query.tensor_descs = &descriptors;
    try std.testing.expectEqual(partition.CapabilityReason.wrong_storage, decide(strided_query).reason);
}

test "CUDA graph legality validates complete arithmetic and layout signatures" {
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const transpose = try b.transpose(x, &.{ 1, 0 });
    var query = partition.CapabilityQuery{ .graph = &graph, .node_id = transpose, .op = graph.node(transpose).op };
    try std.testing.expect(decide(query).can_execute);
    graph.nodeMut(transpose).op.transpose.perm[1] = 1;
    query.op = graph.node(transpose).op;
    try std.testing.expect(!decide(query).can_execute);
    const sum = try b.add(x, x);
    query = .{ .graph = &graph, .node_id = sum, .op = graph.node(sum).op };
    try std.testing.expect(decide(query).can_execute);
    graph.nodeMut(sum).output_shape = ml.graph.Shape.init(.f32, &.{ 4, 2 });
    try std.testing.expect(!decide(query).can_execute);
}
