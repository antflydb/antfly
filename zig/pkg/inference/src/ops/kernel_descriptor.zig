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

//! Device-independent kernel identity and operand geometry. Descriptors carry
//! semantics and representation requirements; each target retains its schedules.
const std = @import("std");
const ml = @import("ml");
const CT = @import("../graph/backend_contracts.zig").CT;

pub const Unary = enum { negate, sqrt, rsqrt, exp, log, sin, cos, tanh, erf, abs, gelu, gelu_exact, relu, silu };
pub const Binary = enum { add, subtract, multiply, divide };
pub const Linear = struct { rows: u32, in_dim: u32, out_dim: u32, bias: bool };
pub const Norm = struct { kind: enum { layer, rms }, dim: u32, eps: f32 };
pub const Softmax = struct { width: u32, logarithmic: bool };
pub const kernel_geometry = @import("kernel_geometry.zig");
pub const ReductionMode = kernel_geometry.ReductionMode;
pub const ReductionGeometry = kernel_geometry.ReductionGeometry;
pub const Dot = kernel_geometry.BatchedDotGeneralPlan;
pub const Reduction = struct { mode: ReductionMode, geometry: ReductionGeometry };
pub const Layout = struct { axes: [8]u32, input_rank: u8, output_rank: u8 };
pub const Operation = union(enum) { transpose: Layout, broadcast: Layout, unary: Unary, binary: Binary, linear: Linear, norm: Norm, softmax: Softmax, reduction: Reduction, dot: Dot };
pub const Descriptor = struct {
    operation: Operation,
    output_shape: ml.graph.Shape,
    input_shapes: [3]ml.graph.Shape,
    input_count: u8,
    /// Minimum alignment; target vector/tile schedules may require more.
    alignment: u32 = @alignOf(f32),
    pub fn elements(self: *const Descriptor) usize {
        return @intCast(self.output_shape.numElements().?);
    }
};
/// Per-invocation scratch. Targets expose their own layout inside this region;
/// its physical owner and reuse lifetime belong to the executable plan.
pub const WorkspaceRequirement = struct { byte_size: usize = 0, alignment: u32 = @alignOf(f32) };
pub const max_auxiliaries = 3;
/// A representation of the output retained for a downstream implementation,
/// or a saved mathematical result. Every plane follows its primary output's
/// physical lifetime; it is never borrowed from per-invocation scratch.
pub const AuxiliaryRole = enum { bf16_mirror, mean, inverse_std };
pub const Auxiliary = struct { role: AuxiliaryRole, shape: ml.graph.Shape };
pub const AuxiliaryRequirements = struct {
    entries: [max_auxiliaries]Auxiliary = undefined,
    count: u8 = 0,
    pub fn items(self: *const AuxiliaryRequirements) []const Auxiliary {
        return self.entries[0..self.count];
    }
};
pub const AuxiliaryBinding = struct { role: AuxiliaryRole, tensor: CT };
pub const Invocation = struct {
    descriptor: *const Descriptor,
    /// Selected implementation policy stays separate from mathematical identity.
    operator_plan: ?*const @import("../graph/operator_plan.zig").OperatorPlan = null,
    inputs: []const CT,
    /// Null is a legality query. A decline must precede any writes/submission.
    output: ?CT = null,
    workspace: ?CT = null,
    auxiliaries: []const AuxiliaryBinding = &.{},
    pub inline fn operatorPlan(self: *const Invocation) ?@import("../graph/operator_plan.zig").OperatorPlan {
        return if (self.operator_plan) |plan| plan.* else null;
    }
};

pub fn unary(op: ml.graph.OpCode) ?Unary {
    return switch (op) {
        .neg => .negate,
        .sqrt => .sqrt,
        .rsqrt => .rsqrt,
        .exp => .exp,
        .log => .log,
        .sin => .sin,
        .cos => .cos,
        .tanh => .tanh,
        .erf => .erf,
        .abs => .abs,
        .fused_gelu => .gelu,
        .fused_gelu_exact => .gelu_exact,
        .fused_relu => .relu,
        .fused_silu => .silu,
        else => null,
    };
}

pub fn lower(graph: *const ml.graph.Graph, id: ml.graph.NodeId) ?Descriptor {
    const node = graph.node(id);
    const shape = node.output_shape;
    const count = shape.numElements() orelse return null;
    if (shape.dtype != .f32 or count <= 0 or count > std.math.maxInt(i32)) return null;
    const operation: Operation = switch (node.op) {
        .transpose, .broadcast_in_dim => blk: {
            if (node.num_inputs != 1 or node.inputs[0] == ml.graph.null_node or node.inputs[0] >= graph.nodeCount()) return null;
            const input = graph.node(node.inputs[0]).output_shape;
            if (input.rank() > 8 or shape.rank() == 0 or shape.rank() > 8) return null;
            var layout = Layout{ .axes = @splat(0), .input_rank = input.rank(), .output_rank = shape.rank() };
            var seen: u16 = 0;
            if (node.op == .transpose) {
                const attrs = node.op.transpose;
                if (attrs.num_axes != input.rank() or shape.rank() != input.rank()) return null;
                for (attrs.perm[0..attrs.num_axes], 0..) |axis, index| {
                    if (axis >= input.rank()) return null;
                    const bit = @as(u16, 1) << @intCast(axis);
                    if (seen & bit != 0 or shape.dims[index] != input.dims[axis]) return null;
                    seen |= bit;
                    layout.axes[index] = axis;
                }
                break :blk .{ .transpose = layout };
            }
            const attrs = node.op.broadcast_in_dim;
            if (attrs.num_axes != input.rank()) return null;
            for (attrs.broadcast_axes[0..attrs.num_axes], 0..) |axis, index| {
                if (axis >= shape.rank()) return null;
                const bit = @as(u16, 1) << @intCast(axis);
                if (seen & bit != 0 or (input.dims[index] != 1 and input.dims[index] != shape.dims[axis])) return null;
                seen |= bit;
                layout.axes[index] = axis;
            }
            break :blk .{ .broadcast = layout };
        },
        .add, .fused_elem_add => .{ .binary = .add },
        .sub => .{ .binary = .subtract },
        .mul, .fused_elem_multiply => .{ .binary = .multiply },
        .div => .{ .binary = .divide },
        .fused_linear, .fused_linear_no_bias => |attrs| blk: {
            if (attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0 or attrs.num_projections != 0 or shape.rank() != 2 or shape.dim(0) != attrs.rows or shape.dim(1) != attrs.out_dim) return null;
            break :blk .{ .linear = .{ .rows = attrs.rows, .in_dim = attrs.in_dim, .out_dim = attrs.out_dim, .bias = node.op == .fused_linear } };
        },
        .dot_general => |attrs| blk: {
            if (node.num_inputs != 2 or attrs.num_contracting != 1 or attrs.num_batch > attrs.lhs_batch.len) return null;
            for (node.getInputs()) |input| if (input == ml.graph.null_node or input >= graph.nodeCount()) return null;
            const lhs = graph.node(node.inputs[0]).output_shape;
            const rhs = graph.node(node.inputs[1]).output_shape;
            const plan = kernel_geometry.planDenseDotGeneral(lhs.dims[0..lhs.rank()], rhs.dims[0..rhs.rank()], attrs.lhs_contracting[0..1], attrs.rhs_contracting[0..1], attrs.lhs_batch[0..attrs.num_batch], attrs.rhs_batch[0..attrs.num_batch]) catch return null;
            if (shape.rank() != lhs.rank()) return null;
            for (0..lhs.rank() - 2) |axis| if (shape.dims[axis] != lhs.dims[axis]) return null;
            if (shape.dim(shape.rank() - 2) != plan.m or shape.dim(shape.rank() - 1) != plan.n) return null;
            break :blk .{ .dot = plan };
        },
        .reduce_sum, .reduce_max, .reduce_mean => |attrs| blk: {
            if (node.num_inputs != 1 or node.inputs[0] == ml.graph.null_node or node.inputs[0] >= graph.nodeCount()) return null;
            const input_shape = graph.node(node.inputs[0]).output_shape;
            if (attrs.num_axes > attrs.axes.len) return null;
            const geometry = ReductionGeometry.init(input_shape.dims[0..input_shape.rank()], attrs.axes[0..attrs.num_axes]) catch return null;
            if (!std.mem.eql(i64, shape.dims[0..shape.rank()], geometry.output_dims[0..geometry.rank])) return null;
            break :blk .{ .reduction = .{ .mode = switch (node.op) {
                .reduce_sum => .sum,
                .reduce_max => .max,
                else => .mean,
            }, .geometry = geometry } };
        },
        .fused_layer_norm, .fused_rms_norm => |attrs| blk: {
            if (shape.rank() == 0 or attrs.dim == 0 or attrs.dim != shape.dim(shape.rank() - 1)) return null;
            break :blk .{ .norm = .{ .kind = if (node.op == .fused_layer_norm) .layer else .rms, .dim = attrs.dim, .eps = attrs.eps } };
        },
        .fused_softmax, .fused_log_softmax => |attrs| blk: {
            if (shape.rank() == 0 or attrs.dim == 0 or attrs.dim != shape.dim(shape.rank() - 1)) return null;
            break :blk .{ .softmax = .{ .width = attrs.dim, .logarithmic = node.op == .fused_log_softmax } };
        },
        else => .{ .unary = unary(node.op) orelse return null },
    };
    const inputs: u8 = switch (operation) {
        .unary => 1,
        .binary, .dot => 2,
        .softmax, .reduction, .transpose, .broadcast => 1,
        .norm => |norm| if (norm.kind == .layer) 3 else 2,
        .linear => |linear| if (linear.bias) 3 else 2,
    };
    if (node.num_inputs != inputs) return null;
    var result = Descriptor{ .operation = operation, .output_shape = shape, .input_count = inputs, .input_shapes = undefined };
    for (node.getInputs(), 0..) |input, index| {
        if (input == ml.graph.null_node or input >= graph.nodeCount()) return null;
        const input_shape = graph.node(input).output_shape;
        const input_count = input_shape.numElements() orelse return null;
        const reduced_weight = index == 1 and (operation == .linear or operation == .dot) and
            (input_shape.dtype == .f16 or input_shape.dtype == .bf16);
        if ((input_shape.dtype != .f32 and !reduced_weight) or input_count <= 0 or input_count > std.math.maxInt(i32)) return null;
        if ((operation == .unary or operation == .binary or operation == .softmax) and input_count != 1 and !std.mem.eql(i64, input_shape.dims[0..input_shape.rank()], shape.dims[0..shape.rank()])) return null;
        if ((operation == .unary or operation == .softmax) and input_count != count) return null;
        result.input_shapes[index] = input_shape;
    }
    if (operation == .linear) {
        const linear = operation.linear;
        if (result.input_shapes[0].numElements().? != @as(u64, linear.rows) * linear.in_dim or result.input_shapes[1].numElements().? != @as(u64, linear.out_dim) * linear.in_dim or (linear.bias and result.input_shapes[2].numElements().? != linear.out_dim)) return null;
    }
    if (operation == .norm) {
        const norm = operation.norm;
        if (!std.mem.eql(i64, result.input_shapes[0].dims[0..result.input_shapes[0].rank()], shape.dims[0..shape.rank()]) or result.input_shapes[1].numElements().? != norm.dim or (norm.kind == .layer and result.input_shapes[2].numElements().? != norm.dim)) return null;
    }
    return result;
}
