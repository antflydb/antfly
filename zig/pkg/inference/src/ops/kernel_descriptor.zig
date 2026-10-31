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
pub const Linear = struct {
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    bias: bool,

    /// Logical dimensions shared with native launch adapters. Empty-tensor
    /// policy and storage/codec byte extents remain the caller's contract.
    pub fn init(rows: usize, in_dim: usize, out_dim: usize, bias: bool) error{DimensionOverflow}!Linear {
        return .{
            .rows = std.math.cast(u32, rows) orelse return error.DimensionOverflow,
            .in_dim = std.math.cast(u32, in_dim) orelse return error.DimensionOverflow,
            .out_dim = std.math.cast(u32, out_dim) orelse return error.DimensionOverflow,
            .bias = bias,
        };
    }
};
pub const Norm = struct { kind: enum { layer, rms }, dim: u32, eps: f32 };
pub const Softmax = struct { width: u32, logarithmic: bool };
pub const Rope = struct {
    seq_len: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    consecutive_pairs: bool,
    total_chunks: usize,
    chunks_per_token: usize,

    pub inline fn init(shape: ?[]const i64, elements: usize, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) error{InvalidRoPEInput}!Rope {
        if (seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0 or elements % head_dim != 0) return error.InvalidRoPEInput;
        const chunks = elements / head_dim;
        if (chunks % seq_len != 0) return error.InvalidRoPEInput;
        const per_token = kernel_geometry.ropeChunksPerToken(shape, chunks, seq_len, head_dim);
        if (per_token == 0) return error.InvalidRoPEInput;
        const result = Rope{ .seq_len = seq_len, .head_dim = head_dim, .rope_dim = rope_dim, .theta = theta, .freq_scale = freq_scale, .position_offset = position_offset, .consecutive_pairs = consecutive_pairs, .total_chunks = chunks, .chunks_per_token = per_token };
        return result.withPosition(position_offset);
    }
    pub inline fn withPosition(self: Rope, offset: usize) error{InvalidRoPEInput}!Rope {
        if (self.seq_len == 0) return error.InvalidRoPEInput;
        if (offset > std.math.maxInt(usize) - (self.seq_len - 1)) return error.InvalidRoPEInput;
        var result = self;
        result.position_offset = offset;
        return result;
    }
    pub inline fn valid(self: Rope, shape: []const i64, elements: usize) bool {
        const expected = init(shape, elements, self.seq_len, self.head_dim, self.rope_dim, self.theta, self.freq_scale, self.position_offset, self.consecutive_pairs) catch return false;
        return expected.total_chunks == self.total_chunks and expected.chunks_per_token == self.chunks_per_token;
    }
    pub inline fn positionAt(self: Rope, chunk: usize) usize {
        return self.position_offset + ((chunk / self.chunks_per_token) % self.seq_len);
    }
};

pub const kernel_geometry = @import("kernel_geometry.zig");
pub const ReductionMode = kernel_geometry.ReductionMode;
pub const ReductionGeometry = kernel_geometry.ReductionGeometry;
pub const Dot = kernel_geometry.BatchedDotGeneralPlan;
pub const Reduction = struct { mode: ReductionMode, geometry: ReductionGeometry };
pub const Layout = struct {
    axes: [8]u32,
    input_rank: u8,
    output_rank: u8,

    pub fn transpose(input: []const i64, output: []const i64, permutation: []const u8) !Layout {
        var resolved: [8]i64 = undefined;
        try kernel_geometry.transposeShape(input, permutation, &resolved);
        if (!std.mem.eql(i64, resolved[0..input.len], output)) return error.InvalidShape;
        var result = Layout{ .axes = @splat(0), .input_rank = @intCast(input.len), .output_rank = @intCast(output.len) };
        for (permutation, 0..) |axis, i| result.axes[i] = axis;
        return result;
    }
};

/// Convert admitted descriptor dimensions at the native API boundary. Keep
/// only logical shapes in descriptors; no cached copy of every target's wires.
pub inline fn nativeDimensions(comptime T: type, shape: ml.graph.Shape) [8]T {
    var dimensions: [8]T = @splat(1);
    for (shape.dims[0..shape.rank()], 0..) |dim, axis| dimensions[axis] = @intCast(dim);
    return dimensions;
}
/// Only normalized logical coordinates are retained; target wire arrays and
/// coalesced affine maps are derived by the actual native consumer.
pub const Slice = struct {
    starts: [8]u32 = @splat(0),
    steps: [8]u32 = @splat(1),

    pub fn init(input: []const i64, output: []const i64, starts: []const i64, limits: []const i64, steps: []const i64) !Slice {
        if (input.len > 8 or output.len != input.len or starts.len != input.len or limits.len != input.len or steps.len != input.len) return error.InvalidShape;
        var result = Slice{};
        for (input, 0..) |dim, axis| {
            if (dim <= 0 or steps[axis] > std.math.maxInt(u32)) return error.InvalidShape;
            const map = try kernel_geometry.SliceAxis.init(true, dim, starts[axis], limits[axis], steps[axis], false);
            if (map.size != output[axis] or map.size <= 0) return error.InvalidShape;
            result.starts[axis] = std.math.cast(u32, map.start) orelse return error.InvalidShape;
            result.steps[axis] = std.math.cast(u32, map.stride) orelse return error.InvalidShape;
        }
        return result;
    }

    pub fn plan(self: Slice, input: ml.graph.Shape, output: ml.graph.Shape) !@import("slice_plan.zig").Plan {
        if (input.rank() != output.rank()) return error.InvalidShape;
        var starts: [8]i64 = undefined;
        var limits: [8]i64 = undefined;
        var steps: [8]i64 = undefined;
        const rank = input.rank();
        for (0..rank) |axis| {
            if (self.steps[axis] == 0 or output.dims[axis] <= 0) return error.InvalidShape;
            starts[axis] = self.starts[axis];
            steps[axis] = self.steps[axis];
            limits[axis] = try std.math.add(i64, starts[axis], try std.math.add(i64, try std.math.mul(i64, output.dims[axis] - 1, steps[axis]), 1));
        }
        return @import("slice_plan.zig").Plan.init(input.dims[0..rank], starts[0..rank], limits[0..rank], steps[0..rank], input.dims[0..rank]);
    }
};
pub const Gather = struct { axis: u8, geometry: kernel_geometry.GatherAxis };
pub const Operation = union(enum) { transpose: Layout, broadcast: Layout, unary: Unary, binary: Binary, linear: Linear, norm: Norm, softmax: Softmax, rope: Rope, reduction: Reduction, dot: Dot, concat: kernel_geometry.ConcatAxis, slice: Slice, gather: Gather };

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

fn inputShape(graph: *const ml.graph.Graph, id: ml.graph.NodeId, index: usize) ?ml.graph.Shape {
    const node = graph.node(id);
    if (index >= node.num_inputs or node.inputs[index] == ml.graph.null_node or node.inputs[index] >= graph.nodeCount()) return null;
    return graph.node(node.inputs[index]).output_shape;
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
                break :blk .{ .transpose = Layout.transpose(input.dims[0..input.rank()], shape.dims[0..shape.rank()], attrs.perm[0..attrs.num_axes]) catch return null };
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
        .concat_prim => |attrs| blk: {
            if (node.num_inputs != 2) return null;
            const lhs = inputShape(graph, id, 0) orelse return null;
            const rhs = inputShape(graph, id, 1) orelse return null;
            var expected: [8]i64 = undefined;
            const geometry = kernel_geometry.ConcatAxis.init(lhs.dims[0..lhs.rank()], rhs.dims[0..rhs.rank()], attrs.axis, &expected) catch return null;
            if (!std.mem.eql(i64, expected[0..lhs.rank()], shape.dims[0..shape.rank()]) or (geometry.count() catch return null) != count) return null;
            break :blk .{ .concat = geometry };
        },
        .slice => |attrs| blk: {
            if (node.num_inputs != 1 or attrs.runtime_starts or attrs.runtime_limits) return null;
            const input = inputShape(graph, id, 0) orelse return null;
            if (attrs.num_axes != input.rank()) return null;
            break :blk .{ .slice = Slice.init(input.dims[0..input.rank()], shape.dims[0..shape.rank()], attrs.starts[0..attrs.num_axes], attrs.limits[0..attrs.num_axes], attrs.strides[0..attrs.num_axes]) catch return null };
        },
        .gather => |attrs| blk: {
            if (node.num_inputs != 2 or attrs.nd or attrs.elements or attrs.batch_dims != 0) return null;
            const input = inputShape(graph, id, 0) orelse return null;
            const indices = inputShape(graph, id, 1) orelse return null;
            const geometry = kernel_geometry.GatherAxis.init(std.math.maxInt(u32), input.dims[0..input.rank()], attrs.axis) catch return null;
            const normalize_indices = indices.rank() > 1 and indices.numElements() == 1;
            const index_shape: []const i64 = if (normalize_indices) &.{1} else indices.dims[0..indices.rank()];
            var expected: [8]i64 = undefined;
            const rank = kernel_geometry.gatherShape(input.dims[0..input.rank()], index_shape, attrs.axis, &expected) catch return null;
            if (!std.mem.eql(i64, expected[0..rank], shape.dims[0..shape.rank()]) or (geometry.count(@intCast(indices.numElements() orelse return null)) catch return null) != count) return null;
            break :blk .{ .gather = .{ .axis = attrs.axis, .geometry = geometry } };
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
        .fused_rope => |attrs| .{ .rope = Rope.init(shape.dims[0..shape.rank()], @intCast(count), attrs.seq_len, attrs.head_dim, if (attrs.rope_dim == 0) attrs.head_dim else attrs.rope_dim, attrs.theta, attrs.freq_scale, attrs.position_offset, attrs.consecutive_pairs) catch return null },
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
        .binary, .dot, .concat, .gather => 2,
        .softmax, .rope, .reduction, .transpose, .broadcast, .slice => 1,
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
        const integer_indices = index == 1 and operation == .gather and
            (input_shape.dtype == .i32 or input_shape.dtype == .i64);
        if ((input_shape.dtype != .f32 and !reduced_weight and !integer_indices) or input_count <= 0 or input_count > std.math.maxInt(i32)) return null;
        if ((operation == .unary or operation == .binary or operation == .softmax or operation == .rope) and input_count != 1 and !std.mem.eql(i64, input_shape.dims[0..input_shape.rank()], shape.dims[0..shape.rank()])) return null;
        if ((operation == .unary or operation == .softmax or operation == .rope) and input_count != count) return null;
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

test "shared layout descriptors lower concrete concat slice and gather without wire copies" {
    var graph = ml.graph.Graph.init(std.testing.allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const a = try builder.parameter("a", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const b = try builder.parameter("b", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const joined = try builder.concat(a, b, 1);
    const sliced = try builder.sliceLastDim(a, 1, 3);
    const indices = try builder.parameter("indices", ml.graph.Shape.init(.f32, &.{2}));
    const gathered = try builder.gather(a, indices, ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    try std.testing.expect(lower(&graph, joined).?.operation == .concat);
    try std.testing.expect(lower(&graph, sliced).?.operation == .slice);
    try std.testing.expect(lower(&graph, gathered).?.operation == .gather);
    graph.nodeMut(sliced).op.slice.strides[0] = 0;
    try std.testing.expect(lower(&graph, sliced) == null);
    graph.nodeMut(gathered).op.gather.nd = true;
    try std.testing.expect(lower(&graph, gathered) == null);
    // New logical records fit beneath the existing reduction payload.
    try std.testing.expect(@sizeOf(Slice) <= @sizeOf(Reduction));
}

test "shared gather descriptors preserve exact index storage admission" {
    var graph = ml.graph.Graph.init(std.testing.allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const values = try builder.parameter("values", ml.graph.Shape.init(.f32, &.{ 3, 4 }));
    inline for (.{ ml.graph.DType.f32, ml.graph.DType.i32, ml.graph.DType.i64 }) |dtype| {
        const indices = try builder.parameter(@tagName(dtype), ml.graph.Shape.init(dtype, &.{2}));
        const gathered = try builder.gather(values, indices, ml.graph.Shape.init(.f32, &.{ 2, 4 }));
        const descriptor = lower(&graph, gathered).?;
        try std.testing.expectEqual(dtype, descriptor.input_shapes[1].dtype);
        try std.testing.expectEqual(@as(usize, 8), descriptor.elements());
    }
    const indices = try builder.parameter("unsupported", ml.graph.Shape.init(.f16, &.{2}));
    const gathered = try builder.gather(values, indices, ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    try std.testing.expect(lower(&graph, gathered) == null);
}

test "ordinary RoPE descriptors lower logical batch positions and reject invalid geometry" {
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{ 6, 16 });
    const input = try builder.parameter("x", shape);
    const id = try graph.addNode(.{
        .op = .{ .fused_rope = .{ .seq_len = 3, .head_dim = 8, .rope_dim = 4, .theta = 10000, .freq_scale = 0.5, .position_offset = 7 } },
        .output_shape = shape,
        .inputs = .{ input, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    const descriptor = lower(&graph, id).?;
    try std.testing.expectEqual(@as(u8, 1), descriptor.input_count);
    const rope = descriptor.operation.rope;
    try std.testing.expectEqual(@as(usize, 12), rope.total_chunks);
    try std.testing.expectEqual(@as(usize, 2), rope.chunks_per_token);
    for ([_]usize{ 7, 7, 8, 8, 9, 9, 7, 7, 8, 8, 9, 9 }, 0..) |expected, chunk| try std.testing.expectEqual(expected, rope.positionAt(chunk));
    try std.testing.expectError(error.InvalidRoPEInput, rope.withPosition(std.math.maxInt(usize)));
    graph.nodeMut(id).op.fused_rope.head_dim = 0;
    try std.testing.expect(lower(&graph, id) == null);
    graph.nodeMut(id).op.fused_rope.head_dim = 8;
    graph.nodeMut(id).op.fused_rope.rope_dim = 3;
    try std.testing.expect(lower(&graph, id) == null);
    graph.nodeMut(id).op.fused_rope.rope_dim = 4;
    graph.nodeMut(id).num_inputs = 3;
    graph.nodeMut(id).inputs[1] = input;
    graph.nodeMut(id).inputs[2] = input;
    try std.testing.expect(lower(&graph, id) == null);
}
