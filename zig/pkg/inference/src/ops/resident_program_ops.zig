// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checked static instruction surface measured from boundary encoder/head/PEFT
//! forward and VJP graphs. It deliberately excludes implicit dtype conversion,
//! dynamic shapes, boolean storage and all host fallback operations.
const std = @import("std");
const ml = @import("ml").graph;
const primitive = @import("resident_training_ops.zig");
pub const CudaReduction = @import("cuda/reduction_plan.zig");

pub const Shape = ml.Shape;
pub const Limits = struct {
    primitive: primitive.Limits = .{},
    max_instruction_work: u64 = 1 << 40,
    max_scratch_bytes: usize = 512 * 1024 * 1024,
    /// Explicit CUDA training execution geometry. Both forward and generated
    /// VJP programs must admit the same device-dependent reduction workspace.
    /// Null retains the existing backend arithmetic contract.
    cuda_reduction: ?CudaReduction.Device = null,
};
pub const Affine = struct {
    output_strides: [8]u32 = @splat(0),
    input_strides: [8]u32 = @splat(0),
    base_offset: u32 = 0,
    rank: u8,
};
pub const Dot = struct { batch: u32 = 1, rows: u32, columns: u32, contracting: u32, lhs_transposed: bool = false, rhs_transposed: bool = false };
pub const Geometry = struct {
    input_elements: [4]usize = @splat(0),
    output_elements: usize,
    /// Temporary logical device bytes, excluding the returned output. Driver
    /// workspace and unified-memory process RSS require separate measurement.
    scratch_bytes: usize = 0,
    /// Peak temporary integer grouping descriptors. No activation payloads.
    host_metadata_bytes: usize = 0,
    /// Admission bound for internal scalar status observations; no activations.
    control_readback_upper_bound_bytes: usize = 0,
    work_items: u64,
    affine: ?Affine = null,
    broadcasts: [4]?Affine = @splat(null),
    dot: ?Dot = null,
    cuda_reduction: ?CudaReduction.Plan = null,
};

pub fn count(shape: Shape, limits: primitive.Limits) !usize {
    if (shape.rank_ > 8 or (shape.dtype != .f32 and shape.dtype != .i32))
        return error.UnsupportedResidentProgramDType;
    return primitive.shapeElements(i64, shape.dims[0..shape.rank_], limits);
}

pub fn strides(shape: Shape) ![8]u32 {
    var result: [8]u32 = @splat(0);
    var stride: u32 = 1;
    var axis: usize = shape.rank_;
    while (axis > 0) {
        axis -= 1;
        if (shape.dims[axis] <= 0 or shape.dims[axis] > std.math.maxInt(i32)) return error.InvalidResidentProgramShape;
        result[axis] = stride;
        stride = std.math.mul(u32, stride, @intCast(shape.dims[axis])) catch return error.ResourceLimitExceeded;
    }
    return result;
}

fn validateAffine(affine: Affine, input: Shape, output: Shape, limits: primitive.Limits) !void {
    const elements = try count(input, limits);
    _ = try count(output, limits);
    if (affine.rank != output.rank_) return error.InvalidResidentProgramShape;
    var last: u64 = affine.base_offset;
    for (0..affine.rank) |axis| last += @as(u64, @intCast(output.dims[axis] - 1)) * affine.input_strides[axis];
    if (last >= elements) return error.InvalidResidentProgramShape;
}

/// NumPy/PyTorch trailing broadcast semantics; scalar and same-sized inputs
/// need no materialized expansion. Other mappings use the resident affine
/// copy kernel, so singleton interior axes never become flat modulo repeats.
pub fn broadcastMap(input: Shape, output: Shape, limits: primitive.Limits) !?Affine {
    const input_count = try count(input, limits);
    const output_count = try count(output, limits);
    if (input.dtype != .f32 or output.dtype != .f32 or input.rank_ > output.rank_)
        return error.InvalidResidentProgramShape;
    const offset = output.rank_ - input.rank_;
    var map = Affine{ .rank = output.rank_, .output_strides = try strides(output) };
    const input_strides = try strides(input);
    for (0..input.rank_) |axis| {
        const out_axis = axis + offset;
        if (input.dims[axis] != 1 and input.dims[axis] != output.dims[out_axis]) return error.InvalidResidentProgramShape;
        map.input_strides[out_axis] = if (input.dims[axis] == 1) 0 else input_strides[axis];
    }
    try validateAffine(map, input, output, limits);
    if (input_count == 1 or input_count == output_count) return null;
    return map;
}

/// Shared worst-case bound for admission and dispatch of profiled scatters.
/// A gather-style CUDA reduction performs five additions in each of 32 lanes
/// for every populated output element, in addition to reading all values.
pub fn scatterWork(output: usize, input: usize, reduction_profile: ml.node.ScatterReduction, limits: primitive.Limits) !usize {
    const base = try primitive.scatterWork(output, input, limits);
    if (reduction_profile == .serial_v1) return base;
    const extra = if (reduction_profile == .pytorch_embedding_v1)
        input // At most one additional partial-sum join per input element.
    else
        std.math.mul(usize, @min(output, input), 160) catch return error.ResourceLimitExceeded;
    const total = std.math.add(usize, base, extra) catch return error.ResourceLimitExceeded;
    if (total > limits.max_scatter_work) return error.ResourceLimitExceeded;
    return total;
}

pub const Instruction = struct {
    op: ml.OpCode,
    output: Shape,
    inputs: [4]Shape = @splat(.{}),
    num_inputs: u8,

    pub fn fromNode(graph: *const ml.Graph, node: *const ml.Node) !Instruction {
        if (node.num_inputs > 4) return error.UnsupportedResidentProgramInstruction;
        var result = Instruction{ .op = node.op, .output = node.output_shape, .num_inputs = node.num_inputs };
        for (node.getInputs(), 0..) |id, i| {
            if (id == ml.null_node or id >= graph.nodes.items.len) return error.InvalidResidentProgramDependency;
            result.inputs[i] = graph.node(id).output_shape;
        }
        return result;
    }

    pub fn validate(self: Instruction, limits: Limits) !Geometry {
        if (self.num_inputs > 4) return error.UnsupportedResidentProgramInstruction;
        const out_count = try count(self.output, limits.primitive);
        var geometry = Geometry{ .output_elements = out_count, .work_items = out_count };
        for (self.inputs[0..self.num_inputs], 0..) |shape, i| geometry.input_elements[i] = try count(shape, limits.primitive);
        const n = self.num_inputs;
        switch (self.op) {
            .frozen_span_features_v1 => |attrs| {
                if (n != 2) return error.InvalidFrozenSpanFeaturesShape;
                try attrs.validate(self.output, self.inputs[0], self.inputs[1]);
                geometry.work_items = try std.math.mul(u64, out_count, 4);
            },
            .fused_prefix_scan_v1 => |attrs| {
                const shape = try attrs.shape();
                if (n != 1 or !self.output.eq(shape) or !self.inputs[0].eq(shape)) return error.InvalidResidentProgramShape;
                geometry.scratch_bytes = try attrs.scratchBytes();
                geometry.work_items = if (attrs.singleVector()) ((@as(u64, attrs.width) + 8191) / 8192) * 8192 * 64 else try std.math.add(u64, try std.math.mul(u64, out_count, 32), 16384);
            },
            .fused_linear => |attrs| {
                if (n != 3 or attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0 or
                    self.inputs[0].dtype != .f32 or geometry.input_elements[0] != try std.math.mul(usize, attrs.rows, attrs.in_dim) or
                    !self.inputs[1].eq(Shape.init(.f32, &.{ attrs.out_dim, attrs.in_dim })) or
                    !self.inputs[2].eq(Shape.init(.f32, &.{attrs.out_dim})) or
                    !self.output.eq(Shape.init(.f32, &.{ attrs.rows, attrs.out_dim })))
                    return error.InvalidResidentProgramShape;
                // Pinned CUDA bias-epilogue profile. Account for its entire
                // heuristic workspace before dispatch, even if the selected
                // algorithm ultimately uses less.
                geometry.scratch_bytes = if (attrs.in_dim > 1 and attrs.out_dim > 1) 1024 * 1024 else 0;
                geometry.work_items = try std.math.mul(u64, out_count, @as(u64, attrs.in_dim) + 1);
            },
            .fused_layer_norm, .fused_layer_norm_backward => |attrs| {
                const backward = self.op == .fused_layer_norm_backward;
                const input = self.inputs[0];
                if (n != (if (backward) @as(u8, 4) else 3) or input.dtype != .f32 or self.output.dtype != .f32 or
                    input.rank_ == 0 or attrs.dim == 0 or attrs.dim % 4 != 0 or
                    input.dims[input.rank_ - 1] != attrs.dim or !std.math.isFinite(attrs.eps) or attrs.eps <= 0)
                    return error.InvalidResidentProgramShape;
                const parameter = Shape.init(.f32, &.{@intCast(attrs.dim)});
                const rows = geometry.input_elements[0] / attrs.dim;
                if (!self.inputs[1].eq(parameter) or !self.inputs[2].eq(parameter) or
                    (backward and !self.inputs[3].eq(input)) or
                    !self.output.eq(if (backward) Shape.init(.f32, &.{ @intCast(rows + 2), @intCast(attrs.dim) }) else input))
                    return error.InvalidResidentProgramShape;
                if (backward) geometry.scratch_bytes = try std.math.mul(usize, rows, 8);
                geometry.work_items = try std.math.mul(u64, geometry.input_elements[0], if (backward) 24 else 12);
            },
            .fused_boundary_training_attention_v1, .fused_boundary_training_attention_backward_v1 => |attrs| {
                const backward = self.op == .fused_boundary_training_attention_backward_v1;
                const layout = try attrs.layout();
                if (n != (if (backward) @as(u8, 4) else 2) or
                    !self.inputs[0].eq(layout.qkvShape()) or !self.inputs[1].eq(attrs.maskShape()) or
                    (backward and (!self.inputs[2].eq(layout.savedShape()) or !self.inputs[3].eq(layout.savedShape()))) or
                    !self.output.eq(if (backward) layout.qkvShape() else layout.savedShape()))
                    return error.InvalidResidentProgramShape;
                geometry.scratch_bytes = try layout.scratchBytes(backward);
                geometry.work_items = try std.math.mul(u64, try std.math.mul(u64, @intCast(layout.bias_elements), attrs.num_heads), if (backward) 512 else 128);
            },
            .fused_deberta_training_attention_v1, .fused_deberta_training_attention_backward_v1 => |attrs| {
                const backward = self.op == .fused_deberta_training_attention_backward_v1;
                const layout = try attrs.layout();
                if (n != (if (backward) @as(u8, 4) else 3) or
                    !self.inputs[0].eq(layout.qkvShape()) or !self.inputs[1].eq(layout.relativeShape()) or
                    !self.inputs[2].eq(layout.controlShape()) or
                    (backward and !self.inputs[3].eq(layout.outputShape())) or
                    !self.output.eq(if (backward) layout.gradientShape() else layout.outputShape()))
                    return error.InvalidResidentProgramShape;
                const attention = try @import("deberta_training_attention_device.zig").plan(attrs, backward, .{
                    .max_tensor_bytes = limits.primitive.max_tensor_bytes,
                    .max_scratch_bytes = limits.max_scratch_bytes,
                    .max_host_metadata_bytes = limits.primitive.max_index_metadata_bytes,
                    .max_work_items = limits.max_instruction_work,
                });
                geometry.scratch_bytes = attention.scratch_bytes;
                geometry.host_metadata_bytes = attention.host_metadata_bytes;
                geometry.work_items = attention.work_items;
                geometry.control_readback_upper_bound_bytes = attention.scalar_readback_bytes;
            },
            .reshape => |attrs| {
                if (n != 1 or attrs.runtime_shape or !attrs.new_shape.eq(self.output) or
                    self.inputs[0].dtype != self.output.dtype or geometry.input_elements[0] != out_count)
                    return error.InvalidResidentProgramShape;
            },
            .stop_gradient => {
                if (n != 1 or !self.inputs[0].eq(self.output)) return error.InvalidResidentProgramShape;
            },
            .convert_dtype => |attrs| {
                if (n != 1 or attrs.target != self.output.dtype or !self.inputs[0].eq(self.output))
                    return error.UnsupportedResidentProgramDType;
            },
            .neg, .sqrt, .rsqrt, .exp, .abs, .fused_gelu_exact, .fused_silu, .fused_sigmoid => {
                if (n != 1 or self.output.dtype != .f32 or !self.inputs[0].eq(self.output)) return error.InvalidResidentProgramShape;
            },
            .fused_gelu_exact_backward, .fused_silu_backward, .fused_sigmoid_backward => {
                if (n != 2 or self.output.dtype != .f32 or !self.inputs[0].eq(self.output) or !self.inputs[1].eq(self.output))
                    return error.InvalidResidentProgramShape;
            },
            .add, .mul, .sub, .div, .less_than, .where_select => {
                if (n != (if (self.op == .where_select) @as(u8, 3) else 2) or self.output.dtype != .f32)
                    return error.InvalidResidentProgramShape;
                for (self.inputs[0..n], 0..) |shape, i| {
                    geometry.broadcasts[i] = try broadcastMap(shape, self.output, limits.primitive);
                    if (geometry.broadcasts[i] != null) geometry.scratch_bytes += out_count * 4;
                }
            },
            .fused_softmax, .fused_softmax_backward => |attrs| {
                const backward = self.op == .fused_softmax_backward;
                if (n != (if (backward) @as(u8, 2) else 1) or self.output.dtype != .f32 or self.output.rank_ == 0 or attrs.dim == 0 or
                    !self.inputs[0].eq(self.output) or attrs.dim != self.output.dims[self.output.rank_ - 1] or
                    (backward and !self.inputs[1].eq(self.output)))
                    return error.InvalidResidentProgramShape;
                if ((attrs.fuse_backward or backward) and attrs.dim > 1024) return error.UnsupportedResidentProgramInstruction;
                geometry.work_items *= 3;
            },
            .transpose => |attrs| {
                if (n != 1 or self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or
                    attrs.num_axes != self.output.rank_ or self.inputs[0].rank_ != self.output.rank_)
                    return error.InvalidResidentProgramShape;
                var seen: [8]bool = @splat(false);
                const in_strides = try strides(self.inputs[0]);
                var map = Affine{ .rank = self.output.rank_, .output_strides = try strides(self.output) };
                for (attrs.perm[0..attrs.num_axes], 0..) |axis, i| {
                    if (axis >= self.inputs[0].rank_ or seen[axis] or self.output.dims[i] != self.inputs[0].dims[axis])
                        return error.InvalidResidentProgramShape;
                    seen[axis] = true;
                    map.input_strides[i] = in_strides[axis];
                }
                try validateAffine(map, self.inputs[0], self.output, limits.primitive);
                geometry.affine = map;
            },
            .broadcast_in_dim => |attrs| {
                if (n != 1 or self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or
                    !attrs.target_shape.eq(self.output) or attrs.num_axes != self.inputs[0].rank_)
                    return error.InvalidResidentProgramShape;
                const in_strides = try strides(self.inputs[0]);
                var map = Affine{ .rank = self.output.rank_, .output_strides = try strides(self.output) };
                var seen: [8]bool = @splat(false);
                for (attrs.broadcast_axes[0..attrs.num_axes], 0..) |axis, i| {
                    if (axis >= self.output.rank_ or seen[axis] or
                        (self.inputs[0].dims[i] != 1 and self.inputs[0].dims[i] != self.output.dims[axis]))
                        return error.InvalidResidentProgramShape;
                    seen[axis] = true;
                    map.input_strides[axis] = if (self.inputs[0].dims[i] == 1) 0 else in_strides[i];
                }
                try validateAffine(map, self.inputs[0], self.output, limits.primitive);
                geometry.affine = map;
            },
            .slice => |attrs| {
                if (n != 1 or self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or attrs.runtime_starts or attrs.runtime_limits or
                    attrs.num_axes != self.output.rank_ or self.inputs[0].rank_ != self.output.rank_)
                    return error.InvalidResidentProgramShape;
                const in_strides = try strides(self.inputs[0]);
                var map = Affine{ .rank = self.output.rank_, .output_strides = try strides(self.output) };
                var offset: u64 = 0;
                for (0..attrs.num_axes) |axis| {
                    const start = attrs.starts[axis];
                    const end = attrs.limits[axis];
                    const step = attrs.strides[axis];
                    if (step <= 0 or start < 0 or end <= start or end > self.inputs[0].dims[axis] or
                        self.output.dims[axis] != @divFloor(end - start - 1, step) + 1)
                        return error.InvalidResidentProgramShape;
                    offset += @as(u64, @intCast(start)) * in_strides[axis];
                    const mapped_stride = std.math.mul(u64, @intCast(step), in_strides[axis]) catch return error.ResourceLimitExceeded;
                    map.input_strides[axis] = std.math.cast(u32, mapped_stride) orelse return error.ResourceLimitExceeded;
                }
                map.base_offset = std.math.cast(u32, offset) orelse return error.ResourceLimitExceeded;
                try validateAffine(map, self.inputs[0], self.output, limits.primitive);
                geometry.affine = map;
            },
            .reduce_sum, .reduce_mean, .reduce_max => |attrs| {
                if (n != 1 or self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or
                    attrs.num_axes == 0 or attrs.num_axes > self.output.rank_ or self.output.rank_ != self.inputs[0].rank_)
                    return error.InvalidResidentProgramShape;
                var expected = self.inputs[0];
                var seen: [8]bool = @splat(false);
                for (attrs.axes[0..attrs.num_axes]) |axis| {
                    if (axis >= expected.rank_ or seen[axis]) return error.InvalidResidentProgramShape;
                    seen[axis] = true;
                    expected.dims[axis] = 1;
                }
                if (!expected.eq(self.output)) return error.InvalidResidentProgramShape;
                geometry.work_items = @as(u64, geometry.input_elements[0]) * attrs.num_axes;
                if (attrs.num_axes > 1) geometry.scratch_bytes = geometry.input_elements[0] * 8;
                if (self.op != .reduce_max) if (limits.cuda_reduction) |device| {
                    const reduction = try CudaReduction.Plan.init(self.inputs[0].dims[0..self.inputs[0].rank_], attrs.axes[0..attrs.num_axes], self.op == .reduce_mean, device);
                    geometry.cuda_reduction = reduction;
                    geometry.scratch_bytes = reduction.scratch_bytes;
                    geometry.work_items = @as(u64, geometry.input_elements[0]) * @max(self.inputs[0].rank_, 4) + @as(u64, reduction.scratch_bytes / 4) * 32;
                };
            },
            .concat_prim => |attrs| {
                if (n != 2 or self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or self.inputs[1].dtype != .f32 or
                    attrs.axis >= self.output.rank_ or self.inputs[0].rank_ != self.output.rank_ or self.inputs[1].rank_ != self.output.rank_)
                    return error.InvalidResidentProgramShape;
                for (0..self.output.rank_) |axis| {
                    const wanted = if (axis == attrs.axis) self.inputs[0].dims[axis] + self.inputs[1].dims[axis] else self.inputs[0].dims[axis];
                    if (self.output.dims[axis] != wanted or (axis != attrs.axis and self.inputs[1].dims[axis] != wanted))
                        return error.InvalidResidentProgramShape;
                }
            },
            .dot_general => |attrs| {
                if (n != 2 or self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or self.inputs[1].dtype != .f32 or
                    attrs.num_contracting != 1 or attrs.num_batch > 1)
                    return error.UnsupportedResidentProgramInstruction;
                const lhs = self.inputs[0];
                const rhs = self.inputs[1];
                const rank: u8 = if (attrs.num_batch == 0) 2 else 3;
                const lhs_contract = attrs.lhs_contracting[0];
                const rhs_contract = attrs.rhs_contracting[0];
                const lhs_transposed = lhs_contract == rank - 2;
                const rhs_transposed = rhs_contract == rank - 1;
                if (lhs.rank_ != rank or rhs.rank_ != rank or self.output.rank_ != rank or
                    (lhs_contract != rank - 1 and !lhs_transposed) or (rhs_contract != rank - 2 and !rhs_transposed) or
                    (attrs.num_batch == 1 and (attrs.lhs_batch[0] != 0 or attrs.rhs_batch[0] != 0 or lhs.dims[0] != rhs.dims[0])))
                    return error.UnsupportedResidentProgramInstruction;
                var expected = lhs;
                const lhs_free = if (lhs_transposed) rank - 1 else rank - 2;
                const rhs_free = if (rhs_transposed) rank - 2 else rank - 1;
                expected.dims[rank - 2] = lhs.dims[lhs_free];
                expected.dims[rank - 1] = rhs.dims[rhs_free];
                if (!expected.eq(self.output) or lhs.dims[lhs_contract] != rhs.dims[rhs_contract]) return error.InvalidResidentProgramShape;
                geometry.dot = .{ .batch = if (rank == 3) @intCast(lhs.dims[0]) else 1, .rows = @intCast(lhs.dims[lhs_free]), .columns = @intCast(rhs.dims[rhs_free]), .contracting = @intCast(lhs.dims[lhs_contract]), .lhs_transposed = lhs_transposed, .rhs_transposed = rhs_transposed };
                geometry.work_items = std.math.mul(u64, out_count, geometry.dot.?.contracting) catch return error.ResourceLimitExceeded;
            },
            .gather => |attrs| {
                if (n != 2 or attrs.axis != 0 or attrs.elements or self.inputs[0].rank_ == 0 or
                    self.output.dtype != .f32 or self.inputs[0].dtype != .f32 or self.inputs[1].dtype != .i32)
                    return error.UnsupportedResidentProgramInstruction;
                const width = geometry.input_elements[0] / @as(usize, @intCast(self.inputs[0].dims[0]));
                if (out_count != try std.math.mul(usize, geometry.input_elements[1], width)) return error.InvalidResidentProgramShape;
            },
            .scatter_add => |attrs| {
                if (attrs.padding_index) |index| {
                    if (attrs.reduction != .pytorch_embedding_v1 or self.output.rank_ == 0 or index >= self.output.dims[0]) return error.InvalidResidentProgramShape;
                }
                if ((n != 2 and n != 3) or attrs.axis != 0 or self.output.dtype != .f32 or self.output.rank_ == 0)
                    return error.UnsupportedResidentProgramInstruction;
                const value_index: usize = if (n == 3) 1 else 0;
                const index_index: usize = value_index + 1;
                if (self.inputs[value_index].dtype != .f32 or self.inputs[index_index].dtype != .i32 or
                    (n == 3 and !self.inputs[0].eq(self.output))) return error.InvalidResidentProgramShape;
                const width = out_count / @as(usize, @intCast(self.output.dims[0]));
                if (geometry.input_elements[value_index] != try std.math.mul(usize, geometry.input_elements[index_index], width))
                    return error.InvalidResidentProgramShape;
                geometry.work_items = try scatterWork(out_count, geometry.input_elements[value_index], attrs.reduction, limits.primitive);
                const groups = try @import("resident_training_groups.zig").plan(geometry.input_elements[index_index], @intCast(self.output.dims[0]), .{ .max_metadata_bytes = limits.primitive.max_index_metadata_bytes });
                // Each private descriptor upload temporarily owns a shared
                // staging buffer. Offsets is the longest descriptor (N+1).
                const upload_staging = try std.math.mul(usize, try std.math.add(usize, geometry.input_elements[index_index], 1), 4);
                geometry.scratch_bytes = try std.math.add(usize, try std.math.add(usize, groups.persistent_bytes, upload_staging), if (n == 3) out_count * 4 else @as(usize, 0));
                geometry.work_items = try std.math.add(u64, geometry.work_items, groups.sort_work);
                geometry.host_metadata_bytes = groups.peak_bytes;
            },
            else => return error.UnsupportedResidentProgramInstruction,
        }
        if (geometry.work_items > limits.max_instruction_work or geometry.scratch_bytes > limits.max_scratch_bytes)
            return error.ResourceLimitExceeded;
        return geometry;
    }
};

test "resident fused linear validates parameter shapes and bounded bias workspace" {
    var instruction = Instruction{
        .op = .{ .fused_linear = .{ .rows = 2, .in_dim = 3, .out_dim = 4 } },
        .output = Shape.init(.f32, &.{ 2, 4 }),
        .inputs = .{ Shape.init(.f32, &.{ 2, 3 }), Shape.init(.f32, &.{ 4, 3 }), Shape.init(.f32, &.{4}), .{} },
        .num_inputs = 3,
    };
    const workspace = 1024 * 1024;
    const admitted = try instruction.validate(.{ .max_scratch_bytes = workspace });
    try std.testing.expectEqual(@as(usize, workspace), admitted.scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_scratch_bytes = workspace - 1 }));
    instruction.inputs[2] = Shape.init(.f32, &.{ 1, 4 });
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.op.fused_linear.out_dim = 1;
    instruction.output = Shape.init(.f32, &.{ 2, 1 });
    instruction.inputs[1] = Shape.init(.f32, &.{ 1, 3 });
    instruction.inputs[2] = Shape.init(.f32, &.{1});
    try std.testing.expectEqual(@as(usize, 0), (try instruction.validate(.{ .max_scratch_bytes = 0 })).scratch_bytes);
}

test "resident program descriptors reject dtype aliases malformed broadcasts and invalid geometry" {
    const limits = Limits{};
    var instruction = Instruction{ .op = .add, .output = Shape.init(.f32, &.{ 2, 3 }), .inputs = .{ Shape.init(.f32, &.{ 2, 3 }), Shape.init(.f32, &.{3}), .{}, .{} }, .num_inputs = 2 };
    const geometry = try instruction.validate(limits);
    try std.testing.expect(geometry.broadcasts[1] != null);
    instruction.inputs[1] = Shape.init(.f32, &.{2});
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(limits));
    instruction = .{ .op = .{ .convert_dtype = .{ .target = .i64 } }, .output = Shape.init(.i64, &.{2}), .inputs = .{ Shape.init(.i32, &.{2}), .{}, .{}, .{} }, .num_inputs = 1 };
    try std.testing.expectError(error.UnsupportedResidentProgramDType, instruction.validate(limits));
    instruction = .{ .op = .{ .transpose = .{ .perm = .{ 0, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 2 } }, .output = Shape.init(.f32, &.{ 2, 2 }), .inputs = .{ Shape.init(.f32, &.{ 2, 2 }), .{}, .{}, .{} }, .num_inputs = 1 };
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(limits));
}

test "resident layer normalization validates packed gradients and scratch admission" {
    const input = Shape.init(.f32, &.{ 65, 4 });
    const parameter = Shape.init(.f32, &.{4});
    var instruction = Instruction{
        .op = .{ .fused_layer_norm_backward = .{ .dim = 4, .eps = 1e-5 } },
        .output = Shape.init(.f32, &.{ 67, 4 }),
        .inputs = .{ input, parameter, parameter, input },
        .num_inputs = 4,
    };
    const geometry = try instruction.validate(.{});
    try std.testing.expectEqual(@as(usize, 65 * 8), geometry.scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_scratch_bytes = 65 * 8 - 1 }));
    instruction.inputs[3] = parameter;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.inputs[3] = input;
    instruction.output = input;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.op = .{ .fused_layer_norm = .{ .dim = 4, .eps = 1e-5 } };
    instruction.num_inputs = 3;
    try std.testing.expectEqual(@as(usize, 0), (try instruction.validate(.{ .max_scratch_bytes = 0 })).scratch_bytes);
    instruction.op.fused_layer_norm.eps = 0;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
}

test "resident program affine maps preserve interior singleton broadcasts and strided slice offsets" {
    const map = (try broadcastMap(Shape.init(.f32, &.{ 2, 1, 3 }), Shape.init(.f32, &.{ 2, 4, 3 }), .{})).?;
    try std.testing.expectEqualSlices(u32, &.{ 12, 3, 1 }, map.output_strides[0..3]);
    try std.testing.expectEqualSlices(u32, &.{ 3, 0, 1 }, map.input_strides[0..3]);
    const instruction = Instruction{
        .op = .{ .slice = .{ .starts = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .limits = .{ 4, 5, 0, 0, 0, 0, 0, 0 }, .strides = .{ 2, 2, 1, 1, 1, 1, 1, 1 }, .num_axes = 2 } },
        .output = Shape.init(.f32, &.{ 2, 3 }),
        .inputs = .{ Shape.init(.f32, &.{ 4, 5 }), .{}, .{}, .{} },
        .num_inputs = 1,
    };
    const geometry = try instruction.validate(.{});
    try std.testing.expectEqual(@as(u32, 5), geometry.affine.?.base_offset);
    try std.testing.expectEqualSlices(u32, &.{ 10, 2 }, geometry.affine.?.input_strides[0..2]);
}

test "resident program replay attention validates four physical inputs and exact device admission" {
    const attrs = ml.DebertaTrainingAttentionAttrs{ .batch = 2, .seq_len = 7, .num_heads = 2, .head_dim = 4, .relative_rows = 512, .dropout_probability = 0.125, .dropout_stream_id = 3 };
    const layout = try attrs.layout();
    var instruction = Instruction{
        .op = .{ .fused_deberta_training_attention_backward_v1 = attrs },
        .output = layout.gradientShape(),
        .inputs = .{ layout.qkvShape(), layout.relativeShape(), layout.controlShape(), layout.outputShape() },
        .num_inputs = 4,
    };
    const geometry = try instruction.validate(.{});
    const attention = try @import("deberta_training_attention_device.zig").plan(attrs, true, .{});
    try std.testing.expectEqualSlices(usize, &attention.input_elements, &geometry.input_elements);
    try std.testing.expectEqual(attention.output_elements, geometry.output_elements);
    try std.testing.expectEqual(attention.scratch_bytes, geometry.scratch_bytes);
    try std.testing.expectEqual(attention.host_metadata_bytes, geometry.host_metadata_bytes);
    try std.testing.expectEqual(attention.work_items, geometry.work_items);
    instruction.num_inputs = 3;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.num_inputs = 4;
    instruction.inputs[2].dtype = .f32;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.inputs[2] = layout.controlShape();
    instruction.inputs[3] = layout.relativeShape();
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.inputs[3] = layout.outputShape();
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = geometry.work_items - 1 }));
}

test "resident fused softmax validates backward inputs and warp extent" {
    const shape = Shape.init(.f32, &.{ 7, 59 });
    var instruction = Instruction{
        .op = .{ .fused_softmax_backward = .{ .dim = 59, .fuse_backward = true } },
        .output = shape,
        .inputs = .{ shape, shape, .{}, .{} },
        .num_inputs = 2,
    };
    const geometry = try instruction.validate(.{ .max_scratch_bytes = 0 });
    try std.testing.expectEqual(@as(usize, 0), geometry.scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = geometry.work_items - 1 }));
    instruction.num_inputs = 1;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.num_inputs = 2;
    instruction.inputs[1] = Shape.init(.f32, &.{ 1, 59 });
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    const large = Shape.init(.f32, &.{ 7, 1025 });
    instruction.inputs = .{ large, large, .{}, .{} };
    instruction.output = large;
    instruction.op.fused_softmax_backward.dim = 1025;
    try std.testing.expectError(error.UnsupportedResidentProgramInstruction, instruction.validate(.{}));
    instruction.op = .{ .fused_softmax = .{ .dim = 1025 } };
    instruction.num_inputs = 1;
    _ = try instruction.validate(.{}); // Existing non-fused profile remains supported.
    instruction.op.fused_softmax.dim = 0;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
}

test "resident batched dot validates retained RHS storage and resource limits" {
    var instruction = Instruction{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output = Shape.init(.f32, &.{ 2, 3, 4 }),
        .inputs = .{ Shape.init(.f32, &.{ 2, 3, 5 }), Shape.init(.f32, &.{ 2, 4, 5 }), .{}, .{} },
        .num_inputs = 2,
    };
    const geometry = try instruction.validate(.{ .max_scratch_bytes = 0 });
    try std.testing.expectEqual(Dot{ .batch = 2, .rows = 3, .columns = 4, .contracting = 5, .rhs_transposed = true }, geometry.dot.?);
    try std.testing.expectEqual(@as(u64, 120), geometry.work_items);
    try std.testing.expectEqual(@as(usize, 0), geometry.scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = 119 }));
    instruction.inputs[1] = Shape.init(.f32, &.{ 2, 4, 6 });
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.inputs[1] = Shape.init(.f32, &.{ 1, 4, 5 });
    try std.testing.expectError(error.UnsupportedResidentProgramInstruction, instruction.validate(.{}));
    instruction.inputs[1] = Shape.init(.f32, &.{ 2, 4, 5 });
    instruction.output = Shape.init(.f32, &.{ 2, 3, 5 });
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.output = Shape.init(.f32, &.{ 2, 3, 4 });
    instruction.op.dot_general.rhs_contracting[0] = 0;
    try std.testing.expectError(error.UnsupportedResidentProgramInstruction, instruction.validate(.{}));
}

test "resident matrix dots validate both retained operand layouts" {
    for ([_]u8{ 2, 3 }) |rank| {
        for ([_]bool{ false, true }) |lt| {
            for ([_]bool{ false, true }) |rt| {
                const lhs_dims = [_]i64{ 2, if (lt) 5 else 3, if (lt) 3 else 5 };
                const rhs_dims = [_]i64{ 2, if (rt) 4 else 5, if (rt) 5 else 4 };
                const out_dims = [_]i64{ 2, 3, 4 };
                var instruction = Instruction{
                    .op = .{ .dot_general = .{
                        .lhs_contracting = .{ if (lt) rank - 2 else rank - 1, 0, 0, 0, 0, 0, 0, 0 },
                        .rhs_contracting = .{ if (rt) rank - 1 else rank - 2, 0, 0, 0, 0, 0, 0, 0 },
                        .num_contracting = 1,
                        .num_batch = if (rank == 3) 1 else 0,
                    } },
                    .output = Shape.init(.f32, out_dims[3 - rank ..]),
                    .inputs = .{ Shape.init(.f32, lhs_dims[3 - rank ..]), Shape.init(.f32, rhs_dims[3 - rank ..]), .{}, .{} },
                    .num_inputs = 2,
                };
                const geometry = try instruction.validate(.{ .max_scratch_bytes = 0 });
                const batch: u32 = if (rank == 3) 2 else 1;
                try std.testing.expectEqual(Dot{ .batch = batch, .rows = 3, .columns = 4, .contracting = 5, .lhs_transposed = lt, .rhs_transposed = rt }, geometry.dot.?);
                try std.testing.expectEqual(@as(u64, 60) * batch, geometry.work_items);
                try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = 60 * batch - 1 }));
                instruction.inputs[1].dims[if (rt) rank - 1 else rank - 2] = 6;
                try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
                instruction.op.dot_general.lhs_contracting[0] = rank;
                try std.testing.expectError(error.UnsupportedResidentProgramInstruction, instruction.validate(.{}));
            }
        }
    }
}

test "resident boundary attention admits exact scratch and rejects malformed saved state" {
    const Attrs = ml.node.BoundaryTrainingAttentionAttrs;
    for ([_]Attrs{ .{ .batch = 1, .seq_len = 1, .num_heads = 1 }, .{ .batch = 2, .seq_len = 65, .num_heads = 4, .window = 3 } }) |attrs| {
        const layout = try attrs.layout();
        var instruction = Instruction{ .op = .{ .fused_boundary_training_attention_backward_v1 = attrs }, .num_inputs = 4, .inputs = .{ layout.qkvShape(), attrs.maskShape(), layout.savedShape(), layout.savedShape() }, .output = layout.qkvShape() };
        const expected = try layout.scratchBytes(true);
        const geometry = try instruction.validate(.{ .max_scratch_bytes = expected });
        try std.testing.expectEqual(expected, geometry.scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), geometry.control_readback_upper_bound_bytes);
        try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_scratch_bytes = expected - 1 }));
        try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = geometry.work_items - 1 }));
        instruction.inputs[2] = layout.attendedShape();
        try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
        instruction.inputs[2] = layout.savedShape();
        instruction.op = .{ .fused_boundary_training_attention_v1 = attrs };
        instruction.num_inputs = 2;
        instruction.output = layout.savedShape();
        try std.testing.expectEqual(try layout.scratchBytes(false), (try instruction.validate(.{})).scratch_bytes);
    }
    try std.testing.expectError(error.InvalidBoundaryTrainingAttentionShape, (Attrs{ .batch = 1, .seq_len = 0, .num_heads = 1 }).layout());
    try std.testing.expectError(error.InvalidBoundaryTrainingAttentionShape, (Attrs{ .batch = 65535, .seq_len = 65535, .num_heads = 65535 }).layout());
}

test "resident prefix scan validates layout scratch and work bounds" {
    const attrs = ml.node.PrefixScanAttrs{ .batch = 1, .width = 16384, .channels = 1 };
    const shape = try attrs.shape();
    var instruction = Instruction{ .op = .{ .fused_prefix_scan_v1 = attrs }, .output = shape, .inputs = .{ shape, .{}, .{}, .{} }, .num_inputs = 1 };
    const admitted = try instruction.validate(.{});
    try std.testing.expectEqual(@as(usize, 8), admitted.scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_scratch_bytes = 7 }));
    instruction.inputs[0] = Shape.init(.f32, &.{ 8192, 2 });
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.inputs[0] = shape;
    instruction.op.fused_prefix_scan_v1.reference = .inner;
    instruction.op.fused_prefix_scan_v1.channels = 2;
    try std.testing.expectError(error.InvalidPrefixScanShape, instruction.validate(.{}));
    instruction.op.fused_prefix_scan_v1 = .{ .batch = 1, .width = 0, .channels = 1 };
    try std.testing.expectError(error.InvalidPrefixScanShape, instruction.validate(.{}));
    instruction.op.fused_prefix_scan_v1 = .{ .batch = 65536, .width = 65536, .channels = 1 };
    try std.testing.expectError(error.InvalidPrefixScanShape, instruction.validate(.{}));
}

test "resident CUDA reference reduction admits exact global workspace and work" {
    const device = CudaReduction.Device{ .multiprocessors = 58, .max_threads_per_multiprocessor = 1536 };
    const instruction = Instruction{ .op = .{ .reduce_sum = .{ .axes = .{ 0, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } }, .output = Shape.init(.f32, &.{ 1, 8 }), .inputs = .{ Shape.init(.f32, &.{ 131072, 8 }), .{}, .{}, .{} }, .num_inputs = 1 };
    const geometry = try instruction.validate(.{ .cuda_reduction = device });
    try std.testing.expect(geometry.cuda_reduction != null);
    try std.testing.expect(geometry.scratch_bytes > 0);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .cuda_reduction = device, .max_scratch_bytes = geometry.scratch_bytes - 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .cuda_reduction = device, .max_instruction_work = geometry.work_items - 1 }));
    try std.testing.expectEqual(geometry.scratch_bytes, (try instruction.validate(.{ .cuda_reduction = device, .max_scratch_bytes = geometry.scratch_bytes })).scratch_bytes);
    // Existing generic arithmetic remains the default, including its workspace.
    try std.testing.expectEqual(@as(usize, 0), (try instruction.validate(.{})).scratch_bytes);
    try std.testing.expect((try instruction.validate(.{})).cuda_reduction == null);
}

test "resident frozen span features admit bounded work without scratch" {
    const attrs = ml.node.FrozenSpanFeaturesAttrs{ .batch = 2, .capacity = 192 };
    var instruction = Instruction{ .op = .{ .frozen_span_features_v1 = attrs }, .output = try attrs.shape(), .inputs = .{ Shape.init(.f32, &.{ 384, 1 }), Shape.init(.f32, &.{ 2, 1 }), .{}, .{} }, .num_inputs = 2 };
    const geometry = try instruction.validate(.{ .max_scratch_bytes = 0 });
    try std.testing.expectEqual(@as(usize, 0), geometry.scratch_bytes);
    try std.testing.expectEqual(@as(u64, 4608), geometry.work_items);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = geometry.work_items - 1 }));
    instruction.inputs[1] = Shape.init(.f32, &.{ 1, 2 });
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, instruction.validate(.{}));
    instruction.inputs[1] = Shape.init(.f32, &.{ 2, 1 });
    instruction.num_inputs = 1;
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, instruction.validate(.{}));
    instruction.num_inputs = 2;
    instruction.op.frozen_span_features_v1.capacity = 0;
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, instruction.validate(.{}));
}

test "resident gather backward admits warp reduction work explicitly" {
    var instruction = Instruction{ .op = .{ .scatter_add = .{ .axis = 0 } }, .output = Shape.init(.f32, &.{ 3, 2 }), .inputs = .{ Shape.init(.f32, &.{ 5, 2 }), Shape.init(.i32, &.{5}), .{}, .{} }, .num_inputs = 2 };
    const serial = try instruction.validate(.{ .primitive = .{ .max_scatter_work = 16 } });
    instruction.op.scatter_add.reduction = .pytorch_gather_v1;
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .primitive = .{ .max_scatter_work = 975 } }));
    const profiled = try instruction.validate(.{ .primitive = .{ .max_scatter_work = 976 } });
    try std.testing.expectEqual(serial.work_items + 960, profiled.work_items);
    try std.testing.expectEqual(serial.scratch_bytes, profiled.scratch_bytes);
    try std.testing.expectEqual(serial.host_metadata_bytes, profiled.host_metadata_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = profiled.work_items - 1 }));
}

test "resident embedding backward admits partial work and validates padding" {
    var instruction = Instruction{ .op = .{ .scatter_add = .{ .axis = 0, .reduction = .pytorch_embedding_v1, .padding_index = 2 } }, .output = Shape.init(.f32, &.{ 3, 2 }), .inputs = .{ Shape.init(.f32, &.{ 5, 2 }), Shape.init(.i32, &.{5}), .{}, .{} }, .num_inputs = 2 };
    const profiled = try instruction.validate(.{ .primitive = .{ .max_scatter_work = 26 } });
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .primitive = .{ .max_scatter_work = 25 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, instruction.validate(.{ .max_instruction_work = profiled.work_items - 1 }));
    instruction.op.scatter_add.padding_index = 3;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
    instruction.op.scatter_add.padding_index = 2;
    instruction.op.scatter_add.reduction = .serial_v1;
    try std.testing.expectError(error.InvalidResidentProgramShape, instruction.validate(.{}));
}
