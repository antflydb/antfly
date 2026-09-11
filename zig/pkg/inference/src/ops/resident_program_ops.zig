// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checked static instruction surface measured from boundary encoder/head/PEFT
//! forward and VJP graphs. It deliberately excludes implicit dtype conversion,
//! dynamic shapes, boolean storage and all host fallback operations.
const std = @import("std");
const ml = @import("ml").graph;
const primitive = @import("resident_training_ops.zig");

pub const Shape = ml.Shape;
pub const Limits = struct {
    primitive: primitive.Limits = .{},
    max_instruction_work: u64 = 1 << 40,
    max_scratch_bytes: usize = 512 * 1024 * 1024,
};
pub const Affine = struct {
    output_strides: [8]u32 = @splat(0),
    input_strides: [8]u32 = @splat(0),
    base_offset: u32 = 0,
    rank: u8,
};
pub const Dot = struct { batch: u32 = 1, rows: u32, columns: u32, contracting: u32 };
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
            .neg, .sqrt, .rsqrt, .exp, .abs, .fused_gelu_exact => {
                if (n != 1 or self.output.dtype != .f32 or !self.inputs[0].eq(self.output)) return error.InvalidResidentProgramShape;
            },
            .fused_gelu_exact_backward => {
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
            .fused_softmax => |attrs| {
                if (n != 1 or self.output.dtype != .f32 or self.output.rank_ == 0 or
                    !self.inputs[0].eq(self.output) or attrs.dim != self.output.dims[self.output.rank_ - 1])
                    return error.InvalidResidentProgramShape;
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
                if (lhs.rank_ != rank or rhs.rank_ != rank or self.output.rank_ != rank or
                    attrs.lhs_contracting[0] != rank - 1 or attrs.rhs_contracting[0] != rank - 2 or
                    (attrs.num_batch == 1 and (attrs.lhs_batch[0] != 0 or attrs.rhs_batch[0] != 0 or lhs.dims[0] != rhs.dims[0])))
                    return error.UnsupportedResidentProgramInstruction;
                var expected = lhs;
                expected.dims[rank - 1] = rhs.dims[rank - 1];
                if (!expected.eq(self.output) or lhs.dims[rank - 1] != rhs.dims[rank - 2]) return error.InvalidResidentProgramShape;
                geometry.dot = .{ .batch = if (rank == 3) @intCast(lhs.dims[0]) else 1, .rows = @intCast(lhs.dims[rank - 2]), .columns = @intCast(rhs.dims[rank - 1]), .contracting = @intCast(lhs.dims[rank - 1]) };
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
                if ((n != 2 and n != 3) or attrs.axis != 0 or self.output.dtype != .f32 or self.output.rank_ == 0)
                    return error.UnsupportedResidentProgramInstruction;
                const value_index: usize = if (n == 3) 1 else 0;
                const index_index: usize = value_index + 1;
                if (self.inputs[value_index].dtype != .f32 or self.inputs[index_index].dtype != .i32 or
                    (n == 3 and !self.inputs[0].eq(self.output))) return error.InvalidResidentProgramShape;
                const width = out_count / @as(usize, @intCast(self.output.dims[0]));
                if (geometry.input_elements[value_index] != try std.math.mul(usize, geometry.input_elements[index_index], width))
                    return error.InvalidResidentProgramShape;
                geometry.work_items = try primitive.scatterWork(out_count, geometry.input_elements[value_index], limits.primitive);
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
