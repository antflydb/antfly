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

//! Static metadata transforms. Readable footprints describe initialized values;
//! they never grant writes, padded capacity, ownership credit or hidden copies.
const std = @import("std");
const ml = @import("ml");
const contracts = @import("../graph/backend_contracts.zig");
const ownership = @import("storage_ownership.zig");
const Shape = ml.graph.Shape;
const Strides = contracts.TensorStrides;

pub const Layout = struct {
    shape: Shape,
    strides: Strides,
    readable_bytes: usize,
};
pub const Request = struct { input: Layout, output: Layout };
pub const Plan = struct {
    node: ml.graph.NodeId,
    source: ml.graph.NodeId,
    input: Shape,
    output: Shape,
    kind: enum { reshape, full_slice, transpose, broadcast },
    axes: [8]u8 = @splat(0),

    pub fn lower(graph: *const ml.graph.Graph, id: ml.graph.NodeId, backend: contracts.BackendKind) ?Plan {
        const node = graph.node(id);
        if (node.num_inputs != 1 or node.inputs[0] >= graph.nodeCount()) return null;
        const input = graph.node(node.inputs[0]).output_shape;
        const output = node.output_shape;
        if (input.dtype != .f32 or output.dtype != .f32 or logicalBytes(input) == null or logicalBytes(output) == null) return null;
        var result = Plan{ .node = id, .source = node.inputs[0], .input = input, .output = output, .kind = .reshape };
        switch (node.op) {
            .reshape => |attrs| {
                if (attrs.runtime_shape or !attrs.new_shape.eq(output) or input.numElements() != output.numElements()) return null;
            },
            .slice => |attrs| {
                if ((backend != .native and backend != .metal) or attrs.runtime_starts or attrs.runtime_limits or attrs.num_axes != input.rank() or !input.eq(output)) return null;
                const slice = @import("../ops/slice_plan.zig").Plan.init(input.dims[0..input.rank()], attrs.starts[0..attrs.num_axes], attrs.limits[0..attrs.num_axes], attrs.strides[0..attrs.num_axes], input.dims[0..input.rank()]) catch return null;
                if (slice.base != 0 or slice.count != slice.input_count or !slice.contiguous()) return null;
                result.kind = .full_slice;
            },
            .transpose => |attrs| {
                if (backend != .native or attrs.num_axes != input.rank() or input.rank() != output.rank()) return null;
                result.kind = .transpose;
                var seen: u16 = 0;
                for (0..input.rank()) |axis| {
                    const from = attrs.perm[axis];
                    if (from >= input.rank() or output.dims[axis] != input.dims[from]) return null;
                    const bit = @as(u16, 1) << @intCast(from);
                    if (seen & bit != 0) return null;
                    seen |= bit;
                    result.axes[axis] = from;
                }
            },
            .broadcast_in_dim => |attrs| {
                if (backend != .native or !attrs.target_shape.eq(output) or attrs.num_axes != input.rank() or output.rank() < input.rank()) return null;
                result.kind = .broadcast;
                var seen: u16 = 0;
                for (0..input.rank()) |axis| {
                    const to = attrs.broadcast_axes[axis];
                    if (to >= output.rank() or (input.dims[axis] != 1 and input.dims[axis] != output.dims[to])) return null;
                    const bit = @as(u16, 1) << @intCast(to);
                    if (seen & bit != 0) return null;
                    seen |= bit;
                    result.axes[axis] = to;
                }
            },
            else => return null,
        }
        return result;
    }

    pub fn apply(self: Plan, source: ownership.Plane) ?ownership.Plane {
        const desc = source.descriptor;
        if (source.role != .primary or !desc.shape.eq(self.input) or desc.quant_format != null or desc.attention_storage != null or desc.attention_kv_format != null) return null;
        switch (desc.current_backend orelse return null) {
            .native => if (desc.storage != .host_f32 and desc.storage != .metadata_view) return null,
            .cuda => if (desc.storage != .cuda_buffer or self.kind != .reshape) return null,
            .metal => if (desc.storage != .metal_buffer or (self.kind != .reshape and self.kind != .full_slice)) return null,
            else => return null,
        }
        const input_bytes = logicalBytes(self.input) orelse return null;
        if (source.logical_bytes != input_bytes) return null;
        const read = readableBytes(self.input, desc.strides) orelse return null;
        if (source.readable_bytes < read) return null;
        var strides = Strides{ .rank = self.output.rank() };
        switch (self.kind) {
            .reshape => {
                if (!dense(self.input, desc.strides)) return null;
                strides = Strides.dense(self.output);
            },
            .full_slice => strides = desc.strides,
            .transpose => for (0..self.output.rank()) |axis| {
                strides.values[axis] = desc.strides.values[self.axes[axis]];
            },
            .broadcast => for (0..self.input.rank()) |axis| {
                strides.values[self.axes[axis]] = if (self.input.dims[axis] == 1) 0 else desc.strides.values[axis];
            },
        }
        // Unit dimensions cannot affect addressing. Canonical dense metadata
        // lets existing Into readers consume actual dense aliases directly.
        if (dense(self.output, strides)) strides = Strides.dense(self.output);
        const output_read = readableBytes(self.output, strides) orelse return null;
        if (output_read > read) return null;
        var result = source;
        result.descriptor.shape = self.output;
        result.descriptor.strides = strides;
        if (desc.current_backend == .native) result.descriptor.storage = .metadata_view;
        result.descriptor.view_source = self.source;
        result.logical_bytes = logicalBytes(self.output) orelse return null;
        result.readable_bytes = output_read;
        result.writable = null;
        result.layout_key = null;
        return result;
    }
};

pub fn logicalBytes(shape: Shape) ?usize {
    if (shape.dtype != .f32 or shape.rank() > 8) return null;
    const count = shape.numElements() orelse return null;
    if (count <= 0) return null;
    return std.math.mul(usize, @intCast(count), @sizeOf(f32)) catch null;
}
pub fn readableBytes(shape: Shape, strides: Strides) ?usize {
    if (logicalBytes(shape) == null or strides.rank != shape.rank()) return null;
    var last: usize = 0;
    for (shape.dims[0..shape.rank()], strides.asSlice()) |dimension, stride| {
        if (dimension <= 0 or stride < 0) return null;
        const extent = std.math.mul(usize, @intCast(dimension - 1), @intCast(stride)) catch return null;
        last = std.math.add(usize, last, extent) catch return null;
    }
    return std.math.mul(usize, std.math.add(usize, last, 1) catch return null, @sizeOf(f32)) catch null;
}
pub fn dense(shape: Shape, strides: Strides) bool {
    if (logicalBytes(shape) == null or strides.rank != shape.rank()) return false;
    const expected = Strides.dense(shape);
    for (shape.dims[0..shape.rank()], strides.asSlice(), expected.asSlice()) |dimension, actual, wanted| {
        if (actual < 0 or (dimension != 1 and actual != wanted)) return false;
    }
    return true;
}
pub fn request(input: ownership.Plane, output: ownership.Plane) Request {
    return .{ .input = .{ .shape = input.descriptor.shape, .strides = input.descriptor.strides, .readable_bytes = readableBytes(input.descriptor.shape, input.descriptor.strides) orelse 0 }, .output = .{ .shape = output.descriptor.shape, .strides = output.descriptor.strides, .readable_bytes = output.readable_bytes } };
}

test "affine facts separate logical bytes from initialized broadcast footprint" {
    const shape = Shape.init(.f32, &.{ 1, 3 });
    var descriptor = contracts.TensorDesc.init(shape, .host_f32);
    descriptor.current_backend = .native;
    const source = ownership.Plane{ .role = .primary, .descriptor = descriptor, .logical_bytes = 12, .readable_bytes = 16, .storage_epoch = 7, .backing = .{ .kind = .host, .base = 4096 }, .byte_offset = 8, .layout_key = 123 };
    const plan = Plan{ .node = 5, .source = 4, .input = shape, .output = Shape.init(.f32, &.{ 2, 3 }), .kind = .broadcast, .axes = .{ 0, 1, 0, 0, 0, 0, 0, 0 } };
    const alias = plan.apply(source) orelse return error.ExpectedAffinePlan;
    try std.testing.expectEqual(@as(usize, 24), alias.logical_bytes);
    try std.testing.expectEqual(@as(usize, 12), alias.readable_bytes);
    try std.testing.expectEqual(@as(usize, 12), request(source, alias).input.readable_bytes);
    try std.testing.expectEqual(@as(usize, 8), alias.byte_offset);
    try std.testing.expect(alias.writable == null and alias.layout_key == null);
    try std.testing.expectEqualSlices(i64, &.{ 0, 1 }, alias.descriptor.strides.asSlice());
    var short = source;
    short.readable_bytes = 8;
    try std.testing.expect(plan.apply(short) == null);
}

test "affine reshape declines strided input and malformed layouts" {
    const shape = Shape.init(.f32, &.{ 2, 3 });
    var descriptor = contracts.TensorDesc.init(shape, .metadata_view);
    descriptor.current_backend = .native;
    descriptor.strides = Strides.init(&.{ 1, 2 });
    const source = ownership.Plane{ .role = .primary, .descriptor = descriptor, .logical_bytes = 24, .readable_bytes = 24, .storage_epoch = 1, .byte_offset = 0 };
    const plan = Plan{ .node = 4, .source = 3, .input = shape, .output = Shape.init(.f32, &.{6}), .kind = .reshape };
    try std.testing.expect(plan.apply(source) == null);
    try std.testing.expect(readableBytes(shape, Strides.init(&.{ -1, 1 })) == null);
    try std.testing.expect(readableBytes(shape, Strides.init(&.{ std.math.maxInt(i64), std.math.maxInt(i64) })) == null);
    try std.testing.expect(readableBytes(Shape.init(.i64, &.{3}), Strides.init(&.{1})) == null);
}

test "affine lowering declines inconsistent static target attributes" {
    var graph = ml.graph.Graph.init(std.testing.allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const reshaped = try builder.reshape(input, Shape.init(.f32, &.{ 3, 2 }));
    try std.testing.expect(Plan.lower(&graph, reshaped, .native) != null);
    graph.nodes.items[reshaped].op.reshape.new_shape = Shape.init(.f32, &.{6});
    try std.testing.expect(Plan.lower(&graph, reshaped, .native) == null);
    const output = Shape.init(.f32, &.{ 2, 2, 3 });
    var axes: [8]u8 = @splat(0);
    axes[0] = 1;
    axes[1] = 2;
    const expanded = try graph.addNode(.{ .op = .{ .broadcast_in_dim = .{ .target_shape = output, .broadcast_axes = axes, .num_axes = 2 } }, .output_shape = output, .inputs = .{ input, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
    try std.testing.expect(Plan.lower(&graph, expanded, .native) != null);
    graph.nodes.items[expanded].op.broadcast_in_dim.target_shape = Shape.init(.f32, &.{12});
    try std.testing.expect(Plan.lower(&graph, expanded, .native) == null);
}

/// Only the complete dense logical view is retained; extra SDK/bank capacity
/// never becomes initialized data or a writable alias capability.
pub fn fullSpanRequest(value: *const Request, actual_shape: []const i64, available_bytes: usize) bool {
    const bytes = logicalBytes(value.input.shape) orelse return false;
    return value.input.shape.rank() == actual_shape.len and std.mem.eql(i64, value.input.shape.dims[0..value.input.shape.rank()], actual_shape) and logicalBytes(value.output.shape) == bytes and value.input.readable_bytes == bytes and value.output.readable_bytes == bytes and bytes <= available_bytes and dense(value.input.shape, value.input.strides) and dense(value.output.shape, value.output.strides);
}
/// Follow the existing planner's alias/root lifetime, never inventing a new
/// root or recoloring a live source. External inputs/constants remain opaque.
pub fn activationRoot(plan: *const @import("../graph/buffer_plan.zig").BufferPlan, node: ml.graph.NodeId, backend: contracts.BackendKind) ?@import("../graph/buffer_plan.zig").LogicalSlot {
    var current = (plan.slotForNode(node) orelse return null).*;
    var depth: usize = 0;
    while (current.kind == .view) : (depth += 1) {
        if (current.backend != backend or depth >= plan.slots.len) return null;
        const parent = current.source_slot orelse return null;
        if (parent >= plan.slots.len) return null;
        current = plan.slots[parent];
    }
    return if (current.backend == backend and current.kind == .allocation) current else null;
}

test "GPU full span alias metadata preserves source offsets and rejects padded or strided claims" {
    inline for (.{ contracts.BackendKind.cuda, contracts.BackendKind.metal }) |backend| {
        const input = Shape.init(.f32, &.{ 2, 3 });
        const output = Shape.init(.f32, &.{6});
        var descriptor = contracts.TensorDesc.init(input, if (backend == .cuda) .cuda_buffer else .metal_buffer);
        descriptor.current_backend = backend;
        const source = ownership.Plane{ .descriptor = descriptor, .role = .primary, .storage_epoch = 7, .backing = .{ .kind = if (backend == .cuda) .cuda else .metal, .base = 4096 }, .byte_offset = 16, .logical_bytes = 24, .readable_bytes = 24 };
        const plan = Plan{ .node = 3, .source = 2, .input = input, .output = output, .kind = .reshape };
        const alias = plan.apply(source) orelse return error.MissingFullSpanAlias;
        try std.testing.expectEqual(@as(usize, 16), alias.byte_offset);
        try std.testing.expectEqual(@as(usize, 24), alias.readable_bytes);
        try std.testing.expect(alias.writable == null and alias.layout_key == null);
        var exact = request(source, alias);
        try std.testing.expect(fullSpanRequest(&exact, &.{ 2, 3 }, 512));
        exact.output.readable_bytes = 28;
        try std.testing.expect(!fullSpanRequest(&exact, &.{ 2, 3 }, 512));
        exact = request(source, alias);
        exact.output.strides = Strides.init(&.{2});
        try std.testing.expect(!fullSpanRequest(&exact, &.{ 2, 3 }, 512));
        exact = request(source, alias);
        try std.testing.expect(!fullSpanRequest(&exact, &.{ 2, 3 }, 20));
        var affine = plan;
        affine.kind = .transpose;
        try std.testing.expect(affine.apply(source) == null);
    }
}
