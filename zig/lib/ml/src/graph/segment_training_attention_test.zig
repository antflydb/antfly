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

const std = @import("std");
const graph = @import("graph.zig");
const node = @import("node.zig");
const Shape = @import("shape.zig").Shape;
const Builder = @import("builder.zig").Builder;
const autodiff = @import("autodiff.zig");
const lower = @import("lower.zig");

const attrs = node.SegmentTrainingAttentionAttrs{
    .batch = 2,
    .seq_len = 7,
    .num_heads = 2,
    .head_dim = 3,
    .window = 4,
    .dropout_probability = 0.125,
    .dropout_stream_id = (@as(u64, 3) << 32) | 2,
};

fn exercise(a: std.mem.Allocator) !void {
    var g = graph.Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const layout = try attrs.layout();
    var params: [3]node.NodeId = undefined;
    inline for (.{ "q", "k", "v" }, 0..) |name, i|
        params[i] = try b.parameter(name, layout.outputShape());
    const qkv = try b.concat(try b.concat(params[0], params[1], 0), params[2], 0);
    const control = try b.parameter("control", layout.controlShape());
    const cotangent = try b.parameter("cotangent", layout.outputShape());
    const output = try b.segmentTrainingAttentionV1(qkv, control, attrs);
    // Even a same-shape stale alternate must not replace the custom VJP.
    g.nodes.items[output].vjp_alternate = params[0];
    try g.markOutput(output);
    const count = g.nodeCount();
    var result = try autodiff.gradientWithSeeds(a, &g, &.{.{ .output = output, .cotangent = cotangent }}, &params, .{ .require_all_gradients = true });
    defer result.deinit();
    try std.testing.expectEqual(count, g.nodeCount());
    try std.testing.expectEqualSlices(node.NodeId, &.{output}, g.outputs.items);
    try std.testing.expectEqual(.fused_segment_training_attention_v1, std.meta.activeTag(result.graph.node(result.id_map[output]).op));
    for (result.param_grads, params) |grad, original| {
        try std.testing.expect(grad != node.null_node);
        try std.testing.expect(result.graph.node(grad).output_shape.eq(g.node(original).output_shape));
    }
    var backwards: usize = 0;
    for (result.graph.nodes.items) |n| {
        if (n.op != .fused_segment_training_attention_backward_v1) continue;
        backwards += 1;
        try std.testing.expectEqualDeep(attrs, n.op.fused_segment_training_attention_backward_v1);
        try std.testing.expectEqual(@as(u8, 3), n.num_inputs);
        try std.testing.expectEqual(result.id_map[control], n.inputs[1]);
        try std.testing.expectEqual(result.id_map[cotangent], n.inputs[2]);
        try std.testing.expect(n.output_shape.eq(layout.qkvShape()));
        try std.testing.expect(result.graph.node(n.inputs[1]).output_shape.eq(layout.controlShape()));
    }
    try std.testing.expectEqual(@as(usize, 1), backwards);
    try std.testing.expect(result.graph.nodeCount() < 64);
    // Lowering a second time must retain the fused replay operation.
    for (result.param_grads) |grad| try result.graph.markOutput(grad);
    var lowered = try lower.lower(a, &result.graph);
    defer lowered.deinit();
    var retained: usize = 0;
    for (lowered.graph.nodes.items) |n| {
        if (n.op == .fused_segment_training_attention_backward_v1) retained += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), retained);
}

test "segment training attention graph preserves all three VJPs and integer control" {
    try exercise(std.testing.allocator);
}

test "segment training attention graph allocation failures preserve caller ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exercise, .{});
}

test "segment training attention graph rejects malformed shapes and unsafe dimensions" {
    const a = std.testing.allocator;
    var g = graph.Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const layout = try attrs.layout();
    const qkv = try b.parameter("qkv", layout.qkvShape());
    const wrong_control = try b.parameter("float_control", Shape.init(.f32, &.{layout.control_elements}));
    const count = g.nodeCount();
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, b.segmentTrainingAttentionV1(qkv, wrong_control, attrs));
    try std.testing.expectError(error.InvalidGraphDependency, b.segmentTrainingAttentionV1(qkv, node.null_node, attrs));
    try std.testing.expectEqual(count, g.nodeCount());
    const control = try b.parameter("control", layout.controlShape());
    const cotangent = try b.parameter("cotangent", layout.outputShape());
    const output = try b.segmentTrainingAttentionV1(qkv, control, attrs);
    g.nodes.items[output].num_inputs = 1;
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, autodiff.gradientWithSeeds(a, &g, &.{.{ .output = output, .cotangent = cotangent }}, &.{qkv}, .{}));
    var invalid = attrs;
    invalid.seq_len = 0;
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, invalid.layout());
    invalid = attrs;
    invalid.dropout_probability = std.math.nan(f32);
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, invalid.layout());
    invalid.dropout_probability = 1;
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, invalid.layout());
    invalid = attrs;
    invalid.num_heads = std.math.maxInt(i32);
    invalid.head_dim = std.math.maxInt(i32);
    try std.testing.expectError(error.Overflow, invalid.layout());
}
