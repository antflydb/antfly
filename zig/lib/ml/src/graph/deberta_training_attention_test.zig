// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const graph = @import("graph.zig");
const node = @import("node.zig");
const Shape = @import("shape.zig").Shape;
const Builder = @import("builder.zig").Builder;
const autodiff = @import("autodiff.zig");
const lower = @import("lower.zig");

const attrs = node.DebertaTrainingAttentionAttrs{
    .batch = 2,
    .seq_len = 7,
    .num_heads = 2,
    .head_dim = 3,
    // Distinct from 2*S-1: VJPs must address the compact relative table.
    .relative_rows = 4,
    .dropout_probability = 0.125,
    .dropout_stream_id = (@as(u64, 3) << 32) | 2,
};

fn exercise(a: std.mem.Allocator) !void {
    var g = graph.Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const layout = try attrs.layout();
    var params: [5]node.NodeId = undefined;
    inline for (.{ "q", "k", "v", "qr", "kr" }, 0..) |name, i|
        params[i] = try b.parameter(name, if (i < 3) layout.outputShape() else Shape.init(.f32, &.{ attrs.relative_rows, layout.hidden }));
    const qkv = try b.concat(try b.concat(params[0], params[1], 0), params[2], 0);
    const relative = try b.concat(params[3], params[4], 0);
    const control = try b.parameter("control", layout.controlShape());
    const cotangent = try b.parameter("cotangent", layout.outputShape());
    const output = try b.debertaTrainingAttentionV1(qkv, relative, control, attrs);
    // Even a same-shape stale alternate must not replace the custom VJP.
    g.nodes.items[output].vjp_alternate = params[0];
    try g.markOutput(output);
    const count = g.nodeCount();
    var result = try autodiff.gradientWithSeeds(a, &g, &.{.{ .output = output, .cotangent = cotangent }}, &params, .{ .require_all_gradients = true });
    defer result.deinit();
    try std.testing.expectEqual(count, g.nodeCount());
    try std.testing.expectEqualSlices(node.NodeId, &.{output}, g.outputs.items);
    try std.testing.expectEqual(.fused_deberta_training_attention_v1, std.meta.activeTag(result.graph.node(result.id_map[output]).op));
    for (result.param_grads, params) |grad, original| {
        try std.testing.expect(grad != node.null_node);
        try std.testing.expect(result.graph.node(grad).output_shape.eq(g.node(original).output_shape));
    }
    var backwards: usize = 0;
    for (result.graph.nodes.items) |n| {
        if (n.op != .fused_deberta_training_attention_backward_v1) continue;
        backwards += 1;
        try std.testing.expectEqualDeep(attrs, n.op.fused_deberta_training_attention_backward_v1);
        try std.testing.expectEqual(@as(u8, 4), n.num_inputs);
        try std.testing.expectEqual(result.id_map[control], n.inputs[2]);
        try std.testing.expectEqual(result.id_map[cotangent], n.inputs[3]);
        try std.testing.expect(n.output_shape.eq(Shape.init(.f32, &.{ 50, 6 })));
        try std.testing.expect(result.graph.node(n.inputs[2]).output_shape.eq(Shape.init(.i32, &.{33})));
    }
    try std.testing.expectEqual(@as(usize, 1), backwards);
    try std.testing.expect(result.graph.nodeCount() < 64);
    // Lowering a second time must retain the four-leaf replay operation.
    for (result.param_grads) |grad| try result.graph.markOutput(grad);
    var lowered = try lower.lower(a, &result.graph);
    defer lowered.deinit();
    var retained: usize = 0;
    for (lowered.graph.nodes.items) |n| {
        if (n.op == .fused_deberta_training_attention_backward_v1) retained += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), retained);
}

test "deberta training attention graph preserves all five VJPs and integer replay control" {
    try exercise(std.testing.allocator);
}

test "deberta training attention graph allocation failures preserve caller ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exercise, .{});
}

test "deberta training attention graph rejects malformed shapes and unsafe dimensions" {
    const a = std.testing.allocator;
    var g = graph.Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const layout = try attrs.layout();
    const qkv = try b.parameter("qkv", layout.qkvShape());
    const relative = try b.parameter("relative", layout.relativeShape());
    const wrong_control = try b.parameter("float_control", Shape.init(.f32, &.{layout.control_elements}));
    const count = g.nodeCount();
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, b.debertaTrainingAttentionV1(qkv, relative, wrong_control, attrs));
    try std.testing.expectError(error.InvalidGraphDependency, b.debertaTrainingAttentionV1(qkv, relative, node.null_node, attrs));
    try std.testing.expectEqual(count, g.nodeCount());
    const control = try b.parameter("control", layout.controlShape());
    const cotangent = try b.parameter("cotangent", layout.outputShape());
    const output = try b.debertaTrainingAttentionV1(qkv, relative, control, attrs);
    g.nodes.items[output].num_inputs = 2;
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, autodiff.gradientWithSeeds(a, &g, &.{.{ .output = output, .cotangent = cotangent }}, &.{qkv}, .{}));
    var invalid = attrs;
    invalid.seq_len = 0;
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, invalid.layout());
    invalid = attrs;
    invalid.dropout_probability = std.math.nan(f32);
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, invalid.layout());
    invalid.dropout_probability = 1;
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, invalid.layout());
    invalid = attrs;
    invalid.num_heads = std.math.maxInt(i32);
    invalid.head_dim = std.math.maxInt(i32);
    try std.testing.expectError(error.Overflow, invalid.layout());
}
