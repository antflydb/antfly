// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const graph = @import("graph.zig");
const node = @import("node.zig");
const Shape = @import("shape.zig").Shape;
const Builder = @import("builder.zig").Builder;
const autodiff = @import("autodiff.zig");

const attrs = node.ModernBertTrainingAttentionAttrs{ .batch = 2, .seq_len = 5, .num_heads = 2, .head_dim = 4, .window = 3 };

test "modernbert training attention differentiates packed rows through one replayed backward node" {
    const a = std.testing.allocator;
    var g = graph.Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const layout = try attrs.layout();
    try std.testing.expect(layout.controlShape().eq(Shape.init(.i32, &.{70})));
    var params: [3]node.NodeId = undefined;
    inline for (.{ "q", "k", "v" }, 0..) |name, i| params[i] = try b.parameter(name, layout.outputShape());
    const qkv = try b.concat(try b.concat(params[0], params[1], 0), params[2], 0);
    const control = try b.parameter("control", layout.controlShape());
    const cotangent = try b.parameter("cotangent", layout.outputShape());
    try std.testing.expectError(error.InvalidModernBertTrainingAttentionShape, b.modernBertTrainingAttentionV1(params[0], control, attrs));
    const output = try b.modernBertTrainingAttentionV1(qkv, control, attrs);
    try std.testing.expect(g.node(output).output_shape.eq(layout.outputShape()));
    var result = try autodiff.gradientWithSeeds(a, &g, &.{.{ .output = output, .cotangent = cotangent }}, &params, .{ .require_all_gradients = true });
    defer result.deinit();
    for (result.param_grads, params) |grad, original| try std.testing.expect(result.graph.node(grad).output_shape.eq(g.node(original).output_shape));
    var backwards: usize = 0;
    for (result.graph.nodes.items) |n| {
        if (n.op != .fused_modernbert_training_attention_backward_v1) continue;
        backwards += 1;
        try std.testing.expectEqual(@as(u8, 3), n.num_inputs);
        try std.testing.expect(n.output_shape.eq(layout.qkvShape()));
    }
    try std.testing.expectEqual(@as(usize, 1), backwards);
    const invalid = node.ModernBertTrainingAttentionAttrs{ .batch = 0, .seq_len = 5, .num_heads = 2, .head_dim = 4 };
    try std.testing.expectError(error.InvalidModernBertTrainingAttentionShape, invalid.layout());
}
