// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub fn write(allocator: std.mem.Allocator, dir: std.Io.Dir, name: []const u8, encoder: bool) !void {
    const ml = @import("ml");
    const onnx = @import("onnx_graph");
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter(if (encoder) "features" else "encoder_hidden_states", ml.graph.Shape.init(.f32, &.{ 1, 2, 4 }));
    try graph.markOutput(if (encoder) try builder.relu(input) else try builder.neg(input));
    const bytes = try onnx.exportGraph(allocator, &graph, .{});
    defer allocator.free(bytes);
    if (encoder) return dir.writeFile(std.testing.io, .{ .sub_path = name, .data = bytes });
    // An independent control input remains part of the public ABI even when
    // this toy decoder's mathematics does not depend on the token identities.
    var controls = ml.graph.Graph.init(allocator);
    defer controls.deinit();
    var control_builder = ml.graph.Builder.init(&controls);
    try controls.markOutput(try control_builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ 1, 2 })));
    const control_bytes = try onnx.exportGraph(allocator, &controls, .{});
    defer allocator.free(control_bytes);
    var model = try onnx.parse(allocator, bytes);
    defer model.deinit();
    var control_model = try onnx.parse(allocator, control_bytes);
    defer control_model.deinit();
    var snapshot = model.onnx;
    var signature = snapshot.graph.?;
    var inputs = [_]onnx.proto.ValueInfoProto{ control_model.onnx.graph.?.inputs[0], signature.inputs[0] };
    signature.inputs = &inputs;
    snapshot.graph = signature;
    const declared = try onnx.serializeModel(allocator, &snapshot);
    defer allocator.free(declared);
    try dir.writeFile(std.testing.io, .{ .sub_path = name, .data = declared });
}
