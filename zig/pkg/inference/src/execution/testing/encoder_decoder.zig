// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub const Options = struct { reading_pair: bool = false, decoder_masks: bool = false, encoder_mask: bool = false };

pub fn write(allocator: std.mem.Allocator, dir: std.Io.Dir, name: []const u8, encoder: bool) !void {
    return writeConfigured(allocator, dir, name, encoder, .{});
}

pub fn writeConfigured(allocator: std.mem.Allocator, dir: std.Io.Dir, name: []const u8, encoder: bool, options: Options) !void {
    const ml = @import("ml");
    const onnx = @import("onnx_graph");
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const hidden_shape: []const i64 = if (options.reading_pair) &.{ 1, 1, 3 } else &.{ 1, 2, 4 };
    const input_shape: []const i64 = if (options.reading_pair and encoder) &.{ 1, 3, 1, 1 } else hidden_shape;
    const input_name = if (encoder) (if (options.reading_pair) "pixel_values" else "features") else "encoder_hidden_states";
    const input = try builder.parameter(input_name, ml.graph.Shape.init(.f32, input_shape));
    const hidden = if (options.reading_pair and encoder) try builder.reshape(input, ml.graph.Shape.init(.f32, hidden_shape)) else input;
    try graph.markOutput(if (encoder) try builder.relu(hidden) else try builder.neg(hidden));
    const bytes = try onnx.exportGraph(allocator, &graph, .{});
    defer allocator.free(bytes);
    if (encoder) return dir.writeFile(std.testing.io, .{ .sub_path = name, .data = bytes });
    // An independent control input remains part of the public ABI even when
    // this toy decoder's mathematics does not depend on the token identities.
    var controls = ml.graph.Graph.init(allocator);
    defer controls.deinit();
    var control_builder = ml.graph.Builder.init(&controls);
    const control_shape = [_]i64{ 1, if (options.reading_pair) 1 else 2 };
    const names: []const []const u8 = if (options.decoder_masks) &.{ "input_ids", "encoder_attention_mask", "decoder_attention_mask" } else if (options.encoder_mask) &.{ "input_ids", "encoder_attention_mask" } else &.{"input_ids"};
    for (names) |control_name| try controls.markOutput(try control_builder.parameter(control_name, ml.graph.Shape.init(.i64, &control_shape)));
    const control_bytes = try onnx.exportGraph(allocator, &controls, .{});
    defer allocator.free(control_bytes);
    var model = try onnx.parse(allocator, bytes);
    defer model.deinit();
    var control_model = try onnx.parse(allocator, control_bytes);
    defer control_model.deinit();
    var snapshot = model.onnx;
    var signature = snapshot.graph.?;
    const control_inputs = control_model.onnx.graph.?.inputs;
    const inputs = try allocator.alloc(onnx.proto.ValueInfoProto, control_inputs.len + signature.inputs.len);
    defer allocator.free(inputs);
    @memcpy(inputs[0..control_inputs.len], control_inputs);
    @memcpy(inputs[control_inputs.len..], signature.inputs);
    signature.inputs = inputs;
    snapshot.graph = signature;
    const declared = try onnx.serializeModel(allocator, &snapshot);
    defer allocator.free(declared);
    try dir.writeFile(std.testing.io, .{ .sub_path = name, .data = declared });
}
