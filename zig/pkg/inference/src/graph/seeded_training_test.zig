// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const ml = @import("ml").graph;
const native = @import("../ops/native_compute.zig");
const interpreter = @import("interpreter.zig");

test "seeded training stop gradient keeps current forward values and blocks the detached VJP" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{2});
    const x = try b.parameter("x", shape);
    const y = try b.mul(try b.stopGradient(try b.mul(x, x)), x);
    const seed = try b.parameter("dy", shape);
    var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{ .require_all_gradients = true });
    defer gradients.deinit();
    gradients.graph.outputs.clearRetainingCapacity();
    try gradients.graph.markOutput(gradients.id_map[y]);
    try gradients.graph.markOutput(gradients.param_grads[0]);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const dy = try cb.fromFloat32Shape(&.{ 1, -2 }, &.{2});
    defer cb.free(dy);
    for ([_][2]f32{ .{ 2, 3 }, .{ -4, 5 } }) |values| {
        const value = try cb.fromFloat32Shape(&values, &.{2});
        defer cb.free(value);
        var result = try interpreter.execute(a, &gradients.graph, &cb, .{ .runtime_inputs = &.{ .{ .node_id = gradients.id_map[x], .value = value }, .{ .node_id = gradients.id_map[seed], .value = dy } } });
        defer result.deinit(&cb);
        const forward = try cb.toFloat32(result.outputs[0], a);
        defer a.free(forward);
        const gradient = try cb.toFloat32(result.outputs[1], a);
        defer a.free(gradient);
        for (values, forward, gradient, [_]f32{ 1, -2 }) |v, actual, grad, cotangent| {
            try std.testing.expectEqual(v * v * v, actual);
            try std.testing.expectEqual(v * v * cotangent, grad);
        }
    }
}

test "seeded training integer constants preserve physical values above the f32 exact range" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const bytes = std.mem.sliceAsBytes(&[_]i32{ 16777217, -16777217 });
    const table = try b.tensorConstBytes(bytes, ml.Shape.init(.i32, &.{2}));
    const index = try b.tensorConstBytes(std.mem.sliceAsBytes(&[_]i32{1}), ml.Shape.init(.i32, &.{1}));
    try graph.markOutput(try b.gather(table, index, ml.Shape.init(.i32, &.{1})));
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var result = try interpreter.execute(a, &graph, &cb, .{ .strict_integer_constants = true });
    defer result.deinit(&cb);
    try std.testing.expectEqual(@import("../backends/tensor.zig").DType.i32, try cb.tensorDType(result.outputs[0]));
    const exact = try compute.toInt64(result.outputs[0], a);
    defer a.free(exact);
    try std.testing.expectEqualSlices(i64, &.{-16777217}, exact);
}

test "seeded training amax divides the adjoint among exact ties" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const x = try b.parameter("x", ml.Shape.init(.f32, &.{ 2, 3 }));
    const y = try b.reduceMax(x, &.{1});
    const seed = try b.parameter("seed", graph.node(y).output_shape);
    var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{ .require_all_gradients = true });
    defer gradients.deinit();
    gradients.graph.outputs.clearRetainingCapacity();
    try gradients.graph.markOutput(gradients.param_grads[0]);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const data = try cb.fromFloat32Shape(&.{ 2, 2, 1, 4, 4, 4 }, &.{ 2, 3 });
    defer cb.free(data);
    const dy = try cb.fromFloat32Shape(&.{ 6, -3 }, &.{2});
    defer cb.free(dy);
    var result = try interpreter.execute(a, &gradients.graph, &cb, .{ .runtime_inputs = &.{ .{ .node_id = gradients.id_map[x], .value = data }, .{ .node_id = gradients.id_map[seed], .value = dy } } });
    defer result.deinit(&cb);
    const actual = try cb.toFloat32(result.outputs[0], a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 3, 3, 0, -1, -1, -1 }, actual);
}

test "seeded training VJP preserves vector seeds and all shared output branches" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const x = try b.parameter("x", ml.Shape.init(.f32, &.{ 2, 2 }));
    const w = try b.parameter("weight", ml.Shape.init(.f32, &.{ 2, 2 }));
    const bias = try b.parameter("bias", ml.Shape.init(.f32, &.{2}));
    const y = try b.linear(x, w, bias, 2, 2, 2);
    const square = try b.mul(y, y);
    const sy = try b.parameter("cotangent_y", ml.Shape.init(.f32, &.{ 2, 2 }));
    const ss = try b.parameter("cotangent_square", ml.Shape.init(.f32, &.{ 2, 2 }));
    // The seed API must establish reachability without mutating caller outputs.
    try graph.markOutput(x);
    var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{ .{ .output = y, .cotangent = sy }, .{ .output = y, .cotangent = sy }, .{ .output = square, .cotangent = ss } }, &.{ w, bias }, .{ .require_all_gradients = true });
    defer gradients.deinit();
    try std.testing.expectEqualSlices(ml.NodeId, &.{x}, graph.outputs.items);
    try std.testing.expect(gradients.forward_node_count > 0 and gradients.forward_node_count < gradients.graph.nodeCount());
    gradients.graph.outputs.clearRetainingCapacity();
    for (gradients.param_grads) |id| try gradients.graph.markOutput(id);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const input_values = [_][]const f32{ &.{ 1, 2, -1, 3 }, &.{ 0.5, -1, 2, 0.25 }, &.{ 0.1, -0.2 }, &.{ 0.3, -0.4, 0.7, 0.2 }, &.{ -0.1, 0.5, 0.2, -0.3 } };
    const ids = [_]ml.NodeId{ x, w, bias, sy, ss };
    var inputs: [ids.len]interpreter.RuntimeInput = undefined;
    var owned: usize = 0;
    defer for (inputs[0..owned]) |input| cb.free(input.value);
    for (ids, input_values, &inputs) |id, values, *input| {
        input.* = .{ .node_id = gradients.id_map[id], .value = try cb.fromFloat32(values) };
        owned += 1;
    }
    var result = try interpreter.execute(a, &gradients.graph, &cb, .{ .runtime_inputs = &inputs });
    defer result.deinit(&cb);
    var expected_weight = [_]f32{0} ** 4;
    var expected_bias = [_]f32{0} ** 2;
    for (0..2) |row| for (0..2) |column| {
        var value = input_values[2][column];
        for (0..2) |k| value += input_values[0][row * 2 + k] * input_values[1][column * 2 + k];
        const dy = 2 * input_values[3][row * 2 + column] + 2 * value * input_values[4][row * 2 + column];
        expected_bias[column] += dy;
        for (0..2) |k| expected_weight[column * 2 + k] += dy * input_values[0][row * 2 + k];
    };
    for (result.outputs, [_][]const f32{ &expected_weight, &expected_bias }) |output, expected| {
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
    }
}

fn checkStrictFailures(a: std.mem.Allocator) !void {
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{2});
    const x = try b.parameter("frozen_features", shape);
    const w = try b.parameter("weight", shape);
    const unused = try b.parameter("inactive_head", shape);
    const seed = try b.parameter("seed", shape);
    const unsupported = try graph.addNode(.{ .op = .{ .conv_general = .{} }, .output_shape = shape, .inputs = .{ x, ml.null_node, ml.null_node, ml.null_node }, .num_inputs = 1 });
    const y = try b.mul(unsupported, w);
    const seeds = [_]ml.autodiff.Seed{.{ .output = y, .cotangent = seed }};
    var result = try ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{ w, unused }, .{});
    defer result.deinit();
    try std.testing.expect(result.param_grads[0] != ml.null_node);
    try std.testing.expectEqual(ml.null_node, result.param_grads[1]);
    try std.testing.expectError(error.NoVjpRule, ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{ x, w }, .{}));
    try std.testing.expectError(error.DisconnectedGradientParameter, ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{ w, unused }, .{ .require_all_gradients = true }));
    try std.testing.expectError(error.TrainableGradientSeed, ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{seed}, .{}));
    try std.testing.expectError(error.DuplicateGradientParameter, ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{ w, w }, .{}));
    try std.testing.expectError(error.InvalidGradientSeed, ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = ml.null_node, .cotangent = seed }}, &.{w}, .{}));
    try std.testing.expectError(error.GradientGraphLimitExceeded, ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{w}, .{ .max_gradient_nodes = 1 }));
    try std.testing.expectError(error.GradientSeedUsedByForward, ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = try b.mul(y, seed), .cotangent = seed }}, &.{w}, .{}));
}

test "seeded training rejects missing gradients malformed seeds and unimplemented live VJPs" {
    try checkStrictFailures(std.testing.allocator);
}

test "seeded training allocation failures preserve graph and gradient ownership" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            var graph = ml.Graph.init(a);
            defer graph.deinit();
            var b = ml.Builder.init(&graph);
            const shape = ml.Shape.init(.f32, &.{ 2, 2 });
            const x = try b.parameter("x", shape);
            const w = try b.parameter("w", shape);
            const seed = try b.parameter("seed", shape);
            const y = try b.linearNoBias(x, w, 2, 2, 2);
            var result = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w }, .{ .require_all_gradients = true });
            defer result.deinit();
            try std.testing.expect(result.param_grads[0] != ml.null_node and result.param_grads[1] != ml.null_node);
            try std.testing.expectEqual(@as(usize, 0), graph.outputs.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

fn exerciseTape(a: std.mem.Allocator, mismatch: bool) !void {
    const training = @import("seeded_training.zig");
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{2});
    const x = try b.parameter("features", shape);
    const w = try b.parameter("weight", shape);
    const h = try b.tanhOp(x);
    const y = try b.mul(h, w);
    const seed = try b.parameter("cotangent", shape);
    var session = try training.Session.init(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{w}, .{});
    defer session.deinit();
    // The retained boundary is a leaf in the backward program. It cannot
    // silently re-run the nonlinear feature extractor or its dropout sites.
    for (session.backward.graph.nodes.items) |node| try std.testing.expect(node.op != .tanh);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const features = try cb.fromFloat32Shape(&.{ 0.2, -0.7 }, &.{2});
    defer cb.free(features);
    const weight = try cb.fromFloat32Shape(&.{ 0.3, 0.9 }, &.{2});
    defer cb.free(weight);
    const cotangent = try cb.fromFloat32Shape(&.{ 2, -3 }, &.{2});
    defer cb.free(cotangent);
    const inputs = [_]interpreter.RuntimeInput{ .{ .node_id = x, .value = features }, .{ .node_id = w, .value = weight } };
    const identity = training.StepIdentity{ .binding = [_]u8{19} ** 32, .optimizer_step = 7, .microbatch = 2 };
    var tape = try session.forward(&cb, &inputs, identity, null);
    defer tape.deinit();
    const decisions = [_]u8{23} ** 32;
    try tape.sealDecisions(decisions);
    try std.testing.expectError(error.TrainingDecisionsAlreadySealed, tape.sealDecisions(decisions));
    try std.testing.expectError(error.TrainingTapeStillLive, session.advanceParameterEpoch());
    const logits = try cb.toFloat32(try tape.logits(0), a);
    defer a.free(logits);
    try std.testing.expectApproxEqAbs(@as(f32, std.math.tanh(@as(f32, 0.2))) * 0.3, logits[0], 1e-6);
    if (mismatch) {
        var wrong = identity;
        wrong.microbatch += 1;
        try std.testing.expectError(error.TrainingTapeIdentityMismatch, tape.backward(wrong, decisions, 1, &.{cotangent}, null));
        try std.testing.expect(tape.consumed and !session.active_tape);
        try session.advanceParameterEpoch();
        return;
    }
    var result = try tape.backward(identity, decisions, 1.25, &.{cotangent}, null);
    defer result.deinit(&cb);
    try std.testing.expectEqual(@as(f32, 1.25), result.loss);
    try std.testing.expectEqualSlices(ml.NodeId, &.{w}, result.parameter_ids);
    const gradient = try cb.toFloat32(result.gradients.outputs[0], a);
    defer a.free(gradient);
    try std.testing.expectApproxEqAbs(2 * @as(f32, std.math.tanh(@as(f32, 0.2))), gradient[0], 1e-6);
    try std.testing.expectApproxEqAbs(-3 * @as(f32, std.math.tanh(@as(f32, -0.7))), gradient[1], 1e-6);
    try std.testing.expect(tape.consumed and !session.active_tape);
    try session.advanceParameterEpoch();
    try std.testing.expectError(error.InvalidTrainingTape, tape.backward(identity, decisions, 1, &.{cotangent}, null));
}

test "seeded training tape cuts forward ancestors and binds immutable step identity" {
    try exerciseTape(std.testing.allocator, false);
    try exerciseTape(std.testing.allocator, true);
}

test "seeded training tape releases retained activations on allocation failures" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            try exerciseTape(a, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

fn exerciseStages(a: std.mem.Allocator, invalid: bool) !void {
    const staged = @import("staged_training.zig");
    const training = @import("seeded_training.zig");
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const x = try b.parameter("encoder_input", ml.Shape.init(.f32, &.{3}));
    const dropout = try b.parameter("dropout", ml.Shape.init(.f32, &.{3}));
    const w = try b.parameter("weight", ml.Shape.init(.f32, &.{2}));
    const selected = try b.parameter("candidates", ml.Shape.init(.i32, &.{2}));
    const h = try b.mul(try b.tanhOp(x), dropout);
    const y = try b.mul(try b.gather(h, selected, ml.Shape.init(.f32, &.{2})), w);
    const sh = try b.parameter("prefix_cotangent", ml.Shape.init(.f32, &.{3}));
    const sy = try b.parameter("suffix_cotangent", ml.Shape.init(.f32, &.{2}));
    var session = try staged.Session.init(a, &graph, &.{ .{ .output = h, .cotangent = sh }, .{ .output = y, .cotangent = sy } }, &.{ x, w }, .{ .deferred_parameters = &.{selected}, .outputs = &.{h} }, .{});
    defer session.deinit();
    for (session.suffix.graph.nodes.items) |node| try std.testing.expect(node.op != .tanh);
    for (session.base.backward.graph.nodes.items) |node| try std.testing.expect(node.op != .tanh);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const input = try cb.fromFloat32Shape(&.{ 0.2, -0.7, 0.5 }, &.{3});
    defer cb.free(input);
    const mask = try cb.fromFloat32Shape(&.{ 2, 0, 0.5 }, &.{3});
    defer cb.free(mask);
    const weight = try cb.fromFloat32Shape(&.{ 0.3, 0.9 }, &.{2});
    defer cb.free(weight);
    const indices = (try cb.fromInt32Shape(&.{ 0, 0 }, &.{2})).?;
    defer cb.free(indices);
    const ph = try cb.fromFloat32Shape(&.{ 1, -2, 3 }, &.{3});
    defer cb.free(ph);
    const py = try cb.fromFloat32Shape(&.{ 2, -3 }, &.{2});
    defer cb.free(py);
    const identity = training.StepIdentity{ .binding = @splat(11), .optimizer_step = 1, .microbatch = 0 };
    var prefix = try session.forwardPrefix(&cb, &.{ .{ .node_id = x, .value = input }, .{ .node_id = dropout, .value = mask }, .{ .node_id = w, .value = weight } }, identity, null);
    defer prefix.deinit();
    try std.testing.expectError(error.TrainingTapeStillLive, session.base.advanceParameterEpoch());
    const prefix_data = try cb.toFloat32(try prefix.logits(0), a);
    defer a.free(prefix_data);
    try std.testing.expectApproxEqAbs(2 * @as(f32, std.math.tanh(@as(f32, 0.2))), prefix_data[0], 1e-6);
    if (invalid) {
        try std.testing.expectError(error.MissingTrainingBinding, prefix.forwardSuffix(identity, @splat(12), &.{}, null));
        try std.testing.expect(prefix.consumed and !session.base.active_tape);
        return;
    }
    var tape = try prefix.forwardSuffix(identity, @splat(12), &.{.{ .node_id = selected, .value = indices }}, null);
    defer tape.deinit();
    try std.testing.expect(prefix.consumed and session.base.active_tape);
    try std.testing.expect(!std.mem.eql(u8, &identity.binding, &tape.identity.binding));
    try std.testing.expectError(error.TrainingTapeStillLive, session.base.advanceParameterEpoch());
    try tape.sealDecisions(@splat(13));
    var result = try tape.backward(tape.identity, @splat(13), 1, &.{ ph, py }, null);
    defer result.deinit(&cb);
    const dx = try cb.toFloat32(result.gradients.outputs[0], a);
    defer a.free(dx);
    const dw = try cb.toFloat32(result.gradients.outputs[1], a);
    defer a.free(dw);
    const t0 = std.math.tanh(@as(f32, 0.2));
    const t2 = std.math.tanh(@as(f32, 0.5));
    try std.testing.expectApproxEqAbs((1 + 2 * @as(f32, 0.3) - 3 * @as(f32, 0.9)) * 2 * (1 - t0 * t0), dx[0], 1e-5);
    try std.testing.expectEqual(@as(f32, 0), dx[1]);
    try std.testing.expectApproxEqAbs(3 * 0.5 * (1 - t2 * t2), dx[2], 1e-5);
    try std.testing.expectApproxEqAbs(4 * t0, dw[0], 1e-6);
    try std.testing.expectApproxEqAbs(-6 * t0, dw[1], 1e-6);
    try session.base.advanceParameterEpoch();
}

test "seeded training stages retain prefix dropout and repeated candidate gradients" {
    try exerciseStages(std.testing.allocator, false);
    try exerciseStages(std.testing.allocator, true);
}

test "seeded training stages release all prefix suffix captures on allocation failures" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            try exerciseStages(a, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

const StageFailure = enum { none, stale, premature, missing, duplicate, finish_early, cancelled, advance_after_final };

fn exerciseMultiStages(a: std.mem.Allocator, failure: StageFailure) !void {
    const training = @import("multi_stage_training.zig");
    const seeded = @import("seeded_training.zig");
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const x = try b.parameter("features", ml.Shape.init(.f32, &.{3}));
    const mask = try b.parameter("encoder_dropout", ml.Shape.init(.f32, &.{3}));
    const w = try b.parameter("candidate_weight", ml.Shape.init(.f32, &.{2}));
    const rw = try b.parameter("relation_weight", ml.Shape.init(.f32, &.{2}));
    const selected = try b.parameter("candidate_indices", ml.Shape.init(.i32, &.{2}));
    const pairs = try b.parameter("relation_indices", ml.Shape.init(.i32, &.{2}));
    const h = try b.mul(try b.tanhOp(x), mask);
    const c = try b.mul(try b.gather(h, selected, ml.Shape.init(.f32, &.{2})), w);
    const y = try b.mul(try b.gather(c, pairs, ml.Shape.init(.f32, &.{2})), rw);
    const sh = try b.parameter("dh", graph.node(h).output_shape);
    const sc = try b.parameter("dc", graph.node(c).output_shape);
    const sy = try b.parameter("dy", graph.node(y).output_shape);
    const seeds = [_]ml.autodiff.Seed{ .{ .output = h, .cotangent = sh }, .{ .output = c, .cotangent = sc }, .{ .output = y, .cotangent = sy } };
    const boundaries = [_]training.Boundary{ .{ .deferred_parameters = &.{selected}, .outputs = &.{h} }, .{ .deferred_parameters = &.{pairs}, .outputs = &.{ c, h } } };
    var session = try training.Session.init(a, &graph, &seeds, &.{ x, w, rw }, &boundaries, .{});
    defer session.deinit();
    // Later stages and backward cannot reach the prefix nonlinear operation.
    for (session.stages[1..]) |stage| for (stage.program.graph.nodes.items) |node| try std.testing.expect(node.op != .tanh);
    for (session.base.backward.graph.nodes.items) |node| try std.testing.expect(node.op != .tanh);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const features = try cb.fromFloat32Shape(&.{ 0.2, -0.7, 0.5 }, &.{3});
    defer cb.free(features);
    const dropout = try cb.fromFloat32Shape(&.{ 2, 0, 0.5 }, &.{3});
    defer cb.free(dropout);
    const weight = try cb.fromFloat32Shape(&.{ 0.3, 0.9 }, &.{2});
    defer cb.free(weight);
    const relation_weight = try cb.fromFloat32Shape(&.{ 0.2, -0.4 }, &.{2});
    defer cb.free(relation_weight);
    const initial = [_]interpreter.RuntimeInput{ .{ .node_id = x, .value = features }, .{ .node_id = mask, .value = dropout }, .{ .node_id = w, .value = weight }, .{ .node_id = rw, .value = relation_weight } };
    const identity = seeded.StepIdentity{ .binding = @splat(31), .optimizer_step = 7, .microbatch = 2 };
    const indices = (try cb.fromInt32Shape(&.{ 0, 0 }, &.{2})) orelse return error.UnsupportedTrainingInteger;
    defer cb.free(indices);
    if (failure == .premature) {
        const bad = initial ++ [_]interpreter.RuntimeInput{.{ .node_id = selected, .value = indices }};
        try std.testing.expectError(error.PrematureTrainingBinding, session.forward(&cb, &bad, identity, null));
        try std.testing.expect(!session.base.active_tape);
        return;
    }
    var staged = try session.forward(&cb, &initial, identity, null);
    defer staged.deinit();
    try std.testing.expectError(error.TrainingTapeStillLive, session.base.advanceParameterEpoch());
    const prefix = try cb.toFloat32(try staged.logits(0), a);
    defer a.free(prefix);
    try std.testing.expect(prefix[0] > prefix[2] and prefix[1] == 0);
    const next = [_]interpreter.RuntimeInput{.{ .node_id = selected, .value = indices }};
    switch (failure) {
        .stale => {
            var wrong = identity;
            wrong.microbatch += 1;
            try std.testing.expectError(error.TrainingTapeIdentityMismatch, staged.advance(wrong, @splat(41), &next, null));
        },
        .missing => try std.testing.expectError(error.MissingTrainingBinding, staged.advance(identity, @splat(41), &.{}, null)),
        .duplicate => try std.testing.expectError(error.MissingTrainingBinding, staged.advance(identity, @splat(41), &(next ++ next), null)),
        .finish_early => try std.testing.expectError(error.IncompleteTrainingStages, staged.finish()),
        .cancelled => {
            const Cancel = struct {
                fn check(_: ?*anyopaque) anyerror!void {
                    return error.Cancelled;
                }
            };
            try std.testing.expectError(error.Cancelled, staged.advance(identity, @splat(41), &next, .{ .check_fn = Cancel.check }));
        },
        else => {},
    }
    if (failure != .none and failure != .advance_after_final) {
        try std.testing.expect(staged.consumed and !session.base.active_tape);
        try session.base.advanceParameterEpoch();
        return;
    }
    try staged.advance(identity, @splat(41), &next, null);
    const candidates = try cb.toFloat32(try staged.logits(0), a);
    defer a.free(candidates);
    const early_again = try cb.toFloat32(try staged.logits(1), a);
    defer a.free(early_again);
    try std.testing.expectEqualSlices(f32, prefix, early_again);
    // The second detached choice must depend on the actual candidate scores.
    const winning: i32 = if (candidates[1] > candidates[0]) 1 else 0;
    try std.testing.expectEqual(@as(i32, 1), winning);
    const pair_indices = (try cb.fromInt32Shape(&.{ winning, winning }, &.{2})) orelse return error.UnsupportedTrainingInteger;
    defer cb.free(pair_indices);
    try staged.advance(identity, @splat(43), &.{.{ .node_id = pairs, .value = pair_indices }}, null);
    if (failure == .advance_after_final) {
        try std.testing.expectError(error.InvalidTrainingStage, staged.advance(identity, @splat(44), &.{}, null));
        try std.testing.expect(staged.consumed and !session.base.active_tape);
        return;
    }
    var tape = try staged.finish();
    defer tape.deinit();
    try std.testing.expect(staged.consumed and session.base.active_tape);
    try std.testing.expect(!std.mem.eql(u8, &identity.binding, &tape.identity.binding));
    const ph = try cb.fromFloat32Shape(&.{ 1, 2, 3 }, &.{3});
    defer cb.free(ph);
    const pc = try cb.fromFloat32Shape(&.{ 2, -3 }, &.{2});
    defer cb.free(pc);
    const py = try cb.fromFloat32Shape(&.{ 4, -5 }, &.{2});
    defer cb.free(py);
    try tape.sealDecisions(@splat(47));
    var result = try tape.backward(tape.identity, @splat(47), 1, &.{ ph, pc, py }, null);
    defer result.deinit(&cb);
    try std.testing.expectEqualSlices(ml.NodeId, &.{ x, w, rw }, result.parameter_ids);
    const t0 = std.math.tanh(@as(f32, 0.2));
    const t2 = std.math.tanh(@as(f32, 0.5));
    const dc0: f32 = 2;
    const dc1: f32 = -3 + 4 * @as(f32, 0.2) - 5 * @as(f32, -0.4);
    const expected = [_][]const f32{
        &.{ (1 + dc0 * @as(f32, 0.3) + dc1 * @as(f32, 0.9)) * 2 * (1 - t0 * t0), 0, 3 * 0.5 * (1 - t2 * t2) },
        &.{ prefix[0] * dc0, prefix[0] * dc1 },
        &.{ candidates[1] * 4, candidates[1] * -5 },
    };
    for (result.gradients.outputs, expected) |output, want| {
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        for (want, actual) |v, got| try std.testing.expectApproxEqAbs(v, got, 1e-5);
    }
    try std.testing.expect(!session.base.active_tape);
    try session.base.advanceParameterEpoch();
}

test "seeded training multi stages retain live candidate decisions and repeated relation gradients" {
    try exerciseMultiStages(std.testing.allocator, .none);
}

test "seeded training multi stages reject stale premature incomplete and cancelled transitions" {
    for ([_]StageFailure{ .stale, .premature, .missing, .duplicate, .finish_early, .cancelled, .advance_after_final }) |failure| try exerciseMultiStages(std.testing.allocator, failure);
}

test "seeded training multi stages release every capture and epoch on allocation failures" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            try exerciseMultiStages(a, .none);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
