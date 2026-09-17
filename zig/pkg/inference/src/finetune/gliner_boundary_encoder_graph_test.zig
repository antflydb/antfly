// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const encoder = @import("gliner_boundary_encoder_graph.zig");
const model = @import("../models/gliner_boundary.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const training = @import("../graph/seeded_training.zig");
const fixture = @import("../architectures/gliner_boundary_parity_test.zig");
const engine = @import("../architectures/gliner_boundary_engine.zig");
const resident_fixture = @import("../graph/resident_training_fixture.zig");
const build_options = @import("build_options");
const metal_runtime = @import("../backends/metal_runtime.zig");
const Allocator = std.mem.Allocator;

fn config() model.Config {
    return .{
        .version = model.config_version,
        .architecture_version = model.architecture_version,
        .max_len = 4096,
        .backbone = .small,
        .head = .{},
        .encoder = .{ .hidden_size = 16, .intermediate_size = 32, .num_hidden_layers = 2, .num_attention_heads = 4, .vocab_size = 64, .max_position_embeddings = 32, .position_buckets = 16, .layer_norm_eps = 1e-7, .hidden_dropout_prob = 0.1, .attention_probs_dropout_prob = 0.1, .pad_token_id = 0 },
    };
}
fn dims(shape: ml.Shape, buffer: *[8]i32) ![]const i32 {
    if (shape.rank_ > 8) return error.InvalidFixtureShape;
    for (shape.dims[0..shape.rank_], buffer[0..shape.rank_]) |dim, *out| out.* = std.math.cast(i32, dim) orelse return error.InvalidFixtureShape;
    return buffer[0..shape.rank_];
}
fn append(a: Allocator, cb: *const ops.ComputeBackend, bindings: *std.ArrayListUnmanaged(interpreter.RuntimeInput), id: ml.NodeId, value: ops.CT) !void {
    bindings.append(a, .{ .node_id = id, .value = value }) catch |err| {
        cb.free(value);
        return err;
    };
}
fn putF32(a: Allocator, cb: *const ops.ComputeBackend, bindings: *std.ArrayListUnmanaged(interpreter.RuntimeInput), graph: *const ml.Graph, id: ml.NodeId, values: []const f32) !void {
    var buffer: [8]i32 = undefined;
    try append(a, cb, bindings, id, try cb.fromFloat32Shape(values, try dims(graph.node(id).output_shape, &buffer)));
}
fn compare(a: Allocator, cb: *const ops.ComputeBackend, tensor: ops.CT, expected: []const f32, absolute: f32, relative: f32) !void {
    const actual = try cb.toFloat32(tensor, a);
    defer a.free(actual);
    try fixture.expectFloats(expected, actual, absolute, relative);
}
fn bindingFor(bound: encoder.BoundInputs, id: ml.NodeId) !encoder.Binding {
    for (bound.bindings) |binding| if (binding.node == id) return binding;
    return error.MissingFixtureBinding;
}

test "GLiNER2.5 routed encoder cotangents sum repeated word query classification relation uses" {
    try routedOracle(false);
}

test "GLiNER2.5 resident Metal routed encoder sums repeated typed routes and all cotangents" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try routedOracle(true);
}

fn routedOracle(comptime resident: bool) !void {
    const a = std.testing.allocator;
    var batch = engine.TestBatch{};
    var prepared = batch.prepared();
    defer prepared.arena.deinit();
    var cfg = engine.TestBatch.config();
    cfg.encoder.hidden_size = 4;
    cfg.encoder.intermediate_size = 8;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var bld = ml.Builder.init(&graph);
    var built = try encoder.build(&bld, &cfg, try encoder.layoutFromPrepared(&cfg, &prepared, .{}), .eval, .{});
    defer built.deinit();
    var bound = try encoder.bindPrepared(a, &built, &cfg, &prepared, .{ .seed = 0, .micro_batch = 0 });
    defer bound.deinit();
    // Replace exactly the encoder output with an independent leaf. The routing
    // nodes remain those produced by build(), and their VJP is evaluated by the
    // retained-tape executor without running encoder ancestors.
    const name = try graph.internString("__encoder_route_leaf");
    const leaf = graph.nodeMut(built.nodes.encoder);
    leaf.op = .{ .parameter = .{ .name_offset = name.offset, .name_len = name.len } };
    leaf.inputs = .{ml.null_node} ** 4;
    leaf.num_inputs = 0;
    leaf.vjp_alternate = ml.null_node;
    try graph.parameters.append(a, built.nodes.encoder);
    const routed = [_]ml.NodeId{ built.nodes.text, built.nodes.queries, built.nodes.classifications, built.nodes.parents, built.nodes.relation_queries };
    var seeds: [5]ml.autodiff.Seed = undefined;
    for (routed, &seeds, 0..) |id, *seed, index| {
        var buffer: [64]u8 = undefined;
        const seed_name = try std.fmt.bufPrint(&buffer, "__route_cotangent_{d}", .{index});
        seed.* = .{ .output = id, .cotangent = try bld.parameter(seed_name, graph.node(id).output_shape) };
    }
    var session = try training.Session.init(a, &graph, &seeds, &.{built.nodes.encoder}, .{ .gradient = .{ .require_all_gradients = true } });
    defer session.deinit();
    try resident_fixture.validate(a, &session);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var device: ?resident_fixture.Device = if (resident) try resident_fixture.Device.init(a) else null;
    defer if (resident) if (device) |*value| value.deinit();
    var runtime = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer {
        for (runtime.items) |value| cb.free(value.value);
        runtime.deinit(a);
    }
    for (bound.bindings) |binding| {
        if (session.differentiated.id_map[binding.node] == ml.null_node) continue;
        var buffer: [8]i32 = undefined;
        const shape = try dims(binding.shape, &buffer);
        const value = switch (binding.values) {
            .f32 => |values| try cb.fromFloat32Shape(values, shape),
            .i32 => |values| (try cb.fromInt32Shape(values, shape)) orelse return error.UnsupportedIntegerFixture,
        };
        try append(a, &cb, &runtime, binding.node, value);
    }
    var states: [96]f32 = undefined;
    for (&states, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index + 1)) * 0.01;
    try putF32(a, &cb, &runtime, &graph, built.nodes.encoder, &states);
    const identity = training.StepIdentity{ .binding = .{0x13} ** 32, .optimizer_step = 1, .microbatch = 2 };
    var tape = try session.forward(&cb, runtime.items, identity, null);
    defer tape.deinit();
    var gradients: [96]f32 = .{0} ** 96;
    var cotangents: [5]ops.CT = undefined;
    var expected_outputs: [5][]const f32 = undefined;
    var allocated: usize = 0;
    defer for (cotangents[0..allocated]) |value| cb.free(value);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var loss: f32 = 0;
    for (routed, 0..) |node, family| {
        const shape = graph.node(node).output_shape;
        const count: usize = @intCast(shape.numElements().?);
        const expected = try scratch.alloc(f32, count);
        expected_outputs[family] = expected;
        const cotangent = try scratch.alloc(f32, count);
        for (cotangent, 0..) |*value, index| value.* = @as(f32, @floatFromInt((index % 7) + family + 1)) * 0.125;
        @memset(expected, 0);
        const route_start: usize = if (family == 4) 4 else family;
        const route_end: usize = if (family == 4) 6 else family + 1;
        for (route_start..route_end) |route_index| {
            const route = built.inputs.routes[route_index];
            const indices = (try bindingFor(bound, route.indices)).values.i32;
            const valid = (try bindingFor(bound, route.valid)).values.f32;
            const output_width: usize = if (family == 4) 8 else 4;
            const column_offset: usize = if (route_index == 5) 4 else 0;
            for (indices, valid, 0..) |index, present, row| {
                if (present == 0) continue;
                for (0..4) |column| {
                    const from = @as(usize, @intCast(index)) * 4 + column;
                    const to = row * output_width + column_offset + column;
                    expected[to] = states[from];
                    gradients[from] += cotangent[to];
                }
            }
        }
        try compare(a, &cb, try tape.logits(family), expected, 0, 0);
        for (expected, cotangent) |output, gradient| loss += output * gradient;
        var buffer: [8]i32 = undefined;
        cotangents[family] = try cb.fromFloat32Shape(cotangent, try dims(shape, &buffer));
        allocated += 1;
    }
    const decisions = [_]u8{0x27} ** 32;
    try tape.sealDecisions(decisions);
    var backward = try tape.backward(identity, decisions, loss, &cotangents, null);
    defer backward.deinit(&cb);
    try std.testing.expectEqual(@as(usize, 1), backward.gradients.outputs.len);
    try compare(a, &cb, backward.gradients.outputs[0], &gradients, 1e-6, 1e-6);
    if (resident) {
        const value = &device.?;
        const gpu = value.backend.computeBackend();
        var actual = try resident_fixture.run(a, &gpu, &cb, &session, runtime.items, &cotangents);
        defer actual.deinit(&gpu);
        for (expected_outputs, 0..) |expected, index| try resident_fixture.expectValues(a, &gpu, try actual.logits(index), expected, 0, 0, "routed output");
        try std.testing.expectEqual(@as(usize, 1), actual.gradients.?.outputs.len);
        try resident_fixture.expectValues(a, &gpu, actual.gradients.?.outputs[0], &gradients, 1e-6, 1e-6, "repeated route VJP");
    }
}
