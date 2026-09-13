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

test "GLiNER2.5 encoder graph matches tiny Transformers train eval and every parameter VJP" {
    try encoderOracle(false);
}

test "GLiNER2.5 resident Metal encoder matches pinned train eval and all 38 parameter VJPs" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try encoderOracle(true);
}

fn encoderOracle(comptime resident: bool) !void {
    const a = std.testing.allocator;
    const metadata = try fixture.fixtureBytes(a, "training_encoder/capture.json");
    defer a.free(metadata);
    var manifest = try std.json.parseFromSlice(struct {
        files_sha256: struct { @"weights.safetensors": []const u8, @"tensors.safetensors": []const u8 },
    }, a, metadata, .{ .ignore_unknown_fields = true });
    defer manifest.deinit();
    var weights = try fixture.TensorFixture.init(a, "training_encoder/weights.safetensors");
    defer weights.deinit();
    var reference = try fixture.TensorFixture.init(a, "training_encoder/tensors.safetensors");
    defer reference.deinit();
    for ([_]struct { bytes: []const u8, hash: []const u8 }{
        .{ .bytes = weights.reader.file_bytes, .hash = manifest.value.files_sha256.@"weights.safetensors" },
        .{ .bytes = reference.reader.file_bytes, .hash = manifest.value.files_sha256.@"tensors.safetensors" },
    }) |file| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(file.bytes, &digest, .{});
        try std.testing.expectEqualStrings(file.hash, &std.fmt.bytesToHex(digest, .lower));
    }
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var device: ?resident_fixture.Device = if (resident) try resident_fixture.Device.init(a) else null;
    defer if (device) |*value| value.deinit();
    const cfg = config();
    for ([_][]const u8{ "padded7", "buckets19" }) |case| for ([_]encoder.Mode{ .eval, .train }) |mode| {
        errdefer std.debug.print("encoder numerical fixture {s}/{s}\n", .{ case, @tagName(mode) });
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const id_tensor = try reference.tensor(try std.fmt.allocPrint(scratch, "{s}.input_ids", .{case}));
        const mask_tensor = try reference.tensor(try std.fmt.allocPrint(scratch, "{s}.attention_mask", .{case}));
        const ids = std.mem.bytesAsSlice(i32, id_tensor.data);
        const mask = std.mem.bytesAsSlice(i32, mask_tensor.data);
        const batch: u32 = @intCast(id_tensor.shape[0]);
        const sequence: u32 = @intCast(id_tensor.shape[1]);
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var bld = ml.Builder.init(&graph);
        var built = try encoder.build(&bld, &cfg, .{ .batch = batch, .sequence = sequence, .words = 0, .queries = 0, .classifications = 0, .groups = 0, .relations = 0 }, mode, .{});
        defer built.deinit();
        var runtime = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer {
            for (runtime.items) |value| cb.free(value.value);
            runtime.deinit(a);
        }
        const aligned_ids = try scratch.alloc(i32, ids.len);
        for (ids, aligned_ids) |source, *target| target.* = source;
        try append(a, &cb, &runtime, built.inputs.input_ids, (try cb.fromInt32Shape(aligned_ids, &.{@intCast(ids.len)})) orelse return error.UnsupportedIntegerFixture);
        const embed_mask = try scratch.alloc(f32, mask.len);
        for (mask, embed_mask) |value, *out| out.* = @floatFromInt(value);
        try putF32(a, &cb, &runtime, &graph, built.inputs.embedding_valid, embed_mask);
        const score_count: usize = @intCast(built.admission.attention_score_elements);
        const score_valid = try scratch.alloc(f32, score_count);
        const score_bias = try scratch.alloc(f32, score_count);
        for (0..batch) |sample| for (0..cfg.encoder.num_attention_heads) |head| for (0..sequence) |query| for (0..sequence) |key| {
            const index = ((sample * cfg.encoder.num_attention_heads + head) * sequence + query) * sequence + key;
            const valid = mask[sample * sequence + query] != 0 and mask[sample * sequence + key] != 0;
            score_valid[index] = if (valid) 1 else 0;
            score_bias[index] = if (valid) 0 else -std.math.floatMax(f32);
        };
        try putF32(a, &cb, &runtime, &graph, built.inputs.attention_valid, score_valid);
        try putF32(a, &cb, &runtime, &graph, built.inputs.attention_bias, score_bias);
        for (built.dropouts) |descriptor| {
            const name = try std.fmt.allocPrint(scratch, "{s}.train.dropout.{s}.{d}", .{ case, @tagName(descriptor.site.kind), descriptor.site.layer });
            const values = try reference.floats(name);
            const replay = try scratch.alloc(f32, values.len);
            try encoder.fillDropout(descriptor, .{ .seed = 2509, .micro_batch = 3 }, replay);
            try std.testing.expectEqualSlices(f32, values, replay);
            try putF32(a, &cb, &runtime, &graph, descriptor.node, values);
        }
        var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
        defer wrt.deinit(a);
        for (graph.parameters.items) |id| {
            const name = graph.parameterName(graph.node(id));
            if (std.mem.startsWith(u8, name, "__")) continue;
            try putF32(a, &cb, &runtime, &graph, id, try weights.floats(name));
            try wrt.append(a, id);
        }
        try std.testing.expectEqual(@as(usize, 38), wrt.items.len);
        const cotangent_id = try bld.parameter("__encoder_output_cotangent", graph.node(built.nodes.encoder).output_shape);
        var session = try training.Session.init(a, &graph, &.{.{ .output = built.nodes.encoder, .cotangent = cotangent_id }}, wrt.items, .{ .gradient = .{ .require_all_gradients = true }, .max_tape_bytes = 16 * 1024 * 1024 });
        defer session.deinit();
        try resident_fixture.validate(a, &session);
        const identity = training.StepIdentity{ .binding = .{0x51} ** 32, .optimizer_step = 0, .microbatch = 3 };
        var tape = try session.forward(&cb, runtime.items, identity, null);
        defer tape.deinit();
        const expected = try reference.floats(try std.fmt.allocPrint(scratch, "{s}.{s}.output", .{ case, @tagName(mode) }));
        try compare(a, &cb, try tape.logits(0), expected, 3e-5, 3e-5);
        if (mode == .eval) {
            if (device) |*value| {
                const gpu = value.backend.computeBackend();
                var actual = try resident_fixture.run(a, &gpu, &cb, &session, runtime.items, null);
                defer actual.deinit(&gpu);
                try resident_fixture.expectValues(a, &gpu, try actual.logits(0), expected, 3e-5, 3e-5, case);
            }
            continue;
        }
        const cotangent = try reference.floats(try std.fmt.allocPrint(scratch, "{s}.cotangent", .{case}));
        const seed = try cb.fromFloat32Shape(cotangent, &.{ @intCast(batch * sequence), @intCast(cfg.encoder.hidden_size) });
        defer cb.free(seed);
        var loss: f32 = 0;
        for (expected, cotangent) |output, gradient| loss += output * gradient;
        const decisions = [_]u8{0x25} ** 32;
        try tape.sealDecisions(decisions);
        var backward = try tape.backward(identity, decisions, loss, &.{seed}, null);
        defer backward.deinit(&cb);
        try std.testing.expectEqual(@as(usize, 38), backward.parameter_ids.len);
        for (backward.parameter_ids, backward.gradients.outputs) |id, value| {
            const name = graph.parameterName(graph.node(id));
            errdefer std.debug.print("encoder parameter VJP {s}\n", .{name});
            const expected_gradient = try reference.floats(try std.fmt.allocPrint(scratch, "{s}.gradient.{s}", .{ case, name }));
            try compare(a, &cb, value, expected_gradient, 3e-4, 5e-5);
        }
        if (device) |*value| {
            const gpu = value.backend.computeBackend();
            var actual = try resident_fixture.run(a, &gpu, &cb, &session, runtime.items, &.{seed});
            defer actual.deinit(&gpu);
            try resident_fixture.expectValues(a, &gpu, try actual.logits(0), expected, 3e-5, 3e-5, case);
            try std.testing.expectEqual(@as(usize, 38), actual.gradients.?.outputs.len);
            for (session.gradient_parameters, actual.gradients.?.outputs) |id, gradient| {
                const name = graph.parameterName(graph.node(id));
                const key = try std.fmt.allocPrint(scratch, "{s}.gradient.{s}", .{ case, name });
                try resident_fixture.expectValues(a, &gpu, gradient, try reference.floats(key), 3e-4, 5e-5, key);
            }
        }
    };
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
    defer if (device) |*value| value.deinit();
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
    if (device) |*value| {
        const gpu = value.backend.computeBackend();
        var actual = try resident_fixture.run(a, &gpu, &cb, &session, runtime.items, &cotangents);
        defer actual.deinit(&gpu);
        for (expected_outputs, 0..) |expected, index| try resident_fixture.expectValues(a, &gpu, try actual.logits(index), expected, 0, 0, "routed output");
        try std.testing.expectEqual(@as(usize, 1), actual.gradients.?.outputs.len);
        try resident_fixture.expectValues(a, &gpu, actual.gradients.?.outputs[0], &gradients, 1e-6, 1e-6, "repeated route VJP");
    }
}
