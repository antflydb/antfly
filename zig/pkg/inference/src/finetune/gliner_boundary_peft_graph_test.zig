// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const peft = @import("gliner_boundary_peft_graph.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const training = @import("../graph/seeded_training.zig");
const fixture = @import("../architectures/gliner_boundary_parity_test.zig");
const resident_fixture = @import("../graph/resident_training_fixture.zig");
const build_options = @import("build_options");
const metal_runtime = @import("../backends/metal_runtime.zig");
const Allocator = std.mem.Allocator;
const query_module = "encoder.layer.0.attention.self.query_proj";
const targets = &.{ "encoder.query", "classification_head" };
const output_names = [_][]const u8{ "query0", "query1", "classifier0" };

const Toy = struct { inputs: [2]ml.NodeId, outputs: [3]ml.NodeId };
fn buildToy(b: *ml.Builder) !Toy {
    const x0 = try b.parameter("__peft_x0", ml.Shape.init(.f32, &.{ 2, 4 }));
    const x1 = try b.parameter("__peft_x1", ml.Shape.init(.f32, &.{ 5, 4 }));
    const qw = try b.parameter(query_module ++ ".weight", ml.Shape.init(.f32, &.{ 3, 4 }));
    const qb = try b.parameter(query_module ++ ".bias", ml.Shape.init(.f32, &.{3}));
    const y0 = try b.linear(x0, qw, qb, 2, 4, 3);
    const y1 = try b.linear(x1, qw, qb, 5, 4, 3);
    const cw = try b.parameter("classifier.0.weight", ml.Shape.init(.f32, &.{ 2, 3 }));
    const cb = try b.parameter("classifier.0.bias", ml.Shape.init(.f32, &.{2}));
    const classification = try b.linear(y0, cw, cb, 2, 3, 2);
    for ([_]ml.NodeId{ y0, y1, classification }) |id| try b.graph.markOutput(id);
    return .{ .inputs = .{ x0, x1 }, .outputs = .{ y0, y1, classification } };
}
fn dims(shape: ml.Shape, buffer: *[8]i32) ![]const i32 {
    if (shape.rank_ > buffer.len) return error.InvalidFixtureShape;
    for (shape.dims[0..shape.rank_], buffer[0..shape.rank_]) |dim, *value| value.* = std.math.cast(i32, dim) orelse return error.InvalidFixtureShape;
    return buffer[0..shape.rank_];
}
fn put(a: Allocator, cb: *const ops.ComputeBackend, bindings: *std.ArrayListUnmanaged(interpreter.RuntimeInput), graph: *const ml.Graph, id: ml.NodeId, values: []const f32) !void {
    var buffer: [8]i32 = undefined;
    const value = try cb.fromFloat32Shape(values, try dims(graph.node(id).output_shape, &buffer));
    errdefer cb.free(value);
    try bindings.append(a, .{ .node_id = id, .value = value });
}
fn compare(a: Allocator, cb: *const ops.ComputeBackend, tensor: ops.CT, expected: []const f32) !void {
    const actual = try cb.toFloat32(tensor, a);
    defer a.free(actual);
    try fixture.expectFloats(expected, actual, 3e-6, 5e-6);
}

test "GLiNER2.5 PEFT graph matches pinned LoRA DoRA shared use dropout and every VJP" {
    try peftOracle(false);
}

test "GLiNER2.5 resident Metal PEFT matches all pinned LoRA DoRA shared dropout outputs and VJPs" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try peftOracle(true);
}

fn peftOracle(comptime resident: bool) !void {
    const a = std.testing.allocator;
    var device: ?resident_fixture.Device = if (resident) try resident_fixture.Device.init(a) else null;
    defer if (device) |*value| value.deinit();
    const metadata = try fixture.fixtureBytes(a, "training_peft/capture.json");
    defer a.free(metadata);
    var manifest = try std.json.parseFromSlice(struct {
        cases: []struct { id: []const u8, kind: peft.Kind, probability: f32, mode: peft.Mode, replay: peft.Replay, loss: f32 },
        provenance: struct { peft: []const u8 },
        files_sha256: struct { @"weights.safetensors": []const u8, @"tensors.safetensors": []const u8 },
    }, a, metadata, .{ .ignore_unknown_fields = true });
    defer manifest.deinit();
    try std.testing.expectEqualStrings("0.17.1", manifest.value.provenance.peft);
    var weights = try fixture.TensorFixture.init(a, "training_peft/weights.safetensors");
    defer weights.deinit();
    var reference = try fixture.TensorFixture.init(a, "training_peft/tensors.safetensors");
    defer reference.deinit();
    for ([_]struct { data: []const u8, sha: []const u8 }{
        .{ .data = weights.reader.file_bytes, .sha = manifest.value.files_sha256.@"weights.safetensors" },
        .{ .data = reference.reader.file_bytes, .sha = manifest.value.files_sha256.@"tensors.safetensors" },
    }) |file| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(file.data, &digest, .{});
        try std.testing.expectEqualStrings(file.sha, &std.fmt.bytesToHex(digest, .lower));
    }
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for (manifest.value.cases) |case| {
        errdefer std.debug.print("PEFT numerical fixture {s}\n", .{case.id});
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var source = ml.Graph.init(a);
        defer source.deinit();
        var b = ml.Builder.init(&source);
        const toy = try buildToy(&b);
        const original_count = source.nodeCount();
        var result = try peft.inject(a, &source, .{ .kind = case.kind, .mode = case.mode, .rank = 2, .alpha = 3, .dropout = case.probability, .targets = targets }, .{});
        defer result.deinit();
        try std.testing.expectEqual(original_count, source.nodeCount());
        try std.testing.expectEqual(@as(usize, 2), result.adapters.len);
        try std.testing.expectEqual(@as(usize, 3), result.uses.len);
        try std.testing.expectEqual(result.uses[0].adapter, result.uses[1].adapter);
        try std.testing.expectEqual(@as(u32, 2), result.uses[0].rows);
        try std.testing.expectEqual(@as(u32, 5), result.uses[1].rows);
        try std.testing.expect(result.uses[0].stream != result.uses[1].stream);
        try std.testing.expectEqual(result.uses[0].output, result.uses[2].input);
        for (toy.outputs, result.graph.outputs.items) |old, current| try std.testing.expectEqual(current, try result.remap(old));
        if (case.kind == .dora) for (result.adapters) |adapter| try std.testing.expect(result.graph.node(adapter.norm).op == .stop_gradient);
        var runtime = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer {
            for (runtime.items) |binding| cb.free(binding.value);
            runtime.deinit(a);
        }
        for (result.graph.parameters.items) |id| {
            const name = result.graph.parameterName(result.graph.node(id));
            if (std.mem.startsWith(u8, name, "__boundary_peft_mask.")) continue;
            const full = try std.fmt.allocPrint(scratch, "{s}.{s}", .{ case.id, name });
            const values = if (std.mem.startsWith(u8, name, "__peft_x")) try reference.floats(full) else try weights.floats(full);
            try put(a, &cb, &runtime, &result.graph, id, values);
        }
        for (result.uses) |use| if (use.mask != ml.null_node) {
            const module = result.adapters[use.adapter].module_name;
            const expected = try reference.floats(try std.fmt.allocPrint(scratch, "{s}.mask.{s}.{d}", .{ case.id, module, use.occurrence }));
            const replayed = try scratch.alloc(f32, expected.len);
            try peft.fillDropout(use, case.replay, replayed);
            try std.testing.expectEqualSlices(f32, expected, replayed);
            try put(a, &cb, &runtime, &result.graph, use.mask, replayed);
        };
        var seeds: [3]ml.autodiff.Seed = undefined;
        var builder = ml.Builder.init(&result.graph);
        for (toy.outputs, output_names, &seeds) |original, name, *seed| {
            const output = try result.remap(original);
            seed.* = .{ .output = output, .cotangent = try builder.parameter(try std.fmt.allocPrint(scratch, "__peft_seed.{s}", .{name}), result.graph.node(output).output_shape) };
        }
        var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
        defer wrt.deinit(a);
        try wrt.appendSlice(a, &toy.inputs);
        try wrt.appendSlice(a, result.trainable_parameters);
        var session = try training.Session.init(a, &result.graph, &seeds, wrt.items, .{ .gradient = .{ .require_all_gradients = true }, .max_tape_bytes = 4 * 1024 * 1024 });
        defer session.deinit();
        try resident_fixture.validate(a, &session);
        const identity = training.StepIdentity{ .binding = .{0x93} ** 32, .optimizer_step = case.replay.optimizer_step, .microbatch = case.replay.micro_batch };
        var tape = try session.forward(&cb, runtime.items, identity, null);
        defer tape.deinit();
        var cotangents = std.ArrayListUnmanaged(ops.CT).empty;
        defer {
            for (cotangents.items) |value| cb.free(value);
            cotangents.deinit(a);
        }
        for (output_names, 0..) |name, index| {
            try compare(a, &cb, try tape.logits(index), try reference.floats(try std.fmt.allocPrint(scratch, "{s}.output.{s}", .{ case.id, name })));
            const values = try reference.floats(try std.fmt.allocPrint(scratch, "{s}.cotangent.{s}", .{ case.id, name }));
            var buffer: [8]i32 = undefined;
            const ct = try cb.fromFloat32Shape(values, try dims(result.graph.node(seeds[index].output).output_shape, &buffer));
            cotangents.append(a, ct) catch |err| {
                cb.free(ct);
                return err;
            };
        }
        const decisions = [_]u8{0x67} ** 32;
        try tape.sealDecisions(decisions);
        var backward = try tape.backward(identity, decisions, case.loss, cotangents.items, null);
        defer backward.deinit(&cb);
        try std.testing.expectEqual(wrt.items.len, backward.parameter_ids.len);
        for (backward.parameter_ids, backward.gradients.outputs) |id, value| {
            const name = result.graph.parameterName(result.graph.node(id));
            errdefer std.debug.print("PEFT parameter/input VJP {s}\n", .{name});
            const expected = try reference.floats(try std.fmt.allocPrint(scratch, "{s}.gradient.{s}", .{ case.id, name }));
            try compare(a, &cb, value, expected);
        }
        if (device) |*value| {
            const gpu = value.backend.computeBackend();
            var actual = try resident_fixture.run(a, &gpu, &cb, &session, runtime.items, cotangents.items);
            defer actual.deinit(&gpu);
            for (output_names, 0..) |name, index| {
                const key = try std.fmt.allocPrint(scratch, "{s}.output.{s}", .{ case.id, name });
                try resident_fixture.expectValues(a, &gpu, try actual.logits(index), try reference.floats(key), 3e-6, 5e-6, key);
            }
            try std.testing.expectEqual(wrt.items.len, actual.gradients.?.outputs.len);
            for (session.gradient_parameters, actual.gradients.?.outputs) |id, gradient| {
                const name = result.graph.parameterName(result.graph.node(id));
                const key = try std.fmt.allocPrint(scratch, "{s}.gradient.{s}", .{ case.id, name });
                try resident_fixture.expectValues(a, &gpu, gradient, try reference.floats(key), 3e-6, 5e-6, key);
            }
        }
    }
}

test "GLiNER2.5 PEFT admission targets replay and function preserving initialization" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    _ = try buildToy(&b);
    const good = peft.Config{ .kind = .dora, .rank = 2, .alpha = 3, .dropout = 0.25, .targets = targets };
    const admitted = try peft.plan(a, &graph, good, .{});
    try std.testing.expectEqual(@as(usize, 2), admitted.adapters);
    try std.testing.expectEqual(@as(usize, 3), admitted.uses);
    try std.testing.expectEqual(@as(usize, (2 * 4 + 5 * 4 + 2 * 3) * 4), admitted.dropout_bytes);
    for ([_]peft.Config{
        .{ .rank = 0 }, .{ .rank = 2, .alpha = 0 }, .{ .rank = 2, .dropout = 1 }, .{ .rank = 2, .alpha = std.math.nan(f32) },
    }) |invalid| try std.testing.expectError(error.InvalidBoundaryPeftConfig, peft.plan(a, &graph, invalid, .{}));
    try std.testing.expectError(error.BoundaryPeftTargetNotResolved, peft.plan(a, &graph, .{ .rank = 2, .targets = &.{"record_head"} }, .{}));
    try std.testing.expectError(error.DuplicateBoundaryPeftTarget, peft.plan(a, &graph, .{ .rank = 2, .targets = &.{ "encoder", "encoder" } }, .{}));
    try std.testing.expectError(error.BoundaryPeftLimitExceeded, peft.plan(a, &graph, good, .{ .max_uses = 2 }));
    try std.testing.expectError(error.BoundaryPeftLimitExceeded, peft.plan(a, &graph, good, .{ .max_dropout_bytes = admitted.dropout_bytes - 1 }));
    var result = try peft.inject(a, &graph, good, .{});
    defer result.deinit();
    try std.testing.expectError(error.BoundaryPeftAlreadyInjected, peft.inject(a, &result.graph, good, .{}));
    const adapter = result.adapters[0];
    var initial = try peft.initialize(a, adapter, &.{ 1, 2, 3, 4, -2, 1, -2, 0, 1, 0, 0, 0 }, 123);
    defer initial.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 0 }, initial.b);
    try std.testing.expectEqualSlices(f32, &.{ @sqrt(@as(f32, 30)), 3, 1 }, initial.magnitude.?);
    for (initial.a) |value| try std.testing.expect(value >= -0.5 and value < 0.5);
    try std.testing.expectError(error.InvalidBoundaryPeftNorm, peft.initialize(a, adapter, &([_]f32{0} ** 12), 123));
    try std.testing.expectError(error.NonFiniteBoundaryPeftWeight, peft.initialize(a, adapter, &([_]f32{std.math.inf(f32)} ** 12), 123));
    var first: [8]f32 = undefined;
    var second: [8]f32 = undefined;
    const replay = peft.Replay{ .seed = 10, .optimizer_step = 20, .micro_batch = 30 };
    try peft.fillDropout(result.uses[0], replay, &first);
    try peft.fillDropout(result.uses[0], replay, &second);
    try std.testing.expectEqualSlices(f32, &first, &second);
    try peft.fillDropout(result.uses[0], .{ .seed = 30, .optimizer_step = 20, .micro_batch = 10 }, &second);
    try std.testing.expect(!std.mem.eql(f32, &first, &second));
    try std.testing.expectError(error.InvalidBoundaryPeftShape, peft.fillDropout(result.uses[0], replay, first[0..7]));
    // Exact module paths resolve equivalently to public aliases.
    try std.testing.expectEqual(@as(usize, 1), (try peft.plan(a, &graph, .{ .rank = 2, .targets = &.{"encoder.encoder.layer.0.attention.self.query_proj"} }, .{})).adapters);
}

fn allocationFailure(a: Allocator, graph: *const ml.Graph) !void {
    var result = try peft.inject(a, graph, .{ .kind = .dora, .rank = 2, .alpha = 3, .dropout = 0.25, .targets = targets }, .{});
    defer result.deinit();
    var initial = try peft.initialize(a, result.adapters[0], &([_]f32{1} ** 12), 1);
    defer initial.deinit();
    var mask: [8]f32 = undefined;
    try peft.fillDropout(result.uses[0], .{ .seed = 1, .optimizer_step = 0, .micro_batch = 0 }, &mask);
}
test "GLiNER2.5 PEFT graph and parameter initialization release every allocation failure" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    _ = try buildToy(&b);
    try std.testing.checkAllAllocationFailures(a, allocationFailure, .{&graph});
}
