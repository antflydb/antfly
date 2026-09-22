// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const architecture = @import("laya_graph.zig");
const objective = @import("laya_objective.zig");
const modern = @import("../architectures/modern_bert.zig");
const tensors = @import("../models/safetensors.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
pub const controller = @import("seeded_gradient_trainer.zig");

pub const Example = struct {
    ids: []const i64,
    markers: []const i64,
    kind: @import("../models/laya.zig").QuestionType,
    target: []const f32,
};

pub fn floatValues(a: std.mem.Allocator, tensor: Tensor) ![]f32 {
    const count = tensor.data.len / tensor.dtype.byteSize();
    const values = try a.alloc(f32, count);
    errdefer a.free(values);
    for (values, 0..) |*value, i| value.* = switch (tensor.dtype) {
        .f32 => @bitCast(std.mem.readInt(u32, tensor.data[i * 4 ..][0..4], .little)),
        .f16 => @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little)))),
        .bf16 => @bitCast(@as(u32, std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little)) << 16),
        else => return error.UnsupportedLayaTrainingDType,
    };
    return values;
}

/// All returned storage belongs to a caller-owned arena.
pub fn parameters(a: std.mem.Allocator, graph: *const ml.Graph, reader: *const tensors.MMapReader) ![]controller.Parameter {
    var out: std.ArrayListUnmanaged(controller.Parameter) = .empty;
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        const name = graph.parameterName(node);
        if (std.mem.startsWith(u8, name, "__")) continue;
        var tensor = try reader.readTensor(name);
        defer tensor.deinit();
        const shape = node.output_shape;
        if (!std.mem.eql(i64, tensor.shape, shape.dims[0..shape.rank()])) return error.InvalidLayaTrainingWeightShape;
        const dims = try a.alloc(i32, tensor.shape.len);
        for (dims, tensor.shape) |*dst, src| dst.* = @intCast(src);
        try out.append(a, .{ .name = try a.dupe(u8, name), .values = try floatValues(a, tensor), .dimensions = dims, .group = if (std.mem.startsWith(u8, name, "encoder.")) 0 else 1 });
    }
    return out.toOwnedSlice(a);
}

pub fn layout(examples: []const Example) !architecture.Layout {
    if (examples.len == 0 or examples.len > 128) return error.InvalidLayaTrainingBatch;
    var sequence: usize = 0;
    var options: usize = 0;
    for (examples) |e| {
        try objective.validateTarget(e.kind, e.target);
        if (e.ids.len == 0 or e.ids.len > 8192 or e.markers.len != e.target.len) return error.InvalidLayaTrainingBatch;
        for (e.markers) |pos| if (pos < 0 or pos >= e.ids.len) return error.InvalidLayaTrainingBatch;
        sequence = @max(sequence, e.ids.len);
        options = @max(options, e.markers.len);
    }
    return .{ .batch = @intCast(examples.len), .sequence = @intCast(sequence), .options = @intCast(options) };
}

/// CTs must be freed before the backend; metadata uses the caller's arena.
pub fn inputs(a: std.mem.Allocator, cb: *const ops.ComputeBackend, graph: *const ml.Graph, built: architecture.Built, cfg: modern.Config, examples: []const Example, random: std.Random, training: bool) ![]interpreter.RuntimeInput {
    const l = try layout(examples);
    var result: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    errdefer for (result.items) |input| cb.free(input.value);
    const ids = try a.alloc(i32, l.batch * l.sequence);
    @memset(ids, 0);
    const kinds = try a.alloc(i32, ids.len);
    const markers = try a.alloc(i32, l.batch * l.options);
    for (examples, 0..) |e, row| {
        for (e.ids, 0..) |v, i| {
            if (v < 0 or v >= cfg.vocab_size) return error.InvalidLayaTrainingToken;
            ids[row * l.sequence + i] = @intCast(v);
        }
        @memset(kinds[row * l.sequence ..][0..l.sequence], @intFromEnum(e.kind));
        @memset(markers[row * l.options ..][0..l.options], @intCast(row * l.sequence));
        for (e.markers, 0..) |pos, i| markers[row * l.options + i] = @intCast(row * l.sequence + @as(usize, @intCast(pos)));
    }
    for ([_]ml.NodeId{ built.inputs.ids, built.inputs.kinds, built.inputs.markers }, [_][]const i32{ ids, kinds, markers }) |id, data| {
        const value = (try cb.fromInt32Shape(data, &.{@intCast(data.len)})) orelse return error.UnsupportedLayaTrainingBackend;
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = id, .value = value });
    }
    for ([_]ml.NodeId{ built.inputs.encoder_bias, built.inputs.head_bias }, [_]u32{ cfg.num_attention_heads, cfg.hidden_size / 64 }) |id, heads| {
        const bias = try a.alloc(f32, l.batch * heads * l.sequence * l.sequence);
        for (bias, 0..) |*v, i| {
            const row = i / (heads * l.sequence * l.sequence);
            v.* = if (i % l.sequence < examples[row].ids.len) 0 else -1e9;
        }
        const value = try cb.fromFloat32Shape(bias, &.{ @intCast(l.batch * heads), @intCast(l.sequence), @intCast(l.sequence) });
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = id, .value = value });
    }
    for (built.dropouts.items) |entry| {
        const shape = graph.node(entry.node).output_shape;
        const mask = try a.alloc(f32, @intCast(shape.numElements().?));
        for (mask) |*v| v.* = if (!training) 1 else if (random.float(f32) < entry.probability) 0 else 1 / (1 - entry.probability);
        var dims: [8]i32 = undefined;
        for (shape.dims[0..shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);
        const value = try cb.fromFloat32Shape(mask, dims[0..shape.rank()]);
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = entry.node, .value = value });
    }
    return result.toOwnedSlice(a);
}

pub const Program = struct {
    graph: ml.Graph,
    built: architecture.Built,
    seed: ml.NodeId,
    wrt: []ml.NodeId,
    gradients: ml.autodiff.GradientResult,

    pub fn init(a: std.mem.Allocator, cfg: modern.Config, l: architecture.Layout, dropout: f32) !Program {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var built = try architecture.build(&builder, cfg, l, dropout);
        errdefer built.deinit(a);
        try graph.markOutput(built.logits);
        const seed = try builder.parameter("__laya_cotangent", graph.node(built.logits).output_shape);
        var ids: std.ArrayListUnmanaged(ml.NodeId) = .empty;
        defer ids.deinit(a);
        for (graph.parameters.items) |id| if (!std.mem.startsWith(u8, graph.parameterName(graph.node(id)), "__")) try ids.append(a, id);
        const wrt = try ids.toOwnedSlice(a);
        errdefer a.free(wrt);
        var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = built.logits, .cotangent = seed }}, wrt, .{ .require_all_gradients = true });
        errdefer gradients.deinit();
        gradients.graph.outputs.clearRetainingCapacity();
        for (gradients.param_grads) |id| try gradients.graph.markOutput(id);
        return .{ .graph = graph, .built = built, .seed = seed, .wrt = wrt, .gradients = gradients };
    }
    pub fn deinit(self: *Program) void {
        self.built.deinit(self.graph.allocator);
        self.graph.allocator.free(self.wrt);
        self.gradients.deinit();
        self.graph.deinit();
    }
};

pub const Report = struct { loss: f32, ce: f32, policy: f32, reward: f32, optimizer: controller.Result };

/// A whole forward/backward pair uses the same immutable weights and dropout
/// masks. The trainer publishes an update only after the binding is released.
pub fn step(a: std.mem.Allocator, program: *Program, trainer: *controller.Trainer, cfg: modern.Config, examples: []const Example, loss_cfg: objective.Config, seed_value: u64) !Report {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const cb = trainer.owner.compute_backend;
    var prng = std.Random.DefaultPrng.init(seed_value);
    const runtime = try inputs(scratch, cb, &program.graph, program.built, cfg, examples, prng.random(), true);
    defer for (runtime) |input| cb.free(input.value);
    var binding = try trainer.bind(&program.graph, null);
    var bound = true;
    defer if (bound) binding.deinit();
    const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, runtime });
    var forward = try interpreter.execute(a, &program.graph, cb, .{ .runtime_inputs = combined, .strict_integer_constants = true });
    defer forward.deinit(cb);
    const logits = try cb.toFloat32(forward.outputs[0], scratch);
    const l = try layout(examples);
    const rows = try scratch.alloc(objective.Row, examples.len);
    for (rows, examples) |*row, e| row.* = .{ .kind = e.kind, .target = e.target };
    const noise = try scratch.alloc(f32, if (loss_cfg.rl_weight == 0) 0 else loss_cfg.group_size * logits.len);
    for (noise) |*value| value.* = prng.random().floatNorm(f32);
    const loss = try objective.evaluate(a, loss_cfg, rows, l.options, logits, noise);
    defer loss.deinit(a);
    const backward_inputs = try scratch.alloc(interpreter.RuntimeInput, combined.len + 1);
    for (combined, backward_inputs[0..combined.len]) |input, *dst| dst.* = .{ .node_id = program.gradients.id_map[input.node_id], .value = input.value };
    const cotangent = try cb.fromFloat32Shape(loss.gradient, &.{ @intCast(l.batch), @intCast(l.options) });
    defer cb.free(cotangent);
    backward_inputs[combined.len] = .{ .node_id = program.gradients.id_map[program.seed], .value = cotangent };
    var backward = try interpreter.execute(a, &program.gradients.graph, cb, .{ .runtime_inputs = backward_inputs, .strict_integer_constants = true });
    defer backward.deinit(cb);
    // Results own their gradient tensors; freeing parameter/input bindings does
    // not invalidate these outputs. No optimizer mutation occurs with a tape live.
    binding.deinit();
    bound = false;
    const outcome = if (trainer.execution == .native) blk: {
        const gradients = try scratch.alloc(controller.Gradient, program.wrt.len);
        for (program.wrt, backward.outputs, gradients) |id, output, *gradient| gradient.* = .{ .name = program.graph.parameterName(program.graph.node(id)), .values = try cb.toFloat32(output, scratch) };
        break :blk try trainer.submit(trainer.identity(), loss.loss, gradients, null);
    } else blk: {
        const gradients = try scratch.alloc(controller.ResidentGradient, program.wrt.len);
        var uploaded: usize = 0;
        defer for (gradients[0..uploaded]) |gradient| cb.free(gradient.value.tensor);
        // The materialized interpreter can return host-backed CTs for some
        // VJPs. Resident AdamW must receive tensors owned by this provider.
        // Admit that transfer explicitly; never relabel a foreign CT.
        for (program.wrt, backward.outputs, gradients) |id, output, *gradient| {
            const values = try cb.toFloat32(output, a);
            defer a.free(values);
            const shape = program.graph.node(id).output_shape;
            var dims: [8]i32 = undefined;
            for (shape.dims[0..shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);
            const owned = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = dims[0..shape.rank()] } }, .{});
            gradient.* = .{ .name = program.graph.parameterName(program.graph.node(id)), .value = .{ .tensor = owned } };
            uploaded += 1;
        }
        break :blk try trainer.submitResident(trainer.identity(), loss.loss, gradients, null);
    };
    return .{ .loss = loss.loss, .ce = loss.ce, .policy = loss.policy, .reward = loss.reward, .optimizer = outcome };
}

pub fn predict(a: std.mem.Allocator, program: *Program, trainer: *controller.Trainer, cfg: modern.Config, examples: []const Example) ![]f32 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const cb = trainer.owner.compute_backend;
    var prng = std.Random.DefaultPrng.init(0);
    const runtime = try inputs(scratch, cb, &program.graph, program.built, cfg, examples, prng.random(), false);
    defer for (runtime) |input| cb.free(input.value);
    var binding = try trainer.bind(&program.graph, null);
    defer binding.deinit();
    const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, runtime });
    var result = try interpreter.execute(a, &program.graph, cb, .{ .runtime_inputs = combined, .strict_integer_constants = true });
    defer result.deinit(cb);
    return cb.toFloat32(result.outputs[0], a);
}
