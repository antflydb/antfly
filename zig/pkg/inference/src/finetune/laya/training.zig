// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../../ops/ops.zig");
const interpreter = @import("../../graph/interpreter.zig");
const architecture = @import("graph.zig");
const objective = @import("objective.zig");
const modern = @import("../../architectures/modern_bert.zig");
const tensors = @import("../../models/safetensors.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
pub const controller = @import("../seeded_gradient_trainer.zig");

const Kind = @import("../../models/laya.zig").QuestionType;
const tree = @import("../../pipelines/laya_tree.zig");

/// One training sequence. An unpacked example holds a single question in
/// `markers`/`kind`/`target`. A tree-packed example (`packed_row`) holds every
/// question of one row, in row order; its `markers`/`kind`/`target` are unused.
pub const Example = struct {
    ids: []const i64,
    markers: []const i64 = &.{},
    kind: Kind = .choice,
    target: []const f32 = &.{},
    packed_row: ?*const Packed = null,

    pub fn questions(self: Example) usize {
        return if (self.packed_row) |p| p.row.questions() else 1;
    }
    pub fn question(self: Example, q: usize) Question {
        const p = self.packed_row orelse return .{ .markers = self.markers, .kind = self.kind, .target = self.target };
        const target = p.targets[q];
        return .{ .markers = p.row.markers[q * p.row.width ..][0..target.len], .kind = p.kinds[q], .target = target };
    }
    /// Logical position and visibility of physical tokens (0..ids.len).
    fn position(self: Example, i: usize) i64 {
        return if (self.packed_row) |p| p.row.positions[i] else @intCast(i);
    }
    fn visible(self: Example, q: usize, k: usize) bool {
        return if (self.packed_row) |p| p.row.visible(q, k) else true;
    }
    /// Question type for the type embedding; null for the shared trunk.
    fn tokenKind(self: Example, i: usize) ?Kind {
        const p = self.packed_row orelse return self.kind;
        const kind = p.row.kinds[i];
        return if (kind == tree.trunk_kind) null else @enumFromInt(kind);
    }
};
pub const Question = struct { markers: []const i64, kind: Kind, target: []const f32 };
pub const Packed = struct {
    row: tree.Row,
    kinds: []const Kind,
    targets: []const []const f32,
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

/// Whether `name` stays at its source value when the lowest `layers` encoder
/// layers are frozen. Freezing any layer also freezes the token embeddings
/// beneath it. Gradients stop at the first trainable layer.
pub fn frozen(name: []const u8, layers: u32) bool {
    if (layers == 0) return false;
    if (std.mem.startsWith(u8, name, "encoder.embeddings.")) return true;
    const prefix = "encoder.layers.";
    if (!std.mem.startsWith(u8, name, prefix)) return false;
    const rest = name[prefix.len..];
    const end = std.mem.indexOfScalar(u8, rest, '.') orelse return false;
    const index = std.fmt.parseInt(u32, rest[0..end], 10) catch return false;
    return index < layers;
}

/// A frozen parameter's value, owned by the caller for the whole run and
/// bound by name into every program. It is never updated or freed by a step.
pub const Frozen = struct { name: []const u8, value: ops.CT };

fn bindFrozen(a: std.mem.Allocator, graph: *const ml.Graph, values: []const Frozen) ![]interpreter.RuntimeInput {
    var result: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    for (graph.parameters.items) |id| {
        const name = graph.parameterName(graph.node(id));
        for (values) |f| if (std.mem.eql(u8, f.name, name)) {
            try result.append(a, .{ .node_id = id, .value = f.value });
            break;
        };
    }
    return result.toOwnedSlice(a);
}

/// All returned storage belongs to a caller-owned arena.
/// Trainable parameters, excluding those frozen by `freeze_layers`.
pub fn parameters(a: std.mem.Allocator, graph: *const ml.Graph, reader: *const tensors.MMapReader, freeze_layers: u32) ![]controller.Parameter {
    var out: std.ArrayListUnmanaged(controller.Parameter) = .empty;
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        const name = graph.parameterName(node);
        if (std.mem.startsWith(u8, name, "__") or frozen(name, freeze_layers)) continue;
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
    var questions: usize = 0;
    for (examples) |e| {
        if (e.ids.len == 0 or e.ids.len > 8192 or e.questions() == 0) return error.InvalidLayaTrainingBatch;
        if (e.packed_row) |p| if (p.row.ids.ptr != e.ids.ptr or p.kinds.len != p.row.questions() or p.targets.len != p.row.questions()) return error.InvalidLayaTrainingBatch;
        for (0..e.questions()) |qi| {
            const q = e.question(qi);
            try objective.validateTarget(q.kind, q.target);
            if (q.markers.len != q.target.len) return error.InvalidLayaTrainingBatch;
            for (q.markers) |pos| if (pos < 0 or pos >= e.ids.len) return error.InvalidLayaTrainingBatch;
            options = @max(options, q.markers.len);
        }
        sequence = @max(sequence, e.ids.len);
        questions += e.questions();
    }
    return .{ .batch = @intCast(examples.len), .sequence = @intCast(sequence), .options = @intCast(options), .questions = @intCast(questions) };
}

/// `layout` rounded up so that a small set of compiled programs covers a
/// whole dataset: sequences to 64 tokens and options to 4, within the
/// model's limits. Padding keys are masked and padded option logits carry no
/// loss, so results are unchanged; graph construction and autodiff (seconds
/// per new shape on the released checkpoint) are no longer paid per step.
pub fn bucketedLayout(examples: []const Example, cfg: modern.Config) !architecture.Layout {
    var l = try layout(examples);
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    const sequence_limit = if (laya.packing.enabled()) laya.packing.max_packed_len else laya.max_len;
    l.sequence = @intCast(@min(std.mem.alignForward(usize, l.sequence, 64), @max(sequence_limit, l.sequence)));
    l.options = @intCast(@min(std.mem.alignForward(usize, l.options, 4), @max(laya.maxOptions(), l.options)));
    return l;
}

/// Decision rows in logit order.
pub fn rows(a: std.mem.Allocator, examples: []const Example) ![]objective.Row {
    var out: std.ArrayListUnmanaged(objective.Row) = .empty;
    for (examples) |e| for (0..e.questions()) |qi| {
        const q = e.question(qi);
        try out.append(a, .{ .kind = q.kind, .target = q.target });
    };
    return out.toOwnedSlice(a);
}

/// CTs must be freed before the backend; metadata uses the caller's arena.
pub fn inputs(a: std.mem.Allocator, cb: *const ops.ComputeBackend, graph: *const ml.Graph, built: architecture.Built, cfg: modern.Config, examples: []const Example, random: std.Random, training: bool) ![]interpreter.RuntimeInput {
    const l = try bucketedLayout(examples, cfg);
    var result: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    errdefer for (result.items) |input| cb.free(input.value);
    const ids = try a.alloc(i32, l.batch * l.sequence);
    @memset(ids, 0);
    const kinds = try a.alloc(i32, ids.len);
    const type_mask = try a.alloc(f32, ids.len * cfg.hidden_size);
    const markers = try a.alloc(i32, l.questions * l.options);
    const positions = try a.alloc(i64, ids.len);
    var question: usize = 0;
    for (examples, 0..) |e, row| {
        for (e.ids, 0..) |v, i| {
            if (v < 0 or v >= cfg.vocab_size) return error.InvalidLayaTrainingToken;
            ids[row * l.sequence + i] = @intCast(v);
        }
        for (0..l.sequence) |i| {
            // Padding repeats the row's last logical position; keys mask it.
            const kind = if (i < e.ids.len) e.tokenKind(i) else if (e.packed_row == null) e.kind else null;
            kinds[row * l.sequence + i] = if (kind) |k| @intFromEnum(k) else 0;
            @memset(type_mask[(row * l.sequence + i) * cfg.hidden_size ..][0..cfg.hidden_size], if (kind != null) 1 else 0);
            positions[row * l.sequence + i] = if (i < e.ids.len) e.position(i) else @intCast(i);
        }
        for (0..e.questions()) |qi| {
            const q = e.question(qi);
            @memset(markers[question * l.options ..][0..l.options], @intCast(row * l.sequence));
            for (q.markers, 0..) |pos, i| markers[question * l.options + i] = @intCast(row * l.sequence + @as(usize, @intCast(pos)));
            question += 1;
        }
    }
    for ([_]ml.NodeId{ built.inputs.ids, built.inputs.kinds, built.inputs.markers }, [_][]const i32{ ids, kinds, markers }) |id, data| {
        const value = (try cb.fromInt32Shape(data, &.{@intCast(data.len)})) orelse return error.UnsupportedLayaTrainingBackend;
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = id, .value = value });
    }
    {
        const value = try cb.fromFloat32Shape(type_mask, &.{ @intCast(ids.len), @intCast(cfg.hidden_size) });
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = built.inputs.type_mask, .value = value });
    }
    const window: u64 = cfg.local_attention_window / 2;
    for ([_]ml.NodeId{ built.inputs.encoder_bias, built.inputs.local_bias, built.inputs.head_bias }, [_]u32{ cfg.num_attention_heads, cfg.num_attention_heads, cfg.hidden_size / 64 }, [_]bool{ false, true, false }) |id, heads, local| {
        const bias = try a.alloc(f32, l.batch * heads * l.sequence * l.sequence);
        const plane = l.sequence * l.sequence;
        for (examples, 0..) |e, row| {
            const first = bias[row * heads * plane ..][0..plane];
            for (0..l.sequence) |q| for (0..l.sequence) |k| {
                var ok = k < e.ids.len and (q >= e.ids.len or e.visible(q, k));
                if (ok and local) ok = @abs(positions[row * l.sequence + q] - positions[row * l.sequence + k]) <= window;
                first[q * l.sequence + k] = if (ok) 0 else -1e9;
            };
            for (1..heads) |h| @memcpy(bias[(row * heads + h) * plane ..][0..plane], first);
        }
        const value = try cb.fromFloat32Shape(bias, &.{ @intCast(l.batch * heads), @intCast(l.sequence), @intCast(l.sequence) });
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = id, .value = value });
    }
    const head_dim = cfg.hidden_size / cfg.num_attention_heads;
    for (built.inputs.rope, [_]f32{ cfg.global_rope_theta, cfg.local_rope_theta }) |ids_pair, theta| {
        const tables = try architecture.ropeTables(a, positions, cfg.num_attention_heads, head_dim, theta);
        for (ids_pair, tables) |id, table| {
            const value = try cb.fromFloat32Shape(table, &.{ @intCast(ids.len * cfg.num_attention_heads), @intCast(head_dim / 2) });
            errdefer cb.free(value);
            try result.append(a, .{ .node_id = id, .value = value });
        }
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
    /// Values bound for parameters outside `wrt`; borrowed, set by the owner.
    frozen: []const Frozen = &.{},

    pub fn init(a: std.mem.Allocator, cfg: modern.Config, l: architecture.Layout, dropout: f32) !Program {
        return initFrozen(a, cfg, l, dropout, 0);
    }

    /// Differentiates only the parameters above the lowest `freeze_layers`
    /// encoder layers; bind the rest through `frozen`.
    pub fn initFrozen(a: std.mem.Allocator, cfg: modern.Config, l: architecture.Layout, dropout: f32, freeze_layers: u32) !Program {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var built = try architecture.build(&builder, cfg, l, dropout);
        errdefer built.deinit(a);
        try graph.markOutput(built.logits);
        const seed = try builder.parameter("__laya_cotangent", graph.node(built.logits).output_shape);
        var ids: std.ArrayListUnmanaged(ml.NodeId) = .empty;
        defer ids.deinit(a);
        for (graph.parameters.items) |id| {
            const name = graph.parameterName(graph.node(id));
            if (!std.mem.startsWith(u8, name, "__") and !frozen(name, freeze_layers)) try ids.append(a, id);
        }
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

/// Run a graph with its device work batched into one command frame on
/// Metal, rather than a submit-and-wait per node, then synchronize once.
/// ANTFLY_LAYA_TRAIN_UNFRAMED restores per-node submission.
pub fn executeFramed(a: std.mem.Allocator, graph: *const ml.Graph, cb: *const ops.ComputeBackend, runtime_inputs: []const interpreter.RuntimeInput) !interpreter.ExecutionResult {
    const framed = cb.kind() == .metal and !cb.decoderRuntimeHasActiveFrame() and
        !@import("antfly_platform").env.getenvBool("ANTFLY_LAYA_TRAIN_UNFRAMED") and try cb.decoderRuntimeBeginFrame();
    errdefer if (framed) cb.decoderRuntimeCancelFrame() catch {};
    const result = try interpreter.execute(a, graph, cb, .{ .runtime_inputs = runtime_inputs, .strict_integer_constants = true });
    if (framed) cb.decoderRuntimeSubmitAndWaitFrame() catch |err| {
        var owned = result;
        owned.deinit(cb);
        return err;
    };
    return result;
}

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
    const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, try bindFrozen(scratch, &program.graph, program.frozen), runtime });
    var forward = try executeFramed(a, &program.graph, cb, combined);
    defer forward.deinit(cb);
    const logits = try cb.toFloat32(forward.outputs[0], scratch);
    const l = try bucketedLayout(examples, cfg);
    const decision_rows = try rows(scratch, examples);
    const noise = try scratch.alloc(f32, if (loss_cfg.rl_weight == 0) 0 else loss_cfg.group_size * logits.len);
    for (noise) |*value| value.* = prng.random().floatNorm(f32);
    const loss = try objective.evaluate(a, loss_cfg, decision_rows, l.options, logits, noise);
    defer loss.deinit(a);
    const backward_inputs = try scratch.alloc(interpreter.RuntimeInput, combined.len + 1);
    for (combined, backward_inputs[0..combined.len]) |input, *dst| dst.* = .{ .node_id = program.gradients.id_map[input.node_id], .value = input.value };
    const cotangent = try cb.fromFloat32Shape(loss.gradient, &.{ @intCast(l.questions), @intCast(l.options) });
    defer cb.free(cotangent);
    backward_inputs[combined.len] = .{ .node_id = program.gradients.id_map[program.seed], .value = cotangent };
    var backward = try executeFramed(a, &program.gradients.graph, cb, backward_inputs);
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
        // Resident AdamW must receive tensors owned by this provider. A dense
        // device gradient is copied on the GPU; the materialized interpreter
        // can also return host-backed CTs for some VJPs, which are uploaded.
        // Either way the transfer is explicit; a foreign CT is never relabeled.
        for (program.wrt, backward.outputs, gradients) |id, output, *gradient| {
            const shape = program.graph.node(id).output_shape;
            var dims: [8]i32 = undefined;
            for (shape.dims[0..shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);
            const owned = cb.residentTrainingPrimitive(&.{ .adopt_f32 = .{ .input = output, .shape = dims[0..shape.rank()] } }, .{}) catch |err| switch (err) {
                error.UnsupportedResidentTrainingPrimitive, error.ResidentTrainingRequiresDeviceTensor => upload: {
                    const values = try cb.toFloat32(output, a);
                    defer a.free(values);
                    break :upload try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = dims[0..shape.rank()] } }, .{});
                },
                else => return err,
            };
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
    const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, try bindFrozen(scratch, &program.graph, program.frozen), runtime });
    var result = try executeFramed(a, &program.graph, cb, combined);
    defer result.deinit(cb);
    return cb.toFloat32(result.outputs[0], a);
}
