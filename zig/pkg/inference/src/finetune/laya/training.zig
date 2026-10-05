// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    /// `upper` selects the per-question upper layers' visibility
    /// (`tree.Row.upper`); unpacked and unfused rows attend the same way in
    /// every layer.
    fn visible(self: Example, q: usize, k: usize, upper: bool) bool {
        const p = self.packed_row orelse return true;
        return if (upper and p.row.fused) p.row.upper().visible(q, k) else p.row.visible(q, k);
    }
    /// Question type for the type embedding; null for the shared trunk.
    fn tokenKind(self: Example, i: usize) ?Kind {
        const p = self.packed_row orelse return self.kind;
        const kind = p.row.kinds[i];
        return if (kind == tree.trunk_kind) null else @fromBackingInt(@intCast(kind));
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

/// Whether `name` stays at its source value: either the lowest `layers`
/// encoder layers are frozen (which also freezes the token embeddings
/// beneath them; gradients stop at the first trainable layer), or `lora`
/// targets the linear `name` belongs to (its weight is adapted through A/B
/// instead of being trained directly; see `architecture.isLoraFrozen`).
/// A `.lora_A`/`.lora_B` adapter has no source value to freeze to, so
/// `freeze_layers` never covers it: it stays trainable even under a frozen
/// layer, which is an ordinary LoRA-on-a-frozen-base configuration.
/// `whole_encoder` freezes every `encoder.*` tensor, the final norm included,
/// so the encoder's output is exactly the source's (a head trained on a
/// shared trunk); a job asks for it with `freeze_layers = num_hidden_layers + 1`.
pub fn frozen(name: []const u8, layers: u32, lora: ?architecture.Lora) bool {
    const adapter = std.mem.endsWith(u8, name, ".lora_A") or std.mem.endsWith(u8, name, ".lora_B");
    if (layers == whole_encoder and !adapter and std.mem.startsWith(u8, name, "encoder.")) return true;
    if (layers > 0 and !adapter) {
        if (std.mem.startsWith(u8, name, "encoder.embeddings.")) return true;
        const prefix = "encoder.layers.";
        if (std.mem.startsWith(u8, name, prefix)) blk: {
            const rest = name[prefix.len..];
            const end = std.mem.indexOfScalar(u8, rest, '.') orelse break :blk;
            const index = std.fmt.parseInt(u32, rest[0..end], 10) catch break :blk;
            if (index < layers) return true;
        }
    }
    if (lora) |cfg| if (architecture.isLoraFrozen(name, cfg.targets)) return true;
    return false;
}

pub const whole_encoder = std.math.maxInt(u32);

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

/// A float runtime input. On Metal it is uploaded to the device once here;
/// a host-backed input would be uploaded again by every op that reads it,
/// in every layer of the forward and the backward graph.
fn floatInput(cb: *const ops.ComputeBackend, values: []const f32, dims: []const i32) !ops.CT {
    if (cb.kind() == .metal and cb.vtable.residentTrainingPrimitive != null)
        return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = dims } }, .{});
    return cb.fromFloat32Shape(values, dims);
}

/// Kaiming-uniform initial values for a `.lora_A` parameter: bound
/// `1/sqrt(in_dim)`, matching `nn.Linear`'s default initializer and
/// `boundary_peft_graph.initializeModule`. Deterministic in `seed` and the
/// parameter's own name, so distinct adapters never share a random stream.
fn loraInitA(a: std.mem.Allocator, dims: []const i32, seed: u64, name: []const u8) ![]f32 {
    if (dims.len != 2 or dims[0] <= 0 or dims[1] <= 0) return error.InvalidLayaTrainingWeightShape;
    const in_dim: usize = @intCast(dims[1]);
    const values = try a.alloc(f32, @as(usize, @intCast(dims[0])) * in_dim);
    errdefer a.free(values);
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(name);
    var random = std.Random.DefaultPrng.init(hasher.final());
    const bound = 1 / @sqrt(@as(f32, @floatFromInt(in_dim)));
    for (values) |*v| v.* = random.random().float(f32) * 2 * bound - bound;
    return values;
}

/// All returned storage belongs to a caller-owned arena.
/// Trainable parameters, excluding those frozen by `freeze_layers` or `lora`.
/// A LoRA `.lora_A`/`.lora_B` parameter has no source tensor: `A` gets a
/// Kaiming-uniform draw and `B` is zero, so training starts identical to the
/// unmodified base model (`graph.zig`'s `Lora` doc comment). A pointer head
/// missing from the source (`laya.decision_head`) gets Kaiming-uniform
/// projections, zero biases and an identity `pointer.norm`.
pub fn parameters(a: std.mem.Allocator, graph: *const ml.Graph, reader: *const tensors.MMapReader, freeze_layers: u32, lora: ?architecture.Lora, seed: u64) ![]controller.Parameter {
    var out: std.ArrayListUnmanaged(controller.Parameter) = .empty;
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        const name = graph.parameterName(node);
        if (std.mem.startsWith(u8, name, "__") or frozen(name, freeze_layers, lora)) continue;
        const shape = node.output_shape;
        const dims = try a.alloc(i32, shape.rank());
        for (dims, shape.dims[0..shape.rank()]) |*dst, dim| dst.* = @intCast(dim);
        const values = if (std.mem.endsWith(u8, name, ".lora_A"))
            try loraInitA(a, dims, seed, name)
        else if (std.mem.endsWith(u8, name, ".lora_B")) blk: {
            const count = shape.numElements() orelse return error.InvalidLayaTrainingWeightShape;
            const zeros = try a.alloc(f32, @intCast(count));
            @memset(zeros, 0);
            break :blk zeros;
        } else if (std.mem.startsWith(u8, name, "pointer.") and reader.header.tensors.get(name) == null) blk: {
            // A new pointer head on a scorer checkpoint: `pointer.norm`
            // starts as the identity, and both projections get a seeded
            // Kaiming-uniform draw (`nn.Linear`'s default) with zero biases.
            // Over normalized rows that gives modest starting logits; a zero
            // query would sit at a saddle where the keys get no gradient.
            if (std.mem.endsWith(u8, name, ".weight") and !std.mem.eql(u8, name, "pointer.norm.weight")) break :blk try loraInitA(a, dims, seed, name);
            const count = shape.numElements() orelse return error.InvalidLayaTrainingWeightShape;
            const values = try a.alloc(f32, @intCast(count));
            @memset(values, if (std.mem.eql(u8, name, "pointer.norm.weight")) 1 else 0);
            break :blk values;
        } else blk: {
            var tensor = try reader.readTensor(name);
            defer tensor.deinit();
            if (!std.mem.eql(i64, tensor.shape, shape.dims[0..shape.rank()])) return error.InvalidLayaTrainingWeightShape;
            break :blk try floatValues(a, tensor);
        };
        try out.append(a, .{ .name = try a.dupe(u8, name), .values = values, .dimensions = dims, .group = parameterGroup(name) });
    }
    return out.toOwnedSlice(a);
}

/// Optimizer group of a parameter: 0 encoder, 1 decision and action heads,
/// 2 a pointer head (`laya.decision_head`), which starts from scratch and
/// gets its own learning rate (`job.Config.pointer_lr`).
pub fn parameterGroup(name: []const u8) u8 {
    if (std.mem.startsWith(u8, name, "encoder.")) return 0;
    if (std.mem.startsWith(u8, name, "pointer.")) return 2;
    return 1;
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
pub fn inputs(a: std.mem.Allocator, cb: *const ops.ComputeBackend, graph: *const ml.Graph, built: architecture.Built, cfg: modern.Config, examples: []const Example, random: std.Random, training: bool, use_fused_attention: bool) ![]interpreter.RuntimeInput {
    const l = try bucketedLayout(examples, cfg);
    var result: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    errdefer for (result.items) |input| cb.free(input.value);
    const ids = try a.alloc(i32, l.batch * l.sequence);
    @memset(ids, 0);
    const kinds = try a.alloc(i32, ids.len);
    const type_mask = try a.alloc(f32, ids.len * cfg.hidden_size);
    const markers = try a.alloc(i32, l.questions * l.options);
    const anchors = try a.alloc(i32, l.questions * l.options);
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
            kinds[row * l.sequence + i] = if (kind) |k| @backingInt(k) else 0;
            @memset(type_mask[(row * l.sequence + i) * cfg.hidden_size ..][0..cfg.hidden_size], if (kind != null) 1 else 0);
            positions[row * l.sequence + i] = if (i < e.ids.len) e.position(i) else @intCast(i);
        }
        for (0..e.questions()) |qi| {
            const q = e.question(qi);
            // The decision's [CLS]: its branch anchor, or the unpacked
            // sequence's first token.
            const anchor: usize = if (e.packed_row) |p| @intCast(p.row.anchors[qi]) else 0;
            @memset(anchors[question * l.options ..][0..l.options], @intCast(row * l.sequence + anchor));
            @memset(markers[question * l.options ..][0..l.options], @intCast(row * l.sequence));
            for (q.markers, 0..) |pos, i| markers[question * l.options + i] = @intCast(row * l.sequence + @as(usize, @intCast(pos)));
            question += 1;
        }
    }
    for ([_]ml.NodeId{ built.inputs.ids, built.inputs.kinds, built.inputs.markers, built.inputs.anchors }, [_][]const i32{ ids, kinds, markers, anchors }) |id, data| {
        const value = (try cb.fromInt32Shape(data, &.{@intCast(data.len)})) orelse return error.UnsupportedLayaTrainingBackend;
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = id, .value = value });
    }
    {
        const value = try floatInput(cb, type_mask, &.{ @intCast(ids.len), @intCast(cfg.hidden_size) });
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = built.inputs.type_mask, .value = value });
    }
    // Per-question upper layers (`packing.fuse_layers`) read a second set of
    // visibility inputs; every other layout feeds only the first.
    const fused = (cfg.laya orelse return error.InvalidLayaConfig).packing.fuse_layers > 0;
    const passes = [_]bool{ false, true };
    for (passes[0 .. @as(usize, 1) + @intFromBool(fused)]) |upper| {
        if (use_fused_attention) {
            // One physical i32 control leaf (SegmentTrainingAttentionAttrs):
            // six zeroed replay limbs (dropout is decorrelated across layers by
            // `dropout_stream_id` alone; per-run seeding is future work), one
            // `apply_dropout` flag, logical positions, then per-row
            // `laya_tree`-style ranges. Padding rows and padding columns keep
            // their zeroed (empty) range, so they see nothing and contribute a
            // safe zero row (unused downstream: only `markers` positions are
            // read out).
            //
            // The fused op's dropout is a graph-time attribute
            // (`SegmentTrainingAttentionAttrs.dropout_probability`, baked in at
            // `Program.init` and shared by every call the cached program serves,
            // training steps and `predict` eval alike) rather than a runtime
            // mask the way the dense path's `drop()` is -- the dense encoder
            // graph turns dropout off outside training by binding an all-ones
            // mask (`training: bool` above, `if (!training) 1 else ...`); the
            // fused op has no such mask, so `apply_dropout` is this call's only
            // way to silence it without rebuilding the graph. Leaving it
            // (incorrectly) on during eval would apply live seeded dropout to
            // the trainer's own eval predictions and to every served head at
            // predict/calibration time.
            const control = try a.alloc(i32, 7 + ids.len + ids.len * 6);
            @memset(control, 0);
            control[6] = @intFromBool(training);
            for (0..ids.len) |i| control[7 + i] = @intCast(positions[i]);
            const ranges_base = 7 + ids.len;
            for (examples, 0..) |e, row| {
                if (e.packed_row) |p| {
                    const row_ranges = try tree.ranges(a, if (upper and p.row.fused) p.row.upper() else p.row, 0);
                    defer a.free(row_ranges);
                    for (row_ranges, 0..) |bound, i| control[ranges_base + row * l.sequence * 6 + i] = @intCast(bound);
                } else {
                    for (0..e.ids.len) |q| {
                        control[ranges_base + (row * l.sequence + q) * 6 + 0] = 0;
                        control[ranges_base + (row * l.sequence + q) * 6 + 1] = @intCast(e.ids.len);
                    }
                }
            }
            const value = (try cb.fromInt32Shape(control, &.{@intCast(control.len)})) orelse return error.UnsupportedLayaTrainingBackend;
            errdefer cb.free(value);
            try result.append(a, .{ .node_id = if (upper) built.inputs.upper_segment_control else built.inputs.segment_control, .value = value });
        } else {
            const window: u64 = cfg.local_attention_window / 2;
            for (if (upper) [_]ml.NodeId{ built.inputs.upper_encoder_bias, built.inputs.upper_local_bias, built.inputs.upper_head_bias } else [_]ml.NodeId{ built.inputs.encoder_bias, built.inputs.local_bias, built.inputs.head_bias }, [_]u32{ cfg.num_attention_heads, cfg.num_attention_heads, cfg.hidden_size / 64 }, [_]bool{ false, true, false }) |id, heads, local| {
                const bias = try a.alloc(f32, l.batch * heads * l.sequence * l.sequence);
                const plane = l.sequence * l.sequence;
                for (examples, 0..) |e, row| {
                    const first = bias[row * heads * plane ..][0..plane];
                    for (0..l.sequence) |q| for (0..l.sequence) |k| {
                        var ok = k < e.ids.len and (q >= e.ids.len or e.visible(q, k, upper));
                        if (ok and local) ok = @abs(positions[row * l.sequence + q] - positions[row * l.sequence + k]) <= window;
                        first[q * l.sequence + k] = if (ok) 0 else -1e9;
                    };
                    for (1..heads) |h| @memcpy(bias[(row * heads + h) * plane ..][0..plane], first);
                }
                const value = try floatInput(cb, bias, &.{ @intCast(l.batch * heads), @intCast(l.sequence), @intCast(l.sequence) });
                errdefer cb.free(value);
                try result.append(a, .{ .node_id = id, .value = value });
            }
        }
    }
    const head_dim = cfg.hidden_size / cfg.num_attention_heads;
    for (built.inputs.rope, [_]f32{ cfg.global_rope_theta, cfg.local_rope_theta }) |ids_pair, theta| {
        const tables = try architecture.ropeTables(a, positions, cfg.num_attention_heads, head_dim, theta);
        for (ids_pair, tables) |id, table| {
            const value = try floatInput(cb, table, &.{ @intCast(ids.len * cfg.num_attention_heads), @intCast(head_dim / 2) });
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
        const value = try floatInput(cb, mask, dims[0..shape.rank()]);
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
    /// Whether `built` uses `fusedAttention` (roadmap step 2c): CPU-only
    /// today. `inputs` must build a `segment_control` runtime value instead
    /// of the three dense bias tensors when this is set.
    use_fused_attention: bool = false,

    pub fn init(a: std.mem.Allocator, cfg: modern.Config, l: architecture.Layout, dropout: f32) !Program {
        return initFrozen(a, cfg, l, dropout, 0, null);
    }

    /// Differentiates only the parameters above the lowest `freeze_layers`
    /// encoder layers and outside any `lora`-adapted linear; bind the rest
    /// through `frozen`.
    pub fn initFrozen(a: std.mem.Allocator, cfg: modern.Config, l: architecture.Layout, dropout: f32, freeze_layers: u32, lora: ?architecture.Lora) !Program {
        return initFrozenFused(a, cfg, l, dropout, freeze_layers, lora, false);
    }

    /// `use_fused_attention` selects the flash-style segment attention op
    /// (`architecture.buildWithAttention`) over the dense materialized-bias
    /// graph, removing the quadratic admission bound up to `max_len` 8192.
    pub fn initFrozenFused(a: std.mem.Allocator, cfg: modern.Config, l: architecture.Layout, dropout: f32, freeze_layers: u32, lora: ?architecture.Lora, use_fused_attention: bool) !Program {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var built = try architecture.buildWithAttention(&builder, cfg, l, dropout, lora, use_fused_attention);
        errdefer built.deinit(a);
        try graph.markOutput(built.logits);
        const seed = try builder.parameter("__laya_cotangent", graph.node(built.logits).output_shape);
        var ids: std.ArrayListUnmanaged(ml.NodeId) = .empty;
        defer ids.deinit(a);
        for (graph.parameters.items) |id| {
            const name = graph.parameterName(graph.node(id));
            if (!std.mem.startsWith(u8, name, "__") and !frozen(name, freeze_layers, lora)) try ids.append(a, id);
        }
        const wrt = try ids.toOwnedSlice(a);
        errdefer a.free(wrt);
        var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = built.logits, .cotangent = seed }}, wrt, .{ .require_all_gradients = true });
        errdefer gradients.deinit();
        gradients.graph.outputs.clearRetainingCapacity();
        for (gradients.param_grads) |id| try gradients.graph.markOutput(id);
        return .{ .graph = graph, .built = built, .seed = seed, .wrt = wrt, .gradients = gradients, .use_fused_attention = use_fused_attention };
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
    const runtime = try inputs(scratch, cb, &program.graph, program.built, cfg, examples, prng.random(), true, program.use_fused_attention);
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
    const cotangent = try floatInput(cb, loss.gradient, &.{ @intCast(l.questions), @intCast(l.options) });
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
    const runtime = try inputs(scratch, cb, &program.graph, program.built, cfg, examples, prng.random(), false, program.use_fused_attention);
    defer for (runtime) |input| cb.free(input.value);
    var binding = try trainer.bind(&program.graph, null);
    defer binding.deinit();
    const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, try bindFrozen(scratch, &program.graph, program.frozen), runtime });
    var result = try executeFramed(a, &program.graph, cb, combined);
    defer result.deinit(cb);
    return cb.toFloat32(result.outputs[0], a);
}
