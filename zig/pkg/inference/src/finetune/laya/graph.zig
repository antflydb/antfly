// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable Laya decision logits with the released ModernBERT layout.
//! Parameters retain upstream safetensors names. Calibration and the auxiliary
//! action head are not part of the typed-decision training objective.
const std = @import("std");
const ml = @import("ml").graph;
const modern = @import("../../architectures/modern_bert.zig");
const B = ml.Builder;
const Id = ml.NodeId;
const Shape = ml.Shape;

pub const Layout = struct { batch: u32, sequence: u32, options: u32 };
pub const Inputs = struct {
    ids: Id,
    kinds: Id, // repeated once per token
    markers: Id, // flattened batch-offset token indices
    encoder_bias: Id, // [B*encoder_heads,S,S], padding only
    head_bias: Id, // [B*head_heads,S,S], padding only
};
pub const Dropout = struct { node: Id, probability: f32 };
pub const Built = struct {
    inputs: Inputs,
    logits: Id,
    dropouts: std.ArrayListUnmanaged(Dropout) = .empty,
    traces: std.ArrayListUnmanaged(struct { name: []const u8, node: Id }) = .empty,
    pub fn deinit(self: *Built, a: std.mem.Allocator) void {
        self.dropouts.deinit(a);
        for (self.traces.items) |entry| a.free(entry.name);
        self.traces.deinit(a);
    }
    fn trace(self: *Built, a: std.mem.Allocator, name: []const u8, node: Id) !void {
        const owned = try a.dupe(u8, name);
        errdefer a.free(owned);
        try self.traces.append(a, .{ .name = owned, .node = node });
    }
};

fn param(b: *B, prefix: []const u8, suffix: []const u8, dims: []const i64) !Id {
    var name: [256]u8 = undefined;
    return b.parameter(try std.fmt.bufPrint(&name, "{s}.{s}", .{ prefix, suffix }), Shape.init(.f32, dims));
}
fn linear(b: *B, x: Id, prefix: []const u8, rows: u32, input: u32, output: u32, bias: bool) !Id {
    const w = try param(b, prefix, "weight", &.{ output, input });
    const fused = if (bias) try b.linear(x, w, try param(b, prefix, "bias", &.{output}), rows, input, output) else try b.linearNoBias(x, w, rows, input, output);
    // Execute the same arithmetic in the forward and backward replay. Generic
    // inference slots cache weight snapshots and are unsuitable for mutable
    // training parameters (including device-only normalization tensors).
    return b.graph.node(fused).vjp_alternate;
}
fn norm(b: *B, x: Id, prefix: []const u8, dim: u32, eps: f32, bias: bool) !Id {
    const w = try param(b, prefix, "weight", &.{dim});
    const z = if (bias) try param(b, prefix, "bias", &.{dim}) else blk: {
        const zeros = try b.graph.allocator.alloc(f32, dim);
        defer b.graph.allocator.free(zeros);
        @memset(zeros, 0);
        break :blk try b.tensorConst(zeros, Shape.init(.f32, &.{dim}));
    };
    const fused = try b.layerNorm(x, w, z, dim, eps);
    return b.graph.node(fused).vjp_alternate;
}
fn drop(b: *B, built: *Built, x: Id, probability: f32) !Id {
    if (probability == 0) return x;
    var name: [64]u8 = undefined;
    const mask = try b.parameter(try std.fmt.bufPrint(&name, "__laya_dropout_{d}", .{built.dropouts.items.len}), b.graph.node(x).output_shape);
    try built.dropouts.append(b.graph.allocator, .{ .node = mask, .probability = probability });
    return b.mul(x, mask);
}
/// PyTorch uses the zero subgradient at the ReLU kink. The generic builder's
/// alternate uses x < 0 and therefore passes the cotangent through exact zero.
pub fn relu(b: *B, x: Id) !Id {
    const shape = b.graph.node(x).output_shape;
    const zero = try b.scalarConst(shape.dtype, 0);
    const positive = try b.graph.addNode(.{ .op = .{ .less_than = {} }, .output_shape = shape, .inputs = .{ zero, x, ml.null_node, ml.null_node }, .num_inputs = 2 });
    return b.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape, .inputs = .{ positive, x, zero, ml.null_node }, .num_inputs = 3 });
}
fn heads(b: *B, x: Id, l: Layout, h: u32, d: u32) !Id {
    const shaped = try b.reshape(x, Shape.init(.f32, &.{ l.batch, l.sequence, h, d }));
    return b.reshape(try b.transpose(shaped, &.{ 0, 2, 1, 3 }), Shape.init(.f32, &.{ l.batch * h, l.sequence, d }));
}
fn attention(b: *B, built: *Built, q: Id, k: Id, v: Id, bias: Id, l: Layout, h: u32, d: u32, dropout: f32) !Id {
    const scores = try b.matmul3DTransB(try heads(b, q, l, h, d), try heads(b, k, l, h, d));
    const scaled = try b.mul(scores, try b.scalarConst(.f32, 1 / @sqrt(@as(f32, @floatFromInt(d)))));
    const probs = try drop(b, built, try b.softmax(try b.add(scaled, bias)), dropout);
    const ctx = try b.matmul3D(probs, try heads(b, v, l, h, d));
    const shaped = try b.reshape(ctx, Shape.init(.f32, &.{ l.batch, h, l.sequence, d }));
    return b.reshape(try b.transpose(shaped, &.{ 0, 2, 1, 3 }), Shape.init(.f32, &.{ l.batch * l.sequence, h * d }));
}

// Split-half RoPE expressed as primitives, preserving the physical token/head
// layout and an exact VJP without depending on inference-only fused kernels.
fn rope(b: *B, x: Id, l: Layout, h: u32, d: u32, theta: f32) !Id {
    const a = b.graph.allocator;
    const n = l.batch * l.sequence * h;
    const cosine = try a.alloc(f32, n * (d / 2));
    defer a.free(cosine);
    const sine = try a.alloc(f32, cosine.len);
    defer a.free(sine);
    for (0..n) |row| for (0..d / 2) |i| {
        const pos = (row / h) % l.sequence;
        const angle = @as(f32, @floatFromInt(pos)) / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * i)) / @as(f32, @floatFromInt(d)));
        cosine[row * (d / 2) + i] = @cos(angle);
        sine[row * (d / 2) + i] = @sin(angle);
    };
    const shape = Shape.init(.f32, &.{ n, d / 2 });
    const c = try b.tensorConst(cosine, shape);
    const s = try b.tensorConst(sine, shape);
    const reshaped = try b.reshape(x, Shape.init(.f32, &.{ n, d }));
    const left = try b.sliceLastDim(reshaped, 0, d / 2);
    const right = try b.sliceLastDim(reshaped, d / 2, d);
    const first = try b.sub(try b.mul(left, c), try b.mul(right, s));
    const second = try b.add(try b.mul(left, s), try b.mul(right, c));
    return b.reshape(try b.concat(first, second, 1), Shape.init(.f32, &.{ l.batch * l.sequence, h * d }));
}

pub fn validate(cfg: modern.Config, l: Layout, head_dropout: f32) !void {
    const lc = cfg.laya orelse return error.InvalidLayaConfig;
    if (cfg.checkpoint_layout != .huggingface_fused_qkv_no_bias or cfg.rope_interleaved or
        cfg.hidden_size < 64 or cfg.hidden_size > 4096 or cfg.hidden_size % 64 != 0 or cfg.num_attention_heads == 0 or
        cfg.hidden_size % cfg.num_attention_heads != 0 or (cfg.hidden_size / cfg.num_attention_heads) % 2 != 0 or
        cfg.global_attn_every_n_layers == 0 or cfg.num_hidden_layers == 0 or cfg.num_hidden_layers > 128 or
        cfg.vocab_size == 0 or cfg.vocab_size > 1024 * 1024 or cfg.intermediate_size == 0 or cfg.intermediate_size > 32768 or lc.head_layers > 16 or
        !std.math.isFinite(cfg.global_rope_theta) or cfg.global_rope_theta <= 0 or !std.math.isFinite(cfg.local_rope_theta) or cfg.local_rope_theta <= 0 or
        !std.math.isFinite(cfg.layer_norm_eps) or cfg.layer_norm_eps <= 0 or
        l.batch == 0 or l.batch > 128 or l.sequence == 0 or l.sequence > lc.max_len or
        l.options < 2 or l.options > 20 or !std.math.isFinite(head_dropout) or head_dropout < 0 or head_dropout >= 1)
        return error.InvalidLayaTrainingLayout;
    // Bound the materialized reference attention before allocating constants.
    if (@as(u64, l.batch) * l.sequence * l.sequence * @max(cfg.num_attention_heads, cfg.hidden_size / 64) > 64 * 1024 * 1024)
        return error.LayaTrainingAttentionLimitExceeded;
}

pub fn build(b: *B, cfg: modern.Config, l: Layout, head_dropout: f32) !Built {
    try validate(cfg, l, head_dropout);
    const lc = cfg.laya.?;
    const h = cfg.hidden_size;
    const n = l.batch * l.sequence;
    const nh = cfg.num_attention_heads;
    const hh = h / 64;
    var result = Built{ .inputs = .{
        .ids = try b.parameter("__laya_ids", Shape.init(.i32, &.{n})),
        .kinds = try b.parameter("__laya_kinds", Shape.init(.i32, &.{n})),
        .markers = try b.parameter("__laya_markers", Shape.init(.i32, &.{l.batch * l.options})),
        .encoder_bias = try b.parameter("__laya_encoder_bias", Shape.init(.f32, &.{ l.batch * nh, l.sequence, l.sequence })),
        .head_bias = try b.parameter("__laya_head_bias", Shape.init(.f32, &.{ l.batch * hh, l.sequence, l.sequence })),
    }, .logits = ml.null_node };
    errdefer result.deinit(b.graph.allocator);
    const embedding = try param(b, "encoder.embeddings.tok_embeddings", "weight", &.{ cfg.vocab_size, h });
    var x = try b.gather(embedding, result.inputs.ids, Shape.init(.f32, &.{ n, h }));
    x = try norm(b, x, "encoder.embeddings.norm", h, cfg.layer_norm_eps, false);
    try result.trace(b.graph.allocator, "encoder.embeddings.norm", x);
    const local = try b.graph.allocator.alloc(f32, l.batch * nh * l.sequence * l.sequence);
    defer b.graph.allocator.free(local);
    for (local, 0..) |*value, i| {
        const q = (i / l.sequence) % l.sequence;
        const k = i % l.sequence;
        const distance = @max(q, k) - @min(q, k);
        value.* = if (distance > cfg.local_attention_window / 2) -1e9 else 0;
    }
    const local_bias = try b.add(result.inputs.encoder_bias, try b.tensorConst(local, b.graph.node(result.inputs.encoder_bias).output_shape));
    for (0..cfg.num_hidden_layers) |layer| {
        var buffer: [128]u8 = undefined;
        var names: [256]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&buffer, "encoder.layers.{d}", .{layer});
        const normalized = if (layer == 0) x else try norm(b, x, try std.fmt.bufPrint(&names, "{s}.attn_norm", .{prefix}), h, cfg.layer_norm_eps, false);
        const qkv = try linear(b, normalized, try std.fmt.bufPrint(&names, "{s}.attn.Wqkv", .{prefix}), n, h, h * 3, false);
        const global = layer % cfg.global_attn_every_n_layers == 0;
        const theta = if (global) cfg.global_rope_theta else cfg.local_rope_theta;
        const q = try rope(b, try b.sliceLastDim(qkv, 0, h), l, nh, h / nh, theta);
        const k = try rope(b, try b.sliceLastDim(qkv, h, h * 2), l, nh, h / nh, theta);
        const v = try b.sliceLastDim(qkv, h * 2, h * 3);
        const attn = try attention(b, &result, q, k, v, if (global) result.inputs.encoder_bias else local_bias, l, nh, h / nh, 0);
        x = try b.add(x, try linear(b, attn, try std.fmt.bufPrint(&names, "{s}.attn.Wo", .{prefix}), n, h, h, false));
        const normed = try norm(b, x, try std.fmt.bufPrint(&names, "{s}.mlp_norm", .{prefix}), h, cfg.layer_norm_eps, false);
        const up = try linear(b, normed, try std.fmt.bufPrint(&names, "{s}.mlp.Wi", .{prefix}), n, h, cfg.intermediate_size * 2, false);
        const gate = try b.geluExact(try b.sliceLastDim(up, 0, cfg.intermediate_size));
        const product = try b.mul(gate, try b.sliceLastDim(up, cfg.intermediate_size, cfg.intermediate_size * 2));
        x = try b.add(x, try linear(b, product, try std.fmt.bufPrint(&names, "{s}.mlp.Wo", .{prefix}), n, cfg.intermediate_size, h, false));
        try result.trace(b.graph.allocator, prefix, x);
    }
    x = try norm(b, x, "encoder.final_norm", h, cfg.layer_norm_eps, false);
    try result.trace(b.graph.allocator, "encoder.final_norm", x);
    const types = try param(b, "type_emb", "weight", &.{ 3, h });
    x = try b.add(x, try b.gather(types, result.inputs.kinds, Shape.init(.f32, &.{ n, h })));
    for (0..lc.head_layers) |layer| {
        var buffer: [128]u8 = undefined;
        var names: [256]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&buffer, "head.layers.{d}", .{layer});
        const n1 = try norm(b, x, try std.fmt.bufPrint(&names, "{s}.norm1", .{prefix}), h, 1e-5, true);
        const w = try param(b, prefix, "self_attn.in_proj_weight", &.{ h * 3, h });
        const bias = try param(b, prefix, "self_attn.in_proj_bias", &.{h * 3});
        const qkv_fused = try b.linear(n1, w, bias, n, h, h * 3);
        const qkv = b.graph.node(qkv_fused).vjp_alternate;
        const attn = try attention(b, &result, try b.sliceLastDim(qkv, 0, h), try b.sliceLastDim(qkv, h, h * 2), try b.sliceLastDim(qkv, h * 2, h * 3), result.inputs.head_bias, l, hh, 64, head_dropout);
        const proj = try linear(b, attn, try std.fmt.bufPrint(&names, "{s}.self_attn.out_proj", .{prefix}), n, h, h, true);
        x = try b.add(x, try drop(b, &result, proj, head_dropout));
        const n2 = try norm(b, x, try std.fmt.bufPrint(&names, "{s}.norm2", .{prefix}), h, 1e-5, true);
        const up = try linear(b, n2, try std.fmt.bufPrint(&names, "{s}.linear1", .{prefix}), n, h, h * 4, true);
        try result.trace(b.graph.allocator, try std.fmt.bufPrint(&names, "{s}.linear1", .{prefix}), up);
        const activated = try drop(b, &result, try relu(b, up), head_dropout);
        const down = try linear(b, activated, try std.fmt.bufPrint(&names, "{s}.linear2", .{prefix}), n, h * 4, h, true);
        x = try b.add(x, try drop(b, &result, down, head_dropout));
        try result.trace(b.graph.allocator, prefix, x);
    }
    const m = try b.gather(x, result.inputs.markers, Shape.init(.f32, &.{ l.batch * l.options, h }));
    const normalized = try norm(b, m, "scorer.0", h, 1e-5, true);
    const up = try linear(b, normalized, "scorer.1", l.batch * l.options, h, h, true);
    const logits = try linear(b, try b.geluExact(up), "scorer.3", l.batch * l.options, h, 1, true);
    result.logits = try b.reshape(logits, Shape.init(.f32, &.{ l.batch, l.options }));
    return result;
}

test "laya training graph retains upstream parameters and all head dropout sites" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = B.init(&graph);
    const cfg = modern.Config{ .laya = .{ .head_layers = 1 }, .vocab_size = 64, .hidden_size = 64, .num_hidden_layers = 2, .num_attention_heads = 2, .intermediate_size = 96, .checkpoint_layout = .huggingface_fused_qkv_no_bias, .rope_interleaved = false };
    var built = try build(&b, cfg, .{ .batch = 2, .sequence = 8, .options = 3 }, 0.1);
    defer built.deinit(a);
    try std.testing.expectEqual(@as(usize, 4), built.dropouts.items.len);
    try std.testing.expectEqual(@as(i64, 6), graph.node(built.logits).output_shape.numElements().?);
    const seed = try b.parameter("__seed", graph.node(built.logits).output_shape);
    var wrt: std.ArrayListUnmanaged(Id) = .empty;
    defer wrt.deinit(a);
    for (graph.parameters.items) |id| {
        const name = graph.parameterName(graph.node(id));
        if (!std.mem.startsWith(u8, name, "__")) try wrt.append(a, id);
    }
    var grads = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = built.logits, .cotangent = seed }}, wrt.items, .{ .require_all_gradients = true });
    defer grads.deinit();
    try std.testing.expectEqual(wrt.items.len, grads.param_grads.len);
}
