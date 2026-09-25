// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable ModernBERT encoder in the Hugging Face checkpoint layout
//! (`encoder.layers.N.attn.Wqkv`, fused `mlp.Wi`, bias-free norms), shared by
//! the training graphs that put task heads on a ModernBERT trunk. Parameters
//! keep upstream safetensors names.
//!
//! Attention is expressed with primitives and materialized per head; masks
//! and split-half RoPE tables are runtime inputs so callers can pad, pack, or
//! restart positions (tree-packed rows) without rebuilding the graph. The
//! caller owns input declaration and naming, and builds the host values.
const std = @import("std");
const ml = @import("ml").graph;
const modern = @import("../architectures/modern_bert.zig");
const B = ml.Builder;
const Id = ml.NodeId;
const Shape = ml.Shape;

/// `batch` rows of `sequence` physical tokens each.
pub const Layout = struct { batch: u32, sequence: u32 };

/// Runtime inputs the encoder reads. The caller declares them, so it controls
/// their names and the order in which graph nodes are created.
pub const Inputs = struct {
    ids: Id, // [N] i32 token ids
    encoder_bias: Id, // [B*heads,S,S] additive mask for global layers
    local_bias: Id, // encoder_bias plus the logical sliding window, for local layers
    rope: [2][2]Id, // [global, local][cos, sin], each [N*heads, head_dim/2]
};

pub const Dropout = struct { node: Id, probability: f32 };
pub const Trace = struct { name: []const u8, node: Id };

/// Dropout masks (runtime inputs named `{prefix}_dropout_{i}`) and named
/// activations collected while building a graph.
pub const Sites = struct {
    prefix: []const u8,
    dropouts: std.ArrayListUnmanaged(Dropout) = .empty,
    traces: std.ArrayListUnmanaged(Trace) = .empty,

    pub fn deinit(self: *Sites, a: std.mem.Allocator) void {
        self.dropouts.deinit(a);
        for (self.traces.items) |entry| a.free(entry.name);
        self.traces.deinit(a);
    }

    pub fn trace(self: *Sites, a: std.mem.Allocator, name: []const u8, node: Id) !void {
        const owned = try a.dupe(u8, name);
        errdefer a.free(owned);
        try self.traces.append(a, .{ .name = owned, .node = node });
    }

    /// Inverted dropout as a multiply by a runtime mask; identity at 0.
    pub fn drop(self: *Sites, b: *B, x: Id, probability: f32) !Id {
        if (probability == 0) return x;
        var name: [96]u8 = undefined;
        const mask = try b.parameter(try std.fmt.bufPrint(&name, "{s}_dropout_{d}", .{ self.prefix, self.dropouts.items.len }), b.graph.node(x).output_shape);
        try self.dropouts.append(b.graph.allocator, .{ .node = mask, .probability = probability });
        return b.mul(x, mask);
    }
};

pub fn param(b: *B, prefix: []const u8, suffix: []const u8, dims: []const i64) !Id {
    var name: [256]u8 = undefined;
    return b.parameter(try std.fmt.bufPrint(&name, "{s}.{s}", .{ prefix, suffix }), Shape.init(.f32, dims));
}

pub fn linear(b: *B, x: Id, prefix: []const u8, rows: u32, input: u32, output: u32, bias: bool) !Id {
    const w = try param(b, prefix, "weight", &.{ output, input });
    const fused = if (bias) try b.linear(x, w, try param(b, prefix, "bias", &.{output}), rows, input, output) else try b.linearNoBias(x, w, rows, input, output);
    // Execute the same arithmetic in the forward and backward replay. Generic
    // inference slots cache weight snapshots and are unsuitable for mutable
    // training parameters (including device-only normalization tensors).
    return b.graph.node(fused).vjp_alternate;
}

pub fn norm(b: *B, x: Id, prefix: []const u8, dim: u32, eps: f32, bias: bool) !Id {
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

/// Scaled dot-product attention over `[N, h*d]` projections with an additive
/// `[B*h,S,S]` bias and optional dropout on the probabilities.
pub fn attention(b: *B, sites: *Sites, q: Id, k: Id, v: Id, bias: Id, l: Layout, h: u32, d: u32, dropout: f32) !Id {
    const scores = try b.matmul3DTransB(try heads(b, q, l, h, d), try heads(b, k, l, h, d));
    const scaled = try b.mul(scores, try b.scalarConst(.f32, 1 / @sqrt(@as(f32, @floatFromInt(d)))));
    const probs = try sites.drop(b, try b.softmax(try b.add(scaled, bias)), dropout);
    const ctx = try b.matmul3D(probs, try heads(b, v, l, h, d));
    const shaped = try b.reshape(ctx, Shape.init(.f32, &.{ l.batch, h, l.sequence, d }));
    return b.reshape(try b.transpose(shaped, &.{ 0, 2, 1, 3 }), Shape.init(.f32, &.{ l.batch * l.sequence, h * d }));
}

/// Split-half RoPE tables for `positions` (one logical position per token),
/// laid out `[tokens * heads, d/2]` to match `rope`.
pub fn ropeTables(a: std.mem.Allocator, positions: []const i64, h: u32, d: u32, theta: f32) ![2][]f32 {
    const cosine = try a.alloc(f32, positions.len * h * (d / 2));
    errdefer a.free(cosine);
    const sine = try a.alloc(f32, cosine.len);
    for (0..positions.len * h) |row| for (0..d / 2) |i| {
        const pos = positions[row / h];
        const angle = @as(f32, @floatFromInt(pos)) / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * i)) / @as(f32, @floatFromInt(d)));
        cosine[row * (d / 2) + i] = @cos(angle);
        sine[row * (d / 2) + i] = @sin(angle);
    };
    return .{ cosine, sine };
}

// Split-half RoPE expressed as primitives, preserving the physical token/head
// layout and an exact VJP without depending on inference-only fused kernels.
// Tables are runtime inputs because tree-packed rows restart positions.
fn rope(b: *B, x: Id, tables: [2]Id, l: Layout, h: u32, d: u32) !Id {
    const n = l.batch * l.sequence * h;
    const c = tables[0];
    const s = tables[1];
    const reshaped = try b.reshape(x, Shape.init(.f32, &.{ n, d }));
    const left = try b.sliceLastDim(reshaped, 0, d / 2);
    const right = try b.sliceLastDim(reshaped, d / 2, d);
    const first = try b.sub(try b.mul(left, c), try b.mul(right, s));
    const second = try b.add(try b.mul(left, s), try b.mul(right, c));
    return b.reshape(try b.concat(first, second, 1), Shape.init(.f32, &.{ l.batch * l.sequence, h * d }));
}

/// Checks the configuration and layout the encoder supports, and bounds the
/// materialized attention before any constant is allocated.
pub fn validate(cfg: modern.Config, l: Layout) !void {
    if (cfg.checkpoint_layout != .huggingface_fused_qkv_no_bias or cfg.rope_interleaved or
        cfg.hidden_size < 64 or cfg.hidden_size > 4096 or cfg.hidden_size % 64 != 0 or cfg.num_attention_heads == 0 or
        cfg.hidden_size % cfg.num_attention_heads != 0 or (cfg.hidden_size / cfg.num_attention_heads) % 2 != 0 or
        cfg.global_attn_every_n_layers == 0 or cfg.num_hidden_layers == 0 or cfg.num_hidden_layers > 128 or
        cfg.vocab_size == 0 or cfg.vocab_size > 1024 * 1024 or cfg.intermediate_size == 0 or cfg.intermediate_size > 32768 or
        !std.math.isFinite(cfg.global_rope_theta) or cfg.global_rope_theta <= 0 or !std.math.isFinite(cfg.local_rope_theta) or cfg.local_rope_theta <= 0 or
        !std.math.isFinite(cfg.layer_norm_eps) or cfg.layer_norm_eps <= 0 or
        l.batch == 0 or l.batch > 128 or l.sequence == 0)
        return error.InvalidModernBertTrainingLayout;
    if (@as(u64, l.batch) * l.sequence * l.sequence * cfg.num_attention_heads > 64 * 1024 * 1024)
        return error.ModernBertTrainingAttentionLimitExceeded;
}

/// Embeddings, `num_hidden_layers` pre-norm layers (global attention every
/// `global_attn_every_n_layers`, local otherwise), and the final norm.
/// Returns the `[batch*sequence, hidden]` hidden states. Traces the embedding
/// norm, every layer output, and the final norm.
pub fn encoder(b: *B, sites: *Sites, cfg: modern.Config, l: Layout, in: Inputs) !Id {
    try validate(cfg, l);
    const a = b.graph.allocator;
    const h = cfg.hidden_size;
    const n = l.batch * l.sequence;
    const nh = cfg.num_attention_heads;
    const embedding = try param(b, "encoder.embeddings.tok_embeddings", "weight", &.{ cfg.vocab_size, h });
    var x = try b.gather(embedding, in.ids, Shape.init(.f32, &.{ n, h }));
    x = try norm(b, x, "encoder.embeddings.norm", h, cfg.layer_norm_eps, false);
    try sites.trace(a, "encoder.embeddings.norm", x);
    for (0..cfg.num_hidden_layers) |layer| {
        var buffer: [128]u8 = undefined;
        var names: [256]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&buffer, "encoder.layers.{d}", .{layer});
        const normalized = if (layer == 0) x else try norm(b, x, try std.fmt.bufPrint(&names, "{s}.attn_norm", .{prefix}), h, cfg.layer_norm_eps, false);
        const qkv = try linear(b, normalized, try std.fmt.bufPrint(&names, "{s}.attn.Wqkv", .{prefix}), n, h, h * 3, false);
        const global = layer % cfg.global_attn_every_n_layers == 0;
        const tables = in.rope[if (global) 0 else 1];
        const q = try rope(b, try b.sliceLastDim(qkv, 0, h), tables, l, nh, h / nh);
        const k = try rope(b, try b.sliceLastDim(qkv, h, h * 2), tables, l, nh, h / nh);
        const v = try b.sliceLastDim(qkv, h * 2, h * 3);
        const attn = try attention(b, sites, q, k, v, if (global) in.encoder_bias else in.local_bias, l, nh, h / nh, 0);
        x = try b.add(x, try linear(b, attn, try std.fmt.bufPrint(&names, "{s}.attn.Wo", .{prefix}), n, h, h, false));
        const normed = try norm(b, x, try std.fmt.bufPrint(&names, "{s}.mlp_norm", .{prefix}), h, cfg.layer_norm_eps, false);
        const up = try linear(b, normed, try std.fmt.bufPrint(&names, "{s}.mlp.Wi", .{prefix}), n, h, cfg.intermediate_size * 2, false);
        const gate = try b.geluExact(try b.sliceLastDim(up, 0, cfg.intermediate_size));
        const product = try b.mul(gate, try b.sliceLastDim(up, cfg.intermediate_size, cfg.intermediate_size * 2));
        x = try b.add(x, try linear(b, product, try std.fmt.bufPrint(&names, "{s}.mlp.Wo", .{prefix}), n, cfg.intermediate_size, h, false));
        try sites.trace(a, prefix, x);
    }
    x = try norm(b, x, "encoder.final_norm", h, cfg.layer_norm_eps, false);
    try sites.trace(a, "encoder.final_norm", x);
    return x;
}

const test_config = modern.Config{ .vocab_size = 64, .hidden_size = 64, .num_hidden_layers = 3, .num_attention_heads = 2, .intermediate_size = 96, .global_attn_every_n_layers = 3, .checkpoint_layout = .huggingface_fused_qkv_no_bias, .rope_interleaved = false };

fn testInputs(b: *B, cfg: modern.Config, l: Layout) !Inputs {
    const n = l.batch * l.sequence;
    const nh = cfg.num_attention_heads;
    const bias = Shape.init(.f32, &.{ l.batch * nh, l.sequence, l.sequence });
    const table = Shape.init(.f32, &.{ n * nh, (cfg.hidden_size / nh) / 2 });
    return .{
        .ids = try b.parameter("__t_ids", Shape.init(.i32, &.{n})),
        .encoder_bias = try b.parameter("__t_encoder_bias", bias),
        .local_bias = try b.parameter("__t_local_bias", bias),
        .rope = .{
            .{ try b.parameter("__t_rope_global_cos", table), try b.parameter("__t_rope_global_sin", table) },
            .{ try b.parameter("__t_rope_local_cos", table), try b.parameter("__t_rope_local_sin", table) },
        },
    };
}

test "modern bert trunk keeps Hugging Face parameter names and differentiates every weight" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = B.init(&graph);
    const l = Layout{ .batch = 2, .sequence = 8 };
    var sites = Sites{ .prefix = "__t" };
    defer sites.deinit(a);
    const hidden = try encoder(&b, &sites, test_config, l, try testInputs(&b, test_config, l));
    try std.testing.expectEqual(@as(i64, 2 * 8 * 64), graph.node(hidden).output_shape.numElements().?);
    // Embedding norm, three layers, final norm; the encoder adds no dropout.
    try std.testing.expectEqual(@as(usize, 5), sites.traces.items.len);
    try std.testing.expectEqual(@as(usize, 0), sites.dropouts.items.len);

    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer names.deinit(a);
    var wrt: std.ArrayListUnmanaged(Id) = .empty;
    defer wrt.deinit(a);
    for (graph.parameters.items) |id| {
        const name = graph.parameterName(graph.node(id));
        if (std.mem.startsWith(u8, name, "__")) continue;
        try names.append(a, name);
        try wrt.append(a, id);
    }
    const expected = [_][]const u8{
        "encoder.embeddings.tok_embeddings.weight", "encoder.embeddings.norm.weight",
        "encoder.layers.0.attn.Wqkv.weight",        "encoder.layers.0.attn.Wo.weight",
        "encoder.layers.0.mlp_norm.weight",         "encoder.layers.0.mlp.Wi.weight",
        "encoder.layers.0.mlp.Wo.weight",           "encoder.layers.1.attn_norm.weight",
        "encoder.layers.1.attn.Wqkv.weight",        "encoder.layers.1.attn.Wo.weight",
        "encoder.layers.1.mlp_norm.weight",         "encoder.layers.1.mlp.Wi.weight",
        "encoder.layers.1.mlp.Wo.weight",           "encoder.layers.2.attn_norm.weight",
        "encoder.layers.2.attn.Wqkv.weight",        "encoder.layers.2.attn.Wo.weight",
        "encoder.layers.2.mlp_norm.weight",         "encoder.layers.2.mlp.Wi.weight",
        "encoder.layers.2.mlp.Wo.weight",           "encoder.final_norm.weight",
    };
    try std.testing.expectEqual(expected.len, names.items.len);
    for (expected, names.items) |want, got| try std.testing.expectEqualStrings(want, got);

    const seed = try b.parameter("__seed", graph.node(hidden).output_shape);
    var grads = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = hidden, .cotangent = seed }}, wrt.items, .{ .require_all_gradients = true });
    defer grads.deinit();
    try std.testing.expectEqual(wrt.items.len, grads.param_grads.len);
}

test "modern bert trunk rejects unsupported layouts and oversized attention" {
    var bad = test_config;
    bad.checkpoint_layout = .separate_qkv_with_bias;
    try std.testing.expectError(error.InvalidModernBertTrainingLayout, validate(bad, .{ .batch = 1, .sequence = 8 }));
    try std.testing.expectError(error.InvalidModernBertTrainingLayout, validate(test_config, .{ .batch = 1, .sequence = 0 }));
    // 1 * 8192^2 * 2 heads = 128Mi > 64Mi.
    try std.testing.expectError(error.ModernBertTrainingAttentionLimitExceeded, validate(test_config, .{ .batch = 1, .sequence = 8192 }));
}
