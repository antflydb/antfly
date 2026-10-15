// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable F32 reference graph for EmbeddingGemma 2.
//!
//! This deliberately uses ordinary graph primitives. Resident inference
//! kernels are not treated as differentiable. A CUDA trainer may lower this
//! graph or implement the contracts in `embedding_gemma2_training.zig`.

const std = @import("std");
const ml = @import("ml");
const training = @import("embedding_gemma2_training.zig");
const model = @import("../models/embedding_gemma2.zig");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const node_mod = ml.graph.node;
const null_node = ml.graph.null_node;

pub const LayerKind = enum { local, global };

pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32 = 512,
    layer_count: u32 = 24,
    intermediate_size: u32 = 2048,
    embedding_dim: u32 = 768,
    query_heads: u32 = 4,
    local_kv_heads: u32 = 2,
    global_kv_heads: u32 = 1,
    local_head_dim: u32 = 256,
    global_head_dim: u32 = 512,
    local_radius: u32 = 512,
    global_every: u32 = 6,
    local_rope_theta: f32 = 10_000,
    global_rope_theta: f32 = 1_000_000,
    rms_epsilon: f32 = 1e-6,
    max_sequence: u32 = training.training_sequence_cap,

    pub fn validate(self: Config) !void {
        if (self.vocab_size == 0 or self.hidden_size == 0 or self.layer_count == 0 or
            self.intermediate_size == 0 or self.embedding_dim == 0 or self.query_heads == 0 or
            self.local_kv_heads == 0 or self.global_kv_heads == 0 or self.local_head_dim == 0 or
            self.global_head_dim == 0 or self.local_radius == 0 or self.global_every == 0 or
            self.max_sequence == 0 or self.max_sequence > training.training_sequence_cap or
            self.query_heads % self.local_kv_heads != 0 or
            self.query_heads % self.global_kv_heads != 0 or
            self.local_head_dim % 2 != 0 or self.global_head_dim % 2 != 0 or
            !std.math.isFinite(self.rms_epsilon) or self.rms_epsilon <= 0 or
            !std.math.isFinite(self.local_rope_theta) or self.local_rope_theta <= 0 or
            !std.math.isFinite(self.global_rope_theta) or self.global_rope_theta <= 0)
            return error.InvalidEmbeddingGemma2GraphConfig;
    }

    pub fn layerKind(self: Config, layer: u32) LayerKind {
        return if ((layer + 1) % self.global_every == 0) .global else .local;
    }

    pub fn headDim(self: Config, layer: u32) u32 {
        return if (self.layerKind(layer) == .global) self.global_head_dim else self.local_head_dim;
    }

    pub fn kvHeads(self: Config, layer: u32) u32 {
        return if (self.layerKind(layer) == .global) self.global_kv_heads else self.local_kv_heads;
    }

    pub fn ropeTheta(self: Config, layer: u32) f32 {
        return if (self.layerKind(layer) == .global) self.global_rope_theta else self.local_rope_theta;
    }

    pub fn isOfficial(self: Config) bool {
        return self.hidden_size == 512 and self.layer_count == 24 and self.intermediate_size == 2048 and
            self.embedding_dim == 768 and self.query_heads == 4 and self.local_kv_heads == 2 and
            self.global_kv_heads == 1 and self.local_head_dim == 256 and self.global_head_dim == 512 and
            self.local_radius == 512 and self.global_every == 6;
    }

    pub fn fromModel(source: model.Config) !Config {
        try source.validate();
        if (!source.isOfficialTextTopology()) return error.UnsupportedEmbeddingGemma2TrainingTopology;
        return .{
            .vocab_size = source.vocab_size,
            .hidden_size = source.hidden_size,
            .layer_count = source.num_hidden_layers,
            .intermediate_size = source.intermediate_size,
            .embedding_dim = source.embedding_dim,
            .query_heads = source.num_attention_heads,
            .local_kv_heads = 2,
            .global_kv_heads = 1,
            .local_head_dim = 256,
            .global_head_dim = 512,
            .local_radius = source.sliding_window,
            .global_every = 6,
            .local_rope_theta = source.rope.sliding_theta,
            .global_rope_theta = source.rope.full_theta,
            .rms_epsilon = source.rms_norm_eps,
            .max_sequence = training.training_sequence_cap,
        };
    }
};

pub const Inputs = struct {
    input_ids: NodeId,
    attention_mask: NodeId,
    input_embeddings: NodeId = null_node,
};

pub const Built = struct {
    token_embeddings: NodeId,
    ple: NodeId,
    final_hidden: NodeId,
    projected_tokens: NodeId,
    embedding: NodeId,
};

pub fn buildForward(
    bld: *Builder,
    config: Config,
    batch: u32,
    sequence: u32,
    requested_dim: u32,
    inputs: Inputs,
) !Built {
    try config.validate();
    if (batch == 0 or sequence == 0 or sequence > config.max_sequence or
        requested_dim == 0 or requested_dim > config.embedding_dim)
        return error.InvalidEmbeddingGemma2GraphShape;

    const b: i64 = @intCast(batch);
    const s: i64 = @intCast(sequence);
    const h: i64 = @intCast(config.hidden_size);
    const rows_i = b * s;
    const rows: u32 = std.math.mul(u32, batch, sequence) catch return error.InvalidEmbeddingGemma2GraphShape;
    const hidden_shape = Shape.init(.f32, &.{ b, s, h });
    if (!bld.graph.node(inputs.input_ids).output_shape.eq(Shape.init(.f32, &.{ b, s })) or
        !bld.graph.node(inputs.attention_mask).output_shape.eq(Shape.init(.f32, &.{ b, s })) or
        (inputs.input_embeddings != null_node and !bld.graph.node(inputs.input_embeddings).output_shape.eq(hidden_shape)))
        return error.InvalidEmbeddingGemma2GraphShape;

    const token_embeddings = if (inputs.input_embeddings != null_node)
        inputs.input_embeddings
    else blk: {
        const ids = try bld.reshape(inputs.input_ids, Shape.init(.f32, &.{rows_i}));
        const weight = try bld.parameter("language_model.embed_tokens.weight", Shape.init(.f32, &.{ @as(i64, @intCast(config.vocab_size)), h }));
        const flat = try bld.embeddingLookup(weight, ids, rows, config.hidden_size);
        // The serving BF16 path rounds sqrt(512) to 22.625. The F32 training
        // graph retains the mathematical scale; parity tests cover the policy.
        const scale = try bld.scalarConst(.f32, @sqrt(@as(f32, @floatFromInt(config.hidden_size))));
        break :blk try bld.reshape(try bld.mul(flat, scale), hidden_shape);
    };

    const hidden_initial_flat = try bld.reshape(token_embeddings, Shape.init(.f32, &.{ rows_i, h }));
    const ple_width = std.math.mul(u32, config.layer_count, config.hidden_size) catch
        return error.InvalidEmbeddingGemma2GraphShape;
    const ple_weight = try bld.parameter(
        "language_model.ple.per_layer_model_projection.weight",
        Shape.init(.f32, &.{ @as(i64, @intCast(ple_width)), h }),
    );
    const ple_projected = try bld.linearNoBias(hidden_initial_flat, ple_weight, rows, config.hidden_size, ple_width);
    const ple_scaled = try bld.mul(ple_projected, try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(config.hidden_size)))));
    const ple_norm_weight = try bld.parameter("language_model.ple.per_layer_projection_norm.weight", Shape.init(.f32, &.{h}));
    const ple_chunks = try bld.reshape(ple_scaled, Shape.init(.f32, &.{ rows_i * @as(i64, @intCast(config.layer_count)), h }));
    const ple_normed_chunks = try bld.rmsNorm(ple_chunks, ple_norm_weight, config.hidden_size, config.rms_epsilon);
    const ple = try bld.reshape(ple_normed_chunks, Shape.init(.f32, &.{ rows_i, @as(i64, @intCast(ple_width)) }));

    const masks = try buildAttentionMasks(bld, inputs.attention_mask, batch, sequence, config.local_radius);
    var hidden = token_embeddings;
    for (0..config.layer_count) |layer_usize| {
        const layer: u32 = @intCast(layer_usize);
        hidden = try buildLayer(bld, config, hidden, ple, masks, batch, sequence, layer);
    }

    const final_norm = try parameter(bld, "language_model.norm.weight", Shape.init(.f32, &.{h}));
    const final_flat = try bld.rmsNorm(try bld.reshape(hidden, Shape.init(.f32, &.{ rows_i, h })), final_norm, config.hidden_size, config.rms_epsilon);
    const final_hidden = try bld.reshape(final_flat, hidden_shape);
    const projection = try parameter(bld, "language_model.embedding_projection.weight", Shape.init(.f32, &.{ @as(i64, @intCast(config.embedding_dim)), h }));
    const projected_flat = try bld.linearNoBias(final_flat, projection, rows, config.hidden_size, config.embedding_dim);
    const projected_tokens = try bld.reshape(projected_flat, Shape.init(.f32, &.{ b, s, @as(i64, @intCast(config.embedding_dim)) }));
    const embedding = try maskedMeanMrlL2(bld, projected_tokens, inputs.attention_mask, batch, sequence, config.embedding_dim, requested_dim);

    return .{
        .token_embeddings = token_embeddings,
        .ple = ple,
        .final_hidden = final_hidden,
        .projected_tokens = projected_tokens,
        .embedding = embedding,
    };
}

const AttentionMasks = struct { global: NodeId, local: NodeId };

fn buildAttentionMasks(bld: *Builder, attention_mask: NodeId, batch: u32, sequence: u32, radius: u32) !AttentionMasks {
    const b: i64 = @intCast(batch);
    const s: i64 = @intCast(sequence);
    const mask4 = try bld.reshape(attention_mask, Shape.init(.f32, &.{ b, 1, 1, s }));
    const target = Shape.init(.f32, &.{ b, 1, s, s });
    const keys = try broadcast(bld, mask4, target, &.{ 0, 1, 2, 3 });
    const invalid = try bld.mul(
        try bld.sub(try bld.scalarConst(.f32, 1), keys),
        try bld.scalarConst(.f32, -1.0e9),
    );

    const count: usize = std.math.mul(usize, sequence, sequence) catch return error.InvalidEmbeddingGemma2GraphShape;
    const data = try bld.graph.allocator.alloc(f32, count);
    defer bld.graph.allocator.free(data);
    for (0..sequence) |query| for (0..sequence) |key| {
        const distance = if (query > key) query - key else key - query;
        data[query * sequence + key] = if (distance <= radius) 0 else -1.0e9;
    };
    const local1 = try bld.tensorConst(data, Shape.init(.f32, &.{ 1, 1, s, s }));
    const local = try bld.add(invalid, try broadcast(bld, local1, target, &.{ 0, 1, 2, 3 }));
    return .{ .global = invalid, .local = local };
}

fn buildLayer(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    ple: NodeId,
    masks: AttentionMasks,
    batch: u32,
    sequence: u32,
    layer: u32,
) !NodeId {
    const b: i64 = @intCast(batch);
    const s: i64 = @intCast(sequence);
    const h: i64 = @intCast(config.hidden_size);
    const rows_i = b * s;
    const rows: u32 = @intCast(rows_i);

    const input_norm = try layerParameter(bld, layer, "input_layernorm.weight", Shape.init(.f32, &.{h}));
    const hidden_flat = try bld.reshape(hidden_in, Shape.init(.f32, &.{ rows_i, h }));
    const attention_input = try bld.rmsNorm(hidden_flat, input_norm, config.hidden_size, config.rms_epsilon);
    const attention = try selfAttention(bld, config, attention_input, masks, batch, sequence, layer);
    const post_attn = try layerParameter(bld, layer, "post_attention_layernorm.weight", Shape.init(.f32, &.{h}));
    const attention_normed = try bld.rmsNorm(attention, post_attn, config.hidden_size, config.rms_epsilon);
    const after_attention = try bld.add(hidden_flat, attention_normed);

    const pre_ffn = try layerParameter(bld, layer, "pre_feedforward_layernorm.weight", Shape.init(.f32, &.{h}));
    const ffn_input = try bld.rmsNorm(after_attention, pre_ffn, config.hidden_size, config.rms_epsilon);
    const gate_weight = try layerParameter(bld, layer, "mlp.gate_proj.weight", Shape.init(.f32, &.{ @as(i64, @intCast(config.intermediate_size)), h }));
    const up_weight = try layerParameter(bld, layer, "mlp.up_proj.weight", Shape.init(.f32, &.{ @as(i64, @intCast(config.intermediate_size)), h }));
    const down_weight = try layerParameter(bld, layer, "mlp.down_proj.weight", Shape.init(.f32, &.{ h, @as(i64, @intCast(config.intermediate_size)) }));
    const gate = try bld.gelu(try bld.linearNoBias(ffn_input, gate_weight, rows, config.hidden_size, config.intermediate_size));
    const up = try bld.linearNoBias(ffn_input, up_weight, rows, config.hidden_size, config.intermediate_size);
    const down = try bld.linearNoBias(try bld.mul(gate, up), down_weight, rows, config.intermediate_size, config.hidden_size);
    const post_ffn = try layerParameter(bld, layer, "post_feedforward_layernorm.weight", Shape.init(.f32, &.{h}));
    const after_ffn = try bld.add(after_attention, try bld.rmsNorm(down, post_ffn, config.hidden_size, config.rms_epsilon));

    const start: i64 = @as(i64, @intCast(layer)) * h;
    const layer_ple = try bld.sliceLastDim(ple, start, start + h);
    const ple_gate_weight = try layerParameter(bld, layer, "ple_block.per_layer_input_gate.weight", Shape.init(.f32, &.{ h, h }));
    const ple_projection_weight = try layerParameter(bld, layer, "ple_block.per_layer_projection.weight", Shape.init(.f32, &.{ h, h }));
    const ple_gate = try bld.gelu(try bld.linearNoBias(after_ffn, ple_gate_weight, rows, config.hidden_size, config.hidden_size));
    const ple_projected = try bld.linearNoBias(try bld.mul(ple_gate, layer_ple), ple_projection_weight, rows, config.hidden_size, config.hidden_size);
    const ple_post_norm = try layerParameter(bld, layer, "ple_block.post_per_layer_input_norm.weight", Shape.init(.f32, &.{h}));
    const with_ple = try bld.add(after_ffn, try bld.rmsNorm(ple_projected, ple_post_norm, config.hidden_size, config.rms_epsilon));
    const scalar = try layerParameter(bld, layer, "layer_scalar", Shape.init(.f32, &.{1}));
    return bld.reshape(try bld.mul(with_ple, scalar), Shape.init(.f32, &.{ b, s, h }));
}

fn selfAttention(
    bld: *Builder,
    config: Config,
    input_flat: NodeId,
    masks: AttentionMasks,
    batch: u32,
    sequence: u32,
    layer: u32,
) !NodeId {
    const b: i64 = @intCast(batch);
    const s: i64 = @intCast(sequence);
    const rows: u32 = std.math.mul(u32, batch, sequence) catch return error.InvalidEmbeddingGemma2GraphShape;
    const rows_i: i64 = @intCast(rows);
    const h: i64 = @intCast(config.hidden_size);
    const heads: i64 = @intCast(config.query_heads);
    const kv_heads_u = config.kvHeads(layer);
    const kv_heads: i64 = @intCast(kv_heads_u);
    const head_dim_u = config.headDim(layer);
    const head_dim: i64 = @intCast(head_dim_u);
    const query_width = config.query_heads * head_dim_u;
    const kv_width = kv_heads_u * head_dim_u;

    const q_weight = try layerParameter(bld, layer, "self_attn.q_proj.weight", Shape.init(.f32, &.{ @as(i64, @intCast(query_width)), h }));
    const k_weight = try layerParameter(bld, layer, "self_attn.k_proj.weight", Shape.init(.f32, &.{ @as(i64, @intCast(kv_width)), h }));
    const v_weight = try layerParameter(bld, layer, "self_attn.v_proj.weight", Shape.init(.f32, &.{ @as(i64, @intCast(kv_width)), h }));
    const q_flat = try bld.linearNoBias(input_flat, q_weight, rows, config.hidden_size, query_width);
    const k_flat = try bld.linearNoBias(input_flat, k_weight, rows, config.hidden_size, kv_width);
    const v_flat = try bld.linearNoBias(input_flat, v_weight, rows, config.hidden_size, kv_width);
    const q_bhsd = try bld.transpose(try bld.reshape(q_flat, Shape.init(.f32, &.{ b, s, heads, head_dim })), &.{ 0, 2, 1, 3 });
    const k_bhsd = try bld.transpose(try bld.reshape(k_flat, Shape.init(.f32, &.{ b, s, kv_heads, head_dim })), &.{ 0, 2, 1, 3 });
    const v_bhsd = try bld.transpose(try bld.reshape(v_flat, Shape.init(.f32, &.{ b, s, kv_heads, head_dim })), &.{ 0, 2, 1, 3 });

    const q_norm_weight = try layerParameter(bld, layer, "self_attn.q_norm.weight", Shape.init(.f32, &.{head_dim}));
    const k_norm_weight = try layerParameter(bld, layer, "self_attn.k_norm.weight", Shape.init(.f32, &.{head_dim}));
    const q_normed = try headNorm(bld, q_bhsd, q_norm_weight, b, heads, s, head_dim, config.rms_epsilon);
    const k_normed = try headNorm(bld, k_bhsd, k_norm_weight, b, kv_heads, s, head_dim, config.rms_epsilon);
    const v_normed = try bareHeadNorm(bld, v_bhsd, b, kv_heads, s, head_dim, config.rms_epsilon);
    const rope = try buildRope(bld, sequence, head_dim_u, config.ropeTheta(layer));
    const q_rope = try applyRope(bld, q_normed, rope, b, heads, s, head_dim, sequence, head_dim_u, config.ropeTheta(layer));
    const k_rope = try applyRope(bld, k_normed, rope, b, kv_heads, s, head_dim, sequence, head_dim_u, config.ropeTheta(layer));

    const fanout = @divExact(config.query_heads, kv_heads_u);
    const k = try gqaFanOut(bld, k_rope, b, kv_heads, @intCast(fanout), s, head_dim);
    const v = try gqaFanOut(bld, v_normed, b, kv_heads, @intCast(fanout), s, head_dim);
    const scores = try dotQK(bld, q_rope, k, b, heads, s, head_dim);
    const mask1 = if (config.layerKind(layer) == .local) masks.local else masks.global;
    const mask = try broadcast(bld, mask1, Shape.init(.f32, &.{ b, heads, s, s }), &.{ 0, 1, 2, 3 });
    const probabilities = try bld.softmax(try bld.add(scores, mask)); // trained scale is exactly one
    const context = try dotPV(bld, probabilities, v, b, heads, s, head_dim);
    const context_flat = try bld.reshape(try bld.transpose(context, &.{ 0, 2, 1, 3 }), Shape.init(.f32, &.{ rows_i, @as(i64, @intCast(query_width)) }));
    const o_weight = try layerParameter(bld, layer, "self_attn.o_proj.weight", Shape.init(.f32, &.{ h, @as(i64, @intCast(query_width)) }));
    return bld.linearNoBias(context_flat, o_weight, rows, query_width, config.hidden_size);
}

const Rope = struct { cos: NodeId, sin: NodeId };

fn buildRope(bld: *Builder, sequence: u32, head_dim: u32, theta: f32) !Rope {
    const half = head_dim / 2;
    const count = std.math.mul(usize, sequence, half) catch return error.InvalidEmbeddingGemma2GraphShape;
    const cos = try bld.graph.allocator.alloc(f32, count);
    defer bld.graph.allocator.free(cos);
    const sin = try bld.graph.allocator.alloc(f32, count);
    defer bld.graph.allocator.free(sin);
    for (0..sequence) |position| for (0..half) |index| {
        const exponent = (2.0 * @as(f32, @floatFromInt(index))) / @as(f32, @floatFromInt(head_dim));
        const angle = @as(f32, @floatFromInt(position)) / std.math.pow(f32, theta, exponent);
        cos[position * half + index] = @cos(angle);
        sin[position * half + index] = @sin(angle);
    };
    const shape = Shape.init(.f32, &.{ @as(i64, @intCast(sequence)), @as(i64, @intCast(half)) });
    return .{ .cos = try bld.tensorConst(cos, shape), .sin = try bld.tensorConst(sin, shape) };
}

fn applyRope(bld: *Builder, input: NodeId, rope: Rope, b: i64, heads: i64, s: i64, d: i64, sequence: u32, head_dim: u32, theta: f32) !NodeId {
    const flat = try bld.reshape(input, Shape.init(.f32, &.{ b * heads * s, d }));
    const rotated = try bld.rope(flat, rope.cos, rope.sin, sequence, head_dim, head_dim, theta);
    return bld.reshape(rotated, Shape.init(.f32, &.{ b, heads, s, d }));
}

fn headNorm(bld: *Builder, input: NodeId, weight: NodeId, b: i64, heads: i64, s: i64, d: i64, eps: f32) !NodeId {
    const flat = try bld.reshape(input, Shape.init(.f32, &.{ b * heads * s, d }));
    return bld.reshape(try bld.rmsNorm(flat, weight, @intCast(d), eps), Shape.init(.f32, &.{ b, heads, s, d }));
}

fn bareHeadNorm(bld: *Builder, input: NodeId, b: i64, heads: i64, s: i64, d: i64, eps: f32) !NodeId {
    const ones = try bld.graph.allocator.alloc(f32, @intCast(d));
    defer bld.graph.allocator.free(ones);
    @memset(ones, 1);
    return headNorm(bld, input, try bld.tensorConst(ones, Shape.init(.f32, &.{d})), b, heads, s, d, eps);
}

fn gqaFanOut(bld: *Builder, input: NodeId, b: i64, kv: i64, repeat: i64, s: i64, d: i64) !NodeId {
    if (repeat == 1) return input;
    const inserted = try bld.reshape(input, Shape.init(.f32, &.{ b, kv, 1, s, d }));
    const expanded = try broadcast(bld, inserted, Shape.init(.f32, &.{ b, kv, repeat, s, d }), &.{ 0, 1, 2, 3, 4 });
    return bld.reshape(expanded, Shape.init(.f32, &.{ b, kv * repeat, s, d }));
}

fn dotQK(bld: *Builder, q: NodeId, k: NodeId, b: i64, heads: i64, s: i64, d: i64) !NodeId {
    _ = d;
    return bld.graph.addNode(.{ .op = .{ .dot_general = .{
        .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
        .rhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
        .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
        .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
        .num_contracting = 1,
        .num_batch = 2,
    } }, .output_shape = Shape.init(.f32, &.{ b, heads, s, s }), .inputs = .{ q, k, null_node, null_node }, .num_inputs = 2 });
}

fn dotPV(bld: *Builder, p: NodeId, v: NodeId, b: i64, heads: i64, s: i64, d: i64) !NodeId {
    return bld.graph.addNode(.{ .op = .{ .dot_general = .{
        .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
        .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
        .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
        .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
        .num_contracting = 1,
        .num_batch = 2,
    } }, .output_shape = Shape.init(.f32, &.{ b, heads, s, d }), .inputs = .{ p, v, null_node, null_node }, .num_inputs = 2 });
}

fn maskedMeanMrlL2(bld: *Builder, tokens: NodeId, attention_mask: NodeId, batch: u32, sequence: u32, full_dim: u32, requested_dim: u32) !NodeId {
    const b: i64 = @intCast(batch);
    const s: i64 = @intCast(sequence);
    const d: i64 = @intCast(full_dim);
    const mask3 = try bld.reshape(attention_mask, Shape.init(.f32, &.{ b, s, 1 }));
    const expanded = try broadcast(bld, mask3, Shape.init(.f32, &.{ b, s, d }), &.{ 0, 1, 2 });
    const sums = try bld.reduceSum(try bld.mul(tokens, expanded), &.{1});
    const counts = try bld.reduceSum(mask3, &.{1});
    const count_expanded = try broadcast(bld, counts, Shape.init(.f32, &.{ b, 1, d }), &.{ 0, 1, 2 });
    const mean = try bld.div(sums, count_expanded);
    const flat = try bld.reshape(mean, Shape.init(.f32, &.{ b, d }));
    const sliced = if (requested_dim == full_dim) flat else try bld.sliceLastDim(flat, 0, @intCast(requested_dim));
    const square_sum = try bld.reduceSum(try bld.mul(sliced, sliced), &.{1});
    const inv_norm = try bld.rsqrt(square_sum);
    const normalized = try bld.mul(sliced, try broadcast(bld, inv_norm, Shape.init(.f32, &.{ b, @as(i64, @intCast(requested_dim)) }), &.{ 0, 1 }));
    return normalized;
}

pub fn buildSymmetricInfoNce(bld: *Builder, queries: NodeId, documents: NodeId, pair_count: u32, dim: u32, temperature: f32) !NodeId {
    if (pair_count == 0 or dim == 0 or !std.math.isFinite(temperature) or temperature <= 0)
        return error.InvalidEmbeddingGemma2Loss;
    const targets_data = try bld.graph.allocator.alloc(f32, @as(usize, pair_count) * pair_count);
    defer bld.graph.allocator.free(targets_data);
    @memset(targets_data, 0);
    for (0..pair_count) |i| targets_data[i * pair_count + i] = 1;
    const targets = try bld.tensorConst(targets_data, Shape.init(.f32, &.{ @as(i64, @intCast(pair_count)), @as(i64, @intCast(pair_count)) }));
    return buildSymmetricInfoNceWithTargets(bld, queries, documents, targets, pair_count, dim, temperature);
}

pub fn buildSymmetricInfoNceWithTargets(bld: *Builder, queries: NodeId, documents: NodeId, targets: NodeId, pair_count: u32, dim: u32, temperature: f32) !NodeId {
    if (pair_count == 0 or dim == 0 or !std.math.isFinite(temperature) or temperature <= 0 or
        !bld.graph.node(queries).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(pair_count)), @as(i64, @intCast(dim)) })) or
        !bld.graph.node(documents).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(pair_count)), @as(i64, @intCast(dim)) })) or
        !bld.graph.node(targets).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(pair_count)), @as(i64, @intCast(pair_count)) })))
        return error.InvalidEmbeddingGemma2Loss;
    const logits = try bld.mul(try bld.matmul2DLayout(queries, documents, false, true), try bld.scalarConst(.f32, 1.0 / temperature));
    const q_loss = try bld.crossEntropyLoss(logits, targets);
    const d_loss = try bld.crossEntropyLoss(try bld.transpose(logits, &.{ 1, 0 }), targets);
    return bld.mul(try bld.add(q_loss, d_loss), try bld.scalarConst(.f32, 0.5));
}

/// Production retrieval objective: every query ranks the complete document
/// candidate pool. Extra document rows are hard negatives and receive no
/// positive target column of their own.
pub fn buildAsymmetricInfoNceWithTargets(
    bld: *Builder,
    queries: NodeId,
    documents: NodeId,
    targets: NodeId,
    query_count: u32,
    document_count: u32,
    dim: u32,
    temperature: f32,
) !NodeId {
    if (query_count == 0 or document_count == 0 or dim == 0 or
        !std.math.isFinite(temperature) or temperature <= 0 or
        !bld.graph.node(queries).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(query_count)), @as(i64, @intCast(dim)) })) or
        !bld.graph.node(documents).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(document_count)), @as(i64, @intCast(dim)) })) or
        !bld.graph.node(targets).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(query_count)), @as(i64, @intCast(document_count)) })))
        return error.InvalidEmbeddingGemma2Loss;
    const logits = try bld.mul(try bld.matmul2DLayout(queries, documents, false, true), try bld.scalarConst(.f32, 1.0 / temperature));
    return bld.crossEntropyLoss(logits, targets);
}

pub fn buildAsymmetricMaskedInfoNce(
    bld: *Builder,
    queries: NodeId,
    documents: NodeId,
    targets: NodeId,
    allowed: NodeId,
    query_count: u32,
    document_count: u32,
    dim: u32,
    temperature: f32,
) !NodeId {
    const matrix_shape = Shape.init(.f32, &.{ @as(i64, @intCast(query_count)), @as(i64, @intCast(document_count)) });
    if (!bld.graph.node(allowed).output_shape.eq(matrix_shape)) return error.InvalidEmbeddingGemma2Loss;
    if (!bld.graph.node(targets).output_shape.eq(matrix_shape)) return error.InvalidEmbeddingGemma2Loss;
    if (!bld.graph.node(queries).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(query_count)), @as(i64, @intCast(dim)) })) or
        !bld.graph.node(documents).output_shape.eq(Shape.init(.f32, &.{ @as(i64, @intCast(document_count)), @as(i64, @intCast(dim)) })) or
        !std.math.isFinite(temperature) or temperature <= 0)
        return error.InvalidEmbeddingGemma2Loss;
    const logits = try bld.mul(try bld.matmul2DLayout(queries, documents, false, true), try bld.scalarConst(.f32, 1.0 / temperature));
    const invalid = try bld.mul(try bld.sub(try bld.scalarConst(.f32, 1), allowed), try bld.scalarConst(.f32, -1.0e9));
    return bld.crossEntropyLoss(try bld.add(logits, invalid), targets);
}

/// Sum equally weighted losses across Matryoshka dimensions. Inputs are the
/// full 768-dimensional unit embeddings; every prefix is normalized again,
/// matching serving-time truncation.
pub fn buildSymmetricMrlInfoNce(
    bld: *Builder,
    full_queries: NodeId,
    full_documents: NodeId,
    pair_count: u32,
    full_dim: u32,
    dimensions: []const u32,
    temperature: f32,
) !NodeId {
    if (dimensions.len == 0) return error.InvalidEmbeddingGemma2Loss;
    var total: ?NodeId = null;
    var prior: u32 = 0;
    for (dimensions) |dim| {
        if (dim == 0 or dim > full_dim or dim <= prior) return error.InvalidEmbeddingGemma2Loss;
        prior = dim;
        const queries = try truncateL2(bld, full_queries, pair_count, full_dim, dim);
        const documents = try truncateL2(bld, full_documents, pair_count, full_dim, dim);
        const loss = try buildSymmetricInfoNce(bld, queries, documents, pair_count, dim, temperature);
        total = if (total) |sum| try bld.add(sum, loss) else loss;
    }
    return bld.mul(total.?, try bld.scalarConst(.f32, 1.0 / @as(f32, @floatFromInt(dimensions.len))));
}

pub fn truncateL2(bld: *Builder, full: NodeId, rows: u32, full_dim: u32, requested_dim: u32) !NodeId {
    const sliced = if (requested_dim == full_dim) full else try bld.sliceLastDim(full, 0, @intCast(requested_dim));
    const norm2 = try bld.reduceSum(try bld.mul(sliced, sliced), &.{1});
    const inv = try bld.rsqrt(norm2);
    return bld.mul(sliced, try broadcast(bld, inv, Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(requested_dim)) }), &.{ 0, 1 }));
}

pub fn injectDefaultLoRA(allocator: std.mem.Allocator, graph: *const Graph, plan: training.TrainingPlan) !ml.graph.lora.LoRAResult {
    return ml.graph.lora.injectLoRA(allocator, graph, .{
        .rank = plan.rank,
        .alpha = plan.alpha,
        .dropout = plan.dropout,
        .target_patterns = &training.default_target_suffixes,
        .strict_target_patterns = true,
    });
}

fn parameter(bld: *Builder, name: []const u8, shape: Shape) !NodeId {
    return bld.parameter(name, shape);
}

fn layerParameter(bld: *Builder, layer: u32, suffix: []const u8, shape: Shape) !NodeId {
    var buffer: [192]u8 = undefined;
    const name = try training.layerWeightName(&buffer, layer, suffix);
    return parameter(bld, name, shape);
}

fn broadcast(bld: *Builder, input: NodeId, target: Shape, axes: []const u8) !NodeId {
    var attrs = node_mod.BroadcastAttrs{ .target_shape = target };
    if (axes.len > attrs.broadcast_axes.len) return error.InvalidEmbeddingGemma2GraphShape;
    attrs.num_axes = @intCast(axes.len);
    @memcpy(attrs.broadcast_axes[0..axes.len], axes);
    return bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = target,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

test "EmbeddingGemma2 graph builds bidirectional local-global forward and real gradients" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const config = Config{
        .vocab_size = 17,
        .hidden_size = 4,
        .layer_count = 2,
        .intermediate_size = 8,
        .embedding_dim = 6,
        .query_heads = 2,
        .local_kv_heads = 1,
        .global_kv_heads = 1,
        .local_head_dim = 2,
        .global_head_dim = 2,
        .local_radius = 1,
        .global_every = 2,
        .max_sequence = 4,
    };
    const ids = try bld.parameter("input_ids", Shape.init(.f32, &.{ 2, 3 }));
    const mask = try bld.parameter("attention_mask", Shape.init(.f32, &.{ 2, 3 }));
    const built = try buildForward(&bld, config, 2, 3, 6, .{ .input_ids = ids, .attention_mask = mask });
    try std.testing.expect(graph.node(built.embedding).output_shape.eq(Shape.init(.f32, &.{ 2, 6 })));

    var lora = try injectDefaultLoRA(allocator, &graph, .{ .rank = 2, .alpha = 4 });
    defer lora.deinit();
    // 2 layers x 7 block projections + one shared embedding projection.
    try std.testing.expectEqual(@as(usize, 15), lora.adapter.adapters.items.len);
    const output = built.embedding;
    var lora_bld = Builder.init(&lora.graph);
    const seed = try lora_bld.parameter("embedding_cotangent", Shape.init(.f32, &.{ 2, 6 }));
    var wrt: std.ArrayListUnmanaged(NodeId) = .empty;
    defer wrt.deinit(allocator);
    for (lora.adapter.adapters.items) |adapter| {
        try wrt.append(allocator, adapter.lora_a_id);
        try wrt.append(allocator, adapter.lora_b_id);
    }
    var gradients = try ml.graph.autodiff.gradientWithSeeds(allocator, &lora.graph, &.{.{ .output = output, .cotangent = seed }}, wrt.items, .{ .require_all_gradients = true });
    defer gradients.deinit();
    try std.testing.expectEqual(wrt.items.len, gradients.param_grads.len);
}

test "EmbeddingGemma2 InfoNCE exposes gradients to both towers" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const queries = try bld.parameter("queries", Shape.init(.f32, &.{ 2, 3 }));
    const documents = try bld.parameter("documents", Shape.init(.f32, &.{ 2, 3 }));
    const loss = try buildSymmetricMrlInfoNce(&bld, queries, documents, 2, 3, &.{ 2, 3 }, 0.05);
    const seed = try bld.parameter("loss_cotangent", graph.node(loss).output_shape);
    var gradients = try ml.graph.autodiff.gradientWithSeeds(allocator, &graph, &.{.{ .output = loss, .cotangent = seed }}, &.{ queries, documents }, .{ .require_all_gradients = true });
    defer gradients.deinit();
    try std.testing.expect(gradients.param_grads[0] != null_node);
    try std.testing.expect(gradients.param_grads[1] != null_node);
}

test "EmbeddingGemma2 local attention mask is symmetric and inclusive" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const mask = try bld.parameter("mask", Shape.init(.f32, &.{ 1, 4 }));
    const built = try buildAttentionMasks(&bld, mask, 1, 4, 1);
    try std.testing.expect(graph.node(built.local).output_shape.eq(Shape.init(.f32, &.{ 1, 1, 4, 4 })));
}

test "EmbeddingGemma2 RMS MLP PLE pool parameter gradients match finite differences" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const hidden = try bld.parameter("hidden_input", Shape.init(.f32, &.{ 2, 4 }));
    const mask = try bld.parameter("attention_mask", Shape.init(.f32, &.{ 1, 2 }));
    const norm_weight = try bld.parameter("norm.weight", Shape.init(.f32, &.{4}));
    const normed = try bld.rmsNorm(hidden, norm_weight, 4, 1e-6);
    const gate_weight = try bld.parameter("language_model.layers.0.mlp.gate_proj.weight", Shape.init(.f32, &.{ 5, 4 }));
    const up_weight = try bld.parameter("language_model.layers.0.mlp.up_proj.weight", Shape.init(.f32, &.{ 5, 4 }));
    const down_weight = try bld.parameter("language_model.layers.0.mlp.down_proj.weight", Shape.init(.f32, &.{ 4, 5 }));
    const gate = try bld.gelu(try bld.linearNoBias(normed, gate_weight, 2, 4, 5));
    const up = try bld.linearNoBias(normed, up_weight, 2, 4, 5);
    const down = try bld.linearNoBias(try bld.mul(gate, up), down_weight, 2, 5, 4);
    const residual = try bld.add(hidden, down);
    const ple = try bld.parameter("ple_slice", Shape.init(.f32, &.{ 2, 4 }));
    const ple_gate_weight = try bld.parameter("language_model.layers.0.ple_block.per_layer_input_gate.weight", Shape.init(.f32, &.{ 4, 4 }));
    const ple_out_weight = try bld.parameter("language_model.layers.0.ple_block.per_layer_projection.weight", Shape.init(.f32, &.{ 4, 4 }));
    const ple_gate = try bld.gelu(try bld.linearNoBias(residual, ple_gate_weight, 2, 4, 4));
    const ple_out = try bld.linearNoBias(try bld.mul(ple_gate, ple), ple_out_weight, 2, 4, 4);
    const post_ple = try bld.add(residual, ple_out);
    const output_projection_node = try bld.parameter("language_model.embedding_projection.weight", Shape.init(.f32, &.{ 3, 4 }));
    const projected = try bld.linearNoBias(post_ple, output_projection_node, 2, 4, 3);
    const embedding = try maskedMeanMrlL2(&bld, try bld.reshape(projected, Shape.init(.f32, &.{ 1, 2, 3 })), mask, 1, 2, 3, 2);
    const loss = try bld.reduceSum(embedding, &.{ 0, 1 });
    try graph.markOutput(loss);

    var values: std.ArrayListUnmanaged([]const f32) = .empty;
    defer values.deinit(allocator);
    var owned: std.ArrayListUnmanaged([]f32) = .empty;
    defer {
        for (owned.items) |slice| allocator.free(slice);
        owned.deinit(allocator);
    }
    var selected_gate: ?NodeId = null;
    var selected_output: ?NodeId = null;
    for (graph.parameters.items) |parameter_id| {
        const node = graph.node(parameter_id);
        const name = graph.parameterName(node);
        const count: usize = @intCast(node.output_shape.numElements() orelse return error.InvalidTestShape);
        const data = try allocator.alloc(f32, count);
        try owned.append(allocator, data);
        if (std.mem.eql(u8, name, "hidden_input")) {
            for (data, 0..) |*value, index| value.* = 0.1 * @as(f32, @floatFromInt(@as(i32, @intCast(index)) - 3));
        } else if (std.mem.eql(u8, name, "attention_mask") or std.mem.indexOf(u8, name, "norm.weight") != null or std.mem.endsWith(u8, name, "layer_scalar")) {
            @memset(data, 1);
        } else {
            for (data, 0..) |*value, index| {
                const signed: i32 = @as(i32, @intCast(index % 7)) - 3;
                value.* = 0.025 * @as(f32, @floatFromInt(signed));
            }
        }
        if (std.mem.eql(u8, name, "language_model.layers.0.mlp.gate_proj.weight")) selected_gate = parameter_id;
        if (std.mem.eql(u8, name, "language_model.embedding_projection.weight")) selected_output = parameter_id;
        try values.append(allocator, data);
    }
    const max_error = try ml.graph.grad_check.checkGradients(
        allocator,
        &graph,
        loss,
        &.{ selected_gate orelse return error.MissingGateWeight, selected_output orelse return error.MissingOutputWeight },
        values.items,
        1e-3,
    );
    try std.testing.expect(max_error < 0.06);
}
