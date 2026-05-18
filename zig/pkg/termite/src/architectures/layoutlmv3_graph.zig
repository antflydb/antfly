// Copyright 2026 Antfly, Inc.
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

//! LayoutLMv3 encoder forward expressed as an `ml.graph.Graph`.
//!
//! This is the graph-building equivalent of the ComputeBackend-based forward
//! in `src/architectures/layoutlmv3.zig`. It is used by the "level 3" training
//! pipeline that needs:
//!   * a primitive / fused op graph to run real autodiff on,
//!   * parameter nodes whose names match the HuggingFace safetensors keys so
//!     that `ml.graph.lora.injectLoRA` can match LoRA target modules such as
//!     `query`, `key`, `value`, `dense`, `intermediate.dense`, `output.dense`,
//!   * a hook-friendly `output_node` the caller can attach a task head to.
//!
//! LayoutLMv3 is BERT-style (bidirectional, no GQA, no RoPE) with the extra
//! 2D positional embeddings derived from a `[batch, seq, 4]` bounding box
//! tensor. The text embedding layer sums:
//!
//!     word + position + token_type + x0 + y0 + x1 + y1 + h + w
//!
//! followed by LayerNorm. Each encoder layer is the standard BERT block:
//! self-attention (Q/K/V with bias, SDPA, output projection with bias,
//! residual, LayerNorm), then a feed-forward MLP (intermediate with GELU,
//! output projection, residual, LayerNorm).
//!
//! TODO(level3):
//!   * Replace the six-index bbox split with a proper `slice` primitive when
//!     it lands in the Builder. For now the struct exposes the individual
//!     `x0_ids_node`..`w_ids_node` parameters alongside `bbox_node` so the
//!     caller can pre-compute the integer coordinate indices.
//!   * Wire in the relative-position / spatial bias that the reference
//!     `forward` adds when `config.has_relative_attention_bias` is set.
//!     This requires slicing the bucketed bias by a mask; deferred.
//!   * Visual (patch) tokens are NOT yet emitted here — this file is the
//!     text-only encoder. Visual tokens live in a separate graph.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.DType;

// ── Config ────────────────────────────────────────────────────────────────

/// Minimal config the graph builder needs. Matches the shape of
/// `src/models/layoutlmv3.zig` but only the fields actually referenced during
/// graph construction. Callers that already have a full `models.layoutlmv3`
/// config can construct this via `Config.fromFull`.
pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    /// Usually `hidden_size / num_attention_heads`. Kept explicit so the
    /// caller can override it for architectures that decouple the two.
    head_dim: u32,
    intermediate_size: u32,
    max_position_embeddings: u32,
    type_vocab_size: u32 = 2,
    max_2d_position_embeddings: u32 = 1024,
    layer_norm_eps: f32 = 1e-5,
};

// ── Public return type ────────────────────────────────────────────────────

/// All node handles a caller needs after `buildForwardGraph` returns.
///
/// `input_ids_node`, `attention_mask_node`, `bbox_node`, `token_type_ids_node`
/// and `position_ids_node` are the top-level inputs the caller is expected to
/// bind at runtime. The additional bbox component fields are exposed so the
/// caller can split the `[batch, seq, 4]` bbox tensor into the six integer
/// index vectors LayoutLMv3's 2D embedding layer wants; see the TODO at the
/// top of this file.
///
/// `output_node` is the final encoder hidden state of shape
/// `[batch, seq, hidden_size]`. Downstream heads (token classifier, sequence
/// classifier, MLM head, etc.) should consume this and emit their own loss.
pub const LayoutLMv3Inputs = struct {
    input_ids: NodeId,
    attention_mask: NodeId,
    bbox: NodeId,
    token_type_ids: NodeId,
    position_ids: NodeId,
    /// Additive attention bias of shape `[batch * num_heads, seq_len, seq_len]`
    /// f32. Valid positions are 0.0, padded positions are a large negative
    /// (e.g. -1e9). Applied inside each encoder layer's manual SDPA
    /// decomposition so that padding tokens are correctly masked.
    attn_bias: NodeId,
    /// Pre-split bbox coordinate indices. The caller supplies x0, y0, x1,
    /// y1, h = y1 - y0, w = x1 - x0, each clamped to
    /// `[0, max_2d_position_embeddings - 1]`.
    x0_ids: NodeId,
    y0_ids: NodeId,
    x1_ids: NodeId,
    y1_ids: NodeId,
    h_ids: NodeId,
    w_ids: NodeId,
};

pub const LayoutLMv3Graph = struct {
    input_ids_node: NodeId,
    attention_mask_node: NodeId,
    bbox_node: NodeId,
    token_type_ids_node: NodeId,
    position_ids_node: NodeId,
    output_node: NodeId,

    /// Additive attention bias node, shape `[batch * num_heads, seq_len, seq_len]`.
    attn_bias_node: NodeId,

    // Auxiliary inputs for the 2D bbox embeddings. See file-level TODO.
    x0_ids_node: NodeId,
    y0_ids_node: NodeId,
    x1_ids_node: NodeId,
    y1_ids_node: NodeId,
    h_ids_node: NodeId,
    w_ids_node: NodeId,
};

// ── Entry point ───────────────────────────────────────────────────────────

pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    inputs: LayoutLMv3Inputs,
) !LayoutLMv3Graph {
    std.debug.assert(config.num_attention_heads * config.head_dim == config.hidden_size);

    const total: u32 = batch * seq_len;
    const H: u32 = config.hidden_size;

    // All top-level input NodeIds come from the caller — this module no
    // longer creates its own placeholders. See the file header for the
    // placeholder-wiring refactor rationale.
    const input_ids_node = inputs.input_ids;
    const attention_mask_node = inputs.attention_mask;
    const attn_bias_node = inputs.attn_bias;
    const bbox_node = inputs.bbox;
    const token_type_ids_node = inputs.token_type_ids;
    const position_ids_node = inputs.position_ids;
    const x0_ids_node = inputs.x0_ids;
    const y0_ids_node = inputs.y0_ids;
    const x1_ids_node = inputs.x1_ids;
    const y1_ids_node = inputs.y1_ids;
    const h_ids_node = inputs.h_ids;
    const w_ids_node = inputs.w_ids;

    // Reshape the rank-2 index tensors down to rank-1 `[total]` for the flat
    // `embeddingLookup` API, which returns `[total, dim]` rows.
    const flat_shape = Shape.init(.i64, &.{@intCast(total)});
    const input_ids_flat = try bld.reshape(input_ids_node, flat_shape);
    const position_ids_flat = try bld.reshape(position_ids_node, flat_shape);
    const token_type_ids_flat = try bld.reshape(token_type_ids_node, flat_shape);
    const x0_flat = try bld.reshape(x0_ids_node, flat_shape);
    const y0_flat = try bld.reshape(y0_ids_node, flat_shape);
    const x1_flat = try bld.reshape(x1_ids_node, flat_shape);
    const y1_flat = try bld.reshape(y1_ids_node, flat_shape);
    const h_flat = try bld.reshape(h_ids_node, flat_shape);
    const w_flat = try bld.reshape(w_ids_node, flat_shape);

    // ── Embeddings ────────────────────────────────────────────────────
    const embedded_flat = try buildEmbeddings(
        bld,
        config,
        total,
        input_ids_flat,
        position_ids_flat,
        token_type_ids_flat,
        x0_flat,
        y0_flat,
        x1_flat,
        y1_flat,
        h_flat,
        w_flat,
    );

    // Promote `[total, H]` → `[batch, seq, H]` so downstream ops see the
    // conventional rank-3 hidden state shape. (The linear layers reshape
    // back to `[total, H]` internally.)
    var hidden = try bld.reshape(
        embedded_flat,
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(H) }),
    );

    // ── Encoder stack ────────────────────────────────────────────────
    var layer_idx: u32 = 0;
    while (layer_idx < config.num_hidden_layers) : (layer_idx += 1) {
        hidden = try buildEncoderLayer(bld, config, batch, seq_len, layer_idx, hidden, attn_bias_node);
    }

    return .{
        .input_ids_node = input_ids_node,
        .attention_mask_node = attention_mask_node,
        .bbox_node = bbox_node,
        .token_type_ids_node = token_type_ids_node,
        .position_ids_node = position_ids_node,
        .output_node = hidden,
        .attn_bias_node = attn_bias_node,
        .x0_ids_node = x0_ids_node,
        .y0_ids_node = y0_ids_node,
        .x1_ids_node = x1_ids_node,
        .y1_ids_node = y1_ids_node,
        .h_ids_node = h_ids_node,
        .w_ids_node = w_ids_node,
    };
}

// ── Embeddings ─────────────────────────────────────────────────────────────

fn buildEmbeddings(
    bld: *Builder,
    config: Config,
    total: u32,
    input_ids_flat: NodeId,
    position_ids_flat: NodeId,
    token_type_ids_flat: NodeId,
    x0_flat: NodeId,
    y0_flat: NodeId,
    x1_flat: NodeId,
    y1_flat: NodeId,
    h_flat: NodeId,
    w_flat: NodeId,
) !NodeId {
    const H: u32 = config.hidden_size;

    // Weight parameters — names mirror the HF safetensors keys so that
    // `ml.graph.lora.injectLoRA` and the weight loader can find them.
    const word_w = try bld.parameter(
        "embeddings.word_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.vocab_size), @intCast(H) }),
    );
    const pos_w = try bld.parameter(
        "embeddings.position_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.max_position_embeddings), @intCast(H) }),
    );
    const tt_w = try bld.parameter(
        "embeddings.token_type_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.type_vocab_size), @intCast(H) }),
    );
    const x_w = try bld.parameter(
        "embeddings.x_position_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.max_2d_position_embeddings), @intCast(H) }),
    );
    const y_w = try bld.parameter(
        "embeddings.y_position_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.max_2d_position_embeddings), @intCast(H) }),
    );
    const h_w = try bld.parameter(
        "embeddings.h_position_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.max_2d_position_embeddings), @intCast(H) }),
    );
    const w_w = try bld.parameter(
        "embeddings.w_position_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.max_2d_position_embeddings), @intCast(H) }),
    );
    const ln_gamma = try bld.parameter(
        "embeddings.LayerNorm.weight",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const ln_beta = try bld.parameter(
        "embeddings.LayerNorm.bias",
        Shape.init(.f32, &.{@intCast(H)}),
    );

    // Nine lookups, each returning `[total, H]`.
    //
    // NOTE: the reference ComputeBackend forward splits the hidden dim into
    // `coord_size*4 + shape_size*2` slabs and concatenates per-coord lookups.
    // That's equivalent to keeping separate small embedding tables per coord
    // and summing — which is what we do here using full `[max_2d, H]` tables.
    // When we train from an HF checkpoint the loader is responsible for
    // zero-padding the narrow (128-dim) per-coord tables out to `H`. TODO:
    // once the Builder has `concat` along the feature axis, switch back to
    // the exact reference layout.
    const word_emb = try bld.embeddingLookup(word_w, input_ids_flat, total, H);
    const pos_emb = try bld.embeddingLookup(pos_w, position_ids_flat, total, H);
    const tt_emb = try bld.embeddingLookup(tt_w, token_type_ids_flat, total, H);
    const x0_emb = try bld.embeddingLookup(x_w, x0_flat, total, H);
    const y0_emb = try bld.embeddingLookup(y_w, y0_flat, total, H);
    const x1_emb = try bld.embeddingLookup(x_w, x1_flat, total, H);
    const y1_emb = try bld.embeddingLookup(y_w, y1_flat, total, H);
    const h_emb = try bld.embeddingLookup(h_w, h_flat, total, H);
    const w_emb = try bld.embeddingLookup(w_w, w_flat, total, H);

    // Elementwise sum — fold left so the graph stays a left-leaning add chain
    // that autodiff's reverse pass handles cleanly.
    var summed = try bld.elemAdd(word_emb, pos_emb);
    summed = try bld.elemAdd(summed, tt_emb);
    summed = try bld.elemAdd(summed, x0_emb);
    summed = try bld.elemAdd(summed, y0_emb);
    summed = try bld.elemAdd(summed, x1_emb);
    summed = try bld.elemAdd(summed, y1_emb);
    summed = try bld.elemAdd(summed, h_emb);
    summed = try bld.elemAdd(summed, w_emb);

    // LayerNorm (gamma, beta, eps). Dropout is intentionally omitted — it is
    // a no-op at inference and during graph-based training we rely on the
    // optimizer/trainer to insert stochastic ops if wanted.
    const normed = try bld.layerNorm(summed, ln_gamma, ln_beta, H, config.layer_norm_eps);
    return normed;
}

// ── Encoder layer ──────────────────────────────────────────────────────────

fn buildEncoderLayer(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    layer_idx: u32,
    hidden: NodeId,
    attn_bias: NodeId,
) !NodeId {
    const H: u32 = config.hidden_size;
    const num_heads: u32 = config.num_attention_heads;
    const head_dim: u32 = config.head_dim;
    const total: u32 = batch * seq_len;

    // ── Parameter names for this layer ───────────────────────────────
    var name_buf: [128]u8 = undefined;

    const q_w = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.self.query.weight", .{layer_idx}),
        Shape.init(.f32, &.{ @intCast(H), @intCast(H) }),
    );
    const q_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.self.query.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const k_w = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.self.key.weight", .{layer_idx}),
        Shape.init(.f32, &.{ @intCast(H), @intCast(H) }),
    );
    const k_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.self.key.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const v_w = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.self.value.weight", .{layer_idx}),
        Shape.init(.f32, &.{ @intCast(H), @intCast(H) }),
    );
    const v_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.self.value.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const o_w = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.output.dense.weight", .{layer_idx}),
        Shape.init(.f32, &.{ @intCast(H), @intCast(H) }),
    );
    const o_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.output.dense.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const attn_ln_g = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.output.LayerNorm.weight", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const attn_ln_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.attention.output.LayerNorm.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const ffn_inter_w = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.intermediate.dense.weight", .{layer_idx}),
        Shape.init(.f32, &.{ @intCast(config.intermediate_size), @intCast(H) }),
    );
    const ffn_inter_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.intermediate.dense.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(config.intermediate_size)}),
    );
    const ffn_out_w = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.output.dense.weight", .{layer_idx}),
        Shape.init(.f32, &.{ @intCast(H), @intCast(config.intermediate_size) }),
    );
    const ffn_out_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.output.dense.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const ffn_ln_g = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.output.LayerNorm.weight", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const ffn_ln_b = try bld.parameter(
        try fmt(&name_buf, "encoder.layer.{d}.output.LayerNorm.bias", .{layer_idx}),
        Shape.init(.f32, &.{@intCast(H)}),
    );

    // ── Self-attention ───────────────────────────────────────────────
    // `linear` consumes a rank-2 `[rows, in_dim]` input. Flatten hidden to
    // `[batch*seq, H]` then restore shape around SDPA.
    const hidden_flat = try bld.reshape(
        hidden,
        Shape.init(.f32, &.{ @intCast(total), @intCast(H) }),
    );

    const q_lin = try bld.linear(hidden_flat, q_w, q_b, total, H, H);
    const k_lin = try bld.linear(hidden_flat, k_w, k_b, total, H, H);
    const v_lin = try bld.linear(hidden_flat, v_w, v_b, total, H, H);

    // Reshape `[total, H]` → `[batch, seq, num_heads, head_dim]`
    // → `[batch, num_heads, seq, head_dim]` → `[batch*num_heads, seq, head_dim]`
    // which is what `sdpa` expects.
    //
    // The Builder doesn't yet have a generic transpose-then-reshape fuser, so
    // we do it in two steps. Note that `reshape` in the builder is a view-ish
    // op; the real data movement happens at lowering time.
    const bhsd_shape = Shape.init(.f32, &.{
        @intCast(batch),
        @intCast(seq_len),
        @intCast(num_heads),
        @intCast(head_dim),
    });
    const q_4d = try bld.reshape(q_lin, bhsd_shape);
    const k_4d = try bld.reshape(k_lin, bhsd_shape);
    const v_4d = try bld.reshape(v_lin, bhsd_shape);

    // [batch, seq, heads, head_dim] → [batch, heads, seq, head_dim]
    const perm: [4]u8 = .{ 0, 2, 1, 3 };
    const q_bhsd = try bld.transpose(q_4d, &perm);
    const k_bhsd = try bld.transpose(k_4d, &perm);
    const v_bhsd = try bld.transpose(v_4d, &perm);

    const bh_shape = Shape.init(.f32, &.{
        @intCast(batch * num_heads),
        @intCast(seq_len),
        @intCast(head_dim),
    });
    const q_bh = try bld.reshape(q_bhsd, bh_shape);
    const k_bh = try bld.reshape(k_bhsd, bh_shape);
    const v_bh = try bld.reshape(v_bhsd, bh_shape);

    // ── Masked scaled dot-product attention (manual decomposition) ────
    // Replaces the fused `bld.sdpa` that lacked an additive bias input.
    // This mirrors the decomposition in `bert_graph.zig` so that the
    // caller-supplied `attn_bias` (0 for valid, -1e9 for padded) correctly
    // masks padding tokens before softmax.
    //
    // scores = Q @ K^T, shape [bh, seq, seq]
    const k_t = try bld.transpose(k_bh, &.{ 0, 2, 1 });
    const scores_raw = try bld.matmul3D(q_bh, k_t);
    // scale by 1/sqrt(head_dim)
    const scale = try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
    const scores_scaled = try bld.mul(scores_raw, scale);
    // Add mask bias (broadcast [bh, seq, seq] + [bh, seq, seq])
    const scores_masked = try bld.add(scores_scaled, attn_bias);
    // Softmax over last axis
    const probs = try bld.softmax(scores_masked);
    // attn_out = probs @ V, shape [bh, seq, head_dim]
    const attn_out_bh = try bld.matmul3D(probs, v_bh);

    // Unwind the head-merge reshape/transpose back to `[total, H]`.
    const attn_bhsd = try bld.reshape(
        attn_out_bh,
        Shape.init(.f32, &.{
            @intCast(batch),
            @intCast(num_heads),
            @intCast(seq_len),
            @intCast(head_dim),
        }),
    );
    const attn_bshd = try bld.transpose(attn_bhsd, &.{ 0, 2, 1, 3 });
    const attn_concat = try bld.reshape(
        attn_bshd,
        Shape.init(.f32, &.{ @intCast(total), @intCast(H) }),
    );

    // Output projection + residual + LayerNorm.
    const attn_proj = try bld.linear(attn_concat, o_w, o_b, total, H, H);
    const attn_residual = try bld.elemAdd(hidden_flat, attn_proj);
    const attn_normed = try bld.layerNorm(attn_residual, attn_ln_g, attn_ln_b, H, config.layer_norm_eps);

    // ── Feed-forward ─────────────────────────────────────────────────
    const ffn_inter = try bld.linear(
        attn_normed,
        ffn_inter_w,
        ffn_inter_b,
        total,
        H,
        config.intermediate_size,
    );
    const ffn_act = try bld.gelu(ffn_inter);
    const ffn_out = try bld.linear(
        ffn_act,
        ffn_out_w,
        ffn_out_b,
        total,
        config.intermediate_size,
        H,
    );
    const ffn_residual = try bld.elemAdd(attn_normed, ffn_out);
    const ffn_normed = try bld.layerNorm(ffn_residual, ffn_ln_g, ffn_ln_b, H, config.layer_norm_eps);

    // Restore `[batch, seq, H]` rank-3 view for the next layer.
    return bld.reshape(
        ffn_normed,
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(H) }),
    );
}

// ── Small helpers ─────────────────────────────────────────────────────────

/// Format into a caller-provided buffer and return the slice. Used to build
/// per-layer parameter names like `encoder.layer.3.attention.self.query.weight`.
fn fmt(buf: []u8, comptime pattern: []const u8, args: anytype) ![]const u8 {
    return std.fmt.bufPrint(buf, pattern, args);
}

// ── Compile-time smoke test ───────────────────────────────────────────────

test "buildForwardGraph wires a tiny LayoutLMv3 encoder" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const cfg = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .head_dim = 8, // 32 / 4
        .intermediate_size = 64,
        .max_position_embeddings = 16,
        .type_vocab_size = 2,
        .max_2d_position_embeddings = 32,
        .layer_norm_eps = 1e-5,
    };

    const batch: u32 = 1;
    const seq_len: u32 = 4;

    const built = try buildForwardGraph(&bld, cfg, batch, seq_len, makeTestInputs(&bld, batch, seq_len, cfg.num_attention_heads) catch unreachable);
    try g.markOutput(built.output_node);

    // Output node should be rank-3 [batch, seq, hidden_size].
    const out_shape = g.node(built.output_node).output_shape;
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, @intCast(batch)), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, @intCast(seq_len)), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, @intCast(cfg.hidden_size)), out_shape.dim(2));

    // We should have registered all of the expected top-level inputs and the
    // six bbox component inputs, plus the embedding and per-layer weights.
    // A precise count is brittle, but we know there's at least:
    //   5 top-level + 1 attn_bias + 6 bbox = 12 input parameters
    //   9 embedding params (word/pos/tt/x/y/h/w + LN gamma/beta)
    //   16 per-layer params × 2 layers = 32
    // → 53 parameters total.
    try std.testing.expect(g.parameters.items.len >= 53);
}

test "buildForwardGraph input node shapes are as expected" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const cfg = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .head_dim = 8,
        .intermediate_size = 64,
        .max_position_embeddings = 16,
    };

    const batch: u32 = 2;
    const seq_len: u32 = 4;

    const built = try buildForwardGraph(&bld, cfg, batch, seq_len, makeTestInputs(&bld, batch, seq_len, cfg.num_attention_heads) catch unreachable);

    const ids_shape = g.node(built.input_ids_node).output_shape;
    try std.testing.expectEqual(@as(u8, 2), ids_shape.rank());
    try std.testing.expectEqual(@as(i64, @intCast(batch)), ids_shape.dim(0));
    try std.testing.expectEqual(@as(i64, @intCast(seq_len)), ids_shape.dim(1));

    const bbox_shape = g.node(built.bbox_node).output_shape;
    try std.testing.expectEqual(@as(u8, 3), bbox_shape.rank());
    try std.testing.expectEqual(@as(i64, 4), bbox_shape.dim(2));
}

fn makeTestInputs(bld: *Builder, batch: u32, seq_len: u32, num_heads: u32) !LayoutLMv3Inputs {
    const bs: [2]i64 = .{ @intCast(batch), @intCast(seq_len) };
    const bs4: [3]i64 = .{ @intCast(batch), @intCast(seq_len), 4 };
    return .{
        .input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &bs)),
        .attention_mask = try bld.parameter("test_attn_mask", Shape.init(.i64, &bs)),
        .attn_bias = try bld.parameter("test_attn_bias", Shape.init(.f32, &.{
            @as(i64, @intCast(batch * num_heads)),
            @as(i64, @intCast(seq_len)),
            @as(i64, @intCast(seq_len)),
        })),
        .bbox = try bld.parameter("test_bbox", Shape.init(.i64, &bs4)),
        .token_type_ids = try bld.parameter("test_tt_ids", Shape.init(.i64, &bs)),
        .position_ids = try bld.parameter("test_pos_ids", Shape.init(.i64, &bs)),
        .x0_ids = try bld.parameter("test_x0", Shape.init(.i64, &bs)),
        .y0_ids = try bld.parameter("test_y0", Shape.init(.i64, &bs)),
        .x1_ids = try bld.parameter("test_x1", Shape.init(.i64, &bs)),
        .y1_ids = try bld.parameter("test_y1", Shape.init(.i64, &bs)),
        .h_ids = try bld.parameter("test_h", Shape.init(.i64, &bs)),
        .w_ids = try bld.parameter("test_w", Shape.init(.i64, &bs)),
    };
}
