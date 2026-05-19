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

// Qwen-family decoder forward pass expressed as an `ml.graph.Graph` built with
// the high-level `Builder` API. This is the "level 3" training entry point:
//
//   - Forward runs as a real computation graph (not ComputeBackend eager ops),
//     which means autodiff can differentiate it node-by-node.
//   - Every Q / K / V / O / gate / up / down projection is emitted as a
//     `fused_linear` / `fused_linear_no_bias` node so that
//     `ml.graph.lora.injectLoRA` can find the projections by parameter name
//     substring (`"q_proj"`, `"v_proj"`, ...) and splice in LoRA adapters.
//   - Weight parameters use the HuggingFace checkpoint naming convention so
//     that a standard HF weight loader can populate the graph's parameter
//     store by matching names.
//
// This file is the graph-building sibling of the ComputeBackend-based eager
// Qwen forward paths. It covers Qwen2 and the Qwen3.5 full-attention text
// layer shape, including Qwen3.5 linear-attention layers for static text
// training graphs.
//
// Out of scope here:
//   - LM head / loss. The graph stops at the final hidden state. The caller
//     bolts on an `lm_head` projection, a softmax, and any training loss.
//   - KV caching / sliding-window attention. This is a training-shaped forward
//     (all tokens at once, causal mask).
//   - Weight population. `buildForwardGraph` only creates `parameter` nodes;
//     the caller is responsible for binding actual tensor data before the
//     graph is lowered to a ComputeBackend for execution.
//
// RoPE cos/sin tables are passed in as input placeholders (parameter nodes
// with `rope_cos` / `rope_sin` names). The caller precomputes them for the
// specific (seq_len, head_dim, theta) combination and binds them at execution
// time. This keeps the graph free of trig primitives and matches how the
// existing `Builder.rope` wrapper expects cos/sin to be provided.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const node_mod = ml.graph.node;
const null_node = ml.graph.null_node;

// ── Public Config ───────────────────────────────────────────────────────────

/// Qwen-family text decoder configuration. Mirrors the fields used by the
/// eager GPT/Qwen config but only includes what's needed to build a
/// training-time forward graph.
pub const Config = struct {
    family: Family = .qwen2,
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    intermediate_size: u32,
    max_position_embeddings: u32,
    rope_theta: f32 = 1_000_000.0,
    rms_norm_eps: f32 = 1e-6,
    rope_partial_factor: f32 = 1.0,
    norm_weight_offset: f32 = 0.0,
    qwen35_has_linear_attention: bool = false,
    qwen35_full_attention_interval: u32 = 0,
    qwen35_linear_conv_kernel_dim: u32 = 0,
    qwen35_linear_key_head_dim: u32 = 0,
    qwen35_linear_value_head_dim: u32 = 0,
    qwen35_linear_num_key_heads: u32 = 0,
    qwen35_linear_num_value_heads: u32 = 0,
    qwen35_attn_output_gate: bool = false,

    pub fn qHeadsPerKv(self: Config) u32 {
        return self.num_attention_heads / self.num_kv_heads;
    }

    pub fn qDim(self: Config) u32 {
        return self.num_attention_heads * self.head_dim;
    }

    pub fn kvDim(self: Config) u32 {
        return self.num_kv_heads * self.head_dim;
    }

    pub fn ropeDim(self: Config) u32 {
        if (self.rope_partial_factor <= 0.0 or self.rope_partial_factor >= 1.0) return self.head_dim;
        const raw = @as(u32, @intFromFloat(@round(@as(f32, @floatFromInt(self.head_dim)) * self.rope_partial_factor)));
        return @max(@as(u32, 2), raw - (raw % 2));
    }

    pub fn layerUsesQwen35LinearAttention(self: Config, layer_index: u32) bool {
        if (self.family != .qwen3_5 or !self.qwen35_has_linear_attention) return false;
        const interval = if (self.qwen35_full_attention_interval > 0) self.qwen35_full_attention_interval else 4;
        return ((layer_index + 1) % interval) != 0;
    }

    pub fn qwen35LinearKeyDim(self: Config) u32 {
        return self.qwen35_linear_num_key_heads * self.qwen35_linear_key_head_dim;
    }

    pub fn qwen35LinearValueDim(self: Config) u32 {
        return self.qwen35_linear_num_value_heads * self.qwen35_linear_value_head_dim;
    }

    pub fn qwen35LinearConvDim(self: Config) u32 {
        return self.qwen35LinearKeyDim() * 2 + self.qwen35LinearValueDim();
    }
};

pub const Family = enum {
    qwen2,
    qwen3_5,
};

/// Caller-provided inputs passed into `buildForwardGraph`. The harness
/// (typically `real_autodiff_trainer`) creates these nodes and threads them
/// in, avoiding the dual-placeholder mismatch that happens when both the
/// harness and the architecture try to own the same inputs.
pub const QwenInputs = struct {
    /// [batch, seq] i64 token IDs.
    input_ids: NodeId,
    /// [seq, head_dim] f32 precomputed RoPE cosines.
    rope_cos: NodeId,
    /// [seq, head_dim] f32 precomputed RoPE sines.
    rope_sin: NodeId,
};

/// Result of graph construction. All NodeIds live in the `Graph` that was
/// passed to `buildForwardGraph` via its `Builder`.
pub const QwenGraph = struct {
    /// Input placeholder: [batch, seq] i64 token IDs. Bind at execution time.
    input_ids_node: NodeId,
    /// Input placeholder: cos/sin tables for RoPE.
    /// Shape convention matches `Builder.rope`'s expected input:
    /// the cos/sin tables are broadcast against `[B*H_any, seq, head_dim]`.
    /// Caller provides them as `[seq, head_dim]` tensors.
    rope_cos_node: NodeId,
    rope_sin_node: NodeId,
    /// Output: final hidden state `[batch, seq, hidden_size]` after
    /// `model.norm` (RMSNorm). Caller projects to logits.
    output_node: NodeId,
};

const Qwen35LinearAttentionStem = struct {
    conv_out: NodeId,
    z: NodeId,
    beta_projection: NodeId,
    a_projection: NodeId,
    conv_dim: u32,
    value_dim: u32,
    value_heads: u32,
};

// ── Small Graph Helpers ────────────────────────────────────────────────────

fn zeroLike(bld: *Builder, shape: Shape) !NodeId {
    const zero = try bld.scalarConst(shape.dtype, 0.0);
    return broadcastInDim(bld, zero, shape, &.{});
}

fn scalarToShape(bld: *Builder, scalar: NodeId, shape: Shape) !NodeId {
    return broadcastInDim(bld, scalar, shape, &.{});
}

// ── Entry point ─────────────────────────────────────────────────────────────

/// Construct the Qwen2 forward graph.
///
/// Weight parameters are emitted as `bld.parameter` nodes following the HF
/// checkpoint naming convention (`model.layers.{i}.self_attn.q_proj.weight`,
/// etc.) so that both weight loaders and `injectLoRA` can find them by name.
///
/// `batch` and `seq_len` are baked into the graph's shape metadata. Re-build
/// the graph if either changes. All shapes are static — no dynamic dims.
pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    inputs: QwenInputs,
) !QwenGraph {
    if (config.num_attention_heads % config.num_kv_heads != 0) {
        return error.InvalidGqaConfig;
    }

    const batch_i: i64 = @intCast(batch);
    const seq_i: i64 = @intCast(seq_len);
    const hidden_i: i64 = @intCast(config.hidden_size);
    const total_i: i64 = batch_i * seq_i;

    // ── Inputs ─────────────────────────────────────────────────────────
    //
    // `inputs.input_ids` is a caller-provided `[batch, seq]` i64 node. We
    // reshape to `[batch*seq]` for the embedding lookup (whose fused op
    // takes a flat index vector), then reshape back to `[batch, seq, hidden]`
    // for downstream ops. `inputs.rope_cos` / `inputs.rope_sin` are also
    // caller-owned and forwarded straight into the RoPE nodes.

    const input_ids = inputs.input_ids;
    const rope_cos = inputs.rope_cos;
    const rope_sin = inputs.rope_sin;

    // Flatten token IDs for the gather-style embedding lookup.
    const flat_ids_shape = Shape.init(.i64, &.{total_i});
    const flat_ids = try bld.reshape(input_ids, flat_ids_shape);

    // ── Token embeddings ───────────────────────────────────────────────
    //
    // `embed_tokens.weight` is `[vocab_size, hidden_size]`. The fused embedding
    // lookup emits `[total, hidden]`. We then reshape to `[batch, seq, hidden]`
    // so that the rest of the stack can treat sequence position as an explicit
    // axis (useful for RoPE broadcast).
    const embed_w = try bld.parameter(
        "model.embed_tokens.weight",
        Shape.init(.f32, &.{ @as(i64, @intCast(config.vocab_size)), hidden_i }),
    );
    const embed_flat = try bld.embeddingLookup(
        embed_w,
        flat_ids,
        @intCast(total_i),
        config.hidden_size,
    );

    const hidden_3d_shape = Shape.init(.f32, &.{ batch_i, seq_i, hidden_i });
    var hidden: NodeId = try bld.reshape(embed_flat, hidden_3d_shape);

    // ── Causal attention mask ──────────────────────────────────────────
    //
    // Built once as a constant `[1, 1, seq, seq]` tensor so that autodiff
    // treats it as a leaf with zero gradient. Value is 0 on the lower
    // triangle (including diagonal) and -1e9 on the upper triangle. Broadcasts
    // against pre-softmax scores shaped `[batch, num_heads, seq, seq]`.
    const causal_mask = try buildCausalMask(bld, seq_len);

    // ── Decoder layers ─────────────────────────────────────────────────
    var layer: u32 = 0;
    while (layer < config.num_hidden_layers) : (layer += 1) {
        hidden = try decoderLayer(
            bld,
            config,
            hidden,
            causal_mask,
            rope_cos,
            rope_sin,
            batch,
            seq_len,
            layer,
        );
    }

    // ── Final norm ─────────────────────────────────────────────────────
    //
    // `model.norm` is an RMSNorm over the hidden dimension, same shape as
    // the per-layer input norms.
    const final_norm_raw = try bld.parameter(
        "model.norm.weight",
        Shape.init(.f32, &.{hidden_i}),
    );
    const final_norm_w = try normWeight(bld, config, final_norm_raw);
    const output = try bld.rmsNorm(hidden, final_norm_w, config.hidden_size, config.rms_norm_eps);

    return QwenGraph{
        .input_ids_node = input_ids,
        .rope_cos_node = rope_cos,
        .rope_sin_node = rope_sin,
        .output_node = output,
    };
}

// ── Decoder block ───────────────────────────────────────────────────────────

fn decoderLayer(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    causal_mask: NodeId,
    rope_cos: NodeId,
    rope_sin: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
) !NodeId {
    const hidden_i: i64 = @intCast(config.hidden_size);
    // --- Pre-norm self-attention ---
    const input_ln_raw = try parameterFmt(
        bld,
        "model.layers.{d}.input_layernorm.weight",
        layer,
        Shape.init(.f32, &.{hidden_i}),
    );
    const input_ln_w = try normWeight(bld, config, input_ln_raw);
    const attn_normed = try bld.rmsNorm(
        hidden_in,
        input_ln_w,
        config.hidden_size,
        config.rms_norm_eps,
    );

    const attn_out = if (config.layerUsesQwen35LinearAttention(layer))
        try qwen35LinearAttentionGraph(bld, config, attn_normed, batch, seq_len, layer)
    else
        try selfAttention(
            bld,
            config,
            attn_normed,
            causal_mask,
            rope_cos,
            rope_sin,
            batch,
            seq_len,
            layer,
        );

    // Residual add. We use the primitive `add` because elemwise LoRA
    // injection doesn't care about the residual link and we don't want a
    // fused_elem_add decomposition for something this trivial.
    const attn_res = try bld.add(hidden_in, attn_out);

    // --- Pre-norm SwiGLU MLP ---
    const post_ln_raw = try parameterFmt(
        bld,
        "model.layers.{d}.post_attention_layernorm.weight",
        layer,
        Shape.init(.f32, &.{hidden_i}),
    );
    const post_ln_w = try normWeight(bld, config, post_ln_raw);
    const mlp_normed = try bld.rmsNorm(
        attn_res,
        post_ln_w,
        config.hidden_size,
        config.rms_norm_eps,
    );

    const mlp_out = try swigluMlp(bld, config, mlp_normed, batch, seq_len, layer);

    return bld.add(attn_res, mlp_out);
}

// ── Self-attention (GQA + RoPE + causal mask) ───────────────────────────────

fn selfAttention(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    causal_mask: NodeId,
    rope_cos: NodeId,
    rope_sin: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
) !NodeId {
    const batch_i: i64 = @intCast(batch);
    const seq_i: i64 = @intCast(seq_len);
    const hidden_i: i64 = @intCast(config.hidden_size);
    const total_i: i64 = batch_i * seq_i;
    const head_dim_i: i64 = @intCast(config.head_dim);
    const num_heads_i: i64 = @intCast(config.num_attention_heads);
    const num_kv_i: i64 = @intCast(config.num_kv_heads);
    const q_dim_i: i64 = num_heads_i * head_dim_i;
    const q_projection_dim_i: i64 = if (config.family == .qwen3_5 and config.qwen35_attn_output_gate) q_dim_i * 2 else q_dim_i;
    const kv_dim_i: i64 = num_kv_i * head_dim_i;

    // ── Flatten hidden_in to 2D for the linear projections ────────────
    //
    // hidden_in is `[batch, seq, hidden]`. The fused linear op takes a
    // 2D `[rows, in_dim]` input. Reshape first, project, reshape back.
    const hidden_flat_shape = Shape.init(.f32, &.{ total_i, hidden_i });
    const hidden_flat = try bld.reshape(hidden_in, hidden_flat_shape);

    // --- Q projection (with bias) ---
    const q_w = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.q_proj.weight",
        layer,
        Shape.init(.f32, &.{ q_projection_dim_i, hidden_i }),
    );
    const q_b = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.q_proj.bias",
        layer,
        Shape.init(.f32, &.{q_projection_dim_i}),
    );
    const q_projected = try bld.linear(
        hidden_flat,
        q_w,
        q_b,
        @intCast(total_i),
        config.hidden_size,
        @intCast(q_projection_dim_i),
    );
    const q_flat, const q_gate = if (q_projection_dim_i == q_dim_i)
        .{ q_projected, null_node }
    else
        .{
            try bld.sliceLastDim(q_projected, 0, q_dim_i),
            try bld.sliceLastDim(q_projected, q_dim_i, q_projection_dim_i),
        };

    // --- K projection (with bias) ---
    const k_w = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.k_proj.weight",
        layer,
        Shape.init(.f32, &.{ kv_dim_i, hidden_i }),
    );
    const k_b = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.k_proj.bias",
        layer,
        Shape.init(.f32, &.{kv_dim_i}),
    );
    const k_flat = try bld.linear(
        hidden_flat,
        k_w,
        k_b,
        @intCast(total_i),
        config.hidden_size,
        config.kvDim(),
    );

    // --- V projection (with bias) ---
    const v_w = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.v_proj.weight",
        layer,
        Shape.init(.f32, &.{ kv_dim_i, hidden_i }),
    );
    const v_b = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.v_proj.bias",
        layer,
        Shape.init(.f32, &.{kv_dim_i}),
    );
    const v_flat = try bld.linear(
        hidden_flat,
        v_w,
        v_b,
        @intCast(total_i),
        config.hidden_size,
        config.kvDim(),
    );

    // ── Reshape Q to `[batch, num_heads, seq, head_dim]` ──────────────
    //
    // After the linear, Q is `[total, num_heads*head_dim]`. We want the
    // head dimension as an explicit axis before RoPE. The typical HF path
    // is: reshape to `[batch, seq, num_heads, head_dim]`, then transpose
    // axes `(0, 2, 1, 3)` to `[batch, num_heads, seq, head_dim]`.
    const q_bsn4_shape = Shape.init(.f32, &.{ batch_i, seq_i, num_heads_i, head_dim_i });
    const q_bsn4 = try bld.reshape(q_flat, q_bsn4_shape);
    const q_bnsd = try bld.transpose(q_bsn4, &.{ 0, 2, 1, 3 });

    // K and V: same pattern with num_kv_heads.
    const k_bsn4_shape = Shape.init(.f32, &.{ batch_i, seq_i, num_kv_i, head_dim_i });
    const k_bsn4 = try bld.reshape(k_flat, k_bsn4_shape);
    const k_bnsd = try bld.transpose(k_bsn4, &.{ 0, 2, 1, 3 });

    const v_bsn4_shape = Shape.init(.f32, &.{ batch_i, seq_i, num_kv_i, head_dim_i });
    const v_bsn4 = try bld.reshape(v_flat, v_bsn4_shape);
    const v_bnsd = try bld.transpose(v_bsn4, &.{ 0, 2, 1, 3 });

    // ── RoPE on Q and K ───────────────────────────────────────────────
    //
    // `Builder.rope` operates on `[B*H, seq, head_dim]`, so we collapse the
    // leading batch/heads axes first, then uncollapse afterwards. We use the
    // same cos/sin tables for both Q (num_heads) and K (num_kv_heads) — they
    // only depend on `seq` and `head_dim`, not the head count.
    const q_bhsd_shape = Shape.init(.f32, &.{ batch_i * num_heads_i, seq_i, head_dim_i });
    const q_merged = try bld.reshape(q_bnsd, q_bhsd_shape);
    const q_rope_merged = try bld.rope(
        q_merged,
        rope_cos,
        rope_sin,
        seq_len,
        config.head_dim,
        config.ropeDim(),
        config.rope_theta,
    );
    const q_bnsd_shape = Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, head_dim_i });
    const q_rope = try bld.reshape(q_rope_merged, q_bnsd_shape);

    const k_bhsd_shape = Shape.init(.f32, &.{ batch_i * num_kv_i, seq_i, head_dim_i });
    const k_merged = try bld.reshape(k_bnsd, k_bhsd_shape);
    const k_rope_merged = try bld.rope(
        k_merged,
        rope_cos,
        rope_sin,
        seq_len,
        config.head_dim,
        config.ropeDim(),
        config.rope_theta,
    );
    const k_kvbnsd_shape = Shape.init(.f32, &.{ batch_i, num_kv_i, seq_i, head_dim_i });
    const k_rope = try bld.reshape(k_rope_merged, k_kvbnsd_shape);

    // ── GQA K/V head fan-out ──────────────────────────────────────────
    //
    // K and V have `num_kv_heads` head groups but we need `num_heads` for
    // the per-query attention computation. Each KV head is reused by
    // `q_heads_per_kv = num_heads / num_kv_heads` query heads.
    //
    // We implement the fan-out with `broadcast_in_dim`, which is primitive
    // (so autodiff handles it) and doesn't duplicate memory at graph-build
    // time. The recipe:
    //
    //   1. reshape K:[B, H_kv, S, D] -> [B, H_kv, 1, S, D]
    //   2. broadcast_in_dim to       [B, H_kv, q_per_kv, S, D]
    //   3. reshape to                [B, H_kv*q_per_kv, S, D] == [B, H, S, D]
    //
    // Same for V. Builder has no `broadcast_in_dim` wrapper, so we call
    // `graph.addNode` directly with a BroadcastAttrs payload.
    const q_per_kv_i: i64 = @divExact(num_heads_i, num_kv_i);

    const k_expanded = try gqaFanOut(bld, k_rope, batch_i, num_kv_i, q_per_kv_i, seq_i, head_dim_i);
    const v_expanded = try gqaFanOut(bld, v_bnsd, batch_i, num_kv_i, q_per_kv_i, seq_i, head_dim_i);

    // ── Attention: scores = Q @ K^T / sqrt(d) ─────────────────────────
    //
    // We intentionally do NOT use `Builder.sdpa` here, because:
    //   (a) sdpa has no causal-mask parameter — it computes bidirectional
    //       attention. Qwen2 needs a causal mask.
    //   (b) sdpa's interface expects Q/K/V pre-merged as `[B*H, S, D]`.
    //       Manually decomposing lets us stay in rank-4 land, which keeps
    //       the broadcasting against the `[1, 1, S, S]` mask straightforward.
    //
    // Shapes at this point:
    //   Q, K, V : [B, H, S, D]
    //
    // scores = Q @ K^T :
    //   lhs_batch     = [0, 1]       (B, H)
    //   lhs_contract  = [3]          (D)
    //   rhs_batch     = [0, 1]
    //   rhs_contract  = [3]
    //   output        = [B, H, S_q, S_k]
    const scores_shape = Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, seq_i });
    const scores = try bld.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = scores_shape,
        .inputs = .{ q_rope, k_expanded, null_node, null_node },
        .num_inputs = 2,
    });

    // Scale by 1/sqrt(head_dim).
    const inv_sqrt_d = try bld.scalarConst(
        .f32,
        1.0 / @sqrt(@as(f32, @floatFromInt(config.head_dim))),
    );
    const scaled_scores = try bld.mul(scores, inv_sqrt_d);

    // Apply causal mask (additive, broadcast `[1, 1, S, S]` over `[B, H, S, S]`).
    const masked_scores = try bld.add(scaled_scores, causal_mask);

    // Softmax along last axis (keys).
    const probs = try bld.softmax(masked_scores);

    // attn = probs @ V : [B, H, S_q, D]
    const attn_shape = Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, head_dim_i });
    const attn_bnsd = try bld.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = attn_shape,
        .inputs = .{ probs, v_expanded, null_node, null_node },
        .num_inputs = 2,
    });

    // ── Reshape back to `[batch*seq, q_dim]` for the output projection ─
    //
    // Transpose `(0, 2, 1, 3)` to `[B, S, H, D]`, then reshape to `[B*S, H*D]`.
    const attn_bsnd = try bld.transpose(attn_bnsd, &.{ 0, 2, 1, 3 });
    const attn_flat_shape = Shape.init(.f32, &.{ total_i, q_dim_i });
    const attn_flat_raw = try bld.reshape(attn_bsnd, attn_flat_shape);
    const attn_flat = if (q_gate == null_node) attn_flat_raw else blk: {
        const gate = try bld.sigmoid(q_gate);
        break :blk try bld.mul(attn_flat_raw, gate);
    };

    // --- O projection (no bias) ---
    const o_w = try parameterFmt(
        bld,
        "model.layers.{d}.self_attn.o_proj.weight",
        layer,
        Shape.init(.f32, &.{ hidden_i, q_dim_i }),
    );
    const o_flat = try bld.linearNoBias(
        attn_flat,
        o_w,
        @intCast(total_i),
        config.qDim(),
        config.hidden_size,
    );

    // Restore the `[batch, seq, hidden]` shape for the residual add.
    const hidden_3d_shape = Shape.init(.f32, &.{ batch_i, seq_i, hidden_i });
    return bld.reshape(o_flat, hidden_3d_shape);
}

// ── SwiGLU MLP ──────────────────────────────────────────────────────────────

fn swigluMlp(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
) !NodeId {
    const batch_i: i64 = @intCast(batch);
    const seq_i: i64 = @intCast(seq_len);
    const hidden_i: i64 = @intCast(config.hidden_size);
    const inter_i: i64 = @intCast(config.intermediate_size);
    const total_i: i64 = batch_i * seq_i;

    const hidden_flat_shape = Shape.init(.f32, &.{ total_i, hidden_i });
    const hidden_flat = try bld.reshape(hidden_in, hidden_flat_shape);

    // --- gate_proj (no bias) ---
    const gate_w = try parameterFmt(
        bld,
        "model.layers.{d}.mlp.gate_proj.weight",
        layer,
        Shape.init(.f32, &.{ inter_i, hidden_i }),
    );
    const gate_linear = try bld.linearNoBias(
        hidden_flat,
        gate_w,
        @intCast(total_i),
        config.hidden_size,
        config.intermediate_size,
    );

    // --- up_proj (no bias) ---
    const up_w = try parameterFmt(
        bld,
        "model.layers.{d}.mlp.up_proj.weight",
        layer,
        Shape.init(.f32, &.{ inter_i, hidden_i }),
    );
    const up_linear = try bld.linearNoBias(
        hidden_flat,
        up_w,
        @intCast(total_i),
        config.hidden_size,
        config.intermediate_size,
    );

    // swigluActivation = silu(gate_linear) * up_linear.
    const activated = try bld.swigluActivation(gate_linear, up_linear);

    // --- down_proj (no bias) ---
    const down_w = try parameterFmt(
        bld,
        "model.layers.{d}.mlp.down_proj.weight",
        layer,
        Shape.init(.f32, &.{ hidden_i, inter_i }),
    );
    const down_linear = try bld.linearNoBias(
        activated,
        down_w,
        @intCast(total_i),
        config.intermediate_size,
        config.hidden_size,
    );

    // Restore `[batch, seq, hidden]` for the residual add.
    const hidden_3d_shape = Shape.init(.f32, &.{ batch_i, seq_i, hidden_i });
    return bld.reshape(down_linear, hidden_3d_shape);
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Emit a `bld.parameter` node with a printf-style name.
///
/// Uses a stack-local 256-byte name buffer — Qwen2 parameter names top out
/// around 60 characters so this is plenty. Returns `error.NameTooLong` if
/// truncated.
fn parameterFmt(
    bld: *Builder,
    comptime fmt_str: []const u8,
    layer: u32,
    shape: Shape,
) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, fmt_str, .{layer}) catch return error.NameTooLong;
    return bld.parameter(name, shape);
}

fn normWeight(bld: *Builder, config: Config, raw_weight: NodeId) !NodeId {
    if (config.norm_weight_offset == 0.0) return raw_weight;
    const offset = try bld.scalarConst(.f32, config.norm_weight_offset);
    return bld.add(raw_weight, offset);
}

fn qwen35CausalDepthwiseConvGraph(
    bld: *Builder,
    input: NodeId,
    weight: NodeId,
    seq_len: u32,
    channels: u32,
    kernel: u32,
) !NodeId {
    if (seq_len == 0 or channels == 0 or kernel == 0) return error.InvalidQwen35LinearAttentionConfig;
    const seq_i: i64 = @intCast(seq_len);
    const channels_i: i64 = @intCast(channels);
    const kernel_i: i64 = @intCast(kernel);
    const input_shape = bld.graph.node(input).output_shape;
    const weight_shape = bld.graph.node(weight).output_shape;
    if (input_shape.rank() != 2 or input_shape.dim(0) != seq_i or input_shape.dim(1) != channels_i) return error.InvalidQwen35LinearAttentionShape;
    if (weight_shape.rank() != 2 or weight_shape.dim(0) != channels_i or weight_shape.dim(1) != kernel_i) return error.InvalidQwen35LinearAttentionShape;

    const allocator = bld.graph.allocator;
    const zero_data = try allocator.alloc(f32, @intCast(channels));
    defer allocator.free(zero_data);
    @memset(zero_data, 0.0);
    const zero_row = try bld.tensorConst(zero_data, Shape.init(.f32, &.{ 1, channels_i }));

    var output: ?NodeId = null;
    var t: u32 = 0;
    while (t < seq_len) : (t += 1) {
        var row_sum: ?NodeId = null;
        var kk: u32 = 0;
        while (kk < kernel) : (kk += 1) {
            const padded_index = @as(i64, @intCast(t)) + @as(i64, @intCast(kk)) + 1 - kernel_i;
            if (padded_index < 0 or padded_index >= seq_i) continue;
            const input_row = try slice2d(
                bld,
                input,
                padded_index,
                padded_index + 1,
                0,
                channels_i,
            );
            const weight_col = try slice2d(
                bld,
                weight,
                0,
                channels_i,
                @intCast(kk),
                @as(i64, @intCast(kk)) + 1,
            );
            const weight_row = try bld.reshape(weight_col, Shape.init(.f32, &.{ 1, channels_i }));
            const term = try bld.mul(input_row, weight_row);
            row_sum = if (row_sum) |acc| try bld.add(acc, term) else term;
        }
        const summed = row_sum orelse zero_row;
        const activated = try bld.silu(summed);
        output = if (output) |prev| try bld.concat(prev, activated, 0) else activated;
    }
    return output orelse error.InvalidQwen35LinearAttentionConfig;
}

fn qwen35LinearAttentionStemGraph(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
) !Qwen35LinearAttentionStem {
    if (batch != 1) return error.UnsupportedQwen35LinearAttentionBatch;
    if (config.qwen35_linear_conv_kernel_dim == 0 or
        config.qwen35_linear_key_head_dim == 0 or
        config.qwen35_linear_value_head_dim == 0 or
        config.qwen35_linear_num_key_heads == 0 or
        config.qwen35_linear_num_value_heads == 0)
    {
        return error.InvalidQwen35LinearAttentionConfig;
    }
    if (config.qwen35_linear_num_value_heads % config.qwen35_linear_num_key_heads != 0) return error.InvalidQwen35LinearAttentionConfig;

    const hidden_i: i64 = @intCast(config.hidden_size);
    const seq_i: i64 = @intCast(seq_len);
    const conv_dim = config.qwen35LinearConvDim();
    const value_dim = config.qwen35LinearValueDim();
    const conv_dim_i: i64 = @intCast(conv_dim);
    const value_dim_i: i64 = @intCast(value_dim);
    const value_heads_i: i64 = @intCast(config.qwen35_linear_num_value_heads);
    const kernel_i: i64 = @intCast(config.qwen35_linear_conv_kernel_dim);

    const hidden_flat = try bld.reshape(hidden_in, Shape.init(.f32, &.{ seq_i, hidden_i }));
    const mixed_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.in_proj_qkv.weight",
        layer,
        Shape.init(.f32, &.{ conv_dim_i, hidden_i }),
    );
    const mixed = try bld.linearNoBias(hidden_flat, mixed_w, seq_len, config.hidden_size, conv_dim);

    const z_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.in_proj_z.weight",
        layer,
        Shape.init(.f32, &.{ value_dim_i, hidden_i }),
    );
    const z = try bld.linearNoBias(hidden_flat, z_w, seq_len, config.hidden_size, value_dim);

    const beta_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.in_proj_b.weight",
        layer,
        Shape.init(.f32, &.{ value_heads_i, hidden_i }),
    );
    const beta_projection = try bld.linearNoBias(hidden_flat, beta_w, seq_len, config.hidden_size, config.qwen35_linear_num_value_heads);

    const a_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.in_proj_a.weight",
        layer,
        Shape.init(.f32, &.{ value_heads_i, hidden_i }),
    );
    const a_projection = try bld.linearNoBias(hidden_flat, a_w, seq_len, config.hidden_size, config.qwen35_linear_num_value_heads);

    const conv_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.conv1d.weight",
        layer,
        Shape.init(.f32, &.{ conv_dim_i, kernel_i }),
    );
    const conv_out = try qwen35CausalDepthwiseConvGraph(
        bld,
        mixed,
        conv_w,
        seq_len,
        conv_dim,
        config.qwen35_linear_conv_kernel_dim,
    );

    return .{
        .conv_out = conv_out,
        .z = z,
        .beta_projection = beta_projection,
        .a_projection = a_projection,
        .conv_dim = conv_dim,
        .value_dim = value_dim,
        .value_heads = config.qwen35_linear_num_value_heads,
    };
}

fn qwen35LinearAttentionGraph(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
) !NodeId {
    const hidden_i: i64 = @intCast(config.hidden_size);
    const value_dim = config.qwen35LinearValueDim();
    const value_dim_i: i64 = @intCast(value_dim);

    const stem = try qwen35LinearAttentionStemGraph(bld, config, hidden_in, batch, seq_len, layer);
    const core = try qwen35RecurrentGatedDeltaRuleGraph(bld, config, stem, seq_len, layer);

    const norm_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.norm.weight",
        layer,
        Shape.init(.f32, &.{@as(i64, @intCast(config.qwen35_linear_value_head_dim))}),
    );
    const gated = try qwen35GatedPerHeadRmsNormGraph(
        bld,
        config,
        core,
        stem.z,
        norm_w,
        seq_len,
        config.qwen35_linear_num_value_heads,
        config.qwen35_linear_value_head_dim,
    );

    const out_w = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.out_proj.weight",
        layer,
        Shape.init(.f32, &.{ hidden_i, value_dim_i }),
    );
    const out_flat = try bld.linearNoBias(gated, out_w, seq_len, value_dim, config.hidden_size);
    return bld.reshape(out_flat, Shape.init(.f32, &.{ 1, @as(i64, @intCast(seq_len)), hidden_i }));
}

fn qwen35RecurrentGatedDeltaRuleGraph(
    bld: *Builder,
    config: Config,
    stem: Qwen35LinearAttentionStem,
    seq_len: u32,
    layer: u32,
) !NodeId {
    if (config.qwen35_linear_num_value_heads % config.qwen35_linear_num_key_heads != 0) return error.InvalidQwen35LinearAttentionConfig;

    const key_heads = config.qwen35_linear_num_key_heads;
    const value_heads = config.qwen35_linear_num_value_heads;
    const key_head_dim = config.qwen35_linear_key_head_dim;
    const value_head_dim = config.qwen35_linear_value_head_dim;
    const key_dim = config.qwen35LinearKeyDim();
    const value_dim = config.qwen35LinearValueDim();
    const repeat = value_heads / key_heads;

    const key_dim_i: i64 = @intCast(key_dim);
    const value_heads_i: i64 = @intCast(value_heads);
    const key_head_dim_i: i64 = @intCast(key_head_dim);
    const value_head_dim_i: i64 = @intCast(value_head_dim);

    const a_log = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.A_log",
        layer,
        Shape.init(.f32, &.{value_heads_i}),
    );
    const dt_bias = try parameterFmt(
        bld,
        "model.layers.{d}.linear_attn.dt_bias",
        layer,
        Shape.init(.f32, &.{value_heads_i}),
    );

    const state_shape = Shape.init(.f32, &.{ key_head_dim_i, value_head_dim_i });
    var states = try bld.graph.allocator.alloc(NodeId, value_heads);
    defer bld.graph.allocator.free(states);
    for (states) |*state| state.* = try zeroLike(bld, state_shape);

    var core: ?NodeId = null;
    var t: u32 = 0;
    while (t < seq_len) : (t += 1) {
        const t_i: i64 = @intCast(t);
        const row = try slice2d(bld, stem.conv_out, t_i, t_i + 1, 0, @intCast(stem.conv_dim));
        const q_all = try slice2d(bld, row, 0, 1, 0, key_dim_i);
        const k_all = try slice2d(bld, row, 0, 1, key_dim_i, key_dim_i * 2);
        const v_all = try slice2d(bld, row, 0, 1, key_dim_i * 2, @intCast(stem.conv_dim));

        var token: ?NodeId = null;
        var vh: u32 = 0;
        while (vh < value_heads) : (vh += 1) {
            const kh = vh / repeat;
            const kh_start: i64 = @as(i64, @intCast(kh)) * key_head_dim_i;
            const vh_start: i64 = @as(i64, @intCast(vh)) * value_head_dim_i;

            const q_src = try slice2d(bld, q_all, 0, 1, kh_start, kh_start + key_head_dim_i);
            const k_src = try slice2d(bld, k_all, 0, 1, kh_start, kh_start + key_head_dim_i);
            const v_src = try slice2d(bld, v_all, 0, 1, vh_start, vh_start + value_head_dim_i);
            const q_norm = try qwen35L2NormRowGraph(bld, q_src);
            const k_norm = try qwen35L2NormRowGraph(bld, k_src);
            const q_scale = try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(key_head_dim))));
            const q = try bld.mul(q_norm, q_scale);

            const beta_raw = try slice2d(bld, stem.beta_projection, t_i, t_i + 1, @intCast(vh), @as(i64, @intCast(vh)) + 1);
            const beta = try bld.sigmoid(beta_raw);
            const a_raw = try slice2d(bld, stem.a_projection, t_i, t_i + 1, @intCast(vh), @as(i64, @intCast(vh)) + 1);
            const a_log_h = try slice1dAsRow(bld, a_log, @intCast(vh), @as(i64, @intCast(vh)) + 1);
            const dt_bias_h = try slice1dAsRow(bld, dt_bias, @intCast(vh), @as(i64, @intCast(vh)) + 1);

            const decay_pre = try bld.add(a_raw, dt_bias_h);
            const softplus = try qwen35SoftplusGraph(bld, decay_pre);
            const neg_a = try bld.neg(try bld.expOp(a_log_h));
            const g = try bld.mul(neg_a, softplus);
            const g_exp = try bld.expOp(g);

            const state_decay = try bld.mul(states[vh], try scalarToShape(bld, g_exp, state_shape));
            const kv_mem = try bld.matmul(k_norm, state_decay);
            const delta = try bld.mul(try bld.sub(v_src, kv_mem), beta);
            const k_t = try bld.transpose(k_norm, &.{ 1, 0 });
            const state_update = try bld.matmul(k_t, delta);
            const state_next = try bld.add(state_decay, state_update);
            states[vh] = state_next;

            const out_head = try bld.matmul(q, state_next);
            token = if (token) |prev| try bld.concat(prev, out_head, 1) else out_head;
        }
        const token_row = token orelse return error.InvalidQwen35LinearAttentionConfig;
        core = if (core) |prev| try bld.concat(prev, token_row, 0) else token_row;
    }

    const result = core orelse return error.InvalidSequenceLength;
    const result_shape = bld.graph.node(result).output_shape;
    if (result_shape.rank() != 2 or result_shape.dim(0) != @as(i64, @intCast(seq_len)) or result_shape.dim(1) != @as(i64, @intCast(value_dim))) {
        return error.InvalidQwen35LinearAttentionShape;
    }
    return result;
}

fn qwen35L2NormRowGraph(bld: *Builder, input: NodeId) !NodeId {
    const squared = try bld.mul(input, input);
    const sum_sq = try bld.reduceSum(squared, &.{1});
    const eps = try bld.scalarConst(.f32, 1e-6);
    const inv = try bld.rsqrt(try bld.add(sum_sq, eps));
    return bld.mul(input, inv);
}

fn qwen35SoftplusGraph(bld: *Builder, input: NodeId) !NodeId {
    const one = try bld.scalarConst(.f32, 1.0);
    const positive = try bld.relu(input);
    const neg_abs = try bld.neg(try bld.absOp(input));
    return bld.add(positive, try bld.logOp(try bld.add(one, try bld.expOp(neg_abs))));
}

fn qwen35GatedPerHeadRmsNormGraph(
    bld: *Builder,
    config: Config,
    core_2d: NodeId,
    z_2d: NodeId,
    raw_norm_weight: NodeId,
    rows: u32,
    heads: u32,
    head_dim: u32,
) !NodeId {
    if (rows == 0 or heads == 0 or head_dim == 0) return error.InvalidQwen35LinearAttentionConfig;
    const rows_i: i64 = @intCast(rows);
    const heads_i: i64 = @intCast(heads);
    const head_dim_i: i64 = @intCast(head_dim);
    const value_dim_i = heads_i * head_dim_i;
    const core_shape = bld.graph.node(core_2d).output_shape;
    const z_shape = bld.graph.node(z_2d).output_shape;
    const norm_shape = bld.graph.node(raw_norm_weight).output_shape;
    if (core_shape.rank() != 2 or core_shape.dim(0) != rows_i or core_shape.dim(1) != value_dim_i) return error.InvalidQwen35LinearAttentionShape;
    if (z_shape.rank() != 2 or z_shape.dim(0) != rows_i or z_shape.dim(1) != value_dim_i) return error.InvalidQwen35LinearAttentionShape;
    if (norm_shape.rank() != 1 or norm_shape.dim(0) != head_dim_i) return error.InvalidQwen35LinearAttentionShape;

    const core_3d_shape = Shape.init(.f32, &.{ rows_i, heads_i, head_dim_i });
    const core_3d = try bld.reshape(core_2d, core_3d_shape);
    const z_3d = try bld.reshape(z_2d, core_3d_shape);

    const squared = try bld.mul(core_3d, core_3d);
    const mean_sq = try bld.reduceMean(squared, &.{2});
    const eps = try bld.scalarConst(.f32, config.rms_norm_eps);
    const inv_rms = try bld.rsqrt(try bld.add(mean_sq, eps));
    const inv_rms_bc = try broadcastInDim(bld, inv_rms, core_3d_shape, &.{ 0, 1 });

    const adjusted_norm = try normWeight(bld, config, raw_norm_weight);
    const norm_bc = try broadcastInDim(bld, adjusted_norm, core_3d_shape, &.{2});
    const gate = try bld.silu(z_3d);

    const normalized = try bld.mul(core_3d, inv_rms_bc);
    const weighted = try bld.mul(normalized, norm_bc);
    const gated = try bld.mul(weighted, gate);
    return bld.reshape(gated, Shape.init(.f32, &.{ rows_i, value_dim_i }));
}

fn broadcastInDim(
    bld: *Builder,
    input: NodeId,
    target_shape: Shape,
    axes: []const u8,
) !NodeId {
    var attrs = node_mod.BroadcastAttrs{ .target_shape = target_shape };
    attrs.num_axes = @intCast(axes.len);
    for (axes, 0..) |axis, idx| attrs.broadcast_axes[idx] = axis;
    return bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = target_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn slice2d(
    bld: *Builder,
    input: NodeId,
    row_start: i64,
    row_end: i64,
    col_start: i64,
    col_end: i64,
) !NodeId {
    const in_shape = bld.graph.node(input).output_shape;
    if (in_shape.rank() != 2) return error.ShapeMismatch;
    if (row_start < 0 or row_end > in_shape.dim(0) or row_start >= row_end) return error.ShapeMismatch;
    if (col_start < 0 or col_end > in_shape.dim(1) or col_start >= col_end) return error.ShapeMismatch;

    var attrs = node_mod.SliceAttrs{};
    attrs.num_axes = 2;
    attrs.starts[0] = row_start;
    attrs.starts[1] = col_start;
    attrs.limits[0] = row_end;
    attrs.limits[1] = col_end;
    attrs.strides[0] = 1;
    attrs.strides[1] = 1;
    const out_shape = Shape.init(in_shape.dtype, &.{ row_end - row_start, col_end - col_start });
    return bld.graph.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = out_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn slice1dAsRow(
    bld: *Builder,
    input: NodeId,
    start: i64,
    end: i64,
) !NodeId {
    const in_shape = bld.graph.node(input).output_shape;
    if (in_shape.rank() != 1) return error.ShapeMismatch;
    if (start < 0 or end > in_shape.dim(0) or start >= end) return error.ShapeMismatch;

    var attrs = node_mod.SliceAttrs{};
    attrs.num_axes = 1;
    attrs.starts[0] = start;
    attrs.limits[0] = end;
    attrs.strides[0] = 1;
    const sliced = try bld.graph.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = Shape.init(in_shape.dtype, &.{end - start}),
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    return bld.reshape(sliced, Shape.init(in_shape.dtype, &.{ 1, end - start }));
}

/// Build a static causal mask as a `[1, 1, seq, seq]` f32 constant.
///
/// Values: 0.0 for j <= i (can attend), -1e9 for j > i (cannot attend).
/// The leading singleton dims are there so that when we add this to
/// pre-softmax scores of shape `[B, H, S, S]`, broadcasting takes care of
/// the batch and head axes automatically.
///
/// The mask is materialized as a proper tensor constant (not a scalar),
/// so the graph's constant pool holds `seq*seq` floats. For typical
/// Qwen2 training shapes (seq=2048 → 16 MB) this is negligible.
fn buildCausalMask(bld: *Builder, seq_len: u32) !NodeId {
    const s: usize = @intCast(seq_len);
    const allocator = bld.graph.allocator;
    var data = try allocator.alloc(f32, s * s);
    defer allocator.free(data);

    const neg_inf: f32 = -1.0e9;
    for (0..s) |i| {
        for (0..s) |j| {
            data[i * s + j] = if (j <= i) 0.0 else neg_inf;
        }
    }

    const mask_shape = Shape.init(.f32, &.{ 1, 1, @as(i64, @intCast(s)), @as(i64, @intCast(s)) });
    return bld.tensorConst(data, mask_shape);
}

/// Grouped-query attention head fan-out:
///   `[B, H_kv, S, D]` -> `[B, H_kv*q_per_kv, S, D]` == `[B, H, S, D]`.
///
/// Implemented as insert-axis + broadcast_in_dim + merge-axes. Every step
/// is a primitive op so autodiff flows through it without hand-written VJPs.
///
/// NOTE on broadcast semantics: `broadcast_in_dim` takes an explicit
/// `broadcast_axes` list naming which output axes each input axis maps to,
/// and synthesizes the rest. For a rank-5 input with a size-1 axis at
/// position 2 broadcast to size `q_per_kv` at output position 2, we set
/// `broadcast_axes = [0, 1, 2, 3, 4]` (identity). The op replicates data
/// along axes where the input is 1 and the target is >1.
fn gqaFanOut(
    bld: *Builder,
    input: NodeId,
    batch_i: i64,
    num_kv_i: i64,
    q_per_kv_i: i64,
    seq_i: i64,
    head_dim_i: i64,
) !NodeId {
    const num_heads_i: i64 = num_kv_i * q_per_kv_i;

    // Fast path: no fan-out needed (num_heads == num_kv_heads).
    if (q_per_kv_i == 1) return input;

    // 1. Insert a size-1 axis: [B, H_kv, S, D] -> [B, H_kv, 1, S, D].
    const inserted_shape = Shape.init(
        .f32,
        &.{ batch_i, num_kv_i, 1, seq_i, head_dim_i },
    );
    const inserted = try bld.reshape(input, inserted_shape);

    // 2. Broadcast along that axis: [B, H_kv, 1, S, D] -> [B, H_kv, q_per_kv, S, D].
    const broadcast_shape = Shape.init(
        .f32,
        &.{ batch_i, num_kv_i, q_per_kv_i, seq_i, head_dim_i },
    );

    var bcast_attrs = node_mod.BroadcastAttrs{ .target_shape = broadcast_shape };
    bcast_attrs.num_axes = 5;
    bcast_attrs.broadcast_axes[0] = 0;
    bcast_attrs.broadcast_axes[1] = 1;
    bcast_attrs.broadcast_axes[2] = 2;
    bcast_attrs.broadcast_axes[3] = 3;
    bcast_attrs.broadcast_axes[4] = 4;

    const broadcasted = try bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = bcast_attrs },
        .output_shape = broadcast_shape,
        .inputs = .{ inserted, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    // 3. Merge H_kv and q_per_kv axes: [B, H_kv, q_per_kv, S, D] -> [B, H, S, D].
    const merged_shape = Shape.init(
        .f32,
        &.{ batch_i, num_heads_i, seq_i, head_dim_i },
    );
    return bld.reshape(broadcasted, merged_shape);
}

// ── Tests ───────────────────────────────────────────────────────────────────
//
// These tests are compile/build-only. They exercise `buildForwardGraph` on
// tiny configs to check that every `Builder` call path we use is reachable
// and that shapes line up. They do NOT execute the graph — that would
// require a populated `WeightStore` and a ComputeBackend, which is handled
// by the colqwen2 trainer harness.

test "buildForwardGraph: tiny Qwen2-shaped config compiles" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .vocab_size = 64,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_kv_heads = 2,
        .head_dim = 4,
        .intermediate_size = 32,
        .max_position_embeddings = 8,
        .rope_theta = 10000.0,
        .rms_norm_eps = 1e-6,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{ 1, 4 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 4, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 4, 4 }));
    const result = try buildForwardGraph(&bld, cfg, 1, 4, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    // Outputs must live in the graph we built.
    try std.testing.expect(result.input_ids_node != null_node);
    try std.testing.expect(result.rope_cos_node != null_node);
    try std.testing.expect(result.rope_sin_node != null_node);
    try std.testing.expect(result.output_node != null_node);

    // Output should be [batch, seq, hidden] = [1, 4, 16].
    const out_shape = graph.node(result.output_node).output_shape;
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 16), out_shape.dim(2));

    // Final node should be a fused RMS norm (the `model.norm` at the top).
    const out_node = graph.node(result.output_node);
    try std.testing.expect(out_node.op.isFused());
}

test "buildForwardGraph: qwen3_5 full-attention layer compiles with gated q projection" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .qwen3_5,
        .vocab_size = 64,
        .hidden_size = 16,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .num_kv_heads = 2,
        .head_dim = 4,
        .intermediate_size = 32,
        .max_position_embeddings = 8,
        .rope_theta = 10000.0,
        .rms_norm_eps = 1e-6,
        .rope_partial_factor = 0.5,
        .norm_weight_offset = 1.0,
        .qwen35_attn_output_gate = true,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{ 1, 4 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 4, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 4, 4 }));
    const result = try buildForwardGraph(&bld, cfg, 1, 4, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    const out_shape = graph.node(result.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 16), out_shape.dim(2));

    var saw_gated_q = false;
    var saw_partial_rope = false;
    var saw_norm_offset = false;
    for (0..graph.nodeCount()) |idx| {
        const node = graph.node(@intCast(idx));
        switch (node.op) {
            .parameter => {
                if (std.mem.eql(u8, graph.parameterName(node), "model.layers.0.self_attn.q_proj.weight")) {
                    saw_gated_q = true;
                    try std.testing.expectEqual(@as(i64, 32), node.output_shape.dim(0));
                    try std.testing.expectEqual(@as(i64, 16), node.output_shape.dim(1));
                }
            },
            .fused_rope => |attrs| {
                saw_partial_rope = true;
                try std.testing.expectEqual(@as(u32, 2), attrs.rope_dim);
                try std.testing.expectEqual(@as(u32, 4), attrs.head_dim);
            },
            .add => {
                const lhs = graph.node(node.inputs[0]);
                const rhs = graph.node(node.inputs[1]);
                if (lhs.op == .parameter and rhs.op == .constant) saw_norm_offset = true;
                if (lhs.op == .constant and rhs.op == .parameter) saw_norm_offset = true;
            },
            else => {},
        }
    }
    try std.testing.expect(saw_gated_q);
    try std.testing.expect(saw_partial_rope);
    try std.testing.expect(saw_norm_offset);
}

test "buildForwardGraph: qwen3_5 linear-attention layer builds recurrent graph" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .qwen3_5,
        .vocab_size = 64,
        .hidden_size = 16,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .num_kv_heads = 2,
        .head_dim = 4,
        .intermediate_size = 32,
        .max_position_embeddings = 8,
        .rms_norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
        .qwen35_has_linear_attention = true,
        .qwen35_full_attention_interval = 4,
        .qwen35_linear_conv_kernel_dim = 2,
        .qwen35_linear_key_head_dim = 4,
        .qwen35_linear_value_head_dim = 4,
        .qwen35_linear_num_key_heads = 1,
        .qwen35_linear_num_value_heads = 2,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{ 1, 4 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 4, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 4, 4 }));
    const result = try buildForwardGraph(&bld, cfg, 1, 4, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    const out_shape = graph.node(result.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 16), out_shape.dim(2));

    var saw_a_log = false;
    var saw_dt_bias = false;
    var saw_norm = false;
    var saw_out_proj = false;
    var saw_silu_gate = false;
    for (0..graph.nodeCount()) |idx| {
        const node = graph.node(@intCast(idx));
        switch (node.op) {
            .parameter => {
                const name = graph.parameterName(node);
                if (std.mem.eql(u8, name, "model.layers.0.linear_attn.A_log")) saw_a_log = true;
                if (std.mem.eql(u8, name, "model.layers.0.linear_attn.dt_bias")) saw_dt_bias = true;
                if (std.mem.eql(u8, name, "model.layers.0.linear_attn.norm.weight")) saw_norm = true;
                if (std.mem.eql(u8, name, "model.layers.0.linear_attn.out_proj.weight")) saw_out_proj = true;
            },
            .fused_silu => saw_silu_gate = true,
            else => {},
        }
    }
    try std.testing.expect(saw_a_log);
    try std.testing.expect(saw_dt_bias);
    try std.testing.expect(saw_norm);
    try std.testing.expect(saw_out_proj);
    try std.testing.expect(saw_silu_gate);
}

test "buildForwardGraph: qwen3_5 hybrid linear and full attention stack builds" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .qwen3_5,
        .vocab_size = 64,
        .hidden_size = 8,
        .num_hidden_layers = 4,
        .num_attention_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 8,
        .rms_norm_eps = 1e-6,
        .rope_partial_factor = 0.5,
        .norm_weight_offset = 1.0,
        .qwen35_has_linear_attention = true,
        .qwen35_full_attention_interval = 4,
        .qwen35_linear_conv_kernel_dim = 2,
        .qwen35_linear_key_head_dim = 4,
        .qwen35_linear_value_head_dim = 4,
        .qwen35_linear_num_key_heads = 1,
        .qwen35_linear_num_value_heads = 2,
        .qwen35_attn_output_gate = true,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{ 1, 2 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 2, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 2, 4 }));
    const result = try buildForwardGraph(&bld, cfg, 1, 2, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    const out_shape = graph.node(result.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(2));

    var saw_linear_layer0 = false;
    var saw_full_layer3 = false;
    for (0..graph.nodeCount()) |idx| {
        const node = graph.node(@intCast(idx));
        if (node.op == .parameter) {
            const name = graph.parameterName(node);
            if (std.mem.eql(u8, name, "model.layers.0.linear_attn.in_proj_qkv.weight")) saw_linear_layer0 = true;
            if (std.mem.eql(u8, name, "model.layers.3.self_attn.q_proj.weight")) saw_full_layer3 = true;
        }
    }
    try std.testing.expect(saw_linear_layer0);
    try std.testing.expect(saw_full_layer3);
}

test "qwen3_5 causal depthwise conv graph uses differentiable primitives" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const input = try bld.parameter("linear_attn.mixed", Shape.init(.f32, &.{ 3, 4 }));
    const weight = try bld.parameter("linear_attn.conv1d.weight.2d", Shape.init(.f32, &.{ 4, 2 }));
    const result = try qwen35CausalDepthwiseConvGraph(&bld, input, weight, 3, 4, 2);

    const out_shape = graph.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));

    var saw_slice = false;
    var saw_concat = false;
    var saw_silu = false;
    var saw_conv_general = false;
    for (0..graph.nodeCount()) |idx| {
        switch (graph.node(@intCast(idx)).op) {
            .slice => saw_slice = true,
            .concat_prim => saw_concat = true,
            .fused_silu => saw_silu = true,
            .conv_general => saw_conv_general = true,
            else => {},
        }
    }

    try std.testing.expect(saw_slice);
    try std.testing.expect(saw_concat);
    try std.testing.expect(saw_silu);
    try std.testing.expect(!saw_conv_general);
}

test "qwen3_5 linear-attention stem builds projections and differentiable conv" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .qwen3_5,
        .vocab_size = 64,
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 8,
        .qwen35_has_linear_attention = true,
        .qwen35_full_attention_interval = 4,
        .qwen35_linear_conv_kernel_dim = 2,
        .qwen35_linear_key_head_dim = 4,
        .qwen35_linear_value_head_dim = 4,
        .qwen35_linear_num_key_heads = 1,
        .qwen35_linear_num_value_heads = 2,
    };

    const hidden = try bld.parameter("hidden", Shape.init(.f32, &.{ 1, 3, 8 }));
    const stem = try qwen35LinearAttentionStemGraph(&bld, cfg, hidden, 1, 3, 0);

    try std.testing.expectEqual(@as(u32, 16), stem.conv_dim);
    try std.testing.expectEqual(@as(u32, 8), stem.value_dim);
    try std.testing.expectEqual(@as(u32, 2), stem.value_heads);
    try std.testing.expectEqual(@as(i64, 3), graph.node(stem.conv_out).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 16), graph.node(stem.conv_out).output_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 3), graph.node(stem.z).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), graph.node(stem.z).output_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 2), graph.node(stem.beta_projection).output_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 2), graph.node(stem.a_projection).output_shape.dim(1));

    var saw_mixed = false;
    var saw_conv = false;
    for (0..graph.nodeCount()) |idx| {
        const node = graph.node(@intCast(idx));
        switch (node.op) {
            .parameter => {
                const name = graph.parameterName(node);
                if (std.mem.eql(u8, name, "model.layers.0.linear_attn.in_proj_qkv.weight")) saw_mixed = true;
                if (std.mem.eql(u8, name, "model.layers.0.linear_attn.conv1d.weight")) saw_conv = true;
            },
            .conv_general => return error.UnexpectedConvGeneral,
            else => {},
        }
    }
    try std.testing.expect(saw_mixed);
    try std.testing.expect(saw_conv);
}

test "qwen3_5 recurrent gated delta rule graph is differentiable" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .qwen3_5,
        .vocab_size = 64,
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 8,
        .rms_norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
        .qwen35_has_linear_attention = true,
        .qwen35_full_attention_interval = 4,
        .qwen35_linear_conv_kernel_dim = 2,
        .qwen35_linear_key_head_dim = 4,
        .qwen35_linear_value_head_dim = 4,
        .qwen35_linear_num_key_heads = 1,
        .qwen35_linear_num_value_heads = 2,
    };

    const conv_out = try bld.parameter("linear_attn.conv_out", Shape.init(.f32, &.{ 2, 16 }));
    const beta = try bld.parameter("linear_attn.beta", Shape.init(.f32, &.{ 2, 2 }));
    const a = try bld.parameter("linear_attn.a", Shape.init(.f32, &.{ 2, 2 }));
    const stem = Qwen35LinearAttentionStem{
        .conv_out = conv_out,
        .z = null_node,
        .beta_projection = beta,
        .a_projection = a,
        .conv_dim = 16,
        .value_dim = 8,
        .value_heads = 2,
    };
    const core = try qwen35RecurrentGatedDeltaRuleGraph(&bld, cfg, stem, 2, 0);
    try std.testing.expect(graph.node(core).output_shape.eq(Shape.init(.f32, &.{ 2, 8 })));

    var a_log: NodeId = null_node;
    var dt_bias: NodeId = null_node;
    for (graph.parameters.items) |param_id| {
        const param = graph.node(param_id);
        const name = graph.parameterName(param);
        if (std.mem.eql(u8, name, "model.layers.0.linear_attn.A_log")) a_log = param_id;
        if (std.mem.eql(u8, name, "model.layers.0.linear_attn.dt_bias")) dt_bias = param_id;
    }
    try std.testing.expect(a_log != null_node);
    try std.testing.expect(dt_bias != null_node);

    const loss = try bld.reduceSum(core, &.{ 0, 1 });
    try graph.markOutput(loss);
    var grad = try ml.graph.autodiff.gradient(allocator, &graph, loss, &.{ conv_out, beta, a, a_log, dt_bias });
    defer grad.deinit();
    for (grad.param_grads) |param_grad| try std.testing.expect(param_grad != null_node);
}

test "qwen3_5 gated per-head rmsnorm graph is differentiable" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .qwen3_5,
        .vocab_size = 64,
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 8,
        .rms_norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    };

    const core = try bld.parameter("linear_attn.core", Shape.init(.f32, &.{ 3, 8 }));
    const z = try bld.parameter("linear_attn.z", Shape.init(.f32, &.{ 3, 8 }));
    const norm_weight = try bld.parameter("model.layers.0.linear_attn.norm.weight", Shape.init(.f32, &.{4}));
    const result = try qwen35GatedPerHeadRmsNormGraph(&bld, cfg, core, z, norm_weight, 3, 2, 4);
    const out_shape = graph.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(1));

    const loss = try bld.reduceSum(result, &.{ 0, 1 });
    try graph.markOutput(loss);
    var grad = try ml.graph.autodiff.gradient(allocator, &graph, loss, &.{ core, z, norm_weight });
    defer grad.deinit();
    try std.testing.expect(grad.param_grads[0] != null_node);
    try std.testing.expect(grad.param_grads[1] != null_node);
    try std.testing.expect(grad.param_grads[2] != null_node);

    var saw_reduce_head_dim = false;
    var saw_inv_rms_broadcast = false;
    var saw_norm_broadcast = false;
    var saw_gate = false;
    for (0..graph.nodeCount()) |idx| {
        switch (graph.node(@intCast(idx)).op) {
            .reduce_mean => |attrs| {
                if (attrs.num_axes == 1 and attrs.axes[0] == 2) saw_reduce_head_dim = true;
            },
            .broadcast_in_dim => |attrs| {
                if (attrs.num_axes == 2 and attrs.broadcast_axes[0] == 0 and attrs.broadcast_axes[1] == 1) {
                    saw_inv_rms_broadcast = true;
                }
                if (attrs.num_axes == 1 and attrs.broadcast_axes[0] == 2) {
                    saw_norm_broadcast = true;
                }
            },
            .fused_silu => saw_gate = true,
            else => {},
        }
    }
    try std.testing.expect(saw_reduce_head_dim);
    try std.testing.expect(saw_inv_rms_broadcast);
    try std.testing.expect(saw_norm_broadcast);
    try std.testing.expect(saw_gate);
}

test "buildForwardGraph: multi-head-no-fanout (num_heads == num_kv_heads)" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    // num_attention_heads == num_kv_heads: gqaFanOut takes the fast path.
    const cfg = Config{
        .vocab_size = 32,
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .num_kv_heads = 2,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 4,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{ 1, 2 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 2, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 2, 4 }));
    _ = try buildForwardGraph(&bld, cfg, 1, 2, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });
}

test "buildForwardGraph: parameter names include HF suffixes" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .vocab_size = 32,
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 4,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{ 1, 2 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 2, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 2, 4 }));
    _ = try buildForwardGraph(&bld, cfg, 1, 2, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    // Walk parameters and check for the key HF-style substrings that
    // `ml.graph.lora.injectLoRA` pattern-matches on.
    var saw_q_proj = false;
    var saw_k_proj = false;
    var saw_v_proj = false;
    var saw_o_proj = false;
    var saw_gate_proj = false;
    var saw_up_proj = false;
    var saw_down_proj = false;
    var saw_embed = false;
    var saw_final_norm = false;

    for (graph.parameters.items) |pid| {
        const name = graph.parameterName(graph.node(pid));
        if (std.mem.indexOf(u8, name, "q_proj.weight") != null) saw_q_proj = true;
        if (std.mem.indexOf(u8, name, "k_proj.weight") != null) saw_k_proj = true;
        if (std.mem.indexOf(u8, name, "v_proj.weight") != null) saw_v_proj = true;
        if (std.mem.indexOf(u8, name, "o_proj.weight") != null) saw_o_proj = true;
        if (std.mem.indexOf(u8, name, "gate_proj.weight") != null) saw_gate_proj = true;
        if (std.mem.indexOf(u8, name, "up_proj.weight") != null) saw_up_proj = true;
        if (std.mem.indexOf(u8, name, "down_proj.weight") != null) saw_down_proj = true;
        if (std.mem.eql(u8, name, "model.embed_tokens.weight")) saw_embed = true;
        if (std.mem.eql(u8, name, "model.norm.weight")) saw_final_norm = true;
    }

    try std.testing.expect(saw_q_proj);
    try std.testing.expect(saw_k_proj);
    try std.testing.expect(saw_v_proj);
    try std.testing.expect(saw_o_proj);
    try std.testing.expect(saw_gate_proj);
    try std.testing.expect(saw_up_proj);
    try std.testing.expect(saw_down_proj);
    try std.testing.expect(saw_embed);
    try std.testing.expect(saw_final_norm);
}

test "Config derived dims match Qwen2-0.5B" {
    const cfg = Config{
        .vocab_size = 151_936,
        .hidden_size = 896,
        .num_hidden_layers = 24,
        .num_attention_heads = 14,
        .num_kv_heads = 2,
        .head_dim = 64,
        .intermediate_size = 4864,
        .max_position_embeddings = 32_768,
    };
    try std.testing.expectEqual(@as(u32, 7), cfg.qHeadsPerKv());
    try std.testing.expectEqual(@as(u32, 896), cfg.qDim());
    try std.testing.expectEqual(@as(u32, 128), cfg.kvDim());
}
