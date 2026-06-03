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

// ModernBERT encoder forward pass as an `ml.graph.Graph`, built with the
// high-level `Builder` API. This is the graph-building sibling of
// `src/architectures/modern_bert.zig`, whose ComputeBackend-based eager
// forward serves as the reference implementation.
//
// Why this exists
// ---------------
// The "level-3" training harness (`real_autodiff_trainer.zig`) requires
// models to be expressed as a computation graph so that
// `ml.graph.autodiff.gradient` can differentiate every op and
// `ml.graph.lora.injectLoRA` can splice LoRA adapters onto the linear
// projections by parameter-name substring. Once this port exists,
// `fused_chunker` — which is built on ModernBERT — can graduate from
// "head-only autodiff" to full-encoder multi-layer LoRA training.
//
// MVP simplifications (TODO)
// --------------------------
// 1. **No windowed attention.** ModernBERT's real architecture alternates
//    global (full) attention layers with local (sliding-window) attention
//    layers. This port uses full bidirectional attention on every layer.
//    The attn_bias placeholder is simply an additive padding mask. Adding
//    local attention means pre-building a banded mask per layer and
//    selecting it by `layer_idx % global_attn_every_n_layers`. Deferred.
// 2. **No LoRA bookkeeping.** `buildForwardGraph` emits plain parameter
//    nodes; LoRA is injected after construction by
//    `ml.graph.lora.injectLoRA`, which matches on substrings like
//    `"query_proj"` / `"value_proj"`.
// 3. **Separate Q/K/V and gate/up parameters** — see "Parameter naming".
//
// Parameter naming
// ----------------
// Names mirror `src/architectures/modern_bert.zig`, with one intentional
// divergence: HF ModernBERT ships `mlp.Wi.weight` as a fused
// `[2*intermediate, hidden]` matrix that the eager path projects and then
// splits on the host CPU. Expressing that split in graph land means a
// `slice` primitive call per forward, which complicates autodiff and LoRA
// routing for no win. Instead we declare separate `mlp.gate_proj.weight`
// and `mlp.up_proj.weight` parameters of shape `[intermediate, hidden]`
// each. **The weight loader is responsible for splitting HF's fused
// `mlp.Wi.weight` into these two halves at load time** — the first
// `intermediate` rows become `gate_proj`, the second `intermediate` rows
// become `up_proj`. The remaining ModernBERT weight names are unchanged
// from the HF checkpoint:
//
//   model.embeddings.tok_embeddings.weight
//   model.embeddings.norm.{weight,bias}
//   model.layers.{i}.attn_norm.{weight,bias}
//   model.layers.{i}.attn.query_proj.{weight,bias}
//   model.layers.{i}.attn.key_proj.{weight,bias}
//   model.layers.{i}.attn.value_proj.{weight,bias}
//   model.layers.{i}.attn.Wo.{weight,bias}
//   model.layers.{i}.mlp_norm.{weight,bias}
//   model.layers.{i}.mlp.gate_proj.weight        (split from mlp.Wi)
//   model.layers.{i}.mlp.up_proj.weight          (split from mlp.Wi)
//   model.layers.{i}.mlp.Wo.weight
//   model.final_norm.{weight,bias}
//
// Input convention
// ----------------
// Following the new graph-port convention, `buildForwardGraph` does NOT
// create its own input placeholders. The caller builds `input_ids`,
// `attn_bias`, `rope_cos`, and `rope_sin` as parameter nodes (or constant
// nodes) ahead of time and threads them in. The returned struct echoes
// those NodeIds back for convenience.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

// ── Public Config ───────────────────────────────────────────────────────────

pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    head_dim: u32,
    intermediate_size: u32,
    max_position_embeddings: u32 = 8192,
    layer_norm_eps: f32 = 1e-5,
    rope_theta: f32 = 160000.0,
    /// Global attention every N layers (MVP: ignored; all layers are full attention).
    global_attn_every_n_layers: u32 = 3,
};

/// Result of graph construction. All NodeIds live in the `Graph` that was
/// passed to `buildForwardGraph` via its `Builder`.
pub const ModernBertGraph = struct {
    /// Input placeholder: `[batch * seq_len]` i64 token IDs — echoed from
    /// the caller-provided argument.
    input_ids_node: NodeId,
    /// Input placeholder: `[batch * num_heads, seq_len, seq_len]` f32 additive
    /// attention bias. Caller builds it (0 for valid positions, -1e9 for
    /// masked / padded). Passed in so this function does not mint a new
    /// placeholder; mirrors bert_graph / qwen2_graph convention.
    attn_bias_node: NodeId,
    /// RoPE cos table, `[seq_len, head_dim]` f32. Caller-precomputed for
    /// `config.rope_theta`.
    rope_cos_node: NodeId,
    /// RoPE sin table, `[seq_len, head_dim]` f32.
    rope_sin_node: NodeId,
    /// Output: `[batch * seq_len, hidden_size]` — final encoder hidden
    /// state after the model-level LayerNorm (`model.final_norm`).
    output_node: NodeId,
};

// ── Entry point ─────────────────────────────────────────────────────────────

/// Construct the ModernBERT encoder forward graph.
///
/// Weight parameters are emitted as `bld.parameter` nodes using the HF
/// ModernBERT checkpoint naming convention (see file header for the one
/// intentional divergence around `mlp.Wi`). No weights are bound — the
/// caller is responsible for populating the parameter store with tensor
/// data before execution.
///
/// `input_ids`, `attn_bias`, `rope_cos`, `rope_sin` must already be valid
/// NodeIds in the graph (caller-provided placeholders or constants). The
/// `batch` and `seq_len` arguments are baked into shape metadata; rebuild
/// the graph if either changes.
pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    attn_bias: NodeId,
    rope_cos: NodeId,
    rope_sin: NodeId,
    batch: u32,
    seq_len: u32,
) !ModernBertGraph {
    const H: u32 = config.hidden_size;
    const total: u32 = batch * seq_len;
    const num_heads: u32 = config.num_attention_heads;
    const head_dim: u32 = config.head_dim;

    if (num_heads * head_dim != H) return error.InvalidHeadDim;

    // ── Embeddings: token lookup + embedding LayerNorm ─────────────────
    //
    // ModernBERT has no absolute position embeddings — RoPE is applied
    // inside each attention layer instead. So the embedding block is just
    // `tok_embeddings → LayerNorm(weight, bias)`, where both tensors are
    // under `model.embeddings.*`.
    var hidden = try embeddings(bld, config, input_ids, total, H);

    // ── Encoder layers ─────────────────────────────────────────────────
    var layer: u32 = 0;
    while (layer < config.num_hidden_layers) : (layer += 1) {
        hidden = try encoderLayer(
            bld,
            config,
            hidden,
            attn_bias,
            rope_cos,
            rope_sin,
            batch,
            seq_len,
            layer,
            num_heads,
            head_dim,
        );
    }

    // ── Final LayerNorm ────────────────────────────────────────────────
    //
    // `model.final_norm` is a LayerNorm over the hidden dimension. The
    // pre-norm residual pattern inside each layer means the unnormalized
    // residual stream needs one last normalization before exiting the
    // encoder — otherwise downstream heads see raw pre-norm activations.
    const final_norm_w = try bld.parameter(
        "model.final_norm.weight",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const final_norm_b = try bld.parameter(
        "model.final_norm.bias",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const output = try bld.layerNorm(hidden, final_norm_w, final_norm_b, H, config.layer_norm_eps);

    return .{
        .input_ids_node = input_ids,
        .attn_bias_node = attn_bias,
        .rope_cos_node = rope_cos,
        .rope_sin_node = rope_sin,
        .output_node = output,
    };
}

// ── Embeddings block ────────────────────────────────────────────────────────

fn embeddings(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    total: u32,
    H: u32,
) !NodeId {
    // Token embeddings: `[vocab_size, hidden]` lookup indexed by flat token ids.
    const tok_emb_param = try bld.parameter(
        "model.embeddings.tok_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.vocab_size), @intCast(H) }),
    );
    const tok_emb = try bld.embeddingLookup(tok_emb_param, input_ids, total, H);

    // Embedding-level LayerNorm (ModernBERT replaces BERT's post-sum norm
    // with a pre-encoder norm).
    const ln_w = try bld.parameter(
        "model.embeddings.norm.weight",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const ln_b = try bld.parameter(
        "model.embeddings.norm.bias",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    return bld.layerNorm(tok_emb, ln_w, ln_b, H, config.layer_norm_eps);
}

// ── Single encoder layer ────────────────────────────────────────────────────
//
// Pre-norm structure:
//
//   x1 = LayerNorm(x)                        (attn_norm)
//   x2 = x + Attention(x1)                   (residual)
//   x3 = LayerNorm(x2)                       (mlp_norm)
//   out = x2 + GeGLU_MLP(x3)                 (residual)
//
// Both residual adds are on the unnormalized stream — the normed tensor
// is fed into the sub-layer, but the residual flows around it.

fn encoderLayer(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    attn_bias: NodeId,
    rope_cos: NodeId,
    rope_sin: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const H: u32 = config.hidden_size;
    const total: u32 = batch * seq_len;

    // ── Pre-attention LayerNorm ────────────────────────────────────────
    const attn_ln_w = try layerParam(bld, layer, "attn_norm.weight", .{H});
    const attn_ln_b = try layerParam(bld, layer, "attn_norm.bias", .{H});
    const attn_normed = try bld.layerNorm(hidden_in, attn_ln_w, attn_ln_b, H, config.layer_norm_eps);

    // ── Q / K / V projections (with bias — HF ModernBERT keeps bias on attn) ─
    //
    // Names match `src/architectures/modern_bert.zig` so both the eager
    // and graph code paths can share a weight loader.
    const q_w = try layerParam(bld, layer, "attn.query_proj.weight", .{ H, H });
    const q_b = try layerParam(bld, layer, "attn.query_proj.bias", .{H});
    const Q_flat = try bld.linear(attn_normed, q_w, q_b, total, H, H);

    const k_w = try layerParam(bld, layer, "attn.key_proj.weight", .{ H, H });
    const k_b = try layerParam(bld, layer, "attn.key_proj.bias", .{H});
    const K_flat = try bld.linear(attn_normed, k_w, k_b, total, H, H);

    const v_w = try layerParam(bld, layer, "attn.value_proj.weight", .{ H, H });
    const v_b = try layerParam(bld, layer, "attn.value_proj.bias", .{H});
    const V_flat = try bld.linear(attn_normed, v_w, v_b, total, H, H);

    // ── Reshape Q/K/V into per-head layout `[B*H, S, D]` ────────────────
    //
    // Start from `[B*S, H*D]`, reshape to `[B, S, H, D]`, transpose to
    // `[B, H, S, D]`, then flatten leading axes to `[B*H, S, D]`. This is
    // the rank-3 layout matmul3D expects.
    const q_bhsd = try splitHeads(bld, Q_flat, batch, seq_len, num_heads, head_dim);
    const k_bhsd = try splitHeads(bld, K_flat, batch, seq_len, num_heads, head_dim);
    const v_bhsd = try splitHeads(bld, V_flat, batch, seq_len, num_heads, head_dim);

    // ── Apply RoPE to Q and K (bidirectional — same cos/sin tables) ─────
    //
    // ModernBERT rotates the full head dimension (`rope_dim == head_dim`).
    // RoPE does not depend on attention direction, so the same `rope_cos`
    // / `rope_sin` tables that would be used by a causal decoder apply
    // here. V is not rotated.
    const q_roped = try bld.rope(
        q_bhsd,
        rope_cos,
        rope_sin,
        seq_len,
        head_dim,
        head_dim,
        config.rope_theta,
    );
    const k_roped = try bld.rope(
        k_bhsd,
        rope_cos,
        rope_sin,
        seq_len,
        head_dim,
        head_dim,
        config.rope_theta,
    );

    // ── Manual masked scaled dot-product attention ─────────────────────
    //
    // Mirrors bert_graph.zig's decomposition so the autodiff VJPs line up.
    // scores = Q @ K^T / sqrt(d), shape `[B*H, S, S]`.
    const k_t = try bld.transpose(k_roped, &.{ 0, 2, 1 });
    const scores_raw = try bld.matmul3D(q_roped, k_t);

    const inv_sqrt_d = try bld.scalarConst(
        .f32,
        1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))),
    );
    const scores_scaled = try bld.mul(scores_raw, inv_sqrt_d);

    // Additive attention bias. The caller is expected to have shaped this
    // as `[B*H, S, S]` (or broadcastable to that) with 0 on valid positions
    // and -1e9 on masked ones. Bidirectional attention: NO causal mask is
    // applied — the bias is pure padding mask.
    const scores_masked = try bld.add(scores_scaled, attn_bias);

    // Softmax along the last axis (keys).
    const probs = try bld.softmax(scores_masked);

    // attn_out = probs @ V, shape `[B*H, S, D]`.
    const attn_bhsd = try bld.matmul3D(probs, v_bhsd);

    // ── Merge heads back to `[B*S, H*D]` for the output projection ─────
    const attn_merged = try mergeHeads(bld, attn_bhsd, batch, seq_len, num_heads, head_dim);

    // ── Output projection (with bias) ──────────────────────────────────
    const o_w = try layerParam(bld, layer, "attn.Wo.weight", .{ H, H });
    const o_b = try layerParam(bld, layer, "attn.Wo.bias", .{H});
    const attn_proj = try bld.linear(attn_merged, o_w, o_b, total, H, H);

    // Pre-norm residual: add projected attention output back onto the
    // *unnormalized* hidden state.
    const hidden_after_attn = try bld.add(attn_proj, hidden_in);

    // ── Pre-MLP LayerNorm ──────────────────────────────────────────────
    const mlp_ln_w = try layerParam(bld, layer, "mlp_norm.weight", .{H});
    const mlp_ln_b = try layerParam(bld, layer, "mlp_norm.bias", .{H});
    const mlp_normed = try bld.layerNorm(
        hidden_after_attn,
        mlp_ln_w,
        mlp_ln_b,
        H,
        config.layer_norm_eps,
    );

    // ── GeGLU MLP ──────────────────────────────────────────────────────
    //
    // GeGLU = GELU-gated linear unit:
    //
    //   gate   = mlp_normed @ gate_proj^T     [total, intermediate]
    //   up     = mlp_normed @ up_proj^T       [total, intermediate]
    //   act    = GELU(gate) * up              [total, intermediate]
    //   out    = act @ Wo^T                   [total, hidden]
    //
    // All three linears are bias-free in HF ModernBERT's MLP. No
    // `Builder.geGluActivation` wrapper exists yet — we compose it out of
    // primitives. swigluActivation() is the closest analogue but uses SiLU
    // instead of GELU, so it's not quite what we want.
    const I: u32 = config.intermediate_size;

    const gate_w = try layerParam(bld, layer, "mlp.gate_proj.weight", .{ I, H });
    const gate_linear = try bld.linearNoBias(mlp_normed, gate_w, total, H, I);

    const up_w = try layerParam(bld, layer, "mlp.up_proj.weight", .{ I, H });
    const up_linear = try bld.linearNoBias(mlp_normed, up_w, total, H, I);

    const gate_gelu = try bld.gelu(gate_linear);
    const activated = try bld.elemMultiply(gate_gelu, up_linear);

    const down_w = try layerParam(bld, layer, "mlp.Wo.weight", .{ H, I });
    const mlp_out = try bld.linearNoBias(activated, down_w, total, I, H);

    // Pre-norm residual on the post-attention stream.
    return bld.add(mlp_out, hidden_after_attn);
}

// ── Head-split / merge helpers ──────────────────────────────────────────────

/// `[B*S, H*D] -> [B*H, S, D]`
///
/// Reshape to `[B, S, H, D]`, transpose `(0, 2, 1, 3)`, flatten batch/head.
fn splitHeads(
    bld: *Builder,
    flat: NodeId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const bsnh = try bld.reshape(
        flat,
        Shape.init(.f32, &.{
            @intCast(batch),
            @intCast(seq_len),
            @intCast(num_heads),
            @intCast(head_dim),
        }),
    );
    const bnsh = try bld.transpose(bsnh, &.{ 0, 2, 1, 3 });
    return bld.reshape(
        bnsh,
        Shape.init(.f32, &.{
            @intCast(batch * num_heads),
            @intCast(seq_len),
            @intCast(head_dim),
        }),
    );
}

/// `[B*H, S, D] -> [B*S, H*D]` (inverse of splitHeads).
fn mergeHeads(
    bld: *Builder,
    bhsd: NodeId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const bnsh = try bld.reshape(
        bhsd,
        Shape.init(.f32, &.{
            @intCast(batch),
            @intCast(num_heads),
            @intCast(seq_len),
            @intCast(head_dim),
        }),
    );
    const bsnh = try bld.transpose(bnsh, &.{ 0, 2, 1, 3 });
    return bld.reshape(
        bsnh,
        Shape.init(.f32, &.{
            @intCast(batch * seq_len),
            @intCast(num_heads * head_dim),
        }),
    );
}

// ── Parameter helper ────────────────────────────────────────────────────────

/// Emit a `bld.parameter` node named `model.layers.{layer}.{suffix}` with a
/// 1- or 2-D f32 shape described by `dims` (a tuple / anonymous struct of
/// `u32` values). Mirrors `bert_graph.zig`'s helper one-for-one so the two
/// files can drift in parallel without surprise.
fn layerParam(bld: *Builder, layer: u32, suffix: []const u8, dims: anytype) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(
        &name_buf,
        "model.layers.{d}.{s}",
        .{ layer, suffix },
    );
    // The parameter store duplicates names internally, but we still need to
    // dupe here because bufPrint writes into stack memory that will be
    // reused on the next call. Dupe into the graph's arena so the lifetime
    // matches the parameter node.
    const owned_name = try bld.graph.allocator.dupe(u8, name);

    const d = dims;
    const shape = switch (@typeInfo(@TypeOf(d)).@"struct".fields.len) {
        1 => Shape.init(.f32, &.{@intCast(d[0])}),
        2 => Shape.init(.f32, &.{ @intCast(d[0]), @intCast(d[1]) }),
        else => @compileError("layerParam: only 1-D or 2-D dims supported"),
    };
    return bld.parameter(owned_name, shape);
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "buildForwardGraph constructs ModernBERT graph with correct output shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .head_dim = 8,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
    };

    const batch: u32 = 1;
    const seq_len: u32 = 8;
    const total: i64 = @intCast(batch * seq_len);
    const head_dim_i: i64 = @intCast(config.head_dim);
    const bh_i: i64 = @intCast(batch * config.num_attention_heads);
    const seq_i: i64 = @intCast(seq_len);

    // Caller-provided input placeholders — per the new convention.
    const input_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.i64, &.{total}),
    );
    const attn_bias = try bld.parameter(
        "__attn_bias",
        Shape.init(.f32, &.{ bh_i, seq_i, seq_i }),
    );
    const rope_cos = try bld.parameter(
        "__rope_cos",
        Shape.init(.f32, &.{ seq_i, head_dim_i }),
    );
    const rope_sin = try bld.parameter(
        "__rope_sin",
        Shape.init(.f32, &.{ seq_i, head_dim_i }),
    );

    const mb_graph = try buildForwardGraph(
        &bld,
        config,
        input_ids,
        attn_bias,
        rope_cos,
        rope_sin,
        batch,
        seq_len,
    );

    // Output shape should be [batch*seq, hidden] = [8, 32].
    const out_shape = g.node(mb_graph.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(1));

    // Sanity check on emitted parameter count. Per layer we emit:
    //   - attn_norm (w, b)                                2
    //   - Q/K/V proj (w, b) × 3                           6
    //   - Wo (w, b)                                       2
    //   - mlp_norm (w, b)                                 2
    //   - gate_proj (w), up_proj (w), Wo (w)              3
    //                                                    = 15
    // Plus embeddings: tok_embeddings.weight + embeddings.norm.{w,b} = 3
    // Plus final_norm.{w,b} = 2
    // Plus 4 caller-provided input placeholders.
    // Total = 3 + 2 + 2*15 + 4 = 39.
    try std.testing.expect(g.parameters.items.len >= 35);
}
