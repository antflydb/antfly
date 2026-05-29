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

// GLiNER2 span-classification head as an `ml.graph` computation graph.
// Counterpart to `architectures/gliner_head.zig` (eager) the same way
// `deberta_graph.zig` is to `deberta.zig`.
//
// The exported builders are used both for head-only execution and for the full
// GLiNER2 DeBERTa+head graph path in `runFullGraph`.
//
// ┌─────────────────────────────────────────────────────────────────────┐
// │ WHAT GETS BUILT (mirrors gliner_head.zig op-by-op)                 │
// │                                                                     │
// │ Inputs (caller-supplied placeholders):                              │
// │   hidden       [batch * seq_len, H]   (encoder output, from        │
// │                                        deberta_graph.buildForwardGraph) │
// │   input_ids    [batch * seq_len]       i64                          │
// │   words_mask   [batch * seq_len]       i64                          │
// │   span_idx     [batch * num_spans * 2] i64                          │
// │                                                                     │
// │ Output:                                                             │
// │   logits       [batch * num_spans, num_labels]                      │
// │                                                                     │
// │ Internal flow (per gliner_head.zig):                                │
// │   1.  extractWordEmbeddings: scatter_add(hidden) by words_mask,     │
// │       divide by counts -> [batch * num_words, H]                    │
// │   2.  extractLabelEmbeddings: gather(hidden) at positions where     │
// │       input_ids == entity_token_id -> [num_labels, H]               │
// │   3.  spanMarkerForward:                                            │
// │       a. project_start  MLP (H -> 4H -> H)                          │
// │       b. project_end    MLP (H -> 4H -> H)                          │
// │       c. gather start_proj[start_idx], end_proj[end_idx]            │
// │       d. concat along last dim -> [total_spans, 2H]                 │
// │       e. relu                                                       │
// │       f. out_project    MLP (2H -> 4H -> H)                         │
// │   4.  countLstmForward:                                             │
// │       a. broadcast pos_embedding[0] to [num_labels, H]              │
// │       b. GRU single step (3-gate: r, z, n) -> [num_labels, H]       │
// │       c. residual: combined = gru_out + label_embeddings            │
// │       d. DownscaledTransformer:                                     │
// │          - in_projector linear (H -> D=128)                         │
// │          - 2 x mini-transformer encoder layer (4 heads @ 32 dim)    │
// │          - concat([transformer_out, combined]) -> [num_labels, D+H] │
// │          - out_projector 3-layer MLP (D+H -> H)                     │
// │   5.  scoring: span_rep @ label_proj^T -> [total_spans, num_labels] │
// └─────────────────────────────────────────────────────────────────────┘
//
// Implementation notes for the future work:
//
//  - Every weight name matches `gliner_head.zig` so a single
//    WeightStore + LoRA injector covers both eager and graph paths.
//  - Most ops (matmul, gather, scatter_add, concat, layerNorm, relu,
//    add) already exist as Builder nodes; sigmoid/tanh for the GRU
//    step and gelu/relu for the FFN do too.
//  - The mini-transformer's QKV split is best done with the same
//    `splitLastDim3` pattern the eager path now uses.  Builder will
//    need a corresponding op or a `gather_axis` workaround.
//  - The Toeplitz gather pattern from deberta_graph is reusable for
//    the start/end span gather (different index source, same shape).
//
// Once the body lands, `session_factory`'s gliner branch can route
// through a single `graph.Runtime.execute(combined_graph)` call --
// `partitioned`/`compiled_preferred` strategies then become real
// options for GLiNER on Metal/CoreML/PJRT.

const std = @import("std");
const ml = @import("ml");
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;

const deberta_config = @import("../models/deberta.zig");

/// Configuration for the GLiNER2 head graph.  Mirrors the runtime
/// `gliner_head.zig` parameters and reuses `deberta_config.Config`'s
/// hidden size and GLiNER label-marker token ids directly.
pub const Config = struct {
    /// DeBERTa hidden dim (H).  Span/label embeddings are sized H.
    hidden_size: u32,
    /// Special tokens marking labels in the input prompt.
    classification_token_id: i64 = 0,
    entity_token_id: i64,
    relation_token_id: i64 = 0,
    /// Downscaled-transformer hidden dim (D=128 in stock GLiNER2).
    downscaled_dim: u32 = 128,
    /// FFN inner dim of the downscaled transformer (D_FFN=256).
    downscaled_ffn_dim: u32 = 256,
    /// Number of mini-transformer layers (2 in stock GLiNER2).
    downscaled_num_layers: u32 = 2,
    /// Number of attention heads in the mini-transformer (4).
    downscaled_num_heads: u32 = 4,
};

pub const LabelMarkerTokens = struct {
    classification: i64 = 0,
    entity: i64,
    relation: i64 = 0,

    fn fromEntityToken(entity_token_id: i64) LabelMarkerTokens {
        return .{ .entity = entity_token_id };
    }

    fn fromConfig(config: Config) LabelMarkerTokens {
        return .{
            .classification = config.classification_token_id,
            .entity = config.entity_token_id,
            .relation = config.relation_token_id,
        };
    }
};

/// Result of `buildForwardGraph`.  All node IDs are graph placeholders
/// that the caller binds at execution time (input_ids, words_mask,
/// span_idx are integer tensors; hidden is the f32 encoder output).
pub const GlinerHeadGraph = struct {
    /// f32 [batch * seq_len, H] -- encoder output (typically the
    /// `output_node` from `deberta_graph.buildForwardGraph`).
    hidden_node: NodeId,
    /// i64 [batch * seq_len] -- token ids (used to find label positions).
    input_ids_node: NodeId,
    /// i64 [batch * seq_len] -- 1-indexed word IDs, 0 for non-word tokens.
    words_mask_node: NodeId,
    /// i64 [batch * num_spans * 2] -- (start_word, end_word) pairs.
    span_idx_node: NodeId,
    /// f32 [batch * num_spans, num_labels] -- final span/label logits.
    logits_node: NodeId,
};

/// Construct the GLiNER2 head forward graph (skeleton variant).  All
/// placeholders must be pre-created by the caller (matches
/// `deberta_graph.buildForwardGraph`'s convention).
///
/// This variant returns `logits_node = null_node` because the head
/// can't be built directly from `(hidden, input_ids, words_mask,
/// span_idx)` -- the f32-only operations (filter words_mask > 0,
/// compare input_ids == entity_token_id, clamp span indices) aren't
/// single Builder ops.  Use `buildForwardGraphPrecomputed` instead and
/// pass the pre-computed indices as additional placeholders.
///
/// Kept for compatibility with any caller that wants the placeholder
/// slots threaded through (e.g. ONNX import, LoRA harness scaffolding).
pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    hidden: NodeId,
    input_ids: NodeId,
    words_mask: NodeId,
    span_idx: NodeId,
    batch: u32,
    seq_len: u32,
    num_words: u32,
    num_spans: u32,
) !GlinerHeadGraph {
    _ = batch;
    _ = seq_len;
    _ = num_words;
    _ = num_spans;
    _ = bld;
    _ = config;
    return .{
        .hidden_node = hidden,
        .input_ids_node = input_ids,
        .words_mask_node = words_mask,
        .span_idx_node = span_idx,
        .logits_node = ml.graph.null_node,
    };
}

/// Inputs to the precomputed-index variant.  All node IDs are caller-
/// created placeholders; the runtime binds them per request:
///
///   hidden               f32 [batch * seq_len, H]   encoder output
///   valid_token_indices  i64 [num_valid_tokens]     positions in `hidden`
///                                                   that have words_mask > 0
///   word_ids             i64 [num_valid_tokens]     0-indexed word IDs
///                                                   for each valid token
///   label_positions      i64 [num_labels]           token positions where
///                                                   input_ids == entity_token_id
///   start_indices        i64 [batch * num_spans]    span start word IDs
///                                                   (clamped to [0, num_words))
///   end_indices          i64 [batch * num_spans]    span end word IDs
///
/// Caller pre-processes words_mask / input_ids / span_idx in CPU
/// before binding the placeholders -- same pattern as
/// `deberta_graph.buildForwardGraph` taking pre-built `attn_bias`.
pub const PrecomputedInputs = struct {
    hidden: NodeId,
    valid_token_indices: NodeId,
    word_ids: NodeId,
    label_positions: NodeId,
    start_indices: NodeId,
    end_indices: NodeId,
    num_valid_tokens: u32,
    num_words: u32,
    num_labels: u32,
    num_spans: u32,
};

/// Full GLiNER2 head forward graph using the helpers above.  Mirrors
/// `gliner_head.forward` op-by-op, with index-derivation steps the
/// graph IR can't express in single ops pushed back to the caller as
/// placeholder bindings (see `PrecomputedInputs`).
///
/// Output:
///   logits_node  f32 [batch * num_spans, num_labels]
///
/// Weight names in the emitted graph match `gliner_head.zig`'s
/// runtime names exactly, so a single WeightStore + LoRA adapter set
/// covers both eager and graph paths.
pub fn buildForwardGraphPrecomputed(
    bld: *Builder,
    config: Config,
    inputs: PrecomputedInputs,
) !NodeId {
    const H: u32 = config.hidden_size;
    const total_spans = inputs.num_spans;

    // 1. extractWordEmbeddings -> [num_words, H]
    const word_embs = try extractWordEmbeddings(
        bld,
        inputs.hidden,
        inputs.valid_token_indices,
        inputs.word_ids,
        inputs.num_valid_tokens,
        inputs.num_words,
        H,
    );

    // 2. extractLabelEmbeddings -> [num_labels, H]
    const label_embs = try gatherLabelEmbeddings(
        bld,
        inputs.hidden,
        inputs.label_positions,
        inputs.num_labels,
        H,
    );

    // 3. spanMarkerForward -> [num_spans, H]
    const span_rep = try spanMarkerForward(
        bld,
        word_embs,
        inputs.start_indices,
        inputs.end_indices,
        inputs.num_words,
        total_spans,
        H,
    );

    // 4. countLstmForward: pos_embedding[0] broadcast as the GRU input,
    //    label_embs as the GRU initial hidden, then DownscaledTransformer.
    //    pos_embedding is [4, H]; we slice the first row and broadcast
    //    via embeddingLookup with all-zero indices to [num_labels, H].
    const Shape = ml.graph.Shape;
    const pos_w = try bld.parameter("count_embed.pos_embedding.weight", Shape.init(.f32, &.{ 4, @intCast(H) }));
    // Caller pre-computes a [num_labels] all-zeros index tensor and
    // binds it to `__pos_zero_indices` so we don't need a constant
    // graph node for it.
    const pos_zero_indices = try bld.parameter("__pos_zero_indices", Shape.init(.i64, &.{@intCast(inputs.num_labels)}));
    const pos_broadcast = try bld.embeddingLookup(pos_w, pos_zero_indices, inputs.num_labels, H);
    const gru_out = try gruStep(bld, label_embs, pos_broadcast, inputs.num_labels, H);

    // Skip connection: combined = gru_out + label_embs.
    const combined = try bld.add(gru_out, label_embs);

    const D = config.downscaled_dim;
    const D_FFN = config.downscaled_ffn_dim;
    const label_proj = try downscaledTransformer(
        bld,
        combined,
        inputs.num_labels,
        H,
        D,
        D_FFN,
        config.downscaled_num_layers,
        config.downscaled_num_heads,
    );

    // 5. Final scoring: span_rep @ label_proj^T -> [num_spans, num_labels].
    return scoreSpansAgainstLabels(bld, span_rep, label_proj, total_spans, H, inputs.num_labels);
}

// ── Building blocks for the head body ────────────────────────────────
//
// These helpers are implementable today with the existing Builder
// surface; they're factored out so the body can be assembled
// incrementally as the missing helpers (concat, scatterAdd, sigmoid)
// land.  Each one mirrors a function in the eager `gliner_head.zig`.

/// Two-layer MLP: linear(in→hidden) → relu → linear(hidden→out).
/// Mirrors `mlp2` in gliner_head.zig.  Weight names follow the
/// PyTorch Sequential convention: `{prefix}.0.weight/bias` for layer 0,
/// `{prefix}.3.weight/bias` for layer 3 (indices 0 and 3 because the
/// PyTorch Sequential is [Linear, Dropout, ReLU, Linear]).
pub fn mlp2(
    bld: *Builder,
    input: NodeId,
    rows: u32,
    in_dim: u32,
    hidden_dim: u32,
    out_dim: u32,
    prefix: []const u8,
) !NodeId {
    var name_buf: [256]u8 = undefined;
    const Shape = ml.graph.Shape;

    const w1_name = try std.fmt.bufPrint(&name_buf, "{s}.0.weight", .{prefix});
    const w1 = try bld.parameter(w1_name, Shape.init(.f32, &.{ @intCast(hidden_dim), @intCast(in_dim) }));
    const b1_name = try std.fmt.bufPrint(&name_buf, "{s}.0.bias", .{prefix});
    const b1 = try bld.parameter(b1_name, Shape.init(.f32, &.{@intCast(hidden_dim)}));
    const h1 = try bld.linear(input, w1, b1, rows, in_dim, hidden_dim);
    const h1_relu = try bld.relu(h1);

    const w2_name = try std.fmt.bufPrint(&name_buf, "{s}.3.weight", .{prefix});
    const w2 = try bld.parameter(w2_name, Shape.init(.f32, &.{ @intCast(out_dim), @intCast(hidden_dim) }));
    const b2_name = try std.fmt.bufPrint(&name_buf, "{s}.3.bias", .{prefix});
    const b2 = try bld.parameter(b2_name, Shape.init(.f32, &.{@intCast(out_dim)}));
    return bld.linear(h1_relu, w2, b2, rows, hidden_dim, out_dim);
}

/// Score `span_rep [total_spans, H]` against `label_proj [num_labels, H]`
/// via `span_rep @ label_proj^T`.  Mirrors the final matmul in
/// `gliner_head.forward`; the only piece of the head that's a single op.
pub fn scoreSpansAgainstLabels(
    bld: *Builder,
    span_rep: NodeId,
    label_proj: NodeId,
    total_spans: u32,
    H: u32,
    num_labels: u32,
) !NodeId {
    return bld.linearNoBias(span_rep, label_proj, total_spans, H, num_labels);
}

/// Gather rows from `hidden` at `label_positions` to extract the
/// hidden states at entity-marker token positions.  Mirrors
/// `extractLabelEmbeddings` in the eager head -- the caller pre-computes
/// the label positions (which token indices contain `entity_token_id`)
/// and passes them as a NodeId placeholder, since locating them
/// requires comparing input_ids to a scalar (not yet exposed in Builder).
pub fn gatherLabelEmbeddings(
    bld: *Builder,
    hidden: NodeId,
    label_positions: NodeId,
    num_labels: u32,
    H: u32,
) !NodeId {
    return bld.embeddingLookup(hidden, label_positions, num_labels, H);
}

/// Span-marker forward.  Two MLPs project `word_embs` through start /
/// end heads; gathered start[start_idx] and end[end_idx] are
/// concatenated along the last dim, ReLU'd, and pushed through a
/// 2H -> 4H -> H out_project MLP.  Mirrors `spanMarkerForward` in the
/// eager head.
///
/// The caller pre-computes `start_indices_node` and `end_indices_node`
/// as i64 placeholders (length total_spans, values in [0, total_words)
/// -- the eager path does the same clamping in CPU before the gather).
pub fn spanMarkerForward(
    bld: *Builder,
    word_embs: NodeId,
    start_indices_node: NodeId,
    end_indices_node: NodeId,
    total_words: u32,
    total_spans: u32,
    H: u32,
) !NodeId {
    const start_proj = try mlp2(bld, word_embs, total_words, H, 4 * H, H, "span_rep.span_rep_layer.project_start");
    const end_proj = try mlp2(bld, word_embs, total_words, H, 4 * H, H, "span_rep.span_rep_layer.project_end");

    const gathered_start = try bld.embeddingLookup(start_proj, start_indices_node, total_spans, H);
    const gathered_end = try bld.embeddingLookup(end_proj, end_indices_node, total_spans, H);

    // Concat along last dim: [total_spans, H] | [total_spans, H] -> [total_spans, 2H]
    const concat = try bld.concat(gathered_start, gathered_end, 1);
    const concat_relu = try bld.relu(concat);

    return mlp2(bld, concat_relu, total_spans, 2 * H, 4 * H, H, "span_rep.span_rep_layer.out_project");
}

/// GRU single step.  Mirrors `gruStep` in the eager head: two linears
/// produce gi=[N,3H] and gh=[N,3H] gate stacks, which are split via
/// `sliceLastDim` into r/z/n components and combined with sigmoid /
/// tanh per the standard GRU equations:
///
///   r = sigmoid(gi_r + gh_r)
///   z = sigmoid(gi_z + gh_z)
///   n = tanh(gi_n + r * gh_n)
///   h_1 = (1 - z) * n + z * h_0
///
/// All ops are graph nodes; autodiff covers each constituent.
pub fn gruStep(
    bld: *Builder,
    h_0: NodeId,
    x: NodeId,
    N: u32,
    H: u32,
) !NodeId {
    const Shape = ml.graph.Shape;
    const w_ih = try bld.parameter("count_embed.gru.weight_ih_l0", Shape.init(.f32, &.{ @intCast(3 * H), @intCast(H) }));
    const w_hh = try bld.parameter("count_embed.gru.weight_hh_l0", Shape.init(.f32, &.{ @intCast(3 * H), @intCast(H) }));
    const b_ih = try bld.parameter("count_embed.gru.bias_ih_l0", Shape.init(.f32, &.{@intCast(3 * H)}));
    const b_hh = try bld.parameter("count_embed.gru.bias_hh_l0", Shape.init(.f32, &.{@intCast(3 * H)}));

    const gi = try bld.linear(x, w_ih, b_ih, N, H, 3 * H);
    const gh = try bld.linear(h_0, w_hh, b_hh, N, H, 3 * H);

    // Slice into r / z / n along the last dim.
    const gi_r = try bld.sliceLastDim(gi, 0, @intCast(H));
    const gi_z = try bld.sliceLastDim(gi, @intCast(H), @intCast(2 * H));
    const gi_n = try bld.sliceLastDim(gi, @intCast(2 * H), @intCast(3 * H));
    const gh_r = try bld.sliceLastDim(gh, 0, @intCast(H));
    const gh_z = try bld.sliceLastDim(gh, @intCast(H), @intCast(2 * H));
    const gh_n = try bld.sliceLastDim(gh, @intCast(2 * H), @intCast(3 * H));

    const r = try bld.sigmoid(try bld.add(gi_r, gh_r));
    const z = try bld.sigmoid(try bld.add(gi_z, gh_z));
    const n = try bld.tanhOp(try bld.add(gi_n, try bld.mul(r, gh_n)));

    // h_1 = (1 - z) * n + z * h_0
    const one = try bld.scalarConst(.f32, 1.0);
    const one_minus_z = try bld.sub(one, z);
    const lhs = try bld.mul(one_minus_z, n);
    const rhs = try bld.mul(z, h_0);
    return bld.add(lhs, rhs);
}

/// Mini transformer encoder layer.  Mirrors `miniTransformerLayerCpu`
/// in the eager head: post-norm style with self-attention then FFN.
///
///   1. self-attention (in_proj 3D linear -> split QKV -> sdpa -> out_proj)
///   2. residual + layerNorm
///   3. FFN (D -> D_FFN -> D, ReLU between)
///   4. residual + layerNorm
///
/// `D` = downscaled hidden, `D_FFN` = FFN inner, `num_heads` divides D
/// (head_dim = D / num_heads).
pub fn miniTransformerLayer(
    bld: *Builder,
    hidden: NodeId,
    N: u32,
    D: u32,
    D_FFN: u32,
    num_heads: u32,
    layer: usize,
) !NodeId {
    const Shape = ml.graph.Shape;
    var name_buf: [256]u8 = undefined;

    // Self-attention block.
    const in_proj_w_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.self_attn.in_proj_weight", .{layer});
    const in_proj_w = try bld.parameter(in_proj_w_name, Shape.init(.f32, &.{ @intCast(3 * D), @intCast(D) }));
    const in_proj_b_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.self_attn.in_proj_bias", .{layer});
    const in_proj_b = try bld.parameter(in_proj_b_name, Shape.init(.f32, &.{@intCast(3 * D)}));
    const qkv = try bld.linear(hidden, in_proj_w, in_proj_b, N, D, 3 * D);

    const Q = try bld.sliceLastDim(qkv, 0, @intCast(D));
    const K = try bld.sliceLastDim(qkv, @intCast(D), @intCast(2 * D));
    const V = try bld.sliceLastDim(qkv, @intCast(2 * D), @intCast(3 * D));

    const head_dim = D / num_heads;
    const Q_heads = try labelTransformerHeads(bld, Q, N, num_heads, head_dim);
    const K_heads = try labelTransformerHeads(bld, K, N, num_heads, head_dim);
    const V_heads = try labelTransformerHeads(bld, V, N, num_heads, head_dim);
    const attn_heads = try bld.sdpa(Q_heads, K_heads, V_heads, 1, N, num_heads, head_dim);
    const attn_out = try bld.reshape(attn_heads, Shape.init(.f32, &.{ @intCast(N), @intCast(D) }));

    const out_proj_w_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.self_attn.out_proj.weight", .{layer});
    const out_proj_w = try bld.parameter(out_proj_w_name, Shape.init(.f32, &.{ @intCast(D), @intCast(D) }));
    const out_proj_b_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.self_attn.out_proj.bias", .{layer});
    const out_proj_b = try bld.parameter(out_proj_b_name, Shape.init(.f32, &.{@intCast(D)}));
    const attn_proj = try bld.linear(attn_out, out_proj_w, out_proj_b, N, D, D);

    // Residual + post-norm 1.
    const res1 = try bld.add(attn_proj, hidden);
    const norm1_w_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.norm1.weight", .{layer});
    const norm1_w = try bld.parameter(norm1_w_name, Shape.init(.f32, &.{@intCast(D)}));
    const norm1_b_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.norm1.bias", .{layer});
    const norm1_b = try bld.parameter(norm1_b_name, Shape.init(.f32, &.{@intCast(D)}));
    const normed1 = try bld.layerNorm(res1, norm1_w, norm1_b, D, 1e-5);

    // FFN.
    const ffn1_w_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.linear1.weight", .{layer});
    const ffn1_w = try bld.parameter(ffn1_w_name, Shape.init(.f32, &.{ @intCast(D_FFN), @intCast(D) }));
    const ffn1_b_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.linear1.bias", .{layer});
    const ffn1_b = try bld.parameter(ffn1_b_name, Shape.init(.f32, &.{@intCast(D_FFN)}));
    const ffn1 = try bld.linear(normed1, ffn1_w, ffn1_b, N, D, D_FFN);
    const ffn1_relu = try bld.relu(ffn1);

    const ffn2_w_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.linear2.weight", .{layer});
    const ffn2_w = try bld.parameter(ffn2_w_name, Shape.init(.f32, &.{ @intCast(D), @intCast(D_FFN) }));
    const ffn2_b_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.linear2.bias", .{layer});
    const ffn2_b = try bld.parameter(ffn2_b_name, Shape.init(.f32, &.{@intCast(D)}));
    const ffn2 = try bld.linear(ffn1_relu, ffn2_w, ffn2_b, N, D_FFN, D);

    // Residual + post-norm 2.
    const res2 = try bld.add(ffn2, normed1);
    const norm2_w_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.norm2.weight", .{layer});
    const norm2_w = try bld.parameter(norm2_w_name, Shape.init(.f32, &.{@intCast(D)}));
    const norm2_b_name = try std.fmt.bufPrint(&name_buf, "count_embed.transformer.transformer.layers.{d}.norm2.bias", .{layer});
    const norm2_b = try bld.parameter(norm2_b_name, Shape.init(.f32, &.{@intCast(D)}));
    return bld.layerNorm(res2, norm2_w, norm2_b, D, 1e-5);
}

fn labelTransformerHeads(
    bld: *Builder,
    flat: NodeId,
    N: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    return bld.reshape(flat, ml.graph.Shape.init(.f32, &.{ @intCast(num_heads), @intCast(N), @intCast(head_dim) }));
}

/// DownscaledTransformer.  Mirrors `downscaledTransformer` in the eager
/// head: in-projector (H -> D), N mini-transformer layers, then concat
/// with the original `combined` skip-connection and a 3-layer
/// out-projector MLP back to H.
///
///   in_projector linear (H -> D)
///   N x miniTransformerLayer        (D -> D, head_dim = D / num_heads)
///   concat(transformer_out, combined) along last dim -> [N_in, D + H]
///   out_projector 3-layer MLP (D+H -> H -> H -> H, ReLU between)
pub fn downscaledTransformer(
    bld: *Builder,
    combined: NodeId, // [num_labels, H] -- the GRU output + label_embeddings residual
    num_labels: u32,
    H: u32,
    D: u32,
    D_FFN: u32,
    num_layers: u32,
    num_heads: u32,
) !NodeId {
    const Shape = ml.graph.Shape;

    // in_projector: [N, H] -> [N, D]
    const in_w = try bld.parameter("count_embed.transformer.in_projector.weight", Shape.init(.f32, &.{ @intCast(D), @intCast(H) }));
    const in_b = try bld.parameter("count_embed.transformer.in_projector.bias", Shape.init(.f32, &.{@intCast(D)}));
    var hidden = try bld.linear(combined, in_w, in_b, num_labels, H, D);

    var layer: usize = 0;
    while (layer < num_layers) : (layer += 1) {
        hidden = try miniTransformerLayer(bld, hidden, num_labels, D, D_FFN, num_heads, layer);
    }

    // out_projector with skip: concat([transformer_out, combined]) along axis=1
    // -> [num_labels, D + H].
    const cat = try bld.concat(hidden, combined, 1);

    // 3-layer MLP: (D+H) -> H -> H -> H, ReLU between each.  The eager
    // head uses out_projector.0/.2/.4 for the three Linear weights; the
    // even indices match the PyTorch Sequential layout
    // [Linear(0), ReLU(1), Linear(2), ReLU(3), Linear(4)].
    const concat_dim = D + H;
    const w0 = try bld.parameter("count_embed.transformer.out_projector.0.weight", Shape.init(.f32, &.{ @intCast(H), @intCast(concat_dim) }));
    const b0 = try bld.parameter("count_embed.transformer.out_projector.0.bias", Shape.init(.f32, &.{@intCast(H)}));
    const h0 = try bld.linear(cat, w0, b0, num_labels, concat_dim, H);
    const h0_relu = try bld.relu(h0);

    const w2 = try bld.parameter("count_embed.transformer.out_projector.2.weight", Shape.init(.f32, &.{ @intCast(H), @intCast(H) }));
    const b2 = try bld.parameter("count_embed.transformer.out_projector.2.bias", Shape.init(.f32, &.{@intCast(H)}));
    const h2 = try bld.linear(h0_relu, w2, b2, num_labels, H, H);
    const h2_relu = try bld.relu(h2);

    const w4 = try bld.parameter("count_embed.transformer.out_projector.4.weight", Shape.init(.f32, &.{ @intCast(H), @intCast(H) }));
    const b4 = try bld.parameter("count_embed.transformer.out_projector.4.bias", Shape.init(.f32, &.{@intCast(H)}));
    return bld.linear(h2_relu, w4, b4, num_labels, H, H);
}

/// Sum-and-average extractor for word embeddings.  Mirrors
/// `extractWordEmbeddings` in the eager head via two scatter-adds:
///
///   sum    = scatterAdd(zeros[num_words, H], hidden_at_word_positions, word_ids)
///   counts = scatterAdd(zeros[num_words, H], ones[num_valid_tokens, H], word_ids)
///   word_embs = sum / max(counts, 1.0)   -- the max-1 keeps unused
///                                            words from producing NaN
///                                            in the output.
///
/// Caller pre-computes (a) the indices into `hidden` of valid word
/// tokens and (b) the corresponding word IDs (0-indexed), since
/// filtering "where words_mask > 0" isn't a single Builder op.  These
/// are passed as i64 placeholders.
pub fn extractWordEmbeddings(
    bld: *Builder,
    hidden: NodeId,
    valid_token_indices: NodeId,
    word_ids: NodeId,
    num_valid_tokens: u32,
    num_words: u32,
    H: u32,
) !NodeId {
    const Shape = ml.graph.Shape;

    // Gather hidden states at valid (word-bearing) token positions.
    const hidden_at_words = try bld.embeddingLookup(hidden, valid_token_indices, num_valid_tokens, H);

    // Initial zero buffers for the scatter destinations.
    const zero = try bld.scalarConst(.f32, 0.0);
    const sum_init = try bld.parameter("__word_emb_sum_init", Shape.init(.f32, &.{ @intCast(num_words), @intCast(H) }));
    _ = zero; // sum_init starts as a parameter; the runtime binds it to zeros
    const counts_init = try bld.parameter("__word_emb_count_init", Shape.init(.f32, &.{ @intCast(num_words), @intCast(H) }));

    const sum = try bld.scatterAdd(sum_init, hidden_at_words, word_ids, 0);

    // For counts: scatter one full hidden-width row per valid token.
    // We use [num_valid_tokens, H] so the final divide is elementwise and does
    // not depend on backend-specific [words, 1] broadcasting.
    // parameter the runtime binds to all-1s.
    const ones = try bld.parameter("__word_emb_ones", Shape.init(.f32, &.{ @intCast(num_valid_tokens), @intCast(H) }));
    const counts = try bld.scatterAdd(counts_init, ones, word_ids, 0);

    // Mean = sum / max(counts, 1) -- keeps unused word slots at 0.
    // Without a `max` op we approximate by adding eps; safe since all
    // populated words have counts >= 1.
    const eps = try bld.scalarConst(.f32, 1e-9);
    const safe_counts = try bld.add(counts, eps);
    return bld.div(sum, safe_counts);
}

test "mlp2 helper builds the [Linear, ReLU, Linear] node sequence" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const input = try bld.parameter("__mlp_input", Shape.init(.f32, &.{ 4, 16 }));
    const out = try mlp2(&bld, input, 4, 16, 32, 16, "test_mlp");

    // The output node is the second linear's result.  Three new params
    // (w1, b1, w2, b2 -- but b1+b2 share a parameter slot with weights)
    // plus 1 input + 4 weight nodes = at least 5 nodes added.  We don't
    // pin the exact graph shape; the important thing is the call
    // wires up without erroring and the final node id is non-null.
    try std.testing.expect(out != ml.graph.null_node);
}

test "scoreSpansAgainstLabels emits a single linearNoBias node" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const span_rep = try bld.parameter("__span_rep", Shape.init(.f32, &.{ 12, 64 }));
    const label_proj = try bld.parameter("__label_proj", Shape.init(.f32, &.{ 5, 64 }));
    const logits = try scoreSpansAgainstLabels(&bld, span_rep, label_proj, 12, 64, 5);
    try std.testing.expect(logits != ml.graph.null_node);
}

test "gatherLabelEmbeddings emits an embeddingLookup node" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const hidden = try bld.parameter("__hidden", Shape.init(.f32, &.{ 8, 64 }));
    const label_positions = try bld.parameter("__label_positions", Shape.init(.i64, &.{3}));
    const labels = try gatherLabelEmbeddings(&bld, hidden, label_positions, 3, 64);
    try std.testing.expect(labels != ml.graph.null_node);
}

test "spanMarkerForward wires gather + concat + relu + mlp2" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const word_embs = try bld.parameter("__word_embs", Shape.init(.f32, &.{ 8, 32 }));
    const start_idx = try bld.parameter("__start_idx", Shape.init(.i64, &.{12}));
    const end_idx = try bld.parameter("__end_idx", Shape.init(.i64, &.{12}));
    const out = try spanMarkerForward(&bld, word_embs, start_idx, end_idx, 8, 12, 32);
    const node = g.node(out);
    try std.testing.expectEqual(@as(i64, 12), node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 32), node.output_shape.dim(1));
}

test "gruStep wires sigmoid + tanh + slice/add/mul into the GRU recurrence" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const h_0 = try bld.parameter("__h0", Shape.init(.f32, &.{ 4, 16 }));
    const x = try bld.parameter("__x", Shape.init(.f32, &.{ 4, 16 }));
    const h_1 = try gruStep(&bld, h_0, x, 4, 16);
    const node = g.node(h_1);
    try std.testing.expectEqual(@as(i64, 4), node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 16), node.output_shape.dim(1));
}

test "extractWordEmbeddings wires scatter_add + scatter_count + divide" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const hidden = try bld.parameter("__hidden", Shape.init(.f32, &.{ 16, 32 }));
    const valid_idx = try bld.parameter("__valid_idx", Shape.init(.i64, &.{8}));
    const word_ids = try bld.parameter("__word_ids", Shape.init(.i64, &.{8}));
    const out = try extractWordEmbeddings(&bld, hidden, valid_idx, word_ids, 8, 4, 32);
    const node = g.node(out);
    try std.testing.expectEqual(@as(i64, 4), node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 32), node.output_shape.dim(1));
}

test "buildForwardGraphPrecomputed wires the full head end-to-end" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;

    // Tiny shapes that exercise every helper without running afoul of
    // the SDPA head-dim divisibility (D=128, num_heads=4 -> head_dim=32).
    const num_valid_tokens: u32 = 12;
    const num_words: u32 = 6;
    const num_labels: u32 = 3;
    const num_spans: u32 = 8;
    const H: u32 = 64;

    const hidden = try bld.parameter("hidden", Shape.init(.f32, &.{ 16, @intCast(H) }));
    const valid_idx = try bld.parameter("valid_idx", Shape.init(.i64, &.{@intCast(num_valid_tokens)}));
    const word_ids = try bld.parameter("word_ids", Shape.init(.i64, &.{@intCast(num_valid_tokens)}));
    const label_pos = try bld.parameter("label_pos", Shape.init(.i64, &.{@intCast(num_labels)}));
    const start_idx = try bld.parameter("start_idx", Shape.init(.i64, &.{@intCast(num_spans)}));
    const end_idx = try bld.parameter("end_idx", Shape.init(.i64, &.{@intCast(num_spans)}));

    const cfg = Config{
        .hidden_size = H,
        .entity_token_id = 99,
        .downscaled_dim = 32,
        .downscaled_ffn_dim = 64,
        .downscaled_num_layers = 2,
        .downscaled_num_heads = 4,
    };
    const logits = try buildForwardGraphPrecomputed(&bld, cfg, .{
        .hidden = hidden,
        .valid_token_indices = valid_idx,
        .word_ids = word_ids,
        .label_positions = label_pos,
        .start_indices = start_idx,
        .end_indices = end_idx,
        .num_valid_tokens = num_valid_tokens,
        .num_words = num_words,
        .num_labels = num_labels,
        .num_spans = num_spans,
    });

    const node = g.node(logits);
    try std.testing.expectEqual(@as(i64, @intCast(num_spans)), node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, @intCast(num_labels)), node.output_shape.dim(1));
}

test "miniTransformerLayer wires sdpa + residual + 2 layernorms + ffn" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const hidden = try bld.parameter("__h", Shape.init(.f32, &.{ 4, 128 }));
    const out = try miniTransformerLayer(&bld, hidden, 4, 128, 256, 4, 0);
    const node = g.node(out);
    // Output preserves [num_labels, D] = [4, 128].
    try std.testing.expectEqual(@as(i64, 4), node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 128), node.output_shape.dim(1));
}

test "downscaledTransformer projects through D, runs layers, concats skip, projects to H" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const Shape = ml.graph.Shape;
    const combined = try bld.parameter("__combined", Shape.init(.f32, &.{ 4, 64 }));
    const out = try downscaledTransformer(&bld, combined, 4, 64, 32, 64, 2, 4);
    const node = g.node(out);
    // Output is [num_labels, H] = [4, 64].
    try std.testing.expectEqual(@as(i64, 4), node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 64), node.output_shape.dim(1));
}

test "buildForwardGraph skeleton compiles + plumbs placeholders" {
    const allocator = std.testing.allocator;
    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // Builder.parameter is the placeholder primitive used by every other
    // graph builder in this codebase (see deberta_graph.zig).
    const Shape = ml.graph.Shape;
    const hidden = try bld.parameter("hidden", Shape.init(.f32, &.{ 8, 64 }));
    const input_ids = try bld.parameter("input_ids", Shape.init(.i64, &.{8}));
    const words_mask = try bld.parameter("words_mask", Shape.init(.i64, &.{8}));
    const span_idx = try bld.parameter("span_idx", Shape.init(.i64, &.{ 12, 2 }));

    const result = try buildForwardGraph(
        &bld,
        .{ .hidden_size = 64, .entity_token_id = 99 },
        hidden,
        input_ids,
        words_mask,
        span_idx,
        1, // batch
        8, // seq_len
        4, // num_words
        6, // num_spans
    );

    // Skeleton wires placeholders through; logits is null until the
    // body is written.  This test keeps the signature live and catches
    // any breaking change to the GlinerHeadGraph shape.
    try std.testing.expectEqual(hidden, result.hidden_node);
    try std.testing.expectEqual(input_ids, result.input_ids_node);
    try std.testing.expectEqual(words_mask, result.words_mask_node);
    try std.testing.expectEqual(span_idx, result.span_idx_node);
    try std.testing.expectEqual(ml.graph.null_node, result.logits_node);
}

// ── CPU pre-processing of raw inputs into the index tensors that the
// graph body consumes.  All are pure host-side derivations -- the
// graph IR can't express them as single ops without comparison /
// boolean primitives, so the production caller does them in CPU
// before binding placeholders.

/// Outputs of `prepGlinerInputs`.  Each owned slice is allocated on
/// the caller's allocator; caller frees via `freePrepInputs`.
pub const PreparedInputs = struct {
    valid_token_indices: []i64,
    word_ids: []i64,
    label_positions: []i64,
    start_indices: []i64,
    end_indices: []i64,
    pos_zero_indices: []i64,
    word_emb_sum_init: []f32,
    word_emb_count_init: []f32,
    word_emb_ones: []f32,

    num_valid_tokens: u32,
    num_words: u32,
    num_labels: u32,
    num_spans: u32,

    pub fn deinit(self: *PreparedInputs, allocator: std.mem.Allocator) void {
        allocator.free(self.valid_token_indices);
        allocator.free(self.word_ids);
        allocator.free(self.label_positions);
        allocator.free(self.start_indices);
        allocator.free(self.end_indices);
        allocator.free(self.pos_zero_indices);
        allocator.free(self.word_emb_sum_init);
        allocator.free(self.word_emb_count_init);
        allocator.free(self.word_emb_ones);
    }
};

/// Pre-process the raw GLiNER inputs into the index tensors and
/// zero-init buffers the graph expects to be bound to placeholders.
/// Mirrors the index-derivation logic in `gliner_head.forward` /
/// `forwardCt`.
pub fn prepGlinerInputs(
    allocator: std.mem.Allocator,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    H: u32,
    entity_token_id: i64,
) !PreparedInputs {
    return prepGlinerInputsWithLabelMarkers(allocator, input_ids, words_mask, span_idx, batch, seq_len, H, LabelMarkerTokens.fromEntityToken(entity_token_id));
}

pub fn prepGlinerInputsWithLabelMarkers(
    allocator: std.mem.Allocator,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    H: u32,
    label_markers: LabelMarkerTokens,
) !PreparedInputs {
    // 1. Walk words_mask once to derive num_words + count valid tokens.
    var max_word_id: i64 = 0;
    var num_valid: usize = 0;
    for (words_mask) |v| {
        if (v > 0) num_valid += 1;
        if (v > max_word_id) max_word_id = v;
    }
    const num_words: usize = @intCast(max_word_id);

    // 2. Build valid_token_indices + word_ids in a single pass.
    var valid_idx = try allocator.alloc(i64, num_valid);
    errdefer allocator.free(valid_idx);
    var word_ids = try allocator.alloc(i64, num_valid);
    errdefer allocator.free(word_ids);
    var slot: usize = 0;
    for (0..batch) |b| {
        for (0..seq_len) |t| {
            const wid = words_mask[b * seq_len + t];
            if (wid <= 0) continue;
            valid_idx[slot] = @intCast(b * seq_len + t);
            // word_ids index into the global [batch * num_words] table:
            // word slot = b * num_words + (wid - 1).
            word_ids[slot] = @intCast(b * num_words + @as(usize, @intCast(wid - 1)));
            slot += 1;
        }
    }

    // 3. Label positions: token offsets where input_ids is a GLiNER label marker.
    //    Eager `extractLabelEmbeddings` only checks the first batch item
    //    (labels are shared across batch); we follow that.
    var label_pos: std.ArrayListUnmanaged(i64) = .empty;
    errdefer label_pos.deinit(allocator);
    for (0..seq_len) |t| {
        if (isGlinerLabelMarkerToken(input_ids[t], label_markers)) {
            try label_pos.append(allocator, @intCast(t));
        }
    }
    const label_positions = try label_pos.toOwnedSlice(allocator);
    errdefer allocator.free(label_positions);
    const num_labels = label_positions.len;

    // 4. Span indices: clamp to [0, num_words) and flatten to global rows.
    const total_elements = span_idx.len;
    const num_spans_per_batch: usize = if (batch == 0) 0 else total_elements / (batch * 2);
    const total_spans = batch * num_spans_per_batch;
    var start_indices = try allocator.alloc(i64, total_spans);
    errdefer allocator.free(start_indices);
    var end_indices = try allocator.alloc(i64, total_spans);
    errdefer allocator.free(end_indices);
    const num_words_clamp: usize = if (num_words == 0) 0 else num_words - 1;
    for (0..batch) |b| {
        for (0..num_spans_per_batch) |s| {
            const span_flat = b * num_spans_per_batch + s;
            const idx = span_flat * 2;
            const si: usize = @min(@as(usize, @intCast(@max(span_idx[idx], 0))), num_words_clamp);
            const ei: usize = @min(@as(usize, @intCast(@max(span_idx[idx + 1], 0))), num_words_clamp);
            start_indices[span_flat] = @intCast(b * num_words + si);
            end_indices[span_flat] = @intCast(b * num_words + ei);
        }
    }

    // 5. Zero-init buffers + ones for the scatter-add accumulators.
    const total_words = batch * num_words;
    const sum_init = try allocator.alloc(f32, total_words * @as(usize, H));
    @memset(sum_init, 0);
    errdefer allocator.free(sum_init);
    const count_init = try allocator.alloc(f32, total_words * @as(usize, H));
    @memset(count_init, 0);
    errdefer allocator.free(count_init);
    const ones = try allocator.alloc(f32, num_valid * @as(usize, H));
    @memset(ones, 1.0);
    errdefer allocator.free(ones);

    // 6. pos_zero_indices: all zeros, length num_labels (broadcasts
    //    pos_embedding[0] across all labels).
    const pos_zero = try allocator.alloc(i64, num_labels);
    @memset(pos_zero, 0);
    errdefer allocator.free(pos_zero);

    return .{
        .valid_token_indices = valid_idx,
        .word_ids = word_ids,
        .label_positions = label_positions,
        .start_indices = start_indices,
        .end_indices = end_indices,
        .pos_zero_indices = pos_zero,
        .word_emb_sum_init = sum_init,
        .word_emb_count_init = count_init,
        .word_emb_ones = ones,
        .num_valid_tokens = @intCast(num_valid),
        .num_words = @intCast(total_words),
        .num_labels = @intCast(num_labels),
        .num_spans = @intCast(total_spans),
    };
}

fn isGlinerLabelMarkerToken(token_id: i64, label_markers: LabelMarkerTokens) bool {
    return token_id == label_markers.entity or
        (label_markers.classification != 0 and token_id == label_markers.classification) or
        (label_markers.relation != 0 and token_id == label_markers.relation);
}

// ── Head-only graph executor ─────────────────────────────────────────
//
// Runs the GLiNER head as a graph, given an already-computed encoder
// hidden state (typically from `deberta_arch.forwardCt`).  This is the
// "head graph + eager encoder" path the session_factory dispatcher
// uses when `--graph-runtime compiled` is requested for gliner.  Once
// `deberta_graph.buildForwardGraph` is wired in too, the encoder + head
// can fuse into a single graph -- this function is a stepping stone.

const interpreter = @import("../graph/interpreter.zig");
const graph_runtime = @import("../graph/runtime.zig");
const deberta_graph = @import("deberta_graph.zig");

const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;

/// Result of `runHeadGraph`.  Owns the logits buffer; caller frees.
pub const HeadGraphResult = struct {
    logits: []f32, // [num_spans, num_labels]
    num_words: u32,
    num_labels: u32,
    num_spans: u32,
};

pub const FullGraphResult = HeadGraphResult;

/// Run the GLiNER head as a graph against the supplied encoder hidden
/// state.  Builds the graph, binds the placeholders the prep helpers
/// produce, executes via the interpreter, and converts the logits back
/// to f32.
///
/// Returns logits as `[num_spans, num_labels]` -- callers reshape to
/// the user-facing `[batch, num_words, max_width, num_labels]` Tensor
/// shape themselves (same as the eager `forward` path).
pub fn runHeadGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_ct: CT,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
) !HeadGraphResult {
    var prep = try prepGlinerInputsWithLabelMarkers(allocator, input_ids, words_mask, span_idx, batch, seq_len, config.hidden_size, LabelMarkerTokens.fromConfig(config));
    errdefer prep.deinit(allocator);

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const Shape = ml.graph.Shape;
    const total_tokens: i64 = @intCast(batch * seq_len);
    const H_i64: i64 = @intCast(config.hidden_size);

    // Caller-supplied placeholders (each bound at execute time).
    const hidden_node = try bld.parameter("__hidden", Shape.init(.f32, &.{ total_tokens, H_i64 }));
    const valid_idx_node = try bld.parameter("__valid_idx", Shape.init(.i64, &.{@intCast(prep.num_valid_tokens)}));
    const word_ids_node = try bld.parameter("__word_ids", Shape.init(.i64, &.{@intCast(prep.num_valid_tokens)}));
    const label_pos_node = try bld.parameter("__label_pos", Shape.init(.i64, &.{@intCast(prep.num_labels)}));
    const start_idx_node = try bld.parameter("__start_idx", Shape.init(.i64, &.{@intCast(prep.num_spans)}));
    const end_idx_node = try bld.parameter("__end_idx", Shape.init(.i64, &.{@intCast(prep.num_spans)}));

    const logits_node = try buildForwardGraphPrecomputed(&bld, config, .{
        .hidden = hidden_node,
        .valid_token_indices = valid_idx_node,
        .word_ids = word_ids_node,
        .label_positions = label_pos_node,
        .start_indices = start_idx_node,
        .end_indices = end_idx_node,
        .num_valid_tokens = prep.num_valid_tokens,
        .num_words = prep.num_words,
        .num_labels = prep.num_labels,
        .num_spans = prep.num_spans,
    });
    try graph.markOutput(logits_node);

    // Bind the runtime inputs.  10 placeholders total: hidden + 5 raw
    // index buffers + 4 zero-init buffers (sum_init / count_init /
    // ones / pos_zero_indices).  The other parameters in the graph
    // (weight tensors named "span_rep.span_rep_layer.project_start.0
    // .weight" etc.) resolve via cb.getWeight from the backend's
    // WeightStore -- the runtime_inputs slice only overrides the
    // `__`-prefixed placeholders.
    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }

    try rt_inputs.append(allocator, .{ .node_id = hidden_node, .value = hidden_ct });

    // Helper: convert an i64 slice to a CT via f32 round-trip (the
    // existing graph_input_binder pattern -- a proper i64 binding
    // path is a separate piece of plumbing).
    const helper = struct {
        fn bindI64(
            cb_inner: *const ComputeBackend,
            alloc_inner: std.mem.Allocator,
            data: []const i64,
        ) !CT {
            const f32_buf = try alloc_inner.alloc(f32, data.len);
            defer alloc_inner.free(f32_buf);
            for (data, 0..) |v, i| f32_buf[i] = @floatFromInt(v);
            const dims = [_]i32{@intCast(data.len)};
            return cb_inner.fromFloat32Shape(f32_buf, &dims);
        }
        fn bindF32Shape(
            cb_inner: *const ComputeBackend,
            data: []const f32,
            shape: []const i32,
        ) !CT {
            return cb_inner.fromFloat32Shape(data, shape);
        }
    };

    const valid_ct = try helper.bindI64(cb, allocator, prep.valid_token_indices);
    try owned_cts.append(allocator, valid_ct);
    try rt_inputs.append(allocator, .{ .node_id = valid_idx_node, .value = valid_ct });

    const word_ids_ct = try helper.bindI64(cb, allocator, prep.word_ids);
    try owned_cts.append(allocator, word_ids_ct);
    try rt_inputs.append(allocator, .{ .node_id = word_ids_node, .value = word_ids_ct });

    const label_pos_ct = try helper.bindI64(cb, allocator, prep.label_positions);
    try owned_cts.append(allocator, label_pos_ct);
    try rt_inputs.append(allocator, .{ .node_id = label_pos_node, .value = label_pos_ct });

    const start_idx_ct = try helper.bindI64(cb, allocator, prep.start_indices);
    try owned_cts.append(allocator, start_idx_ct);
    try rt_inputs.append(allocator, .{ .node_id = start_idx_node, .value = start_idx_ct });

    const end_idx_ct = try helper.bindI64(cb, allocator, prep.end_indices);
    try owned_cts.append(allocator, end_idx_ct);
    try rt_inputs.append(allocator, .{ .node_id = end_idx_node, .value = end_idx_ct });

    // The four parameter-named auxiliary buffers (__pos_zero_indices,
    // __word_emb_sum_init, __word_emb_count_init, __word_emb_ones)
    // were declared inside the head body via `bld.parameter`; they
    // live in the WeightStore lookup path.  For the head-graph
    // executor we register them inline via runtime_inputs the same
    // way as the user-facing placeholders.  Look up their NodeIds
    // from the graph parameter list since the body created them.
    const pos_zero_node = lookupParameter(&graph, "__pos_zero_indices") orelse return error.MissingPlaceholder;
    const sum_init_node = lookupParameter(&graph, "__word_emb_sum_init") orelse return error.MissingPlaceholder;
    const count_init_node = lookupParameter(&graph, "__word_emb_count_init") orelse return error.MissingPlaceholder;
    const ones_node = lookupParameter(&graph, "__word_emb_ones") orelse return error.MissingPlaceholder;

    const pos_zero_ct = try helper.bindI64(cb, allocator, prep.pos_zero_indices);
    try owned_cts.append(allocator, pos_zero_ct);
    try rt_inputs.append(allocator, .{ .node_id = pos_zero_node, .value = pos_zero_ct });

    const sum_init_dims = [_]i32{ @intCast(prep.num_words), @intCast(config.hidden_size) };
    const sum_init_ct = try helper.bindF32Shape(cb, prep.word_emb_sum_init, &sum_init_dims);
    try owned_cts.append(allocator, sum_init_ct);
    try rt_inputs.append(allocator, .{ .node_id = sum_init_node, .value = sum_init_ct });

    const count_init_dims = [_]i32{ @intCast(prep.num_words), @intCast(config.hidden_size) };
    const count_init_ct = try helper.bindF32Shape(cb, prep.word_emb_count_init, &count_init_dims);
    try owned_cts.append(allocator, count_init_ct);
    try rt_inputs.append(allocator, .{ .node_id = count_init_node, .value = count_init_ct });

    const ones_dims = [_]i32{ @intCast(prep.num_valid_tokens), @intCast(config.hidden_size) };
    const ones_ct = try helper.bindF32Shape(cb, prep.word_emb_ones, &ones_dims);
    try owned_cts.append(allocator, ones_ct);
    try rt_inputs.append(allocator, .{ .node_id = ones_node, .value = ones_ct });

    const exec_options = interpreter.ExecuteOptions{
        .runtime_inputs = rt_inputs.items,
    };
    var result = try interpreter.execute(allocator, &graph, cb, exec_options);
    defer result.deinit(cb);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    const logits_f32 = try cb.toFloat32(result.outputs[0], allocator);

    const num_words = prep.num_words;
    const num_labels = prep.num_labels;
    const num_spans = prep.num_spans;
    prep.deinit(allocator);

    return .{
        .logits = logits_f32,
        .num_words = num_words,
        .num_labels = num_labels,
        .num_spans = num_spans,
    };
}

/// Run the full GLiNER encoder + head as one graph-runtime execution.
/// The graph still receives dynamic token/mask/index buffers as runtime
/// inputs, while all model weights resolve through the active backend.
pub fn runFullGraph(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    deberta_cfg: deberta_config.Config,
    head_cfg: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    strategy: graph_runtime.Strategy,
) !FullGraphResult {
    var prep = try prepGlinerInputsWithLabelMarkers(allocator, input_ids, words_mask, span_idx, batch, seq_len, head_cfg.hidden_size, LabelMarkerTokens.fromConfig(head_cfg));
    errdefer prep.deinit(allocator);

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const total_tokens = batch * seq_len;
    const H_i64: i64 = @intCast(head_cfg.hidden_size);
    const heads: usize = @intCast(deberta_cfg.num_attention_heads);

    const input_ids_node = try bld.parameter("__input_ids", ml.graph.Shape.init(.i64, &.{@intCast(total_tokens)}));
    const attn_bias_node = try bld.parameter("__attn_bias", ml.graph.Shape.init(.f32, &.{ @intCast(batch * heads), @intCast(seq_len), @intCast(seq_len) }));
    const embedding_mask_node = try bld.parameter("__embedding_mask", ml.graph.Shape.init(.f32, &.{ @intCast(total_tokens), H_i64 }));

    const encoder = try deberta_graph.buildForwardGraphMasked(
        &bld,
        .{
            .vocab_size = deberta_cfg.vocab_size,
            .hidden_size = deberta_cfg.hidden_size,
            .num_hidden_layers = deberta_cfg.num_hidden_layers,
            .num_attention_heads = deberta_cfg.num_attention_heads,
            .intermediate_size = deberta_cfg.intermediate_size,
            .max_position_embeddings = deberta_cfg.max_position_embeddings,
            .position_buckets = deberta_cfg.position_buckets,
            .layer_norm_eps = deberta_cfg.layer_norm_eps,
            .use_v3_names = true,
        },
        input_ids_node,
        attn_bias_node,
        embedding_mask_node,
        @intCast(batch),
        @intCast(seq_len),
    );

    const valid_idx_node = try bld.parameter("__valid_idx", ml.graph.Shape.init(.i64, &.{@intCast(prep.num_valid_tokens)}));
    const word_ids_node = try bld.parameter("__word_ids", ml.graph.Shape.init(.i64, &.{@intCast(prep.num_valid_tokens)}));
    const label_pos_node = try bld.parameter("__label_pos", ml.graph.Shape.init(.i64, &.{@intCast(prep.num_labels)}));
    const start_idx_node = try bld.parameter("__start_idx", ml.graph.Shape.init(.i64, &.{@intCast(prep.num_spans)}));
    const end_idx_node = try bld.parameter("__end_idx", ml.graph.Shape.init(.i64, &.{@intCast(prep.num_spans)}));

    const logits_node = try buildForwardGraphPrecomputed(&bld, head_cfg, .{
        .hidden = encoder.output_node,
        .valid_token_indices = valid_idx_node,
        .word_ids = word_ids_node,
        .label_positions = label_pos_node,
        .start_indices = start_idx_node,
        .end_indices = end_idx_node,
        .num_valid_tokens = prep.num_valid_tokens,
        .num_words = prep.num_words,
        .num_labels = prep.num_labels,
        .num_spans = prep.num_spans,
    });
    try graph.markOutput(logits_node);

    var rt_inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer rt_inputs.deinit(allocator);
    var owned_cts: std.ArrayListUnmanaged(CT) = .empty;
    defer {
        for (owned_cts.items) |ct| cb.free(ct);
        owned_cts.deinit(allocator);
    }

    const input_ids_ct = try bindI64AsF32(cb, allocator, input_ids);
    try owned_cts.append(allocator, input_ids_ct);
    try rt_inputs.append(allocator, .{ .node_id = input_ids_node, .value = input_ids_ct });

    const attn_bias = try buildAttentionBias(allocator, attention_mask, batch, seq_len, heads);
    defer allocator.free(attn_bias);
    const attn_bias_ct = try cb.fromFloat32Shape(attn_bias, &.{ @intCast(batch * heads), @intCast(seq_len), @intCast(seq_len) });
    try owned_cts.append(allocator, attn_bias_ct);
    try rt_inputs.append(allocator, .{ .node_id = attn_bias_node, .value = attn_bias_ct });

    const embedding_mask = try buildEmbeddingMask(allocator, attention_mask, total_tokens, @intCast(head_cfg.hidden_size));
    defer allocator.free(embedding_mask);
    const embedding_mask_ct = try cb.fromFloat32Shape(embedding_mask, &.{ @intCast(total_tokens), @intCast(head_cfg.hidden_size) });
    try owned_cts.append(allocator, embedding_mask_ct);
    try rt_inputs.append(allocator, .{ .node_id = embedding_mask_node, .value = embedding_mask_ct });

    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, valid_idx_node, prep.valid_token_indices);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, word_ids_node, prep.word_ids);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, label_pos_node, prep.label_positions);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, start_idx_node, prep.start_indices);
    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, end_idx_node, prep.end_indices);

    const pos_zero_node = lookupParameter(&graph, "__pos_zero_indices") orelse return error.MissingPlaceholder;
    const sum_init_node = lookupParameter(&graph, "__word_emb_sum_init") orelse return error.MissingPlaceholder;
    const count_init_node = lookupParameter(&graph, "__word_emb_count_init") orelse return error.MissingPlaceholder;
    const ones_node = lookupParameter(&graph, "__word_emb_ones") orelse return error.MissingPlaceholder;

    try appendI64RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, pos_zero_node, prep.pos_zero_indices);
    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, sum_init_node, prep.word_emb_sum_init, &.{ @intCast(prep.num_words), @intCast(head_cfg.hidden_size) });
    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, count_init_node, prep.word_emb_count_init, &.{ @intCast(prep.num_words), @intCast(head_cfg.hidden_size) });
    try appendF32RuntimeInput(cb, allocator, &rt_inputs, &owned_cts, ones_node, prep.word_emb_ones, &.{ @intCast(prep.num_valid_tokens), @intCast(head_cfg.hidden_size) });

    var runtime = try graph_runtime.Runtime.init(allocator, &graph, cb, strategy);
    defer runtime.deinit();
    var result = try runtime.execute(allocator, &graph, .{ .runtime_inputs = rt_inputs.items });
    defer result.deinit(&runtime);

    if (result.outputs.len == 0) return error.MissingGraphOutput;
    const logits_f32 = try cb.toFloat32(result.outputs[0], allocator);

    const num_words = prep.num_words;
    const num_labels = prep.num_labels;
    const num_spans = prep.num_spans;
    prep.deinit(allocator);

    return .{
        .logits = logits_f32,
        .num_words = num_words,
        .num_labels = num_labels,
        .num_spans = num_spans,
    };
}

fn appendI64RuntimeInput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    rt_inputs: *std.ArrayListUnmanaged(interpreter.RuntimeInput),
    owned_cts: *std.ArrayListUnmanaged(CT),
    node_id: ml.graph.NodeId,
    data: []const i64,
) !void {
    const ct = try bindI64AsF32(cb, allocator, data);
    try owned_cts.append(allocator, ct);
    try rt_inputs.append(allocator, .{ .node_id = node_id, .value = ct });
}

fn appendF32RuntimeInput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    rt_inputs: *std.ArrayListUnmanaged(interpreter.RuntimeInput),
    owned_cts: *std.ArrayListUnmanaged(CT),
    node_id: ml.graph.NodeId,
    data: []const f32,
    shape: []const i32,
) !void {
    const ct = try cb.fromFloat32Shape(data, shape);
    try owned_cts.append(allocator, ct);
    try rt_inputs.append(allocator, .{ .node_id = node_id, .value = ct });
}

fn bindI64AsF32(cb: *const ComputeBackend, allocator: std.mem.Allocator, data: []const i64) !CT {
    const f32_buf = try allocator.alloc(f32, data.len);
    defer allocator.free(f32_buf);
    for (data, 0..) |v, i| f32_buf[i] = @floatFromInt(v);
    const dims = [_]i32{@intCast(data.len)};
    return cb.fromFloat32Shape(f32_buf, &dims);
}

fn buildAttentionBias(
    allocator: std.mem.Allocator,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    heads: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, batch * heads * seq_len * seq_len);
    for (0..batch) |b| {
        for (0..heads) |h| {
            for (0..seq_len) |q| {
                for (0..seq_len) |k| {
                    const mask = attention_mask[b * seq_len + k];
                    const idx = (((b * heads + h) * seq_len + q) * seq_len) + k;
                    out[idx] = if (mask == 0) -1.0e9 else 0.0;
                }
            }
        }
    }
    return out;
}

fn buildEmbeddingMask(
    allocator: std.mem.Allocator,
    attention_mask: []const i64,
    total_tokens: usize,
    hidden_size: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, total_tokens * hidden_size);
    for (0..total_tokens) |row| {
        const value: f32 = @floatFromInt(attention_mask[row]);
        @memset(out[row * hidden_size ..][0..hidden_size], value);
    }
    return out;
}

fn lookupParameter(graph: *const ml.graph.Graph, name: []const u8) ?ml.graph.NodeId {
    for (graph.parameters.items) |id| {
        const n = graph.node(id);
        if (n.op != .parameter) continue;
        if (std.mem.eql(u8, graph.parameterName(n), name)) return id;
    }
    return null;
}

test "prepGlinerInputs derives the right index slices for a tiny case" {
    const allocator = std.testing.allocator;

    // 1 batch, 5 tokens.  Layout:
    //   t=0: schema [P]              -> words_mask = 0
    //   t=1: entity marker [E]       -> words_mask = 0, input_ids = 99
    //   t=2: word1 first sub-token   -> words_mask = 1
    //   t=3: word1 second sub-token  -> words_mask = 1
    //   t=4: word2                   -> words_mask = 2
    const input_ids = [_]i64{ 1, 99, 5, 6, 7 };
    const words_mask = [_]i64{ 0, 0, 1, 1, 2 };
    // 2 spans: (word0, word0), (word0, word1).  span_idx is
    // [batch * num_spans * 2] = [4]:
    const span_idx = [_]i64{ 0, 0, 0, 1 };

    var prep = try prepGlinerInputs(
        allocator,
        &input_ids,
        &words_mask,
        &span_idx,
        1, // batch
        5, // seq_len
        4, // H
        99, // entity_token_id
    );
    defer prep.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 3), prep.num_valid_tokens); // 3 word-bearing tokens
    try std.testing.expectEqual(@as(u32, 2), prep.num_words); // word IDs 1,2 -> num_words=2
    try std.testing.expectEqual(@as(u32, 1), prep.num_labels); // one [E] at t=1
    try std.testing.expectEqual(@as(u32, 2), prep.num_spans);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 4 }, prep.valid_token_indices);
    try std.testing.expectEqualSlices(i64, &.{ 0, 0, 1 }, prep.word_ids);
    try std.testing.expectEqualSlices(i64, &.{1}, prep.label_positions);
    try std.testing.expectEqualSlices(i64, &.{ 0, 0 }, prep.start_indices);
    try std.testing.expectEqualSlices(i64, &.{ 0, 1 }, prep.end_indices);
    try std.testing.expectEqualSlices(i64, &.{0}, prep.pos_zero_indices);
}

test "prepGlinerInputs accepts GLiNER classification and relation markers" {
    const allocator = std.testing.allocator;
    const input_ids = [_]i64{ 52, 11, 51, 12, 53 };
    const words_mask = [_]i64{ 0, 1, 0, 2, 0 };
    const span_idx = [_]i64{ 0, 0, 0, 1 };

    var prep = try prepGlinerInputsWithLabelMarkers(
        allocator,
        &input_ids,
        &words_mask,
        &span_idx,
        1,
        5,
        4,
        .{ .classification = 52, .entity = 51, .relation = 53 },
    );
    defer prep.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 3), prep.num_labels);
    try std.testing.expectEqualSlices(i64, &.{ 0, 2, 4 }, prep.label_positions);
}
