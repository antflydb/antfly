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

// DeBERTa encoder as an ml.graph computation graph, for autodiff-based LoRA
// training. Graph-building equivalent of `deberta.zig`'s ComputeBackend
// forward pass. Emits a Graph of Builder nodes that downstream code can
// feed into `ml.graph.lora.injectLoRA` + `ml.graph.autodiff.gradient`.
//
// ┌─────────────────────────────────────────────────────────────────────┐
// │ DISENTANGLED ATTENTION — IMPLEMENTATION STATUS                     │
// │                                                                     │
// │ Real DeBERTa decomposes attention scores into three components:    │
// │   C2C (content-to-content): Q_c @ K_c^T        ✓ implemented      │
// │   C2P (content-to-position): Q_c @ K_r^T        ✓ implemented      │
// │   P2C (position-to-content): Q_r @ K_c^T        ✓ implemented      │
// │                                                                     │
// │ Total: scores = (C2C + C2P + P2C) / sqrt(3 * head_dim)           │
// │                                                                     │
// │ Relative position embeddings:                                      │
// │   - encoder.rel_embeddings.weight [max_pos, H] shared parameter   │
// │   - encoder.LayerNorm applied (norm_rel_ebd, DeBERTa-v3 style)    │
// │   - Bucket indices computed via relativePositionBucket (log-space) │
// │   - Per-layer Q_r/K_r via shared query_proj/key_proj projections  │
// │                                                                     │
// │ C2P/P2C decomposition strategy:                                    │
// │   1. Toeplitz gather: rel_emb[num_rel, H] → [S*S, H] using       │
// │      constant pair_indices[qi*S+ki] = qi - ki + S - 1             │
// │   2. Reshape to per-head [nh, S, S, D], tile across batch         │
// │   3. Batched matmul3D / elem-mul+reduce for dot products          │
// │   All ops have autodiff support (gather→scatter_add, broadcast,   │
// │   matmul3D→dot_general VJPs, reduce_sum, mul).                    │
// │                                                                     │
// │ Parameter names match HF DeBERTa checkpoint layout, so LoRA       │
// │ injection is unaffected.                                           │
// └─────────────────────────────────────────────────────────────────────┘
//
// Attention masking: passed in by the caller as a pre-built additive bias
// tensor (0 for valid positions, large-negative for padded). The caller
// also supplies the `input_ids` placeholder node. Neither is created
// inside buildForwardGraph — this is the "placeholder-wiring fix" being
// rolled out across all graph ports (see bert_graph.zig for the matching
// signature).

const std = @import("std");
const ml = @import("ml");
const platform = @import("antfly_platform");
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const null_node = ml.graph.null_node;
const deberta_config = @import("../models/deberta.zig");

/// The strict boundary training profile binds physical i32 indices. Preserve
/// that dtype through the differentiable gather; legacy embedding paths keep
/// the existing i64 conversion and fused dispatch behavior.
fn embeddingLookupTyped(bld: *Builder, weight: NodeId, indices: NodeId, total: u32, dim: u32) !NodeId {
    if (bld.graph.node(indices).output_shape.dtype == .i32) {
        return bld.gather(weight, indices, Shape.init(bld.graph.node(weight).output_shape.dtype, &.{ @intCast(total), @intCast(dim) }));
    }
    return bld.embeddingLookup(weight, indices, total, dim);
}

pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    intermediate_size: u32,
    max_position_embeddings: u32 = 512,
    position_buckets: u32 = 256,
    layer_norm_eps: f32 = 1e-7,
    /// true  → DeBERTa-v3 parameter names (query_proj / key_proj / value_proj).
    /// false → DeBERTa-v1/v2 parameter names (query / key / value).
    use_v3_names: bool = true,
};

pub const DebertaGraph = struct {
    /// [batch * seq_len] i64 placeholder. Caller-created, bound at execution.
    input_ids_node: NodeId,
    /// [batch * num_heads, seq_len, seq_len] f32 additive attention bias.
    /// Caller precomputes: 0 for valid positions, -1e9 for padded.
    attn_bias_node: NodeId,
    /// [batch * seq_len, hidden_size] — final encoder output.
    output_node: NodeId,
};

/// Explicit graph construction controls. Legacy inference entry points retain
/// their environment-selected attention route. Training callers choose the
/// materialized route and supply replayable, non-trainable dropout inputs.
pub const Attention = enum { legacy_default, fused, materialized, training_replay_v1 };
pub const DropoutKind = enum { embeddings, relative_positions, attention_probabilities, attention_output, ffn_output };
pub const DropoutSite = struct { kind: DropoutKind, layer: u32 = 0 };
pub const Dropout = struct {
    context: *anyopaque,
    apply: *const fn (*anyopaque, *Builder, NodeId, DropoutSite) anyerror!NodeId,
};
/// Semantic boundaries for regional training replay. Callers retain/remap
/// these IDs through later PEFT injection; node order is not a region contract.
pub const RegionObserver = struct {
    context: *anyopaque,
    prelude: *const fn (*anyopaque, NodeId, NodeId) anyerror!void,
    layer: *const fn (*anyopaque, u32, NodeId, NodeId) anyerror!void,
};
pub const BuildOptions = struct {
    attention: Attention = .legacy_default,
    dropout: ?Dropout = null,
    regions: ?RegionObserver = null,
    /// Multiplicative 0/1 score mask, paired with the additive bias. This
    /// reproduces masked_fill's zero score gradient on invalid query/key pairs.
    attention_score_mask: ?NodeId = null,
    /// Module-call order matters for training dropout and shared Q/K LoRA:
    /// project the normalized relative table before looking up each bucket.
    project_relative_before_gather: bool = false,
    /// Compact physical i32 replay/mask/bucket leaf consumed by the versioned
    /// training operation. Probability dropout is replayed inside attention;
    /// the ordinary callback still owns all other dropout module calls.
    training_attention_v1: ?struct { control: NodeId, probability: f32 } = null,
};

pub fn buildForwardGraphWithOptions(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    attn_bias: NodeId,
    embedding_mask: ?NodeId,
    batch: u32,
    seq_len: u32,
    options: BuildOptions,
) !DebertaGraph {
    if (options.attention == .training_replay_v1) {
        const replay = options.training_attention_v1 orelse return error.InvalidTrainingAttentionPlan;
        if (!options.project_relative_before_gather or options.attention_score_mask != null or attn_bias != null_node)
            return error.InvalidTrainingAttentionPlan;
        const attrs = ml.graph.DebertaTrainingAttentionAttrs{
            .batch = batch,
            .seq_len = seq_len,
            .num_heads = config.num_attention_heads,
            .head_dim = if (config.num_attention_heads == 0) 0 else config.hidden_size / config.num_attention_heads,
            .relative_rows = config.max_position_embeddings,
            .dropout_probability = replay.probability,
            .dropout_stream_id = 0,
        };
        const layout = try attrs.layout();
        if (config.hidden_size != layout.hidden or replay.control == null_node or
            replay.control >= bld.graph.nodes.items.len or
            !bld.graph.node(replay.control).output_shape.eq(layout.controlShape()))
            return error.InvalidTrainingAttentionPlan;
    } else if (options.training_attention_v1 != null) return error.InvalidTrainingAttentionPlan;
    if ((options.dropout != null or options.attention_score_mask != null) and
        options.attention != .materialized and options.attention != .training_replay_v1)
        return error.InvalidTrainingAttentionPlan;
    return buildForwardGraphInternal(bld, config, input_ids, attn_bias, embedding_mask, batch, seq_len, options);
}

fn applyDropout(bld: *Builder, input: NodeId, options: BuildOptions, site: DropoutSite) !NodeId {
    return if (options.dropout) |dropout| dropout.apply(dropout.context, bld, input, site) else input;
}

/// Construct a DeBERTa forward graph with HF-compatible parameter names.
/// `input_ids` and `attn_bias` are created by the caller (typically the
/// autodiff trainer harness) and passed in; this function does NOT create
/// its own placeholders for these inputs.
///
/// The graph has no pre-bound weight values; a WeightStore must be loaded
/// onto the execution backend separately. LoRA is NOT injected here —
/// call `ml.graph.lora.injectLoRA` after construction to add adapters.
pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    attn_bias: NodeId,
    batch: u32,
    seq_len: u32,
) !DebertaGraph {
    return buildForwardGraphInternal(bld, config, input_ids, attn_bias, null, batch, seq_len, .{});
}

/// Construct a DeBERTa forward graph with an explicit embedding mask.
/// This matches the eager GLiNER/DeBERTa path, which zeroes padded token
/// embeddings before the encoder layers in addition to attention masking.
pub fn buildForwardGraphMasked(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    attn_bias: NodeId,
    embedding_mask: NodeId,
    batch: u32,
    seq_len: u32,
) !DebertaGraph {
    return buildForwardGraphInternal(bld, config, input_ids, attn_bias, embedding_mask, batch, seq_len, .{});
}

fn buildForwardGraphInternal(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    attn_bias: NodeId,
    embedding_mask: ?NodeId,
    batch: u32,
    seq_len: u32,
    options: BuildOptions,
) !DebertaGraph {
    const H: u32 = config.hidden_size;
    if (batch == 0 or seq_len == 0 or H == 0 or config.num_attention_heads == 0 or
        H % config.num_attention_heads != 0) return error.InvalidInputShape;
    const total = try std.math.mul(u32, batch, seq_len);
    const num_heads: u32 = config.num_attention_heads;
    const head_dim: u32 = H / num_heads;

    // ──────── Embeddings: word + LayerNorm (NO position embeddings) ────────
    var hidden = try embeddings(bld, config, input_ids, total, H);
    if (embedding_mask) |mask| {
        hidden = try bld.mul(hidden, mask);
    }
    hidden = try applyDropout(bld, hidden, options, .{ .kind = .embeddings });

    // ──────── Relative position embeddings (disentangled attention) ────────
    // 1. Shared rel_embeddings.weight: [max_position, H]
    // 2. DeBERTa-v3 norm_rel_ebd: encoder.LayerNorm applied to raw rel_embeddings
    // 3. Bucket index lookup to get [num_rel, H]
    const training_replay = options.attention == .training_replay_v1;
    const relative = try buildRelativePositionTable(bld, config, seq_len, H, options.attention == .materialized, !training_replay);
    if (options.regions) |observer| try observer.prelude(observer.context, hidden, relative.table);
    const rel_emb_gathered = if (options.project_relative_before_gather)
        relative.table
    else
        try embeddingLookupTyped(bld, relative.table, relative.indices, 2 * seq_len - 1, H);

    // Construct only the constants consumed by the selected attention route.
    // Fused attention uses relative projections directly; constructing these
    // unused quadratic tables would otherwise dominate long-context graph memory.
    const use_fused_attention = switch (options.attention) {
        .legacy_default => fusedDisentangledAttentionEnabledForProduction(),
        .fused => true,
        .materialized => false,
        .training_replay_v1 => true,
    };
    const use_score_gather = !use_fused_attention and (options.attention == .materialized or relScoreGatherEnabled());
    const pair_indices: ?NodeId = if (!use_fused_attention and !use_score_gather)
        try buildPairIndices(bld, seq_len)
    else
        null;
    const rel_score_indices: ?RelScoreIndices = if (use_score_gather)
        if (options.attention == .materialized)
            try buildFlatRelScoreIndices(bld, batch * num_heads, seq_len)
        else
            try buildRelScoreIndices(bld, seq_len)
    else
        null;

    // ──────── Encoder layers ────────
    var layer: u32 = 0;
    while (layer < config.num_hidden_layers) : (layer += 1) {
        const layer_input = hidden;
        hidden = try encoderLayer(
            bld,
            config,
            hidden,
            attn_bias,
            try applyDropout(bld, rel_emb_gathered, options, .{ .kind = .relative_positions, .layer = layer }),
            if (options.project_relative_before_gather and !training_replay) relative.indices else null,
            pair_indices,
            rel_score_indices,
            use_fused_attention,
            batch,
            seq_len,
            layer,
            num_heads,
            head_dim,
            options,
        );
        if (options.regions) |observer| try observer.layer(observer.context, layer, layer_input, hidden);
    }

    return .{
        .input_ids_node = input_ids,
        .attn_bias_node = attn_bias,
        .output_node = hidden,
    };
}

// ──────── Embeddings block ────────
//
// DeBERTa-v3 embeddings are simpler than BERT:
//   - Word embedding lookup
//   - LayerNorm
//
// There are NO position embeddings (DeBERTa uses relative position in
// attention, not absolute position embeddings at the embedding layer).
// There are also no token-type embeddings in the v3 checkpoint layout
// used by GLiNER.
fn embeddings(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    total: u32,
    H: u32,
) !NodeId {
    const word_emb_param = try bld.parameter(
        "embeddings.word_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.vocab_size), @intCast(H) }),
    );
    const word_lookup = try embeddingLookupTyped(bld, word_emb_param, input_ids, total, H);

    const ln_w = try bld.parameter(
        "embeddings.LayerNorm.weight",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const ln_b = try bld.parameter(
        "embeddings.LayerNorm.bias",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    return bld.layerNorm(word_lookup, ln_w, ln_b, H, config.layer_norm_eps);
}

// ──────── Relative position embedding construction ────────
//
// DeBERTa-v3 disentangled attention uses a shared relative position
// embedding table: encoder.rel_embeddings.weight of shape [max_pos, H].
// This is normalized with encoder.LayerNorm (norm_rel_ebd), then indexed
// by log-bucketed relative position IDs to produce a [num_rel, H] tensor
// where num_rel = 2*seq_len - 1.
const RelativePositionTable = struct { table: NodeId, indices: NodeId };

fn buildRelativePositionTable(
    bld: *Builder,
    config: Config,
    seq_len: u32,
    H: u32,
    physical_i32_indices: bool,
    emit_indices: bool,
) !RelativePositionTable {
    const max_pos: u32 = config.max_position_embeddings;
    const num_buckets: u32 = config.position_buckets;
    const num_rel: u32 = 2 * seq_len - 1;

    // Shared relative position embedding table: [max_pos, H]
    const rel_emb_param = try bld.parameter(
        "encoder.rel_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(max_pos), @intCast(H) }),
    );

    // DeBERTa-v3 norm_rel_ebd: apply encoder.LayerNorm to raw rel_embeddings
    const rel_ln_w = try bld.parameter(
        "encoder.LayerNorm.weight",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const rel_ln_b = try bld.parameter(
        "encoder.LayerNorm.bias",
        Shape.init(.f32, &.{@intCast(H)}),
    );
    const rel_emb_normed = try bld.layerNorm(rel_emb_param, rel_ln_w, rel_ln_b, H, config.layer_norm_eps);

    if (!emit_indices) return .{ .table = rel_emb_normed, .indices = null_node };

    // Compute bucket IDs for relative positions -(seq_len-1) to +(seq_len-1).
    // Keep indices integer in graph metadata and physical constant storage.
    const bucket_ids = try bld.graph.allocator.alloc(i64, num_rel);
    defer bld.graph.allocator.free(bucket_ids);

    for (0..num_rel) |i| {
        const rel_pos: i64 = @as(i64, @intCast(i)) - @as(i64, @intCast(seq_len - 1));
        bucket_ids[i] = @intCast(deberta_config.relativePositionBucket(rel_pos, num_buckets, max_pos));
    }

    const bucket_ids_node = if (physical_i32_indices) block: {
        const ids = try bld.graph.allocator.alloc(i32, num_rel);
        defer bld.graph.allocator.free(ids);
        for (bucket_ids, ids) |source, *target| target.* = std.math.cast(i32, source) orelse return error.InvalidInputShape;
        break :block try bld.tensorConstBytes(std.mem.sliceAsBytes(ids), Shape.init(.i32, &.{@intCast(num_rel)}));
    } else try bld.tensorConstBytes(std.mem.sliceAsBytes(bucket_ids), Shape.init(.i64, &.{@intCast(num_rel)}));
    return .{ .table = rel_emb_normed, .indices = bucket_ids_node };
}

// ──────── Rel-score gather formulation (HF-equivalent, low-memory) ────────
//
// The legacy C2P/P2C decomposition Toeplitz-expands the relative embeddings
// to [S*S, H] and materializes [bh*S, S, D] products — batch*S^2*D-sized
// intermediates that dominate both training memory and GPU bandwidth.
// The HF DeBERTa formulation computes the same scores as a contraction over
// the SMALL num_rel = 2S-1 axis followed by a Toeplitz gather of the
// [bh, S, num_rel] score matrix:
//   c2p[b,h,qi,ki] = (Q_c @ K_r^T)[b,h,qi, qi-ki+S-1]
//   p2c[b,h,qi,ki] = (K_c @ Q_r^T)[b,h,ki, qi-ki+S-1]
// Largest intermediate drops from S^2*D (resp. S^2*H) to S*(2S-1) per head —
// a ~D-fold (~64x) reduction. Mathematically identical contraction; the
// accumulation order inside the dot product changes, so results agree to
// fp32 tolerance (not bit-exactness) with the legacy path.
//
// Default-ON: validated parity-clean on the native AND metal strict Python
// gates (entity-only, all metal-readiness checks green) and across the
// native==metal / graph==direct / legacy==restructured correctness ladder.
// It also eliminates the [bh,S,S,D] materialization that OOMed Metal at long
// sequences and is ~28% faster. Set TERMITE_DEBERTA_REL_SCORE_GATHER=0 to fall
// back to the legacy Toeplitz-expansion path.
fn relScoreGatherEnabled() bool {
    // wasm-freestanding has no libc/getenv; the browser path takes the default.
    if (@import("builtin").target.cpu.arch.isWasm()) return true;
    return platform.env.getenvBoolDefault("TERMITE_DEBERTA_REL_SCORE_GATHER", true);
}

// Fused disentangled attention: replaces the c2c/c2p/p2c + softmax + context
// block with a single flash-style kernel (forward) + a custom-VJP backward
// kernel, avoiding the [bh,S,S] score materialization. Default-ON (2026-06-14):
// validated Python-exact on native AND Metal across b2/s64, b2/s128, b4/s128,
// and ~2x lower GPU frame time.
//
// WARNING: TERMITE_DEBERTA_FUSED_ATTENTION=0 selects the legacy score-gather
// path, which is a debugging aid ONLY on native. The score-gather path has a
// known correctness bug on the Metal backend at seq_len>=128 (produces wrong
// numbers), so do NOT disable the fused path for Metal training/inference at
// seq>=128. Left as an escape hatch for native-backend diagnosis; keep the
// default (fused) in production.
/// Returns the effective fused-attention setting used by this graph builder.
///
/// Production callers use this to fail closed before selecting Metal for
/// sequence lengths where the debug-only legacy path is known to be incorrect.
pub fn fusedDisentangledAttentionEnabledForProduction() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return platform.env.getenvBoolDefault("TERMITE_DEBERTA_FUSED_ATTENTION", true);
}

pub fn fuseLayerNormBackwardEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return platform.env.getenvBoolDefault("TERMITE_FUSE_LAYER_NORM", false);
}

const RelScoreIndices = struct {
    /// [S*S] i64: c2p_flat[qi*S+ki] = qi*num_rel + (qi-ki+S-1)
    c2p: NodeId,
    /// [S*S] i64: p2c_flat[qi*S+ki] = ki*num_rel + (qi-ki+S-1)
    p2c: NodeId,
    /// Materialized training uses a plain row gather of the contiguous score
    /// buffer, avoiding the legacy transpose/gather/transpose Metal route.
    flat_heads: bool = false,
};

fn buildFlatRelScoreIndices(bld: *Builder, bh: u32, seq_len: u32) !RelScoreIndices {
    const ss = try std.math.mul(u32, seq_len, seq_len);
    const count = try std.math.mul(u32, bh, ss);
    const num_rel = try std.math.sub(u32, try std.math.mul(u32, seq_len, 2), 1);
    const source_per_head = try std.math.mul(u32, seq_len, num_rel);
    const source_count = try std.math.mul(u32, bh, source_per_head);
    if (source_count > std.math.maxInt(i32)) return error.InvalidInputShape;
    const data = try bld.graph.allocator.alloc(i32, count);
    defer bld.graph.allocator.free(data);
    for (0..bh) |head| for (0..seq_len) |qi| for (0..seq_len) |ki| {
        const rel = qi + seq_len - 1 - ki;
        data[head * ss + qi * seq_len + ki] = @intCast(head * source_per_head + qi * num_rel + rel);
    };
    const c2p = try bld.tensorConstBytes(std.mem.sliceAsBytes(data), Shape.init(.i32, &.{@intCast(count)}));
    for (0..bh) |head| for (0..seq_len) |qi| for (0..seq_len) |ki| {
        const rel = qi + seq_len - 1 - ki;
        data[head * ss + qi * seq_len + ki] = @intCast(head * source_per_head + ki * num_rel + rel);
    };
    const p2c = try bld.tensorConstBytes(std.mem.sliceAsBytes(data), Shape.init(.i32, &.{@intCast(count)}));
    return .{ .c2p = c2p, .p2c = p2c, .flat_heads = true };
}

/// Flat indices into a row-major [S, num_rel] score matrix for the Toeplitz
/// score gather. Store integer constants directly: long-context flattened
/// indices exceed the exact integer range of f32.
fn buildRelScoreIndices(bld: *Builder, seq_len: u32) !RelScoreIndices {
    if (seq_len == 0) return error.InvalidInputShape;
    const ss = try std.math.mul(u32, seq_len, seq_len);
    const num_rel = (try std.math.mul(u32, 2, seq_len)) - 1;
    const data = try bld.graph.allocator.alloc(i64, ss);
    defer bld.graph.allocator.free(data);

    for (0..seq_len) |qi| {
        for (0..seq_len) |ki| {
            const rel_idx: i64 = @as(i64, @intCast(qi)) - @as(i64, @intCast(ki)) + @as(i64, @intCast(seq_len - 1));
            data[qi * seq_len + ki] = @as(i64, @intCast(qi)) * @as(i64, @intCast(num_rel)) + rel_idx;
        }
    }
    const c2p = try bld.tensorConstBytes(std.mem.sliceAsBytes(data), Shape.init(.i64, &.{@intCast(ss)}));

    for (0..seq_len) |qi| {
        for (0..seq_len) |ki| {
            const rel_idx: i64 = @as(i64, @intCast(qi)) - @as(i64, @intCast(ki)) + @as(i64, @intCast(seq_len - 1));
            data[qi * seq_len + ki] = @as(i64, @intCast(ki)) * @as(i64, @intCast(num_rel)) + rel_idx;
        }
    }
    const p2c = try bld.tensorConstBytes(std.mem.sliceAsBytes(data), Shape.init(.i64, &.{@intCast(ss)}));

    return .{ .c2p = c2p, .p2c = p2c };
}

/// [num_rel, H] projected relative embeddings → [bh, D, num_rel], split per
/// head and tiled across batch (the rel embeddings are batch-invariant, so
/// the tile is a broadcast of a tiny [nh, D, num_rel] tensor — NOT the
/// [bh, S, S, D] expansion of the legacy path).
fn relProjPerHeadTiled(
    bld: *Builder,
    rel: NodeId, // [num_rel, H]
    batch: u32,
    num_heads: u32,
    head_dim: u32,
    num_rel: u32,
) !NodeId {
    const bh: u32 = batch * num_heads;
    // [num_rel, H] → [num_rel, nh, D] → [nh, D, num_rel]
    const rel_rnd = try bld.reshape(rel, Shape.init(.f32, &.{
        @intCast(num_rel), @intCast(num_heads), @intCast(head_dim),
    }));
    const rel_ndr = try bld.transpose(rel_rnd, &.{ 1, 2, 0 });
    if (batch == 1) return rel_ndr; // bh == nh

    const rel_4d = try bld.reshape(rel_ndr, Shape.init(.f32, &.{
        1, @intCast(num_heads), @intCast(head_dim), @intCast(num_rel),
    }));
    const target = Shape.init(.f32, &.{
        @intCast(batch), @intCast(num_heads), @intCast(head_dim), @intCast(num_rel),
    });
    const bcast = try bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = target,
            .broadcast_axes = .{ 0, 1, 2, 3, 0, 0, 0, 0 },
            .num_axes = 4,
        } },
        .output_shape = target,
        .inputs = .{ rel_4d, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    return bld.reshape(bcast, Shape.init(.f32, &.{
        @intCast(bh), @intCast(head_dim), @intCast(num_rel),
    }));
}

/// Toeplitz-gather a [bh, S, num_rel] score matrix into [bh, S, S] using a
/// precomputed flat index (see buildRelScoreIndices). Routes through the
/// existing differentiable embeddingLookup (axis-0 row gather) by moving bh
/// to the trailing axis.
fn gatherRelScores(
    bld: *Builder,
    scores: NodeId, // [bh, S, num_rel]
    flat_idx: NodeId, // [S*S] i64 into the flattened [S*num_rel] axis
    bh: u32,
    seq_len: u32,
    num_rel: u32,
    flat_heads: bool,
) !NodeId {
    const ss: u32 = seq_len * seq_len;
    if (flat_heads) {
        const flat = try bld.reshape(scores, Shape.init(.f32, &.{ @intCast(bh * seq_len * num_rel), 1 }));
        const gathered = try embeddingLookupTyped(bld, flat, flat_idx, bh * ss, 1);
        return bld.reshape(gathered, Shape.init(.f32, &.{ @intCast(bh), @intCast(seq_len), @intCast(seq_len) }));
    }
    const flat = try bld.reshape(scores, Shape.init(.f32, &.{
        @intCast(bh), @intCast(seq_len * num_rel),
    }));
    const flat_t = try bld.transpose(flat, &.{ 1, 0 }); // [S*num_rel, bh]
    const gathered = try embeddingLookupTyped(bld, flat_t, flat_idx, ss, bh); // [S*S, bh]
    const gathered_t = try bld.transpose(gathered, &.{ 1, 0 }); // [bh, S*S]
    return bld.reshape(gathered_t, Shape.init(.f32, &.{
        @intCast(bh), @intCast(seq_len), @intCast(seq_len),
    }));
}

/// C2P via score-gather: c2p[bh,qi,ki] = (Q_c @ K_r^T)[bh, qi, qi-ki+S-1].
fn contentToPositionGather(
    bld: *Builder,
    q_c: NodeId, // [bh, S, D]
    k_r: NodeId, // [num_rel, H]
    indices: RelScoreIndices,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const num_rel: u32 = 2 * seq_len - 1;
    const krt = try relProjPerHeadTiled(bld, k_r, batch, num_heads, head_dim, num_rel);
    const scores = try bld.matmul3D(q_c, krt); // [bh, S, num_rel]
    return gatherRelScores(bld, scores, indices.c2p, batch * num_heads, seq_len, num_rel, indices.flat_heads);
}

/// P2C via score-gather: p2c[bh,qi,ki] = (K_c @ Q_r^T)[bh, ki, qi-ki+S-1].
fn positionToContentGather(
    bld: *Builder,
    k_c: NodeId, // [bh, S, D]
    q_r: NodeId, // [num_rel, H]
    indices: RelScoreIndices,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const num_rel: u32 = 2 * seq_len - 1;
    const qrt = try relProjPerHeadTiled(bld, q_r, batch, num_heads, head_dim, num_rel);
    const scores = try bld.matmul3D(k_c, qrt); // [bh, S(ki), num_rel]
    return gatherRelScores(bld, scores, indices.p2c, batch * num_heads, seq_len, num_rel, indices.flat_heads);
}

// ──────── Pair indices for Toeplitz C2P/P2C selection ────────
//
// For disentangled attention, C2P and P2C require gathering from relative
// position embeddings using (qi, ki) → rel_idx = qi - ki + seq_len - 1.
// This builds a constant [S*S] index array used to gather K_r and Q_r
// into [S*S, H] tensors for the C2P and P2C dot products.
fn buildPairIndices(
    bld: *Builder,
    seq_len: u32,
) !NodeId {
    if (seq_len == 0) return error.InvalidInputShape;
    const ss = try std.math.mul(u32, seq_len, seq_len);
    const pair_data = try bld.graph.allocator.alloc(i64, ss);
    defer bld.graph.allocator.free(pair_data);

    for (0..seq_len) |qi| {
        for (0..seq_len) |ki| {
            const rel_idx: i64 = @as(i64, @intCast(qi)) - @as(i64, @intCast(ki)) + @as(i64, @intCast(seq_len - 1));
            pair_data[qi * seq_len + ki] = rel_idx;
        }
    }

    return bld.tensorConstBytes(
        std.mem.sliceAsBytes(pair_data),
        Shape.init(.i64, &.{@intCast(ss)}),
    );
}

// ──────── Content-to-Position (C2P) attention component ────────
//
// Computes: c2p[bh, qi, ki] = dot(Q_c[bh, qi, :], K_r[qi-ki+S-1, :])
//
// Strategy:
//   1. Gather K_r with pair_indices → [S*S, H] (Toeplitz expansion)
//   2. Reshape to per-head [nh, S, S, D], tile across batch → [bh, S, S, D]
//   3. For each (bh, qi): dot Q_c[qi] against K_r_gathered[qi, :, :]
//      via matmul3D: [bh*S, 1, D] @ [bh*S, D, S] → [bh*S, 1, S]
//   4. Reshape to [bh, S, S]
fn contentToPosition(
    bld: *Builder,
    q_c: NodeId, // [bh, S, D] — content queries
    k_r: NodeId, // [num_rel, H] — projected relative position keys
    pair_indices: NodeId, // [S*S] i64
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const bh: u32 = batch * num_heads;
    const ss: u32 = seq_len * seq_len;
    const H: u32 = num_heads * head_dim;

    // 1. Gather K_r with pair_indices: [num_rel, H] → [S*S, H]
    const rel_gathered = try embeddingLookupTyped(bld, k_r, pair_indices, ss, H);

    // 2. Reshape to per-head and tile across batch
    const rel_tiled = try tileRelEmbAcrossBatch(bld, rel_gathered, batch, seq_len, num_heads, head_dim);

    // 3. matmul3D: Q_c[bh*S, 1, D] @ K_r_gathered^T[bh*S, D, S] → [bh*S, 1, S]
    const rel_t = try bld.transpose(rel_tiled, &.{ 0, 2, 1 }); // [bh*S, D, S]
    const q_flat = try bld.reshape(q_c, Shape.init(.f32, &.{
        @intCast(bh * seq_len), 1, @intCast(head_dim),
    }));
    const result_raw = try bld.matmul3D(q_flat, rel_t); // [bh*S, 1, S]

    // 4. Reshape to [bh, S, S]
    return bld.reshape(result_raw, Shape.init(.f32, &.{
        @intCast(bh), @intCast(seq_len), @intCast(seq_len),
    }));
}

// ──────── Position-to-Content (P2C) attention component ────────
//
// Computes: p2c[bh, qi, ki] = dot(Q_r[qi-ki+S-1, :], K_c[bh, ki, :])
//
// Note: K_c is indexed at position ki (not qi). The Q_r gathered tensor
// has entries Q_r[qi-ki+S-1] at position (qi, ki) from the Toeplitz gather.
//
// Strategy:
//   1. Gather Q_r with pair_indices → [S*S, H] (Toeplitz expansion)
//   2. Reshape to per-head [nh, S, S, D], tile across batch → [bh*S, S, D]
//   3. Broadcast K_c across qi: [bh, S, D] → [bh, S, S, D] → [bh*S, S, D]
//   4. Element-wise multiply + reduce_sum over D axis:
//      [bh*S, S, D] * [bh*S, S, D] → reduce → [bh*S, S, 1] → [bh, S, S]
fn positionToContent(
    bld: *Builder,
    k_c: NodeId, // [bh, S, D] — content keys
    q_r: NodeId, // [num_rel, H] — projected relative position queries
    pair_indices: NodeId, // [S*S] i64
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const bh: u32 = batch * num_heads;
    const ss: u32 = seq_len * seq_len;
    const H: u32 = num_heads * head_dim;

    // 1. Gather Q_r with pair_indices: [num_rel, H] → [S*S, H]
    const rel_gathered = try embeddingLookupTyped(bld, q_r, pair_indices, ss, H);

    // 2. Reshape to per-head and tile across batch → [bh*S, S, D]
    const rel_tiled = try tileRelEmbAcrossBatch(bld, rel_gathered, batch, seq_len, num_heads, head_dim);

    // 3. Broadcast K_c across qi dimension:
    //    K_c: [bh, S, D] → [bh, 1, S, D] → broadcast to [bh, S, S, D] → [bh*S, S, D]
    const kc_4d = try bld.reshape(k_c, Shape.init(.f32, &.{
        @intCast(bh), 1, @intCast(seq_len), @intCast(head_dim),
    }));
    const target_kc = Shape.init(.f32, &.{
        @intCast(bh), @intCast(seq_len), @intCast(seq_len), @intCast(head_dim),
    });
    const kc_broadcast = try bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = target_kc,
            .broadcast_axes = .{ 0, 1, 2, 3, 0, 0, 0, 0 },
            .num_axes = 4,
        } },
        .output_shape = target_kc,
        .inputs = .{ kc_4d, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const kc_flat = try bld.reshape(kc_broadcast, Shape.init(.f32, &.{
        @intCast(bh * seq_len), @intCast(seq_len), @intCast(head_dim),
    }));

    // 4. Element-wise multiply: [bh*S, S, D] * [bh*S, S, D] → [bh*S, S, D]
    const product = try bld.mul(rel_tiled, kc_flat);

    // 5. Reduce sum over D (last axis) → [bh*S, S, 1]
    const summed = try bld.reduceSum(product, &.{2});

    // 6. Reshape to [bh, S, S]
    return bld.reshape(summed, Shape.init(.f32, &.{
        @intCast(bh), @intCast(seq_len), @intCast(seq_len),
    }));
}

// ──────── Shared helper: tile rel_emb across batch ────────
//
// Takes [S*S, H] gathered relative embeddings and produces [bh*S, S, D]:
//   [S*S, H] → [S*S, nh, D] → [nh, S, S, D] → broadcast → [bh, S, S, D] → [bh*S, S, D]
fn tileRelEmbAcrossBatch(
    bld: *Builder,
    rel_gathered: NodeId, // [S*S, H]
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const bh: u32 = batch * num_heads;
    const ss: u32 = seq_len * seq_len;

    // [S*S, H] → [S*S, nh, D] → [nh, S*S, D]
    const rel_snh = try bld.reshape(rel_gathered, Shape.init(.f32, &.{
        @intCast(ss), @intCast(num_heads), @intCast(head_dim),
    }));
    const rel_nhs = try bld.transpose(rel_snh, &.{ 1, 0, 2 });
    // → [nh, S, S, D]
    const rel_nhqk = try bld.reshape(rel_nhs, Shape.init(.f32, &.{
        @intCast(num_heads), @intCast(seq_len), @intCast(seq_len), @intCast(head_dim),
    }));

    // Tile across batch if batch > 1 using broadcast_in_dim
    const rel_bhqk = if (batch > 1) blk: {
        // [nh, S, S, D] → [1, nh, S, S, D]
        const rel_5d = try bld.reshape(rel_nhqk, Shape.init(.f32, &.{
            1, @intCast(num_heads), @intCast(seq_len), @intCast(seq_len), @intCast(head_dim),
        }));
        const target_shape = Shape.init(.f32, &.{
            @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(seq_len), @intCast(head_dim),
        });
        const bcast = try bld.graph.addNode(.{
            .op = .{ .broadcast_in_dim = .{
                .target_shape = target_shape,
                .broadcast_axes = .{ 0, 1, 2, 3, 4, 0, 0, 0 },
                .num_axes = 5,
            } },
            .output_shape = target_shape,
            .inputs = .{ rel_5d, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        // [batch, nh, S, S, D] → [bh, S, S, D]
        break :blk try bld.reshape(bcast, Shape.init(.f32, &.{
            @intCast(bh), @intCast(seq_len), @intCast(seq_len), @intCast(head_dim),
        }));
    } else blk: {
        // batch==1: [nh, S, S, D] already has the right shape
        break :blk rel_nhqk;
    };

    // [bh, S, S, D] → [bh*S, S, D]
    return bld.reshape(rel_bhqk, Shape.init(.f32, &.{
        @intCast(bh * seq_len), @intCast(seq_len), @intCast(head_dim),
    }));
}

// ──────── Encoder layer ────────
//
// Self-attention with disentangled attention decomposition (C2C + C2P +
// P2C), followed by output projection, residual, LayerNorm, FFN,
// residual, LayerNorm.
fn encoderLayer(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    attn_bias: NodeId,
    rel_emb_gathered: NodeId,
    relative_indices: ?NodeId,
    pair_indices: ?NodeId,
    rel_score_indices: ?RelScoreIndices,
    use_fused_attention: bool,
    batch: u32,
    seq_len: u32,
    layer: u32,
    num_heads: u32,
    head_dim: u32,
    options: BuildOptions,
) !NodeId {
    const H: u32 = config.hidden_size;
    const I: u32 = config.intermediate_size;
    const total: u32 = batch * seq_len;
    const num_rel: u32 = 2 * seq_len - 1;

    const q_suffix = if (config.use_v3_names) "attention.self.query_proj" else "attention.self.query";
    const k_suffix = if (config.use_v3_names) "attention.self.key_proj" else "attention.self.key";
    const v_suffix = if (config.use_v3_names) "attention.self.value_proj" else "attention.self.value";

    // ──────── Q, K, V linear projections (content) ────────
    const q_w = try layerParam2D(bld, layer, q_suffix, ".weight", H, H);
    const q_b = try layerParam1D(bld, layer, q_suffix, ".bias", H);
    const Q = try bld.linear(hidden_in, q_w, q_b, total, H, H);

    const k_w = try layerParam2D(bld, layer, k_suffix, ".weight", H, H);
    const k_b = try layerParam1D(bld, layer, k_suffix, ".bias", H);
    const K = try bld.linear(hidden_in, k_w, k_b, total, H, H);

    const v_w = try layerParam2D(bld, layer, v_suffix, ".weight", H, H);
    const v_b = try layerParam1D(bld, layer, v_suffix, ".bias", H);
    const V = try bld.linear(hidden_in, v_w, v_b, total, H, H);

    // ──────── Relative position Q_r and K_r projections ────────
    // DeBERTa-v3 with share_att_key=true: Q_r and K_r share the same
    // projection weights as Q and K respectively.
    // rel_emb_gathered: [num_rel, H] → project through same Q/K weights
    const relative_rows = if (relative_indices != null or options.attention == .training_replay_v1) config.max_position_embeddings else num_rel;
    const Q_r_projection = try bld.linear(rel_emb_gathered, q_w, q_b, relative_rows, H, H);
    const K_r_projection = try bld.linear(rel_emb_gathered, k_w, k_b, relative_rows, H, H);
    const Q_r = if (relative_indices) |indices| try embeddingLookupTyped(bld, Q_r_projection, indices, num_rel, H) else Q_r_projection;
    const K_r = if (relative_indices) |indices| try embeddingLookupTyped(bld, K_r_projection, indices, num_rel, H) else K_r_projection;

    // ──────── Disentangled attention: C2C + C2P + P2C ────────
    //
    // C2C: Q_c @ K_c^T (standard content-to-content)
    // C2P: Q_c · K_r[qi-ki+S-1] (content-to-position)
    // P2C: Q_r[qi-ki+S-1] · K_c (position-to-content)
    //
    // scores = (C2C + C2P + P2C) / sqrt(3 * head_dim) + attn_bias
    const attn_merged = if (options.training_attention_v1) |replay| blk: {
        const packed_qkv = try bld.concat(try bld.concat(Q, K, 0), V, 0);
        const packed_relative = try bld.concat(Q_r_projection, K_r_projection, 0);
        break :blk try bld.debertaTrainingAttentionV1(packed_qkv, packed_relative, replay.control, .{
            .batch = batch,
            .seq_len = seq_len,
            .num_heads = num_heads,
            .head_dim = head_dim,
            .relative_rows = config.max_position_embeddings,
            .dropout_probability = replay.probability,
            .dropout_stream_id = (@as(u64, layer) << 32) | (@as(u64, @intFromEnum(DropoutKind.attention_probabilities)) + 1),
        });
    } else if (use_fused_attention) blk: {
        // Single fused kernel over the [batch*seq, H] content projections and
        // the [num_rel, H] relative projections. Inputs are packed to fit the
        // 4-slot node: qkv_packed = [Q;K;V] ([3*total, H]),
        // qr_kr_packed = [Q_r;K_r] ([2*num_rel, H]). Output [total, H] matches
        // attn_merged below. The custom VJP (autodiff.zig) emits the backward
        // kernel; the concat VJPs split the packed grads back to Q/K/V/Q_r/K_r.
        const qk = try bld.concat(Q, K, 0);
        const qkv_packed = try bld.concat(qk, V, 0);
        const qr_kr_packed = try bld.concat(Q_r, K_r, 0);
        break :blk try bld.graph.addNode(.{
            .op = .{ .fused_disentangled_attention = .{
                .batch = batch,
                .seq_len = seq_len,
                .num_heads = num_heads,
                .head_dim = head_dim,
            } },
            .output_shape = Shape.init(.f32, &.{ @intCast(total), @intCast(H) }),
            .inputs = .{ qkv_packed, qr_kr_packed, attn_bias, null_node },
            .num_inputs = 3,
            .vjp_alternate = null_node,
        });
    } else blk: {
        // ──────── Multi-head reshape: [total, H] → [bh, seq, head_dim] ────────
        const q_bsnh = try bld.reshape(Q, Shape.init(.f32, &.{
            @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim),
        }));
        const k_bsnh = try bld.reshape(K, Shape.init(.f32, &.{
            @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim),
        }));
        const v_bsnh = try bld.reshape(V, Shape.init(.f32, &.{
            @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim),
        }));
        // [batch, seq, num_heads, head_dim] → [batch, num_heads, seq, head_dim]
        const q_bnsh = try bld.transpose(q_bsnh, &.{ 0, 2, 1, 3 });
        const k_bnsh = try bld.transpose(k_bsnh, &.{ 0, 2, 1, 3 });
        const v_bnsh = try bld.transpose(v_bsnh, &.{ 0, 2, 1, 3 });
        // Flatten batch*num_heads → [bh, seq, head_dim]
        const q_bhsd = try bld.reshape(q_bnsh, Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim),
        }));
        const k_bhsd = try bld.reshape(k_bnsh, Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim),
        }));
        const v_bhsd = try bld.reshape(v_bnsh, Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim),
        }));

        const k_t = try bld.transpose(k_bhsd, &.{ 0, 2, 1 });
        const c2c = try bld.matmul3D(q_bhsd, k_t);

        const c2p = if (rel_score_indices) |indices|
            try contentToPositionGather(bld, q_bhsd, K_r, indices, batch, seq_len, num_heads, head_dim)
        else
            try contentToPosition(bld, q_bhsd, K_r, pair_indices orelse return error.InvalidAttentionPlan, batch, seq_len, num_heads, head_dim);
        const p2c = if (rel_score_indices) |indices|
            try positionToContentGather(bld, k_bhsd, Q_r, indices, batch, seq_len, num_heads, head_dim)
        else
            try positionToContent(bld, k_bhsd, Q_r, pair_indices orelse return error.InvalidAttentionPlan, batch, seq_len, num_heads, head_dim);

        const scores_sum = try bld.add(c2c, try bld.add(c2p, p2c));
        const scale = try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)) * 3.0));
        const scores_scaled = try bld.mul(scores_sum, scale);
        const scores_valid = if (options.attention_score_mask) |mask| try bld.mul(scores_scaled, mask) else scores_scaled;
        const scores_masked = try bld.add(scores_valid, attn_bias);
        const probs = try applyDropout(bld, try bld.softmax(scores_masked), options, .{ .kind = .attention_probabilities, .layer = layer });
        const attn_bhsd = try bld.matmul3D(probs, v_bhsd);

        // Reshape back: [bh, seq, head_dim] → [total, H]
        const attn_bnsh = try bld.reshape(attn_bhsd, Shape.init(.f32, &.{
            @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(head_dim),
        }));
        const attn_bsnh = try bld.transpose(attn_bnsh, &.{ 0, 2, 1, 3 });
        break :blk try bld.reshape(attn_bsnh, Shape.init(.f32, &.{
            @intCast(total), @intCast(H),
        }));
    };

    // ──────── Attention output projection + residual + LayerNorm ────────
    const o_w = try layerParam2D(bld, layer, "attention.output.dense", ".weight", H, H);
    const o_b = try layerParam1D(bld, layer, "attention.output.dense", ".bias", H);
    const attn_proj = try applyDropout(bld, try bld.linear(attn_merged, o_w, o_b, total, H, H), options, .{ .kind = .attention_output, .layer = layer });

    const attn_res = try bld.add(attn_proj, hidden_in);
    const attn_ln_w = try layerParam1D(bld, layer, "attention.output.LayerNorm", ".weight", H);
    const attn_ln_b = try layerParam1D(bld, layer, "attention.output.LayerNorm", ".bias", H);
    const attn_normed = try bld.layerNorm(attn_res, attn_ln_w, attn_ln_b, H, config.layer_norm_eps);

    // ──────── FFN: intermediate → GELU → output ────────
    const ffn_i_w = try layerParam2D(bld, layer, "intermediate.dense", ".weight", I, H);
    const ffn_i_b = try layerParam1D(bld, layer, "intermediate.dense", ".bias", I);
    const ffn_inter = try bld.linear(attn_normed, ffn_i_w, ffn_i_b, total, H, I);
    const ffn_gelu = try bld.geluExact(ffn_inter);

    const ffn_o_w = try layerParam2D(bld, layer, "output.dense", ".weight", H, I);
    const ffn_o_b = try layerParam1D(bld, layer, "output.dense", ".bias", H);
    const ffn_out = try applyDropout(bld, try bld.linear(ffn_gelu, ffn_o_w, ffn_o_b, total, I, H), options, .{ .kind = .ffn_output, .layer = layer });

    const ffn_res = try bld.add(ffn_out, attn_normed);
    const ffn_ln_w = try layerParam1D(bld, layer, "output.LayerNorm", ".weight", H);
    const ffn_ln_b = try layerParam1D(bld, layer, "output.LayerNorm", ".bias", H);
    return bld.layerNorm(ffn_res, ffn_ln_w, ffn_ln_b, H, config.layer_norm_eps);
}

// ──────── Parameter naming helpers ────────

/// Build a per-layer parameter name like
/// `encoder.layer.{layer}.{prefix}{suffix}` and emit a 2-D parameter node.
fn layerParam2D(
    bld: *Builder,
    layer: u32,
    prefix: []const u8,
    suffix: []const u8,
    d0: u32,
    d1: u32,
) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(
        &name_buf,
        "encoder.layer.{d}.{s}{s}",
        .{ layer, prefix, suffix },
    );
    return bld.parameter(
        name,
        Shape.init(.f32, &.{ @intCast(d0), @intCast(d1) }),
    );
}

/// 1-D variant of `layerParam2D` for biases and LayerNorm parameters.
fn layerParam1D(
    bld: *Builder,
    layer: u32,
    prefix: []const u8,
    suffix: []const u8,
    d0: u32,
) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(
        &name_buf,
        "encoder.layer.{d}.{s}{s}",
        .{ layer, prefix, suffix },
    );
    return bld.parameter(
        name,
        Shape.init(.f32, &.{@intCast(d0)}),
    );
}

// ──────── Tests ────────

test "deberta relative gather constants preserve integer coordinate layouts" {
    var g = Graph.init(std.testing.allocator);
    defer g.deinit();
    var bld = Builder.init(&g);
    const indices = try buildRelScoreIndices(&bld, 3);
    const pair = try buildPairIndices(&bld, 3);
    const expected = [_][]const i64{
        &.{ 2, 1, 0, 8, 7, 6, 14, 13, 12 },
        &.{ 2, 6, 10, 3, 7, 11, 4, 8, 12 },
        &.{ 2, 1, 0, 3, 2, 1, 4, 3, 2 },
    };
    for ([_]NodeId{ indices.c2p, indices.p2c, pair }, expected) |id, values| {
        const node = g.node(id);
        try std.testing.expectEqual(.i64, node.output_shape.dtype);
        // There must be no lossy f32 constant followed by an integer cast.
        try std.testing.expect(node.op == .constant);
        const c = node.op.constant;
        try std.testing.expectEqualSlices(i64, values, g.constantDataAs(i64, c.data_offset, c.data_len));
    }
    try std.testing.expectError(error.InvalidInputShape, buildRelScoreIndices(&bld, 0));
    try std.testing.expectError(error.Overflow, buildRelScoreIndices(&bld, 65536));
    try std.testing.expectError(error.InvalidInputShape, buildPairIndices(&bld, 0));
    try std.testing.expectError(error.Overflow, buildPairIndices(&bld, 65536));
}

test "deberta fused long context graph construction has bounded constant memory" {
    if (!fusedDisentangledAttentionEnabledForProduction()) return error.SkipZigTest;
    // This constructs the real 4096-position graph without allocating model
    // weights or activations. The old eager index tables needed hundreds of MB.
    var storage: [768 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var g = Graph.init(fixed.allocator());
    defer g.deinit();
    var bld = Builder.init(&g);
    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .intermediate_size = 64,
    };
    const input = try bld.parameter("__input_ids", Shape.init(.i64, &.{4096}));
    const bias = try bld.parameter("__attn_bias", Shape.init(.f32, &.{ 4, 4096, 4096 }));
    const output = try buildForwardGraph(&bld, config, input, bias, 1, 4096);
    try std.testing.expectEqual(@as(i64, 4096), g.node(output.output_node).output_shape.dims[0]);
    try std.testing.expect(g.constant_pool.items.len < 4096 * 64);
    var fused_count: usize = 0;
    for (g.nodes.items) |node| {
        if (node.op == .fused_disentangled_attention) fused_count += 1;
        if (node.op == .constant) try std.testing.expect(node.output_shape.numElements().? < 4096 * 4);
    }
    try std.testing.expectEqual(@as(usize, 1), fused_count);
}

test "buildForwardGraph constructs DeBERTa graph with correct output shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
        .position_buckets = 32,
    };

    const batch: u32 = 1;
    const seq_len: u32 = 8;
    const num_heads: u32 = config.num_attention_heads;
    const total: u32 = batch * seq_len;

    // Caller-created placeholders (per the placeholder-wiring fix).
    const input_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.i64, &.{@intCast(total)}),
    );
    const attn_bias = try bld.parameter(
        "__attn_bias",
        Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len),
        }),
    );

    const deberta_graph = try buildForwardGraph(&bld, config, input_ids, attn_bias, batch, seq_len);

    // Output shape should be [batch*seq, hidden] = [8, 32].
    const out_shape = g.node(deberta_graph.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(1));

    // Sanity-check the returned placeholder fields.
    try std.testing.expectEqual(input_ids, deberta_graph.input_ids_node);
    try std.testing.expectEqual(attn_bias, deberta_graph.attn_bias_node);
}

test "buildForwardGraph uses v3 parameter names by default" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
        .position_buckets = 32,
    };

    const batch: u32 = 1;
    const seq_len: u32 = 4;
    const num_heads: u32 = config.num_attention_heads;
    const total: u32 = batch * seq_len;

    const input_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.i64, &.{@intCast(total)}),
    );
    const attn_bias = try bld.parameter(
        "__attn_bias",
        Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len),
        }),
    );

    _ = try buildForwardGraph(&bld, config, input_ids, attn_bias, batch, seq_len);

    // Check that at least one parameter with the v3 `query_proj` name exists.
    var found_query_proj = false;
    for (g.parameters.items) |param_id| {
        const name = g.parameterName(g.node(param_id));
        if (std.mem.indexOf(u8, name, "query_proj") != null) {
            found_query_proj = true;
            break;
        }
    }
    try std.testing.expect(found_query_proj);
}

test "buildForwardGraph has no position_embeddings parameter" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
        .position_buckets = 32,
    };

    const batch: u32 = 1;
    const seq_len: u32 = 4;
    const num_heads: u32 = config.num_attention_heads;
    const total: u32 = batch * seq_len;

    const input_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.i64, &.{@intCast(total)}),
    );
    const attn_bias = try bld.parameter(
        "__attn_bias",
        Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len),
        }),
    );

    _ = try buildForwardGraph(&bld, config, input_ids, attn_bias, batch, seq_len);

    // DeBERTa has NO absolute position embeddings.
    for (g.parameters.items) |param_id| {
        const name = g.parameterName(g.node(param_id));
        try std.testing.expect(std.mem.indexOf(u8, name, "position_embeddings") == null);
    }
}

test "buildForwardGraph parameter count is reasonable" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const config = Config{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
        .position_buckets = 32,
    };

    const batch: u32 = 1;
    const seq_len: u32 = 4;
    const num_heads: u32 = config.num_attention_heads;
    const total: u32 = batch * seq_len;

    const input_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.i64, &.{@intCast(total)}),
    );
    const attn_bias = try bld.parameter(
        "__attn_bias",
        Shape.init(.f32, &.{
            @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len),
        }),
    );

    _ = try buildForwardGraph(&bld, config, input_ids, attn_bias, batch, seq_len);

    // Expected parameter count (only graph-owned `parameter` nodes are counted):
    //   Embeddings: word_embeddings.weight + LN.weight + LN.bias         = 3
    //   Rel position: rel_embeddings.weight + encoder.LN.weight + LN.bias = 3
    //   Per encoder layer:
    //     query_proj  w+b   = 2
    //     key_proj    w+b   = 2
    //     value_proj  w+b   = 2
    //     output.dense w+b  = 2
    //     output.LayerNorm w+b = 2
    //     intermediate.dense w+b = 2
    //     output.dense (ffn) w+b = 2
    //     output.LayerNorm (ffn) w+b = 2
    //   Total per layer = 16
    //   Plus 2 caller-created input placeholders (input_ids, attn_bias).
    //   Grand total = 3 + 3 + 16*num_layers + 2 = 6 + 32 + 2 = 40 for num_layers=2.
    //
    // We assert >= 35 to leave slack while checking the relative position
    // parameters are present.
    try std.testing.expect(g.parameters.items.len >= 35);

    // Verify relative position embedding parameters are present.
    var found_rel_emb = false;
    var found_enc_ln = false;
    for (g.parameters.items) |param_id| {
        const name = g.parameterName(g.node(param_id));
        if (std.mem.eql(u8, name, "encoder.rel_embeddings.weight")) found_rel_emb = true;
        if (std.mem.eql(u8, name, "encoder.LayerNorm.weight")) found_enc_ln = true;
    }
    try std.testing.expect(found_rel_emb);
    try std.testing.expect(found_enc_ln);
}
