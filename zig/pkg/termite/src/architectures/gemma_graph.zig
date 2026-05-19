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

const std = @import("std");
const ml = @import("ml");
const gpt_model = @import("../models/gpt.zig");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const node_mod = ml.graph.node;
const null_node = ml.graph.null_node;

pub const Config = gpt_model.Config;

pub const GemmaInputs = struct {
    input_ids: NodeId,
    input_embeddings: NodeId = null_node,
    rope_cos: NodeId,
    rope_sin: NodeId,
};

pub const GemmaGraph = struct {
    input_ids_node: NodeId,
    rope_cos_node: NodeId,
    rope_sin_node: NodeId,
    output_node: NodeId,
};

pub const LayerRef = struct {
    logical: u32,
    physical: u32,
};

pub const BuildOptions = struct {
    recursive_shared_block_size: ?u32 = null,

    fn layerRef(self: BuildOptions, logical_layer: u32) !LayerRef {
        if (self.recursive_shared_block_size) |shared_block_size| {
            if (shared_block_size == 0) return error.InvalidRecursiveLoRAConfig;
            return .{
                .logical = logical_layer,
                .physical = logical_layer % shared_block_size,
            };
        }
        return .{ .logical = logical_layer, .physical = logical_layer };
    }

    fn parameterLayer(self: BuildOptions, logical_layer: u32) !u32 {
        return (try self.layerRef(logical_layer)).physical;
    }
};

pub fn validateConfig(config: Config) !void {
    if (config.family != .gemma) return error.UnsupportedModelFamily;
    if (config.usesMoe()) return error.UnsupportedGemmaMoeConfig;
    if (config.position_encoding != .rope) return error.UnsupportedPositionEncoding;
}

pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    inputs: GemmaInputs,
) !GemmaGraph {
    return buildForwardGraphWithOptions(bld, config, batch, seq_len, inputs, .{});
}

pub fn buildForwardGraphWithOptions(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    inputs: GemmaInputs,
    options: BuildOptions,
) !GemmaGraph {
    try validateConfig(config);

    const batch_i: i64 = @intCast(batch);
    const seq_i: i64 = @intCast(seq_len);
    const hidden_i: i64 = @intCast(config.hidden_size);
    const total_i: i64 = batch_i * seq_i;

    const input_ids = inputs.input_ids;
    const hidden_3d_shape = Shape.init(.f32, &.{ batch_i, seq_i, hidden_i });
    var hidden = if (inputs.input_embeddings != null_node)
        inputs.input_embeddings
    else blk: {
        const flat_ids = try bld.reshape(input_ids, Shape.init(.f32, &.{total_i}));
        const embed_w = try parameterNamed(
            bld,
            config,
            "model.embed_tokens.weight",
            Shape.init(.f32, &.{ @as(i64, @intCast(config.vocab_size)), hidden_i }),
        );
        const embed_flat = try bld.embeddingLookup(embed_w, flat_ids, @intCast(total_i), config.hidden_size);
        const token_scale = try bld.scalarConst(.f32, config.tokenEmbeddingScale());
        const scaled_embed = if (std.math.approxEqAbs(f32, config.tokenEmbeddingScale(), 1.0, 1e-6))
            embed_flat
        else
            try bld.mul(embed_flat, token_scale);
        break :blk try bld.reshape(scaled_embed, hidden_3d_shape);
    };
    const causal_mask = try buildAttentionMask(bld, seq_len, 0);
    const sliding_mask = if (config.usesGemmaSlidingAttention())
        try buildAttentionMask(bld, seq_len, config.sliding_window)
    else
        causal_mask;

    // Pre-compute PLE vectors [total, ple_total_dim] from initial hidden state.
    const ple_full: ?NodeId = if (config.hasPle())
        try buildPleVectors(bld, config, input_ids, hidden, total_i, hidden_i)
    else
        null;

    var last_sliding_kv: ?KvCache = null;
    var last_full_kv: ?KvCache = null;

    var layer: u32 = 0;
    while (layer < config.num_hidden_layers) : (layer += 1) {
        const layer_ref = try options.layerRef(layer);
        const layer_out = try decoderLayer(
            bld,
            config,
            hidden,
            causal_mask,
            sliding_mask,
            batch,
            seq_len,
            layer_ref,
            options,
            if (config.layerUsesSlidingAttention(layer)) last_sliding_kv else last_full_kv,
            ple_full,
        );
        hidden = layer_out.hidden;
        if (!config.layerSharesKv(layer)) {
            if (config.layerUsesSlidingAttention(layer)) {
                last_sliding_kv = layer_out.produced_kv;
            } else {
                last_full_kv = layer_out.produced_kv;
            }
        }
    }

    const final_norm_w = try adjustedNormWeight(
        bld,
        config,
        "model.norm.weight",
        Shape.init(.f32, &.{hidden_i}),
        config.norm_weight_offset,
    );
    const output = try bld.rmsNorm(hidden, final_norm_w, config.hidden_size, config.norm_eps);

    return .{
        .input_ids_node = input_ids,
        .rope_cos_node = inputs.rope_cos,
        .rope_sin_node = inputs.rope_sin,
        .output_node = output,
    };
}

const KvCache = struct {
    k: NodeId,
    v: NodeId,
};

const LayerOutput = struct {
    hidden: NodeId,
    produced_kv: ?KvCache = null,
};

// --- Per-Layer Embeddings (PLE) ---

/// Build full PLE tensor [total, ple_total_dim] from input_ids and initial hidden state.
/// Matches HF Gemma4TextModel:
///   token_path = embed_per_layer(input_ids) * sqrt(ple_dim)
///   ctx_path = RMSNorm(per_layer_proj(hidden), per_layer_proj_norm)
///   combined = (token_path + ctx_path) * (1/sqrt(2))
fn buildPleVectors(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    hidden_3d: NodeId,
    total_i: i64,
    hidden_i: i64,
) !NodeId {
    const ple_dim_i: i64 = @intCast(config.ple_hidden_size);
    const num_layers_i: i64 = @intCast(config.num_hidden_layers);
    const ple_total_i: i64 = ple_dim_i * num_layers_i;

    // Token path: embed_tokens_per_layer(input_ids) * sqrt(ple_dim)
    const token_embd_w = try parameterNamed(
        bld,
        config,
        "model.embed_tokens_per_layer.weight",
        Shape.init(.f32, &.{ @as(i64, @intCast(config.vocab_size)), ple_total_i }),
    );
    const flat_ids = try bld.reshape(input_ids, Shape.init(.f32, &.{total_i}));
    const token_embd = try bld.embeddingLookup(token_embd_w, flat_ids, @intCast(total_i), @intCast(ple_total_i));
    const embed_scale = try bld.scalarConst(.f32, @sqrt(@as(f32, @floatFromInt(config.ple_hidden_size))));
    const token_scaled = try bld.mul(token_embd, embed_scale);

    // Context path: per_layer_model_proj(hidden_flat) then RMSNorm on ple_dim chunks
    const hidden_flat = try bld.reshape(hidden_3d, Shape.init(.f32, &.{ total_i, hidden_i }));
    const model_proj_w = try parameterNamed(
        bld,
        config,
        "model.per_layer_model_projection.weight",
        Shape.init(.f32, &.{ ple_total_i, hidden_i }),
    );
    const model_proj = try bld.linearNoBias(hidden_flat, model_proj_w, @intCast(total_i), @intCast(hidden_i), @intCast(ple_total_i));

    // RMSNorm: reshape [total, ple_total] -> [total*num_layers, ple_dim] -> norm -> reshape back
    const proj_norm_w = try parameterNamed(
        bld,
        config,
        "model.per_layer_projection_norm.weight",
        Shape.init(.f32, &.{ple_dim_i}),
    );
    const proj_flat = try bld.reshape(model_proj, Shape.init(.f32, &.{ total_i * num_layers_i, ple_dim_i }));
    const proj_normed_flat = try bld.rmsNorm(proj_flat, proj_norm_w, @intCast(config.ple_hidden_size), config.norm_eps);
    const proj_normed = try bld.reshape(proj_normed_flat, Shape.init(.f32, &.{ total_i, ple_total_i }));

    // Combine: (token_scaled + proj_normed) * (1/sqrt(2))
    const combined = try bld.add(token_scaled, proj_normed);
    const combine_scale = try bld.scalarConst(.f32, 1.0 / @sqrt(2.0));
    return bld.mul(combined, combine_scale);
}

/// Apply PLE conditioning to hidden state after FFN residual.
/// HF Gemma4TextDecoderLayer:
///   gate = act_fn(inp_gate(hidden)) # hidden_size -> ple_dim
///   gated = gate * ple_layer # element-wise
///   proj = inp_proj(gated) # ple_dim -> hidden_size
///   normed = post_norm(proj) # RMSNorm
///   result = hidden + normed # residual
fn applyPleInGraph(
    bld: *Builder,
    config: Config,
    hidden: NodeId,
    ple_full: NodeId,
    hidden_i: i64,
    layer_ref: LayerRef,
) !NodeId {
    const ple_dim_i: i64 = @intCast(config.ple_hidden_size);
    const ple_offset: i64 = @as(i64, @intCast(layer_ref.logical)) * ple_dim_i;

    // Slice this layer's PLE vector from [total, ple_total_dim] -> [total, ple_dim]
    const ple_layer = try bld.sliceLastDim(ple_full, ple_offset, ple_offset + ple_dim_i);

    // Flatten hidden for linear ops
    const hidden_shape = bld.graph.node(hidden).output_shape;
    const total_i = hidden_shape.dim(0) * hidden_shape.dim(1);
    const hidden_flat = try bld.reshape(hidden, Shape.init(.f32, &.{ total_i, hidden_i }));

    // Gate: hidden -> ple_dim, then activation
    var name_buf: [256]u8 = undefined;
    const gate_name = std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input_gate.weight", .{layer_ref.physical}) catch return error.NameTooLong;
    const gate_w = try parameterNamed(bld, config, gate_name, Shape.init(.f32, &.{ ple_dim_i, hidden_i }));
    const gate_proj = try bld.linearNoBias(hidden_flat, gate_w, @intCast(total_i), @intCast(hidden_i), @intCast(ple_dim_i));
    // Gemma uses gelu activation for PLE gate
    const gate = try bld.gelu(gate_proj);

    // Gated: gate * ple_layer
    const gated = try bld.mul(gate, ple_layer);

    // Project back: ple_dim -> hidden_size
    var proj_buf: [256]u8 = undefined;
    const proj_name = std.fmt.bufPrint(&proj_buf, "model.layers.{d}.per_layer_projection.weight", .{layer_ref.physical}) catch return error.NameTooLong;
    const proj_w = try parameterNamed(bld, config, proj_name, Shape.init(.f32, &.{ hidden_i, ple_dim_i }));
    const projected = try bld.linearNoBias(gated, proj_w, @intCast(total_i), @intCast(ple_dim_i), @intCast(hidden_i));

    // Post-norm
    var norm_buf: [256]u8 = undefined;
    const norm_name = std.fmt.bufPrint(&norm_buf, "model.layers.{d}.post_per_layer_input_norm.weight", .{layer_ref.physical}) catch return error.NameTooLong;
    const post_norm_w = try parameterNamed(bld, config, norm_name, Shape.init(.f32, &.{hidden_i}));
    const normed = try bld.rmsNorm(projected, post_norm_w, @intCast(hidden_i), config.norm_eps);

    // Residual: reshape normed back to 3D and add to hidden
    const normed_3d = try bld.reshape(normed, hidden_shape);
    return bld.add(hidden, normed_3d);
}

fn decoderLayer(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    causal_mask: NodeId,
    sliding_mask: NodeId,
    batch: u32,
    seq_len: u32,
    layer_ref: LayerRef,
    options: BuildOptions,
    donor_kv: ?KvCache,
    ple_full: ?NodeId,
) !LayerOutput {
    const hidden_i: i64 = @intCast(config.hidden_size);

    const input_ln_w = try adjustedNormWeightFmt(
        bld,
        config,
        "model.layers.{d}.input_layernorm.weight",
        layer_ref.physical,
        Shape.init(.f32, &.{hidden_i}),
        config.norm_weight_offset,
    );
    const attn_normed = try bld.rmsNorm(hidden_in, input_ln_w, config.hidden_size, config.norm_eps);
    const attn = try selfAttention(bld, config, attn_normed, causal_mask, sliding_mask, batch, seq_len, layer_ref, options, donor_kv);
    const attn_out = attn.output;

    const post_attn_norm_w = try adjustedNormWeightFmt(
        bld,
        config,
        "model.layers.{d}.post_attention_layernorm.weight",
        layer_ref.physical,
        Shape.init(.f32, &.{hidden_i}),
        config.norm_weight_offset,
    );
    const attn_post = try bld.rmsNorm(attn_out, post_attn_norm_w, config.hidden_size, config.norm_eps);
    const sa_out = try bld.add(attn_post, hidden_in);

    const ffn_pre_norm_w = try adjustedNormWeightFmt(
        bld,
        config,
        "model.layers.{d}.pre_feedforward_layernorm.weight",
        layer_ref.physical,
        Shape.init(.f32, &.{hidden_i}),
        config.norm_weight_offset,
    );
    const ffn_normed = try bld.rmsNorm(sa_out, ffn_pre_norm_w, config.hidden_size, config.norm_eps);
    const ffn_out = try gatedMlp(bld, config, ffn_normed, batch, seq_len, layer_ref, options);

    const ffn_post_norm_w = try adjustedNormWeightFmt(
        bld,
        config,
        "model.layers.{d}.post_feedforward_layernorm.weight",
        layer_ref.physical,
        Shape.init(.f32, &.{hidden_i}),
        config.norm_weight_offset,
    );
    const ffn_post = try bld.rmsNorm(ffn_out, ffn_post_norm_w, config.hidden_size, config.norm_eps);
    const ffn_residual = try bld.add(ffn_post, sa_out);

    const post_ple = if (ple_full) |ple|
        try applyPleInGraph(bld, config, ffn_residual, ple, hidden_i, layer_ref)
    else
        ffn_residual;

    return .{
        .hidden = post_ple,
        .produced_kv = attn.produced_kv,
    };
}

const AttentionOutput = struct {
    output: NodeId,
    produced_kv: ?KvCache = null,
};

fn selfAttention(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    causal_mask: NodeId,
    sliding_mask: NodeId,
    batch: u32,
    seq_len: u32,
    layer_ref: LayerRef,
    options: BuildOptions,
    donor_kv: ?KvCache,
) !AttentionOutput {
    const batch_i: i64 = @intCast(batch);
    const seq_i: i64 = @intCast(seq_len);
    const hidden_i: i64 = @intCast(config.hidden_size);
    const total_i: i64 = batch_i * seq_i;
    const layer = layer_ref.logical;
    const physical_layer = layer_ref.physical;
    const parameter_layer = try options.parameterLayer(layer);
    const head_dim_u = config.effectiveHeadDimForLayer(parameter_layer);
    const head_dim_i: i64 = @intCast(head_dim_u);
    const num_heads_i: i64 = @intCast(config.num_attention_heads);
    const num_kv_i: i64 = @intCast(config.effectiveKVHeadsForLayer(parameter_layer));
    const q_dim_i: i64 = num_heads_i * head_dim_i;
    const kv_dim_i: i64 = num_kv_i * head_dim_i;
    const uses_shared_kv = config.layerSharesKv(layer);
    if (uses_shared_kv and donor_kv == null) return error.MissingSharedKvDonor;
    const layer_mask = if (config.layerUsesSlidingAttention(layer)) sliding_mask else causal_mask;

    const hidden_flat = try bld.reshape(hidden_in, Shape.init(.f32, &.{ total_i, hidden_i }));

    const q_w = try parameterFmt(
        bld,
        config,
        "model.layers.{d}.self_attn.q_proj.weight",
        physical_layer,
        Shape.init(.f32, &.{ q_dim_i, hidden_i }),
    );
    const q_flat = try bld.linearNoBias(hidden_flat, q_w, @intCast(total_i), config.hidden_size, @intCast(q_dim_i));

    const q_bsn4 = try bld.reshape(q_flat, Shape.init(.f32, &.{ batch_i, seq_i, num_heads_i, head_dim_i }));
    const q_bnsd = try bld.transpose(q_bsn4, &.{ 0, 2, 1, 3 });
    const q_rope = try applyRopeAndHeadNorm(bld, config, q_bnsd, batch_i, num_heads_i, seq_i, head_dim_i, parameter_layer, "q");

    const k_rope: NodeId, const v_attn: NodeId, const produced_kv: ?KvCache = if (uses_shared_kv)
        .{ donor_kv.?.k, donor_kv.?.v, null }
    else blk: {
        const k_w = try parameterFmt(
            bld,
            config,
            "model.layers.{d}.self_attn.k_proj.weight",
            physical_layer,
            Shape.init(.f32, &.{ kv_dim_i, hidden_i }),
        );
        const k_flat = try bld.linearNoBias(hidden_flat, k_w, @intCast(total_i), config.hidden_size, @intCast(kv_dim_i));
        const k_bsn4 = try bld.reshape(k_flat, Shape.init(.f32, &.{ batch_i, seq_i, num_kv_i, head_dim_i }));
        const k_bnsd = try bld.transpose(k_bsn4, &.{ 0, 2, 1, 3 });
        const local_k = try applyRopeAndHeadNorm(bld, config, k_bnsd, batch_i, num_kv_i, seq_i, head_dim_i, parameter_layer, "k");

        const local_v = if (config.layerOmitsVProj(layer))
            k_bnsd
        else blk_v: {
            const v_w = try parameterFmt(
                bld,
                config,
                "model.layers.{d}.self_attn.v_proj.weight",
                physical_layer,
                Shape.init(.f32, &.{ kv_dim_i, hidden_i }),
            );
            const v_flat = try bld.linearNoBias(hidden_flat, v_w, @intCast(total_i), config.hidden_size, @intCast(kv_dim_i));
            const v_bsn4 = try bld.reshape(v_flat, Shape.init(.f32, &.{ batch_i, seq_i, num_kv_i, head_dim_i }));
            break :blk_v try bld.transpose(v_bsn4, &.{ 0, 2, 1, 3 });
        };
        const v_normed = if (config.global_head_dim > 0)
            try bareHeadRmsNorm(bld, local_v, batch_i, num_kv_i, seq_i, head_dim_i, config.norm_eps)
        else
            local_v;
        break :blk .{ local_k, v_normed, KvCache{ .k = local_k, .v = v_normed } };
    };

    const q_per_kv_i: i64 = @divExact(num_heads_i, num_kv_i);
    const k_expanded = try gqaFanOut(bld, k_rope, batch_i, num_kv_i, q_per_kv_i, seq_i, head_dim_i);
    const v_expanded = try gqaFanOut(bld, v_attn, batch_i, num_kv_i, q_per_kv_i, seq_i, head_dim_i);

    const scores = try bld.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, seq_i }),
        .inputs = .{ q_rope, k_expanded, null_node, null_node },
        .num_inputs = 2,
    });
    const inv_sqrt_d = try bld.scalarConst(.f32, if (config.global_head_dim > 0) 1.0 else 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim_u))));
    const scaled_scores = try bld.mul(scores, inv_sqrt_d);
    const masked_scores = try bld.add(scaled_scores, layer_mask);
    const probs = try bld.softmax(masked_scores);

    const attn_bnsd = try bld.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, head_dim_i }),
        .inputs = .{ probs, v_expanded, null_node, null_node },
        .num_inputs = 2,
    });

    const attn_bsnd = try bld.transpose(attn_bnsd, &.{ 0, 2, 1, 3 });
    const attn_flat = try bld.reshape(attn_bsnd, Shape.init(.f32, &.{ total_i, q_dim_i }));
    const o_w = try parameterFmt(
        bld,
        config,
        "model.layers.{d}.self_attn.o_proj.weight",
        physical_layer,
        Shape.init(.f32, &.{ hidden_i, q_dim_i }),
    );
    const o_flat = try bld.linearNoBias(attn_flat, o_w, @intCast(total_i), @intCast(q_dim_i), config.hidden_size);
    return .{
        .output = try bld.reshape(o_flat, Shape.init(.f32, &.{ batch_i, seq_i, hidden_i })),
        .produced_kv = produced_kv,
    };
}

fn gatedMlp(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    batch: u32,
    seq_len: u32,
    layer_ref: LayerRef,
    options: BuildOptions,
) !NodeId {
    const batch_i: i64 = @intCast(batch);
    const seq_i: i64 = @intCast(seq_len);
    const hidden_i: i64 = @intCast(config.hidden_size);
    const layer = layer_ref.logical;
    const physical_layer = layer_ref.physical;
    const parameter_layer = try options.parameterLayer(layer);
    const inter_u = config.intermediateSize(parameter_layer);
    const inter_i: i64 = @intCast(inter_u);
    const total_i: i64 = batch_i * seq_i;

    const hidden_flat = try bld.reshape(hidden_in, Shape.init(.f32, &.{ total_i, hidden_i }));
    const gate_w = try parameterFmt(
        bld,
        config,
        "model.layers.{d}.mlp.gate_proj.weight",
        physical_layer,
        Shape.init(.f32, &.{ inter_i, hidden_i }),
    );
    const gate_linear = try bld.linearNoBias(hidden_flat, gate_w, @intCast(total_i), config.hidden_size, inter_u);
    const gate_act = try applyActivation(bld, config, gate_linear);

    const up_w = try parameterFmt(
        bld,
        config,
        "model.layers.{d}.mlp.up_proj.weight",
        physical_layer,
        Shape.init(.f32, &.{ inter_i, hidden_i }),
    );
    const up_linear = try bld.linearNoBias(hidden_flat, up_w, @intCast(total_i), config.hidden_size, inter_u);
    const gated = try bld.mul(gate_act, up_linear);

    const down_w = try parameterFmt(
        bld,
        config,
        "model.layers.{d}.mlp.down_proj.weight",
        physical_layer,
        Shape.init(.f32, &.{ hidden_i, inter_i }),
    );
    const down_linear = try bld.linearNoBias(gated, down_w, @intCast(total_i), inter_u, config.hidden_size);
    return bld.reshape(down_linear, Shape.init(.f32, &.{ batch_i, seq_i, hidden_i }));
}

fn applyActivation(bld: *Builder, config: Config, input: NodeId) !NodeId {
    return switch (config.activation) {
        .gelu, .gelu_new => bld.gelu(input),
        .silu => bld.silu(input),
        .relu => bld.relu(input),
        else => error.UnsupportedGemmaActivation,
    };
}

fn applyRopeAndHeadNorm(
    bld: *Builder,
    config: Config,
    input: NodeId,
    batch_i: i64,
    num_heads_i: i64,
    seq_i: i64,
    head_dim_i: i64,
    layer: u32,
    proj: []const u8,
) !NodeId {
    var name_buf: [256]u8 = undefined;
    const norm_name = std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.{s}_norm.weight", .{ layer, proj }) catch return error.NameTooLong;
    const norm_w = try adjustedNormWeight(bld, config, norm_name, Shape.init(.f32, &.{head_dim_i}), config.norm_weight_offset);

    const normed_flat = try bld.reshape(input, Shape.init(.f32, &.{ batch_i * num_heads_i * seq_i, head_dim_i }));
    const normed = try bld.rmsNorm(normed_flat, norm_w, @intCast(head_dim_i), config.norm_eps);
    const normed_restored = try bld.reshape(normed, Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, head_dim_i }));

    const active_rope_dim_u = config.layerRopeActiveDim(layer);
    return if (active_rope_dim_u > 0) blk: {
        const rope = try buildLayerRopeTables(bld, config, @intCast(seq_i), layer);
        const merged = try bld.reshape(normed_restored, Shape.init(.f32, &.{ batch_i * num_heads_i * seq_i, head_dim_i }));
        const rotated = try bld.rope(
            merged,
            rope.cos,
            rope.sin,
            @intCast(seq_i),
            @intCast(head_dim_i),
            active_rope_dim_u,
            rope.theta,
        );
        break :blk try bld.reshape(rotated, Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, head_dim_i }));
    } else normed_restored;
}

fn buildLayerRopeTables(
    bld: *Builder,
    config: Config,
    seq_len: u32,
    layer: u32,
) !struct {
    cos: NodeId,
    sin: NodeId,
    theta: f32,
} {
    const rope_active_dim = config.layerRopeActiveDim(layer);
    if (rope_active_dim == 0) return error.InvalidRopeDim;
    const rope_dim = config.layerRopeDim(layer);
    var theta = config.layerRopeTheta(layer);
    if (config.rope_dim_override > 0 and config.usesGemmaSlidingAttention() and !config.layerUsesSlidingAttention(layer) and rope_active_dim < rope_dim) {
        const freq_dim: f32 = @floatFromInt(rope_dim);
        const active_dim: f32 = @floatFromInt(rope_active_dim);
        theta = std.math.pow(f32, theta, active_dim / freq_dim);
    }

    const table = try buildRopeCosSin(bld.graph.allocator, seq_len, rope_active_dim, theta);
    defer bld.graph.allocator.free(table.cos);
    defer bld.graph.allocator.free(table.sin);
    const shape = Shape.init(.f32, &.{ @as(i64, @intCast(seq_len)), @as(i64, @intCast(rope_active_dim / 2)) });
    return .{
        .cos = try bld.tensorConst(table.cos, shape),
        .sin = try bld.tensorConst(table.sin, shape),
        .theta = theta,
    };
}

fn adjustedNormWeight(bld: *Builder, config: Config, name: []const u8, shape: Shape, offset: f32) !NodeId {
    const base = try parameterNamed(bld, config, name, shape);
    if (std.math.approxEqAbs(f32, offset, 0.0, 1e-6)) return base;
    const scalar = try bld.scalarConst(.f32, offset);
    return bld.add(base, scalar);
}

fn adjustedNormWeightFmt(
    bld: *Builder,
    config: Config,
    comptime fmt_str: []const u8,
    layer: u32,
    shape: Shape,
    offset: f32,
) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, fmt_str, .{layer}) catch return error.NameTooLong;
    return adjustedNormWeight(bld, config, name, shape, offset);
}

fn parameterFmt(bld: *Builder, config: Config, comptime fmt_str: []const u8, layer: u32, shape: Shape) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, fmt_str, .{layer}) catch return error.NameTooLong;
    return parameterNamed(bld, config, name, shape);
}

fn parameterNamed(bld: *Builder, config: Config, name: []const u8, shape: Shape) !NodeId {
    var prefixed_buf: [256]u8 = undefined;
    const final_name = try prefixedModelName(&prefixed_buf, config, name);
    return bld.parameter(final_name, shape);
}

fn prefixedModelName(buf: *[256]u8, config: Config, name: []const u8) ![]const u8 {
    if (config.weight_prefix.len == 0 or !std.mem.startsWith(u8, name, "model.")) return name;
    return std.fmt.bufPrint(buf, "{s}.{s}", .{ config.weight_prefix, name["model.".len..] }) catch error.NameTooLong;
}

fn buildAttentionMask(bld: *Builder, seq_len: u32, sliding_window: u32) !NodeId {
    const s: usize = @intCast(seq_len);
    const allocator = bld.graph.allocator;
    var data = try allocator.alloc(f32, s * s);
    defer allocator.free(data);

    const neg_inf: f32 = -1.0e9;
    for (0..s) |i| {
        for (0..s) |j| {
            const in_window = j <= i and (sliding_window == 0 or i - j < sliding_window);
            data[i * s + j] = if (in_window) 0.0 else neg_inf;
        }
    }

    return bld.tensorConst(data, Shape.init(.f32, &.{ 1, 1, @as(i64, @intCast(s)), @as(i64, @intCast(s)) }));
}

fn bareHeadRmsNorm(
    bld: *Builder,
    input: NodeId,
    batch_i: i64,
    num_heads_i: i64,
    seq_i: i64,
    head_dim_i: i64,
    eps: f32,
) !NodeId {
    const allocator = bld.graph.allocator;
    const ones = try allocator.alloc(f32, @intCast(head_dim_i));
    defer allocator.free(ones);
    @memset(ones, 1.0);
    const ones_node = try bld.tensorConst(ones, Shape.init(.f32, &.{head_dim_i}));
    const flat = try bld.reshape(input, Shape.init(.f32, &.{ batch_i * num_heads_i * seq_i, head_dim_i }));
    const normed = try bld.rmsNorm(flat, ones_node, @intCast(head_dim_i), eps);
    return bld.reshape(normed, Shape.init(.f32, &.{ batch_i, num_heads_i, seq_i, head_dim_i }));
}

pub fn buildRopeCosSin(
    allocator: std.mem.Allocator,
    seq_len: u32,
    rope_dim: u32,
    rope_theta: f32,
) !struct { cos: []f32, sin: []f32 } {
    if (rope_dim == 0 or (rope_dim % 2) != 0) return error.InvalidRopeDim;
    const half_dim = rope_dim / 2;
    const total: usize = @as(usize, seq_len) * @as(usize, half_dim);
    const cos = try allocator.alloc(f32, total);
    errdefer allocator.free(cos);
    const sin = try allocator.alloc(f32, total);
    errdefer allocator.free(sin);

    for (0..seq_len) |pos| {
        for (0..half_dim) |i| {
            const inv_freq = 1.0 / std.math.pow(f32, rope_theta, (2.0 * @as(f32, @floatFromInt(i))) / @as(f32, @floatFromInt(rope_dim)));
            const freq = @as(f32, @floatFromInt(pos)) * inv_freq;
            cos[pos * half_dim + i] = @cos(freq);
            sin[pos * half_dim + i] = @sin(freq);
        }
    }
    return .{ .cos = cos, .sin = sin };
}

fn gqaFanOut(
    bld: *Builder,
    input: NodeId,
    batch_i: i64,
    num_kv_i: i64,
    q_per_kv_i: i64,
    seq_i: i64,
    head_dim_i: i64,
) !NodeId {
    if (q_per_kv_i == 1) return input;
    const inserted = try bld.reshape(input, Shape.init(.f32, &.{ batch_i, num_kv_i, 1, seq_i, head_dim_i }));
    const broadcast_shape = Shape.init(.f32, &.{ batch_i, num_kv_i, q_per_kv_i, seq_i, head_dim_i });
    var attrs = node_mod.BroadcastAttrs{ .target_shape = broadcast_shape };
    attrs.num_axes = 5;
    attrs.broadcast_axes[0] = 0;
    attrs.broadcast_axes[1] = 1;
    attrs.broadcast_axes[2] = 2;
    attrs.broadcast_axes[3] = 3;
    attrs.broadcast_axes[4] = 4;
    const broadcasted = try bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = broadcast_shape,
        .inputs = .{ inserted, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    return bld.reshape(broadcasted, Shape.init(.f32, &.{ batch_i, num_kv_i * q_per_kv_i, seq_i, head_dim_i }));
}

test "buildForwardGraph: dense gemma4_text config compiles" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 64,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.f32, &.{ 1, 4 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 4, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 4, 4 }));
    const result = try buildForwardGraph(&bld, cfg, 1, 4, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    try std.testing.expect(result.output_node != null_node);
    const out_shape = graph.node(result.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 16), out_shape.dim(2));
}

test "buildForwardGraphWithOptions: recursive layer mapping reuses physical layer names" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 4,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 64,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.f32, &.{ 1, 4 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 4, 4 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 4, 4 }));
    const result = try buildForwardGraphWithOptions(&bld, cfg, 1, 4, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    }, .{ .recursive_shared_block_size = 2 });

    try std.testing.expect(result.output_node != null_node);
    try std.testing.expect(parameterNameCount(&graph, "model.layers.0.self_attn.q_proj.weight") >= 2);
    try std.testing.expect(parameterNameCount(&graph, "model.layers.1.self_attn.q_proj.weight") >= 2);
    try std.testing.expectEqual(@as(usize, 0), parameterNameCount(&graph, "model.layers.2.self_attn.q_proj.weight"));
    try std.testing.expectEqual(@as(usize, 0), parameterNameCount(&graph, "model.layers.3.self_attn.q_proj.weight"));
}

fn parameterNameCount(graph: *const Graph, name: []const u8) usize {
    var count: usize = 0;
    for (graph.parameters.items) |param_id| {
        const param = graph.node(param_id);
        if (std.mem.eql(u8, graph.parameterName(param), name)) count += 1;
    }
    return count;
}

test "buildForwardGraph: gemma4 sliding and shared-kv config compiles" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);

    const cfg = Config{
        .family = .gemma,
        .hidden_size = 32,
        .num_hidden_layers = 4,
        .num_attention_heads = 4,
        .num_key_value_heads = 1,
        .num_global_key_value_heads = 2,
        .attention_head_dim = 4,
        .global_head_dim = 8,
        .intermediate_size = 64,
        .vocab_size = 64,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
        .sliding_window = 4,
        .sliding_window_pattern = 2,
        .rope_theta = 1_000_000.0,
        .rope_local_theta = 10_000.0,
        .rope_partial_factor = 0.5,
        .rope_dim_override = 2,
        .num_kv_shared_layers = 1,
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.f32, &.{ 1, 4 }));
    const test_cos = try bld.parameter("test_cos", Shape.init(.f32, &.{ 1, 1 }));
    const test_sin = try bld.parameter("test_sin", Shape.init(.f32, &.{ 1, 1 }));
    const result = try buildForwardGraph(&bld, cfg, 1, 4, .{
        .input_ids = test_input_ids,
        .rope_cos = test_cos,
        .rope_sin = test_sin,
    });

    try std.testing.expect(result.output_node != null_node);
    const out_shape = graph.node(result.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(2));
}
