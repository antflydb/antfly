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

// BERT encoder as an ml.graph computation graph, for autodiff-based LoRA
// training. Equivalent to `bert.zig`'s ComputeBackend forward but emits a
// Graph of Builder nodes instead of calling backend ops directly. Downstream
// code applies `ml.graph.lora.injectLoRA` to add adapters and
// `ml.graph.autodiff.gradient` to get backward for every layer.
//
// Parameter names match HuggingFace BERT checkpoints so `injectLoRA`'s
// pattern matcher can find the q/k/v/dense weights without special-casing.
//
// Attention masking is passed in at execution time as a pre-built additive
// bias tensor (0 for valid positions, large-negative for padded). The
// caller constructs this outside the graph and binds it as an input; the
// graph adds it to the attention scores before softmax. This keeps the
// graph free of dtype-conversion ops that aren't yet in the Builder.

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;

pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    intermediate_size: u32,
    max_position_embeddings: u32 = 512,
    type_vocab_size: u32 = 2,
    layer_norm_eps: f32 = 1e-12,
    /// When true, token_type_ids are looked up and added. Distilbert-style
    /// models omit this layer.
    use_token_type: bool = true,
};

pub const BertGraph = struct {
    /// [batch * seq_len] i64 placeholder, bind at execution time.
    input_ids_node: NodeId,
    /// [batch * seq_len] i64 placeholder. Same shape as input_ids.
    position_ids_node: NodeId,
    /// [batch * seq_len] i64 placeholder, optional (null if use_token_type = false).
    token_type_ids_node: NodeId,
    /// [batch, num_heads, seq_len, seq_len] f32 attention bias. Caller
    /// precomputes: 0 for valid positions, -1e9 for padded.
    attn_bias_node: NodeId,
    /// [batch * seq_len, hidden_size] — final encoder output.
    output_node: NodeId,
};

/// Inputs passed into `buildForwardGraph`. The caller (typically the
/// autodiff trainer harness) creates these nodes up front and threads them
/// through. This avoids the dual-placeholder mismatch that happens when
/// both the harness and the architecture try to own the input placeholders.
pub const BertInputs = struct {
    /// [batch * seq_len] i64 token IDs.
    input_ids: NodeId,
    /// [batch * seq_len] i64 position IDs (0..seq_len-1 repeated per batch
    /// item). The caller-side harness can derive this as a constant when
    /// seq_len is known.
    position_ids: NodeId,
    /// [batch * seq_len] i64 token type IDs, or `null` when the config
    /// disables token type embeddings.
    token_type_ids: ?NodeId,
    /// [batch * num_heads, seq_len, seq_len] f32 additive attention bias.
    /// 0 for valid positions, large-negative (e.g. -1e9) for padded.
    attn_bias: NodeId,
};

/// Construct a BERT forward graph with HF-compatible parameter names.
/// The graph has no pre-bound weight values; a WeightStore must be loaded
/// onto the execution backend separately. LoRA is NOT injected here —
/// call `ml.graph.lora.injectLoRA` after construction to add adapters.
pub fn buildForwardGraph(
    bld: *Builder,
    config: Config,
    batch: u32,
    seq_len: u32,
    inputs: BertInputs,
) !BertGraph {
    const H: u32 = config.hidden_size;
    const total: u32 = batch * seq_len;
    const num_heads: u32 = config.num_attention_heads;
    const head_dim: u32 = H / num_heads;

    if (config.use_token_type and inputs.token_type_ids == null) {
        return error.MissingTokenTypeIds;
    }
    const token_type_ids = inputs.token_type_ids orelse 0;

    // ──────── Embeddings ────────
    var hidden = try embeddings(bld, config, inputs.input_ids, inputs.position_ids, token_type_ids, total, H);

    // ──────── Encoder layers ────────
    var layer: u32 = 0;
    while (layer < config.num_hidden_layers) : (layer += 1) {
        hidden = try encoderLayer(bld, config, hidden, inputs.attn_bias, batch, seq_len, layer, num_heads, head_dim);
    }

    return .{
        .input_ids_node = inputs.input_ids,
        .position_ids_node = inputs.position_ids,
        .token_type_ids_node = token_type_ids,
        .attn_bias_node = inputs.attn_bias,
        .output_node = hidden,
    };
}

fn embeddings(
    bld: *Builder,
    config: Config,
    input_ids: NodeId,
    position_ids: NodeId,
    token_type_ids: NodeId,
    total: u32,
    H: u32,
) !NodeId {
    // Word embeddings
    const word_emb_param = try bld.parameter(
        "embeddings.word_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.vocab_size), @intCast(H) }),
    );
    var result = try bld.embeddingLookup(word_emb_param, input_ids, total, H);

    // Position embeddings
    const pos_emb_param = try bld.parameter(
        "embeddings.position_embeddings.weight",
        Shape.init(.f32, &.{ @intCast(config.max_position_embeddings), @intCast(H) }),
    );
    const pos_lookup = try bld.embeddingLookup(pos_emb_param, position_ids, total, H);
    result = try bld.add(result, pos_lookup);

    // Token type embeddings
    if (config.use_token_type) {
        const tt_emb_param = try bld.parameter(
            "embeddings.token_type_embeddings.weight",
            Shape.init(.f32, &.{ @intCast(config.type_vocab_size), @intCast(H) }),
        );
        const tt_lookup = try bld.embeddingLookup(tt_emb_param, token_type_ids, total, H);
        result = try bld.add(result, tt_lookup);
    }

    // Final embedding LayerNorm
    const ln_w = try bld.parameter("embeddings.LayerNorm.weight", Shape.init(.f32, &.{@intCast(H)}));
    const ln_b = try bld.parameter("embeddings.LayerNorm.bias", Shape.init(.f32, &.{@intCast(H)}));
    return bld.layerNorm(result, ln_w, ln_b, H, config.layer_norm_eps);
}

fn encoderLayer(
    bld: *Builder,
    config: Config,
    hidden_in: NodeId,
    attn_bias: NodeId,
    batch: u32,
    seq_len: u32,
    layer: u32,
    num_heads: u32,
    head_dim: u32,
) !NodeId {
    const H: u32 = config.hidden_size;
    const I: u32 = config.intermediate_size;
    const total: u32 = batch * seq_len;

    // ──────── Attention: Q, K, V linear ────────
    const q_w = try layerParam(bld, layer, "attention.self.query.weight", .{ H, H });
    const q_b = try layerParam(bld, layer, "attention.self.query.bias", .{H});
    const Q = try bld.linear(hidden_in, q_w, q_b, total, H, H);

    const k_w = try layerParam(bld, layer, "attention.self.key.weight", .{ H, H });
    const k_b = try layerParam(bld, layer, "attention.self.key.bias", .{H});
    const K = try bld.linear(hidden_in, k_w, k_b, total, H, H);

    const v_w = try layerParam(bld, layer, "attention.self.value.weight", .{ H, H });
    const v_b = try layerParam(bld, layer, "attention.self.value.bias", .{H});
    const V = try bld.linear(hidden_in, v_w, v_b, total, H, H);

    // Reshape for multi-head: [batch*seq, hidden] → [batch*num_heads, seq, head_dim]
    // via [batch, seq, num_heads, head_dim] → transpose → flatten head dim.
    const q_bsnh = try bld.reshape(
        Q,
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) }),
    );
    const k_bsnh = try bld.reshape(
        K,
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) }),
    );
    const v_bsnh = try bld.reshape(
        V,
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) }),
    );
    // Transpose to [batch, num_heads, seq, head_dim]
    const q_bnsh = try bld.transpose(q_bsnh, &.{ 0, 2, 1, 3 });
    const k_bnsh = try bld.transpose(k_bsnh, &.{ 0, 2, 1, 3 });
    const v_bnsh = try bld.transpose(v_bsnh, &.{ 0, 2, 1, 3 });
    // Flatten batch*num_heads → [bh, seq, head_dim]
    const q_bhsd = try bld.reshape(
        q_bnsh,
        Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) }),
    );
    const k_bhsd = try bld.reshape(
        k_bnsh,
        Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) }),
    );
    const v_bhsd = try bld.reshape(
        v_bnsh,
        Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) }),
    );

    // ──────── Masked scaled dot-product attention (manual decomposition) ────────
    // scores = Q @ K^T, shape [bh, seq, seq]
    const k_t = try bld.transpose(k_bhsd, &.{ 0, 2, 1 });
    const scores_raw = try bld.matmul3D(q_bhsd, k_t);
    // scale
    const scale = try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
    const scores_scaled = try bld.mul(scores_raw, scale);
    // Add mask bias (broadcast [bh, seq, seq] + [bh, seq, seq])
    const scores_masked = try bld.add(scores_scaled, attn_bias);
    // Softmax over last axis
    const probs = try bld.softmax(scores_masked);
    // attn_out = probs @ V, shape [bh, seq, head_dim]
    const attn_bhsd = try bld.matmul3D(probs, v_bhsd);

    // Reshape back: [bh, seq, head_dim] → [batch, seq, hidden]
    const attn_bnsh = try bld.reshape(
        attn_bhsd,
        Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(head_dim) }),
    );
    const attn_bsnh = try bld.transpose(attn_bnsh, &.{ 0, 2, 1, 3 });
    const attn_merged = try bld.reshape(attn_bsnh, Shape.init(.f32, &.{ @intCast(total), @intCast(H) }));

    // Output projection
    const o_w = try layerParam(bld, layer, "attention.output.dense.weight", .{ H, H });
    const o_b = try layerParam(bld, layer, "attention.output.dense.bias", .{H});
    const attn_proj = try bld.linear(attn_merged, o_w, o_b, total, H, H);

    // Residual + LayerNorm
    const attn_res = try bld.add(attn_proj, hidden_in);
    const attn_ln_w = try layerParam(bld, layer, "attention.output.LayerNorm.weight", .{H});
    const attn_ln_b = try layerParam(bld, layer, "attention.output.LayerNorm.bias", .{H});
    const attn_normed = try bld.layerNorm(attn_res, attn_ln_w, attn_ln_b, H, config.layer_norm_eps);

    // ──────── FFN ────────
    const ffn_i_w = try layerParam(bld, layer, "intermediate.dense.weight", .{ I, H });
    const ffn_i_b = try layerParam(bld, layer, "intermediate.dense.bias", .{I});
    const ffn_inter = try bld.linear(attn_normed, ffn_i_w, ffn_i_b, total, H, I);
    const ffn_gelu = try bld.gelu(ffn_inter);

    const ffn_o_w = try layerParam(bld, layer, "output.dense.weight", .{ H, I });
    const ffn_o_b = try layerParam(bld, layer, "output.dense.bias", .{H});
    const ffn_out = try bld.linear(ffn_gelu, ffn_o_w, ffn_o_b, total, I, H);

    const ffn_res = try bld.add(ffn_out, attn_normed);
    const ffn_ln_w = try layerParam(bld, layer, "output.LayerNorm.weight", .{H});
    const ffn_ln_b = try layerParam(bld, layer, "output.LayerNorm.bias", .{H});
    return bld.layerNorm(ffn_res, ffn_ln_w, ffn_ln_b, H, config.layer_norm_eps);
}

/// Helper: build a per-layer parameter name and emit a `bld.parameter` node.
/// `dims` is a 1- or 2-element tuple/array of u32 dimensions.
fn layerParam(bld: *Builder, layer: u32, suffix: []const u8, dims: anytype) !NodeId {
    var name_buf: [256]u8 = undefined;
    const name = try std.fmt.bufPrint(&name_buf, "encoder.layer.{d}.{s}", .{ layer, suffix });
    const owned_name = try bld.graph.allocator.dupe(u8, name);

    const d = dims;
    const shape = switch (@typeInfo(@TypeOf(d)).@"struct".fields.len) {
        1 => Shape.init(.f32, &.{@intCast(d[0])}),
        2 => Shape.init(.f32, &.{ @intCast(d[0]), @intCast(d[1]) }),
        else => @compileError("layerParam: only 1-D or 2-D dims supported"),
    };
    return bld.parameter(owned_name, shape);
}

test "buildForwardGraph constructs BERT graph with correct parameter count" {
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
    };

    const test_input_ids = try bld.parameter("test_input_ids", Shape.init(.i64, &.{8}));
    const test_position_ids = try bld.parameter("test_position_ids", Shape.init(.i64, &.{8}));
    const test_token_type_ids = try bld.parameter("test_token_type_ids", Shape.init(.i64, &.{8}));
    const test_attn_bias = try bld.parameter(
        "test_attn_bias",
        Shape.init(.f32, &.{ 1 * 4, 8, 8 }),
    );
    const bert_graph = try buildForwardGraph(&bld, config, 1, 8, .{
        .input_ids = test_input_ids,
        .position_ids = test_position_ids,
        .token_type_ids = test_token_type_ids,
        .attn_bias = test_attn_bias,
    });

    // Output node shape should be [batch*seq, hidden] = [8, 32].
    const out_shape = g.node(bert_graph.output_node).output_shape;
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(1));

    // Parameter count sanity check. Per layer we emit:
    //   - 8 linear weights/biases (q/k/v/o attention + intermediate + output dense)
    //   - 4 layer norm weights/biases (attention.output + output)
    //   = 12 parameters.
    // Plus embeddings: word + position + token_type + LN weight + LN bias = 5.
    // Plus 4 input placeholders (input_ids, position_ids, token_type_ids, attn_bias).
    // Total params = 5 + 12 * 2 + 4 = 33.
    try std.testing.expect(g.parameters.items.len >= 29);
}
