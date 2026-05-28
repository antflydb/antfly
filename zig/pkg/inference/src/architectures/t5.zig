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

// T5 encoder-decoder architecture using abstract ComputeBackend ops.
//
// Single implementation works with any backend (native, MLX, etc).
// T5 differences from BERT:
// - RMSNorm (no bias, no mean subtraction)
// - No bias in linear projections
// - Relative position bias (learned, shared across layers)
// - Encoder: bidirectional self-attention
// - Decoder: causal self-attention + cross-attention
// - Separate encoder and decoder stacks

const std = @import("std");
const ops = @import("../ops/ops.zig");
const linalg = @import("termite_linalg");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const t5_config = @import("../models/t5.zig");

pub const Config = t5_config.Config;

/// Cross-attention cache for T5 decoder. Stores per-layer K/V projections
/// of encoder output, which are constant across decode steps.
pub const T5CrossCache = struct {
    k_cross: []?[]f32,
    v_cross: []?[]f32,
    num_layers: u32,
    alloc: std.mem.Allocator,

    pub fn init(a: std.mem.Allocator, num_layers: u32) !T5CrossCache {
        const k = try a.alloc(?[]f32, num_layers);
        const v = try a.alloc(?[]f32, num_layers);
        @memset(k, null);
        @memset(v, null);
        return .{
            .k_cross = k,
            .v_cross = v,
            .num_layers = num_layers,
            .alloc = a,
        };
    }

    pub fn deinit(self: *T5CrossCache) void {
        for (0..self.num_layers) |i| {
            if (self.k_cross[i]) |k| self.alloc.free(k);
            if (self.v_cross[i]) |v| self.alloc.free(v);
        }
        self.alloc.free(self.k_cross);
        self.alloc.free(self.v_cross);
    }

    pub fn reset(self: *T5CrossCache) void {
        for (0..self.num_layers) |i| {
            if (self.k_cross[i]) |k| {
                self.alloc.free(k);
                self.k_cross[i] = null;
            }
            if (self.v_cross[i]) |v| {
                self.alloc.free(v);
                self.v_cross[i] = null;
            }
        }
    }
};

/// Context for cached T5 decoding.
pub const T5DecodeContext = struct {
    cached_len: usize,
    total_kv_len: usize,
    cross_cache: *T5CrossCache,
};

/// Run the T5 encoder forward pass.
/// Returns an owned f32 slice: [batch * seq_len * d_model].
pub fn encoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const d_model = config.d_model;
    const total = batch * seq_len;

    // 1. Token embeddings (shared.weight or encoder.embed_tokens.weight)
    const embed_w = cb.getWeight("shared.weight") catch try cb.getWeight("encoder.embed_tokens.weight");
    defer cb.free(embed_w);
    var hidden = try cb.embeddingLookup(embed_w, input_ids, total, d_model);

    // 2. Compute relative position bias (shared across all encoder layers, from layer 0)
    const pos_bias = try getEncoderPositionBias(cb, config, seq_len);
    defer cb.free(pos_bias);

    // 3. Encoder blocks
    for (0..config.num_layers) |layer| {
        const new_hidden = try encoderBlock(cb, allocator, config, hidden, attention_mask, pos_bias, batch, seq_len, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 4. Final layer norm
    const final_ln = try cb.getWeight("encoder.final_layer_norm.weight");
    defer cb.free(final_ln);
    const normed = try cb.rmsNorm(hidden, final_ln, d_model, 1e-6);
    cb.free(hidden);

    // 5. Read out to f32
    const result = try cb.toFloat32(normed, allocator);
    cb.free(normed);
    return result;
}

/// Run the T5 decoder forward pass (single step for autoregressive generation).
/// encoder_hidden: [batch * enc_seq * d_model] as CT
/// decoder_input_ids: [batch * dec_seq]
/// Returns logits: [batch * dec_seq * vocab_size] as f32.
pub fn decoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) ![]f32 {
    const d_model = config.d_model;
    const dec_total = batch * dec_seq;
    const num_dec_layers = config.effectiveDecoderLayers();

    // 1. Token embeddings
    const embed_w = cb.getWeight("shared.weight") catch try cb.getWeight("decoder.embed_tokens.weight");
    defer cb.free(embed_w);
    var hidden = try cb.embeddingLookup(embed_w, decoder_input_ids, dec_total, d_model);

    // 2. Compute relative position biases
    const self_attn_bias = try getDecoderSelfAttentionBias(cb, config, dec_seq);
    defer cb.free(self_attn_bias);

    // 3. Decoder blocks
    for (0..num_dec_layers) |layer| {
        const new_hidden = try decoderBlock(cb, allocator, config, hidden, encoder_hidden, encoder_mask, self_attn_bias, batch, dec_seq, enc_seq, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 4. Final layer norm
    const final_ln = try cb.getWeight("decoder.final_layer_norm.weight");
    defer cb.free(final_ln);
    const normed = try cb.rmsNorm(hidden, final_ln, d_model, 1e-6);
    cb.free(hidden);

    // 5. LM head: project to vocab
    const lm_w = cb.getWeight("lm_head.weight") catch blk: {
        // T5 shares embed_tokens weight as lm_head
        break :blk cb.getWeight("shared.weight") catch try cb.getWeight("decoder.embed_tokens.weight");
    };
    defer cb.free(lm_w);
    const logits = try cb.linearNoBias(normed, lm_w, dec_total, d_model, config.vocab_size);
    cb.free(normed);

    const result = try cb.toFloat32(logits, allocator);
    cb.free(logits);
    return result;
}

// --- Encoder block ---

fn encoderBlock(
    cb: *const ComputeBackend,
    _: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    pos_bias: CT,
    batch: usize,
    seq_len: usize,
    layer: usize,
) !CT {
    const d_model = config.d_model;
    const inner_dim = config.innerDim();
    const d_kv = config.d_kv;
    const num_heads = config.num_heads;
    const total = batch * seq_len;

    var name_buf: [256]u8 = undefined;

    // --- Self-attention sublayer ---
    // Pre-norm
    const ln0 = try getBlockWeight(cb, "encoder", layer, "layer.0.layer_norm.weight", &name_buf);
    defer cb.free(ln0);
    const normed = try cb.rmsNorm(hidden, ln0, d_model, 1e-6);
    defer cb.free(normed);

    // Q, K, V projections (no bias in T5)
    const q_w = try getBlockWeight(cb, "encoder", layer, "layer.0.SelfAttention.q.weight", &name_buf);
    defer cb.free(q_w);
    const Q = try cb.linearNoBias(normed, q_w, total, d_model, inner_dim);
    defer cb.free(Q);

    const k_w = try getBlockWeight(cb, "encoder", layer, "layer.0.SelfAttention.k.weight", &name_buf);
    defer cb.free(k_w);
    const K = try cb.linearNoBias(normed, k_w, total, d_model, inner_dim);
    defer cb.free(K);

    const v_w = try getBlockWeight(cb, "encoder", layer, "layer.0.SelfAttention.v.weight", &name_buf);
    defer cb.free(v_w);
    const V = try cb.linearNoBias(normed, v_w, total, d_model, inner_dim);
    defer cb.free(V);

    // Bidirectional self-attention with relative position bias
    const attn_out = try cb.scaledDotProductAttention(Q, K, V, attention_mask, pos_bias, batch, seq_len, num_heads, d_kv);
    defer cb.free(attn_out);

    // Output projection
    const o_w = try getBlockWeight(cb, "encoder", layer, "layer.0.SelfAttention.o.weight", &name_buf);
    defer cb.free(o_w);
    const projected = try cb.linearNoBias(attn_out, o_w, total, inner_dim, d_model);
    defer cb.free(projected);

    // Residual
    const attn_res = try cb.add(projected, hidden);

    // --- FFN sublayer ---
    const ln1 = try getBlockWeight(cb, "encoder", layer, "layer.1.layer_norm.weight", &name_buf);
    defer cb.free(ln1);
    const ffn_normed = try cb.rmsNorm(attn_res, ln1, d_model, 1e-6);
    defer cb.free(ffn_normed);

    const ffn_out = try feedForward(cb, config, ffn_normed, "encoder", layer, total, &name_buf);
    defer cb.free(ffn_out);

    // Residual
    const result = try cb.add(ffn_out, attn_res);
    cb.free(attn_res);

    return result;
}

// --- Decoder block ---

fn decoderBlock(
    cb: *const ComputeBackend,
    _: std.mem.Allocator,
    config: Config,
    hidden: CT,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    self_attn_bias: CT,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
) !CT {
    const d_model = config.d_model;
    const inner_dim = config.innerDim();
    const d_kv = config.d_kv;
    const num_heads = config.num_heads;
    const dec_total = batch * dec_seq;

    var name_buf: [256]u8 = undefined;

    // --- Causal self-attention sublayer ---
    const ln0 = try getBlockWeight(cb, "decoder", layer, "layer.0.layer_norm.weight", &name_buf);
    defer cb.free(ln0);
    const normed = try cb.rmsNorm(hidden, ln0, d_model, 1e-6);
    defer cb.free(normed);

    const q_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.q.weight", &name_buf);
    defer cb.free(q_w);
    const Q_self = try cb.linearNoBias(normed, q_w, dec_total, d_model, inner_dim);
    defer cb.free(Q_self);

    const k_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.k.weight", &name_buf);
    defer cb.free(k_w);
    const K_self = try cb.linearNoBias(normed, k_w, dec_total, d_model, inner_dim);
    defer cb.free(K_self);

    const v_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.v.weight", &name_buf);
    defer cb.free(v_w);
    const V_self = try cb.linearNoBias(normed, v_w, dec_total, d_model, inner_dim);
    defer cb.free(V_self);

    const self_attn = try cb.causalSelfAttention(Q_self, K_self, V_self, self_attn_bias, batch, dec_seq, num_heads, d_kv);
    defer cb.free(self_attn);

    const o_self_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.o.weight", &name_buf);
    defer cb.free(o_self_w);
    const self_proj = try cb.linearNoBias(self_attn, o_self_w, dec_total, inner_dim, d_model);
    defer cb.free(self_proj);

    const self_res = try cb.add(self_proj, hidden);

    // --- Cross-attention sublayer ---
    const ln1 = try getBlockWeight(cb, "decoder", layer, "layer.1.layer_norm.weight", &name_buf);
    defer cb.free(ln1);
    const cross_normed = try cb.rmsNorm(self_res, ln1, d_model, 1e-6);
    defer cb.free(cross_normed);

    const enc_total = batch * enc_seq;

    const q_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.q.weight", &name_buf);
    defer cb.free(q_cross_w);
    const Q_cross = try cb.linearNoBias(cross_normed, q_cross_w, dec_total, d_model, inner_dim);
    defer cb.free(Q_cross);

    const k_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.k.weight", &name_buf);
    defer cb.free(k_cross_w);
    const K_cross = try cb.linearNoBias(encoder_hidden, k_cross_w, enc_total, d_model, inner_dim);
    defer cb.free(K_cross);

    const v_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.v.weight", &name_buf);
    defer cb.free(v_cross_w);
    const V_cross = try cb.linearNoBias(encoder_hidden, v_cross_w, enc_total, d_model, inner_dim);
    defer cb.free(V_cross);

    const cross_attn = try cb.crossAttention(Q_cross, K_cross, V_cross, encoder_mask, batch, dec_seq, enc_seq, num_heads, d_kv);
    defer cb.free(cross_attn);

    const o_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.o.weight", &name_buf);
    defer cb.free(o_cross_w);
    const cross_proj = try cb.linearNoBias(cross_attn, o_cross_w, dec_total, inner_dim, d_model);
    defer cb.free(cross_proj);

    const cross_res = try cb.add(cross_proj, self_res);
    cb.free(self_res);

    // --- FFN sublayer ---
    const ln2 = try getBlockWeight(cb, "decoder", layer, "layer.2.layer_norm.weight", &name_buf);
    defer cb.free(ln2);
    const ffn_normed = try cb.rmsNorm(cross_res, ln2, d_model, 1e-6);
    defer cb.free(ffn_normed);

    const ffn_out = try feedForward(cb, config, ffn_normed, "decoder", layer, dec_total, &name_buf);
    defer cb.free(ffn_out);

    const result = try cb.add(ffn_out, cross_res);
    cb.free(cross_res);

    return result;
}

// --- Feed-forward network ---

fn feedForward(
    cb: *const ComputeBackend,
    config: Config,
    input: CT,
    stack: []const u8,
    layer: usize,
    total: usize,
    name_buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const d_ff = config.d_ff;

    // FFN sublayer index: 1 for encoder, 2 for decoder (decoder has cross-attn at index 1)
    const ffn_idx: usize = if (std.mem.eql(u8, stack, "decoder")) 2 else 1;

    if (config.is_gated_act) {
        // T5v1.1 gated FFN: wi_0 (gate) and wi_1 (up), then element-wise multiply
        const wi0_name = std.fmt.bufPrint(name_buf, "{s}.block.{d}.layer.{d}.DenseReluDense.wi_0.weight", .{ stack, layer, ffn_idx }) catch return error.NameTooLong;
        const wi0_w = try cb.getWeight(wi0_name);
        defer cb.free(wi0_w);
        const gate = try cb.linearNoBias(input, wi0_w, total, d_model, d_ff);
        defer cb.free(gate);
        const gate_act = try cb.silu(gate);
        defer cb.free(gate_act);

        const wi1_name = std.fmt.bufPrint(name_buf, "{s}.block.{d}.layer.{d}.DenseReluDense.wi_1.weight", .{ stack, layer, ffn_idx }) catch return error.NameTooLong;
        const wi1_w = try cb.getWeight(wi1_name);
        defer cb.free(wi1_w);
        const up = try cb.linearNoBias(input, wi1_w, total, d_model, d_ff);
        defer cb.free(up);

        // T5v1.1 gated FFN: hidden = gate_act * up, then wo projects down.
        const gated = try cb.multiply(gate_act, up);
        defer cb.free(gated);

        const wo_name = std.fmt.bufPrint(name_buf, "{s}.block.{d}.layer.{d}.DenseReluDense.wo.weight", .{ stack, layer, ffn_idx }) catch return error.NameTooLong;
        const wo_w = try cb.getWeight(wo_name);
        defer cb.free(wo_w);
        return cb.linearNoBias(gated, wo_w, total, d_ff, d_model);
    }

    // T5v1.0: wi → ReLU → wo
    const wi_name = std.fmt.bufPrint(name_buf, "{s}.block.{d}.layer.{d}.DenseReluDense.wi.weight", .{ stack, layer, ffn_idx }) catch return error.NameTooLong;
    const wi_w = try cb.getWeight(wi_name);
    defer cb.free(wi_w);
    const intermediate = try cb.linearNoBias(input, wi_w, total, d_model, d_ff);
    defer cb.free(intermediate);

    const activated = try cb.relu(intermediate);
    defer cb.free(activated);

    const wo_name = std.fmt.bufPrint(name_buf, "{s}.block.{d}.layer.{d}.DenseReluDense.wo.weight", .{ stack, layer, ffn_idx }) catch return error.NameTooLong;
    const wo_w = try cb.getWeight(wo_name);
    defer cb.free(wo_w);
    return cb.linearNoBias(activated, wo_w, total, d_ff, d_model);
}

// --- Position bias helpers ---

fn getEncoderPositionBias(cb: *const ComputeBackend, config: Config, seq_len: usize) !CT {
    const bias_w = try cb.getWeight("encoder.block.0.layer.0.SelfAttention.relative_attention_bias.weight");
    defer cb.free(bias_w);
    return cb.relativePositionBias(
        bias_w,
        seq_len,
        seq_len,
        config.num_heads,
        config.relative_attention_num_buckets,
        config.relative_attention_max_distance,
        true, // encoder is bidirectional
    );
}

fn getDecoderSelfAttentionBias(cb: *const ComputeBackend, config: Config, dec_seq: usize) !CT {
    const bias_w = try cb.getWeight("decoder.block.0.layer.0.SelfAttention.relative_attention_bias.weight");
    defer cb.free(bias_w);
    return cb.relativePositionBias(
        bias_w,
        dec_seq,
        dec_seq,
        config.num_heads,
        config.relative_attention_num_buckets,
        config.relative_attention_max_distance,
        false, // decoder is unidirectional
    );
}

// --- Cached decoder (KV cache for autoregressive generation) ---

/// Run the T5 decoder forward pass with KV caching.
/// Self-attention uses gqaPagedAttention (active_kv_cache must be set on the compute backend).
/// Cross-attention K/V are cached in dc.cross_cache on first call and reused subsequently.
pub fn decoderForwardCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    dc: T5DecodeContext,
) ![]f32 {
    const d_model = config.d_model;
    const dec_total = batch * dec_seq;
    const num_dec_layers = config.effectiveDecoderLayers();

    // 1. Token embeddings
    const embed_w = cb.getWeight("shared.weight") catch try cb.getWeight("decoder.embed_tokens.weight");
    defer cb.free(embed_w);
    var hidden = try cb.embeddingLookup(embed_w, decoder_input_ids, dec_total, d_model);

    // 2. Self-attention position bias with query offset for cached decode
    const self_attn_bias = try computeDecoderBiasWithOffset(cb, allocator, config, dec_seq, dc.total_kv_len, dc.cached_len);
    defer cb.free(self_attn_bias);

    // 3. Decoder blocks
    for (0..num_dec_layers) |layer| {
        const new_hidden = try decoderBlockCached(cb, allocator, config, hidden, encoder_hidden, encoder_mask, self_attn_bias, batch, dec_seq, enc_seq, layer, dc);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 4. Final layer norm
    const final_ln = try cb.getWeight("decoder.final_layer_norm.weight");
    defer cb.free(final_ln);
    const normed = try cb.rmsNorm(hidden, final_ln, d_model, 1e-6);
    cb.free(hidden);

    // 5. LM head
    const lm_w = cb.getWeight("lm_head.weight") catch blk: {
        break :blk cb.getWeight("shared.weight") catch try cb.getWeight("decoder.embed_tokens.weight");
    };
    defer cb.free(lm_w);
    const logits = try cb.linearNoBias(normed, lm_w, dec_total, d_model, config.vocab_size);
    cb.free(normed);

    const result = try cb.toFloat32(logits, allocator);
    cb.free(logits);
    return result;
}

fn decoderBlockCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    self_attn_bias: CT,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    dc: T5DecodeContext,
) !CT {
    const d_model = config.d_model;
    const inner_dim = config.innerDim();
    const d_kv = config.d_kv;
    const num_heads = config.num_heads;
    const dec_total = batch * dec_seq;

    var name_buf: [256]u8 = undefined;

    // --- Self-attention with KV cache (via gqaPagedAttention) ---
    const ln0 = try getBlockWeight(cb, "decoder", layer, "layer.0.layer_norm.weight", &name_buf);
    defer cb.free(ln0);
    const normed = try cb.rmsNorm(hidden, ln0, d_model, 1e-6);
    defer cb.free(normed);

    const q_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.q.weight", &name_buf);
    defer cb.free(q_w);
    const Q_self = try cb.linearNoBias(normed, q_w, dec_total, d_model, inner_dim);
    defer cb.free(Q_self);

    const k_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.k.weight", &name_buf);
    defer cb.free(k_w);
    const K_self = try cb.linearNoBias(normed, k_w, dec_total, d_model, inner_dim);
    defer cb.free(K_self);

    const v_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.v.weight", &name_buf);
    defer cb.free(v_w);
    const V_self = try cb.linearNoBias(normed, v_w, dec_total, d_model, inner_dim);
    defer cb.free(V_self);

    // MHA = GQA with num_kv_heads == num_heads
    const attn_ctx = ops.AttentionContext{
        .mode = if (dc.cached_len == 0) .paged_prefill else .paged_decode,
        .total_sequence_len = dc.total_kv_len,
        .query_sequence_len = dec_seq,
        .kv_sequence_len = dc.total_kv_len,
        .layer_index = layer,
    };
    const self_attn = try cb.gqaPagedAttention(
        Q_self,
        K_self,
        V_self,
        self_attn_bias,
        attn_ctx,
        batch,
        num_heads,
        num_heads,
        d_kv,
    );
    defer cb.free(self_attn);

    const o_self_w = try getBlockWeight(cb, "decoder", layer, "layer.0.SelfAttention.o.weight", &name_buf);
    defer cb.free(o_self_w);
    const self_proj = try cb.linearNoBias(self_attn, o_self_w, dec_total, inner_dim, d_model);
    defer cb.free(self_proj);

    const self_res = try cb.add(self_proj, hidden);

    // --- Cross-attention (with cached K/V) ---
    const ln1 = try getBlockWeight(cb, "decoder", layer, "layer.1.layer_norm.weight", &name_buf);
    defer cb.free(ln1);
    const cross_normed = try cb.rmsNorm(self_res, ln1, d_model, 1e-6);
    defer cb.free(cross_normed);

    const enc_total = batch * enc_seq;

    const q_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.q.weight", &name_buf);
    defer cb.free(q_cross_w);
    const Q_cross = try cb.linearNoBias(cross_normed, q_cross_w, dec_total, d_model, inner_dim);
    defer cb.free(Q_cross);

    // K/V: compute and cache on first call, reuse on subsequent calls
    const cross_cache = dc.cross_cache;
    if (cross_cache.k_cross[layer] == null) {
        const k_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.k.weight", &name_buf);
        defer cb.free(k_cross_w);
        const K_tmp = try cb.linearNoBias(encoder_hidden, k_cross_w, enc_total, d_model, inner_dim);
        defer cb.free(K_tmp);

        const v_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.v.weight", &name_buf);
        defer cb.free(v_cross_w);
        const V_tmp = try cb.linearNoBias(encoder_hidden, v_cross_w, enc_total, d_model, inner_dim);
        defer cb.free(V_tmp);

        cross_cache.k_cross[layer] = try cb.toFloat32(K_tmp, allocator);
        cross_cache.v_cross[layer] = try cb.toFloat32(V_tmp, allocator);
    }

    // Wrap cached f32 data as CT (fromFloat32Shape copies, so cache is safe)
    const shape = [_]i32{ @intCast(enc_total), @intCast(inner_dim) };
    const K_cross = try cb.fromFloat32Shape(cross_cache.k_cross[layer].?, &shape);
    defer cb.free(K_cross);
    const V_cross = try cb.fromFloat32Shape(cross_cache.v_cross[layer].?, &shape);
    defer cb.free(V_cross);

    const cross_attn = try cb.crossAttention(Q_cross, K_cross, V_cross, encoder_mask, batch, dec_seq, enc_seq, num_heads, d_kv);
    defer cb.free(cross_attn);

    const o_cross_w = try getBlockWeight(cb, "decoder", layer, "layer.1.EncDecAttention.o.weight", &name_buf);
    defer cb.free(o_cross_w);
    const cross_proj = try cb.linearNoBias(cross_attn, o_cross_w, dec_total, inner_dim, d_model);
    defer cb.free(cross_proj);

    const cross_res = try cb.add(cross_proj, self_res);
    cb.free(self_res);

    // --- FFN sublayer ---
    const ln2 = try getBlockWeight(cb, "decoder", layer, "layer.2.layer_norm.weight", &name_buf);
    defer cb.free(ln2);
    const ffn_normed = try cb.rmsNorm(cross_res, ln2, d_model, 1e-6);
    defer cb.free(ffn_normed);

    const ffn_out = try feedForward(cb, config, ffn_normed, "decoder", layer, dec_total, &name_buf);
    defer cb.free(ffn_out);

    const result = try cb.add(ffn_out, cross_res);
    cb.free(cross_res);

    return result;
}

// --- Position bias with offset ---

/// Compute decoder self-attention position bias with query offset.
/// During prefill (q_offset=0), equivalent to getDecoderSelfAttentionBias.
/// During decode (q_offset=cached_len), shifts query positions correctly.
/// Returns [num_heads, q_len, k_len] as a CT tensor.
fn computeDecoderBiasWithOffset(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    q_len: usize,
    k_len: usize,
    q_offset: usize,
) !CT {
    const num_heads = config.num_heads;
    const num_buckets = config.relative_attention_num_buckets;
    const max_distance = config.relative_attention_max_distance;

    const bias_w = try cb.getWeight("decoder.block.0.layer.0.SelfAttention.relative_attention_bias.weight");
    defer cb.free(bias_w);
    const table = try cb.toFloat32(bias_w, allocator);
    defer allocator.free(table);

    const output = try allocator.alloc(f32, num_heads * q_len * k_len);

    for (0..q_len) |qi| {
        for (0..k_len) |ki| {
            // Absolute positions: query at (qi + q_offset), key at ki
            const rel_pos = @as(i64, @intCast(ki)) - @as(i64, @intCast(qi + q_offset));
            const bucket = linalg.t5RelativePositionBucket(rel_pos, num_buckets, max_distance, false);
            for (0..num_heads) |h| {
                output[h * q_len * k_len + qi * k_len + ki] = table[h * num_buckets + bucket];
            }
        }
    }

    const shape = [_]i32{ @intCast(num_heads), @intCast(q_len * k_len) };
    const ct = try cb.fromFloat32Shape(output, &shape);
    allocator.free(output);
    return ct;
}

// --- Weight name helpers ---

fn getBlockWeight(cb: *const ComputeBackend, stack: []const u8, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "{s}.block.{d}.{s}", .{ stack, layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}
