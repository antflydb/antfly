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

// Whisper encoder-decoder architecture using abstract ComputeBackend ops.
//
// Whisper is a speech-to-text model with:
// - Conv1d frontend for mel spectrogram processing
// - Sinusoidal position embeddings in encoder
// - Learned position embeddings in decoder
// - Standard transformer encoder/decoder with cross-attention
// - Pre-norm LayerNorm (applied before attention/FFN)

const std = @import("std");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const whisper_config = @import("../models/whisper.zig");

pub const Config = whisper_config.Config;

/// Run the Whisper encoder forward pass on mel spectrogram features.
/// mel_features: [batch * num_mel_bins * time_steps] as CT (channels-first).
/// Returns encoder hidden states as f32: [batch * enc_seq * d_model].
pub fn encoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    mel_features: CT,
    batch: usize,
    time_steps: usize,
) ![]f32 {
    const d_model = config.d_model;

    // 1. Conv1d frontend: mel [batch, num_mel_bins, time] → [batch, d_model, time]
    const conv1_w = try cb.getWeight("model.encoder.conv1.weight");
    defer cb.free(conv1_w);
    const conv1_b = try cb.getWeight("model.encoder.conv1.bias");
    defer cb.free(conv1_b);
    const conv1_out = try cb.conv1d(mel_features, conv1_w, conv1_b, batch, config.num_mel_bins, d_model, time_steps, 3, 1, 1);
    defer cb.free(conv1_out);
    const conv1_act = try cb.gelu(conv1_out);
    defer cb.free(conv1_act);

    // Second conv: stride=2 downsamples time by 2 → [batch, d_model, enc_time]
    const conv2_w = try cb.getWeight("model.encoder.conv2.weight");
    defer cb.free(conv2_w);
    const conv2_b = try cb.getWeight("model.encoder.conv2.bias");
    defer cb.free(conv2_b);
    const enc_time = (time_steps + 2 * 1 - 3) / 2 + 1;
    const conv2_out = try cb.conv1d(conv1_act, conv2_w, conv2_b, batch, d_model, d_model, time_steps, 3, 2, 1);
    defer cb.free(conv2_out);
    const conv2_act = try cb.gelu(conv2_out);
    defer cb.free(conv2_act);

    // 2. Transpose [batch, d_model, enc_time] → [batch*enc_time, d_model]
    // Read to f32, transpose, re-wrap as CT
    const conv_data = try cb.toFloat32(conv2_act, allocator);
    defer allocator.free(conv_data);

    const total = batch * enc_time;
    const transposed = try allocator.alloc(f32, total * d_model);
    defer allocator.free(transposed);
    for (0..batch) |b| {
        for (0..enc_time) |t| {
            for (0..d_model) |d| {
                transposed[(b * enc_time + t) * d_model + d] = conv_data[(b * d_model + d) * enc_time + t];
            }
        }
    }

    const hidden_shape = [_]i32{
        @intCast(total),
        @intCast(d_model),
    };
    var hidden = try cb.fromFloat32Shape(transposed, &hidden_shape);

    // 3. Add sinusoidal position embeddings
    var pos_ids_buf: [4096]i64 = undefined;
    if (total > 4096) return error.SequenceTooLong;
    const pos_ids = pos_ids_buf[0..total];
    for (0..total) |i| pos_ids[i] = @intCast(i % enc_time);

    const pos_w = try cb.getWeight("model.encoder.embed_positions.weight");
    defer cb.free(pos_w);
    const pos_emb = try cb.embeddingLookup(pos_w, pos_ids, total, d_model);
    defer cb.free(pos_emb);

    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    // 4. Encoder blocks
    var mask_buf: [4096]i64 = undefined;
    const mask = mask_buf[0..total];
    @memset(mask, 1); // audio: attend to everything

    var name_buf: [256]u8 = undefined;

    for (0..config.encoder_layers) |layer| {
        const new_hidden = try encoderBlock(cb, config, hidden, mask, batch, enc_time, layer, &name_buf);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 5. Final layer norm
    const ln_w = try cb.getWeight("model.encoder.layer_norm.weight");
    defer cb.free(ln_w);
    const ln_b = try cb.getWeight("model.encoder.layer_norm.bias");
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);

    const result = try cb.toFloat32(normed, allocator);
    cb.free(normed);
    return result;
}

/// Run the Whisper decoder forward pass.
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

    // 1. Token embeddings
    const embed_w = try cb.getWeight("model.decoder.embed_tokens.weight");
    defer cb.free(embed_w);
    var hidden = try cb.embeddingLookup(embed_w, decoder_input_ids, dec_total, d_model);

    // 2. Learned position embeddings
    var pos_ids_buf: [2048]i64 = undefined;
    if (dec_total > 2048) return error.SequenceTooLong;
    const pos_ids = pos_ids_buf[0..dec_total];
    for (0..dec_total) |i| pos_ids[i] = @intCast(i % dec_seq);

    const pos_w = try cb.getWeight("model.decoder.embed_positions.weight");
    defer cb.free(pos_w);
    const pos_emb = try cb.embeddingLookup(pos_w, pos_ids, dec_total, d_model);
    defer cb.free(pos_emb);

    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    var name_buf: [256]u8 = undefined;

    // 3. Decoder blocks
    for (0..config.decoder_layers) |layer| {
        const new_hidden = try decoderBlock(cb, config, hidden, encoder_hidden, encoder_mask, batch, dec_seq, enc_seq, layer, &name_buf);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 4. Final layer norm
    const ln_w = try cb.getWeight("model.decoder.layer_norm.weight");
    defer cb.free(ln_w);
    const ln_b = try cb.getWeight("model.decoder.layer_norm.bias");
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);

    // 5. LM head: project to vocab
    const lm_w = cb.getWeight("proj_out.weight") catch blk: {
        break :blk cb.getWeight("model.decoder.embed_tokens.weight") catch return error.MissingLMHead;
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
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.encoder_attention_heads;
    const head_dim = config.encoderHeadDim();
    const ffn_dim = config.encoder_ffn_dim;
    const total = batch * seq_len;

    // --- Self-attention sublayer (pre-norm) ---
    const ln_w = try getEncoderWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    defer cb.free(ln_w);
    const ln_b = try getEncoderWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    defer cb.free(ln_b);
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    defer cb.free(normed);

    const Q = try linearWithBias(cb, normed, layer, "encoder", "self_attn.q_proj", total, d_model, d_model, buf);
    defer cb.free(Q);
    const K = try linearWithBias(cb, normed, layer, "encoder", "self_attn.k_proj", total, d_model, d_model, buf);
    defer cb.free(K);
    const V = try linearWithBias(cb, normed, layer, "encoder", "self_attn.v_proj", total, d_model, d_model, buf);
    defer cb.free(V);

    const attn_out = try cb.scaledDotProductAttention(Q, K, V, attention_mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn_out);

    const projected = try linearWithBias(cb, attn_out, layer, "encoder", "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(projected);

    const attn_res = try cb.add(projected, hidden);

    // --- FFN sublayer (pre-norm) ---
    const ffn_ln_w = try getEncoderWeight(cb, layer, "final_layer_norm.weight", buf);
    defer cb.free(ffn_ln_w);
    const ffn_ln_b = try getEncoderWeight(cb, layer, "final_layer_norm.bias", buf);
    defer cb.free(ffn_ln_b);
    const ffn_normed = try cb.layerNorm(attn_res, ffn_ln_w, ffn_ln_b, d_model, 1e-5);
    defer cb.free(ffn_normed);

    const fc1_out = try linearWithBias(cb, ffn_normed, layer, "encoder", "fc1", total, d_model, ffn_dim, buf);
    defer cb.free(fc1_out);
    const activated = try cb.gelu(fc1_out);
    defer cb.free(activated);
    const fc2_out = try linearWithBias(cb, activated, layer, "encoder", "fc2", total, ffn_dim, d_model, buf);
    defer cb.free(fc2_out);

    const result = try cb.add(fc2_out, attn_res);
    cb.free(attn_res);

    return result;
}

// --- Decoder block ---

fn decoderBlock(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const dec_total = batch * dec_seq;
    const enc_total = batch * enc_seq;

    // --- Causal self-attention sublayer ---
    const ln0_w = try getDecoderWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    defer cb.free(ln0_w);
    const ln0_b = try getDecoderWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    defer cb.free(ln0_b);
    const normed = try cb.layerNorm(hidden, ln0_w, ln0_b, d_model, 1e-5);
    defer cb.free(normed);

    const Q_self = try linearWithBias(cb, normed, layer, "decoder", "self_attn.q_proj", dec_total, d_model, d_model, buf);
    defer cb.free(Q_self);
    const K_self = try linearWithBias(cb, normed, layer, "decoder", "self_attn.k_proj", dec_total, d_model, d_model, buf);
    defer cb.free(K_self);
    const V_self = try linearWithBias(cb, normed, layer, "decoder", "self_attn.v_proj", dec_total, d_model, d_model, buf);
    defer cb.free(V_self);

    const self_attn = try cb.causalSelfAttention(Q_self, K_self, V_self, null, batch, dec_seq, num_heads, head_dim);
    defer cb.free(self_attn);

    const self_proj = try linearWithBias(cb, self_attn, layer, "decoder", "self_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(self_proj);

    const self_res = try cb.add(self_proj, hidden);

    // --- Cross-attention sublayer ---
    const ln1_w = try getDecoderWeight(cb, layer, "encoder_attn_layer_norm.weight", buf);
    defer cb.free(ln1_w);
    const ln1_b = try getDecoderWeight(cb, layer, "encoder_attn_layer_norm.bias", buf);
    defer cb.free(ln1_b);
    const cross_normed = try cb.layerNorm(self_res, ln1_w, ln1_b, d_model, 1e-5);
    defer cb.free(cross_normed);

    const Q_cross = try linearWithBias(cb, cross_normed, layer, "decoder", "encoder_attn.q_proj", dec_total, d_model, d_model, buf);
    defer cb.free(Q_cross);
    const K_cross = try linearWithBias(cb, encoder_hidden, layer, "decoder", "encoder_attn.k_proj", enc_total, d_model, d_model, buf);
    defer cb.free(K_cross);
    const V_cross = try linearWithBias(cb, encoder_hidden, layer, "decoder", "encoder_attn.v_proj", enc_total, d_model, d_model, buf);
    defer cb.free(V_cross);

    const cross_attn = try cb.crossAttention(Q_cross, K_cross, V_cross, encoder_mask, batch, dec_seq, enc_seq, num_heads, head_dim);
    defer cb.free(cross_attn);

    const cross_proj = try linearWithBias(cb, cross_attn, layer, "decoder", "encoder_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(cross_proj);

    const cross_res = try cb.add(cross_proj, self_res);
    cb.free(self_res);

    // --- FFN sublayer ---
    const ln2_w = try getDecoderWeight(cb, layer, "final_layer_norm.weight", buf);
    defer cb.free(ln2_w);
    const ln2_b = try getDecoderWeight(cb, layer, "final_layer_norm.bias", buf);
    defer cb.free(ln2_b);
    const ffn_normed = try cb.layerNorm(cross_res, ln2_w, ln2_b, d_model, 1e-5);
    defer cb.free(ffn_normed);

    const fc1_out = try linearWithBias(cb, ffn_normed, layer, "decoder", "fc1", dec_total, d_model, ffn_dim, buf);
    defer cb.free(fc1_out);
    const activated = try cb.gelu(fc1_out);
    defer cb.free(activated);
    const fc2_out = try linearWithBias(cb, activated, layer, "decoder", "fc2", dec_total, ffn_dim, d_model, buf);
    defer cb.free(fc2_out);

    const result = try cb.add(fc2_out, cross_res);
    cb.free(cross_res);

    return result;
}

// --- Weight helpers ---

fn linearWithBias(
    cb: *const ComputeBackend,
    input: CT,
    layer: usize,
    stack: []const u8,
    proj: []const u8,
    rows: usize,
    in_dim: u32,
    out_dim: u32,
    buf: *[256]u8,
) !CT {
    const prefix = if (std.mem.eql(u8, stack, "encoder")) "model.encoder.layers" else "model.decoder.layers";
    const w_name = std.fmt.bufPrint(buf, "{s}.{d}.{s}.weight", .{ prefix, layer, proj }) catch return error.NameTooLong;
    const w = try cb.getWeight(w_name);
    defer cb.free(w);
    const b_name = std.fmt.bufPrint(buf, "{s}.{d}.{s}.bias", .{ prefix, layer, proj }) catch return error.NameTooLong;
    const maybe_b = cb.getWeight(b_name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => null,
        else => return err,
    };
    if (maybe_b) |b| {
        defer cb.free(b);
        return cb.linear(input, w, b, rows, in_dim, out_dim);
    }
    return cb.linearNoBias(input, w, rows, in_dim, out_dim);
}

fn getEncoderWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "model.encoder.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

fn getDecoderWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}
