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
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const whisper_config = @import("../models/whisper.zig");

pub const Config = whisper_config.Config;

/// Run the Whisper encoder forward pass on mel spectrogram features.
/// mel_features: [batch * num_mel_bins * time_steps] as CT (channels-first).
/// Returns encoder hidden states as f32: [batch * enc_seq * d_model].
/// Per-call profile of the encoder, printed with
/// TERMITE_WHISPER_METAL_PROFILE=1. Each mark flushes the open frame so
/// the elapsed time attributes to the ops since the previous mark.
const EncoderProfile = struct {
    enabled: bool,
    frame_active: *bool,
    last_ns: u64 = 0,

    fn start(self: *EncoderProfile) void {
        if (!self.enabled) return;
        self.last_ns = platform.time.monotonicNs();
        std.debug.print("whisper_metal_encoder", .{});
    }

    fn mark(self: *EncoderProfile, cb: *const ComputeBackend, label: []const u8) void {
        if (!self.enabled) return;
        if (self.frame_active.*) cb.decoderRuntimeFlushActiveFrame() catch {};
        const now = platform.time.monotonicNs();
        std.debug.print(" {s}={d}us", .{ label, (now -| self.last_ns) / std.time.ns_per_us });
        self.last_ns = now;
    }

    fn finish(self: *const EncoderProfile) void {
        if (self.enabled) std.debug.print("\n", .{});
    }
};

/// `[batch, d_model, enc_time]` (conv layout) to `[batch * enc_time, d_model]`
/// (token rows). On backends with a device transpose the data never leaves
/// the accelerator; otherwise it goes through the host.
fn timeMajorHidden(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    conv_out: CT,
    batch: usize,
    d_model: usize,
    enc_time: usize,
) !CT {
    const total = batch * enc_time;
    const conv_shape = [_]i64{ @intCast(batch), @intCast(d_model), @intCast(enc_time) };
    const perm = [_]u8{ 0, 2, 1 };
    if (cb.primTranspose(conv_out, &perm, &conv_shape)) |transposed| {
        const rows_shape = [_]i64{ @intCast(total), @intCast(d_model) };
        if (cb.primReshape(transposed, &rows_shape)) |rows| {
            cb.free(transposed);
            return rows;
        } else |_| {}
        cb.free(transposed);
    } else |err| switch (err) {
        error.OutOfMemory => return err,
        else => {},
    }

    const conv_data = try cb.toFloat32(conv_out, allocator);
    defer allocator.free(conv_data);
    const transposed = try allocator.alloc(f32, total * d_model);
    defer allocator.free(transposed);
    for (0..batch) |b| {
        for (0..enc_time) |t| {
            for (0..d_model) |d| {
                transposed[(b * enc_time + t) * d_model + d] = conv_data[(b * d_model + d) * enc_time + t];
            }
        }
    }
    const hidden_shape = [_]i32{ @intCast(total), @intCast(d_model) };
    const host = try cb.fromFloat32Shape(transposed, &hidden_shape);
    return residentProjection(cb, host);
}

/// Run the Whisper encoder over log-mel features `[batch, num_mel_bins,
/// time_steps]` and return the hidden states `[batch * enc_time, d_model]`
/// on the host. On Metal the whole pass is one frame on one compute
/// encoder: the conv stem, a device transpose, and pre-norm blocks whose
/// residual adds are folded into the following layer norm.
pub fn encoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    mel_features: CT,
    batch: usize,
    time_steps: usize,
) ![]f32 {
    const d_model = config.d_model;
    const num_heads = config.encoder_attention_heads;
    const head_dim = config.encoderHeadDim();
    const ffn_dim = config.encoder_ffn_dim;

    var frame_active = try beginWhisperMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    const scope_active = frame_active and (cb.decoderRuntimeBeginPlannedComputeScope() catch false);
    defer if (scope_active) cb.decoderRuntimeEndPlannedComputeScope();
    var profile = EncoderProfile{ .enabled = frame_active and whisperMetalProfileEnabled(), .frame_active = &frame_active };
    profile.start();
    defer profile.finish();

    var fetcher = WeightFetcher{ .cb = cb, .allocator = allocator, .prefix = "model.encoder.layers" };
    defer fetcher.release(cb);
    var buf: [256]u8 = undefined;

    // The caller hands mel features as a host tensor; move them once.
    const mel_device = try cb.ensureDeviceResident(mel_features);
    defer if (mel_device) |t| cb.free(t);
    const mel = mel_device orelse mel_features;

    // 1. Conv1d frontend: [batch, num_mel_bins, time] -> [batch, d_model, enc_time].
    // Preferred route: unfold on the device and run each conv as one dense
    // matmul, which also leaves the result in token-row order so no
    // transpose is needed. Falls back to the direct conv kernels.
    const conv1_w = try fetcher.fetch("model.encoder.conv1.weight");
    const conv1_b = try fetcher.fetch("model.encoder.conv1.bias");
    const conv2_w = try fetcher.fetch("model.encoder.conv2.weight");
    const conv2_b = try fetcher.fetch("model.encoder.conv2.bias");
    const enc_time = (time_steps + 2 * 1 - 3) / 2 + 1;
    const total = batch * enc_time;
    var hidden: CT = undefined;
    var hidden_live = false;
    if (try convViaIm2col(cb, conv1_w, conv1_b, mel, batch, config.num_mel_bins, d_model, time_steps, 3, 1, 1, false)) |conv1_out| {
        defer cb.free(conv1_out);
        const conv1_act = try cb.gelu(conv1_out);
        defer cb.free(conv1_act);
        profile.mark(cb, "conv1");
        if (try convViaIm2col(cb, conv2_w, conv2_b, conv1_act, batch, d_model, d_model, time_steps, 3, 2, 1, true)) |conv2_out| {
            defer cb.free(conv2_out);
            hidden = try cb.gelu(conv2_out);
            hidden_live = true;
        } else {
            // Back to the channel-major kernels for the second conv.
            const rows_shape = [_]i64{ @intCast(batch), @intCast(time_steps), @intCast(d_model) };
            const perm = [_]u8{ 0, 2, 1 };
            const channel_major = try cb.primTranspose(conv1_act, &perm, &rows_shape);
            defer cb.free(channel_major);
            const conv2_out = try cb.conv1d(channel_major, conv2_w, conv2_b, batch, d_model, d_model, time_steps, 3, 2, 1);
            defer cb.free(conv2_out);
            const conv2_act = try cb.gelu(conv2_out);
            defer cb.free(conv2_act);
            hidden = try timeMajorHidden(cb, allocator, conv2_act, batch, d_model, enc_time);
            hidden_live = true;
        }
        profile.mark(cb, "conv2");
    } else {
        const conv1_out = try cb.conv1d(mel, conv1_w, conv1_b, batch, config.num_mel_bins, d_model, time_steps, 3, 1, 1);
        defer cb.free(conv1_out);
        const conv1_act = try cb.gelu(conv1_out);
        defer cb.free(conv1_act);
        profile.mark(cb, "conv1");
        const conv2_out = try cb.conv1d(conv1_act, conv2_w, conv2_b, batch, d_model, d_model, time_steps, 3, 2, 1);
        defer cb.free(conv2_out);
        const conv2_act = try cb.gelu(conv2_out);
        defer cb.free(conv2_act);
        profile.mark(cb, "conv2");
        hidden = try timeMajorHidden(cb, allocator, conv2_act, batch, d_model, enc_time);
        hidden_live = true;
        profile.mark(cb, "transpose");
    }
    defer if (hidden_live) cb.free(hidden);

    var pos_ids_buf: [4096]i64 = undefined;
    if (total > pos_ids_buf.len) return error.SequenceTooLong;
    const pos_ids = pos_ids_buf[0..total];
    for (0..total) |i| pos_ids[i] = @intCast(i % enc_time);
    const pos_w = try fetcher.fetch("model.encoder.embed_positions.weight");
    const pos_emb = try cb.embeddingLookup(pos_w, pos_ids, total, d_model);
    defer cb.free(pos_emb);
    var stream = ResidualStream{ .sum = try cb.add(hidden, pos_emb) };
    defer stream.deinit(cb);
    cb.free(hidden);
    hidden_live = false;
    profile.mark(cb, "positions");

    // 3. Encoder blocks.
    for (0..config.encoder_layers) |layer| {
        const w = EncoderLayerWeights{
            .self_ln = try fetcher.norm(layer, "self_attn_layer_norm", &buf),
            .q = try fetcher.linear(layer, "self_attn.q_proj", &buf),
            .k = try fetcher.linear(layer, "self_attn.k_proj", &buf),
            .v = try fetcher.linear(layer, "self_attn.v_proj", &buf),
            .o = try fetcher.linear(layer, "self_attn.out_proj", &buf),
            .ffn_ln = try fetcher.norm(layer, "final_layer_norm", &buf),
            .fc1 = try fetcher.linear(layer, "fc1", &buf),
            .fc2 = try fetcher.linear(layer, "fc2", &buf),
        };
        const normed = try stream.normalize(cb, w.self_ln, d_model, null);
        defer cb.free(normed);
        const residual = stream.sum;
        profile.mark(cb, "ln1");

        const proj = try projectEncoderQkv(cb, &w, normed, total, d_model);
        defer {
            cb.free(proj.q);
            cb.free(proj.k);
            cb.free(proj.v);
        }
        profile.mark(cb, "qkv");
        // Non-causal attention over all encoder rows; the vision variant
        // selects the flash kernel for head_dim 64 on Metal.
        const attn_out = try cb.scaledDotProductAttentionQwen3VlVision(proj.q, proj.k, proj.v, batch, enc_time, num_heads, head_dim);
        defer cb.free(attn_out);
        profile.mark(cb, "attention");
        const projected = try w.o.apply(cb, attn_out, total, d_model, d_model);
        defer cb.free(projected);
        const after_attn = try addNorm(cb, projected, residual, w.ffn_ln, d_model, null);
        var after_attn_sum_live = true;
        defer if (after_attn_sum_live) cb.free(after_attn.sum);
        defer cb.free(after_attn.normed);
        profile.mark(cb, "out_proj");

        const fc1_out = try w.fc1.apply(cb, after_attn.normed, total, d_model, ffn_dim);
        defer cb.free(fc1_out);
        const activated = try cb.gelu(fc1_out);
        defer cb.free(activated);
        profile.mark(cb, "fc1");
        const fc2_out = try w.fc2.apply(cb, activated, total, ffn_dim, d_model);
        profile.mark(cb, "fc2");

        cb.free(stream.sum);
        stream.sum = after_attn.sum;
        after_attn_sum_live = false;
        stream.pending = fc2_out;
    }

    // 4. Final layer norm, then read the hidden states back.
    const final_ln = Norm{
        .w = try fetcher.fetch("model.encoder.layer_norm.weight"),
        .b = try fetcher.fetch("model.encoder.layer_norm.bias"),
    };
    const normed = try stream.normalize(cb, final_ln, d_model, null);
    defer cb.free(normed);
    profile.mark(cb, "final_norm");
    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }
    const result = try cb.toFloat32(normed, allocator);
    profile.mark(cb, "readback");
    return result;
}

/// A 1-D convolution as im2col plus one dense linear over the backend's
/// matmul path. Output rows are `[batch * out_time, out_channels]`
/// (time-major). Null when the backend lacks the pieces.
fn convViaIm2col(
    cb: *const ComputeBackend,
    weight: CT,
    bias: CT,
    input: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    time_steps: usize,
    kernel_size: usize,
    stride: usize,
    padding: usize,
    time_major: bool,
) !?CT {
    const trace = whisperMetalProfileEnabled();
    const cols = try cb.conv1dIm2col(input, batch, in_channels, time_steps, kernel_size, stride, padding, time_major) orelse {
        if (trace) std.debug.print("whisper_metal_conv: im2col unavailable in={d} k={d}\n", .{ in_channels, kernel_size });
        return null;
    };
    defer cb.free(cols);
    const in_dim = in_channels * kernel_size;
    // The conv weight `[out, in, kernel]` is already the row-major
    // `[out, in * kernel]` matrix the linear slot wants; the slot
    // preparation relabels it.
    const slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = weight, .bias = bias, .in_dim = in_dim, .out_dim = out_channels })) orelse {
        if (trace) std.debug.print("whisper_metal_conv: no linear slot in={d} out={d}\n", .{ in_dim, out_channels });
        return null;
    };
    const out = try cb.decoderRuntimeApplyLinear(&.{ .slot = slot, .input = cols, .in_dim = in_dim, .out_dim = out_channels });
    if (out == null and trace) std.debug.print("whisper_metal_conv: linear declined slot={d} in={d} out={d}\n", .{ slot, in_dim, out_channels });
    return out;
}

const EncoderLayerWeights = struct {
    self_ln: Norm,
    q: Linear,
    k: Linear,
    v: Linear,
    o: Linear,
    ffn_ln: Norm,
    fc1: Linear,
    fc2: Linear,
};

/// Q/K/V over all encoder rows from one fused dispatch when the backend has
/// slots for the three projections, otherwise three linears.
fn projectEncoderQkv(cb: *const ComputeBackend, w: *const EncoderLayerWeights, normed: CT, rows: usize, d_model: usize) !Projections {
    if (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = w.q.w, .bias = w.q.b, .in_dim = d_model, .out_dim = d_model })) |q_slot| {
        if (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = w.k.w, .bias = w.k.b, .in_dim = d_model, .out_dim = d_model })) |k_slot| {
            if (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = w.v.w, .bias = w.v.b, .in_dim = d_model, .out_dim = d_model })) |v_slot| {
                if (q_slot != k_slot and q_slot != v_slot and k_slot != v_slot) {
                    if (try cb.decoderRuntimeApplyLinearQkv(&.{
                        .q_slot = q_slot,
                        .k_slot = k_slot,
                        .v_slot = v_slot,
                        .input = normed,
                        .in_dim = d_model,
                        .q_out_dim = d_model,
                        .kv_out_dim = d_model,
                    })) |triple| {
                        return .{ .q = triple.first, .k = triple.second, .v = triple.third };
                    }
                }
            }
        }
    }
    const q = try w.q.apply(cb, normed, rows, d_model, d_model);
    errdefer cb.free(q);
    const k = try w.k.apply(cb, normed, rows, d_model, d_model);
    errdefer cb.free(k);
    const v = try w.v.apply(cb, normed, rows, d_model, d_model);
    return .{ .q = q, .k = k, .v = v };
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

// --- Incremental decoding ---

fn whisperMetalFramesEnabled() bool {
    return !platform.env.getenvBool("TERMITE_WHISPER_METAL_DISABLE_FRAMES");
}

fn whisperMetalProfileEnabled() bool {
    return platform.env.getenvBool("TERMITE_WHISPER_METAL_PROFILE");
}

/// Per-op GPU accounting for one decoder step, enabled with
/// TERMITE_WHISPER_METAL_PROFILE=1. Each mark flushes the open frame so the
/// elapsed time attributes to the ops issued since the previous mark; it
/// serializes the step and is for diagnosis only.
const StepProfile = struct {
    const Bucket = enum {
        embed,
        self_norm,
        qkv,
        kv_append,
        self_attn,
        self_out,
        cross_norm_q,
        cross_attn,
        cross_out,
        ffn_norm,
        fc1,
        gelu,
        fc2,
        final_norm,
        lm_head,
        readback,
    };
    enabled: bool,
    frame_active: *bool,
    last_ns: u64 = 0,
    /// How many fused kernels the step used, to confirm the fast paths.
    fused_qkv: u32 = 0,
    fused_add_norm: u32 = 0,
    /// K/V projections written straight into the cache slabs (no copy).
    in_place_kv: u32 = 0,
    device_choice: bool = false,
    planned_scope: bool = false,
    totals: [std.enums.values(Bucket).len]u64 = [_]u64{0} ** std.enums.values(Bucket).len,

    fn start(self: *StepProfile) void {
        if (self.enabled) self.last_ns = platform.time.monotonicNs();
    }

    fn mark(self: *StepProfile, cb: *const ComputeBackend, bucket: Bucket) void {
        if (!self.enabled) return;
        if (self.frame_active.*) cb.decoderRuntimeFlushActiveFrame() catch {};
        const now = platform.time.monotonicNs();
        self.totals[@intFromEnum(bucket)] += now -| self.last_ns;
        self.last_ns = now;
    }

    fn report(self: *const StepProfile, step: usize, rows: usize) void {
        if (!self.enabled) return;
        std.debug.print("whisper_metal_step position={d} rows={d} fused_qkv={d} fused_add_norm={d} in_place_kv={d} device_choice={} planned_scope={}", .{ step, rows, self.fused_qkv, self.fused_add_norm, self.in_place_kv, self.device_choice, self.planned_scope });
        for (std.enums.values(Bucket), 0..) |bucket, i| {
            std.debug.print(" {s}={d}us", .{ @tagName(bucket), self.totals[i] / std.time.ns_per_us });
        }
        std.debug.print("\n", .{});
    }
};

/// Open one backend-owned command submission for a decoder step. Every
/// device op issued until `decoderRuntimeSubmitAndWaitFrame` joins it, so a
/// token costs one dispatch and one wait instead of one per op. Returns
/// false on backends without frames; callers then run eagerly.
fn beginWhisperMetalFrame(cb: *const ComputeBackend, regime: ops.DecoderRuntimeFrameRegime) !bool {
    if (cb.kind() != .metal or !whisperMetalFramesEnabled() or cb.decoderRuntimeHasActiveFrame()) return false;
    const active = try cb.decoderRuntimeBeginFrame();
    if (!active) return false;
    errdefer cb.decoderRuntimeCancelFrame() catch {};
    try cb.decoderRuntimeSetActiveFrameRegime(regime);
    return true;
}

/// Replace `tensor` with a device-resident copy when the backend can make
/// one; otherwise return it unchanged. Consumes `tensor`.
fn residentProjection(cb: *const ComputeBackend, tensor: CT) !CT {
    const resident = cb.ensureDeviceResident(tensor) catch |err| {
        cb.free(tensor);
        return err;
    };
    if (resident) |device| {
        cb.free(tensor);
        return device;
    }
    return tensor;
}

/// A projection with an optional bias.
pub const Linear = struct {
    w: CT,
    b: ?CT,

    fn apply(self: Linear, cb: *const ComputeBackend, input: CT, rows: usize, in_dim: usize, out_dim: usize) !CT {
        if (self.b) |b| return cb.linear(input, self.w, b, rows, in_dim, out_dim);
        return cb.linearNoBias(input, self.w, rows, in_dim, out_dim);
    }
};

pub const Norm = struct {
    w: CT,
    b: CT,

    fn apply(self: Norm, cb: *const ComputeBackend, input: CT, dim: usize) !CT {
        return cb.layerNorm(input, self.w, self.b, dim, 1e-5);
    }
};

/// Decoder weights resolved once per transcription. Fetching them per op
/// per token cost a name lookup and a handle churn each time, and on Metal
/// it kept the linear slot cache from being reused between steps.
pub const LayerWeights = struct {
    self_ln: Norm,
    q: Linear,
    k: Linear,
    v: Linear,
    o: Linear,
    /// Prepared runtime slots for q, k, v when the backend can project all
    /// three from one dispatch; null runs them separately.
    qkv_slots: ?[3]usize = null,
    cross_ln: Norm,
    cross_q: Linear,
    cross_o: Linear,
    ffn_ln: Norm,
    fc1: Linear,
    fc2: Linear,
};

pub const DecoderWeights = struct {
    embed: CT,
    positions: CT,
    final_ln: Norm,
    lm_head: CT,
    layers: []LayerWeights,
};

/// Collects every weight handle taken from the backend so they can be
/// released together, whatever partial state a failed load leaves behind.
/// The backend pointer is passed per call rather than stored: the cache
/// outlives the stack frame that built it, and its owner moves the backend.
const WeightFetcher = struct {
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    handles: std.ArrayListUnmanaged(CT) = .empty,
    /// Layer weight name prefix (`model.decoder.layers` or `model.encoder.layers`).
    prefix: []const u8 = "model.decoder.layers",

    fn fetch(self: *WeightFetcher, name: []const u8) !CT {
        const tensor = try self.cb.getWeight(name);
        errdefer self.cb.free(tensor);
        try self.handles.append(self.allocator, tensor);
        return tensor;
    }

    fn fetchOptional(self: *WeightFetcher, name: []const u8) !?CT {
        const tensor = self.cb.getWeight(name) catch |err| switch (err) {
            error.MissingWeight, error.WeightNotFound => return null,
            else => return err,
        };
        errdefer self.cb.free(tensor);
        try self.handles.append(self.allocator, tensor);
        return tensor;
    }

    fn linear(self: *WeightFetcher, layer: usize, proj: []const u8, buf: *[256]u8) !Linear {
        const w_name = std.fmt.bufPrint(buf, "{s}.{d}.{s}.weight", .{ self.prefix, layer, proj }) catch return error.NameTooLong;
        const w = try self.fetch(w_name);
        const b_name = std.fmt.bufPrint(buf, "{s}.{d}.{s}.bias", .{ self.prefix, layer, proj }) catch return error.NameTooLong;
        const b = try self.fetchOptional(b_name);
        return .{ .w = w, .b = b };
    }

    fn norm(self: *WeightFetcher, layer: usize, name: []const u8, buf: *[256]u8) !Norm {
        const w_name = std.fmt.bufPrint(buf, "{s}.{d}.{s}.weight", .{ self.prefix, layer, name }) catch return error.NameTooLong;
        const w = try self.fetch(w_name);
        const b_name = std.fmt.bufPrint(buf, "{s}.{d}.{s}.bias", .{ self.prefix, layer, name }) catch return error.NameTooLong;
        const b = try self.fetch(b_name);
        return .{ .w = w, .b = b };
    }

    fn release(self: *WeightFetcher, cb: *const ComputeBackend) void {
        for (self.handles.items) |tensor| cb.free(tensor);
        self.handles.deinit(self.allocator);
        self.cb = undefined;
    }
};

/// Per-transcription decoder state. Cross-attention keys and values are
/// projected from the encoder output once per layer; self-attention keys
/// and values grow by one row per generated token. Without this, every
/// decode step re-ran the decoder over the whole prefix and re-projected
/// all encoder positions, which made decoding quadratic and dominated
/// transcription latency.
///
/// On backends that can hand out uninitialized device slabs, the self
/// cache is preallocated to `max_target_positions` rows per layer and each
/// token is blitted into place, so the cache never reallocates and the
/// attention kernel reads a zero-copy view of the live prefix. Elsewhere the
/// cache grows by concatenation.
pub const DecodeCache = struct {
    allocator: std.mem.Allocator,
    layers: []LayerCache,
    weights: DecoderWeights,
    fetcher: WeightFetcher,
    /// Tokens already folded into the self-attention cache.
    positions: usize = 0,
    enc_seq: usize,
    encoder_mask: []i64,
    /// All-ones mask covering `max_target_positions` cached keys.
    self_mask: []i64,
    /// Rows per preallocated self-cache slab; 0 when the cache grows by
    /// concatenation instead.
    capacity: usize = 0,

    pub const LayerCache = struct {
        k_cross: CT,
        v_cross: CT,
        k_self: ?CT = null,
        v_self: ?CT = null,
    };

    pub fn init(
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        config: Config,
        encoder_hidden: CT,
        enc_seq: usize,
    ) !DecodeCache {
        const d_model = config.d_model;
        var fetcher = WeightFetcher{ .cb = cb, .allocator = allocator };
        errdefer fetcher.release(cb);
        var buf: [256]u8 = undefined;

        const layer_weights = try allocator.alloc(LayerWeights, config.decoder_layers);
        errdefer allocator.free(layer_weights);
        for (0..config.decoder_layers) |layer| {
            layer_weights[layer] = .{
                .self_ln = try fetcher.norm(layer, "self_attn_layer_norm", &buf),
                .q = try fetcher.linear(layer, "self_attn.q_proj", &buf),
                .k = try fetcher.linear(layer, "self_attn.k_proj", &buf),
                .v = try fetcher.linear(layer, "self_attn.v_proj", &buf),
                .o = try fetcher.linear(layer, "self_attn.out_proj", &buf),
                .cross_ln = try fetcher.norm(layer, "encoder_attn_layer_norm", &buf),
                .cross_q = try fetcher.linear(layer, "encoder_attn.q_proj", &buf),
                .cross_o = try fetcher.linear(layer, "encoder_attn.out_proj", &buf),
                .ffn_ln = try fetcher.norm(layer, "final_layer_norm", &buf),
                .fc1 = try fetcher.linear(layer, "fc1", &buf),
                .fc2 = try fetcher.linear(layer, "fc2", &buf),
            };
        }
        // Fused Q/K/V projection: one dispatch per layer instead of three.
        // Slots live in the backend runtime; a backend without them (or one
        // that ran out of slots) leaves the separate projections in place.
        for (layer_weights) |*lw| {
            const q_slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = lw.q.w, .bias = lw.q.b, .in_dim = d_model, .out_dim = d_model })) orelse break;
            const k_slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = lw.k.w, .bias = lw.k.b, .in_dim = d_model, .out_dim = d_model })) orelse break;
            const v_slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = lw.v.w, .bias = lw.v.b, .in_dim = d_model, .out_dim = d_model })) orelse break;
            if (q_slot == k_slot or q_slot == v_slot or k_slot == v_slot) break;
            lw.qkv_slots = .{ q_slot, k_slot, v_slot };
        }
        const embed = try fetcher.fetch("model.decoder.embed_tokens.weight");
        const weights = DecoderWeights{
            .embed = embed,
            .positions = try fetcher.fetch("model.decoder.embed_positions.weight"),
            .final_ln = .{
                .w = try fetcher.fetch("model.decoder.layer_norm.weight"),
                .b = try fetcher.fetch("model.decoder.layer_norm.bias"),
            },
            .lm_head = fetcher.fetch("proj_out.weight") catch |err| switch (err) {
                error.MissingWeight, error.WeightNotFound => embed,
                else => return err,
            },
            .layers = layer_weights,
        };

        const layers = try allocator.alloc(LayerCache, config.decoder_layers);
        var built: usize = 0;
        errdefer {
            for (layers[0..built]) |layer| {
                cb.free(layer.k_cross);
                cb.free(layer.v_cross);
                if (layer.k_self) |k| cb.free(k);
                if (layer.v_self) |v| cb.free(v);
            }
            allocator.free(layers);
        }
        // The cross projections are one batched pass over the encoder
        // output; on Metal they share a single submission.
        var frame_active = try beginWhisperMetalFrame(cb, .prefill);
        errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
        for (0..config.decoder_layers) |layer| {
            const k = try fetcher.fetch(std.fmt.bufPrint(&buf, "model.decoder.layers.{d}.encoder_attn.k_proj.weight", .{layer}) catch return error.NameTooLong);
            const k_b = try fetcher.fetchOptional(std.fmt.bufPrint(&buf, "model.decoder.layers.{d}.encoder_attn.k_proj.bias", .{layer}) catch return error.NameTooLong);
            const v = try fetcher.fetch(std.fmt.bufPrint(&buf, "model.decoder.layers.{d}.encoder_attn.v_proj.weight", .{layer}) catch return error.NameTooLong);
            const v_b = try fetcher.fetchOptional(std.fmt.bufPrint(&buf, "model.decoder.layers.{d}.encoder_attn.v_proj.bias", .{layer}) catch return error.NameTooLong);
            // Every later step reads these from the device; keep them there
            // rather than re-uploading megabytes per layer per token.
            const k_cross = try residentProjection(cb, try (Linear{ .w = k, .b = k_b }).apply(cb, encoder_hidden, enc_seq, d_model, d_model));
            errdefer cb.free(k_cross);
            const v_cross = try residentProjection(cb, try (Linear{ .w = v, .b = v_b }).apply(cb, encoder_hidden, enc_seq, d_model, d_model));
            layers[layer] = .{ .k_cross = k_cross, .v_cross = v_cross };
            built += 1;
        }
        if (frame_active) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            frame_active = false;
        }

        // Preallocated self-cache slabs, when the backend supports them.
        var capacity: usize = 0;
        const slab_rows = @max(@as(usize, 1), config.max_target_positions);
        if (slab_rows <= std.math.maxInt(i32) and d_model <= std.math.maxInt(i32)) {
            const shape = [_]i32{ @intCast(slab_rows), @intCast(d_model) };
            var all_allocated = true;
            for (layers) |*layer| {
                const k_slab = (try cb.allocUninitF32Shape(&shape)) orelse {
                    all_allocated = false;
                    break;
                };
                layer.k_self = k_slab;
                const v_slab = (try cb.allocUninitF32Shape(&shape)) orelse {
                    all_allocated = false;
                    break;
                };
                layer.v_self = v_slab;
            }
            if (all_allocated) {
                capacity = slab_rows;
            } else {
                for (layers) |*layer| {
                    if (layer.k_self) |k| cb.free(k);
                    if (layer.v_self) |v| cb.free(v);
                    layer.k_self = null;
                    layer.v_self = null;
                }
            }
        }

        const encoder_mask = try allocator.alloc(i64, enc_seq);
        errdefer allocator.free(encoder_mask);
        @memset(encoder_mask, 1);
        const self_mask = try allocator.alloc(i64, slab_rows);
        @memset(self_mask, 1);
        fetcher.cb = undefined;
        return .{
            .allocator = allocator,
            .layers = layers,
            .weights = weights,
            .fetcher = fetcher,
            .enc_seq = enc_seq,
            .encoder_mask = encoder_mask,
            .self_mask = self_mask,
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *DecodeCache, cb: *const ComputeBackend) void {
        for (self.layers) |layer| {
            cb.free(layer.k_cross);
            cb.free(layer.v_cross);
            if (layer.k_self) |k| cb.free(k);
            if (layer.v_self) |v| cb.free(v);
        }
        self.allocator.free(self.layers);
        self.allocator.free(self.weights.layers);
        self.fetcher.release(cb);
        self.allocator.free(self.encoder_mask);
        self.allocator.free(self.self_mask);
        self.* = undefined;
    }

    fn preallocated(self: *const DecodeCache) bool {
        return self.capacity > 0;
    }
};

/// What a decoder step should produce for its last token.
pub const StepOutput = union(enum) {
    /// The full logits row on the host.
    logits,
    /// Nothing: the token only extends the cache (earlier tokens of a
    /// multi-token block).
    none,
    /// Whisper's constrained token choice and log-sum-exp terms computed on
    /// the device from the logits row, so only sixteen floats come back.
    stats: StatsRequest,
    /// Like `stats`, but the step is only encoded: the frame stays open
    /// for `decoderStepSubmit`, the device chooses the token itself, and
    /// `decoderStepAwait` collects the statistics.
    pipelined: PipelinedRequest,
};

pub const StatsRequest = struct {
    params: ops.WhisperLogitsParams,
    suppress: []const i32,
};

pub const PipelinedRequest = struct {
    params: ops.WhisperLogitsParams,
    suppress: []const i32,
    /// Embed the token a previous step left in this backend token slot
    /// instead of the host token (which is then a placeholder).
    device_token_slot: ?usize = null,
};

pub const StepResult = union(enum) {
    logits: []f32,
    none,
    stats: ops.WhisperLogitsStatsRaw,
    /// The step is encoded in the open frame, waiting for `decoderStepSubmit`.
    encoded,
};

/// Encode-ahead decoding is opt-in: with one frame in flight the backend
/// hides only the host encode, which is not on the critical path (whisper
/// tiny: 2.2 ms/token either way, 1.8 ms of it GPU time), so the proven
/// synchronous path stays the default.
fn whisperPipelinedDecodeEnabled() bool {
    return platform.env.getenvBool("TERMITE_WHISPER_ENABLE_PIPELINED_DECODE");
}

fn pipelineTrace(comptime fmt: []const u8, args: anytype) void {
    if (platform.env.getenvBool("TERMITE_WHISPER_TRACE_PIPELINE")) std.debug.print("whisper_pipeline: " ++ fmt ++ "\n", args);
}

/// Switch the backend into (or out of) pipelined decoding, where one step
/// runs on the device while the next is encoded. False when the backend
/// cannot pipeline; the cache must own preallocated slabs so no step
/// reallocates while another is in flight.
pub fn setPipelinedDecode(cb: *const ComputeBackend, cache: *const DecodeCache, enabled: bool) bool {
    if (!enabled) return cb.decoderRuntimeSetWhisperPipelinedFrames(false);
    if (cb.kind() != .metal or !whisperMetalFramesEnabled() or !whisperPipelinedDecodeEnabled()) return false;
    if (!cache.preallocated() or whisperMetalProfileEnabled()) return false;
    return cb.decoderRuntimeSetWhisperPipelinedFrames(true);
}

/// Submit the frame a `.pipelined` step left open. The backend keeps one
/// submitted frame, so the previous step must have been awaited.
pub fn decoderStepSubmit(cb: *const ComputeBackend) !void {
    try cb.decoderRuntimeSubmitFrame();
}

/// Drop an encoded step that will never be submitted (the step before it
/// ended the text).
pub fn decoderStepDiscard(cb: *const ComputeBackend) void {
    if (cb.decoderRuntimeHasActiveFrame()) cb.decoderRuntimeCancelFrame() catch {};
}

/// Wait for the step submitted by `decoderStepSubmit` and read the
/// statistics it left in `slot`. Null when nothing could be read.
pub fn decoderStepAwait(cb: *const ComputeBackend, slot: usize) !?ops.WhisperLogitsStatsRaw {
    try cb.decoderRuntimeWaitSubmittedFrame();
    var stats: ops.WhisperLogitsStatsRaw = undefined;
    if (!cb.whisperLogitsStatsRead(slot, &stats)) return null;
    return stats;
}

/// Decode `tokens` at positions `[cache.positions, cache.positions + len)`.
/// With an empty cache the whole block runs in one causal pass (the decoder
/// prompt); afterwards tokens run one at a time against the cache. Only the
/// last token produces `output`; a `.stats` request falls back to `.logits`
/// when the backend cannot compute the statistics.
pub fn decoderStepCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    tokens: []const i64,
    cache: *DecodeCache,
    output: StepOutput,
) !StepResult {
    if (tokens.len == 0) return error.InvalidInputShape;
    if (cache.positions == 0) return decodeBlockCached(cb, allocator, config, tokens, cache, output);
    var index: usize = 0;
    while (index + 1 < tokens.len) : (index += 1) {
        _ = try decodeBlockCached(cb, allocator, config, tokens[index .. index + 1], cache, .none);
    }
    return decodeBlockCached(cb, allocator, config, tokens[index..], cache, output);
}

/// Logits-only convenience for callers that always read the full row.
pub fn decoderStepCachedLogits(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    tokens: []const i64,
    cache: *DecodeCache,
) ![]f32 {
    return switch (try decoderStepCached(cb, allocator, config, tokens, cache, .logits)) {
        .logits => |row| row,
        else => error.InvalidInputShape,
    };
}

/// The pre-norm residual stream between sublayers. `pending` holds a
/// sublayer output whose addition into `sum` has not happened yet, so the
/// next layer norm can fold the add into its own kernel.
const ResidualStream = struct {
    sum: CT,
    pending: ?CT = null,

    /// Fold any pending output into the stream and return its normalized
    /// view. The caller owns the returned tensor.
    fn normalize(self: *ResidualStream, cb: *const ComputeBackend, norm: Norm, dim: usize, profile: ?*StepProfile) !CT {
        const pending = self.pending orelse return norm.apply(cb, self.sum, dim);
        const pair = try addNorm(cb, pending, self.sum, norm, dim, profile);
        cb.free(pending);
        cb.free(self.sum);
        self.pending = null;
        self.sum = pair.sum;
        return pair.normed;
    }

    fn deinit(self: *ResidualStream, cb: *const ComputeBackend) void {
        if (self.pending) |p| cb.free(p);
        cb.free(self.sum);
        self.* = undefined;
    }
};

const AddNormPair = struct {
    sum: CT,
    normed: CT,
};

/// `sum = a + b` and `normed = norm(sum)`, fused when the backend offers
/// it. Neither input is consumed.
fn addNorm(cb: *const ComputeBackend, a: CT, b: CT, norm: Norm, dim: usize, profile: ?*StepProfile) !AddNormPair {
    if (try cb.addLayerNormSum(a, b, norm.w, norm.b, dim, 1e-5)) |fused| {
        if (profile) |prof| prof.fused_add_norm += 1;
        return .{ .sum = fused.sum, .normed = fused.normed };
    }
    const sum = try cb.add(a, b);
    errdefer cb.free(sum);
    const normed = try norm.apply(cb, sum, dim);
    return .{ .sum = sum, .normed = normed };
}

fn decodeBlockCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    tokens: []const i64,
    cache: *DecodeCache,
    output: StepOutput,
) !StepResult {
    const d_model = config.d_model;
    const n = tokens.len;
    const start = cache.positions;
    if (start + n > config.max_target_positions) return error.SequenceTooLong;
    std.debug.assert(start == 0 or n == 1);
    const weights = &cache.weights;

    // One submission for the whole step. Registered before any tensor so a
    // failure cancels the frame after the intermediates are released.
    var frame_active = try beginWhisperMetalFrame(cb, if (start == 0) .prefill else .decode);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    // A pipelined step must run inside a frame: the caller submits it later
    // instead of waiting for it here.
    const pipelined = output == .pipelined;
    if (pipelined and (!frame_active or n != 1)) {
        pipelineTrace("step declined: frame_active={} tokens={d}", .{ frame_active, n });
        return error.UnsupportedOperation;
    }
    // One compute encoder for the whole step: every runtime op joins it, so
    // the GPU sees a single command sequence rather than an encoder per op.
    // Submitting the frame closes it; the end call only releases the scope.
    const scope_active = frame_active and (cb.decoderRuntimeBeginPlannedComputeScope() catch false);
    defer if (scope_active) cb.decoderRuntimeEndPlannedComputeScope();
    var profile = StepProfile{ .enabled = frame_active and !pipelined and whisperMetalProfileEnabled(), .frame_active = &frame_active };
    profile.planned_scope = scope_active;
    profile.start();

    const device_token_slot: ?usize = if (pipelined) output.pipelined.device_token_slot else null;
    const embedded = if (device_token_slot) |slot|
        (try cb.embeddingLookupDeviceToken(weights.embed, slot, d_model)) orelse {
            pipelineTrace("step declined: device token embedding (slot {d})", .{slot});
            return error.UnsupportedOperation;
        }
    else
        try cb.embeddingLookup(weights.embed, tokens, n, d_model);
    var embedded_live = true;
    defer if (embedded_live) cb.free(embedded);

    var pos_ids_buf: [2048]i64 = undefined;
    if (n > pos_ids_buf.len) return error.SequenceTooLong;
    const pos_ids = pos_ids_buf[0..n];
    for (0..n) |i| pos_ids[i] = @intCast(start + i);
    const pos_emb = try cb.embeddingLookup(weights.positions, pos_ids, n, d_model);
    defer cb.free(pos_emb);
    var stream = ResidualStream{ .sum = try cb.add(embedded, pos_emb) };
    defer stream.deinit(cb);
    cb.free(embedded);
    embedded_live = false;
    profile.mark(cb, .embed);

    for (0..config.decoder_layers) |layer| {
        try decoderBlockCached(cb, allocator, config, &stream, &cache.layers[layer], &weights.layers[layer], cache, n, start, &profile);
    }

    if (output == .none) {
        // Cache-only token: the residual stream is not needed past here.
        if (frame_active) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            frame_active = false;
        }
        cache.positions += n;
        return .none;
    }

    const normed = try stream.normalize(cb, weights.final_ln, d_model, &profile);
    var normed_live = true;
    defer if (normed_live) cb.free(normed);
    profile.mark(cb, .final_norm);

    // Only the last row feeds sampling; project it alone.
    const last_row = if (n == 1) normed else blk: {
        const row = try cb.sliceRows2D(allocator, normed, n - 1, 1, d_model);
        cb.free(normed);
        normed_live = false;
        break :blk row;
    };
    defer if (n != 1) cb.free(last_row);

    const logits = try cb.linearNoBias(last_row, weights.lm_head, 1, d_model, config.vocab_size);
    defer cb.free(logits);
    profile.mark(cb, .lm_head);

    if (pipelined) {
        const request = output.pipelined;
        if (!try cb.whisperLogitsStatsEncode(logits, &request.params, request.suppress)) {
            pipelineTrace("step declined: stats encode", .{});
            return error.UnsupportedOperation;
        }
        // The frame stays open; the caller submits it once the previous
        // step has been awaited (the backend keeps one frame in flight).
        frame_active = false;
        cache.positions += n;
        return .encoded;
    }
    if (output == .stats) {
        const request = output.stats;
        if (try cb.whisperLogitsStatsEncode(logits, &request.params, request.suppress)) {
            if (frame_active) {
                try cb.decoderRuntimeSubmitAndWaitFrame();
                frame_active = false;
            }
            var stats: ops.WhisperLogitsStatsRaw = undefined;
            if (cb.whisperLogitsStatsRead(request.params.stats_slot, &stats)) {
                profile.device_choice = true;
                profile.mark(cb, .readback);
                profile.report(start, n);
                cache.positions += n;
                return .{ .stats = stats };
            }
        }
    }
    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }
    const result = try cb.toFloat32(logits, allocator);
    profile.mark(cb, .readback);
    profile.report(start, n);
    cache.positions += n;
    return .{ .logits = result };
}

/// Fold the freshly projected keys or values for `[start, start + dec_seq)`
/// into the layer's self cache and return the tensor covering every cached
/// row. With a preallocated slab the rows are blitted in place and the
/// result is a view the caller frees; otherwise the cache is regrown by
/// concatenation and the result is the cache tensor itself (not freed).
/// `fresh` is never consumed: the caller still owns it.
const CachedRows = struct {
    tensor: CT,
    owned_view: bool,
};

fn appendSelfCache(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cache: *const DecodeCache,
    slot: *?CT,
    fresh: CT,
    start: usize,
    dec_seq: usize,
    d_model: usize,
) !CachedRows {
    const total = start + dec_seq;
    if (cache.preallocated()) {
        const slab = slot.* orelse return error.InvalidInputShape;
        if (total > cache.capacity) return error.SequenceTooLong;
        const copied = try cb.copyRows2D(allocator, slab, start, fresh, 0, dec_seq, d_model);
        if (!copied) return error.UnsupportedOperation;
        const view = try cb.sliceRows2D(allocator, slab, 0, total, d_model);
        return .{ .tensor = view, .owned_view = true };
    }
    const previous = slot.* orelse return error.InvalidInputShape;
    const grown = try cb.concatRows2D(allocator, previous, fresh, start, dec_seq, d_model);
    cb.free(previous);
    slot.* = grown;
    return .{ .tensor = grown, .owned_view = false };
}

const Projections = struct {
    q: CT,
    k: CT,
    v: CT,
};

fn projectQkv(cb: *const ComputeBackend, w: *const LayerWeights, normed: CT, rows: usize, d_model: usize, profile: *StepProfile) !Projections {
    if (w.qkv_slots) |slots| {
        if (try cb.decoderRuntimeApplyLinearQkv(&.{
            .q_slot = slots[0],
            .k_slot = slots[1],
            .v_slot = slots[2],
            .input = normed,
            .in_dim = d_model,
            .q_out_dim = d_model,
            .kv_out_dim = d_model,
        })) |triple| {
            profile.fused_qkv += 1;
            return .{ .q = triple.first, .k = triple.second, .v = triple.third };
        }
    }
    const q = try w.q.apply(cb, normed, rows, d_model, d_model);
    errdefer cb.free(q);
    const k = try w.k.apply(cb, normed, rows, d_model, d_model);
    errdefer cb.free(k);
    const v = try w.v.apply(cb, normed, rows, d_model, d_model);
    return .{ .q = q, .k = k, .v = v };
}

/// One decoder layer over the residual stream. On return the stream's
/// `sum` is the post-cross-attention residual and `pending` the FFN
/// output, so the next layer norm folds the final add into itself.
fn decoderBlockCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    stream: *ResidualStream,
    layer_cache: *DecodeCache.LayerCache,
    w: *const LayerWeights,
    cache: *const DecodeCache,
    dec_seq: usize,
    start: usize,
    profile: *StepProfile,
) !void {
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const total = start + dec_seq;

    // --- Causal self-attention against the cache ---
    const normed = try stream.normalize(cb, w.self_ln, d_model, profile);
    defer cb.free(normed);
    const hidden = stream.sum;
    profile.mark(cb, .self_norm);

    // With a resident slab and prepared slots, K and V are projected
    // straight into their cache rows (one fused dispatch for a single token,
    // two multi-row linears for the prompt block), so the append is free and
    // no blit splits the command sequence.
    var proj: Projections = undefined;
    var in_place = false;
    if (cache.preallocated() and w.qkv_slots != null and start + dec_seq <= cache.capacity) {
        const slots = w.qkv_slots.?;
        const k_dst = try cb.sliceRows2D(allocator, layer_cache.k_self.?, start, dec_seq, d_model);
        var dst_live = true;
        errdefer if (dst_live) cb.free(k_dst);
        const v_dst = try cb.sliceRows2D(allocator, layer_cache.v_self.?, start, dec_seq, d_model);
        errdefer if (dst_live) cb.free(v_dst);
        var q_in_place: ?CT = null;
        if (dec_seq == 1) {
            q_in_place = try cb.decoderRuntimeApplyLinearQkvInto(&.{
                .q_slot = slots[0],
                .k_slot = slots[1],
                .v_slot = slots[2],
                .input = normed,
                .in_dim = d_model,
                .q_out_dim = d_model,
                .kv_out_dim = d_model,
            }, k_dst, v_dst);
        } else {
            const k_ok = try cb.decoderRuntimeApplyLinearInto(&.{ .slot = slots[1], .input = normed, .in_dim = d_model, .out_dim = d_model }, k_dst);
            const v_ok = k_ok and try cb.decoderRuntimeApplyLinearInto(&.{ .slot = slots[2], .input = normed, .in_dim = d_model, .out_dim = d_model }, v_dst);
            if (v_ok) q_in_place = try w.q.apply(cb, normed, dec_seq, d_model, d_model);
        }
        if (q_in_place) |q| {
            proj = .{ .q = q, .k = k_dst, .v = v_dst };
            in_place = true;
            profile.fused_qkv += 1;
            profile.in_place_kv += 1;
        } else {
            cb.free(k_dst);
            cb.free(v_dst);
        }
        dst_live = false;
    }
    if (!in_place) proj = try projectQkv(cb, w, normed, dec_seq, d_model, profile);
    var kv_consumed = false;
    defer {
        cb.free(proj.q);
        if (!kv_consumed) {
            cb.free(proj.k);
            cb.free(proj.v);
        }
    }
    profile.mark(cb, .qkv);

    var self_attn: CT = undefined;
    if (start == 0 and !cache.preallocated()) {
        // First block without slabs: the projections become the cache.
        self_attn = try cb.causalSelfAttention(proj.q, proj.k, proj.v, null, 1, dec_seq, num_heads, head_dim);
        layer_cache.k_self = proj.k;
        layer_cache.v_self = proj.v;
        kv_consumed = true;
    } else {
        // Slabs take a copy of the fresh rows unless they were projected in
        // place; concatenation copies too. In every case the projections
        // are released with this block.
        const k_rows = if (in_place)
            CachedRows{ .tensor = try cb.sliceRows2D(allocator, layer_cache.k_self.?, 0, total, d_model), .owned_view = true }
        else
            try appendSelfCache(cb, allocator, cache, &layer_cache.k_self, proj.k, start, dec_seq, d_model);
        defer if (k_rows.owned_view) cb.free(k_rows.tensor);
        const v_rows = if (in_place)
            CachedRows{ .tensor = try cb.sliceRows2D(allocator, layer_cache.v_self.?, 0, total, d_model), .owned_view = true }
        else
            try appendSelfCache(cb, allocator, cache, &layer_cache.v_self, proj.v, start, dec_seq, d_model);
        defer if (v_rows.owned_view) cb.free(v_rows.tensor);
        profile.mark(cb, .kv_append);
        if (start == 0) {
            self_attn = try cb.causalSelfAttention(proj.q, proj.k, proj.v, null, 1, dec_seq, num_heads, head_dim);
        } else {
            // One new query over every cached key: causal by construction.
            self_attn = try cb.crossAttention(proj.q, k_rows.tensor, v_rows.tensor, cache.self_mask[0..total], 1, dec_seq, total, num_heads, head_dim);
        }
    }
    defer cb.free(self_attn);
    profile.mark(cb, .self_attn);

    const self_proj = try w.o.apply(cb, self_attn, dec_seq, d_model, d_model);
    defer cb.free(self_proj);
    // Residual add fused into the next layer norm.
    const after_self = try addNorm(cb, self_proj, hidden, w.cross_ln, d_model, profile);
    defer cb.free(after_self.sum);
    defer cb.free(after_self.normed);
    profile.mark(cb, .self_out);

    // --- Cross-attention against the cached encoder projections ---
    const Q_cross = try w.cross_q.apply(cb, after_self.normed, dec_seq, d_model, d_model);
    defer cb.free(Q_cross);
    profile.mark(cb, .cross_norm_q);
    const cross_attn = try cb.crossAttention(Q_cross, layer_cache.k_cross, layer_cache.v_cross, cache.encoder_mask, 1, dec_seq, cache.enc_seq, num_heads, head_dim);
    defer cb.free(cross_attn);
    profile.mark(cb, .cross_attn);
    const cross_proj = try w.cross_o.apply(cb, cross_attn, dec_seq, d_model, d_model);
    defer cb.free(cross_proj);
    const after_cross = try addNorm(cb, cross_proj, after_self.sum, w.ffn_ln, d_model, profile);
    var after_cross_sum_live = true;
    defer if (after_cross_sum_live) cb.free(after_cross.sum);
    defer cb.free(after_cross.normed);
    profile.mark(cb, .cross_out);

    // --- FFN ---
    const fc1_out = try w.fc1.apply(cb, after_cross.normed, dec_seq, d_model, ffn_dim);
    defer cb.free(fc1_out);
    profile.mark(cb, .fc1);
    const activated = try cb.gelu(fc1_out);
    defer cb.free(activated);
    profile.mark(cb, .gelu);
    const fc2_out = try w.fc2.apply(cb, activated, dec_seq, ffn_dim, d_model);
    profile.mark(cb, .fc2);

    // Hand the residual pair to the stream: the add happens inside the next
    // layer norm (or the final one).
    cb.free(stream.sum);
    stream.sum = after_cross.sum;
    after_cross_sum_live = false;
    stream.pending = fc2_out;
}
