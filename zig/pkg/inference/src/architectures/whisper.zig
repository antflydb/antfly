// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

    // CUDA's dense seq2seq contract requires a device layout transform.
    // Missing kernels/errors must fail rather than silently running transpose
    // on the CPU and reuploading an intermediate during encoder execution.
    if (cb.kind() == .cuda) return error.CudaResidentLayoutRequired;
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
    return encoderForwardOutput(false, cb, allocator, config, mel_features, batch, time_steps);
}

pub fn encoderForwardTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    mel_features: CT,
    batch: usize,
    time_steps: usize,
) !CT {
    return encoderForwardOutput(true, cb, allocator, config, mel_features, batch, time_steps);
}

fn EncoderOutput(comptime resident: bool) type {
    return if (resident) CT else []f32;
}

fn encoderForwardOutput(
    comptime resident: bool,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    mel_features: CT,
    batch: usize,
    time_steps: usize,
) !EncoderOutput(resident) {
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
    defer if (comptime !resident) cb.free(normed);
    errdefer if (comptime resident) cb.free(normed);
    profile.mark(cb, "final_norm");
    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }
    if (comptime resident) return normed;
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
    const availability = try cb.conv1dIm2colAvailability(&.{ .batch = batch, .channels = in_channels, .time = time_steps, .kernel = kernel_size, .stride = stride, .padding = padding, .time_major = time_major });
    const certified = availability orelse false;
    var selected_slot: ?usize = null;
    if (availability) |enabled| {
        if (!enabled) return null;
        selected_slot = (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = weight, .bias = bias, .in_dim = in_channels * kernel_size, .out_dim = out_channels })) orelse return null;
    }
    const cols = try cb.conv1dIm2col(input, batch, in_channels, time_steps, kernel_size, stride, padding, time_major) orelse {
        if (certified) return error.PreparedConvolutionCapabilityChanged;
        if (trace) std.debug.print("whisper_metal_conv: im2col unavailable in={d} k={d}\n", .{ in_channels, kernel_size });
        return null;
    };
    defer cb.free(cols);
    const in_dim = in_channels * kernel_size;
    // The conv weight `[out, in, kernel]` is already the row-major
    // `[out, in * kernel]` matrix the linear slot wants; the slot
    // preparation relabels it.
    const slot = if (selected_slot) |ready| ready else (try cb.decoderRuntimeEnsureLinearSlot(&.{ .weight = weight, .bias = bias, .in_dim = in_dim, .out_dim = out_channels })) orelse {
        if (trace) std.debug.print("whisper_metal_conv: no linear slot in={d} out={d}\n", .{ in_dim, out_channels });
        return null;
    };
    const out = try cb.decoderRuntimeApplyLinear(&.{ .slot = slot, .input = cols, .in_dim = in_dim, .out_dim = out_channels });
    if (out == null and certified) return error.PreparedConvolutionCapabilityChanged;
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
    assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule = null,
    frame_active: *bool,
    last_ns: u64 = 0,
    /// How many fused kernels the step used, to confirm the fast paths.
    fused_qkv: u32 = 0,
    fused_add_norm: u32 = 0,
    /// K/V projections written straight into the cache slabs (no copy).
    in_place_kv: u32 = 0,
    device_choice: bool = false,
    planned_scope: bool = false,
    totals: [std.enums.values(Bucket).len]u64 = @as([std.enums.values(Bucket).len]u64, @splat(0)),

    fn start(self: *StepProfile) void {
        if (self.enabled) self.last_ns = platform.time.monotonicNs();
    }

    fn mark(self: *StepProfile, cb: *const ComputeBackend, bucket: Bucket) void {
        if (!self.enabled) return;
        if (self.frame_active.*) cb.decoderRuntimeFlushActiveFrame() catch {};
        const now = platform.time.monotonicNs();
        self.totals[@backingInt(bucket)] += now -| self.last_ns;
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
pub const NativeColdWeights = struct {
    expected_data: *const anyopaque,
    domain: ?*const @import("../runtime/tier/admission_domain.zig").Pin,
};
const WeightFetcher = struct {
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    handles: std.ArrayListUnmanaged(CT) = .empty,
    /// Layer weight name prefix (`model.decoder.layers` or `model.encoder.layers`).
    prefix: []const u8 = "model.decoder.layers",
    cold: ?NativeColdWeights = null,

    fn prepareCold(self: *WeightFetcher, name: []const u8) !void {
        const source = self.cold orelse return;
        const Native = @import("../ops/native_compute.zig").NativeCompute;
        if (source.domain) |pin| {
            if (try Native.prepareTypedWeightOutsideExecution(self.cb, source.expected_data, name, pin)) return;
        }
        const query = Native.authoredResidentSourceQuery(self.cb, name) catch |err| switch (err) {
            error.WeightNotFound => return, // original required/optional lookup owns this distinction
            error.UnsupportedAuthoredWeight => return error.UnsupportedDenseSchedule,
            else => return err,
        };
        if (query.shape) |shape| if (shape.dtype != .f32 or query.quantized != null) return error.UnsupportedDenseSchedule;
    }

    fn fetch(self: *WeightFetcher, name: []const u8) !CT {
        try self.prepareCold(name);
        const tensor = try self.cb.getWeight(name);
        errdefer self.cb.free(tensor);
        try self.handles.append(self.allocator, tensor);
        return tensor;
    }

    fn fetchOptional(self: *WeightFetcher, name: []const u8) !?CT {
        try self.prepareCold(name);
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
/// Canonical allocation geometry shared by cache construction and admission.
/// KV values are F32 on every current backend; opaque source preparation keeps
/// the existing model/static workspace floor until its selected-route bound is
/// available. No private cache is mislabeled as a published output tensor.
pub const CacheGeometry = struct {
    layers: usize,
    width: usize,
    encoder_rows: usize,
    capacity: usize,
    encoder_mask_elements: usize,
    self_mask_elements: usize,
    cross_tensor_bytes: usize,
    self_tensor_bytes: usize,
    known_metadata_bytes: usize,

    pub fn init(config: Config, encoder_rows: usize) !CacheGeometry {
        const mul = cacheMul;
        const add = cacheAdd;
        const width: usize = config.d_model;
        const capacity = @max(@as(usize, 1), config.max_target_positions);
        if (encoder_rows == 0 or width == 0) return error.InvalidInputShape;
        const cross_bytes = try mul(usize, try mul(usize, encoder_rows, width), @sizeOf(f32));
        const self_bytes = try mul(usize, try mul(usize, capacity, width), @sizeOf(f32));
        const masks = try mul(usize, try add(usize, encoder_rows, capacity), @sizeOf(i64));
        const vectors = try mul(usize, config.decoder_layers, @sizeOf(DecodeCache.LayerCache) + @sizeOf(LayerWeights));
        return .{ .layers = config.decoder_layers, .width = width, .encoder_rows = encoder_rows, .capacity = capacity, .encoder_mask_elements = encoder_rows, .self_mask_elements = capacity, .cross_tensor_bytes = cross_bytes, .self_tensor_bytes = self_bytes, .known_metadata_bytes = try add(usize, masks, vectors) };
    }

    pub fn cacheAmounts(self: CacheGeometry, cb: *const ComputeBackend) !@import("../runtime/tier/memory.zig").AdmissionAmounts {
        if (self.layers == 0) return .{ .host_scratch_bytes = self.known_metadata_bytes };
        const functions = cb.vtable.kernels;
        const cross = if (functions) |table| if (table.physical_storage_policy) |_| try ops.physical_storage.reservationBytes(cb.*, .{ .byte_size = self.cross_tensor_bytes }) else self.cross_tensor_bytes else self.cross_tensor_bytes;
        const slab = if (functions) |table| if (table.physical_storage_policy) |_| try ops.physical_storage.reservationBytes(cb.*, .{ .byte_size = self.self_tensor_bytes }) else self.self_tensor_bytes else self.self_tensor_bytes;
        if (cross < self.cross_tensor_bytes or slab < self.self_tensor_bytes) return error.InvalidPhysicalStorageReservation;
        const bytes = try cacheMul(usize, try cacheMul(usize, self.layers, 2), try cacheAdd(usize, cross, slab));
        var result = @import("../runtime/tier/memory.zig").AdmissionAmounts{ .host_scratch_bytes = self.known_metadata_bytes };
        switch (cb.kind()) {
            .native => result.host_kv_bytes = bytes,
            .metal, .cuda => result.backend_kv_bytes = bytes,
            .pjrt, .wasm, .webgpu, .graph => return error.UnsupportedIncrementalDecoderBackend,
        }
        return result;
    }

    pub fn workspaceBytes(self: CacheGeometry, config: Config) !usize {
        const mul = cacheMul;
        const add = cacheAdd;
        const keys = @max(self.encoder_rows, self.capacity);
        const attention = try mul(usize, try mul(usize, keys, config.decoder_attention_heads), 3);
        const activations = try add(usize, try mul(usize, self.width, 6), try mul(usize, config.decoder_ffn_dim, 3));
        const context = try mul(usize, try mul(usize, keys, self.width), 6);
        const constructor = try mul(usize, try mul(usize, self.encoder_rows, self.width), 6);
        const head = try add(usize, self.width, config.vocab_size);
        return mul(usize, @max(constructor, @max(head, @max(attention, @max(activations, context)))), @sizeOf(f32));
    }
};

fn cacheMul(comptime T: type, left: T, right: T) !T {
    return std.math.mul(T, left, right) catch error.ResourceLimitExceeded;
}
fn cacheAdd(comptime T: type, left: T, right: T) !T {
    return std.math.add(T, left, right) catch error.ResourceLimitExceeded;
}

/// Cold retained weight inputs for one request. No encoder projection or KV
/// mutation happens here; the same actual backend must consume this owner.
pub const PreparedDecoderWeights = struct {
    allocator: std.mem.Allocator,
    backend: ComputeBackend,
    weights: DecoderWeights,
    fetcher: WeightFetcher,
    cross: ?[]Cross,
    // Captured by the original lookup, never inferred from a later witness.
    lm_head_source: enum { projection, tied_embedding } = .projection,

    pub const Cross = struct { k: Linear, v: Linear };

    pub fn init(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, resolve_cross: bool) !PreparedDecoderWeights {
        return initWithSource(cb, allocator, config, resolve_cross, null);
    }
    pub fn initWithSource(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, resolve_cross: bool, cold: ?NativeColdWeights) !PreparedDecoderWeights {
        const d_model = config.d_model;
        var fetcher = WeightFetcher{ .cb = cb, .allocator = allocator, .cold = cold };
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
        var lm_head_source: @FieldType(PreparedDecoderWeights, "lm_head_source") = .projection;
        const weights = DecoderWeights{
            .embed = embed,
            .positions = try fetcher.fetch("model.decoder.embed_positions.weight"),
            .final_ln = .{
                .w = try fetcher.fetch("model.decoder.layer_norm.weight"),
                .b = try fetcher.fetch("model.decoder.layer_norm.bias"),
            },
            .lm_head = fetcher.fetch("proj_out.weight") catch |err| switch (err) {
                error.MissingWeight, error.WeightNotFound => tied: {
                    lm_head_source = .tied_embedding;
                    break :tied embed;
                },
                else => return err,
            },
            .layers = layer_weights,
        };

        const cross = if (resolve_cross) try allocator.alloc(Cross, config.decoder_layers) else null;
        errdefer if (cross) |values| allocator.free(values);
        if (cross) |values| {
            for (values, 0..) |*entry, layer| entry.* = .{
                .k = try fetcher.linear(layer, "encoder_attn.k_proj", &buf),
                .v = try fetcher.linear(layer, "encoder_attn.v_proj", &buf),
            };
        }
        fetcher.cb = undefined;
        fetcher.cold = null;
        return .{ .allocator = allocator, .backend = cb.withoutSynchronousBorrows(), .weights = weights, .fetcher = fetcher, .cross = cross, .lm_head_source = lm_head_source };
    }
    pub fn deinit(self: *PreparedDecoderWeights) void {
        self.fetcher.release(&self.backend);
        self.allocator.free(self.weights.layers);
        if (self.cross) |values| self.allocator.free(values);
        self.* = undefined;
    }
};

/// Native-only request schedule. External self slabs are real owned cache CTs;
/// cross/self state remains ordinary cache-owned KV storage. All four slabs
/// are allocated under the existing KV permit; original Into projections fill
/// cross state only after the receiver enters its original gate.
pub const PreparedSingleToken = struct {
    allocator: std.mem.Allocator,
    backend: ComputeBackend,
    schedule: *@import("../execution/dense_decoder_schedule.zig").Schedule,
    source_data: *const anyopaque,
    initial: ?Initial = null,
    self_slabs: []Slab,
    token_node: @import("ml").graph.NodeId,
    position_node: @import("ml").graph.NodeId,
    token: [1]i64 = .{0},
    position: [1]i64 = .{0},
    token_rows: usize,
    position_rows: usize,
    adopted: bool = false,
    cache_layers: []DecodeCache.LayerCache,
    encoder_mask: []i64,
    self_mask: []i64,

    const schedule_mod = @import("../execution/dense_decoder_schedule.zig");
    const NodeId = @import("ml").graph.NodeId;
    const Slab = struct { k: CT, v: CT, cross_k: CT, cross_v: CT };
    const Initial = struct {
        schedule: *schedule_mod.Schedule,
        token_node: NodeId,
        position_node: NodeId,
        tokens: []i64,
        positions: []i64,
    };
    const Authored = struct { schedule: *schedule_mod.Schedule, token_node: NodeId, position_node: NodeId };
    pub const SpecSource = struct {
        cb: *const ComputeBackend,
        data: *const anyopaque,
        config: Config,
        rows: usize,
        weights: *const PreparedDecoderWeights,
        query_rows: usize,
        initial: bool,
        parameter_count: usize,
        const Role = schedule_mod.RoleKey;
        const Kind = @FieldType(Role, "kind");
        const Resolved = struct {
            source_kind: @FieldType(schedule_mod.ParameterSpec, "source_kind"),
            shape: ?@import("ml").graph.Shape = null,
            source_name: []const u8 = "",
            value: ?CT = null,
        };
        const linear_kinds = [_]Kind{ .self_q, .self_k, .self_v, .self_o, .cross_q, .cross_o, .fc1, .fc2 };
        fn linearAt(layer: LayerWeights, kind: Kind) Linear {
            return switch (kind) {
                .self_q => layer.q,
                .self_k => layer.k,
                .self_v => layer.v,
                .self_o => layer.o,
                .cross_q => layer.cross_q,
                .cross_o => layer.cross_o,
                .fc1 => layer.fc1,
                .fc2 => layer.fc2,
                else => unreachable,
            };
        }
        fn layerCount(layer: LayerWeights, initial: bool) usize {
            var count: usize = 6 + linear_kinds.len + (if (initial) @as(usize, 2) else 4);
            for (linear_kinds) |kind| if (linearAt(layer, kind).b != null) {
                count += 1;
            };
            return count;
        }
        pub fn header(self: @This()) schedule_mod.ProgramSpec {
            return .{ .family = .whisper, .phase = if (self.initial) .initial else .decode, .geometry = .{ .layers = self.config.decoder_layers, .width = self.config.d_model, .heads = self.config.decoder_attention_heads, .ffn_dim = self.config.decoder_ffn_dim, .vocab_size = self.config.vocab_size, .encoder_rows = self.rows, .capacity = @max(@as(usize, 1), self.config.max_target_positions), .query_rows = self.query_rows }, .semantics = .{ .epsilon = 1e-5, .activation = .gelu, .norm_order = .pre, .position_offset = 0, .global_final_norm = true }, .parameters = &.{} };
        }
        pub fn len(self: @This()) usize {
            return self.parameter_count;
        }
        fn roleAt(self: @This(), index: usize) !Role {
            var slot = index;
            const globals = [_]Role{ .{ .kind = .token_ids }, .{ .kind = .position_ids }, .{ .kind = .embedding, .part = .weight }, .{ .kind = .positions, .part = .weight } };
            if (slot < globals.len) return globals[slot];
            slot -= globals.len;
            for (self.weights.weights.layers, 0..) |layer, layer_index| {
                const count = layerCount(layer, self.initial);
                if (slot >= count) {
                    slot -= count;
                    continue;
                }
                const groups = [_]Kind{ .self_norm, .self_q, .self_k, .self_v, .self_o, .cross_norm, .cross_q, .cross_o, .ffn_norm, .fc1, .fc2 };
                for (groups) |kind| {
                    const has_bias = switch (kind) {
                        .self_norm, .cross_norm, .ffn_norm => true,
                        else => linearAt(layer, kind).b != null,
                    };
                    if (slot == 0) return .{ .kind = kind, .layer = @intCast(layer_index), .part = .weight };
                    slot -= 1;
                    if (has_bias) {
                        if (slot == 0) return .{ .kind = kind, .layer = @intCast(layer_index), .part = .bias };
                        slot -= 1;
                    }
                }
                const slabs = if (self.initial) &[_]Kind{ .cross_slab_k, .cross_slab_v } else &[_]Kind{ .self_slab_k, .self_slab_v, .cross_slab_k, .cross_slab_v };
                if (slot < slabs.len) return .{ .kind = slabs[slot], .layer = @intCast(layer_index) };
                return error.DenseDefinitionChanged;
            }
            const tail = [_]Role{ .{ .kind = .final_norm, .part = .weight }, .{ .kind = .final_norm, .part = .bias }, .{ .kind = .lm_head, .part = .weight } };
            return if (slot < tail.len) tail[slot] else error.DenseDefinitionChanged;
        }
        fn resolve(self: @This(), role: Role, buffer: *[256]u8) !Resolved {
            const Shape = @import("ml").graph.Shape;
            const g = self.header().geometry;
            if (role.kind == .token_ids or role.kind == .position_ids) {
                if (role.layer != null or role.part != .value) return error.DenseDefinitionChanged;
                return .{ .shape = Shape.init(.i64, &.{@intCast(g.query_rows)}), .source_kind = .host };
            }
            if (role.kind == .self_slab_k or role.kind == .self_slab_v or role.kind == .cross_slab_k or role.kind == .cross_slab_v) {
                const layer = role.layer orelse return error.DenseDefinitionChanged;
                if (layer >= g.layers or role.part != .value or (self.initial and (role.kind == .self_slab_k or role.kind == .self_slab_v))) return error.DenseDefinitionChanged;
                const rows = if (role.kind == .self_slab_k or role.kind == .self_slab_v) g.capacity else g.encoder_rows;
                return .{ .shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(g.width) }), .source_kind = .external };
            }
            var name: []const u8 = undefined;
            var value: CT = undefined;
            if (role.layer) |layer| {
                if (layer >= self.weights.weights.layers.len or role.part == .value) return error.DenseDefinitionChanged;
                const w = self.weights.weights.layers[layer];
                const pair = switch (role.kind) {
                    .self_norm => Linear{ .w = w.self_ln.w, .b = w.self_ln.b },
                    .cross_norm => Linear{ .w = w.cross_ln.w, .b = w.cross_ln.b },
                    .ffn_norm => Linear{ .w = w.ffn_ln.w, .b = w.ffn_ln.b },
                    .self_q, .self_k, .self_v, .self_o, .cross_q, .cross_o, .fc1, .fc2 => linearAt(w, role.kind),
                    else => return error.DenseDefinitionChanged,
                };
                value = if (role.part == .weight) pair.w else pair.b orelse return error.DenseDefinitionChanged;
                const suffix = switch (role.kind) {
                    .self_norm => "self_attn_layer_norm",
                    .cross_norm => "encoder_attn_layer_norm",
                    .ffn_norm => "final_layer_norm",
                    .self_q => "self_attn.q_proj",
                    .self_k => "self_attn.k_proj",
                    .self_v => "self_attn.v_proj",
                    .self_o => "self_attn.out_proj",
                    .cross_q => "encoder_attn.q_proj",
                    .cross_o => "encoder_attn.out_proj",
                    .fc1 => "fc1",
                    .fc2 => "fc2",
                    else => unreachable,
                };
                name = try std.fmt.bufPrint(buffer, "model.decoder.layers.{d}.{s}.{s}", .{ layer, suffix, if (role.part == .weight) "weight" else "bias" });
            } else switch (role.kind) {
                .embedding => {
                    if (role.part != .weight) return error.DenseDefinitionChanged;
                    value = self.weights.weights.embed;
                    name = "model.decoder.embed_tokens.weight";
                },
                .positions => {
                    if (role.part != .weight) return error.DenseDefinitionChanged;
                    value = self.weights.weights.positions;
                    name = "model.decoder.embed_positions.weight";
                },
                .lm_head => {
                    if (role.part != .weight) return error.DenseDefinitionChanged;
                    value = self.weights.weights.lm_head;
                    name = switch (self.weights.lm_head_source) {
                        .projection => "proj_out.weight",
                        .tied_embedding => "model.decoder.embed_tokens.weight",
                    };
                },
                .final_norm => {
                    if (role.part == .value) return error.DenseDefinitionChanged;
                    value = if (role.part == .weight) self.weights.weights.final_ln.w else self.weights.weights.final_ln.b;
                    name = if (role.part == .weight) "model.decoder.layer_norm.weight" else "model.decoder.layer_norm.bias";
                },
                else => return error.DenseDefinitionChanged,
            }
            return .{ .source_kind = .held_weight, .source_name = name, .value = value };
        }
        /// Pure checkpoint-role name sizing. No Native/source query or
        /// allocation: current held shape/stamp/name validation stays in at().
        pub fn expectedNameBytes(self: @This()) !usize {
            var bytes: usize = 0;
            var buffer: [256]u8 = undefined;
            for (0..self.len()) |index| {
                const resolved = try self.resolve(try self.roleAt(index), &buffer);
                bytes = try std.math.add(usize, bytes, resolved.source_name.len);
            }
            return bytes;
        }
        pub fn at(self: @This(), index: usize, buffer: *[256]u8) !schedule_mod.ParameterSpec {
            const role = try self.roleAt(index);
            const resolved = try self.resolve(role, buffer);
            const shape = if (resolved.source_kind == .held_weight)
                try @import("../ops/native_compute.zig").NativeCompute.heldF32WeightShapeNamed(self.cb, self.data, resolved.value.?, resolved.source_name)
            else
                resolved.shape.?;
            if ((shape.numElements() orelse return error.UnsupportedDenseSchedule) <= 0) return error.UnsupportedDenseSchedule;
            return .{ .role = role, .shape = shape, .source_kind = resolved.source_kind, .source_name = resolved.source_name };
        }
        fn bind(self: @This(), a: std.mem.Allocator, spec: *const schedule_mod.ProgramSpec, slabs: []const Slab) ![]schedule_mod.BoundParameter {
            const expected_header = self.header();
            if (spec.family != expected_header.family or spec.phase != expected_header.phase or !std.meta.eql(spec.geometry, expected_header.geometry) or !std.meta.eql(spec.semantics, expected_header.semantics) or spec.parameters.len != self.len() or slabs.len != self.config.decoder_layers) return error.DenseDefinitionChanged;
            const result = try a.alloc(schedule_mod.BoundParameter, spec.parameters.len);
            errdefer a.free(result);
            for (spec.parameters, result, 0..) |declared, *bound, index| {
                if (!std.meta.eql(declared.role, try self.roleAt(index))) return error.DenseDefinitionChanged;
                var name: [256]u8 = undefined;
                const resolved = try self.resolve(declared.role, &name);
                if (declared.source_kind != resolved.source_kind or !std.mem.eql(u8, declared.source_name, resolved.source_name)) return error.DenseDefinitionChanged;
                if (resolved.shape) |shape| if (!declared.shape.eq(shape)) return error.DenseDefinitionChanged;
                bound.* = .{ .role = declared.role, .value = resolved.value, .source_data = if (declared.source_kind == .held_weight) self.data else null };
                if (declared.source_kind == .external) {
                    const slab = slabs[declared.role.layer.?];
                    const value = switch (declared.role.kind) {
                        .self_slab_k => slab.k,
                        .self_slab_v => slab.v,
                        .cross_slab_k => slab.cross_k,
                        .cross_slab_v => slab.cross_v,
                        else => return error.DenseDefinitionChanged,
                    };
                    bound.value = value;
                }
            }
            return result;
        }
    };
    pub fn specSource(cb: *const ComputeBackend, data: *const anyopaque, config: Config, rows: usize, weights: *const PreparedDecoderWeights, query_rows: usize, initial: bool) !?SpecSource {
        const Native = @import("../ops/native_compute.zig");
        if (cb.vtable != &Native.vtable_impl or cb.vtable.addLayerNormSum != null or weights.cross == null or weights.weights.layers.len != config.decoder_layers or weights.cross.?.len != config.decoder_layers) return null;
        for (weights.weights.layers) |w| if (w.qkv_slots != null) return null;
        const embed = try Native.NativeCompute.heldF32WeightPrimaryNamed(cb, data, weights.weights.embed, "model.decoder.embed_tokens.weight");
        const position = try Native.NativeCompute.heldF32WeightPrimaryNamed(cb, data, weights.weights.positions, "model.decoder.embed_positions.weight");
        if (embed.descriptor.shape.rank() != 2 or position.descriptor.shape.rank() != 2 or embed.descriptor.shape.dims[1] != config.d_model or position.descriptor.shape.dims[1] != config.d_model or embed.descriptor.shape.dims[0] <= 0 or position.descriptor.shape.dims[0] <= 0) return null;
        const geometry = try CacheGeometry.init(config, rows);
        if (config.decoder_attention_heads == 0 or config.d_model % config.decoder_attention_heads != 0 or rows > std.math.maxInt(i32) or geometry.capacity > std.math.maxInt(i32) or config.d_model > std.math.maxInt(i32) or config.decoder_ffn_dim > std.math.maxInt(i32) or config.vocab_size > std.math.maxInt(i32)) return null;
        if (query_rows == 0 or (!initial and query_rows != 1) or (initial and (query_rows > config.max_target_positions or query_rows > 2048))) return error.UnsupportedDenseSchedule;
        var count: usize = 7;
        for (weights.weights.layers) |layer| count = try std.math.add(usize, count, SpecSource.layerCount(layer, initial));
        return .{ .cb = cb, .data = data, .config = config, .rows = rows, .weights = weights, .query_rows = query_rows, .initial = initial, .parameter_count = count };
    }

    pub fn init(cb: *const ComputeBackend, source_data: *const anyopaque, a: std.mem.Allocator, config: Config, encoder: CT, rows: usize, weights: *const PreparedDecoderWeights, preparation: ?*schedule_mod.Preparation, spec: *const schedule_mod.ProgramSpec, storage: ?*schedule_mod.ProgramSpecStorage) !?PreparedSingleToken {
        const Native = @import("../ops/native_compute.zig");
        if (cb.vtable != &Native.vtable_impl or cb.vtable.addLayerNormSum != null or weights.cross == null or weights.weights.layers.len != config.decoder_layers or weights.cross.?.len != config.decoder_layers) return null;
        for (weights.weights.layers) |w| if (w.qkv_slots != null) return null;
        const embed_shape = (try Native.NativeCompute.heldF32WeightPrimary(cb, source_data, weights.weights.embed)).descriptor.shape;
        const position_shape = (try Native.NativeCompute.heldF32WeightPrimary(cb, source_data, weights.weights.positions)).descriptor.shape;
        if (embed_shape.rank() != 2 or position_shape.rank() != 2 or embed_shape.dims[1] != config.d_model or position_shape.dims[1] != config.d_model) return null;
        const geometry = try CacheGeometry.init(config, rows);
        if (config.decoder_attention_heads == 0 or config.d_model % config.decoder_attention_heads != 0 or rows > std.math.maxInt(i32) or geometry.capacity > std.math.maxInt(i32) or config.d_model > std.math.maxInt(i32) or config.decoder_ffn_dim > std.math.maxInt(i32) or config.vocab_size > std.math.maxInt(i32) or embed_shape.dims[0] <= 0 or position_shape.dims[0] <= 0) return null;
        if (spec.phase != .decode or spec.geometry.query_rows != 1) return error.DenseDefinitionChanged;
        const slabs = try a.alloc(Slab, config.decoder_layers);
        var built: usize = 0;
        errdefer {
            for (slabs[0..built]) |slab| {
                cb.free(slab.k);
                cb.free(slab.v);
                cb.free(slab.cross_k);
                cb.free(slab.cross_v);
            }
            a.free(slabs);
        }
        const shape = [_]i32{ @intCast(geometry.capacity), @intCast(config.d_model) };
        for (slabs) |*slab| {
            const k = (try cb.allocUninitF32Shape(&shape)) orelse return error.UnsupportedDenseSchedule;
            errdefer cb.free(k);
            const v = (try cb.allocUninitF32Shape(&shape)) orelse return error.UnsupportedDenseSchedule;
            errdefer cb.free(v);
            const cross_shape = [_]i32{ @intCast(rows), @intCast(config.d_model) };
            const cross_k = (try cb.allocUninitF32Shape(&cross_shape)) orelse return error.UnsupportedDenseSchedule;
            errdefer cb.free(cross_k);
            const cross_v = (try cb.allocUninitF32Shape(&cross_shape)) orelse return error.UnsupportedDenseSchedule;
            slab.* = .{ .k = k, .v = v, .cross_k = cross_k, .cross_v = cross_v };
            built += 1;
        }
        const authored = try authorPhase(cb, source_data, a, config, encoder, rows, weights, slabs, preparation, spec, storage);
        errdefer authored.schedule.deinit(.{ .outside_execution = true });
        const cache_layers = try a.alloc(DecodeCache.LayerCache, config.decoder_layers);
        errdefer a.free(cache_layers);
        const encoder_mask = try a.alloc(i64, geometry.encoder_mask_elements);
        errdefer a.free(encoder_mask);
        @memset(encoder_mask, 1);
        const self_mask = try a.alloc(i64, geometry.self_mask_elements);
        errdefer a.free(self_mask);
        @memset(self_mask, 1);
        return .{ .allocator = a, .backend = cb.withoutSynchronousBorrows(), .schedule = authored.schedule, .source_data = source_data, .self_slabs = slabs, .token_node = authored.token_node, .position_node = authored.position_node, .cache_layers = cache_layers, .encoder_mask = encoder_mask, .self_mask = self_mask, .token_rows = @intCast(embed_shape.dims[0]), .position_rows = @intCast(position_shape.dims[0]) };
    }
    fn authorPhase(cb: *const ComputeBackend, source_data: *const anyopaque, a: std.mem.Allocator, config: Config, encoder: CT, rows: usize, weights: *const PreparedDecoderWeights, slabs: []const Slab, preparation: ?*schedule_mod.Preparation, spec: *const schedule_mod.ProgramSpec, storage: ?*schedule_mod.ProgramSpecStorage) !Authored {
        const Native = @import("../ops/native_compute.zig");
        const source = (try specSource(cb, source_data, config, rows, weights, spec.geometry.query_rows, spec.phase == .initial)) orelse return error.UnsupportedDenseSchedule;
        // Cross-cache production is outside this graph. Keep its actual named
        // source and original destination/geometry preflight before any write.
        for (weights.cross.?, slabs, 0..) |cross, slab, layer| {
            var buffer: [256]u8 = undefined;
            for ([_]Linear{ cross.k, cross.v }, [_][]const u8{ "k_proj", "v_proj" }) |pair, projection| {
                _ = try Native.NativeCompute.heldF32WeightPrimaryNamed(cb, source_data, pair.w, try std.fmt.bufPrint(&buffer, "model.decoder.layers.{d}.encoder_attn.{s}.weight", .{ layer, projection }));
                if (pair.b) |bias| _ = try Native.NativeCompute.heldF32WeightPrimaryNamed(cb, source_data, bias, try std.fmt.bufPrint(&buffer, "model.decoder.layers.{d}.encoder_attn.{s}.bias", .{ layer, projection }));
            }
            const k_desc = try schedule_mod.projectionDescriptor(cb, source_data, encoder, cross.k.w, cross.k.b, slab.cross_k, rows, config.d_model, config.d_model);
            const v_desc = try schedule_mod.projectionDescriptor(cb, source_data, encoder, cross.v.w, cross.v.b, slab.cross_v, rows, config.d_model, config.d_model);
            if (!Native.NativeCompute.canApplyDenseProjectionInto(cb, &k_desc, if (cross.k.b) |bias| &.{ encoder, cross.k.w, bias } else &.{ encoder, cross.k.w }, slab.cross_k) or !Native.NativeCompute.canApplyDenseProjectionInto(cb, &v_desc, if (cross.v.b) |bias| &.{ encoder, cross.v.w, bias } else &.{ encoder, cross.v.w }, slab.cross_v)) return error.UnsupportedDenseSchedule;
        }
        const bound = try source.bind(a, spec, slabs);
        defer a.free(bound);
        if (preparation) |prepared| if (prepared.cached) |definition| {
            const schedule = try schedule_mod.Schedule.instantiate(a, cb, definition, bound);
            errdefer schedule.deinit(.{ .outside_execution = true });
            return .{ .schedule = schedule, .token_node = try definition.parameterNode(.{ .kind = .token_ids }), .position_node = try definition.parameterNode(.{ .kind = .position_ids }) };
        };
        return authorPure(cb, a, preparation, spec, storage, bound);
    }
    fn authorPure(cb: *const ComputeBackend, a: std.mem.Allocator, preparation: ?*schedule_mod.Preparation, spec: *const schedule_mod.ProgramSpec, storage: ?*schedule_mod.ProgramSpecStorage, bound: []const schedule_mod.BoundParameter) !Authored {
        if (spec.family != .whisper or spec.semantics.activation != .gelu or spec.semantics.norm_order != .pre or spec.semantics.epsilon != 1e-5 or spec.semantics.position_offset != 0 or !spec.semantics.global_final_norm) return error.DenseDefinitionChanged;
        var b = if (storage) |owned| schedule_mod.Builder.initSpecStorage(a, cb, preparation, owned, bound) else schedule_mod.Builder.initSpec(a, cb, preparation, spec, bound);
        defer b.deinit();
        const nodes = try schedule_mod.authorSemanticGraph(&b);
        return .{ .schedule = try schedule_mod.Schedule.create(&b), .token_node = nodes.token_node, .position_node = nodes.position_node };
    }
    pub fn initInitial(self: *PreparedSingleToken, config: Config, encoder: CT, rows: usize, weights: *const PreparedDecoderWeights, preparation: ?*schedule_mod.Preparation, query_rows: usize, spec: *const schedule_mod.ProgramSpec, storage: ?*schedule_mod.ProgramSpecStorage) !void {
        if (self.initial != null or query_rows == 0 or query_rows > config.max_target_positions or query_rows > 2048) return error.UnsupportedDenseSchedule;
        if (spec.phase != .initial or spec.geometry.query_rows != query_rows) return error.DenseDefinitionChanged;
        const tokens = try self.allocator.alloc(i64, query_rows);
        errdefer self.allocator.free(tokens);
        const positions = try self.allocator.alloc(i64, query_rows);
        errdefer self.allocator.free(positions);
        @memset(tokens, 0);
        for (positions, 0..) |*position, i| position.* = @intCast(i);
        const phase = try authorPhase(&self.backend, self.source_data, self.allocator, config, encoder, rows, weights, self.self_slabs, preparation, spec, storage);
        self.initial = .{ .schedule = phase.schedule, .token_node = phase.token_node, .position_node = phase.position_node, .tokens = tokens, .positions = positions };
    }
    fn bindInitial(self: *PreparedSingleToken, capability: ?@import("../execution/storage_admission.zig").Capability, current: @import("../execution/cleanup_context.zig").Context, enter: bool) !void {
        const phase = if (self.initial) |*value| value else return error.UnsupportedDenseSchedule;
        const Tensor = @import("../backends/tensor.zig").Tensor;
        const shape = [_]i64{@intCast(phase.tokens.len)};
        var tokens = Tensor{ .allocator = self.allocator, .name = "decoder.initial.tokens", .dtype = .i64, .shape = &shape, .data = std.mem.sliceAsBytes(phase.tokens), .owns_data = false, .owns_shape = false };
        var positions = Tensor{ .allocator = self.allocator, .name = "decoder.initial.positions", .dtype = .i64, .shape = &shape, .data = std.mem.sliceAsBytes(phase.positions), .owns_data = false, .owns_shape = false };
        const inputs = [_]@import("../execution/host_ingress.zig").Input{ .{ .node_id = phase.token_node, .value = &tokens }, .{ .node_id = phase.position_node, .value = &positions } };
        if (enter) try phase.schedule.enter(capability, current, &inputs) else {
            const frame = phase.schedule.frame orelse return error.DenseScheduleNotEntered;
            for (try frame.rebindNativeProgramHostInputs(&inputs)) |input| phase.schedule.values[input.node_id] = input.value;
        }
    }
    fn beginInitial(self: *PreparedSingleToken, tokens: []const i64) !?*schedule_mod.Schedule {
        const phase = if (self.initial) |*value| value else return null;
        // A truthful declaration is a routing hint, never permission to change
        // token shape or replay numerical work. Mismatches keep ordinary math.
        if (phase.tokens.len != tokens.len) return null;
        for (tokens) |token| if (token < 0 or @as(u64, @intCast(token)) >= self.token_rows) return error.InvalidTensorShape;
        if (tokens.len > self.position_rows) return error.InvalidTensorShape;
        @memcpy(phase.tokens, tokens);
        try self.bindInitial(null, .{}, false);
        phase.schedule.cursor = 0;
        return phase.schedule;
    }
    pub fn beginExecution(self: *PreparedSingleToken, current: @import("../execution/cleanup_context.zig").Context) !void {
        try self.schedule.beginExecution(current);
        if (self.initial) |phase| try phase.schedule.beginExecution(current);
    }
    fn bindInputs(self: *PreparedSingleToken, capability: ?@import("../execution/storage_admission.zig").Capability, current: @import("../execution/cleanup_context.zig").Context, enter: bool) !void {
        const Tensor = @import("../backends/tensor.zig").Tensor;
        var token = Tensor{ .allocator = self.allocator, .name = "decoder.token", .dtype = .i64, .shape = &.{1}, .data = std.mem.asBytes(&self.token), .owns_data = false, .owns_shape = false };
        var position = Tensor{ .allocator = self.allocator, .name = "decoder.position", .dtype = .i64, .shape = &.{1}, .data = std.mem.asBytes(&self.position), .owns_data = false, .owns_shape = false };
        const inputs = [_]@import("../execution/host_ingress.zig").Input{ .{ .node_id = self.token_node, .value = &token }, .{ .node_id = self.position_node, .value = &position } };
        if (enter) try self.schedule.enter(capability, current, &inputs) else {
            const frame = self.schedule.frame orelse return error.DenseScheduleNotEntered;
            const bound = try frame.rebindNativeProgramHostInputs(&inputs);
            for (bound) |input| self.schedule.values[input.node_id] = input.value;
        }
    }
    pub fn prepareInto(self: *PreparedSingleToken, capability: ?@import("../execution/storage_admission.zig").Capability, current: @import("../execution/cleanup_context.zig").Context) !void {
        try self.bindInputs(capability, current, true);
        if (self.initial != null) try self.bindInitial(capability, current, true);
    }
    fn beginStep(self: *PreparedSingleToken, token: i64, position: usize) !void {
        // Generic gather permits normalized negative indices; original embedding
        // lookup does not. Preserve its exact refusal before changing ingress.
        if (token < 0 or @as(u64, @intCast(token)) >= self.token_rows or position >= self.position_rows) return error.InvalidTensorShape;
        self.token[0] = token;
        self.position[0] = std.math.cast(i64, position) orelse return error.SequenceTooLong;
        try self.bindInputs(null, .{}, false);
        self.schedule.cursor = 0;
    }
    pub fn deinit(self: *PreparedSingleToken, current: @import("../execution/cleanup_context.zig").Context) void {
        if (self.initial) |phase| {
            phase.schedule.deinit(current);
            self.allocator.free(phase.tokens);
            self.allocator.free(phase.positions);
        }
        self.schedule.deinit(current);
        if (!self.adopted) {
            for (self.self_slabs) |slab| {
                self.backend.free(slab.k);
                self.backend.free(slab.v);
                self.backend.free(slab.cross_k);
                self.backend.free(slab.cross_v);
            }
            self.allocator.free(self.cache_layers);
            self.allocator.free(self.encoder_mask);
            self.allocator.free(self.self_mask);
        }
        self.allocator.free(self.self_slabs);
        self.* = undefined;
    }
};

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
    native_program: ?PreparedSingleToken = null,

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
        _ = try CacheGeometry.init(config, enc_seq);
        var prepared = try PreparedDecoderWeights.init(cb, allocator, config, false);
        defer prepared.deinit();
        return initPrepared(cb, allocator, config, encoder_hidden, enc_seq, &prepared, null);
    }

    /// Consumption is explicit: on error the caller still owns all cold weights.
    /// Numerical encoder projections remain at the original execution boundary.
    pub fn initPrepared(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, encoder_hidden: CT, enc_seq: usize, prepared: *PreparedDecoderWeights, program_owner: ?*?PreparedSingleToken) !DecodeCache {
        if (cb.ptr != prepared.backend.ptr or cb.vtable != prepared.backend.vtable or prepared.weights.layers.len != config.decoder_layers) return error.PreparedDecoderWeightOwnerChanged;
        const native = if (program_owner) |owner| if (owner.*) |*program| program else null else null;
        if (native) |program| if (program.schedule.frame == null or program.schedule.frame.?.scope_backends != 1) return error.DenseScheduleNotEntered;
        const geometry = try CacheGeometry.init(config, enc_seq);
        const d_model = config.d_model;
        const weights = prepared.weights;
        const layers = if (native) |program| program.cache_layers else try allocator.alloc(LayerCache, config.decoder_layers);
        var built: usize = 0;
        errdefer {
            for (layers[0..built]) |layer| {
                if (native == null) {
                    cb.free(layer.k_cross);
                    cb.free(layer.v_cross);
                }
                if (native == null) {
                    if (layer.k_self) |k| cb.free(k);
                    if (layer.v_self) |v| cb.free(v);
                }
            }
            if (native == null) allocator.free(layers);
        }
        // The cross projections are one batched pass over the encoder
        // output; on Metal they share a single submission.
        var frame_active = try beginWhisperMetalFrame(cb, .prefill);
        errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
        for (0..config.decoder_layers) |layer| {
            prepared.fetcher.cb = cb;
            var buf: [256]u8 = undefined;
            const cross = if (prepared.cross) |values| values[layer] else PreparedDecoderWeights.Cross{
                .k = try prepared.fetcher.linear(layer, "encoder_attn.k_proj", &buf),
                .v = try prepared.fetcher.linear(layer, "encoder_attn.v_proj", &buf),
            };
            const k = cross.k.w;
            const k_b = cross.k.b;
            const v = cross.v.w;
            const v_b = cross.v.b;
            // Every later step reads these from the device; keep them there
            // rather than re-uploading megabytes per layer per token.
            var k_cross: CT = undefined;
            var v_cross: CT = undefined;
            if (native) |program| {
                const slab = program.self_slabs[layer];
                const Native = @import("../ops/native_compute.zig").NativeCompute;
                const schedule_mod = @import("../execution/dense_decoder_schedule.zig");
                const k_descriptor = try schedule_mod.projectionDescriptor(cb, program.source_data, encoder_hidden, k, k_b, slab.cross_k, enc_seq, d_model, d_model);
                const v_descriptor = try schedule_mod.projectionDescriptor(cb, program.source_data, encoder_hidden, v, v_b, slab.cross_v, enc_seq, d_model, d_model);
                if (!try Native.applyDenseProjectionInto(cb, &k_descriptor, if (k_b) |bias| &.{ encoder_hidden, k, bias } else &.{ encoder_hidden, k }, slab.cross_k)) return error.PreparedDenseProjectionChanged;
                if (!try Native.applyDenseProjectionInto(cb, &v_descriptor, if (v_b) |bias| &.{ encoder_hidden, v, bias } else &.{ encoder_hidden, v }, slab.cross_v)) return error.PreparedDenseProjectionChanged;
                k_cross = slab.cross_k;
                v_cross = slab.cross_v;
            } else {
                k_cross = try residentProjection(cb, try (Linear{ .w = k, .b = k_b }).apply(cb, encoder_hidden, enc_seq, d_model, d_model));
                errdefer cb.free(k_cross);
                v_cross = try residentProjection(cb, try (Linear{ .w = v, .b = v_b }).apply(cb, encoder_hidden, enc_seq, d_model, d_model));
            }
            errdefer if (native == null) {
                cb.free(k_cross);
                cb.free(v_cross);
            };
            if (native == null) {
                if (try cb.prepareCrossAttentionKv(k_cross, v_cross)) |converted| {
                    cb.free(k_cross);
                    cb.free(v_cross);
                    k_cross = converted.first;
                    v_cross = converted.second;
                }
            }
            layers[layer] = .{ .k_cross = k_cross, .v_cross = v_cross };
            built += 1;
        }
        if (frame_active) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            frame_active = false;
        }

        // Preallocated self-cache slabs, when the backend supports them.
        var capacity: usize = 0;
        const slab_rows = geometry.capacity;
        if (native) |program| {
            for (layers, program.self_slabs) |*layer, slab| {
                layer.k_self = slab.k;
                layer.v_self = slab.v;
            }
            capacity = slab_rows;
        } else if (slab_rows <= std.math.maxInt(i32) and d_model <= std.math.maxInt(i32)) {
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

        const encoder_mask = if (native) |program| program.encoder_mask else try allocator.alloc(i64, geometry.encoder_mask_elements);
        errdefer if (native == null) allocator.free(encoder_mask);
        @memset(encoder_mask, 1);
        const self_mask = if (native) |program| program.self_mask else try allocator.alloc(i64, geometry.self_mask_elements);
        @memset(self_mask, 1);
        var fetcher = prepared.fetcher;
        prepared.fetcher.handles = .empty;
        prepared.weights.layers = &.{};
        fetcher.cb = undefined;
        const adopted_program = if (program_owner) |owner| block: {
            const value = owner.*;
            owner.* = null;
            break :block value;
        } else null;
        var program_value = adopted_program;
        if (program_value) |*program| program.adopted = true;
        return .{
            .allocator = allocator,
            .layers = layers,
            .weights = weights,
            .fetcher = fetcher,
            .enc_seq = enc_seq,
            .encoder_mask = encoder_mask,
            .self_mask = self_mask,
            .capacity = capacity,
            .native_program = program_value,
        };
    }

    pub fn deinit(self: *DecodeCache, cb: *const ComputeBackend) void {
        self.deinitWithCleanup(cb, .{});
    }
    pub fn deinitWithCleanup(self: *DecodeCache, cb: *const ComputeBackend, current: @import("../execution/cleanup_context.zig").Context) void {
        if (self.native_program) |*program| program.deinit(current);
        self.native_program = null;
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
    assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule = null,
    pending: ?CT = null,

    /// Fold any pending output into the stream and return its normalized
    /// view. The caller owns the returned tensor.
    fn normalize(self: *ResidualStream, cb: *const ComputeBackend, norm: Norm, dim: usize, profile: ?*StepProfile) !CT {
        const pending = self.pending orelse return stepNorm(cb, profile, norm, self.sum, dim);
        const pair = try addNorm(cb, pending, self.sum, norm, dim, profile);
        stepRelease(cb, profile, pending);
        stepRelease(cb, profile, self.sum);
        self.pending = null;
        self.sum = pair.sum;
        return pair.normed;
    }

    pub fn deinit(self: *ResidualStream, cb: *const ComputeBackend) void {
        if (self.pending) |p| {
            if (self.assigned) |schedule| schedule.releaseTemporary(cb, p) else cb.free(p);
        }
        if (self.assigned) |schedule| schedule.releaseTemporary(cb, self.sum) else cb.free(self.sum);
        self.* = undefined;
    }
};

const AddNormPair = struct {
    sum: CT,
    normed: CT,
};

/// `sum = a + b` and `normed = norm(sum)`, fused when the backend offers
/// it. Neither input is consumed.
fn stepSchedule(profile: ?*StepProfile) ?*@import("../execution/dense_decoder_schedule.zig").Schedule {
    return if (profile) |p| p.assigned else null;
}
fn stepRelease(cb: *const ComputeBackend, profile: ?*StepProfile, value: CT) void {
    if (stepSchedule(profile)) |schedule| schedule.releaseTemporary(cb, value) else cb.free(value);
}
fn stepLinear(cb: *const ComputeBackend, profile: ?*StepProfile, value: Linear, input: CT, rows: usize, width: usize, out: usize) !CT {
    if (stepSchedule(profile)) |schedule| return schedule.execute(if (value.b) |bias| &.{ input, value.w, bias } else &.{ input, value.w }, null);
    return value.apply(cb, input, rows, width, out);
}
fn stepNorm(cb: *const ComputeBackend, profile: ?*StepProfile, value: Norm, input: CT, width: usize) !CT {
    if (stepSchedule(profile)) |schedule| return schedule.execute(&.{ input, value.w, value.b }, null);
    return value.apply(cb, input, width);
}
fn stepAdd(cb: *const ComputeBackend, profile: ?*StepProfile, a: CT, b: CT) !CT {
    if (stepSchedule(profile)) |schedule| return schedule.execute(&.{ a, b }, null);
    return cb.add(a, b);
}
fn stepEmbedding(cb: *const ComputeBackend, profile: *StepProfile, weight: CT, ids: []const i64, rows: usize, width: usize) !CT {
    if (profile.assigned) |schedule| {
        const node = schedule.graph.node(schedule.steps[schedule.cursor]);
        if (rows != ids.len or node.op != .gather or node.output_shape.dims[0] != rows) return error.DenseScheduleOrderChanged;
        return schedule.execute(&.{ weight, schedule.values[node.inputs[1]] orelse return error.MissingValue }, null);
    }
    return cb.embeddingLookup(weight, ids, rows, width);
}
// An identical CT names the same retained header and therefore the same
// actual plane. Query it once; distinct view headers still need both reports.
// Keep the backing, offset and live extent checks on both paths.
fn validatePrefixRows(cb: *const ComputeBackend, actual: CT, whole: CT, bytes: usize) !void {
    const mod = @import("../execution/dense_decoder_schedule.zig");
    const observed = try mod.residentPlane(cb, actual);
    const full = if (actual == whole) observed else try mod.residentPlane(cb, whole);
    if (observed.backing == null or !std.meta.eql(observed.backing, full.backing) or observed.byte_offset != full.byte_offset or observed.readable_bytes < bytes) return error.PreparedDensePrefixBindingChanged;
}

fn stepPrefix(cb: *const ComputeBackend, profile: *StepProfile, q: CT, k: CT, v: CT, mask: []const i64, live: usize, heads: usize, dim: usize) !CT {
    if (profile.assigned) |schedule| {
        if (schedule.descriptors[schedule.steps[schedule.cursor]].?.operation == .dense_attention_rows) return stepCrossRows(cb, profile, q, k, v, mask, 1, live, heads, dim);
        const node = schedule.graph.node(schedule.steps[schedule.cursor]);
        const whole_k = schedule.values[node.inputs[1]] orelse return error.MissingValue;
        const whole_v = schedule.values[node.inputs[2]] orelse return error.MissingValue;
        for ([_]CT{ k, v }, [_]CT{ whole_k, whole_v }) |actual, whole| try validatePrefixRows(cb, actual, whole, live * heads * dim * 4);
        const attention = ops.AttentionContext{ .total_sequence_len = live, .query_sequence_len = 1, .kv_sequence_len = live, .dense_attention_prefix = .{ .live_key_rows = live, .mask = mask } };
        return schedule.execute(&.{ q, whole_k, whole_v }, &attention);
    }
    return cb.crossAttention(q, k, v, mask, 1, 1, live, heads, dim);
}

fn stepCausal(cb: *const ComputeBackend, profile: *StepProfile, q: CT, k: CT, v: CT, rows: usize, heads: usize, dim: usize) !CT {
    if (profile.assigned) |schedule| {
        const attention = ops.AttentionContext{ .total_sequence_len = rows, .query_sequence_len = rows, .kv_sequence_len = rows, .dense_attention_rows = .{} };
        return schedule.execute(&.{ q, k, v }, &attention);
    }
    return cb.causalSelfAttention(q, k, v, null, 1, rows, heads, dim);
}
fn stepCrossRows(cb: *const ComputeBackend, profile: *StepProfile, q: CT, k: CT, v: CT, mask: []const i64, rows: usize, keys: usize, heads: usize, dim: usize) !CT {
    if (profile.assigned) |schedule| {
        const attention = ops.AttentionContext{ .total_sequence_len = keys, .query_sequence_len = rows, .kv_sequence_len = keys, .dense_attention_rows = .{ .mask = mask } };
        return schedule.execute(&.{ q, k, v }, &attention);
    }
    return cb.crossAttention(q, k, v, mask, 1, rows, keys, heads, dim);
}

fn addNorm(cb: *const ComputeBackend, a: CT, b: CT, norm: Norm, dim: usize, profile: ?*StepProfile) !AddNormPair {
    if (try cb.addLayerNormSum(a, b, norm.w, norm.b, dim, 1e-5)) |fused| {
        if (profile) |prof| prof.fused_add_norm += 1;
        return .{ .sum = fused.sum, .normed = fused.normed };
    }
    const sum = try stepAdd(cb, profile, a, b);
    errdefer stepRelease(cb, profile, sum);
    const normed = try stepNorm(cb, profile, norm, sum, dim);
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
    if (n == 1 and start > 0 and !pipelined) if (cache.native_program) |*program| {
        try program.beginStep(tokens[0], start);
        profile.assigned = program.schedule;
    };
    if (start == 0 and !pipelined) if (cache.native_program) |*program| {
        profile.assigned = try program.beginInitial(tokens);
    };
    profile.planned_scope = scope_active;
    profile.start();

    const device_token_slot: ?usize = if (pipelined) output.pipelined.device_token_slot else null;
    const embedded = if (device_token_slot) |slot|
        (try cb.embeddingLookupDeviceToken(weights.embed, slot, d_model)) orelse {
            pipelineTrace("step declined: device token embedding (slot {d})", .{slot});
            return error.UnsupportedOperation;
        }
    else
        try stepEmbedding(cb, &profile, weights.embed, tokens, n, d_model);
    var embedded_live = true;
    defer if (embedded_live) stepRelease(cb, &profile, embedded);

    var pos_ids_buf: [2048]i64 = undefined;
    if (n > pos_ids_buf.len) return error.SequenceTooLong;
    const pos_ids = pos_ids_buf[0..n];
    for (0..n) |i| pos_ids[i] = @intCast(start + i);
    const pos_emb = try stepEmbedding(cb, &profile, weights.positions, pos_ids, n, d_model);
    defer stepRelease(cb, &profile, pos_emb);
    var stream = ResidualStream{ .sum = try stepAdd(cb, &profile, embedded, pos_emb), .assigned = profile.assigned };
    defer stream.deinit(cb);
    stepRelease(cb, &profile, embedded);
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
    defer if (normed_live) stepRelease(cb, &profile, normed);
    profile.mark(cb, .final_norm);

    // Only the last row feeds sampling; project it alone.
    const last_row = if (n == 1) normed else blk: {
        const row = if (profile.assigned) |schedule| try schedule.execute(&.{normed}, null) else try cb.sliceRows2D(allocator, normed, n - 1, 1, d_model);
        stepRelease(cb, &profile, normed);
        normed_live = false;
        break :blk row;
    };
    defer if (n != 1) stepRelease(cb, &profile, last_row);

    const logits = try stepLinear(cb, &profile, .{ .w = weights.lm_head, .b = null }, last_row, 1, d_model, config.vocab_size);
    defer stepRelease(cb, &profile, logits);
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
    full_capacity: bool,
) !CachedRows {
    const total = start + dec_seq;
    if (cache.preallocated()) {
        const slab = slot.* orelse return error.InvalidInputShape;
        if (total > cache.capacity) return error.SequenceTooLong;
        const copied = try cb.copyRows2D(allocator, slab, start, fresh, 0, dec_seq, d_model);
        if (!copied) return error.UnsupportedOperation;
        if (full_capacity) return .{ .tensor = slab, .owned_view = false };
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
    const q = try stepLinear(cb, profile, w.q, normed, rows, d_model, d_model);
    errdefer stepRelease(cb, profile, q);
    const k = try stepLinear(cb, profile, w.k, normed, rows, d_model, d_model);
    errdefer stepRelease(cb, profile, k);
    const v = try stepLinear(cb, profile, w.v, normed, rows, d_model, d_model);
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
    defer stepRelease(cb, profile, normed);
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
        errdefer if (dst_live) stepRelease(cb, profile, k_dst);
        const v_dst = try cb.sliceRows2D(allocator, layer_cache.v_self.?, start, dec_seq, d_model);
        errdefer if (dst_live) stepRelease(cb, profile, v_dst);
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
            if (v_ok) q_in_place = try stepLinear(cb, profile, w.q, normed, dec_seq, d_model, d_model);
        }
        if (q_in_place) |q| {
            proj = .{ .q = q, .k = k_dst, .v = v_dst };
            in_place = true;
            profile.fused_qkv += 1;
            profile.in_place_kv += 1;
        } else {
            stepRelease(cb, profile, k_dst);
            stepRelease(cb, profile, v_dst);
        }
        dst_live = false;
    }
    if (!in_place) proj = try projectQkv(cb, w, normed, dec_seq, d_model, profile);
    var kv_consumed = false;
    defer {
        stepRelease(cb, profile, proj.q);
        if (!kv_consumed) {
            stepRelease(cb, profile, proj.k);
            stepRelease(cb, profile, proj.v);
        }
    }
    profile.mark(cb, .qkv);

    var self_attn: CT = undefined;
    if (start == 0 and !cache.preallocated()) {
        // First block without slabs: the projections become the cache.
        self_attn = try stepCausal(cb, profile, proj.q, proj.k, proj.v, dec_seq, num_heads, head_dim);
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
            try appendSelfCache(cb, allocator, cache, &layer_cache.k_self, proj.k, start, dec_seq, d_model, profile.assigned != null);
        defer if (k_rows.owned_view) stepRelease(cb, profile, k_rows.tensor);
        const v_rows = if (in_place)
            CachedRows{ .tensor = try cb.sliceRows2D(allocator, layer_cache.v_self.?, 0, total, d_model), .owned_view = true }
        else
            try appendSelfCache(cb, allocator, cache, &layer_cache.v_self, proj.v, start, dec_seq, d_model, profile.assigned != null);
        defer if (v_rows.owned_view) stepRelease(cb, profile, v_rows.tensor);
        profile.mark(cb, .kv_append);
        if (start == 0) {
            self_attn = try stepCausal(cb, profile, proj.q, proj.k, proj.v, dec_seq, num_heads, head_dim);
        } else {
            // One new query over every cached key: causal by construction.
            self_attn = if (dec_seq == 1) try stepPrefix(cb, profile, proj.q, k_rows.tensor, v_rows.tensor, cache.self_mask[0..total], total, num_heads, head_dim) else try cb.crossAttention(proj.q, k_rows.tensor, v_rows.tensor, cache.self_mask[0..total], 1, dec_seq, total, num_heads, head_dim);
        }
    }
    defer stepRelease(cb, profile, self_attn);
    profile.mark(cb, .self_attn);

    const self_proj = try stepLinear(cb, profile, w.o, self_attn, dec_seq, d_model, d_model);
    defer stepRelease(cb, profile, self_proj);
    // Residual add fused into the next layer norm.
    const after_self = try addNorm(cb, self_proj, hidden, w.cross_ln, d_model, profile);
    defer stepRelease(cb, profile, after_self.sum);
    defer stepRelease(cb, profile, after_self.normed);
    profile.mark(cb, .self_out);

    // --- Cross-attention against the cached encoder projections ---
    const Q_cross = try stepLinear(cb, profile, w.cross_q, after_self.normed, dec_seq, d_model, d_model);
    defer stepRelease(cb, profile, Q_cross);
    profile.mark(cb, .cross_norm_q);
    const cross_attn = if (dec_seq == 1) try stepPrefix(cb, profile, Q_cross, layer_cache.k_cross, layer_cache.v_cross, cache.encoder_mask, cache.enc_seq, num_heads, head_dim) else try stepCrossRows(cb, profile, Q_cross, layer_cache.k_cross, layer_cache.v_cross, cache.encoder_mask, dec_seq, cache.enc_seq, num_heads, head_dim);
    defer stepRelease(cb, profile, cross_attn);
    profile.mark(cb, .cross_attn);
    const cross_proj = try stepLinear(cb, profile, w.cross_o, cross_attn, dec_seq, d_model, d_model);
    defer stepRelease(cb, profile, cross_proj);
    const after_cross = try addNorm(cb, cross_proj, after_self.sum, w.ffn_ln, d_model, profile);
    var after_cross_sum_live = true;
    defer if (after_cross_sum_live) stepRelease(cb, profile, after_cross.sum);
    defer stepRelease(cb, profile, after_cross.normed);
    profile.mark(cb, .cross_out);

    // --- FFN ---
    var fc1_out: ?CT = null;
    defer if (fc1_out) |tensor| stepRelease(cb, profile, tensor);
    const activated = activation: {
        if (cb.kind() == .cuda) {
            if (w.fc1.b) |bias| {
                if (try cb.linearGelu(after_cross.normed, w.fc1.w, bias, dec_seq, d_model, ffn_dim)) |fused| {
                    profile.mark(cb, .fc1);
                    break :activation fused;
                }
            }
        }
        fc1_out = try stepLinear(cb, profile, w.fc1, after_cross.normed, dec_seq, d_model, ffn_dim);
        profile.mark(cb, .fc1);
        break :activation if (profile.assigned) |schedule| try schedule.execute(&.{fc1_out.?}, null) else try cb.gelu(fc1_out.?);
    };
    defer stepRelease(cb, profile, activated);
    profile.mark(cb, .gelu);
    const fc2_out = try stepLinear(cb, profile, w.fc2, activated, dec_seq, ffn_dim, d_model);
    profile.mark(cb, .fc2);

    // Hand the residual pair to the stream: the add happens inside the next
    // layer norm (or the final one).
    stepRelease(cb, profile, stream.sum);
    stream.sum = after_cross.sum;
    after_cross_sum_live = false;
    stream.pending = fc2_out;
}

fn checkIncrementalResidentEncoder(cb: ComputeBackend, reference: ComputeBackend) !void {
    const a = std.testing.allocator;
    const fixture = @import("../execution/testing/incremental_encoder_decoder.zig");
    const config = fixture.config;
    const features = [_]f32{ 0.2, 0.5, -0.1, 0.4, 0.3, -0.2, 0.7, 0.1 };
    const mel = try cb.fromFloat32Shape(&features, &.{ 1, 2, 4 });
    defer cb.free(mel);
    const mel_reference = try reference.fromFloat32Shape(&features, &.{ 1, 2, 4 });
    defer reference.free(mel_reference);
    const host = try encoderForward(&reference, a, config, mel_reference, 1, 4);
    defer a.free(host);
    const held = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
    const encoded = try encoderForwardTensor(&cb, a, config, mel, 1, 4);
    defer cb.free(encoded);
    try held.expectUnchanged(cb);
    var cache = try DecodeCache.init(&cb, a, config, encoded, 2);
    defer cache.deinit(&cb);
    try held.expectUnchanged(cb);
    const reference_ct = try reference.fromFloat32Shape(host, &.{ 2, 4 });
    defer reference.free(reference_ct);
    var expected_cache = try DecodeCache.init(&reference, a, config, reference_ct, 2);
    defer expected_cache.deinit(&reference);
    const actual_first = try decoderStepCachedLogits(&cb, a, config, &.{ 0, 1 }, &cache);
    defer a.free(actual_first);
    const expected_first = try decoderStepCachedLogits(&reference, a, config, &.{ 0, 1 }, &expected_cache);
    defer a.free(expected_first);
    const actual_second = try decoderStepCachedLogits(&cb, a, config, &.{2}, &cache);
    defer a.free(actual_second);
    const expected_second = try decoderStepCachedLogits(&reference, a, config, &.{2}, &expected_cache);
    defer a.free(expected_second);
    try std.testing.expectEqual(expected_first.len, actual_first.len);
    try std.testing.expectEqual(expected_second.len, actual_second.len);
    for (expected_first, actual_first) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.0001);
    for (expected_second, actual_second) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.0001);
    try std.testing.expectEqual(@as(usize, 3), cache.positions);
    const geometry = try CacheGeometry.init(config, 2);
    const amounts = try geometry.cacheAmounts(&cb);
    const logical = 2 * config.decoder_layers * (geometry.cross_tensor_bytes + geometry.self_tensor_bytes);
    if (cb.kind() == .native) {
        try std.testing.expectEqual(logical, amounts.host_kv_bytes);
        try std.testing.expectEqual(@as(usize, 0), amounts.backend_kv_bytes);
    } else {
        try std.testing.expectEqual(@as(usize, 0), amounts.host_kv_bytes);
        try std.testing.expect(amounts.backend_kv_bytes >= logical);
        if (cb.vtable.kernels) |table| if (table.physical_storage_policy) |_| {
            const actual_bound = 2 * config.decoder_layers * (try ops.physical_storage.reservationBytes(cb, .{ .byte_size = geometry.cross_tensor_bytes }) + try ops.physical_storage.reservationBytes(cb, .{ .byte_size = geometry.self_tensor_bytes }));
            try std.testing.expectEqual(actual_bound, amounts.backend_kv_bytes);
        };
    }
}

test "incremental resident encoder and real Native KV steps match host reference without hidden roundtrip" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(@import("../execution/testing/incremental_encoder_decoder.zig").populate, checkIncrementalResidentEncoder);
}

test "incremental resident encoder and real Metal KV steps match Native reference without hidden roundtrip" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(@import("../execution/testing/incremental_encoder_decoder.zig").populate, checkIncrementalResidentEncoder);
}

test "incremental canonical cache geometry bounds full encoder constructor and rejects overflow" {
    const config: Config = .{ .d_model = 64, .decoder_layers = 2, .decoder_attention_heads = 2, .decoder_ffn_dim = 128, .max_target_positions = 8, .vocab_size = 4 };
    const geometry = try CacheGeometry.init(config, 1024);
    try std.testing.expect(try geometry.workspaceBytes(config) >= 6 * 1024 * 64 * @sizeOf(f32));
    try std.testing.expectEqual(@as(usize, 1024), geometry.encoder_mask_elements);
    try std.testing.expectEqual(@as(usize, 8), geometry.self_mask_elements);
    try std.testing.expectError(error.ResourceLimitExceeded, CacheGeometry.init(config, std.math.maxInt(usize)));
    try std.testing.expectError(error.InvalidInputShape, CacheGeometry.init(config, 0));
}

test "CUDA optional convolution preflight preserves direct default and composes actual unfolded dense slots" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const a = std.testing.allocator;
    var compute = @import("../ops/cuda/cuda_compute.zig").CudaCompute.init(a) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer compute.deinit();
    compute.capability_profile = .transformer_seq2seq;
    try compute.configureSeq2seqPolicy(.{});
    const cb = compute.computeBackend();
    const before = compute.ctx.device_allocations.snapshot();
    const slots = compute.decoder_runtime_linear_slots.count();
    try std.testing.expect((try convViaIm2col(&cb, @ptrFromInt(8), @ptrFromInt(16), @ptrFromInt(24), 2, 2, 2, 4, 3, 2, 1, false)) == null);
    try std.testing.expectEqual(slots, compute.decoder_runtime_linear_slots.count());
    try std.testing.expectEqualDeep(before, compute.ctx.device_allocations.snapshot());
    try compute.configureSeq2seqPolicy(.{ .im2col = true });
    const weights = [_]f32{ 1, 0, -1, 0.5, 1, -0.5, -1, 2, 0, 0, 0.25, 0.75 };
    const biases = [_]f32{ 0.25, -0.75 };
    const input = [_]f32{ 1, 2, 3, 4, 11, 12, 13, 14, 101, 102, 103, 104, 111, 112, 113, 114 };
    const wt = Tensor{ .data = @constCast(std.mem.sliceAsBytes(&weights)), .dtype = .f32, .shape = &.{ 2, 2, 3 }, .name = "conv.weight", .allocator = a, .owns_data = false, .owns_shape = false };
    const bt = Tensor{ .data = @constCast(std.mem.sliceAsBytes(&biases)), .dtype = .f32, .shape = &.{2}, .name = "conv.bias", .allocator = a, .owns_data = false, .owns_shape = false };
    try compute.insertWeightFromTensor(try a.dupe(u8, "conv.weight"), &wt);
    try compute.insertWeightFromTensor(try a.dupe(u8, "conv.bias"), &bt);
    const w = try cb.getWeight("conv.weight");
    defer cb.free(w);
    const b = try cb.getWeight("conv.bias");
    defer cb.free(b);
    const x = try cb.fromFloat32Shape(&input, &.{ 2, 2, 4 });
    defer cb.free(x);
    const direct = try cb.conv1d(x, w, b, 2, 2, 2, 4, 3, 2, 1);
    defer cb.free(direct);
    const unfolded = (try convViaIm2col(&cb, w, b, x, 2, 2, 2, 4, 3, 2, 1, false)) orelse return error.ExpectedUnfoldedConvolution;
    defer cb.free(unfolded);
    const direct_values = try cb.toFloat32(direct, a);
    defer a.free(direct_values);
    const rows = try cb.toFloat32(unfolded, a);
    defer a.free(rows);
    for (0..2) |batch| for (0..2) |time| for (0..2) |channel| {
        var expected = biases[channel];
        for (0..2) |source_channel| for (0..3) |tap| {
            const position = @as(isize, @intCast(time * 2 + tap)) - 1;
            if (position < 0 or position >= 4) continue;
            expected += input[(batch * 2 + source_channel) * 4 + @as(usize, @intCast(position))] * weights[(channel * 2 + source_channel) * 3 + tap];
        };
        try std.testing.expectEqual(expected, rows[(batch * 2 + time) * 2 + channel]);
        try std.testing.expectEqual(expected, direct_values[(batch * 2 + channel) * 2 + time]);
    };
    try std.testing.expect(compute.decoder_runtime_linear_slots.count() > slots);
}

test "Whisper prepared prefix validates identical retained headers and distinct Native views" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const values = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    const whole = try cb.fromFloat32Shape(&values, &.{ 4, 4 });
    defer cb.free(whole);
    try validatePrefixRows(&cb, whole, whole, values.len * @sizeOf(f32));
    try std.testing.expectError(error.PreparedDensePrefixBindingChanged, validatePrefixRows(&cb, whole, whole, (values.len + 1) * @sizeOf(f32)));
    const prefix = try cb.sliceRows2D(a, whole, 0, 2, 4);
    defer cb.free(prefix);
    try std.testing.expect(prefix != whole);
    try validatePrefixRows(&cb, prefix, whole, 8 * @sizeOf(f32));
    try std.testing.expectError(error.PreparedDensePrefixBindingChanged, validatePrefixRows(&cb, prefix, whole, 12 * @sizeOf(f32)));
    const shifted = try cb.sliceRows2D(a, whole, 1, 2, 4);
    defer cb.free(shifted);
    try std.testing.expectError(error.PreparedDensePrefixBindingChanged, validatePrefixRows(&cb, shifted, whole, 8 * @sizeOf(f32)));
    const other = try cb.fromFloat32Shape(&values, &.{ 4, 4 });
    defer cb.free(other);
    try std.testing.expectError(error.PreparedDensePrefixBindingChanged, validatePrefixRows(&cb, other, whole, 8 * @sizeOf(f32)));
}
