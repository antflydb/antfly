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

// DeBERTa-v2/v3 encoder using abstract ComputeBackend ops.
//
// Key difference from BERT: disentangled attention with relative position
// encodings. No absolute position embeddings. Uses content-to-content,
// content-to-position, and position-to-content attention scores.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const deberta_config = @import("../models/deberta.zig");
const mlx_compute_mod = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig") else struct {};

pub const Config = deberta_config.Config;

pub const EncoderProfile = struct {
    embeddings_ns: u64 = 0,
    relative_position_ns: u64 = 0,
    layer_total_ns: u64 = 0,
    qkv_ns: u64 = 0,
    relative_qk_ns: u64 = 0,
    attention_ns: u64 = 0,
    attention_output_ns: u64 = 0,
    ffn_intermediate_ns: u64 = 0,
    ffn_output_ns: u64 = 0,
    layernorm_residual_ns: u64 = 0,

    pub fn add(self: *EncoderProfile, other: EncoderProfile) void {
        self.embeddings_ns += other.embeddings_ns;
        self.relative_position_ns += other.relative_position_ns;
        self.layer_total_ns += other.layer_total_ns;
        self.qkv_ns += other.qkv_ns;
        self.relative_qk_ns += other.relative_qk_ns;
        self.attention_ns += other.attention_ns;
        self.attention_output_ns += other.attention_output_ns;
        self.ffn_intermediate_ns += other.ffn_intermediate_ns;
        self.ffn_output_ns += other.ffn_output_ns;
        self.layernorm_residual_ns += other.layernorm_residual_ns;
    }
};

const RelativePositionEmb = struct {
    embeddings: CT,
    full_to_unique: ?[]i64 = null,
    unique_count: usize,
    full_count: usize,
};

fn monotonicNowNs() u64 {
    // wasm-freestanding has no posix clock; profiling is best-effort there.
    if (@import("builtin").target.cpu.arch.isWasm()) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn profileStart(profile: ?*EncoderProfile) u64 {
    return if (profile != null) monotonicNowNs() else 0;
}

fn profileElapsed(start_ns: u64) u64 {
    if (start_ns == 0) return 0;
    return monotonicNowNs() - start_ns;
}

fn uniqueRelativePositionProjectionEnabled() bool {
    // wasm-freestanding has no libc/getenv. The toggle is a server-side
    // debug knob, so the browser path simply takes the default.
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return platform.env.getenvBool("TERMITE_DEBERTA_UNIQUE_REL_POS");
}

fn metalEncoderFrameEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    if (platform.env.getenvBool("TERMITE_METAL_DISABLE_GLINER_ENCODER_FRAME")) return false;
    return true;
}

fn metalEncoderLayerExecutionEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    if (platform.env.getenvBool("TERMITE_METAL_DISABLE_GLINER_DEBERTA_LAYER_EXECUTION")) return false;
    return true;
}

const DebertaLinearSlotKind = enum(usize) {
    q = 0,
    k = 1,
    v = 2,
    attention_output = 3,
    intermediate = 4,
    output = 5,
};

const DebertaLayerNormSlotKind = enum(usize) {
    attention_output = 0,
    output = 1,
};

fn debertaLinearSlot(layer: usize, kind: DebertaLinearSlotKind) usize {
    return layer * 6 + @intFromEnum(kind);
}

fn debertaLayerNormSlot(layer: usize, kind: DebertaLayerNormSlotKind) usize {
    return layer * 2 + @intFromEnum(kind);
}

fn allAttentionMaskOnes(mask: []const i64, total: usize) bool {
    if (mask.len < total) return false;
    for (mask[0..total]) |value| {
        if (value == 0) return false;
    }
    return true;
}

pub fn preplanMetalDebertaEncoderFrame(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    batch: usize,
    seq_len: usize,
) !bool {
    if (cb.kind() != .metal) return false;
    if (!metalEncoderFrameEnabled()) return false;
    if (cb.decoderRuntimeHasActiveFrame()) return false;
    if (tensorParallelWorldSize(cb) > 1) return false;

    const layer_count: usize = @intCast(config.num_hidden_layers);
    const H: usize = @intCast(config.hidden_size);
    const I: usize = @intCast(config.intermediate_size);
    const heads: usize = @intCast(config.num_attention_heads);
    if (layer_count == 0 or batch == 0 or seq_len == 0 or H == 0 or I == 0 or heads == 0) return false;
    if (H % heads != 0) return false;

    const layers = try allocator.alloc(ops.DebertaEncoderLayerSpec, layer_count);
    defer allocator.free(layers);

    for (0..layer_count) |layer| {
        var q_w_buf: [256]u8 = undefined;
        var q_b_buf: [256]u8 = undefined;
        const q_w = try getLayerWeight(cb, layer, "attention.self.query_proj.weight", &q_w_buf);
        defer cb.free(q_w);
        const q_b = try getLayerWeight(cb, layer, "attention.self.query_proj.bias", &q_b_buf);
        defer cb.free(q_b);

        var k_w_buf: [256]u8 = undefined;
        var k_b_buf: [256]u8 = undefined;
        const k_w = try getLayerWeight(cb, layer, "attention.self.key_proj.weight", &k_w_buf);
        defer cb.free(k_w);
        const k_b = try getLayerWeight(cb, layer, "attention.self.key_proj.bias", &k_b_buf);
        defer cb.free(k_b);

        var v_w_buf: [256]u8 = undefined;
        var v_b_buf: [256]u8 = undefined;
        const v_w = try getLayerWeight(cb, layer, "attention.self.value_proj.weight", &v_w_buf);
        defer cb.free(v_w);
        const v_b = try getLayerWeight(cb, layer, "attention.self.value_proj.bias", &v_b_buf);
        defer cb.free(v_b);

        var attn_o_w_buf: [256]u8 = undefined;
        var attn_o_b_buf: [256]u8 = undefined;
        const attn_o_w = try getLayerWeight(cb, layer, "attention.output.dense.weight", &attn_o_w_buf);
        defer cb.free(attn_o_w);
        const attn_o_b = try getLayerWeight(cb, layer, "attention.output.dense.bias", &attn_o_b_buf);
        defer cb.free(attn_o_b);

        var ffn_i_w_buf: [256]u8 = undefined;
        var ffn_i_b_buf: [256]u8 = undefined;
        const ffn_i_w = try getLayerWeight(cb, layer, "intermediate.dense.weight", &ffn_i_w_buf);
        defer cb.free(ffn_i_w);
        const ffn_i_b = try getLayerWeight(cb, layer, "intermediate.dense.bias", &ffn_i_b_buf);
        defer cb.free(ffn_i_b);

        var ffn_o_w_buf: [256]u8 = undefined;
        var ffn_o_b_buf: [256]u8 = undefined;
        const ffn_o_w = try getLayerWeight(cb, layer, "output.dense.weight", &ffn_o_w_buf);
        defer cb.free(ffn_o_w);
        const ffn_o_b = try getLayerWeight(cb, layer, "output.dense.bias", &ffn_o_b_buf);
        defer cb.free(ffn_o_b);

        var attn_ln_w_buf: [256]u8 = undefined;
        var attn_ln_b_buf: [256]u8 = undefined;
        const attn_ln_w = try getLayerWeight(cb, layer, "attention.output.LayerNorm.weight", &attn_ln_w_buf);
        defer cb.free(attn_ln_w);
        const attn_ln_b = try getLayerWeight(cb, layer, "attention.output.LayerNorm.bias", &attn_ln_b_buf);
        defer cb.free(attn_ln_b);

        var ffn_ln_w_buf: [256]u8 = undefined;
        var ffn_ln_b_buf: [256]u8 = undefined;
        const ffn_ln_w = try getLayerWeight(cb, layer, "output.LayerNorm.weight", &ffn_ln_w_buf);
        defer cb.free(ffn_ln_w);
        const ffn_ln_b = try getLayerWeight(cb, layer, "output.LayerNorm.bias", &ffn_ln_b_buf);
        defer cb.free(ffn_ln_b);

        const q_slot = debertaLinearSlot(layer, .q);
        const k_slot = debertaLinearSlot(layer, .k);
        const v_slot = debertaLinearSlot(layer, .v);
        const attn_o_slot = debertaLinearSlot(layer, .attention_output);
        const ffn_i_slot = debertaLinearSlot(layer, .intermediate);
        const ffn_o_slot = debertaLinearSlot(layer, .output);
        const attn_ln_slot = debertaLayerNormSlot(layer, .attention_output);
        const ffn_ln_slot = debertaLayerNormSlot(layer, .output);

        if (!(try cb.decoderRuntimePrepareLinear(&.{ .slot = q_slot, .weight = q_w, .bias = q_b, .in_dim = H, .out_dim = H }))) return false;
        if (!(try cb.decoderRuntimePrepareLinear(&.{ .slot = k_slot, .weight = k_w, .bias = k_b, .in_dim = H, .out_dim = H }))) return false;
        if (!(try cb.decoderRuntimePrepareLinear(&.{ .slot = v_slot, .weight = v_w, .bias = v_b, .in_dim = H, .out_dim = H }))) return false;
        if (!(try cb.decoderRuntimePrepareLinear(&.{ .slot = attn_o_slot, .weight = attn_o_w, .bias = attn_o_b, .in_dim = H, .out_dim = H }))) return false;
        if (!(try cb.decoderRuntimePrepareLinear(&.{ .slot = ffn_i_slot, .weight = ffn_i_w, .bias = ffn_i_b, .in_dim = H, .out_dim = I }))) return false;
        if (!(try cb.decoderRuntimePrepareLinear(&.{ .slot = ffn_o_slot, .weight = ffn_o_w, .bias = ffn_o_b, .in_dim = I, .out_dim = H }))) return false;
        if (!(try cb.decoderRuntimePrepareLayerNorm(&.{ .slot = attn_ln_slot, .weight = attn_ln_w, .bias = attn_ln_b, .hidden_size = H }))) return false;
        if (!(try cb.decoderRuntimePrepareLayerNorm(&.{ .slot = ffn_ln_slot, .weight = ffn_ln_w, .bias = ffn_ln_b, .hidden_size = H }))) return false;

        layers[layer] = .{
            .q_linear_slot = q_slot,
            .k_linear_slot = k_slot,
            .v_linear_slot = v_slot,
            .attention_output_linear_slot = attn_o_slot,
            .intermediate_linear_slot = ffn_i_slot,
            .output_linear_slot = ffn_o_slot,
            .attention_layer_norm_slot = attn_ln_slot,
            .output_layer_norm_slot = ffn_ln_slot,
        };
    }

    return cb.debertaEncoderPlanFrame(&.{
        .layer_count = layer_count,
        .batch = batch,
        .seq_len = seq_len,
        .hidden_size = H,
        .intermediate_size = I,
        .num_attention_heads = heads,
        .position_buckets = @intCast(config.position_buckets),
        .max_position_embeddings = @intCast(config.max_position_embeddings),
        .norm_eps = config.layer_norm_eps,
        .layers = layers,
    });
}

/// Run the full DeBERTa encoder forward pass and return the result on
/// the backend (a CT).  Callers that consume the encoder output through
/// CT ops (gliner_head, future graph-resident pipelines) should use this
/// variant -- it skips the toFloat32 the legacy `forward` does at the
/// boundary, so on Metal/MLX the hidden state stays device-resident
/// across the encoder/head split, and on native it skips one allocation
/// + memcpy.
pub fn forwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    return forwardCtProfiled(cb, allocator, config, input_ids, attention_mask, batch, seq_len, null);
}

pub fn forwardCtProfiled(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    profile: ?*EncoderProfile,
) !CT {
    const H = config.hidden_size;
    const total = batch * seq_len;

    _ = try preplanMetalDebertaEncoderFrame(cb, allocator, config, batch, seq_len);

    var encoder_frame_active = false;
    if (cb.kind() == .metal and metalEncoderFrameEnabled() and !cb.decoderRuntimeHasActiveFrame()) {
        encoder_frame_active = try cb.decoderRuntimeBeginFrame();
    }
    errdefer if (encoder_frame_active) cb.decoderRuntimeCancelFrame() catch {};

    var timer = profileStart(profile);
    var hidden = try embeddings(cb, allocator, config, input_ids, attention_mask, total, H);
    if (profile) |p| p.embeddings_ns += profileElapsed(timer);
    timer = profileStart(profile);
    const rel_emb = try buildRelativePositionEmbInfo(cb, allocator, config, seq_len);
    defer cb.free(rel_emb.embeddings);
    defer if (rel_emb.full_to_unique) |ids| allocator.free(ids);
    if (profile) |p| p.relative_position_ns += profileElapsed(timer);

    for (0..config.num_hidden_layers) |layer| {
        timer = profileStart(profile);
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, rel_emb, batch, seq_len, layer, profile);
        cb.free(hidden);
        hidden = new_hidden;
        if (profile) |p| p.layer_total_ns += profileElapsed(timer);
    }
    // No post-encoder LayerNorm in DeBERTa-v3 (encoder.LayerNorm is norm_rel_ebd).
    if (encoder_frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        encoder_frame_active = false;
    }
    return hidden;
}

/// Run the full DeBERTa encoder forward pass.
/// Returns an owned f32 slice: [batch * seq_len * hidden_size].  Thin
/// wrapper around `forwardCt` for callers that need an `[]f32`.
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const hidden = try forwardCt(cb, allocator, config, input_ids, attention_mask, batch, seq_len);
    defer cb.free(hidden);
    return cb.toFloat32(hidden, allocator);
}

pub fn forwardUntilLayerCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    stop_layer_exclusive: usize,
) !CT {
    const H = config.hidden_size;
    const total = batch * seq_len;
    const clamped_stop = @min(stop_layer_exclusive, config.num_hidden_layers);

    var hidden = try embeddings(cb, allocator, config, input_ids, attention_mask, total, H);
    const rel_emb = try buildRelativePositionEmbInfo(cb, allocator, config, seq_len);
    defer cb.free(rel_emb.embeddings);
    defer if (rel_emb.full_to_unique) |ids| allocator.free(ids);

    for (0..clamped_stop) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, rel_emb, batch, seq_len, layer, null);
        cb.free(hidden);
        hidden = new_hidden;
    }
    return hidden;
}

pub fn forwardUntilLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    stop_layer_exclusive: usize,
) ![]f32 {
    const hidden = try forwardUntilLayerCt(cb, allocator, config, input_ids, attention_mask, batch, seq_len, stop_layer_exclusive);
    defer cb.free(hidden);
    return cb.toFloat32(hidden, allocator);
}

pub fn forwardFromHidden(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: []const f32,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    start_layer: usize,
) ![]f32 {
    return forwardFromHiddenRange(cb, allocator, config, hidden_in, attention_mask, batch, seq_len, start_layer, config.num_hidden_layers);
}

pub fn forwardFromHiddenRange(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: []const f32,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    start_layer: usize,
    end_layer_exclusive: usize,
) ![]f32 {
    const H = config.hidden_size;
    const total = batch * seq_len;
    if (hidden_in.len != total * H) return error.ShapeMismatch;

    const shape = [_]i32{ @intCast(total), @intCast(H) };
    var hidden = try cb.fromFloat32Shape(hidden_in, &shape);
    const rel_emb = try buildRelativePositionEmbInfo(cb, allocator, config, seq_len);
    defer cb.free(rel_emb.embeddings);
    defer if (rel_emb.full_to_unique) |ids| allocator.free(ids);
    const clamped_start = @min(start_layer, config.num_hidden_layers);
    const clamped_end = @max(clamped_start, @min(end_layer_exclusive, config.num_hidden_layers));
    for (clamped_start..clamped_end) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, rel_emb, batch, seq_len, layer, null);
        cb.free(hidden);
        hidden = new_hidden;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

fn embeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    total: usize,
    H: u32,
) !CT {
    // Word embeddings only (no position embeddings in DeBERTa-v3)
    const word_emb = try cb.getWeight("embeddings.word_embeddings.weight");
    defer cb.free(word_emb);
    const ln_w = try cb.getWeight("embeddings.LayerNorm.weight");
    defer cb.free(ln_w);
    const ln_b = try cb.getWeight("embeddings.LayerNorm.bias");
    defer cb.free(ln_b);

    if (try cb.debertaEmbeddings(.{
        .word_embeddings = word_emb,
        .layer_norm_weight = ln_w,
        .layer_norm_bias = ln_b,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .total = total,
        .hidden_size = H,
        .eps = config.layer_norm_eps,
    })) |fused| {
        return fused;
    }

    const result = try cb.embeddingLookup(word_emb, input_ids, total, H);

    // LayerNorm
    const normed = try cb.layerNorm(result, ln_w, ln_b, H, config.layer_norm_eps);
    cb.free(result);

    // Multiply by attention mask (DeBERTa-v3 zeros out padding embeddings).
    if (allAttentionMaskOnes(attention_mask, total)) return normed;

    // Keep the normalized activations on the backend and only upload the mask.
    const mask_data = try allocator.alloc(f32, total * H);
    defer allocator.free(mask_data);
    for (0..total) |i| {
        const mask_val: f32 = @floatFromInt(attention_mask[i]);
        @memset(mask_data[i * H ..][0..H], mask_val);
    }
    const shape = [_]i32{ @intCast(total), @intCast(H) };
    const mask_ct = try cb.fromFloat32Shape(mask_data, &shape);
    defer cb.free(mask_ct);

    const masked = try cb.multiply(normed, mask_ct);
    cb.free(normed);
    return masked;
}

/// Build relative position embedding lookup for all (i,j) pairs in sequence.
/// Returns a CT tensor of shape [seq_len, H] containing the rel_embeddings
/// indexed by relative position buckets.
fn buildRelativePositionEmb(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    seq_len: usize,
) !CT {
    const info = try buildRelativePositionEmbInfo(cb, allocator, config, seq_len);
    if (info.full_to_unique) |ids| {
        defer allocator.free(ids);
        defer cb.free(info.embeddings);
        return cb.embeddingLookup(info.embeddings, ids, info.full_count, config.hidden_size);
    }
    return info.embeddings;
}

fn buildRelativePositionEmbInfo(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    seq_len: usize,
) !RelativePositionEmb {
    const H = config.hidden_size;
    const num_buckets = config.position_buckets;
    const max_pos = config.max_position_embeddings;

    // Get the rel_embeddings weight: [max_pos, H]
    const rel_weight = try cb.getWeight("encoder.rel_embeddings.weight");
    defer cb.free(rel_weight);

    // DeBERTa-v3 norm_rel_ebd: apply LayerNorm to raw rel_embeddings before lookup
    const ln_w = try cb.getWeight("encoder.LayerNorm.weight");
    defer cb.free(ln_w);
    const ln_b = try cb.getWeight("encoder.LayerNorm.bias");
    defer cb.free(ln_b);
    const normed_rel = try cb.layerNorm(rel_weight, ln_w, ln_b, H, config.layer_norm_eps);
    defer cb.free(normed_rel);

    // Build bucket IDs for all relative positions: we need unique positions from -(seq_len-1) to +(seq_len-1)
    const num_rel = 2 * seq_len - 1;
    const bucket_ids = try allocator.alloc(i64, num_rel);
    defer allocator.free(bucket_ids);
    if (!uniqueRelativePositionProjectionEnabled()) {
        for (0..num_rel) |i| {
            const rel_pos: i64 = @as(i64, @intCast(i)) - @as(i64, @intCast(seq_len - 1));
            bucket_ids[i] = @intCast(deberta_config.relativePositionBucket(rel_pos, num_buckets, max_pos));
        }
        return .{
            .embeddings = try cb.embeddingLookup(normed_rel, bucket_ids, num_rel, H),
            .full_to_unique = null,
            .unique_count = num_rel,
            .full_count = num_rel,
        };
    }

    const full_to_unique = try allocator.alloc(i64, num_rel);
    errdefer allocator.free(full_to_unique);
    const unique_bucket_ids = try allocator.alloc(i64, num_rel);
    defer allocator.free(unique_bucket_ids);
    const bucket_to_unique = try allocator.alloc(i64, max_pos);
    defer allocator.free(bucket_to_unique);
    @memset(bucket_to_unique, -1);

    var unique_count: usize = 0;
    for (0..num_rel) |i| {
        const rel_pos: i64 = @as(i64, @intCast(i)) - @as(i64, @intCast(seq_len - 1));
        const bucket: usize = @intCast(deberta_config.relativePositionBucket(rel_pos, num_buckets, max_pos));
        bucket_ids[i] = @intCast(bucket);
        if (bucket_to_unique[bucket] < 0) {
            bucket_to_unique[bucket] = @intCast(unique_count);
            unique_bucket_ids[unique_count] = @intCast(bucket);
            unique_count += 1;
        }
        full_to_unique[i] = bucket_to_unique[bucket];
    }

    if (unique_count == num_rel) {
        allocator.free(full_to_unique);
        return .{
            .embeddings = try cb.embeddingLookup(normed_rel, bucket_ids, num_rel, H),
            .full_to_unique = null,
            .unique_count = num_rel,
            .full_count = num_rel,
        };
    }

    return .{
        .embeddings = try cb.embeddingLookup(normed_rel, unique_bucket_ids[0..unique_count], unique_count, H),
        .full_to_unique = full_to_unique,
        .unique_count = unique_count,
        .full_count = num_rel,
    };
}

fn encoderLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    rel_emb: RelativePositionEmb,
    batch: usize,
    seq_len: usize,
    layer: usize,
    profile: ?*EncoderProfile,
) !CT {
    _ = allocator;
    const H: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const head_dim = H / num_heads;
    const I: usize = @intCast(config.intermediate_size);
    const total = batch * seq_len;
    const eps = config.layer_norm_eps;

    const tp_world_size = tensorParallelWorldSize(cb);
    const use_tp = tp_world_size > 1;
    if (use_tp and (H % tp_world_size != 0 or num_heads % tp_world_size != 0 or I % tp_world_size != 0)) {
        return error.InvalidTensorParallelShape;
    }
    const local_num_heads = if (use_tp) num_heads / tp_world_size else num_heads;
    const local_hidden = if (use_tp) H / tp_world_size else H;
    const local_intermediate = if (use_tp) I / tp_world_size else I;

    if (!use_tp and metalEncoderFrameEnabled() and metalEncoderLayerExecutionEnabled()) {
        if (try cb.debertaEncoderLayer(&.{
            .layer = .{
                .q_linear_slot = debertaLinearSlot(layer, .q),
                .k_linear_slot = debertaLinearSlot(layer, .k),
                .v_linear_slot = debertaLinearSlot(layer, .v),
                .attention_output_linear_slot = debertaLinearSlot(layer, .attention_output),
                .intermediate_linear_slot = debertaLinearSlot(layer, .intermediate),
                .output_linear_slot = debertaLinearSlot(layer, .output),
                .attention_layer_norm_slot = debertaLayerNormSlot(layer, .attention_output),
                .output_layer_norm_slot = debertaLayerNormSlot(layer, .output),
            },
            .hidden = hidden,
            .relative_embeddings = rel_emb.embeddings,
            .relative_full_to_unique = rel_emb.full_to_unique,
            .relative_unique_count = rel_emb.unique_count,
            .relative_full_count = rel_emb.full_count,
            .attention_mask = attention_mask,
            .batch = batch,
            .seq_len = seq_len,
            .hidden_size = H,
            .intermediate_size = I,
            .num_attention_heads = num_heads,
            .head_dim = head_dim,
            .norm_eps = eps,
        })) |planned| {
            return planned;
        }
    }

    // Self-attention: Q, K, V projections (DeBERTa uses query_proj, key_proj, value_proj)
    var q_w_buf: [256]u8 = undefined;
    var q_b_buf: [256]u8 = undefined;
    var k_w_buf: [256]u8 = undefined;
    var k_b_buf: [256]u8 = undefined;
    var v_w_buf: [256]u8 = undefined;
    var v_b_buf: [256]u8 = undefined;
    const q_w = try getLayerWeight(cb, layer, "attention.self.query_proj.weight", &q_w_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, layer, "attention.self.query_proj.bias", &q_b_buf);
    defer cb.free(q_b);
    const k_w = try getLayerWeight(cb, layer, "attention.self.key_proj.weight", &k_w_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, layer, "attention.self.key_proj.bias", &k_b_buf);
    defer cb.free(k_b);

    const v_w = try getLayerWeight(cb, layer, "attention.self.value_proj.weight", &v_w_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, layer, "attention.self.value_proj.bias", &v_b_buf);
    defer cb.free(v_b);

    var timer = profileStart(profile);
    const qkv = if (use_tp) blk: {
        const Q = try linearReplicatedToMaybeSharded(cb, hidden, q_w, q_b, total, H, H);
        errdefer cb.free(Q);
        const K = try linearReplicatedToMaybeSharded(cb, hidden, k_w, k_b, total, H, H);
        errdefer cb.free(K);
        const V = try linearReplicatedToMaybeSharded(cb, hidden, v_w, v_b, total, H, H);
        break :blk ops.LinearTripleResult{ .first = Q, .second = K, .third = V };
    } else try cb.linearTriple(hidden, q_w, q_b, k_w, k_b, v_w, v_b, total, H, H);
    const Q = qkv.first;
    const K = qkv.second;
    const V = qkv.third;
    defer cb.free(Q);
    defer cb.free(K);
    defer cb.free(V);
    if (profile) |p| p.qkv_ns += profileElapsed(timer);

    // Disentangled attention: compute attention scores with relative position
    // A = Q_c·K_c^T + Q_c·K_r^T + K_c·Q_r^T
    // With share_att_key=true, position projections share content projection weights:
    //   K_r = rel_emb @ W_k + b_k
    //   Q_r = rel_emb @ W_q + b_q
    const num_rel = rel_emb.full_count;
    timer = profileStart(profile);
    const rel_qk = if (use_tp) blk: {
        const Q_r = try linearReplicatedToMaybeSharded(cb, rel_emb.embeddings, q_w, q_b, rel_emb.unique_count, H, H);
        errdefer cb.free(Q_r);
        const K_r = try linearReplicatedToMaybeSharded(cb, rel_emb.embeddings, k_w, k_b, rel_emb.unique_count, H, H);
        break :blk ops.LinearPairResult{ .first = Q_r, .second = K_r };
    } else try cb.linearPair(rel_emb.embeddings, q_w, q_b, k_w, k_b, rel_emb.unique_count, H, H);
    var Q_r = rel_qk.first;
    var K_r = rel_qk.second;
    if (rel_emb.full_to_unique) |ids| {
        const Q_expanded = try cb.embeddingLookup(Q_r, ids, num_rel, H);
        errdefer cb.free(Q_expanded);
        const K_expanded = try cb.embeddingLookup(K_r, ids, num_rel, H);
        cb.free(Q_r);
        cb.free(K_r);
        Q_r = Q_expanded;
        K_r = K_expanded;
    }
    defer cb.free(Q_r);
    defer cb.free(K_r);
    if (profile) |p| p.relative_qk_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const attn_out = try cb.disentangledRelativeAttention(Q, K, V, Q_r, K_r, attention_mask, batch, seq_len, local_num_heads, head_dim);
    defer cb.free(attn_out);
    if (profile) |p| p.attention_ns += profileElapsed(timer);

    // Attention output projection + residual + LayerNorm
    timer = profileStart(profile);
    var attn_proj_w_buf: [256]u8 = undefined;
    var attn_proj_b_buf: [256]u8 = undefined;
    const attn_proj_w = try getLayerWeight(cb, layer, "attention.output.dense.weight", &attn_proj_w_buf);
    defer cb.free(attn_proj_w);
    const attn_proj_b = try getLayerWeight(cb, layer, "attention.output.dense.bias", &attn_proj_b_buf);
    defer cb.free(attn_proj_b);
    var attn_ln_w_buf: [256]u8 = undefined;
    var attn_ln_b_buf: [256]u8 = undefined;
    const attn_ln_w = try getLayerWeight(cb, layer, "attention.output.LayerNorm.weight", &attn_ln_w_buf);
    defer cb.free(attn_ln_w);
    const attn_ln_b = try getLayerWeight(cb, layer, "attention.output.LayerNorm.bias", &attn_ln_b_buf);
    defer cb.free(attn_ln_b);
    const attn_normed = if (!use_tp) blk: {
        if (try cb.denseLinearLayerNorm(&.{
            .input = attn_out,
            .residual = hidden,
            .weight = attn_proj_w,
            .bias = attn_proj_b,
            .layer_norm_weight = attn_ln_w,
            .layer_norm_bias = attn_ln_b,
            .rows = total,
            .in_dim = H,
            .hidden_size = H,
            .eps = eps,
        })) |fused| {
            if (profile) |p| p.attention_output_ns += profileElapsed(timer);
            break :blk fused;
        }
        const attn_proj = try cb.linear(attn_out, attn_proj_w, attn_proj_b, total, local_hidden, H);
        errdefer cb.free(attn_proj);
        if (profile) |p| p.attention_output_ns += profileElapsed(timer);

        timer = profileStart(profile);
        const normed = if (try cb.addLayerNorm(attn_proj, hidden, attn_ln_w, attn_ln_b, H, eps)) |fused|
            fused
        else norm_blk: {
            const attn_res = try cb.add(attn_proj, hidden);
            defer cb.free(attn_res);
            break :norm_blk try cb.layerNorm(attn_res, attn_ln_w, attn_ln_b, H, eps);
        };
        cb.free(attn_proj);
        if (profile) |p| p.layernorm_residual_ns += profileElapsed(timer);
        break :blk normed;
    } else blk: {
        const attn_proj = try linearMaybeShardedToReplicated(cb, attn_out, attn_proj_w, attn_proj_b, total, H, H);
        defer cb.free(attn_proj);
        if (profile) |p| p.attention_output_ns += profileElapsed(timer);

        timer = profileStart(profile);
        const normed = if (try cb.addLayerNorm(attn_proj, hidden, attn_ln_w, attn_ln_b, H, eps)) |fused|
            fused
        else norm_blk: {
            const attn_res = try cb.add(attn_proj, hidden);
            defer cb.free(attn_res);
            break :norm_blk try cb.layerNorm(attn_res, attn_ln_w, attn_ln_b, H, eps);
        };
        if (profile) |p| p.layernorm_residual_ns += profileElapsed(timer);
        break :blk normed;
    };

    // FFN
    timer = profileStart(profile);
    var ffn_i_w_buf: [256]u8 = undefined;
    var ffn_i_b_buf: [256]u8 = undefined;
    const ffn_i_w = try getLayerWeight(cb, layer, "intermediate.dense.weight", &ffn_i_w_buf);
    defer cb.free(ffn_i_w);
    const ffn_i_b = try getLayerWeight(cb, layer, "intermediate.dense.bias", &ffn_i_b_buf);
    defer cb.free(ffn_i_b);
    var ffn_o_w_buf: [256]u8 = undefined;
    var ffn_o_b_buf: [256]u8 = undefined;
    const ffn_o_w = try getLayerWeight(cb, layer, "output.dense.weight", &ffn_o_w_buf);
    defer cb.free(ffn_o_w);
    const ffn_o_b = try getLayerWeight(cb, layer, "output.dense.bias", &ffn_o_b_buf);
    defer cb.free(ffn_o_b);
    var ffn_ln_w_buf: [256]u8 = undefined;
    var ffn_ln_b_buf: [256]u8 = undefined;
    const ffn_ln_w = try getLayerWeight(cb, layer, "output.LayerNorm.weight", &ffn_ln_w_buf);
    defer cb.free(ffn_ln_w);
    const ffn_ln_b = try getLayerWeight(cb, layer, "output.LayerNorm.bias", &ffn_ln_b_buf);
    defer cb.free(ffn_ln_b);

    if (!use_tp) {
        if (try cb.denseFfnLayerNorm(&.{
            .input = attn_normed,
            .residual = attn_normed,
            .first_weight = ffn_i_w,
            .first_bias = ffn_i_b,
            .second_weight = ffn_o_w,
            .second_bias = ffn_o_b,
            .layer_norm_weight = ffn_ln_w,
            .layer_norm_bias = ffn_ln_b,
            .rows = total,
            .hidden_size = H,
            .intermediate_size = I,
            .eps = eps,
            .activation = .gelu,
        })) |fused| {
            cb.free(attn_normed);
            if (profile) |p| p.ffn_output_ns += profileElapsed(timer);
            return fused;
        }
    }

    var ffn_inter_is_gelu = false;
    const ffn_inter = if (use_tp)
        try linearReplicatedToMaybeSharded(cb, attn_normed, ffn_i_w, ffn_i_b, total, H, I)
    else if (try cb.linearGelu(attn_normed, ffn_i_w, ffn_i_b, total, H, I)) |fused| blk: {
        ffn_inter_is_gelu = true;
        break :blk fused;
    } else try cb.linear(attn_normed, ffn_i_w, ffn_i_b, total, H, I);
    if (profile) |p| p.ffn_intermediate_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const ffn_gelu = if (ffn_inter_is_gelu)
        ffn_inter
    else blk: {
        const gelu = cb.gelu(ffn_inter) catch |err| {
            cb.free(ffn_inter);
            return err;
        };
        cb.free(ffn_inter);
        break :blk gelu;
    };
    defer cb.free(ffn_gelu);

    var ffn_out_has_residual = false;
    const ffn_out = if (use_tp)
        try linearMaybeShardedToReplicated(cb, ffn_gelu, ffn_o_w, ffn_o_b, total, I, H)
    else blk: {
        if (isDenseF32Tensor(cb, ffn_o_w)) {
            if (try cb.linearAdd(ffn_gelu, ffn_o_w, ffn_o_b, attn_normed, total, local_intermediate, H)) |fused| {
                ffn_out_has_residual = true;
                break :blk fused;
            }
        }
        break :blk try cb.linear(ffn_gelu, ffn_o_w, ffn_o_b, total, local_intermediate, H);
    };
    if (profile) |p| p.ffn_output_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const out = if (ffn_out_has_residual) blk: {
        cb.free(attn_normed);
        defer cb.free(ffn_out);
        break :blk try cb.layerNorm(ffn_out, ffn_ln_w, ffn_ln_b, H, eps);
    } else if (try cb.addLayerNorm(ffn_out, attn_normed, ffn_ln_w, ffn_ln_b, H, eps)) |fused| blk: {
        cb.free(attn_normed);
        cb.free(ffn_out);
        break :blk fused;
    } else blk: {
        const ffn_res = try cb.add(ffn_out, attn_normed);
        cb.free(attn_normed);
        cb.free(ffn_out);
        defer cb.free(ffn_res);
        break :blk try cb.layerNorm(ffn_res, ffn_ln_w, ffn_ln_b, H, eps);
    };
    if (profile) |p| p.layernorm_residual_ns += profileElapsed(timer);
    return out;
}

fn getLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "encoder.layer.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

fn tensorParallelWorldSize(cb: *const ComputeBackend) usize {
    if (!build_options.enable_mlx) return 1;
    const mlx_compute = mlx_compute_mod.MlxCompute.fromComputeBackend(cb) orelse return 1;
    if (!mlx_compute.tensorParallelEnabled()) return 1;
    return mlx_compute.tensorParallelWorldSize();
}

fn isDenseF32Tensor(cb: *const ComputeBackend, tensor: CT) bool {
    const dtype = cb.tensorDType(tensor) catch return false;
    return dtype == .f32;
}

fn linearReplicatedToMaybeSharded(
    cb: *const ComputeBackend,
    input: CT,
    weight: CT,
    bias: CT,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
) !CT {
    if (build_options.enable_mlx) {
        if (mlx_compute_mod.MlxCompute.fromComputeBackend(cb)) |mlx_compute| {
            if (mlx_compute.tensorParallelEnabled()) {
                return mlx_compute.linearTensorParallelReplicatedToSharded(input, weight, bias, rows, input_dim, output_dim);
            }
        }
    }
    return cb.linear(input, weight, bias, rows, input_dim, output_dim);
}

fn linearMaybeShardedToReplicated(
    cb: *const ComputeBackend,
    input: CT,
    weight: CT,
    bias: CT,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
) !CT {
    if (build_options.enable_mlx) {
        if (mlx_compute_mod.MlxCompute.fromComputeBackend(cb)) |mlx_compute| {
            if (mlx_compute.tensorParallelEnabled()) {
                return mlx_compute.linearTensorParallelShardedToReplicated(input, weight, bias, rows, input_dim, output_dim);
            }
        }
    }
    return cb.linear(input, weight, bias, rows, input_dim, output_dim);
}

test "allAttentionMaskOnes recognizes all-one prefix" {
    try std.testing.expect(allAttentionMaskOnes(&.{ 1, 1, 1 }, 3));
    try std.testing.expect(allAttentionMaskOnes(&.{ 1, 1, 0 }, 2));
    try std.testing.expect(!allAttentionMaskOnes(&.{ 1, 0, 1 }, 3));
    try std.testing.expect(!allAttentionMaskOnes(&.{ 1, 1 }, 3));
}
