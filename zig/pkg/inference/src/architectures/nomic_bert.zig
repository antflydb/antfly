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

// NomicBERT encoder architecture using abstract ComputeBackend ops.
//
// `nomic-ai/nomic-embed-text-v1.5` uses a NomicBERT encoder rather than a
// conventional BERT block. Its HuggingFace checkpoint has no position table:
// it adds word and type embeddings, applies an embedding layer norm, then
// performs full split-half RoPE attention followed by post-norm SwiGLU blocks.

const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");

const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

fn traceMetalEncoderPrepare(comptime format: []const u8, args: anytype) void {
    if (platform.env.getenvBool("TERMITE_METAL_TRACE_NOMIC_BERT_PREPARE")) {
        std.debug.print("nomic_bert_prepare: " ++ format ++ "\n", args);
    }
}

fn traceMetalEncoderTimingEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return platform.env.getenvBool("TERMITE_METAL_TRACE_NOMIC_BERT_TIMING");
}

fn monotonicNowNs() u128 {
    if (@import("builtin").target.cpu.arch.isWasm()) return 0;
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => 0,
    };
}

fn traceMetalEncoderPhase(phase: []const u8, started_at: u128) u128 {
    if (started_at == 0) return 0;
    const finished_at = monotonicNowNs();
    const elapsed_us = if (finished_at >= started_at) @divTrunc(finished_at - started_at, std.time.ns_per_us) else 0;
    std.debug.print("nomic_bert_timing: phase={s} elapsed_us={d}\n", .{ phase, elapsed_us });
    return finished_at;
}

fn metalEncoderFrameEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_NOMIC_BERT_ENCODER_FRAME");
}

// MPSMatrix is faster than the scalar reduction kernels for NomicBERT's F32
// projections on Apple GPUs. Keep the exact F32 weights and offer an escape
// hatch for platforms where the crossover differs.
fn preferF32MpsLinear() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_NOMIC_BERT_F32_MPS");
}

// The backend-owned layer executor is an orchestration optimization only: it
// uses the same prepared projections and exact kernels as the generic path.
// Keep a one-switch escape hatch so production can immediately fall back to
// the portable composition on a new Metal driver.
fn metalEncoderLayerExecutorEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_NOMIC_BERT_LAYER_EXECUTOR");
}

fn metalPoolNormalizeEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_NOMIC_BERT_POOL_NORMALIZE");
}

fn isOptimizedV15Geometry(config: Config) bool {
    return config.hidden_size == 768 and
        config.num_hidden_layers == 12 and
        config.num_attention_heads == 12 and
        config.intermediate_size == 3072 and
        config.rope_theta == 1000.0;
}

const LinearSlotKind = enum(usize) {
    qkv,
    attention_output,
    fc11,
    fc12,
    fc2,
};

const LayerNormSlotKind = enum(usize) {
    attention_output,
    ffn_output,
};

const linear_specs = [_]struct {
    kind: LinearSlotKind,
    weight: []const u8,
    input_intermediate: bool = false,
    output_intermediate: bool = false,
}{
    .{ .kind = .qkv, .weight = "attn.Wqkv.weight", .output_intermediate = true },
    .{ .kind = .attention_output, .weight = "attn.out_proj.weight" },
    .{ .kind = .fc11, .weight = "mlp.fc11.weight", .output_intermediate = true },
    .{ .kind = .fc12, .weight = "mlp.fc12.weight", .output_intermediate = true },
    .{ .kind = .fc2, .weight = "mlp.fc2.weight", .input_intermediate = true },
};

fn linearSlot(layer: usize, kind: LinearSlotKind) usize {
    return layer * linear_specs.len + @intFromEnum(kind);
}

fn layerNormSlot(layer: usize, kind: LayerNormSlotKind) usize {
    return layer * 2 + @intFromEnum(kind);
}

pub const Config = struct {
    vocab_size: u32 = 30528,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    max_position_embeddings: u32 = 8192,
    type_vocab_size: u32 = 2,
    rope_theta: f32 = 1000.0,
    layer_norm_eps: f32 = 1e-12,
};

pub fn isNomicBertModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "nomic_bert") or
        std.mem.eql(u8, model_type, "nomic-bert");
}

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = Config{};
    if (obj.get("vocab_size")) |value| config.vocab_size = jsonU32(value) orelse config.vocab_size;
    if (obj.get("hidden_size")) |value| config.hidden_size = jsonU32(value) orelse config.hidden_size;
    if (obj.get("n_embd")) |value| config.hidden_size = jsonU32(value) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |value| config.num_hidden_layers = jsonU32(value) orelse config.num_hidden_layers;
    if (obj.get("n_layer")) |value| config.num_hidden_layers = jsonU32(value) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |value| config.num_attention_heads = jsonU32(value) orelse config.num_attention_heads;
    if (obj.get("n_head")) |value| config.num_attention_heads = jsonU32(value) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |value| config.intermediate_size = jsonU32(value) orelse config.intermediate_size;
    if (obj.get("n_inner")) |value| config.intermediate_size = jsonU32(value) orelse config.intermediate_size;
    if (obj.get("max_position_embeddings")) |value| config.max_position_embeddings = jsonU32(value) orelse config.max_position_embeddings;
    // Nomic Embed Text keeps GPT-2's `max_position_embeddings` training
    // horizon (2048) alongside its actual supported sequence limit (8192) in
    // `n_positions`. RoPE has no learned position table to constrain it, so
    // honor the latter when present.
    if (obj.get("n_positions")) |value| config.max_position_embeddings = jsonU32(value) orelse config.max_position_embeddings;
    if (obj.get("type_vocab_size")) |value| config.type_vocab_size = jsonU32(value) orelse config.type_vocab_size;
    if (obj.get("layer_norm_eps")) |value| config.layer_norm_eps = jsonF32(value) orelse config.layer_norm_eps;
    if (obj.get("layer_norm_epsilon")) |value| config.layer_norm_eps = jsonF32(value) orelse config.layer_norm_eps;
    if (obj.get("rotary_emb_base")) |value| config.rope_theta = jsonF32(value) orelse config.rope_theta;
    if (obj.get("rope_parameters")) |value| {
        if (value == .object) {
            if (value.object.get("rope_theta")) |theta| config.rope_theta = jsonF32(theta) orelse config.rope_theta;
        }
    }
    return config;
}

fn jsonU32(value: std.json.Value) ?u32 {
    return switch (value) {
        .integer => |integer| std.math.cast(u32, integer),
        else => null,
    };
}

fn jsonF32(value: std.json.Value) ?f32 {
    return switch (value) {
        .float => |float| @floatCast(float),
        .integer => |integer| @floatFromInt(integer),
        else => null,
    };
}

fn validateInputs(
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) !usize {
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const heads: usize = config.num_attention_heads;
    if (batch == 0 or seq_len == 0 or seq_len > config.max_position_embeddings or
        hidden == 0 or intermediate == 0 or heads == 0 or hidden % heads != 0)
    {
        return error.InvalidShape;
    }
    const total = std.math.mul(usize, batch, seq_len) catch return error.InvalidShape;
    if (total > std.math.maxInt(i32) or input_ids.len != total or attention_mask.len != total) return error.InvalidShape;
    _ = std.math.mul(usize, total, hidden) catch return error.InvalidShape;
    _ = std.math.mul(usize, total, intermediate) catch return error.InvalidShape;
    for (input_ids) |token_id| if (token_id < 0 or token_id >= config.vocab_size) return error.InvalidTokenId;
    for (attention_mask) |value| if (value != 0 and value != 1) return error.InvalidAttentionMask;
    if (token_type_ids) |ids| {
        if (ids.len != total or config.type_vocab_size == 0) return error.InvalidShape;
        for (ids) |id| if (id < 0 or id >= config.type_vocab_size) return error.InvalidTokenTypeId;
    }
    return total;
}

pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const hidden = try forwardCT(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len);
    defer cb.free(hidden);
    return cb.toFloat32(hidden, allocator);
}

pub fn forwardCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    return (try forwardCTImpl(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len, null)) orelse
        error.NomicBertEncoderUnavailable;
}

/// Exact Nomic v1.5 resident route that appends mean pooling and optional L2
/// normalization before submitting the encoder frame. Null is returned only
/// before any frame work is encoded; later failures abort the frame.
pub fn forwardEmbeddingCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
    normalize: bool,
) !?CT {
    // Decline before opening the encoder frame so the benchmark and production
    // rollback switch can safely use the established resident composition.
    if (!metalEncoderLayerExecutorEnabled() or
        !metalPoolNormalizeEnabled() or
        !isOptimizedV15Geometry(config)) return null;
    return forwardCTImpl(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len, normalize);
}

fn forwardCTImpl(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
    embedding_normalize: ?bool,
) !?CT {
    var phase_started_at = if (traceMetalEncoderTimingEnabled()) monotonicNowNs() else 0;
    const total = try validateInputs(config, input_ids, attention_mask, token_type_ids, batch, seq_len);
    phase_started_at = traceMetalEncoderPhase("validate", phase_started_at);
    const resident_slots = try preplanMetalEncoder(cb, allocator, config);
    if (embedding_normalize != null and !resident_slots) return null;
    phase_started_at = traceMetalEncoderPhase("preplan", phase_started_at);
    var frame_active = false;
    if (resident_slots and cb.kind() == .metal and metalEncoderFrameEnabled() and !cb.decoderRuntimeHasActiveFrame()) {
        frame_active = try cb.decoderRuntimeBeginFrame();
    }
    if (embedding_normalize != null and !frame_active) return null;
    phase_started_at = traceMetalEncoderPhase("begin_frame", phase_started_at);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    var hidden = try embeddings(cb, allocator, config, input_ids, token_type_ids, total);
    phase_started_at = traceMetalEncoderPhase("embeddings", phase_started_at);
    errdefer cb.free(hidden);
    for (0..config.num_hidden_layers) |layer| {
        const next = try encoderLayer(cb, config, hidden, attention_mask, batch, seq_len, layer, resident_slots);
        cb.free(hidden);
        hidden = next;
    }
    phase_started_at = traceMetalEncoderPhase("layers", phase_started_at);

    if (embedding_normalize) |normalize| {
        const pooled = (try cb.nomicBertPoolNormalize(&.{
            .hidden = hidden,
            .attention_mask = attention_mask,
            .batch = batch,
            .seq_len = seq_len,
            .hidden_size = config.hidden_size,
            .normalize = normalize,
        })) orelse return error.NomicBertPoolNormalizeUnavailable;
        cb.free(hidden);
        hidden = pooled;
        phase_started_at = traceMetalEncoderPhase("pool_normalize", phase_started_at);
    }

    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }
    _ = traceMetalEncoderPhase("submit_wait", phase_started_at);
    return hidden;
}

fn preplanMetalEncoder(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
) !bool {
    if (cb.kind() != .metal) {
        traceMetalEncoderPrepare("decline=backend", .{});
        return false;
    }
    if (!metalEncoderFrameEnabled()) {
        traceMetalEncoderPrepare("decline=frame_disabled", .{});
        return false;
    }
    if (cb.decoderRuntimeHasActiveFrame()) {
        traceMetalEncoderPrepare("decline=frame_active", .{});
        return false;
    }
    const layers: usize = config.num_hidden_layers;
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    if (layers == 0 or hidden == 0 or intermediate == 0) {
        traceMetalEncoderPrepare("decline=invalid_geometry layers={d} hidden={d} intermediate={d}", .{ layers, hidden, intermediate });
        return false;
    }
    if (slotsPrepared(cb, config)) {
        traceMetalEncoderPrepare("ready=already_prepared", .{});
        return true;
    }

    const zero_hidden_bias = try makeZeroBias(cb, allocator, config.hidden_size);
    defer cb.free(zero_hidden_bias);
    const zero_intermediate_bias = try makeZeroBias(cb, allocator, config.intermediate_size);
    defer cb.free(zero_intermediate_bias);
    const zero_qkv_bias = try makeZeroBias(cb, allocator, 3 * @as(usize, config.hidden_size));
    defer cb.free(zero_qkv_bias);

    for (0..layers) |layer| {
        for (linear_specs) |spec| {
            const in_dim = if (spec.input_intermediate) intermediate else hidden;
            const out_dim = if (spec.output_intermediate) if (spec.kind == .qkv) hidden * 3 else intermediate else hidden;
            var name_buf: [128]u8 = undefined;
            const weight = try getLayerWeight(cb, layer, spec.weight, &name_buf);
            defer cb.free(weight);
            const bias = switch (spec.kind) {
                .qkv => zero_qkv_bias,
                .fc11, .fc12 => zero_intermediate_bias,
                .attention_output, .fc2 => zero_hidden_bias,
            };
            if (!(try cb.decoderRuntimePrepareLinear(&.{
                .slot = linearSlot(layer, spec.kind),
                .weight = weight,
                .bias = bias,
                .in_dim = in_dim,
                .out_dim = out_dim,
                .retain_dense_fallback = false,
                .prefer_f32_mps_fallback = preferF32MpsLinear(),
            }))) {
                traceMetalEncoderPrepare("decline=linear layer={d} slot={d} in={d} out={d}", .{ layer, linearSlot(layer, spec.kind), in_dim, out_dim });
                return false;
            }
        }

        var name_buf: [128]u8 = undefined;
        const norm1_weight = try getLayerWeight(cb, layer, "norm1.weight", &name_buf);
        defer cb.free(norm1_weight);
        const norm1_bias = try getLayerWeight(cb, layer, "norm1.bias", &name_buf);
        defer cb.free(norm1_bias);
        if (!(try cb.decoderRuntimePrepareLayerNorm(&.{
            .slot = layerNormSlot(layer, .attention_output),
            .weight = norm1_weight,
            .bias = norm1_bias,
            .hidden_size = hidden,
        }))) {
            traceMetalEncoderPrepare("decline=norm1 layer={d} slot={d} hidden={d}", .{ layer, layerNormSlot(layer, .attention_output), hidden });
            return false;
        }

        const norm2_weight = try getLayerWeight(cb, layer, "norm2.weight", &name_buf);
        defer cb.free(norm2_weight);
        const norm2_bias = try getLayerWeight(cb, layer, "norm2.bias", &name_buf);
        defer cb.free(norm2_bias);
        if (!(try cb.decoderRuntimePrepareLayerNorm(&.{
            .slot = layerNormSlot(layer, .ffn_output),
            .weight = norm2_weight,
            .bias = norm2_bias,
            .hidden_size = hidden,
        }))) {
            traceMetalEncoderPrepare("decline=norm2 layer={d} slot={d} hidden={d}", .{ layer, layerNormSlot(layer, .ffn_output), hidden });
            return false;
        }
    }
    traceMetalEncoderPrepare("ready=prepared", .{});
    return true;
}

fn slotsPrepared(cb: *const ComputeBackend, config: Config) bool {
    const layers: usize = config.num_hidden_layers;
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    for (0..layers) |layer| {
        for (linear_specs) |spec| {
            const in_dim = if (spec.input_intermediate) intermediate else hidden;
            const out_dim = if (spec.output_intermediate) if (spec.kind == .qkv) hidden * 3 else intermediate else hidden;
            if (!cb.decoderRuntimeLinearSlotPrepared(linearSlot(layer, spec.kind), in_dim, out_dim)) return false;
        }
        if (!cb.decoderRuntimeLayerNormSlotPrepared(layerNormSlot(layer, .attention_output), hidden)) return false;
        if (!cb.decoderRuntimeLayerNormSlotPrepared(layerNormSlot(layer, .ffn_output), hidden)) return false;
    }
    return true;
}

fn embeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    token_type_ids: ?[]const i64,
    total: usize,
) !CT {
    const H: usize = config.hidden_size;
    const word = try cb.getWeight("embeddings.word_embeddings.weight");
    defer cb.free(word);
    var hidden = try cb.embeddingLookup(word, input_ids, total, H);

    if (config.type_vocab_size > 0) {
        const types = try cb.getWeight("embeddings.token_type_embeddings.weight");
        defer cb.free(types);
        const ids = try allocator.alloc(i64, total);
        defer allocator.free(ids);
        if (token_type_ids) |provided| @memcpy(ids, provided) else @memset(ids, 0);
        const lookup = try cb.embeddingLookup(types, ids, total, H);
        defer cb.free(lookup);
        const with_types = try cb.add(hidden, lookup);
        cb.free(hidden);
        hidden = with_types;
    }

    const weight = try cb.getWeight("emb_ln.weight");
    defer cb.free(weight);
    const bias = try cb.getWeight("emb_ln.bias");
    defer cb.free(bias);
    const normalized = try cb.layerNorm(hidden, weight, bias, H, config.layer_norm_eps);
    cb.free(hidden);
    return normalized;
}

fn encoderLayer(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
    resident_slots: bool,
) !CT {
    const H: usize = config.hidden_size;
    const heads: usize = config.num_attention_heads;
    const head_dim = H / heads;
    const intermediate: usize = config.intermediate_size;
    const total = batch * seq_len;
    var name_buf: [128]u8 = undefined;

    if (resident_slots and metalEncoderLayerExecutorEnabled()) {
        if (try cb.nomicBertEncoderLayer(&.{
            .layer_index = layer,
            .layer = .{
                .qkv_linear_slot = linearSlot(layer, .qkv),
                .attention_output_linear_slot = linearSlot(layer, .attention_output),
                .fc11_linear_slot = linearSlot(layer, .fc11),
                .fc12_linear_slot = linearSlot(layer, .fc12),
                .fc2_linear_slot = linearSlot(layer, .fc2),
                .attention_layer_norm_slot = layerNormSlot(layer, .attention_output),
                .ffn_layer_norm_slot = layerNormSlot(layer, .ffn_output),
            },
            .hidden = hidden,
            .attention_mask = attention_mask,
            .batch = batch,
            .seq_len = seq_len,
            .hidden_size = H,
            .intermediate_size = intermediate,
            .num_attention_heads = heads,
            .head_dim = head_dim,
            .rope_theta = config.rope_theta,
            .norm_eps = config.layer_norm_eps,
        })) |output| return output;
    }

    const qkv_w = try getLayerWeight(cb, layer, "attn.Wqkv.weight", &name_buf);
    defer cb.free(qkv_w);
    const qkv = try linearNoBiasWithSlot(cb, hidden, qkv_w, total, H, H * 3, if (resident_slots) linearSlot(layer, .qkv) else null);
    defer cb.free(qkv);
    const q = try cb.sliceLastDim(qkv, 0, H);
    defer cb.free(q);
    const k = try cb.sliceLastDim(qkv, H, H * 2);
    defer cb.free(k);
    const v = try cb.sliceLastDim(qkv, H * 2, H * 3);
    defer cb.free(v);
    const rope_q = try cb.rope(q, seq_len, head_dim, head_dim, config.rope_theta, 1.0, 0, false);
    defer cb.free(rope_q);
    const rope_k = try cb.rope(k, seq_len, head_dim, head_dim, config.rope_theta, 1.0, 0, false);
    defer cb.free(rope_k);
    const attended = try cb.scaledDotProductAttention(rope_q, rope_k, v, attention_mask, null, batch, seq_len, heads, head_dim);
    defer cb.free(attended);

    const attn_out_w = try getLayerWeight(cb, layer, "attn.out_proj.weight", &name_buf);
    defer cb.free(attn_out_w);
    const norm1_w = try getLayerWeight(cb, layer, "norm1.weight", &name_buf);
    defer cb.free(norm1_w);
    const norm1_b = try getLayerWeight(cb, layer, "norm1.bias", &name_buf);
    defer cb.free(norm1_b);
    const after_attn = blk: {
        if (resident_slots) {
            if (try cb.decoderRuntimeApplyLinearLayerNorm(&.{
                .linear_slot = linearSlot(layer, .attention_output),
                .layer_norm_slot = layerNormSlot(layer, .attention_output),
                .input = attended,
                .residual = hidden,
                .in_dim = H,
                .hidden_size = H,
                .eps = config.layer_norm_eps,
            })) |fused| break :blk fused;
        }
        const attn_out = try linearNoBiasWithSlot(cb, attended, attn_out_w, total, H, H, if (resident_slots) linearSlot(layer, .attention_output) else null);
        defer cb.free(attn_out);
        if (try cb.addLayerNorm(attn_out, hidden, norm1_w, norm1_b, H, config.layer_norm_eps)) |fused| break :blk fused;
        const residual_attn = try cb.add(attn_out, hidden);
        defer cb.free(residual_attn);
        break :blk try cb.layerNorm(residual_attn, norm1_w, norm1_b, H, config.layer_norm_eps);
    };
    defer cb.free(after_attn);

    const fc11_w = try getLayerWeight(cb, layer, "mlp.fc11.weight", &name_buf);
    defer cb.free(fc11_w);
    const fc12_w = try getLayerWeight(cb, layer, "mlp.fc12.weight", &name_buf);
    defer cb.free(fc12_w);
    const pair = try linearPairWithSlots(
        cb,
        after_attn,
        fc11_w,
        fc12_w,
        total,
        H,
        intermediate,
        if (resident_slots) linearSlot(layer, .fc11) else null,
        if (resident_slots) linearSlot(layer, .fc12) else null,
    );
    defer cb.free(pair.first);
    defer cb.free(pair.second);
    // Nomic's gated MLP computes fc11(x) * SiLU(fc12(x)).
    const gated = (try cb.activationMultiply(pair.second, pair.first, .silu)) orelse blk: {
        const activated = try cb.silu(pair.second);
        defer cb.free(activated);
        break :blk try cb.multiply(pair.first, activated);
    };
    defer cb.free(gated);

    const fc2_w = try getLayerWeight(cb, layer, "mlp.fc2.weight", &name_buf);
    defer cb.free(fc2_w);
    const ffn_out = try linearNoBiasWithSlot(cb, gated, fc2_w, total, intermediate, H, if (resident_slots) linearSlot(layer, .fc2) else null);
    defer cb.free(ffn_out);
    const norm2_w = try getLayerWeight(cb, layer, "norm2.weight", &name_buf);
    defer cb.free(norm2_w);
    const norm2_b = try getLayerWeight(cb, layer, "norm2.bias", &name_buf);
    defer cb.free(norm2_b);
    if (try cb.addLayerNorm(ffn_out, after_attn, norm2_w, norm2_b, H, config.layer_norm_eps)) |fused| {
        return fused;
    }
    const residual_ffn = try cb.add(ffn_out, after_attn);
    defer cb.free(residual_ffn);
    return cb.layerNorm(residual_ffn, norm2_w, norm2_b, H, config.layer_norm_eps);
}

fn linearNoBiasWithSlot(
    cb: *const ComputeBackend,
    input: CT,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    slot: ?usize,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLinear(&.{ .slot = prepared_slot, .input = input, .in_dim = in_dim, .out_dim = out_dim })) |output| return output;
    }
    return cb.linearNoBias(input, weight, rows, in_dim, out_dim);
}

fn linearPairWithSlots(
    cb: *const ComputeBackend,
    input: CT,
    first_weight: CT,
    second_weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    first_slot: ?usize,
    second_slot: ?usize,
) !ops.LinearNoBiasPairResult {
    if (first_slot) |slot_a| if (second_slot) |slot_b| {
        if (try cb.decoderRuntimeApplyLinearPair(&.{ .slot_a = slot_a, .slot_b = slot_b, .input = input, .in_dim = in_dim, .out_dim = out_dim })) |pair| return pair;
    };
    return cb.linearNoBiasPair(input, first_weight, second_weight, rows, in_dim, out_dim);
}

fn makeZeroBias(cb: *const ComputeBackend, allocator: std.mem.Allocator, dim: usize) !CT {
    const values = try allocator.alloc(f32, dim);
    defer allocator.free(values);
    @memset(values, 0);
    return cb.fromFloat32Shape(values, &.{@intCast(dim)});
}

fn getLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[128]u8) !CT {
    const name = std.fmt.bufPrint(buf, "encoder.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

test "NomicBERT config parses the public embed-text architecture" {
    const config = try parseConfig(std.testing.allocator,
        \\{"model_type":"nomic_bert","n_embd":768,"n_layer":12,"n_head":12,"n_inner":3072,"n_positions":8192,"vocab_size":30528,"type_vocab_size":2,"layer_norm_eps":1e-12,"rotary_emb_base":1000}
    );
    try std.testing.expectEqual(@as(u32, 768), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 12), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 3072), config.intermediate_size);
    try std.testing.expectEqual(@as(u32, 8192), config.max_position_embeddings);
    try std.testing.expectEqual(@as(f32, 1000.0), config.rope_theta);
}
