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

// ModernBERT encoder architecture using abstract ComputeBackend ops.
//
// ModernBERT (Warner et al., 2024) is a modernised BERT-family encoder with:
//   - Pre-norm (LayerNorm before each sub-layer, not after)
//   - RoPE positional encoding applied per-layer (no absolute position embeddings)
//   - GeGLU feed-forward networks
//   - Alternating global (full) and local (sliding-window) self-attention
//
// HuggingFace ModernBERT safetensors use bare keys such as
// `embeddings.tok_embeddings.weight` and `layers.N.attn.Wqkv.weight`.
// Session loading canonicalizes that inventory under a `model.` prefix so it
// can coexist with the original fused-chunker checkpoint convention.
//
// Single implementation works with any ComputeBackend (native, etc).

const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const native_compute = @import("../ops/native_compute.zig");
const tensor_mod = @import("../backends/tensor.zig");
const weight_source = @import("../models/weight_source.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

fn metalEncoderFrameEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_MODERNBERT_ENCODER_FRAME");
}

const MetalTrace = struct {
    enabled: bool = false,
    per_layer: bool = false,
    started_ns: u64 = 0,
    started: ops.NativeQuantTimingStats = .{},

    fn init(cb: *const ComputeBackend) MetalTrace {
        if (cb.kind() != .metal) return .{};
        const aggregate = platform.env.getenvBool("TERMITE_METAL_TRACE_MODERNBERT_STAGES");
        const per_layer = platform.env.getenvBool("TERMITE_METAL_TRACE_MODERNBERT_LAYERS");
        if (!aggregate and !per_layer) return .{};
        return .{
            .enabled = true,
            .per_layer = per_layer,
            .started_ns = platform.time.monotonicNs(),
            .started = cb.debugTimingSnapshot().provider,
        };
    }

    fn now(self: MetalTrace) u64 {
        return if (self.per_layer) platform.time.monotonicNs() else 0;
    }

    fn snapshot(self: MetalTrace, cb: *const ComputeBackend) ops.NativeQuantTimingStats {
        return if (self.per_layer) cb.debugTimingSnapshot().provider else .{};
    }

    fn emitLayer(
        self: MetalTrace,
        cb: *const ComputeBackend,
        layer: usize,
        started_ns: u64,
        before: ops.NativeQuantTimingStats,
    ) void {
        if (!self.per_layer) return;
        const after = cb.debugTimingSnapshot().provider;
        std.debug.print(
            "metal_modernbert_layer_profile altered_cadence=true layer={d} host_ms={d:.3} frame_begins={d} submissions={d} wait_ms={d:.3} gpu_ms={d:.3} linear_calls={d}\n",
            .{
                layer,
                @as(f64, @floatFromInt(platform.time.monotonicNs() -| started_ns)) / 1.0e6,
                after.decoder_runtime_frame_begins -| before.decoder_runtime_frame_begins,
                after.decoder_runtime_frame_submits -| before.decoder_runtime_frame_submits,
                @as(f64, @floatFromInt(after.decoder_runtime_frame_wait_nanos -| before.decoder_runtime_frame_wait_nanos)) / 1.0e6,
                @as(f64, @floatFromInt(after.decoder_runtime_frame_gpu_nanos -| before.decoder_runtime_frame_gpu_nanos)) / 1.0e6,
                after.decoder_runtime_apply_linear_calls -| before.decoder_runtime_apply_linear_calls,
            },
        );
    }

    fn emitAggregate(self: MetalTrace, cb: *const ComputeBackend, tokens: usize, layers: usize) void {
        if (!self.enabled) return;
        const after = cb.debugTimingSnapshot().provider;
        std.debug.print(
            "metal_modernbert_profile altered_cadence={s} tokens={d} layers={d} host_ms={d:.3} frame_begins={d} submissions={d} wait_ms={d:.3} gpu_ms={d:.3} linear_calls={d} provider_lifetime_device_peak_bytes={d}\n",
            .{
                if (self.per_layer) "true" else "false",
                tokens,
                layers,
                @as(f64, @floatFromInt(platform.time.monotonicNs() -| self.started_ns)) / 1.0e6,
                after.decoder_runtime_frame_begins -| self.started.decoder_runtime_frame_begins,
                after.decoder_runtime_frame_submits -| self.started.decoder_runtime_frame_submits,
                @as(f64, @floatFromInt(after.decoder_runtime_frame_wait_nanos -| self.started.decoder_runtime_frame_wait_nanos)) / 1.0e6,
                @as(f64, @floatFromInt(after.decoder_runtime_frame_gpu_nanos -| self.started.decoder_runtime_frame_gpu_nanos)) / 1.0e6,
                after.decoder_runtime_apply_linear_calls -| self.started.decoder_runtime_apply_linear_calls,
                after.metal_tensor_device_owned_peak_live_bytes,
            },
        );
    }
};

/// Hugging Face ModernBERT combines Q/K/V and uses four bias-free linears per
/// layer. Keep those weights in fixed provider-owned slots rather than letting
/// each request allocate dynamic slots and upload the same matrices again.
///
/// These slots are intentionally scoped to the ModernBERT Metal provider. A
/// session owns its provider, and request-local MetalCompute wrappers only
/// validate and reuse this metadata, following the BERT encoder pattern.
const ModernBertLinearSlotKind = enum(usize) {
    qkv,
    attention_output,
    ffn_in,
    ffn_out,
};

const modern_bert_linear_specs = [_]struct {
    kind: ModernBertLinearSlotKind,
    weight: []const u8,
    input_intermediate: bool = false,
    output_intermediate: bool = false,
}{
    .{ .kind = .qkv, .weight = "attn.Wqkv.weight", .output_intermediate = true },
    .{ .kind = .attention_output, .weight = "attn.Wo.weight" },
    .{ .kind = .ffn_in, .weight = "mlp.Wi.weight", .output_intermediate = true },
    .{ .kind = .ffn_out, .weight = "mlp.Wo.weight", .input_intermediate = true },
};

fn modernBertLinearSlot(layer: usize, kind: ModernBertLinearSlotKind) usize {
    return layer * modern_bert_linear_specs.len + @backingInt(kind);
}

const ModernBertNormSlotKind = enum(u1) { attention, mlp };

/// Fixed LayerNorm slots: two per layer, then the embedding and final norms.
/// Like the linear slots they stay attached to the model's Metal provider, so
/// requests after the first upload no norm weights.
fn modernBertNormSlot(layer: usize, kind: ModernBertNormSlotKind) usize {
    return layer * 2 + @backingInt(kind);
}
fn modernBertEmbeddingNormSlot(config: Config) usize {
    return @as(usize, @intCast(config.num_hidden_layers)) * 2;
}
fn modernBertFinalNormSlot(config: Config) usize {
    return @as(usize, @intCast(config.num_hidden_layers)) * 2 + 1;
}

/// `layerNorm` through a prepared fixed slot, or null to take the generic path.
fn slottedLayerNorm(cb: *const ComputeBackend, input: CT, slot: ?usize, config: Config) !?CT {
    const prepared = slot orelse return null;
    return cb.decoderRuntimeApplyLayerNorm(&.{ .slot = prepared, .input = input, .hidden_size = @intCast(config.hidden_size), .eps = config.layer_norm_eps });
}

fn metalModernBertEncoderSlotsPrepared(cb: *const ComputeBackend, config: Config) bool {
    if (config.checkpoint_layout != .huggingface_fused_qkv_no_bias) return false;

    const layer_count: usize = @intCast(config.num_hidden_layers);
    const hidden: usize = @intCast(config.hidden_size);
    const intermediate: usize = @intCast(config.intermediate_size);
    for (0..layer_count) |layer| {
        for (modern_bert_linear_specs) |spec| {
            const input_dim = if (spec.input_intermediate) intermediate else hidden;
            const output_dim = if (spec.output_intermediate)
                if (spec.kind == .qkv) hidden * 3 else intermediate * 2
            else
                hidden;
            if (!cb.decoderRuntimeLinearSlotPrepared(
                modernBertLinearSlot(layer, spec.kind),
                input_dim,
                output_dim,
            )) return false;
        }
        if (layer > 0 and !cb.decoderRuntimeLayerNormSlotPrepared(modernBertNormSlot(layer, .attention), hidden)) return false;
        if (!cb.decoderRuntimeLayerNormSlotPrepared(modernBertNormSlot(layer, .mlp), hidden)) return false;
    }
    return cb.decoderRuntimeLayerNormSlotPrepared(modernBertEmbeddingNormSlot(config), hidden) and
        cb.decoderRuntimeLayerNormSlotPrepared(modernBertFinalNormSlot(config), hidden);
}

fn preplanMetalModernBertEncoder(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_zero_bias: CT,
) !bool {
    if (cb.kind() != .metal or
        !metalEncoderFrameEnabled() or
        cb.decoderRuntimeHasActiveFrame() or
        config.checkpoint_layout != .huggingface_fused_qkv_no_bias) return false;

    const layer_count: usize = @intCast(config.num_hidden_layers);
    const hidden: usize = @intCast(config.hidden_size);
    const intermediate: usize = @intCast(config.intermediate_size);
    const heads: usize = @intCast(config.num_attention_heads);
    if (layer_count == 0 or hidden == 0 or intermediate == 0 or heads == 0 or hidden % heads != 0) return false;

    // This metadata check is allocation-free. On every request after the
    // first it avoids even loading the 88 projection weights from safetensors.
    if (metalModernBertEncoderSlotsPrepared(cb, config)) return true;

    // The measured 1B encoder path keeps packed checkpoint weights,
    // but prepare reusable F16 MPS matrices for encoder-shaped GEMMs. Bound
    // the shape and per-matrix staging (largest matrix is 52.5 MiB in F32;
    // all 112 resident F16 matrices total 1,875,378,176 bytes). The override
    // retains the direct-quant path for controlled comparisons.
    const prefer_f16_mps = config.metal_f16_weight_mirrors and hidden == 1792 and intermediate == 3840 and
        layer_count == 28 and heads == 28 and
        platform.env.getenvBoolDefault("TERMITE_METAL_MODERNBERT_1B_F16_MIRRORS", true);

    const qkv_zero_bias = try makeZeroBias(cb, allocator, hidden * 3);
    defer cb.free(qkv_zero_bias);
    const ffn_in_zero_bias = try makeZeroBias(cb, allocator, intermediate * 2);
    defer cb.free(ffn_in_zero_bias);

    for (0..layer_count) |layer| {
        for (modern_bert_linear_specs) |spec| {
            const input_dim = if (spec.input_intermediate) intermediate else hidden;
            const output_dim = if (spec.output_intermediate)
                if (spec.kind == .qkv) hidden * 3 else intermediate * 2
            else
                hidden;
            var name_buf: [256]u8 = undefined;
            const weight = try getLayerWeight(cb, layer, spec.weight, &name_buf);
            defer cb.free(weight);
            const bias = switch (spec.kind) {
                .qkv => qkv_zero_bias,
                .ffn_in => ffn_in_zero_bias,
                .attention_output, .ffn_out => hidden_zero_bias,
            };
            if (!(try cb.decoderRuntimePrepareLinear(&.{
                .slot = modernBertLinearSlot(layer, spec.kind),
                .weight = weight,
                .bias = bias,
                .in_dim = input_dim,
                .out_dim = output_dim,
                // Native F16 safetensors reach Metal directly through the
                // prepare path. No F32 mirror is required for this layout.
                .retain_dense_fallback = prefer_f16_mps,
                .dense_fallback_max_bytes = if (prefer_f16_mps) 64 * 1024 * 1024 else null,
                .allow_direct_quant_fallback = prefer_f16_mps,
                .prefer_f16_mps_fallback = prefer_f16_mps,
                // MPS GEMM outruns the hand-written dense kernels here
                // (Laya-large 3.1x, OpenDecider-nano 1.4x on M4 Max); BF16
                // weights are expanded to F32 for it.
                .prefer_f32_mps_fallback = !prefer_f16_mps and !platform.env.getenvBool("TERMITE_METAL_DISABLE_MODERNBERT_F32_MPS"),
            }))) return false;
        }
    }
    for (0..layer_count) |layer| {
        for ([_]ModernBertNormSlotKind{ .attention, .mlp }) |kind| {
            // HuggingFace ModernBERT makes the layer-0 attention norm an identity.
            if (layer == 0 and kind == .attention) continue;
            var name_buf: [256]u8 = undefined;
            const weight = try getLayerWeight(cb, layer, if (kind == .attention) "attn_norm.weight" else "mlp_norm.weight", &name_buf);
            defer cb.free(weight);
            if (!(try cb.decoderRuntimePrepareLayerNorm(&.{ .slot = modernBertNormSlot(layer, kind), .weight = weight, .bias = hidden_zero_bias, .hidden_size = hidden }))) return false;
        }
    }
    for ([_]struct { name: []const u8, slot: usize }{
        .{ .name = "model.embeddings.norm.weight", .slot = modernBertEmbeddingNormSlot(config) },
        .{ .name = "model.final_norm.weight", .slot = modernBertFinalNormSlot(config) },
    }) |norm| {
        const weight = try cb.getWeight(norm.name);
        defer cb.free(weight);
        if (!(try cb.decoderRuntimePrepareLayerNorm(&.{ .slot = norm.slot, .weight = weight, .bias = hidden_zero_bias, .hidden_size = hidden }))) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub const Config = struct {
    /// Execution preference enabled by the measured GLiNER decision path.
    /// Checkpoint parsing leaves other ModernBERT callers unchanged.
    metal_f16_weight_mirrors: bool = false,
    laya: ?@import("../models/laya.zig").Config = null,
    vocab_size: u32 = 50368,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 22,
    num_attention_heads: u32 = 12,
    /// GeGLU inner dimension.  Wi projects hidden → 2*intermediate_size, then
    /// we split the output, apply GELU to the gate half, multiply, and project
    /// the resulting [total, intermediate_size] down via Wo.
    intermediate_size: u32 = 1152,
    max_position_embeddings: u32 = 8192,
    /// RoPE theta for global (full) attention layers.
    global_rope_theta: f32 = 160000.0,
    /// RoPE theta for local (sliding-window) attention layers.
    local_rope_theta: f32 = 10000.0,
    /// Layers whose index is divisible by this value use full attention.
    /// All other layers use sliding-window (local) attention.
    global_attn_every_n_layers: u32 = 3,
    /// Full sliding-window width: each query attends ±(local_attention_window/2) tokens.
    local_attention_window: u32 = 128,
    layer_norm_eps: f32 = 1e-5,
    /// HuggingFace ModernBERT applies RoPE to the first and second halves of
    /// each head, while the legacy fused-chunker checkpoints use consecutive
    /// rotation pairs.
    rope_interleaved: bool = true,
    use_geglu: bool = true,
    /// LoRA rank for query_proj and value_proj.  0 = LoRA disabled.
    /// When non-zero the encoder tries to load lora_a/lora_b weight tensors
    /// from the active WeightStore and uses linearLoRA for Q/V projections.
    lora_rank: u32 = 0,
    /// LoRA scaling alpha.  The effective scale applied to the LoRA delta is
    /// alpha / rank.  Defaults to rank (i.e., scale = 1.0) when 0 is passed.
    lora_alpha: f32 = 0.0,
    /// The original fused-chunker checkpoint stored independent biased Q/K/V
    /// projections. HuggingFace ModernBERT checkpoints instead have a single
    /// bias-free Wqkv tensor and bias-free norms/output projection.
    checkpoint_layout: CheckpointLayout = .separate_qkv_with_bias,
};

pub const CheckpointLayout = enum {
    separate_qkv_with_bias,
    huggingface_fused_qkv_no_bias,
};

pub fn isModernBertModel(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "modernbert") or
        std.mem.eql(u8, model_type, "modern_bert");
}

/// Parse the structural subset of HuggingFace's ModernBERT config needed by
/// the eager encoder. The public embedding checkpoints use the fused,
/// bias-free layout.
pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidModernBertConfig;
    const obj = parsed.value.object;
    var config = Config{};
    if (obj.get("laya")) |value| config.laya = try @import("../models/laya.zig").Config.parse(value);
    if (obj.get("vocab_size")) |value| config.vocab_size = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("hidden_size")) |value| config.hidden_size = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("num_hidden_layers")) |value| config.num_hidden_layers = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("num_attention_heads")) |value| config.num_attention_heads = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("intermediate_size")) |value| config.intermediate_size = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("max_position_embeddings")) |value| config.max_position_embeddings = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("global_attn_every_n_layers")) |value| config.global_attn_every_n_layers = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("local_attention")) |value| config.local_attention_window = jsonU32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("global_rope_theta")) |value| config.global_rope_theta = jsonF32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("local_rope_theta")) |value| config.local_rope_theta = jsonF32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("layer_norm_eps")) |value| config.layer_norm_eps = jsonF32(value) orelse return error.InvalidModernBertConfig;
    if (obj.get("norm_eps")) |value| config.layer_norm_eps = jsonF32(value) orelse return error.InvalidModernBertConfig;
    // Transformers 5 nests the rope settings per layer type (Ettin, for
    // example, uses 160000 for sliding layers too) and lists each layer's
    // type instead of `global_attn_every_n_layers`.
    if (obj.get("rope_parameters")) |value| {
        if (value != .object) return error.InvalidModernBertConfig;
        config.global_rope_theta = try ropeTheta(value.object, "full_attention");
        config.local_rope_theta = try ropeTheta(value.object, "sliding_attention");
    }
    if (obj.get("layer_types")) |value| try checkLayerTypes(value, config);

    // `modernbert` is Transformers' public checkpoint layout. Keep the
    // historical layout available to the fused-chunker training code.
    if (obj.get("model_type")) |value| {
        if (value != .string or !isModernBertModel(value.string)) return error.InvalidModernBertConfig;
        config.checkpoint_layout = .huggingface_fused_qkv_no_bias;
        // Transformers' `rotate_half` layout is split-half, not
        // consecutive (interleaved) pairs.
        config.rope_interleaved = false;
        try requireOptionalString(obj, "hidden_activation", "gelu");
        // `position_embedding_type` is not checked: Transformers' ModernBERT
        // neither defines nor reads it and always applies RoPE. Checkpoints
        // carry leftover values ("absolute" in answerdotai/ModernBERT-base
        // and Laya, "sans_pos" in Ettin) that do not change the model.
        try requireOptionalBool(obj, "attention_bias", false);
        try requireOptionalBool(obj, "mlp_bias", false);
        try requireOptionalBool(obj, "norm_bias", false);
    }
    if (config.laya) |laya| {
        if (config.hidden_size < 64 or config.hidden_size % 64 != 0 or config.num_attention_heads == 0 or config.hidden_size % config.num_attention_heads != 0 or config.num_hidden_layers == 0 or laya.max_len > config.max_position_embeddings) return error.InvalidLayaConfig;
    }
    try validateConfig(config);
    return config;
}

/// Validate the geometry shared by CPU, Metal, and CUDA execution. Keeping
/// this separate from JSON parsing also protects callers that construct a
/// Config directly (training fixtures and embedded manifests do both).
pub fn validateConfig(config: Config) !void {
    if (config.vocab_size == 0 or
        config.hidden_size == 0 or
        config.num_hidden_layers == 0 or
        config.num_attention_heads == 0 or
        config.intermediate_size == 0 or
        config.max_position_embeddings == 0 or
        config.global_attn_every_n_layers == 0 or
        config.local_attention_window == 0 or
        config.hidden_size % config.num_attention_heads != 0)
    {
        return error.InvalidModernBertConfig;
    }
    // Every supported ModernBERT checkpoint applies full-head RoPE. Both the
    // interleaved and split-half layouts require an even head dimension.
    const head_dim = config.hidden_size / config.num_attention_heads;
    if (head_dim < 2 or head_dim % 2 != 0) return error.InvalidModernBertConfig;
    if (!std.math.isFinite(config.global_rope_theta) or config.global_rope_theta <= 0 or
        !std.math.isFinite(config.local_rope_theta) or config.local_rope_theta <= 0 or
        !std.math.isFinite(config.layer_norm_eps) or config.layer_norm_eps <= 0 or
        !std.math.isFinite(config.lora_alpha) or config.lora_alpha < 0)
        return error.InvalidModernBertConfig;
}

fn ropeTheta(params: std.json.ObjectMap, layer_type: []const u8) !f32 {
    const entry = params.get(layer_type) orelse return error.InvalidModernBertConfig;
    if (entry != .object) return error.InvalidModernBertConfig;
    var fields = entry.object.iterator();
    while (fields.next()) |field| {
        if (!std.mem.eql(u8, field.key_ptr.*, "rope_theta") and !std.mem.eql(u8, field.key_ptr.*, "rope_type"))
            return error.UnsupportedModernBertRope;
    }
    if (entry.object.get("rope_type")) |kind| {
        if (kind != .string or !std.mem.eql(u8, kind.string, "default")) return error.UnsupportedModernBertRope;
    }
    const value = entry.object.get("rope_theta") orelse return error.InvalidModernBertConfig;
    return jsonF32(value) orelse return error.InvalidModernBertConfig;
}

fn requireOptionalString(obj: std.json.ObjectMap, key: []const u8, expected: []const u8) !void {
    const value = obj.get(key) orelse return;
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.UnsupportedModernBertConfig;
}

fn requireOptionalBool(obj: std.json.ObjectMap, key: []const u8, expected: bool) !void {
    const value = obj.get(key) orelse return;
    if (value != .bool or value.bool != expected) return error.UnsupportedModernBertConfig;
}

/// The encoder places full attention on every `global_attn_every_n_layers`-th
/// layer; refuse any other pattern rather than run it wrongly.
fn checkLayerTypes(value: std.json.Value, config: Config) !void {
    if (value != .array or value.array.items.len != config.num_hidden_layers or config.global_attn_every_n_layers == 0) return error.UnsupportedModernBertLayerTypes;
    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.UnsupportedModernBertLayerTypes;
        const want: []const u8 = if (i % config.global_attn_every_n_layers == 0) "full_attention" else "sliding_attention";
        if (!std.mem.eql(u8, item.string, want)) return error.UnsupportedModernBertLayerTypes;
    }
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

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Run the full ModernBERT encoder forward pass.
/// Returns an owned f32 slice of shape [batch * seq_len * hidden_size].
/// Caller must free the returned slice with `allocator.free`.
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    /// 1 = real token, 0 = padding; flat shape [batch * seq_len].
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const result_ct = try forwardCT(cb, allocator, config, input_ids, attention_mask, batch, seq_len);
    defer cb.free(result_ct);
    return cb.toFloat32(result_ct, allocator);
}

/// Run the full ModernBERT encoder forward pass and return a CT.
/// Caller owns the returned tensor and must free it with `cb.free`.
pub fn forwardCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    /// 1 = real token, 0 = padding; flat shape [batch * seq_len].
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    try validateConfig(config);
    const total = std.math.mul(usize, batch, seq_len) catch return error.InvalidInputShape;
    if (batch == 0 or seq_len == 0 or seq_len > config.max_position_embeddings or input_ids.len != total or attention_mask.len != total) return error.InvalidInputShape;
    return forwardImpl(cb, allocator, config, input_ids, attention_mask, batch, seq_len, null, null, null);
}

/// One tree-packed row (see pipelines/laya_tree.zig), attended through
/// `ComputeBackend.segmentAttention` instead of a dense mask. `positions`
/// and `ranges` (three key ranges per row, `[rows * 6]`) cover the rows this
/// forward computes; `key_positions` covers every key of the row. Local
/// layers add the sliding window in logical positions.
pub const Packed = struct {
    positions: []const i64,
    ranges: []const u32,
    key_positions: []const i32,
    /// Per-question upper layers (Laya `packing.fuse_layers`): layers from
    /// `upper_from` on attend with `upper_ranges` instead of `ranges`.
    /// Layer indices count the encoder first, then any head layers.
    upper_ranges: ?[]const u32 = null,
    upper_from: usize = std.math.maxInt(usize),

    /// The visibility layer `layer` attends with.
    pub fn at(self: Packed, layer: usize) Packed {
        var out = self;
        if (self.upper_ranges) |upper| if (layer >= self.upper_from) {
            out.ranges = upper;
        };
        return out;
    }

    /// The same row for a stack that starts `layers` deeper (the head after
    /// the encoder).
    pub fn after(self: Packed, layers: usize) Packed {
        var out = self;
        out.upper_from = self.upper_from -| layers;
        return out;
    }
};

/// Encode one tree-packed row. The result is `[seq_len, hidden]`.
pub fn forwardPackedCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    packed_row: Packed,
) !CT {
    try validateConfig(config);
    const seq_len = input_ids.len;
    // Independent packed branches restart their logical positions, so their
    // combined physical rows can exceed the model's positional window.
    if (seq_len == 0 or seq_len > @import("../models/laya.zig").max_packed_len_limit or packed_row.positions.len != seq_len) return error.InvalidInputShape;
    for (packed_row.positions) |p| if (p < 0 or p >= config.max_position_embeddings) return error.InvalidInputShape;
    const mask = try allocator.alloc(i64, seq_len);
    defer allocator.free(mask);
    @memset(mask, 1);
    return forwardImpl(cb, allocator, config, input_ids, mask, 1, seq_len, packed_row, null, null);
}

/// Cached trunk rows for a branch-only packed forward: per encoder layer,
/// the trunk keys (after RoPE) and values, each `[prefix_rows, hidden]`.
pub const Branches = struct {
    prefix_rows: usize,
    keys: []const CT,
    values: []const CT,
};

/// Per encoder layer keys (after RoPE) and values. Either host copies,
/// `[tokens * hidden]` each, or dense `[tokens, hidden]` backend tensors
/// that the caller owns (`key_tensors`/`value_tensors`, one slot per layer).
pub const Capture = struct {
    keys: []const []f32 = &.{},
    values: []const []f32 = &.{},
    key_tensors: []?CT = &.{},
    value_tensors: []?CT = &.{},

    fn layers(self: Capture) usize {
        return @max(self.keys.len, self.key_tensors.len);
    }
};

/// Encode only the branch tokens of a packed row whose trunk occupies rows
/// `0..prefix_rows` at contiguous positions (the ones the cached keys were
/// captured at; `packed_row.key_positions` must agree). `packed_row.positions`
/// covers the branch tokens; its masks cover the whole row. The result is
/// `[branch tokens, hidden]` and equals those rows of `forwardPackedCT`.
pub fn forwardBranchesCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    branch_ids: []const i64,
    packed_row: Packed,
    branches: Branches,
) !CT {
    try validateConfig(config);
    const rows = branch_ids.len;
    if (rows == 0 or packed_row.positions.len != rows or branches.keys.len != config.num_hidden_layers or branches.values.len != config.num_hidden_layers) return error.InvalidInputShape;
    for (packed_row.positions) |p| if (p < 0 or p >= config.max_position_embeddings) return error.InvalidInputShape;
    const seq_len = std.math.add(usize, branches.prefix_rows, rows) catch return error.InvalidInputShape;
    if (seq_len > @import("../models/laya.zig").max_packed_len_limit) return error.InvalidInputShape;
    const mask = try allocator.alloc(i64, seq_len);
    defer allocator.free(mask);
    @memset(mask, 1);
    return forwardImpl(cb, allocator, config, branch_ids, mask, 1, seq_len, packed_row, branches, null);
}

/// The unpacked encoder forward at logical positions `first_position..`,
/// also copying each layer's keys (after RoPE) and values to `capture`. Used
/// to fill the packed-trunk cache.
pub fn forwardCapturingCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    first_position: usize,
    capture: Capture,
) !CT {
    try validateConfig(config);
    if (capture.layers() != config.num_hidden_layers or @max(capture.values.len, capture.value_tensors.len) != config.num_hidden_layers) return error.InvalidInputShape;
    const n = input_ids.len;
    const mask = try allocator.alloc(i64, n);
    defer allocator.free(mask);
    @memset(mask, 1);
    // Run as a one-segment packed row so captured keys use the same RoPE op
    // and layout as the branch forward that later reads them.
    const positions = try allocator.alloc(i64, n);
    defer allocator.free(positions);
    const key_positions = try allocator.alloc(i32, n);
    defer allocator.free(key_positions);
    const ranges = try allocator.alloc(u32, 6 * n);
    defer allocator.free(ranges);
    const end_position = std.math.add(usize, first_position, n) catch return error.InvalidInputShape;
    if (n == 0 or end_position > config.max_position_embeddings) return error.InvalidInputShape;
    for (positions, key_positions, 0..) |*p, *k, i| {
        p.* = @intCast(first_position + i);
        k.* = @intCast(first_position + i);
        ranges[6 * i ..][0..6].* = .{ 0, @intCast(n), 0, 0, 0, 0 };
    }
    return forwardImpl(cb, allocator, config, input_ids, mask, 1, n, .{ .positions = positions, .ranges = ranges, .key_positions = key_positions }, null, capture);
}

fn forwardImpl(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    packed_row: ?Packed,
    branches: ?Branches,
    capture: ?Capture,
) !CT {
    const trace = MetalTrace.init(cb);
    const zero_bias: ?CT = if (config.checkpoint_layout == .huggingface_fused_qkv_no_bias)
        try makeZeroBias(cb, allocator, config.hidden_size)
    else
        null;
    defer if (zero_bias) |value| cb.free(value);

    // Preparation is lazy on the first request. Fixed slots remain attached
    // to the model's Metal provider after this request-local backend wrapper
    // is destroyed, so steady-state inference never uploads encoder weights.
    const resident_slots = if (zero_bias) |bias|
        try preplanMetalModernBertEncoder(cb, allocator, config, bias)
    else
        false;

    // A ModernBERT encoder has many small dense projections.  On Metal, keep
    // them (and their dependent elementwise ops) in one command buffer so MPS
    // does not submit and wait after every projection.  The frame is owned
    // only here; callers that already compose a frame retain control.
    var encoder_frame_active = false;
    // Interleaved packed RoPE and trunk capture read back to the host, so
    // those forwards run unframed.
    if ((packed_row == null or !config.rope_interleaved) and capture == null and cb.kind() == .metal and metalEncoderFrameEnabled() and !cb.decoderRuntimeHasActiveFrame()) {
        encoder_frame_active = try cb.decoderRuntimeBeginFrame();
    }
    errdefer if (encoder_frame_active) cb.decoderRuntimeCancelFrame() catch {};

    // 1. Token embeddings + embedding LayerNorm.
    //    ModernBERT has no absolute position embeddings; RoPE is applied in each
    //    attention layer instead.
    // On Metal an unpacked batch runs without its padding: dense attention
    // would mask padding and the local window with a host-built
    // `[heads, seq, seq]` bias per layer, and every linear would multiply the
    // padding rows too.
    const row_segments = if (packed_row == null and branches == null and capture == null and
        ((cb.kind() == .metal and metalRowSegmentsEnabled()) or cb.kind() == .native))
        try rowSegments(allocator, input_ids, attention_mask, batch, seq_len)
    else
        null;
    defer if (row_segments) |rows| rows.deinit(allocator);

    // One request-owned position upload, retained across all encoder layers.
    // No cross-request cache or new synchronization boundary is introduced.
    const fusion_positions: ?CT = if (cb.kind() == .metal and !config.rope_interleaved and
        config.checkpoint_layout == .huggingface_fused_qkv_no_bias and
        !@import("antfly_platform").env.getenvBool("TERMITE_METAL_DISABLE_GLINER_QKV_ROPE"))
    blk: {
        const rows = row_segments orelse break :blk null;
        break :blk try cb.fromInt32Shape(rows.positions, &.{@intCast(rows.tokens)});
    } else null;
    defer if (fusion_positions) |positions| cb.free(positions);

    var hidden = if (row_segments) |rows|
        try embeddingsBlock(cb, config, zero_bias, rows.ids, rows.tokens, resident_slots)
    else
        try embeddingsBlock(cb, config, zero_bias, input_ids, input_ids.len, resident_slots);
    errdefer cb.free(hidden);

    // 2. Encoder layers
    for (0..config.num_hidden_layers) |layer_idx| {
        const layer_started_ns = trace.now();
        const layer_started = trace.snapshot(cb);
        try cb.checkExecutionControl();
        const new_hidden = try encoderLayer(
            cb,
            allocator,
            config,
            hidden,
            attention_mask,
            batch,
            seq_len,
            layer_idx,
            zero_bias,
            resident_slots,
            if (packed_row) |row| row.at(layer_idx) else null,
            branches,
            capture,
            row_segments,
            fusion_positions,
        );
        cb.free(hidden);
        hidden = new_hidden;
        if (trace.per_layer and encoder_frame_active) {
            // Diagnostic-only attribution fence. This deliberately changes the
            // production two-layer command-buffer cadence and is unsuitable for
            // end-to-end latency claims.
            try cb.decoderRuntimeSubmitAndWaitFrame();
            encoder_frame_active = false;
            trace.emitLayer(cb, layer_idx, layer_started_ns, layer_started);
            try cb.checkExecutionControl();
            if (layer_idx + 1 < config.num_hidden_layers)
                encoder_frame_active = try cb.decoderRuntimeBeginFrame();
        } else if (cb.execution_control != null and encoder_frame_active and
            (layer_idx + 1) % 2 == 0 and layer_idx + 1 < config.num_hidden_layers)
        {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            encoder_frame_active = false;
            try cb.checkExecutionControl();
            encoder_frame_active = try cb.decoderRuntimeBeginFrame();
        }
    }

    // 3. Final layer norm
    var name_buf: [128]u8 = undefined;
    const fn_w = try cb.getWeight(std.fmt.bufPrint(&name_buf, "model.final_norm.weight", .{}) catch return error.NameTooLong);
    defer cb.free(fn_w);
    const normed_final = if (try slottedLayerNorm(cb, hidden, if (resident_slots) modernBertFinalNormSlot(config) else null, config)) |normed|
        normed
    else if (zero_bias) |bias|
        try cb.layerNorm(hidden, fn_w, bias, @intCast(config.hidden_size), config.layer_norm_eps)
    else blk: {
        const fn_b = try cb.getWeight(std.fmt.bufPrint(&name_buf, "model.final_norm.bias", .{}) catch return error.NameTooLong);
        defer cb.free(fn_b);
        break :blk try cb.layerNorm(hidden, fn_w, fn_b, @intCast(config.hidden_size), config.layer_norm_eps);
    };
    cb.free(hidden);
    hidden = normed_final;
    if (row_segments) |rows| {
        const padded = try cb.embeddingLookup(hidden, rows.restore, rows.restore.len, @intCast(config.hidden_size));
        cb.free(hidden);
        hidden = padded;
    }
    if (encoder_frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        encoder_frame_active = false;
    }
    try cb.checkExecutionControl();
    trace.emitAggregate(cb, if (row_segments) |rows| rows.tokens else batch * seq_len, @intCast(config.num_hidden_layers));
    return hidden;
}

// ---------------------------------------------------------------------------
// Embeddings block
// ---------------------------------------------------------------------------

fn embeddingsBlock(
    cb: *const ComputeBackend,
    config: Config,
    zero_bias: ?CT,
    input_ids: []const i64,
    total: usize,
    resident_slots: bool,
) !CT {
    const H = config.hidden_size;

    // Word / token embeddings
    const tok_emb_w = try cb.getWeight("model.embeddings.tok_embeddings.weight");
    defer cb.free(tok_emb_w);
    const tok_emb = try cb.embeddingLookup(tok_emb_w, input_ids, total, H);
    defer cb.free(tok_emb);

    // Embedding-level LayerNorm (replaces post-sum norm from classic BERT)
    if (try slottedLayerNorm(cb, tok_emb, if (resident_slots) modernBertEmbeddingNormSlot(config) else null, config)) |normed| return normed;
    const ln_w = try cb.getWeight("model.embeddings.norm.weight");
    defer cb.free(ln_w);
    if (zero_bias) |bias| return cb.layerNorm(tok_emb, ln_w, bias, H, config.layer_norm_eps);
    const ln_b = try cb.getWeight("model.embeddings.norm.bias");
    defer cb.free(ln_b);
    return cb.layerNorm(tok_emb, ln_w, ln_b, H, config.layer_norm_eps);
}

// ---------------------------------------------------------------------------
// Single encoder layer
// ---------------------------------------------------------------------------

fn encoderLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer_idx: usize,
    zero_bias: ?CT,
    resident_slots: bool,
    packed_row: ?Packed,
    branches: ?Branches,
    capture: ?Capture,
    row_segments: ?RowSegments,
    fusion_positions: ?CT,
) !CT {
    const H: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const head_dim = H / num_heads;
    const intermediate: usize = @intCast(config.intermediate_size);
    // A branch-only forward projects just the branch rows; attention still
    // spans the cached trunk rows that precede them.
    const prefix_rows: usize = if (branches) |b| b.prefix_rows else 0;
    const total = if (row_segments) |rows| rows.tokens else batch * seq_len - prefix_rows;

    // Layers 0, 3, 6, … use full (global) attention; all others are local.
    const is_global = (layer_idx % @as(usize, @intCast(config.global_attn_every_n_layers))) == 0;
    const rope_theta = if (is_global) config.global_rope_theta else config.local_rope_theta;

    var name_buf: [256]u8 = undefined;

    // -----------------------------------------------------------------------
    // Self-attention sub-layer  (pre-norm)
    // -----------------------------------------------------------------------

    // HuggingFace ModernBERT makes the layer-0 attention norm an identity.
    const identity_attn_norm = config.checkpoint_layout == .huggingface_fused_qkv_no_bias and layer_idx == 0;
    const normed_attn = if (identity_attn_norm) hidden else blk: {
        if (try slottedLayerNorm(cb, hidden, if (resident_slots) modernBertNormSlot(layer_idx, .attention) else null, config)) |normed| break :blk normed;
        const attn_ln_w = try getLayerWeight(cb, layer_idx, "attn_norm.weight", &name_buf);
        defer cb.free(attn_ln_w);
        if (zero_bias) |bias| break :blk try cb.layerNorm(hidden, attn_ln_w, bias, H, config.layer_norm_eps);
        const attn_ln_b = try getLayerWeight(cb, layer_idx, "attn_norm.bias", &name_buf);
        defer cb.free(attn_ln_b);
        break :blk try cb.layerNorm(hidden, attn_ln_w, attn_ln_b, H, config.layer_norm_eps);
    };
    defer if (!identity_attn_norm) cb.free(normed_attn);

    const qkv = try projectQkv(
        cb,
        config,
        normed_attn,
        layer_idx,
        total,
        H,
        if (resident_slots) modernBertLinearSlot(layer_idx, .qkv) else null,
        if (!config.rope_interleaved and packed_row == null and row_segments == null and branches == null)
            .{ .sequence = seq_len, .heads = num_heads, .theta = rope_theta }
        else
            null,
        fusion_positions,
        rope_theta,
        &name_buf,
    );
    defer cb.free(qkv.q);
    defer cb.free(qkv.k);
    defer cb.free(qkv.v);

    // Apply RoPE to Q and K. HuggingFace ModernBERT's `rotate_half` uses
    // split-half rotation; the legacy checkpoint retains interleaved pairs.
    // rope_dim == head_dim: the full head dimension is rotated.
    const rope_positions: ?[]const i64 = if (packed_row) |row| row.positions else if (row_segments) |rows| rows.rope_positions else null;
    const Q = if (qkv.rotated) qkv.q else if (rope_positions) |positions|
        try ropeAtPositions(cb, allocator, qkv.q, positions, num_heads, head_dim, rope_theta, config.rope_interleaved)
    else
        try cb.rope(qkv.q, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, config.rope_interleaved);
    defer if (!qkv.rotated) cb.free(Q);
    const K = if (qkv.rotated) qkv.k else if (rope_positions) |positions|
        try ropeAtPositions(cb, allocator, qkv.k, positions, num_heads, head_dim, rope_theta, config.rope_interleaved)
    else
        try cb.rope(qkv.k, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, config.rope_interleaved);
    defer if (!qkv.rotated) cb.free(K);

    if (capture) |c| try captureLayer(cb, allocator, c.keys, c.values, c.key_tensors, c.value_tensors, layer_idx, K, qkv.v, total, H);
    var joined: [2]?CT = .{ null, null };
    defer for (joined) |tensor| if (tensor) |t| cb.free(t);
    if (branches) |b| {
        joined[0] = try joinRows(cb, allocator, b.keys[layer_idx], prefix_rows, K, total, H);
        joined[1] = try joinRows(cb, allocator, b.values[layer_idx], prefix_rows, qkv.v, total, H);
    }
    const attn_out = if (packed_row) |row|
        try packedAttention(cb, allocator, Q, joined[0] orelse K, joined[1] orelse qkv.v, row, if (is_global) std.math.maxInt(u32) else config.local_attention_window / 2, total, seq_len, num_heads, head_dim)
    else if (!is_global and cb.kind() == .cuda)
        (try cb.encoderLocalAttention(Q, K, qkv.v, attention_mask, batch, seq_len, num_heads, head_dim, config.local_attention_window / 2)) orelse return error.UnsupportedLayaBackend
    else if (row_segments) |rows|
        try cb.segmentAttention(allocator, Q, K, qkv.v, &.{
            .ranges = rows.ranges,
            .query_positions = rows.positions,
            .key_positions = rows.positions,
            .window = if (is_global) std.math.maxInt(u32) else config.local_attention_window / 2,
            .queries = total,
            .keys = total,
            .num_heads = num_heads,
            .head_dim = head_dim,
        })
    else fallback: {
        // For local layers build a sliding-window additive attention bias.
        // Shape: [num_heads * seq_len * seq_len] (shared across the batch).
        // The BLAS sdpaOp detects len == num_heads*seq_len*seq_len and applies it
        // as a per-head shared bias added to raw dot-product scores before softmax.
        const window_bias: ?CT = if (!is_global) blk: {
            const half: usize = @intCast(config.local_attention_window / 2);
            break :blk try buildSlidingWindowBias(cb, allocator, seq_len, num_heads, half);
        } else null;
        defer if (window_bias) |wb| cb.free(wb);

        // Bidirectional scaled dot-product attention (encoder, no causal mask).
        // The padding mask (attention_mask) is consumed by the backend: positions
        // where mask[b*seq_len + ki] == 0 are set to -inf before softmax.
        break :fallback try cb.scaledDotProductAttention(
            Q,
            K,
            qkv.v,
            attention_mask,
            window_bias,
            batch,
            seq_len,
            num_heads,
            head_dim,
        );
    };
    defer cb.free(attn_out);

    // Output projection
    const attn_proj = try projectAttentionOutput(
        cb,
        config,
        attn_out,
        layer_idx,
        total,
        H,
        if (resident_slots) modernBertLinearSlot(layer_idx, .attention_output) else null,
        &name_buf,
    );
    defer cb.free(attn_proj);

    // Residual: add the projected attention output to the *original* (pre-norm)
    // hidden state — pre-norm residual pattern.
    const mlp_ln_w = try getLayerWeight(cb, layer_idx, "mlp_norm.weight", &name_buf);
    defer cb.free(mlp_ln_w);
    const mlp_ln_b = if (zero_bias) |bias| bias else try getLayerWeight(cb, layer_idx, "mlp_norm.bias", &name_buf);
    defer if (zero_bias == null) cb.free(mlp_ln_b);
    const fused_norm = if (cb.kind() == .metal)
        try cb.addLayerNormSumCentered(attn_proj, hidden, mlp_ln_w, mlp_ln_b, H, config.layer_norm_eps)
    else
        null;
    const hidden_after_attn = if (fused_norm) |fused| fused.sum else try cb.add(attn_proj, hidden);
    defer cb.free(hidden_after_attn);

    // -----------------------------------------------------------------------
    // FFN sub-layer  (pre-norm, GeGLU)
    // -----------------------------------------------------------------------

    // Pre-FFN LayerNorm
    const normed_ffn = if (fused_norm) |fused| fused.normed else if (try slottedLayerNorm(cb, hidden_after_attn, if (resident_slots) modernBertNormSlot(layer_idx, .mlp) else null, config)) |normed|
        normed
    else if (zero_bias) |bias|
        try cb.layerNorm(hidden_after_attn, mlp_ln_w, bias, H, config.layer_norm_eps)
    else
        try cb.layerNorm(hidden_after_attn, mlp_ln_w, mlp_ln_b, H, config.layer_norm_eps);
    defer cb.free(normed_ffn);

    // GeGLU feed-forward (Wi and Wo both have no bias in ModernBERT's MLP)
    const Wi_w = try getLayerWeight(cb, layer_idx, "mlp.Wi.weight", &name_buf);
    defer cb.free(Wi_w);
    const Wo_w = try getLayerWeight(cb, layer_idx, "mlp.Wo.weight", &name_buf);
    defer cb.free(Wo_w);

    const ffn_out = try geGluFfn(
        cb,
        normed_ffn,
        Wi_w,
        Wo_w,
        total,
        H,
        intermediate,
        config.checkpoint_layout == .huggingface_fused_qkv_no_bias,
        if (resident_slots) modernBertLinearSlot(layer_idx, .ffn_in) else null,
        if (resident_slots) modernBertLinearSlot(layer_idx, .ffn_out) else null,
    );
    defer cb.free(ffn_out);

    // Residual: add FFN output to post-attention hidden state
    return cb.add(ffn_out, hidden_after_attn);
}

const QkvProjection = struct {
    q: CT,
    k: CT,
    v: CT,
    rotated: bool = false,
};

const QkvRope = struct {
    sequence: usize,
    heads: usize,
    theta: f32,
};

fn projectQkv(
    cb: *const ComputeBackend,
    config: Config,
    input: CT,
    layer_idx: usize,
    rows: usize,
    hidden_size: usize,
    slot: ?usize,
    rope: ?QkvRope,
    fusion_positions: ?CT,
    rope_theta: f32,
    name_buf: *[256]u8,
) !QkvProjection {
    if (config.checkpoint_layout == .huggingface_fused_qkv_no_bias) {
        const qkv_w = try getLayerWeight(cb, layer_idx, "attn.Wqkv.weight", name_buf);
        defer cb.free(qkv_w);
        const qkv = try linearNoBiasWithSlot(
            cb,
            input,
            qkv_w,
            rows,
            hidden_size,
            hidden_size * 3,
            slot,
        );
        defer cb.free(qkv);
        if (rope) |rotation| {
            if (rotation.sequence == 0 or rotation.heads == 0 or rows % rotation.sequence != 0 or hidden_size % rotation.heads != 0) return error.InvalidShape;
            if (try cb.splitQkvRope(qkv, rows / rotation.sequence, rotation.sequence, rotation.heads, hidden_size / rotation.heads, rotation.theta)) |parts| {
                return .{ .q = parts.first, .k = parts.second, .v = parts.third, .rotated = true };
            }
        }
        if (fusion_positions) |positions| {
            if (try cb.packedQkvRope(qkv, positions, hidden_size, hidden_size / config.num_attention_heads, rope_theta)) |fused|
                return .{ .q = fused.first, .k = fused.second, .v = fused.third, .rotated = true };
        }
        // Use direct slices instead of splitLastDim3: Metal's generic split
        // has a GLiNER-only device gate, while sliceLastDim is device-resident
        // for every dense [rows, columns] ModernBERT activation.
        const q = try cb.sliceLastDim(qkv, 0, hidden_size);
        errdefer cb.free(q);
        const k = try cb.sliceLastDim(qkv, hidden_size, hidden_size * 2);
        errdefer cb.free(k);
        const v = try cb.sliceLastDim(qkv, hidden_size * 2, hidden_size * 3);
        return .{ .q = q, .k = k, .v = v };
    }

    const q_w = try getLayerWeight(cb, layer_idx, "attn.query_proj.weight", name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, layer_idx, "attn.query_proj.bias", name_buf);
    defer cb.free(q_b);
    const q = try linearWithLoRA(cb, input, q_w, q_b, layer_idx, "query_proj", config.lora_rank, config.lora_alpha, rows, hidden_size, hidden_size);
    errdefer cb.free(q);

    const k_w = try getLayerWeight(cb, layer_idx, "attn.key_proj.weight", name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, layer_idx, "attn.key_proj.bias", name_buf);
    defer cb.free(k_b);
    const k = try cb.linear(input, k_w, k_b, rows, hidden_size, hidden_size);
    errdefer cb.free(k);

    const v_w = try getLayerWeight(cb, layer_idx, "attn.value_proj.weight", name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, layer_idx, "attn.value_proj.bias", name_buf);
    defer cb.free(v_b);
    const v = try linearWithLoRA(cb, input, v_w, v_b, layer_idx, "value_proj", config.lora_rank, config.lora_alpha, rows, hidden_size, hidden_size);
    return .{ .q = q, .k = k, .v = v };
}

fn projectAttentionOutput(
    cb: *const ComputeBackend,
    config: Config,
    input: CT,
    layer_idx: usize,
    rows: usize,
    hidden_size: usize,
    slot: ?usize,
    name_buf: *[256]u8,
) !CT {
    const out_w = try getLayerWeight(cb, layer_idx, "attn.Wo.weight", name_buf);
    defer cb.free(out_w);
    if (config.checkpoint_layout == .huggingface_fused_qkv_no_bias) {
        return linearNoBiasWithSlot(cb, input, out_w, rows, hidden_size, hidden_size, slot);
    }
    const out_b = try getLayerWeight(cb, layer_idx, "attn.Wo.bias", name_buf);
    defer cb.free(out_b);
    return cb.linear(input, out_w, out_b, rows, hidden_size, hidden_size);
}

fn makeZeroBias(cb: *const ComputeBackend, allocator: std.mem.Allocator, dim: usize) !CT {
    const zeroes = try allocator.alloc(f32, dim);
    defer allocator.free(zeroes);
    @memset(zeroes, 0);
    const shape = [_]i32{@intCast(dim)};
    return cb.fromFloat32Shape(zeroes, &shape);
}

fn linearNoBiasWithSlot(
    cb: *const ComputeBackend,
    input: CT,
    weight: CT,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    slot: ?usize,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLinear(&.{
            .slot = prepared_slot,
            .input = input,
            .in_dim = input_dim,
            .out_dim = output_dim,
        })) |output| return output;
    }
    return cb.linearNoBias(input, weight, rows, input_dim, output_dim);
}

// ---------------------------------------------------------------------------
// GeGLU feed-forward network
// ---------------------------------------------------------------------------
//
// Architecture (matches gopeft / HuggingFace ModernBERT):
//
//   gated  = input @ Wi^T        [total, 2*intermediate]   (no bias)
//   gate   = gated[..., :intermediate]                     first half
//   value  = gated[..., intermediate:]                     second half
//   act    = GELU(gate) * value  [total, intermediate]
//   output = act @ Wo^T          [total, hidden]           (no bias)
//
// Keep the gate/value split on the active backend. Metal exposes device-side
// last-dimension slicing, exact GELU, and multiplication, avoiding a
// per-layer download and re-upload of the large gated projection.

fn geGluFfn(
    cb: *const ComputeBackend,
    input: CT,
    Wi_w: CT,
    Wo_w: CT,
    total: usize,
    hidden_size: usize,
    intermediate_size: usize,
    exact_gelu: bool,
    wi_slot: ?usize,
    wo_slot: ?usize,
) !CT {
    // Project to 2*intermediate.  Wi is [2*intermediate, hidden] (row-major,
    // transposed by the linear op) so the output is [total, 2*intermediate].
    const gated_ct = try linearNoBiasWithSlot(
        cb,
        input,
        Wi_w,
        total,
        hidden_size,
        2 * intermediate_size,
        wi_slot,
    );
    defer cb.free(gated_ct);

    if (exact_gelu) {
        if (try cb.packedGegluExact(gated_ct, total, intermediate_size)) |activated| {
            defer cb.free(activated);
            return linearNoBiasWithSlot(cb, activated, Wo_w, total, intermediate_size, hidden_size, wo_slot);
        }
    }

    const gate_ct = try cb.sliceLastDim(gated_ct, 0, intermediate_size);
    defer cb.free(gate_ct);
    const value_ct = try cb.sliceLastDim(gated_ct, intermediate_size, 2 * intermediate_size);
    defer cb.free(value_ct);

    // HF ACT2FN["gelu"] uses erf, not the tanh approximation. The small
    // per-layer difference accumulates across released 28-layer encoders.
    const activation: ops.DecoderRuntimeActivationKind = if (exact_gelu) .gelu_exact else .gelu;
    const activated_ct = (try cb.activationMultiply(gate_ct, value_ct, activation)) orelse blk: {
        const gate_gelu_ct = if (exact_gelu) (try cb.geluExact(gate_ct)) orelse return error.UnsupportedModernBertActivation else try cb.gelu(gate_ct);
        defer cb.free(gate_gelu_ct);
        break :blk try cb.multiply(gate_gelu_ct, value_ct);
    };
    defer cb.free(activated_ct);

    // Wo is [hidden, intermediate] so the output is [total, hidden].
    return linearNoBiasWithSlot(
        cb,
        activated_ct,
        Wo_w,
        total,
        intermediate_size,
        hidden_size,
        wo_slot,
    );
}

/// Segment-masked attention for `queries` packed rows over all `keys`.
pub fn packedAttention(cb: *const ComputeBackend, allocator: std.mem.Allocator, q: CT, k: CT, v: CT, row: Packed, window: u32, queries: usize, keys: usize, num_heads: usize, head_dim: usize) !CT {
    if (row.positions.len != queries or row.ranges.len != queries * 6 or row.key_positions.len != keys) return error.InvalidInputShape;
    const query_positions = try allocator.alloc(i32, queries);
    defer allocator.free(query_positions);
    for (query_positions, row.positions) |*dst, p| dst.* = @intCast(p);
    return cb.segmentAttention(allocator, q, k, v, &.{
        .ranges = row.ranges,
        .query_positions = query_positions,
        .key_positions = row.key_positions,
        .window = window,
        .queries = queries,
        .keys = keys,
        .num_heads = num_heads,
        .head_dim = head_dim,
    });
}

/// Rows `prefix..seq` of a token-major `[seq, width]` activation.
///
/// On Metal, row joins and slices are last-dimension ops on a flattened
/// `[1, rows * width]` view, which run in the ordered decode stream. Metal's
/// axis-0 concat blits outside that stream and read stale inputs while
/// earlier work was still queued. Other backends execute eagerly and use the
/// row gather and axis-0 concat directly.
pub fn branchRows(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: CT, prefix: usize, seq: usize, width: usize) !CT {
    if (cb.kind() != .metal) {
        const ids = try allocator.alloc(i64, seq - prefix);
        defer allocator.free(ids);
        for (ids, prefix..) |*id, row| id.* = @intCast(row);
        return cb.embeddingLookup(input, ids, ids.len, width);
    }
    const flat = try reshape(cb, allocator, input, &.{ 1, @intCast(seq * width) });
    defer cb.free(flat);
    const tail = try cb.sliceLastDim(flat, prefix * width, seq * width);
    defer cb.free(tail);
    return reshape(cb, allocator, tail, &.{ @intCast(seq - prefix), @intCast(width) });
}

/// `[a_rows + b_rows, width]` from `[a_rows, width]` and `[b_rows, width]`.
pub fn joinRows(cb: *const ComputeBackend, allocator: std.mem.Allocator, a: CT, a_rows: usize, b: CT, b_rows: usize, width: usize) !CT {
    if (cb.kind() != .metal) {
        return cb.primConcatPrim(a, b, 0, &.{ @intCast(a_rows), @intCast(width) }, &.{ @intCast(b_rows), @intCast(width) });
    }
    const left = try reshape(cb, allocator, a, &.{ 1, @intCast(a_rows * width) });
    defer cb.free(left);
    const right = try reshape(cb, allocator, b, &.{ 1, @intCast(b_rows * width) });
    defer cb.free(right);
    const joined = try cb.concat(left, right, 1, a_rows * width, b_rows * width);
    defer cb.free(joined);
    return reshape(cb, allocator, joined, &.{ @intCast(a_rows + b_rows), @intCast(width) });
}

/// Store one layer's keys and values as host copies or as dense tensors.
pub fn captureLayer(cb: *const ComputeBackend, allocator: std.mem.Allocator, host_keys: []const []f32, host_values: []const []f32, key_tensors: []?CT, value_tensors: []?CT, layer: usize, keys: CT, values: CT, rows: usize, width: usize) !void {
    if (key_tensors.len > 0) {
        const shape = [_]i32{ @intCast(rows), @intCast(width) };
        key_tensors[layer] = try reshape(cb, allocator, keys, &shape);
        value_tensors[layer] = try reshape(cb, allocator, values, &shape);
        return;
    }
    for ([_]CT{ keys, values }, [_][]f32{ host_keys[layer], host_values[layer] }) |tensor, dst| {
        const host = try cb.toFloat32(tensor, allocator);
        defer allocator.free(host);
        if (host.len != dst.len) return error.InvalidInputShape;
        @memcpy(dst, host);
    }
}

/// A new handle with a different logical shape over the same elements.
/// Tensors a backend cannot alias (host-backed or strided views) are
/// materialized through the host, which is ordered but synchronizes.
pub fn reshape(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: CT, shape: []const i32) !CT {
    if (try cb.cloneTensorShape(input, shape)) |view| return view;
    const values = try cb.toFloat32(input, allocator);
    defer allocator.free(values);
    return cb.fromFloat32Shape(values, shape);
}

/// RoPE at explicit per-token positions for a token-major `[tokens, heads *
/// head_dim]` projection. Packed rows restart positions at every branch, which
/// the contiguous `rope` op cannot express. The rotation matches `rope` for
/// positions 0..n-1 (see the packed-encoder degenerate-tree test).
///
/// Split-half rotation is M-RoPE with every frequency pair on the first axis,
/// so device backends that implement `mrope` rotate in place; others use the
/// host rotation.
/// An unpacked, right-padded batch run without its padding: the real tokens
/// of every row back to back, each row its own attention segment. Linears,
/// norms and attention then skip padding entirely; `restore` maps every
/// padded slot back to a compact row (padding to its row's first token).
const RowSegments = struct {
    /// Real tokens across the batch.
    tokens: usize,
    ids: []i64,
    /// Per compact token: its row's key range (segment-attention layout).
    ranges: []u32,
    positions: []i32,
    rope_positions: []i64,
    restore: []i64,

    fn deinit(self: RowSegments, allocator: std.mem.Allocator) void {
        allocator.free(self.ids);
        allocator.free(self.ranges);
        allocator.free(self.positions);
        allocator.free(self.rope_positions);
        allocator.free(self.restore);
    }
};

/// Null when a row is empty or not right-padded; the dense path handles it.
fn rowSegments(allocator: std.mem.Allocator, input_ids: []const i64, attention_mask: []const i64, batch: usize, seq_len: usize) !?RowSegments {
    if (attention_mask.len != batch * seq_len or input_ids.len != batch * seq_len) return null;
    if (batch * seq_len > std.math.maxInt(u32)) return null;
    var tokens: usize = 0;
    for (0..batch) |row| {
        const mask = attention_mask[row * seq_len ..][0..seq_len];
        const len = std.mem.indexOfScalar(i64, mask, 0) orelse seq_len;
        if (len == 0) return null;
        for (mask[len..]) |m| if (m != 0) return null;
        tokens += len;
    }
    const ids = try allocator.alloc(i64, tokens);
    errdefer allocator.free(ids);
    const ranges = try allocator.alloc(u32, tokens * 6);
    errdefer allocator.free(ranges);
    const positions = try allocator.alloc(i32, tokens);
    errdefer allocator.free(positions);
    const rope_positions = try allocator.alloc(i64, tokens);
    errdefer allocator.free(rope_positions);
    const restore = try allocator.alloc(i64, batch * seq_len);
    @memset(ranges, 0);
    var offset: usize = 0;
    for (0..batch) |row| {
        const mask = attention_mask[row * seq_len ..][0..seq_len];
        const len = std.mem.indexOfScalar(i64, mask, 0) orelse seq_len;
        for (0..seq_len) |i| {
            restore[row * seq_len + i] = @intCast(offset + if (i < len) i else 0);
            if (i >= len) continue;
            const at = offset + i;
            ids[at] = input_ids[row * seq_len + i];
            ranges[at * 6 ..][0..2].* = .{ @intCast(offset), @intCast(offset + len) };
            positions[at] = @intCast(i);
            rope_positions[at] = @intCast(i);
        }
        offset += len;
    }
    return .{ .tokens = tokens, .ids = ids, .ranges = ranges, .positions = positions, .rope_positions = rope_positions, .restore = restore };
}

fn metalRowSegmentsEnabled() bool {
    return @import("antfly_platform").env.getenvBoolDefault("ANTFLY_MODERNBERT_SEGMENT_ATTENTION", true);
}

fn ropeAtPositions(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    positions: []const i64,
    num_heads: usize,
    head_dim: usize,
    theta: f32,
    interleaved: bool,
) !CT {
    if (!interleaved) {
        const axes = try allocator.alloc(u32, 3 * positions.len);
        defer allocator.free(axes);
        for (0..3) |axis| for (positions, axes[axis * positions.len ..][0..positions.len]) |p, *dst| {
            dst.* = @intCast(p);
        };
        if (try cb.mrope(input, positions.len, head_dim, theta, 1.0, axes, .{ @intCast(head_dim / 2), 0, 0 })) |rotated| return rotated;
    }
    const values = try cb.toFloat32(input, allocator);
    defer allocator.free(values);
    if (values.len != positions.len * num_heads * head_dim) return error.InvalidRoPEInput;
    const chunks = try allocator.alloc(usize, positions.len * num_heads);
    defer allocator.free(chunks);
    for (chunks, 0..) |*chunk, i| chunk.* = @intCast(positions[i / num_heads]);
    native_compute.ropeCore(values, chunks, head_dim, head_dim, theta, 1.0, interleaved);
    return cb.fromFloat32Shape(values, &[_]i32{ @intCast(positions.len), @intCast(num_heads * head_dim) });
}

// ---------------------------------------------------------------------------
// Sliding-window additive attention bias  (local attention layers)
// ---------------------------------------------------------------------------
//
// Returns a CT of flat length [num_heads * seq_len * seq_len] where element
// [h, qi, ki] is:
//   0.0  when |qi - ki| <= window_half  (ki is inside the sliding window)
//   -inf when |qi - ki| >  window_half  (ki is outside the sliding window)
//
// All heads share an identical mask.  The BLAS sdpaOp selects the shared
// head-indexed form when len == num_heads * seq_len * seq_len.

fn buildSlidingWindowBias(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    seq_len: usize,
    num_heads: usize,
    window_half: usize,
) !CT {
    const n = num_heads * seq_len * seq_len;
    const data = try allocator.alloc(f32, n);
    defer allocator.free(data);

    for (0..num_heads) |h| {
        const head_base = h * seq_len * seq_len;
        for (0..seq_len) |qi| {
            for (0..seq_len) |ki| {
                const diff: usize = if (qi >= ki) qi - ki else ki - qi;
                data[head_base + qi * seq_len + ki] =
                    if (diff > window_half) -std.math.inf(f32) else 0.0;
            }
        }
    }

    return cb.fromFloat32(data);
}

// ---------------------------------------------------------------------------
// Weight-name helpers
// ---------------------------------------------------------------------------

/// Build "model.layers.{layer}.{suffix}" and look up the weight tensor.
fn getLayerWeight(
    cb: *const ComputeBackend,
    layer: usize,
    suffix: []const u8,
    buf: *[256]u8,
) !CT {
    const name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

/// Run a linear projection for a LoRA-targeted module (query_proj or value_proj).
///
/// When `lora_rank > 0` and the backend vtable has `linearLoRA`, this function
/// tries to load the LoRA A/B tensors from the WeightStore.  If both are found
/// it calls `cb.linearLoRA`; otherwise it falls back to plain `cb.linear`.
///
/// Weight keys:  "model.layers.{layer}.attn.{proj_name}.lora_{a,b}"
fn linearWithLoRA(
    cb: *const ComputeBackend,
    input: CT,
    base_w: CT,
    base_b: CT,
    layer: usize,
    proj_name: []const u8,
    lora_rank: u32,
    lora_alpha: f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    if (lora_rank > 0 and cb.vtable.linearLoRA != null) {
        var key_a_buf: [128]u8 = undefined;
        var key_b_buf: [128]u8 = undefined;
        const key_a = std.fmt.bufPrint(&key_a_buf, "model.layers.{d}.attn.{s}.lora_a", .{ layer, proj_name }) catch
            return cb.linear(input, base_w, base_b, rows, in_dim, out_dim);
        const key_b = std.fmt.bufPrint(&key_b_buf, "model.layers.{d}.attn.{s}.lora_b", .{ layer, proj_name }) catch
            return cb.linear(input, base_w, base_b, rows, in_dim, out_dim);

        const lora_a = cb.getWeight(key_a) catch |err| switch (err) {
            error.MissingWeight => return cb.linear(input, base_w, base_b, rows, in_dim, out_dim),
            else => return err,
        };
        defer cb.free(lora_a);

        const lora_b = cb.getWeight(key_b) catch |err| switch (err) {
            error.MissingWeight => return cb.linear(input, base_w, base_b, rows, in_dim, out_dim),
            else => return err,
        };
        defer cb.free(lora_b);

        const rank: usize = @intCast(lora_rank);
        // Effective alpha: if caller passed 0.0, use rank so that scale = alpha/rank = 1.0.
        const effective_alpha: f32 = if (lora_alpha == 0.0) @floatFromInt(lora_rank) else lora_alpha;
        return cb.linearLoRA(input, base_w, base_b, lora_a, lora_b, effective_alpha, rank, rows, in_dim, out_dim);
    }
    return cb.linear(input, base_w, base_b, rows, in_dim, out_dim);
}

// ---------------------------------------------------------------------------
// Activation capture types
// ---------------------------------------------------------------------------

/// One captured linear-layer input from the encoder forward pass.
pub const ActivationCapture = struct {
    layer_idx: u32,
    /// "query_proj" or "value_proj" (points into a comptime string literal)
    module_name: []const u8,
    /// Owned flat buffer: [total * in_features] in row-major order.
    /// total = batch * seq_len
    input: []f32,
    in_features: usize,
    out_features: usize,
    total: usize, // batch * seq_len

    pub fn deinit(self: *ActivationCapture, allocator: std.mem.Allocator) void {
        allocator.free(self.input);
        self.* = undefined;
    }
};

/// Buffer of ActivationCapture records from one forward pass.
pub const ActivationBuffer = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayListUnmanaged(ActivationCapture),

    pub fn init(allocator: std.mem.Allocator) ActivationBuffer {
        return .{ .allocator = allocator, .items = .empty };
    }

    pub fn deinit(self: *ActivationBuffer) void {
        for (self.items.items) |*cap| cap.deinit(self.allocator);
        self.items.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn add(
        self: *ActivationBuffer,
        layer_idx: u32,
        module_name: []const u8,
        input_f32: []const f32,
        in_features: usize,
        out_features: usize,
        total: usize,
    ) !void {
        const owned = try self.allocator.dupe(f32, input_f32);
        errdefer self.allocator.free(owned);
        try self.items.append(self.allocator, .{
            .layer_idx = layer_idx,
            .module_name = module_name,
            .input = owned,
            .in_features = in_features,
            .out_features = out_features,
            .total = total,
        });
    }
};

// ---------------------------------------------------------------------------
// Activation-capturing forward pass
// ---------------------------------------------------------------------------

/// Like `forward` but also captures the inputs to query_proj and value_proj
/// in each layer into `captures`.  The returned f32 slice is owned by the caller.
pub fn forwardCapturingActivations(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    captures: *ActivationBuffer,
) ![]f32 {
    const result_ct = try forwardCapturingActivationsCT(
        cb,
        allocator,
        config,
        input_ids,
        attention_mask,
        batch,
        seq_len,
        captures,
    );
    defer cb.free(result_ct);
    return cb.toFloat32(result_ct, allocator);
}

fn forwardCapturingActivationsCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    captures: *ActivationBuffer,
) !CT {
    const total_tokens = batch * seq_len;
    const H: usize = @intCast(config.hidden_size);

    // Collect normed_attn CTs from all layers without downloading them yet.
    // This lets us batch-evaluate all 22 tensors in one GPU sync (one Metal
    // command buffer submission on device backends) instead of one per layer.
    var normed_attn_cts = std.ArrayListUnmanaged(CT).empty;
    defer {
        for (normed_attn_cts.items) |ct| cb.free(ct);
        normed_attn_cts.deinit(allocator);
    }

    // The activation-capture path uses the separate-QKV layout's stored biases.
    var hidden = try embeddingsBlock(cb, config, null, input_ids, total_tokens, false);
    // Free hidden on any error path; the happy path frees it explicitly below.
    errdefer cb.free(hidden);

    for (0..config.num_hidden_layers) |layer_idx| {
        const layer_result = try encoderLayerWithNormedAttn(
            cb,
            allocator,
            config,
            hidden,
            attention_mask,
            batch,
            seq_len,
            layer_idx,
        );
        cb.free(hidden);
        hidden = layer_result.hidden;
        // Transfer ownership of normed_attn to the list.  On append failure,
        // free it immediately before propagating the error.
        normed_attn_cts.append(allocator, layer_result.normed_attn) catch |err| {
            cb.free(layer_result.normed_attn);
            return err;
        };
    }

    // Batch-download all normed_attn tensors — single GPU sync on Metal.
    const batch_results = try cb.toFloat32Batch(normed_attn_cts.items, allocator);
    defer {
        for (batch_results) |r| allocator.free(r);
        allocator.free(batch_results);
    }

    // Populate captures from the downloaded data.
    for (0..config.num_hidden_layers) |layer_idx| {
        const normed_f32 = batch_results[layer_idx];
        try captures.add(@intCast(layer_idx), "query_proj", normed_f32, H, H, total_tokens);
        try captures.add(@intCast(layer_idx), "value_proj", normed_f32, H, H, total_tokens);
    }

    // Final layer norm (same as forwardCT)
    var name_buf: [128]u8 = undefined;
    const fn_w = try cb.getWeight(std.fmt.bufPrint(&name_buf, "model.final_norm.weight", .{}) catch return error.NameTooLong);
    defer cb.free(fn_w);
    const fn_b = try cb.getWeight(std.fmt.bufPrint(&name_buf, "model.final_norm.bias", .{}) catch return error.NameTooLong);
    defer cb.free(fn_b);
    const normed_final = try cb.layerNorm(hidden, fn_w, fn_b, @intCast(config.hidden_size), config.layer_norm_eps);
    cb.free(hidden);
    return normed_final;
}

// ---------------------------------------------------------------------------
// Encoder layer variant that returns the pre-attention normed hidden state
// as an owned CT alongside the layer output.  Used by the batched activation
// capture path so we can defer all GPU→CPU downloads to a single eval call.
// ---------------------------------------------------------------------------

const LayerWithNormedAttn = struct {
    /// The updated hidden state for the next encoder layer.  Caller owns it.
    hidden: CT,
    /// The pre-attention LayerNorm output (normed_attn) for this layer.
    /// Caller owns it; NOT freed inside this function.
    normed_attn: CT,
};

/// Like encoderLayer, but returns normed_attn as a second CT instead of
/// immediately freeing it.  The rest of the layer runs normally so that the
/// returned hidden state is correct.  The caller is responsible for freeing
/// both returned CTs.
fn encoderLayerWithNormedAttn(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer_idx: usize,
) !LayerWithNormedAttn {
    const H: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const head_dim = H / num_heads;
    const intermediate: usize = @intCast(config.intermediate_size);
    const total = batch * seq_len;

    const is_global = (layer_idx % @as(usize, @intCast(config.global_attn_every_n_layers))) == 0;
    const rope_theta = if (is_global) config.global_rope_theta else config.local_rope_theta;

    var name_buf: [256]u8 = undefined;

    // -----------------------------------------------------------------------
    // Self-attention sub-layer  (pre-norm)
    // -----------------------------------------------------------------------

    // Pre-attention LayerNorm — NOT deferred; ownership returned to caller.
    const attn_ln_w = try getLayerWeight(cb, layer_idx, "attn_norm.weight", &name_buf);
    defer cb.free(attn_ln_w);
    const attn_ln_b = try getLayerWeight(cb, layer_idx, "attn_norm.bias", &name_buf);
    defer cb.free(attn_ln_b);
    const normed_attn = try cb.layerNorm(hidden, attn_ln_w, attn_ln_b, H, config.layer_norm_eps);
    // NOTE: no `defer cb.free(normed_attn)` here — returned to caller.

    // Q projection — use linearLoRA if LoRA is enabled in the config.
    const q_w = try getLayerWeight(cb, layer_idx, "attn.query_proj.weight", &name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, layer_idx, "attn.query_proj.bias", &name_buf);
    defer cb.free(q_b);
    const Q_raw = try linearWithLoRA(cb, normed_attn, q_w, q_b, layer_idx, "query_proj", config.lora_rank, config.lora_alpha, total, H, H);
    defer cb.free(Q_raw);

    // K projection — no LoRA on key.
    const k_w = try getLayerWeight(cb, layer_idx, "attn.key_proj.weight", &name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, layer_idx, "attn.key_proj.bias", &name_buf);
    defer cb.free(k_b);
    const K_raw = try cb.linear(normed_attn, k_w, k_b, total, H, H);
    defer cb.free(K_raw);

    // V projection — use linearLoRA if LoRA is enabled in the config.
    const v_w = try getLayerWeight(cb, layer_idx, "attn.value_proj.weight", &name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, layer_idx, "attn.value_proj.bias", &name_buf);
    defer cb.free(v_b);
    const V = try linearWithLoRA(cb, normed_attn, v_w, v_b, layer_idx, "value_proj", config.lora_rank, config.lora_alpha, total, H, H);
    defer cb.free(V);

    // RoPE
    const Q = try cb.rope(Q_raw, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, true);
    defer cb.free(Q);
    const K = try cb.rope(K_raw, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, true);
    defer cb.free(K);

    // Sliding-window bias for local attention layers.
    const window_bias: ?CT = if (!is_global) blk: {
        const half: usize = @intCast(config.local_attention_window / 2);
        break :blk try buildSlidingWindowBias(cb, allocator, seq_len, num_heads, half);
    } else null;
    defer if (window_bias) |wb| cb.free(wb);

    // Bidirectional scaled dot-product attention.
    const attn_out = try cb.scaledDotProductAttention(
        Q,
        K,
        V,
        attention_mask,
        window_bias,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    defer cb.free(attn_out);

    // Output projection
    const out_w = try getLayerWeight(cb, layer_idx, "attn.Wo.weight", &name_buf);
    defer cb.free(out_w);
    const out_b = try getLayerWeight(cb, layer_idx, "attn.Wo.bias", &name_buf);
    defer cb.free(out_b);
    const attn_proj = try cb.linear(attn_out, out_w, out_b, total, H, H);
    defer cb.free(attn_proj);

    // Residual
    const hidden_after_attn = try cb.add(attn_proj, hidden);
    defer cb.free(hidden_after_attn);

    // -----------------------------------------------------------------------
    // FFN sub-layer  (pre-norm, GeGLU)
    // -----------------------------------------------------------------------

    const mlp_ln_w = try getLayerWeight(cb, layer_idx, "mlp_norm.weight", &name_buf);
    defer cb.free(mlp_ln_w);
    const mlp_ln_b = try getLayerWeight(cb, layer_idx, "mlp_norm.bias", &name_buf);
    defer cb.free(mlp_ln_b);
    const normed_ffn = try cb.layerNorm(hidden_after_attn, mlp_ln_w, mlp_ln_b, H, config.layer_norm_eps);
    defer cb.free(normed_ffn);

    const Wi_w = try getLayerWeight(cb, layer_idx, "mlp.Wi.weight", &name_buf);
    defer cb.free(Wi_w);
    const Wo_w = try getLayerWeight(cb, layer_idx, "mlp.Wo.weight", &name_buf);
    defer cb.free(Wo_w);

    const ffn_out = try geGluFfn(cb, normed_ffn, Wi_w, Wo_w, total, H, intermediate, config.checkpoint_layout == .huggingface_fused_qkv_no_bias, null, null);
    defer cb.free(ffn_out);

    return .{
        .hidden = try cb.add(ffn_out, hidden_after_attn),
        .normed_attn = normed_attn,
    };
}

fn encoderLayerCapturing(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer_idx: usize,
    captures: *ActivationBuffer,
) !CT {
    const H: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const head_dim = H / num_heads;
    const intermediate: usize = @intCast(config.intermediate_size);
    const total = batch * seq_len;

    // Layers 0, 3, 6, … use full (global) attention; all others are local.
    const is_global = (layer_idx % @as(usize, @intCast(config.global_attn_every_n_layers))) == 0;
    const rope_theta = if (is_global) config.global_rope_theta else config.local_rope_theta;

    var name_buf: [256]u8 = undefined;

    // -----------------------------------------------------------------------
    // Self-attention sub-layer  (pre-norm)
    // -----------------------------------------------------------------------

    // Pre-attention LayerNorm
    const attn_ln_w = try getLayerWeight(cb, layer_idx, "attn_norm.weight", &name_buf);
    defer cb.free(attn_ln_w);
    const attn_ln_b = try getLayerWeight(cb, layer_idx, "attn_norm.bias", &name_buf);
    defer cb.free(attn_ln_b);
    const normed_attn = try cb.layerNorm(hidden, attn_ln_w, attn_ln_b, H, config.layer_norm_eps);
    defer cb.free(normed_attn);

    // Capture normed_attn as the input to query_proj and value_proj.
    const normed_attn_f32 = try cb.toFloat32(normed_attn, allocator);
    defer allocator.free(normed_attn_f32);
    try captures.add(@intCast(layer_idx), "query_proj", normed_attn_f32, H, H, total);
    try captures.add(@intCast(layer_idx), "value_proj", normed_attn_f32, H, H, total);

    // Q projection — use linearLoRA if LoRA is enabled in the config.
    const q_w = try getLayerWeight(cb, layer_idx, "attn.query_proj.weight", &name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, layer_idx, "attn.query_proj.bias", &name_buf);
    defer cb.free(q_b);
    const Q_raw = try linearWithLoRA(cb, normed_attn, q_w, q_b, layer_idx, "query_proj", config.lora_rank, config.lora_alpha, total, H, H);
    defer cb.free(Q_raw);

    // K projection — no LoRA on key.
    const k_w = try getLayerWeight(cb, layer_idx, "attn.key_proj.weight", &name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, layer_idx, "attn.key_proj.bias", &name_buf);
    defer cb.free(k_b);
    const K_raw = try cb.linear(normed_attn, k_w, k_b, total, H, H);
    defer cb.free(K_raw);

    // V projection — use linearLoRA if LoRA is enabled in the config.
    const v_w = try getLayerWeight(cb, layer_idx, "attn.value_proj.weight", &name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, layer_idx, "attn.value_proj.bias", &name_buf);
    defer cb.free(v_b);
    const V = try linearWithLoRA(cb, normed_attn, v_w, v_b, layer_idx, "value_proj", config.lora_rank, config.lora_alpha, total, H, H);
    defer cb.free(V);

    // Apply RoPE to Q and K.
    // consecutive_pairs=true: ModernBERT uses interleaved rotation pairs
    // (matching gopeft fused_chunker_embedder.go convention).
    // rope_dim == head_dim: the full head dimension is rotated.
    const Q = try cb.rope(Q_raw, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, true);
    defer cb.free(Q);
    const K = try cb.rope(K_raw, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, true);
    defer cb.free(K);

    // For local layers build a sliding-window additive attention bias.
    // Shape: [num_heads * seq_len * seq_len] (shared across the batch).
    // The BLAS sdpaOp detects len == num_heads*seq_len*seq_len and applies it
    // as a per-head shared bias added to raw dot-product scores before softmax.
    const window_bias: ?CT = if (!is_global) blk: {
        const half: usize = @intCast(config.local_attention_window / 2);
        break :blk try buildSlidingWindowBias(cb, allocator, seq_len, num_heads, half);
    } else null;
    defer if (window_bias) |wb| cb.free(wb);

    // Bidirectional scaled dot-product attention (encoder, no causal mask).
    // The padding mask (attention_mask) is consumed by the backend: positions
    // where mask[b*seq_len + ki] == 0 are set to -inf before softmax.
    const attn_out = try cb.scaledDotProductAttention(
        Q,
        K,
        V,
        attention_mask,
        window_bias,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
    defer cb.free(attn_out);

    // Output projection
    const out_w = try getLayerWeight(cb, layer_idx, "attn.Wo.weight", &name_buf);
    defer cb.free(out_w);
    const out_b = try getLayerWeight(cb, layer_idx, "attn.Wo.bias", &name_buf);
    defer cb.free(out_b);
    const attn_proj = try cb.linear(attn_out, out_w, out_b, total, H, H);
    defer cb.free(attn_proj);

    // Residual: add the projected attention output to the *original* (pre-norm)
    // hidden state — pre-norm residual pattern.
    const hidden_after_attn = try cb.add(attn_proj, hidden);
    defer cb.free(hidden_after_attn);

    // -----------------------------------------------------------------------
    // FFN sub-layer  (pre-norm, GeGLU)
    // -----------------------------------------------------------------------

    // Pre-FFN LayerNorm
    const mlp_ln_w = try getLayerWeight(cb, layer_idx, "mlp_norm.weight", &name_buf);
    defer cb.free(mlp_ln_w);
    const mlp_ln_b = try getLayerWeight(cb, layer_idx, "mlp_norm.bias", &name_buf);
    defer cb.free(mlp_ln_b);
    const normed_ffn = try cb.layerNorm(hidden_after_attn, mlp_ln_w, mlp_ln_b, H, config.layer_norm_eps);
    defer cb.free(normed_ffn);

    // GeGLU feed-forward (Wi and Wo both have no bias in ModernBERT's MLP)
    const Wi_w = try getLayerWeight(cb, layer_idx, "mlp.Wi.weight", &name_buf);
    defer cb.free(Wi_w);
    const Wo_w = try getLayerWeight(cb, layer_idx, "mlp.Wo.weight", &name_buf);
    defer cb.free(Wo_w);

    const ffn_out = try geGluFfn(cb, normed_ffn, Wi_w, Wo_w, total, H, intermediate, config.checkpoint_layout == .huggingface_fused_qkv_no_bias, null, null);
    defer cb.free(ffn_out);

    // Residual: add FFN output to post-attention hidden state
    return cb.add(ffn_out, hidden_after_attn);
}

fn deinitTestWeightStore(allocator: std.mem.Allocator, store: *native_compute.WeightStore) void {
    native_compute.deinitPrefetchQueue(store);
    var it = store.resident_weights.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit();
    }
    store.resident_weights.deinit(allocator);
    store.lazy_weights.deinit(allocator);
}

fn putTestWeight(
    allocator: std.mem.Allocator,
    store: *native_compute.WeightStore,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !void {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    var tensor = try tensor_mod.Tensor.initFloat32(allocator, owned_name, shape, data);
    errdefer tensor.deinit();
    try store.resident_weights.put(allocator, owned_name, weight_source.LoadedWeight{ .tensor = tensor });
}

fn putTinyHfLayer(allocator: std.mem.Allocator, store: *native_compute.WeightStore, layer: usize, include_attn_norm: bool) !void {
    var name_buf: [128]u8 = undefined;
    if (include_attn_norm) try putTestWeight(allocator, store, try std.fmt.bufPrint(&name_buf, "model.layers.{d}.attn_norm.weight", .{layer}), &.{4}, &.{ 1, 1, 1, 1 });
    try putTestWeight(allocator, store, try std.fmt.bufPrint(&name_buf, "model.layers.{d}.mlp_norm.weight", .{layer}), &.{4}, &.{ 1, 1, 1, 1 });
    var qkv: [48]f32 = @splat(0);
    var projection: [16]f32 = @splat(0);
    for (0..3) |block| {
        for (0..4) |d| qkv[(block * 4 + d) * 4 + d] = 0.25;
    }
    for (0..4) |d| projection[d * 4 + d] = 0.5;
    try putTestWeight(allocator, store, try std.fmt.bufPrint(&name_buf, "model.layers.{d}.attn.Wqkv.weight", .{layer}), &.{ 12, 4 }, &qkv);
    try putTestWeight(allocator, store, try std.fmt.bufPrint(&name_buf, "model.layers.{d}.attn.Wo.weight", .{layer}), &.{ 4, 4 }, &projection);
    try putTestWeight(allocator, store, try std.fmt.bufPrint(&name_buf, "model.layers.{d}.mlp.Wi.weight", .{layer}), &.{ 8, 4 }, &(@as([32]f32, @splat(0))));
    try putTestWeight(allocator, store, try std.fmt.bufPrint(&name_buf, "model.layers.{d}.mlp.Wo.weight", .{layer}), &.{ 4, 4 }, &(@as([16]f32, @splat(0))));
}

test "HuggingFace ModernBERT config selects fused bias-free checkpoint layout" {
    const cfg = try parseConfig(std.testing.allocator, "{\"model_type\":\"modernbert\",\"hidden_size\":768,\"num_hidden_layers\":22,\"num_attention_heads\":12,\"intermediate_size\":1152,\"vocab_size\":50368,\"max_position_embeddings\":8192,\"local_attention\":128,\"global_attn_every_n_layers\":3}");
    try std.testing.expectEqual(CheckpointLayout.huggingface_fused_qkv_no_bias, cfg.checkpoint_layout);
    try std.testing.expect(!cfg.rope_interleaved);
    try std.testing.expectEqual(@as(u32, 128), cfg.local_attention_window);
    try std.testing.expectEqual(@as(u32, 8192), cfg.max_position_embeddings);
}

test "ModernBERT row segments drop padding and refuse inner padding" {
    const a = std.testing.allocator;
    const rows = (try rowSegments(a, &.{ 7, 8, 9, 4, 5, 0 }, &.{ 1, 1, 1, 1, 1, 0 }, 2, 3)).?;
    defer rows.deinit(a);
    try std.testing.expectEqual(@as(usize, 5), rows.tokens);
    try std.testing.expectEqualSlices(i64, &.{ 7, 8, 9, 4, 5 }, rows.ids);
    try std.testing.expectEqualSlices(u32, &.{ 0, 3, 0, 0, 0, 0 }, rows.ranges[0..6]);
    try std.testing.expectEqualSlices(u32, &.{ 3, 5, 0, 0, 0, 0 }, rows.ranges[4 * 6 ..][0..6]);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, 2, 0, 1 }, rows.positions);
    try std.testing.expectEqualSlices(i64, &.{ 0, 1, 2, 3, 4, 3 }, rows.restore);
    try std.testing.expect((try rowSegments(a, &.{ 1, 2, 3 }, &.{ 1, 0, 1 }, 1, 3)) == null);
    try std.testing.expect((try rowSegments(a, &.{ 1, 2, 3 }, &.{ 0, 0, 0 }, 1, 3)) == null);
}

test "native ModernBERT row segments preserve two-row padded global and local forwards" {
    const a = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer deinitTestWeightStore(a, &store);
    var compute = native_compute.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    try putTestWeight(a, &store, "model.embeddings.tok_embeddings.weight", &.{ 8, 4 }, &.{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
        0, 0, 0, 1,
        1, 1, 0, 0,
        0, 1, 1, 0,
        0, 0, 1, 1,
        1, 0, 0, 1,
    });
    try putTestWeight(a, &store, "model.embeddings.norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try putTestWeight(a, &store, "model.final_norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try putTinyHfLayer(a, &store, 0, false);
    try putTinyHfLayer(a, &store, 1, true);
    const cfg = Config{
        .vocab_size = 8,
        .hidden_size = 4,
        .num_hidden_layers = 2,
        .num_attention_heads = 2,
        .intermediate_size = 4,
        .max_position_embeddings = 8,
        .global_attn_every_n_layers = 2,
        .local_attention_window = 2,
        .checkpoint_layout = .huggingface_fused_qkv_no_bias,
        .rope_interleaved = false,
    };
    const batched = try forward(&cb, a, cfg, &.{ 0, 1, 2, 3, 4, 0 }, &.{ 1, 1, 1, 1, 1, 0 }, 2, 3);
    defer a.free(batched);
    const first = try forward(&cb, a, cfg, &.{ 0, 1, 2 }, &.{ 1, 1, 1 }, 1, 3);
    defer a.free(first);
    const second = try forward(&cb, a, cfg, &.{ 3, 4 }, &.{ 1, 1 }, 1, 2);
    defer a.free(second);
    for (batched[0..12], first) |got, want| try std.testing.expectApproxEqAbs(want, got, 2e-5);
    for (batched[12..20], second) |got, want| try std.testing.expectApproxEqAbs(want, got, 2e-5);
    // Restored padding maps to a valid row within its own segment. Consumers
    // ignore it via the original attention mask, but it must never cross rows.
    for (batched[20..24], second[0..4]) |got, want| try std.testing.expectApproxEqAbs(want, got, 2e-5);
}

test "Transformers 5 ModernBERT config reads per-layer-type rope and layer types" {
    const cfg = try parseConfig(std.testing.allocator,
        \\{"model_type":"modernbert","num_hidden_layers":4,"norm_eps":1e-6,
        \\"layer_types":["full_attention","sliding_attention","sliding_attention","full_attention"],
        \\"rope_parameters":{"full_attention":{"rope_theta":160000.0},"sliding_attention":{"rope_theta":160000.0}}}
    );
    try std.testing.expectEqual(@as(f32, 160000), cfg.global_rope_theta);
    try std.testing.expectEqual(@as(f32, 160000), cfg.local_rope_theta);
    try std.testing.expectEqual(@as(f32, 1e-6), cfg.layer_norm_eps);
    try std.testing.expectError(error.UnsupportedModernBertLayerTypes, parseConfig(std.testing.allocator,
        \\{"model_type":"modernbert","num_hidden_layers":3,
        \\"layer_types":["full_attention","full_attention","sliding_attention"]}
    ));
}

test "GLiNER2.5 Decide 1B ModernBERT geometry is accepted exactly" {
    const cfg = try parseConfig(std.testing.allocator,
        \\{"model_type":"modernbert","vocab_size":50378,"hidden_size":1792,
        \\"intermediate_size":3840,"num_hidden_layers":28,"num_attention_heads":28,
        \\"max_position_embeddings":7999,"local_attention":128,
        \\"global_attn_every_n_layers":3,"position_embedding_type":"sans_pos",
        \\"layer_types":["full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention","sliding_attention","sliding_attention","full_attention"],
        \\"rope_parameters":{"full_attention":{"rope_theta":160000.0},"sliding_attention":{"rope_theta":160000.0}}}
    );
    try std.testing.expectEqual(@as(u32, 1792), cfg.hidden_size);
    try std.testing.expectEqual(@as(u32, 3840), cfg.intermediate_size);
    try std.testing.expectEqual(@as(u32, 28), cfg.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 28), cfg.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 64), cfg.hidden_size / cfg.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 50378), cfg.vocab_size);
    try std.testing.expectEqual(@as(u32, 7999), cfg.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 128), cfg.local_attention_window);
    try std.testing.expectEqual(@as(f32, 160000), cfg.global_rope_theta);
    try std.testing.expectEqual(@as(f32, 160000), cfg.local_rope_theta);
}

test "ModernBERT rejects unsafe execution geometry" {
    for ([_][]const u8{
        "[]",
        "{\"model_type\":\"modernbert\",\"hidden_size\":0}",
        "{\"model_type\":\"modernbert\",\"num_attention_heads\":0}",
        "{\"model_type\":\"modernbert\",\"hidden_size\":768,\"num_attention_heads\":7}",
        "{\"model_type\":\"modernbert\",\"hidden_size\":15,\"num_attention_heads\":3}",
        "{\"model_type\":\"modernbert\",\"global_attn_every_n_layers\":0}",
        "{\"model_type\":\"modernbert\",\"local_attention\":0}",
        "{\"model_type\":\"modernbert\",\"hidden_size\":\"1792\"}",
        "{\"model_type\":\"modernbert\",\"vocab_size\":-1}",
        "{\"model_type\":\"modernbert\",\"global_rope_theta\":0}",
        "{\"model_type\":\"modernbert\",\"norm_eps\":-0.001}",
        "{\"model_type\":\"modernbert\",\"rope_parameters\":[]}",
        "{\"model_type\":\"modernbert\",\"rope_parameters\":{\"full_attention\":{\"rope_theta\":\"160000\"}}}",
        "{\"model_type\":\"modernbert\",\"rope_parameters\":{\"full_attention\":{\"rope_theta\":160000}}}",
        "{\"model_type\":17}",
        "{\"model_type\":\"bert\"}",
    }) |json| {
        try std.testing.expectError(error.InvalidModernBertConfig, parseConfig(std.testing.allocator, json));
    }
    var invalid = Config{};
    invalid.global_rope_theta = std.math.inf(f32);
    try std.testing.expectError(error.InvalidModernBertConfig, validateConfig(invalid));
    invalid = Config{};
    invalid.layer_norm_eps = std.math.nan(f32);
    try std.testing.expectError(error.InvalidModernBertConfig, validateConfig(invalid));
}

test "ModernBERT rejects declared semantics its fused kernels do not implement" {
    const prefix = "{\"model_type\":\"modernbert\",";
    for ([_][]const u8{
        "\"hidden_activation\":\"relu\"}",
        "\"hidden_activation\":false}",
        "\"attention_bias\":true}",
        "\"attention_bias\":\"false\"}",
        "\"mlp_bias\":true}",
        "\"norm_bias\":true}",
    }) |suffix| {
        const json = try std.mem.concat(std.testing.allocator, u8, &.{ prefix, suffix });
        defer std.testing.allocator.free(json);
        try std.testing.expectError(error.UnsupportedModernBertConfig, parseConfig(std.testing.allocator, json));
    }
    for ([_][]const u8{
        "{\"model_type\":\"modernbert\",\"rope_parameters\":{\"full_attention\":{\"rope_theta\":160000,\"rope_type\":\"linear\"},\"sliding_attention\":{\"rope_theta\":160000}}}",
        "{\"model_type\":\"modernbert\",\"rope_parameters\":{\"full_attention\":{\"rope_theta\":160000,\"factor\":2},\"sliding_attention\":{\"rope_theta\":160000}}}",
    }) |json| try std.testing.expectError(error.UnsupportedModernBertRope, parseConfig(std.testing.allocator, json));

    const supported = try parseConfig(std.testing.allocator,
        \\{"model_type":"modernbert","hidden_activation":"gelu","position_embedding_type":"sans_pos",
        \\"attention_bias":false,"mlp_bias":false,"norm_bias":false,
        \\"rope_parameters":{"full_attention":{"rope_theta":160000,"rope_type":"default"},"sliding_attention":{"rope_theta":160000}}}
    );
    try std.testing.expectEqual(CheckpointLayout.huggingface_fused_qkv_no_bias, supported.checkpoint_layout);

    // Transformers ignores `position_embedding_type` for ModernBERT; the
    // reference answerdotai/ModernBERT and Laya checkpoints declare
    // "absolute" and still use RoPE.
    for ([_][]const u8{ "\"absolute\"", "\"sans_pos\"", "null" }) |value| {
        const json = try std.mem.concat(std.testing.allocator, u8, &.{ prefix, "\"position_embedding_type\":", value, "}" });
        defer std.testing.allocator.free(json);
        const parsed = try parseConfig(std.testing.allocator, json);
        try std.testing.expectEqual(CheckpointLayout.huggingface_fused_qkv_no_bias, parsed.checkpoint_layout);
        try std.testing.expect(!parsed.rope_interleaved);
    }
}

test "ModernBERT accepts prepared Laya legacy position metadata without changing RoPE" {
    const prefix =
        \\{"model_type":"modernbert","hidden_size":64,"num_hidden_layers":2,
        \\"num_attention_heads":1,"intermediate_size":96,"max_position_embeddings":128,
        \\"global_rope_theta":160000,"local_rope_theta":10000,
        \\"laya":{"head_layers":2,"max_len":128,"head_max_len":64},
    ;
    const baseline = try parseConfig(std.testing.allocator, prefix ++ "\"attention_bias\":false}");
    for ([_][]const u8{ "\"sans_pos\"", "\"absolute\"", "null", "false", "\"relative\"" }) |position_type| {
        const json = try std.fmt.allocPrint(std.testing.allocator, "{s}\"position_embedding_type\":{s}}}", .{ prefix, position_type });
        defer std.testing.allocator.free(json);
        const config = try parseConfig(std.testing.allocator, json);
        try std.testing.expectEqualDeep(baseline, config);
        try std.testing.expect(config.laya != null);
        try std.testing.expect(!config.rope_interleaved);
        try std.testing.expectEqual(CheckpointLayout.huggingface_fused_qkv_no_bias, config.checkpoint_layout);
    }
    for ([_][]const u8{
        "\"position_embedding_type\":\"absolute\",\"hidden_activation\":\"relu\"}",
        "\"position_embedding_type\":\"absolute\",\"attention_bias\":true}",
        "\"position_embedding_type\":\"absolute\",\"mlp_bias\":true}",
        "\"position_embedding_type\":\"absolute\",\"norm_bias\":true}",
    }) |suffix| {
        const json = try std.mem.concat(std.testing.allocator, u8, &.{ prefix, suffix });
        defer std.testing.allocator.free(json);
        try std.testing.expectError(error.UnsupportedModernBertConfig, parseConfig(std.testing.allocator, json));
    }
    try std.testing.expectError(error.InvalidLayaConfig, parseConfig(std.testing.allocator,
        \\{"model_type":"modernbert","position_embedding_type":"absolute","laya":null}
    ));
    try std.testing.expectError(error.InvalidLayaConfig, parseConfig(std.testing.allocator,
        \\{"model_type":"modernbert","position_embedding_type":"absolute","max_position_embeddings":128,
        \\"laya":{"max_len":512,"head_max_len":64}}
    ));
}

test "HuggingFace ModernBERT fused checkpoint omits layer zero attention norm and all biases" {
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitTestWeightStore(allocator, &store);
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();

    // This is the exact public checkpoint structure in miniature: bare
    // ModernBERT names are canonicalized to model.*, QKV is fused, all norms
    // and projections are bias-free, and layer zero's attention norm is an
    // identity (so its tensors are deliberately absent).
    try putTestWeight(allocator, &store, "model.embeddings.tok_embeddings.weight", &.{ 4, 4 }, &.{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
        0, 0, 0, 1,
    });
    try putTestWeight(allocator, &store, "model.embeddings.norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try putTestWeight(allocator, &store, "model.final_norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try putTestWeight(allocator, &store, "model.layers.0.attn.Wqkv.weight", &.{ 12, 4 }, &(@as([48]f32, @splat(0))));
    try putTestWeight(allocator, &store, "model.layers.0.attn.Wo.weight", &.{ 4, 4 }, &(@as([16]f32, @splat(0))));
    try putTestWeight(allocator, &store, "model.layers.0.mlp_norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try putTestWeight(allocator, &store, "model.layers.0.mlp.Wi.weight", &.{ 8, 4 }, &(@as([32]f32, @splat(0))));
    try putTestWeight(allocator, &store, "model.layers.0.mlp.Wo.weight", &.{ 4, 4 }, &(@as([16]f32, @splat(0))));
    try putTestWeight(allocator, &store, "classifier.0.weight", &.{ 8, 4 }, &(@as([32]f32, @splat(0))));
    try putTestWeight(allocator, &store, "classifier.0.bias", &.{8}, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    try putTestWeight(allocator, &store, "classifier.2.weight", &.{ 1, 8 }, &.{ 1, 1, 1, 1, 1, 1, 1, 1 });
    try putTestWeight(allocator, &store, "classifier.2.bias", &.{1}, &.{0.5});

    const output = try forward(&cb, allocator, .{
        .vocab_size = 4,
        .hidden_size = 4,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .intermediate_size = 4,
        .max_position_embeddings = 8,
        .checkpoint_layout = .huggingface_fused_qkv_no_bias,
    }, &.{ 0, 1 }, &.{ 1, 1 }, 1, 2);
    defer allocator.free(output);
    try std.testing.expectEqual(@as(usize, 8), output.len);

    // The GLiNER2.5 span classifier consumes raw [L] marker states from the
    // selected encoder. Exercise its exact H -> 2H -> 1 geometry against the
    // miniature ModernBERT output so a DeBERTa-only head assumption cannot
    // creep back into the runtime route.
    const hidden = try cb.fromFloat32Shape(output, &.{ 2, 4 });
    defer cb.free(hidden);
    const marker = (try cb.takeRows(hidden, &.{1}, 1, 4)) orelse return error.TestUnexpectedResult;
    defer cb.free(marker);
    const w0 = try cb.getWeight("classifier.0.weight");
    defer cb.free(w0);
    const b0 = try cb.getWeight("classifier.0.bias");
    defer cb.free(b0);
    const first_linear = try cb.linear(marker, w0, b0, 1, 4, 8);
    defer cb.free(first_linear);
    const first = try cb.relu(first_linear);
    defer cb.free(first);
    const w2 = try cb.getWeight("classifier.2.weight");
    defer cb.free(w2);
    const b2 = try cb.getWeight("classifier.2.bias");
    defer cb.free(b2);
    const logits = try cb.linear(first, w2, b2, 1, 8, 1);
    defer cb.free(logits);
    const values = try cb.toFloat32(logits, allocator);
    defer allocator.free(values);
    try std.testing.expectEqualSlices(f32, &.{36.5}, values);
}

test "HuggingFace ModernBERT GeGLU uses exact erf activation" {
    const a = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitTestWeightStore(a, &store);
    var compute = native_compute.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const input = try cb.fromFloat32Shape(&.{1}, &.{ 1, 1 });
    defer cb.free(input);
    const wi = try cb.fromFloat32Shape(&.{ -2, 1 }, &.{ 2, 1 });
    defer cb.free(wi);
    const wo = try cb.fromFloat32Shape(&.{1}, &.{ 1, 1 });
    defer cb.free(wo);
    const result = try geGluFfn(&cb, input, wi, wo, 1, 1, 1, true, null, null);
    defer cb.free(result);
    const values = try cb.toFloat32(result, a);
    defer a.free(values);
    // GELU(-2) = -1 * (1 + erf(-sqrt(2))). Tanh gives -0.0454023.
    try std.testing.expectApproxEqAbs(@as(f32, -0.0455002639), values[0], 3e-7);
}

test "ModernBERT packed GeGLU preserves odd rows widths and exact activation" {
    const a = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitTestWeightStore(a, &store);
    var compute = native_compute.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ([_]usize{ 1, 7, 17 }) |width| {
        const rows = 3;
        const source = try a.alloc(f32, rows * width * 2);
        defer a.free(source);
        for (source, 0..) |*x, i| x.* = @as(f32, @floatFromInt(i % 31)) / 4 - 4;
        const input = try cb.fromFloat32Shape(source, &.{ rows, @intCast(width * 2) });
        defer cb.free(input);
        const fused = (try cb.packedGegluExact(input, rows, width)) orelse return error.MissingPackedGeglu;
        defer cb.free(fused);
        const gate = try cb.sliceLastDim(input, 0, width);
        defer cb.free(gate);
        const value = try cb.sliceLastDim(input, width, width * 2);
        defer cb.free(value);
        const activated = (try cb.geluExact(gate)).?;
        defer cb.free(activated);
        const expected = try cb.multiply(activated, value);
        defer cb.free(expected);
        const actual_values = try cb.toFloat32(fused, a);
        defer a.free(actual_values);
        const expected_values = try cb.toFloat32(expected, a);
        defer a.free(expected_values);
        for (actual_values, expected_values) |actual, reference|
            try std.testing.expectApproxEqAbs(reference, actual, 2e-6);
        try std.testing.expect(try cb.packedGegluExact(input, rows + 1, width) == null);
    }
}
