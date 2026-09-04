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
    return layer * modern_bert_linear_specs.len + @intFromEnum(kind);
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
    }
    return true;
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
                .retain_dense_fallback = false,
            }))) return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub const Config = struct {
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

    const obj = parsed.value.object;
    var config = Config{};
    if (obj.get("vocab_size")) |value| config.vocab_size = jsonU32(value) orelse config.vocab_size;
    if (obj.get("hidden_size")) |value| config.hidden_size = jsonU32(value) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |value| config.num_hidden_layers = jsonU32(value) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |value| config.num_attention_heads = jsonU32(value) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |value| config.intermediate_size = jsonU32(value) orelse config.intermediate_size;
    if (obj.get("max_position_embeddings")) |value| config.max_position_embeddings = jsonU32(value) orelse config.max_position_embeddings;
    if (obj.get("global_attn_every_n_layers")) |value| config.global_attn_every_n_layers = jsonU32(value) orelse config.global_attn_every_n_layers;
    if (obj.get("local_attention")) |value| config.local_attention_window = jsonU32(value) orelse config.local_attention_window;
    if (obj.get("global_rope_theta")) |value| config.global_rope_theta = jsonF32(value) orelse config.global_rope_theta;
    if (obj.get("local_rope_theta")) |value| config.local_rope_theta = jsonF32(value) orelse config.local_rope_theta;
    if (obj.get("layer_norm_eps")) |value| config.layer_norm_eps = jsonF32(value) orelse config.layer_norm_eps;

    // `modernbert` is Transformers' public checkpoint layout. Keep the
    // historical layout available to the fused-chunker training code.
    if (obj.get("model_type")) |value| {
        if (value == .string and isModernBertModel(value.string)) {
            config.checkpoint_layout = .huggingface_fused_qkv_no_bias;
            // Transformers' `rotate_half` layout is split-half, not
            // consecutive (interleaved) pairs.
            config.rope_interleaved = false;
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
    if (cb.kind() == .metal and metalEncoderFrameEnabled() and !cb.decoderRuntimeHasActiveFrame()) {
        encoder_frame_active = try cb.decoderRuntimeBeginFrame();
    }
    errdefer if (encoder_frame_active) cb.decoderRuntimeCancelFrame() catch {};

    // 1. Token embeddings + embedding LayerNorm.
    //    ModernBERT has no absolute position embeddings; RoPE is applied in each
    //    attention layer instead.
    var hidden = try embeddingsBlock(cb, config, zero_bias, input_ids, batch * seq_len);

    // 2. Encoder layers
    for (0..config.num_hidden_layers) |layer_idx| {
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
        );
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 3. Final layer norm
    var name_buf: [128]u8 = undefined;
    const fn_w = try cb.getWeight(std.fmt.bufPrint(&name_buf, "model.final_norm.weight", .{}) catch return error.NameTooLong);
    defer cb.free(fn_w);
    const normed_final = if (zero_bias) |bias|
        try cb.layerNorm(hidden, fn_w, bias, @intCast(config.hidden_size), config.layer_norm_eps)
    else blk: {
        const fn_b = try cb.getWeight(std.fmt.bufPrint(&name_buf, "model.final_norm.bias", .{}) catch return error.NameTooLong);
        defer cb.free(fn_b);
        break :blk try cb.layerNorm(hidden, fn_w, fn_b, @intCast(config.hidden_size), config.layer_norm_eps);
    };
    cb.free(hidden);
    if (encoder_frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        encoder_frame_active = false;
    }
    return normed_final;
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
) !CT {
    const H = config.hidden_size;

    // Word / token embeddings
    const tok_emb_w = try cb.getWeight("model.embeddings.tok_embeddings.weight");
    defer cb.free(tok_emb_w);
    const tok_emb = try cb.embeddingLookup(tok_emb_w, input_ids, total, H);
    defer cb.free(tok_emb);

    // Embedding-level LayerNorm (replaces post-sum norm from classic BERT)
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

    // HuggingFace ModernBERT makes the layer-0 attention norm an identity.
    const identity_attn_norm = config.checkpoint_layout == .huggingface_fused_qkv_no_bias and layer_idx == 0;
    const normed_attn = if (identity_attn_norm) hidden else blk: {
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
        &name_buf,
    );
    defer cb.free(qkv.q);
    defer cb.free(qkv.k);
    defer cb.free(qkv.v);

    // Apply RoPE to Q and K. HuggingFace ModernBERT's `rotate_half` uses
    // split-half rotation; the legacy checkpoint retains interleaved pairs.
    // rope_dim == head_dim: the full head dimension is rotated.
    const Q = try cb.rope(qkv.q, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, config.rope_interleaved);
    defer cb.free(Q);
    const K = try cb.rope(qkv.k, seq_len, head_dim, head_dim, rope_theta, 1.0, 0, config.rope_interleaved);
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
        qkv.v,
        attention_mask,
        window_bias,
        batch,
        seq_len,
        num_heads,
        head_dim,
    );
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
    const hidden_after_attn = try cb.add(attn_proj, hidden);
    defer cb.free(hidden_after_attn);

    // -----------------------------------------------------------------------
    // FFN sub-layer  (pre-norm, GeGLU)
    // -----------------------------------------------------------------------

    // Pre-FFN LayerNorm
    const mlp_ln_w = try getLayerWeight(cb, layer_idx, "mlp_norm.weight", &name_buf);
    defer cb.free(mlp_ln_w);
    const normed_ffn = if (zero_bias) |bias|
        try cb.layerNorm(hidden_after_attn, mlp_ln_w, bias, H, config.layer_norm_eps)
    else blk: {
        const mlp_ln_b = try getLayerWeight(cb, layer_idx, "mlp_norm.bias", &name_buf);
        defer cb.free(mlp_ln_b);
        break :blk try cb.layerNorm(hidden_after_attn, mlp_ln_w, mlp_ln_b, H, config.layer_norm_eps);
    };
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
};

fn projectQkv(
    cb: *const ComputeBackend,
    config: Config,
    input: CT,
    layer_idx: usize,
    rows: usize,
    hidden_size: usize,
    slot: ?usize,
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

    const gate_ct = try cb.sliceLastDim(gated_ct, 0, intermediate_size);
    defer cb.free(gate_ct);
    const value_ct = try cb.sliceLastDim(gated_ct, intermediate_size, 2 * intermediate_size);
    defer cb.free(value_ct);

    const activated_ct = (try cb.activationMultiply(gate_ct, value_ct, .gelu)) orelse blk: {
        const gate_gelu_ct = try cb.gelu(gate_ct);
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

    var hidden = try embeddingsBlock(cb, config, input_ids, total_tokens);
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

    const ffn_out = try geGluFfn(cb, normed_ffn, Wi_w, Wo_w, total, H, intermediate, null, null);
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

    const ffn_out = try geGluFfn(cb, normed_ffn, Wi_w, Wo_w, total, H, intermediate, null, null);
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

test "HuggingFace ModernBERT config selects fused bias-free checkpoint layout" {
    const cfg = try parseConfig(std.testing.allocator, "{\"model_type\":\"modernbert\",\"hidden_size\":768,\"num_hidden_layers\":22,\"num_attention_heads\":12,\"intermediate_size\":1152,\"vocab_size\":50368,\"max_position_embeddings\":8192,\"local_attention\":128,\"global_attn_every_n_layers\":3}");
    try std.testing.expectEqual(CheckpointLayout.huggingface_fused_qkv_no_bias, cfg.checkpoint_layout);
    try std.testing.expect(!cfg.rope_interleaved);
    try std.testing.expectEqual(@as(u32, 128), cfg.local_attention_window);
    try std.testing.expectEqual(@as(u32, 8192), cfg.max_position_embeddings);
}

test "HuggingFace ModernBERT fused checkpoint omits layer zero attention norm and all biases" {
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitTestWeightStore(allocator, &store);
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
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
    try putTestWeight(allocator, &store, "model.layers.0.attn.Wqkv.weight", &.{ 12, 4 }, &([_]f32{0} ** 48));
    try putTestWeight(allocator, &store, "model.layers.0.attn.Wo.weight", &.{ 4, 4 }, &([_]f32{0} ** 16));
    try putTestWeight(allocator, &store, "model.layers.0.mlp_norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try putTestWeight(allocator, &store, "model.layers.0.mlp.Wi.weight", &.{ 8, 4 }, &([_]f32{0} ** 32));
    try putTestWeight(allocator, &store, "model.layers.0.mlp.Wo.weight", &.{ 4, 4 }, &([_]f32{0} ** 16));

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
}
