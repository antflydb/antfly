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
// Weight naming follows the HuggingFace ModernBERT safetensors convention:
//   model.embeddings.tok_embeddings.weight
//   model.embeddings.norm.{weight,bias}
//   model.layers.N.attn_norm.{weight,bias}
//   model.layers.N.attn.{query_proj,key_proj,value_proj}.{weight,bias}
//   model.layers.N.attn.Wo.{weight,bias}
//   model.layers.N.mlp_norm.{weight,bias}
//   model.layers.N.mlp.Wi.weight          [2*intermediate_size, hidden_size]
//   model.layers.N.mlp.Wo.weight          [hidden_size, intermediate_size]
//   model.final_norm.{weight,bias}
//
// Single implementation works with any ComputeBackend (BLAS, MLX, etc).

const std = @import("std");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

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
    use_geglu: bool = true,
    /// LoRA rank for query_proj and value_proj.  0 = LoRA disabled.
    /// When non-zero the encoder tries to load lora_a/lora_b weight tensors
    /// from the active WeightStore and uses linearLoRA for Q/V projections.
    lora_rank: u32 = 0,
    /// LoRA scaling alpha.  The effective scale applied to the LoRA delta is
    /// alpha / rank.  Defaults to rank (i.e., scale = 1.0) when 0 is passed.
    lora_alpha: f32 = 0.0,
};

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
    // 1. Token embeddings + embedding LayerNorm.
    //    ModernBERT has no absolute position embeddings; RoPE is applied in each
    //    attention layer instead.
    var hidden = try embeddingsBlock(cb, config, input_ids, batch * seq_len);

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
        );
        cb.free(hidden);
        hidden = new_hidden;
    }

    // 3. Final layer norm
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
// Embeddings block
// ---------------------------------------------------------------------------

fn embeddingsBlock(
    cb: *const ComputeBackend,
    config: Config,
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
    const ln_b = try cb.getWeight("model.embeddings.norm.bias");
    defer cb.free(ln_b);

    return cb.layerNorm(tok_emb, ln_w, ln_b, H, 1e-5);
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

    const ffn_out = try geGluFfn(cb, allocator, normed_ffn, Wi_w, Wo_w, total, H, intermediate);
    defer cb.free(ffn_out);

    // Residual: add FFN output to post-attention hidden state
    return cb.add(ffn_out, hidden_after_attn);
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
// The gate/value split requires slicing the last dimension.  Rather than
// requiring a backend-native slice primitive we download to f32, split on
// the CPU, apply GELU*mul, and re-upload.  This is backend-agnostic and
// consistent with the existing splitLastDim3 fallback path in ops.zig.

fn geGluFfn(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    Wi_w: CT,
    Wo_w: CT,
    total: usize,
    hidden_size: usize,
    intermediate_size: usize,
) !CT {
    // Project to 2*intermediate.  Wi is [2*intermediate, hidden] (row-major,
    // transposed by the linear op) so the output is [total, 2*intermediate].
    const gated_ct = try cb.linearNoBias(input, Wi_w, total, hidden_size, 2 * intermediate_size);
    defer cb.free(gated_ct);

    // Read back to f32 for the split + GELU*mul on the CPU.
    const gated_data = try cb.toFloat32(gated_ct, allocator);
    defer allocator.free(gated_data);

    if (gated_data.len != total * 2 * intermediate_size) return error.UnexpectedOutputShape;

    // Allocate output buffer: [total, intermediate_size]
    const activated = try allocator.alloc(f32, total * intermediate_size);
    defer allocator.free(activated);

    for (0..total) |row| {
        const src = row * 2 * intermediate_size;
        const gate_row = gated_data[src..][0..intermediate_size];
        const value_row = gated_data[src + intermediate_size ..][0..intermediate_size];
        const dst = row * intermediate_size;
        for (0..intermediate_size) |i| {
            activated[dst + i] = geluTanh(gate_row[i]) * value_row[i];
        }
    }

    // Re-upload the activated tensor and apply the down-projection.
    const activated_ct = try cb.fromFloat32(activated);
    defer cb.free(activated_ct);

    // Wo is [hidden, intermediate] so the output is [total, hidden].
    return cb.linearNoBias(activated_ct, Wo_w, total, intermediate_size, hidden_size);
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
// GELU activation — tanh approximation (matches PyTorch default / gopeft)
// ---------------------------------------------------------------------------
//
//   GELU(x) ≈ 0.5 · x · (1 + tanh(√(2/π) · (x + 0.044715·x³)))

inline fn geluTanh(x: f32) f32 {
    // √(2/π) ≈ 0.7978845608028654
    const k: f32 = 0.7978845608028654;
    const c: f32 = 0.044715;
    return 0.5 * x * (1.0 + std.math.tanh(k * (x + c * x * x * x)));
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
        cb, allocator, config, input_ids, attention_mask, batch, seq_len, captures,
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
    // command buffer submission on MLX) instead of one per layer.
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
            cb, allocator, config, hidden, attention_mask, batch, seq_len, layer_idx,
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

    // Batch-download all normed_attn tensors — single GPU sync on MLX.
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
        Q, K, V, attention_mask, window_bias, batch, seq_len, num_heads, head_dim,
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

    const ffn_out = try geGluFfn(cb, allocator, normed_ffn, Wi_w, Wo_w, total, H, intermediate);
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

    const ffn_out = try geGluFfn(cb, allocator, normed_ffn, Wi_w, Wo_w, total, H, intermediate);
    defer cb.free(ffn_out);

    // Residual: add FFN output to post-attention hidden state
    return cb.add(ffn_out, hidden_after_attn);
}
