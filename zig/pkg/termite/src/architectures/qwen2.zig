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

// Qwen2 text decoder architecture using abstract ComputeBackend ops.
//
// Pure text decoder (no vision tower, no VL projection). Mirrors the shape
// of `bert.zig` so that colqwen2 and other downstream pipelines can swap in
// a native forward pass on top of any ComputeBackend (native, ...).
//
// Qwen2 architectural notes (applies to Qwen2-0.5B / 1.5B / 7B / Coder):
//   - Pre-norm transformer decoder with `num_hidden_layers` blocks.
//   - Each block:
//         x1 = x  + attn( rmsNorm(x)  )
//         x2 = x1 + mlp ( rmsNorm(x1) )
//   - Grouped-query attention (GQA): `num_kv_heads` <= `num_attention_heads`,
//     K/V are replicated (num_heads / num_kv_heads) times along the head axis.
//     The replication is delegated to the backend's `gqaCausalAttention` op
//     which accepts `(num_heads, num_kv_heads)` directly.
//   - RoPE on Q and K only (not V), with `rope_theta = 1_000_000.0`.
//   - SwiGLU MLP: `down_proj( silu(gate_proj(x)) * up_proj(x) )`.
//   - Final `model.norm` RMSNorm after the last layer.
//   - Qwen2 checkpoints include biases on q_proj / k_proj / v_proj (unlike
//     LLaMA). o_proj, gate/up/down have NO bias.
//   - This file returns hidden states (shape `[batch*seq_len, hidden_size]`
//     as a flat f32 slice); the caller applies `lm_head` if logits are
//     desired. That keeps the training / embedding extraction paths aligned
//     with `bert.zig`.
//
// TODO (sliding-window attention): Some Qwen2 variants (e.g. Qwen2-Coder-32k)
// use a sliding window of size `sliding_window`. This first cut implements
// full causal attention only. The Config carries the field so callers can
// still load those weights, and when we wire up a windowed backend op we
// can branch on `config.sliding_window > 0` here. Qwen2-0.5B / 1.5B / 7B
// (the colqwen2 base) all use full attention, so this is adequate for the
// immediate colqwen2 target.

const std = @import("std");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

/// Qwen2 text decoder configuration.
///
/// Reference values for common variants:
///   Qwen2-0.5B:  hidden=896,  layers=24, heads=14, kv_heads=2,  inter=4864,  head_dim=64
///   Qwen2-1.5B:  hidden=1536, layers=28, heads=12, kv_heads=2,  inter=8960,  head_dim=128
///   Qwen2-7B:    hidden=3584, layers=28, heads=28, kv_heads=4,  inter=18944, head_dim=128
pub const Config = struct {
    vocab_size: u32,
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    /// Grouped-query attention: number of K/V heads. Must divide
    /// `num_attention_heads`.
    num_kv_heads: u32,
    /// Per-head dimension. For standard Qwen2 this is
    /// `hidden_size / num_attention_heads`, but Qwen2-7B overrides it to 128.
    head_dim: u32,
    /// SwiGLU inner dimension (gate_proj / up_proj output size).
    intermediate_size: u32,
    max_position_embeddings: u32,
    rope_theta: f32 = 1_000_000.0,
    rms_norm_eps: f32 = 1e-6,
    /// When true, `lm_head.weight` is tied to `model.embed_tokens.weight`.
    /// (Qwen2-0.5B / 1.5B tie; 7B does not.)
    tie_word_embeddings: bool = false,
    /// If > 0, sliding-window attention with this many tokens. 0 = full
    /// causal. Currently only 0 is honored (see TODO at top of file).
    sliding_window: u32 = 0,

    pub fn qHeadsPerKv(self: Config) u32 {
        return self.num_attention_heads / self.num_kv_heads;
    }

    pub fn qDim(self: Config) u32 {
        return self.num_attention_heads * self.head_dim;
    }

    pub fn kvDim(self: Config) u32 {
        return self.num_kv_heads * self.head_dim;
    }
};

// ── Public forward API ──────────────────────────────────────────────────────
// Same four-function shape as bert.zig so downstream code can be generic over
// architectures that use a pre-norm residual stack.

/// Full Qwen2 decoder forward pass: token IDs → final hidden states.
///
/// Returns an owned `f32` slice of shape `[batch * seq_len * hidden_size]`.
/// The caller applies any LM-head / pooling / similarity ops on the returned
/// buffer (or recovers a CT via `fromFloat32Shape` if they want to stay on
/// the backend).
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    return forwardUntilLayer(cb, allocator, config, input_ids, attention_mask, batch, seq_len, config.num_hidden_layers);
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
    const H: usize = @intCast(config.hidden_size);
    const total = batch * seq_len;
    const clamped_stop = @min(stop_layer_exclusive, config.num_hidden_layers);

    var hidden = try embeddings(cb, allocator, config, input_ids, total, H);
    errdefer cb.free(hidden);

    for (0..clamped_stop) |layer| {
        const new_hidden = try decoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // If we ran every layer, apply the final model.norm. When stopping early
    // (e.g. to hand off to a LoRA-trained head that lives between layers), the
    // caller is responsible for any normalization on the returned hidden
    // states — mirrors bert.forwardUntilLayer semantics.
    if (clamped_stop == config.num_hidden_layers) {
        const final_normed = try applyFinalNorm(cb, hidden, config);
        cb.free(hidden);
        hidden = final_normed;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
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
    const H: usize = @intCast(config.hidden_size);
    const total = batch * seq_len;
    if (hidden_in.len != total * H) return error.ShapeMismatch;

    const shape = [_]i32{ @intCast(total), @intCast(H) };
    var hidden = try cb.fromFloat32Shape(hidden_in, &shape);
    errdefer cb.free(hidden);

    const clamped_start = @min(start_layer, config.num_hidden_layers);
    const clamped_end = @max(clamped_start, @min(end_layer_exclusive, config.num_hidden_layers));

    for (clamped_start..clamped_end) |layer| {
        const new_hidden = try decoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (clamped_end == config.num_hidden_layers) {
        const final_normed = try applyFinalNorm(cb, hidden, config);
        cb.free(hidden);
        hidden = final_normed;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

// ── Internal helpers ────────────────────────────────────────────────────────

/// Token embedding lookup. Qwen2 uses RoPE-only positional encoding, so we
/// intentionally do not add any learned/absolute position embeddings here.
fn embeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    total: usize,
    H: usize,
) !CT {
    _ = allocator;
    _ = config;
    const embed_w = try cb.getWeight("model.embed_tokens.weight");
    defer cb.free(embed_w);
    return cb.embeddingLookup(embed_w, input_ids, total, H);
}

/// One decoder block: pre-norm attention + residual, pre-norm SwiGLU MLP +
/// residual. Matches HF `Qwen2DecoderLayer.forward`.
fn decoderLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
) !CT {
    const H: usize = @intCast(config.hidden_size);
    const eps = config.rms_norm_eps;

    // --- Self-attention sublayer: x + attn(rmsnorm(x)) ---
    const attn_normed = try rmsNormForLayer(cb, hidden_in, layer, eps, "input_layernorm", H);
    defer cb.free(attn_normed);

    const attn_out = try attention(cb, allocator, config, attn_normed, attention_mask, batch, seq_len, layer);
    defer cb.free(attn_out);

    const attn_res = try cb.add(hidden_in, attn_out);
    errdefer cb.free(attn_res);

    // --- SwiGLU MLP sublayer: attn_res + mlp(rmsnorm(attn_res)) ---
    const mlp_normed = try rmsNormForLayer(cb, attn_res, layer, eps, "post_attention_layernorm", H);
    defer cb.free(mlp_normed);

    const mlp_out = try mlp(cb, config, mlp_normed, layer);
    defer cb.free(mlp_out);

    const result = try cb.add(attn_res, mlp_out);
    cb.free(attn_res);
    return result;
}

/// Grouped-query self-attention with RoPE and causal masking.
///
/// Weight layout (HF Qwen2):
///   q_proj: [num_heads    * head_dim, hidden]  + bias [num_heads    * head_dim]
///   k_proj: [num_kv_heads * head_dim, hidden]  + bias [num_kv_heads * head_dim]
///   v_proj: [num_kv_heads * head_dim, hidden]  + bias [num_kv_heads * head_dim]
///   o_proj: [hidden, num_heads * head_dim]     (no bias)
///
/// The backend's `gqaCausalAttention` handles K/V head replication internally,
/// applies scaled dot-product + causal mask, and returns
/// `[batch*seq_len, num_heads*head_dim]`.
fn attention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
) !CT {
    _ = allocator;
    _ = attention_mask; // TODO: honor per-token padding mask once we wire the
    // backend's masked attention path into gqaCausalAttention. For a full
    // forward pass on right-padded batches the causal mask already zeros
    // padding-to-real attention; this matches bert.zig's current fidelity.

    const H: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const num_kv_heads: usize = @intCast(config.num_kv_heads);
    const head_dim: usize = @intCast(config.head_dim);
    const q_dim: usize = num_heads * head_dim;
    const kv_dim: usize = num_kv_heads * head_dim;
    const total = batch * seq_len;

    if (num_heads % num_kv_heads != 0) return error.InvalidGqaConfig;

    var name_buf: [256]u8 = undefined;

    // Q projection (with bias).
    const q_w = try getLayerWeight(cb, layer, "self_attn.q_proj.weight", &name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, layer, "self_attn.q_proj.bias", &name_buf);
    defer cb.free(q_b);
    const Q0 = try cb.linear(hidden_in, q_w, q_b, total, H, q_dim);
    defer cb.free(Q0);

    // K projection (with bias).
    const k_w = try getLayerWeight(cb, layer, "self_attn.k_proj.weight", &name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, layer, "self_attn.k_proj.bias", &name_buf);
    defer cb.free(k_b);
    const K0 = try cb.linear(hidden_in, k_w, k_b, total, H, kv_dim);
    defer cb.free(K0);

    // V projection (with bias). No RoPE on V.
    const v_w = try getLayerWeight(cb, layer, "self_attn.v_proj.weight", &name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, layer, "self_attn.v_proj.bias", &name_buf);
    defer cb.free(v_b);
    const V = try cb.linear(hidden_in, v_w, v_b, total, H, kv_dim);
    defer cb.free(V);

    // RoPE on Q and K. Qwen2 uses full head_dim rotation with non-consecutive
    // pairs layout (matches HF default apply_rotary_pos_emb).
    const rope_dim: usize = head_dim; // rotate the whole head
    const freq_scale: f32 = 1.0;
    const consecutive_pairs = false;

    const Q = try cb.rope(Q0, seq_len, head_dim, rope_dim, config.rope_theta, freq_scale, 0, consecutive_pairs);
    defer cb.free(Q);
    const K = try cb.rope(K0, seq_len, head_dim, rope_dim, config.rope_theta, freq_scale, 0, consecutive_pairs);
    defer cb.free(K);

    // Fused GQA causal attention. Backend replicates K/V as needed and applies
    // the lower-triangular causal mask internally.
    const attn_out = try cb.gqaCausalAttention(Q, K, V, null, batch, seq_len, num_heads, num_kv_heads, head_dim);
    defer cb.free(attn_out);

    // o_proj (no bias).
    const o_w = try getLayerWeight(cb, layer, "self_attn.o_proj.weight", &name_buf);
    defer cb.free(o_w);
    return cb.linearNoBias(attn_out, o_w, total, q_dim, H);
}

/// SwiGLU MLP sublayer:
///   down_proj( silu(gate_proj(x)) * up_proj(x) )
fn mlp(
    cb: *const ComputeBackend,
    config: Config,
    hidden_in: CT,
    layer: usize,
) !CT {
    const H: usize = @intCast(config.hidden_size);
    const I: usize = @intCast(config.intermediate_size);

    // Infer row count from the tensor's backend-reported shape when available,
    // but in practice we know total = batch*seq_len from the enclosing layer.
    // We rely on the backend to reshape as needed. `linearNoBias` takes `rows`
    // explicitly — compute it from hidden tensor logical length.
    // For consistency with the rest of the file, we forward-compute rows using
    // the caller's invariants: mlp is only called from decoderLayer where the
    // enclosing tensor has already been sized to [batch*seq_len, H].
    //
    // To keep this helper purely local, we accept the backend's rows-handling
    // via a shape lookup; if the backend does not support tensorShape we use
    // a fallback by asking the first linear to infer rows from the tensor's
    // element count (which the backend knows). Both linearNoBias signatures
    // take rows explicitly, so we need a real value here. Use a tensorShape
    // query; if unavailable, require the caller to have used contiguous 2D
    // layout where rows is encoded in the first dim.

    // In practice, inside this file `mlp` is called with hidden_in already
    // shaped [total, H], and `total` is known in `decoderLayer` but not
    // passed. To avoid an extra parameter and match the bert.zig style, we
    // look up the shape from the backend.
    const shape = cb.tensorShape(hidden_in, std.heap.page_allocator) catch null;
    defer if (shape) |s| std.heap.page_allocator.free(s);
    const total: usize = blk: {
        if (shape) |s| {
            if (s.len >= 2) break :blk @intCast(s[0]);
            if (s.len == 1) break :blk @intCast(@divExact(s[0], @as(i64, @intCast(H))));
        }
        return error.UnknownRowsForMlp;
    };

    var name_buf: [256]u8 = undefined;

    // gate_proj: [I, H] — no bias in Qwen2.
    const gate_w = try getLayerWeight(cb, layer, "mlp.gate_proj.weight", &name_buf);
    defer cb.free(gate_w);
    const gate_raw = try cb.linearNoBias(hidden_in, gate_w, total, H, I);
    defer cb.free(gate_raw);
    const gate_act = try cb.silu(gate_raw);
    defer cb.free(gate_act);

    // up_proj: [I, H] — no bias.
    const up_w = try getLayerWeight(cb, layer, "mlp.up_proj.weight", &name_buf);
    defer cb.free(up_w);
    const up = try cb.linearNoBias(hidden_in, up_w, total, H, I);
    defer cb.free(up);

    const gated = try cb.multiply(gate_act, up);
    defer cb.free(gated);

    // down_proj: [H, I] — no bias.
    const down_w = try getLayerWeight(cb, layer, "mlp.down_proj.weight", &name_buf);
    defer cb.free(down_w);
    return cb.linearNoBias(gated, down_w, total, I, H);
}

/// Apply `model.layers.{layer}.{gain_tensor_name}.weight` as the scale for a
/// standard Qwen2 RMSNorm (no bias, no mean subtraction).
fn rmsNormForLayer(
    cb: *const ComputeBackend,
    hidden: CT,
    layer: usize,
    eps: f32,
    gain_tensor_name: []const u8,
    H: usize,
) !CT {
    var name_buf: [256]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, "model.layers.{d}.{s}.weight", .{ layer, gain_tensor_name }) catch return error.NameTooLong;
    const w = try cb.getWeight(name);
    defer cb.free(w);
    return cb.rmsNorm(hidden, w, H, eps);
}

fn applyFinalNorm(cb: *const ComputeBackend, hidden: CT, config: Config) !CT {
    const H: usize = @intCast(config.hidden_size);
    const w = try cb.getWeight("model.norm.weight");
    defer cb.free(w);
    return cb.rmsNorm(hidden, w, H, config.rms_norm_eps);
}

/// Build a `model.layers.{layer}.{suffix}` name and look up the weight.
fn getLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

// ── Tests ────────────────────────────────────────────────────────────────────
//
// A real forward-pass test needs a fully populated `WeightStore` with every
// Qwen2 parameter for `num_hidden_layers` layers. That bootstrap is a
// non-trivial amount of code (equivalent to a tiny HF safetensors writer) and
// is deferred to the colqwen2 trainer harness which already has a real
// checkpoint loader.
//
// The checks below keep this file honest at `zig build test` time without
// requiring any weights:
//   - Config field types and helper math round-trip correctly.
//   - The four public forward functions are reachable (compile-time takes
//     their address).
//
// When the colqwen2 trainer lands, it will exercise `forward` end-to-end
// against a real Qwen2-0.5B checkpoint.

test "Config derived dims match Qwen2-0.5B" {
    const cfg = Config{
        .vocab_size = 151_936,
        .hidden_size = 896,
        .num_hidden_layers = 24,
        .num_attention_heads = 14,
        .num_kv_heads = 2,
        .head_dim = 64,
        .intermediate_size = 4864,
        .max_position_embeddings = 32_768,
        .tie_word_embeddings = true,
    };
    try std.testing.expectEqual(@as(u32, 7), cfg.qHeadsPerKv());
    try std.testing.expectEqual(@as(u32, 896), cfg.qDim());
    try std.testing.expectEqual(@as(u32, 128), cfg.kvDim());
    try std.testing.expectEqual(@as(f32, 1_000_000.0), cfg.rope_theta);
}

test "Config derived dims match Qwen2-7B (head_dim override)" {
    const cfg = Config{
        .vocab_size = 152_064,
        .hidden_size = 3584,
        .num_hidden_layers = 28,
        .num_attention_heads = 28,
        .num_kv_heads = 4,
        .head_dim = 128,
        .intermediate_size = 18944,
        .max_position_embeddings = 131_072,
    };
    try std.testing.expectEqual(@as(u32, 7), cfg.qHeadsPerKv());
    try std.testing.expectEqual(@as(u32, 3584), cfg.qDim());
    try std.testing.expectEqual(@as(u32, 512), cfg.kvDim());
}

test "public forward API is reachable" {
    // Compile-time references — ensures signatures stay in sync with
    // bert.zig's shape (caller code can be templated over either file).
    const _forward: *const fn (
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        config: Config,
        input_ids: []const i64,
        attention_mask: []const i64,
        batch: usize,
        seq_len: usize,
    ) anyerror![]f32 = &forward;
    _ = _forward;

    const _until: *const fn (
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        config: Config,
        input_ids: []const i64,
        attention_mask: []const i64,
        batch: usize,
        seq_len: usize,
        stop_layer_exclusive: usize,
    ) anyerror![]f32 = &forwardUntilLayer;
    _ = _until;

    const _from: *const fn (
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        config: Config,
        hidden_in: []const f32,
        attention_mask: []const i64,
        batch: usize,
        seq_len: usize,
        start_layer: usize,
    ) anyerror![]f32 = &forwardFromHidden;
    _ = _from;

    const _range: *const fn (
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        config: Config,
        hidden_in: []const f32,
        attention_mask: []const i64,
        batch: usize,
        seq_len: usize,
        start_layer: usize,
        end_layer_exclusive: usize,
    ) anyerror![]f32 = &forwardFromHiddenRange;
    _ = _range;
}
