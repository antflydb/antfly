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

// Non-cached, end-to-end BERT reranker LoRA trainer with a real forward pass.
//
// This module supplements (does NOT replace) the cached-replay training paths
// in `reranker_lora.zig`. It calls the real BERT encoder forward so that:
//
//   * NEFTune noise is applied at the true token-embedding point, not on a
//     precomputed cache that would defeat the regularizer.
//   * The linear head sees features produced by the current LoRA adapters,
//     not a frozen snapshot.
//
// Forward wiring (in `trainStep`):
//
//     1. `bert.forwardUntilLayer(cb, ..., 0)`
//            -> embedding output [B*T, H]
//     2. NEFTune in-place on the embedding buffer (skipped when alpha == 0)
//     3. `bert.forwardFromHidden(cb, ..., 0)` followed by
//        `bert.forwardUntilLayer(cb, ..., last)` path to recover the pre-last-
//        layer hidden state (used as the "input" for LoRA grad accumulation)
//     4. `bert.forwardFromHiddenRange(cb, ..., last, last+1)` to run the final
//        encoder layer and produce the final hidden state
//     5. Mean-pool over valid tokens per example -> pooled [B, H]
//     6. Linear classifier head -> logit [B]
//     7. Binary cross-entropy loss against {0, 1} labels
//
// Backward wiring (surrogate — NOT full autodiff):
//
//     * Exact gradient for the linear head (weight + bias).
//     * `dL/dpooled = (prob - label) * head_weight`.
//     * `dL/dhidden[b, t, :] = dL/dpooled[b, :] / valid_tokens[b]` for valid
//        tokens (mean-pool Jacobian), zero elsewhere.
//     * LoRA gradients are accumulated on the LAST encoder layer's target
//       modules only, using the pre-last-layer hidden state as the linear
//       "input" and `dL/dhidden` as the `output_grad`. This treats every
//       encoder layer up to (and including) the pre-final-layer as a frozen
//       feature extractor: the upstream encoder contributes no gradient to
//       the adapters, matching the "forward-accurate, backward-surrogate"
//       philosophy of this trainer. A follow-up that replaces the surrogate
//       with an autodiff tape would thread `dL/dhidden` through every layer.
//
// Non-goals (explicit follow-ups):
//
//     * DeBERTa support — `RealForwardTrainConfig.use_deberta = true` is
//       parsed but `trainStep` errors with `error.DebertaFollowupPending`.
//       Once deberta.zig's `forwardUntilLayer` is wrapped in the same shape
//       as bert's call site, this becomes a two-line dispatch.
//     * DDP / multi-rank training. Callers can accumulate locally and perform
//       their own all-reduce on the returned gradient buffers.
//     * Full autodiff through every transformer layer. The surrogate
//       described above updates only the LAST layer's LoRA adapters.
//     * Weight-decay / LR-schedule / grad-clip bookkeeping — those are
//       `config` fields for the caller to honour at optimizer-step time.
//
// The LoRAAdapterSet type is owned by `lora_adapter_set.zig`. Its
// `LoRALayer` stores:
//     A: [rank,         in_features]
//     B: [out_features, rank        ]
// but `lora.accumulateLinearLoRAGrads` expects the CPU convention
//     A: [in_features,  rank        ]
//     B: [rank,         out_features]
// so we build transposed scratch copies per accumulation and transpose the
// resulting grads back into `layer.grad_A / grad_B`. This is ugly but keeps
// the on-disk and runtime adapter layout unchanged.

const std = @import("std");
const lora = @import("lora.zig");
const neftune = @import("neftune.zig");
const bert = @import("../architectures/bert.zig");
const bert_types = @import("../models/bert.zig");
const ops = @import("../ops/ops.zig");
const fused_chunker_lora = @import("lora_adapter_set.zig");
const coord_mod = @import("training_memory_coordinator.zig");
const residency_mod = @import("grad_residency.zig");
const budget_mod = @import("training_budget.zig");

pub const LoRAAdapterSet = fused_chunker_lora.LoRAAdapterSet;
pub const LoRALayer = fused_chunker_lora.LoRALayer;
pub const TrainingMemoryCoordinator = coord_mod.TrainingMemoryCoordinator;

// ---------------------------------------------------------------------------
// Public configuration & example types
// ---------------------------------------------------------------------------

pub const RealForwardTrainConfig = struct {
    max_seq_len: u32 = 256,
    batch_size: u32 = 8,
    learning_rate: f32 = 2e-5,
    num_epochs: u32 = 3,
    warmup_steps: u32 = 50,
    weight_decay: f32 = 0.01,
    max_grad_norm: f32 = 1.0,
    neftune_alpha: f32 = 0.0,
    /// false = BERT (supported), true = DeBERTa (follow-up, errors out).
    use_deberta: bool = false,
};

pub const RealForwardExample = struct {
    /// Concatenated query + separator + document token IDs. Length = seq_len.
    input_ids: []const i64,
    /// 1 for real tokens, 0 for padding. Length = input_ids.len.
    attention_mask: []const i64,
    /// Optional segment / token-type IDs (null for DeBERTa). Length = seq_len
    /// when present.
    token_type_ids: ?[]const i64,
    /// Binary relevance label: 1.0 if doc is relevant, else 0.0.
    label: f32,
    /// Always 1 today — batching is handled by concatenating multiple
    /// examples in `trainStep`. The field is kept so a caller can feed a
    /// pre-batched example directly without re-packing.
    batch: usize,
    seq_len: usize,
};

pub const StepResult = struct {
    loss: f32,
    accuracy: f32,
    step: u64,
};

// ---------------------------------------------------------------------------
// Core numerical helpers (pure; unit-tested)
// ---------------------------------------------------------------------------

/// Mean-pool `final_hidden` ([B*T, H]) over valid tokens according to
/// `attention_mask` ([B*T], 1.0 for real, 0.0 for padding). Returns
/// `pooled` [B, H] and `valid_counts` [B] (both caller-owned).
fn meanPool(
    allocator: std.mem.Allocator,
    final_hidden: []const f32,
    attention_mask_f32: []const f32,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
) !struct { pooled: []f32, valid_counts: []f32 } {
    std.debug.assert(final_hidden.len == batch * seq_len * hidden_size);
    std.debug.assert(attention_mask_f32.len == batch * seq_len);

    const pooled = try allocator.alloc(f32, batch * hidden_size);
    errdefer allocator.free(pooled);
    @memset(pooled, 0);

    const valid_counts = try allocator.alloc(f32, batch);
    errdefer allocator.free(valid_counts);
    @memset(valid_counts, 0);

    for (0..batch) |b| {
        var count: f32 = 0;
        for (0..seq_len) |t| {
            const m = attention_mask_f32[b * seq_len + t];
            if (m <= 0.5) continue;
            count += 1;
            const src_base = (b * seq_len + t) * hidden_size;
            const dst_base = b * hidden_size;
            for (0..hidden_size) |h| {
                pooled[dst_base + h] += final_hidden[src_base + h];
            }
        }
        // Clamp to 1 to avoid divide-by-zero on fully-padded rows.
        const denom = if (count > 0) count else 1.0;
        const dst_base = b * hidden_size;
        for (0..hidden_size) |h| pooled[dst_base + h] /= denom;
        valid_counts[b] = count;
    }

    return .{ .pooled = pooled, .valid_counts = valid_counts };
}

fn sigmoid(x: f32) f32 {
    if (x >= 0) {
        const z = @exp(-x);
        return 1.0 / (1.0 + z);
    } else {
        const z = @exp(x);
        return z / (1.0 + z);
    }
}

/// Numerically-stable binary cross entropy from the raw logit.
fn bceFromLogit(logit: f32, label: f32) f32 {
    // log(1 + exp(-|x|)) + max(x, 0) - x * label
    const ax = @abs(logit);
    const softplus = @log(1.0 + @exp(-ax));
    const max_part = if (logit > 0) logit else 0.0;
    return softplus + max_part - logit * label;
}

/// Pure helper: given the final hidden state tensor and the linear head
/// parameters, compute (a) the loss, (b) accuracy, (c) the gradient w.r.t.
/// the head weight/bias, and (d) the gradient w.r.t. the hidden state.
/// `grad_head_weight` and `grad_head_bias` are ACCUMULATED (caller zeroes
/// before the first step of an accumulation window). `d_hidden_out` is
/// overwritten each call — it is treated as a scratch output buffer.
///
/// This is the testable core: it takes no ComputeBackend and no BERT config,
/// so the unit tests can call it directly with synthetic hidden states.
pub fn computeLossAndGrads(
    allocator: std.mem.Allocator,
    final_hidden: []const f32,
    attention_mask_f32: []const f32,
    labels: []const f32,
    head_weight: []const f32,
    head_bias: f32,
    grad_head_weight: []f32,
    grad_head_bias: *f32,
    d_hidden_out: []f32,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
) !struct { loss: f32, accuracy: f32 } {
    std.debug.assert(final_hidden.len == batch * seq_len * hidden_size);
    std.debug.assert(attention_mask_f32.len == batch * seq_len);
    std.debug.assert(labels.len == batch);
    std.debug.assert(head_weight.len == hidden_size);
    std.debug.assert(grad_head_weight.len == hidden_size);
    std.debug.assert(d_hidden_out.len == batch * seq_len * hidden_size);

    const pool_result = try meanPool(allocator, final_hidden, attention_mask_f32, batch, seq_len, hidden_size);
    defer allocator.free(pool_result.pooled);
    defer allocator.free(pool_result.valid_counts);
    const pooled = pool_result.pooled;
    const valid_counts = pool_result.valid_counts;

    @memset(d_hidden_out, 0);

    var total_loss: f32 = 0;
    var correct: u32 = 0;
    for (0..batch) |b| {
        // Forward: logit = dot(pooled[b, :], head_weight) + bias
        var logit: f32 = head_bias;
        const pooled_base = b * hidden_size;
        for (0..hidden_size) |h| logit += pooled[pooled_base + h] * head_weight[h];

        const prob = sigmoid(logit);
        const label = labels[b];
        total_loss += bceFromLogit(logit, label);

        const pred: f32 = if (prob >= 0.5) 1.0 else 0.0;
        if (pred == label) correct += 1;

        // Backward:
        //   dL/dlogit = prob - label
        //   dL/dhead_weight += dL/dlogit * pooled[b, :]
        //   dL/dhead_bias   += dL/dlogit
        //   dL/dpooled       = dL/dlogit * head_weight
        //   dL/dhidden[b,t,:] = dL/dpooled / valid_count[b]  (mean-pool grad)
        const dlogit = prob - label;
        grad_head_bias.* += dlogit;
        for (0..hidden_size) |h| grad_head_weight[h] += dlogit * pooled[pooled_base + h];

        const count = valid_counts[b];
        if (count <= 0) continue;
        const inv_count = 1.0 / count;

        for (0..seq_len) |t| {
            const m = attention_mask_f32[b * seq_len + t];
            if (m <= 0.5) continue;
            const dst_base = (b * seq_len + t) * hidden_size;
            for (0..hidden_size) |h| {
                d_hidden_out[dst_base + h] = dlogit * head_weight[h] * inv_count;
            }
        }
    }

    const bf: f32 = @floatFromInt(batch);
    return .{
        .loss = total_loss / bf,
        .accuracy = @as(f32, @floatFromInt(correct)) / bf,
    };
}

// ---------------------------------------------------------------------------
// LoRA grad helpers (handling the A/B layout mismatch)
// ---------------------------------------------------------------------------

/// Pick the "target modules" we consider for LoRA on the final encoder layer.
/// We match the four reranker defaults used by `reranker_lora.zig` and by the
/// fused chunker adapter set: Q, K, V and the attention output projection.
/// A given adapter set may name these differently (the fused chunker uses the
/// `query_proj`/`key_proj`/`value_proj`/`out_proj` naming, which is also fine
/// — only string matching is used here).
const candidate_lora_modules = [_][]const u8{
    "query",         "key",          "value",        "attention.output.dense",
    "query_proj",    "key_proj",     "value_proj",   "out_proj",
};

const lora_grad_helpers = @import("lora_grad_helpers.zig");
const accumulateLoRAGradsForLayer = lora_grad_helpers.accumulateForLayer;

// ---------------------------------------------------------------------------
// Main training step
// ---------------------------------------------------------------------------

/// Run one training step on a batch of `RealForwardExample` values. All
/// examples must share the same `seq_len`; batching is done by concatenating
/// their input_ids / masks into one flat BERT call.
///
/// On success, accumulates gradients into:
///   * `grad_head_weight` (size = hidden_size)
///   * `grad_head_bias`
///   * `adapter_set.layers[*].grad_A / grad_B` for the final encoder layer's
///     target modules (see `candidate_lora_modules`).
///
/// Caller is responsible for zeroing these buffers before the step if it
/// wants a fresh accumulation, and for running the optimizer step afterwards.
pub fn trainStep(
    allocator: std.mem.Allocator,
    compute_backend: *const ops.ComputeBackend,
    bert_config: bert_types.Config,
    adapter_set: *LoRAAdapterSet,
    head_weight: []f32,
    head_bias: *f32,
    grad_head_weight: []f32,
    grad_head_bias: *f32,
    examples: []const RealForwardExample,
    config: RealForwardTrainConfig,
    step: u64,
    /// Optional Hypura-style memory coordinator. When non-null, the trainer
    /// reserves an activation budget up front (returning `error.BudgetDenied`
    /// on overflow), records the embedding→last-layer segment on the
    /// checkpoint harness for later recompute planning, and pins each
    /// updated LoRA grad block during accumulation so NVMe spill between
    /// micro-batches cannot evict a block mid-backward.
    coord: ?*TrainingMemoryCoordinator,
) !StepResult {
    if (examples.len == 0) return error.EmptyBatch;
    if (config.use_deberta) return error.DebertaFollowupPending;

    const hidden_size: usize = @intCast(bert_config.hidden_size);
    const num_layers: usize = @intCast(bert_config.num_hidden_layers);
    if (num_layers == 0) return error.InvalidBertConfig;
    if (head_weight.len != hidden_size) return error.HeadShapeMismatch;
    if (grad_head_weight.len != hidden_size) return error.HeadGradShapeMismatch;

    // All examples must share seq_len.
    const seq_len = examples[0].seq_len;
    for (examples) |ex| {
        if (ex.seq_len != seq_len) return error.HeterogeneousBatch;
        if (ex.input_ids.len != seq_len) return error.BadInputIdsLength;
        if (ex.attention_mask.len != seq_len) return error.BadMaskLength;
    }
    const batch = examples.len;
    const total = batch * seq_len;

    // ---- Memory coordinator: reserve activations up front ----------------
    // Three hidden-state buffers live simultaneously at peak (embeddings,
    // pre_last, final). Reserve them as a single activation chunk so the
    // coordinator can deny the step before we allocate if the budget is full.
    const activation_bytes: u64 = @as(u64, total) * @as(u64, hidden_size) * @sizeOf(f32) * 3;
    var activation_reserved: bool = false;
    defer if (coord) |c| {
        if (activation_reserved) {
            c.budget.release(.activations, .host, activation_bytes);
        }
    };
    if (coord) |c| {
        const res = c.budget.tryReserve(.activations, .host, activation_bytes);
        if (res.event != .admitted) return error.BudgetDenied;
        activation_reserved = true;
        c.resetSegments();
    }

    // Pack the batch into flat buffers.
    const input_ids = try allocator.alloc(i64, total);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i64, total);
    defer allocator.free(attention_mask);
    const attention_mask_f32 = try allocator.alloc(f32, total);
    defer allocator.free(attention_mask_f32);
    const labels = try allocator.alloc(f32, batch);
    defer allocator.free(labels);

    var has_tt = false;
    for (examples) |ex| {
        if (ex.token_type_ids != null) {
            has_tt = true;
            break;
        }
    }
    var token_type_ids: ?[]i64 = null;
    if (has_tt) {
        token_type_ids = try allocator.alloc(i64, total);
    }
    defer if (token_type_ids) |tt| allocator.free(tt);

    for (examples, 0..) |ex, b| {
        std.mem.copyForwards(i64, input_ids[b * seq_len .. (b + 1) * seq_len], ex.input_ids);
        std.mem.copyForwards(i64, attention_mask[b * seq_len .. (b + 1) * seq_len], ex.attention_mask);
        for (0..seq_len) |t| {
            attention_mask_f32[b * seq_len + t] = @floatFromInt(ex.attention_mask[t]);
        }
        if (token_type_ids) |tt| {
            if (ex.token_type_ids) |src| {
                std.mem.copyForwards(i64, tt[b * seq_len .. (b + 1) * seq_len], src);
            } else {
                @memset(tt[b * seq_len .. (b + 1) * seq_len], 0);
            }
        }
        labels[b] = ex.label;
    }

    // ---- Forward pass ------------------------------------------------------

    // Step 1: embedding output (no transformer layers applied yet).
    const embeddings = try bert.forwardUntilLayer(
        compute_backend,
        allocator,
        bert_config,
        input_ids,
        attention_mask,
        token_type_ids,
        batch,
        seq_len,
        0,
    );
    defer allocator.free(embeddings);

    // Step 2: NEFTune on the real embedding buffer before any attention.
    if (config.neftune_alpha > 0.0) {
        neftune.applyInPlace(
            embeddings,
            attention_mask_f32,
            total,
            hidden_size,
            config.neftune_alpha,
            step,
        );
    }

    // Record the (0, last_layer) segment for the checkpoint harness. The
    // reranker trainer does not currently need to recompute this segment
    // (pre_last_hidden is kept in memory), but recording it makes the
    // harness visible in coordinator stats and unlocks a future full-
    // autodiff variant that frees pre_last_hidden between forward and
    // backward and rebuilds it via `recomputeSegment`.
    if (coord) |c| {
        if (c.harness != null) {
            _ = try c.recordSegment(0, @intCast(num_layers - 1), embeddings, total, hidden_size, null);
        }
    }

    // Step 3: Run layers [0, num_layers - 1) to get the pre-last-layer hidden
    // state. This is what we'll feed `accumulateLinearLoRAGrads` as "inputs".
    const last_layer = num_layers - 1;
    const pre_last_hidden = try bert.forwardFromHiddenRange(
        compute_backend,
        allocator,
        bert_config,
        embeddings,
        attention_mask,
        batch,
        seq_len,
        0,
        last_layer,
    );
    defer allocator.free(pre_last_hidden);

    // Step 4: Run the final layer to get the final hidden state.
    const final_hidden = try bert.forwardFromHiddenRange(
        compute_backend,
        allocator,
        bert_config,
        pre_last_hidden,
        attention_mask,
        batch,
        seq_len,
        last_layer,
        num_layers,
    );
    defer allocator.free(final_hidden);

    // ---- Loss + head gradients + d_hidden --------------------------------

    const d_hidden = try allocator.alloc(f32, total * hidden_size);
    defer allocator.free(d_hidden);

    const core = try computeLossAndGrads(
        allocator,
        final_hidden,
        attention_mask_f32,
        labels,
        head_weight,
        head_bias.*,
        grad_head_weight,
        grad_head_bias,
        d_hidden,
        batch,
        seq_len,
        hidden_size,
    );

    // ---- LoRA grads on the final layer -----------------------------------
    // The surrogate: we treat `pre_last_hidden` as the "input" to each of the
    // final layer's target projections, and `d_hidden` as the gradient at the
    // output of the final layer. This under-counts by ignoring the routing
    // through softmax(QK^T)V, LayerNorms, residuals, etc. — but it keeps the
    // update direction aligned with the classifier's error signal, which is
    // the point of a surrogate trainer.
    const last_u32: u32 = @intCast(last_layer);
    for (candidate_lora_modules, 0..) |mod_name, mod_idx| {
        const layer_ptr = adapter_set.get(last_u32, mod_name) orelse continue;
        if (layer_ptr.in_features != hidden_size) continue;
        // The module may map hidden->intermediate (wi) or hidden->hidden (Q/K/V/out).
        // For the surrogate we only handle hidden->hidden pairs because
        // d_hidden is what we have — a wi/wo grad needs gradient at the MLP
        // intermediate activation, which we don't compute here.
        if (layer_ptr.out_features != hidden_size) continue;

        // Pin the gradient block while we're writing to it — this prevents
        // a concurrent coordinator spill-to-fit from evicting it
        // mid-accumulation. The block must have been registered with the
        // coordinator before this call (callers typically do that once at
        // trainer setup). If the block is not registered, pin silently no-
        // ops on the coordinator's residency tracker and the trainer is
        // still correct.
        const block_id = residency_mod.GradBlockId{
            .layer_idx = last_u32,
            .module_idx = @intCast(mod_idx),
        };
        var pinned: bool = false;
        if (coord) |c| {
            if (c.residency.entry(block_id) != null) {
                c.pinGradBlock(block_id) catch |err| switch (err) {
                    residency_mod.GradResidencyError.UnknownBlock => {},
                    else => return err,
                };
                pinned = true;
            }
        }
        defer if (pinned) {
            if (coord) |c| {
                c.unpinGradBlock(block_id) catch {};
            }
        };

        try accumulateLoRAGradsForLayer(
            allocator,
            layer_ptr,
            adapter_set,
            pre_last_hidden,
            d_hidden,
            total,
        );
    }

    return .{
        .loss = core.loss,
        .accuracy = core.accuracy,
        .step = step,
    };
}

// ---------------------------------------------------------------------------
// Unit tests
//
// We intentionally test `computeLossAndGrads` directly with a hand-built
// synthetic "final hidden" tensor. That avoids standing up a mock
// ComputeBackend (which would require implementing the full vtable) while
// still exercising the parts of this file that matter: the pooling
// Jacobian, the head gradient math, the numerical-stability of the BCE
// helper, and the wiring of NEFTune into the embedding buffer.
// ---------------------------------------------------------------------------

test "computeLossAndGrads: finite loss and non-zero head grads on a random batch" {
    const allocator = std.testing.allocator;
    const batch: usize = 3;
    const seq_len: usize = 5;
    const hidden: usize = 4;

    // Deterministic pseudo-random hidden state.
    const final_hidden = try allocator.alloc(f32, batch * seq_len * hidden);
    defer allocator.free(final_hidden);
    for (final_hidden, 0..) |*v, i| {
        const f: f32 = @floatFromInt(i);
        v.* = @sin(f * 0.37) * 0.5;
    }

    const mask = try allocator.alloc(f32, batch * seq_len);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1.0;
    // Pad out the tail of example 2 to exercise the mean-pool divisor.
    mask[2 * seq_len + 3] = 0.0;
    mask[2 * seq_len + 4] = 0.0;

    const labels = [_]f32{ 1.0, 0.0, 1.0 };

    const head_w = try allocator.alloc(f32, hidden);
    defer allocator.free(head_w);
    head_w[0] = 0.1;
    head_w[1] = -0.2;
    head_w[2] = 0.3;
    head_w[3] = -0.05;

    const grad_head_w = try allocator.alloc(f32, hidden);
    defer allocator.free(grad_head_w);
    @memset(grad_head_w, 0);

    var grad_head_b: f32 = 0;
    const d_hidden = try allocator.alloc(f32, batch * seq_len * hidden);
    defer allocator.free(d_hidden);

    const out = try computeLossAndGrads(
        allocator,
        final_hidden,
        mask,
        &labels,
        head_w,
        0.0,
        grad_head_w,
        &grad_head_b,
        d_hidden,
        batch,
        seq_len,
        hidden,
    );

    try std.testing.expect(std.math.isFinite(out.loss));
    try std.testing.expect(out.loss > 0);

    // At least one head weight grad and the bias grad should be non-zero.
    var any_w_nz = false;
    for (grad_head_w) |g| {
        if (g != 0) {
            any_w_nz = true;
            break;
        }
    }
    try std.testing.expect(any_w_nz);
    try std.testing.expect(grad_head_b != 0);

    // d_hidden on padded positions of example 2 must be exactly zero.
    for (3..seq_len) |t| {
        const base = (2 * seq_len + t) * hidden;
        for (0..hidden) |h| try std.testing.expectEqual(@as(f32, 0), d_hidden[base + h]);
    }
}

test "NEFTune wiring: alpha=0 is a no-op vs. no-NEFTune" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 8;
    const hidden: usize = 6;
    const n = num_tokens * hidden;

    const original = try allocator.alloc(f32, n);
    defer allocator.free(original);
    for (original, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) * 0.01;

    const a = try allocator.dupe(f32, original);
    defer allocator.free(a);
    // alpha = 0 path — exactly what `trainStep` does when config.neftune_alpha == 0.
    // We don't call neftune.applyInPlace at all; just confirm the buffer is untouched.
    try std.testing.expectEqualSlices(f32, original, a);

    // For completeness: calling neftune with alpha=0 explicitly is also a no-op.
    const mask = try allocator.alloc(f32, num_tokens);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1.0;
    neftune.applyInPlace(a, mask, num_tokens, hidden, 0.0, 42);
    try std.testing.expectEqualSlices(f32, original, a);
}

test "NEFTune wiring: alpha > 0 changes loss; same step is deterministic" {
    const allocator = std.testing.allocator;
    const batch: usize = 2;
    const seq_len: usize = 4;
    const hidden: usize = 3;

    // Start from a synthetic "embedding" buffer and pretend it's also the
    // final hidden state (this test checks that NEFTune on the embedding
    // propagates to the loss, which is what the real trainStep does).
    const base = try allocator.alloc(f32, batch * seq_len * hidden);
    defer allocator.free(base);
    for (base, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.21);

    const mask = try allocator.alloc(f32, batch * seq_len);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1.0;

    const labels = [_]f32{ 1.0, 0.0 };
    const head_w = [_]f32{ 0.2, -0.1, 0.05 };

    const runOnce = struct {
        fn go(
            allo: std.mem.Allocator,
            buf: []const f32,
            m: []const f32,
            labs: []const f32,
            hw: []const f32,
            b: usize,
            s: usize,
            h: usize,
        ) !f32 {
            const gw = try allo.alloc(f32, h);
            defer allo.free(gw);
            @memset(gw, 0);
            var gb: f32 = 0;
            const dh = try allo.alloc(f32, b * s * h);
            defer allo.free(dh);
            const r = try computeLossAndGrads(allo, buf, m, labs, hw, 0.0, gw, &gb, dh, b, s, h);
            return r.loss;
        }
    }.go;

    const loss_no_nef = try runOnce(allocator, base, mask, &labels, &head_w, batch, seq_len, hidden);

    const alpha_zero = try allocator.dupe(f32, base);
    defer allocator.free(alpha_zero);
    neftune.applyInPlace(alpha_zero, mask, batch * seq_len, hidden, 0.0, 7);
    const loss_alpha_zero = try runOnce(allocator, alpha_zero, mask, &labels, &head_w, batch, seq_len, hidden);
    try std.testing.expectEqual(loss_no_nef, loss_alpha_zero);

    const alpha_high_a = try allocator.dupe(f32, base);
    defer allocator.free(alpha_high_a);
    neftune.applyInPlace(alpha_high_a, mask, batch * seq_len, hidden, 5.0, 7);
    const loss_alpha_a = try runOnce(allocator, alpha_high_a, mask, &labels, &head_w, batch, seq_len, hidden);

    const alpha_high_b = try allocator.dupe(f32, base);
    defer allocator.free(alpha_high_b);
    neftune.applyInPlace(alpha_high_b, mask, batch * seq_len, hidden, 5.0, 7);
    const loss_alpha_b = try runOnce(allocator, alpha_high_b, mask, &labels, &head_w, batch, seq_len, hidden);

    // Same step -> deterministic.
    try std.testing.expectEqual(loss_alpha_a, loss_alpha_b);
    // Non-zero alpha must have perturbed the loss.
    try std.testing.expect(loss_alpha_a != loss_no_nef);
}

test "accumulateLoRAGradsForLayer: layout transpose round-trip is sane" {
    // Build a minimal LoRAAdapterSet (1 layer, 1 module "query_proj") and
    // accumulate gradients on a trivial hidden-state batch. We only assert
    // that grads become non-zero — the numerical correctness of
    // lora.accumulateLinearLoRAGrads is already covered by `lora.zig`'s tests.
    const allocator = std.testing.allocator;

    var adapters = try LoRAAdapterSet.init(
        allocator,
        .{
            .rank = 2,
            .alpha = 4.0,
            .target_modules = &.{"query_proj"},
            .num_layers = 1,
        },
        4, // hidden_size
        8, // intermediate_size (unused for query_proj)
    );
    defer adapters.deinit();

    const layer = adapters.get(0, "query_proj") orelse return error.MissingLayer;
    try std.testing.expectEqual(@as(usize, 4), layer.in_features);
    try std.testing.expectEqual(@as(usize, 4), layer.out_features);

    // Give B a tiny non-zero so accumulateLinearLoRAGrads' back_rank term
    // doesn't collapse to zero.
    for (layer.B, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i + 1)) * 0.01;

    const rows: usize = 3;
    const inputs = try allocator.alloc(f32, rows * 4);
    defer allocator.free(inputs);
    for (inputs, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.3) + 0.1;

    const output_grads = try allocator.alloc(f32, rows * 4);
    defer allocator.free(output_grads);
    for (output_grads, 0..) |*v, i| v.* = @cos(@as(f32, @floatFromInt(i)) * 0.4) * 0.2;

    try accumulateLoRAGradsForLayer(allocator, layer, &adapters, inputs, output_grads, rows);

    var any_a = false;
    for (layer.grad_A) |g| if (g != 0) {
        any_a = true;
        break;
    };
    var any_b = false;
    for (layer.grad_B) |g| if (g != 0) {
        any_b = true;
        break;
    };
    try std.testing.expect(any_a);
    try std.testing.expect(any_b);
}
