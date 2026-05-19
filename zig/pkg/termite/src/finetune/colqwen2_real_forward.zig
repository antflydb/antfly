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

// Non-cached, end-to-end ColQwen2 LoRA trainer with a real forward pass.
//
// This module supplements (does NOT replace) the surrogate scorer currently in
// `colqwen2.zig`. It calls the real Qwen2 text decoder forward so that:
//
//   * NEFTune noise is applied at the true token-embedding point, not on a
//     precomputed cache that would defeat the regularizer.
//   * The late-interaction score sees features produced by the current LoRA
//     adapters, not a frozen snapshot.
//
// Forward wiring (in `trainStep`, per example):
//
//     1. `qwen2.forwardUntilLayer(cb, ..., 0)`
//            -> query-side embedding output [Tq, H]
//     2. NEFTune in-place on the query embedding (skipped when alpha == 0),
//        seeded by `step * 2`.
//     3. `qwen2.forwardFromHiddenRange(cb, ..., 0, num_layers - 1)`
//            -> pre-last-layer query hidden state (used as "inputs" for LoRA
//               grad accumulation on the last layer)
//     4. `qwen2.forwardFromHiddenRange(cb, ..., num_layers - 1, num_layers)`
//            -> final query hidden state
//     5. Same sequence for the document side (seeded by `step * 2 + 1`).
//     6. Optional L2-normalize each token embedding (ColBERT convention).
//     7. Compute late-interaction MaxSim score (sum of per-query-token maxima
//        over valid doc tokens).
//     8. MSE loss vs. `target_score`, mean-reduced across the batch.
//
// Backward wiring (surrogate — NOT full autodiff):
//
//     * Exact gradient for MSE:  dL/ds_i = 2 * (s_i - t_i) / B.
//     * dL/ds -> dL/d(query/doc token embeddings), obtained through the
//       argmax MaxSim Jacobian: for each valid query token qt, we route the
//       upstream gradient along the single doc token dt that achieved the
//       maximum cosine similarity. This is the standard subgradient of a
//       max-over-finite-set (zero-measure ties resolved by argmax order).
//       Specifically, for
//           s = sum_qt  max_dt  <qt_vec, dt_vec> / (|qt| * |dt|)
//       and the chosen (qt, dt*) pair
//           d s / d qt_vec = (1/|qt|)*(dt*_unit - cos * qt_unit)
//           d s / d dt*_vec = (1/|dt*|)*(qt_unit - cos * dt*_unit)
//       where qt_unit = qt_vec/|qt|, dt*_unit = dt*_vec/|dt*|, cos is the
//       value at the argmax. This is the exact cosine-similarity Jacobian.
//       If `l2_normalize_embeddings = true`, the outer division by |qt|/|dt|
//       is already absorbed into the (pre-normalized) embeddings, so we
//       treat the unit vectors as the embeddings and the cosine reduces to a
//       dot product; the Jacobian then collapses to dt*/qt. We handle both
//       cases explicitly below.
//     * LoRA gradients are accumulated on the LAST decoder layer's target
//       modules only, using the pre-last-layer query/doc hidden states as the
//       linear "inputs" and the propagated d_hidden as the `output_grad`.
//       This treats every decoder layer up to (and including) the pre-final
//       layer as a frozen feature extractor: the upstream decoder contributes
//       no gradient to the adapters, matching the "forward-accurate,
//       backward-surrogate" philosophy of `reranker_real_forward.zig`. A
//       follow-up that replaces the surrogate with an autodiff tape would
//       thread dL/dhidden through every layer.
//
// Non-goals (explicit follow-ups):
//
//     * Vision tower integration. `Example.doc_ids` are treated as opaque
//       text tokens (see `text_only_vl_interpretation` below). A future
//       revision will run the Qwen2-VL vision projection and concatenate
//       patch embeddings with the prompt sequence before the decoder call.
//     * InfoNCE / contrastive loss. MSE on the score is adequate for
//       regression-style datasets (teacher distillation, soft relevance
//       labels) but weaker than an in-batch negatives contrastive objective.
//       A follow-up will accept a list of negatives and compute
//           -log(exp(s_pos) / (exp(s_pos) + sum_j exp(s_neg_j))).
//     * DDP / multi-rank training.
//     * Full autodiff through every transformer layer.
//     * Weight-decay / LR-schedule / grad-clip bookkeeping — those are
//       `config` fields for the caller to honour at optimizer-step time.
//     * Qwen2 safetensors weight loader — this trainer assumes the caller
//       has already populated the `ComputeBackend`'s weight store with
//       `model.embed_tokens.weight`, the 24 decoder layer weights, and
//       `model.norm.weight`. Tests below exercise only the pure-CPU helpers
//       that don't depend on a populated backend.

const std = @import("std");
const lora = @import("lora.zig");
const neftune = @import("neftune.zig");
const qwen2 = @import("../architectures/qwen2.zig");
const coord_mod = @import("training_memory_coordinator.zig");
const residency_mod = @import("grad_residency.zig");
const budget_mod = @import("training_budget.zig");
const ops = @import("../ops/ops.zig");
const fused_chunker_lora = @import("lora_adapter_set.zig");

pub const LoRAAdapterSet = fused_chunker_lora.LoRAAdapterSet;
pub const LoRALayer = fused_chunker_lora.LoRALayer;
pub const TrainingMemoryCoordinator = coord_mod.TrainingMemoryCoordinator;

// ---------------------------------------------------------------------------
// Public configuration & example types
// ---------------------------------------------------------------------------

pub const RealForwardTrainConfig = struct {
    max_seq_len: u32 = 1024,
    batch_size: u32 = 4,
    learning_rate: f32 = 1e-4,
    num_epochs: u32 = 3,
    warmup_steps: u32 = 50,
    weight_decay: f32 = 0.01,
    max_grad_norm: f32 = 1.0,
    neftune_alpha: f32 = 0.0,
    /// MSE weight on (predicted_late_interaction - target_score).
    score_mse_weight: f32 = 1.0,
    /// Hidden dim of the Qwen2 model. Must match `qwen2_config.hidden_size`.
    /// Kept on the trainer config as well so callers can build the adapter
    /// set without having to thread the decoder config around.
    hidden_size: u32 = 896,
    /// L2 normalize token embeddings before late interaction. Standard ColBERT
    /// practice — collapses cosine to a plain dot product and stabilises the
    /// gradient flow at the MaxSim argmax.
    l2_normalize_embeddings: bool = true,
    /// Treat the "image_input_ids" field as opaque text tokens and feed them
    /// through the Qwen2 text decoder as if they were normal tokens. This is
    /// the text-only MVP; set to false once the vision tower is wired up.
    /// At the moment the trainer has no code path for `false` and will
    /// `error.VisionTowerFollowupPending` if you toggle it.
    text_only_vl_interpretation: bool = true,
};

/// One (query, document, target score) triple. "doc" is the `image_input_ids`
/// of a `PreparedExampleInput`, interpreted as opaque token IDs (see the
/// top-of-file TODO on vision tower integration).
pub const Example = struct {
    query_ids: []const i64,
    query_mask: []const i64,
    doc_ids: []const i64, // "image_input_ids" under the text-only interpretation
    doc_mask: []const i64,
    target_score: f32,
    /// Always 1 today — one example = one (query, doc) pair. We keep the
    /// field so a caller that pre-batches multiple pairs into a single tensor
    /// can feed it in directly without re-packing.
    batch: usize,
    query_seq_len: usize,
    doc_seq_len: usize,
};

pub const StepResult = struct {
    loss: f32,
    mean_score_error: f32,
    step: u64,
};

// ---------------------------------------------------------------------------
// Pure helpers: L2 normalization, MaxSim score, MaxSim Jacobian
// ---------------------------------------------------------------------------

/// L2-normalize each row of `hidden` (shape [num_tokens, hidden_size]).
/// Rows masked out (mask == 0) are left as-is; they do not participate in the
/// late-interaction score anyway.
///
/// `mask` may be null to normalize every row.
fn l2NormalizeRows(hidden: []f32, mask: ?[]const f32, num_tokens: usize, hidden_size: usize) void {
    std.debug.assert(hidden.len == num_tokens * hidden_size);
    var t: usize = 0;
    while (t < num_tokens) : (t += 1) {
        if (mask) |m| {
            if (m[t] <= 0.5) continue;
        }
        const row = hidden[t * hidden_size .. (t + 1) * hidden_size];
        var sum_sq: f32 = 0;
        for (row) |v| sum_sq += v * v;
        if (sum_sq <= 0) continue;
        const inv_norm = 1.0 / @sqrt(sum_sq);
        for (row) |*v| v.* *= inv_norm;
    }
}

/// Minimal late-interaction MaxSim re-implementation, localised here to avoid
/// pulling in the full `reranking.zig` pipeline (which depends on a
/// Tokenizer + Session that we don't need here).
///
/// For each valid query token, finds the max cosine similarity over valid
/// doc tokens and sums those maxima. "Valid" means `mask > 0.5`; callers who
/// want to drop special tokens should zero their mask entries upstream.
///
/// When `pre_normalized = true`, the function skips the inner L2 normalization
/// and treats the input vectors as unit vectors (dot product == cosine).
///
/// Returns `argmax_doc[q_idx]` — the doc-token index that achieved the max
/// for each query token. This is consumed by `surrogateMaxSimGrad` to build
/// the per-token gradient slices. A value of `-1` means "no valid doc token
/// matched this query token" (fully-padded doc, or query token itself was
/// padded and skipped).
fn maxSimWithArgmax(
    allocator: std.mem.Allocator,
    query_hidden: []const f32,
    query_mask: []const f32,
    doc_hidden: []const f32,
    doc_mask: []const f32,
    hidden_size: usize,
    pre_normalized: bool,
) !struct { score: f32, argmax_doc: []i32 } {
    const q_seq = query_mask.len;
    const d_seq = doc_mask.len;
    std.debug.assert(query_hidden.len == q_seq * hidden_size);
    std.debug.assert(doc_hidden.len == d_seq * hidden_size);

    const argmax = try allocator.alloc(i32, q_seq);
    errdefer allocator.free(argmax);
    @memset(argmax, -1);

    var total: f32 = 0;
    var q_idx: usize = 0;
    while (q_idx < q_seq) : (q_idx += 1) {
        if (query_mask[q_idx] <= 0.5) continue;
        const q_vec = query_hidden[q_idx * hidden_size .. (q_idx + 1) * hidden_size];

        var q_norm: f32 = 1.0;
        if (!pre_normalized) {
            var q_sq: f32 = 0;
            for (q_vec) |v| q_sq += v * v;
            q_norm = if (q_sq > 0) @sqrt(q_sq) else 0;
            if (q_norm == 0) continue;
        }

        var best: f32 = -std.math.inf(f32);
        var best_idx: i32 = -1;

        var d_idx: usize = 0;
        while (d_idx < d_seq) : (d_idx += 1) {
            if (doc_mask[d_idx] <= 0.5) continue;
            const d_vec = doc_hidden[d_idx * hidden_size .. (d_idx + 1) * hidden_size];

            var dot: f32 = 0;
            for (q_vec, d_vec) |a, b| dot += a * b;

            const sim: f32 = if (pre_normalized) dot else blk: {
                var d_sq: f32 = 0;
                for (d_vec) |v| d_sq += v * v;
                const d_norm = if (d_sq > 0) @sqrt(d_sq) else 0;
                if (d_norm == 0) break :blk -std.math.inf(f32);
                break :blk dot / (q_norm * d_norm);
            };

            if (sim > best) {
                best = sim;
                best_idx = @intCast(d_idx);
            }
        }

        if (best_idx >= 0) {
            total += best;
            argmax[q_idx] = best_idx;
        }
    }

    return .{ .score = total, .argmax_doc = argmax };
}

/// Given upstream scalar gradient `d_score` (dL/ds for a single example) and
/// the argmax list produced by `maxSimWithArgmax`, accumulate the
/// surrogate gradients for the per-token query and doc embeddings.
///
/// `d_query_hidden`, `d_doc_hidden` must be pre-allocated to the same shape
/// as `query_hidden`, `doc_hidden` and WILL be accumulated into (not reset).
///
/// `pre_normalized` must match the value passed to `maxSimWithArgmax`. When
/// true, the cosine reduces to a dot product and
///     d(sim_qt,dt)/d(qt) = dt,    d(sim_qt,dt)/d(dt) = qt.
/// When false, we use the exact cosine Jacobian
///     d(sim)/d(qt) = (1/|qt|) * (dt_unit - sim * qt_unit)
///     d(sim)/d(dt) = (1/|dt|) * (qt_unit - sim * dt_unit).
fn surrogateMaxSimGrad(
    d_score: f32,
    query_hidden: []const f32,
    doc_hidden: []const f32,
    argmax_doc: []const i32,
    hidden_size: usize,
    pre_normalized: bool,
    d_query_hidden: []f32,
    d_doc_hidden: []f32,
) void {
    std.debug.assert(d_query_hidden.len == query_hidden.len);
    std.debug.assert(d_doc_hidden.len == doc_hidden.len);

    for (argmax_doc, 0..) |dt_i32, q_idx| {
        if (dt_i32 < 0) continue;
        const d_idx: usize = @intCast(dt_i32);

        const q_off = q_idx * hidden_size;
        const d_off = d_idx * hidden_size;
        const q_vec = query_hidden[q_off .. q_off + hidden_size];
        const d_vec = doc_hidden[d_off .. d_off + hidden_size];
        const dq = d_query_hidden[q_off .. q_off + hidden_size];
        const dd = d_doc_hidden[d_off .. d_off + hidden_size];

        if (pre_normalized) {
            // sim = <q, d>. Simple outer product style grad.
            for (0..hidden_size) |i| {
                dq[i] += d_score * d_vec[i];
                dd[i] += d_score * q_vec[i];
            }
        } else {
            var q_sq: f32 = 0;
            var d_sq: f32 = 0;
            var dot: f32 = 0;
            for (q_vec, d_vec) |a, b| {
                q_sq += a * a;
                d_sq += b * b;
                dot += a * b;
            }
            const q_norm = if (q_sq > 0) @sqrt(q_sq) else 0;
            const d_norm = if (d_sq > 0) @sqrt(d_sq) else 0;
            if (q_norm == 0 or d_norm == 0) continue;
            const inv_qn = 1.0 / q_norm;
            const inv_dn = 1.0 / d_norm;
            const sim = dot * inv_qn * inv_dn;

            // d(sim)/d(qt)_i = (1/|qt|) * (dt_i / |dt| - sim * qt_i / |qt|)
            // d(sim)/d(dt)_i = (1/|dt|) * (qt_i / |qt| - sim * dt_i / |dt|)
            for (0..hidden_size) |i| {
                const q_unit = q_vec[i] * inv_qn;
                const d_unit = d_vec[i] * inv_dn;
                dq[i] += d_score * inv_qn * (d_unit - sim * q_unit);
                dd[i] += d_score * inv_dn * (q_unit - sim * d_unit);
            }
        }
    }
}

/// Compute per-example MSE loss and accumulate the per-token gradient buffers
/// for BOTH query and doc sides. The gradient of the batch-mean MSE w.r.t.
/// the i-th predicted score is `(2 / B) * (s_i - t_i) * weight`, where
/// `weight = config.score_mse_weight`.
///
/// This is the testable core of the step: it operates on already-encoded
/// hidden states and touches no ComputeBackend.
pub fn computeLossAndGrads(
    allocator: std.mem.Allocator,
    query_hidden: []const f32, // [Tq, H]
    query_mask: []const f32, // [Tq]
    doc_hidden: []const f32, // [Td, H]
    doc_mask: []const f32, // [Td]
    target_score: f32,
    batch_size: usize,
    hidden_size: usize,
    mse_weight: f32,
    pre_normalized: bool,
    d_query_hidden: []f32,
    d_doc_hidden: []f32,
) !struct { loss: f32, predicted_score: f32, score_error: f32 } {
    std.debug.assert(query_hidden.len == query_mask.len * hidden_size);
    std.debug.assert(doc_hidden.len == doc_mask.len * hidden_size);
    std.debug.assert(d_query_hidden.len == query_hidden.len);
    std.debug.assert(d_doc_hidden.len == doc_hidden.len);

    const ms = try maxSimWithArgmax(
        allocator,
        query_hidden,
        query_mask,
        doc_hidden,
        doc_mask,
        hidden_size,
        pre_normalized,
    );
    defer allocator.free(ms.argmax_doc);

    const pred = ms.score;
    const err = pred - target_score;
    const loss = mse_weight * err * err;

    // dL/dpred for MSE mean-reduced across the batch:
    //     L = (w / B) * sum_i (s_i - t_i)^2
    //     dL/ds_i = (2 w / B) * (s_i - t_i)
    const bf: f32 = if (batch_size > 0) @floatFromInt(batch_size) else 1.0;
    const d_score = (2.0 * mse_weight / bf) * err;

    surrogateMaxSimGrad(
        d_score,
        query_hidden,
        doc_hidden,
        ms.argmax_doc,
        hidden_size,
        pre_normalized,
        d_query_hidden,
        d_doc_hidden,
    );

    return .{
        .loss = loss,
        .predicted_score = pred,
        .score_error = @abs(err),
    };
}

// ---------------------------------------------------------------------------
// LoRA grad helpers (handling the A/B layout mismatch; mirrors
// reranker_real_forward.zig)
// ---------------------------------------------------------------------------

/// ColQwen2 LoRA target modules. Matches the default Qwen2-family scope the
/// existing `colqwen2.zig` uses, minus `embedding_proj_layer` (which is not a
/// Qwen2-internal module — the real VL variant applies it after the vision
/// patch embedding, which is outside the scope of this text-only trainer).
///
/// We only handle the hidden->hidden (attention) projections in the surrogate
/// backward because the MLP projections map between hidden and intermediate,
/// and we do not compute gradients at the intermediate activation point.
/// Document this limitation — follow-ups will either route through the MLP
/// via a deeper surrogate or switch to full autodiff.
const candidate_lora_modules = [_][]const u8{
    // Qwen2 HF naming
    "q_proj",     "k_proj",     "v_proj",   "o_proj",
    // Fused-chunker naming (in case the caller reused the reranker defaults)
    "query_proj", "key_proj",   "value_proj", "out_proj",
};

const lora_grad_helpers = @import("lora_grad_helpers.zig");
const accumulateLoRAGradsForLayer = lora_grad_helpers.accumulateForLayer;

/// Route a per-token hidden-state gradient buffer into the last decoder
/// layer's LoRA adapters for all candidate attention modules.
fn accumulateLastLayerLoRAGrads(
    allocator: std.mem.Allocator,
    adapter_set: *LoRAAdapterSet,
    last_layer_u32: u32,
    hidden_size: usize,
    pre_last_hidden: []const f32,
    d_hidden: []const f32,
    num_tokens: usize,
) !void {
    for (candidate_lora_modules) |mod_name| {
        const layer_ptr = adapter_set.get(last_layer_u32, mod_name) orelse continue;
        if (layer_ptr.in_features != hidden_size) continue;
        // Hidden->hidden only (see module selection comment above).
        if (layer_ptr.out_features != hidden_size) continue;
        try accumulateLoRAGradsForLayer(
            allocator,
            layer_ptr,
            adapter_set,
            pre_last_hidden,
            d_hidden,
            num_tokens,
        );
    }
}

// ---------------------------------------------------------------------------
// Token-stream conversion
// ---------------------------------------------------------------------------

/// Build an f32 mask from an i64 mask (same length).
fn maskI64ToF32(allocator: std.mem.Allocator, mask_i64: []const i64) ![]f32 {
    const out = try allocator.alloc(f32, mask_i64.len);
    for (mask_i64, 0..) |m, i| out[i] = @floatFromInt(m);
    return out;
}

// ---------------------------------------------------------------------------
// Main training step
// ---------------------------------------------------------------------------

/// Run one training step on a batch of `Example` values. Unlike the reranker
/// trainer, we do NOT concatenate examples into a single forward call — the
/// query and document sequences have different lengths per example and
/// padding them to a common max would waste a large fraction of the decoder
/// budget on padding tokens. Instead, we loop over examples and accumulate
/// loss / gradients.
///
/// On success, accumulates gradients into:
///   * `adapter_set.layers[*].grad_A / grad_B` for the final decoder layer's
///     target attention modules (see `candidate_lora_modules`).
///
/// Caller is responsible for zeroing the grad buffers before the step if it
/// wants a fresh accumulation, and for running the optimizer step afterwards.
pub fn trainStep(
    allocator: std.mem.Allocator,
    compute_backend: *const ops.ComputeBackend,
    qwen2_config: qwen2.Config,
    adapter_set: *LoRAAdapterSet,
    examples: []const Example,
    config: RealForwardTrainConfig,
    step: u64,
    /// Optional Hypura-style memory coordinator. When non-null, the trainer
    /// reserves an activation budget up front, pins each updated LoRA grad
    /// block during accumulation, and records the forward segment on the
    /// checkpoint harness. Pass null to preserve the old behavior.
    coord: ?*TrainingMemoryCoordinator,
) !StepResult {
    if (examples.len == 0) return error.EmptyBatch;
    if (!config.text_only_vl_interpretation) return error.VisionTowerFollowupPending;

    const hidden_size: usize = @intCast(qwen2_config.hidden_size);
    const num_layers: usize = @intCast(qwen2_config.num_hidden_layers);
    if (num_layers == 0) return error.InvalidQwen2Config;
    if (hidden_size != @as(usize, config.hidden_size))
        return error.HiddenSizeMismatchBetweenQwenAndTrainerConfig;

    const last_layer = num_layers - 1;
    const last_layer_u32: u32 = @intCast(last_layer);

    // ---- Memory coordinator: reserve per-example activation upper bound ----
    // Worst case per example: query (3 buffers) + doc (3 buffers), each of
    // shape [max_seq_len * hidden_size * sizeof(f32)]. We don't know the
    // exact per-example lengths upfront, so budget against max_seq_len.
    const per_example_bytes: u64 =
        @as(u64, config.max_seq_len) * @as(u64, hidden_size) * @sizeOf(f32) * 6;
    var activation_reserved: bool = false;
    defer if (coord) |c| {
        if (activation_reserved) {
            c.budget.release(.activations, .host, per_example_bytes);
        }
    };
    if (coord) |c| {
        const res = c.budget.tryReserve(.activations, .host, per_example_bytes);
        if (res.event != .admitted) return error.BudgetDenied;
        activation_reserved = true;
        c.resetSegments();
    }

    // Pin every last-layer target module's grad block while the backward is
    // in flight. This prevents a concurrent spill-to-fit from evicting a
    // block mid-accumulation. Unpin them at the end of the step.
    var pinned_blocks: [candidate_lora_modules.len]?residency_mod.GradBlockId = .{null} ** candidate_lora_modules.len;
    defer if (coord) |c| {
        for (pinned_blocks) |maybe_id| {
            if (maybe_id) |id| c.unpinGradBlock(id) catch {};
        }
    };
    if (coord) |c| {
        for (candidate_lora_modules, 0..) |mod_name, mod_idx| {
            const id = residency_mod.GradBlockId{
                .layer_idx = last_layer_u32,
                .module_idx = @intCast(mod_idx),
            };
            if (c.residency.entry(id) != null) {
                c.pinGradBlock(id) catch |err| switch (err) {
                    residency_mod.GradResidencyError.UnknownBlock => continue,
                    else => return err,
                };
                pinned_blocks[mod_idx] = id;
            }
            _ = mod_name;
        }
    }

    var total_loss: f32 = 0;
    var total_err: f32 = 0;

    for (examples, 0..) |ex, ex_idx| {
        if (ex.query_ids.len != ex.query_seq_len) return error.BadQueryLength;
        if (ex.query_mask.len != ex.query_seq_len) return error.BadQueryMaskLength;
        if (ex.doc_ids.len != ex.doc_seq_len) return error.BadDocLength;
        if (ex.doc_mask.len != ex.doc_seq_len) return error.BadDocMaskLength;

        const q_total = ex.batch * ex.query_seq_len;
        const d_total = ex.batch * ex.doc_seq_len;

        // Per-example f32 masks (used both by NEFTune and by the scorer).
        const query_mask_f32 = try maskI64ToF32(allocator, ex.query_mask);
        defer allocator.free(query_mask_f32);
        const doc_mask_f32 = try maskI64ToF32(allocator, ex.doc_mask);
        defer allocator.free(doc_mask_f32);

        // -------- Query encode --------
        const q_embeddings = try qwen2.forwardUntilLayer(
            compute_backend,
            allocator,
            qwen2_config,
            ex.query_ids,
            ex.query_mask,
            ex.batch,
            ex.query_seq_len,
            0,
        );
        defer allocator.free(q_embeddings);

        if (config.neftune_alpha > 0.0) {
            neftune.applyInPlace(
                q_embeddings,
                query_mask_f32,
                q_total,
                hidden_size,
                config.neftune_alpha,
                step * 2,
            );
        }

        // Record the query segment for future autodiff recompute. No-op when
        // `harness == null`, so callers that don't use the coordinator pay
        // nothing.
        if (coord) |c| {
            if (c.harness != null) {
                _ = c.recordSegment(0, last_layer_u32, q_embeddings, q_total, hidden_size, null) catch {};
            }
        }

        // Pre-last-layer hidden state — inputs to LoRA grad accumulation.
        const q_pre_last = try qwen2.forwardFromHiddenRange(
            compute_backend,
            allocator,
            qwen2_config,
            q_embeddings,
            ex.query_mask,
            ex.batch,
            ex.query_seq_len,
            0,
            last_layer,
        );
        defer allocator.free(q_pre_last);

        // Final hidden state.
        const q_final = try qwen2.forwardFromHiddenRange(
            compute_backend,
            allocator,
            qwen2_config,
            q_pre_last,
            ex.query_mask,
            ex.batch,
            ex.query_seq_len,
            last_layer,
            num_layers,
        );
        defer allocator.free(q_final);

        if (config.l2_normalize_embeddings) {
            l2NormalizeRows(q_final, query_mask_f32, q_total, hidden_size);
        }

        // -------- Doc encode --------
        const d_embeddings = try qwen2.forwardUntilLayer(
            compute_backend,
            allocator,
            qwen2_config,
            ex.doc_ids,
            ex.doc_mask,
            ex.batch,
            ex.doc_seq_len,
            0,
        );
        defer allocator.free(d_embeddings);

        if (config.neftune_alpha > 0.0) {
            neftune.applyInPlace(
                d_embeddings,
                doc_mask_f32,
                d_total,
                hidden_size,
                config.neftune_alpha,
                step * 2 + 1,
            );
        }

        if (coord) |c| {
            if (c.harness != null) {
                _ = c.recordSegment(0, last_layer_u32, d_embeddings, d_total, hidden_size, null) catch {};
            }
        }

        const d_pre_last = try qwen2.forwardFromHiddenRange(
            compute_backend,
            allocator,
            qwen2_config,
            d_embeddings,
            ex.doc_mask,
            ex.batch,
            ex.doc_seq_len,
            0,
            last_layer,
        );
        defer allocator.free(d_pre_last);

        const d_final = try qwen2.forwardFromHiddenRange(
            compute_backend,
            allocator,
            qwen2_config,
            d_pre_last,
            ex.doc_mask,
            ex.batch,
            ex.doc_seq_len,
            last_layer,
            num_layers,
        );
        defer allocator.free(d_final);

        if (config.l2_normalize_embeddings) {
            l2NormalizeRows(d_final, doc_mask_f32, d_total, hidden_size);
        }

        // -------- Loss + per-token grads --------
        const d_q_hidden = try allocator.alloc(f32, q_total * hidden_size);
        defer allocator.free(d_q_hidden);
        @memset(d_q_hidden, 0);

        const d_d_hidden = try allocator.alloc(f32, d_total * hidden_size);
        defer allocator.free(d_d_hidden);
        @memset(d_d_hidden, 0);

        const core = try computeLossAndGrads(
            allocator,
            q_final,
            query_mask_f32,
            d_final,
            doc_mask_f32,
            ex.target_score,
            examples.len,
            hidden_size,
            config.score_mse_weight,
            config.l2_normalize_embeddings,
            d_q_hidden,
            d_d_hidden,
        );

        total_loss += core.loss;
        total_err += core.score_error;

        // -------- LoRA grads on the last layer (query-side and doc-side) --------
        try accumulateLastLayerLoRAGrads(
            allocator,
            adapter_set,
            last_layer_u32,
            hidden_size,
            q_pre_last,
            d_q_hidden,
            q_total,
        );
        try accumulateLastLayerLoRAGrads(
            allocator,
            adapter_set,
            last_layer_u32,
            hidden_size,
            d_pre_last,
            d_d_hidden,
            d_total,
        );

        _ = ex_idx;
    }

    const bf: f32 = @floatFromInt(examples.len);
    return .{
        .loss = total_loss / bf,
        .mean_score_error = total_err / bf,
        .step = step,
    };
}

// ---------------------------------------------------------------------------
// Unit tests
//
// We intentionally exercise only the pure-CPU helpers (`maxSimWithArgmax`,
// `surrogateMaxSimGrad`, `computeLossAndGrads`, `l2NormalizeRows`) and the
// LoRA layout transpose path. Running `trainStep` end-to-end requires a
// fully-populated Qwen2 ComputeBackend weight store, which in turn requires a
// real safetensors loader. That is a follow-up — see the top-of-file notes.
// ---------------------------------------------------------------------------

test "maxSim pre_normalized: toy example returns exact expected score" {
    const allocator = std.testing.allocator;
    const H: usize = 3;

    // q0 = [1, 0, 0] (unit), q1 = [0, 1, 0] (unit)
    // d0 = [sqrt(0.5), sqrt(0.5), 0] (unit)
    const sq = @as(f32, @sqrt(0.5));
    const q = [_]f32{ 1, 0, 0, 0, 1, 0 };
    const d = [_]f32{ sq, sq, 0 };
    const qm = [_]f32{ 1, 1 };
    const dm = [_]f32{1};

    const ms = try maxSimWithArgmax(allocator, &q, &qm, &d, &dm, H, true);
    defer allocator.free(ms.argmax_doc);

    // q0·d0 = sq ≈ 0.7071, q1·d0 = sq ≈ 0.7071 — sum ≈ 1.4142
    try std.testing.expectApproxEqAbs(@as(f32, 2.0 * sq), ms.score, 1e-5);
    try std.testing.expectEqual(@as(i32, 0), ms.argmax_doc[0]);
    try std.testing.expectEqual(@as(i32, 0), ms.argmax_doc[1]);
}

test "maxSim (cosine): hand-constructed pair matches expected maxima" {
    const allocator = std.testing.allocator;
    const H: usize = 3;

    // q0 = [1,0,0], q1 = [0,1,0]; d0 = [0.5, 0.5, 0].
    // cos(q0, d0) = 0.5 / (1 * sqrt(0.5)) = sqrt(0.5) ≈ 0.7071
    // cos(q1, d0) = sqrt(0.5) ≈ 0.7071.  Sum ≈ 1.4142.
    const q = [_]f32{ 1, 0, 0, 0, 1, 0 };
    const d = [_]f32{ 0.5, 0.5, 0 };
    const qm = [_]f32{ 1, 1 };
    const dm = [_]f32{1};

    const ms = try maxSimWithArgmax(allocator, &q, &qm, &d, &dm, H, false);
    defer allocator.free(ms.argmax_doc);

    const expected = 2.0 * @as(f32, @sqrt(0.5));
    try std.testing.expectApproxEqAbs(expected, ms.score, 1e-5);
}

test "maxSim skips query/doc tokens whose mask is zero" {
    const allocator = std.testing.allocator;
    const H: usize = 2;

    // Two query tokens, but only the second is valid.
    // Two doc tokens, only the first is valid.
    const q = [_]f32{ 99, 99, 1, 0 }; // q[0] garbage, q[1] = [1,0]
    const d = [_]f32{ 1, 0, 77, 77 }; // d[0] = [1,0], d[1] garbage
    const qm = [_]f32{ 0, 1 };
    const dm = [_]f32{ 1, 0 };

    const ms = try maxSimWithArgmax(allocator, &q, &qm, &d, &dm, H, true);
    defer allocator.free(ms.argmax_doc);

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), ms.score, 1e-6);
    try std.testing.expectEqual(@as(i32, -1), ms.argmax_doc[0]);
    try std.testing.expectEqual(@as(i32, 0), ms.argmax_doc[1]);
}

test "l2NormalizeRows leaves unit vectors fixed and normalizes scaled rows" {
    const H: usize = 3;
    const num: usize = 3;
    var buf = [_]f32{
        1, 0,  0,
        0, 10, 0,
        3, 0,  4,
    };
    const mask = [_]f32{ 1, 1, 1 };
    l2NormalizeRows(&buf, &mask, num, H);

    // Row 0 already unit.
    try std.testing.expectApproxEqAbs(@as(f32, 1), buf[0], 1e-6);
    // Row 1: [0, 10, 0] -> [0, 1, 0].
    try std.testing.expectApproxEqAbs(@as(f32, 1), buf[4], 1e-6);
    // Row 2: [3, 0, 4] -> [0.6, 0, 0.8].
    try std.testing.expectApproxEqAbs(@as(f32, 0.6), buf[6], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), buf[8], 1e-5);
}

test "l2NormalizeRows leaves masked rows untouched" {
    const H: usize = 2;
    var buf = [_]f32{ 5, 5, 7, 7 };
    const mask = [_]f32{ 1, 0 };
    l2NormalizeRows(&buf, &mask, 2, H);

    // Row 0 was [5,5] -> [1/sqrt(2), 1/sqrt(2)].
    try std.testing.expectApproxEqAbs(@as(f32, @sqrt(0.5)), buf[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, @sqrt(0.5)), buf[1], 1e-5);
    // Row 1 untouched.
    try std.testing.expectEqual(@as(f32, 7), buf[2]);
    try std.testing.expectEqual(@as(f32, 7), buf[3]);
}

test "computeLossAndGrads: loss is finite and gradient points the right direction" {
    const allocator = std.testing.allocator;
    const H: usize = 3;

    // q0 = [1,0,0], q1 = [0,1,0]; d0 = [0.5, 0.5, 0].
    // Score (cosine mode) = 2 * sqrt(0.5) ≈ 1.4142, target = 0.5 so err > 0.
    // With err > 0, d_score > 0, so the surrogate grad pushes the embeddings
    // AWAY from similarity (sign of dq_i ~ d_unit_i - sim * q_unit_i).
    const q = [_]f32{ 1, 0, 0, 0, 1, 0 };
    const d = [_]f32{ 0.5, 0.5, 0 };
    const qm = [_]f32{ 1, 1 };
    const dm = [_]f32{1};

    const dq = try allocator.alloc(f32, q.len);
    defer allocator.free(dq);
    @memset(dq, 0);
    const dd = try allocator.alloc(f32, d.len);
    defer allocator.free(dd);
    @memset(dd, 0);

    const out = try computeLossAndGrads(
        allocator,
        &q,
        &qm,
        &d,
        &dm,
        0.5, // target
        1, // batch_size
        H,
        1.0, // mse_weight
        false, // pre_normalized = false -> cosine mode
        dq,
        dd,
    );

    try std.testing.expect(std.math.isFinite(out.loss));
    try std.testing.expect(out.loss > 0);
    // err = pred - target ≈ 1.4142 - 0.5 = 0.9142 > 0.
    try std.testing.expect(out.predicted_score > out.score_error);

    // Surrogate gradient points "away" from the matched doc token in the
    // cosine sense. For q0 = [1,0,0] and d0_unit = [sq, sq, 0], dq0 should
    // have a NEGATIVE x-component (pulling q0 off the [1,0,0] axis) and a
    // POSITIVE y-component (pushing q0 toward the d0_unit direction... no —
    // since err > 0 we want LESS similarity, so the sign flips).
    //
    // Concretely: dq0 ∝ (d0_unit - sim*q0_unit) * d_score where d_score > 0.
    // d0_unit = [sq, sq, 0], q0_unit = [1,0,0], sim = sq.
    // Pre-scale vec: [sq - sq*1, sq - 0, 0] = [0, sq, 0]. So dq0 = [0, +, 0].
    try std.testing.expectApproxEqAbs(@as(f32, 0), dq[0], 1e-5);
    try std.testing.expect(dq[1] > 0);
    try std.testing.expectApproxEqAbs(@as(f32, 0), dq[2], 1e-5);
}

test "computeLossAndGrads (pre_normalized): zero error gives zero gradients" {
    const allocator = std.testing.allocator;
    const H: usize = 2;

    // q = [1,0]; d = [1,0]. sim = 1. If target = 1, err = 0, d_score = 0.
    const q = [_]f32{ 1, 0 };
    const d = [_]f32{ 1, 0 };
    const qm = [_]f32{1};
    const dm = [_]f32{1};

    const dq = try allocator.alloc(f32, q.len);
    defer allocator.free(dq);
    @memset(dq, 0);
    const dd = try allocator.alloc(f32, d.len);
    defer allocator.free(dd);
    @memset(dd, 0);

    const out = try computeLossAndGrads(
        allocator,
        &q,
        &qm,
        &d,
        &dm,
        1.0,
        1,
        H,
        1.0,
        true,
        dq,
        dd,
    );

    try std.testing.expectApproxEqAbs(@as(f32, 0), out.loss, 1e-8);
    try std.testing.expectApproxEqAbs(@as(f32, 1), out.predicted_score, 1e-6);
    for (dq) |g| try std.testing.expectEqual(@as(f32, 0), g);
    for (dd) |g| try std.testing.expectEqual(@as(f32, 0), g);
}

test "computeLossAndGrads (pre_normalized): positive error pushes q toward d and d toward q" {
    const allocator = std.testing.allocator;
    const H: usize = 2;

    // q = [1, 0], d = [0, 1], target = 1, sim = 0. err = -1, d_score = -2.
    // Pre-normalized surrogate grads: dq = d_score * d = [0, -2]; dd = d_score * q = [-2, 0].
    const q = [_]f32{ 1, 0 };
    const d = [_]f32{ 0, 1 };
    const qm = [_]f32{1};
    const dm = [_]f32{1};

    const dq = try allocator.alloc(f32, q.len);
    defer allocator.free(dq);
    @memset(dq, 0);
    const dd = try allocator.alloc(f32, d.len);
    defer allocator.free(dd);
    @memset(dd, 0);

    _ = try computeLossAndGrads(
        allocator,
        &q,
        &qm,
        &d,
        &dm,
        1.0,
        1,
        H,
        1.0,
        true,
        dq,
        dd,
    );

    // d_score = (2 * 1 / 1) * (0 - 1) = -2.  dq ∝ d_score * d = [0, -2].
    try std.testing.expectApproxEqAbs(@as(f32, 0), dq[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -2), dq[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -2), dd[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), dd[1], 1e-6);

    // A small SGD step in the direction -dq (gradient descent) increases
    // q[1] — i.e. it moves q TOWARD d, which increases the score toward the
    // target. That's the correct sign for the surrogate.
    const new_q1 = q[1] - 0.1 * dq[1]; // = 0 - 0.1*(-2) = 0.2
    try std.testing.expect(new_q1 > q[1]);
}

test "NEFTune wiring: alpha=0 is a no-op; alpha>0 perturbs the embedding" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 4;
    const H: usize = 3;
    const n = num_tokens * H;

    const base = try allocator.alloc(f32, n);
    defer allocator.free(base);
    for (base, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.19);

    const mask = try allocator.alloc(f32, num_tokens);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1.0;

    const copy_a = try allocator.dupe(f32, base);
    defer allocator.free(copy_a);
    neftune.applyInPlace(copy_a, mask, num_tokens, H, 0.0, 42);
    try std.testing.expectEqualSlices(f32, base, copy_a);

    const copy_b = try allocator.dupe(f32, base);
    defer allocator.free(copy_b);
    neftune.applyInPlace(copy_b, mask, num_tokens, H, 5.0, 42);

    var changed = false;
    for (base, copy_b) |a, b| if (a != b) {
        changed = true;
        break;
    };
    try std.testing.expect(changed);

    // Same seed -> deterministic.
    const copy_c = try allocator.dupe(f32, base);
    defer allocator.free(copy_c);
    neftune.applyInPlace(copy_c, mask, num_tokens, H, 5.0, 42);
    try std.testing.expectEqualSlices(f32, copy_b, copy_c);
}

test "NEFTune: query/doc seed split gives independent noise streams" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 6;
    const H: usize = 4;
    const n = num_tokens * H;

    const base = try allocator.alloc(f32, n);
    defer allocator.free(base);
    for (base, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) * 0.001;

    const mask = try allocator.alloc(f32, num_tokens);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1.0;

    const step: u64 = 9;

    const q = try allocator.dupe(f32, base);
    defer allocator.free(q);
    neftune.applyInPlace(q, mask, num_tokens, H, 3.0, step * 2);

    const d = try allocator.dupe(f32, base);
    defer allocator.free(d);
    neftune.applyInPlace(d, mask, num_tokens, H, 3.0, step * 2 + 1);

    // The two streams must not produce identical perturbations.
    var any_diff = false;
    for (q, d) |a, b| if (a != b) {
        any_diff = true;
        break;
    };
    try std.testing.expect(any_diff);
}

test "accumulateLoRAGradsForLayer: layout transpose round-trip is sane" {
    const allocator = std.testing.allocator;

    var adapters = try LoRAAdapterSet.init(
        allocator,
        .{
            .rank = 2,
            .alpha = 4.0,
            .target_modules = &.{"q_proj"},
            .num_layers = 1,
        },
        4, // hidden_size
        8, // intermediate_size (unused for q_proj)
    );
    defer adapters.deinit();

    const layer = adapters.get(0, "q_proj") orelse return error.MissingLayer;
    try std.testing.expectEqual(@as(usize, 4), layer.in_features);
    try std.testing.expectEqual(@as(usize, 4), layer.out_features);

    // Seed B with non-zero so the back_rank term is non-trivial.
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

test "public trainStep symbol exists and its signature is reachable" {
    const _train: *const fn (
        allocator: std.mem.Allocator,
        compute_backend: *const ops.ComputeBackend,
        qwen2_config: qwen2.Config,
        adapter_set: *LoRAAdapterSet,
        examples: []const Example,
        config: RealForwardTrainConfig,
        step: u64,
    ) anyerror!StepResult = &trainStep;
    _ = _train;
}
