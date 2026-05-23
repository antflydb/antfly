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

//! Level-3 training path for GLiNER2 (real forward + real autodiff).
//!
//! This module plugs `architectures/deberta_graph.zig` into the generic
//! `real_autodiff_trainer.RealAutodiffTrainer` harness and adds a small
//! token-classification head on top of the encoder's final hidden state.
//!
//! GLiNER2 is a NER model built on DeBERTa-v3. The sibling module
//! `gliner2.zig` already offers a surrogate trainer that runs only on cached
//! encoder outputs. This file is the Level-3 counterpart: every micro-batch
//! flows through the full encoder graph and autodiff produces gradients for
//! all LoRA-injected parameters plus the task head (subject to the
//! placeholder-wiring limitations below).
//!
//! TASK SHAPE
//!
//!   * Inputs : `input_ids` [B, S] i64, `attention_mask` [B, S] f32
//!   * Head   : `classifier.weight` [C, H], `classifier.bias` [C]
//!   * Logits : `[B*S, C]`
//!   * Loss   : masked cross-entropy over one-hot `targets` of shape `[B*S, C]`
//!
//! Ignored tokens (HuggingFace's `ignore_index = -100` convention) are
//! handled at data-preparation time: the caller is expected to emit an
//! all-zero row in `targets` for those positions. The graph normalizes the
//! summed token loss by the summed target-row mass, so zero rows contribute
//! neither numerator nor denominator.
//!
//! ATTENTION BIAS
//!
//! `deberta_graph.buildForwardGraph` takes the attention bias as a caller-
//! provided NodeId of shape `[batch * num_heads, seq_len, seq_len]` f32.
//! The bias is built at runtime from the per-step `attention_mask` via
//! `BertPlaceholderPrep.buildAttnBias`, which produces `-1e9` at padded
//! positions and `0.0` at valid positions. The `__gliner2_attn_bias`
//! parameter placeholder is populated in `bindArchInputs` each step.
//!
//! KNOWN LIMITATIONS
//!
//!   * No disentangled relative-position attention — inherited from the
//!     MVP simplification in `deberta_graph.zig`.
//!   * `classifier.weight` and `classifier.bias` are regular trainable
//!     parameters rather than LoRA adapters. The trainer enrolls them through
//!     `regular_trainable_params`, writes optimizer updates back into
//!     trainer-owned slots, and the GLiNER2 training CLI exports them to
//!     `task_head.safetensors`.
//!   * The current full-autodiff MVP trains a token-classification objective.
//!     The span grid in `gliner2_data.zig` is production-shaped and now has a
//!     deterministic prediction decoder, but the real-model span/objective
//!     parity work is still tracked in `GLINER2_READINESS.md`.

const std = @import("std");
const ml = @import("ml");
const deberta_graph = @import("../architectures/deberta_graph.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const gliner2_data = @import("gliner2_data.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");

const Builder = ml.graph.Builder;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

// ── Public API ───────────────────────────────────────────────────────────────

pub const GlinerObjective = enum {
    /// Legacy full-autodiff fallback: token classification over text tokens.
    token,
    /// First graph-native span objective: gather the span start token hidden
    /// state and score entity labels directly over candidate spans.
    span_start,
};

pub const GlinerAutodiffConfig = struct {
    /// Underlying DeBERTa encoder config (hidden size, layers, heads, ...).
    graph_config: deberta_graph.Config,
    /// Number of entity classes, including the "O" (no-entity) class.
    num_classes: u32,
    /// Training objective. Defaults to the existing token-classifier path so
    /// current callers keep their behavior until they opt into span targets.
    objective: GlinerObjective = .token,
    /// HuggingFace-style ignore index. Documented only — see module header;
    /// the MVP relies on zero target rows rather than consuming this value
    /// directly inside the graph.
    ignore_index: i32 = -100,
};

/// Trainer-opaque context that owns the graph-construction state and (after
/// the first step) a handle to the built `DebertaGraph`. The trainer stores
/// a `*anyopaque` pointer to this struct in `TrainerInput.ctx`, and the
/// build-forward / build-loss callbacks cast it back.
///
/// The harness builds the graph exactly once, on the first call to
/// `trainer.step`; subsequent steps reuse the stored graph. That means
/// `built` is populated only after the first step. Construction-only tests
/// (no trainer attached) will observe `built == null`.
pub const GlinerAutodiffCtx = struct {
    config: GlinerAutodiffConfig,
    built: ?deberta_graph.DebertaGraph = null,
    token_logits: ?NodeId = null,
    span_logits: ?NodeId = null,

    pub fn init(config: GlinerAutodiffConfig) GlinerAutodiffCtx {
        return .{ .config = config };
    }

    // ── Trainer callbacks ────────────────────────────────────────────────

    /// Build the forward subgraph and return the encoder's final hidden
    /// state NodeId.
    ///
    /// The harness creates `input_ids` as an f32 placeholder (the trainer's
    /// runtime bind layer uploads a float view). `deberta_graph` consumes
    /// `input_ids` as i64 via `embeddingLookup`; we therefore reshape the
    /// harness-supplied node from `[B, S]` f32 to `[B*S]` first, and let the
    /// downstream embedding op do its own bit-cast / gather. Matches the
    /// pattern used in the `reranker_train.BertAutodiffCtx` callback.
    ///
    /// The `attention_mask` graph node argument is unused here — the
    /// actual per-step attention mask is applied at runtime through
    /// `bindArchInputs`, which populates `__gliner2_attn_bias` via
    /// `BertPlaceholderPrep.buildAttnBias`.
    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        _ = attention_mask;
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const cfg = self.config.graph_config;

        // Flatten harness input_ids [B, S] → [B*S] so the shape matches the
        // `deberta_graph` expectation (its embedding lookup emits one row
        // per flat token).
        const total: u32 = batch * seq_len;
        const flat_ids = try bld.reshape(
            input_ids,
            Shape.init(.f32, &.{@intCast(total)}),
        );

        // Attention bias as a runtime-bindable parameter so that
        // `bindArchInputs` can populate it from the per-step attention
        // mask via `BertPlaceholderPrep.buildAttnBias`. Shape is
        // `[batch * num_heads, seq_len, seq_len]` f32.
        const num_heads: u32 = cfg.num_attention_heads;
        const attn_bias = try bld.parameter(
            "__gliner2_attn_bias",
            Shape.init(.f32, &.{
                @intCast(batch * num_heads),
                @intCast(seq_len),
                @intCast(seq_len),
            }),
        );

        self.built = try deberta_graph.buildForwardGraph(
            bld,
            cfg,
            flat_ids,
            attn_bias,
            batch,
            seq_len,
        );
        return self.built.?.output_node;
    }

    /// Bind architecture-specific placeholders at runtime.
    ///
    /// Populates the `__gliner2_attn_bias` parameter placeholder with a
    /// properly masked attention bias derived from the per-step
    /// `attention_mask` via `BertPlaceholderPrep.buildAttnBias`. This
    /// produces `[batch * num_heads, seq_len, seq_len]` f32 with `-1e9`
    /// at padded positions and `0.0` at valid positions.
    pub fn bindArchInputs(
        ctx_opaque: *anyopaque,
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        rt_map: *std.AutoHashMapUnmanaged(NodeId, CT),
        batch: u32,
        seq_len: u32,
        attention_mask: []const f32,
    ) anyerror!void {
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const num_heads = self.config.graph_config.num_attention_heads;

        if (graph_weight_bridge.findParameterByName(graph, "__gliner2_attn_bias")) |node_id| {
            const bias = try graph_input_binder.BertPlaceholderPrep.buildAttnBias(
                allocator,
                attention_mask,
                batch,
                seq_len,
                num_heads,
            );
            defer allocator.free(bias);
            const dims = [_]i32{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len) };
            const ct = try cb.fromFloat32Shape(bias, &dims);
            try rt_map.put(allocator, node_id, ct);
        }
    }

    pub fn remapGraphNodes(ctx_opaque: *anyopaque, id_map: []const NodeId) anyerror!void {
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        if (self.built) |*built| {
            built.input_ids_node = id_map[built.input_ids_node];
            built.attn_bias_node = id_map[built.attn_bias_node];
            built.output_node = id_map[built.output_node];
        }
        if (self.token_logits) |node_id| self.token_logits = id_map[node_id];
        if (self.span_logits) |node_id| self.span_logits = id_map[node_id];
    }

    /// Task head + scalar loss. Token classification only — GLiNER2 does
    /// not have a sequence-level head worth wiring in the MVP.
    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        return switch (self.config.objective) {
            .token => self.buildTokenLoss(bld, forward_output, targets),
            .span_start => self.buildSpanStartLoss(bld, forward_output, targets),
        };
    }

    // ── Task-specific head ───────────────────────────────────────────────

    /// Token classification head over `[B*S, H]` encoder output.
    ///
    ///   logits[t, :] = encoder_out[t, :] @ W^T + b     # [B*S, C]
    ///   loss          = -sum_t,c targets[t,c] * log_softmax(logits)[t,c]
    ///                   / (sum_t,c targets[t,c] + eps)
    ///
    /// Targets are expected as `[B*S, C]` float tensor. Ignored tokens are
    /// represented by an all-zero row and are excluded from both numerator
    /// and denominator.
    fn buildTokenLoss(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const cfg = self.config.graph_config;
        const H: u32 = cfg.hidden_size;
        const C: u32 = self.config.num_classes;

        // `deberta_graph` already emits a flat `[B*S, H]` tensor, so we do
        // not need to reshape. If a future revision changes the encoder's
        // output rank, this reshape becomes a no-op-equivalent guard; for
        // now we read the rank at graph-construction time and re-flatten
        // defensively so the linear op sees the expected `[rows, H]`.
        const hidden_shape = bld.graph.node(hidden).output_shape;
        const rows: u32 = blk: {
            if (hidden_shape.rank() == 2) {
                break :blk @intCast(hidden_shape.dim(0));
            }
            // Rank-3 fallback: [B, S, H] → [B*S, H].
            const b: i64 = hidden_shape.dim(0);
            const s: i64 = hidden_shape.dim(1);
            break :blk @intCast(b * s);
        };

        const hidden_flat = if (hidden_shape.rank() == 2)
            hidden
        else
            try bld.reshape(
                hidden,
                Shape.init(.f32, &.{ @intCast(rows), @intCast(H) }),
            );

        // Classifier head parameters. Names use the HF token-classification
        // convention (`classifier.weight` / `classifier.bias`) so that a
        // future `injectLoRA` run targeting `classifier.weight` can adopt
        // them, and so `safetensors` save/load is straightforward.
        const head_w = try bld.parameter(
            "classifier.weight",
            Shape.init(.f32, &.{ @intCast(C), @intCast(H) }),
        );
        const head_b = try bld.parameter(
            "classifier.bias",
            Shape.init(.f32, &.{@intCast(C)}),
        );

        const logits = try bld.linear(hidden_flat, head_w, head_b, rows, H, C);
        self.token_logits = logits;

        return buildMaskedSoftTargetCrossEntropyLoss(bld, logits, targets);
    }

    /// Span-start objective over packed targets:
    ///
    ///   targets[:, 0:E]       = multi-label entity targets
    ///   targets[:, E:2E]      = repeated valid-span mask
    ///   targets[:, 2E:2E+1]   = flat token row index into `[B*S, H]`
    ///
    /// The classifier is shared with token mode. Row 0 is the token-mode `O`
    /// class, so span scoring slices logits to entity classes `1..C`.
    fn buildSpanStartLoss(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const C: u32 = self.config.num_classes;
        if (C < 2) return error.InvalidGlinerClassCount;
        const E: u32 = C - 1;
        const H: u32 = self.config.graph_config.hidden_size;

        const hidden_shape = bld.graph.node(hidden).output_shape;
        const hidden_rows: u32 = blk: {
            if (hidden_shape.rank() == 2) break :blk @intCast(hidden_shape.dim(0));
            const b: i64 = hidden_shape.dim(0);
            const s: i64 = hidden_shape.dim(1);
            break :blk @intCast(b * s);
        };
        const hidden_flat = if (hidden_shape.rank() == 2)
            hidden
        else
            try bld.reshape(
                hidden,
                Shape.init(.f32, &.{ @intCast(hidden_rows), @intCast(H) }),
            );

        const target_shape = bld.graph.node(targets).output_shape;
        if (target_shape.rank() != 2) return error.InvalidGlinerSpanTargetShape;
        const span_rows: u32 = @intCast(target_shape.dim(0));
        if (target_shape.dim(1) != @as(i64, @intCast(2 * E + 1))) return error.InvalidGlinerSpanTargetShape;

        const labels = try bld.sliceLastDim(targets, 0, @intCast(E));
        const mask = try bld.sliceLastDim(targets, @intCast(E), @intCast(2 * E));
        const start_idx_2d = try bld.sliceLastDim(targets, @intCast(2 * E), @intCast(2 * E + 1));
        const start_idx_f32 = try bld.reshape(start_idx_2d, Shape.init(.f32, &.{@intCast(span_rows)}));
        const start_idx_i64 = try bld.convertDtype(start_idx_f32, .i64);

        const span_hidden = try bld.gather(
            hidden_flat,
            start_idx_i64,
            Shape.init(.f32, &.{ @intCast(span_rows), @intCast(H) }),
        );

        const head_w = try bld.parameter(
            "classifier.weight",
            Shape.init(.f32, &.{ @intCast(C), @intCast(H) }),
        );
        const head_b = try bld.parameter(
            "classifier.bias",
            Shape.init(.f32, &.{@intCast(C)}),
        );

        const all_logits = try bld.linear(span_hidden, head_w, head_b, span_rows, H, C);
        const entity_logits = try bld.sliceLastDim(all_logits, 1, @intCast(C));
        self.span_logits = entity_logits;

        return buildMaskedSpanStartMseLoss(bld, entity_logits, labels, mask);
    }
};

/// Masked soft-target cross-entropy for `[rows, classes]` logits/targets.
///
/// A target row whose mass is zero is treated as ignored. For standard
/// one-hot targets, the denominator is the number of non-ignored rows. For
/// soft or weighted rows, normalizing by target mass keeps the loss scale
/// stable while still letting callers express per-row weights.
fn buildMaskedSoftTargetCrossEntropyLoss(
    bld: *Builder,
    logits: NodeId,
    targets: NodeId,
) !NodeId {
    const in_shape = bld.graph.node(logits).output_shape;
    const last_axis: u8 = in_shape.rank() - 1;

    const log_probs = try bld.logSoftmax(logits);
    const weighted = try bld.mul(targets, log_probs);
    const class_sum = try bld.reduceSum(weighted, &.{last_axis});

    const row_mass = try bld.reduceSum(targets, &.{last_axis});
    const mass_shape = bld.graph.node(row_mass).output_shape;
    const mass_rank = mass_shape.rank();
    var all_axes: [8]u8 = undefined;
    for (0..mass_rank) |i| all_axes[i] = @intCast(i);

    const total_log_prob = try bld.reduceSum(class_sum, all_axes[0..mass_rank]);
    const neg_total = try bld.neg(total_log_prob);
    const denom = try bld.reduceSum(row_mass, all_axes[0..mass_rank]);
    const eps = try bld.scalarConst(.f32, 1e-12);
    const denom_safe = try bld.add(denom, eps);
    return bld.div(neg_total, denom_safe);
}

fn buildMaskedSpanStartMseLoss(
    bld: *Builder,
    logits: NodeId,
    labels: NodeId,
    mask: NodeId,
) !NodeId {
    const in_shape = bld.graph.node(logits).output_shape;
    const rank = in_shape.rank();
    var all_axes: [8]u8 = undefined;
    for (0..rank) |i| all_axes[i] = @intCast(i);

    const probs = try bld.sigmoid(logits);
    const diff = try bld.sub(probs, labels);
    const masked = try bld.mul(diff, mask);
    const sq = try bld.mul(masked, masked);
    const numerator = try bld.reduceSum(sq, all_axes[0..rank]);
    const denom = try bld.reduceSum(mask, all_axes[0..rank]);
    const eps = try bld.scalarConst(.f32, 1e-12);
    const denom_safe = try bld.add(denom, eps);
    return bld.div(numerator, denom_safe);
}

// ── TrainerInput builders ────────────────────────────────────────────────────

/// Build a `TrainerInput` for one micro-batch. The caller supplies the
/// tokenized batch + per-token one-hot targets and this helper wires them
/// into the format `RealAutodiffTrainer.step` expects.
///
/// The returned value borrows slices from its arguments — keep them alive
/// for the duration of `trainer.step`.
pub fn makeTrainerInput(
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) real_autodiff.TrainerInput {
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &GlinerAutodiffCtx.buildForward,
        .build_loss = &GlinerAutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &GlinerAutodiffCtx.bindArchInputs,
        .remap_graph_nodes = &GlinerAutodiffCtx.remapGraphNodes,
    };
}

/// Convenience wrapper around `trainer.step`. Prefer this over hand-
/// constructing a `TrainerInput` so any future plumbing changes stay
/// localized to this file.
pub fn trainStep(
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) !real_autodiff.StepResult {
    const input = makeTrainerInput(
        ctx,
        input_ids,
        attention_mask,
        targets,
        targets_shape,
        batch,
        seq_len,
    );
    return trainer.step(input);
}

pub fn tokenLogitsForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    batch: u32,
    seq_len: u32,
) ![]f32 {
    const rows: usize = @intCast(batch * seq_len);
    if (input_ids.len != rows or attention_mask.len != rows) return error.InvalidGlinerBatchShape;

    const num_classes: usize = @intCast(ctx.config.num_classes);
    const targets = try allocator.alloc(f32, rows * num_classes);
    defer allocator.free(targets);
    @memset(targets, 0.0);

    const trainer_input = makeTrainerInput(
        ctx,
        input_ids,
        attention_mask,
        targets,
        tokenTargetsShape(batch, seq_len, ctx.config.num_classes),
        batch,
        seq_len,
    );
    try trainer.ensureGraphBuilt(trainer_input);
    var gs = &trainer.graph_state.?;
    const logits_node = ctx.token_logits orelse return error.MissingGlinerTokenLogitsNode;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| trainer.compute_backend.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }

    const input_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.input_ids_node,
        .name = "__input_ids",
        .shape = gs.graph.node(gs.input_ids_node).output_shape,
    };
    const mask_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.attention_mask_node,
        .name = "__attention_mask",
        .shape = gs.graph.node(gs.attention_mask_node).output_shape,
    };
    const targets_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.targets_node,
        .name = "__targets",
        .shape = gs.graph.node(gs.targets_node).output_shape,
    };

    try rt.put(allocator, gs.input_ids_node, try graph_input_binder.bindI64(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, targets));

    for (trainer.lora_params.items) |slot| {
        const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims);
        try rt.put(allocator, slot.node_id, ct);
    }
    for (trainer.regular_params.items) |slot| {
        const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims);
        try rt.put(allocator, slot.node_id, ct);
    }
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }

    const saved_outputs = try allocator.dupe(NodeId, gs.graph.outputs.items);
    defer {
        gs.graph.outputs.clearRetainingCapacity();
        for (saved_outputs) |node_id| gs.graph.outputs.append(allocator, node_id) catch {};
        allocator.free(saved_outputs);
    }
    gs.graph.outputs.clearRetainingCapacity();
    try gs.graph.markOutput(logits_node);

    var rt_inputs = std.ArrayList(interpreter.RuntimeInput).empty;
    defer rt_inputs.deinit(allocator);
    {
        var it = rt.iterator();
        while (it.next()) |entry| {
            try rt_inputs.append(allocator, .{
                .node_id = entry.key_ptr.*,
                .value = entry.value_ptr.*,
            });
        }
    }

    var exec_result = try interpreter.execute(allocator, &gs.graph, trainer.compute_backend, .{
        .runtime_inputs = rt_inputs.items,
    });
    defer exec_result.deinit(trainer.compute_backend);
    return trainer.compute_backend.toFloat32(exec_result.outputs[0], allocator);
}

pub fn spanStartLogitsForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    span_targets: []const f32,
    span_targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) ![]f32 {
    const rows: usize = @intCast(batch * seq_len);
    if (input_ids.len != rows or attention_mask.len != rows) return error.InvalidGlinerBatchShape;
    if (ctx.config.objective != .span_start) return error.InvalidGlinerObjective;

    const trainer_input = makeTrainerInput(
        ctx,
        input_ids,
        attention_mask,
        span_targets,
        span_targets_shape,
        batch,
        seq_len,
    );
    try trainer.ensureGraphBuilt(trainer_input);
    var gs = &trainer.graph_state.?;
    const logits_node = ctx.span_logits orelse return error.MissingGlinerSpanLogitsNode;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| trainer.compute_backend.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }

    const input_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.input_ids_node,
        .name = "__input_ids",
        .shape = gs.graph.node(gs.input_ids_node).output_shape,
    };
    const mask_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.attention_mask_node,
        .name = "__attention_mask",
        .shape = gs.graph.node(gs.attention_mask_node).output_shape,
    };
    const targets_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.targets_node,
        .name = "__targets",
        .shape = gs.graph.node(gs.targets_node).output_shape,
    };

    try rt.put(allocator, gs.input_ids_node, try graph_input_binder.bindI64(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, span_targets));

    for (trainer.lora_params.items) |slot| {
        const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims);
        try rt.put(allocator, slot.node_id, ct);
    }
    for (trainer.regular_params.items) |slot| {
        const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims);
        try rt.put(allocator, slot.node_id, ct);
    }
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }

    const saved_outputs = try allocator.dupe(NodeId, gs.graph.outputs.items);
    defer {
        gs.graph.outputs.clearRetainingCapacity();
        for (saved_outputs) |node_id| gs.graph.outputs.append(allocator, node_id) catch {};
        allocator.free(saved_outputs);
    }
    gs.graph.outputs.clearRetainingCapacity();
    try gs.graph.markOutput(logits_node);

    var rt_inputs = std.ArrayList(interpreter.RuntimeInput).empty;
    defer rt_inputs.deinit(allocator);
    {
        var it = rt.iterator();
        while (it.next()) |entry| {
            try rt_inputs.append(allocator, .{
                .node_id = entry.key_ptr.*,
                .value = entry.value_ptr.*,
            });
        }
    }

    var exec_result = try interpreter.execute(allocator, &gs.graph, trainer.compute_backend, .{
        .runtime_inputs = rt_inputs.items,
    });
    defer exec_result.deinit(trainer.compute_backend);
    return trainer.compute_backend.toFloat32(exec_result.outputs[0], allocator);
}

// ── Shape helpers ────────────────────────────────────────────────────────────

/// Shape of the one-hot target tensor for GLiNER2 token classification:
/// `[batch * seq_len, num_classes]` f32. Callers pass this into
/// `makeTrainerInput` / `trainStep`.
pub fn tokenTargetsShape(batch: u32, seq_len: u32, num_classes: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch * seq_len),
        @intCast(num_classes),
    });
}

/// Shape for packed span-start targets:
/// `[batch * max_spans, 2 * num_entity_types + 1]` f32.
pub fn spanStartTargetsShape(batch: u32, max_spans: u32, num_entity_types: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch * max_spans),
        @intCast(2 * num_entity_types + 1),
    });
}

pub const SpanStartTargetStats = struct {
    valid_span_count: u64 = 0,
    positive_span_label_count: u64 = 0,
    ignored_span_count: u64 = 0,
};

/// Pack `gliner2_data.EncodedBatch` span labels into the single trainer
/// target tensor consumed by `.span_start`.
pub fn fillSpanStartTargetsFromEncodedBatch(
    batch: *const gliner2_data.EncodedBatch,
    out: []f32,
) !SpanStartTargetStats {
    const E = batch.num_entity_types;
    const rows = batch.batch_size * batch.max_spans;
    const width = 2 * E + 1;
    if (out.len != rows * width) return error.InvalidGlinerSpanTargetShape;

    @memset(out, 0.0);
    var stats = SpanStartTargetStats{};

    for (0..batch.batch_size) |sample_idx| {
        const word_pos_offset = sample_idx * batch.max_words_per_sample;
        for (0..batch.max_spans) |span_idx| {
            const flat_span_idx = sample_idx * batch.max_spans + span_idx;
            const row = flat_span_idx * width;

            if (batch.span_mask[flat_span_idx] <= 0.0) {
                stats.ignored_span_count += 1;
                continue;
            }

            const start_word_raw = batch.span_indices[flat_span_idx * 2];
            const end_word_raw = batch.span_indices[flat_span_idx * 2 + 1];
            if (start_word_raw < 0 or end_word_raw < start_word_raw) {
                stats.ignored_span_count += 1;
                continue;
            }
            const start_word: usize = @intCast(start_word_raw);
            if (start_word >= batch.max_words_per_sample) return error.InvalidSpanWordIndex;
            const token_pos_raw = batch.first_token_positions[word_pos_offset + start_word];
            if (token_pos_raw < 0) {
                stats.ignored_span_count += 1;
                continue;
            }
            const token_pos: usize = @intCast(token_pos_raw);
            if (token_pos >= batch.max_length) return error.InvalidTokenPosition;

            for (0..E) |entity_type_idx| {
                const label = batch.span_labels[flat_span_idx * E + entity_type_idx];
                out[row + entity_type_idx] = label;
                out[row + E + entity_type_idx] = 1.0;
                if (label > 0.0) stats.positive_span_label_count += 1;
            }
            out[row + 2 * E] = @as(f32, @floatFromInt(sample_idx * batch.max_length + token_pos));
            stats.valid_span_count += 1;
        }
    }

    return stats;
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Reference mini-config reused across the tests. Matches the smoke test in
/// `deberta_graph.zig` so we hit the same fast codepath.
fn tinyConfig() deberta_graph.Config {
    return .{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
    };
}

test "GlinerAutodiffCtx: init stores config and leaves built null" {
    const cfg = GlinerAutodiffConfig{
        .graph_config = tinyConfig(),
        .num_classes = 5,
    };
    const ctx = GlinerAutodiffCtx.init(cfg);
    try testing.expectEqual(@as(u32, 5), ctx.config.num_classes);
    try testing.expectEqual(GlinerObjective.token, ctx.config.objective);
    try testing.expectEqual(@as(i32, -100), ctx.config.ignore_index);
    try testing.expect(ctx.built == null);
}

test "GlinerAutodiffCtx: span_start objective round-trips through init" {
    const cfg = GlinerAutodiffConfig{
        .graph_config = tinyConfig(),
        .num_classes = 4,
        .objective = .span_start,
    };
    const ctx = GlinerAutodiffCtx.init(cfg);
    try testing.expectEqual(GlinerObjective.span_start, ctx.config.objective);
    try testing.expectEqual(@as(u32, 4), ctx.config.num_classes);
}

test "GlinerAutodiffCtx: custom ignore_index round-trips through init" {
    const cfg = GlinerAutodiffConfig{
        .graph_config = tinyConfig(),
        .num_classes = 9,
        .ignore_index = -1,
    };
    const ctx = GlinerAutodiffCtx.init(cfg);
    try testing.expectEqual(@as(i32, -1), ctx.config.ignore_index);
    try testing.expectEqual(@as(u32, 9), ctx.config.num_classes);
}

test "tokenTargetsShape returns a rank-2 [B*S, C] shape" {
    const s = tokenTargetsShape(2, 8, 5);
    try testing.expectEqual(@as(u8, 2), s.rank());
    try testing.expectEqual(@as(i64, 16), s.dim(0));
    try testing.expectEqual(@as(i64, 5), s.dim(1));

    const s1 = tokenTargetsShape(1, 4, 3);
    try testing.expectEqual(@as(u8, 2), s1.rank());
    try testing.expectEqual(@as(i64, 4), s1.dim(0));
    try testing.expectEqual(@as(i64, 3), s1.dim(1));
}

test "spanStartTargetsShape returns packed span target shape" {
    const s = spanStartTargetsShape(2, 5, 3);
    try testing.expectEqual(@as(u8, 2), s.rank());
    try testing.expectEqual(@as(i64, 10), s.dim(0));
    try testing.expectEqual(@as(i64, 7), s.dim(1));
}

test "fillSpanStartTargetsFromEncodedBatch packs labels masks and token indices" {
    var input_ids = [_]i32{ 10, 11, 12, 13, 14, 0, 0, 0 };
    var attention_mask = [_]i32{ 1, 1, 1, 1, 1, 0, 0, 0 };
    var words_mask = [_]i32{ 0, 0, 1, 2, 3, 0, 0, 0 };
    var first_token_positions = [_]i32{ 2, 3, 4 };
    var word_lengths = [_]f32{ 5, 4, 6 };
    var word_has_digit = [_]f32{0.0} ** 3;
    var word_is_title = [_]f32{0.0} ** 3;
    var word_is_all_caps = [_]f32{0.0} ** 3;
    var span_indices = [_]i32{
        0, 0,
        1, 1,
        2, 2,
    };
    var span_mask = [_]f32{ 1.0, 1.0, 0.0 };
    var span_labels = [_]f32{
        1.0, 0.0,
        0.0, 1.0,
        0.0, 0.0,
    };
    var e_token_positions = [_]i32{ 0, 1 };
    var e_token_end_positions = [_]i32{ 0, 1 };
    var entity_type_kind = [_]i32{ 1, 2 };

    const batch = gliner2_data.EncodedBatch{
        .allocator = testing.allocator,
        .owns_memory = false,
        .input_ids = &input_ids,
        .attention_mask = &attention_mask,
        .words_mask = &words_mask,
        .first_token_positions = &first_token_positions,
        .word_lengths = &word_lengths,
        .word_has_digit = &word_has_digit,
        .word_is_title = &word_is_title,
        .word_is_all_caps = &word_is_all_caps,
        .span_indices = &span_indices,
        .span_mask = &span_mask,
        .span_labels = &span_labels,
        .e_token_positions = &e_token_positions,
        .e_token_end_positions = &e_token_end_positions,
        .entity_type_kind = &entity_type_kind,
        .batch_size = 1,
        .max_length = 8,
        .max_words_per_sample = 3,
        .max_spans = 3,
        .num_entity_types = 2,
    };

    var targets = [_]f32{0.0} ** (3 * 5);
    const stats = try fillSpanStartTargetsFromEncodedBatch(&batch, &targets);
    try testing.expectEqual(@as(u64, 2), stats.valid_span_count);
    try testing.expectEqual(@as(u64, 2), stats.positive_span_label_count);
    try testing.expectEqual(@as(u64, 1), stats.ignored_span_count);

    try testing.expectEqualSlices(f32, &.{ 1.0, 0.0, 1.0, 1.0, 2.0 }, targets[0..5]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 1.0, 1.0, 1.0, 3.0 }, targets[5..10]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0, 0.0, 0.0, 0.0 }, targets[10..15]);
}

test "makeTrainerInput populates the expected fields for token classification" {
    var ctx = GlinerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .num_classes = 4,
    });

    const batch: u32 = 2;
    const seq_len: u32 = 3;
    const num_classes: u32 = 4;

    var input_ids = [_]i64{ 1, 2, 3, 4, 5, 6 };
    var mask = [_]f32{1.0} ** (2 * 3);
    var targets = [_]f32{0.0} ** (2 * 3 * 4);
    // Give each non-ignored token a class; leave one row all-zero to
    // exercise the "ignored via zero row" convention.
    targets[0 * 4 + 1] = 1.0;
    targets[1 * 4 + 2] = 1.0;
    targets[2 * 4 + 0] = 1.0;
    targets[3 * 4 + 3] = 1.0;
    targets[4 * 4 + 1] = 1.0;
    // row 5 intentionally left zero → "ignored" token

    const targets_shape = tokenTargetsShape(batch, seq_len, num_classes);
    const ti = makeTrainerInput(
        &ctx,
        &input_ids,
        &mask,
        &targets,
        targets_shape,
        batch,
        seq_len,
    );

    try testing.expectEqual(@as(u32, batch), ti.batch);
    try testing.expectEqual(@as(u32, seq_len), ti.seq_len);
    try testing.expectEqual(input_ids.len, ti.input_ids.len);
    try testing.expectEqual(mask.len, ti.attention_mask.len);
    try testing.expectEqual(targets.len, ti.targets.len);

    // targets_shape should be rank-2 [B*S, C].
    try testing.expectEqual(@as(u8, 2), ti.targets_shape.rank());
    try testing.expectEqual(@as(i64, batch * seq_len), ti.targets_shape.dim(0));
    try testing.expectEqual(@as(i64, num_classes), ti.targets_shape.dim(1));

    // Callbacks must be wired to the static context methods.
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&GlinerAutodiffCtx.buildForward)),
        @as(*const anyopaque, @ptrCast(ti.build_forward)),
    );
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&GlinerAutodiffCtx.buildLoss)),
        @as(*const anyopaque, @ptrCast(ti.build_loss)),
    );

    // And the ctx pointer should round-trip via the opaque pointer.
    try testing.expectEqual(
        @as(*anyopaque, @ptrCast(&ctx)),
        ti.ctx,
    );
}

test "span_start objective builds span logits and masked loss" {
    const allocator = testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    var ctx = GlinerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .num_classes = 3,
        .objective = .span_start,
    });

    const hidden = try b.parameter("hidden", Shape.init(.f32, &.{ 4, 32 }));
    const targets = try b.parameter("__targets", spanStartTargetsShape(1, 2, 2));
    const loss = try GlinerAutodiffCtx.buildLoss(@ptrCast(&ctx), &b, hidden, targets);
    try g.markOutput(loss);

    const span_logits = ctx.span_logits orelse return error.ExpectedSpanLogitsNode;
    const span_logits_shape = g.node(span_logits).output_shape;
    try testing.expectEqual(@as(u8, 2), span_logits_shape.rank());
    try testing.expectEqual(@as(i64, 2), span_logits_shape.dim(0));
    try testing.expectEqual(@as(i64, 2), span_logits_shape.dim(1));

    const loss_shape = g.node(loss).output_shape;
    try testing.expectEqual(@as(u8, 2), loss_shape.rank());
    try testing.expectEqual(@as(i64, 1), loss_shape.dim(0));
    try testing.expectEqual(@as(i64, 1), loss_shape.dim(1));
}

test "masked token loss excludes zero target rows from denominator" {
    const allocator = testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const logits = try b.tensorConst(&.{
        0.0, 0.0,
        0.0, 0.0,
    }, Shape.init(.f32, &.{ 2, 2 }));
    const targets = try b.tensorConst(&.{
        1.0, 0.0,
        0.0, 0.0,
    }, Shape.init(.f32, &.{ 2, 2 }));
    const loss = try buildMaskedSoftTargetCrossEntropyLoss(&b, logits, targets);
    try g.markOutput(loss);

    var lowered = try ml.graph.lower.lower(allocator, &g);
    defer lowered.deinit();
    var folded = try ml.graph.passes.const_fold.fold(allocator, &lowered.graph);
    defer folded.deinit();

    const out = folded.graph.outputs.items[0];
    const out_node = folded.graph.node(out);
    const attrs = switch (out_node.op) {
        .constant => |attrs| attrs,
        else => return error.ExpectedFoldedConstant,
    };
    const values = folded.graph.constantData(attrs.data_offset, attrs.data_len);
    try testing.expectEqual(@as(usize, 1), values.len);
    try testing.expectApproxEqAbs(@as(f32, @log(2.0)), values[0], 1e-6);
}

test "masked token loss returns zero when every target row is ignored" {
    const allocator = testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const logits = try b.tensorConst(&.{
        2.0, -1.0,
        0.5, 0.25,
    }, Shape.init(.f32, &.{ 2, 2 }));
    const targets = try b.tensorConst(&.{
        0.0, 0.0,
        0.0, 0.0,
    }, Shape.init(.f32, &.{ 2, 2 }));
    const loss = try buildMaskedSoftTargetCrossEntropyLoss(&b, logits, targets);
    try g.markOutput(loss);

    var lowered = try ml.graph.lower.lower(allocator, &g);
    defer lowered.deinit();
    var folded = try ml.graph.passes.const_fold.fold(allocator, &lowered.graph);
    defer folded.deinit();

    const out = folded.graph.outputs.items[0];
    const out_node = folded.graph.node(out);
    const attrs = switch (out_node.op) {
        .constant => |attrs| attrs,
        else => return error.ExpectedFoldedConstant,
    };
    const values = folded.graph.constantData(attrs.data_offset, attrs.data_len);
    try testing.expectEqual(@as(usize, 1), values.len);
    try testing.expectEqual(@as(f32, 0.0), values[0]);
}
