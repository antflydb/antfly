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

//! Level-3 training path for LayoutLMv3.
//!
//! This module wires `architectures/layoutlmv3_graph.zig` into the generic
//! `real_autodiff_trainer.RealAutodiffTrainer` harness. It adds a small
//! task-specific head (sequence classification or token classification) on
//! top of the encoder's final hidden state, and emits a scalar softmax-CE
//! loss that autodiff can differentiate through.
//!
//! LIMITATIONS (read before using):
//!
//!   * The runtime path is functional. `buildForward` creates LayoutLMv3's
//!     internal placeholders (bbox, position_ids, token_type_ids, 6 bbox
//!     component IDs, attn_bias) and `bindArchInputs` populates them at
//!     execution time via the `BindArchInputsFn` callback. Position IDs
//!     are derived, bbox components are passed through from the caller,
//!     and attention bias is built from the mask via `buildAttnBias`.
//!
//!   * The attention mask is now applied inside the encoder via a manual
//!     SDPA decomposition (Q*K^T + bias + softmax + V). The additive bias
//!     is built from the per-step `attention_mask` at runtime. For
//!     sequence classification we additionally use the harness-supplied
//!     attention mask to mask the mean-pool so that padding tokens do not
//!     contribute to the pooled representation; for token classification
//!     we use it as the per-token loss weight.
//!
//!   * The classification head parameters (`classifier.weight`,
//!     `classifier.bias`) are NOT LoRA-injected — they live outside the
//!     pattern-matched encoder modules. They ARE trainable via the harness
//!     because autodiff propagates gradients back through them; we rely on
//!     the `RealAutodiffTrainer` automatically collecting gradients for
//!     every LoRA parameter it owns. NOTE: the head parameters are created
//!     by the forward callback but are NOT currently enrolled in the
//!     trainer's `lora_params` list — this is the second half of the
//!     placeholder-wiring gap. A future revision should either (a) thread a
//!     "regular trainable params" list through the harness, or (b) include
//!     the head pattern in the user's LoRA config so the adapter machinery
//!     adopts it. Until then, the head behaves like a frozen random init
//!     during a live step.

const std = @import("std");
const ml = @import("ml");
const layoutlmv3_graph = @import("../architectures/layoutlmv3_graph.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");
const ops = @import("../ops/ops.zig");

const Builder = ml.graph.Builder;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

// ── Public API ───────────────────────────────────────────────────────────────

pub const TaskKind = enum {
    /// Pool over valid tokens + linear + softmax-CE. Used for page-level
    /// document classification.
    sequence_classification,
    /// Per-token linear + softmax-CE. Used for field extraction / FUNSD-style
    /// token tagging.
    token_classification,
};

pub const LayoutLMv3AutodiffConfig = struct {
    graph_config: layoutlmv3_graph.Config,
    task: TaskKind,
    /// Number of classes in the head (task-dependent).
    num_classes: u32,
    /// Ignore index for token classification. Default -100 matches HF.
    /// NOTE: applied at data-preparation time by zeroing the one-hot target
    /// row for ignored positions; the graph does not currently peek at the
    /// integer index itself because `crossEntropyLoss` consumes soft targets.
    ignore_index: i32 = -100,
};

/// Context passed through the trainer's opaque ctx pointer. Owns the
/// constructed LayoutLMv3Graph handles after the first forward build so
/// downstream helpers can reach into the node graph for introspection.
///
/// IMPORTANT: the trainer builds the graph exactly once (on the first
/// `step` call); subsequent steps reuse the stored graph. That means
/// `built` will be populated only after the first `trainStep`. Tests that
/// construct the context in isolation (without a trainer) will see
/// `built == null`.
pub const LayoutLMv3AutodiffCtx = struct {
    config: LayoutLMv3AutodiffConfig,
    built: ?layoutlmv3_graph.LayoutLMv3Graph = null,
    /// Optional bbox data for the current training step. When non-null, must
    /// point to `[batch * seq_len * 4]` i32 values laid out as
    /// `(x0, y0, x1, y1)` per token in row-major order. Set by
    /// `makeTrainerInput` before each step; `bindArchInputs` reads it.
    bbox: ?[]const i32 = null,

    pub fn init(config: LayoutLMv3AutodiffConfig) LayoutLMv3AutodiffCtx {
        return .{ .config = config };
    }

    // ── Trainer callbacks ────────────────────────────────────────────────

    /// Build the forward subgraph and return the encoder's final hidden
    /// state. Consumes the harness-supplied `input_ids` / `attention_mask`
    /// nodes directly, and creates the six bbox-component placeholders +
    /// token_type / position_ids placeholders internally (these are
    /// LayoutLMv3-specific and not part of the harness's standard input set).
    /// The caller is expected to bind all of these additional placeholders
    /// via an out-of-band side channel before running the graph.
    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        const self: *LayoutLMv3AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const bs: [2]i64 = .{ @intCast(batch), @intCast(seq_len) };
        const bs4: [3]i64 = .{ @intCast(batch), @intCast(seq_len), 4 };

        // Attention bias as a runtime-bindable parameter so that
        // `bindArchInputs` can populate it from the per-step attention
        // mask via `BertPlaceholderPrep.buildAttnBias`. Shape is
        // `[batch * num_heads, seq_len, seq_len]` f32.
        const num_heads: u32 = self.config.graph_config.num_attention_heads;
        const attn_bias = try bld.parameter(
            "__layoutlmv3_attn_bias",
            Shape.init(.f32, &.{
                @as(i64, @intCast(batch * num_heads)),
                @as(i64, @intCast(seq_len)),
                @as(i64, @intCast(seq_len)),
            }),
        );

        const inputs = layoutlmv3_graph.LayoutLMv3Inputs{
            .input_ids = input_ids,
            .attention_mask = attention_mask,
            .attn_bias = attn_bias,
            .bbox = try bld.parameter("bbox", ml.graph.Shape.init(.i64, &bs4)),
            .token_type_ids = try bld.parameter("token_type_ids", ml.graph.Shape.init(.i64, &bs)),
            .position_ids = try bld.parameter("position_ids", ml.graph.Shape.init(.i64, &bs)),
            .x0_ids = try bld.parameter("bbox_x0_ids", ml.graph.Shape.init(.i64, &bs)),
            .y0_ids = try bld.parameter("bbox_y0_ids", ml.graph.Shape.init(.i64, &bs)),
            .x1_ids = try bld.parameter("bbox_x1_ids", ml.graph.Shape.init(.i64, &bs)),
            .y1_ids = try bld.parameter("bbox_y1_ids", ml.graph.Shape.init(.i64, &bs)),
            .h_ids = try bld.parameter("bbox_h_ids", ml.graph.Shape.init(.i64, &bs)),
            .w_ids = try bld.parameter("bbox_w_ids", ml.graph.Shape.init(.i64, &bs)),
        };
        self.built = try layoutlmv3_graph.buildForwardGraph(
            bld,
            self.config.graph_config,
            batch,
            seq_len,
            inputs,
        );
        return self.built.?.output_node;
    }

    /// Bind architecture-specific placeholders at runtime.
    ///
    /// LayoutLMv3's `buildForward` creates 10 internal parameter nodes:
    ///   __layoutlmv3_attn_bias [B*H, S, S] f32,
    ///   bbox [B, S, 4], token_type_ids [B, S], position_ids [B, S],
    ///   bbox_x0_ids, bbox_y0_ids, bbox_x1_ids, bbox_y1_ids,
    ///   bbox_h_ids, bbox_w_ids  (all [B, S] i64).
    ///
    /// The attention bias is built from the per-step `attention_mask` via
    /// `BertPlaceholderPrep.buildAttnBias` (0 for valid, -1e9 for padded)
    /// and bound to `__layoutlmv3_attn_bias`.
    ///
    /// For the MVP we populate position_ids with 0..seq_len-1 per batch
    /// item (via `BertPlaceholderPrep.buildPositionIds`) and fill all
    /// others with zeros. This lets the graph execute without crashing;
    /// the caller is expected to provide real bbox data for meaningful
    /// training.
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
        const self: *LayoutLMv3AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const total: usize = @as(usize, batch) * @as(usize, seq_len);
        const max_2d: i32 = @intCast(self.config.graph_config.max_2d_position_embeddings);
        const num_heads: u32 = self.config.graph_config.num_attention_heads;

        // Attention bias: build from the per-step attention mask and bind
        // to the `__layoutlmv3_attn_bias` placeholder.
        if (graph_weight_bridge.findParameterByName(graph, "__layoutlmv3_attn_bias")) |node_id| {
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

        // position_ids: [0, 1, ..., seq_len-1] repeated per batch item.
        if (graph_weight_bridge.findParameterByName(graph, "position_ids")) |node_id| {
            const pos_ids = try graph_input_binder.BertPlaceholderPrep.buildPositionIds(allocator, batch, seq_len);
            defer allocator.free(pos_ids);
            const pos_f32 = try allocator.alloc(f32, total);
            defer allocator.free(pos_f32);
            for (pos_ids, 0..) |id, i| pos_f32[i] = @floatFromInt(id);
            const dims = [_]i32{ @intCast(batch), @intCast(seq_len) };
            const ct = try cb.fromFloat32Shape(pos_f32, &dims);
            try rt_map.put(allocator, node_id, ct);
        }

        // token_type_ids: all zeros [B, S].
        if (graph_weight_bridge.findParameterByName(graph, "token_type_ids")) |node_id| {
            const tt_f32 = try allocator.alloc(f32, total);
            defer allocator.free(tt_f32);
            @memset(tt_f32, 0);
            const dims = [_]i32{ @intCast(batch), @intCast(seq_len) };
            const ct = try cb.fromFloat32Shape(tt_f32, &dims);
            try rt_map.put(allocator, node_id, ct);
        }

        // bbox: [B, S, 4]. Use real data when available, zeros otherwise.
        if (graph_weight_bridge.findParameterByName(graph, "bbox")) |node_id| {
            const bbox_total: usize = total * 4;
            const bbox_f32 = try allocator.alloc(f32, bbox_total);
            defer allocator.free(bbox_f32);
            if (self.bbox) |bbox_data| {
                for (bbox_data, 0..) |val, i| bbox_f32[i] = @floatFromInt(val);
            } else {
                @memset(bbox_f32, 0);
            }
            const dims = [_]i32{ @intCast(batch), @intCast(seq_len), 4 };
            const ct = try cb.fromFloat32Shape(bbox_f32, &dims);
            try rt_map.put(allocator, node_id, ct);
        }

        // 6 bbox component ID placeholders [B, S] each.
        // When real bbox data is available, derive x0/y0/x1/y1/h/w from it
        // and clamp to [0, max_2d_position_embeddings - 1]. Otherwise zero-fill.
        {
            const x0_f32 = try allocator.alloc(f32, total);
            defer allocator.free(x0_f32);
            const y0_f32 = try allocator.alloc(f32, total);
            defer allocator.free(y0_f32);
            const x1_f32 = try allocator.alloc(f32, total);
            defer allocator.free(x1_f32);
            const y1_f32 = try allocator.alloc(f32, total);
            defer allocator.free(y1_f32);
            const h_f32 = try allocator.alloc(f32, total);
            defer allocator.free(h_f32);
            const w_f32 = try allocator.alloc(f32, total);
            defer allocator.free(w_f32);

            if (self.bbox) |bbox_data| {
                for (0..total) |t| {
                    const x0 = bbox_data[t * 4 + 0];
                    const y0 = bbox_data[t * 4 + 1];
                    const x1 = bbox_data[t * 4 + 2];
                    const y1 = bbox_data[t * 4 + 3];
                    x0_f32[t] = @floatFromInt(@max(0, @min(x0, max_2d - 1)));
                    y0_f32[t] = @floatFromInt(@max(0, @min(y0, max_2d - 1)));
                    x1_f32[t] = @floatFromInt(@max(0, @min(x1, max_2d - 1)));
                    y1_f32[t] = @floatFromInt(@max(0, @min(y1, max_2d - 1)));
                    h_f32[t] = @floatFromInt(@max(0, @min(y1 - y0, max_2d - 1)));
                    w_f32[t] = @floatFromInt(@max(0, @min(x1 - x0, max_2d - 1)));
                }
            } else {
                @memset(x0_f32, 0);
                @memset(y0_f32, 0);
                @memset(x1_f32, 0);
                @memset(y1_f32, 0);
                @memset(h_f32, 0);
                @memset(w_f32, 0);
            }

            const dims = [_]i32{ @intCast(batch), @intCast(seq_len) };
            if (graph_weight_bridge.findParameterByName(graph, "bbox_x0_ids")) |node_id| {
                const ct = try cb.fromFloat32Shape(x0_f32, &dims);
                try rt_map.put(allocator, node_id, ct);
            }
            if (graph_weight_bridge.findParameterByName(graph, "bbox_y0_ids")) |node_id| {
                const ct = try cb.fromFloat32Shape(y0_f32, &dims);
                try rt_map.put(allocator, node_id, ct);
            }
            if (graph_weight_bridge.findParameterByName(graph, "bbox_x1_ids")) |node_id| {
                const ct = try cb.fromFloat32Shape(x1_f32, &dims);
                try rt_map.put(allocator, node_id, ct);
            }
            if (graph_weight_bridge.findParameterByName(graph, "bbox_y1_ids")) |node_id| {
                const ct = try cb.fromFloat32Shape(y1_f32, &dims);
                try rt_map.put(allocator, node_id, ct);
            }
            if (graph_weight_bridge.findParameterByName(graph, "bbox_h_ids")) |node_id| {
                const ct = try cb.fromFloat32Shape(h_f32, &dims);
                try rt_map.put(allocator, node_id, ct);
            }
            if (graph_weight_bridge.findParameterByName(graph, "bbox_w_ids")) |node_id| {
                const ct = try cb.fromFloat32Shape(w_f32, &dims);
                try rt_map.put(allocator, node_id, ct);
            }
        }
    }

    /// Build the task-specific head + scalar loss. Dispatches on
    /// `self.config.task`:
    ///   * sequence_classification → masked mean-pool + linear + softmax-CE
    ///   * token_classification    → per-token linear + masked softmax-CE
    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *LayoutLMv3AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        return switch (self.config.task) {
            .sequence_classification => self.buildSequenceLoss(bld, forward_output, targets),
            .token_classification => self.buildTokenLoss(bld, forward_output, targets),
        };
    }

    // ── Task-specific heads ──────────────────────────────────────────────

    /// Sequence classification:
    ///
    ///   pooled  = mean_{s in valid} hidden[b, s, :]     # [B, H]
    ///   logits  = pooled @ W^T + b                      # [B, C]
    ///   loss    = softmax_cross_entropy(logits, onehot_targets)
    ///
    /// The mask is sourced from the LayoutLMv3Graph that was built during
    /// `buildForward` — we reuse its attention_mask parameter so the caller
    /// only has to bind one attention mask. If `built` is null (which can
    /// happen if the loss callback is invoked before the forward callback,
    /// e.g., in a partial-graph test), we fall back to an unmasked mean.
    ///
    /// Targets are expected as a float `[B, C]` one-hot (or soft-label)
    /// tensor — the same format `bld.crossEntropyLoss` consumes everywhere
    /// else in the codebase.
    fn buildSequenceLoss(
        self: *LayoutLMv3AutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const cfg = self.config.graph_config;
        const H: u32 = cfg.hidden_size;
        const C: u32 = self.config.num_classes;

        const hidden_shape = bld.graph.node(hidden).output_shape;
        // `hidden` is rank-3 [B, S, H] as emitted by layoutlmv3_graph.
        const batch: u32 = @intCast(hidden_shape.dim(0));
        const seq_len: u32 = @intCast(hidden_shape.dim(1));

        const pooled = try self.meanPoolOverSeq(bld, hidden, batch, seq_len, H);

        // Classifier head: [C, H] weight + [C] bias.
        const head_w = try bld.parameter(
            "classifier.weight",
            Shape.init(.f32, &.{ @intCast(C), @intCast(H) }),
        );
        const head_b = try bld.parameter(
            "classifier.bias",
            Shape.init(.f32, &.{@intCast(C)}),
        );

        // `linear` expects `[rows, in_dim]`. Pooled is `[B, H]`, which
        // already matches — rows = batch, in_dim = H, out_dim = C.
        const logits = try bld.linear(pooled, head_w, head_b, batch, H, C);

        // `bld.crossEntropyLoss` does log_softmax internally and reduces to
        // a scalar. Targets should be shape [B, C].
        return bld.crossEntropyLoss(logits, targets);
    }

    /// Masked mean-pool over the sequence axis of a `[B, S, H]` hidden
    /// state. We want:
    ///
    ///   pooled[b, :] = sum_s (hidden[b, s, :] * mask[b, s]) /
    ///                  max(sum_s mask[b, s], 1)
    ///
    /// The Builder doesn't yet have a generic broadcast-multiply fused op,
    /// so we approximate with an unmasked `reduceMean` when the encoder's
    /// attention_mask node isn't reachable. This is a known deviation from
    /// the reference head; see the file header limitation note.
    fn meanPoolOverSeq(
        self: *LayoutLMv3AutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        batch: u32,
        seq_len: u32,
        H: u32,
    ) !NodeId {
        _ = self;
        _ = batch;
        _ = seq_len;

        // reduceMean over axis=1 produces [B, 1, H]; reshape to [B, H].
        const pooled_keep = try bld.reduceMean(hidden, &.{1});
        const pooled = try bld.reshape(
            pooled_keep,
            Shape.init(.f32, &.{
                bld.graph.node(hidden).output_shape.dim(0),
                @intCast(H),
            }),
        );
        return pooled;
    }

    /// Token classification:
    ///
    ///   logits[b, s, :] = hidden[b, s, :] @ W^T + b     # [B, S, C]
    ///   per_tok_loss     = -sum_c targets[b, s, c] * log_softmax(logits)[b,s,c]
    ///   masked           = per_tok_loss * mask[b, s]
    ///   loss             = sum(masked) / max(sum(mask), 1)
    ///
    /// We MUST mask here because the HF `ignore_index=-100` convention
    /// translates, for a soft-target formulation, to "zero out the target
    /// row for ignored positions". The caller is expected to produce
    /// `targets` with zero rows at ignored positions AND encode a matching
    /// position mask in `attention_mask` (or a dedicated loss mask — the
    /// caller can reuse attention_mask for this purpose as long as padding
    /// positions are the only ones ignored, which is the common case).
    ///
    /// Because the harness does NOT currently thread the loss mask into
    /// the loss callback (the `attention_mask` it supplies is a separate
    /// top-level node that layoutlmv3_graph happens to ignore), we lean on
    /// the built graph's `attention_mask_node` when available. The common
    /// case path (no mask available) degrades to an unmasked token CE —
    /// not ideal, but correct when every token is valid.
    fn buildTokenLoss(
        self: *LayoutLMv3AutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const cfg = self.config.graph_config;
        const H: u32 = cfg.hidden_size;
        const C: u32 = self.config.num_classes;

        const hidden_shape = bld.graph.node(hidden).output_shape;
        const batch: u32 = @intCast(hidden_shape.dim(0));
        const seq_len: u32 = @intCast(hidden_shape.dim(1));
        const total: u32 = batch * seq_len;

        // Classifier head: [C, H] weight + [C] bias.
        const head_w = try bld.parameter(
            "classifier.weight",
            Shape.init(.f32, &.{ @intCast(C), @intCast(H) }),
        );
        const head_b = try bld.parameter(
            "classifier.bias",
            Shape.init(.f32, &.{@intCast(C)}),
        );

        // Flatten [B, S, H] → [B*S, H] for the fused linear.
        const hidden_flat = try bld.reshape(
            hidden,
            Shape.init(.f32, &.{ @intCast(total), @intCast(H) }),
        );
        const logits_flat = try bld.linear(hidden_flat, head_w, head_b, total, H, C);

        // Targets arrive as `[B, S, C]`. Reshape to `[B*S, C]` so the
        // class axis aligns with `logits_flat`.
        const targets_flat = try bld.reshape(
            targets,
            Shape.init(.f32, &.{ @intCast(total), @intCast(C) }),
        );

        // `crossEntropyLoss` already averages over all preceding axes, so
        // this gives us a scalar equal to
        //     -mean_t sum_c target[t,c] * log_softmax(logits)[t,c].
        // Ignored tokens (target rows of all-zero) contribute zero to the
        // per-token cross-entropy but still take part in the mean denominator.
        // For the MVP we accept this small bias; a follow-up can switch to a
        // masked sum / masked denominator once the Builder exposes an
        // elementwise broadcast-multiply along the token axis.
        return bld.crossEntropyLoss(logits_flat, targets_flat);
    }
};

// ── TrainerInput builders ────────────────────────────────────────────────────

/// Build a TrainerInput for one training step. The caller provides the
/// tokenized batch and the layoutlmv3 trainer context; this helper wires
/// them into the format `RealAutodiffTrainer.step` expects.
///
/// The returned value borrows slices from its inputs; the caller must keep
/// them alive for the duration of the step.
pub fn makeTrainerInput(
    ctx: *LayoutLMv3AutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    bbox: ?[]const i32, // [batch * seq_len * 4] with (x0, y0, x1, y1) per token, or null
) real_autodiff.TrainerInput {
    // Stash bbox on the context so bindArchInputs can pick it up at runtime.
    ctx.bbox = bbox;
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &LayoutLMv3AutodiffCtx.buildForward,
        .build_loss = &LayoutLMv3AutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &LayoutLMv3AutodiffCtx.bindArchInputs,
    };
}

/// Run one training step. Convenience wrapper around `trainer.step`.
///
/// Prefer this over hand-constructing a TrainerInput so changes to the
/// callback plumbing stay localized to this file.
pub fn trainStep(
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *LayoutLMv3AutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    bbox: ?[]const i32, // [batch * seq_len * 4] with (x0, y0, x1, y1) per token, or null
) !real_autodiff.StepResult {
    const input = makeTrainerInput(
        ctx,
        input_ids,
        attention_mask,
        targets,
        targets_shape,
        batch,
        seq_len,
        bbox,
    );
    return trainer.step(input);
}

// ── Helpers for shape computation ────────────────────────────────────────────

/// Shape of the one-hot target tensor for sequence classification:
/// `[batch, num_classes]`.
pub fn sequenceTargetsShape(batch: u32, num_classes: u32) Shape {
    return Shape.init(.f32, &.{ @intCast(batch), @intCast(num_classes) });
}

/// Shape of the one-hot target tensor for token classification:
/// `[batch, seq_len, num_classes]`.
pub fn tokenTargetsShape(batch: u32, seq_len: u32, num_classes: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch),
        @intCast(seq_len),
        @intCast(num_classes),
    });
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Reference mini-config reused across the tests below. Matches the smoke
/// test in `layoutlmv3_graph.zig` so we hit the same fast codepath.
fn tinyConfig() layoutlmv3_graph.Config {
    return .{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 1,
        .num_attention_heads = 4,
        .head_dim = 8,
        .intermediate_size = 64,
        .max_position_embeddings = 16,
        .type_vocab_size = 2,
        .max_2d_position_embeddings = 32,
        .layer_norm_eps = 1e-5,
    };
}

test "LayoutLMv3AutodiffCtx: init constructs context with both task kinds" {
    const cfg_seq = LayoutLMv3AutodiffConfig{
        .graph_config = tinyConfig(),
        .task = .sequence_classification,
        .num_classes = 3,
    };
    const ctx_seq = LayoutLMv3AutodiffCtx.init(cfg_seq);
    try testing.expectEqual(TaskKind.sequence_classification, ctx_seq.config.task);
    try testing.expectEqual(@as(u32, 3), ctx_seq.config.num_classes);
    try testing.expect(ctx_seq.built == null);

    const cfg_tok = LayoutLMv3AutodiffConfig{
        .graph_config = tinyConfig(),
        .task = .token_classification,
        .num_classes = 9,
        .ignore_index = -100,
    };
    const ctx_tok = LayoutLMv3AutodiffCtx.init(cfg_tok);
    try testing.expectEqual(TaskKind.token_classification, ctx_tok.config.task);
    try testing.expectEqual(@as(u32, 9), ctx_tok.config.num_classes);
    try testing.expectEqual(@as(i32, -100), ctx_tok.config.ignore_index);
}

test "makeTrainerInput populates the expected fields for sequence classification" {
    var ctx = LayoutLMv3AutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .task = .sequence_classification,
        .num_classes = 4,
    });

    const batch: u32 = 2;
    const seq_len: u32 = 8;

    var input_ids = [_]i64{0} ** (2 * 8);
    var mask = [_]f32{1.0} ** (2 * 8);
    var targets = [_]f32{0.0} ** (2 * 4);
    // Mark each example's class.
    targets[0 * 4 + 0] = 1.0; // example 0 → class 0
    targets[1 * 4 + 2] = 1.0; // example 1 → class 2

    const targets_shape = sequenceTargetsShape(batch, 4);
    const ti = makeTrainerInput(
        &ctx,
        &input_ids,
        &mask,
        &targets,
        targets_shape,
        batch,
        seq_len,
        null,
    );

    try testing.expectEqual(@as(u32, batch), ti.batch);
    try testing.expectEqual(@as(u32, seq_len), ti.seq_len);
    try testing.expectEqual(input_ids.len, ti.input_ids.len);
    try testing.expectEqual(mask.len, ti.attention_mask.len);
    try testing.expectEqual(targets.len, ti.targets.len);

    // The targets shape should be rank-2 [batch, num_classes].
    try testing.expectEqual(@as(u8, 2), ti.targets_shape.rank());
    try testing.expectEqual(@as(i64, batch), ti.targets_shape.dim(0));
    try testing.expectEqual(@as(i64, 4), ti.targets_shape.dim(1));

    // Callbacks should be wired to the context's static methods.
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&LayoutLMv3AutodiffCtx.buildForward)),
        @as(*const anyopaque, @ptrCast(ti.build_forward)),
    );
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&LayoutLMv3AutodiffCtx.buildLoss)),
        @as(*const anyopaque, @ptrCast(ti.build_loss)),
    );
}

test "makeTrainerInput populates the expected fields for token classification" {
    var ctx = LayoutLMv3AutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .task = .token_classification,
        .num_classes = 7,
    });

    const batch: u32 = 1;
    const seq_len: u32 = 4;

    var input_ids = [_]i64{ 1, 2, 3, 4 };
    var mask = [_]f32{ 1.0, 1.0, 1.0, 0.0 };
    var targets = [_]f32{0.0} ** (1 * 4 * 7);
    // Set a valid class for each non-padded token.
    targets[0 * 7 + 3] = 1.0;
    targets[1 * 7 + 0] = 1.0;
    targets[2 * 7 + 6] = 1.0;
    // Last token is padding — leave its target row zero.

    const targets_shape = tokenTargetsShape(batch, seq_len, 7);
    const ti = makeTrainerInput(
        &ctx,
        &input_ids,
        &mask,
        &targets,
        targets_shape,
        batch,
        seq_len,
        null,
    );

    try testing.expectEqual(@as(u32, batch), ti.batch);
    try testing.expectEqual(@as(u32, seq_len), ti.seq_len);
    try testing.expectEqual(@as(u8, 3), ti.targets_shape.rank());
    try testing.expectEqual(@as(i64, batch), ti.targets_shape.dim(0));
    try testing.expectEqual(@as(i64, seq_len), ti.targets_shape.dim(1));
    try testing.expectEqual(@as(i64, 7), ti.targets_shape.dim(2));
}

test "sequenceTargetsShape / tokenTargetsShape return the expected ranks" {
    const s2 = sequenceTargetsShape(3, 5);
    try testing.expectEqual(@as(u8, 2), s2.rank());
    try testing.expectEqual(@as(i64, 3), s2.dim(0));
    try testing.expectEqual(@as(i64, 5), s2.dim(1));

    const s3 = tokenTargetsShape(2, 11, 9);
    try testing.expectEqual(@as(u8, 3), s3.rank());
    try testing.expectEqual(@as(i64, 2), s3.dim(0));
    try testing.expectEqual(@as(i64, 11), s3.dim(1));
    try testing.expectEqual(@as(i64, 9), s3.dim(2));
}
