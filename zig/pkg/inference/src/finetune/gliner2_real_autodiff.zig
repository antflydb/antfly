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
//!   * Head   : `task_classifier.weight` [C, H], `task_classifier.bias` [C]
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
//!   * `task_classifier.weight` and `task_classifier.bias` are regular trainable
//!     parameters rather than LoRA adapters. The trainer enrolls them through
//!     `regular_trainable_params`, writes optimizer updates back into
//!     trainer-owned slots, and the GLiNER2 training CLI exports them to
//!     `task_head.safetensors`.
//!   * Objectives: `token` (token classification), `span_start` (graph-native
//!     span-start scoring), and `gliner2_total_loss` (schema-conditioned span
//!     path). The span grid in `gliner2_data.zig` is production-shaped with a
//!     deterministic prediction decoder; span-start graph construction and
//!     target packing (incl. the per-instance count-embedding index) are
//!     unit-tested here. The `gliner2_total_loss` structure, classification, and
//!     count components are all graph-native and exercised against the upstream
//!     Python trainer by the full-task parity gate (`test_gliner2_lora_parity.py`
//!     full-task matrix via `scripts/gliner2/compare_gliner2_lora_python_zig.py`).
//!     Real-model numeric parity is trustworthy on the native backend; the Metal
//!     training-graph-executor path reproduces native step-for-step but has
//!     narrower automated coverage (see that harness for the reconciliation
//!     bounds and the `deberta_graph.zig` disentangled-attention status box).

const std = @import("std");
const ml = @import("ml");
const deberta_graph = @import("../architectures/deberta_graph.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const gliner2_data = @import("gliner2_data.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const graph_runtime = @import("../graph/runtime.zig");
const graph_training = @import("../graph/training.zig");

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
    /// Upstream GLiNER2 total-loss objective. The graph consumes contextual
    /// per-sample schema slots and includes entity/span, classification,
    /// structure/relation, and count components. Cross-runtime parity is gated
    /// component-by-component by the GLiNER2 release harness.
    gliner2_total_loss,
};

pub const SpanStartLossKind = enum {
    bce,
    mse,
};

pub const SpanStartLossReduction = enum {
    mean,
    sum,
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
    /// Positive/negative weights for the span-start multi-label loss. The
    /// defaults match upstream GLiNER2, which applies no positive weighting;
    /// raise the positive weight explicitly (e.g. via --span-positive-weight)
    /// to offset sparse entity-label signal in a large valid-span grid.
    span_start_positive_weight: f32 = 1.0,
    span_start_negative_weight: f32 = 1.0,
    /// Binary cross-entropy is the production span-label objective. MSE is
    /// retained as an explicit compatibility path for old calibration runs.
    span_start_loss: SpanStartLossKind = .bce,
    /// `.sum` matches upstream GLiNER2 `compute_struct_loss`; select `.mean`
    /// explicitly for the legacy normalized local training behavior.
    span_start_loss_reduction: SpanStartLossReduction = .sum,
    /// Maximum number of structure instances (`gold_count`) across the dataset.
    /// `1` (default) keeps the legacy single-instance structure-loss path
    /// byte-identical. `>1` activates the per-instance count-conditioned
    /// einsum that matches upstream `compute_struct_loss`'s
    /// `(gold_count, num_fields, starts, widths)` scoring, where each instance
    /// `i` uses the learned `count_embed.pos_embedding` row `i` (document
    /// order). Set by the trainer from the dataset's max structure count.
    structure_max_instances: u32 = 1,
    /// Maximum upstream schema/task rows per sample. GLiNER2 total-loss packs
    /// classification/count supervision into the task-row prefix of each
    /// sample's span block; compacting those heads to this prefix avoids
    /// materialising masked-out logits for every span row.
    max_schema_tasks: u32 = 1,
    /// For GLiNER2 total-loss structure loss, process whole-sample span blocks
    /// in groups of at most this many samples. `0` keeps the legacy full-batch
    /// structure projection. A value such as `8` preserves LoRA attachment and
    /// loss semantics while reducing the peak span-head tensor shapes for
    /// large batches.
    structure_span_chunk_samples: u32 = 0,
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
    span_debug_start_hidden: ?NodeId = null,
    span_debug_end_hidden: ?NodeId = null,
    span_debug_projected_span: ?NodeId = null,
    span_debug_schema_hidden: ?NodeId = null,
    span_debug_count_gru_state: ?NodeId = null,
    span_debug_count_state: ?NodeId = null,
    span_debug_count_in_project: ?NodeId = null,
    span_debug_count_layer0: ?NodeId = null,
    span_debug_count_layer1: ?NodeId = null,
    span_debug_count_out0: ?NodeId = null,
    span_debug_count_out2: ?NodeId = null,
    span_debug_schema_projection: ?NodeId = null,
    gliner2_structure_loss: ?NodeId = null,
    gliner2_classification_logits: ?NodeId = null,
    gliner2_count_logits: ?NodeId = null,
    gliner2_classification_loss: ?NodeId = null,
    gliner2_count_loss: ?NodeId = null,
    graph_batch: u32 = 0,
    graph_seq_len: u32 = 0,

    pub fn init(config: GlinerAutodiffConfig) GlinerAutodiffCtx {
        return .{ .config = config };
    }

    fn graphVariant(self: *const GlinerAutodiffCtx) [4]u64 {
        return .{
            self.config.num_classes,
            self.config.structure_max_instances,
            self.config.max_schema_tasks,
            self.config.structure_span_chunk_samples,
        };
    }

    pub fn captureGraphContext(ctx_opaque: *anyopaque, allocator: std.mem.Allocator) anyerror!?*anyopaque {
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const snapshot = try allocator.create(GlinerAutodiffCtx);
        snapshot.* = self.*;
        return @ptrCast(snapshot);
    }

    pub fn restoreGraphContext(ctx_opaque: *anyopaque, state: ?*anyopaque) anyerror!void {
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const snapshot: *const GlinerAutodiffCtx = @ptrCast(@alignCast(state orelse return error.MissingGraphContextState));
        self.* = snapshot.*;
    }

    pub fn deinitGraphContext(state: ?*anyopaque, allocator: std.mem.Allocator) void {
        const snapshot: *GlinerAutodiffCtx = @ptrCast(@alignCast(state orelse return));
        allocator.destroy(snapshot);
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
        const config = self.config;
        self.* = .{ .config = config };
        const cfg = self.config.graph_config;
        self.graph_batch = batch;
        self.graph_seq_len = seq_len;

        // Keep LayerNorm fused so autodiff differentiates it with the custom
        // `fused_layer_norm_backward` VJP (op-count win) instead of lowering to
        // ~11 primitives + their backward. Flag-gated, default-off.
        bld.fuse_layer_norm_backward = deberta_graph.fuseLayerNormBackwardEnabled();

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
        if (self.span_debug_start_hidden) |node_id| self.span_debug_start_hidden = id_map[node_id];
        if (self.span_debug_end_hidden) |node_id| self.span_debug_end_hidden = id_map[node_id];
        if (self.span_debug_projected_span) |node_id| self.span_debug_projected_span = id_map[node_id];
        if (self.span_debug_schema_hidden) |node_id| self.span_debug_schema_hidden = id_map[node_id];
        if (self.span_debug_count_gru_state) |node_id| self.span_debug_count_gru_state = id_map[node_id];
        if (self.span_debug_count_state) |node_id| self.span_debug_count_state = id_map[node_id];
        if (self.span_debug_count_in_project) |node_id| self.span_debug_count_in_project = id_map[node_id];
        if (self.span_debug_count_layer0) |node_id| self.span_debug_count_layer0 = id_map[node_id];
        if (self.span_debug_count_layer1) |node_id| self.span_debug_count_layer1 = id_map[node_id];
        if (self.span_debug_count_out0) |node_id| self.span_debug_count_out0 = id_map[node_id];
        if (self.span_debug_count_out2) |node_id| self.span_debug_count_out2 = id_map[node_id];
        if (self.span_debug_schema_projection) |node_id| self.span_debug_schema_projection = id_map[node_id];
        if (self.gliner2_structure_loss) |node_id| self.gliner2_structure_loss = id_map[node_id];
        if (self.gliner2_classification_logits) |node_id| self.gliner2_classification_logits = id_map[node_id];
        if (self.gliner2_count_logits) |node_id| self.gliner2_count_logits = id_map[node_id];
        if (self.gliner2_classification_loss) |node_id| self.gliner2_classification_loss = id_map[node_id];
        if (self.gliner2_count_loss) |node_id| self.gliner2_count_loss = id_map[node_id];
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
            .gliner2_total_loss => self.buildGliner2TotalLoss(bld, forward_output, targets),
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

        const head_w = try bld.parameter(
            "task_classifier.weight",
            Shape.init(.f32, &.{ @intCast(C), @intCast(H) }),
        );
        const head_b = try bld.parameter(
            "task_classifier.bias",
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
        return self.buildSpanStartLossForBatch(bld, hidden, targets, self.graph_batch);
    }

    fn buildSpanStartLossForBatch(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
        graph_batch: u32,
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
        const legacy_unweighted_start_only_width: i64 = @intCast(2 * E + 1);
        const legacy_weighted_start_only_width: i64 = @intCast(3 * E + 1);
        const legacy_unweighted_target_width: i64 = @intCast(2 * E + 2);
        const legacy_weighted_target_width: i64 = @intCast(3 * E + 2);
        const schema_legacy_unweighted_target_width: i64 = @intCast(4 * E + 2);
        const schema_count_unweighted_target_width: i64 = @intCast(5 * E + 2);
        const schema_count_weighted_target_width: i64 = @intCast(6 * E + 2);
        const target_width = target_shape.dim(1);
        const is_schema_conditioned = target_width == schema_legacy_unweighted_target_width or target_width == schema_count_unweighted_target_width or target_width == schema_count_weighted_target_width;
        const has_schema_count_idx = target_width == schema_count_unweighted_target_width or target_width == schema_count_weighted_target_width;
        const has_label_positive_weights = target_width == schema_count_weighted_target_width or target_width == legacy_weighted_target_width or target_width == legacy_weighted_start_only_width;
        const has_end_idx = is_schema_conditioned or target_width == legacy_unweighted_target_width or target_width == legacy_weighted_target_width;
        if (target_width != schema_legacy_unweighted_target_width and
            target_width != schema_count_unweighted_target_width and
            target_width != schema_count_weighted_target_width and
            target_width != legacy_unweighted_target_width and
            target_width != legacy_weighted_target_width and
            target_width != legacy_unweighted_start_only_width and
            target_width != legacy_weighted_start_only_width) return error.InvalidGlinerSpanTargetShape;

        const labels = try bld.sliceLastDim(targets, 0, @intCast(E));
        const mask = try bld.sliceLastDim(targets, @intCast(E), @intCast(2 * E));
        const label_positive_weights = if (has_label_positive_weights)
            try bld.sliceLastDim(targets, @intCast(2 * E), @intCast(3 * E))
        else
            null;
        const schema_idx_offset: u32 = if (has_label_positive_weights) 3 * E else 2 * E;
        const row_idx_offset: u32 = schema_idx_offset + E;
        const count_idx_offset: u32 = row_idx_offset + E;
        const start_idx_offset: u32 = if (is_schema_conditioned)
            if (has_schema_count_idx) count_idx_offset + E else row_idx_offset + E
        else if (has_label_positive_weights) 3 * E else 2 * E;
        const start_idx_2d = try bld.sliceLastDim(targets, @intCast(start_idx_offset), @intCast(start_idx_offset + 1));
        const start_idx_f32 = try bld.reshape(start_idx_2d, Shape.init(.f32, &.{@intCast(span_rows)}));
        const start_idx_i64 = try bld.convertDtype(start_idx_f32, .i64);

        const start_hidden = try bld.gather(
            hidden_flat,
            start_idx_i64,
            Shape.init(.f32, &.{ @intCast(span_rows), @intCast(H) }),
        );
        const end_hidden_for_parity = if (has_end_idx) blk: {
            const end_idx_2d = try bld.sliceLastDim(targets, @intCast(start_idx_offset + 1), @intCast(start_idx_offset + 2));
            const end_idx_f32 = try bld.reshape(end_idx_2d, Shape.init(.f32, &.{@intCast(span_rows)}));
            const end_idx_i64 = try bld.convertDtype(end_idx_f32, .i64);
            break :blk try bld.gather(
                hidden_flat,
                end_idx_i64,
                Shape.init(.f32, &.{ @intCast(span_rows), @intCast(H) }),
            );
        } else start_hidden;
        const projected_span = try buildParitySpanProjection(bld, start_hidden, end_hidden_for_parity, span_rows, H);
        self.span_debug_start_hidden = start_hidden;
        self.span_debug_end_hidden = end_hidden_for_parity;
        self.span_debug_projected_span = projected_span;
        const entity_logits = if (is_schema_conditioned) blk: {
            if (graph_batch == 0 or span_rows == 0 or @mod(span_rows, graph_batch) != 0) return error.InvalidGlinerSpanTargetShape;
            const max_spans_per_sample = span_rows / graph_batch;
            const compact_schema_rows = graph_batch * E;

            const first_span_rows_buf = try bld.graph.allocator.alloc(f32, graph_batch);
            defer bld.graph.allocator.free(first_span_rows_buf);
            for (first_span_rows_buf, 0..) |*dst, sample_idx| {
                dst.* = @floatFromInt(sample_idx * max_spans_per_sample);
            }
            const first_span_rows_f32 = try bld.tensorConst(
                first_span_rows_buf,
                Shape.init(.f32, &.{@intCast(graph_batch)}),
            );
            const first_span_rows_i64 = try bld.convertDtype(first_span_rows_f32, .i64);

            const schema_idx_2d = try bld.sliceLastDim(targets, @intCast(schema_idx_offset), @intCast(schema_idx_offset + E));
            const compact_schema_idx_2d = try bld.gather(
                schema_idx_2d,
                first_span_rows_i64,
                Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(E) }),
            );
            const schema_idx_f32 = try bld.reshape(compact_schema_idx_2d, Shape.init(.f32, &.{@intCast(compact_schema_rows)}));
            const schema_idx_i64 = try bld.convertDtype(schema_idx_f32, .i64);
            const schema_hidden = try bld.gather(
                hidden_flat,
                schema_idx_i64,
                Shape.init(.f32, &.{ @intCast(compact_schema_rows), @intCast(H) }),
            );
            self.span_debug_schema_hidden = schema_hidden;
            // H1: the count-embed attention key mask is the *schema activity*
            // mask and must never be the structure BCE mask at `[E, 2E)`.
            // `fillSpanStartTargetsFromEncodedBatchWithOptions` writes that
            // block per (span, field) as the BCE weight — 1.0, or
            // `--span-hard-negative-weight` for negatives that overlap a
            // positive — and the trainer's `applySpanNegativeMask` then
            // stochastically rewrites entries to 0.0
            // (`--span-negative-mask-rate`, default 0.5). Gathering it here fed
            // the negative sampler into `buildCountScaledDotProductAttention`,
            // which (a) made the schema projection differ between training and
            // eval (the eval adapter never masks) and (b) let a weight > 1 flip
            // the additive bias sign: `(1 - 2) * -1e9 = +1e9` pins attention
            // onto exactly the key it was meant to down-weight, which is
            // bit-identical to masking every other field out. Upstream applies
            // the random mask only to the loss weight — `count_embed` always
            // sees the full schema. The `.span_start` encoder builds one global
            // entity-type list shared by every sample, so all `E` fields are
            // genuinely active and the true activity mask is all-ones; keeping
            // it explicit (rather than passing `null`) leaves the hook in place
            // should this layout ever gain a real per-sample active-field block
            // like the total-loss layout's `gliner2TotalLossActiveFieldsOffset`.
            const compact_active_mask_buf = try bld.graph.allocator.alloc(f32, compact_schema_rows);
            defer bld.graph.allocator.free(compact_active_mask_buf);
            @memset(compact_active_mask_buf, 1.0);
            const compact_active_mask = try bld.tensorConst(
                compact_active_mask_buf,
                Shape.init(.f32, &.{@intCast(compact_schema_rows)}),
            );
            const count_state_idx_i64 = if (has_schema_count_idx) count_idx: {
                const count_idx_2d = try bld.sliceLastDim(targets, @intCast(count_idx_offset), @intCast(count_idx_offset + E));
                const compact_count_idx_2d = try bld.gather(
                    count_idx_2d,
                    first_span_rows_i64,
                    Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(E) }),
                );
                const count_idx_f32 = try bld.reshape(compact_count_idx_2d, Shape.init(.f32, &.{@intCast(compact_schema_rows)}));
                break :count_idx try bld.convertDtype(count_idx_f32, .i64);
            } else null;
            var count_debug_nodes: CountEmbedDebugNodes = .{};
            const compact_schema_projection = try buildParityCountEmbed(bld, schema_hidden, count_state_idx_i64, compact_active_mask, compact_schema_rows, E, H, &count_debug_nodes);
            self.span_debug_count_gru_state = count_debug_nodes.gru_state;
            self.span_debug_count_state = count_debug_nodes.count_state;
            self.span_debug_count_in_project = count_debug_nodes.in_project;
            self.span_debug_count_layer0 = count_debug_nodes.layer0;
            self.span_debug_count_layer1 = count_debug_nodes.layer1;
            self.span_debug_count_out0 = count_debug_nodes.out0;
            self.span_debug_count_out2 = count_debug_nodes.out2;
            self.span_debug_schema_projection = compact_schema_projection;

            const schema_repeat_rows_buf = try bld.graph.allocator.alloc(f32, span_rows * E);
            defer bld.graph.allocator.free(schema_repeat_rows_buf);
            for (0..span_rows) |span_row| {
                const sample_idx = span_row / max_spans_per_sample;
                for (0..E) |entity_type_idx| {
                    schema_repeat_rows_buf[span_row * E + entity_type_idx] = @floatFromInt(sample_idx * E + entity_type_idx);
                }
            }
            const schema_repeat_rows_f32 = try bld.tensorConst(
                schema_repeat_rows_buf,
                Shape.init(.f32, &.{@intCast(span_rows * E)}),
            );
            const schema_repeat_rows_i64 = try bld.convertDtype(schema_repeat_rows_f32, .i64);
            const schema_projection = try bld.gather(
                compact_schema_projection,
                schema_repeat_rows_i64,
                Shape.init(.f32, &.{ @intCast(span_rows * E), @intCast(H) }),
            );

            const row_idx_2d = try bld.sliceLastDim(targets, @intCast(row_idx_offset), @intCast(row_idx_offset + E));
            const row_idx_f32 = try bld.reshape(row_idx_2d, Shape.init(.f32, &.{@intCast(span_rows * E)}));
            const row_idx_i64 = try bld.convertDtype(row_idx_f32, .i64);
            const repeated_span = try bld.gather(
                projected_span,
                row_idx_i64,
                Shape.init(.f32, &.{ @intCast(span_rows * E), @intCast(H) }),
            );
            const span_schema_product = try bld.mul(repeated_span, schema_projection);
            const span_schema_score_flat = try bld.reduceSum(span_schema_product, &.{1});
            const schema_logits = try bld.reshape(span_schema_score_flat, Shape.init(.f32, &.{ @intCast(span_rows), @intCast(E) }));
            if (self.config.objective == .gliner2_total_loss) break :blk schema_logits;
            const aux_scalar = try buildSchemaObjectiveAuxiliaryZeroScalar(bld, projected_span, span_rows, H, C);
            break :blk try bld.add(schema_logits, aux_scalar);
        } else blk: {
            const head_w = try bld.parameter(
                "task_classifier.weight",
                Shape.init(.f32, &.{ @intCast(C), @intCast(H) }),
            );
            const head_b = try bld.parameter(
                "task_classifier.bias",
                Shape.init(.f32, &.{@intCast(C)}),
            );

            const all_logits = try bld.linear(projected_span, head_w, head_b, span_rows, H, C);
            const parity_scalar = try buildParityAuxiliaryHeads(bld, projected_span, span_rows, H);
            const all_logits_with_parity = try bld.add(all_logits, parity_scalar);
            break :blk try bld.sliceLastDim(all_logits_with_parity, 1, @intCast(C));
        };
        self.span_logits = entity_logits;

        return switch (self.config.span_start_loss) {
            .bce => buildMaskedSpanStartBceLoss(
                bld,
                entity_logits,
                labels,
                mask,
                label_positive_weights,
                self.config.span_start_positive_weight,
                self.config.span_start_negative_weight,
                self.config.span_start_loss_reduction,
            ),
            .mse => buildMaskedSpanStartMseLoss(
                bld,
                entity_logits,
                labels,
                mask,
                label_positive_weights,
                self.config.span_start_positive_weight,
                self.config.span_start_negative_weight,
                self.config.span_start_loss_reduction,
            ),
        };
    }

    fn buildGliner2TotalLoss(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const C: u32 = self.config.num_classes;
        if (C < 2) return error.InvalidGlinerClassCount;
        const E: u32 = C - 1;
        const H: u32 = self.config.graph_config.hidden_size;
        const target_shape = bld.graph.node(targets).output_shape;
        if (target_shape.rank() != 2) return error.InvalidGlinerSpanTargetShape;
        const expected_width: i64 = @intCast(gliner2TotalLossTargetWidthEx(@intCast(E), self.config.structure_max_instances));
        if (target_shape.dim(1) != expected_width) return error.InvalidGlinerSpanTargetShape;

        const structure_loss = try self.buildGliner2PerInstanceStructureLoss(bld, hidden, targets, E, H);
        const classification_loss = try self.buildGliner2ClassificationLoss(bld, hidden, targets);
        const count_loss = try self.buildGliner2CountLoss(bld, hidden, targets);
        self.gliner2_structure_loss = structure_loss;
        self.gliner2_classification_loss = classification_loss;
        self.gliner2_count_loss = count_loss;
        return bld.add(try bld.add(structure_loss, classification_loss), count_loss);
    }

    /// Per-instance count-conditioned structure loss matching upstream
    /// `compute_struct_loss`'s `(gold_count, num_fields, starts, widths)`
    /// einsum. Reads the legacy structure columns plus direct trailing
    /// per-instance label and mask tensors when there is more than one
    /// instance, and scores each span against every
    /// `(instance, field)` projection, with `labs[span, instance, field]` and a
    /// mask gating `instance < gold_count`, active fields, and valid spans.
    fn buildGliner2PerInstanceStructureLoss(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
        E: u32,
        H: u32,
    ) !NodeId {
        const target_shape = bld.graph.node(targets).output_shape;
        const span_rows: u32 = @intCast(target_shape.dim(0));
        const target_width: u32 = @intCast(target_shape.dim(1));
        const graph_batch = self.graph_batch;
        if (graph_batch == 0 or span_rows == 0 or @mod(span_rows, graph_batch) != 0) return error.InvalidGlinerSpanTargetShape;
        const max_spans_per_sample = span_rows / graph_batch;

        const chunk_samples = self.config.structure_span_chunk_samples;
        if (chunk_samples > 0 and graph_batch > chunk_samples and self.config.span_start_loss_reduction == .sum) {
            var total_loss: ?NodeId = null;
            var sample_start: u32 = 0;
            while (sample_start < graph_batch) {
                const sample_count = @min(chunk_samples, graph_batch - sample_start);
                const chunk_rows = sample_count * max_spans_per_sample;
                const row_indices = try buildSpanSampleChunkRowIndices(bld, sample_start, sample_count, max_spans_per_sample);
                const chunk_targets = try bld.gather(
                    targets,
                    row_indices,
                    Shape.init(.f32, &.{ @intCast(chunk_rows), @intCast(target_width) }),
                );
                const chunk_loss = try self.buildGliner2PerInstanceStructureLossForTargets(
                    bld,
                    hidden,
                    chunk_targets,
                    E,
                    H,
                    sample_count,
                );
                total_loss = if (total_loss) |acc| try bld.add(acc, chunk_loss) else chunk_loss;
                sample_start += sample_count;
            }
            return total_loss orelse error.InvalidGlinerSpanTargetShape;
        }

        return self.buildGliner2PerInstanceStructureLossForTargets(
            bld,
            hidden,
            targets,
            E,
            H,
            graph_batch,
        );
    }

    fn buildGliner2PerInstanceStructureLossForTargets(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
        E: u32,
        H: u32,
        graph_batch: u32,
    ) !NodeId {
        const max_gold = self.config.structure_max_instances;
        const target_shape = bld.graph.node(targets).output_shape;
        const span_rows: u32 = @intCast(target_shape.dim(0));
        if (graph_batch == 0 or span_rows == 0 or @mod(span_rows, graph_batch) != 0) return error.InvalidGlinerSpanTargetShape;
        const max_spans_per_sample = span_rows / graph_batch;
        const compact_schema_rows = graph_batch * E;

        const hidden_flat = try flattenHiddenForLoss(bld, hidden, H);

        // Legacy total-loss structure layout (unweighted):
        // labels[0:E], mask[E:2E], schema_idx[2E:3E], row_idx[3E:4E],
        // count_idx[4E:5E], start[5E], end[5E+1].
        const schema_idx_offset: u32 = 2 * E;
        const start_idx_offset: u32 = 5 * E;

        // Span representation (identical to buildSpanStartLoss).
        const start_idx_2d = try bld.sliceLastDim(targets, @intCast(start_idx_offset), @intCast(start_idx_offset + 1));
        const start_idx_i64 = try bld.convertDtype(try bld.reshape(start_idx_2d, Shape.init(.f32, &.{@intCast(span_rows)})), .i64);
        const end_idx_2d = try bld.sliceLastDim(targets, @intCast(start_idx_offset + 1), @intCast(start_idx_offset + 2));
        const end_idx_i64 = try bld.convertDtype(try bld.reshape(end_idx_2d, Shape.init(.f32, &.{@intCast(span_rows)})), .i64);
        const start_hidden = try bld.gather(hidden_flat, start_idx_i64, Shape.init(.f32, &.{ @intCast(span_rows), @intCast(H) }));
        const end_hidden = try bld.gather(hidden_flat, end_idx_i64, Shape.init(.f32, &.{ @intCast(span_rows), @intCast(H) }));
        const projected_span = try buildParitySpanProjection(bld, start_hidden, end_hidden, span_rows, H);
        self.span_debug_projected_span = projected_span;

        // First-span-row indices: one row per sample, to gather the per-sample
        // schema field embeddings + active mask (which are span-invariant).
        const first_span_rows_buf = try bld.graph.allocator.alloc(f32, graph_batch);
        defer bld.graph.allocator.free(first_span_rows_buf);
        for (first_span_rows_buf, 0..) |*dst, sample_idx| {
            dst.* = @floatFromInt(sample_idx * max_spans_per_sample);
        }
        const first_span_rows_i64 = try bld.convertDtype(
            try bld.tensorConst(first_span_rows_buf, Shape.init(.f32, &.{@intCast(graph_batch)})),
            .i64,
        );

        const schema_idx_2d = try bld.sliceLastDim(targets, @intCast(schema_idx_offset), @intCast(schema_idx_offset + E));
        const compact_schema_idx_2d = try bld.gather(schema_idx_2d, first_span_rows_i64, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(E) }));
        const schema_idx_i64 = try bld.convertDtype(try bld.reshape(compact_schema_idx_2d, Shape.init(.f32, &.{@intCast(compact_schema_rows)})), .i64);
        const schema_hidden = try bld.gather(hidden_flat, schema_idx_i64, Shape.init(.f32, &.{ @intCast(compact_schema_rows), @intCast(H) }));
        self.span_debug_schema_hidden = schema_hidden;

        // Schema activity comes from its own never-mutated block, not from the
        // structure BCE mask at [E, 2E): the trainer's stochastic negative
        // masking rewrites that mask in place, and upstream applies the random
        // mask only to the loss weight — `count_embed` always sees the full
        // schema.
        const active_offset: i64 = @intCast(gliner2TotalLossActiveFieldsOffset(@intCast(E), max_gold));
        const active_2d = try bld.sliceLastDim(targets, active_offset, active_offset + @as(i64, @intCast(E))); // [span_rows, E]
        const compact_active_2d = try bld.gather(active_2d, first_span_rows_i64, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(E) }));
        const compact_active = try bld.reshape(compact_active_2d, Shape.init(.f32, &.{@intCast(compact_schema_rows)}));

        // Each active schema field carries its upstream task id. The count
        // transformer is invoked independently per schema upstream, so this
        // id becomes a block-diagonal attention mask below. Without it,
        // fields from mixed entity/JSON/relation tasks cross-attend.
        const task_group_offset: i64 = @intCast(gliner2TotalLossTaskGroupsOffset(@intCast(E), max_gold));
        const task_groups_2d = try bld.sliceLastDim(targets, task_group_offset, task_group_offset + @as(i64, @intCast(E)));
        const compact_task_groups_2d = try bld.gather(task_groups_2d, first_span_rows_i64, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(E) }));
        const compact_task_groups = try bld.reshape(compact_task_groups_2d, Shape.init(.f32, &.{@intCast(compact_schema_rows)}));

        // struct_proj [graph_batch*max_gold*E, H] ordered [sample, instance, field].
        const struct_proj = try buildParityCountEmbedSequence(bld, schema_hidden, compact_active, compact_task_groups, graph_batch, E, H, max_gold);
        self.span_debug_schema_projection = struct_proj;

        // scores[span, instance*E + field] = projected_span · struct_proj.
        const span_3d = try bld.reshape(projected_span, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(max_spans_per_sample), @intCast(H) }));
        const proj_3d = try bld.reshape(struct_proj, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(max_gold * E), @intCast(H) }));
        const proj_3d_t = try bld.transpose(proj_3d, &.{ 0, 2, 1 }); // [graph_batch, H, max_gold*E]
        const scores_3d = try bld.matmul3D(span_3d, proj_3d_t); // [graph_batch, max_spans, max_gold*E]
        const scores_flat = try bld.reshape(scores_3d, Shape.init(.f32, &.{ @intCast(span_rows), @intCast(max_gold * E) }));
        self.span_logits = scores_flat;

        // Direct per-instance targets preserve repeated values and allow one
        // independent negative-mask decision per (instance, field, span), as
        // in upstream's [gold_count, fields, starts, widths] tensor.
        const labs_flat, const mask_flat = if (max_gold > 1) blk: {
            const instance_values: i64 = @intCast(max_gold * E);
            const instance_labels_offset: i64 = @intCast(gliner2TotalLossInstanceLabelsOffset(@intCast(E)));
            const instance_masks_offset: i64 = @intCast(gliner2TotalLossInstanceMasksOffset(@intCast(E), max_gold));
            break :blk .{
                try bld.sliceLastDim(targets, instance_labels_offset, instance_labels_offset + instance_values),
                try bld.sliceLastDim(targets, instance_masks_offset, instance_masks_offset + instance_values),
            };
        } else .{
            try bld.sliceLastDim(targets, 0, @intCast(E)),
            try bld.sliceLastDim(targets, @intCast(E), @intCast(2 * E)),
        };

        return buildMaskedSpanStartBceLoss(
            bld,
            scores_flat,
            labs_flat,
            mask_flat,
            null,
            self.config.span_start_positive_weight,
            self.config.span_start_negative_weight,
            self.config.span_start_loss_reduction,
        );
    }

    fn buildGliner2ClassificationLoss(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const C: u32 = self.config.num_classes;
        const E: u32 = C - 1;
        const H: u32 = self.config.graph_config.hidden_size;
        const target_shape = bld.graph.node(targets).output_shape;
        const rows: u32 = @intCast(target_shape.dim(0));
        const graph_batch = self.graph_batch;
        if (graph_batch == 0 or rows == 0 or @mod(rows, graph_batch) != 0) return error.InvalidGlinerSpanTargetShape;
        const max_spans_per_sample = rows / graph_batch;
        const schema_tasks = @min(max_spans_per_sample, @max(@as(u32, 1), self.config.max_schema_tasks));
        const compact_rows = graph_batch * schema_tasks;
        const task_row_indices = try buildSchemaTaskRowIndices(bld, graph_batch, max_spans_per_sample, schema_tasks);
        const labels_offset: i64 = @intCast(gliner2TotalLossClassificationLabelsOffset(@intCast(E)));
        const mask_offset: i64 = @intCast(gliner2TotalLossClassificationMaskOffset(@intCast(E)));
        const schema_idx_offset: i64 = @intCast(2 * E);

        const hidden_flat = try flattenHiddenForLoss(bld, hidden, H);
        const schema_idx_all = try bld.sliceLastDim(targets, schema_idx_offset, schema_idx_offset + @as(i64, @intCast(E)));
        const schema_idx_2d = try bld.gather(
            schema_idx_all,
            task_row_indices,
            Shape.init(.f32, &.{ @intCast(compact_rows), @intCast(E) }),
        );
        const schema_idx_f32 = try bld.reshape(schema_idx_2d, Shape.init(.f32, &.{@intCast(compact_rows * E)}));
        const schema_idx_i64 = try bld.convertDtype(schema_idx_f32, .i64);
        const schema_hidden = try bld.gather(
            hidden_flat,
            schema_idx_i64,
            Shape.init(.f32, &.{ @intCast(compact_rows * E), @intCast(H) }),
        );
        const classifier_0 = try linearNamed(bld, schema_hidden, "classifier.0.weight", "classifier.0.bias", compact_rows * E, H, H * 2);
        const classifier_act = try bld.relu(classifier_0);
        const classifier_logits_flat = try linearNamed(bld, classifier_act, "classifier.2.weight", "classifier.2.bias", compact_rows * E, H * 2, 1);
        const classifier_logits = try bld.reshape(classifier_logits_flat, Shape.init(.f32, &.{ @intCast(compact_rows), @intCast(E) }));
        self.gliner2_classification_logits = classifier_logits;
        const labels_all = try bld.sliceLastDim(targets, labels_offset, labels_offset + @as(i64, @intCast(E)));
        const labels = try bld.gather(
            labels_all,
            task_row_indices,
            Shape.init(.f32, &.{ @intCast(compact_rows), @intCast(E) }),
        );
        const mask_all = try bld.sliceLastDim(targets, mask_offset, mask_offset + @as(i64, @intCast(E)));
        const mask = try bld.gather(
            mask_all,
            task_row_indices,
            Shape.init(.f32, &.{ @intCast(compact_rows), @intCast(E) }),
        );
        return buildMaskedSpanStartBceLoss(
            bld,
            classifier_logits,
            labels,
            mask,
            null,
            1.0,
            1.0,
            .sum,
        );
    }

    fn buildGliner2CountLoss(
        self: *GlinerAutodiffCtx,
        bld: *Builder,
        hidden: NodeId,
        targets: NodeId,
    ) !NodeId {
        const C: u32 = self.config.num_classes;
        const E: u32 = C - 1;
        const H: u32 = self.config.graph_config.hidden_size;
        const target_shape = bld.graph.node(targets).output_shape;
        const rows: u32 = @intCast(target_shape.dim(0));
        const graph_batch = self.graph_batch;
        if (graph_batch == 0 or rows == 0 or @mod(rows, graph_batch) != 0) return error.InvalidGlinerSpanTargetShape;
        const max_spans_per_sample = rows / graph_batch;
        const schema_tasks = @min(max_spans_per_sample, @max(@as(u32, 1), self.config.max_schema_tasks));
        const compact_rows = graph_batch * schema_tasks;
        const task_row_indices = try buildSchemaTaskRowIndices(bld, graph_batch, max_spans_per_sample, schema_tasks);
        const count_labels_offset: i64 = @intCast(gliner2TotalLossCountLabelsOffset(@intCast(E)));
        const count_mask_offset: i64 = @intCast(gliner2TotalLossCountMaskOffset(@intCast(E)));
        const parent_idx_offset: i64 = @intCast(gliner2TotalLossParentIndexOffset(@intCast(E)));

        const hidden_flat = try flattenHiddenForLoss(bld, hidden, H);
        const parent_idx_all = try bld.sliceLastDim(targets, parent_idx_offset, parent_idx_offset + 1);
        const parent_idx_2d = try bld.gather(
            parent_idx_all,
            task_row_indices,
            Shape.init(.f32, &.{ @intCast(compact_rows), 1 }),
        );
        const parent_idx_f32 = try bld.reshape(parent_idx_2d, Shape.init(.f32, &.{@intCast(compact_rows)}));
        const parent_idx_i64 = try bld.convertDtype(parent_idx_f32, .i64);
        const parent_hidden = try bld.gather(
            hidden_flat,
            parent_idx_i64,
            Shape.init(.f32, &.{ @intCast(compact_rows), @intCast(H) }),
        );
        const count_pred_0 = try linearNamed(bld, parent_hidden, "count_pred.0.weight", "count_pred.0.bias", compact_rows, H, H * 2);
        const count_pred_act = try bld.relu(count_pred_0);
        const count_logits = try linearNamed(bld, count_pred_act, "count_pred.2.weight", "count_pred.2.bias", compact_rows, H * 2, 20);
        self.gliner2_count_logits = count_logits;
        const log_probs = try bld.logSoftmax(count_logits);
        const count_labels_all = try bld.sliceLastDim(targets, count_labels_offset, count_labels_offset + 20);
        const count_labels = try bld.gather(
            count_labels_all,
            task_row_indices,
            Shape.init(.f32, &.{ @intCast(compact_rows), 20 }),
        );
        const count_mask_all = try bld.sliceLastDim(targets, count_mask_offset, count_mask_offset + 1);
        const count_mask = try bld.gather(
            count_mask_all,
            task_row_indices,
            Shape.init(.f32, &.{ @intCast(compact_rows), 1 }),
        );
        const weighted = try bld.mul(count_labels, log_probs);
        const per_row = try bld.reduceSum(weighted, &.{1});
        const masked = try bld.mul(per_row, count_mask);
        return bld.neg(try bld.reduceSum(masked, &.{ 0, 1 }));
    }
};

fn buildSchemaTaskRowIndices(
    bld: *Builder,
    graph_batch: u32,
    max_spans_per_sample: u32,
    schema_tasks: u32,
) !NodeId {
    if (graph_batch == 0 or max_spans_per_sample == 0 or schema_tasks == 0) return error.InvalidGlinerSpanTargetShape;
    if (schema_tasks > max_spans_per_sample) return error.InvalidGlinerSpanTargetShape;
    const rows: usize = @as(usize, graph_batch) * @as(usize, schema_tasks);
    const row_indices = try bld.graph.allocator.alloc(f32, rows);
    defer bld.graph.allocator.free(row_indices);
    for (0..graph_batch) |sample_idx| {
        for (0..schema_tasks) |task_idx| {
            row_indices[@as(usize, sample_idx) * @as(usize, schema_tasks) + task_idx] =
                @floatFromInt(sample_idx * max_spans_per_sample + task_idx);
        }
    }
    const rows_f32 = try bld.tensorConst(row_indices, Shape.init(.f32, &.{@intCast(rows)}));
    return bld.convertDtype(rows_f32, .i64);
}

fn buildSpanSampleChunkRowIndices(
    bld: *Builder,
    sample_start: u32,
    sample_count: u32,
    max_spans_per_sample: u32,
) !NodeId {
    if (sample_count == 0 or max_spans_per_sample == 0) return error.InvalidGlinerSpanTargetShape;
    const rows: usize = @as(usize, sample_count) * @as(usize, max_spans_per_sample);
    const row_indices = try bld.graph.allocator.alloc(f32, rows);
    defer bld.graph.allocator.free(row_indices);
    for (0..sample_count) |local_sample_idx| {
        const global_sample_idx = sample_start + @as(u32, @intCast(local_sample_idx));
        for (0..max_spans_per_sample) |span_idx| {
            row_indices[local_sample_idx * @as(usize, max_spans_per_sample) + span_idx] =
                @floatFromInt(global_sample_idx * max_spans_per_sample + @as(u32, @intCast(span_idx)));
        }
    }
    const rows_f32 = try bld.tensorConst(row_indices, Shape.init(.f32, &.{@intCast(rows)}));
    return bld.convertDtype(rows_f32, .i64);
}

fn buildParitySpanProjection(
    bld: *Builder,
    start_hidden: NodeId,
    end_hidden: NodeId,
    span_rows: u32,
    hidden_size: u32,
) !NodeId {
    const ff_dim: u32 = hidden_size * 4;

    const start_0 = try linearNamed(bld, start_hidden, "span_rep.span_rep_layer.project_start.0.weight", "span_rep.span_rep_layer.project_start.0.bias", span_rows, hidden_size, ff_dim);
    const start_act = try bld.relu(start_0);
    const start_out = try linearNamed(bld, start_act, "span_rep.span_rep_layer.project_start.3.weight", "span_rep.span_rep_layer.project_start.3.bias", span_rows, ff_dim, hidden_size);

    const end_0 = try linearNamed(bld, end_hidden, "span_rep.span_rep_layer.project_end.0.weight", "span_rep.span_rep_layer.project_end.0.bias", span_rows, hidden_size, ff_dim);
    const end_act = try bld.relu(end_0);
    const end_out = try linearNamed(bld, end_act, "span_rep.span_rep_layer.project_end.3.weight", "span_rep.span_rep_layer.project_end.3.bias", span_rows, ff_dim, hidden_size);

    const joined = try bld.concat(start_out, end_out, 1);
    const joined_relu = try bld.relu(joined);
    const out_0 = try linearNamed(bld, joined_relu, "span_rep.span_rep_layer.out_project.0.weight", "span_rep.span_rep_layer.out_project.0.bias", span_rows, hidden_size * 2, ff_dim);
    const out_act = try bld.relu(out_0);
    return linearNamed(bld, out_act, "span_rep.span_rep_layer.out_project.3.weight", "span_rep.span_rep_layer.out_project.3.bias", span_rows, ff_dim, hidden_size);
}

fn buildParityAuxiliaryHeads(
    bld: *Builder,
    hidden: NodeId,
    rows: u32,
    hidden_size: u32,
) !NodeId {
    const classifier_mid: u32 = hidden_size * 2;
    const classifier_0 = try linearNamed(bld, hidden, "classifier.0.weight", "classifier.0.bias", rows, hidden_size, classifier_mid);
    const classifier_act = try bld.relu(classifier_0);
    const classifier_scalar = try linearNamed(bld, classifier_act, "classifier.2.weight", "classifier.2.bias", rows, classifier_mid, 1);

    const count_pred_0 = try linearNamed(bld, hidden, "count_pred.0.weight", "count_pred.0.bias", rows, hidden_size, classifier_mid);
    const count_pred_act = try bld.relu(count_pred_0);
    const count_pred = try linearNamed(bld, count_pred_act, "count_pred.2.weight", "count_pred.2.bias", rows, classifier_mid, 20);
    const count_pred_sum = try bld.reduceSum(count_pred, &.{ 0, 1 });

    const count_embed = try buildParityCountEmbed(bld, hidden, null, null, rows, rows, hidden_size, null);
    const count_embed_sum = try bld.reduceSum(count_embed, &.{ 0, 1 });
    const classifier_sum = try bld.reduceSum(classifier_scalar, &.{ 0, 1 });
    const aux_sum = try bld.add(try bld.add(classifier_sum, count_pred_sum), count_embed_sum);
    const zero = try bld.scalarConst(.f32, 0.0);
    return bld.mul(aux_sum, zero);
}

fn flattenHiddenForLoss(bld: *Builder, hidden: NodeId, hidden_size: u32) !NodeId {
    const hidden_shape = bld.graph.node(hidden).output_shape;
    if (hidden_shape.rank() == 2) return hidden;
    if (hidden_shape.rank() != 3) return error.InvalidGlinerHiddenShape;
    const rows: i64 = hidden_shape.dim(0) * hidden_shape.dim(1);
    return bld.reshape(hidden, Shape.init(.f32, &.{ rows, @intCast(hidden_size) }));
}

fn broadcastInDim(
    bld: *Builder,
    input: NodeId,
    target_shape: Shape,
    axes: []const u8,
) !NodeId {
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = target_shape };
    if (axes.len > attrs.broadcast_axes.len) return error.InvalidTensorShape;
    for (axes, 0..) |axis, idx| attrs.broadcast_axes[idx] = axis;
    attrs.num_axes = @intCast(axes.len);
    return bld.graph.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = target_shape,
        .inputs = .{ input, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
}

fn buildSchemaObjectiveAuxiliaryZeroScalar(
    bld: *Builder,
    hidden: NodeId,
    rows: u32,
    hidden_size: u32,
    num_classes: u32,
) !NodeId {
    const classifier_mid: u32 = hidden_size * 2;
    const task_head_w = try bld.parameter(
        "task_classifier.weight",
        Shape.init(.f32, &.{ @intCast(num_classes), @intCast(hidden_size) }),
    );
    const task_head_b = try bld.parameter(
        "task_classifier.bias",
        Shape.init(.f32, &.{@intCast(num_classes)}),
    );
    const task_head = try bld.linear(hidden, task_head_w, task_head_b, rows, hidden_size, num_classes);

    const classifier_0 = try linearNamed(bld, hidden, "classifier.0.weight", "classifier.0.bias", rows, hidden_size, classifier_mid);
    const classifier_act = try bld.relu(classifier_0);
    const classifier_scalar = try linearNamed(bld, classifier_act, "classifier.2.weight", "classifier.2.bias", rows, classifier_mid, 1);

    const count_pred_0 = try linearNamed(bld, hidden, "count_pred.0.weight", "count_pred.0.bias", rows, hidden_size, classifier_mid);
    const count_pred_act = try bld.relu(count_pred_0);
    const count_pred = try linearNamed(bld, count_pred_act, "count_pred.2.weight", "count_pred.2.bias", rows, classifier_mid, 20);

    const task_head_sum = try bld.reduceSum(task_head, &.{ 0, 1 });
    const classifier_sum = try bld.reduceSum(classifier_scalar, &.{ 0, 1 });
    const count_pred_sum = try bld.reduceSum(count_pred, &.{ 0, 1 });
    const aux_sum = try bld.add(try bld.add(task_head_sum, classifier_sum), count_pred_sum);
    const zero = try bld.scalarConst(.f32, 0.0);
    return bld.mul(aux_sum, zero);
}

const CountEmbedDebugNodes = struct {
    gru_state: ?NodeId = null,
    count_state: ?NodeId = null,
    in_project: ?NodeId = null,
    layer0: ?NodeId = null,
    layer1: ?NodeId = null,
    out0: ?NodeId = null,
    out2: ?NodeId = null,
};

fn buildParityCountEmbed(
    bld: *Builder,
    hidden: NodeId,
    count_state_indices: ?NodeId,
    active_schema_mask: ?NodeId,
    rows: u32,
    attention_seq_len: u32,
    hidden_size: u32,
    debug_nodes: ?*CountEmbedDebugNodes,
) !NodeId {
    const gru_state = try buildCountLstmV2UnrolledState(bld, hidden, count_state_indices, rows, hidden_size);
    const count_state = try bld.add(gru_state, hidden);
    if (debug_nodes) |nodes| {
        nodes.gru_state = gru_state;
        nodes.count_state = count_state;
    }
    return buildCountEmbedProjectionHead(bld, count_state, active_schema_mask, null, rows, attention_seq_len, hidden_size, debug_nodes);
}

/// The shared `count_embed.transformer` projection head: in_projector →
/// 2-layer DownscaledTransformer (attention over `attention_seq_len` fields) →
/// concat(transformer_out, count_state) → out_projector. `count_state`
/// (= GRU output + pc_emb) is `[rows, hidden_size]`; the result is the
/// per-row projection `[rows, hidden_size]`. Used by both the legacy
/// single-instance path and the per-instance sequence path.
fn buildCountEmbedProjectionHead(
    bld: *Builder,
    count_state: NodeId,
    active_schema_mask: ?NodeId,
    task_group_ids: ?NodeId,
    rows: u32,
    attention_seq_len: u32,
    hidden_size: u32,
    debug_nodes: ?*CountEmbedDebugNodes,
) !NodeId {
    const count_dim: u32 = 128;
    const count_ff: u32 = 256;
    const in_project = try linearNamed(bld, count_state, "count_embed.transformer.in_projector.weight", "count_embed.transformer.in_projector.bias", rows, hidden_size, count_dim);

    const layer0_out = try buildCountTransformerLayer(bld, in_project, active_schema_mask, task_group_ids, rows, attention_seq_len, count_dim, count_ff, 0);
    const layer1_out = try buildCountTransformerLayer(bld, layer0_out, active_schema_mask, task_group_ids, rows, attention_seq_len, count_dim, count_ff, 1);

    const transformer_joined = try bld.concat(layer1_out, count_state, 1);
    const out0 = try bld.relu(try linearNamed(bld, transformer_joined, "count_embed.transformer.out_projector.0.weight", "count_embed.transformer.out_projector.0.bias", rows, count_dim + hidden_size, hidden_size));
    const out2 = try bld.relu(try linearNamed(bld, out0, "count_embed.transformer.out_projector.2.weight", "count_embed.transformer.out_projector.2.bias", rows, hidden_size, hidden_size));
    if (debug_nodes) |nodes| {
        nodes.in_project = in_project;
        nodes.layer0 = layer0_out;
        nodes.layer1 = layer1_out;
        nodes.out0 = out0;
        nodes.out2 = out2;
    }
    return linearNamed(bld, out2, "count_embed.transformer.out_projector.4.weight", "count_embed.transformer.out_projector.4.bias", rows, hidden_size, hidden_size);
}

/// Per-instance count-conditioned schema projection matching upstream
/// `CountLSTMv2.forward(schema_emb[1:], gold_count)` for `gold_count = max_gold`
/// instances. Produces `[graph_batch * max_gold * E, H]` ordered
/// `[sample, instance, field]`: each instance `i` uses GRU state after pos
/// steps `0..i` (the learned `count_embed.pos_embedding` row sequence) with
/// `h0 = schema_hidden` (the field embeddings = pc_emb), then `+ pc_emb` and
/// the shared transformer head. `schema_hidden` is `[graph_batch * E, H]`.
fn buildParityCountEmbedSequence(
    bld: *Builder,
    schema_hidden: NodeId,
    active_schema_mask: ?NodeId,
    task_group_ids: NodeId,
    graph_batch: u32,
    field_count: u32,
    hidden_size: u32,
    max_gold: u32,
) !NodeId {
    const rows = graph_batch * field_count;
    const out_rows = graph_batch * max_gold * field_count;

    // GRU sequence: state_concat[step, sample, field] = output[step] = hidden
    // after pos steps 0..step starting from pc_emb. Shape [max_gold*rows, H].
    const seq = try buildCountGruSequenceStates(bld, schema_hidden, rows, hidden_size, max_gold);
    const seq_4d = try bld.reshape(seq, Shape.init(.f32, &.{ @intCast(max_gold), @intCast(graph_batch), @intCast(field_count), @intCast(hidden_size) }));
    // → [sample, instance, field, H]
    const seq_t = try bld.transpose(seq_4d, &.{ 1, 0, 2, 3 });
    const output_seq = try bld.reshape(seq_t, Shape.init(.f32, &.{ @intCast(out_rows), @intCast(hidden_size) }));

    // pc_emb broadcast over the instance axis: [graph_batch, field, H] →
    // [graph_batch, max_gold, field, H].
    const pc_3d = try bld.reshape(schema_hidden, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(field_count), @intCast(hidden_size) }));
    const pc_b = try broadcastInDim(
        bld,
        pc_3d,
        Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(max_gold), @intCast(field_count), @intCast(hidden_size) }),
        &.{ 0, 2, 3 },
    );
    const pc_flat = try bld.reshape(pc_b, Shape.init(.f32, &.{ @intCast(out_rows), @intCast(hidden_size) }));
    const count_state = try bld.add(output_seq, pc_flat);

    const mask_b: ?NodeId = if (active_schema_mask) |m| blk: {
        const m_2d = try bld.reshape(m, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(field_count) }));
        const m_b = try broadcastInDim(
            bld,
            m_2d,
            Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(max_gold), @intCast(field_count) }),
            &.{ 0, 2 },
        );
        break :blk try bld.reshape(m_b, Shape.init(.f32, &.{@intCast(out_rows)}));
    } else null;

    const groups_2d = try bld.reshape(task_group_ids, Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(field_count) }));
    const groups_b = try broadcastInDim(
        bld,
        groups_2d,
        Shape.init(.f32, &.{ @intCast(graph_batch), @intCast(max_gold), @intCast(field_count) }),
        &.{ 0, 2 },
    );
    const groups_flat = try bld.reshape(groups_b, Shape.init(.f32, &.{@intCast(out_rows)}));

    return buildCountEmbedProjectionHead(bld, count_state, mask_b, groups_flat, out_rows, field_count, hidden_size, null);
}

/// GRU sequence states for the per-instance count embed: returns the first
/// `num_steps` hidden states concatenated along axis 0 as `[num_steps*rows, H]`
/// with layout `[step, row]`. `step` = instance index, `row` = (sample, field).
fn buildCountGruSequenceStates(
    bld: *Builder,
    hidden: NodeId,
    rows: u32,
    hidden_size: u32,
    num_steps: u32,
) !NodeId {
    const pos_weight = try bld.parameter(
        "count_embed.pos_embedding.weight",
        Shape.init(.f32, &.{ 20, @intCast(hidden_size) }),
    );
    var state = hidden;
    var state_concat: NodeId = undefined;
    var step: u32 = 0;
    while (step < num_steps) : (step += 1) {
        const pos_rows = try buildCountPositionRows(bld, pos_weight, @intCast(step), hidden, rows, hidden_size);
        state = try buildCountGruStep(bld, pos_rows, state, rows, hidden_size);
        if (step == 0) {
            state_concat = state;
        } else {
            state_concat = try bld.concat(state_concat, state, 0);
        }
    }
    return state_concat;
}

fn buildCountTransformerLayer(
    bld: *Builder,
    input: NodeId,
    active_schema_mask: ?NodeId,
    task_group_ids: ?NodeId,
    rows: u32,
    attention_seq_len: u32,
    hidden_size: u32,
    ff_size: u32,
    comptime layer: usize,
) !NodeId {
    const prefix = comptime std.fmt.comptimePrint("count_embed.transformer.transformer.layers.{d}", .{layer});
    const attn_out = try buildCountSelfAttention(bld, input, active_schema_mask, task_group_ids, rows, attention_seq_len, hidden_size, layer);
    const attn_resid = try bld.add(input, attn_out);
    const norm1 = try layerNormNamed(
        bld,
        attn_resid,
        prefix ++ ".norm1.weight",
        prefix ++ ".norm1.bias",
        hidden_size,
    );
    const ff1 = try bld.relu(try linearNamed(
        bld,
        norm1,
        prefix ++ ".linear1.weight",
        prefix ++ ".linear1.bias",
        rows,
        hidden_size,
        ff_size,
    ));
    const ff2 = try linearNamed(
        bld,
        ff1,
        prefix ++ ".linear2.weight",
        prefix ++ ".linear2.bias",
        rows,
        ff_size,
        hidden_size,
    );
    const ff_resid = try bld.add(norm1, ff2);
    return layerNormNamed(
        bld,
        ff_resid,
        prefix ++ ".norm2.weight",
        prefix ++ ".norm2.bias",
        hidden_size,
    );
}

fn buildCountSelfAttention(
    bld: *Builder,
    input: NodeId,
    active_schema_mask: ?NodeId,
    task_group_ids: ?NodeId,
    rows: u32,
    attention_seq_len: u32,
    hidden_size: u32,
    comptime layer: usize,
) !NodeId {
    const prefix = comptime std.fmt.comptimePrint("count_embed.transformer.transformer.layers.{d}.self_attn", .{layer});
    const qkv = try linearNamed(
        bld,
        input,
        prefix ++ ".in_proj_weight",
        prefix ++ ".in_proj_bias",
        rows,
        hidden_size,
        hidden_size * 3,
    );
    const q = try bld.sliceLastDim(qkv, 0, @intCast(hidden_size));
    const k = try bld.sliceLastDim(qkv, @intCast(hidden_size), @intCast(hidden_size * 2));
    const v = try bld.sliceLastDim(qkv, @intCast(hidden_size * 2), @intCast(hidden_size * 3));
    const context = try buildCountScaledDotProductAttention(bld, q, k, v, active_schema_mask, task_group_ids, rows, attention_seq_len, hidden_size, 4);
    return linearNamed(
        bld,
        context,
        prefix ++ ".out_proj.weight",
        prefix ++ ".out_proj.bias",
        rows,
        hidden_size,
        hidden_size,
    );
}

fn buildCountScaledDotProductAttention(
    bld: *Builder,
    q: NodeId,
    k: NodeId,
    v: NodeId,
    active_schema_mask: ?NodeId,
    task_group_ids: ?NodeId,
    rows: u32,
    attention_seq_len: u32,
    hidden_size: u32,
    num_heads: u32,
) !NodeId {
    if (attention_seq_len == 0 or @mod(rows, attention_seq_len) != 0) return error.InvalidGlinerSpanTargetShape;
    const batch = rows / attention_seq_len;
    const head_dim = hidden_size / num_heads;
    const q_bsnh = try bld.reshape(q, Shape.init(.f32, &.{ @intCast(batch), @intCast(attention_seq_len), @intCast(num_heads), @intCast(head_dim) }));
    const k_bsnh = try bld.reshape(k, Shape.init(.f32, &.{ @intCast(batch), @intCast(attention_seq_len), @intCast(num_heads), @intCast(head_dim) }));
    const v_bsnh = try bld.reshape(v, Shape.init(.f32, &.{ @intCast(batch), @intCast(attention_seq_len), @intCast(num_heads), @intCast(head_dim) }));
    const q_bnsh = try bld.transpose(q_bsnh, &.{ 0, 2, 1, 3 });
    const k_bnsh = try bld.transpose(k_bsnh, &.{ 0, 2, 1, 3 });
    const v_bnsh = try bld.transpose(v_bsnh, &.{ 0, 2, 1, 3 });
    const q_bhsd = try bld.reshape(q_bnsh, Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(attention_seq_len), @intCast(head_dim) }));
    const k_bhsd = try bld.reshape(k_bnsh, Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(attention_seq_len), @intCast(head_dim) }));
    const v_bhsd = try bld.reshape(v_bnsh, Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(attention_seq_len), @intCast(head_dim) }));
    const k_t = try bld.transpose(k_bhsd, &.{ 0, 2, 1 });
    const scores = try bld.matmul3D(q_bhsd, k_t);
    const scale = try bld.scalarConst(.f32, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
    var scaled_scores = try bld.mul(scores, scale);
    var allowed_keys: ?NodeId = null;
    if (active_schema_mask) |mask| {
        const mask_be = try bld.reshape(mask, Shape.init(.f32, &.{ @intCast(batch), @intCast(attention_seq_len) }));
        const mask_bhss = try broadcastInDim(
            bld,
            mask_be,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(attention_seq_len), @intCast(attention_seq_len) }),
            &.{ 0, 3 },
        );
        allowed_keys = try bld.reshape(mask_bhss, Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(attention_seq_len), @intCast(attention_seq_len) }));
    }
    if (task_group_ids) |groups| {
        const groups_be = try bld.reshape(groups, Shape.init(.f32, &.{ @intCast(batch), @intCast(attention_seq_len) }));
        const query_groups = try broadcastInDim(
            bld,
            groups_be,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(attention_seq_len), @intCast(attention_seq_len) }),
            &.{ 0, 2 },
        );
        const key_groups = try broadcastInDim(
            bld,
            groups_be,
            Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(attention_seq_len), @intCast(attention_seq_len) }),
            &.{ 0, 3 },
        );
        const group_delta = try bld.absOp(try bld.sub(query_groups, key_groups));
        const one = try bld.scalarConst(.f32, 1.0);
        const same_group = try bld.relu(try bld.sub(one, group_delta));
        const same_group_bhs = try bld.reshape(same_group, Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(attention_seq_len), @intCast(attention_seq_len) }));
        allowed_keys = if (allowed_keys) |active| try bld.mul(active, same_group_bhs) else same_group_bhs;
    }
    if (allowed_keys) |allowed| {
        const one = try bld.scalarConst(.f32, 1.0);
        const inactive = try bld.sub(one, allowed);
        const neg_large = try bld.scalarConst(.f32, -1.0e9);
        const key_bias = try bld.mul(inactive, neg_large);
        scaled_scores = try bld.add(scaled_scores, key_bias);
    }
    const probs = try bld.softmax(scaled_scores);
    const context_bhsd = try bld.matmul3D(probs, v_bhsd);
    const context_bnsh = try bld.reshape(context_bhsd, Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(attention_seq_len), @intCast(head_dim) }));
    const context_bsnh = try bld.transpose(context_bnsh, &.{ 0, 2, 1, 3 });
    return bld.reshape(context_bsnh, Shape.init(.f32, &.{ @intCast(rows), @intCast(hidden_size) }));
}

fn buildCountLstmV2UnrolledState(
    bld: *Builder,
    hidden: NodeId,
    count_state_indices: ?NodeId,
    rows: u32,
    hidden_size: u32,
) !NodeId {
    const pos_weight = try bld.parameter(
        "count_embed.pos_embedding.weight",
        Shape.init(.f32, &.{ 20, @intCast(hidden_size) }),
    );
    var state = hidden;
    var state_concat: NodeId = undefined;
    for (0..20) |step| {
        const pos_rows = try buildCountPositionRows(bld, pos_weight, @intCast(step), hidden, rows, hidden_size);
        state = try buildCountGruStep(bld, pos_rows, state, rows, hidden_size);
        if (step == 0) {
            state_concat = state;
        } else {
            state_concat = try bld.concat(state_concat, state, 0);
        }
    }

    if (count_state_indices) |indices| {
        return bld.gather(
            state_concat,
            indices,
            Shape.init(.f32, &.{ @intCast(rows), @intCast(hidden_size) }),
        );
    }
    return state;
}

fn buildCountPositionRows(
    bld: *Builder,
    pos_weight: NodeId,
    step: i64,
    hidden: NodeId,
    rows: u32,
    hidden_size: u32,
) !NodeId {
    const step_idx = try bld.tensorConstBytes(std.mem.asBytes(&step), Shape.init(.i64, &.{1}));
    const pos = try bld.gather(
        pos_weight,
        step_idx,
        Shape.init(.f32, &.{ 1, @intCast(hidden_size) }),
    );
    _ = rows;
    return bld.add(try bld.mul(hidden, try bld.scalarConst(.f32, 0.0)), pos);
}

fn buildCountGruStep(
    bld: *Builder,
    pos_rows: NodeId,
    hidden_state: NodeId,
    rows: u32,
    hidden_size: u32,
) !NodeId {
    const gi = try linearNamed(bld, pos_rows, "count_embed.gru.weight_ih_l0", "count_embed.gru.bias_ih_l0", rows, hidden_size, hidden_size * 3);
    const gh = try linearNamed(bld, hidden_state, "count_embed.gru.weight_hh_l0", "count_embed.gru.bias_hh_l0", rows, hidden_size, hidden_size * 3);

    const i_r = try bld.sliceLastDim(gi, 0, @intCast(hidden_size));
    const i_z = try bld.sliceLastDim(gi, @intCast(hidden_size), @intCast(hidden_size * 2));
    const i_n = try bld.sliceLastDim(gi, @intCast(hidden_size * 2), @intCast(hidden_size * 3));
    const h_r = try bld.sliceLastDim(gh, 0, @intCast(hidden_size));
    const h_z = try bld.sliceLastDim(gh, @intCast(hidden_size), @intCast(hidden_size * 2));
    const h_n = try bld.sliceLastDim(gh, @intCast(hidden_size * 2), @intCast(hidden_size * 3));

    const r = try bld.sigmoid(try bld.add(i_r, h_r));
    const z = try bld.sigmoid(try bld.add(i_z, h_z));
    const n = try bld.tanhOp(try bld.add(i_n, try bld.mul(r, h_n)));
    const one = try bld.scalarConst(.f32, 1.0);
    const one_minus_z = try bld.sub(one, z);
    return bld.add(try bld.mul(one_minus_z, n), try bld.mul(z, hidden_state));
}

fn layerNormNamed(
    bld: *Builder,
    input: NodeId,
    weight_name: []const u8,
    bias_name: []const u8,
    dim: u32,
) !NodeId {
    const gamma = try bld.parameter(weight_name, Shape.init(.f32, &.{@intCast(dim)}));
    const beta = try bld.parameter(bias_name, Shape.init(.f32, &.{@intCast(dim)}));
    return bld.layerNorm(input, gamma, beta, dim, 1e-5);
}

fn linearNamed(
    bld: *Builder,
    input: NodeId,
    weight_name: []const u8,
    bias_name: []const u8,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
) !NodeId {
    const w = try bld.parameter(
        weight_name,
        Shape.init(.f32, &.{ @intCast(out_dim), @intCast(in_dim) }),
    );
    const b = try bld.parameter(
        bias_name,
        Shape.init(.f32, &.{@intCast(out_dim)}),
    );
    return bld.linear(input, w, b, rows, in_dim, out_dim);
}

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
    label_positive_weights: ?NodeId,
    positive_weight: f32,
    negative_weight: f32,
    reduction: SpanStartLossReduction,
) !NodeId {
    const in_shape = bld.graph.node(logits).output_shape;
    const rank = in_shape.rank();
    var all_axes: [8]u8 = undefined;
    for (0..rank) |i| all_axes[i] = @intCast(i);

    const probs = try bld.sigmoid(logits);
    const diff = try bld.sub(probs, labels);
    const pos_weight = label_positive_weights orelse try bld.scalarConst(.f32, positive_weight);
    const neg_weight = try bld.scalarConst(.f32, negative_weight);
    const one = try bld.scalarConst(.f32, 1.0);
    const pos_term = try bld.mul(labels, pos_weight);
    const inverse_labels = try bld.sub(one, labels);
    const neg_term = try bld.mul(inverse_labels, neg_weight);
    const label_weights = try bld.add(pos_term, neg_term);
    const weighted_mask = try bld.mul(mask, label_weights);
    const sq = try bld.mul(diff, diff);
    const weighted_sq = try bld.mul(sq, weighted_mask);
    const numerator = try bld.reduceSum(weighted_sq, all_axes[0..rank]);
    if (reduction == .sum) return numerator;
    const denom = try bld.reduceSum(weighted_mask, all_axes[0..rank]);
    const eps = try bld.scalarConst(.f32, 1e-12);
    const denom_safe = try bld.add(denom, eps);
    return bld.div(numerator, denom_safe);
}

fn buildMaskedSpanStartBceLoss(
    bld: *Builder,
    logits: NodeId,
    labels: NodeId,
    mask: NodeId,
    label_positive_weights: ?NodeId,
    positive_weight: f32,
    negative_weight: f32,
    reduction: SpanStartLossReduction,
) !NodeId {
    if (label_positive_weights == null) {
        return bld.maskedBceWithLogitsLoss(logits, labels, mask, .{
            .positive_weight = positive_weight,
            .negative_weight = negative_weight,
            .eps = 1e-6,
            .reduction = if (reduction == .sum) .sum else .mean,
        });
    }

    const in_shape = bld.graph.node(logits).output_shape;
    const rank = in_shape.rank();
    var all_axes: [8]u8 = undefined;
    for (0..rank) |i| all_axes[i] = @intCast(i);

    const eps = try bld.scalarConst(.f32, 1e-6);
    const one = try bld.scalarConst(.f32, 1.0);
    const pos_weight = label_positive_weights orelse try bld.scalarConst(.f32, positive_weight);
    const neg_weight = try bld.scalarConst(.f32, negative_weight);

    // Split the mask into a 0/1 validity gate and the full loss weight. `validity`
    // = min(mask, 1) built as 1 - relu(1 - mask): it zeros masked positions without
    // scaling the logit by the (possibly >1) weight, so BCE runs on the true logit
    // while the full `mask` still weights the loss + denom below. For a 0/1 mask
    // validity == mask, so this is identical to the previous form (parity-safe);
    // for a hard-negative weight (mask>1) it weights the loss instead of the logit.
    const validity = try bld.sub(one, try bld.relu(try bld.sub(one, mask)));
    const safe_logits = try bld.mul(logits, validity);
    const safe_labels = try bld.mul(labels, validity);
    const relu_logits = try bld.relu(safe_logits);
    const labels_logits = try bld.mul(safe_labels, safe_logits);
    const abs_logits = try bld.absOp(safe_logits);
    const neg_abs_logits = try bld.neg(abs_logits);
    const exp_neg_abs = try bld.expOp(neg_abs_logits);
    const log_term = try bld.logOp(try bld.add(one, exp_neg_abs));
    const bce = try bld.add(try bld.sub(relu_logits, labels_logits), log_term);
    const inverse_labels = try bld.sub(one, safe_labels);
    const pos_term = try bld.mul(safe_labels, pos_weight);
    const neg_term = try bld.mul(inverse_labels, neg_weight);
    const label_weights = try bld.add(pos_term, neg_term);
    const weighted_loss = try bld.mul(bce, label_weights);
    const masked_loss = try bld.mul(weighted_loss, mask);
    const numerator = try bld.reduceSum(masked_loss, all_axes[0..rank]);
    if (reduction == .sum) return numerator;

    const weighted_mask = try bld.mul(mask, label_weights);
    const denom = try bld.reduceSum(weighted_mask, all_axes[0..rank]);
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
        .capture_graph_context = &GlinerAutodiffCtx.captureGraphContext,
        .restore_graph_context = &GlinerAutodiffCtx.restoreGraphContext,
        .deinit_graph_context = &GlinerAutodiffCtx.deinitGraphContext,
        .graph_variant = ctx.graphVariant(),
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
    var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| {
            if (!borrowed_rt.contains(entry.key_ptr.*)) trainer.compute_backend.free(entry.value_ptr.*);
        }
        borrowed_rt.deinit(allocator);
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

    try rt.put(allocator, gs.input_ids_node, try bindEvalInputIds(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, targets));

    try bindEvalTrainables(allocator, trainer, &rt, &borrowed_rt);
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }
    try bindEvalLoraDropoutMasks(allocator, trainer, gs, &rt);

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

    return executeEvalGraphOutputToFloat32(allocator, trainer, &gs.graph, rt_inputs.items);
}

pub fn gliner2ClassificationLogitsForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) ![]f32 {
    if (ctx.config.objective != .gliner2_total_loss) return error.InvalidGlinerObjective;
    const trainer_input = makeTrainerInput(
        ctx,
        input_ids,
        attention_mask,
        targets,
        targets_shape,
        batch,
        seq_len,
    );
    try trainer.ensureGraphBuilt(trainer_input);
    const logits_node = ctx.gliner2_classification_logits orelse return error.MissingGliner2ClassificationLossNode;
    return evalSpanStartNodeForBatch(
        allocator,
        trainer,
        ctx,
        input_ids,
        attention_mask,
        targets,
        targets_shape,
        batch,
        seq_len,
        logits_node,
    );
}

pub const Gliner2EvalLogits = struct {
    span: []f32,
    classification: []f32,
    count: []f32,

    pub fn deinit(self: *Gliner2EvalLogits, allocator: std.mem.Allocator) void {
        allocator.free(self.span);
        allocator.free(self.classification);
        allocator.free(self.count);
        self.* = undefined;
    }
};

/// Execute the shared encoder once and return every logit family needed by
/// native held-out decoding. Calling the three single-output accessors would
/// repeat the full DeBERTa forward pass for the same record.
pub fn gliner2EvalLogitsForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) !Gliner2EvalLogits {
    if (ctx.config.objective != .gliner2_total_loss) return error.InvalidGlinerObjective;
    const trainer_input = makeTrainerInput(ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len);
    try trainer.ensureGraphBuilt(trainer_input);
    var gs = &trainer.graph_state.?;
    const output_nodes = [_]NodeId{
        ctx.span_logits orelse return error.MissingGlinerSpanLogitsNode,
        ctx.gliner2_classification_logits orelse return error.MissingGliner2ClassificationLogitsNode,
        ctx.gliner2_count_logits orelse return error.MissingGliner2CountLogitsNode,
    };

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| {
            if (!borrowed_rt.contains(entry.key_ptr.*)) trainer.compute_backend.free(entry.value_ptr.*);
        }
        borrowed_rt.deinit(allocator);
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
    try rt.put(allocator, gs.input_ids_node, try bindEvalInputIds(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, targets));
    try bindEvalTrainables(allocator, trainer, &rt, &borrowed_rt);
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }
    try bindEvalLoraDropoutMasks(allocator, trainer, gs, &rt);

    const saved_outputs = try allocator.dupe(NodeId, gs.graph.outputs.items);
    defer {
        gs.graph.outputs.clearRetainingCapacity();
        for (saved_outputs) |node_id| gs.graph.outputs.append(allocator, node_id) catch {};
        allocator.free(saved_outputs);
    }
    gs.graph.outputs.clearRetainingCapacity();
    for (output_nodes) |node_id| try gs.graph.markOutput(node_id);

    var rt_inputs = std.ArrayList(interpreter.RuntimeInput).empty;
    defer rt_inputs.deinit(allocator);
    var it = rt.iterator();
    while (it.next()) |entry| {
        try rt_inputs.append(allocator, .{ .node_id = entry.key_ptr.*, .value = entry.value_ptr.* });
    }

    const strategy: graph_runtime.Strategy = if (evalUsesEagerDeviceInterpreter(trainer.compute_backend.kind()))
        .interpreter
    else
        try evalRuntimeStrategy(trainer);
    var runtime = try graph_runtime.Runtime.init(allocator, &gs.graph, trainer.compute_backend, strategy);
    defer runtime.deinit();
    var exec_result = try runtime.execute(allocator, &gs.graph, .{ .runtime_inputs = rt_inputs.items });
    defer exec_result.deinit(&runtime);
    if (exec_result.outputs.len != output_nodes.len) return error.InvalidEvalOutputArity;

    const span = try trainer.compute_backend.toFloat32(exec_result.outputs[0], allocator);
    errdefer allocator.free(span);
    const classification = try trainer.compute_backend.toFloat32(exec_result.outputs[1], allocator);
    errdefer allocator.free(classification);
    const count = try trainer.compute_backend.toFloat32(exec_result.outputs[2], allocator);
    return .{ .span = span, .classification = classification, .count = count };
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
    if (ctx.config.objective != .span_start and ctx.config.objective != .gliner2_total_loss) return error.InvalidGlinerObjective;

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
    var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| {
            if (!borrowed_rt.contains(entry.key_ptr.*)) trainer.compute_backend.free(entry.value_ptr.*);
        }
        borrowed_rt.deinit(allocator);
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

    try rt.put(allocator, gs.input_ids_node, try bindEvalInputIds(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, span_targets));

    try bindEvalTrainables(allocator, trainer, &rt, &borrowed_rt);
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }
    try bindEvalLoraDropoutMasks(allocator, trainer, gs, &rt);

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

    return executeEvalGraphOutputToFloat32(allocator, trainer, &gs.graph, rt_inputs.items);
}

pub const SpanStartComponentDebug = struct {
    positive_row: usize,
    positive_entity: usize,
    schema_row: usize,
    start_hidden_norm: f64,
    end_hidden_norm: f64,
    projected_span_norm: f64,
    schema_hidden_norm: f64,
    count_gru_state_norm: f64,
    count_state_norm: f64,
    count_in_project_norm: f64,
    count_layer0_norm: f64,
    count_layer1_norm: f64,
    count_out0_norm: f64,
    count_out2_norm: f64,
    schema_projection_norm: f64,
    projected_schema_dot: f64,
    projected_span_mean: f64,
    count_gru_state_mean: f64,
    count_state_mean: f64,
    count_in_project_mean: f64,
    count_layer0_mean: f64,
    count_layer1_mean: f64,
    count_out0_mean: f64,
    count_out2_mean: f64,
    schema_projection_mean: f64,
    negative_row: usize,
    negative_entity: usize,
    negative_logit: f64,
    negative_start_hidden_norm: f64,
    negative_end_hidden_norm: f64,
    negative_schema_hidden_norm: f64,
    negative_projected_span_norm: f64,
    negative_schema_projection_norm: f64,
    negative_projected_schema_dot: f64,
    negative_start_hidden_mean: f64,
    negative_end_hidden_mean: f64,
    negative_schema_hidden_mean: f64,
    negative_projected_span_mean: f64,
    negative_schema_projection_mean: f64,
};

pub const Gliner2TotalLossComponentDebug = struct {
    classification_loss: f64,
    structure_loss: f64,
    count_loss: f64,
    total_loss: f64,
};

pub fn gliner2TotalLossComponentDebugForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) !Gliner2TotalLossComponentDebug {
    if (ctx.config.objective != .gliner2_total_loss) return error.InvalidGlinerObjective;
    if (ctx.config.num_classes < 2) return error.InvalidGlinerClassCount;
    if (targets_shape.rank() != 2) return error.InvalidGlinerSpanTargetShape;
    const rows: usize = @intCast(targets_shape.dim(0));
    const target_width: usize = @intCast(targets_shape.dim(1));
    if (targets.len != rows * target_width) return error.InvalidGlinerSpanTargetShape;
    const E: usize = @intCast(ctx.config.num_classes - 1);
    const expected_width = gliner2TotalLossTargetWidthEx(E, ctx.config.structure_max_instances);
    if (target_width != expected_width) return error.InvalidGlinerSpanTargetShape;

    const structure_mask_mass = sumPackedTargetColumns(targets, rows, target_width, E, E);
    const classification_mask_mass = sumPackedTargetColumns(
        targets,
        rows,
        target_width,
        gliner2TotalLossClassificationMaskOffset(E),
        E,
    );
    const count_mask_mass = sumPackedTargetColumns(
        targets,
        rows,
        target_width,
        gliner2TotalLossCountMaskOffset(E),
        1,
    );

    const trainer_input = makeTrainerInput(
        ctx,
        input_ids,
        attention_mask,
        targets,
        targets_shape,
        batch,
        seq_len,
    );
    try trainer.ensureGraphBuilt(trainer_input);

    const structure_node = ctx.gliner2_structure_loss orelse return error.MissingGliner2StructureLossNode;
    const classification_node = ctx.gliner2_classification_loss orelse return error.MissingGliner2ClassificationLossNode;
    const count_node = ctx.gliner2_count_loss orelse return error.MissingGliner2CountLossNode;
    const structure = if (structure_mask_mass > 0.0)
        try evalScalarNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len, structure_node)
    else
        0.0;
    const classification = if (classification_mask_mass > 0.0)
        try evalScalarNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len, classification_node)
    else
        0.0;
    const count = if (count_mask_mass > 0.0)
        try evalScalarNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len, count_node)
    else
        0.0;
    return .{
        .classification_loss = classification,
        .structure_loss = structure,
        .count_loss = count,
        .total_loss = classification + structure + count,
    };
}

fn sumPackedTargetColumns(targets: []const f32, rows: usize, width: usize, start: usize, count: usize) f64 {
    var total: f64 = 0.0;
    if (start >= width) return total;
    const end = @min(width, start + count);
    for (0..rows) |row| {
        const base = row * width;
        for (start..end) |col| {
            total += targets[base + col];
        }
    }
    return total;
}

fn evalScalarNodeForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    output_node: NodeId,
) !f64 {
    if (trainer.compute_backend.kind() == .metal) {
        return evalScalarNodeViaTrainStepForBatch(allocator, trainer, ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len, output_node);
    }
    const values = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len, output_node);
    defer allocator.free(values);
    if (values.len == 0) return error.InvalidTensorShape;
    return @floatCast(values[0]);
}

fn evalScalarNodeViaTrainStepForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    output_node: NodeId,
) !f64 {
    const rows: usize = @intCast(batch * seq_len);
    if (input_ids.len != rows or attention_mask.len != rows) return error.InvalidGlinerBatchShape;

    const trainer_input = makeTrainerInput(ctx, input_ids, attention_mask, targets, targets_shape, batch, seq_len);
    try trainer.ensureGraphBuilt(trainer_input);
    var gs = &trainer.graph_state.?;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| {
            if (!borrowed_rt.contains(entry.key_ptr.*)) trainer.compute_backend.free(entry.value_ptr.*);
        }
        borrowed_rt.deinit(allocator);
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

    try rt.put(allocator, gs.input_ids_node, try bindEvalInputIds(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, targets));
    try bindEvalTrainables(allocator, trainer, &rt, &borrowed_rt);
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }
    try bindEvalLoraDropoutMasks(allocator, trainer, gs, &rt);

    const no_trainables: []const []const u8 = &.{};
    var result = try graph_training.trainStep(allocator, &gs.graph, output_node, trainer.compute_backend, rt, .{
        .trainable_params = no_trainables,
    });
    defer result.deinit();
    return @floatCast(result.loss);
}

pub fn spanStartComponentDebugForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    span_targets: []const f32,
    span_targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    entity_types: usize,
) !SpanStartComponentDebug {
    if (entity_types == 0) return error.InvalidEntityTypes;
    if (ctx.config.objective != .span_start and ctx.config.objective != .gliner2_total_loss) return error.InvalidGlinerObjective;
    if (span_targets_shape.rank() != 2) return error.InvalidGlinerSpanTargetShape;
    const span_rows: usize = @intCast(span_targets_shape.dim(0));
    const target_width: usize = @intCast(span_targets_shape.dim(1));
    if (span_rows == 0 or span_targets.len != span_rows * target_width) return error.InvalidGlinerSpanTargetShape;
    if (target_width < 2 * entity_types + 2) return error.InvalidGlinerSpanTargetShape;

    var positive_row: usize = 0;
    var positive_entity: usize = 0;
    var found_positive = false;
    for (0..entity_types) |entity_idx| {
        for (0..span_rows) |row_idx| {
            const row = row_idx * target_width;
            if (span_targets[row + entity_idx] > 0.0) {
                positive_row = row_idx;
                positive_entity = entity_idx;
                found_positive = true;
                break;
            }
        }
        if (found_positive) break;
    }
    if (!found_positive) return error.MissingPositiveSpanLabel;

    const batch_usize: usize = @intCast(batch);
    if (batch_usize == 0 or @mod(span_rows, batch_usize) != 0) return error.InvalidGlinerSpanTargetShape;
    const max_spans_per_sample = span_rows / batch_usize;
    const sample_idx = positive_row / max_spans_per_sample;
    const schema_row = sample_idx * entity_types + positive_entity;
    const H: usize = @intCast(ctx.config.graph_config.hidden_size);

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

    const start_hidden = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_start_hidden orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(start_hidden);
    const end_hidden = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_end_hidden orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(end_hidden);
    const projected_span = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_projected_span orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(projected_span);
    const schema_hidden = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_schema_hidden orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(schema_hidden);
    const count_gru_state = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_gru_state orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_gru_state);
    const count_state = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_state orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_state);
    const count_in_project = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_in_project orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_in_project);
    const count_layer0 = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_layer0 orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_layer0);
    const count_layer1 = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_layer1 orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_layer1);
    const count_out0 = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_out0 orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_out0);
    const count_out2 = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_count_out2 orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(count_out2);
    const schema_projection = try evalSpanStartNodeForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len, ctx.span_debug_schema_projection orelse return error.MissingGlinerSpanLogitsNode);
    defer allocator.free(schema_projection);
    const logits = try spanStartLogitsForBatch(allocator, trainer, ctx, input_ids, attention_mask, span_targets, span_targets_shape, batch, seq_len);
    defer allocator.free(logits);

    const span_offset = positive_row * H;
    const schema_offset = schema_row * H;
    const count_offset = schema_row * 128;
    if (start_hidden.len < span_offset + H or end_hidden.len < span_offset + H or projected_span.len < span_offset + H) return error.InvalidTensorShape;
    if (schema_hidden.len < schema_offset + H or schema_projection.len < schema_offset + H) return error.InvalidTensorShape;
    if (count_gru_state.len < schema_offset + H or count_state.len < schema_offset + H or count_out0.len < schema_offset + H or count_out2.len < schema_offset + H) return error.InvalidTensorShape;
    if (count_in_project.len < count_offset + 128 or count_layer0.len < count_offset + 128 or count_layer1.len < count_offset + 128) return error.InvalidTensorShape;
    if (logits.len != span_rows * entity_types) return error.InvalidGlinerSpanLogitsShape;

    var negative_row: usize = 0;
    var negative_entity: usize = 0;
    var negative_logit: f64 = -std.math.inf(f64);
    var found_negative = false;
    for (0..span_rows) |row_idx| {
        const target_row = row_idx * target_width;
        const logit_row = row_idx * entity_types;
        for (0..entity_types) |entity_idx| {
            if (span_targets[target_row + entity_idx] > 0.0) continue;
            if (span_targets[target_row + entity_types + entity_idx] <= 0.0) continue;
            const logit: f64 = @floatCast(logits[logit_row + entity_idx]);
            if (!found_negative or logit > negative_logit) {
                negative_row = row_idx;
                negative_entity = entity_idx;
                negative_logit = logit;
                found_negative = true;
            }
        }
    }
    if (!found_negative) return error.MissingPositiveSpanLabel;
    const negative_sample_idx = negative_row / max_spans_per_sample;
    const negative_schema_row = negative_sample_idx * entity_types + negative_entity;
    const negative_span_offset = negative_row * H;
    const negative_schema_offset = negative_schema_row * H;
    if (start_hidden.len < negative_span_offset + H or end_hidden.len < negative_span_offset + H or projected_span.len < negative_span_offset + H) return error.InvalidTensorShape;
    if (schema_hidden.len < negative_schema_offset + H or schema_projection.len < negative_schema_offset + H) return error.InvalidTensorShape;

    return .{
        .positive_row = positive_row,
        .positive_entity = positive_entity,
        .schema_row = schema_row,
        .start_hidden_norm = vectorNorm(start_hidden[span_offset .. span_offset + H]),
        .end_hidden_norm = vectorNorm(end_hidden[span_offset .. span_offset + H]),
        .projected_span_norm = vectorNorm(projected_span[span_offset .. span_offset + H]),
        .schema_hidden_norm = vectorNorm(schema_hidden[schema_offset .. schema_offset + H]),
        .count_gru_state_norm = vectorNorm(count_gru_state[schema_offset .. schema_offset + H]),
        .count_state_norm = vectorNorm(count_state[schema_offset .. schema_offset + H]),
        .count_in_project_norm = vectorNorm(count_in_project[count_offset .. count_offset + 128]),
        .count_layer0_norm = vectorNorm(count_layer0[count_offset .. count_offset + 128]),
        .count_layer1_norm = vectorNorm(count_layer1[count_offset .. count_offset + 128]),
        .count_out0_norm = vectorNorm(count_out0[schema_offset .. schema_offset + H]),
        .count_out2_norm = vectorNorm(count_out2[schema_offset .. schema_offset + H]),
        .schema_projection_norm = vectorNorm(schema_projection[schema_offset .. schema_offset + H]),
        .projected_schema_dot = vectorDot(projected_span[span_offset .. span_offset + H], schema_projection[schema_offset .. schema_offset + H]),
        .projected_span_mean = vectorMean(projected_span[span_offset .. span_offset + H]),
        .count_gru_state_mean = vectorMean(count_gru_state[schema_offset .. schema_offset + H]),
        .count_state_mean = vectorMean(count_state[schema_offset .. schema_offset + H]),
        .count_in_project_mean = vectorMean(count_in_project[count_offset .. count_offset + 128]),
        .count_layer0_mean = vectorMean(count_layer0[count_offset .. count_offset + 128]),
        .count_layer1_mean = vectorMean(count_layer1[count_offset .. count_offset + 128]),
        .count_out0_mean = vectorMean(count_out0[schema_offset .. schema_offset + H]),
        .count_out2_mean = vectorMean(count_out2[schema_offset .. schema_offset + H]),
        .schema_projection_mean = vectorMean(schema_projection[schema_offset .. schema_offset + H]),
        .negative_row = negative_row,
        .negative_entity = negative_entity,
        .negative_logit = negative_logit,
        .negative_start_hidden_norm = vectorNorm(start_hidden[negative_span_offset .. negative_span_offset + H]),
        .negative_end_hidden_norm = vectorNorm(end_hidden[negative_span_offset .. negative_span_offset + H]),
        .negative_schema_hidden_norm = vectorNorm(schema_hidden[negative_schema_offset .. negative_schema_offset + H]),
        .negative_projected_span_norm = vectorNorm(projected_span[negative_span_offset .. negative_span_offset + H]),
        .negative_schema_projection_norm = vectorNorm(schema_projection[negative_schema_offset .. negative_schema_offset + H]),
        .negative_projected_schema_dot = vectorDot(projected_span[negative_span_offset .. negative_span_offset + H], schema_projection[negative_schema_offset .. negative_schema_offset + H]),
        .negative_start_hidden_mean = vectorMean(start_hidden[negative_span_offset .. negative_span_offset + H]),
        .negative_end_hidden_mean = vectorMean(end_hidden[negative_span_offset .. negative_span_offset + H]),
        .negative_schema_hidden_mean = vectorMean(schema_hidden[negative_schema_offset .. negative_schema_offset + H]),
        .negative_projected_span_mean = vectorMean(projected_span[negative_span_offset .. negative_span_offset + H]),
        .negative_schema_projection_mean = vectorMean(schema_projection[negative_schema_offset .. negative_schema_offset + H]),
    };
}

fn evalSpanStartNodeForBatch(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GlinerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    span_targets: []const f32,
    span_targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    output_node: NodeId,
) ![]f32 {
    const rows: usize = @intCast(batch * seq_len);
    if (input_ids.len != rows or attention_mask.len != rows) return error.InvalidGlinerBatchShape;

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

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| {
            if (!borrowed_rt.contains(entry.key_ptr.*)) trainer.compute_backend.free(entry.value_ptr.*);
        }
        borrowed_rt.deinit(allocator);
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

    try rt.put(allocator, gs.input_ids_node, try bindEvalInputIds(trainer.compute_backend, allocator, input_placeholder, input_ids));
    try rt.put(allocator, gs.attention_mask_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask));
    try rt.put(allocator, gs.targets_node, try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, span_targets));

    try bindEvalTrainables(allocator, trainer, &rt, &borrowed_rt);
    if (trainer_input.bind_arch_inputs) |bind_fn| {
        try bind_fn(trainer_input.ctx, trainer.compute_backend, allocator, &gs.graph, &rt, batch, seq_len, attention_mask);
    }
    try bindEvalLoraDropoutMasks(allocator, trainer, gs, &rt);

    const saved_outputs = try allocator.dupe(NodeId, gs.graph.outputs.items);
    defer {
        gs.graph.outputs.clearRetainingCapacity();
        for (saved_outputs) |node_id| gs.graph.outputs.append(allocator, node_id) catch {};
        allocator.free(saved_outputs);
    }
    gs.graph.outputs.clearRetainingCapacity();
    try gs.graph.markOutput(output_node);

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

    return executeEvalGraphOutputToFloat32(allocator, trainer, &gs.graph, rt_inputs.items);
}

fn vectorNorm(values: []const f32) f64 {
    var sum_sq: f64 = 0.0;
    for (values) |value| {
        const v: f64 = @floatCast(value);
        sum_sq += v * v;
    }
    return std.math.sqrt(sum_sq);
}

fn vectorDot(a: []const f32, b: []const f32) f64 {
    var out: f64 = 0.0;
    for (a, b) |av, bv| out += @as(f64, @floatCast(av)) * @as(f64, @floatCast(bv));
    return out;
}

fn vectorMean(values: []const f32) f64 {
    if (values.len == 0) return 0.0;
    var sum: f64 = 0.0;
    for (values) |value| sum += @floatCast(value);
    return sum / @as(f64, @floatFromInt(values.len));
}

fn bindEvalTrainables(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    rt: *std.AutoHashMapUnmanaged(NodeId, CT),
    borrowed_rt: *std.AutoHashMapUnmanaged(NodeId, void),
) !void {
    for (trainer.lora_params.items) |slot| {
        if (slot.device) |device| {
            try borrowed_rt.put(allocator, slot.node_id, {});
            try rt.put(allocator, slot.node_id, device.weight);
        } else {
            try rt.put(allocator, slot.node_id, try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims));
        }
    }
    for (trainer.regular_params.items) |slot| {
        if (slot.device) |device| {
            try borrowed_rt.put(allocator, slot.node_id, {});
            try rt.put(allocator, slot.node_id, device.weight);
        } else {
            try rt.put(allocator, slot.node_id, try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims));
        }
    }
}

fn bindEvalLoraDropoutMasks(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gs: *real_autodiff.RealAutodiffTrainer.GraphState,
    rt: *std.AutoHashMapUnmanaged(NodeId, CT),
) !void {
    if (trainer.config.lora.dropout <= 0.0) return;
    for (gs.lora_adapter.adapters.items) |info| {
        if (info.dropout_mask_id == ml.graph.null_node) continue;
        const mask_node = gs.graph.node(info.dropout_mask_id);
        var mask_len: usize = 1;
        for (mask_node.output_shape.dims[0..mask_node.output_shape.rank()]) |dim| {
            if (dim <= 0) return error.InvalidTensorShape;
            mask_len *= @intCast(dim);
        }
        const mask = try allocator.alloc(f32, mask_len);
        defer allocator.free(mask);
        @memset(mask, 1.0);
        const placeholder = graph_input_binder.PlaceholderInfo{
            .node_id = info.dropout_mask_id,
            .name = gs.graph.parameterName(mask_node),
            .shape = mask_node.output_shape,
        };
        try rt.put(allocator, info.dropout_mask_id, try graph_input_binder.bindF32(trainer.compute_backend, allocator, placeholder, mask));
    }
}

fn bindEvalInputIds(
    backend: *const ComputeBackend,
    allocator: std.mem.Allocator,
    placeholder: graph_input_binder.PlaceholderInfo,
    input_ids: []const i64,
) !CT {
    const input_ids_f32 = try allocator.alloc(f32, input_ids.len);
    defer allocator.free(input_ids_f32);
    for (input_ids, 0..) |id, i| input_ids_f32[i] = @floatFromInt(id);
    return graph_input_binder.bindF32(backend, allocator, placeholder, input_ids_f32);
}

fn executeEvalGraphOutputToFloat32(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    graph: *const Graph,
    runtime_inputs: []const interpreter.RuntimeInput,
) ![]f32 {
    // Held-out evaluation is intentionally eager on accelerator backends. The
    // training graph remains compiled/cached, while eval builds small
    // objective-specific graphs that do not have standalone Metal or CUDA
    // compiled-partition executors. Running those graphs through the backend
    // interpreter still executes their kernels and keeps tensors on-device.
    const strategy: graph_runtime.Strategy = if (evalUsesEagerDeviceInterpreter(trainer.compute_backend.kind()))
        .interpreter
    else
        try evalRuntimeStrategy(trainer);
    var runtime = try graph_runtime.Runtime.init(allocator, graph, trainer.compute_backend, strategy);
    defer runtime.deinit();

    var exec_result = try runtime.execute(allocator, graph, .{ .runtime_inputs = runtime_inputs });
    defer exec_result.deinit(&runtime);
    if (exec_result.outputs.len != 1) return error.InvalidEvalOutputArity;
    return trainer.compute_backend.toFloat32(exec_result.outputs[0], allocator);
}

fn evalUsesEagerDeviceInterpreter(kind: ops.BackendKind) bool {
    return kind == .metal or kind == .cuda;
}

fn evalRuntimeStrategy(trainer: *const real_autodiff.RealAutodiffTrainer) !graph_runtime.Strategy {
    return switch (trainer.config.execution_engine) {
        .interpreter => if (trainer.config.compiled_required) error.CompiledEvalRequiresCompiledBackend else .interpreter,
        .compiled_device => blk: {
            if (!trainer.compute_backend.supportsDeviceTraining()) {
                if (trainer.config.compiled_required) return error.DeviceTrainingUnavailable;
                break :blk .interpreter;
            }
            break :blk if (trainer.config.compiled_required) .compiled_required else .compiled_preferred;
        },
        .compiled_metal => blk: {
            if (trainer.compute_backend.kind() != .metal) {
                if (trainer.config.compiled_required) return error.CompiledMetalRequiresMetalBackend;
                break :blk .interpreter;
            }
            break :blk if (trainer.config.compiled_required) .compiled_required else .compiled_preferred;
        },
    };
}

test "GLiNER2 held-out eval uses eager device execution for Metal and CUDA" {
    try std.testing.expect(evalUsesEagerDeviceInterpreter(.metal));
    try std.testing.expect(evalUsesEagerDeviceInterpreter(.cuda));
    try std.testing.expect(!evalUsesEagerDeviceInterpreter(.native));
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

/// Packed span-start target width:
/// labels[E], mask[E], schema_token_indices[E], row_repeat_indices[E],
/// count_state_indices[E],
/// start_token_index, end_token_index.
pub fn spanStartTargetWidth(num_entity_types: usize) usize {
    return 5 * num_entity_types + 2;
}

/// Packed weighted span-start target width:
/// labels[E], mask[E], positive_weights[E], schema_token_indices[E],
/// row_repeat_indices[E], count_state_indices[E], start_token_index, end_token_index.
pub fn weightedSpanStartTargetWidth(num_entity_types: usize) usize {
    return 6 * num_entity_types + 2;
}

pub fn gliner2TotalLossClassificationLabelsOffset(num_entity_types: usize) usize {
    return spanStartTargetWidth(num_entity_types);
}

pub fn gliner2TotalLossClassificationMaskOffset(num_entity_types: usize) usize {
    return gliner2TotalLossClassificationLabelsOffset(num_entity_types) + num_entity_types;
}

pub fn gliner2TotalLossCountLabelsOffset(num_entity_types: usize) usize {
    return gliner2TotalLossClassificationMaskOffset(num_entity_types) + num_entity_types;
}

pub fn gliner2TotalLossCountMaskOffset(num_entity_types: usize) usize {
    return gliner2TotalLossCountLabelsOffset(num_entity_types) + 20;
}

pub fn gliner2TotalLossParentIndexOffset(num_entity_types: usize) usize {
    return gliner2TotalLossCountMaskOffset(num_entity_types) + 1;
}

fn gliner2TotalLossBaseTargetWidth(num_entity_types: usize) usize {
    return gliner2TotalLossParentIndexOffset(num_entity_types) + 1;
}

/// Per-instance structure-loss trailing columns. When `max_instances > 1` the
/// total-loss target appends direct per-instance labels and masks
/// (`max_instances * E` each). These tensors preserve repeated values and
/// independent stochastic negative masks. Every total-loss layout then
/// appends one task-group id per field so count-transformer attention stays
/// block-diagonal across upstream schemas, followed by one active-field flag
/// per field; `max_instances == 1` continues to read labels and masks from
/// the legacy base columns.
pub fn gliner2TotalLossInstanceLabelsOffset(num_entity_types: usize) usize {
    return gliner2TotalLossBaseTargetWidth(num_entity_types);
}

pub fn gliner2TotalLossInstanceMasksOffset(num_entity_types: usize, max_instances: u32) usize {
    return gliner2TotalLossInstanceLabelsOffset(num_entity_types) + @as(usize, max_instances) * num_entity_types;
}

pub fn gliner2TotalLossTaskGroupsOffset(num_entity_types: usize, max_instances: u32) usize {
    if (max_instances > 1) {
        return gliner2TotalLossInstanceMasksOffset(num_entity_types, max_instances) +
            @as(usize, max_instances) * num_entity_types;
    }
    return gliner2TotalLossBaseTargetWidth(num_entity_types);
}

/// Which schema fields the count transformer may attend to. Upstream
/// `_compute_sample_loss` drops zero-count structure schemas before they reach
/// `count_embed`, so this is narrower than the task-group column (which is
/// non-zero for every structure field) and cannot be derived from the
/// structure BCE mask at `[E, 2E)`: `applySpanNegativeMask` mutates that mask.
pub fn gliner2TotalLossActiveFieldsOffset(num_entity_types: usize, max_instances: u32) usize {
    return gliner2TotalLossTaskGroupsOffset(num_entity_types, max_instances) + num_entity_types;
}

pub fn gliner2TotalLossTargetWidthEx(num_entity_types: usize, max_instances: u32) usize {
    return gliner2TotalLossActiveFieldsOffset(num_entity_types, max_instances) + num_entity_types;
}

pub fn gliner2TotalLossTargetWidth(num_entity_types: usize) usize {
    return gliner2TotalLossTargetWidthEx(num_entity_types, 1);
}

pub fn gliner2TotalLossTargetsShapeEx(batch: u32, max_rows: u32, num_entity_types: u32, max_instances: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch * max_rows),
        @intCast(gliner2TotalLossTargetWidthEx(@intCast(num_entity_types), max_instances)),
    });
}

pub fn gliner2TotalLossTargetsShape(batch: u32, max_rows: u32, num_entity_types: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch * max_rows),
        @intCast(gliner2TotalLossTargetWidth(@intCast(num_entity_types))),
    });
}

/// Shape for packed span-start targets:
/// `[batch * max_spans, 5 * num_entity_types + 2]` f32.
pub fn spanStartTargetsShape(batch: u32, max_spans: u32, num_entity_types: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch * max_spans),
        @intCast(spanStartTargetWidth(@intCast(num_entity_types))),
    });
}

/// Shape for packed span-start targets with per-label positive weights:
/// `[batch * max_spans, 6 * num_entity_types + 2]` f32.
pub fn weightedSpanStartTargetsShape(batch: u32, max_spans: u32, num_entity_types: u32) Shape {
    return Shape.init(.f32, &.{
        @intCast(batch * max_spans),
        @intCast(weightedSpanStartTargetWidth(@intCast(num_entity_types))),
    });
}

pub const max_span_start_entity_types = 256;

pub const SpanStartTargetStats = struct {
    valid_span_count: u64 = 0,
    positive_span_label_count: u64 = 0,
    ignored_span_count: u64 = 0,
    entity_type_count: usize = 0,
    positive_counts_by_entity_type: [max_span_start_entity_types]u64 = @as([max_span_start_entity_types]u64, @splat(0)),
};

pub const SpanStartTargetOptions = struct {
    positive_weights_by_entity_type: ?[]const f32 = null,
    hard_negative_weight: f32 = 1.0,
};

/// Pack `gliner2_data.EncodedBatch` span labels into the single trainer
/// target tensor consumed by `.span_start`.
pub fn fillSpanStartTargetsFromEncodedBatch(
    batch: *const gliner2_data.EncodedBatch,
    out: []f32,
) !SpanStartTargetStats {
    return fillSpanStartTargetsFromEncodedBatchWithOptions(batch, .{}, out);
}

pub fn fillWeightedSpanStartTargetsFromEncodedBatch(
    batch: *const gliner2_data.EncodedBatch,
    positive_weights_by_entity_type: []const f32,
    out: []f32,
) !SpanStartTargetStats {
    return fillSpanStartTargetsFromEncodedBatchWithOptions(batch, .{
        .positive_weights_by_entity_type = positive_weights_by_entity_type,
    }, out);
}

pub fn fillSpanStartTargetsFromEncodedBatchWithOptions(
    batch: *const gliner2_data.EncodedBatch,
    options: SpanStartTargetOptions,
    out: []f32,
) !SpanStartTargetStats {
    const E = batch.num_entity_types;
    const rows = batch.batch_size * batch.max_spans;
    if (!std.math.isFinite(options.hard_negative_weight) or options.hard_negative_weight <= 0.0) return error.InvalidSpanHardNegativeWeight;
    if (options.positive_weights_by_entity_type) |weights| {
        if (weights.len != E) return error.InvalidGlinerSpanTargetShape;
        for (weights) |weight| {
            if (!std.math.isFinite(weight) or weight <= 0.0) return error.InvalidSpanPositiveWeight;
        }
    }
    const has_positive_weights = options.positive_weights_by_entity_type != null;
    const width = if (has_positive_weights) weightedSpanStartTargetWidth(E) else spanStartTargetWidth(E);
    if (out.len != rows * width) return error.InvalidGlinerSpanTargetShape;
    if (E > max_span_start_entity_types) return error.TooManyEntityTypes;

    @memset(out, 0.0);
    var stats = SpanStartTargetStats{ .entity_type_count = E };

    for (0..batch.batch_size) |sample_idx| {
        const word_pos_offset = sample_idx * batch.max_words_per_sample;
        // Document-order ordinal of the valid span within this sample. The
        // count-embedding index (count_idx column) selects a per-instance count
        // state, so successive valid spans must map to successive state blocks.
        var instance_ordinal: usize = 0;
        for (0..batch.max_spans) |span_idx| {
            const flat_span_idx = sample_idx * batch.max_spans + span_idx;
            const row = flat_span_idx * width;
            const schema_idx_offset = if (has_positive_weights) 3 * E else 2 * E;
            const row_idx_offset = schema_idx_offset + E;
            const count_idx_offset = row_idx_offset + E;

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
            const end_word: usize = @intCast(end_word_raw);
            if (start_word >= batch.max_words_per_sample) return error.InvalidSpanWordIndex;
            if (end_word >= batch.max_words_per_sample) return error.InvalidSpanWordIndex;
            const token_pos_raw = batch.first_token_positions[word_pos_offset + start_word];
            if (token_pos_raw < 0) {
                stats.ignored_span_count += 1;
                continue;
            }
            const end_token_pos_raw = batch.first_token_positions[word_pos_offset + end_word];
            if (end_token_pos_raw < 0) {
                stats.ignored_span_count += 1;
                continue;
            }
            const token_pos: usize = @intCast(token_pos_raw);
            const end_token_pos: usize = @intCast(end_token_pos_raw);
            if (token_pos >= batch.max_length) return error.InvalidTokenPosition;
            if (end_token_pos >= batch.max_length) return error.InvalidTokenPosition;

            // Schema/row/count columns are written only for valid spans, so masked
            // rows stay zero (their mask column is 0 → excluded from the loss). The
            // graph gathers schema/count solely from the first span row per sample,
            // which is always valid, so this preserves graph behavior while giving
            // each valid span its own instance ordinal.
            for (0..E) |entity_type_idx| {
                const label_token_pos_raw = batch.e_token_positions[sample_idx * E + entity_type_idx];
                if (label_token_pos_raw < 0) return error.InvalidTokenPosition;
                const label_token_pos: usize = @intCast(label_token_pos_raw);
                if (label_token_pos >= batch.max_length) return error.InvalidTokenPosition;
                out[row + schema_idx_offset + entity_type_idx] = @as(f32, @floatFromInt(sample_idx * batch.max_length + label_token_pos));
                out[row + row_idx_offset + entity_type_idx] = @as(f32, @floatFromInt(flat_span_idx));
                const compact_schema_row_idx = sample_idx * E + entity_type_idx;
                const state_idx = instance_ordinal * batch.batch_size * E + compact_schema_row_idx;
                out[row + count_idx_offset + entity_type_idx] = @as(f32, @floatFromInt(state_idx));
            }

            for (0..E) |entity_type_idx| {
                const label = batch.span_labels[flat_span_idx * E + entity_type_idx];
                out[row + entity_type_idx] = label;
                out[row + E + entity_type_idx] = if (label <= 0.0 and options.hard_negative_weight > 1.0 and spanOverlapsPositiveLabel(batch, sample_idx, span_idx))
                    options.hard_negative_weight
                else
                    1.0;
                if (options.positive_weights_by_entity_type) |weights| {
                    out[row + 2 * E + entity_type_idx] = weights[entity_type_idx];
                }
                if (label > 0.0) {
                    stats.positive_span_label_count += 1;
                    stats.positive_counts_by_entity_type[entity_type_idx] += 1;
                }
            }
            const start_idx_offset = count_idx_offset + E;
            out[row + start_idx_offset] = @as(f32, @floatFromInt(sample_idx * batch.max_length + token_pos));
            out[row + start_idx_offset + 1] = @as(f32, @floatFromInt(sample_idx * batch.max_length + end_token_pos));
            stats.valid_span_count += 1;
            instance_ordinal += 1;
        }
    }

    return stats;
}

fn spanOverlapsPositiveLabel(batch: *const gliner2_data.EncodedBatch, sample_idx: usize, span_idx: usize) bool {
    const E = batch.num_entity_types;
    const flat_span_idx = sample_idx * batch.max_spans + span_idx;
    const start_raw = batch.span_indices[flat_span_idx * 2];
    const end_raw = batch.span_indices[flat_span_idx * 2 + 1];
    if (start_raw < 0 or end_raw < start_raw) return false;
    const start: usize = @intCast(start_raw);
    const end: usize = @intCast(end_raw);
    for (0..batch.max_spans) |candidate_idx| {
        const flat_candidate_idx = sample_idx * batch.max_spans + candidate_idx;
        if (batch.span_mask[flat_candidate_idx] <= 0.0) continue;
        var has_positive = false;
        for (0..E) |entity_type_idx| {
            if (batch.span_labels[flat_candidate_idx * E + entity_type_idx] > 0.0) {
                has_positive = true;
                break;
            }
        }
        if (!has_positive) continue;
        const candidate_start_raw = batch.span_indices[flat_candidate_idx * 2];
        const candidate_end_raw = batch.span_indices[flat_candidate_idx * 2 + 1];
        if (candidate_start_raw < 0 or candidate_end_raw < candidate_start_raw) continue;
        const candidate_start: usize = @intCast(candidate_start_raw);
        const candidate_end: usize = @intCast(candidate_end_raw);
        if (start <= candidate_end and candidate_start <= end) return true;
    }
    return false;
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
    try testing.expectEqual(@as(i64, 17), s.dim(1));

    const weighted = weightedSpanStartTargetsShape(2, 5, 3);
    try testing.expectEqual(@as(u8, 2), weighted.rank());
    try testing.expectEqual(@as(i64, 10), weighted.dim(0));
    try testing.expectEqual(@as(i64, 20), weighted.dim(1));
}

test "fillSpanStartTargetsFromEncodedBatch packs labels masks and token indices" {
    var input_ids = [_]i32{ 10, 11, 12, 13, 14, 0, 0, 0 };
    var attention_mask = [_]i32{ 1, 1, 1, 1, 1, 0, 0, 0 };
    var words_mask = [_]i32{ 0, 0, 1, 2, 3, 0, 0, 0 };
    var first_token_positions = [_]i32{ 2, 3, 4 };
    var word_lengths = [_]f32{ 5, 4, 6 };
    var word_has_digit = @as([3]f32, @splat(0.0));
    var word_is_title = @as([3]f32, @splat(0.0));
    var word_is_all_caps = @as([3]f32, @splat(0.0));
    var span_indices = [_]i32{
        0, 0,
        1, 2,
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

    var targets = @as([(3 * 12)]f32, @splat(0.0));
    const stats = try fillSpanStartTargetsFromEncodedBatch(&batch, &targets);
    try testing.expectEqual(@as(u64, 2), stats.valid_span_count);
    try testing.expectEqual(@as(u64, 2), stats.positive_span_label_count);
    try testing.expectEqual(@as(u64, 1), stats.ignored_span_count);
    try testing.expectEqual(@as(usize, 2), stats.entity_type_count);
    try testing.expectEqual(@as(u64, 1), stats.positive_counts_by_entity_type[0]);
    try testing.expectEqual(@as(u64, 1), stats.positive_counts_by_entity_type[1]);

    try testing.expectEqualSlices(f32, &.{ 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 2.0, 2.0 }, targets[0..12]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 2.0, 3.0, 3.0, 4.0 }, targets[12..24]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 }, targets[24..36]);
}

test "fillWeightedSpanStartTargetsFromEncodedBatch packs per-label positive weights" {
    var input_ids = [_]i32{ 10, 11, 12, 13, 14, 0, 0, 0 };
    var attention_mask = [_]i32{ 1, 1, 1, 1, 1, 0, 0, 0 };
    var words_mask = [_]i32{ 0, 0, 1, 2, 3, 0, 0, 0 };
    var first_token_positions = [_]i32{ 2, 3, 4 };
    var word_lengths = [_]f32{ 5, 4, 6 };
    var word_has_digit = @as([3]f32, @splat(0.0));
    var word_is_title = @as([3]f32, @splat(0.0));
    var word_is_all_caps = @as([3]f32, @splat(0.0));
    var span_indices = [_]i32{
        0, 0,
        1, 2,
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

    var targets = @as([(3 * 14)]f32, @splat(0.0));
    const weights = [_]f32{ 32.0, 96.0 };
    const stats = try fillWeightedSpanStartTargetsFromEncodedBatch(&batch, &weights, &targets);
    try testing.expectEqual(@as(u64, 2), stats.valid_span_count);
    try testing.expectEqual(@as(u64, 2), stats.positive_span_label_count);
    try testing.expectEqualSlices(f32, &.{ 1.0, 0.0, 1.0, 1.0, 32.0, 96.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 2.0, 2.0 }, targets[0..14]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 1.0, 1.0, 1.0, 32.0, 96.0, 0.0, 1.0, 1.0, 1.0, 2.0, 3.0, 3.0, 4.0 }, targets[14..28]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 }, targets[28..42]);
}

test "fillSpanStartTargetsFromEncodedBatchWithOptions weights overlapping hard negatives" {
    var input_ids = [_]i32{ 10, 11, 12, 13, 14, 0, 0, 0 };
    var attention_mask = [_]i32{ 1, 1, 1, 1, 1, 0, 0, 0 };
    var words_mask = [_]i32{ 0, 0, 1, 2, 3, 0, 0, 0 };
    var first_token_positions = [_]i32{ 2, 3, 4 };
    var word_lengths = [_]f32{ 5, 4, 6 };
    var word_has_digit = @as([3]f32, @splat(0.0));
    var word_is_title = @as([3]f32, @splat(0.0));
    var word_is_all_caps = @as([3]f32, @splat(0.0));
    var span_indices = [_]i32{
        0, 1,
        1, 1,
        2, 2,
    };
    var span_mask = [_]f32{ 1.0, 1.0, 1.0 };
    var span_labels = [_]f32{
        1.0, 0.0,
        0.0, 0.0,
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

    var targets = @as([(3 * 12)]f32, @splat(0.0));
    _ = try fillSpanStartTargetsFromEncodedBatchWithOptions(&batch, .{ .hard_negative_weight = 4.0 }, &targets);

    // count_idx column [8,9] is the per-instance count-state index
    // (instance_ordinal * batch*E + sample*E + entity_type); it is structural and
    // independent of a span's positive/negative status, so span 0 (ordinal 0) is
    // {0,1}, span 1 (ordinal 1) is {2,3}, span 2 (ordinal 2) is {4,5}.
    try testing.expectEqualSlices(f32, &.{ 1.0, 0.0, 1.0, 4.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0 }, targets[0..12]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0, 4.0, 4.0, 0.0, 1.0, 1.0, 1.0, 2.0, 3.0, 3.0, 3.0 }, targets[12..24]);
    try testing.expectEqualSlices(f32, &.{ 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 2.0, 2.0, 4.0, 5.0, 4.0, 4.0 }, targets[24..36]);
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
    var mask = @as([(2 * 3)]f32, @splat(1.0));
    var targets = @as([(2 * 3 * 4)]f32, @splat(0.0));
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

test "GLiNER2 graph cache restores component nodes and resets omitted state across shapes" {
    const allocator = testing.allocator;
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var ctx = GlinerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .num_classes = 2,
        .objective = .gliner2_total_loss,
        .structure_max_instances = 1,
        .max_schema_tasks = 1,
    });
    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{
            .rank = 2,
            .alpha = 2.0,
            .target_patterns = &.{"query_proj.weight"},
            .strict_target_patterns = true,
        },
        .graph_cache_capacity = 2,
    });
    defer trainer.deinit();

    var ids = @as([16]i64, @splat(0));
    var mask = @as([16]f32, @splat(1.0));
    var targets = @as([512]f32, @splat(0.0));
    const shape_a = gliner2TotalLossTargetsShapeEx(1, 2, 1, 1);
    const input_a = makeTrainerInput(
        &ctx,
        ids[0..8],
        mask[0..8],
        targets[0..@intCast(shape_a.numElements().?)],
        shape_a,
        1,
        8,
    );
    try trainer.ensureGraphBuilt(input_a);
    const count_a = ctx.gliner2_count_logits orelse return error.ExpectedCountLogitsNode;
    const structure_a = ctx.gliner2_structure_loss orelse return error.ExpectedStructureLossNode;
    try testing.expect(count_a < trainer.graph_state.?.graph.nodeCount());
    try testing.expect(structure_a < trainer.graph_state.?.graph.nodeCount());

    // This field is not populated by total-loss graph construction. A miss
    // must clear it instead of capturing an A-graph NodeId in B's snapshot.
    ctx.token_logits = count_a;
    ctx.config.num_classes = 3;
    ctx.config.structure_max_instances = 2;
    ctx.config.max_schema_tasks = 2;
    const shape_b = gliner2TotalLossTargetsShapeEx(1, 4, 2, 2);
    const input_b = makeTrainerInput(
        &ctx,
        ids[0..16],
        mask[0..16],
        targets[0..@intCast(shape_b.numElements().?)],
        shape_b,
        1,
        16,
    );
    try trainer.ensureGraphBuilt(input_b);
    try testing.expect(ctx.token_logits == null);
    const count_b = ctx.gliner2_count_logits orelse return error.ExpectedCountLogitsNode;
    try testing.expect(count_b < trainer.graph_state.?.graph.nodeCount());

    try trainer.ensureGraphBuilt(input_a);
    try testing.expectEqual(@as(u32, 2), ctx.config.num_classes);
    try testing.expectEqual(@as(u32, 1), ctx.config.structure_max_instances);
    try testing.expectEqual(@as(u32, 1), ctx.config.max_schema_tasks);
    try testing.expectEqual(count_a, ctx.gliner2_count_logits.?);
    try testing.expectEqual(structure_a, ctx.gliner2_structure_loss.?);
    try testing.expect(ctx.gliner2_count_logits.? < trainer.graph_state.?.graph.nodeCount());
    const cache = trainer.graphCacheStats();
    try testing.expectEqual(@as(u64, 2), cache.builds);
    try testing.expectEqual(@as(u64, 1), cache.hits);
    try testing.expectEqual(@as(usize, 2), cache.resident_signatures);
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
    // In the real trainer buildForward runs before buildLoss and latches
    // graph_batch (see real_autodiff_trainer buildTrainingGraph); replicate that
    // here since this test exercises buildLoss directly. targets are batch=1.
    ctx.graph_batch = 1;

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

/// Run the already-built graph's single output with `targets` swapped in for
/// `targets_node`. Caller owns the returned f32 slice.
fn evalGraphOutputForTargets(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    g: *const Graph,
    shared_inputs: []const interpreter.RuntimeInput,
    targets_node: NodeId,
    targets: []const f32,
    targets_shape: Shape,
) ![]f32 {
    const dims = [2]i32{ @intCast(targets_shape.dim(0)), @intCast(targets_shape.dim(1)) };
    const targets_ct = try cb.fromFloat32Shape(targets, &dims);
    defer cb.free(targets_ct);

    const inputs = try allocator.alloc(interpreter.RuntimeInput, shared_inputs.len + 1);
    defer allocator.free(inputs);
    @memcpy(inputs[0..shared_inputs.len], shared_inputs);
    inputs[shared_inputs.len] = .{ .node_id = targets_node, .value = targets_ct };

    var result = try interpreter.execute(allocator, g, cb, .{ .runtime_inputs = inputs });
    defer result.deinit(cb);
    return cb.toFloat32(result.outputs[0], allocator);
}

test "count-embed schema projection is invariant to span negative masking" {
    const allocator = testing.allocator;
    const native_compute = @import("../ops/native_compute.zig");

    const graph_batch: u32 = 2;
    const seq_len: u32 = 8;
    const max_spans: u32 = 2;
    const E: usize = 2;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    var ctx = GlinerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .num_classes = @intCast(E + 1),
        .objective = .gliner2_total_loss,
        .structure_max_instances = 1,
        .max_schema_tasks = 1,
    });
    // buildForward would latch this in the real trainer.
    ctx.graph_batch = graph_batch;

    const hidden = try b.parameter("hidden", Shape.init(.f32, &.{
        @intCast(graph_batch * seq_len),
        @intCast(tinyConfig().hidden_size),
    }));
    const targets_shape = gliner2TotalLossTargetsShapeEx(graph_batch, max_spans, @intCast(E), 1);
    const targets_node = try b.parameter("__targets", targets_shape);
    _ = try GlinerAutodiffCtx.buildLoss(@ptrCast(&ctx), &b, hidden, targets_node);
    const projection = ctx.span_debug_schema_projection orelse return error.ExpectedSchemaProjectionNode;
    try g.markOutput(projection);

    const width = gliner2TotalLossTargetWidthEx(E, 1);
    const task_groups_offset = gliner2TotalLossTaskGroupsOffset(E, 1);
    const active_offset = gliner2TotalLossActiveFieldsOffset(E, 1);
    const rows: usize = graph_batch * max_spans;

    const unmasked = try allocator.alloc(f32, rows * width);
    defer allocator.free(unmasked);
    @memset(unmasked, 0.0);
    for (0..rows) |row_idx| {
        const sample = row_idx / max_spans;
        const row = unmasked[row_idx * width ..][0..width];
        for (0..E) |field| {
            row[E + field] = 1.0;
            row[2 * E + field] = @floatFromInt(sample * seq_len + 1 + field);
            row[3 * E + field] = @floatFromInt(row_idx);
            row[4 * E + field] = @floatFromInt(sample * E + field);
            row[task_groups_offset + field] = 1.0;
            row[active_offset + field] = 1.0;
        }
        row[5 * E] = @floatFromInt(sample * seq_len + 3);
        row[5 * E + 1] = @floatFromInt(sample * seq_len + 4);
    }

    // What mask-rate 1.0 packing produces: every negative loses its structure
    // BCE mask and nothing else in the row moves.
    const masked = try allocator.dupe(f32, unmasked);
    defer allocator.free(masked);
    for (0..rows) |row_idx| {
        const row = masked[row_idx * width ..][0..width];
        for (0..E) |field| row[E + field] = 0.0;
    }

    // Same rows, but the second schema field is genuinely inactive.
    const narrowed = try allocator.dupe(f32, unmasked);
    defer allocator.free(narrowed);
    for (0..rows) |row_idx| {
        narrowed[row_idx * width + active_offset + 1] = 0.0;
    }

    var weight_store = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    const cb = compute.computeBackend();

    var owned = std.ArrayListUnmanaged(CT).empty;
    defer {
        for (owned.items) |ct| cb.free(ct);
        owned.deinit(allocator);
    }
    var shared_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer shared_inputs.deinit(allocator);
    for (g.parameters.items) |param_id| {
        if (param_id == targets_node) continue;
        const shape = g.node(param_id).output_shape;
        const count: usize = @intCast(shape.numElements() orelse return error.UnexpectedDynamicParameterShape);
        const data = try allocator.alloc(f32, count);
        defer allocator.free(data);
        for (data, 0..) |*dst, idx| {
            dst.* = 0.25 + 0.5 * @sin(@as(f32, @floatFromInt(idx)) * 0.37 + @as(f32, @floatFromInt(param_id)) * 1.13);
        }
        var dims: [ml.graph.shape.max_rank]i32 = undefined;
        for (0..shape.rank()) |axis| dims[axis] = @intCast(shape.dim(@intCast(axis)));
        const ct = try cb.fromFloat32Shape(data, dims[0..shape.rank()]);
        try owned.append(allocator, ct);
        try shared_inputs.append(allocator, .{ .node_id = param_id, .value = ct });
    }

    const baseline = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, unmasked, targets_shape);
    defer allocator.free(baseline);
    const after_negative_mask = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, masked, targets_shape);
    defer allocator.free(after_negative_mask);
    try testing.expectEqualSlices(f32, baseline, after_negative_mask);

    // Guard against a vacuous pass: the active block must still drive the
    // count transformer's block-diagonal attention.
    const after_deactivation = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, narrowed, targets_shape);
    defer allocator.free(after_deactivation);
    try testing.expect(!std.mem.eql(f32, baseline, after_deactivation));
}

test "span_start count-embed schema projection ignores the structure BCE mask" {
    const allocator = testing.allocator;
    const native_compute = @import("../ops/native_compute.zig");

    const graph_batch: u32 = 2;
    const seq_len: u32 = 8;
    const max_spans: u32 = 2;
    const E: usize = 2;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    var ctx = GlinerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .num_classes = @intCast(E + 1),
        .objective = .span_start,
    });
    // buildForward would latch this in the real trainer.
    ctx.graph_batch = graph_batch;

    const hidden = try b.parameter("hidden", Shape.init(.f32, &.{
        @intCast(graph_batch * seq_len),
        @intCast(tinyConfig().hidden_size),
    }));
    const targets_shape = spanStartTargetsShape(graph_batch, max_spans, @intCast(E));
    const targets_node = try b.parameter("__targets", targets_shape);
    _ = try GlinerAutodiffCtx.buildLoss(@ptrCast(&ctx), &b, hidden, targets_node);
    const projection = ctx.span_debug_schema_projection orelse return error.ExpectedSchemaProjectionNode;
    try g.markOutput(projection);

    // Unweighted schema+count layout: labels[0:E], mask[E:2E], schema_idx[2E:3E],
    // row_idx[3E:4E], count_idx[4E:5E], start[5E], end[5E+1].
    const width = spanStartTargetWidth(E);
    const mask_offset = E;
    const schema_idx_offset = 2 * E;
    const row_idx_offset = 3 * E;
    const count_idx_offset = 4 * E;
    const rows: usize = graph_batch * max_spans;

    const baseline_targets = try allocator.alloc(f32, rows * width);
    defer allocator.free(baseline_targets);
    @memset(baseline_targets, 0.0);
    for (0..rows) |row_idx| {
        const sample = row_idx / max_spans;
        const ordinal = row_idx % max_spans;
        const row = baseline_targets[row_idx * width ..][0..width];
        for (0..E) |field| {
            // Exactly what the packer writes for a valid span with
            // `hard_negative_weight == 1`: an all-ones BCE mask row.
            row[mask_offset + field] = 1.0;
            row[schema_idx_offset + field] = @floatFromInt(sample * seq_len + 1 + field);
            row[row_idx_offset + field] = @floatFromInt(row_idx);
            row[count_idx_offset + field] = @floatFromInt(ordinal * graph_batch * E + sample * E + field);
        }
        row[5 * E] = @floatFromInt(sample * seq_len + 3);
        row[5 * E + 1] = @floatFromInt(sample * seq_len + 4);
    }

    // (1) `applySpanNegativeMask` dropping field 1's negative. Note the drop has
    // to be partial to be observable at all: the key bias is uniform over the
    // key axis when every field is dropped, and softmax subtracts the row max,
    // so an all-zero mask is a no-op even without the fix.
    const negative_masked = try allocator.dupe(f32, baseline_targets);
    defer allocator.free(negative_masked);
    for (0..rows) |row_idx| {
        negative_masked[row_idx * width + mask_offset + 1] = 0.0;
    }

    // (2) `--span-hard-negative-weight 2` on field 0. Through the key-mask path
    // this yielded `(1 - 2) * -1e9 = +1e9` — the opposite sign — pinning
    // attention onto field 0 and making the projection bit-identical to (1),
    // i.e. to masking every other field out entirely.
    const hard_negative_weighted = try allocator.dupe(f32, baseline_targets);
    defer allocator.free(hard_negative_weighted);
    for (0..rows) |row_idx| {
        hard_negative_weighted[row_idx * width + mask_offset] = 2.0;
    }

    // (3) Anti-vacuity control: move the schema token indices, which legitimately
    // select different encoder rows for `schema_hidden`.
    const schema_shifted = try allocator.dupe(f32, baseline_targets);
    defer allocator.free(schema_shifted);
    for (0..rows) |row_idx| {
        const sample = row_idx / max_spans;
        const row = schema_shifted[row_idx * width ..][0..width];
        for (0..E) |field| row[schema_idx_offset + field] = @floatFromInt(sample * seq_len + 5 + field);
    }

    var weight_store = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    const cb = compute.computeBackend();

    var owned = std.ArrayListUnmanaged(CT).empty;
    defer {
        for (owned.items) |ct| cb.free(ct);
        owned.deinit(allocator);
    }
    var shared_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer shared_inputs.deinit(allocator);
    for (g.parameters.items) |param_id| {
        if (param_id == targets_node) continue;
        const shape = g.node(param_id).output_shape;
        const count: usize = @intCast(shape.numElements() orelse return error.UnexpectedDynamicParameterShape);
        const data = try allocator.alloc(f32, count);
        defer allocator.free(data);
        // Deterministic but full-rank, and small enough that the count
        // transformer's attention logits stay unsaturated. A `sin(a*idx + b)`
        // fill is tempting but useless here: every row of a weight matrix is
        // then the same sinusoid at a different phase, i.e. the matrix is rank
        // 2, the whole head collapses onto a 2-D subspace, and even forcing
        // attention onto a single key moves the projection by only an ULP.
        var prng = std.Random.DefaultPrng.init(0x5eed_0000 +% @as(u64, param_id));
        const rng = prng.random();
        for (data) |*dst| dst.* = 0.25 * (rng.float(f32) - 0.5);
        var dims: [ml.graph.shape.max_rank]i32 = undefined;
        for (0..shape.rank()) |axis| dims[axis] = @intCast(shape.dim(@intCast(axis)));
        const ct = try cb.fromFloat32Shape(data, dims[0..shape.rank()]);
        try owned.append(allocator, ct);
        try shared_inputs.append(allocator, .{ .node_id = param_id, .value = ct });
    }

    const baseline = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, baseline_targets, targets_shape);
    defer allocator.free(baseline);

    const after_negative_mask = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, negative_masked, targets_shape);
    defer allocator.free(after_negative_mask);
    try testing.expectEqualSlices(f32, baseline, after_negative_mask);

    const after_hard_negative_weight = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, hard_negative_weighted, targets_shape);
    defer allocator.free(after_hard_negative_weight);
    try testing.expectEqualSlices(f32, baseline, after_hard_negative_weight);
    for (after_hard_negative_weight) |value| try testing.expect(std.math.isFinite(value));

    // Both BCE-mask perturbations are inert, so no mask value — including one
    // above 1.0 — can reach the `(1 - mask) * -1e9` key bias and flip its sign.
    // The control below proves the projection is not simply constant, and that
    // the fixture is well conditioned enough for a real change to be visible far
    // above rounding noise.
    const after_schema_shift = try evalGraphOutputForTargets(allocator, &cb, &g, shared_inputs.items, targets_node, schema_shifted, targets_shape);
    defer allocator.free(after_schema_shift);
    var max_control_delta: f32 = 0.0;
    var max_control_magnitude: f32 = 0.0;
    for (baseline, after_schema_shift) |a, c| {
        max_control_delta = @max(max_control_delta, @abs(a - c));
        max_control_magnitude = @max(max_control_magnitude, @abs(a));
    }
    try testing.expect(max_control_delta > 0.01 * max_control_magnitude);
}

test "span_start objective accepts weighted span targets" {
    const allocator = testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    var ctx = GlinerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .num_classes = 3,
        .objective = .span_start,
    });
    // buildForward would latch this in the real trainer; targets are batch=1.
    ctx.graph_batch = 1;

    const hidden = try b.parameter("hidden", Shape.init(.f32, &.{ 4, 32 }));
    const targets = try b.parameter("__targets", weightedSpanStartTargetsShape(1, 2, 2));
    const loss = try GlinerAutodiffCtx.buildLoss(@ptrCast(&ctx), &b, hidden, targets);
    try g.markOutput(loss);

    const span_logits = ctx.span_logits orelse return error.ExpectedSpanLogitsNode;
    const span_logits_shape = g.node(span_logits).output_shape;
    try testing.expectEqual(@as(i64, 2), span_logits_shape.dim(0));
    try testing.expectEqual(@as(i64, 2), span_logits_shape.dim(1));

    const loss_shape = g.node(loss).output_shape;
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
