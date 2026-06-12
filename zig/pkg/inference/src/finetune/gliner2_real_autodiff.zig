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
//!   * Loss   : cross-entropy over one-hot `targets` of shape `[B*S, C]`
//!
//! Ignored tokens (HuggingFace's `ignore_index = -100` convention) are
//! handled at data-preparation time: the caller is expected to emit an
//! all-zero row in `targets` for those positions, which contributes zero to
//! the `crossEntropyLoss` numerator. The MVP does NOT mask them out of the
//! per-token mean denominator, so ignored tokens apply a small downward
//! bias to the reported loss. A follow-up can add a broadcast-multiply
//! along the token axis and flip to masked sum / masked denominator.
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
//!   * The `classifier.*` parameters are NOT LoRA adapters, so they are
//!     not yet enrolled in the trainer's `lora_params` list. Autodiff
//!     still computes their gradients, but the harness does not currently
//!     write them back. This mirrors the gap called out in
//!     `layoutlmv3_real_autodiff.zig` and will be fixed by the same
//!     "regular trainable params" thread-through that the harness needs.

const std = @import("std");
const ml = @import("ml");
const deberta_graph = @import("../architectures/deberta_graph.zig");
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

pub const GlinerAutodiffConfig = struct {
    /// Underlying DeBERTa encoder config (hidden size, layers, heads, ...).
    graph_config: deberta_graph.Config,
    /// Number of entity classes, including the "O" (no-entity) class.
    num_classes: u32,
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

    /// Task head + scalar loss. Token classification only — GLiNER2 does
    /// not have a sequence-level head worth wiring in the MVP.
    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *GlinerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        return self.buildTokenLoss(bld, forward_output, targets);
    }

    // ── Task-specific head ───────────────────────────────────────────────

    /// Token classification head over `[B*S, H]` encoder output.
    ///
    ///   logits[t, :] = encoder_out[t, :] @ W^T + b     # [B*S, C]
    ///   loss          = -mean_t sum_c targets[t,c] * log_softmax(logits)[t,c]
    ///
    /// Targets are expected as `[B*S, C]` float tensor. Ignored tokens are
    /// represented by an all-zero row; they contribute 0 to the numerator
    /// of the cross-entropy but still count in the mean denominator. See
    /// the module header for the documented MVP bias this introduces.
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

        // `crossEntropyLoss` does log_softmax + one-hot NLL internally and
        // reduces to a scalar. Targets must be shape `[rows, C]`.
        return bld.crossEntropyLoss(logits, targets);
    }
};

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
    try testing.expectEqual(@as(i32, -100), ctx.config.ignore_index);
    try testing.expect(ctx.built == null);
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
