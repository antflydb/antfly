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

//! Level-3 training path for ColQwen2.
//!
//! This module wires `architectures/qwen2_graph.zig` into the generic
//! `real_autodiff_trainer.RealAutodiffTrainer` harness, producing a training
//! loop where the **forward pass AND the backward pass** both run through the
//! `ml.graph` autodiff machinery — no surrogate gradient sewn in by hand.
//!
//! ── Where this fits in the ColQwen2 trainer hierarchy ──────────────────────
//!
//!   * `colqwen2.zig`              → Level 1. Hashed surrogate features.
//!                                     Fastest, completely fake embeddings.
//!   * `colqwen2_real_forward.zig` → Level 2. Real Qwen2 forward via the
//!                                     eager `qwen2.zig` path, surrogate
//!                                     (MaxSim-Jacobian) backward.
//!   * `colqwen2_real_autodiff.zig` (this file) → Level 3. Real Qwen2 forward
//!                                     via the `qwen2_graph.zig` graph
//!                                     builder, real autodiff backward
//!                                     through every decoder layer via the
//!                                     `RealAutodiffTrainer` harness.
//!
//! ── Option A vs. Option B: where does MaxSim live? ─────────────────────────
//!
//! True ColBERT-style late interaction fundamentally operates on **two
//! sequences**: one for the query and one for the document. Each is run
//! through the encoder independently, then a cross-token similarity matrix
//! is reduced (max over doc tokens, sum over query tokens). The
//! `RealAutodiffTrainer` harness is built around a single forward callback
//! that consumes ONE `input_ids` placeholder and produces ONE loss — it
//! doesn't natively host a dual-stream graph.
//!
//! Two options:
//!
//!   * **Option A — graph-native MaxSim.** Build the dual-stream forward
//!     inside `buildForward` (two calls to `qwen2_graph.buildForwardGraph`
//!     with separate embedding outputs), then express the similarity matrix
//!     + `reduceMax` + `reduceSum` directly in the graph. Full autodiff
//!     through scoring. Requires re-plumbing the harness to accept two
//!     input-id placeholders, which is out of scope for this file.
//!
//!   * **Option B — pooled MSE approximation (THIS FILE).** Interpret the
//!     harness-supplied sequence as the query (or "side A"), mean-pool its
//!     final hidden state, collapse to a scalar per batch example via a
//!     second reduce over the hidden dim, and MSE against a caller-supplied
//!     target scalar. This is **not late interaction**; it's a simple
//!     representation-matching loss that still exercises the full Qwen2
//!     autodiff stack end-to-end. The optimizer sees real gradients for
//!     every LoRA A/B parameter in every decoder layer.
//!
//! Option B is chosen here as the MVP. It is not a retrieval loss — it is a
//! scaffold that lets us validate the graph-level training path with the
//! same harness that LayoutLMv3 / BERT / GLiNER2 use. A follow-up will
//! replace it with Option A once the harness grows a dual-stream API.
//!
//! ── RoPE cos/sin tables ────────────────────────────────────────────────────
//!
//! `qwen2_graph.buildForwardGraph` requires pre-computed `[seq_len, head_dim]`
//! cos/sin tables as caller-provided NodeIds. We precompute them in pure
//! Zig at `buildForward` time and splice them into the graph via
//! `bld.tensorConst` — this makes them leaf constants from autodiff's
//! perspective, which is exactly what we want (no gradient flow through the
//! positional encoding, matching the HuggingFace reference).
//!
//! The values follow the standard RoPE formula:
//!
//!     inv_freq[i] = 1 / theta^(2i / head_dim)    for i in [0, head_dim/2)
//!     freq[pos, i] = pos * inv_freq[i]
//!     cos[pos, j] = cos(freq[pos, j % (head_dim/2)])
//!     sin[pos, j] = sin(freq[pos, j % (head_dim/2)])
//!
//! for j in [0, head_dim). The "duplicate each half_dim entry to fill
//! head_dim" layout matches the convention the Builder's `rope` op expects
//! (see `qwen2_graph.zig` rope usage and the existing `rope_tables` helper
//! in `architectures/qwen2.zig`).
//!
//! ── Limitations ────────────────────────────────────────────────────────────
//!
//!   * **Option B MVP loss, not true late interaction.** See Option A/B
//!     discussion above. A dual-stream forward + a graph-native MaxSim is a
//!     follow-up.
//!
//!   * **No vision tower.** ColQwen2 is a vision-language model; this
//!     trainer runs only the text decoder. The image side is a follow-up.
//!
//!   * **No InfoNCE / contrastive.** MSE on a pooled-scalar target is the
//!     simplest non-trivial scalar loss. A contrastive objective requires
//!     either a second forward (negatives) or in-batch negatives with a
//!     log-softmax, both of which need the dual-stream refactor anyway.

const std = @import("std");
const ml = @import("ml");
const qwen2_graph = @import("../architectures/qwen2_graph.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
// graph_input_binder no longer needed after removing the colqwen.__input_ids workaround.
// graph_weight_bridge no longer needed: the colqwen.__input_ids backup
// parameter has been removed in favour of the auto-inserted convert_dtype.
const ops = @import("../ops/ops.zig");

const Builder = ml.graph.Builder;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

// ── Public API ───────────────────────────────────────────────────────────────

pub const ColQwenAutodiffConfig = struct {
    graph_config: qwen2_graph.Config,
    /// If true, run the MVP pooled-MSE path (see file header Option B).
    /// If false, the trainer fails with
    /// `error.TrueLateInteractionNotYetSupported` at the first forward-build
    /// — reserved for the follow-up that wires a proper dual-stream forward.
    use_pooled_mse_mvp: bool = true,
};

/// Context passed to the harness's opaque `ctx` pointer. Holds the built
/// `QwenGraph` handle once `buildForward` has been invoked so callers can
/// introspect the text-decoder output node (useful for tests and for the
/// follow-up dual-stream wiring).
///
/// IMPORTANT: the trainer builds the graph exactly once (on the first `step`
/// call); subsequent steps reuse it. `built` will therefore be populated only
/// after the first `trainStep`. Tests that construct the context in isolation
/// will see `built == null`.
pub const ColQwenAutodiffCtx = struct {
    config: ColQwenAutodiffConfig,
    built: ?qwen2_graph.QwenGraph = null,

    pub fn init(config: ColQwenAutodiffConfig) ColQwenAutodiffCtx {
        return .{ .config = config };
    }

    // ── Trainer callbacks ────────────────────────────────────────────────

    /// Build the forward subgraph.
    ///
    /// Creates private i64 `input_ids` + f32 RoPE cos/sin placeholders
    /// (tensor constants) inside the caller-owned Builder, calls
    /// `qwen2_graph.buildForwardGraph`, and returns the final hidden-state
    /// NodeId.
    ///
    /// The harness-supplied `input_ids` / `attention_mask` nodes are
    /// currently IGNORED — see the file-header limitation note on the
    /// input_ids dtype mismatch.
    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        _ = attention_mask;
        const self: *ColQwenAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));

        if (!self.config.use_pooled_mse_mvp) {
            // Reserved for the follow-up dual-stream graph path.
            return error.TrueLateInteractionNotYetSupported;
        }

        // 1. Thread the harness's input_ids through to the encoder.
        //
        //    The harness creates `__input_ids` as an f32 placeholder.
        //    `embeddingLookup` auto-inserts a `convert_dtype(f32 -> i64)`
        //    when the indices dtype is not i64, so we can pass the
        //    harness's f32 node directly.
        const ids_node = input_ids;

        // 2. RoPE cos/sin tables as leaf tensor constants.
        const head_dim = self.config.graph_config.head_dim;
        const rope_theta = self.config.graph_config.rope_theta;

        const cos_data = try buildRopeCosTable(
            bld.graph.allocator,
            seq_len,
            head_dim,
            rope_theta,
        );
        defer bld.graph.allocator.free(cos_data);
        const sin_data = try buildRopeSinTable(
            bld.graph.allocator,
            seq_len,
            head_dim,
            rope_theta,
        );
        defer bld.graph.allocator.free(sin_data);

        const rope_shape = Shape.init(.f32, &.{
            @intCast(seq_len),
            @intCast(head_dim),
        });
        const cos_node = try bld.tensorConst(cos_data, rope_shape);
        const sin_node = try bld.tensorConst(sin_data, rope_shape);

        // 3. Construct the decoder forward graph.
        self.built = try qwen2_graph.buildForwardGraph(
            bld,
            self.config.graph_config,
            batch,
            seq_len,
            .{
                .input_ids = ids_node,
                .rope_cos = cos_node,
                .rope_sin = sin_node,
            },
        );

        return self.built.?.output_node;
    }

    /// Bind architecture-specific placeholders at runtime.
    ///
    /// ColQwen2 no longer needs a private i64 input_ids placeholder:
    /// `embeddingLookup` auto-inserts `convert_dtype(f32 -> i64)`.
    /// RoPE cos/sin tables are built as `tensorConst` nodes
    /// (compile-time constants) and do NOT need runtime binding.
    ///
    /// Qwen2 uses a causal mask baked into the graph; padding mask is a
    /// follow-up for variable-length batched training.
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
        // The `colqwen.__input_ids` backup parameter is no longer needed:
        // `embeddingLookup` auto-inserts a `convert_dtype(f32 -> i64)`
        // so the harness's f32 `__input_ids` flows through directly.
        _ = ctx_opaque;
        _ = cb;
        _ = allocator;
        _ = graph;
        _ = rt_map;
        _ = batch;
        _ = seq_len;
        _ = attention_mask;
    }

    /// Build the MVP pooled-MSE loss.
    ///
    ///   hidden    : [B, S, H]     (from buildForward)
    ///   pooled_3d : [B, 1, H]     = reduceMean(hidden, axes=[1])
    ///   pooled    : [B, H]        = reshape(pooled_3d)
    ///   scalar_3d : [B, 1]        = reduceSum(pooled, axes=[1])  # collapse hidden
    ///   scalar    : [B, 1]                                       (same shape)
    ///   loss      : scalar        = mseLoss(scalar, targets)     # targets [B, 1]
    ///
    /// This is NOT a retrieval loss. It's a scalar representation-matching
    /// loss that exists purely to exercise the full autodiff path from a
    /// scalar objective back through every decoder layer to every LoRA
    /// parameter. Replace with Option A (graph-native MaxSim on a dual
    /// stream) for real late-interaction training.
    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *ColQwenAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));

        if (!self.config.use_pooled_mse_mvp) {
            return error.TrueLateInteractionNotYetSupported;
        }

        const hidden_shape = bld.graph.node(forward_output).output_shape;
        // `forward_output` is rank-3 [B, S, H] from qwen2_graph.
        const batch_i: i64 = hidden_shape.dim(0);
        const hidden_i: i64 = hidden_shape.dim(2);

        // 1. Mean-pool over seq axis (axis=1).
        const pooled_keep = try bld.reduceMean(forward_output, &.{1});

        // 2. Reshape [B, 1, H] → [B, H].
        const pooled = try bld.reshape(
            pooled_keep,
            Shape.init(.f32, &.{ batch_i, hidden_i }),
        );

        // 3. Collapse hidden dim → [B, 1] per-example scalar.
        const scalar_keep = try bld.reduceSum(pooled, &.{1});
        const scalar = try bld.reshape(
            scalar_keep,
            Shape.init(.f32, &.{ batch_i, 1 }),
        );

        // 4. MSE against the caller-supplied [B, 1] target.
        return bld.mseLoss(scalar, targets);
    }
};

// ── TrainerInput helpers ─────────────────────────────────────────────────────

/// Build a `TrainerInput` for one step. The caller provides the tokenised
/// batch, a pooled-scalar target tensor (shape `[batch, 1]`) and the trainer
/// context; this helper wires them into the format the harness expects.
///
/// The returned value borrows slices from its inputs; the caller must keep
/// them alive for the duration of the `step` call.
pub fn makeTrainerInput(
    ctx: *ColQwenAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) real_autodiff.TrainerInput {
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &ColQwenAutodiffCtx.buildForward,
        .build_loss = &ColQwenAutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &ColQwenAutodiffCtx.bindArchInputs,
    };
}

/// Run one training step. Thin convenience wrapper around `trainer.step`
/// that builds the `TrainerInput` on the caller's behalf.
pub fn trainStep(
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *ColQwenAutodiffCtx,
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

/// Shape of the MVP target tensor: one scalar per batch example, stored as
/// `[batch, 1]` so that shape broadcasting inside `mseLoss` is trivial.
pub fn pooledTargetsShape(batch: u32) Shape {
    return Shape.init(.f32, &.{ @intCast(batch), 1 });
}

// ── RoPE table construction ─────────────────────────────────────────────────

/// Allocate and fill the `[seq_len, head_dim]` RoPE cosine table.
///
/// Layout matches the HF / `qwen2.zig` convention: the first `head_dim/2`
/// columns hold `cos(pos * inv_freq[i])` and the next `head_dim/2` columns
/// duplicate those values (so a single `cos(...)` lookup applies to both
/// the even- and odd-indexed channels of the rotated pair).
fn buildRopeCosTable(
    allocator: std.mem.Allocator,
    seq_len: u32,
    head_dim: u32,
    theta: f32,
) ![]f32 {
    const half: u32 = head_dim / 2;
    const total: usize = @as(usize, seq_len) * @as(usize, head_dim);
    const buf = try allocator.alloc(f32, total);
    errdefer allocator.free(buf);

    var pos: u32 = 0;
    while (pos < seq_len) : (pos += 1) {
        const row = buf[pos * head_dim .. (pos + 1) * head_dim];
        var i: u32 = 0;
        while (i < half) : (i += 1) {
            const exponent: f32 = @as(f32, @floatFromInt(2 * i)) /
                @as(f32, @floatFromInt(head_dim));
            const inv_freq: f32 = 1.0 / std.math.pow(f32, theta, exponent);
            const angle: f32 = @as(f32, @floatFromInt(pos)) * inv_freq;
            const c: f32 = @cos(angle);
            row[i] = c;
            row[i + half] = c;
        }
    }
    return buf;
}

/// Sibling of `buildRopeCosTable` — same layout, sine values.
fn buildRopeSinTable(
    allocator: std.mem.Allocator,
    seq_len: u32,
    head_dim: u32,
    theta: f32,
) ![]f32 {
    const half: u32 = head_dim / 2;
    const total: usize = @as(usize, seq_len) * @as(usize, head_dim);
    const buf = try allocator.alloc(f32, total);
    errdefer allocator.free(buf);

    var pos: u32 = 0;
    while (pos < seq_len) : (pos += 1) {
        const row = buf[pos * head_dim .. (pos + 1) * head_dim];
        var i: u32 = 0;
        while (i < half) : (i += 1) {
            const exponent: f32 = @as(f32, @floatFromInt(2 * i)) /
                @as(f32, @floatFromInt(head_dim));
            const inv_freq: f32 = 1.0 / std.math.pow(f32, theta, exponent);
            const angle: f32 = @as(f32, @floatFromInt(pos)) * inv_freq;
            const s: f32 = @sin(angle);
            row[i] = s;
            row[i + half] = s;
        }
    }
    return buf;
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Tiny config shared by all build-only tests below. Dimensions are picked
/// so the graph stays small (≪ 1 MiB of activations) and fits in the unit
/// test allocator.
fn tinyConfig() qwen2_graph.Config {
    return .{
        .vocab_size = 32,
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 4,
        .intermediate_size = 16,
        .max_position_embeddings = 4,
        .rope_theta = 10000.0,
        .rms_norm_eps = 1e-6,
    };
}

test "ColQwenAutodiffCtx: init constructs context with expected defaults" {
    const cfg = ColQwenAutodiffConfig{
        .graph_config = tinyConfig(),
    };
    const ctx = ColQwenAutodiffCtx.init(cfg);
    try testing.expect(ctx.config.use_pooled_mse_mvp);
    try testing.expect(ctx.built == null);
    try testing.expectEqual(@as(u32, 8), ctx.config.graph_config.hidden_size);
}

test "pooledTargetsShape: rank-2 [batch, 1] f32" {
    const s = pooledTargetsShape(4);
    try testing.expectEqual(DType.f32, s.dtype);
    try testing.expectEqual(@as(u8, 2), s.rank());
    try testing.expectEqual(@as(i64, 4), s.dim(0));
    try testing.expectEqual(@as(i64, 1), s.dim(1));
}

test "makeTrainerInput populates the expected fields" {
    var ctx = ColQwenAutodiffCtx.init(.{ .graph_config = tinyConfig() });

    const batch: u32 = 2;
    const seq_len: u32 = 4;

    var input_ids = [_]i64{0} ** (2 * 4);
    var mask = [_]f32{1.0} ** (2 * 4);
    var targets = [_]f32{ 0.5, -0.25 };
    // Stored as [batch, 1].

    const targets_shape = pooledTargetsShape(batch);
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

    try testing.expectEqual(@as(u8, 2), ti.targets_shape.rank());
    try testing.expectEqual(@as(i64, batch), ti.targets_shape.dim(0));
    try testing.expectEqual(@as(i64, 1), ti.targets_shape.dim(1));

    // Callbacks should be wired to the context's static methods.
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&ColQwenAutodiffCtx.buildForward)),
        @as(*const anyopaque, @ptrCast(ti.build_forward)),
    );
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&ColQwenAutodiffCtx.buildLoss)),
        @as(*const anyopaque, @ptrCast(ti.build_loss)),
    );
}

test "buildRopeCosTable / buildRopeSinTable: shape and boundary values" {
    const allocator = testing.allocator;

    const seq_len: u32 = 3;
    const head_dim: u32 = 4;
    const theta: f32 = 10000.0;

    const cos_buf = try buildRopeCosTable(allocator, seq_len, head_dim, theta);
    defer allocator.free(cos_buf);
    const sin_buf = try buildRopeSinTable(allocator, seq_len, head_dim, theta);
    defer allocator.free(sin_buf);

    try testing.expectEqual(@as(usize, seq_len * head_dim), cos_buf.len);
    try testing.expectEqual(@as(usize, seq_len * head_dim), sin_buf.len);

    // pos=0 → angle=0 → cos=1, sin=0 for every column.
    for (0..head_dim) |i| {
        try testing.expectApproxEqAbs(@as(f32, 1.0), cos_buf[i], 1e-6);
        try testing.expectApproxEqAbs(@as(f32, 0.0), sin_buf[i], 1e-6);
    }

    // Duplicated-halves layout: column i and column i+half must hold the
    // same value at every position.
    const half: u32 = head_dim / 2;
    var pos: u32 = 0;
    while (pos < seq_len) : (pos += 1) {
        var i: u32 = 0;
        while (i < half) : (i += 1) {
            const a = cos_buf[pos * head_dim + i];
            const b = cos_buf[pos * head_dim + i + half];
            try testing.expectApproxEqAbs(a, b, 1e-6);

            const sa = sin_buf[pos * head_dim + i];
            const sb = sin_buf[pos * head_dim + i + half];
            try testing.expectApproxEqAbs(sa, sb, 1e-6);
        }
    }
}

test "ColQwenAutodiffCtx.buildForward: constructs a graph with HF parameter names" {
    const allocator = testing.allocator;

    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    var ctx = ColQwenAutodiffCtx.init(.{ .graph_config = tinyConfig() });

    const batch: u32 = 1;
    const seq_len: u32 = 2;

    // Harness-style placeholders (ignored by our buildForward).
    const ids_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const dummy_ids = try bld.parameter("__input_ids", ids_shape);
    const mask_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const dummy_mask = try bld.parameter("__attention_mask", mask_shape);

    const out = try ColQwenAutodiffCtx.buildForward(
        @ptrCast(&ctx),
        &bld,
        dummy_ids,
        dummy_mask,
        batch,
        seq_len,
    );

    try testing.expect(ctx.built != null);

    // Final hidden state must be rank-3 [B, S, H].
    const out_shape = g.node(out).output_shape;
    try testing.expectEqual(@as(u8, 3), out_shape.rank());
    try testing.expectEqual(@as(i64, batch), out_shape.dim(0));
    try testing.expectEqual(@as(i64, seq_len), out_shape.dim(1));
    try testing.expectEqual(@as(i64, 8), out_shape.dim(2));

    // Walk parameters and check for the LoRA-target substrings. These are
    // the same names injectLoRA pattern-matches on, so we know the graph is
    // ready for adapter injection.
    var saw_q_proj = false;
    var saw_o_proj = false;
    var saw_gate_proj = false;
    for (g.parameters.items) |pid| {
        const name = g.parameterName(g.node(pid));
        if (std.mem.indexOf(u8, name, "q_proj.weight") != null) saw_q_proj = true;
        if (std.mem.indexOf(u8, name, "o_proj.weight") != null) saw_o_proj = true;
        if (std.mem.indexOf(u8, name, "gate_proj.weight") != null) saw_gate_proj = true;
    }
    try testing.expect(saw_q_proj);
    try testing.expect(saw_o_proj);
    try testing.expect(saw_gate_proj);
}

test "ColQwenAutodiffCtx.buildLoss: pooled-MSE MVP emits a scalar loss node" {
    const allocator = testing.allocator;

    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    var ctx = ColQwenAutodiffCtx.init(.{ .graph_config = tinyConfig() });

    const batch: u32 = 1;
    const seq_len: u32 = 2;

    const ids_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const dummy_ids = try bld.parameter("__input_ids", ids_shape);
    const mask_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const dummy_mask = try bld.parameter("__attention_mask", mask_shape);

    const out = try ColQwenAutodiffCtx.buildForward(
        @ptrCast(&ctx),
        &bld,
        dummy_ids,
        dummy_mask,
        batch,
        seq_len,
    );

    const targets = try bld.parameter("__targets", pooledTargetsShape(batch));
    const loss = try ColQwenAutodiffCtx.buildLoss(
        @ptrCast(&ctx),
        &bld,
        out,
        targets,
    );

    // MSE loss is a scalar (rank 0 after reduceMean over all axes).
    const loss_shape = g.node(loss).output_shape;
    // mseLoss keeps rank but sets every reduced dim to 1. We just verify
    // the total element count is 1.
    try testing.expectEqual(@as(?i64, 1), loss_shape.numElements());
}

test "ColQwenAutodiffCtx: use_pooled_mse_mvp=false is rejected with TrueLateInteractionNotYetSupported" {
    const allocator = testing.allocator;

    var g = ml.graph.Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    var ctx = ColQwenAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .use_pooled_mse_mvp = false,
    });

    const batch: u32 = 1;
    const seq_len: u32 = 2;
    const ids_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const dummy_ids = try bld.parameter("__input_ids", ids_shape);
    const mask_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const dummy_mask = try bld.parameter("__attention_mask", mask_shape);

    const err = ColQwenAutodiffCtx.buildForward(
        @ptrCast(&ctx),
        &bld,
        dummy_ids,
        dummy_mask,
        batch,
        seq_len,
    );
    try testing.expectError(error.TrueLateInteractionNotYetSupported, err);
}
