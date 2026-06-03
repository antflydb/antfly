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

//! Level-3 training path for the fused chunker.
//!
//! Wires `src/architectures/modern_bert_graph.zig` into the generic
//! `real_autodiff_trainer.RealAutodiffTrainer` harness and stacks a
//! 2-layer boundary-detection MLP head on top of ModernBERT's final
//! hidden state. This unlocks LoRA fine-tuning of the encoder's
//! attention / MLP projections alongside the head, which is the
//! headline win over `fused_chunker_train.zig` — that path freezes the
//! encoder and only autodiffs the head.
//!
//! HEAD SHAPE (intentionally identical to `fused_chunker_loss.BoundaryHeadGraph`):
//!
//!     features [total, hidden]
//!       -> linear(w1[head_hidden, hidden] + b1[head_hidden])
//!       -> gelu
//!       -> linear(w2[2, head_hidden] + b2[2])
//!       -> crossEntropyLoss(logits, targets[total, 2])
//!
//! The parameter names are prefixed with `boundary_head.` so that LoRA
//! target-pattern matching can opt the head in or out explicitly:
//!
//!     boundary_head.w1
//!     boundary_head.b1
//!     boundary_head.w2
//!     boundary_head.b2
//!
//! LIMITATIONS (read before using — mirrors `layoutlmv3_real_autodiff.zig`):
//!
//!   * RoPE cos/sin tables are built inside `buildForward` as Zig-side
//!     precomputed `tensorConst` nodes. Values come from
//!     `config.graph_config.rope_theta`. The tables are sized
//!     `[seq_len, head_dim]` to match the per-layer `bld.rope` call.
//!
//!   * The attn_bias is a runtime-bindable parameter of shape
//!     `[batch * num_heads, seq, seq]`, populated each step from the
//!     per-token attention mask via `BertPlaceholderPrep.buildAttnBias`.
//!     Padded positions receive `-1e9`; valid positions receive `0.0`.
//!
//!   * Head parameters are NOT LoRA-injected (they live outside the
//!     encoder module pattern), so unless the caller's LoRA config
//!     explicitly targets `boundary_head.`, they behave as a frozen
//!     deterministic init at step time. Until the harness grows a
//!     "regular trainable params" list, the practical training flow is:
//!     either include `boundary_head.w` in the LoRA target patterns (so
//!     the adapter mechanism adopts them), or pre-train the head with
//!     `fused_chunker_train.zig` and then run this level-3 path with a
//!     frozen head.

const std = @import("std");
const ml = @import("ml");
const modern_bert_graph = @import("../architectures/modern_bert_graph.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");
const ops = @import("../ops/ops.zig");

const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const Graph = ml.graph.Graph;
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

// ── Public config ────────────────────────────────────────────────────────────

pub const FusedChunkerAutodiffConfig = struct {
    graph_config: modern_bert_graph.Config,
    /// Hidden width of the boundary MLP head. Standard: 256.
    head_hidden_dim: u32 = 256,
};

/// Context owned by the caller and threaded through the trainer's opaque
/// pointer. `built` is populated on the first `buildForward` call so
/// downstream code (metrics, introspection) can reach into the
/// ModernBertGraph handles if needed.
pub const FusedChunkerAutodiffCtx = struct {
    config: FusedChunkerAutodiffConfig,
    built: ?modern_bert_graph.ModernBertGraph = null,

    pub fn init(config: FusedChunkerAutodiffConfig) FusedChunkerAutodiffCtx {
        return .{ .config = config };
    }

    // ── Trainer callbacks ────────────────────────────────────────────────

    /// Build the ModernBERT encoder forward subgraph and return its final
    /// hidden state NodeId (shape `[batch * seq_len, hidden_size]`).
    ///
    /// The harness's f32 `input_ids` is reshaped to rank-1
    /// `[batch * seq_len]`.  `embeddingLookup` auto-inserts a
    /// `convert_dtype(f32 -> i64)` so no separate i64 placeholder is
    /// needed.
    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        _ = attention_mask;

        const self: *FusedChunkerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const cfg = self.config.graph_config;

        const total_i: i64 = @intCast(batch * seq_len);
        const seq_i: i64 = @intCast(seq_len);
        const bh_i: i64 = @intCast(batch * cfg.num_attention_heads);

        // ── Input IDs ───────────────────────────────────────────────────
        //
        // Reshape the harness's f32 `[batch, seq_len]` input_ids to
        // rank-1 `[batch * seq_len]`.  `embeddingLookup` will
        // auto-insert `convert_dtype(f32 -> i64)` for the gather.
        const mb_input_ids = try bld.reshape(
            input_ids,
            Shape.init(.f32, &.{total_i}),
        );

        // ── RoPE cos / sin tables ───────────────────────────────────────
        //
        // Precomputed in Zig as `tensorConst` nodes so autodiff (and the
        // const-folding pass) can treat them as inert constants. The
        // tables are shaped `[seq_len, head_dim]` and use the standard
        // half-rotation RoPE layout where the inverse frequencies are
        //
        //     inv_freq[k] = 1 / theta^(2k / head_dim)   for k in [0, head_dim/2)
        //
        // and `cos[pos, 2k]   = cos(pos * inv_freq[k])`
        //     `cos[pos, 2k+1] = cos(pos * inv_freq[k])` (duplicated so the
        //     full `head_dim` width lines up with the rotated tensor).
        // Same for sin.
        const rope_cos = try buildRopeTable(
            bld,
            seq_len,
            cfg.head_dim,
            cfg.rope_theta,
            .cos,
        );
        const rope_sin = try buildRopeTable(
            bld,
            seq_len,
            cfg.head_dim,
            cfg.rope_theta,
            .sin,
        );

        // ── Attention bias ───────────────────────────────────────────────
        //
        // Runtime-bindable parameter populated by `bindArchInputs` from
        // the per-step attention mask via `BertPlaceholderPrep.buildAttnBias`.
        // Shape `[B*H, S, S]` f32, with `-1e9` at padded positions.
        const attn_bias = try bld.parameter(
            "__modernbert_attn_bias",
            Shape.init(.f32, &.{ bh_i, seq_i, seq_i }),
        );

        // ── Encoder forward pass ────────────────────────────────────────
        const mb = try modern_bert_graph.buildForwardGraph(
            bld,
            cfg,
            mb_input_ids,
            attn_bias,
            rope_cos,
            rope_sin,
            batch,
            seq_len,
        );
        self.built = mb;
        return mb.output_node;
    }

    /// Bind architecture-specific placeholders at runtime.
    ///
    /// ModernBERT's `buildForward` creates:
    ///   - `__modernbert_attn_bias` f32 parameter for the attention bias.
    /// The `__modernbert_input_ids` backup parameter has been removed;
    /// `embeddingLookup` auto-inserts `convert_dtype(f32 -> i64)`.
    /// RoPE cos/sin tables are built as `tensorConst` nodes (compile-time
    /// constants, no runtime binding needed).
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
        const self: *FusedChunkerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));

        // The `__modernbert_input_ids` backup parameter is no longer
        // needed: `embeddingLookup` auto-inserts a `convert_dtype(f32 ->
        // i64)` so the harness's f32 `__input_ids` flows through directly.

        // Attention bias: derived from attention_mask via buildAttnBias.
        // Produces [batch * num_heads, seq_len, seq_len] with -1e9 at
        // padded positions and 0.0 at valid positions.
        const num_heads = self.config.graph_config.num_attention_heads;
        if (graph_weight_bridge.findParameterByName(graph, "__modernbert_attn_bias")) |node_id| {
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

    /// Build the boundary-head MLP and return the scalar
    /// cross-entropy loss NodeId.
    ///
    /// `forward_output` is the encoder's final hidden state with shape
    /// `[batch * seq_len, hidden_size]`. `targets` is the harness's
    /// `__targets` placeholder, which for this task is a per-token
    /// two-class soft-label tensor of shape `[batch * seq_len, 2]` — see
    /// `boundaryTargetsShape`.
    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *FusedChunkerAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const cfg = self.config.graph_config;
        const H: u32 = cfg.hidden_size;
        const head_hidden: u32 = self.config.head_hidden_dim;

        // Pull `total` from the incoming hidden-state shape so we don't
        // have to re-thread batch/seq through the loss callback.
        const hidden_shape = bld.graph.node(forward_output).output_shape;
        const total: u32 = @intCast(hidden_shape.dim(0));

        // ── Head parameter nodes ────────────────────────────────────────
        //
        // Names are prefixed with `boundary_head.` so LoRA target
        // patterns can match them explicitly (e.g. `"boundary_head."`
        // adopts the entire head into the adapter list). The shapes
        // match `fused_chunker_loss.BoundaryHeadGraph` one-for-one.
        const w1 = try bld.parameter(
            "boundary_head.w1",
            Shape.init(.f32, &.{ @intCast(head_hidden), @intCast(H) }),
        );
        const b1 = try bld.parameter(
            "boundary_head.b1",
            Shape.init(.f32, &.{@intCast(head_hidden)}),
        );
        const w2 = try bld.parameter(
            "boundary_head.w2",
            Shape.init(.f32, &.{ 2, @intCast(head_hidden) }),
        );
        const b2 = try bld.parameter(
            "boundary_head.b2",
            Shape.init(.f32, &.{2}),
        );

        // ── MLP forward ─────────────────────────────────────────────────
        const dense = try bld.linear(forward_output, w1, b1, total, H, head_hidden);
        const activated = try bld.gelu(dense);
        const logits = try bld.linear(activated, w2, b2, total, head_hidden, 2);

        return bld.crossEntropyLoss(logits, targets);
    }
};

// ── RoPE / attn-bias construction helpers ───────────────────────────────────

const RopeKind = enum { cos, sin };

/// Build a `[seq_len, head_dim]` f32 constant containing the RoPE
/// cos or sin table for the given `theta`.
///
/// Layout matches `bld.rope`'s expectation: entry `[pos, dim]` holds
/// `cos/sin(pos * inv_freq[dim / 2])`, i.e. each pair of adjacent
/// columns shares the same inverse-frequency value. This is the
/// "duplicated-pair" layout the fused RoPE op consumes directly, as
/// opposed to the half-dim interleaved layout used by some HF kernels.
fn buildRopeTable(
    bld: *Builder,
    seq_len: u32,
    head_dim: u32,
    theta: f32,
    kind: RopeKind,
) !NodeId {
    const allocator = bld.graph.allocator;

    const half: u32 = head_dim / 2;
    // Inverse frequencies: inv_freq[k] = theta^(-2k/head_dim).
    // Computed once on the stack/heap at graph-build time.
    var inv_freq = try allocator.alloc(f32, half);
    defer allocator.free(inv_freq);
    {
        const hd_f: f32 = @floatFromInt(head_dim);
        for (0..half) |k| {
            const exponent: f32 = @as(f32, @floatFromInt(2 * k)) / hd_f;
            inv_freq[k] = std.math.pow(f32, theta, -exponent);
        }
    }

    const n: usize = @as(usize, seq_len) * @as(usize, head_dim);
    var data = try allocator.alloc(f32, n);
    defer allocator.free(data);

    for (0..seq_len) |pos| {
        const pos_f: f32 = @floatFromInt(pos);
        for (0..head_dim) |d| {
            const k = d / 2;
            const angle = pos_f * inv_freq[k];
            const v: f32 = switch (kind) {
                .cos => @cos(angle),
                .sin => @sin(angle),
            };
            data[pos * @as(usize, head_dim) + d] = v;
        }
    }

    const shape = Shape.init(.f32, &.{
        @as(i64, @intCast(seq_len)),
        @as(i64, @intCast(head_dim)),
    });
    return bld.tensorConst(data, shape);
}

/// Build a zero-filled `[batch*num_heads, seq_len, seq_len]` f32
/// constant used as the additive attention bias. Zero everywhere means
/// "no masking" — bidirectional attention across the full sequence.
fn buildZeroAttnBias(bld: *Builder, bh_i: i64, seq_i: i64) !NodeId {
    const allocator = bld.graph.allocator;
    const total: usize = @as(usize, @intCast(bh_i)) *
        @as(usize, @intCast(seq_i)) *
        @as(usize, @intCast(seq_i));
    const data = try allocator.alloc(f32, total);
    defer allocator.free(data);
    @memset(data, 0.0);

    const shape = Shape.init(.f32, &.{ bh_i, seq_i, seq_i });
    return bld.tensorConst(data, shape);
}

// ── TrainerInput builder + step wrapper ─────────────────────────────────────

/// Build a `TrainerInput` for a single fused-chunker training step. The
/// returned value borrows the provided slices; the caller owns their
/// lifetimes and must keep them alive for the duration of the step.
pub fn makeTrainerInput(
    ctx: *FusedChunkerAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) real_autodiff.TrainerInput {
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &FusedChunkerAutodiffCtx.buildForward,
        .build_loss = &FusedChunkerAutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &FusedChunkerAutodiffCtx.bindArchInputs,
    };
}

/// Run one training step. Convenience wrapper around
/// `RealAutodiffTrainer.step` that hides the `TrainerInput`
/// construction.
pub fn trainStep(
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *FusedChunkerAutodiffCtx,
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

// ── Shape helpers ───────────────────────────────────────────────────────────

/// Shape of the per-token boundary target tensor: `[batch*seq_len, 2]`
/// soft labels (row [1, 0] = no-boundary, row [0, 1] = boundary).
pub fn boundaryTargetsShape(batch: u32, seq_len: u32) Shape {
    return Shape.init(.f32, &.{ @intCast(batch * seq_len), 2 });
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Tiny ModernBERT config used across the construction tests. Mirrors
/// the shape-only test in `modern_bert_graph.zig` so the same fast path
/// is exercised.
fn tinyConfig() modern_bert_graph.Config {
    return .{
        .vocab_size = 100,
        .hidden_size = 32,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .head_dim = 8,
        .intermediate_size = 64,
        .max_position_embeddings = 64,
    };
}

test "FusedChunkerAutodiffCtx: init stores config without building graph" {
    const cfg = FusedChunkerAutodiffConfig{
        .graph_config = tinyConfig(),
        .head_hidden_dim = 128,
    };
    const ctx = FusedChunkerAutodiffCtx.init(cfg);
    try testing.expectEqual(@as(u32, 128), ctx.config.head_hidden_dim);
    try testing.expectEqual(@as(u32, 32), ctx.config.graph_config.hidden_size);
    try testing.expect(ctx.built == null);
}

test "boundaryTargetsShape returns rank-2 [batch*seq, 2]" {
    const s = boundaryTargetsShape(2, 8);
    try testing.expectEqual(@as(u8, 2), s.rank());
    try testing.expectEqual(@as(i64, 16), s.dim(0));
    try testing.expectEqual(@as(i64, 2), s.dim(1));
}

test "makeTrainerInput populates fields and callback pointers" {
    var ctx = FusedChunkerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .head_hidden_dim = 64,
    });

    const batch: u32 = 1;
    const seq_len: u32 = 4;

    var input_ids = [_]i64{ 1, 2, 3, 4 };
    var mask = [_]f32{ 1.0, 1.0, 1.0, 1.0 };
    var targets = [_]f32{ 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0 };

    const targets_shape = boundaryTargetsShape(batch, seq_len);
    const ti = makeTrainerInput(
        &ctx,
        &input_ids,
        &mask,
        &targets,
        targets_shape,
        batch,
        seq_len,
    );

    try testing.expectEqual(batch, ti.batch);
    try testing.expectEqual(seq_len, ti.seq_len);
    try testing.expectEqual(input_ids.len, ti.input_ids.len);
    try testing.expectEqual(mask.len, ti.attention_mask.len);
    try testing.expectEqual(targets.len, ti.targets.len);
    try testing.expectEqual(@as(u8, 2), ti.targets_shape.rank());
    try testing.expectEqual(@as(i64, 4), ti.targets_shape.dim(0));
    try testing.expectEqual(@as(i64, 2), ti.targets_shape.dim(1));

    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&FusedChunkerAutodiffCtx.buildForward)),
        @as(*const anyopaque, @ptrCast(ti.build_forward)),
    );
    try testing.expectEqual(
        @as(*const anyopaque, @ptrCast(&FusedChunkerAutodiffCtx.buildLoss)),
        @as(*const anyopaque, @ptrCast(ti.build_loss)),
    );
}

test "buildForward constructs ModernBERT + RoPE + attn_bias constants" {
    const allocator = testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    var ctx = FusedChunkerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .head_hidden_dim = 16,
    });

    const batch: u32 = 1;
    const seq_len: u32 = 8;

    // Stand-in harness placeholders (the callback ignores both).
    const dummy_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) }),
    );
    const dummy_mask = try bld.parameter(
        "__attention_mask",
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) }),
    );

    const out = try FusedChunkerAutodiffCtx.buildForward(
        @ptrCast(&ctx),
        &bld,
        dummy_ids,
        dummy_mask,
        batch,
        seq_len,
    );

    // Output of the encoder callback is `[batch*seq, hidden]`.
    const out_shape = g.node(out).output_shape;
    try testing.expectEqual(@as(i64, 8), out_shape.dim(0));
    try testing.expectEqual(@as(i64, 32), out_shape.dim(1));

    // `ctx.built` should be populated after the first forward build.
    try testing.expect(ctx.built != null);
}

test "buildLoss wires boundary head + cross-entropy on top of forward output" {
    const allocator = testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    var ctx = FusedChunkerAutodiffCtx.init(.{
        .graph_config = tinyConfig(),
        .head_hidden_dim = 16,
    });

    const batch: u32 = 1;
    const seq_len: u32 = 4;

    const dummy_ids = try bld.parameter(
        "__input_ids",
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) }),
    );
    const dummy_mask = try bld.parameter(
        "__attention_mask",
        Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) }),
    );

    const fwd = try FusedChunkerAutodiffCtx.buildForward(
        @ptrCast(&ctx),
        &bld,
        dummy_ids,
        dummy_mask,
        batch,
        seq_len,
    );

    const targets = try bld.parameter(
        "__targets",
        boundaryTargetsShape(batch, seq_len),
    );

    const loss = try FusedChunkerAutodiffCtx.buildLoss(
        @ptrCast(&ctx),
        &bld,
        fwd,
        targets,
    );

    // Loss is a scalar (reduction over all tokens + classes).
    const loss_shape = g.node(loss).output_shape;
    try testing.expectEqual(@as(u8, 0), loss_shape.rank());

    // All four head parameters should be present in the graph's name
    // table. We scan by name because LoRA won't have been injected yet
    // and the parameter list holds everything the Builder emitted.
    var found_w1 = false;
    var found_b1 = false;
    var found_w2 = false;
    var found_b2 = false;
    for (g.parameters.items) |pid| {
        const name = g.parameterName(g.node(pid));
        if (std.mem.eql(u8, name, "boundary_head.w1")) found_w1 = true;
        if (std.mem.eql(u8, name, "boundary_head.b1")) found_b1 = true;
        if (std.mem.eql(u8, name, "boundary_head.w2")) found_w2 = true;
        if (std.mem.eql(u8, name, "boundary_head.b2")) found_b2 = true;
    }
    try testing.expect(found_w1);
    try testing.expect(found_b1);
    try testing.expect(found_w2);
    try testing.expect(found_b2);
}
