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

// End-to-end Qwen2 LoRA training integration test (level-3 pipeline).
//
// Validates the full pipeline:
//   graph construction -> LoRA injection -> autodiff -> execution -> loss -> optimizer step
//
// After N training steps the loss must decrease, proving that:
//   - qwen2_graph builds a valid forward graph (with GQA + RoPE + causal mask)
//   - LoRA injection finds and wraps q_proj/v_proj projections
//   - autodiff produces real gradients through the full decoder stack
//   - the optimizer updates LoRA weights
//   - subsequent forward passes reflect those weight changes
//
// NOTE: This test requires the full build module graph (`ml`, system BLAS linkage).
// Run via `zig build test-qwen2-e2e` after adding a build step in build.zig,
// or by referencing this file from an existing test root that has the required
// imports.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const qwen2_graph = @import("../../architectures/qwen2_graph.zig");
const native_compute_mod = @import("../../ops/native_compute.zig");
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = @import("../../ops/ops.zig");
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const real_autodiff = @import("../real_autodiff_trainer.zig");
const graph_input_binder = @import("../graph_input_binder.zig");
const weight_source = @import("../../models/weight_source.zig");
const LoadedWeight = weight_source.LoadedWeight;
const Tensor = @import("../../backends/tensor.zig").Tensor;

// ── Tiny Qwen2 config ──────────────────────────────────────────────────

const HIDDEN: u32 = 32;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 4;
const NUM_KV_HEADS: u32 = 2;
const HEAD_DIM: u32 = 8;
const INTERMEDIATE: u32 = 64;
const VOCAB: u32 = 64;
const MAX_POS: u32 = 32;

const BATCH: u32 = 2;
const SEQ_LEN: u32 = 8;

const qwen_config = qwen2_graph.Config{
    .vocab_size = VOCAB,
    .hidden_size = HIDDEN,
    .num_hidden_layers = NUM_LAYERS,
    .num_attention_heads = NUM_HEADS,
    .num_kv_heads = NUM_KV_HEADS,
    .head_dim = HEAD_DIM,
    .intermediate_size = INTERMEDIATE,
    .max_position_embeddings = MAX_POS,
    .rope_theta = 10000.0,
    .rms_norm_eps = 1e-6,
};

// ── Qwen2 autodiff context ─────────────────────────────────────────────

const Qwen2AutodiffCtx = struct {
    graph_config: qwen2_graph.Config,
    built: ?qwen2_graph.QwenGraph = null,

    /// Build the Qwen2 forward graph. Creates RoPE cos/sin as tensor
    /// constants (baked into the graph constant pool, no runtime binding
    /// needed) and threads the harness-provided input_ids directly into
    /// the decoder. embeddingLookup auto-inserts convert_dtype(f32->i64).
    fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        _ = attention_mask;
        const self: *Qwen2AutodiffCtx = @ptrCast(@alignCast(ctx_opaque));

        const head_dim = self.graph_config.head_dim;
        const rope_theta = self.graph_config.rope_theta;

        // Build RoPE cos/sin tables as compile-time tensor constants.
        const rope = try graph_input_binder.QwenPlaceholderPrep.buildRopeCosSin(
            bld.graph.allocator,
            seq_len,
            head_dim,
            rope_theta,
        );
        defer bld.graph.allocator.free(rope.cos);
        defer bld.graph.allocator.free(rope.sin);

        const rope_shape = Shape.init(.f32, &.{
            @as(i64, @intCast(seq_len)),
            @as(i64, @intCast(head_dim)),
        });
        const cos_node = try bld.tensorConst(rope.cos, rope_shape);
        const sin_node = try bld.tensorConst(rope.sin, rope_shape);

        self.built = try qwen2_graph.buildForwardGraph(
            bld,
            self.graph_config,
            batch,
            seq_len,
            .{
                .input_ids = input_ids,
                .rope_cos = cos_node,
                .rope_sin = sin_node,
            },
        );

        return self.built.?.output_node;
    }

    /// Pooled-MSE loss: mean-pool over seq axis, collapse hidden dim to a
    /// per-example scalar, MSE against [B, 1] targets.
    fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        _ = ctx_opaque;

        // forward_output: [B, S, H] from qwen2_graph.
        const hidden_shape = bld.graph.node(forward_output).output_shape;
        const batch_i: i64 = hidden_shape.dim(0);
        const hidden_i: i64 = hidden_shape.dim(2);

        // Mean-pool over seq axis (axis=1) -> [B, 1, H].
        const pooled_keep = try bld.reduceMean(forward_output, &.{1});

        // Reshape [B, 1, H] -> [B, H].
        const pooled = try bld.reshape(
            pooled_keep,
            Shape.init(.f32, &.{ batch_i, hidden_i }),
        );

        // Collapse hidden dim -> [B, 1] per-example scalar.
        const scalar = try bld.reduceSum(pooled, &.{1});

        // MSE against [B, 1] targets.
        return bld.mseLoss(scalar, targets);
    }

    /// Qwen2 uses a causal mask baked into the graph and RoPE as tensor
    /// constants. No additional runtime bindings are needed.
    fn bindArchInputs(
        ctx_opaque: *anyopaque,
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        rt_map: *std.AutoHashMapUnmanaged(NodeId, CT),
        batch: u32,
        seq_len: u32,
        attention_mask: []const f32,
    ) anyerror!void {
        _ = ctx_opaque;
        _ = cb;
        _ = allocator;
        _ = graph;
        _ = rt_map;
        _ = batch;
        _ = seq_len;
        _ = attention_mask;
    }
};

/// Build a TrainerInput for one Qwen2 training step.
fn makeQwen2TrainerInput(
    ctx: *Qwen2AutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) real_autodiff.TrainerInput {
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &Qwen2AutodiffCtx.buildForward,
        .build_loss = &Qwen2AutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &Qwen2AutodiffCtx.bindArchInputs,
    };
}

// ── Weight population helpers ──────────────────────────────────────────

/// Create a LoadedWeight with random f32 data for the given shape and name.
fn makeRandomWeight(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, rng: std.Random) !LoadedWeight {
    var n_elems: usize = 1;
    for (shape) |d| n_elems *= @intCast(d);

    const data = try allocator.alloc(f32, n_elems);
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(@max(n_elems, 1))));
    for (data) |*v| v.* = (rng.float(f32) * 2.0 - 1.0) * scale;

    const tensor = try Tensor.initFloat32(allocator, name, shape, data);
    allocator.free(data);

    return LoadedWeight{ .tensor = tensor };
}

/// Register a single named weight into the store.
fn putWeight(
    allocator: std.mem.Allocator,
    store: *WeightStore,
    name: []const u8,
    shape: []const i64,
    rng: std.Random,
) !void {
    var lw = try makeRandomWeight(allocator, name, shape, rng);
    store.resident_weights.putAssumeCapacity(lw.tensor.name, lw);
    _ = &lw;
}

/// Populate the WeightStore with every parameter Qwen2 expects.
fn populateQwen2Weights(allocator: std.mem.Allocator, store: *WeightStore, rng: std.Random) !void {
    const H: i64 = HIDDEN;
    const I: i64 = INTERMEDIATE;
    const V: i64 = VOCAB;
    const Q_DIM: i64 = NUM_HEADS * HEAD_DIM;
    const KV_DIM: i64 = NUM_KV_HEADS * HEAD_DIM;

    // Count total parameters.
    // Global: embed_tokens (1) + final norm (1) = 2.
    // Per layer: input_layernorm (1) + q_proj w+b (2) + k_proj w+b (2)
    //          + v_proj w+b (2) + o_proj w (1) + post_attn_ln (1)
    //          + gate_proj (1) + up_proj (1) + down_proj (1) = 12.
    const total_params: u32 = 2 + 12 * NUM_LAYERS;
    try store.resident_weights.ensureTotalCapacity(allocator, total_params);

    // ── Global ──
    try putWeight(allocator, store, "model.embed_tokens.weight", &.{ V, H }, rng);
    try putWeight(allocator, store, "model.norm.weight", &.{H}, rng);

    // ── Decoder layers ──
    for (0..NUM_LAYERS) |layer| {
        var pfx_buf: [256]u8 = undefined;
        const pfx = std.fmt.bufPrint(&pfx_buf, "model.layers.{d}.", .{layer}) catch unreachable;

        const suffixes_2d = [_]struct { name: []const u8, shape: [2]i64 }{
            .{ .name = "self_attn.q_proj.weight", .shape = .{ Q_DIM, H } },
            .{ .name = "self_attn.k_proj.weight", .shape = .{ KV_DIM, H } },
            .{ .name = "self_attn.v_proj.weight", .shape = .{ KV_DIM, H } },
            .{ .name = "self_attn.o_proj.weight", .shape = .{ H, Q_DIM } },
            .{ .name = "mlp.gate_proj.weight", .shape = .{ I, H } },
            .{ .name = "mlp.up_proj.weight", .shape = .{ I, H } },
            .{ .name = "mlp.down_proj.weight", .shape = .{ H, I } },
        };
        const suffixes_1d = [_]struct { name: []const u8, shape: [1]i64 }{
            .{ .name = "input_layernorm.weight", .shape = .{H} },
            .{ .name = "self_attn.q_proj.bias", .shape = .{Q_DIM} },
            .{ .name = "self_attn.k_proj.bias", .shape = .{KV_DIM} },
            .{ .name = "self_attn.v_proj.bias", .shape = .{KV_DIM} },
            .{ .name = "post_attention_layernorm.weight", .shape = .{H} },
        };

        for (suffixes_2d) |s| {
            var full_buf: [256]u8 = undefined;
            const full_name = std.fmt.bufPrint(&full_buf, "{s}{s}", .{ pfx, s.name }) catch unreachable;
            const owned_name = try allocator.dupe(u8, full_name);
            try putWeight(allocator, store, owned_name, &s.shape, rng);
        }
        for (suffixes_1d) |s| {
            var full_buf: [256]u8 = undefined;
            const full_name = std.fmt.bufPrint(&full_buf, "{s}{s}", .{ pfx, s.name }) catch unreachable;
            const owned_name = try allocator.dupe(u8, full_name);
            try putWeight(allocator, store, owned_name, &s.shape, rng);
        }
    }
}

// ── The test ───────────────────────────────────────────────────────────

test "Qwen2 e2e: loss decreases over training steps" {
    const allocator = std.testing.allocator;

    // 1. Populate weight store with random Qwen2 parameters.
    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer {
        var it = weight_store.resident_weights.iterator();
        while (it.next()) |entry| {
            var lw = entry.value_ptr.*;
            // Free names we heap-allocated for per-layer params.
            if (std.mem.startsWith(u8, lw.tensor.name, "model.layers.")) {
                allocator.free(lw.tensor.name);
            }
            lw.deinit();
        }
        weight_store.resident_weights.deinit(allocator);
    }

    var prng = std.Random.DefaultPrng.init(12345);
    const rng = prng.random();
    try populateQwen2Weights(allocator, &weight_store, rng);

    // 2. Create compute backend.
    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    // 3. Create the RealAutodiffTrainer with LoRA targeting q_proj + v_proj.
    const lora_targets = [_][]const u8{ "q_proj", "v_proj" };
    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = .{
                .rank = 4,
                .alpha = 1.0,
                .target_patterns = &lora_targets,
            },
            .lr_schedule = .{ .constant = 1e-3 },
            .max_grad_norm = 1.0,
            .grad_accum_steps = 1,
            .lora_a_init_std = 0.02,
            .seed = 42,
        },
    );
    defer trainer.deinit();

    // 4. Prepare synthetic training data.
    var ctx = Qwen2AutodiffCtx{
        .graph_config = qwen_config,
    };

    // Random token IDs in [0, VOCAB).
    var input_ids: [BATCH * SEQ_LEN]i64 = undefined;
    for (&input_ids) |*id| id.* = @intCast(rng.intRangeAtMost(u32, 0, VOCAB - 1));

    // All-ones attention mask (no padding).
    var attention_mask: [BATCH * SEQ_LEN]f32 = undefined;
    @memset(&attention_mask, 1.0);

    // Targets: [B, 1] scalar per batch element.
    var targets: [BATCH]f32 = undefined;
    for (&targets) |*t| t.* = rng.float(f32) * 2.0 - 1.0;
    const targets_shape = Shape.init(.f32, &.{ @as(i64, BATCH), 1 });

    // 5. Run training steps and record losses.
    const num_steps: usize = 5;
    var losses: [num_steps]f32 = undefined;

    for (0..num_steps) |step_i| {
        const trainer_input = makeQwen2TrainerInput(
            &ctx,
            &input_ids,
            &attention_mask,
            &targets,
            targets_shape,
            BATCH,
            SEQ_LEN,
        );
        const result = try trainer.step(trainer_input);
        losses[step_i] = result.loss;
    }

    // 6. Assert loss decreased: final < initial.
    const initial_loss = losses[0];
    const final_loss = losses[num_steps - 1];
    if (final_loss >= initial_loss) {
        std.debug.print(
            "FAIL: loss did not decrease. initial={d:.6}, final={d:.6}\nAll losses: ",
            .{ initial_loss, final_loss },
        );
        for (losses) |l| std.debug.print("{d:.6} ", .{l});
        std.debug.print("\n", .{});
        return error.LossDidNotDecrease;
    }
}
