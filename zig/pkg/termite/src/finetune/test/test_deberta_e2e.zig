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

// End-to-end DeBERTa-v3 LoRA training integration test (level-3 pipeline).
//
// Validates the full pipeline:
//   graph construction -> LoRA injection -> autodiff -> execution -> loss -> optimizer step
//
// After N training steps the loss must decrease, proving that:
//   - deberta_graph builds a valid forward graph (with disentangled attention:
//     C2C + C2P + P2C, relative position embeddings, bucket indices)
//   - LoRA injection finds and wraps query_proj/value_proj projections
//   - autodiff produces real gradients through the full encoder stack
//   - the optimizer updates LoRA weights
//   - subsequent forward passes reflect those weight changes
//
// NOTE: This test requires the full build module graph (`ml`, system BLAS linkage).
// Run via `zig build test-deberta-e2e` after adding a build step in build.zig,
// or by referencing this file from an existing test root that has the required
// imports.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const deberta_graph = @import("../../architectures/deberta_graph.zig");
const native_compute_mod = @import("../../ops/native_compute.zig");
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = @import("../../ops/ops.zig");
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const real_autodiff = @import("../real_autodiff_trainer.zig");
const graph_input_binder = @import("../graph_input_binder.zig");
const graph_weight_bridge = @import("../graph_weight_bridge.zig");
const weight_source = @import("../../models/weight_source.zig");
const LoadedWeight = weight_source.LoadedWeight;
const Tensor = @import("../../backends/tensor.zig").Tensor;

// ── Tiny DeBERTa config ────────────────────────────────────────────────

const HIDDEN: u32 = 64;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 4;
const INTERMEDIATE: u32 = 128;
const VOCAB: u32 = 100;
const MAX_POS: u32 = 32;
const POS_BUCKETS: u32 = 16;

const BATCH: u32 = 2;
const SEQ_LEN: u32 = 8;

const deberta_config = deberta_graph.Config{
    .vocab_size = VOCAB,
    .hidden_size = HIDDEN,
    .num_hidden_layers = NUM_LAYERS,
    .num_attention_heads = NUM_HEADS,
    .intermediate_size = INTERMEDIATE,
    .max_position_embeddings = MAX_POS,
    .position_buckets = POS_BUCKETS,
    .layer_norm_eps = 1e-7,
    .use_v3_names = true,
};

// ── DeBERTa autodiff context ───────────────────────────────────────────

const DebertaAutodiffCtx = struct {
    graph_config: deberta_graph.Config,
    built: ?deberta_graph.DebertaGraph = null,

    /// Build the DeBERTa forward graph.
    ///
    /// Creates a `__deberta_attn_bias` placeholder that `bindArchInputs`
    /// populates at runtime from the per-step attention mask. The harness
    /// input_ids (f32 placeholder) are reshaped to [B*S] and passed through
    /// to the encoder; embeddingLookup auto-inserts convert_dtype(f32->i64).
    fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        _ = attention_mask;
        const self: *DebertaAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const cfg = self.graph_config;

        // Flatten harness input_ids [B, S] -> [B*S] to match deberta_graph
        // expectation (flat token indices for embedding lookup).
        const total: u32 = batch * seq_len;
        const flat_ids = try bld.reshape(
            input_ids,
            Shape.init(.f32, &.{@as(i64, @intCast(total))}),
        );

        // Attention bias as a runtime-bindable parameter.
        // Shape: [batch * num_heads, seq_len, seq_len].
        const num_heads: u32 = cfg.num_attention_heads;
        const attn_bias = try bld.parameter(
            "__deberta_attn_bias",
            Shape.init(.f32, &.{
                @as(i64, @intCast(batch * num_heads)),
                @as(i64, @intCast(seq_len)),
                @as(i64, @intCast(seq_len)),
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

    /// MSE loss on the encoder output. DeBERTa encoder output is [B*S, H];
    /// we reduce to a per-token scalar for a simple regression loss that
    /// exercises the full backward pass.
    fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        _ = ctx_opaque;
        return bld.mseLoss(forward_output, targets);
    }

    /// Bind the `__deberta_attn_bias` placeholder from the per-step
    /// attention mask using `BertPlaceholderPrep.buildAttnBias`.
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
        const self: *DebertaAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const num_heads = self.graph_config.num_attention_heads;

        if (graph_weight_bridge.findParameterByName(graph, "__deberta_attn_bias")) |node_id| {
            const bias = try graph_input_binder.BertPlaceholderPrep.buildAttnBias(
                allocator,
                attention_mask,
                batch,
                seq_len,
                num_heads,
            );
            defer allocator.free(bias);
            const dims = [_]i32{
                @intCast(batch * num_heads),
                @intCast(seq_len),
                @intCast(seq_len),
            };
            const ct = try cb.fromFloat32Shape(bias, &dims);
            try rt_map.put(allocator, node_id, ct);
        }
    }
};

/// Build a TrainerInput for one DeBERTa training step.
fn makeDebertaTrainerInput(
    ctx: *DebertaAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
) real_autodiff.TrainerInput {
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &DebertaAutodiffCtx.buildForward,
        .build_loss = &DebertaAutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &DebertaAutodiffCtx.bindArchInputs,
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

/// Populate the WeightStore with every parameter DeBERTa-v3 expects.
fn populateDebertaWeights(allocator: std.mem.Allocator, store: *WeightStore, rng: std.Random) !void {
    const H: i64 = HIDDEN;
    const I: i64 = INTERMEDIATE;
    const V: i64 = VOCAB;
    const P: i64 = MAX_POS;

    // Count total parameters.
    // Global: word_emb (1) + emb_LN w+b (2) + rel_emb (1) + enc_LN w+b (2) = 6.
    // Per layer: q/k/v proj w+b (6) + attn output dense w+b (2)
    //          + attn output LN w+b (2) + intermediate dense w+b (2)
    //          + output dense w+b (2) + output LN w+b (2) = 16.
    const total_params: u32 = 6 + 16 * NUM_LAYERS;
    try store.resident_weights.ensureTotalCapacity(allocator, total_params);

    // ── Embeddings ──
    try putWeight(allocator, store, "embeddings.word_embeddings.weight", &.{ V, H }, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.weight", &.{H}, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.bias", &.{H}, rng);

    // ── Encoder-level relative position embeddings ──
    try putWeight(allocator, store, "encoder.rel_embeddings.weight", &.{ P, H }, rng);
    try putWeight(allocator, store, "encoder.LayerNorm.weight", &.{H}, rng);
    try putWeight(allocator, store, "encoder.LayerNorm.bias", &.{H}, rng);

    // ── Encoder layers (v3 names) ──
    for (0..NUM_LAYERS) |layer| {
        var pfx_buf: [256]u8 = undefined;
        const pfx = std.fmt.bufPrint(&pfx_buf, "encoder.layer.{d}.", .{layer}) catch unreachable;

        const suffixes_2d = [_]struct { name: []const u8, shape: [2]i64 }{
            .{ .name = "attention.self.query_proj.weight", .shape = .{ H, H } },
            .{ .name = "attention.self.key_proj.weight", .shape = .{ H, H } },
            .{ .name = "attention.self.value_proj.weight", .shape = .{ H, H } },
            .{ .name = "attention.output.dense.weight", .shape = .{ H, H } },
            .{ .name = "intermediate.dense.weight", .shape = .{ I, H } },
            .{ .name = "output.dense.weight", .shape = .{ H, I } },
        };
        const suffixes_1d = [_]struct { name: []const u8, shape: [1]i64 }{
            .{ .name = "attention.self.query_proj.bias", .shape = .{H} },
            .{ .name = "attention.self.key_proj.bias", .shape = .{H} },
            .{ .name = "attention.self.value_proj.bias", .shape = .{H} },
            .{ .name = "attention.output.dense.bias", .shape = .{H} },
            .{ .name = "attention.output.LayerNorm.weight", .shape = .{H} },
            .{ .name = "attention.output.LayerNorm.bias", .shape = .{H} },
            .{ .name = "intermediate.dense.bias", .shape = .{I} },
            .{ .name = "output.dense.bias", .shape = .{H} },
            .{ .name = "output.LayerNorm.weight", .shape = .{H} },
            .{ .name = "output.LayerNorm.bias", .shape = .{H} },
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

test "DeBERTa e2e: loss decreases over training steps" {
    const allocator = std.testing.allocator;

    // 1. Populate weight store with random DeBERTa parameters.
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
            if (std.mem.startsWith(u8, lw.tensor.name, "encoder.layer.")) {
                allocator.free(lw.tensor.name);
            }
            lw.deinit();
        }
        weight_store.resident_weights.deinit(allocator);
    }

    var prng = std.Random.DefaultPrng.init(54321);
    const rng = prng.random();
    try populateDebertaWeights(allocator, &weight_store, rng);

    // 2. Create compute backend.
    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    // 3. Create the RealAutodiffTrainer with LoRA targeting query_proj + value_proj.
    const lora_targets = [_][]const u8{ "query_proj", "value_proj" };
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
    var ctx = DebertaAutodiffCtx{
        .graph_config = deberta_config,
    };

    const total = BATCH * SEQ_LEN;

    // Random token IDs in [0, VOCAB).
    var input_ids: [total]i64 = undefined;
    for (&input_ids) |*id| id.* = @intCast(rng.intRangeAtMost(u32, 0, VOCAB - 1));

    // All-ones attention mask (no padding).
    var attention_mask: [total]f32 = undefined;
    @memset(&attention_mask, 1.0);

    // Targets: [B*S, H] — match encoder output shape for MSE loss.
    var targets: [total * HIDDEN]f32 = undefined;
    for (&targets) |*t| t.* = rng.float(f32) * 2.0 - 1.0;
    const targets_shape = Shape.init(.f32, &.{ @as(i64, total), @as(i64, HIDDEN) });

    // 5. Run training steps and record losses.
    const num_steps: usize = 5;
    var losses: [num_steps]f32 = undefined;

    for (0..num_steps) |step_i| {
        const trainer_input = makeDebertaTrainerInput(
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
