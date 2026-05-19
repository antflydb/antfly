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

// End-to-end fused-chunker (ModernBERT) LoRA training integration test (level-3 pipeline).
//
// Validates the full pipeline:
//   graph construction -> LoRA injection -> autodiff -> execution -> loss -> optimizer step
//
// After N training steps the loss must decrease, proving that:
//   - modern_bert_graph builds a valid forward graph (with RoPE + GeGLU MLP)
//   - The fused-chunker boundary-detection MLP head (boundary_head.w1/b1/w2/b2)
//     is wired on top of the encoder output
//   - LoRA injection finds and wraps query_proj/value_proj projections
//   - autodiff produces real gradients through the full encoder + head
//   - the optimizer updates LoRA weights
//   - subsequent forward passes reflect those weight changes
//
// NOTE: This test requires the full build module graph (`ml`, BLAS linkage).
// Run via `zig build test-fused-chunker-e2e` after adding a build step in
// build.zig, or by referencing this file from an existing test root that has
// the required imports.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const modern_bert_graph = @import("../../architectures/modern_bert_graph.zig");
const native_compute_mod = @import("../../ops/native_compute.zig");
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = @import("../../ops/ops.zig");

const real_autodiff = @import("../real_autodiff_trainer.zig");
const fused_chunker_autodiff = @import("../fused_chunker_real_autodiff.zig");
const weight_source = @import("../../models/weight_source.zig");
const LoadedWeight = weight_source.LoadedWeight;
const Tensor = @import("../../backends/tensor.zig").Tensor;

// ── Tiny ModernBERT config (for fused chunker backbone) ──────────────

const HIDDEN: u32 = 32;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 4;
const HEAD_DIM: u32 = 8;
const INTERMEDIATE: u32 = 64;
const VOCAB: u32 = 64;
const MAX_POS: u32 = 32;
const HEAD_HIDDEN_DIM: u32 = 32;

const BATCH: u32 = 2;
const SEQ_LEN: u32 = 8;

const modern_bert_config = modern_bert_graph.Config{
    .vocab_size = VOCAB,
    .hidden_size = HIDDEN,
    .num_hidden_layers = NUM_LAYERS,
    .num_attention_heads = NUM_HEADS,
    .head_dim = HEAD_DIM,
    .intermediate_size = INTERMEDIATE,
    .max_position_embeddings = MAX_POS,
    .layer_norm_eps = 1e-5,
    .rope_theta = 160000.0,
};

// ── Weight population helpers ─────────────────────────────────────────

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

/// Populate the WeightStore with every parameter ModernBERT + boundary head expects.
fn populateFusedChunkerWeights(allocator: std.mem.Allocator, store: *WeightStore, rng: std.Random) !void {
    const H: i64 = HIDDEN;
    const I: i64 = INTERMEDIATE;
    const V: i64 = VOCAB;
    const HH: i64 = HEAD_HIDDEN_DIM;

    // Count total parameters.
    // Global: tok_emb (1) + emb norm w+b (2) + final_norm w+b (2) = 5.
    // Per layer: attn_norm w+b (2) + q/k/v proj w+b (6) + Wo w+b (2)
    //          + mlp_norm w+b (2) + gate_proj (1) + up_proj (1) + mlp.Wo (1) = 15.
    // Boundary head: w1 + b1 + w2 + b2 = 4.
    const total_params: u32 = 5 + 15 * NUM_LAYERS + 4;
    try store.resident_weights.ensureTotalCapacity(allocator, total_params);

    // ── Embeddings ──
    try putWeight(allocator, store, "model.embeddings.tok_embeddings.weight", &.{ V, H }, rng);
    try putWeight(allocator, store, "model.embeddings.norm.weight", &.{H}, rng);
    try putWeight(allocator, store, "model.embeddings.norm.bias", &.{H}, rng);

    // ── Final norm ──
    try putWeight(allocator, store, "model.final_norm.weight", &.{H}, rng);
    try putWeight(allocator, store, "model.final_norm.bias", &.{H}, rng);

    // ── Boundary head ──
    try putWeight(allocator, store, "boundary_head.w1", &.{ HH, H }, rng);
    try putWeight(allocator, store, "boundary_head.b1", &.{HH}, rng);
    try putWeight(allocator, store, "boundary_head.w2", &.{ 2, HH }, rng);
    try putWeight(allocator, store, "boundary_head.b2", &.{2}, rng);

    // ── Encoder layers ──
    for (0..NUM_LAYERS) |layer| {
        var pfx_buf: [256]u8 = undefined;
        const pfx = std.fmt.bufPrint(&pfx_buf, "model.layers.{d}.", .{layer}) catch unreachable;

        const suffixes_2d = [_]struct { name: []const u8, shape: [2]i64 }{
            .{ .name = "attn.query_proj.weight", .shape = .{ H, H } },
            .{ .name = "attn.key_proj.weight", .shape = .{ H, H } },
            .{ .name = "attn.value_proj.weight", .shape = .{ H, H } },
            .{ .name = "attn.Wo.weight", .shape = .{ H, H } },
            .{ .name = "mlp.gate_proj.weight", .shape = .{ I, H } },
            .{ .name = "mlp.up_proj.weight", .shape = .{ I, H } },
            .{ .name = "mlp.Wo.weight", .shape = .{ H, I } },
        };
        const suffixes_1d = [_]struct { name: []const u8, shape: [1]i64 }{
            .{ .name = "attn_norm.weight", .shape = .{H} },
            .{ .name = "attn_norm.bias", .shape = .{H} },
            .{ .name = "attn.query_proj.bias", .shape = .{H} },
            .{ .name = "attn.key_proj.bias", .shape = .{H} },
            .{ .name = "attn.value_proj.bias", .shape = .{H} },
            .{ .name = "attn.Wo.bias", .shape = .{H} },
            .{ .name = "mlp_norm.weight", .shape = .{H} },
            .{ .name = "mlp_norm.bias", .shape = .{H} },
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

// ── The test ──────────────────────────────────────────────────────────

test "Fused chunker e2e: loss decreases over training steps" {
    const allocator = std.testing.allocator;

    // 1. Populate weight store with random ModernBERT + boundary head parameters.
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

    var prng = std.Random.DefaultPrng.init(67890);
    const rng = prng.random();
    try populateFusedChunkerWeights(allocator, &weight_store, rng);

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
    var ctx = fused_chunker_autodiff.FusedChunkerAutodiffCtx.init(.{
        .graph_config = modern_bert_config,
        .head_hidden_dim = HEAD_HIDDEN_DIM,
    });

    const total = BATCH * SEQ_LEN;

    // Random token IDs in [0, VOCAB).
    var input_ids: [total]i64 = undefined;
    for (&input_ids) |*id| id.* = @intCast(rng.intRangeAtMost(u32, 0, VOCAB - 1));

    // All-ones attention mask (no padding).
    var attention_mask: [total]f32 = undefined;
    @memset(&attention_mask, 1.0);

    // Random boundary targets [B*S, 2] (one-hot for boundary/no-boundary).
    var targets: [total * 2]f32 = undefined;
    @memset(&targets, 0.0);
    for (0..total) |t| {
        const cls: u32 = rng.intRangeAtMost(u32, 0, 1);
        targets[t * 2 + cls] = 1.0;
    }
    const targets_shape = fused_chunker_autodiff.boundaryTargetsShape(BATCH, SEQ_LEN);

    // 5. Run training steps and record losses.
    const num_steps: usize = 5;
    var losses: [num_steps]f32 = undefined;

    for (0..num_steps) |step_i| {
        const trainer_input = fused_chunker_autodiff.makeTrainerInput(
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

    // 7. Stronger assertion: at least one LoRA B weight is non-zero after
    //    training, proving the optimizer actually wrote back updates.
    //    LoRA params are stored as [A, B, A, B, ...] pairs. B matrices are
    //    at odd indices.
    var found_nonzero_b = false;
    for (trainer.lora_params.items, 0..) |slot, idx| {
        if (idx % 2 == 1) { // B matrix
            for (slot.weights) |w| {
                if (w != 0.0) {
                    found_nonzero_b = true;
                    break;
                }
            }
            if (found_nonzero_b) break;
        }
    }
    if (!found_nonzero_b) {
        std.debug.print("FAIL: all LoRA B weights are still zero after training\n", .{});
        return error.LoraWeightsNotUpdated;
    }
}
