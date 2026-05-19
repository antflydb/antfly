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

// End-to-end LayoutLMv3 LoRA training integration test (level-3 pipeline).
//
// Validates the full pipeline:
//   graph construction -> LoRA injection -> autodiff -> execution -> loss -> optimizer step
//
// After N training steps the loss must decrease, proving that:
//   - layoutlmv3_graph builds a valid forward graph (with 2D positional
//     embeddings: word + position + token_type + x0 + y0 + x1 + y1 + h + w)
//   - The token-classification head (classifier.weight + classifier.bias)
//     is wired on top of the encoder output
//   - LoRA injection finds and wraps query/value projections
//   - autodiff produces real gradients through the full encoder + head
//   - the optimizer updates LoRA weights
//   - subsequent forward passes reflect those weight changes
//
// NOTE: This test requires the full build module graph (`ml`, BLAS linkage).
// Run via `zig build test-layoutlmv3-e2e` after adding a build step in
// build.zig, or by referencing this file from an existing test root that has
// the required imports.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const layoutlmv3_graph = @import("../../architectures/layoutlmv3_graph.zig");
const native_compute_mod = @import("../../ops/native_compute.zig");
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = @import("../../ops/ops.zig");

const real_autodiff = @import("../real_autodiff_trainer.zig");
const layoutlmv3_autodiff = @import("../layoutlmv3_real_autodiff.zig");
const weight_source = @import("../../models/weight_source.zig");
const LoadedWeight = weight_source.LoadedWeight;
const Tensor = @import("../../backends/tensor.zig").Tensor;

// ── Tiny LayoutLMv3 config ───────────────────────────────────────────

const HIDDEN: u32 = 64;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 4;
const HEAD_DIM: u32 = 16;
const INTERMEDIATE: u32 = 128;
const VOCAB: u32 = 100;
const MAX_POS: u32 = 32;
const TYPE_VOCAB: u32 = 2;
const MAX_2D: u32 = 32;
const NUM_CLASSES: u32 = 5;

const BATCH: u32 = 2;
const SEQ_LEN: u32 = 8;

const layoutlmv3_config = layoutlmv3_graph.Config{
    .vocab_size = VOCAB,
    .hidden_size = HIDDEN,
    .num_hidden_layers = NUM_LAYERS,
    .num_attention_heads = NUM_HEADS,
    .head_dim = HEAD_DIM,
    .intermediate_size = INTERMEDIATE,
    .max_position_embeddings = MAX_POS,
    .type_vocab_size = TYPE_VOCAB,
    .max_2d_position_embeddings = MAX_2D,
    .layer_norm_eps = 1e-5,
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

/// Populate the WeightStore with every parameter LayoutLMv3 + classifier head expects.
fn populateLayoutLMv3Weights(allocator: std.mem.Allocator, store: *WeightStore, rng: std.Random) !void {
    const H: i64 = HIDDEN;
    const I: i64 = INTERMEDIATE;
    const V: i64 = VOCAB;
    const P: i64 = MAX_POS;
    const T: i64 = TYPE_VOCAB;
    const D2: i64 = MAX_2D;
    const C: i64 = NUM_CLASSES;

    // Count total parameters.
    // Embeddings: word (1) + pos (1) + token_type (1) + x_pos (1) + y_pos (1)
    //           + h_pos (1) + w_pos (1) + LN w+b (2) = 9.
    // Per layer: q/k/v w+b (6) + attn output dense w+b (2)
    //          + attn output LN w+b (2) + intermediate dense w+b (2)
    //          + output dense w+b (2) + output LN w+b (2) = 16.
    // Classifier head: w + b = 2.
    const total_params: u32 = 9 + 16 * NUM_LAYERS + 2;
    try store.resident_weights.ensureTotalCapacity(allocator, total_params);

    // ── Embeddings ──
    try putWeight(allocator, store, "embeddings.word_embeddings.weight", &.{ V, H }, rng);
    try putWeight(allocator, store, "embeddings.position_embeddings.weight", &.{ P, H }, rng);
    try putWeight(allocator, store, "embeddings.token_type_embeddings.weight", &.{ T, H }, rng);
    try putWeight(allocator, store, "embeddings.x_position_embeddings.weight", &.{ D2, H }, rng);
    try putWeight(allocator, store, "embeddings.y_position_embeddings.weight", &.{ D2, H }, rng);
    try putWeight(allocator, store, "embeddings.h_position_embeddings.weight", &.{ D2, H }, rng);
    try putWeight(allocator, store, "embeddings.w_position_embeddings.weight", &.{ D2, H }, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.weight", &.{H}, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.bias", &.{H}, rng);

    // ── Classifier head ──
    try putWeight(allocator, store, "classifier.weight", &.{ C, H }, rng);
    try putWeight(allocator, store, "classifier.bias", &.{C}, rng);

    // ── Encoder layers ──
    for (0..NUM_LAYERS) |layer| {
        var pfx_buf: [256]u8 = undefined;
        const pfx = std.fmt.bufPrint(&pfx_buf, "encoder.layer.{d}.", .{layer}) catch unreachable;

        const suffixes_2d = [_]struct { name: []const u8, shape: [2]i64 }{
            .{ .name = "attention.self.query.weight", .shape = .{ H, H } },
            .{ .name = "attention.self.key.weight", .shape = .{ H, H } },
            .{ .name = "attention.self.value.weight", .shape = .{ H, H } },
            .{ .name = "attention.output.dense.weight", .shape = .{ H, H } },
            .{ .name = "intermediate.dense.weight", .shape = .{ I, H } },
            .{ .name = "output.dense.weight", .shape = .{ H, I } },
        };
        const suffixes_1d = [_]struct { name: []const u8, shape: [1]i64 }{
            .{ .name = "attention.self.query.bias", .shape = .{H} },
            .{ .name = "attention.self.key.bias", .shape = .{H} },
            .{ .name = "attention.self.value.bias", .shape = .{H} },
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

// ── The test ──────────────────────────────────────────────────────────

test "LayoutLMv3 e2e: loss decreases over training steps" {
    const allocator = std.testing.allocator;

    // 1. Populate weight store with random LayoutLMv3 + classifier parameters.
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

    var prng = std.Random.DefaultPrng.init(99999);
    const rng = prng.random();
    try populateLayoutLMv3Weights(allocator, &weight_store, rng);

    // 2. Create compute backend.
    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    // 3. Create the RealAutodiffTrainer with LoRA targeting query + value.
    const lora_targets = [_][]const u8{ "query", "value" };
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
    var ctx = layoutlmv3_autodiff.LayoutLMv3AutodiffCtx.init(.{
        .graph_config = layoutlmv3_config,
        .task = .token_classification,
        .num_classes = NUM_CLASSES,
    });

    const total = BATCH * SEQ_LEN;

    // Random token IDs in [0, VOCAB).
    var input_ids: [total]i64 = undefined;
    for (&input_ids) |*id| id.* = @intCast(rng.intRangeAtMost(u32, 0, VOCAB - 1));

    // All-ones attention mask (no padding).
    var attention_mask: [total]f32 = undefined;
    @memset(&attention_mask, 1.0);

    // Random one-hot targets [B, S, num_classes] for token classification.
    var targets: [BATCH * SEQ_LEN * NUM_CLASSES]f32 = undefined;
    @memset(&targets, 0.0);
    for (0..total) |t| {
        const cls: u32 = rng.intRangeAtMost(u32, 0, NUM_CLASSES - 1);
        targets[t * NUM_CLASSES + cls] = 1.0;
    }
    const targets_shape = layoutlmv3_autodiff.tokenTargetsShape(BATCH, SEQ_LEN, NUM_CLASSES);

    // Random bbox data [B*S*4] with values in [0, MAX_2D - 1].
    // Layout: (x0, y0, x1, y1) per token. Ensure x1 >= x0, y1 >= y0.
    var bbox_data: [total * 4]i32 = undefined;
    for (0..total) |t| {
        const x0: i32 = @intCast(rng.intRangeAtMost(u32, 0, MAX_2D / 2));
        const y0: i32 = @intCast(rng.intRangeAtMost(u32, 0, MAX_2D / 2));
        const x1: i32 = x0 + @as(i32, @intCast(rng.intRangeAtMost(u32, 1, MAX_2D / 2 - 1)));
        const y1: i32 = y0 + @as(i32, @intCast(rng.intRangeAtMost(u32, 1, MAX_2D / 2 - 1)));
        bbox_data[t * 4 + 0] = x0;
        bbox_data[t * 4 + 1] = y0;
        bbox_data[t * 4 + 2] = x1;
        bbox_data[t * 4 + 3] = y1;
    }

    // 5. Run training steps and record losses.
    const num_steps: usize = 5;
    var losses: [num_steps]f32 = undefined;

    for (0..num_steps) |step_i| {
        const trainer_input = layoutlmv3_autodiff.makeTrainerInput(
            &ctx,
            &input_ids,
            &attention_mask,
            &targets,
            targets_shape,
            BATCH,
            SEQ_LEN,
            &bbox_data,
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
