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

// End-to-end BERT LoRA training integration test (level-3 pipeline).
//
// Validates the full pipeline:
//   graph construction → LoRA injection → autodiff → execution → loss → optimizer step
//
// After N training steps the loss must decrease, proving that:
//   - bert_graph builds a valid forward graph
//   - LoRA injection finds and wraps query/value projections
//   - autodiff produces real gradients through the full encoder
//   - the optimizer updates LoRA weights
//   - subsequent forward passes reflect those weight changes
//
// NOTE: This test requires the full build module graph (`ml`, `termite_linalg`,
// system BLAS linkage). Run via `zig build test-bert-e2e` after adding a build step
// in build.zig, or by referencing this file from an existing test root that
// has the required imports.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const bert_graph = @import("../../architectures/bert_graph.zig");
const native_compute_mod = @import("../../ops/native_compute.zig");
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = @import("../../ops/ops.zig");

const real_autodiff = @import("../real_autodiff_trainer.zig");
const reranker_train = @import("../reranker_train.zig");
const weight_source = @import("../../models/weight_source.zig");
const LoadedWeight = weight_source.LoadedWeight;
const Tensor = @import("../../backends/tensor.zig").Tensor;

// ── BERT tiny config ────────────────────────────────────────────────────

const HIDDEN: u32 = 64;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 4;
const INTERMEDIATE: u32 = 128;
const VOCAB: u32 = 100;
const MAX_POS: u32 = 32;

const BATCH: u32 = 2;
const SEQ_LEN: u32 = 8;

const bert_config = bert_graph.Config{
    .vocab_size = VOCAB,
    .hidden_size = HIDDEN,
    .num_hidden_layers = NUM_LAYERS,
    .num_attention_heads = NUM_HEADS,
    .intermediate_size = INTERMEDIATE,
    .max_position_embeddings = MAX_POS,
    .use_token_type = true,
};

// ── Weight population helpers ───────────────────────────────────────────

/// Create a Tensor with random f32 data for the given shape and name.
fn makeRandomWeight(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, rng: std.Random) !LoadedWeight {
    var n_elems: usize = 1;
    for (shape) |d| n_elems *= @intCast(d);

    const data = try allocator.alloc(f32, n_elems);
    // Small random values scaled by 1/sqrt(n) for reasonable initialisation.
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
    // The resident_weights map borrows the key from the tensor's own name
    // allocation which lives as long as the LoadedWeight.
    store.resident_weights.putAssumeCapacity(lw.tensor.name, lw);
    _ = &lw;
}

/// Populate the WeightStore with every parameter BERT expects.
fn populateBertWeights(allocator: std.mem.Allocator, store: *WeightStore, rng: std.Random) !void {
    const H: i64 = HIDDEN;
    const I: i64 = INTERMEDIATE;
    const V: i64 = VOCAB;
    const P: i64 = MAX_POS;

    // Count total parameters to pre-size the hashmap.
    // Embeddings: 5 params. Per layer: 16 params. Total = 5 + 16*NUM_LAYERS.
    const total_params: u32 = 5 + 16 * NUM_LAYERS;
    try store.resident_weights.ensureTotalCapacity(allocator, total_params);

    // ── Embeddings ──
    try putWeight(allocator, store, "embeddings.word_embeddings.weight", &.{ V, H }, rng);
    try putWeight(allocator, store, "embeddings.position_embeddings.weight", &.{ P, H }, rng);
    try putWeight(allocator, store, "embeddings.token_type_embeddings.weight", &.{ 2, H }, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.weight", &.{H}, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.bias", &.{H}, rng);

    // ── Encoder layers ──
    var name_buf: [256]u8 = undefined;
    for (0..NUM_LAYERS) |layer| {
        const pfx_len = std.fmt.count("encoder.layer.{d}.", .{layer});
        const pfx = std.fmt.bufPrint(&name_buf, "encoder.layer.{d}.", .{layer}) catch unreachable;
        _ = pfx_len;

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

// ── The test ────────────────────────────────────────────────────────────

test "BERT e2e: loss decreases over training steps" {
    const allocator = std.testing.allocator;

    // 1. Populate weight store with random BERT parameters.
    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    // Cleanup: deinit every LoadedWeight we inserted.
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

    var prng = std.Random.DefaultPrng.init(12345);
    const rng = prng.random();
    try populateBertWeights(allocator, &weight_store, rng);

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
    var bert_ctx = reranker_train.BertAutodiffCtx{
        .graph_config = bert_config,
    };

    // Random token IDs in [0, VOCAB).
    var input_ids: [BATCH * SEQ_LEN]i64 = undefined;
    for (&input_ids) |*id| id.* = @intCast(rng.intRangeAtMost(u32, 0, VOCAB - 1));

    // All-ones attention mask (no padding).
    var attention_mask: [BATCH * SEQ_LEN]f32 = undefined;
    @memset(&attention_mask, 1.0);

    // Random regression targets: one scalar per token position.
    const total = BATCH * SEQ_LEN;
    var targets: [total * HIDDEN]f32 = undefined;
    for (&targets) |*t| t.* = rng.float(f32) * 2.0 - 1.0;

    const targets_shape = Shape.init(.f32, &.{ @intCast(total), @intCast(HIDDEN) });

    // 5. Run training steps and record losses.
    const num_steps: usize = 5;
    var losses: [num_steps]f32 = undefined;

    for (0..num_steps) |step_i| {
        const trainer_input = reranker_train.bertTrainerInput(
            &bert_ctx,
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

    // Verify every step actually ran the optimizer (grad_accum_steps = 1).
    // (If we got here without error, all 5 steps executed successfully.)
}
