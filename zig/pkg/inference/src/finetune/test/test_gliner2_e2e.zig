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

// End-to-end GLiNER2 LoRA training integration test (level-3 pipeline).
//
// Validates the full pipeline:
//   graph construction -> LoRA injection -> autodiff -> execution -> loss -> optimizer step
//
// After N training steps the loss must decrease, proving that:
//   - deberta_graph builds a valid forward graph (with disentangled attention)
//   - GLiNER2's token-classification head (classifier.weight + classifier.bias)
//     is wired on top of the encoder output
//   - LoRA injection finds and wraps query_proj/value_proj projections
//   - autodiff produces real gradients through the full encoder + head
//   - the optimizer updates LoRA weights
//   - subsequent forward passes reflect those weight changes
//
// NOTE: This test requires the full build module graph (`ml`, BLAS linkage).
// Run via `zig build test-gliner2-e2e` after adding a build step in build.zig,
// or by referencing this file from an existing test root that has the required
// imports.

const std = @import("std");
const ml = @import("ml");
const inference = @import("inference_internal");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const deberta_graph = inference.architectures.deberta_graph;
const native_compute_mod = inference.native_compute.native;
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = inference.ops;

const real_autodiff = inference.finetune.real_autodiff_trainer;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const gliner2_bundle = inference.finetune.gliner2;
const gliner2_data = inference.finetune.gliner2_data;
const weight_source = inference.models.weight_source;
const LoadedWeight = weight_source.LoadedWeight;
const Tensor = inference.backends.Tensor;
const compat = inference.io.compat;

// ── Tiny DeBERTa config (for GLiNER2 backbone) ───────────────────────

const HIDDEN: u32 = 64;
const NUM_LAYERS: u32 = 2;
const NUM_HEADS: u32 = 4;
const INTERMEDIATE: u32 = 128;
const VOCAB: u32 = 100;
const MAX_POS: u32 = 32;
const POS_BUCKETS: u32 = 16;
const NUM_CLASSES: u32 = 5; // O + 4 entity types

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

/// Populate the WeightStore with every parameter GLiNER2 (DeBERTa + head) expects.
fn populateGliner2Weights(allocator: std.mem.Allocator, store: *WeightStore, rng: std.Random) !void {
    const H: i64 = HIDDEN;
    const I: i64 = INTERMEDIATE;
    const V: i64 = VOCAB;
    const P: i64 = MAX_POS;
    const C: i64 = NUM_CLASSES;

    // Count total parameters.
    // Global: word_emb (1) + emb_LN w+b (2) + rel_emb (1) + enc_LN w+b (2) = 6.
    // Per layer: q/k/v proj w+b (6) + attn output dense w+b (2)
    //          + attn output LN w+b (2) + intermediate dense w+b (2)
    //          + output dense w+b (2) + output LN w+b (2) = 16.
    // Head: classifier.weight (1) + classifier.bias (1) = 2.
    const total_params: u32 = 6 + 16 * NUM_LAYERS + 2;
    try store.resident_weights.ensureTotalCapacity(allocator, total_params);

    // ── Embeddings ──
    try putWeight(allocator, store, "embeddings.word_embeddings.weight", &.{ V, H }, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.weight", &.{H}, rng);
    try putWeight(allocator, store, "embeddings.LayerNorm.bias", &.{H}, rng);

    // ── Encoder-level relative position embeddings ──
    try putWeight(allocator, store, "encoder.rel_embeddings.weight", &.{ P, H }, rng);
    try putWeight(allocator, store, "encoder.LayerNorm.weight", &.{H}, rng);
    try putWeight(allocator, store, "encoder.LayerNorm.bias", &.{H}, rng);

    // ── Classifier head ──
    try putWeight(allocator, store, "classifier.weight", &.{ C, H }, rng);
    try putWeight(allocator, store, "classifier.bias", &.{C}, rng);

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

// ── The test ──────────────────────────────────────────────────────────

test "GLiNER2 e2e: loss decreases over training steps" {
    const allocator = std.testing.allocator;

    // 1. Populate weight store with random DeBERTa + classifier parameters.
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
    try populateGliner2Weights(allocator, &weight_store, rng);

    // 2. Create compute backend.
    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    // 3. Create the RealAutodiffTrainer with LoRA targeting query_proj + value_proj.
    const lora_targets = [_][]const u8{ "query_proj", "value_proj" };
    const regular_trainable_params = [_][]const u8{ "classifier.weight", "classifier.bias" };
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
            .regular_trainable_params = &regular_trainable_params,
        },
    );
    defer trainer.deinit();

    // 4. Prepare synthetic training data.
    var ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = deberta_config,
        .num_classes = NUM_CLASSES,
    });

    const total = BATCH * SEQ_LEN;

    // Random token IDs in [0, VOCAB).
    var input_ids: [total]i64 = undefined;
    for (&input_ids) |*id| id.* = @intCast(rng.intRangeAtMost(u32, 0, VOCAB - 1));

    // All-ones attention mask (no padding).
    var attention_mask: [total]f32 = undefined;
    @memset(&attention_mask, 1.0);

    // Random one-hot targets [B*S, num_classes].
    var targets: [total * NUM_CLASSES]f32 = undefined;
    @memset(&targets, 0.0);
    for (0..total) |t| {
        const cls: u32 = rng.intRangeAtMost(u32, 0, NUM_CLASSES - 1);
        targets[t * NUM_CLASSES + cls] = 1.0;
    }
    const targets_shape = gliner2_autodiff.tokenTargetsShape(BATCH, SEQ_LEN, NUM_CLASSES);

    // 5. Run training steps and record losses.
    const num_steps: usize = 5;
    var losses: [num_steps]f32 = undefined;

    for (0..num_steps) |step_i| {
        const trainer_input = gliner2_autodiff.makeTrainerInput(
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

    try std.testing.expectEqual(@as(usize, 2), trainer.regular_params.items.len);
    var classifier_head_updated = false;
    for (trainer.regular_params.items) |slot| {
        const original = weight_store.resident_weights.getPtr(slot.name) orelse return error.MissingClassifierHead;
        const original_data = original.tensor.asFloat32();
        try std.testing.expectEqual(original_data.len, slot.weights.len);
        for (slot.weights, original_data) |after, before| {
            if (after != before) {
                classifier_head_updated = true;
                break;
            }
        }
        if (classifier_head_updated) break;
    }
    if (!classifier_head_updated) {
        std.debug.print("FAIL: classifier head weights did not update\n", .{});
        return error.ClassifierHeadNotUpdated;
    }
}

test "GLiNER2 inference: fixed text produces deterministic token logits" {
    const allocator = std.testing.allocator;

    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer {
        var it = weight_store.resident_weights.iterator();
        while (it.next()) |entry| {
            var lw = entry.value_ptr.*;
            if (std.mem.startsWith(u8, lw.tensor.name, "encoder.layer.")) {
                allocator.free(lw.tensor.name);
            }
            lw.deinit();
        }
        weight_store.resident_weights.deinit(allocator);
    }

    var prng = std.Random.DefaultPrng.init(777);
    const rng = prng.random();
    try populateGliner2Weights(allocator, &weight_store, rng);

    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    const lora_targets = [_][]const u8{ "query_proj", "value_proj" };
    const regular_trainable_params = [_][]const u8{ "classifier.weight", "classifier.bias" };
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
            .regular_trainable_params = &regular_trainable_params,
        },
    );
    defer trainer.deinit();

    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    const entity_types = [_][]const u8{ "organization", "location" };
    const examples = [_]gliner2_data.Example{
        .{ .text = "google in london", .entities = &.{} },
    };
    const infer_seq_len: u32 = 32;
    var batch = try gliner2_data.buildSimpleBatch(allocator, &tokenizer, &examples, &entity_types, infer_seq_len, 4, 1);
    defer batch.deinit();

    var input_ids: [infer_seq_len]i64 = undefined;
    var attention_mask: [infer_seq_len]f32 = undefined;
    for (0..infer_seq_len) |idx| {
        input_ids[idx] = batch.input_ids[idx];
        attention_mask[idx] = @floatFromInt(batch.attention_mask[idx]);
    }

    var ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = deberta_config,
        .num_classes = NUM_CLASSES,
    });
    var zero_targets: [infer_seq_len * NUM_CLASSES]f32 = undefined;
    @memset(&zero_targets, 0.0);
    const bootstrap_input = gliner2_autodiff.makeTrainerInput(
        &ctx,
        &input_ids,
        &attention_mask,
        &zero_targets,
        gliner2_autodiff.tokenTargetsShape(1, infer_seq_len, NUM_CLASSES),
        1,
        infer_seq_len,
    );
    try trainer.ensureGraphBuilt(bootstrap_input);

    const classifier_bias = [_]f32{ -0.75, -0.25, 0.0, 0.5, 1.25 };
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, "classifier.weight")) {
            @memset(slot.weights, 0.0);
        } else if (std.mem.eql(u8, slot.name, "classifier.bias")) {
            try std.testing.expectEqual(classifier_bias.len, slot.weights.len);
            @memcpy(slot.weights, &classifier_bias);
        }
    }

    const logits = try gliner2_autodiff.tokenLogitsForBatch(
        allocator,
        &trainer,
        &ctx,
        &input_ids,
        &attention_mask,
        1,
        infer_seq_len,
    );
    defer allocator.free(logits);
    try std.testing.expectEqual(@as(usize, infer_seq_len * NUM_CLASSES), logits.len);

    const logits_again = try gliner2_autodiff.tokenLogitsForBatch(
        allocator,
        &trainer,
        &ctx,
        &input_ids,
        &attention_mask,
        1,
        infer_seq_len,
    );
    defer allocator.free(logits_again);

    for (0..infer_seq_len) |row_idx| {
        const row = logits[row_idx * NUM_CLASSES ..][0..NUM_CLASSES];
        const row_again = logits_again[row_idx * NUM_CLASSES ..][0..NUM_CLASSES];
        for (classifier_bias, 0..) |expected, class_idx| {
            try std.testing.expectApproxEqAbs(expected, row[class_idx], 1e-5);
            try std.testing.expectApproxEqAbs(row[class_idx], row_again[class_idx], 1e-6);
        }
        try std.testing.expectEqual(@as(usize, 4), argmax(row));
    }

    const out_dir = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_e2e_task_head_export_{d}", .{std.posix.system.getpid()});
    defer allocator.free(out_dir);
    compat.cwd().deleteTree(compat.io(), out_dir) catch {};
    try compat.cwd().createDirPath(compat.io(), out_dir);
    defer compat.cwd().deleteTree(compat.io(), out_dir) catch {};

    const weight_slot = regularParamSlot(&trainer, "classifier.weight") orelse return error.MissingClassifierHead;
    const bias_slot = regularParamSlot(&trainer, "classifier.bias") orelse return error.MissingClassifierHead;
    const task_head_params = [_]gliner2_bundle.AutodiffAdapterParam{
        .{
            .name = weight_slot.name,
            .dims = weight_slot.dims,
            .weights = weight_slot.weights,
        },
        .{
            .name = bias_slot.name,
            .dims = bias_slot.dims,
            .weights = bias_slot.weights,
        },
    };
    var exported_head = try gliner2_bundle.exportAutodiffRegularParamsAsSafetensors(allocator, out_dir, &task_head_params);
    defer gliner2_bundle.freeAutodiffRegularParamExportSummary(allocator, &exported_head);

    var reloaded_head = try gliner2_bundle.loadClassifierTaskHead(allocator, exported_head.checkpoint_path);
    defer reloaded_head.deinit();
    try std.testing.expectEqual(@as(usize, NUM_CLASSES), reloaded_head.num_classes);
    try std.testing.expectEqual(@as(usize, HIDDEN), reloaded_head.hidden_size);
    try std.testing.expectEqualSlices(f32, weight_slot.weights, reloaded_head.weight);
    try std.testing.expectEqualSlices(f32, bias_slot.weights, reloaded_head.bias);

    const zero_hidden = [_]f32{0.0} ** (2 * HIDDEN);
    const reloaded_logits = try reloaded_head.scoreRowsAlloc(allocator, &zero_hidden);
    defer allocator.free(reloaded_logits);
    for (0..2) |row_idx| {
        const row = reloaded_logits[row_idx * NUM_CLASSES ..][0..NUM_CLASSES];
        for (classifier_bias, 0..) |expected, class_idx| {
            try std.testing.expectApproxEqAbs(expected, row[class_idx], 1e-6);
        }
        try std.testing.expectEqual(@as(usize, 4), argmax(row));
    }
}

fn argmax(values: []const f32) usize {
    std.debug.assert(values.len > 0);
    var best_idx: usize = 0;
    var best = values[0];
    for (values[1..], 1..) |value, idx| {
        if (value > best) {
            best = value;
            best_idx = idx;
        }
    }
    return best_idx;
}

fn regularParamSlot(
    trainer: *const real_autodiff.RealAutodiffTrainer,
    name: []const u8,
) ?*const real_autodiff.RealAutodiffTrainer.ParamSlot {
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, name)) return slot;
    }
    return null;
}
