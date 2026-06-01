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

// Real-world GLiNER2 training validation test (level-3 pipeline).
//
// Unlike the synthetic e2e tests that use tiny random weights, this test
// loads actual safetensors weights from a HuggingFace-cached GLiNER2 model,
// parses real NER annotations from a JSONL file, tokenizes with the HF
// GLiNER2 tokenizer/prompt path, and runs 2 LoRA training steps through the
// full DeBERTa encoder + token-classification head.
//
// The key integration point this proves:
//   - Safetensors loading with `encoder.` prefix stripping maps HF weight
//     names to our graph parameter names.
//   - Real 768-dim DeBERTa-v3 weights flow through the forward graph.
//   - LoRA injection + autodiff + optimizer produce finite, decreasing loss.
//   - At least one LoRA B weight is non-zero (optimizer actually updated).
//
// MODEL/DATA:
//   Set TERMITE_GLINER2_REAL_MODEL_DIR to a local fastino/gliner2-base-v1
//   directory and TERMITE_GLINER2_REAL_NER_JSONL to a JSONL file with
//   {text, entities} rows. The test skips when either variable is absent.
//
// NOTE: This test requires the full build module graph (`ml`, BLAS linkage).
// It will NOT compile standalone via `zig test`. Run via:
//   zig build test-gliner2-real-training
// with the environment variables above when you want the real-model gate.

const std = @import("std");
const ml = @import("ml");
const platform = @import("antfly_platform");
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
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const real_autodiff = inference.finetune.real_autodiff_trainer;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const gliner2_bundle = inference.finetune.gliner2;
const gliner2_data = inference.finetune.gliner2_data;
const weight_source_mod = inference.models.weight_source;
const safetensors = inference.models.safetensors;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = inference.backends.Tensor;
const compat = inference.io.compat;

const model_dir_env = "TERMITE_GLINER2_REAL_MODEL_DIR";
const ner_data_env = "TERMITE_GLINER2_REAL_NER_JSONL";

// ── DeBERTa config matching the encoder_config/config.json ──────────────

const graph_config = deberta_graph.Config{
    .vocab_size = 128011,
    .hidden_size = 768,
    .num_hidden_layers = 12,
    .num_attention_heads = 12,
    .intermediate_size = 3072,
    .max_position_embeddings = 512,
    .position_buckets = 256,
    .layer_norm_eps = 1e-7,
    .use_v3_names = true,
};

// ── Training dimensions (kept smoke-sized; native full-model autodiff is slow)

const BATCH: u32 = 1;
const SEQ_LEN: u32 = 64;
const NUM_CLASSES: u32 = 5; // O + PER + ORG + LOC + MISC
const NUM_STEPS: usize = 2;

// ── Prefix stripping for HF -> our graph parameter names ────────────────

/// Strip the leading `encoder.` prefix that HuggingFace GLiNER2 uses.
///
/// HF names:  encoder.embeddings.word_embeddings.weight
///            encoder.encoder.layer.0.attention.self.query_proj.weight
///
/// Our names: embeddings.word_embeddings.weight
///            encoder.layer.0.attention.self.query_proj.weight
fn stripEncoderPrefix(name: []const u8) []const u8 {
    const prefix = "encoder.";
    if (std.mem.startsWith(u8, name, prefix)) {
        return name[prefix.len..];
    }
    return name;
}

// ── Batch buffer construction (same HF tokenizer path as CLI) ───────────

/// Build label-to-class-index mapping from examples.
/// Class 0 = O (no entity). Labels are assigned 1..NUM_CLASSES-1.
fn buildLabelMap(
    allocator: std.mem.Allocator,
    entity_types: []const []const u8,
) !std.StringHashMapUnmanaged(u32) {
    var label_map = std.StringHashMapUnmanaged(u32){};
    errdefer label_map.deinit(allocator);
    if (entity_types.len + 1 > NUM_CLASSES) return error.TooManyEntityTypes;
    for (entity_types, 0..) |label, idx| {
        try label_map.put(allocator, label, @intCast(idx + 1));
    }
    return label_map;
}

const BatchTargetStats = struct {
    supervised_token_count: u64 = 0,
    entity_token_count: u64 = 0,
    ignored_token_count: u64 = 0,
};

/// Fill input_ids, attention_mask, and one-hot targets for one batch.
/// Uses GLiNER2's HF tokenizer/prompt path and word-level entity alignment.
fn fillBatchBuffers(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    examples: []const gliner2_data.Example,
    seq_len: u32,
    num_classes: u32,
    label_map: *const std.StringHashMapUnmanaged(u32),
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
) BatchTargetStats {
    const sl: usize = seq_len;
    const nc: usize = num_classes;
    var stats = BatchTargetStats{};

    var tok_ids_buf: [4096]i32 = undefined;
    var tok_mask_buf: [4096]i32 = undefined;
    var words_mask_buf: [4096]i32 = undefined;
    var first_pos_buf: [4096]i32 = undefined;
    var e_tok_pos_buf: [128]i32 = undefined;
    var e_tok_end_buf: [128]i32 = undefined;

    for (examples, 0..) |example, b| {
        const tok_offset = b * sl;
        const tgt_offset = b * sl * nc;

        const tok_ids = tok_ids_buf[0..sl];
        const tok_mask = tok_mask_buf[0..sl];
        const words_mask = words_mask_buf[0..sl];
        const first_pos = first_pos_buf[0..sl];
        const e_pos = e_tok_pos_buf[0..@min(entity_types.len, e_tok_pos_buf.len)];
        const e_end = e_tok_end_buf[0..@min(entity_types.len, e_tok_end_buf.len)];

        const result = tokenizer.encodeInto(
            allocator,
            example.text,
            entity_types,
            tok_ids,
            tok_mask,
            words_mask,
            first_pos,
            e_pos,
            e_end,
        );
        const num_words = result.num_words;

        for (0..sl) |p| {
            input_ids[tok_offset + p] = @as(i64, tok_ids[p]);
            attention_mask[tok_offset + p] = @as(f32, @floatFromInt(tok_mask[p]));
        }

        @memset(targets[tgt_offset .. tgt_offset + sl * nc], 0.0);

        var word_class_buf: [4096]u32 = undefined;
        const max_words = @min(num_words, word_class_buf.len);
        @memset(word_class_buf[0..max_words], 0);

        var word_starts: [4096]usize = undefined;
        var word_ends: [4096]usize = undefined;
        var n_words: usize = 0;
        var iter = std.mem.tokenizeAny(u8, example.text, " \t\r\n");
        while (iter.next()) |word| {
            if (n_words >= max_words) break;
            const word_start = @intFromPtr(word.ptr) - @intFromPtr(example.text.ptr);
            word_starts[n_words] = word_start;
            word_ends[n_words] = word_start + word.len;
            n_words += 1;
        }

        for (example.entities) |ent| {
            const cls = label_map.get(ent.label) orelse 0;
            if (cls == 0) continue;
            for (0..n_words) |w| {
                if (word_starts[w] < ent.end and word_ends[w] > ent.start) {
                    word_class_buf[w] = cls;
                }
            }
        }

        for (0..sl) |p| {
            const row = tgt_offset + p * nc;
            const wm = words_mask[p];
            if (wm > 0 and @as(usize, @intCast(wm - 1)) < max_words) {
                const cls = word_class_buf[@intCast(wm - 1)];
                targets[row + cls] = 1.0;
                stats.supervised_token_count += 1;
                if (cls > 0) stats.entity_token_count += 1;
            } else if (tok_mask[p] != 0) {
                targets[row] = 1.0;
                stats.supervised_token_count += 1;
            } else {
                stats.ignored_token_count += 1;
            }
        }
    }

    return stats;
}

const FixedTextTopEntity = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    score: f32,
};

fn decodeFixedTextTopEntity(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    label_map: *const std.StringHashMapUnmanaged(u32),
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !FixedTextTopEntity {
    const fixed_examples = [_]gliner2_data.Example{
        .{ .text = "Alice joined Acme in Paris", .entities = &.{} },
    };
    var infer_input_ids: [SEQ_LEN]i64 = undefined;
    var infer_attention_mask: [SEQ_LEN]f32 = undefined;
    var infer_targets: [SEQ_LEN * NUM_CLASSES]f32 = undefined;
    const infer_stats = fillBatchBuffers(
        allocator,
        tokenizer,
        entity_types,
        &fixed_examples,
        SEQ_LEN,
        NUM_CLASSES,
        label_map,
        &infer_input_ids,
        &infer_attention_mask,
        &infer_targets,
    );
    if (infer_stats.supervised_token_count == 0) return error.NoSupervisedTokens;

    const infer_logits = try gliner2_autodiff.tokenLogitsForBatch(
        allocator,
        trainer,
        gliner_ctx,
        &infer_input_ids,
        &infer_attention_mask,
        BATCH,
        SEQ_LEN,
    );
    defer allocator.free(infer_logits);
    try std.testing.expectEqual(@as(usize, SEQ_LEN * NUM_CLASSES), infer_logits.len);

    const infer_logits_again = try gliner2_autodiff.tokenLogitsForBatch(
        allocator,
        trainer,
        gliner_ctx,
        &infer_input_ids,
        &infer_attention_mask,
        BATCH,
        SEQ_LEN,
    );
    defer allocator.free(infer_logits_again);

    for (infer_logits, infer_logits_again, 0..) |value, repeated, idx| {
        if (!std.math.isFinite(value)) {
            std.debug.print("FAIL: non-finite inference logit at index {d}: {d}\n", .{ idx, value });
            return error.NonFiniteLogit;
        }
        try std.testing.expectApproxEqAbs(value, repeated, 1e-5);
    }

    var decoded_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        tokenizer,
        &fixed_examples,
        entity_types,
        @intCast(SEQ_LEN),
        4,
        @intCast(BATCH),
    );
    defer decoded_batch.deinit();
    const span_scores = try gliner2_data.tokenLogitsToSpanScoresAlloc(allocator, &decoded_batch, infer_logits, @intCast(NUM_CLASSES));
    defer allocator.free(span_scores);
    const span_scores_again = try gliner2_data.tokenLogitsToSpanScoresAlloc(allocator, &decoded_batch, infer_logits_again, @intCast(NUM_CLASSES));
    defer allocator.free(span_scores_again);
    try std.testing.expectEqual(span_scores.len, span_scores_again.len);
    var max_span_score: f32 = 0.0;
    for (span_scores, span_scores_again, 0..) |score, repeated, idx| {
        if (!std.math.isFinite(score)) {
            std.debug.print("FAIL: non-finite span score at index {d}: {d}\n", .{ idx, score });
            return error.NonFiniteSpanScore;
        }
        try std.testing.expectApproxEqAbs(score, repeated, 1e-5);
        max_span_score = @max(max_span_score, score);
    }
    if (max_span_score <= 0.0) return error.NoPositiveSpanScores;

    const threshold = @max(@as(f32, 0.0), max_span_score - 1e-6);
    const entity_predictions = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &decoded_batch,
        &fixed_examples,
        entity_types,
        span_scores,
        threshold,
    );
    defer allocator.free(entity_predictions);
    const entity_predictions_again = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &decoded_batch,
        &fixed_examples,
        entity_types,
        span_scores_again,
        threshold,
    );
    defer allocator.free(entity_predictions_again);
    try std.testing.expect(entity_predictions.len > 0);
    try std.testing.expectEqual(entity_predictions.len, entity_predictions_again.len);
    for (entity_predictions, entity_predictions_again) |pred, repeated| {
        try std.testing.expectEqual(pred.sample_index, repeated.sample_index);
        try std.testing.expectEqual(pred.span_index, repeated.span_index);
        try std.testing.expectEqual(pred.word_start, repeated.word_start);
        try std.testing.expectEqual(pred.word_end, repeated.word_end);
        try std.testing.expectEqual(pred.start, repeated.start);
        try std.testing.expectEqual(pred.end, repeated.end);
        try std.testing.expectEqual(pred.entity_type_index, repeated.entity_type_index);
        try std.testing.expectEqualStrings(pred.text, repeated.text);
        try std.testing.expectEqualStrings(pred.label, repeated.label);
        try std.testing.expectApproxEqAbs(pred.score, repeated.score, 1e-5);
    }

    return .{
        .text = entity_predictions[0].text,
        .label = entity_predictions[0].label,
        .start = entity_predictions[0].start,
        .end = entity_predictions[0].end,
        .score = entity_predictions[0].score,
    };
}

fn ensureFixedTextGraphBuilt(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    label_map: *const std.StringHashMapUnmanaged(u32),
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !void {
    const fixed_examples = [_]gliner2_data.Example{
        .{ .text = "Alice joined Acme in Paris", .entities = &.{} },
    };
    var infer_input_ids: [SEQ_LEN]i64 = undefined;
    var infer_attention_mask: [SEQ_LEN]f32 = undefined;
    var infer_targets: [SEQ_LEN * NUM_CLASSES]f32 = undefined;
    const infer_stats = fillBatchBuffers(
        allocator,
        tokenizer,
        entity_types,
        &fixed_examples,
        SEQ_LEN,
        NUM_CLASSES,
        label_map,
        &infer_input_ids,
        &infer_attention_mask,
        &infer_targets,
    );
    if (infer_stats.supervised_token_count == 0) return error.NoSupervisedTokens;

    const trainer_input = gliner2_autodiff.makeTrainerInput(
        gliner_ctx,
        &infer_input_ids,
        &infer_attention_mask,
        &infer_targets,
        gliner2_autodiff.tokenTargetsShape(BATCH, SEQ_LEN, NUM_CLASSES),
        BATCH,
        SEQ_LEN,
    );
    try trainer.ensureGraphBuilt(trainer_input);
}

fn collectAutodiffAdapterParams(
    allocator: std.mem.Allocator,
    trainer: *const real_autodiff.RealAutodiffTrainer,
) ![]gliner2_bundle.AutodiffAdapterParam {
    const params = try allocator.alloc(gliner2_bundle.AutodiffAdapterParam, trainer.lora_params.items.len);
    for (trainer.lora_params.items, 0..) |slot, idx| {
        params[idx] = .{
            .name = slot.name,
            .dims = slot.dims,
            .weights = slot.weights,
        };
    }
    return params;
}

fn collectRegularTrainableParams(
    allocator: std.mem.Allocator,
    trainer: *const real_autodiff.RealAutodiffTrainer,
) ![]gliner2_bundle.AutodiffAdapterParam {
    const params = try allocator.alloc(gliner2_bundle.AutodiffAdapterParam, trainer.regular_params.items.len);
    for (trainer.regular_params.items, 0..) |slot, idx| {
        params[idx] = .{
            .name = slot.name,
            .dims = slot.dims,
            .weights = slot.weights,
        };
    }
    return params;
}

fn loadPeftAdaptersIntoTrainer(
    allocator: std.mem.Allocator,
    adapter_checkpoint_path: []const u8,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer reader.deinit();
    for (trainer.lora_params.items) |*slot| {
        const peft_name = try autodiffSlotNameToPeftName(allocator, slot.name);
        defer allocator.free(peft_name);
        var tensor = try reader.readTensor(peft_name);
        defer tensor.deinit();
        if (tensor.elementCount() != slot.weights.len) return error.AdapterTensorShapeMismatch;
        try copyTensorF32Into(slot.weights, &tensor);
        @memset(slot.grad_accum, 0.0);
    }
}

fn copyTensorF32Into(dst: []f32, tensor: *const Tensor) !void {
    if (tensor.dtype != .f32) return error.AdapterTensorDTypeMismatch;
    if (tensor.data.len != dst.len * @sizeOf(f32)) return error.AdapterTensorShapeMismatch;
    for (dst, 0..) |*value, idx| {
        const raw = tensor.data[idx * @sizeOf(f32) ..][0..@sizeOf(f32)];
        value.* = @bitCast(std.mem.readInt(u32, raw, .little));
    }
}

fn loadTaskHeadIntoTrainer(
    head: *const gliner2_bundle.ClassifierTaskHead,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, "classifier.weight")) {
            if (slot.weights.len != head.weight.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.weight);
            @memset(slot.grad_accum, 0.0);
        } else if (std.mem.eql(u8, slot.name, "classifier.bias")) {
            if (slot.weights.len != head.bias.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.bias);
            @memset(slot.grad_accum, 0.0);
        }
    }
}

fn autodiffSlotNameToPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        const base = name[0 .. name.len - ".lora_A".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_A");
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        const base = name[0 .. name.len - ".lora_B".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_B");
    }
    return error.InvalidAutodiffAdapterName;
}

fn autodiffBaseToPeftName(allocator: std.mem.Allocator, base_no_weight: []const u8, adapter_name: []const u8) ![]const u8 {
    if (std.mem.startsWith(u8, base_no_weight, "encoder.layer.")) {
        return std.fmt.allocPrint(allocator, "encoder.{s}.{s}.weight", .{ base_no_weight, adapter_name });
    }
    return std.fmt.allocPrint(allocator, "{s}.{s}.weight", .{ base_no_weight, adapter_name });
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    return tensor_name;
}

// ── The test ────────────────────────────────────────────────────────────

test "GLiNER2 real training: loss decreases on actual model weights" {
    const allocator = std.testing.allocator;
    const model_dir = platform.env.getenv(model_dir_env) orelse return error.SkipZigTest;
    const ner_data_path = platform.env.getenv(ner_data_env) orelse return error.SkipZigTest;
    const safetensors_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);

    // ----------------------------------------------------------------
    // 1. Load safetensors weights with encoder. prefix stripping
    // ----------------------------------------------------------------
    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };

    // Track which names we heap-allocated so we can free them.
    var owned_names = std.ArrayListUnmanaged([]const u8).empty;
    defer owned_names.deinit(allocator);

    defer {
        var it = weight_store.resident_weights.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        weight_store.resident_weights.deinit(allocator);
        for (owned_names.items) |n| allocator.free(n);
    }

    // Open the safetensors file via the existing SafetensorsSource.
    const source_ptr = SafetensorsSource.initAbsolute(allocator, safetensors_path) catch |err| {
        std.debug.print("SKIP: could not open safetensors file at {s}: {}\n", .{ safetensors_path, err });
        return; // skip test if model not available
    };
    var ws = source_ptr.weightSource();
    defer ws.deinit();

    const hf_names = try ws.listNames(allocator);
    defer allocator.free(hf_names);

    var loaded_count: usize = 0;
    var skipped_count: usize = 0;
    for (hf_names) |hf_name| {
        // Strip the `encoder.` prefix so HF names map to our graph names.
        const stripped = stripEncoderPrefix(hf_name);

        // Load the tensor from the safetensors file (using the original HF name).
        var lw = ws.getTensor(hf_name) catch |err| {
            std.debug.print("warning: could not load tensor '{s}': {}\n", .{ hf_name, err });
            skipped_count += 1;
            continue;
        };

        // Store under the stripped name. We need to own the name string.
        const owned_name = try allocator.dupe(u8, stripped);
        try owned_names.append(allocator, owned_name);

        // Update the tensor's name to match the stripped version.
        lw.tensor.name = owned_name;

        try weight_store.resident_weights.put(allocator, owned_name, lw);
        loaded_count += 1;
    }

    std.debug.print("loaded {d} weights ({d} skipped) from safetensors with prefix stripping\n", .{
        loaded_count,
        skipped_count,
    });

    // Sanity: we should have at least the embedding + all 12 layer weights.
    // DeBERTa-v3-base: 6 global + 16*12 layers = 198 encoder params.
    if (loaded_count < 100) {
        std.debug.print("FAIL: only loaded {d} weights, expected ~198+\n", .{loaded_count});
        return error.InsufficientWeights;
    }

    // Add classifier head weights (random init -- not in the base model).
    {
        const H: i64 = graph_config.hidden_size;
        const C: i64 = NUM_CLASSES;
        var prng = std.Random.DefaultPrng.init(12345);
        const rng = prng.random();

        // classifier.weight [C, H]
        {
            const n_elems: usize = @intCast(C * H);
            const data = try allocator.alloc(f32, n_elems);
            defer allocator.free(data);
            const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(n_elems)));
            for (data) |*v| v.* = (rng.float(f32) * 2.0 - 1.0) * scale;
            const name = try allocator.dupe(u8, "classifier.weight");
            try owned_names.append(allocator, name);
            const tensor = try Tensor.initFloat32(allocator, name, &.{ C, H }, data);
            try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
        }
        // classifier.bias [C]
        {
            const n_elems: usize = @intCast(C);
            const data = try allocator.alloc(f32, n_elems);
            defer allocator.free(data);
            @memset(data, 0.0);
            const name = try allocator.dupe(u8, "classifier.bias");
            try owned_names.append(allocator, name);
            const tensor = try Tensor.initFloat32(allocator, name, &.{C}, data);
            try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
        }
    }

    // ----------------------------------------------------------------
    // 2. Create compute backend
    // ----------------------------------------------------------------
    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    // ----------------------------------------------------------------
    // 3. Load NER training data + real GLiNER2 tokenizer
    // ----------------------------------------------------------------
    var ner_loaded = gliner2_data.loadExamples(allocator, ner_data_path, null) catch |err| {
        std.debug.print("SKIP: could not load NER data from {s}: {}\n", .{ ner_data_path, err });
        return; // skip test if data not available
    };
    defer ner_loaded.deinit();

    const examples = ner_loaded.examples;
    if (examples.len == 0) {
        std.debug.print("SKIP: no NER examples loaded\n", .{});
        return;
    }

    std.debug.print("loaded {d} NER examples for training\n", .{examples.len});

    const entity_types = try gliner2_data.buildLabelVocab(allocator, examples, null);
    defer {
        for (entity_types) |label| allocator.free(label);
        allocator.free(entity_types);
    }
    var label_map = try buildLabelMap(allocator, entity_types);
    defer label_map.deinit(allocator);

    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, model_dir);
    defer tokenizer.deinit(allocator);

    std.debug.print("entity labels: {d} (num_classes={d}) tokenizer_vocab={d}\n", .{ label_map.count(), NUM_CLASSES, tokenizer.vocab_size });

    // ----------------------------------------------------------------
    // 4. Prepare batch buffers
    // ----------------------------------------------------------------
    const batch_tokens: usize = @as(usize, BATCH) * @as(usize, SEQ_LEN);
    const nc: usize = NUM_CLASSES;

    var input_ids = try allocator.alloc(i64, batch_tokens);
    defer allocator.free(input_ids);
    var attention_mask = try allocator.alloc(f32, batch_tokens);
    defer allocator.free(attention_mask);
    var targets_buf = try allocator.alloc(f32, batch_tokens * nc);
    defer allocator.free(targets_buf);

    // Pad examples to BATCH size by repeating if needed.
    var padded_examples: [BATCH]gliner2_data.Example = undefined;
    for (0..BATCH) |i| {
        padded_examples[i] = examples[i % examples.len];
    }

    const target_stats = fillBatchBuffers(
        allocator,
        &tokenizer,
        entity_types,
        &padded_examples,
        SEQ_LEN,
        NUM_CLASSES,
        &label_map,
        input_ids,
        attention_mask,
        targets_buf,
    );
    if (target_stats.supervised_token_count == 0) return error.NoSupervisedTokens;
    if (target_stats.entity_token_count == 0) return error.NoEntityPositiveTokens;
    std.debug.print(
        "target stats: supervised={d} entity_positive={d} ignored={d}\n",
        .{ target_stats.supervised_token_count, target_stats.entity_token_count, target_stats.ignored_token_count },
    );

    // ----------------------------------------------------------------
    // 5. Create GlinerAutodiffCtx + RealAutodiffTrainer
    // ----------------------------------------------------------------
    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = NUM_CLASSES,
    });

    const lora_targets = [_][]const u8{ "query_proj", "value_proj" };
    const regular_trainable_params = [_][]const u8{ "classifier.weight", "classifier.bias" };
    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = .{
                .rank = 8,
                .alpha = 16.0,
                .target_patterns = &lora_targets,
            },
            .lr_schedule = .{ .constant = 1e-3 },
            .max_grad_norm = 1.0,
            .grad_accum_steps = 1,
            .lora_a_init_std = 0.02,
            .hidden_size_hint = graph_config.hidden_size,
            .num_layers_hint = graph_config.num_hidden_layers,
            .seed = 42,
            .regular_trainable_params = &regular_trainable_params,
        },
    );
    defer trainer.deinit();

    // ----------------------------------------------------------------
    // 6. Run fixed-text read-only inference before training
    // ----------------------------------------------------------------
    {
        const pre_train_top = try decodeFixedTextTopEntity(allocator, &tokenizer, entity_types, &label_map, &trainer, &gliner_ctx);
        try std.testing.expectEqual(@as(usize, 18), pre_train_top.start);
        try std.testing.expectEqual(@as(usize, 20), pre_train_top.end);
        try std.testing.expectEqualStrings("in", pre_train_top.text);
        try std.testing.expectEqualStrings("person", pre_train_top.label);
        try std.testing.expectApproxEqAbs(@as(f32, 0.251425), pre_train_top.score, 1e-4);
        std.debug.print(
            "fixed-text inference logits: rows={d} classes={d}; decoded top entity text='{s}' label='{s}' score={d:.6}\n",
            .{ SEQ_LEN, NUM_CLASSES, pre_train_top.text, pre_train_top.label, pre_train_top.score },
        );
    }

    // ----------------------------------------------------------------
    // 7. Run training steps
    // ----------------------------------------------------------------
    const targets_shape = gliner2_autodiff.tokenTargetsShape(BATCH, SEQ_LEN, NUM_CLASSES);
    const ab: usize = BATCH;
    const sl: usize = SEQ_LEN;

    var losses: [NUM_STEPS]f32 = undefined;

    for (0..NUM_STEPS) |step_i| {
        const trainer_input = gliner2_autodiff.makeTrainerInput(
            &gliner_ctx,
            input_ids[0 .. ab * sl],
            attention_mask[0 .. ab * sl],
            targets_buf[0 .. ab * sl * nc],
            targets_shape,
            BATCH,
            SEQ_LEN,
        );

        const result = try trainer.step(trainer_input);
        losses[step_i] = result.loss;
        std.debug.print("step {d}: loss={d:.6}  grad_norm={d:.4}\n", .{
            step_i,
            result.loss,
            result.grad_norm,
        });
    }

    // ----------------------------------------------------------------
    // 8. Assertions
    // ----------------------------------------------------------------

    // 8a. All losses must be finite (no NaN/Inf).
    for (losses, 0..) |l, i| {
        if (std.math.isNan(l) or std.math.isInf(l)) {
            std.debug.print("FAIL: loss at step {d} is not finite: {d}\n", .{ i, l });
            return error.NonFiniteLoss;
        }
    }

    // 8b. Loss at step 2 < loss at step 0 (training is learning).
    if (losses[NUM_STEPS - 1] >= losses[0]) {
        std.debug.print(
            "FAIL: loss did not decrease. step_0={d:.6}, step_{d}={d:.6}\nAll losses: ",
            .{ losses[0], NUM_STEPS - 1, losses[NUM_STEPS - 1] },
        );
        for (losses) |l| std.debug.print("{d:.6} ", .{l});
        std.debug.print("\n", .{});
        return error.LossDidNotDecrease;
    }

    // 8c. At least one LoRA B weight is non-zero after training.
    var found_nonzero_b = false;
    for (trainer.lora_params.items, 0..) |slot, idx| {
        if (idx % 2 == 1) { // B matrix (odd indices)
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

    // 8d. The classifier task head is a regular trainable parameter in this
    // production-aligned gate; the zero-initialized bias should move.
    try std.testing.expectEqual(@as(usize, 2), trainer.regular_params.items.len);
    var classifier_bias_updated = false;
    for (trainer.regular_params.items) |slot| {
        if (!std.mem.eql(u8, slot.name, "classifier.bias")) continue;
        for (slot.weights) |w| {
            if (w != 0.0) {
                classifier_bias_updated = true;
                break;
            }
        }
    }
    if (!classifier_bias_updated) return error.ClassifierBiasNotUpdated;

    // 8e. Run decoded fixed-text inference again after training so the
    // updated in-memory LoRA/task-head state is exercised through the same
    // entity-level postprocessing path.
    const post_train_top = try decodeFixedTextTopEntity(allocator, &tokenizer, entity_types, &label_map, &trainer, &gliner_ctx);
    if (!std.math.isFinite(post_train_top.score) or post_train_top.score <= 0.0) return error.NoPositiveSpanScores;
    std.debug.print(
        "post-training fixed-text decoded top entity text='{s}' label='{s}' score={d:.6}\n",
        .{ post_train_top.text, post_train_top.label, post_train_top.score },
    );

    // 8f. Export the trained PEFT adapter + task head, load them into a fresh
    // trainer, and require decoded inference to match the in-memory trained
    // state. This exercises the saved artifacts, not only trainer-owned
    // buffers.
    {
        const out_dir = try std.fmt.allocPrint(allocator, "/private/tmp/termite_gliner2_real_reload_{d}", .{std.posix.system.getpid()});
        defer allocator.free(out_dir);
        compat.cwd().deleteTree(compat.io(), out_dir) catch {};
        try compat.cwd().createDirPath(compat.io(), out_dir);
        defer compat.cwd().deleteTree(compat.io(), out_dir) catch {};

        const adapter_params = try collectAutodiffAdapterParams(allocator, &trainer);
        defer allocator.free(adapter_params);
        var peft_export = try gliner2_bundle.exportAutodiffAdaptersAsPeftBundle(
            allocator,
            out_dir,
            model_dir,
            8,
            16.0,
            &lora_targets,
            adapter_params,
        );
        defer gliner2_bundle.freeAutodiffAdapterExportSummary(allocator, &peft_export);

        const regular_params = try collectRegularTrainableParams(allocator, &trainer);
        defer allocator.free(regular_params);
        var regular_export = try gliner2_bundle.exportAutodiffRegularParamsAsSafetensors(allocator, out_dir, regular_params);
        defer gliner2_bundle.freeAutodiffRegularParamExportSummary(allocator, &regular_export);

        var reloaded_trainer = try real_autodiff.RealAutodiffTrainer.init(
            allocator,
            &cb,
            .{
                .lora = .{
                    .rank = 8,
                    .alpha = 16.0,
                    .target_patterns = &lora_targets,
                },
                .lr_schedule = .{ .constant = 1e-3 },
                .max_grad_norm = 1.0,
                .grad_accum_steps = 1,
                .lora_a_init_std = 0.02,
                .hidden_size_hint = graph_config.hidden_size,
                .num_layers_hint = graph_config.num_hidden_layers,
                .seed = 999,
                .regular_trainable_params = &regular_trainable_params,
            },
        );
        defer reloaded_trainer.deinit();
        var reloaded_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
            .graph_config = graph_config,
            .num_classes = NUM_CLASSES,
        });
        try ensureFixedTextGraphBuilt(allocator, &tokenizer, entity_types, &label_map, &reloaded_trainer, &reloaded_ctx);
        try loadPeftAdaptersIntoTrainer(allocator, peft_export.adapter_checkpoint_path, &reloaded_trainer);
        var reloaded_head = try gliner2_bundle.loadClassifierTaskHead(allocator, regular_export.checkpoint_path);
        defer reloaded_head.deinit();
        try loadTaskHeadIntoTrainer(&reloaded_head, &reloaded_trainer);

        const reloaded_top = try decodeFixedTextTopEntity(allocator, &tokenizer, entity_types, &label_map, &reloaded_trainer, &reloaded_ctx);
        try std.testing.expectEqual(post_train_top.start, reloaded_top.start);
        try std.testing.expectEqual(post_train_top.end, reloaded_top.end);
        try std.testing.expectEqualStrings(post_train_top.text, reloaded_top.text);
        try std.testing.expectEqualStrings(post_train_top.label, reloaded_top.label);
        try std.testing.expectApproxEqAbs(post_train_top.score, reloaded_top.score, 1e-5);
        std.debug.print(
            "reloaded fixed-text decoded top entity text='{s}' label='{s}' score={d:.6}\n",
            .{ reloaded_top.text, reloaded_top.label, reloaded_top.score },
        );
    }

    std.debug.print(
        "PASS: GLiNER2 real training -- {d} steps, loss {d:.6} -> {d:.6}, LoRA B weights and classifier bias updated\n",
        .{ NUM_STEPS, losses[0], losses[NUM_STEPS - 1] },
    );
}
