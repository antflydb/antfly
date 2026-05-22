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
// parses real NER annotations from a JSONL file, and runs 3 LoRA training
// steps through the full DeBERTa encoder + token-classification head.
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
const termite = @import("termite_internal");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

const deberta_graph = termite.architectures.deberta_graph;
const native_compute_mod = termite.native_compute.native;
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const ops_mod = termite.ops;
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const real_autodiff = termite.finetune.real_autodiff_trainer;
const gliner2_autodiff = termite.finetune.gliner2_real_autodiff;
const weight_source_mod = termite.models.weight_source;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = termite.backends.Tensor;

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

// ── Training dimensions (small for speed) ───────────────────────────────

const BATCH: u32 = 4;
const SEQ_LEN: u32 = 64;
const NUM_CLASSES: u32 = 5; // O + PER + ORG + LOC + MISC
const NUM_STEPS: usize = 3;

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

// ── JSONL NER data parsing ──────────────────────────────────────────────

const NerEntity = struct {
    label: []const u8,
    start: usize,
    end: usize,
};

const NerExample = struct {
    text: []const u8,
    entities: []NerEntity,
};

/// Parse NER examples from a JSONL file. Returns at most `max_examples`.
/// All strings are owned by the returned arena.
fn loadNerExamples(
    allocator: std.mem.Allocator,
    path: []const u8,
    max_examples: usize,
) !struct { examples: []NerExample, arena: std.heap.ArenaAllocator } {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    const content = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        path,
        aa,
        .limited(64 * 1024 * 1024),
    );

    var examples = std.ArrayListUnmanaged(NerExample).empty;

    var line_iter = std.mem.splitScalar(u8, content, '\n');
    while (line_iter.next()) |line| {
        if (line.len == 0) continue;
        if (examples.items.len >= max_examples) break;

        const parsed = std.json.parseFromSlice(std.json.Value, aa, line, .{}) catch continue;
        const obj = parsed.value.object;

        const text = blk: {
            const v = obj.get("text") orelse continue;
            break :blk switch (v) {
                .string => |s| s,
                else => continue,
            };
        };

        var entities = std.ArrayListUnmanaged(NerEntity).empty;
        if (obj.get("entities")) |ents_val| {
            if (ents_val == .array) {
                for (ents_val.array.items) |ent_val| {
                    if (ent_val != .object) continue;
                    const ent_obj = ent_val.object;
                    const label = switch (ent_obj.get("label") orelse continue) {
                        .string => |s| s,
                        else => continue,
                    };
                    const start: usize = switch (ent_obj.get("start") orelse continue) {
                        .integer => |i| @intCast(i),
                        else => continue,
                    };
                    const end: usize = switch (ent_obj.get("end") orelse continue) {
                        .integer => |i| @intCast(i),
                        else => continue,
                    };
                    try entities.append(aa, .{ .label = label, .start = start, .end = end });
                }
            }
        }

        try examples.append(aa, .{
            .text = text,
            .entities = try entities.toOwnedSlice(aa),
        });
    }

    return .{
        .examples = try examples.toOwnedSlice(aa),
        .arena = arena,
    };
}

// ── Batch buffer construction (same placeholder tokenization as CLI) ────

/// Build label-to-class-index mapping from examples.
/// Class 0 = O (no entity). Labels are assigned 1..NUM_CLASSES-1.
fn buildLabelMap(
    allocator: std.mem.Allocator,
    examples: []const NerExample,
) !std.StringHashMapUnmanaged(u32) {
    var label_map = std.StringHashMapUnmanaged(u32){};
    var next_class: u32 = 1;
    for (examples) |ex| {
        for (ex.entities) |ent| {
            if (label_map.get(ent.label) == null) {
                const cls = if (next_class < NUM_CLASSES) next_class else NUM_CLASSES - 1;
                try label_map.put(allocator, ent.label, cls);
                if (next_class < NUM_CLASSES) next_class += 1;
            }
        }
    }
    return label_map;
}

/// Fill input_ids, attention_mask, and one-hot targets for one batch.
/// Uses placeholder char-level tokenization (same as the CLI driver).
fn fillBatchBuffers(
    examples: []const NerExample,
    seq_len: u32,
    num_classes: u32,
    label_map: *const std.StringHashMapUnmanaged(u32),
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
) void {
    const sl: usize = seq_len;
    const nc: usize = num_classes;

    for (examples, 0..) |example, b| {
        const tok_offset = b * sl;
        const tgt_offset = b * sl * nc;

        // [CLS] token
        input_ids[tok_offset] = 101;
        attention_mask[tok_offset] = 1.0;
        var pos: usize = 1;

        var char_offsets_buf: [4096]usize = undefined;
        const char_offsets = char_offsets_buf[0..sl];
        char_offsets[0] = 0;

        for (example.text, 0..) |ch, ci| {
            if (pos >= sl - 1) break;
            input_ids[tok_offset + pos] = @as(i64, ch) + 1000;
            attention_mask[tok_offset + pos] = 1.0;
            char_offsets[pos] = ci;
            pos += 1;
        }

        // [SEP] token
        if (pos < sl) {
            input_ids[tok_offset + pos] = 102;
            attention_mask[tok_offset + pos] = 1.0;
            char_offsets[pos] = example.text.len;
            pos += 1;
        }

        const real_len = pos;

        // Pad remainder
        while (pos < sl) : (pos += 1) {
            input_ids[tok_offset + pos] = 0;
            attention_mask[tok_offset + pos] = 0.0;
            char_offsets[pos] = example.text.len;
        }

        // Zero the target region
        @memset(targets[tgt_offset .. tgt_offset + sl * nc], 0.0);

        // Build per-char entity class array
        var char_class_buf: [8192]u32 = undefined;
        const text_len = example.text.len;
        const max_char = @min(text_len, char_class_buf.len);
        @memset(char_class_buf[0..max_char], 0);

        for (example.entities) |ent| {
            const cls = label_map.get(ent.label) orelse 0;
            if (cls == 0) continue;
            const span_start = @min(ent.start, max_char);
            const span_end = @min(ent.end, max_char);
            for (span_start..span_end) |ci| {
                char_class_buf[ci] = cls;
            }
        }

        // Assign targets for each real token
        for (0..real_len) |p| {
            const row = tgt_offset + p * nc;
            if (p == 0 or p == real_len - 1) {
                targets[row] = 1.0; // [CLS]/[SEP] -> O class
                continue;
            }
            const ci = char_offsets[p];
            if (ci < max_char) {
                const cls = char_class_buf[ci];
                targets[row + cls] = 1.0;
            } else {
                targets[row] = 1.0; // O class
            }
        }
    }
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
    defer {
        for (hf_names) |n| allocator.free(n);
        allocator.free(hf_names);
    }

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
    // 3. Load NER training data
    // ----------------------------------------------------------------
    var ner_loaded = loadNerExamples(allocator, ner_data_path, BATCH) catch |err| {
        std.debug.print("SKIP: could not load NER data from {s}: {}\n", .{ ner_data_path, err });
        return; // skip test if data not available
    };
    defer ner_loaded.arena.deinit();

    const examples = ner_loaded.examples;
    if (examples.len == 0) {
        std.debug.print("SKIP: no NER examples loaded\n", .{});
        return;
    }

    std.debug.print("loaded {d} NER examples for training\n", .{examples.len});

    // Build label map from training data.
    var label_map = try buildLabelMap(allocator, examples);
    defer label_map.deinit(allocator);

    std.debug.print("entity labels: {d} (num_classes={d})\n", .{ label_map.count(), NUM_CLASSES });

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
    var padded_examples: [BATCH]NerExample = undefined;
    for (0..BATCH) |i| {
        padded_examples[i] = examples[i % examples.len];
    }

    fillBatchBuffers(
        &padded_examples,
        SEQ_LEN,
        NUM_CLASSES,
        &label_map,
        input_ids,
        attention_mask,
        targets_buf,
    );

    // ----------------------------------------------------------------
    // 5. Create GlinerAutodiffCtx + RealAutodiffTrainer
    // ----------------------------------------------------------------
    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = NUM_CLASSES,
    });

    const lora_targets = [_][]const u8{ "query_proj", "value_proj" };
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
        },
    );
    defer trainer.deinit();

    // ----------------------------------------------------------------
    // 6. Run training steps
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
    // 7. Assertions
    // ----------------------------------------------------------------

    // 7a. All losses must be finite (no NaN/Inf).
    for (losses, 0..) |l, i| {
        if (std.math.isNan(l) or std.math.isInf(l)) {
            std.debug.print("FAIL: loss at step {d} is not finite: {d}\n", .{ i, l });
            return error.NonFiniteLoss;
        }
    }

    // 7b. Loss at step 2 < loss at step 0 (training is learning).
    if (losses[NUM_STEPS - 1] >= losses[0]) {
        std.debug.print(
            "FAIL: loss did not decrease. step_0={d:.6}, step_{d}={d:.6}\nAll losses: ",
            .{ losses[0], NUM_STEPS - 1, losses[NUM_STEPS - 1] },
        );
        for (losses) |l| std.debug.print("{d:.6} ", .{l});
        std.debug.print("\n", .{});
        return error.LossDidNotDecrease;
    }

    // 7c. At least one LoRA B weight is non-zero after training.
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

    std.debug.print(
        "PASS: GLiNER2 real training -- {d} steps, loss {d:.6} -> {d:.6}, LoRA B weights updated\n",
        .{ NUM_STEPS, losses[0], losses[NUM_STEPS - 1] },
    );
}
