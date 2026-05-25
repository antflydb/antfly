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

// CLI training driver for GLiNER2 (DeBERTa-based NER) via the level-3
// real-autodiff path.
//
// Wires: CLI arg parsing → DeBERTa config loading → safetensors weight
// loading → JSONL data ingestion → GlinerAutodiffCtx + RealAutodiffTrainer
// step loop → LoRA adapter saving.
//
// Usage:
//   train-gliner2-autodiff --model-dir <path> --train-data <path> --out-dir <path> [options]
//
// Options:
//   --model-dir <path>           Directory with DeBERTa model (config.json + model.safetensors + tokenizer.json)
//   --train-data <path>          JSONL training data (file or directory)
//   --out-dir <path>             Output directory for saved LoRA adapters
//   --epochs <n>                 Number of training epochs (default: 3)
//   --batch-size <n>             Examples per step (default: 8)
//   --seq-len <n>                Max sequence length (default: 256)
//   --learning-rate <f>          Learning rate (default: 2e-5)
//   --lora-rank <n>              LoRA rank (default: 16)
//   --lora-alpha <f>             LoRA alpha scaling (default: 32)
//   --lora-targets <csv>         Target module patterns (default: "query_proj,value_proj")
//   --num-classes <n>            Entity classes including O (default: 5)
//   --max-examples <n>           Cap on training examples (0 = all, default: 0)
//   --max-grad-norm <f>          Gradient clipping norm (default: 1.0)
//   --grad-accum <n>             Gradient accumulation steps (default: 1)
//   --seed <n>                   RNG seed (default: 42)

const std = @import("std");
const termite = @import("termite_internal");
const ml = @import("ml");
const native_compute = termite.native_compute.native;
const compat = termite.io.compat;
const weight_source_mod = termite.models.weight_source;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const Tensor = termite.backends.Tensor;

// Finetune module imports — accessed via the termite internal module tree.
const gliner2_data = termite.finetune.gliner2_data;
const gliner2_autodiff = termite.finetune.gliner2_real_autodiff;
const real_autodiff = termite.finetune.real_autodiff_trainer;
const deberta_graph = @import("../../architectures/deberta_graph.zig");

const print = std.debug.print;

// ---------------------------------------------------------------------------
// CLI options
// ---------------------------------------------------------------------------

const Options = struct {
    model_dir: []const u8,
    train_data: []const u8,
    out_dir: []const u8,
    epochs: u32 = 3,
    batch_size: u32 = 8,
    seq_len: u32 = 256,
    learning_rate: f32 = 2e-5,
    lora_rank: u32 = 16,
    lora_alpha: f32 = 32.0,
    lora_targets: []const u8 = "query_proj,value_proj",
    num_classes: u32 = 5,
    max_examples: usize = 0,
    max_grad_norm: f32 = 1.0,
    grad_accum: u32 = 1,
    seed: u64 = 42,
};

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next(); // skip binary name

    // -- Parse CLI args ----------------------------------------------------
    var model_dir: ?[]const u8 = null;
    var train_data: ?[]const u8 = null;
    var out_dir: ?[]const u8 = null;
    var epochs: u32 = 3;
    var batch_size: u32 = 8;
    var seq_len: u32 = 256;
    var learning_rate: f32 = 2e-5;
    var lora_rank: u32 = 16;
    var lora_alpha: f32 = 32.0;
    var lora_targets: []const u8 = "query_proj,value_proj";
    var num_classes: u32 = 5;
    var max_examples: usize = 0;
    var max_grad_norm: f32 = 1.0;
    var grad_accum: u32 = 1;
    var seed: u64 = 42;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--train-data")) {
            train_data = args.next() orelse return error.MissingTrainData;
        } else if (std.mem.eql(u8, arg, "--out-dir")) {
            out_dir = args.next() orelse return error.MissingOutDir;
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            const val = args.next() orelse return error.MissingEpochs;
            epochs = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            const val = args.next() orelse return error.MissingBatchSize;
            batch_size = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            const val = args.next() orelse return error.MissingSeqLen;
            seq_len = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--learning-rate") or std.mem.eql(u8, arg, "--lr")) {
            const val = args.next() orelse return error.MissingLearningRate;
            learning_rate = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lora-rank")) {
            const val = args.next() orelse return error.MissingLoraRank;
            lora_rank = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--lora-alpha")) {
            const val = args.next() orelse return error.MissingLoraAlpha;
            lora_alpha = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lora-targets")) {
            lora_targets = args.next() orelse return error.MissingLoraTargets;
        } else if (std.mem.eql(u8, arg, "--num-classes")) {
            const val = args.next() orelse return error.MissingNumClasses;
            num_classes = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const val = args.next() orelse return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            const val = args.next() orelse return error.MissingMaxGradNorm;
            max_grad_norm = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            const val = args.next() orelse return error.MissingGradAccum;
            grad_accum = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            const val = args.next() orelse return error.MissingSeed;
            seed = try std.fmt.parseUnsigned(u64, val, 10);
        } else {
            print("error: unknown argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArguments;
        }
    }

    const opts = Options{
        .model_dir = model_dir orelse {
            print("error: --model-dir is required\n", .{});
            printUsage();
            return error.InvalidArguments;
        },
        .train_data = train_data orelse {
            print("error: --train-data is required\n", .{});
            printUsage();
            return error.InvalidArguments;
        },
        .out_dir = out_dir orelse {
            print("error: --out-dir is required\n", .{});
            printUsage();
            return error.InvalidArguments;
        },
        .epochs = epochs,
        .batch_size = batch_size,
        .seq_len = seq_len,
        .learning_rate = learning_rate,
        .lora_rank = lora_rank,
        .lora_alpha = lora_alpha,
        .lora_targets = lora_targets,
        .num_classes = num_classes,
        .max_examples = max_examples,
        .max_grad_norm = max_grad_norm,
        .grad_accum = grad_accum,
        .seed = seed,
    };

    try runTraining(allocator, opts);
}

// ---------------------------------------------------------------------------
// Core training routine
// ---------------------------------------------------------------------------

fn runTraining(allocator: std.mem.Allocator, opts: Options) !void {
    // ------------------------------------------------------------------
    // 1. Create output directory
    // ------------------------------------------------------------------
    try compat.cwd().createDirPath(compat.io(), opts.out_dir);

    print("train-gliner2-autodiff\n  model_dir={s}\n  train_data={s}\n  out_dir={s}\n", .{
        opts.model_dir,
        opts.train_data,
        opts.out_dir,
    });
    print("  epochs={d} batch_size={d} seq_len={d} lr={e:.6}\n", .{
        opts.epochs,
        opts.batch_size,
        opts.seq_len,
        opts.learning_rate,
    });
    print("  lora_rank={d} lora_alpha={d:.1} lora_targets={s}\n", .{
        opts.lora_rank,
        opts.lora_alpha,
        opts.lora_targets,
    });
    print("  num_classes={d} seed={d} max_grad_norm={d:.2} grad_accum={d}\n", .{
        opts.num_classes,
        opts.seed,
        opts.max_grad_norm,
        opts.grad_accum,
    });

    // ------------------------------------------------------------------
    // 2. Load DeBERTa config — GLiNER2 stores the encoder config under
    //    encoder_config/config.json, falling back to config.json.
    // ------------------------------------------------------------------
    var config_path_buf: [512]u8 = undefined;
    const encoder_config_path = try std.fmt.bufPrint(&config_path_buf, "{s}/encoder_config/config.json", .{opts.model_dir});
    const config_bytes = compat.cwd().readFileAlloc(compat.io(), encoder_config_path, allocator, .limited(8 * 1024 * 1024)) catch blk: {
        var fallback_buf: [512]u8 = undefined;
        const fallback_path = try std.fmt.bufPrint(&fallback_buf, "{s}/config.json", .{opts.model_dir});
        break :blk try compat.cwd().readFileAlloc(compat.io(), fallback_path, allocator, .limited(8 * 1024 * 1024));
    };
    defer allocator.free(config_bytes);

    const deberta_config = try parseDebertaConfig(allocator, config_bytes);
    print("  deberta: hidden={d} layers={d} heads={d} vocab={d}\n", .{
        deberta_config.hidden_size,
        deberta_config.num_hidden_layers,
        deberta_config.num_attention_heads,
        deberta_config.vocab_size,
    });

    // ------------------------------------------------------------------
    // 3. Set up compute backend + load weights.
    // ------------------------------------------------------------------
    var st_path_buf: [512]u8 = undefined;
    const st_path = try std.fmt.bufPrint(&st_path_buf, "{s}/model.safetensors", .{opts.model_dir});

    var native_ws: native_compute.WeightStore = undefined;
    var native_backend: native_compute.NativeCompute = undefined;

    // SafetensorsSource kept alive for native path (mmap'd data).
    var safetensors_source: ?*SafetensorsSource = null;
    defer if (safetensors_source) |s| s.weightSource().deinit();

    const cb = blk: {
        native_ws = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        };

        if (SafetensorsSource.initAbsolute(allocator, st_path)) |src| {
            safetensors_source = src;
            const ws = src.weightSource();
            if (ws.listNames(allocator)) |names| {
                defer allocator.free(names);
                var loaded_count: usize = 0;
                for (names) |name| {
                    if (ws.getTensor(name)) |lw| {
                        const stripped = stripEncoderPrefix(name);
                        const owned_name = try allocator.dupe(u8, stripped);
                        errdefer allocator.free(owned_name);
                        try native_ws.resident_weights.put(allocator, owned_name, lw);
                        loaded_count += 1;
                    } else |_| {}
                }
                print("  loaded {d} weights (native) from {s}\n", .{ loaded_count, st_path });
            } else |err| {
                print("warning: could not list weights: {}\n", .{err});
            }
        } else |err| {
            return err;
        }

        // Initialize classifier head.
        {
            var rng_init = std.Random.DefaultPrng.init(opts.seed);
            var prng_init = rng_init.random();
            const H = deberta_config.hidden_size;
            const C = opts.num_classes;

            const w_data = try allocator.alloc(f32, C * H);
            const sd: f32 = 0.02;
            for (w_data) |*v| v.* = prng_init.floatNorm(f32) * sd;
            const w_tensor = try Tensor.initFloat32(allocator, "classifier.weight", &.{ C, H }, w_data);
            allocator.free(w_data);
            try native_ws.resident_weights.put(allocator, try allocator.dupe(u8, "classifier.weight"), .{ .tensor = w_tensor });

            const b_data = try allocator.alloc(f32, C);
            @memset(b_data, 0.0);
            const b_tensor = try Tensor.initFloat32(allocator, "classifier.bias", &.{C}, b_data);
            allocator.free(b_data);
            try native_ws.resident_weights.put(allocator, try allocator.dupe(u8, "classifier.bias"), .{ .tensor = b_tensor });
            print("  initialized classifier head (native): [{d}, {d}] + [{d}]\n", .{ C, H, C });
        }

        native_backend = native_compute.NativeCompute.init(allocator, &native_ws, null);
        break :blk native_backend.computeBackend();
    };

    print("  backend: native CPU/BLAS\n", .{});

    // ------------------------------------------------------------------
    // 5. Load training data (JSONL with text + entities)
    // ------------------------------------------------------------------
    var train_loaded = try gliner2_data.loadExamples(allocator, opts.train_data, null);
    defer train_loaded.deinit();

    var examples = train_loaded.examples;
    if (opts.max_examples > 0 and examples.len > opts.max_examples) {
        examples = examples[0..opts.max_examples];
    }

    const stats = try gliner2_data.computeStats(allocator, examples);
    print("  training examples: {d} (avg_chars={d:.1}, avg_entities={d:.2}, unique_labels={d})\n", .{
        stats.num_examples,
        stats.avg_text_chars,
        stats.avg_entities,
        stats.unique_labels,
    });

    if (examples.len == 0) {
        print("error: no training examples loaded\n", .{});
        return error.NoTrainingData;
    }

    // ------------------------------------------------------------------
    // 6. Build a label-to-class-index mapping from the training data
    // ------------------------------------------------------------------
    // Class 0 is always the "O" (no entity) class. Entity labels seen in
    // the data are assigned indices 1..num_classes-1 in order of first
    // appearance; any labels beyond (num_classes - 1) are clamped to the
    // last slot. This keeps the mapping deterministic without requiring
    // the user to provide an explicit label list.
    var label_map = std.StringHashMapUnmanaged(u32){};
    defer label_map.deinit(allocator);
    var next_class: u32 = 1; // 0 = O
    for (examples) |ex| {
        for (ex.entities) |ent| {
            if (label_map.get(ent.label) == null) {
                const cls = if (next_class < opts.num_classes) next_class else opts.num_classes - 1;
                try label_map.put(allocator, ent.label, cls);
                if (next_class < opts.num_classes) next_class += 1;
            }
        }
    }
    // Build ordered entity type list for the tokenizer prompt.
    var entity_types = std.ArrayListUnmanaged([]const u8).empty;
    defer entity_types.deinit(allocator);
    {
        var it = label_map.iterator();
        while (it.next()) |entry| {
            try entity_types.append(allocator, entry.key_ptr.*);
        }
    }
    print("  entity labels mapped: {d} (num_classes={d})\n", .{ label_map.count(), opts.num_classes });

    // ------------------------------------------------------------------
    // 6b. Initialize the HF tokenizer for proper DeBERTa-v3 encoding
    // ------------------------------------------------------------------
    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, opts.model_dir);
    defer tokenizer.deinit(allocator);
    print("  tokenizer: vocab_size={d} cls={d} sep={d} ent={d} sep_text={d}\n", .{
        tokenizer.vocab_size,
        tokenizer.cls_id,
        tokenizer.sep_id,
        tokenizer.ent_id,
        tokenizer.sep_text_token_id,
    });

    // ------------------------------------------------------------------
    // 7. Parse LoRA target patterns
    // ------------------------------------------------------------------
    var target_patterns = std.ArrayListUnmanaged([]const u8).empty;
    defer target_patterns.deinit(allocator);
    {
        var iter = std.mem.tokenizeScalar(u8, opts.lora_targets, ',');
        while (iter.next()) |tok| {
            try target_patterns.append(allocator, std.mem.trim(u8, tok, " "));
        }
    }

    // ------------------------------------------------------------------
    // 8. Build the DeBERTa graph config + GlinerAutodiffCtx
    // ------------------------------------------------------------------
    const graph_config = deberta_graph.Config{
        .vocab_size = deberta_config.vocab_size,
        .hidden_size = deberta_config.hidden_size,
        .num_hidden_layers = deberta_config.num_hidden_layers,
        .num_attention_heads = deberta_config.num_attention_heads,
        .intermediate_size = deberta_config.intermediate_size,
        .max_position_embeddings = deberta_config.max_position_embeddings,
        .position_buckets = deberta_config.position_buckets,
    };

    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = opts.num_classes,
    });

    // ------------------------------------------------------------------
    // 9. Initialize the RealAutodiffTrainer
    // ------------------------------------------------------------------
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = opts.lora_rank,
        .alpha = opts.lora_alpha,
        .target_patterns = target_patterns.items,
    };

    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = lora_config,
            .optimizer = .{},
            .lr_schedule = .{ .constant = opts.learning_rate },
            .max_grad_norm = opts.max_grad_norm,
            .grad_accum_steps = opts.grad_accum,
            .hidden_size_hint = deberta_config.hidden_size,
            .num_layers_hint = deberta_config.num_hidden_layers,
            .seed = opts.seed,
        },
    );
    defer trainer.deinit();

    // ------------------------------------------------------------------
    // 10. Training loop
    // ------------------------------------------------------------------
    const total_examples = examples.len;
    const steps_per_epoch = (total_examples + opts.batch_size - 1) / opts.batch_size;

    print("\nStarting training: {d} epochs x {d} steps/epoch ({d} examples)\n", .{
        opts.epochs,
        steps_per_epoch,
        total_examples,
    });

    // Pre-allocate batch buffers.
    const sl: usize = opts.seq_len;
    const bs: usize = opts.batch_size;
    const nc: usize = opts.num_classes;
    const batch_tokens = bs * sl;

    var input_ids = try allocator.alloc(i64, batch_tokens);
    defer allocator.free(input_ids);
    var attention_mask = try allocator.alloc(f32, batch_tokens);
    defer allocator.free(attention_mask);
    // Targets: one-hot [batch * seq_len, num_classes]
    var targets_buf = try allocator.alloc(f32, batch_tokens * nc);
    defer allocator.free(targets_buf);

    var rng = std.Random.DefaultPrng.init(opts.seed);
    var prng = rng.random();

    var cumulative_loss: f64 = 0.0;
    var total_steps: u64 = 0;

    for (0..opts.epochs) |epoch| {
        // Shuffle examples at the start of each epoch.
        prng.shuffle(gliner2_data.Example, examples);

        var epoch_loss: f64 = 0.0;
        var epoch_steps: u64 = 0;

        var batch_start: usize = 0;
        while (batch_start < total_examples) {
            const batch_end = @min(batch_start + bs, total_examples);
            const actual_batch: u32 = @intCast(batch_end - batch_start);
            const ab: usize = actual_batch;

            // Tokenize batch + build entity targets.
            fillBatchBuffers(
                allocator,
                &tokenizer,
                entity_types.items,
                examples[batch_start..batch_end],
                opts.seq_len,
                opts.num_classes,
                &label_map,
                input_ids,
                attention_mask,
                targets_buf,
            );

            // Build TrainerInput via the GLiNER2 convenience builder.
            const targets_shape = gliner2_autodiff.tokenTargetsShape(
                actual_batch,
                opts.seq_len,
                opts.num_classes,
            );

            const trainer_input = gliner2_autodiff.makeTrainerInput(
                &gliner_ctx,
                input_ids[0 .. ab * sl],
                attention_mask[0 .. ab * sl],
                targets_buf[0 .. ab * sl * nc],
                targets_shape,
                actual_batch,
                opts.seq_len,
            );

            const result = try trainer.step(trainer_input);
            epoch_loss += result.loss;
            epoch_steps += 1;
            total_steps += 1;

            if (total_steps % 10 == 0 or batch_end >= total_examples) {
                print("  [epoch {d}/{d}] step {d}/{d}  loss={d:.6}  grad_norm={d:.4}{s}\n", .{
                    epoch + 1,
                    opts.epochs,
                    epoch_steps,
                    steps_per_epoch,
                    result.loss,
                    result.grad_norm,
                    if (result.optimizer_stepped) "" else " (accum)",
                });
            }

            batch_start = batch_end;
        }

        const avg_epoch_loss = if (epoch_steps > 0) epoch_loss / @as(f64, @floatFromInt(epoch_steps)) else 0.0;
        cumulative_loss += avg_epoch_loss;

        // -- End-of-epoch evaluation summary --------------------------------
        // Cross-entropy loss is the primary eval metric for token
        // classification. Loss ≈ -log(p_correct), so:
        //   loss=0.5 → ~61% accuracy, loss=0.1 → ~90%, loss=0.01 → ~99%
        const approx_acc: f64 = @exp(-avg_epoch_loss) * 100.0;
        var gold_ent_count: u64 = 0;
        for (examples) |ex| gold_ent_count += ex.entities.len;

        print("  epoch {d}/{d} complete -- avg_loss={d:.6}  ~acc={d:.1}%  ({d} gold entities)\n", .{
            epoch + 1,
            opts.epochs,
            avg_epoch_loss,
            approx_acc,
            gold_ent_count,
        });
    }

    // ------------------------------------------------------------------
    // 11. Save adapters
    // ------------------------------------------------------------------
    try trainer.saveAdapters(opts.out_dir);
    print("\nLoRA adapters saved to {s}\n", .{opts.out_dir});

    const final_avg = if (opts.epochs > 0) cumulative_loss / @as(f64, @floatFromInt(opts.epochs)) else 0.0;
    print("training complete -- {d} total steps, final avg loss={d:.6}\n", .{ total_steps, final_avg });
}

// ---------------------------------------------------------------------------
// DeBERTa config parsing
// ---------------------------------------------------------------------------

/// Subset of DeBERTa config.json fields needed for the graph builder.
const DebertaJsonConfig = struct {
    vocab_size: u32 = 128100,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    max_position_embeddings: u32 = 512,
    position_buckets: u32 = 256,
};

fn parseDebertaConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !DebertaJsonConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = DebertaJsonConfig{};

    if (obj.get("vocab_size")) |v| config.vocab_size = jsonU32(v) orelse config.vocab_size;
    if (obj.get("hidden_size")) |v| config.hidden_size = jsonU32(v) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |v| config.num_hidden_layers = jsonU32(v) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |v| config.num_attention_heads = jsonU32(v) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |v| config.intermediate_size = jsonU32(v) orelse config.intermediate_size;
    if (obj.get("max_position_embeddings")) |v| config.max_position_embeddings = jsonU32(v) orelse config.max_position_embeddings;
    if (obj.get("position_buckets")) |v| config.position_buckets = jsonU32(v) orelse config.position_buckets;

    return config;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

// ---------------------------------------------------------------------------
// Batch buffer construction (real HF tokenizer + entity targets)
// ---------------------------------------------------------------------------

/// Fills input_ids, attention_mask, and one-hot targets for one batch
/// using the real DeBERTa-v3 Unigram tokenizer with the GLiNER2 HF
/// prompt format: [P] entity_types... [E] [SEP_TEXT] text_tokens...
///
/// Entity annotations map to word-level positions via the tokenizer's
/// `words_mask` / `first_token_positions` outputs, then to one-hot
/// targets per token position.
fn fillBatchBuffers(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    batch_examples: []const gliner2_data.Example,
    seq_len: u32,
    num_classes: u32,
    label_map: *const std.StringHashMapUnmanaged(u32),
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
) void {
    const sl: usize = seq_len;
    const nc: usize = num_classes;

    // Scratch buffers for the tokenizer (i32 outputs).
    var tok_ids_buf: [4096]i32 = undefined;
    var tok_mask_buf: [4096]i32 = undefined;
    var words_mask_buf: [4096]i32 = undefined;
    var first_pos_buf: [4096]i32 = undefined;
    var e_tok_pos_buf: [128]i32 = undefined;
    var e_tok_end_buf: [128]i32 = undefined;

    for (batch_examples, 0..) |example, b| {
        const tok_offset = b * sl;
        const tgt_offset = b * sl * nc;

        // -- Tokenize with real HF tokenizer --------------------------------
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

        // Convert i32 token IDs → i64 and i32 mask → f32.
        for (0..sl) |p| {
            input_ids[tok_offset + p] = @as(i64, tok_ids[p]);
            attention_mask[tok_offset + p] = @as(f32, @floatFromInt(tok_mask[p]));
        }

        // -- Build word-level entity class map --------------------------------
        // Map entity byte spans to word indices, then to class IDs.
        // word_class[w] = class index for word w, or 0 (O) if no entity.
        var word_class_buf: [4096]u32 = undefined;
        const max_words = @min(num_words, word_class_buf.len);
        @memset(word_class_buf[0..max_words], 0);

        // Build a mapping from byte offset → word index by splitting
        // the text the same way the tokenizer does (whitespace split).
        var word_starts: [4096]usize = undefined;
        var word_ends: [4096]usize = undefined;
        var n_words: usize = 0;
        {
            var iter = std.mem.tokenizeAny(u8, example.text, " \t\r\n");
            while (iter.next()) |word| {
                if (n_words >= max_words) break;
                // Compute byte offset: iter.index points past the delimiter.
                const word_start = @intFromPtr(word.ptr) - @intFromPtr(example.text.ptr);
                word_starts[n_words] = word_start;
                word_ends[n_words] = word_start + word.len;
                n_words += 1;
            }
        }

        // For each entity, find overlapping words.
        for (example.entities) |ent| {
            const cls = label_map.get(ent.label) orelse 0;
            if (cls == 0) continue;
            for (0..n_words) |w| {
                // Word overlaps entity if [word_start, word_end) ∩ [ent.start, ent.end) ≠ ∅
                if (word_starts[w] < ent.end and word_ends[w] > ent.start) {
                    word_class_buf[w] = cls;
                }
            }
        }

        // -- Build one-hot targets from word classes --------------------------
        @memset(targets[tgt_offset .. tgt_offset + sl * nc], 0.0);

        for (0..sl) |p| {
            const row = tgt_offset + p * nc;
            const wm = words_mask[p];
            if (wm > 0 and @as(usize, @intCast(wm - 1)) < max_words) {
                // This token belongs to word (wm-1). Use its class.
                const cls = word_class_buf[@intCast(wm - 1)];
                targets[row + cls] = 1.0;
            } else if (tok_mask[p] != 0) {
                // Non-padding, non-word token (special tokens) → O class.
                targets[row] = 1.0;
            }
            // Padding (tok_mask==0): all-zero → contributes 0 to loss.
        }
    }
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

fn stripEncoderPrefix(name: []const u8) []const u8 {
    const prefix = "encoder.";
    if (std.mem.startsWith(u8, name, prefix)) {
        return name[prefix.len..];
    }
    return name;
}

fn printUsage() void {
    // Zig 0.16: std.debug.print requires a format tuple. For plain string
    // output, use a no-arg format with the text inlined.
    std.debug.print("{s}", .{
        \\usage: train-gliner2-autodiff --model-dir <path> --train-data <path> --out-dir <path> [options]
        \\
        \\required:
        \\  --model-dir <path>        Directory with DeBERTa model (config.json + model.safetensors)
        \\  --train-data <path>       JSONL training data with {text, entities} per line
        \\  --out-dir <path>          Output directory for LoRA adapter weights
        \\
        \\options:
        \\  --epochs <n>              Number of training epochs (default: 3)
        \\  --batch-size <n>          Examples per step (default: 8)
        \\  --seq-len <n>             Max sequence length (default: 256)
        \\  --learning-rate, --lr <f> Learning rate (default: 2e-5)
        \\  --lora-rank <n>           LoRA rank (default: 16)
        \\  --lora-alpha <f>          LoRA alpha scaling (default: 32)
        \\  --lora-targets <csv>      Target module patterns (default: query_proj,value_proj)
        \\  --num-classes <n>         Entity classes incl. O tag (default: 5)
        \\  --max-examples <n>        Cap on training examples (default: 0 = all)
        \\  --max-grad-norm <f>       Gradient clipping norm (default: 1.0)
        \\  --grad-accum <n>          Gradient accumulation steps (default: 1)
        \\  --seed <n>                RNG seed (default: 42)
        \\
        \\notes:
        \\  Tokenization uses a placeholder char-level encoding. For production
        \\  quality, wire gliner2_data.Tokenizer.initGLiNER2HF for proper Unigram
        \\  tokenization of the DeBERTa-v3 vocabulary.
        \\
        \\example:
        \\  train-gliner2-autodiff --model-dir /models/deberta-v3-base \
        \\    --train-data /data/ner/train.jsonl --out-dir /output/lora \
        \\    --epochs 5 --batch-size 16 --num-classes 7 --learning-rate 2e-5
        \\
    });
}
