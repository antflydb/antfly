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

// CLI training driver for level-3 LoRA reranker training via the real-autodiff
// pipeline (RerankerTrainingMode.real_autodiff).
//
// This is the production entry point that wires: CLI arg parsing -> BERT config
// loading -> safetensors weight loading -> data ingestion -> RealAutodiffTrainer
// step loop -> LoRA adapter saving.
//
// Usage:
//   train-reranker-autodiff --model-dir <path> --train-data <path> --out-dir <path> [options]
//
// Options:
//   --model-dir <path>           Directory with base model weights + config.json
//   --train-data <path>          JSONL training data (file or directory)
//   --out-dir <path>             Output directory for saved LoRA adapters
//   --epochs <n>                 Number of training epochs (default: 3)
//   --batch-size <n>             Examples per step (default: 8)
//   --seq-len <n>                Max sequence length (default: 256)
//   --learning-rate <f>          Learning rate (default: 2e-5)
//   --lora-rank <n>              LoRA rank (default: 16)
//   --lora-alpha <f>             LoRA alpha scaling (default: 32)
//   --lora-targets <csv>         Target module patterns (default: "query,value")
//   --max-examples <n>           Cap on training examples (0 = all, default: 0)
//   --neftune-alpha <f>          NEFTune noise scale (default: 0, disabled)
//   --seed <n>                   RNG seed (default: 42)
//   --max-grad-norm <f>          Gradient clipping norm (default: 1.0)
//   --grad-accum <n>             Gradient accumulation steps (default: 1)

const std = @import("std");
const native_compute = @import("../../ops/native_compute.zig");
const bert_types = @import("../../models/bert.zig");
const bert_graph = @import("../../architectures/bert_graph.zig");
const reranker_train = @import("../reranker_train.zig");
const reranker_data = @import("../reranker_data.zig");
const real_autodiff = @import("../real_autodiff_trainer.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const compat = @import("../../io/compat.zig");
const ml = @import("ml");

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
    lora_targets: []const u8 = "query,value",
    max_examples: usize = 0,
    neftune_alpha: f32 = 0.0,
    seed: u64 = 42,
    max_grad_norm: f32 = 1.0,
    grad_accum: u32 = 1,
    train_split: ?[]const u8 = null,
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
    var lora_targets: []const u8 = "query,value";
    var max_examples: usize = 0;
    var neftune_alpha: f32 = 0.0;
    var seed: u64 = 42;
    var max_grad_norm: f32 = 1.0;
    var grad_accum: u32 = 1;
    var train_split: ?[]const u8 = null;

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
        } else if (std.mem.eql(u8, arg, "--learning-rate")) {
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
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const val = args.next() orelse return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--neftune-alpha")) {
            const val = args.next() orelse return error.MissingNeftuneAlpha;
            neftune_alpha = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            const val = args.next() orelse return error.MissingSeed;
            seed = try std.fmt.parseUnsigned(u64, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            const val = args.next() orelse return error.MissingMaxGradNorm;
            max_grad_norm = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            const val = args.next() orelse return error.MissingGradAccum;
            grad_accum = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--split")) {
            train_split = args.next() orelse return error.MissingSplit;
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
        .max_examples = max_examples,
        .neftune_alpha = neftune_alpha,
        .seed = seed,
        .max_grad_norm = max_grad_norm,
        .grad_accum = grad_accum,
        .train_split = train_split,
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

    print("train-reranker-autodiff\n  model_dir={s}\n  train_data={s}\n  out_dir={s}\n", .{
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
    print("  seed={d} max_grad_norm={d:.2} grad_accum={d}\n", .{
        opts.seed,
        opts.max_grad_norm,
        opts.grad_accum,
    });

    // ------------------------------------------------------------------
    // 2. Load BERT config from model-dir/config.json
    // ------------------------------------------------------------------
    var config_path_buf: [512]u8 = undefined;
    const config_path = try std.fmt.bufPrint(&config_path_buf, "{s}/config.json", .{opts.model_dir});
    const config_bytes = try compat.cwd().readFileAlloc(compat.io(), config_path, allocator, .limited(8 * 1024 * 1024));
    defer allocator.free(config_bytes);
    const bert_config = try bert_types.parseConfig(allocator, config_bytes);
    print("  bert: hidden={d} layers={d} heads={d} vocab={d}\n", .{
        bert_config.hidden_size,
        bert_config.num_hidden_layers,
        bert_config.num_attention_heads,
        bert_config.vocab_size,
    });

    // ------------------------------------------------------------------
    // 3. Set up native compute backend with weight store
    // ------------------------------------------------------------------
    var weight_store = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer {
        var it = weight_store.resident_weights.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        weight_store.resident_weights.deinit(allocator);
    }

    var native_backend = native_compute.NativeCompute.init(allocator, &weight_store, null);
    const cb = native_backend.computeBackend();

    // ------------------------------------------------------------------
    // 4. Load safetensors weights into the weight store
    // ------------------------------------------------------------------
    var st_path_buf: [512]u8 = undefined;
    const st_path = try std.fmt.bufPrint(&st_path_buf, "{s}/model.safetensors", .{opts.model_dir});
    if (SafetensorsSource.initAbsolute(allocator, st_path)) |src| {
        var source_ptr = src;
        defer source_ptr.weightSource().deinit();
        const ws = source_ptr.weightSource();
        if (ws.listNames(allocator)) |names| {
            defer {
                for (names) |n| allocator.free(n);
                allocator.free(names);
            }
            var loaded_count: usize = 0;
            for (names) |name| {
                if (ws.getTensor(name)) |lw| {
                    const owned_name = try allocator.dupe(u8, name);
                    errdefer allocator.free(owned_name);
                    try weight_store.resident_weights.put(allocator, owned_name, lw);
                    loaded_count += 1;
                } else |_| {}
            }
            print("  loaded {d} weights from {s}\n", .{ loaded_count, st_path });
        } else |err| {
            print("warning: could not list weights: {}\n", .{err});
        }
    } else |err| {
        // Try sharded safetensors (model.safetensors.index.json) as fallback
        print("warning: single safetensors not found ({}) — trying sharded index\n", .{err});
        var idx_path_buf: [512]u8 = undefined;
        const idx_path = try std.fmt.bufPrint(&idx_path_buf, "{s}/model.safetensors.index.json", .{opts.model_dir});
        const sharded_src = try weight_source_mod.ShardedSafetensorsSource.initAbsolute(allocator, idx_path);
        var sharded_ptr = sharded_src;
        defer sharded_ptr.weightSource().deinit();
        const sws = sharded_ptr.weightSource();
        if (sws.listNames(allocator)) |names| {
            defer {
                for (names) |n| allocator.free(n);
                allocator.free(names);
            }
            var loaded_count: usize = 0;
            for (names) |name| {
                if (sws.getTensor(name)) |lw| {
                    const owned_name = try allocator.dupe(u8, name);
                    errdefer allocator.free(owned_name);
                    try weight_store.resident_weights.put(allocator, owned_name, lw);
                    loaded_count += 1;
                } else |_| {}
            }
            print("  loaded {d} sharded weights\n", .{loaded_count});
        } else |load_err| {
            return load_err;
        }
    }

    // ------------------------------------------------------------------
    // 5. Load training data
    // ------------------------------------------------------------------
    var train_loaded = try reranker_data.loadExamples(allocator, opts.train_data, opts.train_split);
    defer train_loaded.deinit();

    var examples = train_loaded.examples;
    if (opts.max_examples > 0 and examples.len > opts.max_examples) {
        examples = examples[0..opts.max_examples];
    }

    const stats = reranker_data.computeStats(examples);
    print("  training examples: {d} (groups={d}, avg_score={d:.3})\n", .{
        stats.num_examples,
        stats.num_query_groups,
        stats.avg_score,
    });

    if (examples.len == 0) {
        print("error: no training examples loaded\n", .{});
        return error.NoTrainingData;
    }

    // ------------------------------------------------------------------
    // 6. Parse LoRA target patterns
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
    // 7. Build the BERT graph config
    // ------------------------------------------------------------------
    const graph_config = bert_graph.Config{
        .vocab_size = bert_config.vocab_size,
        .hidden_size = bert_config.hidden_size,
        .num_hidden_layers = bert_config.num_hidden_layers,
        .num_attention_heads = bert_config.num_attention_heads,
        .intermediate_size = bert_config.intermediate_size,
        .max_position_embeddings = bert_config.max_position_embeddings,
        .type_vocab_size = bert_config.type_vocab_size,
    };

    // ------------------------------------------------------------------
    // 8. Initialize the RealAutodiffTrainer
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
            .hidden_size_hint = bert_config.hidden_size,
            .num_layers_hint = bert_config.num_hidden_layers,
            .seed = opts.seed,
        },
    );
    defer trainer.deinit();

    // ------------------------------------------------------------------
    // 9. Training loop
    // ------------------------------------------------------------------
    const total_examples = examples.len;
    const steps_per_epoch = (total_examples + opts.batch_size - 1) / opts.batch_size;

    print("\nStarting training: {d} epochs x {d} steps/epoch ({d} examples)\n", .{
        opts.epochs,
        steps_per_epoch,
        total_examples,
    });

    // Pre-allocate batch buffers.
    const batch_tokens = @as(usize, opts.batch_size) * @as(usize, opts.seq_len);
    var input_ids = try allocator.alloc(i64, batch_tokens);
    defer allocator.free(input_ids);
    var attention_mask = try allocator.alloc(f32, batch_tokens);
    defer allocator.free(attention_mask);
    var targets_buf = try allocator.alloc(f32, opts.batch_size);
    defer allocator.free(targets_buf);

    var rng = std.Random.DefaultPrng.init(opts.seed);
    var prng = rng.random();

    var cumulative_loss: f64 = 0.0;
    var total_steps: u64 = 0;

    for (0..opts.epochs) |epoch| {
        // Shuffle examples at the start of each epoch.
        prng.shuffle(reranker_data.Example, examples);

        var epoch_loss: f64 = 0.0;
        var epoch_steps: u64 = 0;

        var batch_start: usize = 0;
        while (batch_start < total_examples) {
            const batch_end = @min(batch_start + opts.batch_size, total_examples);
            const actual_batch: u32 = @intCast(batch_end - batch_start);

            // Fill batch buffers with simple char-level tokenization.
            // A real deployment would wire in the HF tokenizer here via
            // tokenizer_batch.zig; this placeholder unblocks the training
            // loop without a tokenizer dependency.
            fillBatchBuffers(
                examples[batch_start..batch_end],
                opts.seq_len,
                input_ids,
                attention_mask,
                targets_buf,
                opts.neftune_alpha,
                &prng,
            );

            // Build the TrainerInput via the BERT convenience builder.
            var bert_ctx = reranker_train.BertAutodiffCtx{
                .graph_config = graph_config,
            };
            const targets_shape = ml.graph.Shape.init(.f32, &.{@as(i64, @intCast(actual_batch))});

            const trainer_input = reranker_train.bertTrainerInput(
                &bert_ctx,
                input_ids[0 .. @as(usize, actual_batch) * @as(usize, opts.seq_len)],
                attention_mask[0 .. @as(usize, actual_batch) * @as(usize, opts.seq_len)],
                targets_buf[0..actual_batch],
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
        print("  epoch {d}/{d} complete — avg_loss={d:.6}\n", .{ epoch + 1, opts.epochs, avg_epoch_loss });
    }

    // ------------------------------------------------------------------
    // 10. Save adapters
    // ------------------------------------------------------------------
    try trainer.saveAdapters(opts.out_dir);
    print("\nLoRA adapters saved to {s}\n", .{opts.out_dir});

    const final_avg = if (opts.epochs > 0) cumulative_loss / @as(f64, @floatFromInt(opts.epochs)) else 0.0;
    print("training complete — {d} total steps, final avg loss={d:.6}\n", .{ total_steps, final_avg });
}

// ---------------------------------------------------------------------------
// Batch buffer construction (placeholder tokenization)
// ---------------------------------------------------------------------------

/// Fills input_ids, attention_mask, and targets for one batch from raw
/// Example data. Uses a trivial char-to-id mapping as a placeholder for a
/// real tokenizer. Query and document are concatenated with a [SEP] token
/// (id = 102, matching BERT convention).
fn fillBatchBuffers(
    batch_examples: []const reranker_data.Example,
    seq_len: u32,
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
    neftune_alpha: f32,
    prng: *std.Random,
) void {
    const sl: usize = seq_len;

    for (batch_examples, 0..) |example, b| {
        const offset = b * sl;
        // [CLS] token
        input_ids[offset] = 101;
        attention_mask[offset] = 1.0;
        var pos: usize = 1;

        // Query tokens (char-level placeholder)
        for (example.query) |ch| {
            if (pos >= sl - 1) break; // leave room for final [SEP]
            input_ids[offset + pos] = @as(i64, ch) + 1000; // offset into vocab
            attention_mask[offset + pos] = 1.0;
            pos += 1;
        }

        // [SEP] between query and document
        if (pos < sl - 1) {
            input_ids[offset + pos] = 102;
            attention_mask[offset + pos] = 1.0;
            pos += 1;
        }

        // Document tokens
        for (example.document) |ch| {
            if (pos >= sl - 1) break;
            input_ids[offset + pos] = @as(i64, ch) + 1000;
            attention_mask[offset + pos] = 1.0;
            pos += 1;
        }

        // Final [SEP]
        if (pos < sl) {
            input_ids[offset + pos] = 102;
            attention_mask[offset + pos] = 1.0;
            pos += 1;
        }

        // Pad remainder
        while (pos < sl) : (pos += 1) {
            input_ids[offset + pos] = 0;
            attention_mask[offset + pos] = 0.0;
        }

        // NEFTune: add uniform noise to non-padding mask positions
        if (neftune_alpha > 0.0) {
            const valid_len: f32 = @floatFromInt(@min(pos, sl));
            const scale = neftune_alpha / @sqrt(valid_len * @as(f32, @floatFromInt(sl)));
            for (0..sl) |j| {
                if (attention_mask[offset + j] > 0.0) {
                    const noise = (prng.float(f32) - 0.5) * 2.0 * scale;
                    // Apply noise to mask (the forward pass sees it as an
                    // embedding-level perturbation via the mask channel).
                    attention_mask[offset + j] += noise;
                }
            }
        }

        targets[b] = example.score;
    }
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

fn printUsage() void {
    print(
        \\usage: train-reranker-autodiff --model-dir <path> --train-data <path> --out-dir <path> [options]
        \\
        \\required:
        \\  --model-dir <path>        Directory with base model (config.json + model.safetensors)
        \\  --train-data <path>       JSONL training data file or directory
        \\  --out-dir <path>          Output directory for LoRA adapter weights
        \\
        \\options:
        \\  --epochs <n>              Number of training epochs (default: 3)
        \\  --batch-size <n>          Examples per step (default: 8)
        \\  --seq-len <n>             Max sequence length (default: 256)
        \\  --learning-rate <f>       Learning rate (default: 2e-5)
        \\  --lora-rank <n>           LoRA rank (default: 16)
        \\  --lora-alpha <f>          LoRA alpha scaling (default: 32)
        \\  --lora-targets <csv>      Target module patterns (default: query,value)
        \\  --max-examples <n>        Cap on training examples (default: 0 = all)
        \\  --neftune-alpha <f>       NEFTune noise scale (default: 0)
        \\  --seed <n>                RNG seed (default: 42)
        \\  --max-grad-norm <f>       Gradient clipping norm (default: 1.0)
        \\  --grad-accum <n>          Gradient accumulation steps (default: 1)
        \\  --split <name>            Dataset split filter (default: none)
        \\
        \\example:
        \\  train-reranker-autodiff --model-dir /models/bge-reranker-v2 \
        \\    --train-data /data/reranker/train.jsonl --out-dir /output/lora \
        \\    --epochs 5 --batch-size 16 --lora-rank 16 --learning-rate 2e-5
        \\
    , .{});
}
