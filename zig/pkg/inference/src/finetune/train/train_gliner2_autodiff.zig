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
//   --eval-data <path>           Disjoint held-out JSONL evaluated in process
//   --out-dir <path>             Output directory for saved LoRA adapters
//   --epochs <n>                 Number of training epochs (default: 10)
//   --batch-size <n>             Examples per step (default: 2)
//   --seq-len <n>                Max sequence length (default: 256)
//   --learning-rate <f>          Learning rate (default: 5e-4)
//   --lr-scheduler <name>        linear, cosine, cosine_restarts, or constant (default: linear)
//   --warmup-ratio <f>           Warmup fraction when --warmup-steps is 0 (default: 0.1)
//   --warmup-steps <n>           Explicit optimizer warmup steps (default: 0)
//   --weight-decay <f>           AdamW weight decay (default: 0.01)
//   --lora-rank <n>              LoRA rank (default: 16)
//   --lora-alpha <f>             LoRA alpha scaling (default: 32)
//   --lora-dropout <f>           LoRA dropout probability (default: 0)
//   --lora-targets <csv>         Target module groups (default: upstream GLiNER2 LoRA groups)
//   --num-classes <n>            Entity classes including O (default: 5)
//   --objective <name>           token, span-start, or gliner2-total-loss (default: gliner2-total-loss)
//   --max-span-width <n>         Max span width for span objectives (default: 8)
//   --max-examples <n>           Cap on training examples (0 = all, default: 0)
//   --max-steps <n>              Exact optimizer-step count, cycling data if needed (0 = epochs)
//   --max-grad-norm <f>          Gradient clipping norm (default: 1.0)
//   --grad-accum <n>             Gradient accumulation steps (default: 1)
//   --seed <n>                   RNG seed (default: 42)
//   --initial-adapter-checkpoint <path>
//                                  Optional PEFT safetensors checkpoint used to seed LoRA weights
//   --lora-only-trainables       Freeze regular task-head params; train LoRA only
//   --eval-every-epochs <n>      Evaluate every N epochs (default: 1 with --eval-data)
//   --eval-batch-size <n>        Logical held-out batch size (default: 8)
//   --early-stopping-patience <n>
//                                  Stop after N non-improving evals (0 = disabled)
//   --early-stopping-threshold <f>
//                                  Minimum eval-loss decrease (default: 0)

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const ml = @import("ml");
const native_compute = inference.native_compute.native;
const metal_compute = if (build_options.enable_metal) inference.native_compute.metal else struct {};
const gpu_hosted_store = inference.native_compute.gpu_hosted_store;
const metal_runtime = inference.metal_runtime;
const compat = inference.io.compat;
const weight_source_mod = inference.models.weight_source;
const safetensors = inference.models.safetensors;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = inference.backends.Tensor;
const MetalWeightStore = if (build_options.enable_metal) gpu_hosted_store.WeightStore else void;

// Finetune module imports — accessed via the termite internal module tree.
const gliner2_data = inference.finetune.gliner2_data;
const gliner2_bundle = inference.finetune.gliner2;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const real_autodiff = inference.finetune.real_autodiff_trainer;
const optimizers = ml.graph.optimizers;
const run_validation = inference.finetune.gliner2_run_validation;
const deberta_arch = inference.architectures.deberta;
const deberta_graph = inference.architectures.deberta_graph;

const print = std.debug.print;
const max_total_loss_schema_slots = gliner2_autodiff.max_span_start_entity_types;
const max_graph_cache_capacity: u8 = 8;

// ---------------------------------------------------------------------------
// CLI options
// ---------------------------------------------------------------------------

const Options = struct {
    model_dir: []const u8,
    train_data: []const u8,
    eval_data: ?[]const u8 = null,
    out_dir: []const u8,
    epochs: u32 = 10,
    batch_size: u32 = 2,
    seq_len: u32 = 256,
    learning_rate: f32 = 5e-4,
    lr_scheduler: LrScheduler = .linear,
    warmup_ratio: f32 = 0.1,
    warmup_steps: u32 = 0,
    num_cycles: f32 = 0.5,
    weight_decay: f32 = 0.01,
    lora_rank: u32 = 16,
    lora_alpha: f32 = 32.0,
    lora_dropout: f32 = gliner2_bundle.default_lora_dropout,
    lora_targets: []const u8 = "encoder,span_rep,classifier,count_embed,count_pred",
    num_classes: u32 = 5,
    entity_types_csv: ?[]const u8 = null,
    objective: gliner2_autodiff.GlinerObjective = .gliner2_total_loss,
    max_span_width: u32 = 8,
    span_loss: gliner2_autodiff.SpanStartLossKind = .bce,
    span_loss_reduction: gliner2_autodiff.SpanStartLossReduction = .sum,
    span_positive_weight: f32 = 1.0,
    span_label_positive_weights: ?[]const u8 = null,
    span_negative_weight: f32 = 1.0,
    span_hard_negative_weight: f32 = 1.0,
    span_negative_mask_rate: f32 = 0.5,
    max_examples: usize = 0,
    max_steps: u64 = 0,
    max_grad_norm: f32 = 1.0,
    grad_accum: u32 = 1,
    seed: u64 = 42,
    initial_adapter_checkpoint: ?[]const u8 = null,
    backend: Gliner2TrainBackend = .auto,
    compiled_required: bool = false,
    dump_span_parity: bool = false,
    dump_optimizer_parity: bool = false,
    lora_only_trainables: bool = false,
    deterministic: bool = false,
    report_to: ReportTo = .stdout,
    allow_large_memory: bool = false,
    activation_checkpointing: bool = false,
    activation_checkpoint_interval: u32 = 1,
    activation_checkpoint_strategy: ml.graph.checkpoint.CheckpointStrategy = .every_n_layers,
    structure_span_chunk_samples: u32 = 0,
    graph_cache_capacity: u8 = 2,
    checkpoint_every_epochs: u32 = 0,
    checkpoint_keep_last: u32 = 3,
    resume_checkpoint: ?[]const u8 = null,
    eval_every_epochs: u32 = 1,
    eval_batch_size: u32 = 8,
    early_stopping_patience: u32 = 0,
    early_stopping_threshold: f64 = 0.0,
};

const Gliner2TrainBackend = enum {
    auto,
    metal,
    native,
};

const LrScheduler = enum {
    linear,
    cosine,
    cosine_restarts,
    constant,
};

const ReportTo = enum {
    stdout,
    jsonl,
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
    var eval_data: ?[]const u8 = null;
    var out_dir: ?[]const u8 = null;
    var epochs: u32 = 10;
    var batch_size: u32 = 2;
    var seq_len: u32 = 256;
    var learning_rate: f32 = 5e-4;
    var lr_scheduler: LrScheduler = .linear;
    var warmup_ratio: f32 = 0.1;
    var warmup_steps: u32 = 0;
    var num_cycles: f32 = 0.5;
    var weight_decay: f32 = 0.01;
    var lora_rank: u32 = 16;
    var lora_alpha: f32 = 32.0;
    var lora_dropout: f32 = gliner2_bundle.default_lora_dropout;
    var lora_targets: []const u8 = "encoder,span_rep,classifier,count_embed,count_pred";
    var num_classes: u32 = 5;
    var entity_types_csv: ?[]const u8 = null;
    var objective: gliner2_autodiff.GlinerObjective = .gliner2_total_loss;
    var max_span_width: u32 = 8;
    var span_loss: gliner2_autodiff.SpanStartLossKind = .bce;
    var span_loss_reduction: gliner2_autodiff.SpanStartLossReduction = .sum;
    var span_positive_weight: f32 = 1.0;
    var span_label_positive_weights: ?[]const u8 = null;
    var span_negative_weight: f32 = 1.0;
    var span_hard_negative_weight: f32 = 1.0;
    var span_negative_mask_rate: f32 = 0.5;
    var max_examples: usize = 0;
    var max_steps: u64 = 0;
    var max_grad_norm: f32 = 1.0;
    var grad_accum: u32 = 1;
    var seed: u64 = 42;
    var initial_adapter_checkpoint: ?[]const u8 = null;
    var backend: Gliner2TrainBackend = .auto;
    var compiled_required: bool = false;
    var dump_span_parity: bool = false;
    var dump_optimizer_parity: bool = false;
    var lora_only_trainables: bool = false;
    var deterministic: bool = false;
    var report_to: ReportTo = .stdout;
    var allow_large_memory: bool = false;
    var activation_checkpointing: bool = false;
    var activation_checkpoint_interval: u32 = 1;
    var activation_checkpoint_strategy: ml.graph.checkpoint.CheckpointStrategy = .every_n_layers;
    var structure_span_chunk_samples: u32 = 0;
    var graph_cache_capacity: u8 = 2;
    var checkpoint_every_epochs: u32 = 0;
    var checkpoint_keep_last: u32 = 3;
    var resume_checkpoint: ?[]const u8 = null;
    var eval_every_epochs: u32 = 1;
    var eval_batch_size: u32 = 8;
    var early_stopping_patience: u32 = 0;
    var early_stopping_threshold: f64 = 0.0;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, arg, "--model-dir")) {
            model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--train-data")) {
            train_data = args.next() orelse return error.MissingTrainData;
        } else if (std.mem.eql(u8, arg, "--eval-data")) {
            eval_data = args.next() orelse return error.MissingEvalData;
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
        } else if (std.mem.eql(u8, arg, "--lr-scheduler")) {
            const val = args.next() orelse return error.MissingLrScheduler;
            lr_scheduler = parseLrScheduler(val) orelse {
                print("error: unsupported --lr-scheduler '{s}' (expected linear, cosine, cosine_restarts, or constant)\n", .{val});
                return error.InvalidLrScheduler;
            };
        } else if (std.mem.eql(u8, arg, "--warmup-ratio")) {
            const val = args.next() orelse return error.MissingWarmupRatio;
            warmup_ratio = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--warmup-steps")) {
            const val = args.next() orelse return error.MissingWarmupSteps;
            warmup_steps = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--num-cycles")) {
            const val = args.next() orelse return error.MissingNumCycles;
            num_cycles = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--weight-decay")) {
            const val = args.next() orelse return error.MissingWeightDecay;
            weight_decay = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lora-rank")) {
            const val = args.next() orelse return error.MissingLoraRank;
            lora_rank = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--lora-alpha")) {
            const val = args.next() orelse return error.MissingLoraAlpha;
            lora_alpha = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lora-dropout")) {
            const val = args.next() orelse return error.MissingLoraDropout;
            lora_dropout = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lora-targets")) {
            lora_targets = args.next() orelse return error.MissingLoraTargets;
        } else if (std.mem.eql(u8, arg, "--num-classes")) {
            const val = args.next() orelse return error.MissingNumClasses;
            num_classes = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--entity-types")) {
            entity_types_csv = args.next() orelse return error.MissingEntityTypes;
        } else if (std.mem.eql(u8, arg, "--objective")) {
            const val = args.next() orelse return error.MissingObjective;
            objective = try parseObjective(val);
        } else if (std.mem.eql(u8, arg, "--max-span-width")) {
            const val = args.next() orelse return error.MissingMaxSpanWidth;
            max_span_width = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--span-loss")) {
            const val = args.next() orelse return error.MissingSpanLoss;
            span_loss = try parseSpanLoss(val);
        } else if (std.mem.eql(u8, arg, "--span-loss-reduction")) {
            const val = args.next() orelse return error.MissingSpanLossReduction;
            span_loss_reduction = try parseSpanLossReduction(val);
        } else if (std.mem.eql(u8, arg, "--span-positive-weight")) {
            const val = args.next() orelse return error.MissingSpanPositiveWeight;
            span_positive_weight = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--span-label-positive-weights")) {
            span_label_positive_weights = args.next() orelse return error.MissingSpanLabelPositiveWeights;
        } else if (std.mem.eql(u8, arg, "--span-negative-weight")) {
            const val = args.next() orelse return error.MissingSpanNegativeWeight;
            span_negative_weight = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--span-hard-negative-weight")) {
            const val = args.next() orelse return error.MissingSpanHardNegativeWeight;
            span_hard_negative_weight = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--span-negative-mask-rate")) {
            const val = args.next() orelse return error.MissingSpanNegativeMaskRate;
            span_negative_mask_rate = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const val = args.next() orelse return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-steps")) {
            const val = args.next() orelse return error.MissingMaxSteps;
            max_steps = try std.fmt.parseUnsigned(u64, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            const val = args.next() orelse return error.MissingMaxGradNorm;
            max_grad_norm = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            const val = args.next() orelse return error.MissingGradAccum;
            grad_accum = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            const val = args.next() orelse return error.MissingSeed;
            seed = try std.fmt.parseUnsigned(u64, val, 10);
        } else if (std.mem.eql(u8, arg, "--initial-adapter-checkpoint")) {
            initial_adapter_checkpoint = args.next() orelse return error.MissingInitialAdapterCheckpoint;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            const val = args.next() orelse return error.MissingBackend;
            backend = parseBackend(val) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--compiled-required")) {
            compiled_required = true;
        } else if (std.mem.eql(u8, arg, "--dump-span-parity")) {
            dump_span_parity = true;
        } else if (std.mem.eql(u8, arg, "--dump-optimizer-parity")) {
            dump_optimizer_parity = true;
        } else if (std.mem.eql(u8, arg, "--lora-only-trainables")) {
            lora_only_trainables = true;
        } else if (std.mem.eql(u8, arg, "--deterministic")) {
            deterministic = true;
        } else if (std.mem.eql(u8, arg, "--allow-large-memory")) {
            allow_large_memory = true;
        } else if (std.mem.eql(u8, arg, "--activation-checkpointing")) {
            activation_checkpointing = true;
        } else if (std.mem.eql(u8, arg, "--activation-checkpoint-interval")) {
            const val = args.next() orelse return error.MissingActivationCheckpointInterval;
            activation_checkpoint_interval = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--activation-checkpoint-strategy")) {
            const val = args.next() orelse return error.MissingActivationCheckpointStrategy;
            activation_checkpoint_strategy = parseCheckpointStrategy(val) orelse {
                print("error: unsupported --activation-checkpoint-strategy '{s}' (expected every-n-layers, attention-outputs, or parameters-only)\n", .{val});
                return error.InvalidActivationCheckpointStrategy;
            };
        } else if (std.mem.eql(u8, arg, "--structure-span-chunk-samples")) {
            const val = args.next() orelse return error.MissingStructureSpanChunkSamples;
            structure_span_chunk_samples = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--graph-cache-capacity")) {
            const val = args.next() orelse return error.MissingGraphCacheCapacity;
            graph_cache_capacity = try std.fmt.parseUnsigned(u8, val, 10);
        } else if (std.mem.eql(u8, arg, "--checkpoint-every-epochs")) {
            const val = args.next() orelse return error.MissingCheckpointEveryEpochs;
            checkpoint_every_epochs = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--checkpoint-keep-last")) {
            const val = args.next() orelse return error.MissingCheckpointKeepLast;
            checkpoint_keep_last = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--resume-checkpoint")) {
            resume_checkpoint = args.next() orelse return error.MissingResumeCheckpoint;
        } else if (std.mem.eql(u8, arg, "--eval-every-epochs")) {
            const val = args.next() orelse return error.MissingEvalEveryEpochs;
            eval_every_epochs = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--eval-batch-size")) {
            const val = args.next() orelse return error.MissingEvalBatchSize;
            eval_batch_size = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--early-stopping-patience")) {
            const val = args.next() orelse return error.MissingEarlyStoppingPatience;
            early_stopping_patience = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--early-stopping-threshold")) {
            const val = args.next() orelse return error.MissingEarlyStoppingThreshold;
            early_stopping_threshold = try std.fmt.parseFloat(f64, val);
        } else if (std.mem.eql(u8, arg, "--report-to")) {
            const val = args.next() orelse return error.MissingReportTo;
            report_to = parseReportTo(val) orelse {
                print("error: unsupported --report-to '{s}' (expected stdout or jsonl)\n", .{val});
                return error.InvalidReportTo;
            };
        } else {
            print("error: unknown argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArguments;
        }
    }

    if (deterministic) {
        // Force off every per-step stochastic regularizer on this training
        // path so repeated runs (and Python/Zig parity comparisons) are
        // bitwise-stable given the same seed and adapter init.
        //
        // NEFTune-style embedding noise is not wired into the GLiNER2
        // real-autodiff path, so LoRA dropout masks and span negative
        // masking are the only per-step stochastic inputs to disable here.
        if (lora_dropout != 0.0) {
            print("warning: --deterministic overrides --lora-dropout {d:.3} -> 0.0\n", .{lora_dropout});
            lora_dropout = 0.0;
        }
        if (span_negative_mask_rate != 0.0) {
            print("warning: --deterministic overrides --span-negative-mask-rate {d:.3} -> 0.0\n", .{span_negative_mask_rate});
            span_negative_mask_rate = 0.0;
        }
    }
    if (lr_scheduler == .cosine_restarts and (!std.math.isFinite(num_cycles) or num_cycles <= 0.0)) return error.InvalidNumCycles;

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
        .eval_data = eval_data,
        .out_dir = out_dir orelse {
            print("error: --out-dir is required\n", .{});
            printUsage();
            return error.InvalidArguments;
        },
        .epochs = epochs,
        .batch_size = batch_size,
        .seq_len = seq_len,
        .learning_rate = learning_rate,
        .lr_scheduler = lr_scheduler,
        .warmup_ratio = warmup_ratio,
        .warmup_steps = warmup_steps,
        .num_cycles = num_cycles,
        .weight_decay = weight_decay,
        .lora_rank = lora_rank,
        .lora_alpha = lora_alpha,
        .lora_dropout = lora_dropout,
        .lora_targets = lora_targets,
        .num_classes = num_classes,
        .entity_types_csv = entity_types_csv,
        .objective = objective,
        .max_span_width = max_span_width,
        .span_loss = span_loss,
        .span_loss_reduction = span_loss_reduction,
        .span_positive_weight = span_positive_weight,
        .span_label_positive_weights = span_label_positive_weights,
        .span_negative_weight = span_negative_weight,
        .span_hard_negative_weight = span_hard_negative_weight,
        .span_negative_mask_rate = span_negative_mask_rate,
        .max_examples = max_examples,
        .max_steps = max_steps,
        .max_grad_norm = max_grad_norm,
        .grad_accum = grad_accum,
        .seed = seed,
        .initial_adapter_checkpoint = initial_adapter_checkpoint,
        .backend = backend,
        .compiled_required = compiled_required,
        .dump_span_parity = dump_span_parity,
        .dump_optimizer_parity = dump_optimizer_parity,
        .lora_only_trainables = lora_only_trainables,
        .deterministic = deterministic,
        .report_to = report_to,
        .allow_large_memory = allow_large_memory,
        .activation_checkpointing = activation_checkpointing,
        .activation_checkpoint_interval = activation_checkpoint_interval,
        .activation_checkpoint_strategy = activation_checkpoint_strategy,
        .structure_span_chunk_samples = structure_span_chunk_samples,
        .graph_cache_capacity = graph_cache_capacity,
        .checkpoint_every_epochs = checkpoint_every_epochs,
        .checkpoint_keep_last = checkpoint_keep_last,
        .resume_checkpoint = resume_checkpoint,
        .eval_every_epochs = eval_every_epochs,
        .eval_batch_size = eval_batch_size,
        .early_stopping_patience = early_stopping_patience,
        .early_stopping_threshold = early_stopping_threshold,
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
    try syncDirectoryChain(opts.out_dir);

    print("train-gliner2-autodiff\n  model_dir={s}\n  train_data={s}\n  out_dir={s}\n", .{
        opts.model_dir,
        opts.train_data,
        opts.out_dir,
    });
    print("  epochs={d} batch_size={d} seq_len={d} lr={e:.6} weight_decay={e:.6}\n", .{
        opts.epochs,
        opts.batch_size,
        opts.seq_len,
        opts.learning_rate,
        opts.weight_decay,
    });
    print("  lora_rank={d} lora_alpha={d:.1} lora_dropout={d:.3} lora_targets={s}\n", .{
        opts.lora_rank,
        opts.lora_alpha,
        opts.lora_dropout,
        opts.lora_targets,
    });
    try gliner2_bundle.validateLoRADropout(opts.lora_dropout);
    if (!std.math.isFinite(opts.learning_rate) or opts.learning_rate <= 0.0) return error.InvalidLearningRate;
    if (!std.math.isFinite(opts.weight_decay) or opts.weight_decay < 0.0) return error.InvalidWeightDecay;
    if (!std.math.isFinite(opts.lora_alpha) or opts.lora_alpha <= 0.0) return error.InvalidLoRAAlpha;
    if (!std.math.isFinite(opts.max_grad_norm) or opts.max_grad_norm <= 0.0) return error.InvalidMaxGradNorm;
    print("  num_classes={d} seed={d} max_grad_norm={d:.2} grad_accum={d}\n", .{
        opts.num_classes,
        opts.seed,
        opts.max_grad_norm,
        opts.grad_accum,
    });
    if (opts.max_steps > 0) {
        print("  max_steps={d} (exact-step mode; cycles training data as needed)\n", .{opts.max_steps});
    }
    print("  objective={s} max_span_width={d} span_loss={s} span_loss_reduction={s} span_pos_weight={d:.3} span_neg_weight={d:.3} span_hard_neg_weight={d:.3}\n", .{
        objectiveName(opts.objective),
        opts.max_span_width,
        spanLossName(opts.span_loss),
        spanLossReductionName(opts.span_loss_reduction),
        opts.span_positive_weight,
        opts.span_negative_weight,
        opts.span_hard_negative_weight,
    });
    if (opts.span_label_positive_weights) |weights| {
        print("  span_label_positive_weights={s}\n", .{weights});
    }
    if (!std.math.isFinite(opts.span_positive_weight) or opts.span_positive_weight <= 0.0) return error.InvalidSpanPositiveWeight;
    if (!std.math.isFinite(opts.span_negative_weight) or opts.span_negative_weight <= 0.0) return error.InvalidSpanNegativeWeight;
    if (!std.math.isFinite(opts.span_hard_negative_weight) or opts.span_hard_negative_weight <= 0.0) return error.InvalidSpanHardNegativeWeight;
    if (!std.math.isFinite(opts.span_negative_mask_rate) or opts.span_negative_mask_rate < 0.0 or opts.span_negative_mask_rate > 1.0) return error.InvalidSpanNegativeMaskRate;
    if (!std.math.isFinite(opts.warmup_ratio) or opts.warmup_ratio < 0.0 or opts.warmup_ratio > 1.0) return error.InvalidWarmupRatio;

    // Bounds-check integer CLI args before they feed size/shape math downstream.
    // A 0 here causes div-by-zero, cast/underflow, or a silent no-op; num_classes
    // must be >= 2 because the entity-type count is num_classes - 1.
    if (opts.batch_size < 1) {
        print("error: --batch-size must be >= 1 (got {d})\n", .{opts.batch_size});
        return error.InvalidBatchSize;
    }
    if (opts.seq_len < 1) {
        print("error: --seq-len must be >= 1 (got {d})\n", .{opts.seq_len});
        return error.InvalidSeqLen;
    }
    if (opts.max_span_width < 1) {
        print("error: --max-span-width must be >= 1 (got {d})\n", .{opts.max_span_width});
        return error.InvalidMaxSpanWidth;
    }
    if (opts.lora_rank < 1) {
        print("error: --lora-rank must be >= 1 (got {d})\n", .{opts.lora_rank});
        return error.InvalidLoRARank;
    }
    if (opts.num_classes < 2) {
        print("error: --num-classes must be >= 2 (entity types = num_classes - 1; got {d})\n", .{opts.num_classes});
        return error.InvalidNumClasses;
    }
    if (opts.grad_accum < 1) {
        print("error: --grad-accum must be >= 1 (got {d})\n", .{opts.grad_accum});
        return error.InvalidGradAccum;
    }
    if (opts.epochs < 1) {
        print("error: --epochs must be >= 1 (got {d})\n", .{opts.epochs});
        return error.InvalidEpochs;
    }
    if (opts.objective == .gliner2_total_loss) {
        if (opts.span_loss != .bce) return error.UnsupportedTotalLossSpanLoss;
        if (opts.span_loss_reduction != .sum) return error.UnsupportedTotalLossReduction;
        if (opts.span_positive_weight != 1.0 or opts.span_negative_weight != 1.0) return error.UnsupportedTotalLossClassWeights;
        if (opts.span_label_positive_weights != null) return error.UnsupportedTotalLossLabelWeights;
        if (opts.span_hard_negative_weight != 1.0) return error.UnsupportedTotalLossHardNegativeWeight;
    }
    if (opts.resume_checkpoint != null and opts.initial_adapter_checkpoint != null) {
        return error.ResumeCheckpointConflictsWithInitialAdapter;
    }
    if (opts.eval_data != null and opts.eval_every_epochs == 0) return error.InvalidEvalEveryEpochs;
    if (opts.eval_data != null and opts.eval_batch_size == 0) return error.InvalidEvalBatchSize;
    if (!std.math.isFinite(opts.early_stopping_threshold) or opts.early_stopping_threshold < 0.0) return error.InvalidEarlyStoppingThreshold;
    if (opts.eval_data == null and opts.early_stopping_patience > 0) return error.EarlyStoppingRequiresEvalData;
    if (opts.graph_cache_capacity == 0 or opts.graph_cache_capacity > max_graph_cache_capacity) return error.InvalidGraphCacheCapacity;

    // Capture immutable provenance before opening the training inputs. A
    // directory dataset remains supported, but production release validation
    // requires a single JSONL file so its exact bytes can be tied to the
    // exported adapter.
    const train_data_sha256 = try sha256RegularFileAlloc(allocator, opts.train_data);
    defer if (train_data_sha256) |value| allocator.free(value);
    const eval_data_sha256 = if (opts.eval_data) |path|
        (try sha256RegularFileAlloc(allocator, path)) orelse return error.EvalRequiresSingleFile
    else
        null;
    defer if (eval_data_sha256) |value| allocator.free(value);
    if (train_data_sha256) |train_digest| {
        if (eval_data_sha256) |eval_digest| {
            if (std.mem.eql(u8, train_digest, eval_digest)) return error.TrainEvalDataNotDisjoint;
        }
    }
    const initial_adapter_checkpoint_sha256 = try initialAdapterCheckpointSha256Alloc(
        allocator,
        opts.initial_adapter_checkpoint,
    );
    defer if (initial_adapter_checkpoint_sha256) |value| allocator.free(value);
    const resume_checkpoint_sha256 = try resumeCheckpointSha256Alloc(allocator, opts.resume_checkpoint);
    defer if (resume_checkpoint_sha256) |value| allocator.free(value);
    const base_model_fingerprint_sha256 = try gliner2BaseModelFingerprintAlloc(allocator, opts.model_dir);
    defer allocator.free(base_model_fingerprint_sha256);

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
    if (opts.objective == .gliner2_total_loss) {
        var backbone = try gliner2_bundle.loadBackboneConfig(allocator, opts.model_dir);
        defer backbone.deinit();
        if (!std.mem.eql(u8, backbone.counting_layer, "count_lstm_v2") or
            !std.mem.eql(u8, backbone.token_pooling, "first") or
            backbone.count_embed_dim != 128 or
            backbone.count_embed_layers != 2 or
            backbone.count_embed_heads != 4 or
            backbone.count_embed_ffn != 256 or
            backbone.max_count_embed != 20)
        {
            return error.UnsupportedGliner2CountingArchitecture;
        }
        if (opts.max_span_width > backbone.max_width) {
            print("error: --max-span-width={d} exceeds model max_width={d}\n", .{ opts.max_span_width, backbone.max_width });
            return error.Gliner2MaxSpanWidthExceeded;
        }
    }
    // ------------------------------------------------------------------
    // 3. Set up compute backend + load weights
    //
    // Use Metal when available, falling back to native CPU/BLAS.
    // ------------------------------------------------------------------
    var st_path_buf: [512]u8 = undefined;
    const st_path = try std.fmt.bufPrint(&st_path_buf, "{s}/model.safetensors", .{opts.model_dir});

    // We need these variables to live for the whole function regardless
    // of which backend branch we take.
    var metal_ws: MetalWeightStore = undefined;
    var metal_backend: if (build_options.enable_metal) metal_compute.MetalCompute else void = undefined;
    var native_ws: native_compute.WeightStore = undefined;
    var native_backend: native_compute.NativeCompute = undefined;

    // SafetensorsSource kept alive for native path (mmap'd data).
    var safetensors_source: ?*SafetensorsSource = null;
    defer if (safetensors_source) |s| s.weightSource().deinit();

    const force_native = envFlag("TERMITE_GLINER2_FORCE_NATIVE");
    const metal_runtime_available = if (comptime build_options.enable_metal)
        (!force_native and metal_runtime.metalDeviceAvailable())
    else
        false;
    const selected_backend = selectBackend(opts.backend, force_native, metal_runtime_available) catch |err| {
        switch (err) {
            error.MetalBackendUnavailable => print("error: --backend metal requested but Metal is not built or no Metal device is available\n", .{}),
        }
        return err;
    };

    const cb = if (selected_backend == .metal) blk: {
        if (comptime build_options.enable_metal) {
            metal_ws = .{
                .allocator = allocator,
                .prefix = "",
                .lazy_weights = .{},
            };
            try loadSafetensorsIntoGpuHostedStore(allocator, &metal_ws, st_path);
            if (opts.objective != .gliner2_total_loss) {
                try initClassifierHeadInGpuHostedStore(allocator, &metal_ws, opts.seed, deberta_config.hidden_size, opts.num_classes);
            }
            metal_compute.initPrefetchQueue(&metal_ws, allocator);
            metal_backend = try metal_compute.MetalCompute.init(allocator, &metal_ws, null);
            break :blk metal_backend.computeBackend();
        } else unreachable;
    } else blk: {
        // ── Native CPU/BLAS fallback ─────────────────────────────────
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

        if (opts.objective != .gliner2_total_loss) {
            try initClassifierHeadInNativeStore(allocator, &native_ws, opts.seed, deberta_config.hidden_size, opts.num_classes);
        }
        try initParityTopLevelWeightsNative(allocator, &native_ws, deberta_config.hidden_size);

        native_backend = native_compute.NativeCompute.init(allocator, &native_ws, null);
        break :blk native_backend.computeBackend();
    };
    defer switch (selected_backend) {
        .native => deinitNativeWeightStore(allocator, &native_ws),
        .metal => if (comptime build_options.enable_metal) deinitGpuHostedWeightStore(allocator, &metal_ws),
        else => {},
    };
    defer switch (selected_backend) {
        .metal => if (comptime build_options.enable_metal) metal_backend.deinit(),
        else => {},
    };

    print("  backend: {s}\n", .{backendLabel(selected_backend)});

    // ------------------------------------------------------------------
    // 5. Load training data (JSONL with text + entities)
    // ------------------------------------------------------------------
    var train_loaded: ?gliner2_data.LoadedExamples = if (opts.objective == .gliner2_total_loss)
        null
    else
        try gliner2_data.loadExamples(allocator, opts.train_data, null);
    defer if (train_loaded) |*loaded| loaded.deinit();
    var train_records_loaded = if (opts.objective == .gliner2_total_loss)
        try gliner2_data.loadTrainingRecords(allocator, opts.train_data, null)
    else
        null;
    defer if (train_records_loaded) |*loaded| loaded.deinit();

    var training_records: []gliner2_data.UpstreamRecord = if (train_records_loaded) |*loaded| loaded.records else &.{};
    const derived_examples = if (opts.objective == .gliner2_total_loss)
        try deriveExamplesFromTrainingRecords(allocator, training_records)
    else
        null;
    defer if (derived_examples) |items| freeDerivedExamples(allocator, items);
    var examples: []gliner2_data.Example = if (train_loaded) |*loaded| loaded.examples else derived_examples.?;
    if (opts.max_examples > 0 and examples.len > opts.max_examples) {
        examples = examples[0..opts.max_examples];
    }
    if (opts.max_examples > 0 and training_records.len > opts.max_examples) {
        training_records = training_records[0..opts.max_examples];
    }

    var eval_loaded: ?gliner2_data.LoadedExamples = if (opts.eval_data) |path|
        if (opts.objective == .gliner2_total_loss) null else try gliner2_data.loadExamples(allocator, path, null)
    else
        null;
    defer if (eval_loaded) |*loaded| loaded.deinit();
    var eval_records_loaded: ?gliner2_data.LoadedTrainingRecords = if (opts.eval_data) |path|
        if (opts.objective == .gliner2_total_loss) try gliner2_data.loadTrainingRecords(allocator, path, null) else null
    else
        null;
    defer if (eval_records_loaded) |*loaded| loaded.deinit();
    const eval_records: []gliner2_data.UpstreamRecord = if (eval_records_loaded) |*loaded| loaded.records else &.{};
    const derived_eval_examples = if (opts.objective == .gliner2_total_loss and opts.eval_data != null)
        try deriveExamplesFromTrainingRecords(allocator, eval_records)
    else
        null;
    defer if (derived_eval_examples) |items| freeDerivedExamples(allocator, items);
    const eval_examples: []gliner2_data.Example = if (eval_loaded) |*loaded| loaded.examples else derived_eval_examples orelse &.{};

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
    if (opts.eval_data != null and eval_examples.len == 0) return error.NoEvalData;
    if (opts.objective == .gliner2_total_loss and eval_records.len != eval_examples.len) return error.InvalidGliner2Example;
    if (opts.eval_data != null) try ensureDisjointExampleTexts(allocator, examples, eval_examples);
    const effective_batch_size = @min(examples.len, @as(usize, opts.batch_size));
    if (opts.objective == .gliner2_total_loss and training_records.len != examples.len) {
        print("error: gliner2-total-loss requires aligned upstream records ({d}) and flattened examples ({d})\n", .{ training_records.len, examples.len });
        return error.InvalidGliner2Example;
    }

    // ------------------------------------------------------------------
    // 6. Build a label-to-class-index mapping from the training data
    // ------------------------------------------------------------------
    // Class 0 is always the "O" (no entity) class. Prefer an explicit caller
    // entity order so training, manifest export, and evaluation agree on
    // class IDs; legacy direct invocations fall back to sorted dataset labels.
    var label_map = std.StringHashMapUnmanaged(u32){};
    defer label_map.deinit(allocator);
    const extra_entity_types = if (opts.entity_types_csv) |csv| try parseEntityTypesCsvOwned(allocator, csv) else null;
    defer if (extra_entity_types) |items| {
        for (items) |label| allocator.free(label);
        allocator.free(items);
    };
    const regular_entity_types = if (opts.objective == .gliner2_total_loss)
        null
    else if (extra_entity_types) |items|
        try dupeStringSlice(allocator, items)
    else
        try gliner2_data.buildLabelVocab(allocator, examples, null);
    defer if (regular_entity_types) |items| {
        for (items) |label| allocator.free(label);
        allocator.free(items);
    };
    const total_loss_entity_labels = if (opts.objective == .gliner2_total_loss)
        try gliner2_data.buildUpstreamEntityLabelVocab(allocator, training_records, extra_entity_types)
    else
        null;
    defer if (total_loss_entity_labels) |items| {
        for (items) |label| allocator.free(label);
        allocator.free(items);
    };
    const total_loss_dataset_axes = if (opts.objective == .gliner2_total_loss)
        try computeTotalLossDatasetAxes(allocator, training_records, eval_records)
    else
        null;
    const total_loss_schema_slots = if (opts.objective == .gliner2_total_loss) blk: {
        const count = total_loss_dataset_axes.?.schema_slots;
        if (count == 0) return error.InvalidEntityTypes;
        if (count > max_total_loss_schema_slots) {
            print("error: one training or held-out sample requires {d} contextual schema slots; maximum is {d}\n", .{ count, max_total_loss_schema_slots });
            return error.TooManySchemaSlots;
        }
        const slots = try allocator.alloc([]const u8, count);
        @memset(slots, "");
        break :blk slots;
    } else null;
    defer if (total_loss_schema_slots) |slots| allocator.free(slots);
    const entity_types = total_loss_schema_slots orelse regular_entity_types.?;
    const manifest_entity_labels = total_loss_entity_labels orelse regular_entity_types.?;
    const total_loss_entity_label_positive_counts = if (opts.objective == .gliner2_total_loss)
        try gliner2_data.countUpstreamEntityLabelPositives(allocator, training_records, manifest_entity_labels)
    else
        null;
    defer if (total_loss_entity_label_positive_counts) |counts| allocator.free(counts);
    const effective_num_classes: u32 = if (opts.objective == .gliner2_total_loss)
        @intCast(entity_types.len + 1)
    else
        opts.num_classes;
    if (entity_types.len + 1 > effective_num_classes) {
        print("error: dataset has {d} entity labels but num_classes={d} only has {d} entity slots\n", .{
            entity_types.len,
            effective_num_classes,
            if (effective_num_classes > 0) effective_num_classes - 1 else 0,
        });
        return error.TooManyEntityTypes;
    }
    if ((opts.objective == .span_start or opts.objective == .gliner2_total_loss) and entity_types.len + 1 != @as(usize, @intCast(effective_num_classes))) {
        print("error: span-start objective currently requires num_classes == entity_label_count + 1 ({d}); got {d}\n", .{
            entity_types.len + 1,
            effective_num_classes,
        });
        return error.SpanObjectiveRequiresExactClassCount;
    }
    if (opts.objective != .gliner2_total_loss) {
        for (entity_types, 0..) |label, idx| {
            try label_map.put(allocator, label, @intCast(idx + 1));
        }
        try ensureEvalLabelsKnown(eval_examples, &label_map);
    }
    if (opts.objective == .gliner2_total_loss) {
        print("  contextual schema slots: {d} (bounded per sample); manifest entity labels: {d}\n", .{ entity_types.len, manifest_entity_labels.len });
    } else {
        print("  entity labels mapped: {d} (num_classes={d})\n", .{ label_map.count(), effective_num_classes });
    }
    if (opts.objective == .gliner2_total_loss) {
        switch (selected_backend) {
            .metal => if (comptime build_options.enable_metal) try initClassifierHeadInGpuHostedStore(
                allocator,
                &metal_ws,
                opts.seed,
                deberta_config.hidden_size,
                effective_num_classes,
            ),
            .native => try initClassifierHeadInNativeStore(
                allocator,
                &native_ws,
                opts.seed,
                deberta_config.hidden_size,
                effective_num_classes,
            ),
            else => unreachable,
        }
    }
    const resolved_span_label_positive_weights = try resolveSpanLabelPositiveWeights(
        allocator,
        opts.span_label_positive_weights,
        entity_types,
        opts.span_positive_weight,
    );
    defer allocator.free(resolved_span_label_positive_weights);

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
    // 6c. Fit-to-data sequence length. The DeBERTa graph bakes in seq_len
    // (relative-position tables) and is built+cached once, so padding every
    // batch to the fixed --seq-len wastes S^2-proportional attention compute
    // (and can OOM) on padding. Scan the ACTUAL encoded token length of the
    // data (via the same batch builders the training loop uses) and size the
    // single graph to that, capped by --seq-len (upper bound) and the
    // relative-position table limit (512). Matches upstream GLiNER2Trainer's
    // dynamic padding. The .token objective keeps the fixed length.
    // ------------------------------------------------------------------
    const effective_seq_len: usize = blk: {
        if (opts.objective == .token) break :blk opts.seq_len;
        // Escape hatch: force the fixed --seq-len (e.g. to reproduce a
        // fixed-padding baseline or match another runner's seq length).
        if (std.c.getenv("TERMITE_GLINER2_DISABLE_FIT_SEQ_LEN") != null) break :blk opts.seq_len;
        var scanned_max: usize = 0;
        var i: usize = 0;
        while (i < examples.len) : (i += 1) {
            const used = switch (opts.objective) {
                .gliner2_total_loss => try gliner2_data.measureUpstreamRecordEncodedLength(
                    allocator,
                    &tokenizer,
                    training_records[i],
                    opts.seq_len,
                ),
                else => try gliner2_data.measureSimpleExampleRequiredLength(
                    allocator,
                    &tokenizer,
                    examples[i],
                    entity_types,
                    opts.seq_len,
                ),
            };
            if (used > scanned_max) scanned_max = used;
        }
        i = 0;
        while (i < eval_examples.len) : (i += 1) {
            const used = switch (opts.objective) {
                .gliner2_total_loss => try gliner2_data.measureUpstreamRecordEncodedLength(
                    allocator,
                    &tokenizer,
                    eval_records[i],
                    opts.seq_len,
                ),
                else => try gliner2_data.measureSimpleExampleRequiredLength(
                    allocator,
                    &tokenizer,
                    eval_examples[i],
                    entity_types,
                    opts.seq_len,
                ),
            };
            if (used > scanned_max) scanned_max = used;
        }
        const floor_len: usize = @as(usize, @intCast(opts.max_span_width)) + 8;
        const rounded = (@max(scanned_max, floor_len) + 7) / 8 * 8;
        break :blk @min(@as(usize, 512), @min(opts.seq_len, rounded));
    };
    print("  effective seq_len: {d} (cap --seq-len={d})\n", .{ effective_seq_len, opts.seq_len });

    const structure_max_instances: u32 = if (total_loss_dataset_axes) |axes|
        axes.structure_instances
    else
        1;
    if (structure_max_instances > 1) {
        print("  structure max instances (per-instance struct loss): {d}\n", .{structure_max_instances});
    }
    const max_schema_tasks: u32 = if (total_loss_dataset_axes) |axes|
        axes.schema_tasks
    else
        1;
    if (opts.objective == .gliner2_total_loss) {
        print("  max schema/task rows per sample: {d}\n", .{max_schema_tasks});
        if (opts.structure_span_chunk_samples > 0) {
            print("  structure span chunk samples: {d}\n", .{opts.structure_span_chunk_samples});
        }
    }
    const total_loss_graph_limits = TotalLossGraphLimits{
        .seq_len = effective_seq_len,
        .schema_slots = entity_types.len,
        .structure_instances = structure_max_instances,
        .schema_tasks = max_schema_tasks,
        .max_span_width = opts.max_span_width,
    };
    if (opts.objective == .gliner2_total_loss) {
        print("  graph shapes: batch-local seq/slots/instances/tasks; cache capacity={d} (active included)\n", .{opts.graph_cache_capacity});
    }

    // ------------------------------------------------------------------
    // 6d. Memory pre-flight. The disentangled-attention intermediates scale
    // batch*S^2 and have previously hard-OOMed the whole machine (not just
    // this process) on 16GB hosts. Legacy frame-retained execution must budget
    // every layer's transient attention workspace. Metal's default in-frame
    // private-buffer reuse budgets the live workspace instead, matching the
    // allocator behavior this gate is meant to prove.
    // ------------------------------------------------------------------
    var estimated_peak_bytes: u64 = 0;
    {
        const reuse_preflight = selected_backend == .metal and metalBufferReuseEnabledForPreflight();
        const span_objective = opts.objective == .span_start or opts.objective == .gliner2_total_loss;
        // The upstream batch builder allocates one word row per token, making
        // this the exact fixed-shape span-row count for the selected graph.
        const est_max_spans: u64 = @as(u64, effective_seq_len) * @as(u64, @max(opts.max_span_width, 1));
        const est_entity_types: u64 = @intCast(@max(entity_types.len, 1));
        const preflight_batch_size: u64 = @intCast(effective_batch_size);
        const est = estimateTrainingPeakBytes(
            @intCast(deberta_config.vocab_size),
            @intCast(deberta_config.hidden_size),
            @intCast(deberta_config.intermediate_size),
            @intCast(deberta_config.num_hidden_layers),
            @intCast(deberta_config.num_attention_heads),
            preflight_batch_size,
            effective_seq_len,
            reuse_preflight,
            span_objective,
            opts.objective == .gliner2_total_loss,
            est_max_spans,
            est_entity_types,
            structure_max_instances,
            max_schema_tasks,
        );
        estimated_peak_bytes = est;
        // Budget against the OS's memory report. Prefer *available* memory (accounts
        // for the base model + other processes already resident) at 80%; fall back to
        // 60% of physical when the OS doesn't report available. Works on macOS and
        // Linux (via /proc/meminfo); on unsupported hosts the whole check is skipped.
        if (inference.runtime.tier.memory.currentSystemMemoryInfo()) |mem_info| {
            const total: u64 = @intCast(mem_info.total_bytes);
            const basis_bytes: u64 = if (mem_info.available_bytes) |a| @intCast(a) else total;
            const budget: u64 = if (mem_info.available_bytes != null) basis_bytes * 8 / 10 else basis_bytes * 6 / 10;
            print("  estimated peak memory ({s}): {d:.2} GiB (budget {d:.2} GiB of {d:.2} GiB {s})\n", .{
                if (reuse_preflight) "metal-reuse-live-set" else "frame-retained",
                @as(f64, @floatFromInt(est)) / (1024.0 * 1024.0 * 1024.0),
                @as(f64, @floatFromInt(budget)) / (1024.0 * 1024.0 * 1024.0),
                @as(f64, @floatFromInt(basis_bytes)) / (1024.0 * 1024.0 * 1024.0),
                if (mem_info.available_bytes != null) "available" else "physical",
            });
            if (est > budget and !opts.allow_large_memory) {
                print("error: estimated peak memory exceeds the safe budget; lower --batch-size or --seq-len (attention intermediates scale batch*seq^2), enable Metal buffer reuse, or pass --allow-large-memory to proceed anyway\n", .{});
                return error.EstimatedMemoryExceedsBudget;
            }
            if (est > budget) {
                print("warning: --allow-large-memory set; proceeding past the safe memory budget\n", .{});
            }
        }
    }

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
    const resolved_target_patterns = try gliner2_bundle.expandLoRATargetModules(allocator, target_patterns.items);
    defer {
        for (resolved_target_patterns) |item| allocator.free(item);
        allocator.free(resolved_target_patterns);
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
        .layer_norm_eps = deberta_config.layer_norm_eps,
    };

    if (selected_backend == .metal) {
        const preplan_config = debertaArchConfigFromJson(deberta_config);
        const preplanned = deberta_arch.preplanMetalDebertaEncoderFrame(
            &cb,
            allocator,
            preplan_config,
            @intCast(effective_batch_size),
            effective_seq_len,
        ) catch |err| blk: {
            print("warning: Metal DeBERTa encoder frame preplan failed: {s}; continuing with graph runtime fallback\n", .{@errorName(err)});
            break :blk false;
        };
        print("  metal deberta encoder frame preplan: {s}\n", .{if (preplanned) "ready" else "not-ready"});
    }

    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = effective_num_classes,
        .objective = opts.objective,
        .span_start_loss = opts.span_loss,
        .span_start_loss_reduction = opts.span_loss_reduction,
        .span_start_positive_weight = opts.span_positive_weight,
        .span_start_negative_weight = opts.span_negative_weight,
        .structure_max_instances = structure_max_instances,
        .max_schema_tasks = max_schema_tasks,
        .structure_span_chunk_samples = opts.structure_span_chunk_samples,
    });

    // ------------------------------------------------------------------
    // 9. Initialize the RealAutodiffTrainer
    // ------------------------------------------------------------------
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = opts.lora_rank,
        .alpha = opts.lora_alpha,
        .dropout = opts.lora_dropout,
        .target_patterns = resolved_target_patterns,
        .strict_target_patterns = true,
    };
    const regular_trainable_params_with_classifier = [_][]const u8{ "task_classifier.weight", "task_classifier.bias" };
    const regular_trainable_params_bias_only = [_][]const u8{"task_classifier.bias"};
    const no_regular_trainable_params = [_][]const u8{};
    const regular_trainable_params = if (opts.objective == .gliner2_total_loss or opts.lora_only_trainables)
        no_regular_trainable_params[0..]
    else if (stringSliceContains(resolved_target_patterns, "classifier"))
        regular_trainable_params_bias_only[0..]
    else
        regular_trainable_params_with_classifier[0..];
    print("  trainable mode: LoRA{s} regular_trainable_params={d}\n", .{
        if (regular_trainable_params.len == 0) " only" else " + regular head",
        regular_trainable_params.len,
    });

    const total_examples = examples.len;
    // Match the pinned upstream DataLoader: shrink one small dataset batch,
    // otherwise drop a final partial training batch.
    const drop_last = total_examples > @as(usize, opts.batch_size);
    const examples_per_epoch = if (drop_last)
        (total_examples / effective_batch_size) * effective_batch_size
    else
        total_examples;
    const steps_per_epoch = examples_per_epoch / effective_batch_size;
    const steps_per_epoch_u64: u64 = @intCast(steps_per_epoch);
    const grad_accum_u64: u64 = opts.grad_accum;
    // Accelerate flushes a final partial accumulation at the end of each
    // epoch. Plan that update too; floor division would make the optimizer
    // target stop the data loop before the last micro-batch.
    const optimizer_steps_per_epoch = optimizerStepsForMicroBatches(steps_per_epoch_u64, grad_accum_u64);
    // Upstream GLiNER2 sizes the LR schedule with floor division
    // (len(train_loader) // grad_accum, clamped to >= 1) while still
    // executing the end-of-epoch partial flush. Flush steps past that
    // schedule horizon run at LR 0 (both LambdaLR's linear lambda and our
    // warmup_linear clamp to zero there), so they are weight no-ops on both
    // sides. Mirror it exactly: the schedule and warmup use the floor-based
    // horizon; the execution plan keeps the ceil-based per-epoch step count
    // so the flush still runs.
    const schedule_steps_per_epoch: u64 = scheduleStepsForMicroBatches(steps_per_epoch_u64, grad_accum_u64);
    const target_optimizer_steps: u64 = if (opts.max_steps > 0)
        opts.max_steps
    else
        @as(u64, opts.epochs) * optimizer_steps_per_epoch;
    if (target_optimizer_steps > std.math.maxInt(u32)) return error.TooManyOptimizerSteps;
    const schedule_horizon_steps: u64 = if (opts.max_steps > 0)
        opts.max_steps
    else
        @as(u64, opts.epochs) * schedule_steps_per_epoch;
    const planned_epoch_count: u64 = if (opts.max_steps > 0)
        (target_optimizer_steps + schedule_steps_per_epoch - 1) / schedule_steps_per_epoch
    else
        opts.epochs;
    const configured_warmup_steps: u64 = if (opts.warmup_steps > 0)
        opts.warmup_steps
    else
        @intFromFloat(@floor(@as(f64, @floatFromInt(schedule_horizon_steps)) * @as(f64, opts.warmup_ratio)));
    if (configured_warmup_steps > std.math.maxInt(u32)) return error.TooManyWarmupSteps;
    // Match Transformers: an explicit warmup may exceed the run length, in
    // which case every executed optimizer step remains in linear warmup.
    const resolved_warmup_steps: u32 = @intCast(configured_warmup_steps);
    const schedule_total_steps: u32 = @intCast(schedule_horizon_steps);
    const lr_schedule: optimizers.LearningRateSchedule = switch (opts.lr_scheduler) {
        .linear => .{ .warmup_linear = .{
            .initial_lr = opts.learning_rate,
            .warmup_steps = resolved_warmup_steps,
            .total_steps = schedule_total_steps,
        } },
        .cosine => .{ .warmup_cosine = .{
            .initial_lr = opts.learning_rate,
            .min_lr = 0.0,
            .warmup_steps = resolved_warmup_steps,
            .total_steps = schedule_total_steps,
        } },
        .cosine_restarts => .{ .warmup_cosine_restarts = .{
            .initial_lr = opts.learning_rate,
            .warmup_steps = resolved_warmup_steps,
            .total_steps = schedule_total_steps,
            .num_cycles = opts.num_cycles,
        } },
        .constant => .{ .warmup_constant = .{
            .initial_lr = opts.learning_rate,
            .warmup_steps = resolved_warmup_steps,
        } },
    };
    print("  lr_scheduler={s} warmup_steps={d} optimizer_steps={d} schedule_horizon={d}\n", .{
        @tagName(opts.lr_scheduler),
        resolved_warmup_steps,
        target_optimizer_steps,
        schedule_horizon_steps,
    });
    const resolved_activation_checkpoint_config = activationCheckpointConfig(
        opts.activation_checkpointing,
        opts.activation_checkpoint_interval,
        opts.activation_checkpoint_strategy,
    );
    const checkpointing_enabled = opts.checkpoint_every_epochs > 0 or opts.resume_checkpoint != null or opts.eval_data != null;
    if (checkpointing_enabled and train_data_sha256 == null) return error.CheckpointRequiresSingleTrainingFile;
    if (checkpointing_enabled and opts.initial_adapter_checkpoint != null) return error.CheckpointWithInitialAdapterUnsupported;
    if (checkpointing_enabled and selected_backend == .metal and !opts.compiled_required) {
        // A permissive Metal run may switch between compiled and interpreter
        // execution across process boundaries. Require one execution contract
        // so a checkpoint resume cannot silently change numerical order.
        return error.MetalCheckpointRequiresCompiledExecution;
    }
    const training_executable_sha256 = if (checkpointing_enabled)
        try selfExecutableSha256Alloc(allocator)
    else
        null;
    defer if (training_executable_sha256) |value| allocator.free(value);
    const training_runtime_identity = if (checkpointing_enabled)
        try runtimeMathIdentityAlloc(allocator, selected_backend)
    else
        null;
    defer if (training_runtime_identity) |value| allocator.free(value);
    const training_state_fingerprint: ?[32]u8 = if (checkpointing_enabled)
        try trainingStateFingerprint(
            allocator,
            opts,
            backendLabel(selected_backend),
            train_data_sha256.?,
            eval_data_sha256,
            base_model_fingerprint_sha256,
            training_executable_sha256.?,
            training_runtime_identity.?,
            resolved_target_patterns,
            entity_types,
            manifest_entity_labels,
            regular_trainable_params,
            resolved_span_label_positive_weights,
            effective_seq_len,
            effective_batch_size,
            effective_num_classes,
            structure_max_instances,
            max_schema_tasks,
            examples_per_epoch,
            target_optimizer_steps,
            resolved_warmup_steps,
            resolved_activation_checkpoint_config,
        )
    else
        null;
    const training_state_fingerprint_sha256 = if (training_state_fingerprint) |fingerprint|
        try digestHexAlloc(allocator, &fingerprint)
    else
        null;
    defer if (training_state_fingerprint_sha256) |value| allocator.free(value);

    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = lora_config,
            .optimizer = .{ .weight_decay = opts.weight_decay },
            .lr_schedule = lr_schedule,
            .max_grad_norm = opts.max_grad_norm,
            .grad_accum_steps = opts.grad_accum,
            .hidden_size_hint = deberta_config.hidden_size,
            .num_layers_hint = deberta_config.num_hidden_layers,
            .seed = opts.seed,
            .regular_trainable_params = regular_trainable_params,
            .execution_engine = switch (selected_backend) {
                .metal => .compiled_metal,
                else => .interpreter,
            },
            .compiled_required = opts.compiled_required,
            .checkpoint_config = resolved_activation_checkpoint_config,
            .graph_cache_capacity = opts.graph_cache_capacity,
            .graph_cache_build_reserve_bytes = estimated_peak_bytes,
        },
    );
    defer trainer.deinit();

    if (opts.objective == .gliner2_total_loss) {
        // Upstream only builds loss terms for tasks present in the batch, so
        // PyTorch leaves grad=None (no Adam step, no weight decay) for these
        // head modules whenever the accumulation window carried no matching
        // task. Mirror that by gating their optimizer step on per-window
        // task presence, marked from each micro-batch's task_type_ids below.
        try trainer.registerConditionalOptimizerFamily("classifier.");
        try trainer.registerConditionalOptimizerFamily("count_pred.");
        try trainer.registerConditionalOptimizerFamily("span_rep.");
        try trainer.registerConditionalOptimizerFamily("count_embed.");
    }

    // ------------------------------------------------------------------
    // 10. Training loop
    // ------------------------------------------------------------------
    if (opts.max_steps > 0) {
        print("\nStarting training: max_steps={d}, {d} steps/epoch ({d} examples), planned data passes={d}\n", .{
            opts.max_steps,
            steps_per_epoch,
            total_examples,
            planned_epoch_count,
        });
    } else {
        print("\nStarting training: {d} epochs x {d} steps/epoch ({d} examples)\n", .{
            opts.epochs,
            steps_per_epoch,
            total_examples,
        });
    }

    // Pre-allocate batch buffers (sized to the fit-to-data effective length).
    const sl: usize = effective_seq_len;
    const bs = effective_batch_size;
    const nc: usize = effective_num_classes;
    const batch_tokens = bs * sl;
    const use_label_positive_weights = opts.span_label_positive_weights != null;
    const span_entity_types: usize = if (effective_num_classes > 1) @as(usize, @intCast(effective_num_classes)) - 1 else 0;
    const span_target_width: usize = if (opts.objective == .gliner2_total_loss)
        gliner2_autodiff.gliner2TotalLossTargetWidthEx(span_entity_types, structure_max_instances)
    else if (use_label_positive_weights)
        gliner2_autodiff.weightedSpanStartTargetWidth(span_entity_types)
    else
        gliner2_autodiff.spanStartTargetWidth(span_entity_types);
    // The batch builder floors words-per-sample at 10 (gliner2_data.computeMaxWordsPerSample),
    // so at a very short effective seq_len it emits max_spans = 10 * max_span_width, which
    // exceeds sl * max_span_width. Size targets_buf for that floor to avoid a heap overflow.
    const max_words_per_sample = @max(sl, 10);
    const max_span_target_values = bs * max_words_per_sample * @as(usize, @intCast(opts.max_span_width)) * span_target_width;
    const target_buf_values = @max(batch_tokens * nc, max_span_target_values);

    var input_ids = try allocator.alloc(i64, batch_tokens);
    defer allocator.free(input_ids);
    var attention_mask = try allocator.alloc(f32, batch_tokens);
    defer allocator.free(attention_mask);
    // Token mode: [batch * seq_len, num_classes].
    // Span mode: [batch * max_spans, 2 * entity_types + 2], or
    // [batch * max_spans, 3 * entity_types + 2] when per-label positive
    // weights are packed into the target tensor.
    var targets_buf = try allocator.alloc(f32, target_buf_values);
    defer allocator.free(targets_buf);
    // Staging windows for one full effective batch. Upstream drops a final
    // partial batch when the dataset is larger than the requested batch.
    var batch_examples = try allocator.alloc(gliner2_data.Example, bs);
    defer allocator.free(batch_examples);
    var batch_records = try allocator.alloc(gliner2_data.UpstreamRecord, bs);
    defer allocator.free(batch_records);

    if (opts.initial_adapter_checkpoint != null or opts.resume_checkpoint != null) {
        try ensureTrainerGraphBuiltFromFirstBatch(
            allocator,
            opts,
            effective_seq_len,
            total_loss_graph_limits,
            &tokenizer,
            entity_types,
            examples,
            training_records,
            &label_map,
            effective_num_classes,
            use_label_positive_weights,
            resolved_span_label_positive_weights,
            input_ids,
            attention_mask,
            targets_buf,
            &trainer,
            &gliner_ctx,
        );
    }
    if (opts.initial_adapter_checkpoint) |checkpoint_path| {
        try loadPeftAdaptersIntoTrainer(allocator, checkpoint_path, &trainer);
        print("  loaded initial LoRA adapter checkpoint: {s}\n", .{checkpoint_path});
    }
    if (opts.resume_checkpoint) |checkpoint_path| {
        try trainer.loadTrainingState(checkpoint_path, &training_state_fingerprint.?);
        print("  restored training state: {s} (micro_batches={d}, optimizer_steps={d})\n", .{
            checkpoint_path,
            trainer.microBatchSteps(),
            trainer.optimizerSteps(),
        });
    }

    const resumed_micro_batches = trainer.microBatchSteps();
    const resumed_optimizer_steps = trainer.optimizerSteps();
    if (resumed_micro_batches % steps_per_epoch_u64 != 0) return error.CheckpointNotAtEpochBoundary;
    const resumed_epochs = resumed_micro_batches / steps_per_epoch_u64;
    if (resumed_optimizer_steps > target_optimizer_steps or resumed_epochs > planned_epoch_count) return error.CheckpointPastRequestedRun;

    const metrics_path = try std.fs.path.join(allocator, &.{ opts.out_dir, run_validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const checkpoint_dir = try std.fs.path.join(allocator, &.{ opts.out_dir, "checkpoints" });
    defer allocator.free(checkpoint_dir);
    const retained_resume_checkpoint_relative_path = if (resume_checkpoint_sha256) |digest|
        try retainedResumeCheckpointRelativePathAlloc(allocator, digest)
    else
        null;
    defer if (retained_resume_checkpoint_relative_path) |path| allocator.free(path);
    const retained_resume_checkpoint_path = if (retained_resume_checkpoint_relative_path) |relative_path|
        try std.fs.path.join(allocator, &.{ opts.out_dir, relative_path })
    else
        null;
    defer if (retained_resume_checkpoint_path) |path| allocator.free(path);
    const resume_metrics = if (opts.resume_checkpoint != null)
        try inspectResumeMetrics(
            allocator,
            metrics_path,
            resumed_micro_batches,
            resumed_optimizer_steps,
            resumed_epochs,
            entity_types.len,
            if (opts.eval_data != null) opts.eval_every_epochs else 0,
            opts.early_stopping_threshold,
        )
    else
        ResumeMetrics{};
    if (opts.resume_checkpoint) |checkpoint_path| {
        const metrics_prefix_sha256 = try sha256FilePrefixDigest(metrics_path, resume_metrics.prefix_len);
        try trainer.validateTrainingStateMetricsPrefix(checkpoint_path, &metrics_prefix_sha256);
        try truncateFileToPrefix(metrics_path, resume_metrics.prefix_len);
        try retainResumeCheckpoint(
            allocator,
            checkpoint_path,
            retained_resume_checkpoint_path.?,
            resume_checkpoint_sha256.?,
        );
    }
    if (checkpointing_enabled) {
        // A reused output directory can contain checkpoints from an abandoned
        // future. Remove them before this run can advertise or retain them.
        try prunePeriodicCheckpoints(allocator, checkpoint_dir, resumed_epochs, opts.checkpoint_keep_last);
        try pruneBestCheckpointsAfter(allocator, checkpoint_dir, resumed_epochs);
    }
    if (opts.eval_data != null) {
        if (opts.resume_checkpoint != null and resume_metrics.eval_state.best_loss != null) {
            const best_checkpoint_path = try bestCheckpointPathAlloc(
                allocator,
                checkpoint_dir,
                resume_metrics.eval_state.best_epoch,
            );
            defer allocator.free(best_checkpoint_path);
            const best_counters = try trainer.inspectTrainingState(best_checkpoint_path, &training_state_fingerprint.?);
            const expected_best_micro_batches = try std.math.mul(u64, resume_metrics.eval_state.best_epoch, steps_per_epoch_u64);
            if (best_counters.micro_batch_steps != expected_best_micro_batches) return error.BestCheckpointDoesNotMatchResumeMetrics;
            if (resume_metrics.best_metrics_prefix_len == 0) return error.BestCheckpointDoesNotMatchResumeMetrics;
            const best_metrics_prefix_sha256 = try sha256FilePrefixDigest(metrics_path, resume_metrics.best_metrics_prefix_len);
            try trainer.validateTrainingStateMetricsPrefix(best_checkpoint_path, &best_metrics_prefix_sha256);
        }
    }

    var rng = std.Random.DefaultPrng.init(opts.seed);
    var prng = rng.random();
    replayCompletedEpochShuffles(&prng, opts, examples, training_records, resumed_epochs);

    var cumulative_loss: f64 = resume_metrics.loss_sum;
    var total_steps: u64 = resume_metrics.step_count;
    var completed_epochs: u64 = resume_metrics.epoch_count;
    var run_target_stats = resume_metrics.target_stats;
    var eval_state = resume_metrics.eval_state;
    var early_stopped = resumeStopsBeforeTraining(opts.resume_checkpoint != null, eval_state, opts.early_stopping_patience);
    var metrics_file = if (opts.resume_checkpoint != null)
        try compat.cwd().openFile(compat.io(), metrics_path, .{ .mode = .read_write })
    else
        try compat.cwd().createFile(compat.io(), metrics_path, .{ .truncate = true });
    defer metrics_file.close(compat.io());
    if (opts.resume_checkpoint == null) try syncDirectoryChain(opts.out_dir);
    var metrics_buffer: [64 * 1024]u8 = undefined;
    var metrics_file_writer = metrics_file.writer(compat.io(), &metrics_buffer);
    if (opts.resume_checkpoint != null) {
        const metrics_stat = try metrics_file.stat(compat.io());
        try metrics_file_writer.seekTo(metrics_stat.size);
    }
    defer metrics_file_writer.end() catch {};
    const metrics_writer = &metrics_file_writer.interface;

    var epoch_idx: u64 = resumed_epochs;
    while (trainer.optimizerSteps() < target_optimizer_steps and
        (opts.max_steps > 0 or epoch_idx < opts.epochs) and
        !early_stopped) : (epoch_idx += 1)
    {
        const epoch_number: usize = @intCast(epoch_idx + 1);
        // Shuffle examples at the start of each epoch.
        if (opts.objective == .gliner2_total_loss) {
            // --deterministic pins batch order for cross-runtime parity runs,
            // matching the Python harness's --no-train-shuffle loader.
            if (!opts.dump_span_parity and !opts.deterministic) shuffleExamplesAndRecords(&prng, examples, training_records);
        } else {
            prng.shuffle(gliner2_data.Example, examples);
        }

        const epoch_started_ns = monotonicNowNs();
        var epoch_loss: f64 = 0.0;
        var epoch_steps: u64 = 0;
        var epoch_target_stats = BatchTargetStats{};

        var batch_start: usize = 0;
        while (batch_start < examples_per_epoch and trainer.optimizerSteps() < target_optimizer_steps) {
            const batch_end = batch_start + bs;
            for (0..bs) |slot| {
                const src = batch_start + slot;
                batch_examples[slot] = examples[src];
                if (opts.objective == .gliner2_total_loss) batch_records[slot] = training_records[src];
            }
            const actual_batch: u32 = @intCast(bs);
            const ab: usize = bs;
            const total_loss_shape = if (opts.objective == .gliner2_total_loss)
                try totalLossBatchShape(allocator, &tokenizer, batch_records[0..ab], total_loss_graph_limits)
            else
                null;
            if (total_loss_shape) |shape| shape.apply(&gliner_ctx);
            const batch_sl = if (total_loss_shape) |shape| shape.seq_len else sl;
            const batch_entity_types = if (total_loss_shape) |shape| shape.schema_slots else entity_types.len;
            const batch_structure_instances = if (total_loss_shape) |shape| shape.structure_instances else structure_max_instances;

            // Tokenize batch + build entity/span targets.
            const step_started_ns = monotonicNowNs();
            var target_stats = BatchTargetStats{};
            var targets_shape: ml.graph.Shape = undefined;
            var target_slice: []const f32 = undefined;
            // Task families present in this micro-batch (gliner2-total-loss
            // only); feeds the conditional optimizer-family gating so absent
            // heads mirror upstream's grad=None non-steps.
            var batch_has_classifications = false;
            var batch_has_structure = false;
            var batch_has_nonentity_structure = false;
            switch (opts.objective) {
                .token => {
                    target_stats = fillBatchBuffers(
                        allocator,
                        &tokenizer,
                        entity_types,
                        batch_examples,
                        @intCast(batch_sl),
                        opts.num_classes,
                        &label_map,
                        input_ids,
                        attention_mask,
                        targets_buf[0 .. ab * batch_sl * nc],
                    );
                    targets_shape = gliner2_autodiff.tokenTargetsShape(
                        actual_batch,
                        @intCast(batch_sl),
                        opts.num_classes,
                    );
                    target_slice = targets_buf[0 .. ab * batch_sl * nc];
                },
                .span_start, .gliner2_total_loss => {
                    var encoded = if (opts.objective == .gliner2_total_loss)
                        try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
                            allocator,
                            &tokenizer,
                            batch_records,
                            total_loss_shape.?.schema_slots,
                            batch_sl,
                            opts.max_span_width,
                            ab,
                        )
                    else
                        try gliner2_data.buildSimpleBatch(
                            allocator,
                            &tokenizer,
                            batch_examples,
                            entity_types,
                            batch_sl,
                            opts.max_span_width,
                            ab,
                        );
                    defer encoded.deinit();

                    for (encoded.task_type_ids) |task_id| switch (task_id) {
                        gliner2_data.upstreamTaskTypeId(.entities) => batch_has_structure = true,
                        gliner2_data.upstreamTaskTypeId(.json_structures), gliner2_data.upstreamTaskTypeId(.relations) => {
                            batch_has_structure = true;
                            batch_has_nonentity_structure = true;
                        },
                        gliner2_data.upstreamTaskTypeId(.classifications) => batch_has_classifications = true,
                        else => {},
                    };

                    if (encoded.input_ids.len != ab * batch_sl or encoded.attention_mask.len != ab * batch_sl) return error.InvalidGlinerBatchShape;
                    for (0..ab * batch_sl) |i| {
                        input_ids[i] = encoded.input_ids[i];
                        attention_mask[i] = @floatFromInt(encoded.attention_mask[i]);
                    }

                    const width = if (opts.objective == .gliner2_total_loss)
                        gliner2_autodiff.gliner2TotalLossTargetWidthEx(encoded.num_entity_types, batch_structure_instances)
                    else if (use_label_positive_weights)
                        gliner2_autodiff.weightedSpanStartTargetWidth(encoded.num_entity_types)
                    else
                        gliner2_autodiff.spanStartTargetWidth(encoded.num_entity_types);
                    const target_len = encoded.batch_size * encoded.max_spans * width;
                    _ = if (opts.objective == .gliner2_total_loss)
                        try fillGliner2TotalLossTargetsFromRecords(
                            allocator,
                            &encoded,
                            batch_records,
                            batch_structure_instances,
                            targets_buf[0..target_len],
                        )
                    else
                        try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatchWithOptions(
                            &encoded,
                            .{
                                .positive_weights_by_entity_type = if (use_label_positive_weights) resolved_span_label_positive_weights else null,
                                .hard_negative_weight = opts.span_hard_negative_weight,
                            },
                            targets_buf[0..target_len],
                        );
                    if (opts.span_negative_mask_rate > 0.0) {
                        if (opts.objective == .gliner2_total_loss and batch_structure_instances > 1) {
                            applyGliner2InstanceNegativeMask(
                                targets_buf[0..target_len],
                                encoded.num_entity_types,
                                width,
                                batch_structure_instances,
                                opts.span_negative_mask_rate,
                                opts.seed ^ total_steps,
                            );
                        } else {
                            applySpanNegativeMask(
                                targets_buf[0..target_len],
                                encoded.num_entity_types,
                                width,
                                opts.span_negative_mask_rate,
                                opts.seed ^ total_steps,
                            );
                        }
                    }
                    target_stats = try BatchTargetStats.fromPackedSpanTargets(
                        targets_buf[0..target_len],
                        width,
                        encoded.num_entity_types,
                        opts.objective,
                        batch_structure_instances,
                    );
                    targets_shape = if (opts.objective == .gliner2_total_loss)
                        gliner2_autodiff.gliner2TotalLossTargetsShapeEx(
                            actual_batch,
                            @intCast(encoded.max_spans),
                            @intCast(encoded.num_entity_types),
                            batch_structure_instances,
                        )
                    else if (use_label_positive_weights)
                        gliner2_autodiff.weightedSpanStartTargetsShape(
                            actual_batch,
                            @intCast(encoded.max_spans),
                            @intCast(encoded.num_entity_types),
                        )
                    else
                        gliner2_autodiff.spanStartTargetsShape(
                            actual_batch,
                            @intCast(encoded.max_spans),
                            @intCast(encoded.num_entity_types),
                        );
                    target_slice = targets_buf[0..target_len];
                    if (opts.dump_span_parity and total_steps == 0) {
                        printSpanPreprocessDebug(&encoded);
                    }
                },
            }
            const target_built_ns = monotonicNowNs();

            // Build TrainerInput via the GLiNER2 convenience builder.
            const trainer_input = gliner2_autodiff.makeTrainerInput(
                &gliner_ctx,
                input_ids[0 .. ab * batch_sl],
                attention_mask[0 .. ab * batch_sl],
                target_slice,
                targets_shape,
                actual_batch,
                @intCast(batch_sl),
            );

            if (opts.dump_span_parity and opts.objective == .span_start and total_steps == 0) {
                const logits = try gliner2_autodiff.spanStartLogitsForBatch(
                    allocator,
                    &trainer,
                    &gliner_ctx,
                    input_ids[0 .. ab * batch_sl],
                    attention_mask[0 .. ab * batch_sl],
                    target_slice,
                    targets_shape,
                    actual_batch,
                    @intCast(batch_sl),
                );
                defer allocator.free(logits);
                try printSpanParityDebug(logits, target_slice, targets_shape, batch_entity_types, use_label_positive_weights, opts);
                const components = try gliner2_autodiff.spanStartComponentDebugForBatch(
                    allocator,
                    &trainer,
                    &gliner_ctx,
                    input_ids[0 .. ab * batch_sl],
                    attention_mask[0 .. ab * batch_sl],
                    target_slice,
                    targets_shape,
                    actual_batch,
                    @intCast(batch_sl),
                    batch_entity_types,
                );
                printSpanComponentDebug(components);
            }
            if (opts.dump_span_parity and opts.objective == .gliner2_total_loss and total_steps == 0) {
                // The legacy span-parity dumps assume the single-instance
                // structure logits shape `[span_rows, E]`. The per-instance
                // structure loss emits `[span_rows, max_gold * E]`, so skip
                // those dumps there and still emit the component-loss debug
                // (which the parity harness reads).
                // ponytail: Metal full-task parity only needs total/classification debug; the legacy span dump is schema-unsafe there.
                if (gliner_ctx.config.structure_max_instances == 1 and selected_backend != .metal) {
                    const logits = try gliner2_autodiff.spanStartLogitsForBatch(
                        allocator,
                        &trainer,
                        &gliner_ctx,
                        input_ids[0 .. ab * batch_sl],
                        attention_mask[0 .. ab * batch_sl],
                        target_slice,
                        targets_shape,
                        actual_batch,
                        @intCast(batch_sl),
                    );
                    defer allocator.free(logits);
                    try printSpanParityDebug(logits, target_slice, targets_shape, batch_entity_types, false, opts);
                }
                const components = try gliner2_autodiff.gliner2TotalLossComponentDebugForBatch(
                    allocator,
                    &trainer,
                    &gliner_ctx,
                    input_ids[0 .. ab * batch_sl],
                    attention_mask[0 .. ab * batch_sl],
                    target_slice,
                    targets_shape,
                    actual_batch,
                    @intCast(batch_sl),
                );
                printGliner2TotalLossComponentDebug(components);
                const classification_logits = try gliner2_autodiff.gliner2ClassificationLogitsForBatch(
                    allocator,
                    &trainer,
                    &gliner_ctx,
                    input_ids[0 .. ab * batch_sl],
                    attention_mask[0 .. ab * batch_sl],
                    target_slice,
                    targets_shape,
                    actual_batch,
                    @intCast(batch_sl),
                );
                defer allocator.free(classification_logits);
                const classification_targets = try compactGliner2TaskRowTargetsForDebug(
                    allocator,
                    target_slice,
                    targets_shape,
                    actual_batch,
                    gliner_ctx.config.max_schema_tasks,
                );
                defer allocator.free(classification_targets.targets);
                try printGliner2ClassificationDebug(classification_logits, classification_targets.targets, classification_targets.shape, batch_entity_types);
            }

            if (opts.objective == .gliner2_total_loss) {
                if (batch_has_classifications) trainer.markOptimizerFamilyPresent("classifier.");
                if (batch_has_structure) {
                    trainer.markOptimizerFamilyPresent("span_rep.");
                    trainer.markOptimizerFamilyPresent("count_embed.");
                }
                if (batch_has_nonentity_structure) trainer.markOptimizerFamilyPresent("count_pred.");
            }
            const result = try trainer.step(trainer_input);
            const optimized_loss = optimizedLossForReporting(result.loss, opts.grad_accum);
            const step_finished_ns = monotonicNowNs();
            const timing = StepTiming{
                .target_build_ns = elapsedNs(step_started_ns, target_built_ns),
                .train_step_ns = elapsedNs(target_built_ns, step_finished_ns),
                .step_wall_ns = elapsedNs(step_started_ns, step_finished_ns),
                .profile = result.profile,
            };
            epoch_loss += optimized_loss;
            cumulative_loss += optimized_loss;
            epoch_steps += 1;
            epoch_target_stats.add(target_stats);
            run_target_stats.add(target_stats);
            total_steps += 1;
            if (result.optimizer_stepped) {
                if (opts.dump_optimizer_parity) {
                    try printOptimizerParityDump(allocator, &trainer, trainer.optimizerSteps());
                }
            }
            try writeStepMetric(metrics_writer, epoch_number, total_steps, epoch_steps, trainer.optimizerSteps(), optimized_loss, result.loss, result.grad_norm, result.learning_rate, result.optimizer_stepped, opts.objective, target_stats, timing, batch_sl, total_loss_shape);

            if (opts.report_to == .jsonl) {
                print(
                    "{{\"event\":\"step\",\"step\":{d},\"epoch\":{d},\"epoch_step\":{d},\"loss\":{d:.9},\"grad_norm\":{d:.9},\"lr\":{e:.6}}}\n",
                    .{ total_steps, epoch_number, epoch_steps, optimized_loss, result.grad_norm, result.learning_rate },
                );
            }
            if (total_steps % 10 == 0 or batch_end >= examples_per_epoch or trainer.optimizerSteps() >= target_optimizer_steps) {
                print("  [epoch {d}/{d}] step {d}/{d}  loss={d:.6}  grad_norm={d:.4}  supervised_tok/s={d:.2}{s}\n", .{
                    epoch_number,
                    planned_epoch_count,
                    epoch_steps,
                    steps_per_epoch,
                    optimized_loss,
                    result.grad_norm,
                    timing.supervisedTokensPerSecond(target_stats),
                    if (result.optimizer_stepped) "" else " (accum)",
                });
            }

            batch_start = batch_end;
        }

        if (trainer.optimizerSteps() < target_optimizer_steps) {
            const flush_started_ns = monotonicNowNs();
            if (try trainer.flushAccumulatedGradients()) |flush| {
                if (opts.dump_optimizer_parity) {
                    try printOptimizerParityDump(allocator, &trainer, flush.optimizer_step);
                }
                print("  flushed partial accumulation at optimizer step {d}  grad_norm={d:.4}\n", .{
                    flush.optimizer_step,
                    flush.grad_norm,
                });
                try writeOptimizerStepMetric(metrics_writer, epoch_number, total_steps, flush, elapsedNs(flush_started_ns, monotonicNowNs()));
                if (opts.report_to == .jsonl) {
                    print(
                        "{{\"event\":\"optimizer_step\",\"step\":{d},\"optimizer_step\":{d},\"partial_accumulation\":true,\"micro_batches\":{d},\"grad_norm\":{d:.9},\"lr\":{e:.6}}}\n",
                        .{ total_steps, flush.optimizer_step, flush.micro_batches, flush.grad_norm, flush.learning_rate },
                    );
                }
            }
        }

        const avg_epoch_loss = if (epoch_steps > 0) epoch_loss / @as(f64, @floatFromInt(epoch_steps)) else 0.0;
        completed_epochs += 1;

        // -- End-of-epoch evaluation summary --------------------------------
        var gold_ent_count: u64 = 0;
        for (examples[0..examples_per_epoch]) |ex| gold_ent_count += ex.entities.len;

        print("  epoch {d}/{d} complete -- avg_loss={d:.6}  ({d} gold entities)\n", .{
            epoch_number,
            planned_epoch_count,
            avg_epoch_loss,
            gold_ent_count,
        });
        const epoch_timing = EpochTiming{
            .epoch_wall_ns = elapsedNs(epoch_started_ns, monotonicNowNs()),
        };
        var new_best = false;
        if (opts.eval_data != null and
            epoch_steps == steps_per_epoch_u64 and
            completed_epochs % @as(u64, opts.eval_every_epochs) == 0)
        {
            const eval_loss = try evaluateDatasetLoss(
                allocator,
                opts,
                effective_seq_len,
                total_loss_graph_limits,
                &tokenizer,
                entity_types,
                eval_examples,
                eval_records,
                &label_map,
                effective_num_classes,
                use_label_positive_weights,
                resolved_span_label_positive_weights,
                input_ids,
                attention_mask,
                targets_buf,
                batch_examples,
                batch_records,
                &trainer,
                &gliner_ctx,
            );
            new_best = try eval_state.observe(completed_epochs, eval_loss, opts.early_stopping_threshold);
            try writeEvalMetric(metrics_writer, epoch_number, eval_loss, eval_examples.len, new_best, eval_state);
            early_stopped = eval_state.shouldStop(opts.early_stopping_patience);
            print("  held-out eval -- loss={d:.6}  best={d:.6} (epoch {d}){s}\n", .{
                eval_loss,
                eval_state.best_loss.?,
                eval_state.best_epoch,
                if (new_best) " new-best" else "",
            });
        }
        try writeEpochMetric(metrics_writer, epoch_number, avg_epoch_loss, gold_ent_count, epoch_steps, opts.objective, epoch_target_stats, epoch_timing);
        if (new_best) {
            try metrics_writer.flush();
            try metrics_file.sync(compat.io());
            const metrics_stat = try metrics_file.stat(compat.io());
            const metrics_prefix_sha256 = try sha256FilePrefixDigest(metrics_path, metrics_stat.size);
            try compat.cwd().createDirPath(compat.io(), checkpoint_dir);
            try syncDirectoryChain(checkpoint_dir);
            const best_checkpoint_path = try bestCheckpointPathAlloc(allocator, checkpoint_dir, completed_epochs);
            defer allocator.free(best_checkpoint_path);
            try trainer.saveTrainingState(best_checkpoint_path, &training_state_fingerprint.?, &metrics_prefix_sha256);
            print("  best checkpoint saved: {s}\n", .{best_checkpoint_path});
        }
        if (opts.checkpoint_every_epochs > 0 and
            epoch_steps == steps_per_epoch_u64 and
            completed_epochs % @as(u64, opts.checkpoint_every_epochs) == 0)
        {
            try metrics_writer.flush();
            try metrics_file.sync(compat.io());
            const metrics_stat = try metrics_file.stat(compat.io());
            const metrics_prefix_sha256 = try sha256FilePrefixDigest(metrics_path, metrics_stat.size);
            try compat.cwd().createDirPath(compat.io(), checkpoint_dir);
            try syncDirectoryChain(checkpoint_dir);
            const checkpoint_path = try std.fmt.allocPrint(allocator, "{s}/epoch-{d}.safetensors", .{ checkpoint_dir, completed_epochs });
            defer allocator.free(checkpoint_path);
            try trainer.saveTrainingState(checkpoint_path, &training_state_fingerprint.?, &metrics_prefix_sha256);
            try prunePeriodicCheckpoints(allocator, checkpoint_dir, completed_epochs, opts.checkpoint_keep_last);
            print("  recoverable checkpoint saved: {s}\n", .{checkpoint_path});
        }
        if (early_stopped) {
            print("  early stopping triggered after {d} non-improving evaluations\n", .{eval_state.bad_epochs});
            break;
        }
    }
    const graph_cache_stats = trainer.graphCacheStats();
    try writeGraphCacheMetric(metrics_writer, graph_cache_stats);
    try metrics_writer.flush();
    try metrics_file.sync(compat.io());

    // ------------------------------------------------------------------
    // 11. Save adapters
    // ------------------------------------------------------------------
    const best_checkpoint_relative_path = if (eval_state.best_loss != null)
        try bestCheckpointRelativePathAlloc(allocator, eval_state.best_epoch)
    else
        null;
    defer if (best_checkpoint_relative_path) |path| allocator.free(path);
    const best_checkpoint_path = if (best_checkpoint_relative_path) |relative_path|
        try std.fs.path.join(allocator, &.{ opts.out_dir, relative_path })
    else
        null;
    defer if (best_checkpoint_path) |path| allocator.free(path);
    if (eval_state.best_loss != null) {
        try trainer.loadTrainingStateWeightsForExport(best_checkpoint_path.?, &training_state_fingerprint.?);
        print("  selected best checkpoint from epoch {d} for export\n", .{eval_state.best_epoch});
    }
    try trainer.syncDeviceTrainablesToHost();
    try trainer.saveAdapters(opts.out_dir);
    const autodiff_params = try collectAutodiffAdapterParams(allocator, &trainer);
    defer allocator.free(autodiff_params);
    var peft_export = try gliner2_bundle.exportAutodiffAdaptersAsPeftBundle(
        allocator,
        opts.out_dir,
        opts.model_dir,
        opts.lora_rank,
        opts.lora_alpha,
        opts.lora_dropout,
        target_patterns.items,
        autodiff_params,
    );
    defer gliner2_bundle.freeAutodiffAdapterExportSummary(allocator, &peft_export);
    var regular_params = try collectTaskHeadExportParams(allocator, &trainer, effective_num_classes, deberta_config.hidden_size);
    defer regular_params.deinit();
    var regular_export = try gliner2_bundle.exportAutodiffRegularParamsAsSafetensors(
        allocator,
        opts.out_dir,
        regular_params.params,
    );
    defer gliner2_bundle.freeAutodiffRegularParamExportSummary(allocator, &regular_export);
    const adapter_bundle_fingerprint_sha256 = try gliner2AdapterBundleFingerprintAlloc(allocator, opts.out_dir);
    defer allocator.free(adapter_bundle_fingerprint_sha256);
    const best_checkpoint_sha256 = if (eval_state.best_loss != null)
        (try sha256RegularFileAlloc(allocator, best_checkpoint_path.?)) orelse return error.BestCheckpointNotRegular
    else
        null;
    defer if (best_checkpoint_sha256) |value| allocator.free(value);

    const final_avg = if (total_steps > 0) cumulative_loss / @as(f64, @floatFromInt(total_steps)) else 0.0;
    try writeTrainingManifest(
        allocator,
        opts,
        backendLabel(selected_backend),
        train_data_sha256,
        eval_data_sha256,
        initial_adapter_checkpoint_sha256,
        base_model_fingerprint_sha256,
        adapter_bundle_fingerprint_sha256,
        deberta_config.hidden_size,
        effective_num_classes,
        manifest_entity_labels,
        resolved_span_label_positive_weights,
        examples.len,
        completed_epochs,
        steps_per_epoch,
        total_steps,
        trainer.optimizerSteps(),
        resolved_warmup_steps,
        final_avg,
        trainer.lora_params.items.len,
        peft_export.exported_tensor_count,
        regular_export.exported_tensor_count,
        regular_trainable_params,
        peft_export.resolved_target_modules,
        total_loss_entity_label_positive_counts,
        run_target_stats,
        eval_state,
        early_stopped,
        best_checkpoint_relative_path,
        best_checkpoint_sha256,
        .{
            .checkpoint_path = retained_resume_checkpoint_relative_path,
            .checkpoint_sha256 = resume_checkpoint_sha256,
            .training_state_fingerprint_sha256 = training_state_fingerprint_sha256,
            .restored_micro_batch_steps = if (opts.resume_checkpoint != null) resumed_micro_batches else null,
            .restored_optimizer_steps = if (opts.resume_checkpoint != null) resumed_optimizer_steps else null,
            .restored_epochs = if (opts.resume_checkpoint != null) resumed_epochs else null,
        },
        graph_cache_stats,
    );
    print("\nLoRA adapters saved to {s}\n", .{opts.out_dir});

    print("training complete -- {d} total steps, final avg loss={d:.6}\n", .{ total_steps, final_avg });
}

fn writeGraphCacheMetric(writer: *std.Io.Writer, stats: real_autodiff.RealAutodiffTrainer.GraphCacheStats) !void {
    try std.json.Stringify.value(.{
        .event = "graph_cache",
        .capacity = stats.capacity,
        .build_reserve_bytes = stats.build_reserve_bytes,
        .builds = stats.builds,
        .hits = stats.hits,
        .active_reuses = stats.active_reuses,
        .evictions = stats.evictions,
        .resident_signatures = stats.resident_signatures,
        .peak_resident_signatures = stats.peak_resident_signatures,
        .runtime_input_policy = "active-entry-only",
    }, .{}, writer);
    try writer.writeByte('\n');
}

fn optimizedLossForReporting(raw_loss: f32, grad_accum: u32) f32 {
    return raw_loss / @as(f32, @floatFromInt(grad_accum));
}

fn writeStepMetric(
    writer: *std.Io.Writer,
    epoch: usize,
    global_step: u64,
    epoch_step: u64,
    optimizer_step: u64,
    loss: f32,
    raw_loss: f32,
    grad_norm: f32,
    learning_rate: f32,
    optimizer_stepped: bool,
    objective: gliner2_autodiff.GlinerObjective,
    target_stats: BatchTargetStats,
    timing: StepTiming,
    graph_seq_len: usize,
    total_loss_shape: ?TotalLossBatchShape,
) !void {
    try std.json.Stringify.value(.{
        .event = "step",
        .epoch = epoch,
        .step = global_step,
        .epoch_step = epoch_step,
        .optimizer_step = optimizer_step,
        .loss = loss,
        .raw_loss = raw_loss,
        .grad_norm = grad_norm,
        .learning_rate = learning_rate,
        .optimizer_stepped = optimizer_stepped,
        .supervised_token_count = target_stats.supervised_token_count,
        .entity_token_count = target_stats.entity_token_count,
        .ignored_token_count = target_stats.ignored_token_count,
        .entity_token_rate = target_stats.entityTokenRate(),
        .entity_label_positive_counts = if (objective == .gliner2_total_loss) null else target_stats.positiveCounts(),
        .schema_slot_positive_counts = if (objective == .gliner2_total_loss) target_stats.positiveCounts() else null,
        .graph_seq_len = graph_seq_len,
        .graph_schema_slots = if (total_loss_shape) |shape| shape.schema_slots else null,
        .graph_structure_instances = if (total_loss_shape) |shape| shape.structure_instances else null,
        .graph_schema_tasks = if (total_loss_shape) |shape| shape.schema_tasks else null,
        .target_build_ms = nsToMillis(timing.target_build_ns),
        .train_step_ms = nsToMillis(timing.train_step_ns),
        .step_wall_ms = nsToMillis(timing.step_wall_ns),
        .graph_build_ms = nsToMillis(timing.profile.graph_build_ns),
        .runtime_input_ms = nsToMillis(timing.profile.runtime_input_ns),
        .compile_ms = nsToMillis(timing.profile.compile_ns),
        .autodiff_ms = nsToMillis(timing.profile.autodiff_ns),
        .execute_ms = nsToMillis(timing.profile.execute_ns),
        .extract_ms = nsToMillis(timing.profile.extract_ns),
        .optimizer_update_ms = nsToMillis(timing.profile.optimizer_update_ns),
        .device_optimizer_ms = nsToMillis(timing.profile.device_optimizer_ns),
        .optimizer_backend = @tagName(timing.profile.optimizer_backend),
        .device_trainable_transfer_count = timing.profile.device_resident_transfer_count,
        .device_resident_transfer_count = timing.profile.device_resident_transfer_count,
        .device_trainable_bytes = timing.profile.device_trainable_bytes,
        .graph_executor_fallback_reason = timing.profile.graph_executor_fallback_reason,
        .graph_executor_partitions = timing.profile.graph_executor_partitions,
        .graph_executor_command_dispatches = timing.profile.graph_executor_command_dispatches,
        .graph_executor_planned_dispatches = timing.profile.graph_executor_planned_dispatches,
        .graph_executor_interpreter_fallbacks = timing.profile.graph_executor_interpreter_fallbacks,
        .graph_executor_host_outputs = timing.profile.graph_executor_host_outputs,
        .graph_executor_true_host_outputs = timing.profile.graph_executor_true_host_outputs,
        .graph_executor_host_output_command = timing.profile.graph_executor_host_output_command,
        .graph_executor_host_output_interpreter = timing.profile.graph_executor_host_output_interpreter,
        .graph_executor_host_output_pre_materialized_constant = timing.profile.graph_executor_host_output_pre_materialized_constant,
        .graph_executor_host_output_runtime_region = timing.profile.graph_executor_host_output_runtime_region,
        .graph_executor_host_output_parameter = timing.profile.graph_executor_host_output_parameter,
        .graph_executor_host_output_unattributed = timing.profile.graph_executor_host_output_unattributed,
        .graph_executor_device_outputs = timing.profile.graph_executor_device_outputs,
        .graph_executor_device_output_parameter = timing.profile.graph_executor_device_output_parameter,
        .graph_executor_metal_frame_chunk_boundaries = timing.profile.graph_executor_metal_frame_chunk_boundaries,
        .graph_executor_metal_frame_chunk_promoted_values = timing.profile.graph_executor_metal_frame_chunk_promoted_values,
        .graph_executor_metal_frame_chunk_swept_values = timing.profile.graph_executor_metal_frame_chunk_swept_values,
        .graph_executor_metal_chunk_local_output_peak_bytes = timing.profile.graph_executor_metal_chunk_local_output_peak_bytes,
        .graph_executor_metal_chunk_local_output_live_bytes = timing.profile.graph_executor_metal_chunk_local_output_live_bytes,
        .graph_executor_metal_chunk_local_output_allocations = timing.profile.graph_executor_metal_chunk_local_output_allocations,
        .graph_executor_metal_chunk_local_output_reuse_hits = timing.profile.graph_executor_metal_chunk_local_output_reuse_hits,
        .graph_executor_metal_chunk_local_output_consumed_hints = timing.profile.graph_executor_metal_chunk_local_output_consumed_hints,
        .graph_executor_metal_chunk_local_output_unconsumed_hints = timing.profile.graph_executor_metal_chunk_local_output_unconsumed_hints,
        .graph_executor_metal_chunk_local_output_spill_bytes = timing.profile.graph_executor_metal_chunk_local_output_spill_bytes,
        .graph_executor_metal_chunk_local_output_alias_conflicts = timing.profile.graph_executor_metal_chunk_local_output_alias_conflicts,
        .graph_executor_metal_chunk_local_output_resets = timing.profile.graph_executor_metal_chunk_local_output_resets,
        .graph_executor_metal_chunk_local_output_reset_freed_bytes = timing.profile.graph_executor_metal_chunk_local_output_reset_freed_bytes,
        .graph_executor_metal_chunk_local_output_discard_freed_bytes = timing.profile.graph_executor_metal_chunk_local_output_discard_freed_bytes,
        .graph_executor_metal_chunk_local_output_reset_live_carry_values = timing.profile.graph_executor_metal_chunk_local_output_reset_live_carry_values,
        .graph_executor_metal_gather_input_promotions = timing.profile.graph_executor_metal_gather_input_promotions,
        .graph_executor_metal_gather_input_promotion_bytes = timing.profile.graph_executor_metal_gather_input_promotion_bytes,
        .graph_executor_metal_gather_input_promotion_ms = nsToMillis(timing.profile.graph_executor_metal_gather_input_promotion_ns),
        .graph_executor_metal_gather_output_promotions = timing.profile.graph_executor_metal_gather_output_promotions,
        .graph_executor_metal_gather_output_promotion_bytes = timing.profile.graph_executor_metal_gather_output_promotion_bytes,
        .graph_executor_metal_gather_output_promotion_ms = nsToMillis(timing.profile.graph_executor_metal_gather_output_promotion_ns),
        .graph_executor_metal_reduce_input_promotions = timing.profile.graph_executor_metal_reduce_input_promotions,
        .graph_executor_metal_reduce_input_promotion_bytes = timing.profile.graph_executor_metal_reduce_input_promotion_bytes,
        .graph_executor_metal_reduce_input_promotion_ms = nsToMillis(timing.profile.graph_executor_metal_reduce_input_promotion_ns),
        .graph_executor_metal_resident_input_cache_hits = timing.profile.graph_executor_metal_resident_input_cache_hits,
        .graph_executor_metal_resident_input_cache_misses = timing.profile.graph_executor_metal_resident_input_cache_misses,
        .graph_executor_metal_resident_input_cache_unique_promotions = timing.profile.graph_executor_metal_resident_input_cache_unique_promotions,
        .graph_executor_metal_resident_input_cache_retained_live_bytes = timing.profile.graph_executor_metal_resident_input_cache_retained_live_bytes,
        .graph_executor_metal_resident_input_cache_retained_peak_bytes = timing.profile.graph_executor_metal_resident_input_cache_retained_peak_bytes,
        .graph_executor_metal_resident_input_cache_reused_bytes = timing.profile.graph_executor_metal_resident_input_cache_reused_bytes,
        .graph_executor_metal_resident_input_cache_released_bytes = timing.profile.graph_executor_metal_resident_input_cache_released_bytes,
        .graph_executor_metal_eager_arena_peak_bytes = timing.profile.graph_executor_metal_eager_arena_peak_bytes,
        .graph_executor_metal_eager_arena_live_bytes = timing.profile.graph_executor_metal_eager_arena_live_bytes,
        .graph_executor_metal_eager_arena_reuse_hits = timing.profile.graph_executor_metal_eager_arena_reuse_hits,
        .graph_executor_metal_eager_arena_allocations = timing.profile.graph_executor_metal_eager_arena_allocations,
        .graph_executor_metal_eager_arena_spill_bytes = timing.profile.graph_executor_metal_eager_arena_spill_bytes,
        .graph_executor_metal_eager_arena_hazard_declines = timing.profile.graph_executor_metal_eager_arena_hazard_declines,
        .graph_executor_metal_eager_arena_alias_conflicts = timing.profile.graph_executor_metal_eager_arena_alias_conflicts,
        .graph_executor_metal_eager_arena_alias_reclaims = timing.profile.graph_executor_metal_eager_arena_alias_reclaims,
        .graph_executor_metal_eager_arena_alias_reclaim_bytes = timing.profile.graph_executor_metal_eager_arena_alias_reclaim_bytes,
        .graph_executor_regions = timing.profile.graph_executor_regions,
        .graph_executor_region_ops = timing.profile.graph_executor_region_ops,
        .graph_executor_runtime_region_dispatches = timing.profile.graph_executor_runtime_region_dispatches,
        .graph_executor_runtime_region_fallbacks = timing.profile.graph_executor_runtime_region_fallbacks,
        .graph_executor_runtime_region_active_regions = timing.profile.graph_executor_runtime_region_active_regions,
        .graph_executor_runtime_region_covered_nodes = timing.profile.graph_executor_runtime_region_covered_nodes,
        .graph_executor_runtime_region_elided_nodes = timing.profile.graph_executor_runtime_region_elided_nodes,
        .graph_executor_runtime_region_plan_compiles = timing.profile.graph_executor_runtime_region_plan_compiles,
        .graph_executor_runtime_region_plan_reuses = timing.profile.graph_executor_runtime_region_plan_reuses,
        .graph_executor_runtime_region_pre_skipped_nodes = timing.profile.graph_executor_runtime_region_pre_skipped_nodes,
        .graph_executor_runtime_region_pre_skipped_transposes = timing.profile.graph_executor_runtime_region_pre_skipped_transposes,
        .graph_executor_runtime_region_pre_skip_declined_external_consumers = timing.profile.graph_executor_runtime_region_pre_skip_declined_external_consumers,
        .metal_deberta_ffn_forward_regions = timing.profile.metal_deberta_ffn_forward_regions,
        .metal_deberta_encoder_lora_layer_regions = timing.profile.metal_deberta_encoder_lora_layer_regions,
        .metal_deberta_encoder_lora_residual_layernorm_regions = timing.profile.metal_deberta_encoder_lora_residual_layernorm_regions,
        .metal_deberta_encoder_lora_layer_scaffold_regions = timing.profile.metal_deberta_encoder_lora_layer_scaffold_regions,
        .metal_deberta_encoder_lora_layer_fallbacks = timing.profile.metal_deberta_encoder_lora_layer_fallbacks,
        .metal_lora_backward_regions = timing.profile.metal_lora_backward_regions,
        .metal_low_rank_lora_backward_regions = timing.profile.metal_low_rank_lora_backward_regions,
        .metal_rank_adapter_backward_regions = timing.profile.metal_rank_adapter_backward_regions,
        .metal_ffn_gelu_backward_regions = timing.profile.metal_ffn_gelu_backward_regions,
        .metal_head_mlp_forward_regions = timing.profile.metal_head_mlp_forward_regions,
        .metal_head_mlp_backward_regions = timing.profile.metal_head_mlp_backward_regions,
        .metal_command_dot_general_dispatches = timing.profile.metal_command_dot_general_dispatches,
        .metal_command_head_dot_dispatches = timing.profile.metal_command_head_dot_dispatches,
        .metal_command_transpose_dispatches = timing.profile.metal_command_transpose_dispatches,
        .metal_command_gather_dispatches = timing.profile.metal_command_gather_dispatches,
        .metal_command_reduce_dispatches = timing.profile.metal_command_reduce_dispatches,
        .metal_command_elementwise_dispatches = timing.profile.metal_command_elementwise_dispatches,
        .metal_command_activation_dispatches = timing.profile.metal_command_activation_dispatches,
        .metal_command_activation_backward_dispatches = timing.profile.metal_command_activation_backward_dispatches,
        .metal_command_other_dispatches = timing.profile.metal_command_other_dispatches,
        .graph_executor_runtime_frame_candidates = timing.profile.graph_executor_runtime_frame_candidates,
        .graph_executor_runtime_frame_eligible = timing.profile.graph_executor_runtime_frame_eligible,
        .graph_executor_runtime_frame_metadata_ready = timing.profile.graph_executor_runtime_frame_metadata_ready,
        .graph_executor_runtime_frame_ineligible_no_regions = timing.profile.graph_executor_runtime_frame_ineligible_no_regions,
        .graph_executor_runtime_frame_ineligible_missing_qkv = timing.profile.graph_executor_runtime_frame_ineligible_missing_qkv,
        .graph_executor_runtime_frame_ineligible_missing_attention = timing.profile.graph_executor_runtime_frame_ineligible_missing_attention,
        .graph_executor_runtime_frame_ineligible_missing_ffn = timing.profile.graph_executor_runtime_frame_ineligible_missing_ffn,
        .graph_executor_runtime_frame_ineligible_missing_ple = timing.profile.graph_executor_runtime_frame_ineligible_missing_ple,
        .graph_executor_runtime_frame_ineligible_single_row = timing.profile.graph_executor_runtime_frame_ineligible_single_row,
        .graph_executor_runtime_frame_ineligible_non_layer_order = timing.profile.graph_executor_runtime_frame_ineligible_non_layer_order,
        .graph_executor_runtime_frame_ineligible_shape_mismatch = timing.profile.graph_executor_runtime_frame_ineligible_shape_mismatch,
        .graph_executor_runtime_frame_ineligible_missing_model_metadata = timing.profile.graph_executor_runtime_frame_ineligible_missing_model_metadata,
        .graph_executor_plan_build_ms = nsToMillis(timing.profile.graph_executor_plan_build_ns),
        .graph_executor_buffer_plan_build_ms = nsToMillis(timing.profile.graph_executor_buffer_plan_build_ns),
        .graph_executor_plan_cache_hits = timing.profile.graph_executor_plan_cache_hits,
        .graph_executor_plan_cache_misses = timing.profile.graph_executor_plan_cache_misses,
        .metal_frame_wait_ms = nsToMillis(timing.profile.metal_frame_wait_ns),
        .metal_frame_gpu_ms = nsToMillis(timing.profile.metal_frame_gpu_ns),
        .metal_tensor_device_owned_live_bytes = timing.profile.metal_tensor_device_owned_live_bytes,
        .metal_tensor_device_owned_peak_live_bytes = timing.profile.metal_tensor_device_owned_peak_live_bytes,
        .metal_tensor_device_owned_peak_lt_256kb_bytes = timing.profile.metal_tensor_device_owned_peak_lt_256kb_bytes,
        .metal_tensor_device_owned_peak_256kb_1mb_bytes = timing.profile.metal_tensor_device_owned_peak_256kb_1mb_bytes,
        .metal_tensor_device_owned_peak_1mb_4mb_bytes = timing.profile.metal_tensor_device_owned_peak_1mb_4mb_bytes,
        .metal_tensor_device_owned_peak_4mb_16mb_bytes = timing.profile.metal_tensor_device_owned_peak_4mb_16mb_bytes,
        .metal_tensor_device_owned_peak_ge_16mb_bytes = timing.profile.metal_tensor_device_owned_peak_ge_16mb_bytes,
        .metal_runtime_total_bytes = timing.profile.metal_runtime_total_bytes,
        .metal_runtime_scratch_bytes = timing.profile.metal_runtime_scratch_bytes,
        .metal_runtime_scratch_pool_bytes = timing.profile.metal_runtime_scratch_pool_bytes,
        .metal_runtime_graph_plan_bytes = timing.profile.metal_runtime_graph_plan_bytes,
        .metal_runtime_dense_linear_bytes = timing.profile.metal_runtime_dense_linear_bytes,
        .metal_runtime_attention_span_bytes = timing.profile.metal_runtime_attention_span_bytes,
        .metal_runtime_hidden_state_bytes = timing.profile.metal_runtime_hidden_state_bytes,
        .metal_runtime_frame_retained_bytes = timing.profile.metal_runtime_frame_retained_bytes,
        .metal_runtime_reuse_pool_bytes = timing.profile.metal_runtime_reuse_pool_bytes,
        .metal_runtime_reuse_pool_slots = timing.profile.metal_runtime_reuse_pool_slots,
        .metal_runtime_reuse_pool_peak_slots = timing.profile.metal_runtime_reuse_pool_peak_slots,
        .metal_runtime_reuse_alloc_delta = timing.profile.metal_runtime_reuse_alloc_delta,
        .metal_runtime_reuse_hit_delta = timing.profile.metal_runtime_reuse_hit_delta,
        .metal_runtime_reuse_hit_rate = ratio(timing.profile.metal_runtime_reuse_hit_delta, timing.profile.metal_runtime_reuse_alloc_delta),
        .metal_last_frame_compute_encoders = timing.profile.metal_last_frame_compute_encoders,
        .metal_last_frame_blit_encoders = timing.profile.metal_last_frame_blit_encoders,
        .metal_last_frame_planned_scopes = timing.profile.metal_last_frame_planned_scopes,
        .metal_last_frame_planned_barriers = timing.profile.metal_last_frame_planned_barriers,
        .metal_last_frame_planned_command_ops = timing.profile.metal_last_frame_planned_command_ops,
        .metal_deberta_encoder_plan_attempts = timing.profile.metal_deberta_encoder_plan_attempts,
        .metal_deberta_encoder_plan_successes = timing.profile.metal_deberta_encoder_plan_successes,
        .metal_deberta_encoder_plan_reuses = timing.profile.metal_deberta_encoder_plan_reuses,
        .metal_deberta_encoder_plan_failures = timing.profile.metal_deberta_encoder_plan_failures,
        .metal_deberta_encoder_layer_attempts = timing.profile.metal_deberta_encoder_layer_attempts,
        .metal_deberta_encoder_layer_successes = timing.profile.metal_deberta_encoder_layer_successes,
        .metal_deberta_encoder_layer_fallbacks = timing.profile.metal_deberta_encoder_layer_fallbacks,
        .metal_deberta_relative_qk_pair_calls = timing.profile.metal_deberta_relative_qk_pair_calls,
        .metal_deberta_relative_qk_pair_fallbacks = timing.profile.metal_deberta_relative_qk_pair_fallbacks,
        .metal_deberta_ffn_fused_calls = timing.profile.metal_deberta_ffn_fused_calls,
        .metal_deberta_ffn_fused_mps_matmuls = timing.profile.metal_deberta_ffn_fused_mps_matmuls,
        .metal_deberta_ffn_fused_fallbacks = timing.profile.metal_deberta_ffn_fused_fallbacks,
        .metal_deberta_attention_flash_calls = timing.profile.metal_deberta_attention_flash_calls,
        .metal_deberta_attention_gemm_calls = timing.profile.metal_deberta_attention_gemm_calls,
        .metal_deberta_attention_gemm_fallbacks = timing.profile.metal_deberta_attention_gemm_fallbacks,
        .metal_deberta_attention_legacy_calls = timing.profile.metal_deberta_attention_legacy_calls,
        .trainer_total_ms = nsToMillis(timing.profile.total_ns),
        .peak_resident_bytes = timing.profile.peak_resident_bytes,
        .supervised_tokens_per_second = timing.supervisedTokensPerSecond(target_stats),
    }, .{ .emit_null_optional_fields = false }, writer);
    try writer.writeByte('\n');
}

fn writeOptimizerStepMetric(
    writer: *std.Io.Writer,
    epoch: usize,
    global_step: u64,
    flush: real_autodiff.FlushResult,
    optimizer_update_ns: u64,
) !void {
    try std.json.Stringify.value(.{
        .event = "optimizer_step",
        .epoch = epoch,
        .step = global_step,
        .optimizer_step = flush.optimizer_step,
        .partial_accumulation = true,
        .micro_batches = flush.micro_batches,
        .grad_norm = flush.grad_norm,
        .learning_rate = flush.learning_rate,
        .optimizer_update_ms = nsToMillis(optimizer_update_ns),
    }, .{}, writer);
    try writer.writeByte('\n');
}

fn writeEpochMetric(
    writer: *std.Io.Writer,
    epoch: usize,
    avg_loss: f64,
    gold_entities: u64,
    steps: u64,
    objective: gliner2_autodiff.GlinerObjective,
    target_stats: BatchTargetStats,
    timing: EpochTiming,
) !void {
    try std.json.Stringify.value(.{
        .event = "epoch",
        .epoch = epoch,
        .avg_loss = avg_loss,
        .gold_entities = gold_entities,
        .steps = steps,
        .supervised_token_count = target_stats.supervised_token_count,
        .entity_token_count = target_stats.entity_token_count,
        .ignored_token_count = target_stats.ignored_token_count,
        .entity_token_rate = target_stats.entityTokenRate(),
        .entity_label_positive_counts = if (objective == .gliner2_total_loss) null else target_stats.positiveCounts(),
        .schema_slot_positive_counts = if (objective == .gliner2_total_loss) target_stats.positiveCounts() else null,
        .epoch_wall_ms = nsToMillis(timing.epoch_wall_ns),
        .supervised_tokens_per_second = tokensPerSecond(target_stats.supervised_token_count, timing.epoch_wall_ns),
    }, .{ .emit_null_optional_fields = false }, writer);
    try writer.writeByte('\n');
}

const EvalSelectionState = struct {
    best_loss: ?f64 = null,
    best_epoch: u64 = 0,
    bad_epochs: u32 = 0,
    eval_count: u64 = 0,

    fn observe(self: *EvalSelectionState, epoch: u64, loss: f64, threshold: f64) !bool {
        if (!std.math.isFinite(loss) or !std.math.isFinite(threshold) or threshold < 0.0) return error.NonFiniteEvalLoss;
        const previous_best = self.best_loss;
        const is_best = previous_best == null or loss < previous_best.?;
        const early_stopping_improved = previous_best == null or loss < previous_best.? - threshold;
        if (is_best) {
            self.best_loss = loss;
            self.best_epoch = epoch;
        }
        if (early_stopping_improved) {
            self.bad_epochs = 0;
        } else {
            self.bad_epochs = std.math.add(u32, self.bad_epochs, 1) catch std.math.maxInt(u32);
        }
        self.eval_count += 1;
        return is_best;
    }

    fn shouldStop(self: EvalSelectionState, patience: u32) bool {
        return patience > 0 and self.bad_epochs >= patience;
    }
};

fn resumeStopsBeforeTraining(resumed: bool, state: EvalSelectionState, patience: u32) bool {
    return resumed and state.shouldStop(patience);
}

fn writeEvalMetric(
    writer: *std.Io.Writer,
    epoch: usize,
    eval_loss: f64,
    examples: usize,
    is_best: bool,
    state: EvalSelectionState,
) !void {
    try std.json.Stringify.value(.{
        .event = "eval",
        .epoch = epoch,
        .eval_loss = eval_loss,
        .examples = examples,
        .is_best = is_best,
        .best_eval_loss = state.best_loss,
        .best_epoch = state.best_epoch,
        .early_stopping_bad_epochs = state.bad_epochs,
    }, .{ .emit_null_optional_fields = false }, writer);
    try writer.writeByte('\n');
}

const ResumeProvenance = struct {
    checkpoint_path: ?[]const u8 = null,
    checkpoint_sha256: ?[]const u8 = null,
    training_state_fingerprint_sha256: ?[]const u8 = null,
    restored_micro_batch_steps: ?u64 = null,
    restored_optimizer_steps: ?u64 = null,
    restored_epochs: ?u64 = null,
};

fn writeTrainingManifest(
    allocator: std.mem.Allocator,
    opts: Options,
    backend_label: []const u8,
    train_data_sha256: ?[]const u8,
    eval_data_sha256: ?[]const u8,
    initial_adapter_checkpoint_sha256: ?[]const u8,
    base_model_fingerprint_sha256: []const u8,
    adapter_bundle_fingerprint_sha256: []const u8,
    hidden_size: u32,
    num_classes: u32,
    entity_labels: []const []const u8,
    span_label_positive_weights: []const f32,
    example_count: usize,
    completed_epochs: u64,
    steps_per_epoch: usize,
    total_steps: u64,
    optimizer_steps: u64,
    resolved_warmup_steps: u32,
    final_avg_loss: f64,
    adapter_parameter_file_count: usize,
    peft_adapter_tensor_count: usize,
    regular_trainable_tensor_count: usize,
    regular_trainable_params: []const []const u8,
    resolved_lora_targets: []const []const u8,
    total_loss_entity_label_positive_counts: ?[]const u64,
    target_stats: BatchTargetStats,
    eval_state: EvalSelectionState,
    early_stopped: bool,
    best_checkpoint_relative_path: ?[]const u8,
    best_checkpoint_sha256: ?[]const u8,
    resume_provenance: ResumeProvenance,
    graph_cache_stats: real_autodiff.RealAutodiffTrainer.GraphCacheStats,
) !void {
    if (opts.resume_checkpoint != null and
        (resume_provenance.checkpoint_path == null or
            resume_provenance.checkpoint_sha256 == null or
            resume_provenance.training_state_fingerprint_sha256 == null or
            resume_provenance.restored_micro_batch_steps == null or
            resume_provenance.restored_optimizer_steps == null or
            resume_provenance.restored_epochs == null))
    {
        return error.MissingResumeProvenance;
    }
    if (opts.resume_checkpoint != null) {
        const expected_resume_path = try retainedResumeCheckpointRelativePathAlloc(
            allocator,
            resume_provenance.checkpoint_sha256.?,
        );
        defer allocator.free(expected_resume_path);
        if (!std.mem.eql(u8, resume_provenance.checkpoint_path.?, expected_resume_path)) {
            return error.InvalidRetainedResumeCheckpointPath;
        }
    }
    if ((eval_state.best_loss != null) !=
        (best_checkpoint_relative_path != null and best_checkpoint_sha256 != null))
    {
        return error.InconsistentBestCheckpointProvenance;
    }
    const manifest_path = try std.fs.path.join(allocator, &.{ opts.out_dir, run_validation.manifest_file_name });
    defer allocator.free(manifest_path);

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .schema_version = run_validation.expected_manifest_schema_version,
        .artifact_family_version = run_validation.expected_artifact_family_version,
        .model_dir = opts.model_dir,
        .backend = backend_label,
        .compiled_required = opts.compiled_required,
        .graph_shape_policy = if (opts.objective == .gliner2_total_loss) "batch-local-v1" else "fixed",
        .graph_seq_bucket_multiple = if (opts.objective == .gliner2_total_loss) @as(?u32, 8) else null,
        .graph_schema_bucket_policy = if (opts.objective == .gliner2_total_loss) "bounded-next-power-of-two" else null,
        .graph_cache_capacity = graph_cache_stats.capacity,
        .graph_cache_build_reserve_bytes = graph_cache_stats.build_reserve_bytes,
        .graph_cache_builds = graph_cache_stats.builds,
        .graph_cache_hits = graph_cache_stats.hits,
        .graph_cache_active_reuses = graph_cache_stats.active_reuses,
        .graph_cache_evictions = graph_cache_stats.evictions,
        .graph_cache_resident_signatures = graph_cache_stats.resident_signatures,
        .graph_cache_peak_resident_signatures = graph_cache_stats.peak_resident_signatures,
        .graph_cache_runtime_input_policy = "active-entry-only",
        .train_data = opts.train_data,
        .train_data_sha256 = train_data_sha256,
        .eval_data = opts.eval_data,
        .eval_data_sha256 = eval_data_sha256,
        .eval_every_epochs = if (opts.eval_data != null) opts.eval_every_epochs else null,
        .eval_batch_size = if (opts.eval_data != null) opts.eval_batch_size else null,
        .early_stopping_patience = opts.early_stopping_patience,
        .early_stopping_threshold = opts.early_stopping_threshold,
        .early_stopped = early_stopped,
        .eval_count = eval_state.eval_count,
        .best_eval_loss = eval_state.best_loss,
        .best_epoch = if (eval_state.best_loss != null) eval_state.best_epoch else null,
        .early_stopping_bad_epochs = eval_state.bad_epochs,
        .selected_checkpoint = best_checkpoint_relative_path orelse "final",
        .best_checkpoint_sha256 = best_checkpoint_sha256,
        .initial_adapter_checkpoint_used = opts.initial_adapter_checkpoint != null,
        .initial_adapter_checkpoint = opts.initial_adapter_checkpoint,
        .initial_adapter_checkpoint_sha256 = initial_adapter_checkpoint_sha256,
        .resume_checkpoint_used = opts.resume_checkpoint != null,
        .resume_checkpoint = resume_provenance.checkpoint_path,
        .resume_checkpoint_sha256 = resume_provenance.checkpoint_sha256,
        .training_state_fingerprint_sha256 = resume_provenance.training_state_fingerprint_sha256,
        .resume_restored_micro_batch_steps = resume_provenance.restored_micro_batch_steps,
        .resume_restored_optimizer_steps = resume_provenance.restored_optimizer_steps,
        .resume_restored_epochs = resume_provenance.restored_epochs,
        .base_model_fingerprint_sha256 = base_model_fingerprint_sha256,
        .adapter_bundle_fingerprint_sha256 = adapter_bundle_fingerprint_sha256,
        .out_dir = opts.out_dir,
        .metrics_file = run_validation.metrics_file_name,
        .adapter_parameter_format = "real_autodiff_bin/v1",
        .adapter_parameter_file_count = adapter_parameter_file_count,
        .peft_adapter_checkpoint = gliner2_bundle.adapter_checkpoint_file_name,
        .peft_adapter_config = gliner2_bundle.adapter_config_file_name,
        .peft_adapter_tensor_count = peft_adapter_tensor_count,
        .regular_trainable_checkpoint = gliner2_bundle.task_head_checkpoint_file_name,
        .regular_trainable_checkpoint_role = if (opts.objective == .gliner2_total_loss)
            "inference_graph_compatibility_only"
        else
            "trained_task_head",
        .regular_trainable_tensor_count = regular_trainable_tensor_count,
        .regular_trainable_params = regular_trainable_params,
        .lora_only_trainables = opts.objective == .gliner2_total_loss or opts.lora_only_trainables,
        .deterministic = opts.deterministic,
        .model_dropout = "disabled",
        .requested_epochs = opts.epochs,
        .epochs = completed_epochs,
        .max_steps = opts.max_steps,
        .requested_max_steps = opts.max_steps,
        .effective_steps = optimizer_steps,
        .optimizer_steps = optimizer_steps,
        .steps_per_epoch = steps_per_epoch,
        .cycled_epochs = if (completed_epochs > 0) completed_epochs - 1 else 0,
        .batch_size = opts.batch_size,
        .effective_batch_size = @min(example_count, @as(usize, opts.batch_size)),
        .drop_last = example_count > @as(usize, opts.batch_size),
        .examples_per_epoch = if (example_count > @as(usize, opts.batch_size))
            (example_count / @as(usize, opts.batch_size)) * @as(usize, opts.batch_size)
        else
            example_count,
        .partial_accumulation_divisor = "configured_steps_upstream_compatible",
        .seq_len = opts.seq_len,
        .learning_rate = opts.learning_rate,
        .lr_scheduler = @tagName(opts.lr_scheduler),
        .warmup_ratio = opts.warmup_ratio,
        .warmup_steps = resolved_warmup_steps,
        .num_cycles = opts.num_cycles,
        .lora_rank = opts.lora_rank,
        .lora_alpha = opts.lora_alpha,
        .lora_dropout = opts.lora_dropout,
        .lora_targets = opts.lora_targets,
        .resolved_lora_targets = resolved_lora_targets,
        .num_classes = num_classes,
        .schema_slot_count = if (opts.objective == .gliner2_total_loss) num_classes - 1 else null,
        .objective = objectiveName(opts.objective),
        .max_span_width = opts.max_span_width,
        .span_loss = spanLossName(opts.span_loss),
        .span_loss_reduction = spanLossReductionName(opts.span_loss_reduction),
        .span_positive_weight = opts.span_positive_weight,
        .use_span_label_positive_weights = opts.span_label_positive_weights != null,
        .span_label_positive_weights = span_label_positive_weights,
        .span_negative_weight = opts.span_negative_weight,
        .span_hard_negative_weight = opts.span_hard_negative_weight,
        .hidden_size = hidden_size,
        .entity_labels = entity_labels,
        .entity_label_count = entity_labels.len,
        .entity_label_positive_counts = total_loss_entity_label_positive_counts orelse target_stats.positiveCounts(),
        .schema_slot_positive_counts = if (opts.objective == .gliner2_total_loss) target_stats.positiveCounts() else null,
        .supervised_token_count = target_stats.supervised_token_count,
        .entity_token_count = target_stats.entity_token_count,
        .ignored_token_count = target_stats.ignored_token_count,
        .entity_token_rate = target_stats.entityTokenRate(),
        .max_examples = opts.max_examples,
        .max_grad_norm = opts.max_grad_norm,
        .grad_accum = opts.grad_accum,
        .seed = opts.seed,
        .example_count = example_count,
        .micro_batch_steps = total_steps,
        .total_steps = total_steps,
        .final_avg_loss = final_avg_loss,
    }, .{ .whitespace = .indent_2, .emit_null_optional_fields = false }, &buffer.writer);
    try buffer.writer.writeByte('\n');
    try writeFileAtomic(allocator, manifest_path, buffer.written());
}

fn sha256RegularFileAlloc(allocator: std.mem.Allocator, path: []const u8) !?[]const u8 {
    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind != .file) return null;

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try updateSha256FromFile(&hasher, path);
    return try sha256HexAlloc(allocator, &hasher);
}

fn initialAdapterCheckpointSha256Alloc(allocator: std.mem.Allocator, path: ?[]const u8) !?[]const u8 {
    const checkpoint_path = path orelse return null;
    return (try sha256RegularFileAlloc(allocator, checkpoint_path)) orelse error.InitialAdapterCheckpointNotRegular;
}

fn resumeCheckpointSha256Alloc(allocator: std.mem.Allocator, path: ?[]const u8) !?[]const u8 {
    const checkpoint_path = path orelse return null;
    return (try sha256RegularFileAlloc(allocator, checkpoint_path)) orelse error.ResumeCheckpointNotRegular;
}

fn selfExecutableSha256Alloc(allocator: std.mem.Allocator) ![]const u8 {
    const path = try std.process.executablePathAlloc(compat.io(), allocator);
    defer allocator.free(path);
    return (try sha256RegularFileAlloc(allocator, path)) orelse error.TrainingExecutableNotRegular;
}

fn runtimeMathIdentityAlloc(allocator: std.mem.Allocator, backend: Gliner2TrainBackend) ![]const u8 {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) {
        return std.fmt.allocPrint(allocator, "{s}|{s}|{s}", .{
            backendLabel(backend),
            @tagName(builtin.cpu.arch),
            @tagName(builtin.os.tag),
        });
    }
    const uts = std.posix.uname();
    return std.fmt.allocPrint(allocator, "{s}|{s}|{s}|{s}|{s}|{s}", .{
        backendLabel(backend),
        std.mem.sliceTo(&uts.sysname, 0),
        std.mem.sliceTo(&uts.nodename, 0),
        std.mem.sliceTo(&uts.release, 0),
        std.mem.sliceTo(&uts.version, 0),
        std.mem.sliceTo(&uts.machine, 0),
    });
}

fn gliner2BaseModelFingerprintAlloc(allocator: std.mem.Allocator, model_dir: []const u8) ![]const u8 {
    const relative_paths = [_][]const u8{
        "model.safetensors",
        "config.json",
        "encoder_config/config.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "special_tokens_map.json",
        "added_tokens.json",
        "spm.model",
    };
    return fingerprintRelativeFilesAlloc(allocator, model_dir, &relative_paths);
}

fn gliner2AdapterBundleFingerprintAlloc(allocator: std.mem.Allocator, adapter_dir: []const u8) ![]const u8 {
    const relative_paths = [_][]const u8{
        gliner2_bundle.adapter_checkpoint_file_name,
        gliner2_bundle.adapter_config_file_name,
        gliner2_bundle.task_head_checkpoint_file_name,
    };
    return fingerprintRelativeFilesAlloc(allocator, adapter_dir, &relative_paths);
}

fn fingerprintRelativeFilesAlloc(allocator: std.mem.Allocator, dir: []const u8, relative_paths: []const []const u8) ![]const u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for (relative_paths) |relative_path| {
        const path = try std.fs.path.join(allocator, &.{ dir, relative_path });
        defer allocator.free(path);
        const stat = compat.cwd().statFile(compat.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.RequiredFingerprintFileMissing,
            else => return err,
        };
        if (stat.kind != .file) return error.RequiredFingerprintFileNotRegular;
        hasher.update(relative_path);
        hasher.update(&.{0});
        try updateSha256FromFile(&hasher, path);
        hasher.update(&.{0});
    }
    return sha256HexAlloc(allocator, &hasher);
}

fn updateSha256FromFile(hasher: *std.crypto.hash.sha2.Sha256, path: []const u8) !void {
    const io = compat.io();
    var file = try compat.cwd().openFile(io, path, .{});
    defer file.close(io);

    var buffer: [64 * 1024]u8 = undefined;
    while (true) {
        const n = file.readStreaming(io, &.{buffer[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        hasher.update(buffer[0..n]);
    }
}

fn sha256FilePrefixDigest(path: []const u8, prefix_len: u64) ![std.crypto.hash.sha2.Sha256.digest_length]u8 {
    const io = compat.io();
    var file = try compat.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (prefix_len > stat.size) return error.TrainingMetricsCheckpointMismatch;

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var remaining = prefix_len;
    var buffer: [64 * 1024]u8 = undefined;
    while (remaining > 0) {
        const limit: usize = @intCast(@min(remaining, buffer.len));
        const n = file.readStreaming(io, &.{buffer[0..limit]}) catch |err| switch (err) {
            error.EndOfStream => return error.TrainingMetricsCheckpointMismatch,
            else => return err,
        };
        if (n == 0) return error.TrainingMetricsCheckpointMismatch;
        hasher.update(buffer[0..n]);
        remaining -= n;
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn sha256HexAlloc(allocator: std.mem.Allocator, hasher: *std.crypto.hash.sha2.Sha256) ![]const u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digestHexAlloc(allocator, &digest);
}

fn digestHexAlloc(allocator: std.mem.Allocator, digest: *const [std.crypto.hash.sha2.Sha256.digest_length]u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "sha256:{s}", .{std.fmt.bytesToHex(digest.*, .lower)});
}

fn optimizerStepsForMicroBatches(micro_batches: u64, grad_accum: u64) u64 {
    std.debug.assert(micro_batches > 0);
    std.debug.assert(grad_accum > 0);
    return micro_batches / grad_accum + @intFromBool(micro_batches % grad_accum != 0);
}

/// Upstream GLiNER2's LR-schedule steps per epoch: floor division with a
/// >= 1 clamp (trainer.py `len(train_loader) // gradient_accumulation_steps`
/// then "Fix Bug #1"). Distinct from `optimizerStepsForMicroBatches`, which
/// counts *executed* steps including the end-of-epoch partial flush.
fn scheduleStepsForMicroBatches(micro_batches: u64, grad_accum: u64) u64 {
    std.debug.assert(micro_batches > 0);
    std.debug.assert(grad_accum > 0);
    return @max(micro_batches / grad_accum, 1);
}

fn trainingStateFingerprint(
    allocator: std.mem.Allocator,
    opts: Options,
    backend: []const u8,
    train_data_sha256: []const u8,
    eval_data_sha256: ?[]const u8,
    base_model_fingerprint_sha256: []const u8,
    training_executable_sha256: []const u8,
    training_runtime_identity: []const u8,
    resolved_target_patterns: []const []const u8,
    entity_types: []const []const u8,
    manifest_entity_labels: []const []const u8,
    regular_trainable_params: []const []const u8,
    span_label_positive_weights: []const f32,
    effective_seq_len: usize,
    effective_batch_size: usize,
    effective_num_classes: u32,
    structure_max_instances: u32,
    max_schema_tasks: u32,
    examples_per_epoch: usize,
    target_optimizer_steps: u64,
    resolved_warmup_steps: u32,
    activation_checkpoint_config: ?ml.graph.checkpoint.CheckpointConfig,
) ![32]u8 {
    var encoded: std.Io.Writer.Allocating = .init(allocator);
    defer encoded.deinit();
    try std.json.Stringify.value(.{
        .schema_version = "antfly-gliner2-training-state-v2",
        .train_data_sha256 = train_data_sha256,
        .eval_data_sha256 = eval_data_sha256,
        .base_model_fingerprint_sha256 = base_model_fingerprint_sha256,
        .training_executable_sha256 = training_executable_sha256,
        .training_runtime_identity = training_runtime_identity,
        .inference_version = build_options.inference_version,
        .build_enable_system_blas = build_options.enable_system_blas,
        .build_enable_metal = build_options.enable_metal,
        .build_enable_cuda = build_options.enable_cuda,
        .build_enable_pjrt = build_options.enable_pjrt,
        .build_enable_onnx = build_options.enable_onnx,
        .build_target_arch = @tagName(builtin.cpu.arch),
        .build_target_os = @tagName(builtin.os.tag),
        .build_optimize_mode = @tagName(builtin.mode),
        .backend = backend,
        .objective = objectiveName(opts.objective),
        .epochs = opts.epochs,
        .max_steps = opts.max_steps,
        .max_examples = opts.max_examples,
        .batch_size = opts.batch_size,
        .effective_batch_size = effective_batch_size,
        .seq_len = opts.seq_len,
        .effective_seq_len = effective_seq_len,
        .max_span_width = opts.max_span_width,
        .effective_num_classes = effective_num_classes,
        .structure_max_instances = structure_max_instances,
        .max_schema_tasks = max_schema_tasks,
        .examples_per_epoch = examples_per_epoch,
        .learning_rate = opts.learning_rate,
        .lr_scheduler = @tagName(opts.lr_scheduler),
        .warmup_ratio = opts.warmup_ratio,
        .warmup_steps = resolved_warmup_steps,
        .num_cycles = opts.num_cycles,
        .target_optimizer_steps = target_optimizer_steps,
        .weight_decay = opts.weight_decay,
        .max_grad_norm = opts.max_grad_norm,
        .grad_accum = opts.grad_accum,
        .lora_rank = opts.lora_rank,
        .lora_alpha = opts.lora_alpha,
        .lora_dropout = opts.lora_dropout,
        .resolved_target_patterns = resolved_target_patterns,
        .entity_types = entity_types,
        .manifest_entity_labels = manifest_entity_labels,
        .regular_trainable_params = regular_trainable_params,
        .span_loss = spanLossName(opts.span_loss),
        .span_loss_reduction = spanLossReductionName(opts.span_loss_reduction),
        .span_positive_weight = opts.span_positive_weight,
        .use_span_label_positive_weights = opts.span_label_positive_weights != null,
        .span_label_positive_weights = span_label_positive_weights,
        .span_negative_weight = opts.span_negative_weight,
        .span_hard_negative_weight = opts.span_hard_negative_weight,
        .span_negative_mask_rate = opts.span_negative_mask_rate,
        .seed = opts.seed,
        .lora_only_trainables = opts.lora_only_trainables,
        .deterministic = opts.deterministic,
        // This diagnostic suppresses the total-loss epoch shuffle and is
        // therefore part of the training semantics, not just log output.
        .dump_span_parity = opts.dump_span_parity,
        .compiled_required = opts.compiled_required,
        .graph_shape_policy = if (opts.objective == .gliner2_total_loss) "batch-local-v1" else "fixed",
        .graph_seq_bucket_multiple = if (opts.objective == .gliner2_total_loss) @as(u32, 8) else 0,
        .graph_schema_bucket_policy = if (opts.objective == .gliner2_total_loss) "bounded-next-power-of-two" else "fixed",
        .graph_cache_capacity = opts.graph_cache_capacity,
        .graph_cache_runtime_input_policy = "active-entry-only",
        .activation_checkpointing = activation_checkpoint_config != null,
        .activation_checkpoint_interval = if (activation_checkpoint_config) |config| config.layer_interval else 0,
        .activation_checkpoint_strategy = if (activation_checkpoint_config) |config| checkpointStrategyName(config.strategy) else "disabled",
        .structure_span_chunk_samples = opts.structure_span_chunk_samples,
        .eval_every_epochs = if (opts.eval_data != null) opts.eval_every_epochs else 0,
        .eval_batch_size = if (opts.eval_data != null) opts.eval_batch_size else 0,
        .early_stopping_patience = opts.early_stopping_patience,
        .early_stopping_threshold = opts.early_stopping_threshold,
    }, .{}, &encoded.writer);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(encoded.written());
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn writeFileAtomic(allocator: std.mem.Allocator, path: []const u8, data: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);
    compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    errdefer compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tmp_path, .data = data });
    try std.Io.Dir.rename(compat.cwd(), tmp_path, compat.cwd(), path, compat.io());
}

/// Emit one `GLINER2_OPT_PARITY` JSON line per optimizer step with, for every
/// host-resident LoRA parameter: the post-update weights, Adam m/v state and
/// per-parameter step counter (first 8 elements + f64 abs-sum per tensor).
/// Consumed by scripts/compare_gliner2_lora_python_zig.py --dump-optimizer-parity.
fn printOptimizerParityDump(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    step: u64,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    const w = &buffer.writer;
    try w.print("GLINER2_OPT_PARITY {{\"step\":{d},\"tensors\":[", .{step});
    var first = true;
    for (trainer.lora_params.items) |slot| {
        if (slot.device != null) continue;
        const peft_name = gliner2_bundle.autodiffParamNameToPeftName(allocator, slot.name) catch continue;
        defer allocator.free(peft_name);
        if (!first) try w.writeAll(",");
        first = false;
        const state = trainer.optimizer_state.param_states.getPtr(slot.name);
        const step_count: u32 = if (state) |s| s.step_count else 0;
        try w.print("{{\"name\":\"{s}\",\"step_count\":{d}", .{ peft_name, step_count });
        try writeOptParityTensor(w, "weight", slot.weights);
        try writeOptParityTensor(w, "m", if (state) |s| s.m else &.{});
        try writeOptParityTensor(w, "v", if (state) |s| s.v else &.{});
        try w.writeAll("}");
    }
    try w.writeAll("]}\n");
    print("{s}", .{buffer.written()});
}

fn writeOptParityTensor(w: *std.Io.Writer, name: []const u8, values: []const f32) !void {
    try w.print(",\"{s}\":[", .{name});
    const head_len = @min(values.len, 8);
    for (values[0..head_len], 0..) |v, idx| {
        if (idx > 0) try w.writeAll(",");
        try w.print("{e}", .{v});
    }
    var abs_sum: f64 = 0.0;
    for (values) |v| abs_sum += @abs(@as(f64, v));
    try w.print("],\"{s}_abs_sum\":{e}", .{ name, abs_sum });
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

fn ensureTrainerGraphBuiltFromFirstBatch(
    allocator: std.mem.Allocator,
    opts: Options,
    effective_seq_len: usize,
    total_loss_graph_limits: TotalLossGraphLimits,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    examples: []const gliner2_data.Example,
    training_records: []const gliner2_data.UpstreamRecord,
    label_map: *const std.StringHashMapUnmanaged(u32),
    effective_num_classes: u32,
    use_label_positive_weights: bool,
    resolved_span_label_positive_weights: []const f32,
    input_ids: []i64,
    attention_mask: []f32,
    targets_buf: []f32,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !void {
    if (examples.len == 0) return error.NoTrainingExamples;
    const ab = @min(@as(usize, opts.batch_size), examples.len);

    var batch_examples = try allocator.alloc(gliner2_data.Example, ab);
    defer allocator.free(batch_examples);
    var batch_records = try allocator.alloc(gliner2_data.UpstreamRecord, ab);
    defer allocator.free(batch_records);
    for (0..ab) |slot| {
        batch_examples[slot] = examples[slot];
        if (opts.objective == .gliner2_total_loss) batch_records[slot] = training_records[slot];
    }

    const trainer_input = try prepareTrainerInputForBatch(
        allocator,
        opts,
        effective_seq_len,
        total_loss_graph_limits,
        tokenizer,
        entity_types,
        batch_examples,
        batch_records,
        label_map,
        effective_num_classes,
        use_label_positive_weights,
        resolved_span_label_positive_weights,
        input_ids,
        attention_mask,
        targets_buf,
        gliner_ctx,
    );
    try trainer.ensureGraphBuilt(trainer_input);
}

fn prepareTrainerInputForBatch(
    allocator: std.mem.Allocator,
    opts: Options,
    effective_seq_len: usize,
    total_loss_graph_limits: TotalLossGraphLimits,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    batch_examples: []const gliner2_data.Example,
    batch_records: []const gliner2_data.UpstreamRecord,
    label_map: *const std.StringHashMapUnmanaged(u32),
    effective_num_classes: u32,
    use_label_positive_weights: bool,
    resolved_span_label_positive_weights: []const f32,
    input_ids: []i64,
    attention_mask: []f32,
    targets_buf: []f32,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !real_autodiff.TrainerInput {
    const ab = batch_examples.len;
    if (ab == 0 or (opts.objective == .gliner2_total_loss and batch_records.len != ab)) return error.InvalidEvalBatch;
    const actual_batch: u32 = @intCast(ab);
    const total_loss_shape = if (opts.objective == .gliner2_total_loss)
        try totalLossBatchShape(allocator, tokenizer, batch_records, total_loss_graph_limits)
    else
        null;
    if (total_loss_shape) |shape| shape.apply(gliner_ctx);
    const sl = if (total_loss_shape) |shape| shape.seq_len else effective_seq_len;
    const nc: usize = if (total_loss_shape) |shape| shape.schema_slots + 1 else effective_num_classes;
    var targets_shape: ml.graph.Shape = undefined;
    var target_slice: []const f32 = undefined;
    switch (opts.objective) {
        .token => {
            _ = fillBatchBuffers(
                allocator,
                tokenizer,
                entity_types,
                batch_examples,
                @intCast(sl),
                effective_num_classes,
                label_map,
                input_ids,
                attention_mask,
                targets_buf[0 .. ab * sl * nc],
            );
            targets_shape = gliner2_autodiff.tokenTargetsShape(actual_batch, @intCast(sl), effective_num_classes);
            target_slice = targets_buf[0 .. ab * sl * nc];
        },
        .span_start, .gliner2_total_loss => {
            var encoded = if (opts.objective == .gliner2_total_loss)
                try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
                    allocator,
                    tokenizer,
                    batch_records,
                    total_loss_shape.?.schema_slots,
                    sl,
                    opts.max_span_width,
                    ab,
                )
            else
                try gliner2_data.buildSimpleBatch(
                    allocator,
                    tokenizer,
                    batch_examples,
                    entity_types,
                    sl,
                    opts.max_span_width,
                    ab,
                );
            defer encoded.deinit();

            if (encoded.input_ids.len != ab * sl or encoded.attention_mask.len != ab * sl) return error.InvalidGlinerBatchShape;
            for (0..ab * sl) |i| {
                input_ids[i] = encoded.input_ids[i];
                attention_mask[i] = @floatFromInt(encoded.attention_mask[i]);
            }

            const width = if (opts.objective == .gliner2_total_loss)
                gliner2_autodiff.gliner2TotalLossTargetWidthEx(encoded.num_entity_types, total_loss_shape.?.structure_instances)
            else if (use_label_positive_weights)
                gliner2_autodiff.weightedSpanStartTargetWidth(encoded.num_entity_types)
            else
                gliner2_autodiff.spanStartTargetWidth(encoded.num_entity_types);
            const target_len = encoded.batch_size * encoded.max_spans * width;
            _ = if (opts.objective == .gliner2_total_loss)
                try fillGliner2TotalLossTargetsFromRecords(
                    allocator,
                    &encoded,
                    batch_records,
                    total_loss_shape.?.structure_instances,
                    targets_buf[0..target_len],
                )
            else
                try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatchWithOptions(
                    &encoded,
                    .{
                        .positive_weights_by_entity_type = if (use_label_positive_weights) resolved_span_label_positive_weights else null,
                        .hard_negative_weight = opts.span_hard_negative_weight,
                    },
                    targets_buf[0..target_len],
                );
            targets_shape = if (opts.objective == .gliner2_total_loss)
                gliner2_autodiff.gliner2TotalLossTargetsShapeEx(
                    actual_batch,
                    @intCast(encoded.max_spans),
                    @intCast(encoded.num_entity_types),
                    total_loss_shape.?.structure_instances,
                )
            else if (use_label_positive_weights)
                gliner2_autodiff.weightedSpanStartTargetsShape(
                    actual_batch,
                    @intCast(encoded.max_spans),
                    @intCast(encoded.num_entity_types),
                )
            else
                gliner2_autodiff.spanStartTargetsShape(
                    actual_batch,
                    @intCast(encoded.max_spans),
                    @intCast(encoded.num_entity_types),
                );
            target_slice = targets_buf[0..target_len];
        },
    }

    return gliner2_autodiff.makeTrainerInput(
        gliner_ctx,
        input_ids[0 .. ab * sl],
        attention_mask[0 .. ab * sl],
        target_slice,
        targets_shape,
        actual_batch,
        @intCast(sl),
    );
}

fn evaluateDatasetLoss(
    allocator: std.mem.Allocator,
    opts: Options,
    effective_seq_len: usize,
    total_loss_graph_limits: TotalLossGraphLimits,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    eval_examples: []const gliner2_data.Example,
    eval_records: []const gliner2_data.UpstreamRecord,
    label_map: *const std.StringHashMapUnmanaged(u32),
    effective_num_classes: u32,
    use_label_positive_weights: bool,
    resolved_span_label_positive_weights: []const f32,
    input_ids: []i64,
    attention_mask: []f32,
    targets_buf: []f32,
    batch_examples: []gliner2_data.Example,
    batch_records: []gliner2_data.UpstreamRecord,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !f64 {
    if (eval_examples.len == 0 or batch_examples.len == 0) return error.NoEvalData;
    const bs = batch_examples.len;
    var loss_sum: f64 = 0.0;
    var logical_batches: u64 = 0;
    var logical_start: usize = 0;
    while (logical_start < eval_examples.len) {
        const logical_end = @min(eval_examples.len, logical_start + @as(usize, opts.eval_batch_size));
        const logical_len = logical_end - logical_start;
        var logical_example_loss_sum: f64 = 0.0;
        var start = logical_start;
        while (start + bs <= logical_end) : (start += bs) {
            for (0..bs) |slot| {
                batch_examples[slot] = eval_examples[start + slot];
                if (opts.objective == .gliner2_total_loss) batch_records[slot] = eval_records[start + slot];
            }
            const input = try prepareTrainerInputForBatch(
                allocator,
                opts,
                effective_seq_len,
                total_loss_graph_limits,
                tokenizer,
                entity_types,
                batch_examples,
                batch_records,
                label_map,
                effective_num_classes,
                use_label_positive_weights,
                resolved_span_label_positive_weights,
                input_ids,
                attention_mask,
                targets_buf,
                gliner_ctx,
            );
            const result = try trainer.evaluate(input);
            if (!std.math.isFinite(result.loss)) return error.NonFiniteEvalLoss;
            logical_example_loss_sum += physicalEvalLossContribution(opts.objective, result.loss, bs, false);
        }
        while (start < logical_end) : (start += 1) {
            for (0..bs) |slot| {
                batch_examples[slot] = eval_examples[start];
                if (opts.objective == .gliner2_total_loss) batch_records[slot] = eval_records[start];
            }
            const input = try prepareTrainerInputForBatch(
                allocator,
                opts,
                effective_seq_len,
                total_loss_graph_limits,
                tokenizer,
                entity_types,
                batch_examples,
                batch_records,
                label_map,
                effective_num_classes,
                use_label_positive_weights,
                resolved_span_label_positive_weights,
                input_ids,
                attention_mask,
                targets_buf,
                gliner_ctx,
            );
            const result = try trainer.evaluate(input);
            if (!std.math.isFinite(result.loss)) return error.NonFiniteEvalLoss;
            logical_example_loss_sum += physicalEvalLossContribution(opts.objective, result.loss, bs, true);
        }
        loss_sum += logicalEvalBatchLoss(opts.objective, logical_example_loss_sum, logical_len);
        logical_batches += 1;
        logical_start = logical_end;
    }
    return loss_sum / @as(f64, @floatFromInt(logical_batches));
}

fn physicalEvalLossContribution(
    objective: gliner2_autodiff.GlinerObjective,
    reported_loss: f32,
    physical_batch_size: usize,
    replicated_single: bool,
) f64 {
    const loss: f64 = reported_loss;
    if (objective == .gliner2_total_loss) {
        return if (replicated_single) loss / @as(f64, @floatFromInt(physical_batch_size)) else loss;
    }
    return if (replicated_single) loss else loss * @as(f64, @floatFromInt(physical_batch_size));
}

fn logicalEvalBatchLoss(
    objective: gliner2_autodiff.GlinerObjective,
    contribution_sum: f64,
    logical_batch_size: usize,
) f64 {
    return if (objective == .gliner2_total_loss)
        contribution_sum
    else
        contribution_sum / @as(f64, @floatFromInt(logical_batch_size));
}

fn loadPeftAdaptersIntoTrainer(
    allocator: std.mem.Allocator,
    adapter_checkpoint_path: []const u8,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer reader.deinit();
    for (trainer.lora_params.items) |*slot| {
        const standard_name = try gliner2_bundle.autodiffParamNameToPeftName(allocator, slot.name);
        defer allocator.free(standard_name);
        var tensor = reader.readTensor(standard_name) catch blk: {
            // Backward-compatible fallbacks for adapters emitted before Zig
            // wrote standard `base_model.model.*.lora_[AB].weight` keys.
            const legacy_official_name = try autodiffSlotNameToOfficialPeftName(allocator, slot.name);
            defer allocator.free(legacy_official_name);
            break :blk reader.readTensor(legacy_official_name) catch {
                const legacy_zig_name = try autodiffSlotNameToZigPeftName(allocator, slot.name);
                defer allocator.free(legacy_zig_name);
                break :blk try reader.readTensor(legacy_zig_name);
            };
        };
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

fn autodiffSlotNameToOfficialPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        const base = name[0 .. name.len - ".lora_A".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_A", false);
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        const base = name[0 .. name.len - ".lora_B".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_B", false);
    }
    return error.InvalidAutodiffAdapterName;
}

fn autodiffSlotNameToZigPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        const base = name[0 .. name.len - ".lora_A".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_A", true);
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        const base = name[0 .. name.len - ".lora_B".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_B", true);
    }
    return error.InvalidAutodiffAdapterName;
}

fn autodiffBaseToPeftName(allocator: std.mem.Allocator, base_no_weight: []const u8, adapter_name: []const u8, zig_weight_suffix: bool) ![]const u8 {
    const suffix = if (zig_weight_suffix) ".weight" else "";
    if (std.mem.startsWith(u8, base_no_weight, "encoder.layer.")) {
        return std.fmt.allocPrint(allocator, "encoder.{s}.{s}{s}", .{ base_no_weight, adapter_name, suffix });
    }
    return std.fmt.allocPrint(allocator, "{s}.{s}{s}", .{ base_no_weight, adapter_name, suffix });
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    return tensor_name;
}

const TaskHeadExportParams = struct {
    allocator: std.mem.Allocator,
    params: []gliner2_bundle.AutodiffAdapterParam,
    owned_dims: [][]i32,
    owned_weights: [][]f32,

    fn deinit(self: *TaskHeadExportParams) void {
        for (self.owned_dims) |dims| self.allocator.free(dims);
        for (self.owned_weights) |weights| self.allocator.free(weights);
        self.allocator.free(self.owned_dims);
        self.allocator.free(self.owned_weights);
        self.allocator.free(self.params);
        self.* = undefined;
    }
};

fn collectTaskHeadExportParams(
    allocator: std.mem.Allocator,
    trainer: *const real_autodiff.RealAutodiffTrainer,
    num_classes: u32,
    hidden_size: u32,
) !TaskHeadExportParams {
    const source_names = [_][]const u8{ "task_classifier.weight", "task_classifier.bias" };
    const export_names = [_][]const u8{ "classifier.weight", "classifier.bias" };
    var params = try allocator.alloc(gliner2_bundle.AutodiffAdapterParam, source_names.len);
    errdefer allocator.free(params);
    var owned_dims = try allocator.alloc([]i32, source_names.len);
    errdefer allocator.free(owned_dims);
    var owned_weights = try allocator.alloc([]f32, source_names.len);
    errdefer allocator.free(owned_weights);
    for (owned_dims) |*dims| dims.* = &.{};
    for (owned_weights) |*weights| weights.* = &.{};
    errdefer {
        for (owned_dims) |dims| allocator.free(dims);
        for (owned_weights) |weights| allocator.free(weights);
    }

    for (source_names, 0..) |name, idx| {
        if (findRegularParamSlot(trainer, name)) |slot| {
            owned_dims[idx] = try allocator.dupe(i32, slot.dims);
            owned_weights[idx] = try allocator.dupe(f32, slot.weights);
        } else {
            const ct = try trainer.compute_backend.getWeight(name);
            defer trainer.compute_backend.free(ct);
            owned_weights[idx] = try trainer.compute_backend.toFloat32(ct, allocator);
            owned_dims[idx] = try inferTaskHeadDims(allocator, name, owned_weights[idx].len, num_classes, hidden_size);
        }
        params[idx] = .{
            .name = export_names[idx],
            .dims = owned_dims[idx],
            .weights = owned_weights[idx],
        };
    }

    return .{
        .allocator = allocator,
        .params = params,
        .owned_dims = owned_dims,
        .owned_weights = owned_weights,
    };
}

fn findRegularParamSlot(
    trainer: *const real_autodiff.RealAutodiffTrainer,
    name: []const u8,
) ?*const real_autodiff.RealAutodiffTrainer.ParamSlot {
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, name)) return slot;
    }
    return null;
}

fn inferTaskHeadDims(
    allocator: std.mem.Allocator,
    name: []const u8,
    element_count: usize,
    num_classes: u32,
    hidden_size: u32,
) ![]i32 {
    if (std.mem.eql(u8, name, "task_classifier.bias")) {
        if (element_count != num_classes) return error.TaskHeadParameterShapeMismatch;
        const dims = try allocator.alloc(i32, 1);
        dims[0] = @intCast(num_classes);
        return dims;
    }
    if (std.mem.eql(u8, name, "task_classifier.weight")) {
        const expected = @as(usize, @intCast(num_classes)) * @as(usize, @intCast(hidden_size));
        if (element_count != expected) return error.TaskHeadParameterShapeMismatch;
        const dims = try allocator.alloc(i32, 2);
        dims[0] = @intCast(num_classes);
        dims[1] = @intCast(hidden_size);
        return dims;
    }
    return error.InvalidTaskHeadParameter;
}

fn deinitNativeWeightStore(allocator: std.mem.Allocator, weight_store: *native_compute.WeightStore) void {
    var it = weight_store.resident_weights.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit();
    }
    weight_store.resident_weights.deinit(allocator);
    weight_store.lazy_weights.deinit(allocator);
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
    layer_norm_eps: f32 = 1e-7,
};

fn debertaArchConfigFromJson(config: DebertaJsonConfig) deberta_arch.Config {
    return .{
        .vocab_size = config.vocab_size,
        .hidden_size = config.hidden_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .intermediate_size = config.intermediate_size,
        .max_position_embeddings = config.max_position_embeddings,
        .position_buckets = config.position_buckets,
        .layer_norm_eps = config.layer_norm_eps,
    };
}

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
    if (obj.get("layer_norm_eps")) |v| config.layer_norm_eps = jsonF32(v) orelse config.layer_norm_eps;

    return config;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn jsonF32(val: std.json.Value) ?f32 {
    return switch (val) {
        .float => |f| @floatCast(f),
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

// ---------------------------------------------------------------------------
// Batch buffer construction (real HF tokenizer + entity targets)
// ---------------------------------------------------------------------------

const BatchTargetStats = struct {
    supervised_token_count: u64 = 0,
    entity_token_count: u64 = 0,
    ignored_token_count: u64 = 0,
    entity_type_count: usize = 0,
    positive_counts_by_entity_type: [gliner2_autodiff.max_span_start_entity_types]u64 = [_]u64{0} ** gliner2_autodiff.max_span_start_entity_types,

    fn entityTokenRate(self: BatchTargetStats) f64 {
        if (self.supervised_token_count == 0) return 0.0;
        return @as(f64, @floatFromInt(self.entity_token_count)) /
            @as(f64, @floatFromInt(self.supervised_token_count));
    }

    fn positiveCounts(self: *const BatchTargetStats) []const u64 {
        return self.positive_counts_by_entity_type[0..self.entity_type_count];
    }

    fn add(self: *BatchTargetStats, other: BatchTargetStats) void {
        self.supervised_token_count += other.supervised_token_count;
        self.entity_token_count += other.entity_token_count;
        self.ignored_token_count += other.ignored_token_count;
        if (other.entity_type_count > self.entity_type_count) self.entity_type_count = other.entity_type_count;
        for (0..other.entity_type_count) |idx| {
            self.positive_counts_by_entity_type[idx] += other.positive_counts_by_entity_type[idx];
        }
    }

    fn fromPackedSpanTargets(
        targets: []const f32,
        row_width: usize,
        num_entity_types: usize,
        objective: gliner2_autodiff.GlinerObjective,
        max_instances: u32,
    ) !BatchTargetStats {
        if (num_entity_types > gliner2_autodiff.max_span_start_entity_types or
            row_width < 2 * num_entity_types or targets.len % row_width != 0)
        {
            return error.InvalidGlinerSpanTargetShape;
        }
        const instance_count: usize = if (objective == .gliner2_total_loss and max_instances > 1)
            @intCast(max_instances)
        else
            1;
        const labels_offset = if (instance_count > 1)
            gliner2_autodiff.gliner2TotalLossInstanceLabelsOffset(num_entity_types)
        else
            0;
        const masks_offset = if (instance_count > 1)
            gliner2_autodiff.gliner2TotalLossInstanceMasksOffset(num_entity_types, max_instances)
        else
            num_entity_types;
        const values_per_row = instance_count * num_entity_types;
        if (masks_offset + values_per_row > row_width) return error.InvalidGlinerSpanTargetShape;

        var out = BatchTargetStats{ .entity_type_count = num_entity_types };
        const rows = targets.len / row_width;
        for (0..rows) |row_idx| {
            const row = targets[row_idx * row_width ..][0..row_width];
            for (0..instance_count) |instance_idx| {
                for (0..num_entity_types) |entity_idx| {
                    const offset = instance_idx * num_entity_types + entity_idx;
                    if (row[masks_offset + offset] > 0.0) {
                        out.supervised_token_count += 1;
                        if (row[labels_offset + offset] > 0.0) {
                            out.entity_token_count += 1;
                            out.positive_counts_by_entity_type[entity_idx] += 1;
                        }
                    } else {
                        out.ignored_token_count += 1;
                    }
                }
            }
        }
        return out;
    }
};

const ResumeMetrics = struct {
    step_count: u64 = 0,
    epoch_count: u64 = 0,
    max_optimizer_step: u64 = 0,
    loss_sum: f64 = 0.0,
    target_stats: BatchTargetStats = .{},
    prefix_len: u64 = 0,
    eval_state: EvalSelectionState = .{},
    best_metrics_prefix_len: u64 = 0,
};

const max_resume_metric_line_bytes = 4 * 1024 * 1024;

const ResumeMetricsParser = struct {
    result: ResumeMetrics,
    current_epoch_steps: u64 = 0,
    expected_steps: u64,
    expected_optimizer_steps: u64,
    expected_epochs: u64,
    entity_type_count: usize,
    eval_every_epochs: u32,
    early_stopping_threshold: f64,
    saw_eval_for_epoch: bool = false,
    eval_was_best: bool = false,

    fn init(
        expected_steps: u64,
        expected_optimizer_steps: u64,
        expected_epochs: u64,
        entity_type_count: usize,
        eval_every_epochs: u32,
        early_stopping_threshold: f64,
    ) !ResumeMetricsParser {
        if (entity_type_count > gliner2_autodiff.max_span_start_entity_types) {
            return error.TooManyEntityTypes;
        }
        return .{
            .result = .{ .target_stats = .{ .entity_type_count = entity_type_count } },
            .expected_steps = expected_steps,
            .expected_optimizer_steps = expected_optimizer_steps,
            .expected_epochs = expected_epochs,
            .entity_type_count = entity_type_count,
            .eval_every_epochs = eval_every_epochs,
            .early_stopping_threshold = early_stopping_threshold,
        };
    }

    fn processLine(self: *ResumeMetricsParser, allocator: std.mem.Allocator, raw_line: []const u8, line_end: u64) !void {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) return;

        const parsed = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch
            return error.InvalidTrainingMetrics;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidTrainingMetrics;
        const object = parsed.value.object;
        const event_value = object.get("event") orelse return error.InvalidTrainingMetrics;
        if (event_value != .string) return error.InvalidTrainingMetrics;

        if (std.mem.eql(u8, event_value.string, "step")) {
            if (self.result.step_count >= self.expected_steps) return error.TrainingMetricsCheckpointMismatch;
            const step = jsonU64(object.get("step") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const epoch = jsonU64(object.get("epoch") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const epoch_step = jsonU64(object.get("epoch_step") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const optimizer_step = jsonU64(object.get("optimizer_step") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const encoded_loss = jsonF64(object.get("loss") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const loss: f32 = @floatCast(encoded_loss);
            if (step != self.result.step_count + 1 or
                epoch != self.result.epoch_count + 1 or
                epoch_step != self.current_epoch_steps + 1 or
                !std.math.isFinite(encoded_loss) or
                !std.math.isFinite(loss))
            {
                return error.TrainingMetricsCheckpointMismatch;
            }

            var step_stats = BatchTargetStats{ .entity_type_count = self.entity_type_count };
            step_stats.supervised_token_count = jsonU64(object.get("supervised_token_count") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            step_stats.entity_token_count = jsonU64(object.get("entity_token_count") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            step_stats.ignored_token_count = jsonU64(object.get("ignored_token_count") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const counts_value = object.get("schema_slot_positive_counts") orelse
                object.get("entity_label_positive_counts") orelse
                return error.InvalidTrainingMetrics;
            try loadResumePositiveCounts(&step_stats, counts_value, self.entity_type_count);

            self.result.step_count += 1;
            self.current_epoch_steps += 1;
            // The live run adds an f32 loss promoted to f64. Round the JSON
            // value back through f32 so resumed aggregate metrics are exact.
            self.result.loss_sum += @as(f64, loss);
            self.result.target_stats.add(step_stats);
            self.result.max_optimizer_step = @max(self.result.max_optimizer_step, optimizer_step);
        } else if (std.mem.eql(u8, event_value.string, "optimizer_step")) {
            const step = jsonU64(object.get("step") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const epoch = jsonU64(object.get("epoch") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const optimizer_step = jsonU64(object.get("optimizer_step") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            if (step != self.result.step_count or epoch != self.result.epoch_count + 1) {
                return error.TrainingMetricsCheckpointMismatch;
            }
            self.result.max_optimizer_step = @max(self.result.max_optimizer_step, optimizer_step);
        } else if (std.mem.eql(u8, event_value.string, "eval")) {
            const epoch = jsonU64(object.get("epoch") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const eval_loss = jsonF64(object.get("eval_loss") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const is_best_value = object.get("is_best") orelse return error.InvalidTrainingMetrics;
            if (is_best_value != .bool or
                epoch != self.result.epoch_count + 1 or
                self.saw_eval_for_epoch or
                self.eval_every_epochs == 0 or
                epoch % self.eval_every_epochs != 0)
            {
                return error.TrainingMetricsCheckpointMismatch;
            }
            const improved = try self.result.eval_state.observe(epoch, eval_loss, self.early_stopping_threshold);
            if (improved != is_best_value.bool) return error.TrainingMetricsCheckpointMismatch;
            self.saw_eval_for_epoch = true;
            self.eval_was_best = improved;
        } else if (std.mem.eql(u8, event_value.string, "epoch")) {
            const epoch = jsonU64(object.get("epoch") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const steps = jsonU64(object.get("steps") orelse return error.InvalidTrainingMetrics) orelse
                return error.InvalidTrainingMetrics;
            const expected_eval = self.eval_every_epochs > 0 and epoch % self.eval_every_epochs == 0;
            if (epoch != self.result.epoch_count + 1 or
                steps != self.current_epoch_steps or
                steps == 0 or
                self.saw_eval_for_epoch != expected_eval)
            {
                return error.TrainingMetricsCheckpointMismatch;
            }
            self.result.epoch_count += 1;
            self.current_epoch_steps = 0;
            self.saw_eval_for_epoch = false;
            self.result.prefix_len = line_end;
            if (self.eval_was_best) self.result.best_metrics_prefix_len = line_end;
            self.eval_was_best = false;
        } else {
            return error.InvalidTrainingMetrics;
        }
    }

    fn finish(self: *ResumeMetricsParser) !ResumeMetrics {
        if (self.result.step_count != self.expected_steps or
            self.result.epoch_count != self.expected_epochs or
            self.result.max_optimizer_step != self.expected_optimizer_steps or
            self.current_epoch_steps != 0)
        {
            return error.TrainingMetricsCheckpointMismatch;
        }
        return self.result;
    }
};

fn inspectResumeMetrics(
    allocator: std.mem.Allocator,
    metrics_path: []const u8,
    expected_steps: u64,
    expected_optimizer_steps: u64,
    expected_epochs: u64,
    entity_type_count: usize,
    eval_every_epochs: u32,
    early_stopping_threshold: f64,
) !ResumeMetrics {
    const io = compat.io();
    var file = try compat.cwd().openFile(io, metrics_path, .{});
    defer file.close(io);
    const reader_buffer = try allocator.alloc(u8, max_resume_metric_line_bytes);
    defer allocator.free(reader_buffer);
    var reader = file.readerStreaming(io, reader_buffer);
    var parser = try ResumeMetricsParser.init(expected_steps, expected_optimizer_steps, expected_epochs, entity_type_count, eval_every_epochs, early_stopping_threshold);
    var cursor: u64 = 0;
    while (parser.result.epoch_count < expected_epochs) {
        const raw_line = reader.interface.takeDelimiterInclusive('\n') catch |err| switch (err) {
            error.EndOfStream => return error.IncompleteTrainingMetricsLine,
            else => return err,
        };
        cursor += @intCast(raw_line.len);
        try parser.processLine(allocator, raw_line[0 .. raw_line.len - 1], cursor);
    }
    const result = try parser.finish();
    const metrics_len = try file.length(io);
    if (result.prefix_len > metrics_len) return error.TrainingMetricsCheckpointMismatch;
    return result;
}

fn truncateFileToPrefix(path: []const u8, prefix_len: u64) !void {
    const io = compat.io();
    var file = try compat.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    const metrics_len = try file.length(io);
    if (prefix_len > metrics_len) return error.TrainingMetricsCheckpointMismatch;
    if (prefix_len != metrics_len) {
        try file.setLength(io, prefix_len);
        try file.sync(io);
    }
}

fn syncDirectoryChain(path: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;
    var current = if (path.len == 0) "." else path;
    while (true) {
        var dir = if (std.fs.path.isAbsolute(current))
            try std.Io.Dir.openDirAbsolute(compat.io(), current, .{ .iterate = true })
        else
            try compat.cwd().openDir(compat.io(), current, .{ .iterate = true });
        defer dir.close(compat.io());
        while (true) switch (std.posix.errno(std.posix.system.fsync(dir.handle))) {
            .SUCCESS => break,
            .INTR => continue,
            .INVAL => break,
            .BADF => return error.InvalidFileDescriptor,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .DQUOT => return error.DiskQuota,
            else => |err| return std.posix.unexpectedErrno(err),
        };

        const parent = std.fs.path.dirname(current) orelse if (std.fs.path.isAbsolute(current)) "/" else ".";
        if (std.mem.eql(u8, parent, current)) return;
        current = parent;
    }
}

fn inspectResumeMetricsJsonl(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    expected_steps: u64,
    expected_optimizer_steps: u64,
    expected_epochs: u64,
    entity_type_count: usize,
    eval_every_epochs: u32,
    early_stopping_threshold: f64,
) !ResumeMetrics {
    var parser = try ResumeMetricsParser.init(expected_steps, expected_optimizer_steps, expected_epochs, entity_type_count, eval_every_epochs, early_stopping_threshold);
    var cursor: usize = 0;

    while (cursor < bytes.len and parser.result.epoch_count < expected_epochs) {
        const relative_end = std.mem.indexOfScalar(u8, bytes[cursor..], '\n') orelse
            return error.IncompleteTrainingMetricsLine;
        const next_cursor = cursor + relative_end + 1;
        const line = bytes[cursor .. next_cursor - 1];
        cursor = next_cursor;
        try parser.processLine(allocator, line, @intCast(cursor));
    }
    if (parser.result.prefix_len == 0 and expected_epochs == 0) parser.result.prefix_len = @intCast(cursor);
    return parser.finish();
}

fn loadResumePositiveCounts(stats: *BatchTargetStats, value: std.json.Value, entity_type_count: usize) !void {
    if (value != .array or value.array.items.len != entity_type_count) {
        return error.InvalidTrainingMetrics;
    }
    for (value.array.items, 0..) |count_value, idx| {
        stats.positive_counts_by_entity_type[idx] = jsonU64(count_value) orelse
            return error.InvalidTrainingMetrics;
    }
}

fn jsonU64(value: std.json.Value) ?u64 {
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else null,
        else => null,
    };
}

fn jsonF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .float => |number| number,
        .integer => |number| @floatFromInt(number),
        else => null,
    };
}

const StepTiming = struct {
    target_build_ns: u64,
    train_step_ns: u64,
    step_wall_ns: u64,
    profile: real_autodiff.StepProfile = .{},

    fn supervisedTokensPerSecond(self: StepTiming, stats: BatchTargetStats) f64 {
        return tokensPerSecond(stats.supervised_token_count, self.step_wall_ns);
    }
};

const EpochTiming = struct {
    epoch_wall_ns: u64,
};

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn elapsedNs(start_ns: u64, end_ns: u64) u64 {
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn nsToMillis(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn tokensPerSecond(tokens: u64, ns: u64) f64 {
    if (tokens == 0 or ns == 0) return 0.0;
    const seconds = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
    return @as(f64, @floatFromInt(tokens)) / seconds;
}

fn ratio(numerator: u64, denominator: u64) f64 {
    if (denominator == 0) return 0.0;
    return @as(f64, @floatFromInt(numerator)) / @as(f64, @floatFromInt(denominator));
}

/// Fills input_ids, attention_mask, and one-hot targets for one batch
/// using the real DeBERTa-v3 Unigram tokenizer with the GLiNER2 HF
/// prompt format: [P] entity_types... [E] [SEP_TEXT] text_tokens...
///
/// Entity annotations map to word-level positions via the tokenizer's
/// `words_mask` / `first_token_positions` outputs, then to one-hot
/// targets per text-token position. Prompt, entity-label, separator, and
/// padding tokens are represented by all-zero rows so they do not contribute
/// to the token-classifier fallback loss.
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
) BatchTargetStats {
    const sl: usize = seq_len;
    const nc: usize = num_classes;
    var stats = BatchTargetStats{ .entity_type_count = entity_types.len };

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
                stats.supervised_token_count += 1;
                if (cls > 0) {
                    stats.entity_token_count += 1;
                    const entity_idx: usize = @intCast(cls - 1);
                    if (entity_idx < stats.entity_type_count) {
                        stats.positive_counts_by_entity_type[entity_idx] += 1;
                    }
                }
            } else if (tok_mask[p] != 0) {
                // Non-padding, non-word token (prompt/special tokens): ignore.
                stats.ignored_token_count += 1;
            } else {
                stats.ignored_token_count += 1;
            }
            // Padding (tok_mask==0): all-zero → contributes 0 to loss.
        }
    }

    return stats;
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

fn parseBackend(value: []const u8) ?Gliner2TrainBackend {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    return null;
}

fn parseLrScheduler(value: []const u8) ?LrScheduler {
    if (std.mem.eql(u8, value, "linear")) return .linear;
    if (std.mem.eql(u8, value, "cosine")) return .cosine;
    if (std.mem.eql(u8, value, "cosine_restarts")) return .cosine_restarts;
    if (std.mem.eql(u8, value, "constant")) return .constant;
    return null;
}

fn parseReportTo(value: []const u8) ?ReportTo {
    if (std.ascii.eqlIgnoreCase(value, "stdout")) return .stdout;
    if (std.ascii.eqlIgnoreCase(value, "jsonl")) return .jsonl;
    return null;
}

fn parseCheckpointStrategy(value: []const u8) ?ml.graph.checkpoint.CheckpointStrategy {
    if (std.ascii.eqlIgnoreCase(value, "every-n-layers") or std.ascii.eqlIgnoreCase(value, "every_n_layers")) return .every_n_layers;
    if (std.ascii.eqlIgnoreCase(value, "attention-outputs") or std.ascii.eqlIgnoreCase(value, "attention_outputs")) return .attention_outputs;
    if (std.ascii.eqlIgnoreCase(value, "parameters-only") or std.ascii.eqlIgnoreCase(value, "parameters_only")) return .parameters_only;
    return null;
}

fn checkpointStrategyName(strategy: ml.graph.checkpoint.CheckpointStrategy) []const u8 {
    return switch (strategy) {
        .every_n_layers => "every-n-layers",
        .attention_outputs => "attention-outputs",
        .parameters_only => "parameters-only",
    };
}

fn selectBackend(
    requested: Gliner2TrainBackend,
    force_native: bool,
    metal_available: bool,
) !Gliner2TrainBackend {
    if (force_native) return .native;
    return switch (requested) {
        .auto => if (metal_available) .metal else .native,
        .metal => if (metal_available) .metal else error.MetalBackendUnavailable,
        .native => .native,
    };
}

fn backendLabel(backend: Gliner2TrainBackend) []const u8 {
    return switch (backend) {
        .auto => "auto",
        .metal => "Metal",
        .native => "native CPU/BLAS",
    };
}

fn loadSafetensorsIntoGpuHostedStore(
    allocator: std.mem.Allocator,
    weight_store: *MetalWeightStore,
    st_path: []const u8,
) !void {
    if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
    var source = try SafetensorsSource.initAbsolute(allocator, st_path);
    errdefer source.weightSource().deinit();
    const ws = source.weightSource();
    const names = try ws.listNames(allocator);
    defer allocator.free(names);

    var loaded_count: usize = 0;
    for (names) |name| {
        var loaded = ws.getTensor(name) catch continue;
        defer loaded.deinit();
        var owned_loaded = try cloneLoadedWeight(allocator, loaded, stripEncoderPrefix(name));
        errdefer owned_loaded.deinit();
        const stripped = stripEncoderPrefix(name);
        const owned_name = try allocator.dupe(u8, stripped);
        errdefer allocator.free(owned_name);
        try weight_store.lazy_weights.put(allocator, owned_name, .{
            .tensor_ref = undefined,
            .host_loaded = owned_loaded,
            .active_tier = .host,
            .loaded_bytes = owned_loaded.tensor.data.len,
        });
        loaded_count += 1;
    }
    print("  loaded {d} weights via Metal from {s}\n", .{ loaded_count, st_path });
    source.weightSource().deinit();
}

fn cloneLoadedWeight(allocator: std.mem.Allocator, loaded: LoadedWeight, name: []const u8) !LoadedWeight {
    if (loaded.quantized or loaded.quantized_storage != null) return error.UnsupportedQuantizedTrainingWeight;
    const owned_data = try allocator.dupe(u8, loaded.tensor.data);
    errdefer allocator.free(owned_data);
    const owned_shape = try allocator.dupe(i64, loaded.tensor.shape);
    errdefer allocator.free(owned_shape);
    _ = name;
    return .{
        .tensor = .{
            .data = owned_data,
            .dtype = loaded.tensor.dtype,
            .shape = owned_shape,
            .name = "",
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        },
        .quantized = false,
    };
}

fn initClassifierHeadInGpuHostedStore(
    allocator: std.mem.Allocator,
    weight_store: *MetalWeightStore,
    seed: u64,
    hidden_size: u32,
    num_classes: u32,
) !void {
    if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
    var rng_init = std.Random.DefaultPrng.init(seed);
    var prng_init = rng_init.random();
    const H = hidden_size;
    const C = num_classes;

    const w_data = try allocator.alloc(f32, @as(usize, @intCast(C)) * @as(usize, @intCast(H)));
    defer allocator.free(w_data);
    const sd: f32 = 0.02;
    for (w_data) |*v| v.* = prng_init.floatNorm(f32) * sd;
    const w_tensor = try Tensor.initFloat32(allocator, "task_classifier.weight", &.{ C, H }, w_data);
    try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "task_classifier.weight"), .{
        .tensor_ref = undefined,
        .host_loaded = .{ .tensor = w_tensor },
        .active_tier = .host,
        .loaded_bytes = w_tensor.data.len,
    });

    const b_data = try allocator.alloc(f32, C);
    defer allocator.free(b_data);
    @memset(b_data, 0.0);
    const b_tensor = try Tensor.initFloat32(allocator, "task_classifier.bias", &.{C}, b_data);
    try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "task_classifier.bias"), .{
        .tensor_ref = undefined,
        .host_loaded = .{ .tensor = b_tensor },
        .active_tier = .host,
        .loaded_bytes = b_tensor.data.len,
    });
    print("  initialized classifier head (Metal): [{d}, {d}] + [{d}]\n", .{ C, H, C });
    try initParityTopLevelWeightsMetal(allocator, weight_store, H);
}

fn initClassifierHeadInNativeStore(
    allocator: std.mem.Allocator,
    weight_store: *native_compute.WeightStore,
    seed: u64,
    hidden_size: u32,
    num_classes: u32,
) !void {
    var rng_init = std.Random.DefaultPrng.init(seed);
    var prng_init = rng_init.random();
    const H = hidden_size;
    const C = num_classes;

    const w_data = try allocator.alloc(f32, @as(usize, @intCast(C)) * @as(usize, @intCast(H)));
    defer allocator.free(w_data);
    const sd: f32 = 0.02;
    for (w_data) |*v| v.* = prng_init.floatNorm(f32) * sd;
    const w_tensor = try Tensor.initFloat32(allocator, "task_classifier.weight", &.{ C, H }, w_data);
    try weight_store.resident_weights.put(
        allocator,
        try allocator.dupe(u8, "task_classifier.weight"),
        .{ .tensor = w_tensor },
    );

    const b_data = try allocator.alloc(f32, C);
    defer allocator.free(b_data);
    @memset(b_data, 0.0);
    const b_tensor = try Tensor.initFloat32(allocator, "task_classifier.bias", &.{C}, b_data);
    try weight_store.resident_weights.put(
        allocator,
        try allocator.dupe(u8, "task_classifier.bias"),
        .{ .tensor = b_tensor },
    );
    print("  initialized classifier head (native): [{d}, {d}] + [{d}]\n", .{ C, H, C });
}

fn fillRectIdentity(data: []f32, out_dim: usize, in_dim: usize) void {
    @memset(data, 0.0);
    for (0..@min(out_dim, in_dim)) |i| data[i * in_dim + i] = 1.0;
}

fn initParityTopLevelWeightsNative(
    allocator: std.mem.Allocator,
    weight_store: *native_compute.WeightStore,
    hidden_size: u32,
) !void {
    const specs = parityWeightSpecs(hidden_size);
    for (specs) |spec| {
        const out_dim: usize = @intCast(spec.out_dim);
        const in_dim: usize = @intCast(spec.in_dim);
        const data = try allocator.alloc(f32, out_dim * in_dim);
        defer allocator.free(data);
        fillRectIdentity(data, out_dim, in_dim);
        const tensor = try Tensor.initFloat32(allocator, spec.name, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) }, data);
        try native_wsPutMissingOwned(allocator, weight_store, spec.name, tensor);
    }
    const vector_specs = parityVectorSpecs(hidden_size);
    for (vector_specs) |spec| {
        const dim: usize = @intCast(spec.dim);
        const data = try allocator.alloc(f32, dim);
        defer allocator.free(data);
        fillVector(data, spec.fill);
        const tensor = try Tensor.initFloat32(allocator, spec.name, &.{@as(i64, @intCast(dim))}, data);
        try native_wsPutMissingOwned(allocator, weight_store, spec.name, tensor);
    }
}

fn native_wsPutMissingOwned(
    allocator: std.mem.Allocator,
    weight_store: *native_compute.WeightStore,
    name: []const u8,
    tensor: Tensor,
) !void {
    if (weight_store.resident_weights.contains(name)) {
        var discard = tensor;
        discard.deinit();
        return;
    }
    const owned_name = try allocator.dupe(u8, name);
    try weight_store.resident_weights.put(allocator, owned_name, .{ .tensor = tensor });
}

fn initParityTopLevelWeightsMetal(
    allocator: std.mem.Allocator,
    weight_store: *MetalWeightStore,
    hidden_size: u32,
) !void {
    if (comptime !build_options.enable_metal) return;
    const specs = parityWeightSpecs(hidden_size);
    for (specs) |spec| {
        if (weight_store.lazy_weights.contains(spec.name)) continue;
        const out_dim: usize = @intCast(spec.out_dim);
        const in_dim: usize = @intCast(spec.in_dim);
        const data = try allocator.alloc(f32, out_dim * in_dim);
        defer allocator.free(data);
        fillRectIdentity(data, out_dim, in_dim);
        const tensor = try Tensor.initFloat32(allocator, spec.name, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) }, data);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, spec.name), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
    const vector_specs = parityVectorSpecs(hidden_size);
    for (vector_specs) |spec| {
        if (weight_store.lazy_weights.contains(spec.name)) continue;
        const dim: usize = @intCast(spec.dim);
        const data = try allocator.alloc(f32, dim);
        defer allocator.free(data);
        fillVector(data, spec.fill);
        const tensor = try Tensor.initFloat32(allocator, spec.name, &.{@as(i64, @intCast(dim))}, data);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, spec.name), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
}

const ParityWeightSpec = struct {
    name: []const u8,
    out_dim: u32,
    in_dim: u32,
};

fn parityWeightSpecs(hidden_size: u32) [25]ParityWeightSpec {
    const H = hidden_size;
    const FF = H * 4;
    const MID = H * 2;
    const COUNT: u32 = 128;
    const COUNT_FF: u32 = 256;
    return .{
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .out_dim = FF, .in_dim = H },
        .{ .name = "span_rep.span_rep_layer.project_start.3.weight", .out_dim = H, .in_dim = FF },
        .{ .name = "span_rep.span_rep_layer.project_end.0.weight", .out_dim = FF, .in_dim = H },
        .{ .name = "span_rep.span_rep_layer.project_end.3.weight", .out_dim = H, .in_dim = FF },
        .{ .name = "span_rep.span_rep_layer.out_project.0.weight", .out_dim = FF, .in_dim = H * 2 },
        .{ .name = "span_rep.span_rep_layer.out_project.3.weight", .out_dim = H, .in_dim = FF },
        .{ .name = "classifier.0.weight", .out_dim = MID, .in_dim = H },
        .{ .name = "classifier.2.weight", .out_dim = 1, .in_dim = MID },
        .{ .name = "count_pred.0.weight", .out_dim = MID, .in_dim = H },
        .{ .name = "count_pred.2.weight", .out_dim = 20, .in_dim = MID },
        .{ .name = "count_embed.pos_embedding.weight", .out_dim = 20, .in_dim = H },
        .{ .name = "count_embed.gru.weight_ih_l0", .out_dim = H * 3, .in_dim = H },
        .{ .name = "count_embed.gru.weight_hh_l0", .out_dim = H * 3, .in_dim = H },
        .{ .name = "count_embed.transformer.in_projector.weight", .out_dim = COUNT, .in_dim = H },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.in_proj_weight", .out_dim = COUNT * 3, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.out_proj.weight", .out_dim = COUNT, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear1.weight", .out_dim = COUNT_FF, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear2.weight", .out_dim = COUNT, .in_dim = COUNT_FF },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.in_proj_weight", .out_dim = COUNT * 3, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.out_proj.weight", .out_dim = COUNT, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear1.weight", .out_dim = COUNT_FF, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear2.weight", .out_dim = COUNT, .in_dim = COUNT_FF },
        .{ .name = "count_embed.transformer.out_projector.0.weight", .out_dim = H, .in_dim = H + COUNT },
        .{ .name = "count_embed.transformer.out_projector.2.weight", .out_dim = H, .in_dim = H },
        .{ .name = "count_embed.transformer.out_projector.4.weight", .out_dim = H, .in_dim = H },
    };
}

const ParityVectorFill = enum { zero, one };

const ParityVectorSpec = struct {
    name: []const u8,
    dim: u32,
    fill: ParityVectorFill = .zero,
};

fn fillVector(data: []f32, fill: ParityVectorFill) void {
    @memset(data, switch (fill) {
        .zero => 0.0,
        .one => 1.0,
    });
}

fn parityVectorSpecs(hidden_size: u32) [26]ParityVectorSpec {
    const H = hidden_size;
    const MID = H * 2;
    const COUNT: u32 = 128;
    const COUNT_FF: u32 = 256;
    return .{
        .{ .name = "classifier.0.bias", .dim = MID },
        .{ .name = "classifier.2.bias", .dim = 1 },
        .{ .name = "count_pred.0.bias", .dim = MID },
        .{ .name = "count_pred.2.bias", .dim = 20 },
        .{ .name = "count_embed.gru.bias_ih_l0", .dim = H * 3 },
        .{ .name = "count_embed.gru.bias_hh_l0", .dim = H * 3 },
        .{ .name = "count_embed.transformer.in_projector.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.in_proj_bias", .dim = COUNT * 3 },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.out_proj.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm1.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm1.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear1.bias", .dim = COUNT_FF },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm2.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.in_proj_bias", .dim = COUNT * 3 },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.out_proj.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm1.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm1.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear1.bias", .dim = COUNT_FF },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm2.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.out_projector.0.bias", .dim = H },
        .{ .name = "count_embed.transformer.out_projector.2.bias", .dim = H },
        .{ .name = "count_embed.transformer.out_projector.4.bias", .dim = H },
    };
}

fn deinitGpuHostedWeightStore(allocator: std.mem.Allocator, weight_store: *MetalWeightStore) void {
    if (comptime !build_options.enable_metal) return;
    metal_compute.deinitPrefetchQueue(weight_store);
    var it = weight_store.lazy_weights.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        if (entry.value_ptr.host_loaded) |*loaded| loaded.deinit();
        if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
    }
    weight_store.lazy_weights.deinit(allocator);
}

const SpanParityDebugStats = struct {
    rows: usize = 0,
    entity_types: usize = 0,
    logits_count: usize = 0,
    valid_weight_count: usize = 0,
    positive_count: usize = 0,
    mask_weight_sum: f64 = 0.0,
    weighted_mask_sum: f64 = 0.0,
    logits_min: f64 = 0.0,
    logits_max: f64 = 0.0,
    logits_mean: f64 = 0.0,
    positive_logits_mean: f64 = 0.0,
    negative_logits_mean: f64 = 0.0,
    bce_unweighted_sum: f64 = 0.0,
    bce_masked_sum: f64 = 0.0,
    bce_masked_positive_sum: f64 = 0.0,
    bce_masked_negative_sum: f64 = 0.0,
    bce_weighted_sum: f64 = 0.0,
    bce_weighted_positive_sum: f64 = 0.0,
    bce_weighted_negative_sum: f64 = 0.0,
    bce_weighted_mean: f64 = 0.0,
};

fn printSpanParityDebug(
    logits: []const f32,
    targets: []const f32,
    targets_shape: ml.graph.Shape,
    entity_types: usize,
    has_label_positive_weights: bool,
    opts: Options,
) !void {
    if (entity_types == 0) return error.InvalidEntityTypes;
    const rows: usize = @intCast(targets_shape.dims[0]);
    const width: usize = @intCast(targets_shape.dims[1]);
    const expected_min_width = if (has_label_positive_weights)
        gliner2_autodiff.weightedSpanStartTargetWidth(entity_types)
    else
        gliner2_autodiff.spanStartTargetWidth(entity_types);
    if (width < expected_min_width) return error.InvalidGlinerSpanTargetShape;
    if (targets.len != rows * width) return error.InvalidGlinerSpanTargetShape;
    if (logits.len != rows * entity_types) return error.InvalidGlinerSpanLogitsShape;

    var stats = SpanParityDebugStats{
        .rows = rows,
        .entity_types = entity_types,
        .logits_count = logits.len,
        .logits_min = 1.0e300,
        .logits_max = -1.0e300,
    };
    var logits_sum: f64 = 0.0;
    var pos_logits_sum: f64 = 0.0;
    var neg_logits_sum: f64 = 0.0;
    var neg_count: usize = 0;
    const top_k = 5;
    var top_bce = [_]f64{-1.0} ** top_k;
    var top_logits = [_]f64{0.0} ** top_k;
    var top_rows = [_]usize{0} ** top_k;
    var top_entities = [_]usize{0} ** top_k;

    for (0..rows) |row_idx| {
        const target_row = row_idx * width;
        const logit_row = row_idx * entity_types;
        for (0..entity_types) |entity_idx| {
            const label = targets[target_row + entity_idx];
            const mask_weight = targets[target_row + entity_types + entity_idx];
            const logit = @as(f64, @floatCast(logits[logit_row + entity_idx]));
            stats.logits_min = @min(stats.logits_min, logit);
            stats.logits_max = @max(stats.logits_max, logit);
            logits_sum += logit;

            const is_positive = label > 0.0;
            if (is_positive) {
                stats.positive_count += 1;
                pos_logits_sum += logit;
            } else {
                neg_count += 1;
                neg_logits_sum += logit;
            }

            const bce = stableBceWithLogits(logit, if (is_positive) 1.0 else 0.0);
            stats.bce_unweighted_sum += bce;
            if (mask_weight > 0.0) {
                stats.valid_weight_count += 1;
                const mask_weight64 = @as(f64, @floatCast(mask_weight));
                const label_weight: f64 = if (is_positive) blk: {
                    if (has_label_positive_weights) {
                        const positive_weights_offset = 2 * entity_types;
                        break :blk @as(f64, @floatCast(targets[target_row + positive_weights_offset + entity_idx]));
                    }
                    break :blk @as(f64, @floatCast(opts.span_positive_weight));
                } else @as(f64, @floatCast(opts.span_negative_weight));
                stats.mask_weight_sum += mask_weight64;
                stats.weighted_mask_sum += mask_weight64 * label_weight;
                stats.bce_masked_sum += bce * mask_weight64;
                stats.bce_weighted_sum += bce * mask_weight64 * label_weight;
                if (is_positive) {
                    stats.bce_masked_positive_sum += bce * mask_weight64;
                    stats.bce_weighted_positive_sum += bce * mask_weight64 * label_weight;
                } else {
                    stats.bce_masked_negative_sum += bce * mask_weight64;
                    stats.bce_weighted_negative_sum += bce * mask_weight64 * label_weight;
                    if (bce > top_bce[top_k - 1]) {
                        var insert_idx: usize = top_k - 1;
                        while (insert_idx > 0 and bce > top_bce[insert_idx - 1]) : (insert_idx -= 1) {
                            top_bce[insert_idx] = top_bce[insert_idx - 1];
                            top_logits[insert_idx] = top_logits[insert_idx - 1];
                            top_rows[insert_idx] = top_rows[insert_idx - 1];
                            top_entities[insert_idx] = top_entities[insert_idx - 1];
                        }
                        top_bce[insert_idx] = bce;
                        top_logits[insert_idx] = logit;
                        top_rows[insert_idx] = row_idx;
                        top_entities[insert_idx] = entity_idx;
                    }
                }
            }
        }
    }

    if (logits.len > 0) stats.logits_mean = logits_sum / @as(f64, @floatFromInt(logits.len));
    if (stats.positive_count > 0) stats.positive_logits_mean = pos_logits_sum / @as(f64, @floatFromInt(stats.positive_count));
    if (neg_count > 0) stats.negative_logits_mean = neg_logits_sum / @as(f64, @floatFromInt(neg_count));
    if (stats.weighted_mask_sum > 0.0) stats.bce_weighted_mean = stats.bce_weighted_sum / stats.weighted_mask_sum;

    print(
        "SPAN_PARITY_DEBUG {{\"rows\":{d},\"entity_types\":{d},\"logits_count\":{d},\"valid_weight_count\":{d},\"positive_count\":{d},\"mask_weight_sum\":{d:.9},\"weighted_mask_sum\":{d:.9},\"logits_min\":{d:.9},\"logits_max\":{d:.9},\"logits_mean\":{d:.9},\"positive_logits_mean\":{d:.9},\"negative_logits_mean\":{d:.9},\"bce_unweighted_sum\":{d:.9},\"bce_masked_sum\":{d:.9},\"bce_masked_positive_sum\":{d:.9},\"bce_masked_negative_sum\":{d:.9},\"bce_weighted_sum\":{d:.9},\"bce_weighted_positive_sum\":{d:.9},\"bce_weighted_negative_sum\":{d:.9},\"bce_weighted_mean\":{d:.9},\"top_valid_negative_logits\":[",
        .{
            stats.rows,
            stats.entity_types,
            stats.logits_count,
            stats.valid_weight_count,
            stats.positive_count,
            stats.mask_weight_sum,
            stats.weighted_mask_sum,
            stats.logits_min,
            stats.logits_max,
            stats.logits_mean,
            stats.positive_logits_mean,
            stats.negative_logits_mean,
            stats.bce_unweighted_sum,
            stats.bce_masked_sum,
            stats.bce_masked_positive_sum,
            stats.bce_masked_negative_sum,
            stats.bce_weighted_sum,
            stats.bce_weighted_positive_sum,
            stats.bce_weighted_negative_sum,
            stats.bce_weighted_mean,
        },
    );
    var printed: usize = 0;
    for (0..top_k) |idx| {
        if (top_bce[idx] < 0.0) continue;
        if (printed > 0) print(",", .{});
        const row = top_rows[idx];
        print(
            "{{\"row\":{d},\"entity\":{d},\"start\":{d},\"width\":{d},\"logit\":{d:.9},\"bce\":{d:.9}}}",
            .{
                row,
                top_entities[idx],
                row / opts.max_span_width,
                row % opts.max_span_width,
                top_logits[idx],
                top_bce[idx],
            },
        );
        printed += 1;
    }
    print("],\"reduction\":\"{s}\"}}\n", .{spanLossReductionName(opts.span_loss_reduction)});
}

fn printSpanComponentDebug(components: gliner2_autodiff.SpanStartComponentDebug) void {
    print(
        "SPAN_COMPONENT_DEBUG {{\"positive_row\":{d},\"positive_entity\":{d},\"schema_row\":{d},\"start_hidden_norm\":{d:.9},\"end_hidden_norm\":{d:.9},\"projected_span_norm\":{d:.9},\"schema_hidden_norm\":{d:.9},\"count_gru_state_norm\":{d:.9},\"count_state_norm\":{d:.9},\"count_in_project_norm\":{d:.9},\"count_layer0_norm\":{d:.9},\"count_layer1_norm\":{d:.9},\"count_out0_norm\":{d:.9},\"count_out2_norm\":{d:.9},\"schema_projection_norm\":{d:.9},\"projected_schema_dot\":{d:.9},\"projected_span_mean\":{d:.9},\"count_gru_state_mean\":{d:.9},\"count_state_mean\":{d:.9},\"count_in_project_mean\":{d:.9},\"count_layer0_mean\":{d:.9},\"count_layer1_mean\":{d:.9},\"count_out0_mean\":{d:.9},\"count_out2_mean\":{d:.9},\"schema_projection_mean\":{d:.9}",
        .{
            components.positive_row,
            components.positive_entity,
            components.schema_row,
            components.start_hidden_norm,
            components.end_hidden_norm,
            components.projected_span_norm,
            components.schema_hidden_norm,
            components.count_gru_state_norm,
            components.count_state_norm,
            components.count_in_project_norm,
            components.count_layer0_norm,
            components.count_layer1_norm,
            components.count_out0_norm,
            components.count_out2_norm,
            components.schema_projection_norm,
            components.projected_schema_dot,
            components.projected_span_mean,
            components.count_gru_state_mean,
            components.count_state_mean,
            components.count_in_project_mean,
            components.count_layer0_mean,
            components.count_layer1_mean,
            components.count_out0_mean,
            components.count_out2_mean,
            components.schema_projection_mean,
        },
    );
    print(
        ",\"negative_row\":{d},\"negative_entity\":{d},\"negative_logit\":{d:.9},\"negative_start_hidden_norm\":{d:.9},\"negative_end_hidden_norm\":{d:.9},\"negative_schema_hidden_norm\":{d:.9},\"negative_projected_span_norm\":{d:.9},\"negative_schema_projection_norm\":{d:.9},\"negative_projected_schema_dot\":{d:.9},\"negative_start_hidden_mean\":{d:.9},\"negative_end_hidden_mean\":{d:.9},\"negative_schema_hidden_mean\":{d:.9},\"negative_projected_span_mean\":{d:.9},\"negative_schema_projection_mean\":{d:.9}}}\n",
        .{
            components.negative_row,
            components.negative_entity,
            components.negative_logit,
            components.negative_start_hidden_norm,
            components.negative_end_hidden_norm,
            components.negative_schema_hidden_norm,
            components.negative_projected_span_norm,
            components.negative_schema_projection_norm,
            components.negative_projected_schema_dot,
            components.negative_start_hidden_mean,
            components.negative_end_hidden_mean,
            components.negative_schema_hidden_mean,
            components.negative_projected_span_mean,
            components.negative_schema_projection_mean,
        },
    );
}

fn printGliner2TotalLossComponentDebug(components: gliner2_autodiff.Gliner2TotalLossComponentDebug) void {
    print(
        "GLINER2_TOTAL_LOSS_COMPONENT_DEBUG {{\"classification_loss\":{d:.9},\"structure_loss\":{d:.9},\"count_loss\":{d:.9},\"total_loss\":{d:.9}}}\n",
        .{
            components.classification_loss,
            components.structure_loss,
            components.count_loss,
            components.total_loss,
        },
    );
}

fn printGliner2ClassificationDebug(
    logits: []const f32,
    targets: []const f32,
    targets_shape: ml.graph.Shape,
    entity_types: usize,
) !void {
    if (entity_types == 0) return error.InvalidEntityTypes;
    if (targets_shape.rank() != 2) return error.InvalidGlinerSpanTargetShape;
    const rows: usize = @intCast(targets_shape.dims[0]);
    const target_width: usize = @intCast(targets_shape.dims[1]);
    if (targets.len != rows * target_width) return error.InvalidGlinerSpanTargetShape;
    if (logits.len != rows * entity_types) return error.InvalidGlinerSpanLogitsShape;
    const label_offset = gliner2_autodiff.gliner2TotalLossClassificationLabelsOffset(entity_types);
    const mask_offset = gliner2_autodiff.gliner2TotalLossClassificationMaskOffset(entity_types);
    if (target_width < mask_offset + entity_types) return error.InvalidGlinerSpanTargetShape;

    var valid_count: usize = 0;
    var positive_count: usize = 0;
    var logits_min: f64 = std.math.inf(f64);
    var logits_max: f64 = -std.math.inf(f64);
    var logits_sum: f64 = 0.0;
    var label_sum: f64 = 0.0;
    var mask_sum: f64 = 0.0;
    var bce_sum: f64 = 0.0;

    for (0..rows) |row_idx| {
        const target_row = row_idx * target_width;
        const logit_row = row_idx * entity_types;
        for (0..entity_types) |entity_idx| {
            const label = @as(f64, @floatCast(targets[target_row + label_offset + entity_idx]));
            const mask = @as(f64, @floatCast(targets[target_row + mask_offset + entity_idx]));
            const logit = @as(f64, @floatCast(logits[logit_row + entity_idx]));
            label_sum += label;
            mask_sum += mask;
            if (label > 0.0) positive_count += 1;
            if (mask > 0.0) {
                valid_count += 1;
                logits_min = @min(logits_min, logit);
                logits_max = @max(logits_max, logit);
                logits_sum += logit;
                bce_sum += stableBceWithLogits(logit, label) * mask;
            }
        }
    }

    if (valid_count == 0) {
        logits_min = 0.0;
        logits_max = 0.0;
    }
    const logits_mean = if (valid_count > 0) logits_sum / @as(f64, @floatFromInt(valid_count)) else 0.0;
    print(
        "GLINER2_CLASSIFICATION_DEBUG {{\"rows\":{d},\"entity_types\":{d},\"logits_count\":{d},\"valid_count\":{d},\"positive_count\":{d},\"label_sum\":{d:.9},\"mask_sum\":{d:.9},\"logits_min\":{d:.9},\"logits_max\":{d:.9},\"logits_mean\":{d:.9},\"bce_sum\":{d:.9},\"logits_head\":[",
        .{
            rows,
            entity_types,
            logits.len,
            valid_count,
            positive_count,
            label_sum,
            mask_sum,
            logits_min,
            logits_max,
            logits_mean,
            bce_sum,
        },
    );
    const head_count = @min(logits.len, 16);
    for (0..head_count) |idx| {
        if (idx != 0) print(",", .{});
        print("{d:.9}", .{logits[idx]});
    }
    print("],\"labels_head\":[", .{});
    const label_head_count = @min(rows * entity_types, 16);
    for (0..label_head_count) |idx| {
        if (idx != 0) print(",", .{});
        const row_idx = idx / entity_types;
        const entity_idx = idx % entity_types;
        print("{d:.1}", .{targets[row_idx * target_width + label_offset + entity_idx]});
    }
    print("],\"mask_head\":[", .{});
    for (0..label_head_count) |idx| {
        if (idx != 0) print(",", .{});
        const row_idx = idx / entity_types;
        const entity_idx = idx % entity_types;
        print("{d:.1}", .{targets[row_idx * target_width + mask_offset + entity_idx]});
    }
    print("],\"valid_logits_head\":[", .{});
    var valid_printed: usize = 0;
    outer: for (0..rows) |row_idx| {
        const target_row = row_idx * target_width;
        const logit_row = row_idx * entity_types;
        for (0..entity_types) |entity_idx| {
            if (targets[target_row + mask_offset + entity_idx] <= 0.0) continue;
            if (valid_printed != 0) print(",", .{});
            print("{d:.9}", .{logits[logit_row + entity_idx]});
            valid_printed += 1;
            if (valid_printed >= 16) break :outer;
        }
    }
    print("],\"valid_labels_head\":[", .{});
    valid_printed = 0;
    outer_labels: for (0..rows) |row_idx| {
        const target_row = row_idx * target_width;
        for (0..entity_types) |entity_idx| {
            if (targets[target_row + mask_offset + entity_idx] <= 0.0) continue;
            if (valid_printed != 0) print(",", .{});
            print("{d:.1}", .{targets[target_row + label_offset + entity_idx]});
            valid_printed += 1;
            if (valid_printed >= 16) break :outer_labels;
        }
    }
    print("]}}\n", .{});
}

const CompactClassificationTargets = struct {
    targets: []f32,
    shape: ml.graph.Shape,
};

fn compactGliner2TaskRowTargetsForDebug(
    allocator: std.mem.Allocator,
    targets: []const f32,
    targets_shape: ml.graph.Shape,
    batch: u32,
    max_schema_tasks: u32,
) !CompactClassificationTargets {
    if (targets_shape.rank() != 2 or batch == 0 or max_schema_tasks == 0) return error.InvalidGlinerSpanTargetShape;
    const rows: usize = @intCast(targets_shape.dims[0]);
    const width: usize = @intCast(targets_shape.dims[1]);
    if (targets.len != rows * width) return error.InvalidGlinerSpanTargetShape;
    if (@mod(rows, @as(usize, batch)) != 0) return error.InvalidGlinerSpanTargetShape;
    const max_spans_per_sample = rows / @as(usize, batch);
    const schema_tasks = @min(max_spans_per_sample, @as(usize, max_schema_tasks));
    const compact_rows = @as(usize, batch) * schema_tasks;
    const compact = try allocator.alloc(f32, compact_rows * width);
    errdefer allocator.free(compact);
    for (0..@as(usize, batch)) |sample_idx| {
        for (0..schema_tasks) |task_idx| {
            const src_row = sample_idx * max_spans_per_sample + task_idx;
            const dst_row = sample_idx * schema_tasks + task_idx;
            @memcpy(
                compact[dst_row * width .. (dst_row + 1) * width],
                targets[src_row * width .. (src_row + 1) * width],
            );
        }
    }
    return .{
        .targets = compact,
        .shape = ml.graph.Shape.init(.f32, &.{ @intCast(compact_rows), @intCast(width) }),
    };
}

fn printSpanPreprocessDebug(batch: *const gliner2_data.EncodedBatch) void {
    const input_ids = batch.input_ids[0..batch.max_length];
    const attention_mask = batch.attention_mask[0..batch.max_length];
    const first_positions = batch.first_token_positions[0..batch.max_words_per_sample];
    const span_mask = batch.span_mask[0..batch.max_spans];
    const span_indices = batch.span_indices[0 .. batch.max_spans * 2];
    const span_labels = batch.span_labels[0 .. batch.max_spans * batch.num_entity_types];
    print("SPAN_PREPROCESS_DEBUG {{\"batch_size\":{d},\"max_length\":{d},\"max_words_per_sample\":{d},\"max_spans\":{d},\"num_entity_types\":{d},\"max_schemas\":{d},\"max_schema_specials\":{d},\"input_ids\":", .{
        batch.batch_size,
        batch.max_length,
        batch.max_words_per_sample,
        batch.max_spans,
        batch.num_entity_types,
        batch.max_schemas,
        batch.max_schema_specials,
    });
    printI32JsonArray(input_ids);
    print(",\"input_ids_all\":", .{});
    printI32JsonArray(batch.input_ids[0 .. batch.batch_size * batch.max_length]);
    print(",\"attention_mask\":", .{});
    printI32JsonArray(attention_mask);
    print(",\"attention_mask_all\":", .{});
    printI32JsonArray(batch.attention_mask[0 .. batch.batch_size * batch.max_length]);
    print(",\"first_token_positions\":", .{});
    printI32JsonArray(first_positions);
    print(",\"first_token_positions_all\":", .{});
    printI32JsonArray(batch.first_token_positions[0 .. batch.batch_size * batch.max_words_per_sample]);
    print(",\"e_token_positions\":", .{});
    printI32JsonArray(batch.e_token_positions[0..batch.num_entity_types]);
    print(",\"e_token_end_positions\":", .{});
    printI32JsonArray(batch.e_token_end_positions[0..batch.num_entity_types]);
    print(",\"e_token_positions_all\":", .{});
    printI32JsonArray(batch.e_token_positions[0 .. batch.batch_size * batch.num_entity_types]);
    print(",\"e_token_end_positions_all\":", .{});
    printI32JsonArray(batch.e_token_end_positions[0 .. batch.batch_size * batch.num_entity_types]);
    if (batch.text_word_counts.len > 0) {
        print(",\"text_word_counts\":", .{});
        printI32JsonArray(batch.text_word_counts[0..batch.batch_size]);
    }
    if (batch.schema_counts.len > 0) {
        print(",\"schema_counts\":", .{});
        printI32JsonArray(batch.schema_counts[0..batch.batch_size]);
    }
    if (batch.task_type_ids.len > 0 and batch.max_schemas > 0) {
        print(",\"task_type_ids\":", .{});
        printI32JsonArray(batch.task_type_ids[0..batch.max_schemas]);
        print(",\"task_type_ids_all\":", .{});
        printI32JsonArray(batch.task_type_ids[0 .. batch.batch_size * batch.max_schemas]);
    }
    if (batch.schema_special_counts.len > 0 and batch.max_schemas > 0) {
        print(",\"schema_special_counts\":", .{});
        printI32JsonArray(batch.schema_special_counts[0..batch.max_schemas]);
        print(",\"schema_special_counts_all\":", .{});
        printI32JsonArray(batch.schema_special_counts[0 .. batch.batch_size * batch.max_schemas]);
    }
    if (batch.schema_special_positions.len > 0 and batch.max_schemas > 0 and batch.max_schema_specials > 0) {
        print(",\"schema_special_positions\":", .{});
        printI32JsonArray(batch.schema_special_positions[0 .. batch.max_schemas * batch.max_schema_specials]);
        print(",\"schema_special_positions_all\":", .{});
        printI32JsonArray(batch.schema_special_positions[0 .. batch.batch_size * batch.max_schemas * batch.max_schema_specials]);
    }
    if (batch.entity_type_kind.len > 0) {
        print(",\"entity_type_kind\":", .{});
        printI32JsonArray(batch.entity_type_kind[0..batch.num_entity_types]);
        print(",\"entity_type_kind_all\":", .{});
        printI32JsonArray(batch.entity_type_kind[0 .. batch.batch_size * batch.num_entity_types]);
    }
    print(",\"span_indices\":", .{});
    printI32JsonArray(span_indices);
    print(",\"span_mask\":", .{});
    printF32JsonArray(span_mask);
    print(",\"span_labels\":", .{});
    printF32JsonArray(span_labels);
    print(",\"span_labels_all\":", .{});
    printF32JsonArray(batch.span_labels[0 .. batch.batch_size * batch.max_spans * batch.num_entity_types]);
    print("}}\n", .{});
}

fn applySpanNegativeMask(
    targets: []f32,
    entity_types: usize,
    row_width: usize,
    mask_rate: f32,
    seed: u64,
) void {
    if (entity_types == 0) return;
    if (row_width < entity_types * 2 or targets.len % row_width != 0) return;
    const rows = targets.len / row_width;
    var prng = std.Random.DefaultPrng.init(seed);
    var rng = prng.random();
    for (0..rows) |row_idx| {
        const row = row_idx * row_width;
        for (0..entity_types) |entity_idx| {
            const label = targets[row + entity_idx];
            const mask_idx = row + entity_types + entity_idx;
            if (label <= 0.0 and targets[mask_idx] > 0.0 and rng.float(f32) < mask_rate) {
                targets[mask_idx] = 0.0;
            }
        }
    }
}

fn applyGliner2InstanceNegativeMask(
    targets: []f32,
    entity_types: usize,
    row_width: usize,
    max_instances: u32,
    mask_rate: f32,
    seed: u64,
) void {
    if (entity_types == 0 or max_instances <= 1 or targets.len % row_width != 0) return;
    const labels_offset = gliner2_autodiff.gliner2TotalLossInstanceLabelsOffset(entity_types);
    const masks_offset = gliner2_autodiff.gliner2TotalLossInstanceMasksOffset(entity_types, max_instances);
    const instance_values = @as(usize, max_instances) * entity_types;
    if (masks_offset + instance_values > row_width) return;

    var prng = std.Random.DefaultPrng.init(seed);
    var rng = prng.random();
    const rows = targets.len / row_width;
    for (0..rows) |row_idx| {
        const row = row_idx * row_width;
        for (0..instance_values) |idx| {
            const label = targets[row + labels_offset + idx];
            const mask_idx = row + masks_offset + idx;
            if (label <= 0.0 and targets[mask_idx] > 0.0 and rng.float(f32) < mask_rate) {
                targets[mask_idx] = 0.0;
            }
        }
    }
}

fn printI32JsonArray(values: []const i32) void {
    print("[", .{});
    for (values, 0..) |value, idx| {
        if (idx != 0) print(",", .{});
        print("{d}", .{value});
    }
    print("]", .{});
}

fn printF32JsonArray(values: []const f32) void {
    print("[", .{});
    for (values, 0..) |value, idx| {
        if (idx != 0) print(",", .{});
        print("{d:.6}", .{value});
    }
    print("]", .{});
}

fn stableBceWithLogits(logit: f64, label: f64) f64 {
    return @max(logit, 0.0) - logit * label + @log(1.0 + @exp(-@abs(logit)));
}

// ---------------------------------------------------------------------------
// Memory pre-flight
// ---------------------------------------------------------------------------

/// Conservative fp32 peak-footprint estimate for a GLiNER2 LoRA training run.
/// In addition to encoder attention, full-task mode explicitly includes the
/// fixed-shape structure score [B,S,I,E], count-transformer [B,I,E,*],
/// classification [B,T,E,*], count [B,T,*], and packed-target tensors. The
/// multipliers cover retained forward values and backward adjoints; all shape
/// arithmetic saturates so hostile CLI dimensions fail the budget instead of
/// overflowing to a deceptively small estimate.
fn estimateTrainingPeakBytes(
    vocab_size: u64,
    hidden_size: u64,
    intermediate_size: u64,
    num_layers: u64,
    num_heads: u64,
    batch_size: u64,
    seq_len: u64,
    metal_reuse_live_set: bool,
    span_objective: bool,
    full_task_objective: bool,
    max_spans: u64,
    num_entity_types: u64,
    structure_max_instances: u32,
    max_schema_tasks: u32,
) u64 {
    const f32_size: u64 = 4;
    const head_dim = hidden_size / @max(num_heads, 1);
    const bh = saturatedProduct(&.{ batch_size, num_heads });
    const ss = saturatedProduct(&.{ seq_len, seq_len });
    const encoder_layer_weights = saturatedSum(&.{
        saturatedProduct(&.{ 4, hidden_size, hidden_size }),
        saturatedProduct(&.{ 2, hidden_size, intermediate_size }),
    });
    const weights = saturatedProduct(&.{
        saturatedSum(&.{
            saturatedProduct(&.{ vocab_size, hidden_size }),
            saturatedProduct(&.{ num_layers, encoder_layer_weights }),
        }),
        f32_size,
    });
    const attn_per_layer = saturatedProduct(&.{
        saturatedSum(&.{
            saturatedProduct(&.{ bh, ss, head_dim }),
            saturatedProduct(&.{ ss, hidden_size }),
        }),
        f32_size,
    });
    const encoder_live = if (metal_reuse_live_set) blk: {
        const hidden_workspace = saturatedProduct(&.{
            batch_size,
            seq_len,
            saturatedSum(&.{ hidden_size, intermediate_size }),
            f32_size,
        });
        break :blk saturatedSum(&.{
            saturatedProduct(&.{ 3, attn_per_layer }),
            saturatedProduct(&.{ 4, hidden_workspace }),
        });
    } else saturatedProduct(&.{ 2, num_layers, attn_per_layer });

    const head_live = if (!span_objective)
        0
    else if (!full_task_objective)
        // Legacy schema span head materializes repeated [B*S*E,H] products.
        saturatedProduct(&.{ 3, batch_size, max_spans, num_entity_types, hidden_size, f32_size })
    else blk: {
        const instances: u64 = @intCast(@max(structure_max_instances, 1));
        const schema_tasks: u64 = @intCast(@max(max_schema_tasks, 1));
        const span_rows = saturatedProduct(&.{ batch_size, max_spans });
        const instance_field_rows = saturatedProduct(&.{ batch_size, instances, num_entity_types });
        const task_field_rows = saturatedProduct(&.{ batch_size, schema_tasks, num_entity_types });
        const task_rows = saturatedProduct(&.{ batch_size, schema_tasks });
        const target_width: u64 = @intCast(gliner2_autodiff.gliner2TotalLossTargetWidthEx(
            @intCast(num_entity_types),
            @intCast(instances),
        ));

        // Span endpoints/projection, count-GRU sequence/projection, structure
        // scores plus direct labels/masks, and two 4-head count-transformer
        // attention matrices. The 12x GRU/projection term covers its gates and
        // retained unrolled states while preserving the exact B*I*E*H axes.
        const structure = saturatedSum(&.{
            saturatedProduct(&.{ 3, span_rows, hidden_size }),
            saturatedProduct(&.{ 12, instance_field_rows, hidden_size }),
            saturatedProduct(&.{ 3, span_rows, instances, num_entity_types }),
            saturatedProduct(&.{ 2, batch_size, instances, 4, num_entity_types, num_entity_types }),
            saturatedProduct(&.{ 2, instance_field_rows, 3 * 128 + 2 * 256 }),
        });
        // classifier.0 produces/activates 2H rows; classifier.2 produces E
        // logits. Count prediction similarly retains two 2H rows and three
        // 20-way tensors (logits, log-probs, labels).
        const classification = saturatedProduct(&.{ task_field_rows, saturatedSum(&.{ saturatedProduct(&.{ 5, hidden_size }), 3 }) });
        const count = saturatedProduct(&.{ task_rows, saturatedSum(&.{ saturatedProduct(&.{ 5, hidden_size }), 60 }) });
        const targets = saturatedProduct(&.{ span_rows, target_width });
        // Forward values, adjoints, and one execution scratch/live copy.
        break :blk saturatedProduct(&.{
            3,
            saturatedSum(&.{ structure, classification, count, targets }),
            f32_size,
        });
    };
    return saturatedSum(&.{ weights, encoder_live, head_live });
}

fn saturatedProduct(values: []const u64) u64 {
    var result: u64 = 1;
    for (values) |value| result = std.math.mul(u64, result, value) catch return std.math.maxInt(u64);
    return result;
}

fn saturatedSum(values: []const u64) u64 {
    var result: u64 = 0;
    for (values) |value| result = std.math.add(u64, result, value) catch return std.math.maxInt(u64);
    return result;
}

test "full-task memory estimate grows with instance and schema tensors and saturates" {
    const base = estimateTrainingPeakBytes(128_100, 768, 3072, 12, 12, 2, 64, true, true, true, 512, 4, 1, 1);
    const wider = estimateTrainingPeakBytes(128_100, 768, 3072, 12, 12, 2, 64, true, true, true, 512, 4, 4, 8);
    try std.testing.expect(wider > base);
    try std.testing.expectEqual(
        std.math.maxInt(u64),
        estimateTrainingPeakBytes(
            std.math.maxInt(u64),
            std.math.maxInt(u64),
            std.math.maxInt(u64),
            std.math.maxInt(u64),
            1,
            std.math.maxInt(u64),
            std.math.maxInt(u64),
            false,
            true,
            false,
            std.math.maxInt(u64),
            std.math.maxInt(u64),
            1,
            1,
        ),
    );
}

fn metalBufferReuseEnabledForPreflight() bool {
    const raw = std.c.getenv("TERMITE_METAL_BUFFER_REUSE") orelse return true;
    const value = std.mem.span(raw);
    if (value.len == 0) return true;
    return !(value[0] == '0' or value[0] == 'f' or value[0] == 'F');
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
        \\  --eval-data <path>        Content-disjoint held-out JSONL (optional)
        \\  --out-dir <path>          Output directory for LoRA adapter weights
        \\
        \\options:
        \\  --epochs <n>              Number of training epochs (default: 10)
        \\  --batch-size <n>          Examples per step (default: 2)
        \\  --seq-len <n>             Max sequence length (default: 256)
        \\  --learning-rate, --lr <f> Learning rate (default: 5e-4)
        \\  --lr-scheduler <name>     linear, cosine, cosine_restarts, or constant (default: linear)
        \\  --warmup-ratio <f>        Warmup fraction when --warmup-steps is 0 (default: 0.1)
        \\  --warmup-steps <n>        Explicit optimizer warmup steps (default: 0)
        \\  --num-cycles <f>          Cosine-restart cycles (default: 0.5)
        \\  --weight-decay <f>        AdamW weight decay (default: 0.01)
        \\  --lora-rank <n>           LoRA rank (default: 16)
        \\  --lora-alpha <f>          LoRA alpha scaling (default: 32)
        \\  --lora-dropout <f>        LoRA dropout probability (default: 0)
        \\  --lora-targets <csv>      Target module groups (default: encoder,span_rep,classifier,count_embed,count_pred)
        \\  --num-classes <n>         Entity classes incl. O tag (default: 5; total-loss derives this)
        \\  --entity-types <csv>      Entity label order seed for classes 1..N
        \\  --objective <name>        token, span-start, or gliner2-total-loss (default: gliner2-total-loss)
        \\  --max-span-width <n>      Max span width for span objectives (default: 8)
        \\  --span-loss <name>        bce or mse for span-start labels (default: bce)
        \\  --span-loss-reduction <r> mean or sum (default: sum)
        \\  --span-positive-weight <f> Positive span-label loss weight (default: 1)
        \\  --span-label-positive-weights <csv> Per-label positive weights, e.g. person=32,organization=96
        \\  --span-negative-weight <f> Negative span-label loss weight (default: 1)
        \\  --span-hard-negative-weight <f> Extra negative weight for spans overlapping gold entities (default: 1)
        \\  --span-negative-mask-rate <f> Randomly mask this fraction of negative span labels (default: 0.5)
        \\  --max-examples <n>        Cap on training examples (default: 0 = all)
        \\  --max-steps <n>           Exact optimizer-step count; cycles data as needed (default: 0 = use epochs)
        \\  --checkpoint-every-epochs <n>
        \\                            Save recoverable weights + Adam state every N complete epochs
        \\  --checkpoint-keep-last <n>
        \\                            Retain the newest N periodic checkpoints (default: 3; 0 = all)
        \\  --resume-checkpoint <path>
        \\                            Resume an epoch-boundary checkpoint in the same output directory
        \\                            (Metal checkpoint/save/resume also requires --compiled-required)
        \\  --eval-every-epochs <n>   Run held-out loss every N complete epochs (default: 1)
        \\  --eval-batch-size <n>     Logical held-out batch size (default: 8; no drop-last)
        \\  --early-stopping-patience <n>
        \\                            Stop after N non-improving evals (default: 0 = disabled)
        \\  --early-stopping-threshold <f>
        \\                            Required eval-loss decrease (default: 0)
        \\  --max-grad-norm <f>       Gradient clipping norm (default: 1.0)
        \\  --grad-accum <n>          Gradient accumulation steps (default: 1)
        \\  --seed <n>                RNG seed (default: 42)
        \\  --initial-adapter-checkpoint <path> Seed LoRA weights from a PEFT safetensors checkpoint
        \\  --backend <name>          auto, metal, or native (default: auto)
        \\  --compiled-required       Fail if the requested compiled backend cannot run
        \\  --lora-only-trainables    Freeze regular task-head params; train LoRA params only
        \\  --deterministic           Disable per-step stochastic regularization (forces lora-dropout=0
        \\                            and span-negative-mask-rate=0; prints a warning when overriding)
        \\                            and pin gliner2-total-loss training data order (no epoch shuffle)
        \\  --allow-large-memory      Proceed even when the estimated peak memory exceeds the safe
        \\                            budget (~60% of physical RAM); risks a system-wide OOM
        \\  --activation-checkpointing Recompute non-checkpoint activations during backward to lower
        \\                            Metal peak memory for large batch/seq runs
        \\  --activation-checkpoint-interval <n>
        \\                            Save every Nth checkpoint boundary (default: 1)
        \\  --activation-checkpoint-strategy <name>
        \\                            every-n-layers, attention-outputs, or parameters-only
        \\                            (default: every-n-layers)
        \\  --structure-span-chunk-samples <n>
        \\                            Chunk GLiNER2 structure span loss by whole-sample groups
        \\                            to reduce large span-head tensors (0 = disabled)
        \\  --graph-cache-capacity <n>
        \\                            Retain at most N shape-specialized graphs, active included
        \\                            (1..8, default: 2; inactive runtime inputs are released)
        \\  --report-to <name>        stdout or jsonl; jsonl emits one JSON object per step/eval to stdout
        \\  --dump-span-parity        Print first span-start batch logits/label/mask BCE stats as JSON
        \\  --dump-optimizer-parity   Print per-step LoRA weight/Adam m/v state heads as JSON
        \\                            (GLINER2_OPT_PARITY lines, host optimizer paths only)
        \\
        \\notes:
        \\  Tokenization uses gliner2_data.Tokenizer.initGLiNER2HF and the
        \\  GLiNER2 prompt format backed by the model tokenizer files.
        \\
        \\example:
        \\  train-gliner2-autodiff --model-dir /models/deberta-v3-base \
        \\    --train-data /data/ner/train.jsonl --out-dir /output/lora \
        \\    --epochs 5 --batch-size 16 --num-classes 7 --learning-rate 2e-5
        \\  train-gliner2-autodiff --model-dir /models/gliner2 \
        \\    --train-data /data/ner/train.jsonl --out-dir /output/span-lora \
        \\    --objective span-start --max-span-width 4
        \\
    });
}

fn parseObjective(value: []const u8) !gliner2_autodiff.GlinerObjective {
    if (std.mem.eql(u8, value, "token")) return .token;
    if (std.mem.eql(u8, value, "span-start") or std.mem.eql(u8, value, "span_start")) return .span_start;
    if (std.mem.eql(u8, value, "gliner2-total-loss") or std.mem.eql(u8, value, "gliner2_total_loss")) return .gliner2_total_loss;
    print("error: unsupported --objective '{s}' (expected token, span-start, or gliner2-total-loss)\n", .{value});
    return error.InvalidObjective;
}

fn parseSpanLoss(value: []const u8) !gliner2_autodiff.SpanStartLossKind {
    if (std.mem.eql(u8, value, "bce") or std.mem.eql(u8, value, "binary-cross-entropy")) return .bce;
    if (std.mem.eql(u8, value, "mse")) return .mse;
    print("error: unsupported --span-loss '{s}' (expected bce or mse)\n", .{value});
    return error.InvalidSpanLoss;
}

fn parseSpanLossReduction(value: []const u8) !gliner2_autodiff.SpanStartLossReduction {
    if (std.mem.eql(u8, value, "mean")) return .mean;
    if (std.mem.eql(u8, value, "sum")) return .sum;
    print("error: unsupported --span-loss-reduction '{s}' (expected mean or sum)\n", .{value});
    return error.InvalidSpanLossReduction;
}

fn resolveSpanLabelPositiveWeights(
    allocator: std.mem.Allocator,
    csv: ?[]const u8,
    entity_labels: []const []const u8,
    default_weight: f32,
) ![]f32 {
    const weights = try allocator.alloc(f32, entity_labels.len);
    errdefer allocator.free(weights);
    @memset(weights, default_weight);
    if (csv == null) return weights;

    var seen = try allocator.alloc(bool, entity_labels.len);
    defer allocator.free(seen);
    @memset(seen, false);

    var iter = std.mem.splitScalar(u8, csv.?, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        const eq_idx = std.mem.indexOfScalar(u8, item, '=') orelse return error.InvalidSpanLabelPositiveWeights;
        const label = std.mem.trim(u8, item[0..eq_idx], " \t\r\n");
        const value_text = std.mem.trim(u8, item[eq_idx + 1 ..], " \t\r\n");
        if (label.len == 0 or value_text.len == 0) return error.InvalidSpanLabelPositiveWeights;
        const label_idx = indexOfEntityLabel(entity_labels, label) orelse
            return error.UnknownSpanLabelPositiveWeight;
        if (seen[label_idx]) return error.DuplicateSpanLabelPositiveWeight;
        const weight = try std.fmt.parseFloat(f32, value_text);
        if (!std.math.isFinite(weight) or weight <= 0.0) return error.InvalidSpanPositiveWeight;
        weights[label_idx] = weight;
        seen[label_idx] = true;
    }
    return weights;
}

fn parseEntityTypesCsvOwned(allocator: std.mem.Allocator, csv: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }

    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        for (out.items) |existing| {
            if (std.mem.eql(u8, existing, item)) return error.DuplicateEntityType;
        }
        try out.append(allocator, try allocator.dupe(u8, item));
    }
    if (out.items.len == 0) return error.NoEntityTypesProvided;
    return out.toOwnedSlice(allocator);
}

fn dupeStringSlice(allocator: std.mem.Allocator, items: []const []const u8) ![][]const u8 {
    const out = try allocator.alloc([]const u8, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| allocator.free(item);
        allocator.free(out);
    }
    for (items, 0..) |item, idx| {
        out[idx] = try allocator.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn deriveExamplesFromTrainingRecords(
    allocator: std.mem.Allocator,
    records: []const gliner2_data.UpstreamRecord,
) ![]gliner2_data.Example {
    var examples = std.ArrayListUnmanaged(gliner2_data.Example).empty;
    errdefer {
        for (examples.items) |example| allocator.free(example.entities);
        examples.deinit(allocator);
    }
    try examples.ensureTotalCapacity(allocator, records.len);
    for (records) |record| {
        var entity_count: usize = 0;
        for (record.tasks) |task| {
            if (task.kind == .classifications) continue;
            for (task.fields) |field| {
                if (field.start == null and field.end == null) continue;
                const start = field.start orelse return error.InvalidGliner2Example;
                const end = field.end orelse return error.InvalidGliner2Example;
                if (start >= end or end > record.text.len) return error.InvalidGliner2Example;
                entity_count += 1;
            }
        }
        const entities = try allocator.alloc(gliner2_data.Entity, entity_count);
        var entity_idx: usize = 0;
        for (record.tasks) |task| {
            if (task.kind == .classifications) continue;
            for (task.fields) |field| {
                if (field.start == null and field.end == null) continue;
                const start = field.start.?;
                const end = field.end.?;
                entities[entity_idx] = .{
                    .text = record.text[start..end],
                    .label = field.name,
                    .start = start,
                    .end = end,
                };
                entity_idx += 1;
            }
        }
        examples.appendAssumeCapacity(.{ .text = record.text, .entities = entities });
    }
    return examples.toOwnedSlice(allocator);
}

fn freeDerivedExamples(allocator: std.mem.Allocator, examples: []gliner2_data.Example) void {
    for (examples) |example| allocator.free(example.entities);
    allocator.free(examples);
}

fn ensureDisjointExampleTexts(
    allocator: std.mem.Allocator,
    train_examples: []const gliner2_data.Example,
    eval_examples: []const gliner2_data.Example,
) !void {
    var train_texts = std.StringHashMapUnmanaged(void){};
    defer train_texts.deinit(allocator);
    for (train_examples) |example| try train_texts.put(allocator, example.text, {});
    for (eval_examples) |example| {
        if (train_texts.contains(example.text)) return error.TrainEvalDataNotDisjoint;
    }
}

fn ensureEvalLabelsKnown(
    eval_examples: []const gliner2_data.Example,
    label_map: *const std.StringHashMapUnmanaged(u32),
) !void {
    for (eval_examples) |example| {
        for (example.entities) |entity| {
            if (!label_map.contains(entity.label)) return error.UnknownEvalEntityLabel;
        }
    }
}

fn shuffleExamplesAndRecords(prng: *std.Random, examples: []gliner2_data.Example, records: []gliner2_data.UpstreamRecord) void {
    if (examples.len != records.len) return;
    var i = examples.len;
    while (i > 1) {
        i -= 1;
        const j = prng.intRangeLessThan(usize, 0, i + 1);
        std.mem.swap(gliner2_data.Example, &examples[i], &examples[j]);
        std.mem.swap(gliner2_data.UpstreamRecord, &records[i], &records[j]);
    }
}

fn replayCompletedEpochShuffles(
    prng: *std.Random,
    opts: Options,
    examples: []gliner2_data.Example,
    records: []gliner2_data.UpstreamRecord,
    completed_epochs: u64,
) void {
    var epoch: u64 = 0;
    while (epoch < completed_epochs) : (epoch += 1) {
        if (opts.objective == .gliner2_total_loss) {
            if (!opts.dump_span_parity and !opts.deterministic) shuffleExamplesAndRecords(prng, examples, records);
        } else {
            prng.shuffle(gliner2_data.Example, examples);
        }
    }
}

fn bestCheckpointRelativePathAlloc(allocator: std.mem.Allocator, epoch: u64) ![]const u8 {
    if (epoch == 0) return error.InvalidBestCheckpointEpoch;
    return std.fmt.allocPrint(allocator, "checkpoints/best-epoch-{d}.safetensors", .{epoch});
}

fn bestCheckpointPathAlloc(allocator: std.mem.Allocator, checkpoint_dir: []const u8, epoch: u64) ![]const u8 {
    if (epoch == 0) return error.InvalidBestCheckpointEpoch;
    return std.fmt.allocPrint(allocator, "{s}/best-epoch-{d}.safetensors", .{ checkpoint_dir, epoch });
}

fn retainedResumeCheckpointRelativePathAlloc(allocator: std.mem.Allocator, digest: []const u8) ![]const u8 {
    const prefix = "sha256:";
    if (digest.len != prefix.len + 64 or !std.mem.startsWith(u8, digest, prefix)) {
        return error.InvalidResumeCheckpointDigest;
    }
    return std.fmt.allocPrint(allocator, "checkpoints/resume-source-{s}.safetensors", .{digest[prefix.len..]});
}

fn retainResumeCheckpoint(
    allocator: std.mem.Allocator,
    source_path: []const u8,
    retained_path: []const u8,
    expected_sha256: []const u8,
) !void {
    if (!std.mem.eql(u8, source_path, retained_path)) {
        try std.Io.Dir.copyFile(
            compat.cwd(),
            source_path,
            compat.cwd(),
            retained_path,
            compat.io(),
            .{ .make_path = true, .replace = true },
        );
        var retained_file = try compat.cwd().openFile(compat.io(), retained_path, .{});
        defer retained_file.close(compat.io());
        try retained_file.sync(compat.io());
        if (std.fs.path.dirname(retained_path)) |dir_path| try syncDirectoryChain(dir_path);
    }
    const retained_sha256 = (try sha256RegularFileAlloc(allocator, retained_path)) orelse
        return error.ResumeCheckpointNotRegular;
    defer allocator.free(retained_sha256);
    if (!std.mem.eql(u8, retained_sha256, expected_sha256)) return error.RetainedResumeCheckpointDigestMismatch;
}

fn prunePeriodicCheckpoints(allocator: std.mem.Allocator, checkpoint_dir: []const u8, current_epoch: u64, keep_last: u32) !void {
    return pruneEpochCheckpoints(allocator, checkpoint_dir, "epoch-", current_epoch, keep_last);
}

fn pruneBestCheckpointsAfter(allocator: std.mem.Allocator, checkpoint_dir: []const u8, current_epoch: u64) !void {
    return pruneEpochCheckpoints(allocator, checkpoint_dir, "best-epoch-", current_epoch, 0);
}

fn pruneEpochCheckpoints(
    allocator: std.mem.Allocator,
    checkpoint_dir: []const u8,
    prefix: []const u8,
    current_epoch: u64,
    keep_last: u32,
) !void {
    var dir = compat.cwd().openDir(compat.io(), checkpoint_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer dir.close(compat.io());
    var epochs = std.ArrayListUnmanaged(u64).empty;
    defer epochs.deinit(allocator);
    var iterator = dir.iterate();
    while (try iterator.next(compat.io())) |entry| {
        if (entry.kind != .file or
            !std.mem.startsWith(u8, entry.name, prefix) or
            !std.mem.endsWith(u8, entry.name, ".safetensors"))
        {
            continue;
        }
        const number = entry.name[prefix.len .. entry.name.len - ".safetensors".len];
        const epoch = std.fmt.parseUnsigned(u64, number, 10) catch continue;
        const canonical_name = try std.fmt.allocPrint(allocator, "{s}{d}.safetensors", .{ prefix, epoch });
        defer allocator.free(canonical_name);
        if (!std.mem.eql(u8, entry.name, canonical_name)) continue;
        try epochs.append(allocator, epoch);
    }
    std.mem.sort(u64, epochs.items, {}, std.sort.asc(u64));
    var retained_end: usize = epochs.items.len;
    while (retained_end > 0 and epochs.items[retained_end - 1] > current_epoch) : (retained_end -= 1) {
        const epoch = epochs.items[retained_end - 1];
        const name = try std.fmt.allocPrint(allocator, "{s}{d}.safetensors", .{ prefix, epoch });
        defer allocator.free(name);
        try dir.deleteFile(compat.io(), name);
    }
    if (keep_last == 0) return;
    const keep: usize = @intCast(keep_last);
    if (retained_end <= keep) return;
    for (epochs.items[0 .. retained_end - keep]) |epoch| {
        const name = try std.fmt.allocPrint(allocator, "{s}{d}.safetensors", .{ prefix, epoch });
        defer allocator.free(name);
        try dir.deleteFile(compat.io(), name);
    }
}

test "resume metrics restore checkpoint prefix and cumulative state" {
    const metrics =
        "{\"event\":\"step\",\"epoch\":1,\"step\":1,\"epoch_step\":1,\"optimizer_step\":0,\"loss\":2.5,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":2,\"schema_slot_positive_counts\":[1,0]}\n" ++
        "{\"event\":\"optimizer_step\",\"epoch\":1,\"step\":1,\"optimizer_step\":1}\n" ++
        "{\"event\":\"epoch\",\"epoch\":1,\"steps\":1}\n" ++
        "{\"event\":\"step\",\"epoch\":2,\"step\":2,\"epoch_step\":1,\"optimizer_step\":2,\"loss\":1.5,\"supervised_token_count\":3,\"entity_token_count\":1,\"ignored_token_count\":1,\"schema_slot_positive_counts\":[0,1]}\n" ++
        "{\"event\":\"epoch\",\"epoch\":2,\"steps\":1}\n";
    const first_epoch_end = std.mem.indexOf(u8, metrics, "{\"event\":\"step\",\"epoch\":2") orelse unreachable;
    const restored = try inspectResumeMetricsJsonl(std.testing.allocator, metrics, 1, 1, 1, 2, 0, 0.0);
    try std.testing.expectEqual(@as(u64, 1), restored.step_count);
    try std.testing.expectEqual(@as(u64, 1), restored.epoch_count);
    try std.testing.expectEqual(@as(u64, 1), restored.max_optimizer_step);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), restored.loss_sum, 1e-12);
    try std.testing.expectEqual(@as(u64, 4), restored.target_stats.supervised_token_count);
    try std.testing.expectEqual(@as(u64, 1), restored.target_stats.positive_counts_by_entity_type[0]);
    try std.testing.expectEqual(@as(u64, @intCast(first_epoch_end)), restored.prefix_len);
}

test "resume metrics reject state counters that do not match checkpoint" {
    const metrics =
        "{\"event\":\"step\",\"epoch\":1,\"step\":1,\"epoch_step\":1,\"optimizer_step\":1,\"loss\":2.5,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":2,\"entity_label_positive_counts\":[1]}\n" ++
        "{\"event\":\"epoch\",\"epoch\":1,\"steps\":1}\n";
    try std.testing.expectError(
        error.TrainingMetricsCheckpointMismatch,
        inspectResumeMetricsJsonl(std.testing.allocator, metrics, 2, 1, 1, 1, 0, 0.0),
    );
}

test "eval selection honors threshold patience and resume metrics" {
    var state = EvalSelectionState{};
    try std.testing.expect(try state.observe(1, 2.0, 0.1));
    // Upstream saves every strict new best, while patience only resets when
    // the decrease also clears the configured threshold.
    try std.testing.expect(try state.observe(2, 1.95, 0.1));
    try std.testing.expect(state.shouldStop(1));
    try std.testing.expect(resumeStopsBeforeTraining(true, state, 1));
    try std.testing.expect(!resumeStopsBeforeTraining(false, state, 1));
    try std.testing.expect(try state.observe(3, 1.8, 0.1));
    try std.testing.expectEqual(@as(u64, 3), state.best_epoch);
    try std.testing.expectEqual(@as(u32, 0), state.bad_epochs);

    const metrics =
        "{\"event\":\"step\",\"epoch\":1,\"step\":1,\"epoch_step\":1,\"optimizer_step\":1,\"loss\":2.5,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":2,\"schema_slot_positive_counts\":[1]}\n" ++
        "{\"event\":\"eval\",\"epoch\":1,\"eval_loss\":2.0,\"is_best\":true}\n" ++
        "{\"event\":\"epoch\",\"epoch\":1,\"steps\":1}\n" ++
        "{\"event\":\"step\",\"epoch\":2,\"step\":2,\"epoch_step\":1,\"optimizer_step\":2,\"loss\":1.5,\"supervised_token_count\":3,\"entity_token_count\":1,\"ignored_token_count\":1,\"schema_slot_positive_counts\":[1]}\n" ++
        "{\"event\":\"eval\",\"epoch\":2,\"eval_loss\":1.95,\"is_best\":true}\n" ++
        "{\"event\":\"epoch\",\"epoch\":2,\"steps\":1}\n";
    const restored = try inspectResumeMetricsJsonl(std.testing.allocator, metrics, 2, 2, 2, 1, 1, 0.1);
    try std.testing.expectEqual(@as(u64, 2), restored.eval_state.eval_count);
    try std.testing.expectEqual(@as(u64, 2), restored.eval_state.best_epoch);
    try std.testing.expectEqual(@as(u32, 1), restored.eval_state.bad_epochs);
    try std.testing.expectEqual(@as(f64, 1.95), restored.eval_state.best_loss.?);
    try std.testing.expectEqual(@as(u64, metrics.len), restored.best_metrics_prefix_len);
}

test "total-loss eval preserves upstream logical batch sums across physical shapes" {
    const example_losses = [_]f32{ 1, 2, 3, 4, 5 };
    const logical_batch_size: usize = 2;
    for ([_]usize{ 1, 2, 4 }) |physical_batch_size| {
        var logical_start: usize = 0;
        var batch_loss_sum: f64 = 0.0;
        var logical_batches: usize = 0;
        while (logical_start < example_losses.len) {
            const logical_end = @min(example_losses.len, logical_start + logical_batch_size);
            var contribution_sum: f64 = 0.0;
            var start = logical_start;
            while (start + physical_batch_size <= logical_end) : (start += physical_batch_size) {
                var reported_loss: f32 = 0.0;
                for (example_losses[start .. start + physical_batch_size]) |loss| reported_loss += loss;
                contribution_sum += physicalEvalLossContribution(.gliner2_total_loss, reported_loss, physical_batch_size, false);
            }
            while (start < logical_end) : (start += 1) {
                const replicated_reported_loss = example_losses[start] * @as(f32, @floatFromInt(physical_batch_size));
                contribution_sum += physicalEvalLossContribution(.gliner2_total_loss, replicated_reported_loss, physical_batch_size, true);
            }
            batch_loss_sum += logicalEvalBatchLoss(.gliner2_total_loss, contribution_sum, logical_end - logical_start);
            logical_batches += 1;
            logical_start = logical_end;
        }
        // Upstream averages the logical batch sums: (1+2 + 3+4 + 5) / 3.
        try std.testing.expectApproxEqAbs(@as(f64, 5.0), batch_loss_sum / @as(f64, @floatFromInt(logical_batches)), 1e-12);
    }
}

test "held-out examples reject training text overlap and unknown labels" {
    var no_entities: [0]gliner2_data.Entity = .{};
    const train = [_]gliner2_data.Example{.{ .text = "same", .entities = &no_entities }};
    const eval = [_]gliner2_data.Example{.{ .text = "same", .entities = &no_entities }};
    try std.testing.expectError(
        error.TrainEvalDataNotDisjoint,
        ensureDisjointExampleTexts(std.testing.allocator, &train, &eval),
    );

    var labels = std.StringHashMapUnmanaged(u32){};
    defer labels.deinit(std.testing.allocator);
    try labels.put(std.testing.allocator, "person", 1);
    var entities = [_]gliner2_data.Entity{.{ .text = "Acme", .start = 0, .end = 4, .label = "company" }};
    const unknown = [_]gliner2_data.Example{.{ .text = "Acme", .entities = &entities }};
    try std.testing.expectError(error.UnknownEvalEntityLabel, ensureEvalLabelsKnown(&unknown, &labels));
}

test "periodic checkpoint retention keeps newest canonical files" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{
        "epoch-1.safetensors",
        "epoch-3.safetensors",
        "epoch-10.safetensors",
        "epoch-03.safetensors",
        "notes.txt",
    }) |name| {
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = "x" });
    }
    const root = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer std.testing.allocator.free(root);
    try prunePeriodicCheckpoints(std.testing.allocator, root, 10, 2);
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "epoch-1.safetensors", .{}));
    _ = try tmp.dir.statFile(std.testing.io, "epoch-3.safetensors", .{});
    _ = try tmp.dir.statFile(std.testing.io, "epoch-10.safetensors", .{});
    _ = try tmp.dir.statFile(std.testing.io, "epoch-03.safetensors", .{});
    _ = try tmp.dir.statFile(std.testing.io, "notes.txt", .{});
}

test "periodic checkpoint retention removes abandoned future epochs" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "epoch-1.safetensors", .data = "old" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "epoch-3.safetensors", .data = "current" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "epoch-8.safetensors", .data = "stale" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "epoch-9.safetensors", .data = "stale" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "epoch-10.safetensors", .data = "stale" });
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    try prunePeriodicCheckpoints(std.testing.allocator, root, 3, 2);
    _ = try tmp.dir.statFile(std.testing.io, "epoch-1.safetensors", .{});
    _ = try tmp.dir.statFile(std.testing.io, "epoch-3.safetensors", .{});
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "epoch-8.safetensors", .{}));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "epoch-9.safetensors", .{}));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "epoch-10.safetensors", .{}));
}

test "best checkpoint history remains resumable while abandoned future bests are removed" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "best-epoch-1.safetensors", .data = "best one" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "best-epoch-2.safetensors", .data = "best two" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "best-epoch-3.safetensors", .data = "future" });
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    try pruneBestCheckpointsAfter(std.testing.allocator, root, 2);
    _ = try tmp.dir.statFile(std.testing.io, "best-epoch-1.safetensors", .{});
    _ = try tmp.dir.statFile(std.testing.io, "best-epoch-2.safetensors", .{});
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "best-epoch-3.safetensors", .{}));
}

test "resume source is retained under its content digest" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "epoch-1.safetensors", .data = "recoverable state" });
    const root = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(root);
    const source_path = try std.fs.path.join(allocator, &.{ root, "epoch-1.safetensors" });
    defer allocator.free(source_path);
    const source_sha256 = (try sha256RegularFileAlloc(allocator, source_path)).?;
    defer allocator.free(source_sha256);
    const retained_relative_path = try retainedResumeCheckpointRelativePathAlloc(allocator, source_sha256);
    defer allocator.free(retained_relative_path);
    const retained_path = try std.fs.path.join(allocator, &.{ root, retained_relative_path });
    defer allocator.free(retained_path);

    try retainResumeCheckpoint(allocator, source_path, retained_path, source_sha256);
    const retained_sha256 = (try sha256RegularFileAlloc(allocator, retained_path)).?;
    defer allocator.free(retained_sha256);
    try std.testing.expectEqualStrings(source_sha256, retained_sha256);
}

test "optimizer step planning includes partial accumulation" {
    try std.testing.expectEqual(@as(u64, 1), optimizerStepsForMicroBatches(1, 2));
    try std.testing.expectEqual(@as(u64, 1), optimizerStepsForMicroBatches(2, 2));
    try std.testing.expectEqual(@as(u64, 2), optimizerStepsForMicroBatches(3, 2));
    try std.testing.expectEqual(@as(u64, 3), optimizerStepsForMicroBatches(5, 2));
    // Upstream floor-based schedule horizon: floor(micro/accum) clamped >= 1.
    try std.testing.expectEqual(@as(u64, 1), scheduleStepsForMicroBatches(1, 2));
    try std.testing.expectEqual(@as(u64, 1), scheduleStepsForMicroBatches(2, 2));
    try std.testing.expectEqual(@as(u64, 1), scheduleStepsForMicroBatches(3, 2));
    try std.testing.expectEqual(@as(u64, 2), scheduleStepsForMicroBatches(5, 2));
    try std.testing.expectEqual(@as(u64, 1), scheduleStepsForMicroBatches(2, 5));
}

test "training state fingerprint binds label ordering and execution settings" {
    const patterns = [_][]const u8{"encoder"};
    const labels = [_][]const u8{ "person", "organization" };
    const reordered_labels = [_][]const u8{ "organization", "person" };
    const regular_params = [_][]const u8{"task_classifier.bias"};
    const weights = [_]f32{ 1.0, 2.0 };
    const opts = Options{
        .model_dir = "model",
        .train_data = "train.jsonl",
        .out_dir = "out",
        .objective = .span_start,
    };
    const base = try trainingStateFingerprint(
        std.testing.allocator,
        opts,
        "native",
        "sha256:train",
        null,
        "sha256:model",
        "sha256:executable",
        "runtime:native",
        &patterns,
        &labels,
        &labels,
        &regular_params,
        &weights,
        64,
        2,
        3,
        1,
        1,
        4,
        2,
        0,
        null,
    );
    const reordered = try trainingStateFingerprint(
        std.testing.allocator,
        opts,
        "native",
        "sha256:train",
        null,
        "sha256:model",
        "sha256:executable",
        "runtime:native",
        &patterns,
        &reordered_labels,
        &reordered_labels,
        &regular_params,
        &weights,
        64,
        2,
        3,
        1,
        1,
        4,
        2,
        0,
        null,
    );
    try std.testing.expect(!std.mem.eql(u8, base[0..], reordered[0..]));

    var weighted_opts = opts;
    weighted_opts.span_label_positive_weights = "person=1,organization=2";
    const weighted = try trainingStateFingerprint(
        std.testing.allocator,
        weighted_opts,
        "native",
        "sha256:train",
        null,
        "sha256:model",
        "sha256:executable",
        "runtime:native",
        &patterns,
        &labels,
        &labels,
        &regular_params,
        &weights,
        64,
        2,
        3,
        1,
        1,
        4,
        2,
        0,
        null,
    );
    try std.testing.expect(!std.mem.eql(u8, base[0..], weighted[0..]));

    var dump_opts = opts;
    dump_opts.dump_span_parity = true;
    const dump = try trainingStateFingerprint(
        std.testing.allocator,
        dump_opts,
        "native",
        "sha256:train",
        null,
        "sha256:model",
        "sha256:executable",
        "runtime:native",
        &patterns,
        &labels,
        &labels,
        &regular_params,
        &weights,
        64,
        2,
        3,
        1,
        1,
        4,
        2,
        0,
        null,
    );
    try std.testing.expect(!std.mem.eql(u8, base[0..], dump[0..]));

    const checkpointed = try trainingStateFingerprint(
        std.testing.allocator,
        opts,
        "native",
        "sha256:train",
        null,
        "sha256:model",
        "sha256:executable",
        "runtime:native",
        &patterns,
        &labels,
        &labels,
        &regular_params,
        &weights,
        64,
        2,
        3,
        1,
        1,
        4,
        2,
        0,
        .{ .strategy = .parameters_only, .layer_interval = 2 },
    );
    try std.testing.expect(!std.mem.eql(u8, base[0..], checkpointed[0..]));
}

/// Activation (gradient) checkpointing config, env-gated. Enable with
/// `TERMITE_GLINER2_ACTIVATION_CHECKPOINTING=1` to recompute non-checkpoint
/// forward activations during backward instead of keeping them live — bounds
/// peak activation memory (trades ~1.3-2x forward compute for memory), needed
/// for large batch/seq where the full activation tape OOMs the GPU. Optional
/// `TERMITE_GLINER2_CHECKPOINT_INTERVAL=N` (default 1) saves every N layers.
/// `TERMITE_GLINER2_CHECKPOINT_STRATEGY=parameters-only` keeps only roots/loss
/// and recomputes forward activations needed by backward.
fn activationCheckpointConfig(
    cli_enabled: bool,
    cli_interval: u32,
    cli_strategy: ml.graph.checkpoint.CheckpointStrategy,
) ?ml.graph.checkpoint.CheckpointConfig {
    const env_enabled = blk: {
        const cstr = std.c.getenv("TERMITE_GLINER2_ACTIVATION_CHECKPOINTING") orelse break :blk false;
        const val = std.mem.span(cstr);
        break :blk val.len > 0 and val[0] != '0';
    };
    if (!cli_enabled and !env_enabled) return null;
    var interval: u32 = if (cli_interval >= 1) cli_interval else 1;
    if (std.c.getenv("TERMITE_GLINER2_CHECKPOINT_INTERVAL")) |ic| {
        if (std.fmt.parseInt(u32, std.mem.trim(u8, std.mem.span(ic), " \t\r\n"), 10)) |n| {
            if (n >= 1) interval = n;
        } else |_| {}
    }
    var strategy = cli_strategy;
    if (std.c.getenv("TERMITE_GLINER2_CHECKPOINT_STRATEGY")) |sc| {
        const trimmed = std.mem.trim(u8, std.mem.span(sc), " \t\r\n");
        if (parseCheckpointStrategy(trimmed)) |parsed| {
            strategy = parsed;
        } else {
            print("warning: ignoring unsupported TERMITE_GLINER2_CHECKPOINT_STRATEGY='{s}'\n", .{trimmed});
        }
    }
    print("  activation checkpointing: ON (strategy={s} interval={d})\n", .{ checkpointStrategyName(strategy), interval });
    return .{ .strategy = strategy, .layer_interval = interval };
}

/// Max structure `gold_count` (capped at upstream's 19) across all
/// json_structures / relations tasks in the dataset. Drives
/// `structure_max_instances`: `1` keeps the legacy single-instance structure
/// loss; `>1` activates the per-instance count-conditioned einsum.
fn computeMaxStructureInstances(records: []const gliner2_data.UpstreamRecord) u32 {
    var max_instances: u32 = 1;
    for (records) |record| {
        for (record.tasks) |task| {
            switch (task.kind) {
                .json_structures, .relations => {
                    const c: u32 = @intCast(@min(task.count, @as(usize, 19)));
                    if (c > max_instances) max_instances = c;
                },
                else => {},
            }
        }
    }
    return max_instances;
}

fn computeMaxSchemaTasks(records: []const gliner2_data.UpstreamRecord) u32 {
    var max_tasks: u32 = 1;
    for (records) |record| {
        const count: u32 = @intCast(@max(record.tasks.len, @as(usize, 1)));
        if (count > max_tasks) max_tasks = count;
    }
    return max_tasks;
}

const TotalLossDatasetAxes = struct {
    schema_slots: usize,
    structure_instances: u32,
    schema_tasks: u32,
};

fn computeTotalLossDatasetAxes(
    allocator: std.mem.Allocator,
    training_records: []const gliner2_data.UpstreamRecord,
    eval_records: []const gliner2_data.UpstreamRecord,
) !TotalLossDatasetAxes {
    return .{
        .schema_slots = @max(
            try gliner2_data.maxUpstreamRecordLabelSlots(allocator, training_records),
            try gliner2_data.maxUpstreamRecordLabelSlots(allocator, eval_records),
        ),
        .structure_instances = @max(
            computeMaxStructureInstances(training_records),
            computeMaxStructureInstances(eval_records),
        ),
        .schema_tasks = @max(
            computeMaxSchemaTasks(training_records),
            computeMaxSchemaTasks(eval_records),
        ),
    };
}

const TotalLossGraphLimits = struct {
    seq_len: usize,
    schema_slots: usize,
    structure_instances: u32,
    schema_tasks: u32,
    max_span_width: u32,
};

const TotalLossBatchShape = struct {
    seq_len: usize,
    schema_slots: usize,
    structure_instances: u32,
    schema_tasks: u32,

    fn apply(self: TotalLossBatchShape, ctx: *gliner2_autodiff.GlinerAutodiffCtx) void {
        ctx.config.num_classes = @intCast(self.schema_slots + 1);
        ctx.config.structure_max_instances = self.structure_instances;
        ctx.config.max_schema_tasks = self.schema_tasks;
    }
};

fn totalLossBatchShape(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    records: []const gliner2_data.UpstreamRecord,
    limits: TotalLossGraphLimits,
) !TotalLossBatchShape {
    if (records.len == 0) return error.InvalidGlinerBatchShape;
    var required_seq_len: usize = 0;
    for (records) |record| {
        required_seq_len = @max(required_seq_len, try gliner2_data.measureUpstreamRecordEncodedLength(
            allocator,
            tokenizer,
            record,
            limits.seq_len,
        ));
    }
    const required_slots = try gliner2_data.maxUpstreamRecordLabelSlots(allocator, records);
    if (required_slots == 0 or required_slots > limits.schema_slots) return error.TooManySchemaSlots;
    const required_instances = computeMaxStructureInstances(records);
    if (required_instances > limits.structure_instances) return error.StructureExceedsTrainingShape;
    const required_tasks = computeMaxSchemaTasks(records);
    if (required_tasks > limits.schema_tasks) return error.TasksExceedTrainingShape;
    return bucketTotalLossShape(required_seq_len, required_slots, required_instances, required_tasks, limits);
}

fn bucketTotalLossShape(
    required_seq_len: usize,
    required_slots: usize,
    required_instances: u32,
    required_tasks: u32,
    limits: TotalLossGraphLimits,
) TotalLossBatchShape {
    return .{
        .seq_len = @min(limits.seq_len, roundUpMultiple(@max(required_seq_len, @as(usize, limits.max_span_width) + 8), 8)),
        .schema_slots = boundedPowerOfTwoBucket(required_slots, limits.schema_slots),
        .structure_instances = @intCast(boundedPowerOfTwoBucket(required_instances, limits.structure_instances)),
        .schema_tasks = @intCast(boundedPowerOfTwoBucket(required_tasks, limits.schema_tasks)),
    };
}

fn roundUpMultiple(value: usize, multiple: usize) usize {
    std.debug.assert(multiple > 0);
    return (value + multiple - 1) / multiple * multiple;
}

fn boundedPowerOfTwoBucket(value: anytype, cap: @TypeOf(value)) @TypeOf(value) {
    std.debug.assert(value > 0 and cap >= value);
    const bucket = std.math.ceilPowerOfTwo(@TypeOf(value), value) catch cap;
    return @min(bucket, cap);
}

fn fillGliner2TotalLossTargetsFromRecords(
    allocator: std.mem.Allocator,
    encoded: *const gliner2_data.EncodedBatch,
    records: []const gliner2_data.UpstreamRecord,
    max_instances: u32,
    out: []f32,
) !gliner2_autodiff.SpanStartTargetStats {
    if (records.len != encoded.batch_size) return error.InvalidGlinerBatchShape;
    const E = encoded.num_entity_types;
    const total_width = gliner2_autodiff.gliner2TotalLossTargetWidthEx(E, max_instances);
    const rows = encoded.batch_size * encoded.max_spans;
    if (out.len != rows * total_width) return error.InvalidGlinerSpanTargetShape;
    @memset(out, 0.0);

    var stats = gliner2_autodiff.SpanStartTargetStats{ .entity_type_count = E };

    const cls_labels_offset = gliner2_autodiff.gliner2TotalLossClassificationLabelsOffset(E);
    const cls_mask_offset = gliner2_autodiff.gliner2TotalLossClassificationMaskOffset(E);
    const count_labels_offset = gliner2_autodiff.gliner2TotalLossCountLabelsOffset(E);
    const count_mask_offset = gliner2_autodiff.gliner2TotalLossCountMaskOffset(E);
    const parent_idx_offset = gliner2_autodiff.gliner2TotalLossParentIndexOffset(E);
    const task_groups_offset = gliner2_autodiff.gliner2TotalLossTaskGroupsOffset(E, max_instances);
    const schema_idx_offset = 2 * E;
    const row_idx_offset = schema_idx_offset + E;
    const count_idx_offset = row_idx_offset + E;
    const start_idx_offset = count_idx_offset + E;

    for (records, 0..) |record, sample_idx| {
        const entity_labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, &.{record}, null);
        defer {
            for (entity_labels) |label| allocator.free(label);
            allocator.free(entity_labels);
        }
        if (entity_labels.len > E) return error.TooManySchemaSlots;
        const task_groups = try allocator.alloc(f32, E);
        defer allocator.free(task_groups);
        try fillGliner2StructureTaskGroups(allocator, record, entity_labels, task_groups);
        const word_pos_offset = sample_idx * encoded.max_words_per_sample;
        const schema_count = if (encoded.schema_counts.len > sample_idx)
            @as(usize, @intCast(@max(encoded.schema_counts[sample_idx], 0)))
        else
            record.tasks.len;
        for (0..encoded.max_spans) |span_idx| {
            const flat_span_idx = sample_idx * encoded.max_spans + span_idx;
            const row = out[flat_span_idx * total_width ..][0..total_width];
            @memcpy(row[task_groups_offset .. task_groups_offset + E], task_groups);
            for (0..E) |entity_type_idx| {
                const label_token_pos_raw = encoded.e_token_positions[sample_idx * E + entity_type_idx];
                const label_token_pos: usize = if (label_token_pos_raw >= 0) @intCast(label_token_pos_raw) else 0;
                row[schema_idx_offset + entity_type_idx] = @floatFromInt(sample_idx * encoded.max_length + label_token_pos);
                row[row_idx_offset + entity_type_idx] = @floatFromInt(flat_span_idx);
                const count_state = if (encoded.entity_type_kind.len > sample_idx * E + entity_type_idx and encoded.entity_type_kind[sample_idx * E + entity_type_idx] > 0)
                    @as(usize, @intCast(@max(encoded.entity_type_kind[sample_idx * E + entity_type_idx] - 2, 0)))
                else
                    0;
                row[count_idx_offset + entity_type_idx] = @floatFromInt(count_state * encoded.batch_size * E + sample_idx * E + entity_type_idx);
            }

            if (encoded.span_mask[flat_span_idx] <= 0.0) {
                stats.ignored_span_count += 1;
                continue;
            }
            const start_word_raw = encoded.span_indices[flat_span_idx * 2];
            const end_word_raw = encoded.span_indices[flat_span_idx * 2 + 1];
            if (start_word_raw < 0 or end_word_raw < start_word_raw) {
                stats.ignored_span_count += 1;
                continue;
            }
            const start_word: usize = @intCast(start_word_raw);
            const end_word: usize = @intCast(end_word_raw);
            if (start_word >= encoded.max_words_per_sample or end_word >= encoded.max_words_per_sample) return error.InvalidSpanWordIndex;
            const start_token_raw = encoded.first_token_positions[word_pos_offset + start_word];
            const end_token_raw = encoded.first_token_positions[word_pos_offset + end_word];
            if (start_token_raw < 0 or end_token_raw < 0) {
                stats.ignored_span_count += 1;
                continue;
            }
            row[start_idx_offset] = @floatFromInt(sample_idx * encoded.max_length + @as(usize, @intCast(start_token_raw)));
            row[start_idx_offset + 1] = @floatFromInt(sample_idx * encoded.max_length + @as(usize, @intCast(end_token_raw)));
            stats.valid_span_count += 1;
            for (0..E) |entity_type_idx| {
                const active = encoded.entity_type_kind.len > sample_idx * E + entity_type_idx and encoded.entity_type_kind[sample_idx * E + entity_type_idx] > 1;
                if (!active) continue;
                const label = encoded.span_labels[flat_span_idx * E + entity_type_idx];
                row[entity_type_idx] = label;
                row[E + entity_type_idx] = 1.0;
                if (label > 0.0) {
                    stats.positive_span_label_count += 1;
                    stats.positive_counts_by_entity_type[entity_type_idx] += 1;
                }
            }
        }

        const task_limit = @min(record.tasks.len, encoded.max_spans);
        for (record.tasks[0..task_limit], 0..) |task, task_idx| {
            if (task_idx >= schema_count) break;
            const row_idx = sample_idx * encoded.max_spans + task_idx;
            const row = out[row_idx * total_width ..][0..total_width];
            row[parent_idx_offset] = @floatFromInt(sample_idx * encoded.max_length + upstreamSchemaParentTokenPosition(encoded, sample_idx, task_idx));
            switch (task.kind) {
                .classifications => {
                    for (task.labels) |label| {
                        const qualified_label = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, label);
                        defer allocator.free(qualified_label);
                        const label_idx = indexOfEntityLabel(entity_labels, qualified_label) orelse continue;
                        row[cls_mask_offset + label_idx] = 1.0;
                    }
                    for (task.true_labels) |label| {
                        const qualified_label = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, label);
                        defer allocator.free(qualified_label);
                        const label_idx = indexOfEntityLabel(entity_labels, qualified_label) orelse continue;
                        row[cls_labels_offset + label_idx] = 1.0;
                        row[cls_mask_offset + label_idx] = 1.0;
                    }
                },
                .entities => {},
                .json_structures, .relations => {
                    // Upstream `_compute_sample_loss` skips schemas whose
                    // structure count is zero entirely (no structure loss and
                    // no count loss), so only emit a count target for
                    // schemas with at least one gold instance.
                    if (task.count > 0) {
                        const count = @min(task.count, @as(usize, 19));
                        row[count_labels_offset + count] = 1.0;
                        row[count_mask_offset] = 1.0;
                    }
                },
            }
        }
    }

    // Direct per-instance structure labels and masks (only when activated).
    // They mirror upstream's [gold_count, fields, starts, widths] tensors so
    // repeated field values and stochastic negative masks remain independent.
    if (max_instances > 1) {
        const instance_labels_offset = gliner2_autodiff.gliner2TotalLossInstanceLabelsOffset(E);
        const instance_masks_offset = gliner2_autodiff.gliner2TotalLossInstanceMasksOffset(E, max_instances);
        const max_span_width = if (encoded.max_words_per_sample > 0)
            encoded.max_spans / encoded.max_words_per_sample
        else
            0;
        for (records, 0..) |record, sample_idx| {
            const entity_labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, &.{record}, null);
            defer {
                for (entity_labels) |label| allocator.free(label);
                allocator.free(entity_labels);
            }
            if (entity_labels.len > E) return error.TooManySchemaSlots;
            for (0..encoded.max_spans) |span_idx| {
                const flat = sample_idx * encoded.max_spans + span_idx;
                const row = out[flat * total_width ..][0..total_width];
                for (0..E) |label| {
                    const kind = encoded.entity_type_kind[sample_idx * E + label];
                    if (kind <= 1 or row[E + label] <= 0.0) continue;
                    const gold_count: usize = @intCast(@min(kind - 1, @as(i32, @intCast(max_instances))));
                    for (0..gold_count) |instance| {
                        row[instance_masks_offset + instance * E + label] = 1.0;
                    }
                }
            }

            const char_to_word = try gliner2_data.buildCharToWordMap(allocator, record.text);
            defer allocator.free(char_to_word);
            const prefix_word_count = record.prefix_tokens.len;
            for (record.tasks) |task| {
                if (task.kind == .classifications) continue;
                for (task.fields) |field| {
                    const span_idx = gliner2_data.locateUpstreamFieldSpanIdx(
                        field,
                        char_to_word,
                        prefix_word_count,
                        encoded.max_words_per_sample,
                        max_span_width,
                    ) orelse return error.AnnotationOutsideBatch;
                    const label_idx = (if (task.kind == .entities)
                        indexOfEntityLabel(entity_labels, field.name)
                    else blk: {
                        const label = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, field.name);
                        defer allocator.free(label);
                        break :blk indexOfEntityLabel(entity_labels, label);
                    }) orelse continue;
                    // Upstream caps count-conditioned structure instances at
                    // 19 and ignores later occurrences.
                    if (field.instance >= @as(usize, max_instances)) continue;
                    const flat = sample_idx * encoded.max_spans + span_idx;
                    out[flat * total_width + instance_labels_offset + field.instance * E + label_idx] = 1.0;
                }
            }
        }
    }

    return stats;
}

fn fillGliner2StructureTaskGroups(
    allocator: std.mem.Allocator,
    record: gliner2_data.UpstreamRecord,
    entity_labels: []const []const u8,
    out: []f32,
) !void {
    if (entity_labels.len > out.len) return error.InvalidGlinerSpanTargetShape;
    @memset(out, 0.0);
    for (record.tasks, 0..) |task, task_idx| {
        if (task.kind == .classifications) continue;
        if (task.schema_fields.len > 0) {
            for (task.schema_fields) |schema_field| {
                const label = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, schema_field);
                defer allocator.free(label);
                const label_idx = indexOfEntityLabel(entity_labels, label) orelse continue;
                out[label_idx] = @floatFromInt(task_idx + 1);
            }
            continue;
        }
        for (task.fields) |field| {
            const label_idx = (if (task.kind == .entities)
                indexOfEntityLabel(entity_labels, field.name)
            else blk: {
                const label = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, field.name);
                defer allocator.free(label);
                break :blk indexOfEntityLabel(entity_labels, label);
            }) orelse continue;
            out[label_idx] = @floatFromInt(task_idx + 1);
        }
    }
}

fn indexOfEntityLabel(entity_labels: []const []const u8, label: []const u8) ?usize {
    for (entity_labels, 0..) |candidate, idx| {
        if (std.mem.eql(u8, candidate, label)) return idx;
    }
    return null;
}

fn upstreamSchemaParentTokenPosition(encoded: *const gliner2_data.EncodedBatch, sample_idx: usize, task_idx: usize) usize {
    if (encoded.max_schemas == 0 or encoded.max_schema_specials == 0) return 1;
    if (task_idx >= encoded.max_schemas) return 1;
    const count_idx = sample_idx * encoded.max_schemas + task_idx;
    if (count_idx >= encoded.schema_special_counts.len or encoded.schema_special_counts[count_idx] <= 0) return 1;
    const pos_idx = (sample_idx * encoded.max_schemas + task_idx) * encoded.max_schema_specials;
    if (pos_idx >= encoded.schema_special_positions.len) return 1;
    const raw = encoded.schema_special_positions[pos_idx];
    if (raw < 0) return 1;
    return @intCast(raw);
}

fn stringSliceContains(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn spanLossName(loss: gliner2_autodiff.SpanStartLossKind) []const u8 {
    return switch (loss) {
        .bce => "bce",
        .mse => "mse",
    };
}

fn spanLossReductionName(reduction: gliner2_autodiff.SpanStartLossReduction) []const u8 {
    return switch (reduction) {
        .mean => "mean",
        .sum => "sum",
    };
}

fn objectiveName(objective: gliner2_autodiff.GlinerObjective) []const u8 {
    return switch (objective) {
        .token => "token",
        .span_start => "span-start",
        .gliner2_total_loss => "gliner2-total-loss",
    };
}

fn envFlag(name: [:0]const u8) bool {
    const value = std.c.getenv(name) orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

test "resolveSpanLabelPositiveWeights applies defaults and overrides" {
    const allocator = std.testing.allocator;
    const labels = [_][]const u8{ "location", "organization", "person" };
    const weights = try resolveSpanLabelPositiveWeights(allocator, "organization=96,person=48", labels[0..], 32.0);
    defer allocator.free(weights);

    try std.testing.expectEqual(@as(usize, 3), weights.len);
    try std.testing.expectEqual(@as(f32, 32.0), weights[0]);
    try std.testing.expectEqual(@as(f32, 96.0), weights[1]);
    try std.testing.expectEqual(@as(f32, 48.0), weights[2]);
}

test "resolveSpanLabelPositiveWeights rejects unknown labels" {
    const allocator = std.testing.allocator;
    const labels = [_][]const u8{ "location", "organization", "person" };
    try std.testing.expectError(
        error.UnknownSpanLabelPositiveWeight,
        resolveSpanLabelPositiveWeights(allocator, "product=96", labels[0..], 32.0),
    );
}

test "parseEntityTypesCsvOwned preserves caller order" {
    const allocator = std.testing.allocator;
    const labels = try parseEntityTypesCsvOwned(allocator, "person, organization,location");
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }

    try std.testing.expectEqual(@as(usize, 3), labels.len);
    try std.testing.expectEqualStrings("person", labels[0]);
    try std.testing.expectEqualStrings("organization", labels[1]);
    try std.testing.expectEqualStrings("location", labels[2]);
}

test "full-task structure fields retain task-local attention groups" {
    const entity_fields = [_]gliner2_data.UpstreamField{.{ .name = "person", .value = "Alice" }};
    const product_fields = [_]gliner2_data.UpstreamField{
        .{ .name = "name", .value = "widget" },
        .{ .name = "status", .value = "available" },
    };
    const relation_fields = [_]gliner2_data.UpstreamField{.{ .name = "head", .value = "Alice" }};
    const tasks = [_]gliner2_data.UpstreamTask{
        .{ .kind = .entities, .name = "entities", .fields = &entity_fields, .count = 1 },
        .{ .kind = .classifications, .name = "sentiment", .labels = &.{ "yes", "no" } },
        .{ .kind = .json_structures, .name = "product", .fields = &product_fields, .count = 1 },
        .{ .kind = .relations, .name = "founded", .fields = &relation_fields, .count = 1 },
    };
    const product_name = try gliner2_data.upstreamTaskFieldKey(std.testing.allocator, .json_structures, "product", "name");
    defer std.testing.allocator.free(product_name);
    const product_status = try gliner2_data.upstreamTaskFieldKey(std.testing.allocator, .json_structures, "product", "status");
    defer std.testing.allocator.free(product_status);
    const founded_head = try gliner2_data.upstreamTaskFieldKey(std.testing.allocator, .relations, "founded", "head");
    defer std.testing.allocator.free(founded_head);
    const sentiment_yes = try gliner2_data.upstreamTaskFieldKey(std.testing.allocator, .classifications, "sentiment", "yes");
    defer std.testing.allocator.free(sentiment_yes);
    const labels = [_][]const u8{ "person", product_name, product_status, founded_head, sentiment_yes };
    var groups: [labels.len]f32 = undefined;
    try fillGliner2StructureTaskGroups(
        std.testing.allocator,
        .{ .text = "Alice", .tasks = &tasks },
        &labels,
        &groups,
    );

    try std.testing.expectEqualSlices(f32, &.{ 1.0, 3.0, 3.0, 4.0, 0.0 }, &groups);
    try std.testing.expect(groups[1] == groups[2]);
    try std.testing.expect(groups[0] != groups[1]);
    try std.testing.expect(groups[1] != groups[3]);
}

test "full-task negative masking is independent per structure instance" {
    const entity_types: usize = 1;
    const max_instances: u32 = 2;
    const width = gliner2_autodiff.gliner2TotalLossTargetWidthEx(entity_types, max_instances);
    var targets = try std.testing.allocator.alloc(f32, width);
    defer std.testing.allocator.free(targets);
    @memset(targets, 0.0);

    const labels_offset = gliner2_autodiff.gliner2TotalLossInstanceLabelsOffset(entity_types);
    const masks_offset = gliner2_autodiff.gliner2TotalLossInstanceMasksOffset(entity_types, max_instances);
    targets[labels_offset] = 1.0;
    targets[masks_offset] = 1.0;
    targets[masks_offset + 1] = 1.0;

    applyGliner2InstanceNegativeMask(targets, entity_types, width, max_instances, 1.0, 42);
    try std.testing.expectEqual(@as(f32, 1.0), targets[masks_offset]);
    try std.testing.expectEqual(@as(f32, 0.0), targets[masks_offset + 1]);
}

test "packed target stats count only final active schema cells" {
    const E = 3;
    const width = 2 * E;
    var targets = [_]f32{0.0} ** (2 * width);
    targets[0] = 1.0;
    targets[E + 0] = 1.0;
    targets[E + 1] = 1.0;
    targets[width + 1] = 1.0;
    targets[width + E + 1] = 1.0;

    const stats = try BatchTargetStats.fromPackedSpanTargets(
        &targets,
        width,
        E,
        .span_start,
        1,
    );
    try std.testing.expectEqual(@as(u64, 3), stats.supervised_token_count);
    try std.testing.expectEqual(@as(u64, 2), stats.entity_token_count);
    try std.testing.expectEqual(@as(u64, 3), stats.ignored_token_count);
    try std.testing.expectEqualSlices(u64, &.{ 1, 1, 0 }, stats.positiveCounts());
}

test "reported loss matches upstream gradient accumulation scaling" {
    try std.testing.expectEqual(@as(f32, 3.0), optimizedLossForReporting(12.0, 4));
}

test "full-task batch graph axes use bounded local buckets" {
    const limits = TotalLossGraphLimits{
        .seq_len = 128,
        .schema_slots = 7,
        .structure_instances = 19,
        .schema_tasks = 5,
        .max_span_width = 2,
    };
    const local = bucketTotalLossShape(17, 3, 3, 3, limits);
    try std.testing.expectEqual(@as(usize, 24), local.seq_len);
    try std.testing.expectEqual(@as(usize, 4), local.schema_slots);
    try std.testing.expectEqual(@as(u32, 4), local.structure_instances);
    try std.testing.expectEqual(@as(u32, 4), local.schema_tasks);

    const capped = bucketTotalLossShape(127, 7, 19, 5, limits);
    try std.testing.expectEqual(@as(usize, 128), capped.seq_len);
    try std.testing.expectEqual(limits.schema_slots, capped.schema_slots);
    try std.testing.expectEqual(limits.structure_instances, capped.structure_instances);
    try std.testing.expectEqual(limits.schema_tasks, capped.schema_tasks);
}

test "full-task graph limits include wider held-out contextual axes" {
    const train_tasks = [_]gliner2_data.UpstreamTask{.{
        .kind = .entities,
        .name = "entities",
        .schema_fields = &.{"person"},
    }};
    const eval_tasks = [_]gliner2_data.UpstreamTask{
        .{
            .kind = .json_structures,
            .name = "product",
            .schema_fields = &.{ "name", "price", "status" },
            .count = 3,
        },
        .{
            .kind = .relations,
            .name = "works_for",
            .schema_fields = &.{ "head", "tail" },
            .count = 2,
        },
        .{
            .kind = .classifications,
            .name = "priority",
            .labels = &.{ "normal", "urgent" },
            .true_labels = &.{"urgent"},
        },
    };
    const train_records = [_]gliner2_data.UpstreamRecord{.{ .text = "Alice", .tasks = &train_tasks }};
    const eval_records = [_]gliner2_data.UpstreamRecord{.{ .text = "A wider held-out sample", .tasks = &eval_tasks }};

    const train_axes = try computeTotalLossDatasetAxes(std.testing.allocator, &train_records, &.{});
    const combined = try computeTotalLossDatasetAxes(std.testing.allocator, &train_records, &eval_records);
    const eval_slots = try gliner2_data.maxUpstreamRecordLabelSlots(std.testing.allocator, &eval_records);
    try std.testing.expect(eval_slots > train_axes.schema_slots);
    try std.testing.expectEqual(eval_slots, combined.schema_slots);
    try std.testing.expectEqual(@as(u32, 3), combined.structure_instances);
    try std.testing.expectEqual(@as(u32, 3), combined.schema_tasks);
}

test "training manifest uses canonical full-task objective spelling" {
    try std.testing.expectEqualStrings("gliner2-total-loss", objectiveName(.gliner2_total_loss));
}

test "training manifest serializes initial adapter provenance and requested max steps" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "initial-adapter.safetensors" });
    defer allocator.free(checkpoint_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = checkpoint_path, .data = "initial adapter" });
    const checkpoint_sha256 = (try initialAdapterCheckpointSha256Alloc(allocator, checkpoint_path)).?;
    defer allocator.free(checkpoint_sha256);

    var opts = Options{
        .model_dir = "/model",
        .train_data = "/train.jsonl",
        .out_dir = out_dir,
        .max_steps = 7,
        .initial_adapter_checkpoint = checkpoint_path,
    };
    const labels = [_][]const u8{"person"};
    const weights = [_]f32{1.0};
    const positive_counts = [_]u64{1};
    const resolved_targets = [_][]const u8{"encoder.encoder.layer.0.attention.self.query_proj"};
    const stats = BatchTargetStats{ .entity_type_count = 1 };
    const cache_stats = real_autodiff.RealAutodiffTrainer.GraphCacheStats{
        .capacity = 2,
        .build_reserve_bytes = 1024,
        .builds = 2,
        .hits = 1,
        .active_reuses = 4,
        .evictions = 0,
        .resident_signatures = 2,
        .peak_resident_signatures = 2,
    };
    try writeTrainingManifest(
        allocator,
        opts,
        "native CPU/BLAS",
        "sha256:train",
        null,
        checkpoint_sha256,
        "sha256:model",
        "sha256:adapter",
        128,
        2,
        &labels,
        &weights,
        32,
        1,
        1,
        7,
        7,
        0,
        1.0,
        2,
        2,
        2,
        &.{},
        &resolved_targets,
        &positive_counts,
        stats,
        .{},
        false,
        null,
        null,
        .{},
        cache_stats,
    );

    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, run_validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const manifest_bytes = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(1024 * 1024));
    defer allocator.free(manifest_bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, manifest_bytes, .{});
    defer parsed.deinit();
    const manifest = parsed.value.object;
    try std.testing.expect(manifest.get("initial_adapter_checkpoint_used").?.bool);
    try std.testing.expectEqualStrings(checkpoint_path, manifest.get("initial_adapter_checkpoint").?.string);
    try std.testing.expectEqualStrings(checkpoint_sha256, manifest.get("initial_adapter_checkpoint_sha256").?.string);
    try std.testing.expectEqual(@as(i64, 7), manifest.get("max_steps").?.integer);
    try std.testing.expectEqual(@as(i64, 7), manifest.get("requested_max_steps").?.integer);
    try std.testing.expectEqual(@as(i64, 2), manifest.get("graph_cache_capacity").?.integer);
    try std.testing.expectEqual(@as(i64, 1), manifest.get("graph_cache_hits").?.integer);
    try std.testing.expectEqualStrings("batch-local-v1", manifest.get("graph_shape_policy").?.string);
    try std.testing.expectEqualStrings("active-entry-only", manifest.get("graph_cache_runtime_input_policy").?.string);

    opts.initial_adapter_checkpoint = null;
    try writeTrainingManifest(
        allocator,
        opts,
        "native CPU/BLAS",
        "sha256:train",
        null,
        null,
        "sha256:model",
        "sha256:adapter",
        128,
        2,
        &labels,
        &weights,
        32,
        1,
        1,
        7,
        7,
        0,
        1.0,
        2,
        2,
        2,
        &.{},
        &resolved_targets,
        &positive_counts,
        stats,
        .{},
        false,
        null,
        null,
        .{},
        cache_stats,
    );
    const clean_manifest_bytes = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(1024 * 1024));
    defer allocator.free(clean_manifest_bytes);
    var clean_parsed = try std.json.parseFromSlice(std.json.Value, allocator, clean_manifest_bytes, .{});
    defer clean_parsed.deinit();
    const clean_manifest = clean_parsed.value.object;
    try std.testing.expect(!clean_manifest.get("initial_adapter_checkpoint_used").?.bool);
    try std.testing.expect(clean_manifest.get("initial_adapter_checkpoint") == null);
    try std.testing.expect(clean_manifest.get("initial_adapter_checkpoint_sha256") == null);

    opts.resume_checkpoint = checkpoint_path;
    const resume_sha256 = (try resumeCheckpointSha256Alloc(allocator, checkpoint_path)).?;
    defer allocator.free(resume_sha256);
    const retained_resume_path = try retainedResumeCheckpointRelativePathAlloc(allocator, resume_sha256);
    defer allocator.free(retained_resume_path);
    try writeTrainingManifest(
        allocator,
        opts,
        "native CPU/BLAS",
        "sha256:train",
        null,
        null,
        "sha256:model",
        "sha256:adapter",
        128,
        2,
        &labels,
        &weights,
        32,
        1,
        1,
        7,
        7,
        0,
        1.0,
        2,
        2,
        2,
        &.{},
        &resolved_targets,
        &positive_counts,
        stats,
        .{},
        false,
        null,
        null,
        .{
            .checkpoint_path = retained_resume_path,
            .checkpoint_sha256 = resume_sha256,
            .training_state_fingerprint_sha256 = "sha256:state",
            .restored_micro_batch_steps = 3,
            .restored_optimizer_steps = 3,
            .restored_epochs = 3,
        },
        cache_stats,
    );
    const resumed_manifest_bytes = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(1024 * 1024));
    defer allocator.free(resumed_manifest_bytes);
    var resumed_parsed = try std.json.parseFromSlice(std.json.Value, allocator, resumed_manifest_bytes, .{});
    defer resumed_parsed.deinit();
    const resumed_manifest = resumed_parsed.value.object;
    try std.testing.expect(resumed_manifest.get("resume_checkpoint_used").?.bool);
    try std.testing.expectEqualStrings(retained_resume_path, resumed_manifest.get("resume_checkpoint").?.string);
    try std.testing.expectEqualStrings(resume_sha256, resumed_manifest.get("resume_checkpoint_sha256").?.string);
    try std.testing.expectEqual(@as(i64, 3), resumed_manifest.get("resume_restored_epochs").?.integer);

    try std.testing.expectError(
        error.InitialAdapterCheckpointNotRegular,
        initialAdapterCheckpointSha256Alloc(allocator, out_dir),
    );
}

test "artifact fingerprint rejects a missing required file" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "present", .data = "present" });
    const dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(dir);
    const required = [_][]const u8{ "present", "missing" };
    try std.testing.expectError(
        error.RequiredFingerprintFileMissing,
        fingerprintRelativeFilesAlloc(allocator, dir, &required),
    );
}
