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
//   --objective <name>           token or span-start (default: token)
//   --max-span-width <n>         Max span width for span-start objective (default: 4)
//   --max-examples <n>           Cap on training examples (0 = all, default: 0)
//   --max-grad-norm <f>          Gradient clipping norm (default: 1.0)
//   --grad-accum <n>             Gradient accumulation steps (default: 1)
//   --seed <n>                   RNG seed (default: 42)

const std = @import("std");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const ml = @import("ml");
const native_compute = inference.native_compute.native;
const metal_compute = if (build_options.enable_metal) inference.native_compute.metal else struct {};
const gpu_hosted_store = inference.native_compute.gpu_hosted_store;
const metal_runtime = inference.metal_runtime;
const compat = inference.io.compat;
const weight_source_mod = inference.models.weight_source;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = inference.backends.Tensor;
const MetalWeightStore = if (build_options.enable_metal) gpu_hosted_store.WeightStore else void;

// MLX backend (Apple Silicon GPU acceleration).
const mlx_mod = inference.backends.mlx;
const mlx = if (build_options.enable_mlx) mlx_mod else struct {};
const mlx_compute = if (build_options.enable_mlx) inference.native_compute.mlx else struct {};
const mlx_c = if (build_options.enable_mlx) mlx_mod.c else struct {};

// Finetune module imports — accessed via the termite internal module tree.
const gliner2_data = inference.finetune.gliner2_data;
const gliner2_bundle = inference.finetune.gliner2;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const real_autodiff = inference.finetune.real_autodiff_trainer;
const run_validation = inference.finetune.gliner2_run_validation;
const deberta_graph = inference.architectures.deberta_graph;

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
    entity_types_csv: ?[]const u8 = null,
    objective: gliner2_autodiff.GlinerObjective = .token,
    max_span_width: u32 = 4,
    span_loss: gliner2_autodiff.SpanStartLossKind = .bce,
    span_positive_weight: f32 = 32.0,
    span_label_positive_weights: ?[]const u8 = null,
    span_negative_weight: f32 = 1.0,
    span_hard_negative_weight: f32 = 1.0,
    max_examples: usize = 0,
    max_grad_norm: f32 = 1.0,
    grad_accum: u32 = 1,
    seed: u64 = 42,
    backend: Gliner2TrainBackend = .auto,
    compiled_required: bool = false,
};

const Gliner2TrainBackend = enum {
    auto,
    metal,
    mlx,
    native,
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
    var entity_types_csv: ?[]const u8 = null;
    var objective: gliner2_autodiff.GlinerObjective = .token;
    var max_span_width: u32 = 4;
    var span_loss: gliner2_autodiff.SpanStartLossKind = .bce;
    var span_positive_weight: f32 = 32.0;
    var span_label_positive_weights: ?[]const u8 = null;
    var span_negative_weight: f32 = 1.0;
    var span_hard_negative_weight: f32 = 1.0;
    var max_examples: usize = 0;
    var max_grad_norm: f32 = 1.0;
    var grad_accum: u32 = 1;
    var seed: u64 = 42;
    var backend: Gliner2TrainBackend = .auto;
    var compiled_required: bool = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, arg, "--model-dir")) {
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
        } else if (std.mem.eql(u8, arg, "--backend")) {
            const val = args.next() orelse return error.MissingBackend;
            backend = parseBackend(val) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--compiled-required")) {
            compiled_required = true;
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
        .entity_types_csv = entity_types_csv,
        .objective = objective,
        .max_span_width = max_span_width,
        .span_loss = span_loss,
        .span_positive_weight = span_positive_weight,
        .span_label_positive_weights = span_label_positive_weights,
        .span_negative_weight = span_negative_weight,
        .span_hard_negative_weight = span_hard_negative_weight,
        .max_examples = max_examples,
        .max_grad_norm = max_grad_norm,
        .grad_accum = grad_accum,
        .seed = seed,
        .backend = backend,
        .compiled_required = compiled_required,
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
    print("  objective={s} max_span_width={d} span_loss={s} span_pos_weight={d:.3} span_neg_weight={d:.3} span_hard_neg_weight={d:.3}\n", .{
        objectiveName(opts.objective),
        opts.max_span_width,
        spanLossName(opts.span_loss),
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
    // 3. Set up compute backend + load weights
    //
    // Use MLX (Apple Silicon GPU) when available, falling back to
    // native CPU BLAS.
    // ------------------------------------------------------------------
    var st_path_buf: [512]u8 = undefined;
    const st_path = try std.fmt.bufPrint(&st_path_buf, "{s}/model.safetensors", .{opts.model_dir});

    // We need these variables to live for the whole function regardless
    // of which backend branch we take.
    var mlx_ws: if (build_options.enable_mlx) mlx_compute.WeightStore else void = undefined;
    var mlx_backend: if (build_options.enable_mlx) *mlx_compute.MlxCompute else void = undefined;
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
    const mlx_runtime_available = if (comptime build_options.enable_mlx)
        (!force_native and (mlx.metalDeviceAvailable() or mlx.allowCpuStreamWithoutMetal()))
    else
        false;
    if (comptime build_options.enable_mlx) {
        if (force_native) {
            print("info: TERMITE_GLINER2_FORCE_NATIVE is set; using native CPU/BLAS\n", .{});
        } else if (!mlx_runtime_available) {
            print("warning: MLX build enabled but no Metal device is available; falling back to native CPU/BLAS\n", .{});
        }
    }

    const selected_backend = selectBackend(opts.backend, force_native, metal_runtime_available, mlx_runtime_available) catch |err| {
        switch (err) {
            error.MetalBackendUnavailable => print("error: --backend metal requested but Metal is not built or no Metal device is available\n", .{}),
            error.MlxBackendUnavailable => print("error: --backend mlx requested but MLX is not built or unavailable\n", .{}),
        }
        return err;
    };

    const cb = if (selected_backend == .metal) blk: {
        if (comptime build_options.enable_metal) {
            metal_ws = .{
                .allocator = allocator,
                .resident_weights = if (comptime build_options.enable_mlx) mlx_c.mlx_map_string_to_array_new() else {},
                .stream = if (comptime build_options.enable_mlx) mlx.openDefaultStream().stream else {},
                .prefix = "",
                .lazy_weights = .{},
            };
            try loadSafetensorsIntoGpuHostedStore(allocator, &metal_ws, st_path);
            try initClassifierHeadInGpuHostedStore(allocator, &metal_ws, opts.seed, deberta_config.hidden_size, opts.num_classes);
            metal_compute.initPrefetchQueue(&metal_ws, allocator);
            metal_backend = try metal_compute.MetalCompute.init(allocator, &metal_ws, null);
            break :blk metal_backend.computeBackend();
        } else unreachable;
    } else if (selected_backend == .mlx) blk: {
        if (comptime !build_options.enable_mlx) unreachable;
        // ── MLX path: load weights directly into MLX arrays ──────────
        const raw_weights = try mlx.loadSafetensors(st_path, allocator, mlx.openDefaultStream().stream);
        // Build a new map with "encoder." prefix stripped.
        const stripped_weights = mlx_c.mlx_map_string_to_array_new();
        const it = mlx_c.mlx_map_string_to_array_iterator_new(raw_weights);
        defer _ = mlx_c.mlx_map_string_to_array_iterator_free(it);
        var loaded_count: usize = 0;
        while (true) {
            var key: [*c]const u8 = null;
            var val = mlx_c.mlx_array_new();
            if (mlx_c.mlx_map_string_to_array_iterator_next(&key, &val, it) != 0) {
                _ = mlx_c.mlx_array_free(val);
                break;
            }
            if (key == null) {
                _ = mlx_c.mlx_array_free(val);
                break;
            }
            const name = std.mem.span(key);
            const stripped = stripEncoderPrefix(name);
            const stripped_z = try allocator.dupeSentinel(u8, stripped, 0);
            defer allocator.free(stripped_z);
            _ = mlx_c.mlx_map_string_to_array_insert(stripped_weights, stripped_z.ptr, val);
            _ = mlx_c.mlx_array_free(val);
            loaded_count += 1;
        }
        _ = mlx_c.mlx_map_string_to_array_free(raw_weights);
        print("  loaded {d} weights via MLX from {s}\n", .{ loaded_count, st_path });

        // Initialize classifier head as MLX arrays.
        {
            var rng_init = std.Random.DefaultPrng.init(opts.seed);
            var prng_init = rng_init.random();
            const H = deberta_config.hidden_size;
            const C = opts.num_classes;

            const w_data = try allocator.alloc(f32, C * H);
            defer allocator.free(w_data);
            const sd: f32 = 0.02;
            for (w_data) |*v| v.* = prng_init.floatNorm(f32) * sd;
            const w_shape = [_]i32{ @intCast(C), @intCast(H) };
            const w_arr = mlx.arrayFromFloat32(w_data, &w_shape);
            try mlx.insertWeight(stripped_weights, allocator, "classifier.weight", w_arr);

            const b_data = try allocator.alloc(f32, C);
            defer allocator.free(b_data);
            @memset(b_data, 0.0);
            const b_shape = [_]i32{@intCast(C)};
            const b_arr = mlx.arrayFromFloat32(b_data, &b_shape);
            try mlx.insertWeight(stripped_weights, allocator, "classifier.bias", b_arr);
            print("  initialized classifier head (MLX): [{d}, {d}] + [{d}]\n", .{ C, H, C });
        }

        mlx_ws = .{
            .allocator = allocator,
            .resident_weights = stripped_weights,
            .stream = mlx.openDefaultStream().stream,
            .prefix = "",
            .lazy_weights = .{},
        };
        mlx_backend = try allocator.create(mlx_compute.MlxCompute);
        mlx_backend.* = try mlx_compute.MlxCompute.init(allocator, &mlx_ws, null);
        break :blk mlx_backend.computeBackend();
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
    defer switch (selected_backend) {
        .native => deinitNativeWeightStore(allocator, &native_ws),
        .metal => if (comptime build_options.enable_metal) deinitGpuHostedWeightStore(allocator, &metal_ws),
        else => {},
    };
    defer switch (selected_backend) {
        .metal => if (comptime build_options.enable_metal) metal_backend.deinit(),
        .mlx => if (comptime build_options.enable_mlx) mlx_backend.deinit(),
        else => {},
    };

    print("  backend: {s}\n", .{backendLabel(selected_backend)});

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
    // Class 0 is always the "O" (no entity) class. Prefer an explicit caller
    // entity order so training, manifest export, and evaluation agree on
    // class IDs; legacy direct invocations fall back to sorted dataset labels.
    var label_map = std.StringHashMapUnmanaged(u32){};
    defer label_map.deinit(allocator);
    const entity_types = if (opts.entity_types_csv) |csv|
        try parseEntityTypesCsvOwned(allocator, csv)
    else
        try gliner2_data.buildLabelVocab(allocator, examples, null);
    defer {
        for (entity_types) |label| allocator.free(label);
        allocator.free(entity_types);
    }
    if (entity_types.len + 1 > opts.num_classes) {
        print("error: dataset has {d} entity labels but num_classes={d} only has {d} entity slots\n", .{
            entity_types.len,
            opts.num_classes,
            if (opts.num_classes > 0) opts.num_classes - 1 else 0,
        });
        return error.TooManyEntityTypes;
    }
    if (opts.objective == .span_start and entity_types.len + 1 != @as(usize, @intCast(opts.num_classes))) {
        print("error: span-start objective currently requires num_classes == entity_label_count + 1 ({d}); got {d}\n", .{
            entity_types.len + 1,
            opts.num_classes,
        });
        return error.SpanObjectiveRequiresExactClassCount;
    }
    for (entity_types, 0..) |label, idx| {
        try label_map.put(allocator, label, @intCast(idx + 1));
    }
    print("  entity labels mapped: {d} (num_classes={d})\n", .{ label_map.count(), opts.num_classes });
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
        .objective = opts.objective,
        .span_start_loss = opts.span_loss,
        .span_start_positive_weight = opts.span_positive_weight,
        .span_start_negative_weight = opts.span_negative_weight,
    });

    // ------------------------------------------------------------------
    // 9. Initialize the RealAutodiffTrainer
    // ------------------------------------------------------------------
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = opts.lora_rank,
        .alpha = opts.lora_alpha,
        .target_patterns = target_patterns.items,
    };
    const regular_trainable_params = [_][]const u8{ "classifier.weight", "classifier.bias" };

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
            .regular_trainable_params = &regular_trainable_params,
            .execution_engine = switch (selected_backend) {
                .metal => .compiled_metal,
                .mlx => .compiled_mlx,
                else => .interpreter,
            },
            .compiled_required = opts.compiled_required,
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
    const use_label_positive_weights = opts.span_label_positive_weights != null;
    const span_entity_types: usize = if (opts.num_classes > 1) @as(usize, @intCast(opts.num_classes)) - 1 else 0;
    const span_target_width: usize = if (use_label_positive_weights)
        gliner2_autodiff.weightedSpanStartTargetWidth(span_entity_types)
    else
        gliner2_autodiff.spanStartTargetWidth(span_entity_types);
    const max_span_target_values = bs * sl * @as(usize, @intCast(opts.max_span_width)) * span_target_width;
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

    var rng = std.Random.DefaultPrng.init(opts.seed);
    var prng = rng.random();

    var cumulative_loss: f64 = 0.0;
    var total_steps: u64 = 0;
    var run_target_stats = BatchTargetStats{};
    var metrics_jsonl: std.Io.Writer.Allocating = .init(allocator);
    defer metrics_jsonl.deinit();

    for (0..opts.epochs) |epoch| {
        // Shuffle examples at the start of each epoch.
        prng.shuffle(gliner2_data.Example, examples);

        const epoch_started_ns = monotonicNowNs();
        var epoch_loss: f64 = 0.0;
        var epoch_steps: u64 = 0;
        var epoch_target_stats = BatchTargetStats{};

        var batch_start: usize = 0;
        while (batch_start < total_examples) {
            const batch_end = @min(batch_start + bs, total_examples);
            const actual_batch: u32 = @intCast(batch_end - batch_start);
            const ab: usize = actual_batch;

            // Tokenize batch + build entity/span targets.
            const step_started_ns = monotonicNowNs();
            var target_stats = BatchTargetStats{};
            var targets_shape: ml.graph.Shape = undefined;
            var target_slice: []const f32 = undefined;
            switch (opts.objective) {
                .token => {
                    target_stats = fillBatchBuffers(
                        allocator,
                        &tokenizer,
                        entity_types,
                        examples[batch_start..batch_end],
                        opts.seq_len,
                        opts.num_classes,
                        &label_map,
                        input_ids,
                        attention_mask,
                        targets_buf[0 .. ab * sl * nc],
                    );
                    targets_shape = gliner2_autodiff.tokenTargetsShape(
                        actual_batch,
                        opts.seq_len,
                        opts.num_classes,
                    );
                    target_slice = targets_buf[0 .. ab * sl * nc];
                },
                .span_start => {
                    var encoded = try gliner2_data.buildSimpleBatch(
                        allocator,
                        &tokenizer,
                        examples[batch_start..batch_end],
                        entity_types,
                        opts.seq_len,
                        opts.max_span_width,
                        ab,
                    );
                    defer encoded.deinit();

                    if (encoded.input_ids.len != ab * sl or encoded.attention_mask.len != ab * sl) return error.InvalidGlinerBatchShape;
                    for (0..ab * sl) |i| {
                        input_ids[i] = encoded.input_ids[i];
                        attention_mask[i] = @floatFromInt(encoded.attention_mask[i]);
                    }

                    const width = if (use_label_positive_weights)
                        gliner2_autodiff.weightedSpanStartTargetWidth(encoded.num_entity_types)
                    else
                        gliner2_autodiff.spanStartTargetWidth(encoded.num_entity_types);
                    const target_len = encoded.batch_size * encoded.max_spans * width;
                    const span_stats = try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatchWithOptions(
                        &encoded,
                        .{
                            .positive_weights_by_entity_type = if (use_label_positive_weights) resolved_span_label_positive_weights else null,
                            .hard_negative_weight = opts.span_hard_negative_weight,
                        },
                        targets_buf[0..target_len],
                    );
                    target_stats = BatchTargetStats.fromSpanStart(span_stats, encoded.num_entity_types);
                    targets_shape = if (use_label_positive_weights)
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
            const target_built_ns = monotonicNowNs();

            // Build TrainerInput via the GLiNER2 convenience builder.
            const trainer_input = gliner2_autodiff.makeTrainerInput(
                &gliner_ctx,
                input_ids[0 .. ab * sl],
                attention_mask[0 .. ab * sl],
                target_slice,
                targets_shape,
                actual_batch,
                opts.seq_len,
            );

            const result = try trainer.step(trainer_input);
            const step_finished_ns = monotonicNowNs();
            const timing = StepTiming{
                .target_build_ns = elapsedNs(step_started_ns, target_built_ns),
                .train_step_ns = elapsedNs(target_built_ns, step_finished_ns),
                .step_wall_ns = elapsedNs(step_started_ns, step_finished_ns),
                .profile = result.profile,
            };
            epoch_loss += result.loss;
            epoch_steps += 1;
            epoch_target_stats.add(target_stats);
            run_target_stats.add(target_stats);
            total_steps += 1;
            try writeStepMetric(&metrics_jsonl.writer, epoch + 1, total_steps, epoch_steps, result.loss, result.grad_norm, result.optimizer_stepped, target_stats, timing);

            if (total_steps % 10 == 0 or batch_end >= total_examples) {
                print("  [epoch {d}/{d}] step {d}/{d}  loss={d:.6}  grad_norm={d:.4}  supervised_tok/s={d:.2}{s}\n", .{
                    epoch + 1,
                    opts.epochs,
                    epoch_steps,
                    steps_per_epoch,
                    result.loss,
                    result.grad_norm,
                    timing.supervisedTokensPerSecond(target_stats),
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
        const epoch_timing = EpochTiming{
            .epoch_wall_ns = elapsedNs(epoch_started_ns, monotonicNowNs()),
        };
        try writeEpochMetric(&metrics_jsonl.writer, epoch + 1, avg_epoch_loss, approx_acc, gold_ent_count, epoch_steps, epoch_target_stats, epoch_timing);
    }

    // ------------------------------------------------------------------
    // 11. Save adapters
    // ------------------------------------------------------------------
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
        target_patterns.items,
        autodiff_params,
    );
    defer gliner2_bundle.freeAutodiffAdapterExportSummary(allocator, &peft_export);
    const regular_params = try collectRegularTrainableParams(allocator, &trainer);
    defer allocator.free(regular_params);
    var regular_export = try gliner2_bundle.exportAutodiffRegularParamsAsSafetensors(
        allocator,
        opts.out_dir,
        regular_params,
    );
    defer gliner2_bundle.freeAutodiffRegularParamExportSummary(allocator, &regular_export);

    const metrics_path = try std.fs.path.join(allocator, &.{ opts.out_dir, run_validation.metrics_file_name });
    defer allocator.free(metrics_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = metrics_path, .data = metrics_jsonl.written() });

    const final_avg = if (opts.epochs > 0) cumulative_loss / @as(f64, @floatFromInt(opts.epochs)) else 0.0;
    try writeTrainingManifest(allocator, opts, backendLabel(selected_backend), deberta_config.hidden_size, entity_types, resolved_span_label_positive_weights, examples.len, total_steps, final_avg, trainer.lora_params.items.len, peft_export.exported_tensor_count, regular_export.exported_tensor_count, run_target_stats);
    print("\nLoRA adapters saved to {s}\n", .{opts.out_dir});

    print("training complete -- {d} total steps, final avg loss={d:.6}\n", .{ total_steps, final_avg });
}

fn writeStepMetric(
    writer: *std.Io.Writer,
    epoch: usize,
    global_step: u64,
    epoch_step: u64,
    loss: f32,
    grad_norm: f32,
    optimizer_stepped: bool,
    target_stats: BatchTargetStats,
    timing: StepTiming,
) !void {
    try std.json.Stringify.value(.{
        .event = "step",
        .epoch = epoch,
        .step = global_step,
        .epoch_step = epoch_step,
        .loss = loss,
        .grad_norm = grad_norm,
        .optimizer_stepped = optimizer_stepped,
        .supervised_token_count = target_stats.supervised_token_count,
        .entity_token_count = target_stats.entity_token_count,
        .ignored_token_count = target_stats.ignored_token_count,
        .entity_token_rate = target_stats.entityTokenRate(),
        .entity_label_positive_counts = target_stats.positiveCounts(),
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
        .trainer_total_ms = nsToMillis(timing.profile.total_ns),
        .peak_resident_bytes = timing.profile.peak_resident_bytes,
        .supervised_tokens_per_second = timing.supervisedTokensPerSecond(target_stats),
    }, .{}, writer);
    try writer.writeByte('\n');
}

fn writeEpochMetric(
    writer: *std.Io.Writer,
    epoch: usize,
    avg_loss: f64,
    approx_accuracy_percent: f64,
    gold_entities: u64,
    steps: u64,
    target_stats: BatchTargetStats,
    timing: EpochTiming,
) !void {
    try std.json.Stringify.value(.{
        .event = "epoch",
        .epoch = epoch,
        .avg_loss = avg_loss,
        .approx_accuracy_percent = approx_accuracy_percent,
        .gold_entities = gold_entities,
        .steps = steps,
        .supervised_token_count = target_stats.supervised_token_count,
        .entity_token_count = target_stats.entity_token_count,
        .ignored_token_count = target_stats.ignored_token_count,
        .entity_token_rate = target_stats.entityTokenRate(),
        .entity_label_positive_counts = target_stats.positiveCounts(),
        .epoch_wall_ms = nsToMillis(timing.epoch_wall_ns),
        .supervised_tokens_per_second = tokensPerSecond(target_stats.supervised_token_count, timing.epoch_wall_ns),
    }, .{}, writer);
    try writer.writeByte('\n');
}

fn writeTrainingManifest(
    allocator: std.mem.Allocator,
    opts: Options,
    backend_label: []const u8,
    hidden_size: u32,
    entity_labels: []const []const u8,
    span_label_positive_weights: []const f32,
    example_count: usize,
    total_steps: u64,
    final_avg_loss: f64,
    adapter_parameter_file_count: usize,
    peft_adapter_tensor_count: usize,
    regular_trainable_tensor_count: usize,
    target_stats: BatchTargetStats,
) !void {
    const manifest_path = try std.fs.path.join(allocator, &.{ opts.out_dir, run_validation.manifest_file_name });
    defer allocator.free(manifest_path);

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .schema_version = "gliner2_autodiff_training/v1",
        .artifact_family_version = "gliner2_autodiff_adapter/v1",
        .model_dir = opts.model_dir,
        .backend = backend_label,
        .compiled_required = opts.compiled_required,
        .train_data = opts.train_data,
        .out_dir = opts.out_dir,
        .metrics_file = run_validation.metrics_file_name,
        .adapter_parameter_format = "real_autodiff_bin/v1",
        .adapter_parameter_file_count = adapter_parameter_file_count,
        .peft_adapter_checkpoint = gliner2_bundle.adapter_checkpoint_file_name,
        .peft_adapter_config = gliner2_bundle.adapter_config_file_name,
        .peft_adapter_tensor_count = peft_adapter_tensor_count,
        .regular_trainable_checkpoint = gliner2_bundle.task_head_checkpoint_file_name,
        .regular_trainable_tensor_count = regular_trainable_tensor_count,
        .regular_trainable_params = .{ "classifier.weight", "classifier.bias" },
        .epochs = opts.epochs,
        .batch_size = opts.batch_size,
        .seq_len = opts.seq_len,
        .learning_rate = opts.learning_rate,
        .lora_rank = opts.lora_rank,
        .lora_alpha = opts.lora_alpha,
        .lora_targets = opts.lora_targets,
        .num_classes = opts.num_classes,
        .objective = objectiveName(opts.objective),
        .max_span_width = opts.max_span_width,
        .span_loss = spanLossName(opts.span_loss),
        .span_positive_weight = opts.span_positive_weight,
        .span_label_positive_weights = span_label_positive_weights,
        .span_negative_weight = opts.span_negative_weight,
        .span_hard_negative_weight = opts.span_hard_negative_weight,
        .hidden_size = hidden_size,
        .entity_labels = entity_labels,
        .entity_label_count = entity_labels.len,
        .entity_label_positive_counts = target_stats.positiveCounts(),
        .supervised_token_count = target_stats.supervised_token_count,
        .entity_token_count = target_stats.entity_token_count,
        .ignored_token_count = target_stats.ignored_token_count,
        .entity_token_rate = target_stats.entityTokenRate(),
        .max_examples = opts.max_examples,
        .max_grad_norm = opts.max_grad_norm,
        .grad_accum = opts.grad_accum,
        .seed = opts.seed,
        .example_count = example_count,
        .total_steps = total_steps,
        .final_avg_loss = final_avg_loss,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try buffer.writer.writeByte('\n');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = manifest_path, .data = buffer.written() });
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

const BatchTargetStats = struct {
    supervised_token_count: u64 = 0,
    entity_token_count: u64 = 0,
    ignored_token_count: u64 = 0,
    entity_type_count: usize = 0,
    positive_counts_by_entity_type: [gliner2_autodiff.max_span_start_entity_types]u64 = @as([gliner2_autodiff.max_span_start_entity_types]u64, @splat(0)),

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

    fn fromSpanStart(stats: gliner2_autodiff.SpanStartTargetStats, num_entity_types: usize) BatchTargetStats {
        var out = BatchTargetStats{
            .supervised_token_count = stats.valid_span_count * @as(u64, @intCast(num_entity_types)),
            .entity_token_count = stats.positive_span_label_count,
            .ignored_token_count = stats.ignored_span_count * @as(u64, @intCast(num_entity_types)),
            .entity_type_count = stats.entity_type_count,
        };
        for (0..stats.entity_type_count) |idx| {
            out.positive_counts_by_entity_type[idx] = stats.positive_counts_by_entity_type[idx];
        }
        return out;
    }
};

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
    var stats = BatchTargetStats{};

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
                if (cls > 0) stats.entity_token_count += 1;
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
    if (std.ascii.eqlIgnoreCase(value, "mlx")) return .mlx;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    return null;
}

fn selectBackend(
    requested: Gliner2TrainBackend,
    force_native: bool,
    metal_available: bool,
    mlx_available: bool,
) !Gliner2TrainBackend {
    if (force_native) return .native;
    return switch (requested) {
        .auto => if (metal_available) .metal else if (mlx_available) .mlx else .native,
        .metal => if (metal_available) .metal else error.MetalBackendUnavailable,
        .mlx => if (mlx_available) .mlx else error.MlxBackendUnavailable,
        .native => .native,
    };
}

fn backendLabel(backend: Gliner2TrainBackend) []const u8 {
    return switch (backend) {
        .auto => "auto",
        .metal => "Metal",
        .mlx => "MLX (Apple Silicon)",
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
    const w_tensor = try Tensor.initFloat32(allocator, "classifier.weight", &.{ C, H }, w_data);
    try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "classifier.weight"), .{
        .tensor_ref = undefined,
        .host_loaded = .{ .tensor = w_tensor },
        .active_tier = .host,
        .loaded_bytes = w_tensor.data.len,
    });

    const b_data = try allocator.alloc(f32, C);
    defer allocator.free(b_data);
    @memset(b_data, 0.0);
    const b_tensor = try Tensor.initFloat32(allocator, "classifier.bias", &.{C}, b_data);
    try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "classifier.bias"), .{
        .tensor_ref = undefined,
        .host_loaded = .{ .tensor = b_tensor },
        .active_tier = .host,
        .loaded_bytes = b_tensor.data.len,
    });
    print("  initialized classifier head (Metal): [{d}, {d}] + [{d}]\n", .{ C, H, C });
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
    if (comptime build_options.enable_mlx) {
        _ = mlx_c.mlx_map_string_to_array_free(weight_store.resident_weights);
    }
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
        \\  --entity-types <csv>      Entity label order for classes 1..N
        \\  --objective <name>        token or span-start (default: token)
        \\  --max-span-width <n>      Max span width for span-start objective (default: 4)
        \\  --span-loss <name>        bce or mse for span-start labels (default: bce)
        \\  --span-positive-weight <f> Positive span-label loss weight (default: 32)
        \\  --span-label-positive-weights <csv> Per-label positive weights, e.g. person=32,organization=96
        \\  --span-negative-weight <f> Negative span-label loss weight (default: 1)
        \\  --span-hard-negative-weight <f> Extra negative weight for spans overlapping gold entities (default: 1)
        \\  --max-examples <n>        Cap on training examples (default: 0 = all)
        \\  --max-grad-norm <f>       Gradient clipping norm (default: 1.0)
        \\  --grad-accum <n>          Gradient accumulation steps (default: 1)
        \\  --seed <n>                RNG seed (default: 42)
        \\  --backend <name>          auto, metal, mlx, or native (default: auto)
        \\  --compiled-required       Fail if the requested compiled backend cannot run
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
    print("error: unsupported --objective '{s}' (expected token or span-start)\n", .{value});
    return error.InvalidObjective;
}

fn parseSpanLoss(value: []const u8) !gliner2_autodiff.SpanStartLossKind {
    if (std.mem.eql(u8, value, "bce") or std.mem.eql(u8, value, "binary-cross-entropy")) return .bce;
    if (std.mem.eql(u8, value, "mse")) return .mse;
    print("error: unsupported --span-loss '{s}' (expected bce or mse)\n", .{value});
    return error.InvalidSpanLoss;
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
        const label_idx = indexOfEntityLabel(entity_labels, label) orelse {
            print("error: unknown label in --span-label-positive-weights: {s}\n", .{label});
            return error.UnknownSpanLabelPositiveWeight;
        };
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

fn indexOfEntityLabel(entity_labels: []const []const u8, label: []const u8) ?usize {
    for (entity_labels, 0..) |candidate, idx| {
        if (std.mem.eql(u8, candidate, label)) return idx;
    }
    return null;
}

fn spanLossName(loss: gliner2_autodiff.SpanStartLossKind) []const u8 {
    return switch (loss) {
        .bce => "bce",
        .mse => "mse",
    };
}

fn objectiveName(objective: gliner2_autodiff.GlinerObjective) []const u8 {
    return switch (objective) {
        .token => "token",
        .span_start => "span-start",
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
