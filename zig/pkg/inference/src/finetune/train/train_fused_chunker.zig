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

// Training binary for the fused chunker-embedder.
//
// Usage:
//   train-fused-chunker --data <path> --output <dir> [options]
//
// Options:
//   --data <path>           JSONL data path (file or directory)
//   --output <dir>          Output directory for checkpoints
//   --model-dir <dir>       Model directory (for future encoder loading, currently unused)
//   --epochs <n>            Number of epochs (default: 10)
//   --batch-size <n>        Batch size (default: 16)
//   --lr <f>                Learning rate (default: 1e-4)
//   --hidden-size <n>       Encoder hidden size (default: 768)
//   --max-seq-len <n>       Max token sequence length (default: 384)
//   --checkpoint-every <n>  Save checkpoint every N epochs (default: 0=disabled)
//   --split <name>          Dataset split name filter (default: "train")
//   --seed <n>              Random seed (default: 42)
//   --lora-rank <n>         LoRA rank (default: 0 = disabled)
//   --intermediate-size <n> ModernBERT intermediate_size (default: 1152)
//   --backend native|mlx|auto Select compute backend (default: auto)

const std = @import("std");
const build_options = @import("build_options");
const native_compute = @import("../../ops/native_compute.zig");
const ops_mod = @import("../../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const mlx_compute_mod = if (build_options.enable_mlx) @import("../../ops/mlx_compute.zig") else struct {
    pub const MlxCompute = void;
    pub const WeightStore = void;
};
const mlx_mod = if (build_options.enable_mlx) @import("../../backends/mlx.zig") else struct {
    pub const c = struct {
        pub fn mlx_map_string_to_array_new() void {}
        pub const mlx_array = void;
        pub const mlx_map_string_to_array = void;
    };
    pub fn openDefaultStream() struct { stream: void } {
        return .{ .stream = {} };
    }
    pub fn arrayFromFloat32(_: []const f32, _: []const i32) void {}
    pub fn arrayFromTensor(_: std.mem.Allocator, _: anytype, _: bool) error{}!void {}
    pub fn insertWeight(_: void, _: std.mem.Allocator, _: []const u8, _: void) error{}!void {}
};
const fused_chunker_train = @import("../fused_chunker_train.zig");
const fused_chunker_data = @import("../fused_chunker_data.zig");
const fused_chunker_mod = @import("../fused_chunker.zig");
const fused_chunker_splade = @import("../fused_chunker_splade.zig");
const fused_chunker_loss = @import("../fused_chunker_loss.zig");
const safetensors_checkpoint = @import("../safetensors_checkpoint.zig");
const compat = @import("../../io/compat.zig");
const modern_bert = @import("../../architectures/modern_bert.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const tokenizer_batch_mod = @import("../tokenizer_batch.zig");
const TokenizerBatch = tokenizer_batch_mod.TokenizerBatch;
const TokenFnCtx = tokenizer_batch_mod.TokenFnCtx;
const fused_chunker_lora = @import("../lora_adapter_set.zig");
const tensor_mod = @import("../../backends/tensor.zig");
const lora = @import("../lora.zig");
const segmented_encoder = @import("../../graph/segmented_encoder.zig");
const ml = @import("ml");
const optimizers = ml.graph.optimizers;

const FusedTrainer = fused_chunker_train.FusedTrainer;
const FusedTrainingConfig = fused_chunker_train.FusedTrainingConfig;
const TrainStepSummary = fused_chunker_train.TrainStepSummary;

const print = std.debug.print;

// ---------------------------------------------------------------------------
// CLI options
// ---------------------------------------------------------------------------

const Options = struct {
    data_path: []const u8,
    output_dir: []const u8,
    model_dir: ?[]const u8 = null,
    epochs: u32 = 10,
    batch_size: u32 = 16,
    learning_rate: f32 = 1e-4,
    hidden_size: u32 = 768,
    max_seq_len: u32 = 384,
    checkpoint_every: u32 = 0,
    split: []const u8 = "train",
    seed: u64 = 42,
    lora_rank: u32 = 0,
    intermediate_size: u32 = 1152,
    backend: enum { native, mlx, auto } = .auto,
    // Feature 2: gradient accumulation
    grad_accum: u32 = 1,
    // Feature 3: schedule-free AdamW
    schedule_free: bool = false,
    // Feature 5: NEFTune
    neftune_alpha: f32 = 0.0,
    // Feature 1: XBM
    xbm_capacity: usize = 0,
    // Feature 6: LLRD
    llrd_decay: f32 = 1.0,
    // Feature 4: LoRA+
    lora_plus_ratio: f32 = 1.0,
    // Feature 8: length bucketing
    length_bucketing: bool = false,
    bucket_size: usize = 256,
    // mixed precision flag (stored for downstream use)
    mixed_precision: bool = false,
    // SPLADE sparse embedding head
    splade: bool = false,
    lambda_splade: f32 = 0.15,
    lambda_flops: f32 = 3e-5,
    splade_focus_epoch: u32 = 4,
    // Matryoshka Representation Learning
    mrl: bool = false,
    mrl_dims_str: []const u8 = "768,256,128",
    // Checkpoint resumption
    resume_from: []const u8 = "",
    save_optimizer_state: bool = false,
};

// ---------------------------------------------------------------------------
// Dummy token function (placeholder until tokenizer is wired in)
// ---------------------------------------------------------------------------

/// Fills out_ids and out_mask with zeros and returns 0 tokens produced.
/// This is a placeholder used when no tokenizer is loaded.
fn dummyTokenFn(_: void, text: []const u8, out_ids: []i32, out_mask: []i32, out_offsets: ?[][2]u32) usize {
    _ = text;
    _ = out_offsets;
    @memset(out_ids, 0);
    @memset(out_mask, 0);
    return 0;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next(); // skip binary name

    // Parse CLI
    var data_path: ?[]const u8 = null;
    var output_dir: ?[]const u8 = null;
    var model_dir: ?[]const u8 = null;
    var epochs: u32 = 10;
    var batch_size: u32 = 16;
    var learning_rate: f32 = 1e-4;
    var hidden_size: u32 = 768;
    var max_seq_len: u32 = 384;
    var checkpoint_every: u32 = 0;
    var split: []const u8 = "train";
    var seed: u64 = 42;
    var lora_rank: u32 = 0;
    var intermediate_size: u32 = 1152;
    var backend: @TypeOf((Options{
        .data_path = "",
        .output_dir = "",
    }).backend) = .auto;
    var grad_accum: u32 = 1;
    var schedule_free: bool = false;
    var neftune_alpha: f32 = 0.0;
    var xbm_capacity: usize = 0;
    var llrd_decay: f32 = 1.0;
    var lora_plus_ratio: f32 = 1.0;
    var length_bucketing: bool = false;
    var bucket_size: usize = 256;
    var mixed_precision: bool = false;
    var splade: bool = false;
    var lambda_splade: f32 = 0.15;
    var lambda_flops: f32 = 3e-5;
    var splade_focus_epoch: u32 = 4;
    var mrl: bool = false;
    var mrl_dims_str: []const u8 = "768,256,128";
    var resume_from: []const u8 = "";
    var save_optimizer_state: bool = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--data")) {
            data_path = args.next() orelse return error.MissingDataPath;
        } else if (std.mem.eql(u8, arg, "--output")) {
            output_dir = args.next() orelse return error.MissingOutputDir;
        } else if (std.mem.eql(u8, arg, "--model-dir")) {
            model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            const val = args.next() orelse return error.MissingEpochs;
            epochs = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            const val = args.next() orelse return error.MissingBatchSize;
            batch_size = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--lr")) {
            const val = args.next() orelse return error.MissingLr;
            learning_rate = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--hidden-size")) {
            const val = args.next() orelse return error.MissingHiddenSize;
            hidden_size = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-seq-len")) {
            const val = args.next() orelse return error.MissingMaxSeqLen;
            max_seq_len = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--checkpoint-every")) {
            const val = args.next() orelse return error.MissingCheckpointEvery;
            checkpoint_every = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--split")) {
            split = args.next() orelse return error.MissingSplit;
        } else if (std.mem.eql(u8, arg, "--seed")) {
            const val = args.next() orelse return error.MissingSeed;
            seed = try std.fmt.parseUnsigned(u64, val, 10);
        } else if (std.mem.eql(u8, arg, "--lora-rank")) {
            const val = args.next() orelse return error.MissingLoraRank;
            lora_rank = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--intermediate-size")) {
            const val = args.next() orelse return error.MissingIntermediateSize;
            intermediate_size = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            const val = args.next() orelse return error.MissingBackend;
            if (std.mem.eql(u8, val, "native") or std.mem.eql(u8, val, "blas")) {
                backend = .native;
            } else if (std.mem.eql(u8, val, "mlx")) {
                backend = .mlx;
            } else if (std.mem.eql(u8, val, "auto")) {
                backend = .auto;
            } else {
                print("error: unknown backend '{s}': expected native, mlx, or auto\n", .{val});
                std.process.exit(1);
            }
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            const val = args.next() orelse return error.MissingGradAccum;
            grad_accum = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--schedule-free")) {
            schedule_free = true;
        } else if (std.mem.eql(u8, arg, "--neftune-alpha")) {
            const val = args.next() orelse return error.MissingNeftuneAlpha;
            neftune_alpha = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--xbm-capacity")) {
            const val = args.next() orelse return error.MissingXbmCapacity;
            xbm_capacity = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--llrd-decay")) {
            const val = args.next() orelse return error.MissingLlrdDecay;
            llrd_decay = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lora-plus-ratio")) {
            const val = args.next() orelse return error.MissingLoraPlusRatio;
            lora_plus_ratio = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--length-bucketing")) {
            length_bucketing = true;
        } else if (std.mem.eql(u8, arg, "--bucket-size")) {
            const val = args.next() orelse return error.MissingBucketSize;
            bucket_size = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--mixed-precision")) {
            mixed_precision = true;
        } else if (std.mem.eql(u8, arg, "--splade")) {
            splade = true;
        } else if (std.mem.eql(u8, arg, "--lambda-splade")) {
            const val = args.next() orelse return error.MissingLambdaSplade;
            lambda_splade = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--lambda-flops")) {
            const val = args.next() orelse return error.MissingLambdaFlops;
            lambda_flops = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--splade-focus-epoch")) {
            const val = args.next() orelse return error.MissingSPLADEFocusEpoch;
            splade_focus_epoch = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--mrl")) {
            mrl = true;
        } else if (std.mem.eql(u8, arg, "--mrl-dims")) {
            mrl_dims_str = args.next() orelse return error.MissingMrlDims;
        } else if (std.mem.eql(u8, arg, "--resume-from")) {
            resume_from = args.next() orelse return error.MissingResumeFrom;
        } else if (std.mem.eql(u8, arg, "--save-optimizer-state")) {
            save_optimizer_state = true;
        } else {
            print("unknown argument: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    const opts = Options{
        .data_path = data_path orelse {
            print("error: --data is required\n", .{});
            printUsage();
            std.process.exit(1);
        },
        .output_dir = output_dir orelse {
            print("error: --output is required\n", .{});
            printUsage();
            std.process.exit(1);
        },
        .model_dir = model_dir,
        .epochs = epochs,
        .batch_size = batch_size,
        .learning_rate = learning_rate,
        .hidden_size = hidden_size,
        .max_seq_len = max_seq_len,
        .checkpoint_every = checkpoint_every,
        .split = split,
        .seed = seed,
        .lora_rank = lora_rank,
        .intermediate_size = intermediate_size,
        .backend = backend,
        .grad_accum = grad_accum,
        .schedule_free = schedule_free,
        .neftune_alpha = neftune_alpha,
        .xbm_capacity = xbm_capacity,
        .llrd_decay = llrd_decay,
        .lora_plus_ratio = lora_plus_ratio,
        .length_bucketing = length_bucketing,
        .bucket_size = bucket_size,
        .mixed_precision = mixed_precision,
        .splade = splade,
        .lambda_splade = lambda_splade,
        .lambda_flops = lambda_flops,
        .splade_focus_epoch = splade_focus_epoch,
        .mrl = mrl,
        .mrl_dims_str = mrl_dims_str,
        .resume_from = resume_from,
        .save_optimizer_state = save_optimizer_state,
    };

    try run(allocator, opts);
}

// ---------------------------------------------------------------------------
// Phase 3A: LoRA pre-merge helpers
// ---------------------------------------------------------------------------

/// Merge LoRA delta into WeightStore base weights before encoder forward.
/// Returns a map of original weight byte slices that must be passed to restoreLoRAWeights.
fn mergeLoRAIntoWeights(
    allocator: std.mem.Allocator,
    weight_store: *native_compute.WeightStore,
    la: *const fused_chunker_lora.LoRAAdapterSet,
) !std.StringHashMapUnmanaged([]u8) {
    var originals = std.StringHashMapUnmanaged([]u8).empty;
    errdefer {
        var it = originals.iterator();
        while (it.next()) |e| allocator.free(e.value_ptr.*);
        originals.deinit(allocator);
    }

    for (la.layers) |*ll| {
        // Weight key follows modern_bert.zig's getLayerWeight convention:
        // "model.layers.N.attn.query_proj.weight" etc.
        const suffix: []const u8 = if (std.mem.eql(u8, ll.module_name, "query_proj"))
            "attn.query_proj.weight"
        else if (std.mem.eql(u8, ll.module_name, "value_proj"))
            "attn.value_proj.weight"
        else if (std.mem.eql(u8, ll.module_name, "key_proj"))
            "attn.key_proj.weight"
        else if (std.mem.eql(u8, ll.module_name, "out_proj"))
            "attn.Wo.weight"
        else
            continue;

        var key_buf: [128]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "model.layers.{d}.{s}", .{ ll.layer_idx, suffix });

        const lw = weight_store.resident_weights.getPtr(key) orelse continue;
        if (lw.tensor.dtype != .f32) continue;

        const base_aligned: []align(@alignOf(f32)) const u8 = @alignCast(lw.tensor.data);
        const base_f32 = std.mem.bytesAsSlice(f32, base_aligned);

        // Allocate merged buffer
        const merged = try allocator.alloc(f32, base_f32.len);
        errdefer allocator.free(merged);

        const base_mat = lora.Matrix{
            .rows = ll.out_features,
            .cols = ll.in_features,
            .data = base_f32,
        };
        lora.mergeInto(base_mat, ll.asMatrixA(), ll.asMatrixB(), la.config.alpha, merged);

        // Save original data bytes before replacing
        const orig_bytes = try allocator.dupe(u8, lw.tensor.data);
        try originals.put(allocator, try allocator.dupe(u8, key), orig_bytes);

        // Replace tensor data with merged (as bytes)
        lw.tensor.data = std.mem.sliceAsBytes(merged);
    }
    return originals;
}

/// Restore original weight data after encoder forward.
fn restoreLoRAWeights(
    allocator: std.mem.Allocator,
    weight_store: *native_compute.WeightStore,
    originals: *std.StringHashMapUnmanaged([]u8),
) void {
    var it = originals.iterator();
    while (it.next()) |e| {
        if (weight_store.resident_weights.getPtr(e.key_ptr.*)) |lw| {
            // Free the merged buffer (the current tensor.data)
            const merged_aligned: []align(@alignOf(f32)) const u8 = @alignCast(lw.tensor.data);
            const merged_f32 = std.mem.bytesAsSlice(f32, merged_aligned);
            allocator.free(merged_f32);
            // Restore original
            lw.tensor.data = e.value_ptr.*;
        } else {
            allocator.free(e.value_ptr.*);
        }
        allocator.free(e.key_ptr.*);
    }
    originals.deinit(allocator);
}

/// Insert (or update) a LoRA matrix in the BLAS WeightStore under `key`.
/// The tensor is a 2-D f32 matrix of shape [rows, cols].
/// If a weight already exists under `key` its data is replaced with a fresh
/// copy of `data` so that optimizer updates are visible each step.
fn insertLoRAIntoBlasStore(
    allocator: std.mem.Allocator,
    weight_store: *native_compute.WeightStore,
    key: []const u8,
    data: []const f32,
    rows: usize,
    cols: usize,
) !void {
    const shape = [2]i64{ @intCast(rows), @intCast(cols) };
    if (weight_store.resident_weights.getPtr(key)) |existing| {
        // Update the data in place: free old bytes, copy fresh data.
        const new_bytes = try existing.tensor.allocator.dupe(u8, std.mem.sliceAsBytes(data));
        if (existing.tensor.owns_data) {
            existing.tensor.allocator.free(existing.tensor.data);
        }
        existing.tensor.data = new_bytes;
        existing.tensor.owns_data = true;
        return;
    }
    var tensor = try tensor_mod.Tensor.initFloat32(allocator, key, &shape, data);
    errdefer tensor.deinit();
    const owned_key = try allocator.dupe(u8, key);
    errdefer allocator.free(owned_key);
    try weight_store.resident_weights.put(allocator, owned_key, weight_source_mod.LoadedWeight{ .tensor = tensor });
}

// ---------------------------------------------------------------------------
// Core training routine
// ---------------------------------------------------------------------------

fn run(allocator: std.mem.Allocator, opts: Options) !void {
    // ------------------------------------------------------------------
    // 1. Create output directory
    // ------------------------------------------------------------------
    try compat.cwd().createDirPath(compat.io(), opts.output_dir);

    print("train-fused-chunker data={s} output={s} epochs={d} batch_size={d} lr={d} hidden={d} max_seq_len={d} seed={d}\n", .{
        opts.data_path,
        opts.output_dir,
        opts.epochs,
        opts.batch_size,
        opts.learning_rate,
        opts.hidden_size,
        opts.max_seq_len,
        opts.seed,
    });

    if (opts.model_dir) |mdir| {
        print("model_dir={s}\n", .{mdir});
    } else {
        print("model_dir=none (encoder features will be zero-filled)\n", .{});
    }

    // ------------------------------------------------------------------
    // 2. Set up a minimal ComputeBackend for graph-based boundary head ops
    //    (also used for the encoder forward pass when weights are loaded)
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

    const use_mlx = switch (opts.backend) {
        .mlx => true,
        .native => false,
        .auto => build_options.enable_mlx,
    };

    if (use_mlx and !build_options.enable_mlx) {
        print("error: MLX support not compiled in\n", .{});
        std.process.exit(1);
    }

    if (opts.mixed_precision and !use_mlx) {
        std.debug.print("Warning: --mixed-precision requires MLX backend, ignoring.\n", .{});
    }

    // Declare both backends at outer scope so their addresses are stable for
    // the ComputeBackend vtable pointer that FusedTrainer holds.
    var blas_backend = native_compute.NativeCompute.init(allocator, &weight_store, null);

    // MLX backend and its WeightStore are conditionally compiled.
    // When enable_mlx = false these are void (zero size) and never used.
    const MlxWeightStoreT = if (build_options.enable_mlx) mlx_compute_mod.WeightStore else void;
    const MlxComputeT = if (build_options.enable_mlx) mlx_compute_mod.MlxCompute else void;
    var mlx_weight_store: MlxWeightStoreT = undefined;
    var mlx_backend: MlxComputeT = undefined;

    const cb: ComputeBackend = if (build_options.enable_mlx) blk: {
        if (use_mlx) {
            mlx_weight_store = mlx_compute_mod.WeightStore{
                .allocator = allocator,
                .resident_weights = mlx_mod.c.mlx_map_string_to_array_new(),
                .stream = mlx_mod.openDefaultStream().stream,
                .prefix = "",
                .lazy_weights = .{},
            };
            mlx_backend = if (opts.mixed_precision)
                try mlx_compute_mod.MlxCompute.initMixedPrecision(allocator, &mlx_weight_store, null)
            else
                try mlx_compute_mod.MlxCompute.init(allocator, &mlx_weight_store, null);
            break :blk mlx_backend.computeBackend();
        } else {
            break :blk blas_backend.computeBackend();
        }
    } else blas_backend.computeBackend();

    print("backend: {s}\n", .{if (use_mlx) "mlx" else "native"});

    // ------------------------------------------------------------------
    // 2a. Tokenizer loading
    // ------------------------------------------------------------------
    var tokenizer_opt: ?TokenizerBatch = null;
    defer if (tokenizer_opt) |*tb| tb.deinit();

    if (opts.model_dir) |mdir| {
        tokenizer_opt = TokenizerBatch.loadFromDir(allocator, mdir, opts.max_seq_len) catch |err| blk: {
            print("warning: could not load tokenizer from {s}: {}\n", .{ mdir, err });
            break :blk null;
        };
        if (tokenizer_opt != null) print("tokenizer loaded from {s}\n", .{mdir});
    }

    // ------------------------------------------------------------------
    // 2b. Weight loading into WeightStore
    // ------------------------------------------------------------------
    var encoder_loaded = false;
    if (opts.model_dir) |mdir| {
        var path_buf: [512]u8 = undefined;
        const st_path = std.fmt.bufPrint(&path_buf, "{s}/model.safetensors", .{mdir}) catch null;
        if (st_path) |p| {
            const exists = compat.cwd().statFile(compat.io(), p, .{}) catch null;
            if (exists != null) {
                if (SafetensorsSource.initAbsolute(allocator, p)) |src| {
                    var source_ptr = src;
                    defer source_ptr.weightSource().deinit();
                    const ws = source_ptr.weightSource();
                    if (ws.listNames(allocator)) |names| {
                        defer {
                            for (names) |n| allocator.free(n);
                            allocator.free(names);
                        }
                        var load_ok = true;
                        for (names) |name| {
                            if (ws.getTensor(name)) |lw| {
                                const owned_name = allocator.dupe(u8, name) catch {
                                    load_ok = false;
                                    break;
                                };
                                weight_store.resident_weights.put(allocator, owned_name, lw) catch {
                                    allocator.free(owned_name);
                                    load_ok = false;
                                    break;
                                };
                                // Fix 1: When MLX is active, also insert each weight into
                                // mlx_weight_store so cb.getWeight() finds it on the MLX path.
                                if (comptime build_options.enable_mlx) {
                                    if (use_mlx) {
                                        const arr = mlx_mod.arrayFromTensor(allocator, &lw.tensor, false) catch {
                                            load_ok = false;
                                            break;
                                        };
                                        mlx_mod.insertWeight(
                                            mlx_weight_store.resident_weights,
                                            allocator,
                                            name,
                                            arr,
                                        ) catch {
                                            load_ok = false;
                                            break;
                                        };
                                    }
                                }
                            } else |_| {}
                        }
                        if (load_ok) {
                            encoder_loaded = true;
                            print("loaded {d} weights from {s}\n", .{ names.len, p });
                        }
                    } else |err| {
                        print("warning: could not list weights from {s}: {}\n", .{ p, err });
                    }
                } else |err| {
                    print("warning: could not open {s}: {}\n", .{ p, err });
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 2c. LoRA adapter init
    // ------------------------------------------------------------------
    var lora_adapters_opt: ?fused_chunker_lora.LoRAAdapterSet = null;
    defer if (lora_adapters_opt) |*la| la.deinit();

    if (opts.lora_rank > 0) {
        lora_adapters_opt = try fused_chunker_lora.LoRAAdapterSet.init(
            allocator,
            fused_chunker_lora.LoRAConfig{
                .rank = opts.lora_rank,
                .num_layers = 22,
                .lora_plus_ratio = opts.lora_plus_ratio,
            },
            @intCast(opts.hidden_size),
            @intCast(opts.intermediate_size),
        );
        print("LoRA adapters: rank={d} target_modules=query_proj,value_proj,key_proj,out_proj lora_plus_ratio={d:.1}\n", .{ opts.lora_rank, opts.lora_plus_ratio });
    }

    // ------------------------------------------------------------------
    // 2d. LoRA optimizer state
    // ------------------------------------------------------------------
    var lora_opt_state = optimizers.OptimizerState.init(allocator);
    defer lora_opt_state.deinit();

    // ------------------------------------------------------------------
    // 2e. SPLADE projection weight W and Adam state
    // ------------------------------------------------------------------
    const splade_vocab_size: usize = 50368;

    var splade_w: ?[]f32 = null;
    var splade_adam_m: ?[]f32 = null;
    var splade_adam_v: ?[]f32 = null;
    var splade_adam_step: u64 = 0;

    if (opts.splade and encoder_loaded) {
        const w_size = splade_vocab_size * @as(usize, opts.hidden_size);
        const w_alloc = try allocator.alloc(f32, w_size);
        errdefer allocator.free(w_alloc);
        const m_alloc = try allocator.alloc(f32, w_size);
        errdefer allocator.free(m_alloc);
        const v_alloc = try allocator.alloc(f32, w_size);
        // No errdefer needed here: if this fails, the outer defers never see
        // non-null values, so there's no double-free risk; m_alloc and w_alloc
        // are freed by their own errdefers above.

        @memset(m_alloc, 0);
        @memset(v_alloc, 0);

        // Kaiming uniform init: scale = sqrt(2 / hidden_size)
        const init_std: f32 = @sqrt(2.0 / @as(f32, @floatFromInt(opts.hidden_size)));
        var splade_prng = std.Random.DefaultPrng.init(opts.seed ^ 0x5F1ADE00);
        const splade_rng = splade_prng.random();
        for (w_alloc) |*w_val| {
            w_val.* = (splade_rng.float(f32) * 2.0 - 1.0) * init_std;
        }

        // Commit all three allocations (only after all three succeed).
        splade_w = w_alloc;
        splade_adam_m = m_alloc;
        splade_adam_v = v_alloc;
        print("SPLADE weight W initialized: vocab={d} hidden={d} init_std={d}\n", .{ splade_vocab_size, opts.hidden_size, init_std });
    }

    defer if (splade_w) |w| allocator.free(w);
    defer if (splade_adam_m) |m| allocator.free(m);
    defer if (splade_adam_v) |v| allocator.free(v);

    // ------------------------------------------------------------------
    // 3. Load training samples
    // ------------------------------------------------------------------
    print("loading samples from {s} (split={s})...\n", .{ opts.data_path, opts.split });
    var loaded = try fused_chunker_data.loadSamples(allocator, opts.data_path, opts.split);
    defer loaded.deinit();

    const samples = loaded.samples;
    if (samples.len == 0) {
        print("error: no samples found\n", .{});
        std.process.exit(1);
    }

    const stats = fused_chunker_data.computeStats(samples);
    print("loaded {d} samples  avg_chars={d:.0}  avg_chunks={d:.1}  min_chunks={d}  max_chunks={d}  with_positives={d}\n", .{
        stats.num_samples,
        stats.avg_text_chars,
        stats.avg_chunks_per_sample,
        stats.min_chunks,
        stats.max_chunks,
        stats.samples_with_positives,
    });

    // ------------------------------------------------------------------
    // 4. Build FusedTrainingConfig and FusedTrainer
    // ------------------------------------------------------------------

    // Estimate total_steps: samples / batch_size * epochs (rough)
    const steps_per_epoch = @max(@as(u32, 1), @as(u32, @intCast(samples.len)) / @max(1, opts.batch_size));
    const total_steps = steps_per_epoch * opts.epochs;
    const warmup_steps = @min(@as(u32, 50), total_steps / 10);

    // Parse --mrl-dims comma-separated string (e.g. "768,256,128") into a fixed-size array.
    var mrl_dims_buf: [8]u32 = undefined;
    var mrl_dims_count: usize = 0;
    if (opts.mrl) {
        var it = std.mem.splitScalar(u8, opts.mrl_dims_str, ',');
        while (it.next()) |token| {
            if (mrl_dims_count >= mrl_dims_buf.len) break;
            const trimmed = std.mem.trim(u8, token, " \t");
            if (trimmed.len == 0) continue;
            mrl_dims_buf[mrl_dims_count] = try std.fmt.parseUnsigned(u32, trimmed, 10);
            mrl_dims_count += 1;
        }
    }

    var loss_config = fused_chunker_loss.FusedLossConfig{};
    loss_config.enable_splade = opts.splade;
    loss_config.lambda_splade = opts.lambda_splade;
    loss_config.lambda_flops = opts.lambda_flops;
    loss_config.splade_focus_epoch = opts.splade_focus_epoch;
    loss_config.use_mrl = opts.mrl;
    if (opts.mrl and mrl_dims_count > 0) {
        loss_config.mrl_dims = mrl_dims_buf[0..mrl_dims_count];
    }

    const config = FusedTrainingConfig{
        .max_seq_len = opts.max_seq_len,
        .hidden_size = opts.hidden_size,
        .embedding_dim = opts.hidden_size,
        .batch_size = opts.batch_size,
        .num_epochs = opts.epochs,
        .learning_rate = opts.learning_rate,
        .warmup_steps = warmup_steps,
        .total_steps = @max(1, total_steps),
        .seed = opts.seed,
        .checkpoint_every = opts.checkpoint_every,
        // Feature 2: gradient accumulation
        .grad_accum_steps = @max(1, opts.grad_accum),
        // Feature 3: schedule-free AdamW
        .use_schedule_free = opts.schedule_free,
        // Feature 1: XBM
        .xbm_capacity = opts.xbm_capacity,
        // Feature 5: NEFTune
        .neftune_alpha = opts.neftune_alpha,
        // Feature 6: LLRD
        .llrd_decay = opts.llrd_decay,
        // Feature 8: length bucketing
        .length_bucketing = opts.length_bucketing,
        .bucket_size = opts.bucket_size,
        // mixed precision
        .mixed_precision = opts.mixed_precision,
        // SPLADE
        .enable_splade = loss_config.enable_splade,
        .lambda_splade = loss_config.lambda_splade,
        .lambda_flops = loss_config.lambda_flops,
        .splade_focus_epoch = loss_config.splade_focus_epoch,
        // MRL
        .use_mrl = loss_config.use_mrl,
    };

    var trainer = try FusedTrainer.init(allocator, config, &cb);
    defer trainer.deinit();

    print("trainer ready  boundary_head hidden={d} mlp_dim={d}\n", .{
        config.hidden_size,
        config.boundary_mlp_dim,
    });

    // ------------------------------------------------------------------
    // 4a. Resume from checkpoint (if --resume-from was supplied)
    // ------------------------------------------------------------------
    if (opts.resume_from.len > 0) {
        print("resuming weights from {s}\n", .{opts.resume_from});
        try trainer.loadCheckpoint(allocator, opts.resume_from);

        // Look for a companion optimizer-state file next to the checkpoint.
        // Convention: replace the extension of the checkpoint path with
        // "_optimizer.safetensors", e.g.
        //   checkpoint_final.safetensors -> checkpoint_final_optimizer.safetensors
        var opt_state_path_buf: [512]u8 = undefined;
        const opt_state_path = blk: {
            // Strip a trailing .safetensors or .bin extension if present, then
            // append the optimizer-state suffix.
            const base = if (std.mem.endsWith(u8, opts.resume_from, ".safetensors"))
                opts.resume_from[0 .. opts.resume_from.len - ".safetensors".len]
            else if (std.mem.endsWith(u8, opts.resume_from, ".bin"))
                opts.resume_from[0 .. opts.resume_from.len - ".bin".len]
            else
                opts.resume_from;
            break :blk std.fmt.bufPrint(&opt_state_path_buf, "{s}_optimizer.safetensors", .{base}) catch null;
        };
        if (opt_state_path) |p| {
            const exists = compat.cwd().statFile(compat.io(), p, .{}) catch null;
            if (exists != null) {
                print("restoring optimizer state from {s}\n", .{p});
                trainer.loadOptimizerState(allocator, p) catch |err| {
                    print("warning: could not load optimizer state from {s}: {}\n", .{ p, err });
                };
            }
        }
    }

    // ------------------------------------------------------------------
    // 5. Training loop
    // ------------------------------------------------------------------

    // Build a mutable index array for shuffling
    var indices = try allocator.alloc(usize, samples.len);
    defer allocator.free(indices);
    for (indices, 0..) |*idx, i| idx.* = i;

    var prng = std.Random.DefaultPrng.init(opts.seed);
    const rng = prng.random();

    // Global step counter for NEFTune PRNG seeding — never resets across epochs.
    var global_neft_step: u64 = 0;

    if (tokenizer_opt == null) {
        print("warning: tokenizer not loaded — using dummy zero-fill token_fn; boundary labels will be inactive until encoder is wired\n", .{});
    }

    const max_chunks: usize = 32;
    const batch_sz: usize = @intCast(opts.batch_size);
    const max_seq: usize = @intCast(opts.max_seq_len);

    for (0..opts.epochs) |epoch| {
        // Shuffle indices using Fisher-Yates
        var i: usize = indices.len;
        while (i > 1) {
            i -= 1;
            const j = rng.uintLessThan(usize, i + 1);
            const tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }

        // Feature 8: Length bucketing — sort within windows after shuffle
        var bucketed_indices: ?[]usize = null;
        defer if (bucketed_indices) |b| allocator.free(b);
        const active_indices: []const usize = if (opts.length_bucketing) blk: {
            bucketed_indices = try fused_chunker_data.sortByLength(
                allocator,
                samples,
                indices,
                opts.bucket_size,
            );
            break :blk bucketed_indices.?;
        } else indices;

        var step: u32 = 0;
        var batch_start: usize = 0;

        while (batch_start < active_indices.len) {
            const batch_end = @min(batch_start + batch_sz, active_indices.len);
            const batch_indices = active_indices[batch_start..batch_end];
            batch_start = batch_end;

            // Assemble token batch — use real tokenizer when available, dummy otherwise.
            var batch: fused_chunker_data.FusedBatch = undefined;
            if (tokenizer_opt) |*tb| {
                var tok_ctx = tb.makeTokenFnCtx();
                batch = try fused_chunker_data.assembleTokenBatch(
                    allocator,
                    samples,
                    batch_indices,
                    max_seq,
                    max_chunks,
                    &tok_ctx,
                    TokenFnCtx.call,
                );
            } else {
                batch = try fused_chunker_data.assembleTokenBatch(
                    allocator,
                    samples,
                    batch_indices,
                    max_seq,
                    max_chunks,
                    {},
                    dummyTokenFn,
                );
            }
            defer batch.deinit(allocator);

            // TODO: hard_neg_ids are assembled but not yet used in the loss computation.
            // Wire them into the contrastive loss for improved training signal.
            // For now, free them immediately to avoid wasted memory.
            if (batch.hard_neg_ids) |ids| {
                allocator.free(ids);
                batch.hard_neg_ids = null;
            }
            if (batch.hard_neg_mask) |mask| {
                allocator.free(mask);
                batch.hard_neg_mask = null;
            }

            const actual_batch = batch.batch_size;
            // total_tokens: use allocated shape [actual_batch * max_seq] so tensors
            // stay valid regardless of how many real tokens the tokenizer produced.
            const total_tokens: usize = actual_batch * max_seq;

            // ------------------------------------------------------------------
            // Phase 3A: Pre-merge LoRA weights into WeightStore before encoder
            // forward so that the compute backend sees the merged weights.
            // Skip when linearLoRA is available: it applies the LoRA delta inline
            // without needing the base weights mutated (Fix 3).
            // ------------------------------------------------------------------
            var lora_originals = std.StringHashMapUnmanaged([]u8).empty;
            var lora_merged = false;
            if (cb.vtable.linearLoRA == null) {
                if (lora_adapters_opt) |*la| {
                    if (encoder_loaded) {
                        lora_originals = try mergeLoRAIntoWeights(allocator, &weight_store, la);
                        lora_merged = true;
                    }
                }
            }
            defer if (lora_merged) restoreLoRAWeights(allocator, &weight_store, &lora_originals);

            // ------------------------------------------------------------------
            // Fix 2: Register LoRA A/B tensors in the active WeightStore so that
            // modern_bert.zig can retrieve them via cb.getWeight for linearLoRA.
            // Key names: "model.layers.{N}.attn.{query,value}_proj.lora_{a,b}"
            // ------------------------------------------------------------------
            if (lora_adapters_opt) |*la| {
                if (encoder_loaded) {
                    const rank: usize = @intCast(la.config.rank);
                    for (la.layers) |*ll| {
                        const layer_n = ll.layer_idx;
                        const mod = ll.module_name;
                        // Only register tensors for LoRA-targeted projections.
                        const proj_name: []const u8 = if (std.mem.eql(u8, mod, "query_proj"))
                            "query_proj"
                        else if (std.mem.eql(u8, mod, "value_proj"))
                            "value_proj"
                        else
                            continue;

                        var key_buf_a: [128]u8 = undefined;
                        var key_buf_b: [128]u8 = undefined;
                        const key_a = try std.fmt.bufPrint(
                            &key_buf_a,
                            "model.layers.{d}.attn.{s}.lora_a",
                            .{ layer_n, proj_name },
                        );
                        const key_b = try std.fmt.bufPrint(
                            &key_buf_b,
                            "model.layers.{d}.attn.{s}.lora_b",
                            .{ layer_n, proj_name },
                        );

                        if (comptime build_options.enable_mlx) {
                            if (use_mlx) {
                                // MLX path: create mlx_array handles and insert into the map.
                                // A: [rank, in_features]
                                const shape_a = [2]i32{ @intCast(rank), @intCast(ll.in_features) };
                                const arr_a = mlx_mod.arrayFromFloat32(ll.A, &shape_a);
                                try mlx_mod.insertWeight(mlx_weight_store.resident_weights, allocator, key_a, arr_a);
                                // B: [out_features, rank]
                                const shape_b = [2]i32{ @intCast(ll.out_features), @intCast(rank) };
                                const arr_b = mlx_mod.arrayFromFloat32(ll.B, &shape_b);
                                try mlx_mod.insertWeight(mlx_weight_store.resident_weights, allocator, key_b, arr_b);
                            } else {
                                try insertLoRAIntoBlasStore(allocator, &weight_store, key_a, ll.A, rank, ll.in_features);
                                try insertLoRAIntoBlasStore(allocator, &weight_store, key_b, ll.B, ll.out_features, rank);
                            }
                        } else {
                            try insertLoRAIntoBlasStore(allocator, &weight_store, key_a, ll.A, rank, ll.in_features);
                            try insertLoRAIntoBlasStore(allocator, &weight_store, key_b, ll.B, ll.out_features, rank);
                        }
                    }
                }
            }

            // ------------------------------------------------------------------
            // Encoder forward pass (or zero-fill fallback)
            // ------------------------------------------------------------------
            var features_owned: ?[]f32 = null;
            defer if (features_owned) |f| allocator.free(f);

            var activations_opt: ?modern_bert.ActivationBuffer = null;
            defer if (activations_opt) |*ab| ab.deinit();

            if (encoder_loaded) {
                // Convert i32 token IDs → i64 for modern_bert.forward
                const ids_i64 = try allocator.alloc(i64, total_tokens);
                defer allocator.free(ids_i64);
                for (batch.input_ids[0..total_tokens], ids_i64) |id32, *id64| id64.* = @intCast(id32);

                const mask_i64 = try allocator.alloc(i64, total_tokens);
                defer allocator.free(mask_i64);
                for (batch.attention_mask[0..total_tokens], mask_i64) |m32, *m64| m64.* = @intCast(m32);

                const lora_alpha_for_config: f32 = if (lora_adapters_opt) |la| la.config.alpha else 0.0;
                const bert_config = modern_bert.Config{
                    .hidden_size = opts.hidden_size,
                    .intermediate_size = opts.intermediate_size,
                    .lora_rank = if (lora_adapters_opt != null) opts.lora_rank else 0,
                    .lora_alpha = lora_alpha_for_config,
                };

                if (lora_adapters_opt != null) {
                    var act_buf = modern_bert.ActivationBuffer.init(allocator);
                    const fwd = try modern_bert.forwardCapturingActivations(
                        &cb,
                        allocator,
                        bert_config,
                        ids_i64,
                        mask_i64,
                        actual_batch,
                        max_seq,
                        &act_buf,
                    );
                    features_owned = fwd;
                    activations_opt = act_buf;
                } else {
                    features_owned = try modern_bert.forward(
                        &cb,
                        allocator,
                        bert_config,
                        ids_i64,
                        mask_i64,
                        actual_batch,
                        max_seq,
                    );
                }
            }

            // Fallback: zero-fill when encoder is not loaded.
            var features_fallback: ?[]f32 = null;
            defer if (features_fallback) |f| allocator.free(f);

            var features: []const f32 = if (features_owned) |f| f else blk: {
                const zeros = try allocator.alloc(f32, total_tokens * @as(usize, opts.hidden_size));
                @memset(zeros, 0);
                features_fallback = zeros;
                break :blk zeros;
            };

            // Feature 5: NEFTune — add uniform noise to features during training.
            var neftune_buf: ?[]f32 = null;
            defer if (neftune_buf) |b| allocator.free(b);
            if (opts.neftune_alpha > 0.0 and total_tokens > 0) {
                const hidden_size_f = @as(f32, @floatFromInt(opts.hidden_size));
                const seq_len_f = @as(f32, @floatFromInt(total_tokens));
                const noise_scale = opts.neftune_alpha / @sqrt(seq_len_f * hidden_size_f);
                const noisy = try allocator.dupe(f32, features);
                neftune_buf = noisy;
                var neft_prng = std.Random.DefaultPrng.init(opts.seed ^ global_neft_step);
                global_neft_step += 1;
                const neft_rng = neft_prng.random();
                for (noisy) |*f| {
                    const noise = (neft_rng.float(f32) * 2.0 - 1.0) * noise_scale;
                    f.* += noise;
                }
                features = noisy;
            }

            // Attention mask: convert i32 -> f32
            const attn_mask_f32 = try allocator.alloc(f32, total_tokens);
            defer allocator.free(attn_mask_f32);
            for (batch.attention_mask[0..total_tokens], attn_mask_f32) |m, *out| {
                out.* = @floatFromInt(m);
            }

            // Boundary labels one-hot [total_tokens * 2]: build from batch.boundary_labels
            // batch.boundary_labels is [batch_size * max_seq_len] with 1.0 at boundary positions
            const boundary_labels_2 = try allocator.alloc(f32, total_tokens * 2);
            defer allocator.free(boundary_labels_2);
            for (0..total_tokens) |t| {
                const is_boundary = batch.boundary_labels[t] > 0.5;
                boundary_labels_2[t * 2 + 0] = if (is_boundary) 0.0 else 1.0; // class 0: non-boundary
                boundary_labels_2[t * 2 + 1] = if (is_boundary) 1.0 else 0.0; // class 1: boundary
            }

            // Placeholder chunk embeddings: zero-filled [B * max_chunks * hidden_size]
            const E: usize = @intCast(opts.hidden_size);
            const C: usize = max_chunks;
            const chunk_embed_len = actual_batch * C * E;
            const chunk_embeddings = try allocator.alloc(f32, chunk_embed_len);
            defer allocator.free(chunk_embeddings);
            @memset(chunk_embeddings, 0);

            // Chunk mask: [B * max_chunks] — use batch.chunk_mask directly
            // (already sized batch_size * max_chunks)
            const chunk_mask = batch.chunk_mask;

            // Doc IDs: [B * max_chunks] — assign each sample its own doc id
            const doc_ids = try allocator.alloc(u32, actual_batch * C);
            defer allocator.free(doc_ids);
            for (0..actual_batch) |b_idx| {
                for (0..C) |c_idx| {
                    doc_ids[b_idx * C + c_idx] = @intCast(b_idx);
                }
            }

            // ------------------------------------------------------------------
            // Training step + optional LoRA backprop
            // ------------------------------------------------------------------
            var summary: TrainStepSummary = undefined;

            if (lora_adapters_opt) |*la| {
                if (activations_opt) |*act_buf| {
                    // Use the gradient-returning variant so we can backprop into LoRA.
                    var result_with_grad = try trainer.trainStepWithEncoderGrad(
                        allocator,
                        features,
                        boundary_labels_2,
                        attn_mask_f32,
                        chunk_embeddings,
                        chunk_mask,
                        doc_ids,
                        total_tokens,
                        actual_batch,
                        C,
                        E,
                    );
                    defer result_with_grad.deinit(allocator);
                    summary = result_with_grad.summary;

                    if (result_with_grad.features_grad) |d_features| {
                        // Build parallel slices from ActivationBuffer.
                        const n_caps = act_buf.items.items.len;
                        const cap_layers = try allocator.alloc(u32, n_caps);
                        defer allocator.free(cap_layers);
                        const cap_modules = try allocator.alloc([]const u8, n_caps);
                        defer allocator.free(cap_modules);
                        const cap_inputs = try allocator.alloc([]const f32, n_caps);
                        defer allocator.free(cap_inputs);
                        const cap_in_feat = try allocator.alloc(usize, n_caps);
                        defer allocator.free(cap_in_feat);
                        const cap_out_feat = try allocator.alloc(usize, n_caps);
                        defer allocator.free(cap_out_feat);
                        for (act_buf.items.items, 0..) |cap, ci| {
                            cap_layers[ci] = cap.layer_idx;
                            cap_modules[ci] = cap.module_name;
                            cap_inputs[ci] = cap.input;
                            cap_in_feat[ci] = cap.in_features;
                            cap_out_feat[ci] = cap.out_features;
                        }

                        try segmented_encoder.backwardLoRADirect(
                            cb,
                            allocator,
                            cap_layers,
                            cap_modules,
                            cap_inputs,
                            cap_in_feat,
                            cap_out_feat,
                            d_features,
                            la.layers,
                            la.config.alpha,
                        );

                        // Apply optimizer steps for all LoRA parameters.
                        // Feature 4 (LoRA+): use lr * lora_plus_ratio for lora_B.
                        // Feature 6 (LLRD): use per-layer decayed learning rate.
                        const base_lr = summary.learning_rate;
                        const num_layers_f: f32 = @floatFromInt(la.config.num_layers);
                        for (la.layers) |*ll| {
                            // LLRD: layer 0 = first encoder layer (shallowest, closest to embeddings) → lowest LR
                            // layer N-1 = last encoder layer (deepest, closest to task head) → highest LR (= base_lr)
                            // Formula: lr[i] = base_lr * decay^(num_layers - 1 - i)
                            const layer_lr = if (opts.llrd_decay != 1.0) blk_lr: {
                                const exp_f: f32 = num_layers_f - 1.0 - @as(f32, @floatFromInt(ll.layer_idx));
                                break :blk_lr base_lr * std.math.pow(f32, opts.llrd_decay, exp_f);
                            } else base_lr;
                            const b_lr = layer_lr * la.config.lora_plus_ratio;
                            const a_key = try std.fmt.allocPrint(
                                allocator,
                                "lora.{d}.{s}.A",
                                .{ ll.layer_idx, ll.module_name },
                            );
                            defer allocator.free(a_key);
                            const b_key = try std.fmt.allocPrint(
                                allocator,
                                "lora.{d}.{s}.B",
                                .{ ll.layer_idx, ll.module_name },
                            );
                            defer allocator.free(b_key);
                            try optimizers.step(trainer.optimizer, &lora_opt_state, layer_lr, a_key, ll.A, ll.grad_A);
                            try optimizers.step(trainer.optimizer, &lora_opt_state, b_lr, b_key, ll.B, ll.grad_B);
                        }
                        la.zeroGrads();
                    }
                } else {
                    // LoRA enabled but no activations captured (encoder not loaded).
                    summary = try trainer.trainStep(
                        allocator,
                        features,
                        boundary_labels_2,
                        attn_mask_f32,
                        chunk_embeddings,
                        chunk_mask,
                        doc_ids,
                        total_tokens,
                        actual_batch,
                        C,
                        E,
                    );
                }
            } else {
                summary = try trainer.trainStep(
                    allocator,
                    features,
                    boundary_labels_2,
                    attn_mask_f32,
                    chunk_embeddings,
                    chunk_mask,
                    doc_ids,
                    total_tokens,
                    actual_batch,
                    C,
                    E,
                );
            }

            // ------------------------------------------------------------------
            // SPLADE training: forward + backward + AdamW update for W.
            // Activates only after splade_focus_epoch so boundary training
            // stabilises first.
            // ------------------------------------------------------------------
            if (opts.splade and encoder_loaded and epoch >= @as(usize, opts.splade_focus_epoch)) {
                if (splade_w) |w| {
                    // Build a fused_chunker Config with just the fields we need.
                    const splade_fused_config = fused_chunker_mod.Config{
                        .hidden_size = opts.hidden_size,
                        .splade_config = .{
                            .vocab_size = @intCast(splade_vocab_size),
                            .pooling = .max,
                        },
                    };

                    // Count valid chunks first so we can allocate compact arrays.
                    var num_valid_chunks: usize = 0;
                    for (0..actual_batch * C) |ci| {
                        if (chunk_mask[ci] > 0.5) num_valid_chunks += 1;
                    }

                    if (num_valid_chunks >= 2) {
                        // Compute per-chunk SPLADE vectors (compact: num_valid_chunks entries).
                        // features: [actual_batch * max_seq * hidden_size]
                        const splade_vecs = try fused_chunker_mod.computeChunkSpladeVectors(
                            allocator,
                            splade_fused_config,
                            features,
                            w,
                            batch.chunk_starts,
                            batch.chunk_ends,
                            chunk_mask,
                            actual_batch,
                            max_seq,
                            C,
                        );
                        defer allocator.free(splade_vecs);

                        // Build compact all-ones mask and compact doc_ids for the
                        // contrastive loss (splade_vecs is already compact).
                        const compact_mask = try allocator.alloc(f32, num_valid_chunks);
                        defer allocator.free(compact_mask);
                        @memset(compact_mask, 1.0);

                        const compact_doc_ids = try allocator.alloc(u32, num_valid_chunks);
                        defer allocator.free(compact_doc_ids);
                        var vi_fill: usize = 0;
                        for (0..actual_batch) |b_idx| {
                            for (0..C) |c_idx| {
                                if (chunk_mask[b_idx * C + c_idx] > 0.5) {
                                    compact_doc_ids[vi_fill] = @intCast(b_idx);
                                    vi_fill += 1;
                                }
                            }
                        }

                        // Contrastive loss + gradient w.r.t. splade_vecs.
                        var splade_contrastive = try fused_chunker_splade.computeSpladeContrastiveLoss(
                            allocator,
                            splade_vecs,
                            compact_mask,
                            compact_doc_ids,
                            num_valid_chunks,
                            @intCast(splade_vocab_size),
                            (fused_chunker_loss.FusedLossConfig{}).temperature,
                        );
                        defer splade_contrastive.deinit(allocator);

                        // FLOPS regularization loss + gradient.
                        const flops_grad = try allocator.alloc(f32, num_valid_chunks * splade_vocab_size);
                        defer allocator.free(flops_grad);
                        const flops_loss = fused_chunker_splade.computeSpladeFlopsLoss(
                            splade_vecs,
                            flops_grad,
                            num_valid_chunks,
                            @intCast(splade_vocab_size),
                            opts.lambda_flops,
                        );
                        _ = flops_loss;

                        // Combined gradient: lambda_splade * contrastive_grad + flops_grad.
                        const combined_grad = try allocator.alloc(f32, num_valid_chunks * splade_vocab_size);
                        defer allocator.free(combined_grad);
                        for (combined_grad, splade_contrastive.grad, flops_grad) |*cg, sg, fg| {
                            cg.* = opts.lambda_splade * sg + fg;
                        }

                        // Backprop through SPLADE to get dL/dW.
                        // Re-run forward with info per chunk to get argmax tokens.
                        const dW = try allocator.alloc(f32, splade_vocab_size * @as(usize, opts.hidden_size));
                        defer allocator.free(dW);
                        @memset(dW, 0);

                        var valid_chunk_idx: usize = 0;
                        for (0..actual_batch) |b_idx| {
                            for (0..C) |c_idx| {
                                const mask_val = chunk_mask[b_idx * C + c_idx];
                                if (mask_val < 0.5) continue;

                                const tok_start: usize = @intCast(@max(0, batch.chunk_starts[b_idx * C + c_idx]));
                                const tok_end: usize = @min(
                                    @as(usize, @intCast(@max(0, batch.chunk_ends[b_idx * C + c_idx]))),
                                    max_seq,
                                );
                                if (tok_start >= tok_end) {
                                    valid_chunk_idx += 1;
                                    continue;
                                }

                                const chunk_tokens = tok_end - tok_start;
                                const H: usize = opts.hidden_size;
                                const hidden_offset = b_idx * max_seq * H + tok_start * H;
                                const chunk_hidden = features[hidden_offset .. hidden_offset + chunk_tokens * H];

                                var info = try fused_chunker_splade.computeSpladeActivationWithInfo(
                                    allocator,
                                    chunk_hidden,
                                    w,
                                    chunk_tokens,
                                    H,
                                    @intCast(splade_vocab_size),
                                );
                                defer info.deinit();

                                const chunk_grad = combined_grad[valid_chunk_idx * splade_vocab_size .. (valid_chunk_idx + 1) * splade_vocab_size];
                                fused_chunker_splade.backwardSpladeWeight(chunk_grad, &info, chunk_hidden, H, dW);

                                valid_chunk_idx += 1;
                            }
                        }

                        // AdamW update for W.
                        splade_adam_step += 1;
                        const splade_lr = summary.learning_rate;
                        if (splade_lr > 0) {
                            const beta1: f32 = 0.9;
                            const beta2: f32 = 0.999;
                            const eps: f32 = 1e-8;
                            const t_f: f32 = @floatFromInt(splade_adam_step);
                            const bc1: f32 = 1.0 - std.math.pow(f32, beta1, t_f);
                            const bc2: f32 = 1.0 - std.math.pow(f32, beta2, t_f);
                            for (w, splade_adam_m.?, splade_adam_v.?, dW) |*wi, *mi, *vi, gi| {
                                mi.* = beta1 * mi.* + (1.0 - beta1) * gi;
                                vi.* = beta2 * vi.* + (1.0 - beta2) * gi * gi;
                                const m_hat = mi.* / bc1;
                                const v_hat = vi.* / bc2;
                                wi.* -= splade_lr * m_hat / (@sqrt(v_hat) + eps);
                            }
                        }

                        print("  splade_loss: {d:.4}\n", .{splade_contrastive.loss});
                    }
                }
            }

            step += 1;

            print(
                "epoch {d}/{d} step {d} | loss {d:.4} | boundary {d:.4} | contrastive {d:.4} | lr {d}\n",
                .{
                    epoch + 1,
                    opts.epochs,
                    summary.step,
                    summary.total_loss,
                    summary.boundary_loss,
                    summary.contrastive_loss,
                    summary.learning_rate,
                },
            );
        }

        print("epoch {d}/{d} done  steps={d}\n", .{ epoch + 1, opts.epochs, step });

        // Flush any partial gradient accumulation window left at epoch end.
        try trainer.flushEpochEnd(allocator);

        // Optional checkpoint save
        if (opts.checkpoint_every > 0 and (epoch + 1) % opts.checkpoint_every == 0) {
            var path_buf: [512]u8 = undefined;
            const ckpt_path = try std.fmt.bufPrint(&path_buf, "{s}/checkpoint_epoch_{d}.safetensors", .{
                opts.output_dir,
                epoch + 1,
            });
            try trainer.saveCheckpoint(allocator, ckpt_path);
            print("checkpoint saved to {s}\n", .{ckpt_path});

            if (opts.save_optimizer_state) {
                var opt_path_buf: [512]u8 = undefined;
                const opt_path = try std.fmt.bufPrint(&opt_path_buf, "{s}/checkpoint_epoch_{d}_optimizer.safetensors", .{
                    opts.output_dir,
                    epoch + 1,
                });
                try trainer.saveOptimizerState(allocator, opt_path);
                print("optimizer state saved to {s}\n", .{opt_path});
            }

            // Save SPLADE projection weight W if training is active.
            if (splade_w) |w| {
                const splade_ckpt_path = try std.fmt.allocPrint(allocator, "{s}/splade_w_epoch_{d}.safetensors", .{ opts.output_dir, epoch + 1 });
                defer allocator.free(splade_ckpt_path);
                const splade_tensors = [_]safetensors_checkpoint.NamedTensor{
                    .{ .name = "splade_proj_weight", .data = w, .shape = &.{ splade_vocab_size, @as(usize, opts.hidden_size) } },
                };
                try safetensors_checkpoint.save(allocator, splade_ckpt_path, &splade_tensors);
                print("SPLADE weight saved to {s}\n", .{splade_ckpt_path});
            }
        }
    }

    // ------------------------------------------------------------------
    // 6. Save final checkpoint
    // ------------------------------------------------------------------
    var final_buf: [512]u8 = undefined;
    const final_path = try std.fmt.bufPrint(&final_buf, "{s}/checkpoint_final.safetensors", .{opts.output_dir});
    try trainer.saveCheckpoint(allocator, final_path);
    print("final checkpoint saved to {s}\n", .{final_path});

    if (opts.save_optimizer_state) {
        var opt_final_buf: [512]u8 = undefined;
        const opt_final_path = try std.fmt.bufPrint(&opt_final_buf, "{s}/checkpoint_final_optimizer.safetensors", .{opts.output_dir});
        try trainer.saveOptimizerState(allocator, opt_final_path);
        print("optimizer state saved to {s}\n", .{opt_final_path});
    }

    // Save final SPLADE projection weight W.
    if (splade_w) |w| {
        const splade_final_path = try std.fmt.allocPrint(allocator, "{s}/splade_w_final.safetensors", .{opts.output_dir});
        defer allocator.free(splade_final_path);
        const splade_tensors = [_]safetensors_checkpoint.NamedTensor{
            .{ .name = "splade_proj_weight", .data = w, .shape = &.{ splade_vocab_size, @as(usize, opts.hidden_size) } },
        };
        try safetensors_checkpoint.save(allocator, splade_final_path, &splade_tensors);
        print("SPLADE weight saved to {s}\n", .{splade_final_path});
    }

    print("training complete\n", .{});
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

fn printUsage() void {
    print(
        \\usage: train-fused-chunker --data <path> --output <dir> [options]
        \\
        \\  --data <path>             JSONL data path (file or directory)
        \\  --output <dir>            Output directory for checkpoints
        \\  --model-dir <dir>         Model directory (tokenizer + encoder weights)
        \\  --epochs <n>              Number of epochs (default: 10)
        \\  --batch-size <n>          Batch size (default: 16)
        \\  --lr <f>                  Learning rate (default: 1e-4)
        \\  --hidden-size <n>         Encoder hidden size (default: 768)
        \\  --max-seq-len <n>         Max token sequence length (default: 384)
        \\  --checkpoint-every <n>    Save checkpoint every N epochs (0=disabled)
        \\  --split <name>            Dataset split name filter (default: "train")
        \\  --seed <n>                Random seed (default: 42)
        \\  --lora-rank <n>           LoRA rank (default: 0 = disabled)
        \\  --intermediate-size <n>   ModernBERT intermediate_size (default: 1152)
        \\  --backend native|mlx|auto   Compute backend (default: auto)
        \\  --grad-accum <n>          Gradient accumulation steps (default: 1)
        \\  --schedule-free           Use Schedule-Free AdamW
        \\  --neftune-alpha <f>       NEFTune noise magnitude (default: 0.0=disabled)
        \\  --xbm-capacity <n>        Cross-Batch Memory capacity (default: 0=disabled)
        \\  --llrd-decay <f>          Layer-wise LR decay (default: 1.0=disabled)
        \\  --lora-plus-ratio <f>     LoRA+ B/A LR ratio (default: 1.0=disabled)
        \\  --length-bucketing        Enable length bucketing
        \\  --bucket-size <n>         Bucket window size (default: 256)
        \\  --mixed-precision         Enable bf16 mixed precision (MLX only)
        \\  --splade                  Enable SPLADE sparse embedding head
        \\  --lambda-splade <f>       SPLADE contrastive loss weight (default: 0.15)
        \\  --lambda-flops <f>        SPLADE FLOPS regularization weight (default: 3e-5)
        \\  --splade-focus-epoch <n>  Epoch when SPLADE activates (default: 4)
        \\  --mrl                     Enable Matryoshka Representation Learning
        \\  --mrl-dims <s>            Comma-separated MRL dims (default: "768,256,128")
        \\  --resume-from <path>      Resume training from a checkpoint file
        \\  --save-optimizer-state    Save Adam optimizer state alongside each checkpoint
        \\
    , .{});
}
