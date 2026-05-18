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

// Eval binary for the fused chunker-embedder boundary head.
//
// Usage:
//   eval_fused_chunker --data <path> --checkpoint <file> [options]
//
// Options:
//   --data <path>        JSONL eval data path (file or directory)
//   --checkpoint <file>  Checkpoint file written by train_fused_chunker
//   --split <name>       Dataset split filter (default: "val")
//   --batch-size <n>     Batch size (default: 32)
//   --hidden-size <n>    Hidden size (default: 768)
//   --max-seq-len <n>    Max seq len (default: 384)
//   --max-chunks <n>     Max chunks per sample (default: 32)
//   --backend blas|mlx|auto  Compute backend (default: auto)

const std = @import("std");
const build_options = @import("build_options");
const blas_compute = @import("../../ops/blas_compute.zig");
const mlx_compute_mod = if (build_options.enable_mlx) @import("../../ops/mlx_compute.zig") else struct {
    pub const MlxCompute = void;
    pub const WeightStore = void;
};
const mlx_mod = if (build_options.enable_mlx) @import("../../backends/mlx.zig") else struct {
    pub const c = struct {
        pub fn mlx_map_string_to_array_new() void {}
    };
    pub fn openDefaultStream() struct { stream: void } {
        return .{ .stream = {} };
    }
};
const ops_mod = @import("../../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const fused_chunker_data = @import("../fused_chunker_data.zig");
const fused_chunker_train = @import("../fused_chunker_train.zig");
const FusedTrainer = fused_chunker_train.FusedTrainer;
const FusedTrainingConfig = fused_chunker_train.FusedTrainingConfig;

const print = std.debug.print;

// ---------------------------------------------------------------------------
// Dense retrieval evaluation metrics
// ---------------------------------------------------------------------------

pub const RetrievalMetrics = struct {
    recall_at_1: f64,
    recall_at_10: f64,
    mrr: f64,
    num_queries: usize,
};

/// Compute retrieval metrics from chunk embeddings.
///
/// embeddings:  [num_chunks * embed_dim] — chunk embeddings (need not be pre-normalised)
/// doc_ids:     [num_chunks] — chunks sharing a doc_id are positives
/// chunk_mask:  [num_chunks] — 1.0 valid, 0.0 padding
/// num_chunks:  total number of chunks (including padding)
/// embed_dim:   embedding dimension
///
/// Algorithm (O(V²) where V = number of valid chunks):
///   1. Filter to valid chunks (chunk_mask > 0).
///   2. L2-normalise each valid embedding.
///   3. For each query i: compute dot-product similarity to all other valid chunks,
///      rank by descending similarity (excluding self), find positives (same doc_id).
///   4. Accumulate Recall@1, Recall@10, MRR; average over queries with at least one positive.
pub fn computeRetrievalMetrics(
    allocator: std.mem.Allocator,
    embeddings: []const f32,
    doc_ids: []const u32,
    chunk_mask: []const f32,
    num_chunks: usize,
    embed_dim: usize,
) !RetrievalMetrics {
    // Step 1: collect valid indices.
    var valid_idx = std.ArrayListUnmanaged(usize).empty;
    defer valid_idx.deinit(allocator);

    for (0..num_chunks) |i| {
        if (chunk_mask[i] > 0.5) {
            try valid_idx.append(allocator, i);
        }
    }
    const V = valid_idx.items.len;

    if (V < 2) {
        return RetrievalMetrics{
            .recall_at_1 = 0,
            .recall_at_10 = 0,
            .mrr = 0,
            .num_queries = 0,
        };
    }

    // Step 2: L2-normalise valid embeddings into a compact [V * embed_dim] buffer.
    const norm_vecs = try allocator.alloc(f32, V * embed_dim);
    defer allocator.free(norm_vecs);

    const compact_doc_ids = try allocator.alloc(u32, V);
    defer allocator.free(compact_doc_ids);

    for (0..V) |ci| {
        const orig_i = valid_idx.items[ci];
        compact_doc_ids[ci] = doc_ids[orig_i];

        const src = embeddings[orig_i * embed_dim .. orig_i * embed_dim + embed_dim];
        const dst = norm_vecs[ci * embed_dim .. ci * embed_dim + embed_dim];

        var sum_sq: f32 = 0;
        for (src) |v| sum_sq += v * v;
        const inv_norm: f32 = if (sum_sq > 1e-24) 1.0 / @sqrt(sum_sq) else 0;
        for (src, dst) |s, *d| d.* = s * inv_norm;
    }

    // Step 3: for each query, compute similarities, rank, accumulate metrics.
    const sims = try allocator.alloc(f32, V);
    defer allocator.free(sims);

    // Reusable scratch buffer for sorting indices.
    const rank_buf = try allocator.alloc(usize, V);
    defer allocator.free(rank_buf);

    var sum_r1: f64 = 0;
    var sum_r10: f64 = 0;
    var sum_mrr: f64 = 0;
    var num_queries: usize = 0;

    for (0..V) |qi| {
        // Check whether this query has any positives.
        var has_positive = false;
        for (0..V) |j| {
            if (j != qi and compact_doc_ids[j] == compact_doc_ids[qi]) {
                has_positive = true;
                break;
            }
        }
        if (!has_positive) continue;

        // Compute dot-product similarities to all other valid chunks.
        const qi_base = qi * embed_dim;
        for (0..V) |j| {
            if (j == qi) {
                sims[j] = -std.math.inf(f32); // exclude self from ranking
                continue;
            }
            const qj_base = j * embed_dim;
            var dot: f32 = 0;
            for (0..embed_dim) |k| {
                dot += norm_vecs[qi_base + k] * norm_vecs[qj_base + k];
            }
            sims[j] = dot;
        }

        // Build rank buffer [0..V] and partial-sort to find the top-10.
        // We use a simple selection approach: track the top-K indices by sim value.
        // For V up to ~512 a full sort is fine.
        for (0..V) |k| rank_buf[k] = k;
        // Sort descending by similarity.
        std.sort.pdq(usize, rank_buf, sims, struct {
            fn lessThan(sim_slice: []const f32, a: usize, b: usize) bool {
                return sim_slice[a] > sim_slice[b]; // descending
            }
        }.lessThan);

        // Recall@1
        if (compact_doc_ids[rank_buf[0]] == compact_doc_ids[qi]) {
            sum_r1 += 1.0;
        }

        // Recall@10 and MRR
        var found_r10 = false;
        var first_positive_rank: usize = 0; // 1-based, 0 means not found yet
        const top_k = @min(V - 1, 10); // at most V-1 non-self results
        for (0..top_k) |rank_idx| {
            const j = rank_buf[rank_idx];
            if (compact_doc_ids[j] == compact_doc_ids[qi]) {
                if (!found_r10) {
                    sum_r10 += 1.0;
                    found_r10 = true;
                }
                if (first_positive_rank == 0) {
                    first_positive_rank = rank_idx + 1; // 1-based
                }
            }
        }
        // If the first positive wasn't in top-10, search the rest for MRR.
        if (first_positive_rank == 0) {
            for (10..V - 1) |rank_idx| {
                const j = rank_buf[rank_idx];
                if (compact_doc_ids[j] == compact_doc_ids[qi]) {
                    first_positive_rank = rank_idx + 1;
                    break;
                }
            }
        }
        if (first_positive_rank > 0) {
            sum_mrr += 1.0 / @as(f64, @floatFromInt(first_positive_rank));
        }

        num_queries += 1;
    }

    if (num_queries == 0) {
        return RetrievalMetrics{
            .recall_at_1 = 0,
            .recall_at_10 = 0,
            .mrr = 0,
            .num_queries = 0,
        };
    }

    const nq_f: f64 = @floatFromInt(num_queries);
    return RetrievalMetrics{
        .recall_at_1 = sum_r1 / nq_f,
        .recall_at_10 = sum_r10 / nq_f,
        .mrr = sum_mrr / nq_f,
        .num_queries = num_queries,
    };
}

const Options = struct {
    data_path: []const u8,
    checkpoint_path: []const u8,
    split: []const u8 = "val",
    batch_size: u32 = 32,
    hidden_size: u32 = 768,
    max_seq_len: u32 = 384,
    max_chunks: u32 = 32,
    backend: enum { native, mlx, auto } = .auto,
};

fn printUsage() void {
    print(
        \\Usage: eval-fused-chunker --data <path> --checkpoint <file> [options]
        \\
        \\Options:
        \\  --data <path>            JSONL eval data path (file or directory)
        \\  --checkpoint <file>      Checkpoint file written by train_fused_chunker
        \\  --split <name>           Dataset split filter (default: "val")
        \\  --batch-size <n>         Batch size (default: 32)
        \\  --hidden-size <n>        Hidden size (default: 768)
        \\  --max-seq-len <n>        Max seq len (default: 384)
        \\  --max-chunks <n>         Max chunks per sample (default: 32)
        \\  --backend blas|mlx|auto  Compute backend (default: auto)
        \\
    , .{});
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next(); // skip argv[0]

    var data_path: ?[]const u8 = null;
    var checkpoint_path: ?[]const u8 = null;
    var split: []const u8 = "val";
    var batch_size: u32 = 32;
    var hidden_size: u32 = 768;
    var max_seq_len: u32 = 384;
    var max_chunks: u32 = 32;
    var backend: @TypeOf((Options{
        .data_path = "",
        .checkpoint_path = "",
    }).backend) = .auto;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--data")) {
            data_path = args.next() orelse {
                print("error: --data requires a value\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--checkpoint")) {
            checkpoint_path = args.next() orelse {
                print("error: --checkpoint requires a value\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--split")) {
            split = args.next() orelse {
                print("error: --split requires a value\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            const value = args.next() orelse {
                print("error: --batch-size requires a value\n", .{});
                std.process.exit(1);
            };
            batch_size = std.fmt.parseUnsigned(u32, value, 10) catch {
                print("error: invalid --batch-size value: {s}\n", .{value});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--hidden-size")) {
            const value = args.next() orelse {
                print("error: --hidden-size requires a value\n", .{});
                std.process.exit(1);
            };
            hidden_size = std.fmt.parseUnsigned(u32, value, 10) catch {
                print("error: invalid --hidden-size value: {s}\n", .{value});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--max-seq-len")) {
            const value = args.next() orelse {
                print("error: --max-seq-len requires a value\n", .{});
                std.process.exit(1);
            };
            max_seq_len = std.fmt.parseUnsigned(u32, value, 10) catch {
                print("error: invalid --max-seq-len value: {s}\n", .{value});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--max-chunks")) {
            const value = args.next() orelse {
                print("error: --max-chunks requires a value\n", .{});
                std.process.exit(1);
            };
            max_chunks = std.fmt.parseUnsigned(u32, value, 10) catch {
                print("error: invalid --max-chunks value: {s}\n", .{value});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--backend")) {
            const value = args.next() orelse {
                print("error: --backend requires a value\n", .{});
                std.process.exit(1);
            };
            if (std.mem.eql(u8, value, "blas")) {
                backend = .native;
            } else if (std.mem.eql(u8, value, "mlx")) {
                backend = .mlx;
            } else if (std.mem.eql(u8, value, "auto")) {
                backend = .auto;
            } else {
                print("error: unknown backend '{s}': expected blas, mlx, or auto\n", .{value});
                std.process.exit(1);
            }
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            print("error: unknown argument: {s}\n", .{arg});
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
        .checkpoint_path = checkpoint_path orelse {
            print("error: --checkpoint is required\n", .{});
            printUsage();
            std.process.exit(1);
        },
        .split = split,
        .batch_size = batch_size,
        .hidden_size = hidden_size,
        .max_seq_len = max_seq_len,
        .max_chunks = max_chunks,
        .backend = backend,
    };

    const use_mlx = switch (opts.backend) {
        .mlx => true,
        .native => false,
        .auto => build_options.enable_mlx,
    };

    if (use_mlx and !build_options.enable_mlx) {
        print("error: MLX support not compiled in\n", .{});
        std.process.exit(1);
    }

    // Set up compute backend (WeightStore is empty for eval — features are zero-filled)
    var weight_store = blas_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };

    var blas_backend = blas_compute.BlasCompute.init(allocator, &weight_store, null);

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
            mlx_backend = try mlx_compute_mod.MlxCompute.init(allocator, &mlx_weight_store, null);
            break :blk mlx_backend.computeBackend();
        } else {
            break :blk blas_backend.computeBackend();
        }
    } else blas_backend.computeBackend();

    print("backend: {s}\n", .{if (use_mlx) "mlx" else "blas"});

    // Set up trainer (owns the boundary head weights)
    const config = FusedTrainingConfig{
        .hidden_size = opts.hidden_size,
        .max_seq_len = opts.max_seq_len,
        .max_chunks = opts.max_chunks,
        .batch_size = opts.batch_size,
    };
    var trainer = try FusedTrainer.init(allocator, config, &cb);
    defer trainer.deinit();

    // Load checkpoint
    trainer.loadCheckpoint(allocator, opts.checkpoint_path) catch |err| {
        print("error: failed to load checkpoint '{s}': {}\n", .{ opts.checkpoint_path, err });
        std.process.exit(1);
    };

    // Load eval samples
    var loaded = fused_chunker_data.loadSamples(allocator, opts.data_path, opts.split) catch |err| {
        print("error: failed to load eval data from '{s}': {}\n", .{ opts.data_path, err });
        std.process.exit(1);
    };
    defer loaded.deinit();

    const samples = loaded.samples;
    if (samples.len == 0) {
        print("error: no samples found in '{s}' for split '{s}'\n", .{ opts.data_path, opts.split });
        std.process.exit(1);
    }

    print("Loaded {d} eval samples from '{s}' (split='{s}')\n", .{ samples.len, opts.data_path, opts.split });

    // Build lists of feature/label/mask batches for trainer.evaluate()
    var features_list = std.ArrayListUnmanaged([]const f32).empty;
    defer {
        for (features_list.items) |f| allocator.free(f);
        features_list.deinit(allocator);
    }

    var labels_list = std.ArrayListUnmanaged([]const f32).empty;
    defer {
        for (labels_list.items) |l| allocator.free(l);
        labels_list.deinit(allocator);
    }

    var mask_list = std.ArrayListUnmanaged([]const f32).empty;
    defer {
        for (mask_list.items) |m| allocator.free(m);
        mask_list.deinit(allocator);
    }

    var total_tokens_list = std.ArrayListUnmanaged(usize).empty;
    defer total_tokens_list.deinit(allocator);

    // Accumulate chunk-level data for dense retrieval evaluation.
    // chunk_embeddings_all: flat [total_chunks * hs] — zero-filled in this binary
    //   (real embeddings would come from an encoder forward pass).
    // chunk_mask_all:       flat [total_chunks] — 1.0 valid
    // chunk_doc_ids_all:    flat [total_chunks] — global sample index for each chunk
    var chunk_embeddings_all = std.ArrayListUnmanaged(f32).empty;
    defer chunk_embeddings_all.deinit(allocator);

    var chunk_mask_all = std.ArrayListUnmanaged(f32).empty;
    defer chunk_mask_all.deinit(allocator);

    var chunk_doc_ids_all = std.ArrayListUnmanaged(u32).empty;
    defer chunk_doc_ids_all.deinit(allocator);

    // Batch up samples
    const bs: usize = @intCast(opts.batch_size);
    const msl: usize = @intCast(opts.max_seq_len);
    const mc: usize = @intCast(opts.max_chunks);
    const hs: usize = @intCast(opts.hidden_size);

    // Dummy tokeniser: fills ids/mask with zeros, returns max_seq_len tokens
    const dummy_token_fn = struct {
        fn call(
            _ctx: void,
            text: []const u8,
            out_ids: []i32,
            out_mask: []i32,
            out_offsets: ?[][2]u32,
        ) usize {
            _ = _ctx;
            _ = text;
            @memset(out_ids, 0);
            @memset(out_mask, 1);
            if (out_offsets) |off| @memset(off, .{ 0, 0 });
            return out_ids.len;
        }
    }.call;

    var sample_idx: usize = 0;
    while (sample_idx < samples.len) {
        const end = @min(sample_idx + bs, samples.len);
        const count = end - sample_idx;

        // Build index slice for this batch
        const indices = try allocator.alloc(usize, count);
        defer allocator.free(indices);
        for (0..count) |k| indices[k] = sample_idx + k;

        var batch = try fused_chunker_data.assembleTokenBatch(
            allocator,
            samples,
            indices,
            msl,
            mc,
            {},
            dummy_token_fn,
        );
        defer batch.deinit(allocator);

        // total tokens in this batch = batch_size * max_seq_len (all tokens active)
        const total_tokens: usize = count * msl;

        // Zero-filled encoder output placeholder: [total_tokens * hidden_size]
        const features = try allocator.alloc(f32, total_tokens * hs);
        @memset(features, 0.0);
        try features_list.append(allocator, features);

        // Build one-hot boundary labels [total_tokens * 2] from flat boundary_labels
        const labels = try allocator.alloc(f32, total_tokens * 2);
        for (0..total_tokens) |t| {
            const is_boundary = batch.boundary_labels[t] > 0.5;
            labels[t * 2 + 0] = if (is_boundary) 0.0 else 1.0;
            labels[t * 2 + 1] = if (is_boundary) 1.0 else 0.0;
        }
        try labels_list.append(allocator, labels);

        // Build f32 attention mask [total_tokens] from i32 attention_mask
        const mask = try allocator.alloc(f32, total_tokens);
        for (0..total_tokens) |t| {
            mask[t] = if (batch.attention_mask[t] != 0) 1.0 else 0.0;
        }
        try mask_list.append(allocator, mask);

        try total_tokens_list.append(allocator, total_tokens);

        // Accumulate chunk-level data for retrieval evaluation.
        // Chunk embeddings are zero-filled here (no encoder in eval binary);
        // computeRetrievalMetrics will be skipped when embeddings are all-zero.
        const num_chunks_batch = count * mc;
        // Zero-filled embeddings: [num_chunks_batch * hs]
        try chunk_embeddings_all.appendNTimes(allocator, 0.0, num_chunks_batch * hs);
        // chunk_mask from the assembled batch
        try chunk_mask_all.appendSlice(allocator, batch.chunk_mask[0..num_chunks_batch]);
        // doc_ids: the global sample index for each chunk position in the batch.
        // Chunk position c within the batch belongs to sample (sample_idx + c / mc).
        for (0..num_chunks_batch) |c| {
            const global_sample = sample_idx + c / mc;
            try chunk_doc_ids_all.append(allocator, @intCast(global_sample));
        }

        sample_idx = end;
    }

    // Run evaluation
    const summary = try trainer.evaluate(
        allocator,
        features_list.items,
        labels_list.items,
        mask_list.items,
        total_tokens_list.items,
    );

    // Print results
    print("\n=== Eval Results ===\n", .{});
    print("Checkpoint:  {s}\n", .{opts.checkpoint_path});
    print("Data:        {s}\n", .{opts.data_path});
    print("Batches:     {d}\n", .{summary.num_batches});
    print("F1:          {d:.4}\n", .{summary.boundary_f1});
    print("Precision:   {d:.4}\n", .{summary.boundary_precision});
    print("Recall:      {d:.4}\n", .{summary.boundary_recall});

    // Dense retrieval evaluation.
    // Only run when chunk embeddings are non-zero.  In this eval binary the encoder
    // is not present so features are zero-filled; skip silently in that case.
    // When integrated with a real encoder the embeddings will be non-zero and this
    // block will automatically activate.
    const emb_slice = chunk_embeddings_all.items;
    var emb_sum: f32 = 0;
    for (emb_slice) |v| emb_sum += @abs(v);

    if (emb_sum > 0.0) {
        const total_chunks_all = chunk_mask_all.items.len;
        const retrieval = try computeRetrievalMetrics(
            allocator,
            emb_slice,
            chunk_doc_ids_all.items,
            chunk_mask_all.items,
            total_chunks_all,
            hs,
        );
        print("\n=== Dense Retrieval Metrics ===\n", .{});
        print("Queries:     {d}\n", .{retrieval.num_queries});
        print("Recall@1:    {d:.4}\n", .{retrieval.recall_at_1});
        print("Recall@10:   {d:.4}\n", .{retrieval.recall_at_10});
        print("MRR:         {d:.4}\n", .{retrieval.mrr});
    } else {
        // Embeddings are zero-filled (no encoder in this eval binary).
        // Retrieval metrics require real chunk embeddings from an encoder forward pass.
        print("\n(Dense retrieval metrics skipped: chunk embeddings are zero-filled)\n", .{});
    }
}
