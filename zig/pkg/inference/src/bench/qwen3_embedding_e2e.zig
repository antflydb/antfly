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

// Pretokenized Qwen3-Embedding encoder benchmark. Model load and tokenization
// are outside the timed region: synthetic token ids are fed straight into
// EmbeddingPipeline.embedTokenized. Supports a rectangular mode (--batch and
// --seq-len) and a ragged mode (--lengths CSV, padded to the batch max) so
// padding waste is measurable via real vs padded tok/s.

const std = @import("std");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const backends = inference.backends;
const model_manager_mod = inference.server.model_manager;
const native_backend_guard = inference.native_backend_guard;
const metal_runtime = inference.metal_runtime;

extern fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Qwen3-Embedding tokenizer vocabulary size; synthetic ids must stay below it.
const qwen3_vocab_size: i64 = 151669;
/// Qwen3 EOS token id (<|endoftext|>); last-token pooling reads this position.
const qwen3_eos_id: i64 = 151643;

const BackendChoice = enum { native, metal, cuda };

const Options = struct {
    model_dir: []const u8 = "",
    backend: BackendChoice = .metal,
    batch: usize = 1,
    seq_len: usize = 256,
    lengths: []usize = &.{},
    warmup_iters: usize = 2,
    measure_iters: usize = 10,
    print_embedding: bool = false,
    show_help: bool = false,
};

const Timing = struct {
    avg_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    min_ns: u64,
    max_ns: u64,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    var opts = try parseArgs(allocator, init);
    if (opts.show_help) {
        printUsage();
        return;
    }
    if (opts.model_dir.len == 0) {
        if (getenv("ANTFLY_INFERENCE_QWEN3_EMBEDDING_MODEL")) |env_dir| {
            opts.model_dir = std.mem.span(env_dir);
        }
    }
    // Ragged mode: --lengths overrides --batch/--seq-len; pad to the batch max.
    if (opts.lengths.len != 0) {
        opts.batch = opts.lengths.len;
        var max_len: usize = 0;
        for (opts.lengths) |len| {
            if (len == 0) return error.InvalidLengths;
            max_len = @max(max_len, len);
        }
        opts.seq_len = max_len;
    }
    if (opts.model_dir.len == 0 or opts.batch == 0 or opts.seq_len == 0 or opts.measure_iters == 0) {
        printUsage();
        return error.InvalidArguments;
    }
    try ensureBackendAvailable(opts.backend);

    var session_manager = backends.SessionManager.initWithIo(allocator, init.io);
    session_manager.preferred_backends = switch (opts.backend) {
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
        .cuda => &.{backends.BackendType.cuda},
    };

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const model = model_manager.loadFromDir(opts.model_dir) catch |err| {
        std.debug.print("qwen3_embedding_e2e: model_load_error={s}\n", .{@errorName(err)});
        return err;
    };
    try model.ensureEmbeddingAssets(true, false, false);
    const expected_backend: backends.BackendType = switch (opts.backend) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
    };
    if (model.session.backend() != expected_backend) return error.UnexpectedBackend;

    var pipeline = model.embeddingPipeline(allocator);
    // Qwen3-Embedding contract: last-token pooling over the trailing EOS,
    // normalized output, batch-max trimming, and the Metal-resident encoder.
    pipeline.config.pooling = .last;
    pipeline.config.normalize = true;
    pipeline.config.trim_padding_to_batch_max = true;
    pipeline.config.resident_qwen3_embedding = true;
    pipeline.config.ensure_trailing_eos_id = @as(i32, @intCast(qwen3_eos_id));
    pipeline.config.resident_projection_required = opts.backend == .metal;

    const token_count = std.math.mul(usize, opts.batch, opts.seq_len) catch return error.InvalidInputShape;
    const input_ids = try allocator.alloc(i64, token_count);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i64, token_count);
    defer allocator.free(attention_mask);
    var real_tokens_per_iter: usize = 0;
    for (0..opts.batch) |row| {
        const active_len = if (opts.lengths.len != 0) opts.lengths[row] else opts.seq_len;
        real_tokens_per_iter += active_len;
        for (0..opts.seq_len) |position| {
            const index = row * opts.seq_len + position;
            if (position + 1 < active_len) {
                const token = 4 + @as(i64, @intCast(index % 1024));
                std.debug.assert(token < qwen3_vocab_size);
                input_ids[index] = token;
                attention_mask[index] = 1;
            } else if (position + 1 == active_len) {
                // EOS at the last active position: last-token pooling reads it.
                input_ids[index] = qwen3_eos_id;
                attention_mask[index] = 1;
            } else {
                // Right padding; masked out, id follows the EOS-as-pad convention.
                input_ids[index] = qwen3_eos_id;
                attention_mask[index] = 0;
            }
        }
    }

    for (0..opts.warmup_iters) |_| {
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        freeEmbeddings(allocator, embeddings);
    }

    const before_resident = model.resident_projection_stats.snapshot();
    const samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(samples);
    var checksum: f64 = 0;
    var dimensions: usize = 0;
    for (samples) |*sample| {
        const start = nowNs();
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        sample.* = nowNs() - start;
        checksum += embeddingChecksum(embeddings);
        dimensions = if (embeddings.len == 0) 0 else embeddings[0].len;
        freeEmbeddings(allocator, embeddings);
    }
    const resident = model.resident_projection_stats.snapshot();
    const timing = try summarize(allocator, samples);
    const total_ns = total(samples);
    const total_seconds = @as(f64, @floatFromInt(total_ns)) / 1.0e9;
    const measured_embeddings = std.math.mul(usize, opts.batch, opts.measure_iters) catch return error.InvalidArguments;
    const embeddings_per_second = if (total_ns == 0) 0.0 else @as(f64, @floatFromInt(measured_embeddings)) / total_seconds;
    const real_tokens = std.math.mul(usize, real_tokens_per_iter, opts.measure_iters) catch return error.InvalidArguments;
    const padded_tokens = std.math.mul(usize, token_count, opts.measure_iters) catch return error.InvalidArguments;
    const real_tokens_per_second = if (total_ns == 0) 0.0 else @as(f64, @floatFromInt(real_tokens)) / total_seconds;
    const padded_tokens_per_second = if (total_ns == 0) 0.0 else @as(f64, @floatFromInt(padded_tokens)) / total_seconds;
    const padding_waste_ratio = if (real_tokens == 0)
        0.0
    else
        @as(f64, @floatFromInt(padded_tokens)) / @as(f64, @floatFromInt(real_tokens));

    std.debug.print(
        "qwen3_embedding_e2e backend={s} batch={} seq_len={} ragged={} iters={} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} embeddings_s={d:.2} real_tok_s={d:.1} padded_tok_s={d:.1} padding_waste={d:.3} dims={} resident_text={}/{} checksum={d:.6}\n",
        .{
            @tagName(opts.backend),
            opts.batch,
            opts.seq_len,
            opts.lengths.len != 0,
            opts.measure_iters,
            nsToMs(timing.avg_ns),
            nsToMs(timing.p50_ns),
            nsToMs(timing.p95_ns),
            nsToMs(timing.min_ns),
            nsToMs(timing.max_ns),
            embeddings_per_second,
            real_tokens_per_second,
            padded_tokens_per_second,
            padding_waste_ratio,
            dimensions,
            resident.text_success - before_resident.text_success,
            resident.text_fallback - before_resident.text_fallback,
            checksum,
        },
    );
    if (opts.print_embedding) {
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        defer freeEmbeddings(allocator, embeddings);
        std.debug.print("embedding_json=[", .{});
        for (embeddings[0], 0..) |value, index| {
            if (index != 0) std.debug.print(",", .{});
            std.debug.print("{d:.9}", .{value});
        }
        std.debug.print("]\n", .{});
    }
}

fn ensureBackendAvailable(backend: BackendChoice) !void {
    if (backend == .cuda and !build_options.enable_cuda) return error.CudaNotEnabled;
    if (backend != .metal) return;
    if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
    }
}

fn parseArgs(allocator: std.mem.Allocator, init: std.process.Init) !Options {
    var opts = Options{};
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            opts.model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = std.meta.stringToEnum(BackendChoice, args.next() orelse return error.MissingBackend) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--batch")) {
            opts.batch = try std.fmt.parseInt(usize, args.next() orelse return error.MissingBatch, 10);
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            opts.seq_len = try std.fmt.parseInt(usize, args.next() orelse return error.MissingSequenceLength, 10);
        } else if (std.mem.eql(u8, arg, "--lengths")) {
            opts.lengths = try parseLengths(allocator, args.next() orelse return error.MissingLengths);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingWarmupIters, 10);
        } else if (std.mem.eql(u8, arg, "--iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingMeasureIters, 10);
        } else if (std.mem.eql(u8, arg, "--print-embedding")) {
            opts.print_embedding = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            opts.show_help = true;
            return opts;
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
    return opts;
}

fn parseLengths(allocator: std.mem.Allocator, csv: []const u8) ![]usize {
    var list: std.ArrayList(usize) = .empty;
    errdefer list.deinit(allocator);
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |piece| {
        const trimmed = std.mem.trim(u8, piece, " \t");
        if (trimmed.len == 0) continue;
        try list.append(allocator, try std.fmt.parseInt(usize, trimmed, 10));
    }
    if (list.items.len == 0) return error.InvalidLengths;
    return list.toOwnedSlice(allocator);
}

fn summarize(allocator: std.mem.Allocator, samples: []const u64) !Timing {
    const sorted = try allocator.dupe(u64, samples);
    defer allocator.free(sorted);
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
    return .{
        .avg_ns = total(samples) / samples.len,
        .p50_ns = sorted[percentileIndex(sorted.len, 50)],
        .p95_ns = sorted[percentileIndex(sorted.len, 95)],
        .min_ns = sorted[0],
        .max_ns = sorted[sorted.len - 1],
    };
}

fn percentileIndex(len: usize, percentile: usize) usize {
    if (len <= 1) return 0;
    const rank = (len * percentile + 99) / 100;
    return @min(len - 1, if (rank == 0) 0 else rank - 1);
}

fn total(samples: []const u64) u64 {
    var value: u64 = 0;
    for (samples) |sample| value += sample;
    return value;
}

fn freeEmbeddings(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |embedding| allocator.free(embedding);
    allocator.free(embeddings);
}

fn embeddingChecksum(embeddings: []const []const f32) f64 {
    var sum: f64 = 0;
    for (embeddings) |embedding| {
        for (embedding[0..@min(embedding.len, 16)]) |value| sum += value;
    }
    return sum;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn printUsage() void {
    std.debug.print(
        "usage: zig build bench-qwen3-embedding-e2e -Doptimize=ReleaseFast -- --model-dir <qwen3-embedding dir> [--backend metal|cuda|native] [--batch N] [--seq-len N] [--lengths 20,256,1024] [--warmup N] [--iters N] [--print-embedding]\n" ++
            "model dir falls back to $ANTFLY_INFERENCE_QWEN3_EMBEDDING_MODEL; --lengths enables ragged mode (batch = number of lengths, padded to the max)\n",
        .{},
    );
}
