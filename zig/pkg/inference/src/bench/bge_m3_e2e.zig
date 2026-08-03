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

// Pretokenized BGE-M3 encoder benchmark. Model load, tokenization, and response
// serialization are outside the timed region.

const std = @import("std");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const backends = inference.backends;
const session_factory = inference.architectures.session_factory;
const kernel_jit = inference.graph.kernel_jit;
const model_manager_mod = inference.server.model_manager;
const native_backend_guard = inference.native_backend_guard;
const metal_runtime = inference.metal_runtime;
const metal_generated_quant_stats = inference.metal_generated_quant_stats;

extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;

const BackendChoice = enum { native, metal, cuda };

const Options = struct {
    model_dir: []const u8 = "",
    backend: BackendChoice = .metal,
    batch: usize = 1,
    seq_len: usize = 256,
    warmup_iters: usize = 2,
    measure_iters: usize = 10,
    validate_specialized_attention: bool = false,
    tune_generated_kernels: bool = false,
    print_embedding: bool = false,
    show_help: bool = false,
    kernel_jit: kernel_jit.Config = .{},
    kernel_jit_mode_explicit: bool = false,
};

const Timing = struct {
    avg_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    min_ns: u64,
    max_ns: u64,
};

const AttentionValidation = struct {
    max_abs: f32,
    cosine: f64,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const opts = try parseArgs(init);
    if (opts.show_help) {
        printUsage();
        return;
    }
    if (opts.model_dir.len == 0 or opts.batch == 0 or opts.seq_len == 0 or opts.measure_iters == 0) {
        printUsage();
        return error.InvalidArguments;
    }
    try ensureBackendAvailable(opts.backend);
    if (opts.tune_generated_kernels and
        (opts.backend != .metal or !opts.kernel_jit.mode.activates()))
    {
        return error.GeneratedKernelTuningRequiresActiveMetalJit;
    }

    var session_manager = backends.SessionManager.initWithIo(allocator, init.io);
    session_manager.preferred_backends = switch (opts.backend) {
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
        .cuda => &.{backends.BackendType.cuda},
    };
    session_manager.kernel_jit = opts.kernel_jit;
    if (opts.tune_generated_kernels) session_manager.kernel_jit_load_context = .startup_preload;

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const model = model_manager.loadFromDir(opts.model_dir) catch |err| {
        std.debug.print("bge_m3_e2e: model_load_error={s}\n", .{@errorName(err)});
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
    pipeline.config.resident_projection_required = opts.backend != .native;
    if (opts.backend != .native and !pipeline.config.resident_text_encoder) {
        return error.ResidentTextEncoderUnavailable;
    }

    const token_count = std.math.mul(usize, opts.batch, opts.seq_len) catch return error.InvalidInputShape;
    const input_ids = try allocator.alloc(i64, token_count);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i64, token_count);
    defer allocator.free(attention_mask);
    for (input_ids, 0..) |*token, index| token.* = @intCast(4 + (index % 1024));
    @memset(attention_mask, 1);

    for (0..opts.warmup_iters) |_| {
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        freeEmbeddings(allocator, embeddings);
    }

    var jit_tuned: usize = 0;
    if (opts.tune_generated_kernels) {
        if (!try session_factory.beginMetalWorkloadProfile(model.session, .encoder)) {
            return error.MetalWorkloadProfileUnavailable;
        }
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        freeEmbeddings(allocator, embeddings);
        var profile = (try session_factory.endMetalWorkloadProfile(model.session, allocator, true)) orelse
            return error.MetalWorkloadProfileUnavailable;
        jit_tuned = profile.tuning_qualified_count;
        profile.deinit();
    }

    const attention_validation: ?AttentionValidation = if (opts.validate_specialized_attention) blk: {
        if (opts.backend == .native) return error.SpecializedAttentionRequiresGpu;
        if (opts.seq_len != 256) return error.SpecializedAttentionRequiresSequence256;
        try setSpecializedAttention(opts.backend, false);
        const generic = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        defer freeEmbeddings(allocator, generic);
        try setSpecializedAttention(opts.backend, true);
        const specialized = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        defer freeEmbeddings(allocator, specialized);
        break :blk compareEmbeddings(generic, specialized);
    } else null;

    const before_generated = metal_generated_quant_stats.snapshotForSession(allocator, model.session);
    const before_resident = model.resident_projection_stats.snapshot();
    const samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(samples);
    var checksum: f64 = 0;
    for (samples) |*sample| {
        const start = nowNs();
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        sample.* = nowNs() - start;
        checksum += embeddingChecksum(embeddings);
        freeEmbeddings(allocator, embeddings);
    }
    const generated = metal_generated_quant_stats.Stats.diff(
        before_generated,
        metal_generated_quant_stats.snapshotForSession(allocator, model.session),
    );
    var debug_backend = try session_factory.getComputeBackend(model.session, allocator);
    defer debug_backend.deinit();
    const provider_stats = debug_backend.debugTimingSnapshot().provider;
    const resident = model.resident_projection_stats.snapshot();
    const timing = try summarize(allocator, samples);
    const total_ns = total(samples);
    const measured_embeddings = std.math.mul(usize, opts.batch, opts.measure_iters) catch return error.InvalidArguments;
    const embeddings_per_second = if (total_ns == 0) 0.0 else @as(f64, @floatFromInt(measured_embeddings)) /
        (@as(f64, @floatFromInt(total_ns)) / 1.0e9);

    std.debug.print(
        "bge_m3_e2e backend={s} batch={} seq_len={} iters={} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} embeddings_s={d:.2} resident_text={}/{} generated_total={} generated_q4_k={} generated_q6_k={} q4_k_fallback_rows_65_plus={} q6_k_fallback_rows_65_plus={} jit_exact_q4_k={} jit_tuned={} dense_f16_mb={} dense_f16_slots={} qkv_pack_mb={} runtime_mb={} mps_linears={} ",
        .{
            @tagName(opts.backend),
            opts.batch,
            opts.seq_len,
            opts.measure_iters,
            nsToMs(timing.avg_ns),
            nsToMs(timing.p50_ns),
            nsToMs(timing.p95_ns),
            nsToMs(timing.min_ns),
            nsToMs(timing.max_ns),
            embeddings_per_second,
            resident.text_success - before_resident.text_success,
            resident.text_fallback - before_resident.text_fallback,
            generated.generatedTotal(),
            generated.q4_k + generated.q4_k_bias + generated.q4_k_bias_gelu,
            generated.q6_k + generated.q6_k_bias + generated.q6_k_bias_gelu,
            generated.q4_k_rows_65_plus,
            generated.q6_k_rows_65_plus,
            generated.jit_exact_q4_k,
            jit_tuned,
            provider_stats.metal_runtime_dense_linear_f16_weight_bytes / (1024 * 1024),
            provider_stats.metal_runtime_dense_linear_f16_slots,
            provider_stats.metal_runtime_dense_qkv_packed_bytes / (1024 * 1024),
            provider_stats.metal_runtime_total_bytes / (1024 * 1024),
            provider_stats.metal_runtime_last_frame_mps_dense_linear_count,
        },
    );
    std.debug.print(
        "quant_qkv={} qkv_packed={}/{} ffn_fused={}/{}/{} checksum={d:.6} attention_max_abs={d:.7} attention_cosine={d:.8}\n",
        .{
            provider_stats.metal_runtime_last_frame_compute_quant_qkv_count,
            provider_stats.metal_runtime_dense_qkv_packed_calls,
            provider_stats.metal_runtime_dense_qkv_packed_fallbacks,
            provider_stats.metal_runtime_deberta_ffn_fused_calls,
            provider_stats.metal_runtime_deberta_ffn_fused_mps_matmuls,
            provider_stats.metal_runtime_deberta_ffn_fused_fallbacks,
            checksum,
            if (attention_validation) |validation| validation.max_abs else @as(f32, -1),
            if (attention_validation) |validation| validation.cosine else @as(f64, -1),
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

fn setSpecializedAttention(backend: BackendChoice, enabled: bool) !void {
    const result = switch (backend) {
        .metal => setenv("TERMITE_METAL_DISABLE_BERT_PREFILL_ATTENTION", if (enabled) "0" else "1", 1),
        .cuda => setenv("ANTFLY_INFERENCE_CUDA_BERT_PREFILL_ATTENTION", if (enabled) "1" else "0", 1),
        .native => return error.SpecializedAttentionRequiresGpu,
    };
    if (result != 0) return error.EnvironmentMutationFailed;
}

fn compareEmbeddings(reference: []const []const f32, candidate: []const []const f32) AttentionValidation {
    var max_abs: f32 = 0;
    var dot: f64 = 0;
    var reference_norm: f64 = 0;
    var candidate_norm: f64 = 0;
    for (reference, candidate) |reference_embedding, candidate_embedding| {
        for (reference_embedding, candidate_embedding) |reference_value, candidate_value| {
            max_abs = @max(max_abs, @abs(reference_value - candidate_value));
            dot += @as(f64, reference_value) * @as(f64, candidate_value);
            reference_norm += @as(f64, reference_value) * @as(f64, reference_value);
            candidate_norm += @as(f64, candidate_value) * @as(f64, candidate_value);
        }
    }
    return .{
        .max_abs = max_abs,
        .cosine = if (reference_norm == 0 or candidate_norm == 0) 0 else dot / @sqrt(reference_norm * candidate_norm),
    };
}

fn parseArgs(init: std.process.Init) !Options {
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
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingWarmupIters, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingMeasureIters, 10);
        } else if (std.mem.eql(u8, arg, "--validate-specialized-attention")) {
            opts.validate_specialized_attention = true;
        } else if (std.mem.eql(u8, arg, "--tune-generated-kernels")) {
            opts.tune_generated_kernels = true;
        } else if (std.mem.eql(u8, arg, "--print-embedding")) {
            opts.print_embedding = true;
        } else if (std.mem.eql(u8, arg, "--kernel-jit-mode")) {
            opts.kernel_jit.mode = std.meta.stringToEnum(kernel_jit.Mode, args.next() orelse return error.MissingKernelJitMode) orelse return error.InvalidKernelJitMode;
            opts.kernel_jit_mode_explicit = true;
        } else if (std.mem.eql(u8, arg, "--kernel-jit-cache-dir")) {
            opts.kernel_jit.cache_dir = args.next() orelse return error.MissingKernelJitCacheDir;
        } else if (std.mem.eql(u8, arg, "--kernel-jit-max-cache-mb")) {
            opts.kernel_jit.max_cache_bytes_mb = try std.fmt.parseInt(usize, args.next() orelse return error.MissingKernelJitMaxCacheMb, 10);
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            opts.show_help = true;
            return opts;
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
    if (opts.tune_generated_kernels and !opts.kernel_jit_mode_explicit) opts.kernel_jit.mode = .on;
    try opts.kernel_jit.validate();
    return opts;
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
        "usage: zig build bench-bge-m3-e2e -Doptimize=ReleaseFast -- --model-dir <bge-m3.gguf|dir> [--backend metal|cuda|native] [--batch N] [--seq-len 256] [--warmup-iters N] [--measure-iters N] [--validate-specialized-attention] [--tune-generated-kernels] [--print-embedding] [--kernel-jit-mode off|shadow|on|required] [--kernel-jit-cache-dir PATH]\n",
        .{},
    );
}
