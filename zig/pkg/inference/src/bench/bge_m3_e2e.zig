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
const builtin = @import("builtin");
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
extern fn sysctlbyname(
    name: [*:0]const u8,
    oldp: ?*anyopaque,
    oldlenp: *usize,
    newp: ?*const anyopaque,
    newlen: usize,
) c_int;

const DarwinSwapUsage = extern struct {
    total: u64,
    avail: u64,
    used: u64,
    pagesize: u32,
    encrypted: u32,
};

const BackendChoice = enum { native, metal, cuda };

const Options = struct {
    model_dir: []const u8 = "",
    model_sha: []const u8 = "",
    fixture_path: []const u8 = "src/bench/testdata/bge_m3_tokens.json",
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

const Fixture = struct {
    model: []const u8,
    seed: u64,
    vocab_size: usize,
    pad_token_id: i64,
    source_text: []const u8,
    input_ids_16: []const i64,
    attention_mask_16: []const i64,
    input_ids_128: []const i64,
    attention_mask_128: []const i64,
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

    const fixture_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, opts.fixture_path, allocator, .limited(64 * 1024));
    defer allocator.free(fixture_bytes);
    var fixture_doc = try std.json.parseFromSlice(Fixture, allocator, fixture_bytes, .{ .ignore_unknown_fields = false });
    defer fixture_doc.deinit();
    const fixture = fixture_doc.value;
    try validateFixture(fixture);

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
    const fixture_row: ?[]const i64 = switch (opts.seq_len) {
        16 => fixture.input_ids_16,
        128 => fixture.input_ids_128,
        else => null,
    };
    const fixture_mask: ?[]const i64 = switch (opts.seq_len) {
        16 => fixture.attention_mask_16,
        128 => fixture.attention_mask_128,
        else => null,
    };
    if (fixture_row) |row_ids| {
        const row_mask = fixture_mask.?;
        for (0..opts.batch) |row| {
            @memcpy(input_ids[row * opts.seq_len ..][0..opts.seq_len], row_ids);
            @memcpy(attention_mask[row * opts.seq_len ..][0..opts.seq_len], row_mask);
        }
    } else {
        for (input_ids, 0..) |*token, index| token.* = @intCast(4 + (index % 1024));
        @memset(attention_mask, 1);
    }

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

    var debug_backend = try session_factory.getComputeBackend(model.session, allocator);
    defer debug_backend.deinit();
    debug_backend.resetDebugTimingStats();
    const before_generated = metal_generated_quant_stats.snapshotForSession(allocator, model.session);
    const before_resident = model.resident_projection_stats.snapshot();
    const samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(samples);
    var checksum: f64 = 0;
    const rss_before = currentResidentBytes();
    const swap_before = currentSwapBytes();
    for (samples) |*sample| {
        const start = nowNs();
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        sample.* = nowNs() - start;
        checksum = embeddingChecksum(embeddings);
        freeEmbeddings(allocator, embeddings);
    }
    const rss_after = currentResidentBytes();
    const swap_after = currentSwapBytes();
    const generated = metal_generated_quant_stats.Stats.diff(
        before_generated,
        metal_generated_quant_stats.snapshotForSession(allocator, model.session),
    );
    const provider_stats = debug_backend.debugTimingSnapshot().provider;
    const resident = model.resident_projection_stats.snapshot();
    const timing = try summarize(allocator, samples);
    const total_ns = total(samples);
    const measured_embeddings = std.math.mul(usize, opts.batch, opts.measure_iters) catch return error.InvalidArguments;
    const embeddings_per_second = if (total_ns == 0) 0.0 else @as(f64, @floatFromInt(measured_embeddings)) /
        (@as(f64, @floatFromInt(total_ns)) / 1.0e9);

    std.debug.print(
        "{{\"kind\":\"bge_m3_direct\",\"model\":\"{s}\",\"model_sha\":\"{s}\",\"backend\":\"{s}\",\"device\":\"{s}\",\"fixture_seed\":{d},\"batch\":{d},\"sequence_length\":{d},\"warmups\":{d},\"repeats\":{d},\"mean_ms\":{d:.6},\"p50_ms\":{d:.6},\"p95_ms\":{d:.6},\"min_ms\":{d:.6},\"max_ms\":{d:.6},\"direct_gpu_frame_ms\":{d:.6},\"embeddings_per_second\":{d:.6},",
        .{
            fixture.model,
            opts.model_sha,
            @tagName(opts.backend),
            switch (opts.backend) {
                .native => "cpu",
                .metal => "mps",
                .cuda => "cuda",
            },
            if (fixture_row != null) fixture.seed else 0,
            opts.batch,
            opts.seq_len,
            opts.warmup_iters,
            opts.measure_iters,
            nsToMs(timing.avg_ns),
            nsToMs(timing.p50_ns),
            nsToMs(timing.p95_ns),
            nsToMs(timing.min_ns),
            nsToMs(timing.max_ns),
            if (opts.measure_iters == 0)
                @as(f64, 0)
            else
                @as(f64, @floatFromInt(provider_stats.decoder_runtime_frame_gpu_nanos)) /
                    @as(f64, @floatFromInt(opts.measure_iters)) /
                    @as(f64, std.time.ns_per_ms),
            embeddings_per_second,
        },
    );
    std.debug.print(
        "\"command_counts\":{{\"compute_encoders\":{d},\"blit_encoders\":{d},\"planned_ops\":{d},\"mps_dense_linear\":{d},\"quant_qkv\":{d}}},\"optimized\":{{\"resident_text_successes\":{d},\"resident_text_fallbacks\":{d},\"qkv_packed_calls\":{d},\"qkv_packed_fallbacks\":{d},\"ffn_fused_calls\":{d},\"ffn_fused_mps_matmuls\":{d},\"ffn_fused_fallbacks\":{d},\"generated_total\":{d},\"generated_q4_k\":{d},\"generated_q6_k\":{d},\"jit_exact_q4_k\":{d},\"jit_tuned\":{d}}},",
        .{
            provider_stats.metal_runtime_last_frame_compute_encoder_count,
            provider_stats.metal_runtime_last_frame_blit_encoder_count,
            provider_stats.metal_runtime_last_frame_planned_command_op_count,
            provider_stats.metal_runtime_last_frame_mps_dense_linear_count,
            provider_stats.metal_runtime_last_frame_compute_quant_qkv_count,
            resident.text_success - before_resident.text_success,
            resident.text_fallback - before_resident.text_fallback,
            provider_stats.metal_runtime_dense_qkv_packed_calls,
            provider_stats.metal_runtime_dense_qkv_packed_fallbacks,
            provider_stats.metal_runtime_deberta_ffn_fused_calls,
            provider_stats.metal_runtime_deberta_ffn_fused_mps_matmuls,
            provider_stats.metal_runtime_deberta_ffn_fused_fallbacks,
            generated.generatedTotal(),
            generated.q4_k + generated.q4_k_bias + generated.q4_k_bias_gelu,
            generated.q6_k + generated.q6_k_bias + generated.q6_k_bias_gelu,
            generated.jit_exact_q4_k,
            jit_tuned,
        },
    );
    std.debug.print(
        "\"memory\":{{\"runtime_bytes\":{d},\"dense_f32_weight_bytes\":{d},\"dense_f32_slots\":{d},\"dense_bf16_weight_bytes\":{d},\"dense_bf16_slots\":{d},\"dense_f16_weight_bytes\":{d},\"dense_f16_slots\":{d},\"qkv_pack_bytes\":{d}}},\"output_checksum\":{d:.9},\"rss_bytes\":{{\"before\":{d},\"after\":{d}}},\"swap_bytes\":{{\"before\":{d},\"after\":{d},\"available\":{}}},\"attention_validation\":{{\"max_abs\":{d:.9},\"cosine\":{d:.9}}}}}\n",
        .{
            provider_stats.metal_runtime_total_bytes,
            provider_stats.metal_runtime_dense_linear_f32_weight_bytes,
            provider_stats.metal_runtime_dense_linear_f32_slots,
            provider_stats.metal_runtime_dense_linear_bf16_weight_bytes,
            provider_stats.metal_runtime_dense_linear_bf16_slots,
            provider_stats.metal_runtime_dense_linear_f16_weight_bytes,
            provider_stats.metal_runtime_dense_linear_f16_slots,
            provider_stats.metal_runtime_dense_qkv_packed_bytes,
            checksum,
            rss_before,
            rss_after,
            swap_before orelse 0,
            swap_after orelse 0,
            swap_before != null and swap_after != null,
            if (attention_validation) |validation| validation.max_abs else @as(f32, -1),
            if (attention_validation) |validation| validation.cosine else @as(f64, -1),
        },
    );
    if (opts.print_embedding) {
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        defer freeEmbeddings(allocator, embeddings);
        std.debug.print(
            "{{\"kind\":\"bge_m3_direct_embeddings\",\"model_sha\":\"{s}\",\"batch\":{d},\"sequence_length\":{d},\"embeddings\":[",
            .{ opts.model_sha, opts.batch, opts.seq_len },
        );
        for (embeddings, 0..) |embedding, embedding_index| {
            if (embedding_index != 0) std.debug.print(",", .{});
            std.debug.print("[", .{});
            for (embedding, 0..) |value, value_index| {
                if (value_index != 0) std.debug.print(",", .{});
                std.debug.print("{d:.9}", .{value});
            }
            std.debug.print("]", .{});
        }
        std.debug.print("]}}\n", .{});
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

fn validateFixture(fixture: Fixture) !void {
    if (!std.mem.eql(u8, fixture.model, "BAAI/bge-m3") or
        fixture.vocab_size != 250002 or
        fixture.pad_token_id != 1 or
        fixture.source_text.len == 0 or
        fixture.input_ids_16.len != 16 or
        fixture.attention_mask_16.len != 16 or
        fixture.input_ids_128.len != 128 or
        fixture.attention_mask_128.len != 128)
    {
        return error.InvalidBgeM3BenchmarkFixture;
    }
    const vocab_size: i64 = @intCast(fixture.vocab_size);
    for (fixture.input_ids_16, fixture.attention_mask_16) |id, mask| {
        if (id < 0 or id >= vocab_size or (mask != 0 and mask != 1)) return error.InvalidFixtureToken;
    }
    for (fixture.input_ids_128, fixture.attention_mask_128) |id, mask| {
        if (id < 0 or id >= vocab_size or (mask != 0 and mask != 1)) return error.InvalidFixtureToken;
    }
}

fn parseArgs(init: std.process.Init) !Options {
    var opts = Options{};
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            opts.model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--model-sha")) {
            opts.model_sha = args.next() orelse return error.MissingModelSha;
        } else if (std.mem.eql(u8, arg, "--fixture")) {
            opts.fixture_path = args.next() orelse return error.MissingFixturePath;
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
        } else if (std.mem.eql(u8, arg, "--print-embedding") or std.mem.eql(u8, arg, "--print-embeddings")) {
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

fn currentResidentBytes() usize {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    const maxrss: usize = @intCast(usage.maxrss);
    return switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}

fn currentSwapBytes() ?u64 {
    if (builtin.os.tag != .macos) return null;
    var usage: DarwinSwapUsage = undefined;
    var usage_len: usize = @sizeOf(DarwinSwapUsage);
    if (sysctlbyname("vm.swapusage", @ptrCast(&usage), &usage_len, null, 0) != 0 or usage_len != @sizeOf(DarwinSwapUsage)) {
        return null;
    }
    return usage.used;
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
        "usage: zig build bench-bge-m3-e2e -Doptimize=ReleaseFast -- --model-dir <bge-m3.gguf|dir> [--model-sha SHA256] [--fixture src/bench/testdata/bge_m3_tokens.json] [--backend metal|cuda|native] [--batch N] [--seq-len 16|128|256] [--warmup-iters N] [--measure-iters N] [--validate-specialized-attention] [--tune-generated-kernels] [--print-embeddings] [--kernel-jit-mode off|shadow|on|required] [--kernel-jit-cache-dir PATH]\n",
        .{},
    );
}
