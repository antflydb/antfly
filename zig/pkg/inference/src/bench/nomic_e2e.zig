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

// Pretokenized Nomic Embed Text v1.5 benchmark. The timed region is exactly
// EmbeddingPipeline.embedTokenized: model loading and tokenization are
// intentionally outside every sample.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const backends = inference.backends;
const model_manager_mod = inference.server.model_manager;
const native_backend_guard = inference.native_backend_guard;
const metal_runtime = inference.metal_runtime;

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

const BackendChoice = enum { native, metal };

const Options = struct {
    model_dir: []const u8 = "",
    model_sha: []const u8 = "",
    fixture_path: []const u8 = "src/bench/testdata/nomic_v15_tokens.json",
    backend: BackendChoice = .metal,
    batch: usize = 1,
    seq_len: usize = 16,
    warmup_iters: usize = 3,
    measure_iters: usize = 10,
    print_embeddings: bool = false,
    show_help: bool = false,
};

const Fixture = struct {
    model: []const u8,
    seed: u64,
    vocab_size: usize,
    attention_mask: []const u8,
    input_ids_16: []const i64,
    input_ids_128: []const i64,
};

const Timing = struct {
    mean_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const opts = try parseArgs(init);
    if (opts.show_help) {
        printUsage();
        return;
    }
    try validateOptions(opts);
    try ensureBackendAvailable(opts.backend);

    const fixture_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, opts.fixture_path, allocator, .limited(64 * 1024));
    defer allocator.free(fixture_bytes);
    var fixture_doc = try std.json.parseFromSlice(Fixture, allocator, fixture_bytes, .{ .ignore_unknown_fields = false });
    defer fixture_doc.deinit();
    const fixture = fixture_doc.value;
    try validateFixture(fixture);
    const fixture_ids = switch (opts.seq_len) {
        16 => fixture.input_ids_16,
        128 => fixture.input_ids_128,
        else => return error.UnsupportedBenchmarkSequenceLength,
    };
    if (fixture_ids.len != opts.seq_len) return error.InvalidFixtureSequenceLength;

    var session_manager = backends.SessionManager.initWithIo(allocator, init.io);
    session_manager.preferred_backends = switch (opts.backend) {
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
    };
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const model = model_manager.loadFromDir(opts.model_dir) catch |err| {
        std.debug.print("nomic_e2e: model_load_error={s}\n", .{@errorName(err)});
        return err;
    };
    try model.ensureEmbeddingAssets(true, false, false);
    const expected_backend: backends.BackendType = switch (opts.backend) {
        .native => .native,
        .metal => .metal,
    };
    if (model.session.backend() != expected_backend) return error.UnexpectedBackend;

    var pipeline = model.embeddingPipeline(allocator);
    pipeline.config.resident_projection_required = opts.backend == .metal;
    if (opts.backend == .metal and !pipeline.config.resident_text_encoder) {
        return error.ResidentTextEncoderUnavailable;
    }

    const token_count = try std.math.mul(usize, opts.batch, opts.seq_len);
    const input_ids = try allocator.alloc(i64, token_count);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i64, token_count);
    defer allocator.free(attention_mask);
    for (0..opts.batch) |row| {
        @memcpy(input_ids[row * opts.seq_len ..][0..opts.seq_len], fixture_ids);
    }
    @memset(attention_mask, 1);

    for (0..opts.warmup_iters) |_| {
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        freeEmbeddings(allocator, embeddings);
    }

    const samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(samples);
    const gpu_samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(gpu_samples);
    const frame_wait_samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(frame_wait_samples);
    const encoder_cpu_samples = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(encoder_cpu_samples);
    var checksum: f64 = 0;
    var last_compute_encoders: u64 = 0;
    var last_blit_encoders: u64 = 0;
    var last_command_ops: u64 = 0;
    const rss_before = currentResidentBytes();
    const swap_before = currentSwapBytes();
    for (samples, gpu_samples, frame_wait_samples, encoder_cpu_samples) |*sample, *gpu_sample, *frame_wait_sample, *encoder_cpu_sample| {
        const start = nowNs();
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        sample.* = nowNs() - start;
        const after = pipeline.lastResidentBackendTiming() orelse return error.ResidentTimingUnavailable;
        gpu_sample.* = @intCast(after.provider.decoder_runtime_frame_gpu_nanos);
        frame_wait_sample.* = @intCast(after.provider.decoder_runtime_frame_wait_nanos);
        encoder_cpu_sample.* = @intCast(after.provider.metal_runtime_nomic_bert_encoder_layer_nanos);
        last_compute_encoders = after.provider.metal_runtime_last_frame_compute_encoder_count;
        last_blit_encoders = after.provider.metal_runtime_last_frame_blit_encoder_count;
        last_command_ops = after.provider.metal_runtime_last_frame_planned_command_op_count;
        // Both benchmark implementations report one representative output,
        // not a repeat-count-dependent sum. Each measured iteration consumes
        // identical fixture rows, so retaining the latest checksum also makes
        // native and PyTorch evidence directly comparable.
        checksum = embeddingChecksum(embeddings);
        freeEmbeddings(allocator, embeddings);
    }
    const rss_after = currentResidentBytes();
    const swap_after = currentSwapBytes();
    const timing = try summarize(allocator, samples);
    const gpu_timing = try summarize(allocator, gpu_samples);
    const frame_wait_timing = try summarize(allocator, frame_wait_samples);
    const encoder_cpu_timing = try summarize(allocator, encoder_cpu_samples);
    const provider = pipeline.lastResidentBackendTiming().?.provider;

    // JSONL is intentional: callers can append one fully-qualified cell at a
    // time to an artifact outside the worktree without parsing log text.
    std.debug.print(
        "{{\"kind\":\"nomic_direct\",\"model\":\"{s}\",\"model_sha\":\"{s}\",\"backend\":\"{s}\",\"device\":\"{s}\",\"fixture_seed\":{d},\"batch\":{d},\"sequence_length\":{d},\"warmups\":{d},\"repeats\":{d},\"mean_ms\":{d:.6},\"p50_ms\":{d:.6},\"p95_ms\":{d:.6},\"gpu_frame_p50_ms\":{d:.6},\"gpu_frame_p95_ms\":{d:.6},\"frame_wait_p50_ms\":{d:.6},\"encoder_cpu_p50_ms\":{d:.6}",
        .{
            fixture.model,
            opts.model_sha,
            @tagName(opts.backend),
            if (opts.backend == .metal) "mps" else "cpu",
            fixture.seed,
            opts.batch,
            opts.seq_len,
            opts.warmup_iters,
            opts.measure_iters,
            nsToMs(timing.mean_ns),
            nsToMs(timing.p50_ns),
            nsToMs(timing.p95_ns),
            nsToMs(gpu_timing.p50_ns),
            nsToMs(gpu_timing.p95_ns),
            nsToMs(frame_wait_timing.p50_ns),
            nsToMs(encoder_cpu_timing.p50_ns),
        },
    );
    std.debug.print(
        ",\"command_counts\":{{\"compute_encoders\":{d},\"blit_encoders\":{d},\"planned_ops\":{d},\"mps_dense_linear\":{d},\"attention\":{d},\"attention_project\":{d},\"ffn\":{d},\"ffn_norm\":{d},\"embedding\":{d},\"tail\":{d},\"other\":{d}}},\"nomic_layers\":{{\"attempts\":{d},\"successes\":{d},\"fallbacks\":{d},\"borrowed_f32_weights\":{d},\"rope_pairs\":{d},\"rope_pair_fallbacks\":{d},\"sdpa_q8\":{d},\"ffn_fused\":{d},\"ffn_fused_failures\":{d},\"pool_normalize_attempts\":{d},\"pool_normalize_successes\":{d},\"pool_normalize_failures\":{d}}},\"residency\":{{\"device_buffers_created\":{d},\"to_host_calls\":{d},\"to_host_device_calls\":{d}}},\"output_checksum\":{d:.9},\"rss_bytes\":{{\"before\":{d},\"after\":{d}}},\"swap_bytes\":{{\"before\":{d},\"after\":{d},\"available\":{}}}}}\n",
        .{
            last_compute_encoders,
            last_blit_encoders,
            last_command_ops,
            provider.metal_runtime_last_frame_mps_dense_linear_count,
            provider.metal_runtime_last_frame_compute_region_attention_count,
            provider.metal_runtime_last_frame_compute_region_attention_project_count,
            provider.metal_runtime_last_frame_compute_region_ffn_count,
            provider.metal_runtime_last_frame_compute_region_ffn_norm_count,
            provider.metal_runtime_last_frame_compute_region_embedding_count,
            provider.metal_runtime_last_frame_compute_region_tail_count,
            provider.metal_runtime_last_frame_compute_region_other_count,
            provider.metal_runtime_nomic_bert_encoder_layer_attempts,
            provider.metal_runtime_nomic_bert_encoder_layer_successes,
            provider.metal_runtime_nomic_bert_encoder_layer_fallbacks,
            provider.metal_runtime_nomic_bert_borrowed_f32_weight_hits,
            provider.metal_runtime_nomic_bert_rope_pair_calls,
            provider.metal_runtime_nomic_bert_rope_pair_fallbacks,
            provider.metal_runtime_nomic_bert_sdpa_q8_calls,
            provider.metal_runtime_nomic_bert_ffn_fused_calls,
            provider.metal_runtime_nomic_bert_ffn_fused_failures,
            provider.metal_runtime_nomic_bert_pool_normalize_attempts,
            provider.metal_runtime_nomic_bert_pool_normalize_successes,
            provider.metal_runtime_nomic_bert_pool_normalize_failures,
            provider.metal_tensor_device_owned_buffers_created,
            provider.metal_tensor_to_host_calls,
            provider.metal_tensor_to_host_device_calls,
            checksum,
            rss_before,
            rss_after,
            swap_before orelse 0,
            swap_after orelse 0,
            swap_before != null and swap_after != null,
        },
    );
    if (opts.print_embeddings) {
        const embeddings = try pipeline.embedTokenized(input_ids, attention_mask, opts.batch, opts.seq_len);
        defer freeEmbeddings(allocator, embeddings);
        std.debug.print(
            "{{\"kind\":\"nomic_direct_embeddings\",\"model_sha\":\"{s}\",\"batch\":{d},\"sequence_length\":{d},\"embeddings\":[",
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

fn validateOptions(opts: Options) !void {
    if (opts.model_dir.len == 0 or opts.model_sha.len == 0) return error.MissingModelIdentity;
    if (opts.batch != 1 and opts.batch != 2 and opts.batch != 4) return error.UnsupportedBenchmarkBatch;
    if (opts.seq_len != 16 and opts.seq_len != 128) return error.UnsupportedBenchmarkSequenceLength;
    if (opts.warmup_iters != 3 or opts.measure_iters != 10) return error.BenchmarkContractRequiresThreeWarmupsAndTenRepeats;
}

fn validateFixture(fixture: Fixture) !void {
    if (!std.mem.eql(u8, fixture.model, "nomic-ai/nomic-embed-text-v1.5") or
        fixture.vocab_size != 30528 or
        !std.mem.eql(u8, fixture.attention_mask, "all_ones") or
        fixture.input_ids_16.len != 16 or fixture.input_ids_128.len != 128)
    {
        return error.InvalidNomicBenchmarkFixture;
    }
    const vocab_size: i64 = @intCast(fixture.vocab_size);
    for (fixture.input_ids_16) |id| if (id < 0 or id >= vocab_size) return error.InvalidFixtureToken;
    for (fixture.input_ids_128) |id| if (id < 0 or id >= vocab_size) return error.InvalidFixtureToken;
}

fn ensureBackendAvailable(backend: BackendChoice) !void {
    if (backend != .metal) return;
    if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
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
        } else if (std.mem.eql(u8, arg, "--print-embeddings")) {
            opts.print_embeddings = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            opts.show_help = true;
            return opts;
        } else {
            return error.InvalidArguments;
        }
    }
    return opts;
}

fn summarize(allocator: std.mem.Allocator, samples: []const u64) !Timing {
    const sorted = try allocator.dupe(u64, samples);
    defer allocator.free(sorted);
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
    var total_ns: u128 = 0;
    for (samples) |sample| total_ns += sample;
    return .{
        .mean_ns = @intCast(total_ns / samples.len),
        .p50_ns = sorted[percentileIndex(sorted.len, 50)],
        .p95_ns = sorted[percentileIndex(sorted.len, 95)],
    };
}

fn percentileIndex(len: usize, percentile: usize) usize {
    if (len <= 1) return 0;
    const rank = (len * percentile + 99) / 100;
    return @min(len - 1, if (rank == 0) 0 else rank - 1);
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
        "usage: zig build bench-nomic-e2e -Doptimize=ReleaseFast -- --model-dir <nomic-dir> --model-sha <sha256> [--fixture src/bench/testdata/nomic_v15_tokens.json] [--backend metal|native] [--batch 1|2|4] [--seq-len 16|128] [--warmup-iters 3] [--measure-iters 10] [--print-embeddings]\n",
        .{},
    );
}
