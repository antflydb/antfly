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

// Real-bundle reranker benchmark.
//
// Loads a local reranker once via ModelManager and measures repeated
// RerankingPipeline.rerank calls.  The measured row includes tokenization,
// forward pass, score extraction, and sigmoid/softmax postprocess, but not
// process startup or model loading.

const std = @import("std");

const inference = @import("inference_internal");
const platform = inference.platform;
const backends = inference.backends;
const native_backend_choice = inference.native_backend_choice;
const session_factory = inference.architectures.session_factory;
const metal_generated_quant_stats = @import("metal_generated_quant_stats.zig");

const print = std.debug.print;
const MetalGeneratedQuantStats = metal_generated_quant_stats.Stats;

const OutputFormat = enum {
    text,
    csv,
};

const sweep_doc_counts = [_]usize{ 1, 2, 4, 8, 16, 32 };

const Options = struct {
    model_dir: []const u8 = "",
    query: []const u8 = "what is CUDA",
    docs: std.ArrayListUnmanaged([]const u8) = .empty,
    backend: native_backend_choice.Choice = .native,
    warmup_iters: usize = 1,
    measure_iters: usize = 5,
    batch_sweep: bool = false,
    format: OutputFormat = .text,
    owns_model_dir: bool = false,
    owns_query: bool = false,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        if (self.owns_model_dir) allocator.free(self.model_dir);
        if (self.owns_query) allocator.free(self.query);
        for (self.docs.items) |doc| allocator.free(doc);
        self.docs.deinit(allocator);
    }
};

pub fn main(init: std.process.Init) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    if (wantsHelp(init)) {
        printUsage();
        return;
    }

    var opts = try parseArgs(allocator, init);
    defer opts.deinit(allocator);
    if (opts.docs.items.len == 0) {
        try opts.docs.append(allocator, try allocator.dupe(u8, "CUDA is a parallel computing platform for NVIDIA GPUs."));
        try opts.docs.append(allocator, try allocator.dupe(u8, "A sourdough recipe uses flour and water."));
    }
    if (opts.model_dir.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }

    var session_manager = backends.SessionManager.init(allocator);
    try native_backend_choice.validate(opts.backend);
    native_backend_choice.configureSessionPreference(&session_manager, opts.backend);

    var model_manager = inference.server.model_manager.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    var pipeline = model.rerankingPipeline(allocator);

    if (opts.format == .csv) printCsvHeader();

    if (opts.batch_sweep) {
        for (sweep_doc_counts) |doc_count| {
            const docs = try docsForCount(allocator, opts.docs.items, doc_count);
            const result = try runTimed(&pipeline, model.session, allocator, opts, docs);
            printResult(opts, doc_count, result);
            allocator.free(docs);
        }
    } else {
        const result = try runTimed(&pipeline, model.session, allocator, opts, opts.docs.items);
        printResult(opts, opts.docs.items.len, result);
    }

    if (opts.format == .text and model.session.backend() != .onnx) {
        var debug_backend = try session_factory.getComputeBackend(model.session, allocator);
        defer debug_backend.deinit();
        const provider_stats = debug_backend.debugTimingSnapshot().provider;
        print(
            "provider_stats dense_f16_mb={} dense_f16_slots={} qkv_pack_mb={} relative_cache_mb={} runtime_mb={} mps_linears={} quant_qkv={} quant_linear={} qkv_packed={}/{} ffn_fused={}/{}/{} attention_flash={} attention_legacy={} attention_gemm={}/{}\n",
            .{
                provider_stats.metal_runtime_dense_linear_f16_weight_bytes / (1024 * 1024),
                provider_stats.metal_runtime_dense_linear_f16_slots,
                provider_stats.metal_runtime_dense_qkv_packed_bytes / (1024 * 1024),
                provider_stats.metal_runtime_deberta_relative_cache_bytes / (1024 * 1024),
                provider_stats.metal_runtime_total_bytes / (1024 * 1024),
                provider_stats.metal_runtime_last_frame_mps_dense_linear_count,
                provider_stats.metal_runtime_last_frame_compute_quant_qkv_count,
                provider_stats.metal_runtime_last_frame_compute_quant_linear_count,
                provider_stats.metal_runtime_dense_qkv_packed_calls,
                provider_stats.metal_runtime_dense_qkv_packed_fallbacks,
                provider_stats.metal_runtime_deberta_ffn_fused_calls,
                provider_stats.metal_runtime_deberta_ffn_fused_mps_matmuls,
                provider_stats.metal_runtime_deberta_ffn_fused_fallbacks,
                provider_stats.metal_runtime_deberta_attention_flash_calls,
                provider_stats.metal_runtime_deberta_attention_legacy_calls,
                provider_stats.metal_runtime_deberta_attention_gemm_calls,
                provider_stats.metal_runtime_deberta_attention_gemm_fallbacks,
            },
        );
        const stage = provider_stats.metal_stage_timing;
        print(
            "metal_timing cumulative_gpu_ms={d:.3} stage_enabled={} stage_supported={} stage_complete={} sampled={} attention_ms={d:.3} ffn_ms={d:.3} embedding_ms={d:.3} other_ms={d:.3} stage_failures={}\n",
            .{
                @as(f64, @floatFromInt(provider_stats.decoder_runtime_frame_gpu_nanos)) / 1.0e6,
                stage.enabled,
                stage.supported,
                stage.complete,
                stage.prefill.sampled_frames,
                @as(f64, @floatFromInt(stage.prefill.attention_nanos)) / 1.0e6,
                @as(f64, @floatFromInt(stage.prefill.ffn_nanos)) / 1.0e6,
                @as(f64, @floatFromInt(stage.prefill.embedding_nanos)) / 1.0e6,
                @as(f64, @floatFromInt(stage.prefill.other_nanos)) / 1.0e6,
                stage.failureCount(),
            },
        );
    }
}

const BenchResult = struct {
    avg_ms: f64,
    p50_ms: f64,
    min_ms: f64,
    p95_ms: f64,
    docs_per_s: f64,
    checksum: f64,
    graph_stats: GraphStats = .{},
    metal_generated_quant: MetalGeneratedQuantStats = .{},
};

const GraphStats = struct {
    captures: u64 = 0,
    replays: u64 = 0,
    fallbacks: u64 = 0,
    capture_failures: u64 = 0,
    buckets: u64 = 0,
    graph_owned_bytes: u64 = 0,
    dynamic_copy_bytes: u64 = 0,
    last_fallback_reason: []const u8 = "none",
};

fn runTimed(
    pipeline: anytype,
    session: backends.Session,
    allocator: std.mem.Allocator,
    opts: Options,
    docs: []const []const u8,
) !BenchResult {
    var checksum: f64 = 0;
    for (0..opts.warmup_iters) |_| {
        const scores = try pipeline.rerank(opts.query, docs);
        for (scores) |score| checksum += score;
        allocator.free(scores);
    }

    const before_metal_generated = metal_generated_quant_stats.snapshotForSession(allocator, session);
    var total_ns: u64 = 0;
    var min_ns: u64 = std.math.maxInt(u64);
    const samples_ns = try allocator.alloc(u64, opts.measure_iters);
    defer allocator.free(samples_ns);
    for (0..opts.measure_iters) |idx| {
        const start = nowNs();
        const scores = try pipeline.rerank(opts.query, docs);
        const elapsed = nowNs() - start;
        total_ns += elapsed;
        min_ns = @min(min_ns, elapsed);
        samples_ns[idx] = elapsed;
        for (scores) |score| checksum += score;
        allocator.free(scores);
    }
    const after_metal_generated = metal_generated_quant_stats.snapshotForSession(allocator, session);

    std.mem.sort(u64, samples_ns, {}, struct {
        fn lessThan(_: void, a: u64, b: u64) bool {
            return a < b;
        }
    }.lessThan);

    const avg_ms = nsToMs(total_ns / opts.measure_iters);
    const p50_ms = medianMs(samples_ns);
    const min_ms = nsToMs(min_ns);
    const p95_ms = nsToMs(samples_ns[(samples_ns.len - 1) * 95 / 100]);
    const docs_per_s = if (avg_ms == 0)
        0
    else
        (@as(f64, @floatFromInt(docs.len)) * 1000.0) / avg_ms;

    return .{
        .avg_ms = avg_ms,
        .p50_ms = p50_ms,
        .min_ms = min_ms,
        .p95_ms = p95_ms,
        .docs_per_s = docs_per_s,
        .checksum = checksum,
        .graph_stats = .{},
        .metal_generated_quant = MetalGeneratedQuantStats.diff(before_metal_generated, after_metal_generated),
    };
}

fn printResult(opts: Options, doc_count: usize, result: BenchResult) void {
    switch (opts.format) {
        .text => {
            print("reranker_e2e backend={s} model={s} docs={d} warmup={d} measure={d}\n", .{
                @tagName(opts.backend),
                opts.model_dir,
                doc_count,
                opts.warmup_iters,
                opts.measure_iters,
            });
            print("avg_ms={d:.3} p50_ms={d:.3} min_ms={d:.3} p95_ms={d:.3} docs_per_s={d:.3} checksum={d:.6}\n", .{
                result.avg_ms,
                result.p50_ms,
                result.min_ms,
                result.p95_ms,
                result.docs_per_s,
                result.checksum,
            });
            print("graph_mode={s} qmatmul_variant={s} graph_captures={d} graph_replays={d} graph_fallbacks={d} graph_capture_failures={d} graph_buckets={d} graph_owned_bytes={d} graph_dynamic_copy_bytes={d} graph_last_fallback_reason={s}\n", .{
                graphModeName(),
                qmatmulVariantName(),
                result.graph_stats.captures,
                result.graph_stats.replays,
                result.graph_stats.fallbacks,
                result.graph_stats.capture_failures,
                result.graph_stats.buckets,
                result.graph_stats.graph_owned_bytes,
                result.graph_stats.dynamic_copy_bytes,
                result.graph_stats.last_fallback_reason,
            });
            const metal_generated_top = result.metal_generated_quant.topFamily();
            print("metal_generated_quant={d} metal_generated_top={s}:{d} metal_generated_families={d} metal_generated_q4_k={d}/{d}/{d} metal_generated_q5_k={d}/{d}/{d} metal_generated_q6_k={d}/{d}/{d} metal_generated_q8_0={d}/{d}/{d}/{d} metal_q4_k_rows={d}/{d}/{d}/{d} metal_q6_k_rows={d}/{d}/{d}/{d} metal_q8_0_rows={d}/{d}/{d}/{d}\n", .{
                result.metal_generated_quant.generatedTotal(),
                metal_generated_top.name,
                metal_generated_top.count,
                result.metal_generated_quant.nonzeroFamilyCount(),
                result.metal_generated_quant.q4_k,
                result.metal_generated_quant.q4_k_bias,
                result.metal_generated_quant.q4_k_bias_gelu,
                result.metal_generated_quant.q5_k,
                result.metal_generated_quant.q5_k_bias,
                result.metal_generated_quant.q5_k_bias_gelu,
                result.metal_generated_quant.q6_k,
                result.metal_generated_quant.q6_k_bias,
                result.metal_generated_quant.q6_k_bias_gelu,
                result.metal_generated_quant.q8_0,
                result.metal_generated_quant.q8_0_bias,
                result.metal_generated_quant.q8_0_bias_gelu,
                result.metal_generated_quant.q8_0_relu,
                result.metal_generated_quant.q4_k_rows_1,
                result.metal_generated_quant.q4_k_rows_2_8,
                result.metal_generated_quant.q4_k_rows_9_64,
                result.metal_generated_quant.q4_k_rows_65_plus,
                result.metal_generated_quant.q6_k_rows_1,
                result.metal_generated_quant.q6_k_rows_2_8,
                result.metal_generated_quant.q6_k_rows_9_64,
                result.metal_generated_quant.q6_k_rows_65_plus,
                result.metal_generated_quant.q8_0_rows_1,
                result.metal_generated_quant.q8_0_rows_2_8,
                result.metal_generated_quant.q8_0_rows_9_64,
                result.metal_generated_quant.q8_0_rows_65_plus,
            });
        },
        .csv => {
            print("{s},{s},{s},{s},{d},{d},{d},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.6},{d},{d},{d},{d},{d},{d},{d},{s}\n", .{
                @tagName(opts.backend),
                graphModeName(),
                qmatmulVariantName(),
                opts.model_dir,
                doc_count,
                opts.warmup_iters,
                opts.measure_iters,
                result.avg_ms,
                result.p50_ms,
                result.min_ms,
                result.p95_ms,
                result.docs_per_s,
                result.checksum,
                result.graph_stats.captures,
                result.graph_stats.replays,
                result.graph_stats.fallbacks,
                result.graph_stats.capture_failures,
                result.graph_stats.buckets,
                result.graph_stats.graph_owned_bytes,
                result.graph_stats.dynamic_copy_bytes,
                result.graph_stats.last_fallback_reason,
            });
        },
    }
}

fn printCsvHeader() void {
    print("backend,graph_mode,qmatmul_variant,model,docs,warmup_iters,measure_iters,avg_ms,p50_ms,min_ms,p95_ms,docs_per_s,checksum,graph_captures,graph_replays,graph_fallbacks,graph_capture_failures,graph_buckets,graph_owned_bytes,graph_dynamic_copy_bytes,graph_last_fallback_reason\n", .{});
}

fn graphModeName() []const u8 {
    if (platform.env.getenvBool("ANTFLY_CUDA_REQUIRE_DEBERTA_GRAPHS")) return "required";
    if (platform.env.getenvBool("ANTFLY_CUDA_ENABLE_DEBERTA_GRAPHS")) return "enabled";
    return "disabled";
}

fn qmatmulVariantName() []const u8 {
    return platform.env.getenv("ANTFLY_CUDA_QMATMUL_VARIANT") orelse "auto";
}

fn docsForCount(allocator: std.mem.Allocator, source: []const []const u8, count: usize) ![]const []const u8 {
    if (source.len == 0) return error.InvalidArguments;
    const docs = try allocator.alloc([]const u8, count);
    for (docs, 0..) |*doc, idx| {
        doc.* = source[idx % source.len];
    }
    return docs;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn medianMs(sorted_ns: []const u64) f64 {
    std.debug.assert(sorted_ns.len > 0);
    const upper = sorted_ns.len / 2;
    if (sorted_ns.len % 2 != 0) return nsToMs(sorted_ns[upper]);
    return (nsToMs(sorted_ns[upper - 1]) + nsToMs(sorted_ns[upper])) / 2.0;
}

fn wantsHelp(init: std.process.Init) bool {
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return true;
    }
    return false;
}

fn parseArgs(allocator: std.mem.Allocator, init: std.process.Init) !Options {
    var opts = Options{};
    errdefer opts.deinit(allocator);

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            if (opts.owns_model_dir) allocator.free(opts.model_dir);
            opts.model_dir = try allocator.dupe(u8, args_iter.next() orelse return error.MissingModelDirValue);
            opts.owns_model_dir = true;
        } else if (std.mem.eql(u8, arg, "--query")) {
            if (opts.owns_query) allocator.free(opts.query);
            opts.query = try allocator.dupe(u8, args_iter.next() orelse return error.MissingQueryValue);
            opts.owns_query = true;
        } else if (std.mem.eql(u8, arg, "--doc")) {
            try opts.docs.append(allocator, try allocator.dupe(u8, args_iter.next() orelse return error.MissingDocumentValue));
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = native_backend_choice.parse(args_iter.next() orelse return error.MissingBackendValue) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingWarmupItersValue, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingMeasureItersValue, 10);
        } else if (std.mem.eql(u8, arg, "--batch-sweep")) {
            opts.batch_sweep = true;
        } else if (std.mem.eql(u8, arg, "--format")) {
            opts.format = parseFormat(args_iter.next() orelse return error.MissingFormatValue) orelse return error.InvalidFormat;
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
    if (opts.measure_iters == 0) return error.InvalidMeasureIters;
    return opts;
}

fn parseFormat(value: []const u8) ?OutputFormat {
    if (std.mem.eql(u8, value, "text")) return .text;
    if (std.mem.eql(u8, value, "csv")) return .csv;
    return null;
}

fn printUsage() void {
    print(
        \\usage: zig build bench-reranker-e2e -- --model-dir <dir> [--backend auto|native|cuda|metal|mlx|onnx] [--query TEXT] [--doc TEXT]... [--warmup-iters N] [--measure-iters N] [--batch-sweep] [--format text|csv]
        \\
    , .{});
}
