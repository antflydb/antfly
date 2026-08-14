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

// Real-bundle GLiNER2 recognition benchmark.
//
// This loads a prepared split GLiNER2 bundle once via the production
// ModelManager, then measures repeated recognizeBatch calls against the loaded
// session.  The warm row is the number to compare with CLIP/CLAP warm e2e
// rows: tokenization + encoder/head forward + postprocess, without model-load
// or process-startup noise.

const std = @import("std");

const build_options = @import("build_options");
const inference = @import("inference_internal");
const backends = inference.backends;
const graph_runtime = inference.graph.runtime;
const kernel_jit = inference.graph.kernel_jit;
const model_manager_mod = inference.server.model_manager;
const native_compute = inference.native_compute.native;
const metal_generated_quant_stats = @import("metal_generated_quant_stats.zig");
const kernel_jit_profile_output = inference.kernel_jit_profile_output;
const session_factory = inference.architectures.session_factory;

const MetalGeneratedQuantStats = metal_generated_quant_stats.Stats;

const BackendChoice = enum {
    auto,
    native,
    metal,
    onnx,
    cuda,
};

const OutputFormat = enum {
    text,
    csv,
};

const BenchTask = enum {
    entities,
    relations,
    both,
};

const Options = struct {
    model_dir: []const u8 = "",
    text: []const u8 = "John Smith works for Apple Inc. and lives in San Francisco. Apple Inc. is located in Cupertino.",
    text_file: ?[]const u8 = null,
    text_batch_file: ?[]const u8 = null,
    text_explicit: bool = false,
    text_repeat: usize = 1,
    print_encoder_seq_len: bool = false,
    expected_encoder_seq_len: ?usize = null,
    backend: BackendChoice = .native,
    kernel_jit: kernel_jit.Config = .{},
    kernel_jit_mode_explicit: bool = false,
    kernel_jit_profile_out: ?[]const u8 = null,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    warmup_iters: usize = 1,
    measure_iters: usize = 5,
    batch_size: usize = 1,
    batch_size_explicit: bool = false,
    format: OutputFormat = .text,
    task: BenchTask = .entities,
    dump_entities: bool = false,
    ragged: bool = false,
    labels: std.ArrayListUnmanaged([]const u8) = .empty,
    relation_labels: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.labels.deinit(allocator);
        self.relation_labels.deinit(allocator);
    }
};

const QuantCounters = struct {
    q4q5: u64 = 0,
    q4q5_pair: u64 = 0,
    q4q5_triple: u64 = 0,
    q4q5_panel: u64 = 0,
    dequant: u64 = 0,
    dequant_pair: u64 = 0,
    dequant_triple: u64 = 0,
    q8_0: u64 = 0,
    q8_0_pair: u64 = 0,
    q8_0_triple: u64 = 0,
};

/// Per-request CUDA evidence for dense FP16 encoder dispatch. The benchmark
/// reports the aggregate over the measured samples, allowing the CSV result
/// to prove that timing came from the intended tensor-core route instead of
/// the F32 compatibility fallback.
const CudaCounters = struct {
    h2d_bytes: usize = 0,
    d2h_bytes: usize = 0,
    f16_cublaslt_linear_calls: usize = 0,
    f16_cublaslt_qkv_calls: usize = 0,
    f16_activation_staging_calls: usize = 0,
    f16_cublaslt_fallbacks: usize = 0,
    f16_scalar_linear_calls: usize = 0,
    deberta_fused_attention_calls: usize = 0,
    deberta_fused_attention_fallbacks: usize = 0,
    deberta_stream_f16_attention_calls: usize = 0,
    deberta_stream_f16_attention_fallbacks: usize = 0,
    deberta_stream_f16_staging_calls: usize = 0,
    deberta_materialized_f16_attention_calls: usize = 0,
    deberta_materialized_f16_attention_fallbacks: usize = 0,
    deberta_materialized_workspace_rejections: usize = 0,
    deberta_materialized_workspace_peak_bytes: usize = 0,
    deberta_generated_tc_attention_calls: usize = 0,
    deberta_generated_tc_m32_attention_calls: usize = 0,
    deberta_generated_tc_m16_attention_calls: usize = 0,
    deberta_generated_tc_attention_fallbacks: usize = 0,

    fn add(self: CudaCounters, other: CudaCounters) CudaCounters {
        return .{
            .h2d_bytes = self.h2d_bytes + other.h2d_bytes,
            .d2h_bytes = self.d2h_bytes + other.d2h_bytes,
            .f16_cublaslt_linear_calls = self.f16_cublaslt_linear_calls + other.f16_cublaslt_linear_calls,
            .f16_cublaslt_qkv_calls = self.f16_cublaslt_qkv_calls + other.f16_cublaslt_qkv_calls,
            .f16_activation_staging_calls = self.f16_activation_staging_calls + other.f16_activation_staging_calls,
            .f16_cublaslt_fallbacks = self.f16_cublaslt_fallbacks + other.f16_cublaslt_fallbacks,
            .f16_scalar_linear_calls = self.f16_scalar_linear_calls + other.f16_scalar_linear_calls,
            .deberta_fused_attention_calls = self.deberta_fused_attention_calls + other.deberta_fused_attention_calls,
            .deberta_fused_attention_fallbacks = self.deberta_fused_attention_fallbacks + other.deberta_fused_attention_fallbacks,
            .deberta_stream_f16_attention_calls = self.deberta_stream_f16_attention_calls + other.deberta_stream_f16_attention_calls,
            .deberta_stream_f16_attention_fallbacks = self.deberta_stream_f16_attention_fallbacks + other.deberta_stream_f16_attention_fallbacks,
            .deberta_stream_f16_staging_calls = self.deberta_stream_f16_staging_calls + other.deberta_stream_f16_staging_calls,
            .deberta_materialized_f16_attention_calls = self.deberta_materialized_f16_attention_calls + other.deberta_materialized_f16_attention_calls,
            .deberta_materialized_f16_attention_fallbacks = self.deberta_materialized_f16_attention_fallbacks + other.deberta_materialized_f16_attention_fallbacks,
            .deberta_materialized_workspace_rejections = self.deberta_materialized_workspace_rejections + other.deberta_materialized_workspace_rejections,
            .deberta_materialized_workspace_peak_bytes = @max(self.deberta_materialized_workspace_peak_bytes, other.deberta_materialized_workspace_peak_bytes),
            .deberta_generated_tc_attention_calls = self.deberta_generated_tc_attention_calls + other.deberta_generated_tc_attention_calls,
            .deberta_generated_tc_m32_attention_calls = self.deberta_generated_tc_m32_attention_calls + other.deberta_generated_tc_m32_attention_calls,
            .deberta_generated_tc_m16_attention_calls = self.deberta_generated_tc_m16_attention_calls + other.deberta_generated_tc_m16_attention_calls,
            .deberta_generated_tc_attention_fallbacks = self.deberta_generated_tc_attention_fallbacks + other.deberta_generated_tc_attention_fallbacks,
        };
    }
};

const Sample = struct {
    elapsed_ns: u64,
    entity_count: usize,
    relation_count: usize = 0,
    score_sum: f64,
    relation_score_sum: f64 = 0.0,
    quant: QuantCounters = .{},
    cuda: CudaCounters = .{},
    metal_generated_quant: MetalGeneratedQuantStats = .{},
    native_quant_stats_enabled: bool = false,
};

const Result = struct {
    task: BenchTask,
    mode: []const u8,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    min_ms: f64,
    max_ms: f64,
    entity_count: usize,
    relation_count: usize,
    score_sum: f64,
    relation_score_sum: f64,
    quant: QuantCounters,
    cuda: CudaCounters,
    metal_generated_quant: MetalGeneratedQuantStats,
    native_quant_stats_enabled: bool,
};

const TaskResult = struct {
    first: Result,
    warm: Result,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    var opts = try parseArgs(allocator, init);
    defer opts.deinit(allocator);
    if (opts.model_dir.len == 0) {
        printUsage();
        return error.MissingModelDir;
    }

    var session_manager = backends.SessionManager.initWithIo(allocator, init.io);
    configureBackendPreference(&session_manager, opts.backend);
    session_manager.kernel_jit = opts.kernel_jit;
    session_manager.kernel_jit_load_context = .startup_preload;
    session_manager.graph_runtime_strategy = opts.graph_runtime_strategy;

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const load_start = nowNs();
    const model = try model_manager.loadFromDir(opts.model_dir);
    const load_elapsed_ns = nowNs() - load_start;
    if (!model.isGlinerModel()) return error.NotGlinerModel;

    const fixture_text_owned = if (opts.text_file) |path|
        try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .limited(1024 * 1024))
    else
        null;
    defer if (fixture_text_owned) |bytes| allocator.free(bytes);
    const fixture_batch_owned = if (opts.text_batch_file) |path|
        try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .limited(8 * 1024 * 1024))
    else
        null;
    defer if (fixture_batch_owned) |bytes| allocator.free(bytes);

    var fixture_batch = std.ArrayListUnmanaged([]const u8).empty;
    defer fixture_batch.deinit(allocator);
    if (fixture_batch_owned) |bytes| {
        if (opts.text_repeat != 1) return error.TextRepeatUnsupportedForBatchFile;
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |line| {
            const text = std.mem.trim(u8, line, " \t\r");
            if (text.len > 0) try fixture_batch.append(allocator, text);
        }
        if (fixture_batch.items.len == 0) return error.EmptyTextFixture;
        if (opts.batch_size_explicit and opts.batch_size != fixture_batch.items.len) return error.BatchSizeDoesNotMatchTextBatch;
        opts.batch_size = fixture_batch.items.len;
    }
    if (opts.batch_size == 0) return error.InvalidBatchSize;

    const source_text = if (fixture_text_owned) |bytes| std.mem.trim(u8, bytes, " \t\r\n") else opts.text;
    var bench_text_owned: ?[]const u8 = null;
    defer if (bench_text_owned) |bytes| allocator.free(bytes);
    const texts = try allocator.alloc([]const u8, opts.batch_size);
    defer allocator.free(texts);
    var ragged_texts = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (ragged_texts.items) |item| allocator.free(item);
        ragged_texts.deinit(allocator);
    }
    if (fixture_batch.items.len > 0) {
        if (opts.ragged) return error.RaggedUnsupportedForBatchFile;
        @memcpy(texts, fixture_batch.items);
    } else {
        if (source_text.len == 0) return error.EmptyTextFixture;
        bench_text_owned = try repeatedText(allocator, source_text, opts.text_repeat);
        const bench_text = bench_text_owned.?;
        @memset(texts, bench_text);
        if (opts.ragged and opts.batch_size > 1) {
            // Real batches are ragged: give item i the first (i+1)/B fraction
            // of the words so per-item word counts differ.
            for (texts, 0..) |*slot, index| {
                var word_count: usize = 0;
                var word_it = std.mem.tokenizeScalar(u8, bench_text, ' ');
                while (word_it.next()) |_| word_count += 1;
                const keep = @max(1, word_count * (index + 1) / opts.batch_size);
                var end: usize = bench_text.len;
                var seen: usize = 0;
                var it = std.mem.tokenizeScalar(u8, bench_text, ' ');
                while (it.next()) |word| {
                    seen += 1;
                    if (seen == keep) {
                        end = @intFromPtr(word.ptr) - @intFromPtr(bench_text.ptr) + word.len;
                        break;
                    }
                }
                const copy = try allocator.dupe(u8, bench_text[0..end]);
                try ragged_texts.append(allocator, copy);
                slot.* = copy;
            }
        }
    }
    const labels: ?[]const []const u8 = if (opts.labels.items.len > 0) opts.labels.items else null;
    const relation_labels: ?[]const []const u8 = if (opts.relation_labels.items.len > 0) opts.relation_labels.items else null;

    var pipeline = model.glinerPipeline(allocator);
    if (opts.print_encoder_seq_len or opts.expected_encoder_seq_len != null) {
        const entity_labels = labels orelse return error.ExpectedEncoderSeqLenRequiresLabels;
        for (texts, 0..) |text, row_index| {
            const actual = try pipeline.entityEncoderTokenCount(text, entity_labels);
            if (opts.expected_encoder_seq_len) |expected| {
                std.debug.print("gliner2_e2e: row={} encoder_seq_len={} expected={}\n", .{ row_index, actual, expected });
                if (actual != expected) return error.UnexpectedEncoderSeqLen;
            } else {
                std.debug.print("gliner2_e2e: row={} encoder_seq_len={}\n", .{ row_index, actual });
            }
        }
    }

    var rows = std.ArrayListUnmanaged(Result).empty;
    defer rows.deinit(allocator);
    var profile_started = false;

    if (opts.task == .entities or opts.task == .both) {
        const start_profile = opts.kernel_jit_profile_out != null and !profile_started;
        const task_result = try runBenchmarkTask(allocator, &pipeline, model.session, texts, labels, relation_labels, .entities, load_elapsed_ns, opts.warmup_iters, opts.measure_iters, opts.dump_entities, start_profile);
        profile_started = profile_started or start_profile;
        try rows.append(allocator, task_result.first);
        try rows.append(allocator, task_result.warm);
    }

    if (opts.task == .relations or opts.task == .both) {
        const start_profile = opts.kernel_jit_profile_out != null and !profile_started;
        const task_result = try runBenchmarkTask(allocator, &pipeline, model.session, texts, labels, relation_labels, .relations, load_elapsed_ns, opts.warmup_iters, opts.measure_iters, opts.dump_entities, start_profile);
        profile_started = profile_started or start_profile;
        try rows.append(allocator, task_result.first);
        try rows.append(allocator, task_result.warm);
    }

    if (opts.kernel_jit_profile_out) |path| {
        if (comptime !build_options.enable_metal) return error.MetalWorkloadProfileUnavailable;
        if (!profile_started) return error.MetalWorkloadProfileUnavailable;
        var capture = (try session_factory.endMetalWorkloadProfile(model.session, allocator, true)) orelse
            return error.MetalWorkloadProfileUnavailable;
        defer capture.deinit();
        const report = capture.report(.{
            .batch = @intCast(opts.batch_size),
        });
        try kernel_jit_profile_output.writeFile(allocator, init.io, path, report);
        kernel_jit_profile_output.logWriteSummary(path, report);
    }

    switch (opts.format) {
        .text => {
            for (rows.items) |row| printText(opts, row);
        },
        .csv => {
            var stdout_buffer: [4096]u8 = undefined;
            var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
            const stdout = &stdout_writer.interface;
            try printCsvHeader(stdout);
            for (rows.items) |row| try printCsv(stdout, opts, row);
            try stdout.flush();
        },
    }

    if (opts.format == .text and model.session.backend() != .onnx) {
        var debug_backend = try session_factory.getComputeBackend(model.session, allocator);
        defer debug_backend.deinit();
        const provider_stats = debug_backend.debugTimingSnapshot().provider;
        std.debug.print(
            "provider_stats: mps_linears={} dense_f16_mb={} dense_f16_slots={} qkv_pack_mb={} relative_cache_mb={} runtime_mb={} quant_qkv={} quant_linear={} qkv_packed={}/{} ffn_fused={}/{}/{} attention_flash={} attention_legacy={} attention_gemm={}/{} compute_encoders={} last_frame_compute_encoders={} last_frame_attention={}\n",
            .{
                provider_stats.metal_runtime_last_frame_mps_dense_linear_count,
                provider_stats.metal_runtime_dense_linear_f16_weight_bytes / (1024 * 1024),
                provider_stats.metal_runtime_dense_linear_f16_slots,
                provider_stats.metal_runtime_dense_qkv_packed_bytes / (1024 * 1024),
                provider_stats.metal_runtime_deberta_relative_cache_bytes / (1024 * 1024),
                provider_stats.metal_runtime_total_bytes / (1024 * 1024),
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
                provider_stats.metal_runtime_compute_encoder_count,
                provider_stats.metal_runtime_last_frame_compute_encoder_count,
                provider_stats.metal_runtime_last_frame_compute_attention_count,
            },
        );
    }
}

fn runBenchmarkTask(
    allocator: std.mem.Allocator,
    pipeline: anytype,
    session: backends.Session,
    texts: []const []const u8,
    labels: ?[]const []const u8,
    relation_labels: ?[]const []const u8,
    task: BenchTask,
    load_elapsed_ns: u64,
    warmup_iters: usize,
    measure_iters: usize,
    dump_entities: bool,
    start_profile: bool,
) !TaskResult {
    const first = try runTask(allocator, pipeline, session, texts, labels, relation_labels, task, dump_entities);
    const first_run = Result{
        .task = task,
        .mode = "first_run",
        .avg_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .p50_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .p95_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .min_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .max_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .entity_count = first.entity_count,
        .relation_count = first.relation_count,
        .score_sum = first.score_sum,
        .relation_score_sum = first.relation_score_sum,
        .quant = first.quant,
        .cuda = first.cuda,
        .metal_generated_quant = first.metal_generated_quant,
        .native_quant_stats_enabled = first.native_quant_stats_enabled,
    };

    for (0..warmup_iters) |_| {
        _ = try runTask(allocator, pipeline, session, texts, labels, relation_labels, task, false);
    }
    if (start_profile and !try session_factory.beginMetalWorkloadProfile(session, .encoder)) {
        return error.MetalWorkloadProfileUnavailable;
    }

    const samples = try allocator.alloc(Sample, measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| {
        sample.* = try runTask(allocator, pipeline, session, texts, labels, relation_labels, task, false);
    }
    const warm = try resultFromSamples(
        allocator,
        task,
        "warm_loaded_session",
        samples,
        session.backend() == .cuda,
    );

    return .{ .first = first_run, .warm = warm };
}

fn runTask(
    allocator: std.mem.Allocator,
    pipeline: anytype,
    session: backends.Session,
    texts: []const []const u8,
    labels: ?[]const []const u8,
    relation_labels: ?[]const []const u8,
    task: BenchTask,
    dump_entities: bool,
) !Sample {
    native_compute.resetNativeQuantDispatchStats();
    const before_metal_generated = metal_generated_quant_stats.snapshotForSession(allocator, session);
    const before_cuda = session_factory.getCudaRuntimeStats(session);
    const start = nowNs();

    switch (task) {
        .entities => {
            const entities = try pipeline.recognizeBatch(texts, labels);
            const elapsed_ns = nowNs() - start;
            defer freeEntities(pipeline.allocator, entities);

            var entity_count: usize = 0;
            var score_sum: f64 = 0.0;
            for (entities) |row| {
                entity_count += row.len;
                for (row) |entity| score_sum += entity.score;
            }
            if (dump_entities) dumpEntityRows("entities", entities);
            const after_metal_generated = metal_generated_quant_stats.snapshotForSession(allocator, session);
            return .{
                .elapsed_ns = elapsed_ns,
                .entity_count = entity_count,
                .score_sum = score_sum,
                .quant = quantCountersFromStats(native_compute.nativeQuantDispatchStats()),
                .cuda = cudaCountersFromStats(before_cuda, session_factory.getCudaRuntimeStats(session)),
                .metal_generated_quant = MetalGeneratedQuantStats.diff(before_metal_generated, after_metal_generated),
                .native_quant_stats_enabled = native_compute.nativeQuantDispatchStatsEnabled(),
            };
        },
        .relations => {
            const extracted = try pipeline.extractRelationsBatch(texts, labels, relation_labels);
            const elapsed_ns = nowNs() - start;
            defer freeEntities(pipeline.allocator, extracted.entities);
            defer freeRelations(pipeline.allocator, extracted.relations);

            var entity_count: usize = 0;
            var relation_count: usize = 0;
            var score_sum: f64 = 0.0;
            var relation_score_sum: f64 = 0.0;
            for (extracted.entities) |row| {
                entity_count += row.len;
                for (row) |entity| score_sum += entity.score;
            }
            for (extracted.relations) |row| {
                relation_count += row.len;
                for (row) |relation| relation_score_sum += relation.score;
            }
            if (dump_entities) {
                dumpEntityRows("relations.entities", extracted.entities);
                dumpRelationRows(extracted.relations);
            }
            const after_metal_generated = metal_generated_quant_stats.snapshotForSession(allocator, session);
            return .{
                .elapsed_ns = elapsed_ns,
                .entity_count = entity_count,
                .relation_count = relation_count,
                .score_sum = score_sum,
                .relation_score_sum = relation_score_sum,
                .quant = quantCountersFromStats(native_compute.nativeQuantDispatchStats()),
                .cuda = cudaCountersFromStats(before_cuda, session_factory.getCudaRuntimeStats(session)),
                .metal_generated_quant = MetalGeneratedQuantStats.diff(before_metal_generated, after_metal_generated),
                .native_quant_stats_enabled = native_compute.nativeQuantDispatchStatsEnabled(),
            };
        },
        .both => unreachable,
    }
}

fn resultFromSamples(
    allocator: std.mem.Allocator,
    task: BenchTask,
    mode: []const u8,
    samples: []const Sample,
    use_interpolated_percentiles: bool,
) !Result {
    if (samples.len == 0) return error.InvalidMeasureIters;
    const sorted = try allocator.dupe(Sample, samples);
    defer allocator.free(sorted);
    std.mem.sort(Sample, sorted, {}, struct {
        fn lessThan(_: void, a: Sample, b: Sample) bool {
            return a.elapsed_ns < b.elapsed_ns;
        }
    }.lessThan);

    var total_ns: u128 = 0;
    for (samples) |sample| total_ns += sample.elapsed_ns;
    var metal_generated_quant = MetalGeneratedQuantStats{};
    for (samples) |sample| metal_generated_quant = metal_generated_quant.add(sample.metal_generated_quant);
    var cuda = CudaCounters{};
    for (samples) |sample| cuda = cuda.add(sample.cuda);
    const avg_ns: u64 = @intCast(total_ns / samples.len);
    const p50_idx = samples.len / 2;
    const p95_idx = @min(samples.len - 1, (samples.len * 95 + 99) / 100 - 1);
    const last = samples[samples.len - 1];
    return .{
        .task = task,
        .mode = mode,
        .avg_ms = nsToMs(avg_ns),
        .p50_ms = if (use_interpolated_percentiles) percentileMs(sorted, 0.50) else nsToMs(sorted[p50_idx].elapsed_ns),
        .p95_ms = if (use_interpolated_percentiles) percentileMs(sorted, 0.95) else nsToMs(sorted[p95_idx].elapsed_ns),
        .min_ms = nsToMs(sorted[0].elapsed_ns),
        .max_ms = nsToMs(sorted[sorted.len - 1].elapsed_ns),
        .entity_count = last.entity_count,
        .relation_count = last.relation_count,
        .score_sum = last.score_sum,
        .relation_score_sum = last.relation_score_sum,
        .quant = last.quant,
        .cuda = cuda,
        .metal_generated_quant = metal_generated_quant,
        .native_quant_stats_enabled = last.native_quant_stats_enabled,
    };
}

fn freeEntities(allocator: std.mem.Allocator, all_entities: anytype) void {
    for (all_entities) |entities| {
        for (entities) |entity| allocator.free(entity.text);
        allocator.free(entities);
    }
    allocator.free(all_entities);
}

fn freeRelations(allocator: std.mem.Allocator, all_relations: anytype) void {
    for (all_relations) |relations| {
        for (relations) |*relation| relation.deinit(allocator);
        allocator.free(relations);
    }
    allocator.free(all_relations);
}

fn dumpEntityRows(prefix: []const u8, all_entities: anytype) void {
    for (all_entities, 0..) |entities, row_idx| {
        for (entities, 0..) |entity, entity_idx| {
            std.debug.print(
                "{s}[{}][{}]: label={s} span={}..{} score={d:.8} text=\"{s}\"\n",
                .{ prefix, row_idx, entity_idx, entity.label, entity.start, entity.end, entity.score, entity.text },
            );
        }
    }
}

fn dumpRelationRows(all_relations: anytype) void {
    for (all_relations, 0..) |relations, row_idx| {
        for (relations, 0..) |relation, relation_idx| {
            std.debug.print(
                "relations[{}][{}]: label={s} score={d:.8} head={s}@{}..{} tail={s}@{}..{}\n",
                .{
                    row_idx,
                    relation_idx,
                    relation.label,
                    relation.score,
                    relation.head.text,
                    relation.head.start,
                    relation.head.end,
                    relation.tail.text,
                    relation.tail.start,
                    relation.tail.end,
                },
            );
        }
    }
}

fn quantCountersFromStats(stats: native_compute.NativeQuantDispatchStats) QuantCounters {
    return .{
        .q4q5 = stats.q4_q5_k_q8k_activation,
        .q4q5_pair = stats.q4_q5_k_q8k_activation_pair,
        .q4q5_triple = stats.q4_q5_k_q8k_activation_triple,
        .q4q5_panel = stats.q4_q5_k_prepared_panel,
        .dequant = stats.dequant_sgemm,
        .dequant_pair = stats.dequant_sgemm_pair,
        .dequant_triple = stats.dequant_sgemm_triple,
        .q8_0 = stats.q8_0_direct,
        .q8_0_pair = stats.q8_0_pair,
        .q8_0_triple = stats.q8_0_triple,
    };
}

fn cudaCountersFromStats(before: anytype, after: anytype) CudaCounters {
    if (comptime build_options.enable_cuda) {
        const before_stats = before orelse return .{};
        const after_stats = after orelse return .{};
        return .{
            .h2d_bytes = after_stats.h2d_bytes - before_stats.h2d_bytes,
            .d2h_bytes = after_stats.d2h_bytes - before_stats.d2h_bytes,
            .f16_cublaslt_linear_calls = after_stats.f16_cublaslt_linear_calls - before_stats.f16_cublaslt_linear_calls,
            .f16_cublaslt_qkv_calls = after_stats.f16_cublaslt_qkv_calls - before_stats.f16_cublaslt_qkv_calls,
            .f16_activation_staging_calls = after_stats.f16_cublaslt_activation_staging_calls - before_stats.f16_cublaslt_activation_staging_calls,
            .f16_cublaslt_fallbacks = after_stats.f16_cublaslt_fallbacks - before_stats.f16_cublaslt_fallbacks,
            .f16_scalar_linear_calls = after_stats.f16_scalar_linear_calls - before_stats.f16_scalar_linear_calls,
            .deberta_fused_attention_calls = after_stats.deberta_fused_attention_calls - before_stats.deberta_fused_attention_calls,
            .deberta_fused_attention_fallbacks = after_stats.deberta_fused_attention_fallbacks - before_stats.deberta_fused_attention_fallbacks,
            .deberta_stream_f16_attention_calls = after_stats.deberta_stream_f16_attention_calls - before_stats.deberta_stream_f16_attention_calls,
            .deberta_stream_f16_attention_fallbacks = after_stats.deberta_stream_f16_attention_fallbacks - before_stats.deberta_stream_f16_attention_fallbacks,
            .deberta_stream_f16_staging_calls = after_stats.deberta_stream_f16_staging_calls - before_stats.deberta_stream_f16_staging_calls,
            .deberta_materialized_f16_attention_calls = after_stats.deberta_materialized_f16_attention_calls - before_stats.deberta_materialized_f16_attention_calls,
            .deberta_materialized_f16_attention_fallbacks = after_stats.deberta_materialized_f16_attention_fallbacks - before_stats.deberta_materialized_f16_attention_fallbacks,
            .deberta_materialized_workspace_rejections = after_stats.deberta_materialized_workspace_rejections - before_stats.deberta_materialized_workspace_rejections,
            .deberta_materialized_workspace_peak_bytes = after_stats.deberta_materialized_workspace_peak_bytes,
            .deberta_generated_tc_attention_calls = after_stats.deberta_generated_tc_attention_calls - before_stats.deberta_generated_tc_attention_calls,
            .deberta_generated_tc_m32_attention_calls = after_stats.deberta_generated_tc_m32_attention_calls - before_stats.deberta_generated_tc_m32_attention_calls,
            .deberta_generated_tc_m16_attention_calls = after_stats.deberta_generated_tc_m16_attention_calls - before_stats.deberta_generated_tc_m16_attention_calls,
            .deberta_generated_tc_attention_fallbacks = after_stats.deberta_generated_tc_attention_fallbacks - before_stats.deberta_generated_tc_attention_fallbacks,
        };
    } else {
        return .{};
    }
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
        .onnx => &.{backends.BackendType.onnx},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
    };
}

fn parseArgs(allocator: std.mem.Allocator, init: std.process.Init) !Options {
    var opts = Options{};
    errdefer opts.deinit(allocator);
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            opts.model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--text")) {
            if (opts.text_file != null or opts.text_batch_file != null) return error.DuplicateTextSource;
            opts.text = args.next() orelse return error.MissingText;
            opts.text_explicit = true;
        } else if (std.mem.eql(u8, arg, "--text-file")) {
            if (opts.text_explicit or opts.text_file != null or opts.text_batch_file != null) return error.DuplicateTextSource;
            opts.text_file = args.next() orelse return error.MissingTextFile;
        } else if (std.mem.eql(u8, arg, "--text-batch-file")) {
            if (opts.text_explicit or opts.text_file != null or opts.text_batch_file != null) return error.DuplicateTextSource;
            opts.text_batch_file = args.next() orelse return error.MissingTextBatchFile;
        } else if (std.mem.eql(u8, arg, "--text-repeat")) {
            opts.text_repeat = try std.fmt.parseInt(usize, args.next() orelse return error.MissingTextRepeat, 10);
            if (opts.text_repeat == 0) return error.InvalidTextRepeat;
        } else if (std.mem.eql(u8, arg, "--print-encoder-seq-len")) {
            opts.print_encoder_seq_len = true;
        } else if (std.mem.eql(u8, arg, "--expect-encoder-seq-len")) {
            if (opts.expected_encoder_seq_len != null) return error.DuplicateExpectedEncoderSeqLen;
            opts.expected_encoder_seq_len = try std.fmt.parseInt(usize, args.next() orelse return error.MissingExpectedEncoderSeqLen, 10);
            if (opts.expected_encoder_seq_len.? == 0) return error.InvalidExpectedEncoderSeqLen;
        } else if (std.mem.eql(u8, arg, "--task")) {
            opts.task = parseTask(args.next() orelse return error.MissingTask) orelse return error.InvalidTask;
        } else if (std.mem.eql(u8, arg, "--label")) {
            try opts.labels.append(allocator, args.next() orelse return error.MissingLabel);
        } else if (std.mem.eql(u8, arg, "--relation-label")) {
            try opts.relation_labels.append(allocator, args.next() orelse return error.MissingRelationLabel);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = parseBackend(args.next() orelse return error.MissingBackend) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--kernel-jit-mode")) {
            opts.kernel_jit.mode = std.meta.stringToEnum(kernel_jit.Mode, args.next() orelse return error.MissingKernelJitMode) orelse return error.InvalidKernelJitMode;
            opts.kernel_jit_mode_explicit = true;
        } else if (std.mem.eql(u8, arg, kernel_jit_profile_output.cli_flag)) {
            const path = args.next() orelse return error.MissingKernelJitProfileOut;
            try kernel_jit_profile_output.validateOutputPath(path);
            opts.kernel_jit_profile_out = path;
        } else if (std.mem.eql(u8, arg, kernel_jit_profile_output.qualified_profile_cli_flag)) {
            const path = args.next() orelse return error.MissingKernelJitQualifiedProfile;
            try kernel_jit_profile_output.validateOutputPath(path);
            opts.kernel_jit.qualified_profile_path = path;
        } else if (std.mem.eql(u8, arg, "--kernel-jit-cache-dir")) {
            opts.kernel_jit.cache_dir = args.next() orelse return error.MissingKernelJitCacheDir;
        } else if (std.mem.eql(u8, arg, "--kernel-jit-max-cache-mb")) {
            opts.kernel_jit.max_cache_bytes_mb = try std.fmt.parseInt(usize, args.next() orelse return error.MissingKernelJitMaxCacheMb, 10);
        } else if (std.mem.eql(u8, arg, "--kernel-jit-preload-budget-ms")) {
            opts.kernel_jit.preload_budget_ms = try std.fmt.parseInt(u64, args.next() orelse return error.MissingKernelJitPreloadBudgetMs, 10);
        } else if (std.mem.eql(u8, arg, "--graph-runtime")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(args.next() orelse return error.MissingGraphRuntime) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingWarmupIters, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingMeasureIters, 10);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            opts.batch_size = try std.fmt.parseInt(usize, args.next() orelse return error.MissingBatchSize, 10);
            opts.batch_size_explicit = true;
        } else if (std.mem.eql(u8, arg, "--format")) {
            opts.format = parseFormat(args.next() orelse return error.MissingFormat) orelse return error.InvalidFormat;
        } else if (std.mem.eql(u8, arg, "--dump-entities")) {
            opts.dump_entities = true;
        } else if (std.mem.eql(u8, arg, "--ragged")) {
            opts.ragged = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
    opts.kernel_jit.mode = try kernel_jit.resolveProfileCaptureMode(
        opts.kernel_jit.mode,
        opts.kernel_jit_mode_explicit,
        opts.kernel_jit_profile_out != null,
        opts.kernel_jit.qualified_profile_path != null,
    );
    opts.kernel_jit.profile_capture_only = opts.kernel_jit_profile_out != null;
    try kernel_jit.validateMetalProfileBackend(
        opts.backend == .metal,
        opts.kernel_jit_profile_out != null,
        opts.kernel_jit.qualified_profile_path != null,
    );
    try opts.kernel_jit.validate();
    return opts;
}

fn parseTask(value: []const u8) ?BenchTask {
    if (std.ascii.eqlIgnoreCase(value, "entities")) return .entities;
    if (std.ascii.eqlIgnoreCase(value, "relations")) return .relations;
    if (std.ascii.eqlIgnoreCase(value, "both")) return .both;
    return null;
}

fn parseBackend(value: []const u8) ?BackendChoice {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "onnx")) return .onnx;
    if (std.ascii.eqlIgnoreCase(value, "cuda")) return .cuda;
    return null;
}

fn parseFormat(value: []const u8) ?OutputFormat {
    if (std.ascii.eqlIgnoreCase(value, "text")) return .text;
    if (std.ascii.eqlIgnoreCase(value, "csv")) return .csv;
    return null;
}

fn printText(opts: Options, result: Result) void {
    std.debug.print(
        "{s}/{s}: model_dir={s} backend={s} batch_size={} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} entity_count={} relation_count={} score_sum={d:.6} relation_score_sum={d:.6} native_quant_stats={s} q4q5={} q4q5_pair={} q4q5_triple={} q4q5_panel={} dequant={} dequant_pair={} dequant_triple={} q8_0={} q8_0_pair={} q8_0_triple={} cuda_h2d_bytes={} cuda_d2h_bytes={} cuda_f16_cublaslt_linear_calls={} cuda_f16_cublaslt_qkv_calls={} cuda_f16_activation_staging_calls={} cuda_f16_cublaslt_fallbacks={} cuda_f16_scalar_linear_calls={}",
        .{
            @tagName(result.task),
            result.mode,
            opts.model_dir,
            @tagName(opts.backend),
            opts.batch_size,
            result.avg_ms,
            result.p50_ms,
            result.p95_ms,
            result.min_ms,
            result.max_ms,
            result.entity_count,
            result.relation_count,
            result.score_sum,
            result.relation_score_sum,
            if (result.native_quant_stats_enabled) "enabled" else "disabled",
            result.quant.q4q5,
            result.quant.q4q5_pair,
            result.quant.q4q5_triple,
            result.quant.q4q5_panel,
            result.quant.dequant,
            result.quant.dequant_pair,
            result.quant.dequant_triple,
            result.quant.q8_0,
            result.quant.q8_0_pair,
            result.quant.q8_0_triple,
            result.cuda.h2d_bytes,
            result.cuda.d2h_bytes,
            result.cuda.f16_cublaslt_linear_calls,
            result.cuda.f16_cublaslt_qkv_calls,
            result.cuda.f16_activation_staging_calls,
            result.cuda.f16_cublaslt_fallbacks,
            result.cuda.f16_scalar_linear_calls,
        },
    );
    std.debug.print(
        " cuda_deberta_fused_attention_calls={} cuda_deberta_fused_attention_fallbacks={} cuda_deberta_stream_f16_attention_calls={} cuda_deberta_stream_f16_attention_fallbacks={} cuda_deberta_stream_f16_staging_calls={} cuda_deberta_materialized_f16_attention_calls={} cuda_deberta_materialized_f16_attention_fallbacks={} cuda_deberta_materialized_workspace_rejections={} cuda_deberta_materialized_workspace_peak_bytes={} cuda_deberta_generated_tc_attention_calls={} cuda_deberta_generated_tc_m32_attention_calls={} cuda_deberta_generated_tc_m16_attention_calls={} cuda_deberta_generated_tc_attention_fallbacks={}\n",
        .{
            result.cuda.deberta_fused_attention_calls,
            result.cuda.deberta_fused_attention_fallbacks,
            result.cuda.deberta_stream_f16_attention_calls,
            result.cuda.deberta_stream_f16_attention_fallbacks,
            result.cuda.deberta_stream_f16_staging_calls,
            result.cuda.deberta_materialized_f16_attention_calls,
            result.cuda.deberta_materialized_f16_attention_fallbacks,
            result.cuda.deberta_materialized_workspace_rejections,
            result.cuda.deberta_materialized_workspace_peak_bytes,
            result.cuda.deberta_generated_tc_attention_calls,
            result.cuda.deberta_generated_tc_m32_attention_calls,
            result.cuda.deberta_generated_tc_m16_attention_calls,
            result.cuda.deberta_generated_tc_attention_fallbacks,
        },
    );
    const metal_generated_top = result.metal_generated_quant.topFamily();
    std.debug.print(
        "{s}/{s}: metal_generated_quant={} metal_generated_top={s}:{} metal_generated_families={} metal_generated_q4_k={}/{}/{} metal_generated_q5_k={}/{}/{} metal_generated_q6_k={}/{}/{} metal_generated_q8_0={}/{}/{}/{} metal_q4_k_rows={}/{}/{}/{} metal_q6_k_rows={}/{}/{}/{} metal_q8_0_rows={}/{}/{}/{}\n",
        .{
            @tagName(result.task),
            result.mode,
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
        },
    );
    std.debug.print(
        "{s}/{s}: metal_jit_exact_q4_0={} metal_jit_exact_q4_k={}\n",
        .{
            @tagName(result.task),
            result.mode,
            result.metal_generated_quant.jit_exact_q4_0,
            result.metal_generated_quant.jit_exact_q4_k,
        },
    );
}

fn printCsvHeader(writer: *std.Io.Writer) !void {
    try writer.writeAll("task,mode,model_dir,backend,batch_size,avg_ms,p50_ms,p95_ms,min_ms,max_ms,entity_count,relation_count,score_sum,relation_score_sum,native_quant_stats_enabled,q4q5,q4q5_pair,q4q5_triple,q4q5_panel,dequant,dequant_pair,dequant_triple,q8_0,q8_0_pair,q8_0_triple,metal_jit_exact_q4_0,metal_jit_exact_q4_k,cuda_h2d_bytes,cuda_d2h_bytes,cuda_f16_cublaslt_linear_calls,cuda_f16_cublaslt_qkv_calls,cuda_f16_activation_staging_calls,cuda_f16_cublaslt_fallbacks,cuda_f16_scalar_linear_calls,cuda_deberta_fused_attention_calls,cuda_deberta_fused_attention_fallbacks,cuda_deberta_stream_f16_attention_calls,cuda_deberta_stream_f16_attention_fallbacks,cuda_deberta_stream_f16_staging_calls,cuda_deberta_materialized_f16_attention_calls,cuda_deberta_materialized_f16_attention_fallbacks,cuda_deberta_materialized_workspace_rejections,cuda_deberta_materialized_workspace_peak_bytes,cuda_deberta_generated_tc_attention_calls,cuda_deberta_generated_tc_m32_attention_calls,cuda_deberta_generated_tc_m16_attention_calls,cuda_deberta_generated_tc_attention_fallbacks\n");
}

fn printCsv(writer: *std.Io.Writer, opts: Options, result: Result) !void {
    try writer.print(
        "{s},{s},{s},{s},{},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{},{},{d:.6},{d:.6}",
        .{
            @tagName(result.task),
            result.mode,
            opts.model_dir,
            @tagName(opts.backend),
            opts.batch_size,
            result.avg_ms,
            result.p50_ms,
            result.p95_ms,
            result.min_ms,
            result.max_ms,
            result.entity_count,
            result.relation_count,
            result.score_sum,
            result.relation_score_sum,
        },
    );
    try writer.print(
        ",{},{},{},{},{},{},{},{},{},{},{},{},{}",
        .{
            result.native_quant_stats_enabled,
            result.quant.q4q5,
            result.quant.q4q5_pair,
            result.quant.q4q5_triple,
            result.quant.q4q5_panel,
            result.quant.dequant,
            result.quant.dequant_pair,
            result.quant.dequant_triple,
            result.quant.q8_0,
            result.quant.q8_0_pair,
            result.quant.q8_0_triple,
            result.metal_generated_quant.jit_exact_q4_0,
            result.metal_generated_quant.jit_exact_q4_k,
        },
    );
    try writer.print(
        ",{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        .{
            result.cuda.h2d_bytes,
            result.cuda.d2h_bytes,
            result.cuda.f16_cublaslt_linear_calls,
            result.cuda.f16_cublaslt_qkv_calls,
            result.cuda.f16_activation_staging_calls,
            result.cuda.f16_cublaslt_fallbacks,
            result.cuda.f16_scalar_linear_calls,
            result.cuda.deberta_fused_attention_calls,
            result.cuda.deberta_fused_attention_fallbacks,
            result.cuda.deberta_stream_f16_attention_calls,
            result.cuda.deberta_stream_f16_attention_fallbacks,
            result.cuda.deberta_stream_f16_staging_calls,
            result.cuda.deberta_materialized_f16_attention_calls,
            result.cuda.deberta_materialized_f16_attention_fallbacks,
            result.cuda.deberta_materialized_workspace_rejections,
            result.cuda.deberta_materialized_workspace_peak_bytes,
            result.cuda.deberta_generated_tc_attention_calls,
            result.cuda.deberta_generated_tc_m32_attention_calls,
            result.cuda.deberta_generated_tc_m16_attention_calls,
            result.cuda.deberta_generated_tc_attention_fallbacks,
        },
    );
}

fn printUsage() void {
    std.debug.print(
        \\usage: zig build bench-gliner2-e2e -- --model-dir <dir> [--task entities|relations|both] [--text TEXT | --text-file PATH | --text-batch-file PATH] [--text-repeat N] [--print-encoder-seq-len] [--expect-encoder-seq-len N] [--batch-size N] [--ragged] [--label NAME]... [--relation-label NAME]... [--backend auto|native|metal|onnx|cuda] [--kernel-jit-mode off|shadow|on|required] [--kernel-jit-cache-dir path] [--kernel-jit-max-cache-mb N] [--kernel-jit-preload-budget-ms N] [--kernel-jit-profile-out path] [--kernel-jit-qualified-profile path] [--graph-runtime partitioned] [--warmup-iters N] [--measure-iters N] [--format text|csv] [--dump-entities]
        \\  --text-batch-file reads one non-empty input per line and derives batch size unless --batch-size is supplied and matches.
        \\  --ragged applies only to a single repeated text source.
        \\  Profile capture selects shadow mode unless a conflicting mode is explicit.
        \\
    , .{});
}

fn repeatedText(allocator: std.mem.Allocator, text: []const u8, repeat: usize) ![]const u8 {
    if (repeat == 0) return error.InvalidTextRepeat;
    if (repeat == 1) return allocator.dupe(u8, text);
    const repeated_len = try std.math.mul(usize, text.len, repeat);
    const spaces = repeat - 1;
    const out = try allocator.alloc(u8, try std.math.add(usize, repeated_len, spaces));
    var offset: usize = 0;
    for (0..repeat) |i| {
        if (i != 0) {
            out[offset] = ' ';
            offset += 1;
        }
        @memcpy(out[offset..][0..text.len], text);
        offset += text.len;
    }
    return out;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

/// CUDA comparison rows use this interpolation to match the Fastino reference
/// harness. Other backends retain the benchmark's established nearest-rank
/// output so this CUDA work does not rewrite their historical results.
fn percentileMs(sorted: []const Sample, ratio: f64) f64 {
    std.debug.assert(sorted.len > 0);
    std.debug.assert(ratio >= 0.0 and ratio <= 1.0);
    const position = @as(f64, @floatFromInt(sorted.len - 1)) * ratio;
    const lower: usize = @intFromFloat(@floor(position));
    const upper: usize = @intFromFloat(@ceil(position));
    if (lower == upper) return nsToMs(sorted[lower].elapsed_ns);
    const fraction = position - @as(f64, @floatFromInt(lower));
    const lower_ns = @as(f64, @floatFromInt(sorted[lower].elapsed_ns));
    const upper_ns = @as(f64, @floatFromInt(sorted[upper].elapsed_ns));
    return (lower_ns * (1.0 - fraction) + upper_ns * fraction) / 1.0e6;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}
