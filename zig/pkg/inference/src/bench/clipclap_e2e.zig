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

// Real-bundle CLIP/CLAP embedding benchmark.
//
// Unlike clipclap_native.zig, this loads an actual prepared model directory and
// exercises the production embedding pipeline: tokenization, image decode/resize,
// audio decode/mel features, encoder, pooling/projection/normalize, and response
// serialization.

const std = @import("std");
const build_options = @import("build_options");

const inference = @import("inference_internal");
const backends = inference.backends;
const graph_runtime = inference.graph.runtime;
const graph_executor_stats = inference.graph.executor_stats;
const native_compute = inference.native_compute.native;
const model_manager_mod = inference.server.model_manager;
const embedding_mod = inference.pipelines.embedding;
const native_backend_guard = inference.native_backend_guard;
const metal_runtime = inference.metal_runtime;
const mlx_backend = inference.mlx_backend;

const max_file_bytes = 512 * 1024 * 1024;

const BackendChoice = enum {
    auto,
    onnx,
    native,
    metal,
    mlx,
    cuda,
};

const OutputFormat = enum {
    text,
    csv,
};

const Modality = enum {
    text,
    image,
    audio,
    mixed,
};

const InputRef = struct {
    modality: Modality,
    index: usize,
};

const Options = struct {
    model_dir: []const u8 = "",
    backend: BackendChoice = .auto,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    resident_projection_required: bool = false,
    warmup_iters: usize = 1,
    measure_iters: usize = 5,
    include_cold: bool = true,
    format: OutputFormat = .text,
    texts: std.ArrayListUnmanaged([]const u8) = .empty,
    image_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    audio_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    order: std.ArrayListUnmanaged(InputRef) = .empty,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        self.image_paths.deinit(allocator);
        self.audio_paths.deinit(allocator);
        self.order.deinit(allocator);
    }
};

const LoadedFiles = struct {
    items: [][]const u8,
    bytes: usize,
    elapsed_ns: u64,

    fn deinit(self: LoadedFiles, allocator: std.mem.Allocator) void {
        for (self.items) |bytes| allocator.free(bytes);
        allocator.free(self.items);
    }
};

const RunOutput = struct {
    text: [][]f32,
    image: [][]f32,
    audio: [][]f32,
    embeddings: usize,
    values: usize,
    checksum: f64,

    fn deinit(self: RunOutput, allocator: std.mem.Allocator) void {
        freeEmbeddings(allocator, self.text);
        freeEmbeddings(allocator, self.image);
        freeEmbeddings(allocator, self.audio);
    }
};

const RequestSample = struct {
    total_ns: u64 = 0,
    file_read_ns: u64 = 0,
    embed_ns: u64 = 0,
    serialize_ns: u64 = 0,
    file_bytes: usize = 0,
    response_bytes: usize = 0,
    embeddings: usize = 0,
    values: usize = 0,
    checksum: f64 = 0,
};

const Timing = struct {
    total_ns: u64 = 0,
    avg_ns: u64 = 0,
    p50_ns: u64 = 0,
    p95_ns: u64 = 0,
    min_ns: u64 = 0,
    max_ns: u64 = 0,
    iters: usize = 0,

    fn throughput(self: Timing, embeddings_per_iter: usize) f64 {
        if (self.total_ns == 0) return 0;
        return @as(f64, @floatFromInt(embeddings_per_iter * self.iters)) / nsToSeconds(self.total_ns);
    }
};

const BenchResult = struct {
    mode: []const u8,
    modality: Modality,
    backend: BackendChoice,
    actual_backend: backends.BackendType,
    batch: usize,
    timing: Timing,
    file_read_avg_ns: u64 = 0,
    embed_avg_ns: u64 = 0,
    serialize_avg_ns: u64 = 0,
    file_bytes: usize = 0,
    response_bytes: usize = 0,
    embeddings: usize = 0,
    values: usize = 0,
    checksum: f64 = 0,
    resident_stats: embedding_mod.ResidentProjectionStats = .{},
    quant_stats: native_compute.NativeQuantDispatchStats = .{},
    graph_stats: graph_executor_stats.ExecutionStats = .{},
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    if (wantsHelp(init)) {
        printUsage();
        return;
    }
    var opts = try parseArgs(allocator, init);
    defer opts.deinit(allocator);

    if (opts.model_dir.len == 0 or opts.order.items.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }
    try ensureRequestedMetalHostedBackendAvailable(opts.backend);

    if (opts.format == .csv) printCsvHeader();

    if (opts.include_cold) {
        const cold = try runColdOnce(allocator, init.io, opts);
        printResult(cold, opts.format);
    }

    const warm_files = try loadAllFiles(allocator, init.io, opts);
    defer {
        warm_files.images.deinit(allocator);
        warm_files.audio.deinit(allocator);
    }

    var loaded = try loadBundle(allocator, init.io, opts);
    defer loaded.deinit();

    for (0..opts.warmup_iters) |_| {
        var warmup = try runRequestWithBytes(allocator, loaded.model, opts, warm_files.images.items, warm_files.audio.items);
        warmup.deinit(allocator);
    }

    const warm_cached = try runWarmCachedBytes(allocator, loaded.model, opts, warm_files);
    printResult(warm_cached, opts.format);

    const warm_cli_like = try runWarmWithFileReads(allocator, init.io, loaded.model, opts);
    printResult(warm_cli_like, opts.format);
}

fn wantsHelp(init: std.process.Init) bool {
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return true;
    }
    return false;
}

const LoadedBundle = struct {
    model_manager: model_manager_mod.ModelManager,
    model: *model_manager_mod.LoadedModel,

    fn deinit(self: *LoadedBundle) void {
        self.model_manager.deinit();
    }
};

fn loadBundle(allocator: std.mem.Allocator, io: std.Io, opts: Options) !LoadedBundle {
    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);
    session_manager.graph_runtime_strategy = opts.graph_runtime_strategy;
    if (opts.graph_runtime_strategy == null) {
        graph_executor_stats.printBypass("inference.clipclap_e2e_bench", "embedding_pipeline_direct_runtime");
    }

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    errdefer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    try model.ensureEmbeddingAssets(
        opts.texts.items.len > 0,
        opts.image_paths.items.len > 0,
        opts.audio_paths.items.len > 0,
    );
    return .{ .model_manager = model_manager, .model = model };
}

fn runColdOnce(allocator: std.mem.Allocator, io: std.Io, opts: Options) !BenchResult {
    native_compute.resetNativeQuantDispatchStats();
    graph_executor_stats.reset();
    const start = nowNs();
    var loaded = try loadBundle(allocator, io, opts);
    defer loaded.deinit();
    const files = try loadAllFiles(allocator, io, opts);
    defer {
        files.images.deinit(allocator);
        files.audio.deinit(allocator);
    }
    const embed_start = nowNs();
    var output = try runRequestWithBytes(allocator, loaded.model, opts, files.images.items, files.audio.items);
    defer output.deinit(allocator);
    const embed_ns = nowNs() - embed_start;
    const serialize = try serializeResponse(allocator, opts, output);
    defer allocator.free(serialize.bytes);
    const total_ns = nowNs() - start;
    const sample = RequestSample{
        .total_ns = total_ns,
        .file_read_ns = files.images.elapsed_ns + files.audio.elapsed_ns,
        .embed_ns = embed_ns,
        .serialize_ns = serialize.elapsed_ns,
        .file_bytes = files.images.bytes + files.audio.bytes,
        .response_bytes = serialize.bytes.len,
        .embeddings = output.embeddings,
        .values = output.values,
        .checksum = output.checksum,
    };
    return resultFromSamples(allocator, "cold_load_cli_like", opts, loaded.model.session.backend(), &.{sample}, loaded.model.resident_projection_stats.snapshot());
}

const AllFiles = struct {
    images: LoadedFiles,
    audio: LoadedFiles,
};

fn loadAllFiles(allocator: std.mem.Allocator, io: std.Io, opts: Options) !AllFiles {
    return .{
        .images = try loadFiles(allocator, io, opts.image_paths.items),
        .audio = try loadFiles(allocator, io, opts.audio_paths.items),
    };
}

fn runWarmCachedBytes(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    opts: Options,
    files: AllFiles,
) !BenchResult {
    native_compute.resetNativeQuantDispatchStats();
    graph_executor_stats.reset();
    const before_stats = model.resident_projection_stats.snapshot();
    const samples = try allocator.alloc(RequestSample, opts.measure_iters);
    defer allocator.free(samples);

    for (samples) |*sample| {
        const start = nowNs();
        var output = try runRequestWithBytes(allocator, model, opts, files.images.items, files.audio.items);
        defer output.deinit(allocator);
        const embed_ns = nowNs() - start;
        const serialize = try serializeResponse(allocator, opts, output);
        defer allocator.free(serialize.bytes);
        sample.* = .{
            .total_ns = nowNs() - start,
            .file_read_ns = 0,
            .embed_ns = embed_ns,
            .serialize_ns = serialize.elapsed_ns,
            .file_bytes = files.images.bytes + files.audio.bytes,
            .response_bytes = serialize.bytes.len,
            .embeddings = output.embeddings,
            .values = output.values,
            .checksum = output.checksum,
        };
    }
    return resultFromSamples(allocator, "warm_cached_bytes", opts, model.session.backend(), samples, diffResidentStats(before_stats, model.resident_projection_stats.snapshot()));
}

fn runWarmWithFileReads(
    allocator: std.mem.Allocator,
    io: std.Io,
    model: *model_manager_mod.LoadedModel,
    opts: Options,
) !BenchResult {
    native_compute.resetNativeQuantDispatchStats();
    graph_executor_stats.reset();
    const before_stats = model.resident_projection_stats.snapshot();
    const samples = try allocator.alloc(RequestSample, opts.measure_iters);
    defer allocator.free(samples);

    for (samples) |*sample| {
        const start = nowNs();
        const files = try loadAllFiles(allocator, io, opts);
        defer {
            files.images.deinit(allocator);
            files.audio.deinit(allocator);
        }
        const embed_start = nowNs();
        var output = try runRequestWithBytes(allocator, model, opts, files.images.items, files.audio.items);
        defer output.deinit(allocator);
        const embed_ns = nowNs() - embed_start;
        const serialize = try serializeResponse(allocator, opts, output);
        defer allocator.free(serialize.bytes);
        sample.* = .{
            .total_ns = nowNs() - start,
            .file_read_ns = files.images.elapsed_ns + files.audio.elapsed_ns,
            .embed_ns = embed_ns,
            .serialize_ns = serialize.elapsed_ns,
            .file_bytes = files.images.bytes + files.audio.bytes,
            .response_bytes = serialize.bytes.len,
            .embeddings = output.embeddings,
            .values = output.values,
            .checksum = output.checksum,
        };
    }
    return resultFromSamples(allocator, "warm_file_read_cli_like", opts, model.session.backend(), samples, diffResidentStats(before_stats, model.resident_projection_stats.snapshot()));
}

fn runRequestWithBytes(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    opts: Options,
    image_bytes: []const []const u8,
    audio_bytes: []const []const u8,
) !RunOutput {
    var pipeline = model.embeddingPipeline(allocator);
    pipeline.config.resident_projection_required = opts.resident_projection_required;

    const text = if (opts.texts.items.len > 0)
        try pipeline.embed(opts.texts.items)
    else
        try allocator.alloc([]f32, 0);
    errdefer freeEmbeddings(allocator, text);

    const image = if (image_bytes.len > 0)
        try pipeline.embedImages(image_bytes)
    else
        try allocator.alloc([]f32, 0);
    errdefer freeEmbeddings(allocator, image);

    const audio = if (audio_bytes.len > 0)
        try pipeline.embedAudio(audio_bytes)
    else
        try allocator.alloc([]f32, 0);
    errdefer freeEmbeddings(allocator, audio);

    var values: usize = 0;
    var checksum_value: f64 = 0;
    for (text) |emb| {
        values += emb.len;
        checksum_value += checksum(emb);
    }
    for (image) |emb| {
        values += emb.len;
        checksum_value += checksum(emb);
    }
    for (audio) |emb| {
        values += emb.len;
        checksum_value += checksum(emb);
    }
    return .{
        .text = text,
        .image = image,
        .audio = audio,
        .embeddings = text.len + image.len + audio.len,
        .values = values,
        .checksum = checksum_value,
    };
}

const SerializeResult = struct {
    bytes: []u8,
    elapsed_ns: u64,
};

fn serializeResponse(allocator: std.mem.Allocator, opts: Options, output: RunOutput) !SerializeResult {
    const start = nowNs();
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(allocator);
    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, opts.model_dir);
    try buf.appendSlice(allocator, ",\"modalities\":[");
    for (opts.order.items, 0..) |item, i| {
        if (i > 0) try buf.append(allocator, ',');
        const name = switch (item.modality) {
            .text => "text",
            .image => "image",
            .audio => "audio",
            .mixed => "mixed",
        };
        try jsonEncodeString(&buf, allocator, name);
    }
    try buf.appendSlice(allocator, "],\"embeddings\":[");
    for (opts.order.items, 0..) |item, i| {
        if (i > 0) try buf.append(allocator, ',');
        const emb = switch (item.modality) {
            .text => output.text[item.index],
            .image => output.image[item.index],
            .audio => output.audio[item.index],
            .mixed => unreachable,
        };
        try appendEmbeddingJson(&buf, allocator, emb);
    }
    try buf.appendSlice(allocator, "]}");
    return .{ .bytes = try buf.toOwnedSlice(allocator), .elapsed_ns = nowNs() - start };
}

fn appendEmbeddingJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, emb: []const f32) !void {
    try buf.append(allocator, '[');
    for (emb, 0..) |value, i| {
        if (i > 0) try buf.append(allocator, ',');
        const num = try std.fmt.allocPrint(allocator, "{d:.7}", .{value});
        defer allocator.free(num);
        try buf.appendSlice(allocator, num);
    }
    try buf.append(allocator, ']');
}

fn resultFromSamples(
    allocator: std.mem.Allocator,
    mode: []const u8,
    opts: Options,
    actual_backend: backends.BackendType,
    samples: []const RequestSample,
    resident_stats: embedding_mod.ResidentProjectionStats,
) !BenchResult {
    const timing = try timingFromSamples(allocator, samples);
    var file_read_total: u64 = 0;
    var embed_total: u64 = 0;
    var serialize_total: u64 = 0;
    var checksum_value: f64 = 0;
    for (samples) |sample| {
        file_read_total += sample.file_read_ns;
        embed_total += sample.embed_ns;
        serialize_total += sample.serialize_ns;
        checksum_value += sample.checksum;
    }
    return .{
        .mode = mode,
        .modality = effectiveModality(opts),
        .backend = opts.backend,
        .actual_backend = actual_backend,
        .batch = samples[0].embeddings,
        .timing = timing,
        .file_read_avg_ns = file_read_total / samples.len,
        .embed_avg_ns = embed_total / samples.len,
        .serialize_avg_ns = serialize_total / samples.len,
        .file_bytes = samples[0].file_bytes,
        .response_bytes = samples[0].response_bytes,
        .embeddings = samples[0].embeddings,
        .values = samples[0].values,
        .checksum = checksum_value,
        .resident_stats = resident_stats,
        .quant_stats = native_compute.nativeQuantDispatchStats(),
        .graph_stats = graph_executor_stats.snapshot(),
    };
}

fn timingFromSamples(allocator: std.mem.Allocator, samples: []const RequestSample) !Timing {
    if (samples.len == 0) return error.InvalidMeasureIters;
    const sorted = try allocator.alloc(u64, samples.len);
    defer allocator.free(sorted);
    var total: u64 = 0;
    for (samples, 0..) |sample, i| {
        sorted[i] = sample.total_ns;
        total += sample.total_ns;
    }
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
    return .{
        .total_ns = total,
        .avg_ns = total / samples.len,
        .p50_ns = sorted[percentileIndex(sorted.len, 50)],
        .p95_ns = sorted[percentileIndex(sorted.len, 95)],
        .min_ns = sorted[0],
        .max_ns = sorted[sorted.len - 1],
        .iters = samples.len,
    };
}

fn percentileIndex(len: usize, percentile: usize) usize {
    if (len <= 1) return 0;
    const rank = (len * percentile + 99) / 100;
    return @min(len - 1, if (rank == 0) 0 else rank - 1);
}

fn loadFiles(allocator: std.mem.Allocator, io: std.Io, paths: []const []const u8) !LoadedFiles {
    const start = nowNs();
    const out = try allocator.alloc([]const u8, paths.len);
    for (out) |*bytes| bytes.* = &.{};
    errdefer {
        for (out) |bytes| {
            if (bytes.len > 0) allocator.free(bytes);
        }
        allocator.free(out);
    }
    var total_bytes: usize = 0;
    for (paths, 0..) |path, i| {
        out[i] = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_file_bytes));
        total_bytes += out[i].len;
    }
    return .{ .items = out, .bytes = total_bytes, .elapsed_ns = nowNs() - start };
}

fn freeEmbeddings(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |emb| allocator.free(emb);
    allocator.free(embeddings);
}

fn checksum(values: []const f32) f64 {
    var sum: f64 = 0;
    const limit = @min(values.len, 32);
    for (values[0..limit]) |value| sum += value;
    return sum;
}

fn diffResidentStats(
    before: embedding_mod.ResidentProjectionStats,
    after: embedding_mod.ResidentProjectionStats,
) embedding_mod.ResidentProjectionStats {
    return .{
        .text_success = after.text_success - before.text_success,
        .text_fallback = after.text_fallback - before.text_fallback,
        .image_success = after.image_success - before.image_success,
        .image_fallback = after.image_fallback - before.image_fallback,
        .audio_success = after.audio_success - before.audio_success,
        .audio_fallback = after.audio_fallback - before.audio_fallback,
    };
}

fn effectiveModality(opts: Options) Modality {
    var modality: ?Modality = null;
    for (opts.order.items) |item| {
        if (modality == null) {
            modality = item.modality;
        } else if (modality.? != item.modality) {
            return .mixed;
        }
    }
    return modality orelse .mixed;
}

fn printResult(result: BenchResult, format: OutputFormat) void {
    switch (format) {
        .text => {
            std.debug.print(
                "e2e mode={s} modality={s} backend={s} actual_backend={s} batch={} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} throughput_embeddings_s={d:.2} file_read_avg_ms={d:.3} embed_avg_ms={d:.3} serialize_avg_ms={d:.3} file_bytes={} response_bytes={} values={} checksum={d:.6}",
                .{
                    result.mode,
                    @tagName(result.modality),
                    @tagName(result.backend),
                    @tagName(result.actual_backend),
                    result.batch,
                    nsToMs(result.timing.avg_ns),
                    nsToMs(result.timing.p50_ns),
                    nsToMs(result.timing.p95_ns),
                    nsToMs(result.timing.min_ns),
                    nsToMs(result.timing.max_ns),
                    result.timing.throughput(result.embeddings),
                    nsToMs(result.file_read_avg_ns),
                    nsToMs(result.embed_avg_ns),
                    nsToMs(result.serialize_avg_ns),
                    result.file_bytes,
                    result.response_bytes,
                    result.values,
                    result.checksum,
                },
            );
            std.debug.print(
                " resident_text={}/{} resident_image={}/{} resident_audio={}/{} q4q5={} q4q5_pair={} q4q5_triple={} packed_qkv_mr4={} packed_qkv_mr2={} q4q5_panel={} dequant={} dequant_pair={} dequant_triple={} q8k_alloc_ms={d:.3} q8k_quant_ms={d:.3} q4q5_compute_ms={d:.3} q4q5_triple_compute_ms={d:.3} dequant_fetch_ms={d:.3} dequant_sgemm_compute_ms={d:.3} graph_partitions={} graph_planned={} graph_commands={} graph_fallbacks={} host_outputs={} boundary_materializations={}\n",
                .{
                    result.resident_stats.text_success,
                    result.resident_stats.text_fallback,
                    result.resident_stats.image_success,
                    result.resident_stats.image_fallback,
                    result.resident_stats.audio_success,
                    result.resident_stats.audio_fallback,
                    result.quant_stats.q4_q5_k_q8k_activation,
                    result.quant_stats.q4_q5_k_q8k_activation_pair,
                    result.quant_stats.q4_q5_k_q8k_activation_triple,
                    result.quant_stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4,
                    result.quant_stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2,
                    result.quant_stats.q4_q5_k_prepared_panel,
                    result.quant_stats.dequant_sgemm,
                    result.quant_stats.dequant_sgemm_pair,
                    result.quant_stats.dequant_sgemm_triple,
                    nsToMs(result.quant_stats.q8k_activation_alloc_ns),
                    nsToMs(result.quant_stats.q8k_activation_quant_ns),
                    nsToMs(result.quant_stats.q4q5_q8k_compute_ns),
                    nsToMs(result.quant_stats.q4q5_q8k_triple_compute_ns),
                    nsToMs(result.quant_stats.dequant_fetch_ns),
                    nsToMs(result.quant_stats.dequant_sgemm_compute_ns),
                    result.graph_stats.partitions_executed,
                    result.graph_stats.planned_operator_dispatches,
                    result.graph_stats.backend_command_dispatches,
                    result.graph_stats.interpreter_fallbacks,
                    result.graph_stats.host_materialized_outputs,
                    result.graph_stats.boundary_output_materializations,
                },
            );
        },
        .csv => {
            std.debug.print(
                "{s},{s},{s},{s},{},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.2},{d:.3},{d:.3},{d:.3},{},{},{},{d:.6},{},{},{},{},{},{},{},{},{},{},{},{},",
                .{
                    result.mode,
                    @tagName(result.modality),
                    @tagName(result.backend),
                    @tagName(result.actual_backend),
                    result.batch,
                    nsToMs(result.timing.avg_ns),
                    nsToMs(result.timing.p50_ns),
                    nsToMs(result.timing.p95_ns),
                    nsToMs(result.timing.min_ns),
                    nsToMs(result.timing.max_ns),
                    result.timing.throughput(result.embeddings),
                    nsToMs(result.file_read_avg_ns),
                    nsToMs(result.embed_avg_ns),
                    nsToMs(result.serialize_avg_ns),
                    result.file_bytes,
                    result.response_bytes,
                    result.values,
                    result.checksum,
                    result.resident_stats.text_success,
                    result.resident_stats.text_fallback,
                    result.resident_stats.image_success,
                    result.resident_stats.image_fallback,
                    result.resident_stats.audio_success,
                    result.resident_stats.audio_fallback,
                    result.quant_stats.q4_q5_k_q8k_activation,
                    result.quant_stats.q4_q5_k_q8k_activation_pair,
                    result.quant_stats.q4_q5_k_q8k_activation_triple,
                    result.quant_stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr4,
                    result.quant_stats.q4_q5_k_q8k_triple_packed_qkv_panel16_mr2,
                    result.quant_stats.q4_q5_k_prepared_panel,
                },
            );
            std.debug.print(
                "{},{},{},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{},{},{},{},{},{}\n",
                .{
                    result.quant_stats.dequant_sgemm,
                    result.quant_stats.dequant_sgemm_pair,
                    result.quant_stats.dequant_sgemm_triple,
                    nsToMs(result.quant_stats.q8k_activation_alloc_ns),
                    nsToMs(result.quant_stats.q8k_activation_quant_ns),
                    nsToMs(result.quant_stats.q4q5_q8k_compute_ns),
                    nsToMs(result.quant_stats.q4q5_q8k_triple_compute_ns),
                    nsToMs(result.quant_stats.dequant_fetch_ns),
                    nsToMs(result.quant_stats.dequant_sgemm_compute_ns),
                    result.graph_stats.partitions_executed,
                    result.graph_stats.planned_operator_dispatches,
                    result.graph_stats.backend_command_dispatches,
                    result.graph_stats.interpreter_fallbacks,
                    result.graph_stats.host_materialized_outputs,
                    result.graph_stats.boundary_output_materializations,
                },
            );
        },
    }
}

fn printCsvHeader() void {
    std.debug.print("mode,modality,backend,actual_backend,batch,avg_ms,p50_ms,p95_ms,min_ms,max_ms,throughput_embeddings_s,file_read_avg_ms,embed_avg_ms,serialize_avg_ms,file_bytes,response_bytes,values,checksum,resident_text_success,resident_text_fallback,resident_image_success,resident_image_fallback,resident_audio_success,resident_audio_fallback,q4q5,q4q5_pair,q4q5_triple,packed_qkv_mr4,packed_qkv_mr2,q4q5_panel,dequant,dequant_pair,dequant_triple,q8k_alloc_ms,q8k_quant_ms,q4q5_compute_ms,q4q5_triple_compute_ms,dequant_fetch_ms,dequant_sgemm_compute_ms,graph_partitions,graph_planned,graph_commands,graph_fallbacks,host_outputs,boundary_materializations\n", .{});
}

fn parseArgs(allocator: std.mem.Allocator, init: std.process.Init) !Options {
    var opts = Options{};
    errdefer opts.deinit(allocator);
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            opts.model_dir = args_iter.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = parseBackendChoice(args_iter.next() orelse return error.MissingBackend) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--graph-runtime")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(args_iter.next() orelse return error.MissingGraphRuntime) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.startsWith(u8, arg, "--graph-runtime=")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(arg["--graph-runtime=".len..]) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.eql(u8, arg, "--resident-projection-required")) {
            opts.resident_projection_required = true;
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingWarmupIters, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args_iter.next() orelse return error.MissingMeasureIters, 10);
        } else if (std.mem.eql(u8, arg, "--no-cold")) {
            opts.include_cold = false;
        } else if (std.mem.eql(u8, arg, "--format")) {
            opts.format = parseOutputFormat(args_iter.next() orelse return error.MissingFormat) orelse return error.InvalidFormat;
        } else if (std.mem.eql(u8, arg, "--text")) {
            const idx = opts.texts.items.len;
            try opts.texts.append(allocator, args_iter.next() orelse return error.MissingText);
            try opts.order.append(allocator, .{ .modality = .text, .index = idx });
        } else if (std.mem.eql(u8, arg, "--image")) {
            const idx = opts.image_paths.items.len;
            try opts.image_paths.append(allocator, args_iter.next() orelse return error.MissingImage);
            try opts.order.append(allocator, .{ .modality = .image, .index = idx });
        } else if (std.mem.eql(u8, arg, "--audio")) {
            const idx = opts.audio_paths.items.len;
            try opts.audio_paths.append(allocator, args_iter.next() orelse return error.MissingAudio);
            try opts.order.append(allocator, .{ .modality = .audio, .index = idx });
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
    if (opts.measure_iters == 0) return error.InvalidMeasureIters;
    return opts;
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    inline for (@typeInfo(BackendChoice).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn parseOutputFormat(value: []const u8) ?OutputFormat {
    inline for (@typeInfo(OutputFormat).@"enum".fields) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.onnx, backends.BackendType.metal, backends.BackendType.mlx, backends.BackendType.native }
        else if (build_options.enable_metal)
            &.{ backends.BackendType.onnx, backends.BackendType.metal, backends.BackendType.native }
        else if (build_options.enable_mlx)
            &.{ backends.BackendType.onnx, backends.BackendType.mlx, backends.BackendType.native }
        else
            &.{ backends.BackendType.onnx, backends.BackendType.native },
        .onnx => &.{backends.BackendType.onnx},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .mlx => if (build_options.enable_mlx) &.{backends.BackendType.mlx} else &.{backends.BackendType.native},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
    };
}

fn ensureRequestedMetalHostedBackendAvailable(choice: BackendChoice) !void {
    if (choice == .cuda and !build_options.enable_cuda) return error.CudaNotEnabled;
    if (choice != .metal and choice != .mlx) return;
    if (choice == .metal) {
        if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
            native_backend_guard.printFailure(failure);
            return native_backend_guard.raise(failure);
        }
        return;
    }
    const mlx_metal_available = if (build_options.enable_mlx) mlx_backend.metalDeviceAvailable() else false;
    if (native_backend_guard.checkMlx(build_options.enable_mlx, mlx_metal_available)) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
    }
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, ch),
        }
    }
    try buf.append(allocator, '"');
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

fn nsToSeconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e9;
}

fn printUsage() void {
    std.debug.print(
        \\usage: zig build bench-clipclap-e2e -- --model-dir <dir> [--backend auto|onnx|native|metal|mlx|cuda] [--graph-runtime interpreter|partitioned|compiled|compiled-required] [--text <text>]... [--image <path>]... [--audio <path>]...
        \\  Options:
        \\    --warmup-iters N                 Warm request iterations before measurement (default 1)
        \\    --measure-iters N                Measurement iterations (default 5)
        \\    --no-cold                        Skip the cold load + CLI-like single run
        \\    --resident-projection-required   Fail if resident projection falls back
        \\    --format text|csv                Output format (default text)
        \\
    , .{});
}
