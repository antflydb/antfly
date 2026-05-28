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

const inference = @import("inference_internal");
const backends = inference.backends;
const graph_runtime = inference.graph.runtime;
const model_manager_mod = inference.server.model_manager;
const native_compute = inference.native_compute.native;

const BackendChoice = enum {
    auto,
    native,
    metal,
    mlx,
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
    text_repeat: usize = 1,
    backend: BackendChoice = .native,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    warmup_iters: usize = 1,
    measure_iters: usize = 5,
    batch_size: usize = 1,
    format: OutputFormat = .text,
    task: BenchTask = .entities,
    dump_entities: bool = false,
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

const Sample = struct {
    elapsed_ns: u64,
    entity_count: usize,
    relation_count: usize = 0,
    score_sum: f64,
    relation_score_sum: f64 = 0.0,
    quant: QuantCounters = .{},
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
    session_manager.graph_runtime_strategy = opts.graph_runtime_strategy;

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const load_start = nowNs();
    const model = try model_manager.loadFromDir(opts.model_dir);
    const load_elapsed_ns = nowNs() - load_start;
    if (!model.isGlinerModel()) return error.NotGlinerModel;

    if (opts.batch_size == 0) return error.InvalidBatchSize;
    const bench_text = try repeatedText(allocator, opts.text, opts.text_repeat);
    defer allocator.free(bench_text);
    const texts = try allocator.alloc([]const u8, opts.batch_size);
    defer allocator.free(texts);
    @memset(texts, bench_text);
    const labels: ?[]const []const u8 = if (opts.labels.items.len > 0) opts.labels.items else null;
    const relation_labels: ?[]const []const u8 = if (opts.relation_labels.items.len > 0) opts.relation_labels.items else null;

    var pipeline = model.glinerPipeline(allocator);

    var rows = std.ArrayListUnmanaged(Result).empty;
    defer rows.deinit(allocator);

    if (opts.task == .entities or opts.task == .both) {
        const task_result = try runBenchmarkTask(allocator, &pipeline, texts, labels, relation_labels, .entities, load_elapsed_ns, opts.warmup_iters, opts.measure_iters, opts.dump_entities);
        try rows.append(allocator, task_result.first);
        try rows.append(allocator, task_result.warm);
    }

    if (opts.task == .relations or opts.task == .both) {
        const task_result = try runBenchmarkTask(allocator, &pipeline, texts, labels, relation_labels, .relations, load_elapsed_ns, opts.warmup_iters, opts.measure_iters, opts.dump_entities);
        try rows.append(allocator, task_result.first);
        try rows.append(allocator, task_result.warm);
    }

    switch (opts.format) {
        .text => {
            for (rows.items) |row| printText(opts, row);
        },
        .csv => {
            printCsvHeader();
            for (rows.items) |row| printCsv(opts, row);
        },
    }
}

fn runBenchmarkTask(
    allocator: std.mem.Allocator,
    pipeline: anytype,
    texts: []const []const u8,
    labels: ?[]const []const u8,
    relation_labels: ?[]const []const u8,
    task: BenchTask,
    load_elapsed_ns: u64,
    warmup_iters: usize,
    measure_iters: usize,
    dump_entities: bool,
) !TaskResult {
    const first = try runTask(pipeline, texts, labels, relation_labels, task, dump_entities);
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
        .native_quant_stats_enabled = first.native_quant_stats_enabled,
    };

    for (0..warmup_iters) |_| {
        _ = try runTask(pipeline, texts, labels, relation_labels, task, false);
    }

    const samples = try allocator.alloc(Sample, measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| {
        sample.* = try runTask(pipeline, texts, labels, relation_labels, task, false);
    }
    const warm = try resultFromSamples(allocator, task, "warm_loaded_session", samples);

    return .{ .first = first_run, .warm = warm };
}

fn runTask(
    pipeline: anytype,
    texts: []const []const u8,
    labels: ?[]const []const u8,
    relation_labels: ?[]const []const u8,
    task: BenchTask,
    dump_entities: bool,
) !Sample {
    native_compute.resetNativeQuantDispatchStats();
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
            return .{
                .elapsed_ns = elapsed_ns,
                .entity_count = entity_count,
                .score_sum = score_sum,
                .quant = quantCountersFromStats(native_compute.nativeQuantDispatchStats()),
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
            return .{
                .elapsed_ns = elapsed_ns,
                .entity_count = entity_count,
                .relation_count = relation_count,
                .score_sum = score_sum,
                .relation_score_sum = relation_score_sum,
                .quant = quantCountersFromStats(native_compute.nativeQuantDispatchStats()),
                .native_quant_stats_enabled = native_compute.nativeQuantDispatchStatsEnabled(),
            };
        },
        .both => unreachable,
    }
}

fn resultFromSamples(allocator: std.mem.Allocator, task: BenchTask, mode: []const u8, samples: []const Sample) !Result {
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
    const avg_ns: u64 = @intCast(total_ns / samples.len);
    const p50_idx = samples.len / 2;
    const p95_idx = @min(samples.len - 1, (samples.len * 95 + 99) / 100 - 1);
    const last = samples[samples.len - 1];
    return .{
        .task = task,
        .mode = mode,
        .avg_ms = nsToMs(avg_ns),
        .p50_ms = nsToMs(sorted[p50_idx].elapsed_ns),
        .p95_ms = nsToMs(sorted[p95_idx].elapsed_ns),
        .min_ms = nsToMs(sorted[0].elapsed_ns),
        .max_ms = nsToMs(sorted[sorted.len - 1].elapsed_ns),
        .entity_count = last.entity_count,
        .relation_count = last.relation_count,
        .score_sum = last.score_sum,
        .relation_score_sum = last.relation_score_sum,
        .quant = last.quant,
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

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
        .mlx => &.{backends.BackendType.mlx},
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
            opts.text = args.next() orelse return error.MissingText;
        } else if (std.mem.eql(u8, arg, "--text-repeat")) {
            opts.text_repeat = try std.fmt.parseInt(usize, args.next() orelse return error.MissingTextRepeat, 10);
            if (opts.text_repeat == 0) return error.InvalidTextRepeat;
        } else if (std.mem.eql(u8, arg, "--task")) {
            opts.task = parseTask(args.next() orelse return error.MissingTask) orelse return error.InvalidTask;
        } else if (std.mem.eql(u8, arg, "--label")) {
            try opts.labels.append(allocator, args.next() orelse return error.MissingLabel);
        } else if (std.mem.eql(u8, arg, "--relation-label")) {
            try opts.relation_labels.append(allocator, args.next() orelse return error.MissingRelationLabel);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = parseBackend(args.next() orelse return error.MissingBackend) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--graph-runtime")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(args.next() orelse return error.MissingGraphRuntime) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingWarmupIters, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingMeasureIters, 10);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            opts.batch_size = try std.fmt.parseInt(usize, args.next() orelse return error.MissingBatchSize, 10);
        } else if (std.mem.eql(u8, arg, "--format")) {
            opts.format = parseFormat(args.next() orelse return error.MissingFormat) orelse return error.InvalidFormat;
        } else if (std.mem.eql(u8, arg, "--dump-entities")) {
            opts.dump_entities = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
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
    if (std.ascii.eqlIgnoreCase(value, "mlx")) return .mlx;
    return null;
}

fn parseFormat(value: []const u8) ?OutputFormat {
    if (std.ascii.eqlIgnoreCase(value, "text")) return .text;
    if (std.ascii.eqlIgnoreCase(value, "csv")) return .csv;
    return null;
}

fn printText(opts: Options, result: Result) void {
    std.debug.print(
        "{s}/{s}: model_dir={s} backend={s} batch_size={} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} entity_count={} relation_count={} score_sum={d:.6} relation_score_sum={d:.6} native_quant_stats={s} q4q5={} q4q5_pair={} q4q5_triple={} q4q5_panel={} dequant={} dequant_pair={} dequant_triple={} q8_0={} q8_0_pair={} q8_0_triple={}\n",
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
        },
    );
}

fn printCsvHeader() void {
    std.debug.print("task,mode,model_dir,backend,batch_size,avg_ms,p50_ms,p95_ms,min_ms,max_ms,entity_count,relation_count,score_sum,relation_score_sum,native_quant_stats_enabled,q4q5,q4q5_pair,q4q5_triple,q4q5_panel,dequant,dequant_pair,dequant_triple,q8_0,q8_0_pair,q8_0_triple\n", .{});
}

fn printCsv(opts: Options, result: Result) void {
    std.debug.print(
        "{s},{s},{s},{s},{},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{},{},{d:.6},{d:.6},{},{},{},{},{},{},{},{},{},{},{}\n",
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
        },
    );
}

fn printUsage() void {
    std.debug.print(
        \\usage: zig build bench-gliner2-e2e -- --model-dir <dir> [--task entities|relations|both] [--text TEXT] [--text-repeat N] [--batch-size N] [--label NAME]... [--relation-label NAME]... [--backend auto|native|metal|mlx] [--graph-runtime partitioned] [--warmup-iters N] [--measure-iters N] [--format text|csv] [--dump-entities]
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

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}
