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

const termite = @import("termite_internal");
const backends = termite.backends;
const graph_runtime = termite.graph.runtime;
const model_manager_mod = termite.server.model_manager;

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

const Options = struct {
    model_dir: []const u8 = "",
    text: []const u8 = "Apple hired John Smith in Paris.",
    backend: BackendChoice = .native,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    warmup_iters: usize = 1,
    measure_iters: usize = 5,
    format: OutputFormat = .text,
    labels: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.labels.deinit(allocator);
    }
};

const Sample = struct {
    elapsed_ns: u64,
    entity_count: usize,
    score_sum: f64,
};

const Result = struct {
    mode: []const u8,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    min_ms: f64,
    max_ms: f64,
    entity_count: usize,
    score_sum: f64,
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

    const texts = [_][]const u8{opts.text};
    const labels: ?[]const []const u8 = if (opts.labels.items.len > 0) opts.labels.items else null;

    var pipeline = model.glinerPipeline(allocator);

    const first = try runRecognize(&pipeline, &texts, labels);
    const first_run = Result{
        .mode = "first_run",
        .avg_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .p50_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .p95_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .min_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .max_ms = nsToMs(load_elapsed_ns + first.elapsed_ns),
        .entity_count = first.entity_count,
        .score_sum = first.score_sum,
    };

    for (0..opts.warmup_iters) |_| {
        _ = try runRecognize(&pipeline, &texts, labels);
    }

    const samples = try allocator.alloc(Sample, opts.measure_iters);
    defer allocator.free(samples);
    for (samples) |*sample| {
        sample.* = try runRecognize(&pipeline, &texts, labels);
    }
    const warm = try resultFromSamples(allocator, "warm_loaded_session", samples);

    switch (opts.format) {
        .text => {
            printText(opts, first_run);
            printText(opts, warm);
        },
        .csv => {
            std.debug.print("mode,model_dir,backend,avg_ms,p50_ms,p95_ms,min_ms,max_ms,entity_count,score_sum\n", .{});
            printCsv(opts, first_run);
            printCsv(opts, warm);
        },
    }
}

fn runRecognize(pipeline: anytype, texts: []const []const u8, labels: ?[]const []const u8) !Sample {
    const start = nowNs();
    const entities = try pipeline.recognizeBatch(texts, labels);
    const elapsed_ns = nowNs() - start;
    defer freeEntities(pipeline.allocator, entities);

    var entity_count: usize = 0;
    var score_sum: f64 = 0.0;
    for (entities) |row| {
        entity_count += row.len;
        for (row) |entity| score_sum += entity.score;
    }
    return .{ .elapsed_ns = elapsed_ns, .entity_count = entity_count, .score_sum = score_sum };
}

fn resultFromSamples(allocator: std.mem.Allocator, mode: []const u8, samples: []const Sample) !Result {
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
        .mode = mode,
        .avg_ms = nsToMs(avg_ns),
        .p50_ms = nsToMs(sorted[p50_idx].elapsed_ns),
        .p95_ms = nsToMs(sorted[p95_idx].elapsed_ns),
        .min_ms = nsToMs(sorted[0].elapsed_ns),
        .max_ms = nsToMs(sorted[sorted.len - 1].elapsed_ns),
        .entity_count = last.entity_count,
        .score_sum = last.score_sum,
    };
}

fn freeEntities(allocator: std.mem.Allocator, all_entities: anytype) void {
    for (all_entities) |entities| {
        for (entities) |entity| allocator.free(entity.text);
        allocator.free(entities);
    }
    allocator.free(all_entities);
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
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            opts.model_dir = args.next() orelse return error.MissingModelDir;
        } else if (std.mem.eql(u8, arg, "--text")) {
            opts.text = args.next() orelse return error.MissingText;
        } else if (std.mem.eql(u8, arg, "--label")) {
            try opts.labels.append(allocator, args.next() orelse return error.MissingLabel);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = parseBackend(args.next() orelse return error.MissingBackend) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--graph-runtime")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(args.next() orelse return error.MissingGraphRuntime) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingWarmupIters, 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            opts.measure_iters = try std.fmt.parseInt(usize, args.next() orelse return error.MissingMeasureIters, 10);
        } else if (std.mem.eql(u8, arg, "--format")) {
            opts.format = parseFormat(args.next() orelse return error.MissingFormat) orelse return error.InvalidFormat;
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
        "{s}: model_dir={s} backend={s} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} min_ms={d:.3} max_ms={d:.3} entity_count={} score_sum={d:.6}\n",
        .{ result.mode, opts.model_dir, @tagName(opts.backend), result.avg_ms, result.p50_ms, result.p95_ms, result.min_ms, result.max_ms, result.entity_count, result.score_sum },
    );
}

fn printCsv(opts: Options, result: Result) void {
    std.debug.print(
        "{s},{s},{s},{d:.3},{d:.3},{d:.3},{d:.3},{d:.3},{},{d:.6}\n",
        .{ result.mode, opts.model_dir, @tagName(opts.backend), result.avg_ms, result.p50_ms, result.p95_ms, result.min_ms, result.max_ms, result.entity_count, result.score_sum },
    );
}

fn printUsage() void {
    std.debug.print(
        \\usage: zig build bench-gliner2-e2e -- --model-dir <dir> [--text TEXT] [--label NAME]... [--backend native] [--graph-runtime partitioned] [--warmup-iters N] [--measure-iters N] [--format text|csv]
        \\
    , .{});
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
