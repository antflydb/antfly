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
const backends = inference.backends;
const native_backend_choice = inference.native_backend_choice;

const print = std.debug.print;

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
            const result = try runTimed(&pipeline, allocator, opts, docs);
            printResult(opts, doc_count, result);
            allocator.free(docs);
        }
    } else {
        const result = try runTimed(&pipeline, allocator, opts, opts.docs.items);
        printResult(opts, opts.docs.items.len, result);
    }
}

const BenchResult = struct {
    avg_ms: f64,
    min_ms: f64,
    p95_ms: f64,
    docs_per_s: f64,
    checksum: f64,
};

fn runTimed(
    pipeline: anytype,
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

    std.mem.sort(u64, samples_ns, {}, struct {
        fn lessThan(_: void, a: u64, b: u64) bool {
            return a < b;
        }
    }.lessThan);

    const avg_ms = nsToMs(total_ns / opts.measure_iters);
    const min_ms = nsToMs(min_ns);
    const p95_ms = nsToMs(samples_ns[(samples_ns.len - 1) * 95 / 100]);
    const docs_per_s = if (avg_ms == 0)
        0
    else
        (@as(f64, @floatFromInt(docs.len)) * 1000.0) / avg_ms;

    return .{
        .avg_ms = avg_ms,
        .min_ms = min_ms,
        .p95_ms = p95_ms,
        .docs_per_s = docs_per_s,
        .checksum = checksum,
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
            print("avg_ms={d:.3} min_ms={d:.3} p95_ms={d:.3} docs_per_s={d:.3} checksum={d:.6}\n", .{
                result.avg_ms,
                result.min_ms,
                result.p95_ms,
                result.docs_per_s,
                result.checksum,
            });
        },
        .csv => {
            print("{s},{s},{d},{d},{d},{d:.3},{d:.3},{d:.3},{d:.3},{d:.6}\n", .{
                @tagName(opts.backend),
                opts.model_dir,
                doc_count,
                opts.warmup_iters,
                opts.measure_iters,
                result.avg_ms,
                result.min_ms,
                result.p95_ms,
                result.docs_per_s,
                result.checksum,
            });
        },
    }
}

fn printCsvHeader() void {
    print("backend,model,docs,warmup_iters,measure_iters,avg_ms,min_ms,p95_ms,docs_per_s,checksum\n", .{});
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
