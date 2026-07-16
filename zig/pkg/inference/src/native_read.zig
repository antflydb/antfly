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

const std = @import("std");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const metal_runtime = if (build_options.enable_metal) @import("backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }
};
const c_file = @import("util/c_file.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_guard = @import("native_backend_guard.zig");
const platform = @import("antfly_platform");
const readers_mod = @import("readers/reader.zig");
const reading_pipeline = @import("pipelines/reading.zig");
const runtime = @import("runtime/root.zig");
const compat = @import("io/compat.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    onnx,
    native,
    metal,
    cuda,
};

const Options = struct {
    model_dir: []const u8,
    image_path: []const u8,
    backend: BackendChoice = .auto,
    prompt: ?[]const u8 = null,
    max_tokens: ?usize = null,
    cache_dtype: ?[]const u8 = null,
    warmup_iters: usize = 1,
    measure_iters: ?usize = null,
    json_timing_path: ?[]const u8 = null,
};

const Timing = struct {
    total_ns: u64,
    avg_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    min_ns: u64,
    max_ns: u64,
    iters: usize,
};

const ReadBenchmarkResult = struct {
    load_ns: u64,
    timing: Timing,
    image_bytes: usize,
    last_text: []const u8,
    telemetry: reading_pipeline.ReadTelemetry,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try ensureRequestedBackendAvailable(opts.backend);
    const image_data = try c_file.readFile(allocator, opts.image_path);
    defer allocator.free(image_data);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    if (debug_cuda_session) std.log.info("read: load reader start model={s}", .{opts.model_dir});
    const load_start_ns = nowNs();
    var reader = try readers_mod.LoadedReader.loadFromDir(
        allocator,
        opts.model_dir,
        &model_manager.session_manager,
        &model_manager,
        null,
        .{},
    );
    const load_ns = nowNs() - load_start_ns;
    if (debug_cuda_session) std.log.info("read: load reader done model={s}", .{opts.model_dir});
    defer {
        if (debug_cuda_session) std.log.info("read: reader deinit start model={s}", .{opts.model_dir});
        reader.deinit();
        if (debug_cuda_session) std.log.info("read: reader deinit done model={s}", .{opts.model_dir});
    }

    if (opts.measure_iters != null) {
        const bench = try runWarmBenchmark(allocator, &reader, image_data, opts, load_ns);
        defer allocator.free(bench.last_text);
        try writeBenchmarkJson(allocator, io, opts.model_dir, opts, bench);
        return;
    }

    if (debug_cuda_session) std.log.info("read: inference start model={s}", .{opts.model_dir});
    var result = try reader.read(image_data, .{
        .prompt = opts.prompt,
        .max_tokens = opts.max_tokens,
        .cache_dtype = opts.cache_dtype,
    });
    if (debug_cuda_session) std.log.info("read: inference done model={s}", .{opts.model_dir});
    defer result.deinit();
    try writeResultJson(allocator, io, opts.model_dir, result);
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
        .image_path = args[1],
    };

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--prompt")) {
            i += 1;
            if (i >= args.len) return error.MissingPromptValue;
            opts.prompt = args[i];
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            i += 1;
            if (i >= args.len) return error.MissingMaxTokensValue;
            opts.max_tokens = try std.fmt.parseInt(usize, args[i], 10);
            if (opts.max_tokens.? == 0) return error.InvalidMaxTokens;
        } else if (std.mem.eql(u8, arg, "--cache-dtype")) {
            i += 1;
            if (i >= args.len) return error.MissingCacheDtype;
            if (runtime.kv.pool.parseKvDType(args[i]) == null) return error.InvalidCacheDtype;
            opts.cache_dtype = args[i];
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingWarmupIters;
            opts.warmup_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingMeasureIters;
            opts.measure_iters = try std.fmt.parseInt(usize, args[i], 10);
            if (opts.measure_iters.? == 0) return error.InvalidMeasureIters;
        } else if (std.mem.eql(u8, arg, "--json-timing")) {
            i += 1;
            if (i >= args.len) return error.MissingJsonTimingPath;
            opts.json_timing_path = args[i];
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn runWarmBenchmark(
    allocator: std.mem.Allocator,
    reader: *readers_mod.LoadedReader,
    image_data: []const u8,
    opts: Options,
    load_ns: u64,
) !ReadBenchmarkResult {
    const read_opts = readers_mod.ReadOptions{
        .prompt = opts.prompt,
        .max_tokens = opts.max_tokens,
        .cache_dtype = opts.cache_dtype,
    };

    for (0..opts.warmup_iters) |_| {
        var result = try reader.read(image_data, read_opts);
        result.deinit();
    }

    const measure_iters = opts.measure_iters orelse return error.InvalidMeasureIters;
    const samples = try allocator.alloc(u64, measure_iters);
    defer allocator.free(samples);

    var last_text: []const u8 = "";
    var telemetry: reading_pipeline.ReadTelemetry = undefined;
    var have_baseline = false;
    errdefer if (have_baseline) allocator.free(last_text);
    for (samples) |*sample| {
        const start = nowNs();
        var result = try reader.read(image_data, read_opts);
        defer result.deinit();
        sample.* = nowNs() - start;
        const current_telemetry = reading_pipeline.lastReadTelemetry();
        if (!have_baseline) {
            last_text = try allocator.dupe(u8, result.text);
            telemetry = current_telemetry;
            have_baseline = true;
        } else if (!std.mem.eql(u8, last_text, result.text)) {
            return error.BenchmarkOutputDrift;
        } else if (!readTelemetryEql(telemetry, current_telemetry)) {
            return error.BenchmarkTelemetryDrift;
        }
    }

    return .{
        .load_ns = load_ns,
        .timing = try timingFromSamples(allocator, samples),
        .image_bytes = image_data.len,
        .last_text = last_text,
        .telemetry = telemetry,
    };
}

fn readTelemetryEql(a: reading_pipeline.ReadTelemetry, b: reading_pipeline.ReadTelemetry) bool {
    return a.resident_decoder == b.resident_decoder and
        a.kv_cache == b.kv_cache and
        optionalStringEql(a.kv_cache_mode, b.kv_cache_mode) and
        optionalStringEql(a.lm_head_path, b.lm_head_path) and
        a.cuda_graph_replay == b.cuda_graph_replay and
        optionalStringEql(a.cuda_graph_fallback_reason, b.cuda_graph_fallback_reason) and
        a.cuda_graph_capture_steps == b.cuda_graph_capture_steps and
        a.cuda_graph_replay_steps == b.cuda_graph_replay_steps and
        a.generated_tokens == b.generated_tokens;
}

fn optionalStringEql(a: ?[]const u8, b: ?[]const u8) bool {
    if (a) |value| return if (b) |other| std.mem.eql(u8, value, other) else false;
    return b == null;
}

fn timingFromSamples(allocator: std.mem.Allocator, samples: []const u64) !Timing {
    if (samples.len == 0) return error.InvalidMeasureIters;
    const sorted = try allocator.dupe(u64, samples);
    defer allocator.free(sorted);
    var total: u64 = 0;
    for (samples) |sample| total += sample;
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

const ParsedInt = struct { value: i64, len: usize };

fn parseJsonInt(s: []const u8) ?ParsedInt {
    if (s.len == 0) return null;
    var i: usize = 0;
    var neg = false;
    if (s[0] == '-') {
        neg = true;
        i = 1;
    }
    if (i >= s.len or s[i] < '0' or s[i] > '9') return null;
    var val: i64 = 0;
    while (i < s.len and s[i] >= '0' and s[i] <= '9') {
        val = val * 10 + @as(i64, s[i] - '0');
        i += 1;
    }
    return .{ .value = if (neg) -val else val, .len = i };
}

fn parseJsonFloatArray(data: []const u8, key: []const u8) ?[3]f32 {
    const key_pos = std.mem.indexOf(u8, data, key) orelse return null;
    const after = data[key_pos + key.len ..];
    var pos: usize = 0;
    while (pos < after.len and (after[pos] == ' ' or after[pos] == ':' or after[pos] == '\t' or after[pos] == '\n')) pos += 1;
    if (pos >= after.len or after[pos] != '[') return null;
    pos += 1;

    var result: [3]f32 = undefined;
    var count: usize = 0;
    while (count < 3 and pos < after.len) {
        while (pos < after.len and (after[pos] == ' ' or after[pos] == ',' or after[pos] == '\n')) pos += 1;
        if (pos >= after.len or after[pos] == ']') break;
        const end = blk: {
            var e = pos;
            while (e < after.len and after[e] != ',' and after[e] != ']' and after[e] != ' ' and after[e] != '\n') e += 1;
            break :blk e;
        };
        result[count] = std.fmt.parseFloat(f32, after[pos..end]) catch return null;
        count += 1;
        pos = end;
    }
    if (count != 3) return null;
    return result;
}

fn writeResultJson(allocator: std.mem.Allocator, io: std.Io, model_name: []const u8, result: readers_mod.Result) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"text\":");
    try jsonEncodeString(&buf, allocator, result.text);
    if (result.fields.len > 0) {
        try buf.appendSlice(allocator, ",\"fields\":{");
        for (result.fields, 0..) |field, i| {
            if (i > 0) try buf.append(allocator, ',');
            try jsonEncodeString(&buf, allocator, field.name);
            try buf.append(allocator, ':');
            try jsonEncodeString(&buf, allocator, field.value);
        }
        try buf.append(allocator, '}');
    }
    if (result.regions.len > 0) {
        try buf.appendSlice(allocator, ",\"regions\":[");
        for (result.regions, 0..) |region, i| {
            if (i > 0) try buf.append(allocator, ',');
            try buf.appendSlice(allocator, "{\"text\":");
            try jsonEncodeString(&buf, allocator, region.text);
            try buf.appendSlice(allocator, ",\"bbox\":[");
            for (region.bbox, 0..) |coord, coord_idx| {
                if (coord_idx > 0) try buf.append(allocator, ',');
                try appendFloatJson(&buf, allocator, coord);
            }
            try buf.append(allocator, ']');
            if (region.confidence) |confidence| {
                try buf.appendSlice(allocator, ",\"confidence\":");
                try appendFloatJson(&buf, allocator, confidence);
            }
            if (region.label) |label| {
                try buf.appendSlice(allocator, ",\"label\":");
                try jsonEncodeString(&buf, allocator, label);
            }
            try buf.append(allocator, '}');
        }
        try buf.append(allocator, ']');
    }
    try buf.appendSlice(allocator, "}\n");

    try writeToStdout(io, buf.items);
}

fn writeToStdout(io: std.Io, data: []const u8) !void {
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
    try stdout_writer.interface.writeAll(data);
    try stdout_writer.interface.flush();
}

fn writeBenchmarkJson(allocator: std.mem.Allocator, io: std.Io, model_name: []const u8, opts: Options, result: ReadBenchmarkResult) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"backend\":");
    try jsonEncodeString(&buf, allocator, @tagName(opts.backend));
    try buf.appendSlice(allocator, ",\"mode\":\"warm_read\"");
    try buf.appendSlice(allocator, ",\"prompt\":");
    if (opts.prompt) |prompt| {
        try jsonEncodeString(&buf, allocator, prompt);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"max_tokens\":");
    if (opts.max_tokens) |max_tokens| {
        try appendIntJson(&buf, allocator, max_tokens);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"cache_dtype\":");
    if (opts.cache_dtype) |cache_dtype| {
        try jsonEncodeString(&buf, allocator, cache_dtype);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"image_bytes\":");
    try appendIntJson(&buf, allocator, result.image_bytes);
    try buf.appendSlice(allocator, ",\"warmup_iters\":");
    try appendIntJson(&buf, allocator, opts.warmup_iters);
    try buf.appendSlice(allocator, ",\"measure_iters\":");
    try appendIntJson(&buf, allocator, result.timing.iters);
    try buf.appendSlice(allocator, ",\"iterations_consistent\":true");
    try buf.appendSlice(allocator, ",\"load_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.load_ns));
    try buf.appendSlice(allocator, ",\"avg_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.timing.avg_ns));
    try buf.appendSlice(allocator, ",\"p50_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.timing.p50_ns));
    try buf.appendSlice(allocator, ",\"p95_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.timing.p95_ns));
    try buf.appendSlice(allocator, ",\"min_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.timing.min_ns));
    try buf.appendSlice(allocator, ",\"max_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.timing.max_ns));
    try buf.appendSlice(allocator, ",\"total_ms\":");
    try appendFloatJson(&buf, allocator, nsToMs(result.timing.total_ns));
    try buf.appendSlice(allocator, ",\"generated_tokens\":");
    if (result.telemetry.generated_tokens) |generated_tokens| {
        try appendIntJson(&buf, allocator, generated_tokens);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"resident_decoder\":");
    try appendBoolJson(&buf, allocator, result.telemetry.resident_decoder);
    try buf.appendSlice(allocator, ",\"kv_cache\":");
    try appendBoolJson(&buf, allocator, result.telemetry.kv_cache);
    try buf.appendSlice(allocator, ",\"kv_cache_mode\":");
    if (result.telemetry.kv_cache_mode) |mode| {
        try jsonEncodeString(&buf, allocator, mode);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"lm_head_path\":");
    if (result.telemetry.lm_head_path) |path| {
        try jsonEncodeString(&buf, allocator, path);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"cuda_graph_replay\":");
    try appendBoolJson(&buf, allocator, result.telemetry.cuda_graph_replay);
    try buf.appendSlice(allocator, ",\"cuda_graph_capture_steps\":");
    try appendIntJson(&buf, allocator, result.telemetry.cuda_graph_capture_steps);
    try buf.appendSlice(allocator, ",\"cuda_graph_replay_steps\":");
    try appendIntJson(&buf, allocator, result.telemetry.cuda_graph_replay_steps);
    try buf.appendSlice(allocator, ",\"cuda_graph_fallback_reason\":");
    if (result.telemetry.cuda_graph_fallback_reason) |reason| {
        try jsonEncodeString(&buf, allocator, reason);
    } else {
        try buf.appendSlice(allocator, "null");
    }
    try buf.appendSlice(allocator, ",\"last_text\":");
    try jsonEncodeString(&buf, allocator, result.last_text);
    try buf.appendSlice(allocator, "}\n");

    if (opts.json_timing_path) |path| {
        try compat.cwd().writeFile(io, .{ .sub_path = path, .data = buf.items });
    }
    try writeToStdout(io, buf.items);
}

fn appendFloatJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: anytype) !void {
    const value_str = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(value_str);
    try buf.appendSlice(allocator, value_str);
}

fn appendIntJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: anytype) !void {
    const value_str = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(value_str);
    try buf.appendSlice(allocator, value_str);
}

fn appendBoolJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: bool) !void {
    try buf.appendSlice(allocator, if (value) "true" else "false");
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
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_cuda)
            &.{ backends.BackendType.cuda, backends.BackendType.native }
        else if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .onnx => &.{backends.BackendType.onnx},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
    };
}

fn ensureRequestedBackendAvailable(choice: BackendChoice) !void {
    switch (choice) {
        .auto, .native => return,
        .onnx => return,
        .cuda => if (!build_options.enable_cuda) return error.BackendUnavailable,
        .metal => {
            if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
                native_backend_guard.printFailure(failure);
                return native_backend_guard.raise(failure);
            }
            return;
        },
    }
}

fn printUsage() void {
    print(
        \\usage: antfly inference read <model-dir> <image-path> [--backend auto|onnx|native|metal|cuda] [--prompt <prompt>] [--max-tokens <n>] [--cache-dtype f16|f32|int8|fp8|int4|polar4|turbo3] [--warmup-iters <n>] [--measure-iters <n>] [--json-timing <path>]
        \\  Runs local document/image reading and prints a JSON response to stdout.
        \\  With --measure-iters, keeps the model loaded and prints warm request latency JSON. --json-timing also writes that JSON to a file.
        \\
    , .{});
}

test "parseArgs accepts backend, prompt, and max tokens" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "/tmp/image.jpg",
        "--backend",
        "metal",
        "--prompt",
        "<CAPTION>",
        "--max-tokens",
        "128",
        "--cache-dtype",
        "turbo3",
        "--warmup-iters",
        "2",
        "--measure-iters",
        "3",
        "--json-timing",
        "/tmp/read.json",
    });

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("/tmp/image.jpg", opts.image_path);
    try std.testing.expectEqual(BackendChoice.metal, opts.backend);
    try std.testing.expectEqualStrings("<CAPTION>", opts.prompt.?);
    try std.testing.expectEqual(@as(?usize, 128), opts.max_tokens);
    try std.testing.expectEqualStrings("turbo3", opts.cache_dtype.?);
    try std.testing.expectEqual(@as(usize, 2), opts.warmup_iters);
    try std.testing.expectEqual(@as(?usize, 3), opts.measure_iters);
    try std.testing.expectEqualStrings("/tmp/read.json", opts.json_timing_path.?);
}

test "parseArgs rejects zero max tokens" {
    try std.testing.expectError(error.InvalidMaxTokens, parseArgs(&.{
        "/tmp/model",
        "/tmp/image.jpg",
        "--max-tokens",
        "0",
    }));
}

test "parseBackendChoice accepts onnx" {
    try std.testing.expectEqual(BackendChoice.onnx, parseBackendChoice("onnx").?);
}

test "parseBackendChoice accepts cuda" {
    try std.testing.expectEqual(BackendChoice.cuda, parseBackendChoice("cuda").?);
}

test "benchmark telemetry equality detects drift" {
    const baseline = reading_pipeline.ReadTelemetry{
        .resident_decoder = true,
        .kv_cache = true,
        .kv_cache_mode = "preallocated",
        .generated_tokens = 8,
    };
    try std.testing.expect(readTelemetryEql(baseline, baseline));

    var drifted = baseline;
    drifted.generated_tokens = 7;
    try std.testing.expect(!readTelemetryEql(baseline, drifted));
}
