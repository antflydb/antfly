// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Persistent, direct-core GLiNER2.5 CPU benchmark worker. The Python driver
//! owns pairing, independent output/token validation, resource supervision,
//! and the evidence report. This binary never claims serving qualification.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const model = inference.models.gliner_boundary;
const processor = inference.pipelines.gliner_boundary_processor;
const schema_mod = inference.pipelines.extraction_schema;
const pipeline = inference.pipelines.gliner_boundary_pipeline;
const engine = inference.architectures.gliner_boundary_engine;
const native = inference.native_compute.native;
const Allocator = std.mem.Allocator;
const on_cuda = build_options.enable_cuda;
const scope = if (on_cuda) "gliner25_direct_core_cuda_comparison_v1" else "gliner25_direct_core_cpu_fp32";
const timing_boundary = "schema_parse_compile+processor+encoder+heads+decode+temporary_cleanup";

comptime {
    if (builtin.mode != .ReleaseFast) @compileError("GLiNER2.5 benchmark requires -Doptimize=ReleaseFast for the complete dependency graph");
    if (build_options.enable_metal or build_options.enable_onnx or build_options.enable_pjrt)
        @compileError("GLiNER2.5 CPU benchmark requires -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false");
}

const Pin = struct { sha256: []const u8, size_bytes: usize };
const Files = struct {
    @"config.json": Pin,
    @"encoder_config/config.json": Pin,
    @"model.safetensors": Pin,
    @"tokenizer.json": Pin,
    @"tokenizer_config.json": Pin,
};
const Case = struct { id: []const u8, text: []const u8, schema: std.json.Value };
const Fixture = struct { format_version: u32, source_commit: []const u8, model: []const u8, model_id: []const u8, revision: []const u8, model_files: Files, cases: []const Case };
const Options = struct { model_dir: []const u8 = "", cases_path: []const u8 = "", threads: usize = 1, batch_size: usize = 1, timeout_ms: u64 = 30000, max_commands: usize = 2048 };
const Command = struct { request_id: u32, op: enum { validate, run, stop }, case_id: []const u8 = "" };
const Extraction = struct {
    output: pipeline.Result,
    input_ids: ?[]i64 = null,
    encoder_shape: [2]usize,
    fn deinit(self: *Extraction, a: Allocator) void {
        self.output.deinit();
        if (self.input_ids) |ids| a.free(ids);
    }
};

fn nowNs() !u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts)) != .SUCCESS) return error.MonotonicClockUnavailable;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}
fn hash(bytes: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}
fn verifyBytes(pin: Pin, bytes: []const u8) !void {
    if (pin.size_bytes != bytes.len or !std.mem.eql(u8, pin.sha256, &hash(bytes))) return error.BenchmarkArtifactIdentityMismatch;
}
fn readPinned(a: Allocator, directory: []const u8, name: []const u8, pin: Pin) ![]u8 {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const bytes = try inference.util.c_file.readFileMax(a, path, @min(pin.size_bytes, 32 * 1024 * 1024));
    errdefer a.free(bytes);
    try verifyBytes(pin, bytes);
    return bytes;
}
fn loadWeights(a: Allocator, reader: *const inference.models.safetensors.MMapReader, backbone: model.Backbone) !native.WeightStore {
    const descriptors = try a.alloc(inference.models.tensor_access.Descriptor, reader.header.tensors.count());
    defer a.free(descriptors);
    var iter = reader.header.tensors.iterator();
    var n: usize = 0;
    while (iter.next()) |entry| {
        const tensor = try reader.readTensor(entry.key_ptr.*);
        descriptors[n] = .{ .name = entry.key_ptr.*, .shape = tensor.shape, .encoding = .{ .dense = tensor.dtype }, .byte_len = tensor.data.len, .quantized = false };
        n += 1;
    }
    _ = try inference.models.gliner_boundary_artifact.validate(a, backbone, .fp32, descriptors, null);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    errdefer store.deinitOwned();
    iter = reader.header.tensors.iterator();
    while (iter.next()) |entry| {
        const original = entry.key_ptr.*;
        const name = try a.dupe(u8, if (std.mem.startsWith(u8, original, "encoder.")) original["encoder.".len..] else original);
        errdefer a.free(name);
        var tensor = try reader.readTensor(original);
        tensor.name = name;
        if (store.resident_weights.contains(name)) return error.DuplicateBenchmarkTensor;
        try store.resident_weights.put(a, name, .{ .tensor = tensor });
    }
    return store;
}
fn extract(a: Allocator, cb: *const inference.ops.ComputeBackend, config: *const model.Config, tokenizer: inference.tokenizer.Tokenizer, text: []const u8, schema_json: []const u8, capture_tokens: bool, timeout_ms: u64, batch_size: usize) !Extraction {
    const deadline = try std.math.add(u64, try nowNs(), try std.math.mul(u64, timeout_ms, std.time.ns_per_ms));
    const control = inference.InferenceExecutionControl{ .deadline_ns = deadline };
    var schema = try schema_mod.compile(a, schema_json, .{});
    defer schema.deinit();
    const items = try a.alloc(processor.Item, batch_size);
    defer a.free(items);
    const batch_schemas = try a.alloc(*const schema_mod.CompiledSchema, batch_size);
    defer a.free(batch_schemas);
    for (items, batch_schemas) |*item, *compiled| {
        item.* = .{ .text = text, .schema = &schema };
        compiled.* = &schema;
    }
    var prepared = try processor.prepare(a, tokenizer, items, .{ .max_batch_items = batch_size, .max_text_words = 128, .max_sequence_tokens = 512, .max_queries = 64, .control = control });
    defer prepared.deinit();
    const tokens = if (capture_tokens) try a.dupe(i64, prepared.input_ids) else null;
    errdefer if (tokens) |ids| a.free(ids);
    if (comptime on_cuda) {
        var result = try inference.architectures.gliner_boundary_request_device.run(cb, a, config, &prepared, batch_schemas, .{
            .pipeline = .{ .offset_unit = .unicode_codepoints, .control = control, .head_limits = .{ .max_batch = batch_size }, .task_limits = .{ .math = .{ .max_batch = batch_size } } },
            .limits = .{ .encoder = .{ .max_batch = batch_size }, .head = .{ .max_batch = batch_size } },
        });
        errdefer result.deinit();
        try control.check();
        return .{ .output = result.outputs, .input_ids = tokens, .encoder_shape = .{ batch_size, prepared.input_ids.len / batch_size } };
    }
    var encoded = try engine.encodeNative(cb, a, config, &prepared, .{ .control = control });
    defer encoded.deinit();
    var output = try pipeline.runNative(cb, a, config, &prepared, batch_schemas, .{ .text_states = encoded.text_states, .query_states = encoded.query_states, .classification_states = encoded.classification_states, .text_lengths = encoded.text_lengths }, .{ .offset_unit = .unicode_codepoints, .control = control });
    errdefer output.deinit();
    try control.check();
    return .{ .output = output, .input_ids = tokens, .encoder_shape = .{ batch_size, prepared.input_ids.len / batch_size } };
}
fn emit(a: Allocator, writer: *std.Io.Writer, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > 4 * 1024 * 1024) return error.BenchmarkResponseLimitExceeded;
    try writer.writeAll(bytes);
    try writer.writeByte('\n');
    try writer.flush();
}
fn parseArgs(init: std.process.Init) !Options {
    var options = Options{};
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        const next = args.next() orelse return error.MissingBenchmarkArgument;
        if (std.mem.eql(u8, arg, "--model-dir")) options.model_dir = next else if (std.mem.eql(u8, arg, "--cases")) options.cases_path = next else if (std.mem.eql(u8, arg, "--threads")) options.threads = try std.fmt.parseInt(usize, next, 10) else if (std.mem.eql(u8, arg, "--batch-size")) options.batch_size = try std.fmt.parseInt(usize, next, 10) else if (std.mem.eql(u8, arg, "--timeout-ms")) options.timeout_ms = try std.fmt.parseInt(u64, next, 10) else if (std.mem.eql(u8, arg, "--max-commands")) options.max_commands = try std.fmt.parseInt(usize, next, 10) else return error.UnknownBenchmarkArgument;
    }
    if (options.model_dir.len == 0 or options.cases_path.len == 0 or options.threads == 0 or options.threads > 32 or options.timeout_ms == 0 or options.timeout_ms > 60000 or options.max_commands == 0 or options.max_commands > 4096) return error.InvalidBenchmarkOptions;
    if (options.batch_size == 0 or options.batch_size > 64 or (!on_cuda and options.batch_size != 1)) return error.InvalidBenchmarkOptions;
    if (!build_options.enable_system_blas and options.threads != 1) return error.BenchmarkThreadControlUnavailable;
    return options;
}
fn verifyThreadEnvironment(a: Allocator, threads: usize) !void {
    const expected = try std.fmt.allocPrint(a, "{d}", .{threads});
    defer a.free(expected);
    inline for (.{ "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS" }) |name| {
        const value = std.c.getenv(name) orelse return error.MissingBenchmarkThreadControl;
        if (!std.mem.eql(u8, std.mem.span(value), expected)) return error.InvalidBenchmarkThreadControl;
    }
}
pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const options = try parseArgs(init);
    try verifyThreadEnvironment(a, options.threads);
    const bytes = try inference.util.c_file.readFileMax(a, options.cases_path, 2 * 1024 * 1024);
    defer a.free(bytes);
    if (bytes.len > 2 * 1024 * 1024) return error.BenchmarkFixtureTooLarge;
    const parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const fixture = parsed.value;
    if (fixture.format_version != 2 or fixture.cases.len == 0 or fixture.cases.len > 32 or !std.mem.eql(u8, fixture.source_commit, "3c913c7369301133d3b7699252074c4303ada50e")) return error.UnpinnedBenchmarkFixture;
    const schemas = try a.alloc([]const u8, fixture.cases.len);
    defer a.free(schemas);
    var compiled_count: usize = 0;
    defer for (schemas[0..compiled_count]) |schema| a.free(schema);
    for (fixture.cases, 0..) |case, i| {
        if (case.text.len > 65536 or case.id.len == 0 or case.id.len > 64) return error.InvalidBenchmarkCase;
        for (fixture.cases[0..i]) |prior| if (std.mem.eql(u8, prior.id, case.id)) return error.DuplicateBenchmarkCase;
        schemas[i] = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        compiled_count += 1;
    }
    const config_bytes = try readPinned(a, options.model_dir, "config.json", fixture.model_files.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, options.model_dir, "encoder_config/config.json", fixture.model_files.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    if (!std.mem.eql(u8, @tagName(config.backbone), fixture.model)) return error.BenchmarkArtifactIdentityMismatch;
    const tok_config = try readPinned(a, options.model_dir, "tokenizer_config.json", fixture.model_files.@"tokenizer_config.json");
    defer a.free(tok_config);
    const tok_bytes = try readPinned(a, options.model_dir, "tokenizer.json", fixture.model_files.@"tokenizer.json");
    defer a.free(tok_bytes);
    const tokenizer = try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(a, tok_bytes, .{ .strict_unigram_normalizer = true });
    defer tokenizer.tokenizer().deinitTokenizer();
    const weights_path = try std.fs.path.join(a, &.{ options.model_dir, "model.safetensors" });
    defer a.free(weights_path);
    var reader = try inference.models.safetensors.MMapReader.openFileAbsolute(a, weights_path);
    defer reader.deinit();
    try verifyBytes(fixture.model_files.@"model.safetensors", reader.file_bytes);
    var store = try loadWeights(a, &reader, config.backbone);
    defer store.deinitOwned();
    // Avoid a second native worker pool: BLAS alone owns the requested dense
    // math thread budget. Without BLAS only the explicit one-thread profile is
    // admitted. Default Io otherwise creates platform-dependent parallelism.
    var backend = if (on_cuda) try inference.native_compute.cuda.CudaCompute.init(a) else native.NativeCompute.initWithIo(a, &store, null, std.Io.Threaded.global_single_threaded.io());
    defer backend.deinit();
    if (comptime on_cuda) {
        if (backend.kernels.gliner25_boundary_f32 == null) return error.CudaKernelUnavailable;
        var it = store.resident_weights.iterator();
        while (it.next()) |entry| {
            const name = try a.dupe(u8, entry.key_ptr.*);
            backend.insertWeightFromLoaded(name, entry.value_ptr) catch |err| {
                a.free(name);
                return err;
            };
        }
    }
    const cb = backend.computeBackend();
    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const cases_digest = hash(bytes);
    try emit(a, &stdout.interface, .{ .event = "ready", .batch_size = options.batch_size, .arm = "native", .scope = scope, .cuda_runtime = if (on_cuda) .{ .device_name = backend.ctx.info.nameSlice(), .driver_version = backend.ctx.info.driver_version, .compute_major = backend.ctx.info.compute_major, .compute_minor = backend.ctx.info.compute_minor } else null, .backend = if (on_cuda) "cuda" else "native", .synchronization_policy = if (on_cuda) "cuda_stream_before_start_and_after_extract_v1" else "synchronous_cpu_v1", .timing_boundary = timing_boundary, .model = fixture.model, .model_id = fixture.model_id, .revision = fixture.revision, .model_files = fixture.model_files, .cases_sha256 = cases_digest[0..], .build_mode = @tagName(builtin.mode), .zig_version = builtin.zig_version_string, .threads = options.threads, .scheduler = "serial_io", .system_blas = build_options.enable_system_blas, .dtype = "float32", .qualification = false });
    var stdin_buffer: [4096]u8 = undefined;
    var stdin = std.Io.File.stdin().readerStreaming(init.io, &stdin_buffer);
    var count: usize = 0;
    while (try stdin.interface.takeDelimiter('\n')) |line| {
        if (line.len > 2048 or count >= options.max_commands) return error.BenchmarkCommandLimitExceeded;
        count += 1;
        const command = try std.json.parseFromSlice(Command, a, line, .{});
        defer command.deinit();
        const cmd = command.value;
        if (cmd.op == .stop) {
            try verifyBytes(fixture.model_files.@"model.safetensors", reader.file_bytes);
            try emit(a, &stdout.interface, .{ .event = "stopped", .request_id = cmd.request_id });
            return;
        }
        var case_index: ?usize = null;
        for (fixture.cases, 0..) |case, i| if (std.mem.eql(u8, cmd.case_id, case.id)) {
            case_index = i;
            break;
        };
        const i = case_index orelse return error.UnknownBenchmarkCase;
        if (comptime on_cuda) try inference.native_compute.cuda.gliner25_api.synchronizeAndDrainDeferredDeviceFrees(&backend);
        const stats_before = cb.trainingRuntimeStats();
        const started = try nowNs();
        var result = try extract(a, &cb, &config, tokenizer.tokenizer(), fixture.cases[i].text, schemas[i], cmd.op == .validate, options.timeout_ms, options.batch_size);
        if (comptime on_cuda) try inference.native_compute.cuda.gliner25_api.synchronizeAndDrainDeferredDeviceFrees(&backend);
        const elapsed = (try nowNs()) - started;
        const stats_after = cb.trainingRuntimeStats();
        if (on_cuda and (stats_after.to_float32_calls != stats_before.to_float32_calls or stats_after.download_alloc_calls != stats_before.download_alloc_calls))
            return error.BenchmarkUnexpectedHostFallback;
        defer result.deinit(a);
        if (elapsed == 0 or result.output.samples.len != options.batch_size) return error.InvalidBenchmarkResult;
        try emit(a, &stdout.interface, .{ .event = "result", .arm = "native", .request_id = cmd.request_id, .case_id = cmd.case_id, .duration_ns = elapsed, .cuda_transfers = if (on_cuda) .{ .h2d_bytes = stats_after.h2d_bytes - stats_before.h2d_bytes, .d2h_bytes = stats_after.d2h_bytes - stats_before.d2h_bytes, .kernel_launches = stats_after.kernel_launches - stats_before.kernel_launches, .host_fallback_calls = stats_after.to_float32_calls - stats_before.to_float32_calls } else null, .batch_size = options.batch_size, .encoder_shape = result.encoder_shape, .input_ids = result.input_ids, .output = result.output.samples[0], .outputs = if (options.batch_size > 1) result.output.samples else null });
    }
    return error.BenchmarkProtocolEndedWithoutStop;
}
