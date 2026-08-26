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
const structlog = @import("structlog");
const inference = @import("inference");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

const print = std.debug.print;

/// Returns ~/.antfly/inference/models if $HOME is set, otherwise falls back to ./models.
fn defaultModelsDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./models";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "models" }) catch "./models";
}

/// Returns ~/.antfly/inference/ml if $HOME is set, otherwise falls back to ./ml.
fn defaultMlDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_ML_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./ml";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "ml" }) catch "./ml";
}

const RunConfig = struct {
    const WarmModelConfig = struct {
        kind: []const u8,
        name: []const u8,
        backend: ?[]const u8 = null,
        format: ?[]const u8 = null,
        quantization: ?[]const u8 = null,
    };

    const PromptCacheConfig = struct {
        enabled: bool = false,
        mode: inference.runtime.kv.prompt_cache.Mode = .block_hash,
        max_bytes_mb: usize = 512,
        min_tokens: usize = 64,
        ttl_ms: u64 = 300_000,
    };

    const InferenceAdmissionConfig = struct {
        max_concurrent_requests: ?u32 = null,
    };

    const AdmissionConfig = struct {
        inference: ?InferenceAdmissionConfig = null,
    };

    models_dir: ?[]const u8 = null,
    ml_dir: ?[]const u8 = null,
    content_security: ?inference.scraping.ContentSecurityConfig = null,
    s3_credentials: ?inference.scraping.S3CredentialsConfig = null,
    preload: []const WarmModelConfig = &.{},
    keep_alive_ms: ?u64 = null,
    max_loaded_models: ?usize = null,
    /// Deprecated compatibility alias for admission.inference.max_concurrent_requests.
    max_concurrent_requests: ?u32 = null,
    admission: ?AdmissionConfig = null,
    pool_size: ?usize = null,
    generation_batching: ?inference.server.GenerationBatchingConfig = null,
    kernel_jit: ?inference.graph.kernel_jit.Config = null,
    prompt_cache: ?PromptCacheConfig = null,
    process_memory_budget_mb: ?usize = null,
};

fn resolvedMaxConcurrentRequests(cfg: RunConfig) !?u32 {
    const canonical = if (cfg.admission) |admission|
        if (admission.inference) |inference_admission|
            inference_admission.max_concurrent_requests
        else
            null
    else
        null;
    if (canonical != null and cfg.max_concurrent_requests != null and
        canonical.? != cfg.max_concurrent_requests.?)
    {
        return error.InvalidInferenceConfig;
    }
    return canonical orelse cfg.max_concurrent_requests;
}

fn loadRunConfig(allocator: std.mem.Allocator, path: []const u8) !std.json.Parsed(RunConfig) {
    const raw = try inference.util.c_file.readFileMax(allocator, path, std.math.maxInt(usize));
    defer allocator.free(raw);
    return try parseRunConfig(allocator, raw);
}

fn parseRunConfig(allocator: std.mem.Allocator, raw: []const u8) !std.json.Parsed(RunConfig) {
    var raw_tree = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{});
    defer raw_tree.deinit();
    const root = switch (raw_tree.value) {
        .object => |object| object,
        else => return error.InvalidInferenceConfig,
    };
    if (root.get("admission")) |admission_value| {
        const admission = switch (admission_value) {
            .object => |object| object,
            else => return error.InvalidInferenceConfig,
        };
        if (admission.count() != @intFromBool(admission.get("inference") != null))
            return error.InvalidInferenceConfig;
        if (admission.get("inference")) |inference_value| {
            const inference_admission = switch (inference_value) {
                .object => |object| object,
                else => return error.InvalidInferenceConfig,
            };
            if (inference_admission.count() != @intFromBool(inference_admission.get("max_concurrent_requests") != null))
                return error.InvalidInferenceConfig;
        }
    }
    const parsed = try std.json.parseFromSlice(RunConfig, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    if (parsed.value.prompt_cache) |value| {
        try (inference.server.PromptCacheConfig{
            .enabled = value.enabled,
            .mode = value.mode,
            .max_bytes_mb = value.max_bytes_mb,
            .min_tokens = value.min_tokens,
            .ttl_ms = value.ttl_ms,
        }).validate();
    }
    if (parsed.value.kernel_jit) |value| try value.validate();
    _ = try resolvedMaxConcurrentRequests(parsed.value);
    return parsed;
}

fn parseBackendType(value: []const u8) ?inference.backends.BackendType {
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "xla") or std.mem.eql(u8, value, "pjrt")) return .pjrt;
    if (std.mem.eql(u8, value, "wasm") or std.mem.eql(u8, value, "webgpu")) return .wasm;
    return null;
}

fn parseOptionalBackendType(value: ?[]const u8) !?inference.backends.BackendType {
    const raw = value orelse return null;
    if (std.mem.eql(u8, raw, "auto")) return null;
    return parseBackendType(raw) orelse error.InvalidArguments;
}

fn parsePreloadModelKind(value: []const u8) ?inference.server.WarmModelKind {
    inline for (std.meta.fields(inference.server.WarmModelKind)) |field| {
        if (std.mem.eql(u8, value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn parsePreloadModelFlag(value: []const u8) !inference.server.WarmModel {
    const separator = std.mem.indexOfScalar(u8, value, ':') orelse return error.InvalidArguments;
    const kind_name = value[0..separator];
    var model_name = value[separator + 1 ..];
    var backend: ?inference.backends.BackendType = null;
    if (std.mem.indexOfScalar(u8, model_name, ':')) |backend_separator| {
        const backend_name = model_name[0..backend_separator];
        if (parseBackendType(backend_name)) |parsed_backend| {
            backend = parsed_backend;
            model_name = model_name[backend_separator + 1 ..];
        }
    }
    if (model_name.len == 0) return error.InvalidArguments;
    return .{
        .kind = parsePreloadModelKind(kind_name) orelse return error.InvalidArguments,
        .name = model_name,
        .backend = backend,
        .format = null,
        .quantization = null,
    };
}

test "preload model parser preserves registry variants and recognizes explicit backends" {
    const variant = try parsePreloadModelFlag("embedder:owner/model:i8");
    try std.testing.expectEqual(inference.server.WarmModelKind.embedder, variant.kind);
    try std.testing.expectEqualStrings("owner/model:i8", variant.name);
    try std.testing.expect(variant.backend == null);

    const multi_component_variant = try parsePreloadModelFlag("generator:owner/model:gguf:Q4_K_M");
    try std.testing.expectEqualStrings("owner/model:gguf:Q4_K_M", multi_component_variant.name);
    try std.testing.expect(multi_component_variant.backend == null);

    const backend = try parsePreloadModelFlag("generator:metal:owner/model:i8");
    try std.testing.expectEqualStrings("owner/model:i8", backend.name);
    try std.testing.expectEqual(inference.backends.BackendType.metal, backend.backend.?);
}

fn parseAdmissionLimit(value: []const u8) !usize {
    return try std.fmt.parseInt(usize, value, 10);
}

fn parseKernelJitMode(value: []const u8) !inference.graph.kernel_jit.Mode {
    return std.meta.stringToEnum(inference.graph.kernel_jit.Mode, value) orelse error.InvalidArguments;
}

fn resolveKernelJitConfig(
    config: ?inference.graph.kernel_jit.Config,
    env_mode: ?[]const u8,
    cli_mode: ?inference.graph.kernel_jit.Mode,
) !inference.graph.kernel_jit.Config {
    var resolved: inference.graph.kernel_jit.Config = config orelse .{};
    if (cli_mode) |value|
        resolved.mode = value
    else if (env_mode) |value|
        resolved.mode = try parseKernelJitMode(value);
    try resolved.validate();
    return resolved;
}

fn parsePositiveU64(value: []const u8) !u64 {
    const parsed = try std.fmt.parseInt(u64, value, 10);
    if (parsed == 0) return error.InvalidArguments;
    return parsed;
}

fn preloadModelsFromConfig(allocator: std.mem.Allocator, values: []const RunConfig.WarmModelConfig) ![]inference.server.WarmModel {
    if (values.len == 0) return &.{};
    const out = try allocator.alloc(inference.server.WarmModel, values.len);
    errdefer allocator.free(out);
    for (values, 0..) |value, i| {
        out[i] = .{
            .kind = parsePreloadModelKind(value.kind) orelse return error.InvalidArguments,
            .name = value.name,
            .backend = try parseOptionalBackendType(value.backend),
            .format = value.format,
            .quantization = value.quantization,
        };
    }
    return out;
}

pub fn main(init: std.process.Init) !void {
    const allocator = platform.allocator.processAllocator(std.heap.smp_allocator);

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_buf: [64][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_iter.next()) |arg| {
        if (args_len < args_buf.len) {
            args_buf[args_len] = arg;
            args_len += 1;
        }
    }
    const args = args_buf[0..args_len];

    if (args.len < 2) {
        printUsage("inference");
        return;
    }

    return try runFromArgs(init, allocator, "antfly inference", args[1..]);
}

pub fn runFromArgs(
    init: std.process.Init,
    allocator: std.mem.Allocator,
    usage_name: []const u8,
    args: []const []const u8,
) !void {
    if (args.len == 0) {
        printUsage(usage_name);
        return;
    }

    const command = args[0];
    const command_args = args[1..];
    if (std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h") or std.mem.eql(u8, command, "help")) {
        printUsage(usage_name);
    } else if (std.mem.eql(u8, command, "run")) {
        if (inference.run_options.isHelpRequest(command_args)) {
            printRunUsage(usage_name);
            return;
        }
        if (build_options.skip_openapi) {
            print("inference run is unavailable when built with -Dskip-openapi=true\n", .{});
            return;
        }
        try runServer(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "embed")) {
        try inference.native_embed.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "classify")) {
        try inference.native_classify.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "rerank")) {
        try inference.native_rerank.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "generate")) {
        try inference.native_generate.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "chat")) {
        try inference.native_chat.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "compile-artifact")) {
        try inference.native_compile.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "export")) {
        try inference.native_export.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "quantize")) {
        try inference.native_quantize.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "quant-kernel-codegen")) {
        try inference.native_quant_kernel_codegen.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "run-artifact")) {
        try inference.native_run_artifact.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "transcribe")) {
        try inference.native_transcribe.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "read")) {
        try inference.native_read.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "extract")) {
        try inference.native_extract.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "compare")) {
        try inference.compare_generate.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "finetune")) {
        try inference.finetune_cli.main(init, command_args);
    } else if (std.mem.eql(u8, command, "cuda-info")) {
        try inference.cuda_info.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "metal-info")) {
        printMetalInfo();
    } else if (std.mem.eql(u8, command, "bench-cuda")) {
        try inference.cuda_microbench.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "smoke")) {
        try inference.native_smoke.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "list")) {
        try listModels(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "pull")) {
        try pullModel(allocator, init.io, usage_name, command_args);
    } else if (std.mem.eql(u8, command, "convert")) {
        try inference.tabular.cli.convertMain(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "version")) {
        printVersion();
    } else {
        print("unknown command: {s}\n", .{command});
        printUsage(usage_name);
    }
}

fn printRunUsage(usage_name: []const u8) void {
    print(
        \\usage: {s} run [options]
        \\
        \\options:
        \\  --host <address>                    Listen address (default: 127.0.0.1)
        \\  --port <port>                       Listen port (default: 8090)
        \\  --models-dir <path>                 AI model directory
        \\  --ml-dir <path>                     Predictor model directory
        \\  --config <path>                     JSON run configuration
        \\  --max-loaded-models <count>          Residency limit; 0 means unlimited
        \\  --max-concurrent-requests <count>    Request concurrency limit
        \\  --process-memory-budget-mb <n>       Whole-process/container memory envelope
        \\  --preload-model <spec>               Warm a model at startup; repeatable
        \\  --allow-unknown-models               Allow models absent from the registry
        \\  -h, --help                           Show this help and exit
        \\
    , .{usage_name});
}

const ProcessMemoryBudgetSource = enum {
    cli,
    canonical_environment,
    compatibility_environment,
    config,
    automatic,
};

const ProcessMemoryBudgetResolution = struct {
    value_mib: ?usize,
    source: ProcessMemoryBudgetSource,
};

fn resolveProcessMemoryBudgetMib(
    cli_override: ?usize,
    canonical_env_value: ?[]const u8,
    compatibility_env_value: ?[]const u8,
    config_value: ?usize,
) !ProcessMemoryBudgetResolution {
    if (cli_override) |value| return .{ .value_mib = value, .source = .cli };
    if (canonical_env_value) |raw| return .{
        .value_mib = std.fmt.parseUnsigned(usize, raw, 10) catch return error.InvalidArguments,
        .source = .canonical_environment,
    };
    if (compatibility_env_value) |raw| return .{
        .value_mib = std.fmt.parseUnsigned(usize, raw, 10) catch return error.InvalidArguments,
        .source = .compatibility_environment,
    };
    if (config_value) |value| return .{ .value_mib = value, .source = .config };
    return .{ .value_mib = null, .source = .automatic };
}

fn runServer(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    structlog.init(.{ .formatter = .json, .level = .info });

    var host: []const u8 = "127.0.0.1";
    var port: u16 = 8090;
    var models_dir: []const u8 = defaultModelsDir(allocator);
    var ml_dir: []const u8 = defaultMlDir(allocator);
    var config_path: ?[]const u8 = null;
    const max_loaded_models_override = try inference.run_options.parseMaxLoadedModelsOverride(args);
    var max_concurrent_requests_override: ?usize = null;
    var process_memory_budget_mb_override: ?usize = null;
    var budget_overrides_mib = inference.runtime.tier.memory.BudgetOverridesMib{};
    var kernel_jit_mode_override: ?inference.graph.kernel_jit.Mode = null;
    var allow_insecure_public_bind = false;
    var allow_unknown_models = false;
    var models_overridden = false;
    var ml_overridden = false;
    var preload_models = std.ArrayListUnmanaged(inference.server.WarmModel).empty;
    defer preload_models.deinit(allocator);

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--host") and i + 1 < args.len) {
            host = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            port = std.fmt.parseInt(u16, args[i + 1], 10) catch 8090;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--models-dir") and i + 1 < args.len) {
            models_dir = args[i + 1];
            models_overridden = true;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--ml-dir") and i + 1 < args.len) {
            ml_dir = args[i + 1];
            ml_overridden = true;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--config") and i + 1 < args.len) {
            config_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--max-concurrent-requests") and i + 1 < args.len) {
            max_concurrent_requests_override = try parseAdmissionLimit(args[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--process-memory-budget-mb") and i + 1 < args.len) {
            process_memory_budget_mb_override = try parseAdmissionLimit(args[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--host-budget-mb") and i + 1 < args.len) {
            budget_overrides_mib.host = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--backend-budget-mb") and i + 1 < args.len) {
            budget_overrides_mib.backend = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--combined-budget-mb") and i + 1 < args.len) {
            budget_overrides_mib.combined = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--kv-budget-mb") and i + 1 < args.len) {
            budget_overrides_mib.kv = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--scratch-budget-mb") and i + 1 < args.len) {
            budget_overrides_mib.scratch = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--kernel-jit-mode")) {
            if (i + 1 >= args.len) return error.MissingKernelJitMode;
            kernel_jit_mode_override = try parseKernelJitMode(args[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--preload-model") and i + 1 < args.len) {
            try preload_models.append(allocator, try parsePreloadModelFlag(args[i + 1]));
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--allow-insecure-public-bind")) {
            allow_insecure_public_bind = true;
        } else if (std.mem.eql(u8, args[i], "--allow-unknown-models")) {
            allow_unknown_models = true;
        } else {
            return error.InvalidArguments;
        }
    }

    var loaded_cfg: ?std.json.Parsed(RunConfig) = if (config_path) |path| try loadRunConfig(allocator, path) else null;
    defer if (loaded_cfg) |*parsed| parsed.deinit();
    if (loaded_cfg) |parsed| {
        const cfg = parsed.value;
        if (!models_overridden) {
            if (cfg.models_dir) |value| models_dir = value;
        }
        if (!ml_overridden) {
            if (cfg.ml_dir) |value| ml_dir = value;
        }
    }

    print("antfly inference v{s}\n", .{build_options.inference_version});
    print("backends: native={} onnx={} onnx_runtime={} metal={} cuda={}\n", .{
        build_options.enable_native,
        !build_options.enable_wasm,
        build_options.enable_onnx,
        build_options.enable_metal,
        build_options.enable_cuda,
    });
    print("ai models: {s}\n", .{models_dir});
    print("ml models: {s}\n", .{ml_dir});

    // Leave SIGINT/SIGTERM on the default OS behavior for now. The previous
    // signal-context stop path could close the listener while accept() was in
    // flight, which panicked under Zig's threaded IO backend.

    var config_preload_models: []inference.server.WarmModel = &.{};
    defer if (config_preload_models.len > 0) allocator.free(config_preload_models);

    const budget_override_limits = try budget_overrides_mib.toByteLimits();
    const process_memory_resolution = try resolveProcessMemoryBudgetMib(
        process_memory_budget_mb_override,
        platform.env.getenv("ANTFLY_PROCESS_MEMORY_BUDGET_MB"),
        platform.env.getenv("ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB"),
        if (loaded_cfg) |parsed| parsed.value.process_memory_budget_mb else null,
    );
    const process_memory_limit_bytes = if (process_memory_resolution.value_mib) |value|
        (try (inference.runtime.tier.memory.BudgetOverridesMib{ .host = value }).toByteLimits()).host_limit_bytes
    else
        0;
    std.log.info(
        "process memory policy operator_source={s} configured_limit_bytes={d}",
        .{ @tagName(process_memory_resolution.source), process_memory_limit_bytes },
    );
    var node_cfg = inference.server.NodeConfig{
        .models_dir = models_dir,
        .ml_dir = ml_dir,
        .generation_budget_overrides = .{
            .host_limit_bytes = budget_override_limits.host_limit_bytes,
            .backend_limit_bytes = budget_override_limits.backend_limit_bytes,
            .combined_limit_bytes = budget_override_limits.combined_limit_bytes,
            .kv_limit_bytes = budget_override_limits.kv_limit_bytes,
            .scratch_limit_bytes = budget_override_limits.scratch_limit_bytes,
        },
        .preload = preload_models.items,
        .process_memory_limit_bytes = process_memory_limit_bytes,
        .process_memory_limit_provenance = if (process_memory_resolution.value_mib != null)
            .explicit
        else
            .automatic,
        .allow_insecure_public_bind = allow_insecure_public_bind,
        .allow_unknown_models = allow_unknown_models,
    };
    if (loaded_cfg) |parsed| {
        const cfg = parsed.value;
        node_cfg.content_security = cfg.content_security;
        node_cfg.s3_credentials = cfg.s3_credentials;
        if (preload_models.items.len == 0) {
            config_preload_models = try preloadModelsFromConfig(allocator, cfg.preload);
            node_cfg.preload = config_preload_models;
        }
        if (cfg.keep_alive_ms) |value| node_cfg.keep_alive_ms = value;
        if (cfg.max_loaded_models) |value| node_cfg.max_loaded_models = value;
        if (try resolvedMaxConcurrentRequests(cfg)) |value| node_cfg.max_concurrent_requests = value;
        if (cfg.pool_size) |value| node_cfg.pool_size = value;
        if (cfg.generation_batching) |value| node_cfg.generation_batching = value;
        if (cfg.prompt_cache) |value| node_cfg.prompt_cache = .{
            .enabled = value.enabled,
            .mode = value.mode,
            .max_bytes_mb = value.max_bytes_mb,
            .min_tokens = value.min_tokens,
            .ttl_ms = value.ttl_ms,
        };
    }
    node_cfg.kernel_jit = try resolveKernelJitConfig(
        if (loaded_cfg) |parsed| parsed.value.kernel_jit else null,
        platform.env.getenv("ANTFLY_INFERENCE_KERNEL_JIT_MODE"),
        kernel_jit_mode_override,
    );
    print("kernel jit: mode={s} cache_mb={d} preload_budget_ms={d}\n", .{
        @tagName(node_cfg.kernel_jit.mode),
        node_cfg.kernel_jit.max_cache_bytes_mb,
        node_cfg.kernel_jit.preload_budget_ms,
    });
    if (node_cfg.kernel_jit.qualified_profile_path) |path| {
        print("kernel jit qualified profile: {s}\n", .{path});
    }
    if (max_loaded_models_override) |value| node_cfg.max_loaded_models = value;
    if (max_concurrent_requests_override) |value| node_cfg.max_concurrent_requests = value;

    var node = try inference.server.Node.init(allocator, node_cfg);
    defer node.deinit();
    node.attachIo(io);

    try node.warmConfiguredModelsBeforeServing(allocator);
    node.configureForcedRunAdmissionDenialsFromEnvironmentForTesting();
    print("listening on {s}:{d}\n", .{ host, port });
    try node.serve(allocator, io, host, port);

    print("server stopped.\n", .{});
}

fn listModels(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var models_dir: []const u8 = defaultModelsDir(allocator);
    if (args.len > 0 and !std.mem.startsWith(u8, args[0], "--")) {
        models_dir = args[0];
    }

    var reg = inference.registry.ModelRegistry.init(allocator, models_dir);
    defer reg.deinit();

    const models = try reg.discover(io);
    defer allocator.free(models);

    for (models) |m| {
        print("{s:<12} {s}\n", .{ @tagName(m.kind), m.name });
    }
}

fn pullModel(allocator: std.mem.Allocator, io: std.Io, usage_name: []const u8, args: []const []const u8) !void {
    if (args.len == 0) {
        print("usage: {s} pull <owner/name|hf:owner/name>[:gguf|:gguf:Q4_K_M|:mmproj] [--token <hf-token>] [--models-dir <dir>] [--tasks <task1,task2>] [--capabilities <cap1,cap2>] [--projector <auto|none|Q8_0|filename>] [--max-artifact-bytes <n>] [--max-model-bytes <n>]\n", .{usage_name});
        print("       {s} pull hf:<owner>/<repo> --type predictor [--name <predictor-name>] [--ml-dir <dir>] [--file <repo-path>] [--framework auto|onnx|xgboost|lightgbm]\n", .{usage_name});
        print("       {s} pull <https-url-to-tabular-artifact> --name <predictor-name> [--ml-dir <dir>] [--token <bearer-token>]\n", .{usage_name});
        print("variants: <model-ref>:gguf, <model-ref>:gguf:Q4_K, <model-ref>:onnx, <model-ref>:hybrid, <model-ref>:safetensors\n", .{});
        print("CLIP/CLAP v0.2 example: {s} pull antflydb/clipclap:gguf:Q4_K\n", .{usage_name});
        return;
    }
    const ref = args[0];
    if (inference.tabular.cli.isHttpUrl(ref) or isPredictorPull(args)) {
        try inference.tabular.cli.pullMain(allocator, io, args, defaultMlDir(allocator));
        return;
    }

    // Parse optional --token flag
    var token: ?[]const u8 = null;
    var tasks_csv: ?[]const u8 = null;
    var capabilities_csv: ?[]const u8 = null;
    var models_dir: []const u8 = defaultModelsDir(allocator);
    var projector_selection: inference.registry.download.ProjectorSelection = .auto;
    var max_artifact_bytes = inference.registry.download.default_max_artifact_bytes;
    var max_model_bytes = inference.registry.download.default_max_model_bytes;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--token") and i + 1 < args.len) {
            token = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--models-dir") and i + 1 < args.len) {
            models_dir = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--ml-dir") and i + 1 < args.len) {
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--tasks") and i + 1 < args.len) {
            tasks_csv = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--capabilities") and i + 1 < args.len) {
            capabilities_csv = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--projector") and i + 1 < args.len) {
            projector_selection = inference.registry.download.parseProjectorSelection(args[i + 1]) orelse return error.InvalidArguments;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--max-artifact-bytes") and i + 1 < args.len) {
            max_artifact_bytes = try parsePositiveU64(args[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--max-model-bytes") and i + 1 < args.len) {
            max_model_bytes = try parsePositiveU64(args[i + 1]);
            i += 1;
        }
    }

    // Also check HF_TOKEN env var.
    if (token == null) {
        token = platform.env.getenv("HF_TOKEN");
    }

    print("pulling {s}...\n", .{ref});

    var reg = inference.registry.ModelRegistry.init(allocator, models_dir);
    defer reg.deinit();
    try reg.pull(io, ref, .{
        .token = token,
        .max_artifact_bytes = max_artifact_bytes,
        .max_model_bytes = max_model_bytes,
    }, tasks_csv, capabilities_csv, projector_selection);

    print("done.\n", .{});
}

fn isPredictorPull(args: []const []const u8) bool {
    if (args.len == 0 or !inference.tabular.cli.isHuggingFaceRef(args[0])) return false;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--type") and i + 1 < args.len) {
            return std.mem.eql(u8, args[i + 1], "predictor") or std.mem.eql(u8, args[i + 1], "predictors");
        }
    }
    return false;
}

pub fn printVersion() void {
    print("antfly inference v{s}\n", .{build_options.inference_version});
    print("backends: native={} onnx={} onnx_runtime={} metal={} cuda={}\n", .{
        build_options.enable_native,
        !build_options.enable_wasm,
        build_options.enable_onnx,
        build_options.enable_metal,
        build_options.enable_cuda,
    });
}

fn printMetalInfo() void {
    print("metal build_enabled={}\n", .{build_options.enable_metal});
    if (comptime build_options.enable_metal) {
        print("metal device_available={}\n", .{inference.metal_runtime.metalDeviceAvailable()});
    }
}

fn printUsage(usage_name: []const u8) void {
    print(
        \\Usage: {s} <command> [options]
        \\
        \\Commands:
        \\  run       Start the inference server
        \\  embed     Run native text/image/audio embedding from the command line
        \\  classify  Run native text classification from the command line
        \\  rerank    Run native text reranking from the command line
        \\  generate  Run native text generation from the command line
        \\  chat      Interactive chat with a local model (pulls known models on first use)
        \\  compile-artifact Compile one or more traced generation artifacts
        \\  export    Convert a model artifact to ONNX, GGUF, or safetensors
        \\  quantize  Create a quantized model variant
        \\  quant-kernel-codegen Verify or rewrite dev-generated quant kernel sources
        \\  run-artifact Run or validate a compiled offline artifact
        \\  transcribe Run native audio transcription from the command line
        \\  read      Run image/document reading from the command line
        \\  extract   Run native entity, relation, or structured extraction from the command line
        \\  compare   Compare inference backends or implementations
        \\  finetune  Run fine-tuning recipes, datasets, adapters, train/eval, and workflows
        \\  smoke     Run a native GGUF/SafeTensors smoke test
        \\  cuda-info Inspect CUDA Driver API availability and optionally run CUDA smoke checks
        \\  metal-info Inspect Metal device availability
        \\  bench-cuda Benchmark CUDA Q4_K, GLiNER2, and CLIP/CLAP kernel shapes
        \\  list      List available models
        \\  pull      Download a HuggingFace model, or pull a hosted tabular_model.json predictor URL
        \\  convert   Convert a native ML model (XGBoost/LightGBM/ONNX) to the antfly tabular IR
        \\  version   Print version information
        \\
        \\Run options:
        \\  --host <addr>     Listen address (default: 127.0.0.1)
        \\  --allow-insecure-public-bind Allow a non-loopback listener without built-in auth or TLS
        \\  --port <port>     Listen port (default: 8090)
        \\  --models-dir <dir>    AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <dir>        Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --config <path>       JSON runtime configuration, including full kernel JIT policy
        \\  --max-loaded-models <n> Bound resident model count with LRU eviction
        \\  --max-concurrent-requests <n> Bound weighted in-flight request capacity before returning 503
        \\  --host-budget-mb <n> Process-wide inference host-memory admission override (MiB)
        \\  --backend-budget-mb <n> Process-wide inference device-memory admission override (MiB)
        \\  --combined-budget-mb <n> Process-wide inference combined-memory admission override (MiB)
        \\  --kv-budget-mb <n> Process-wide inference KV-cache admission override (MiB)
        \\  --scratch-budget-mb <n> Process-wide inference scratch-memory admission override (MiB)
        \\  --kernel-jit-mode <off|shadow|on|required> JIT startup-preloaded Metal/CUDA models
        \\  --preload-model <kind:name|kind:backend:name> Preload and warm a configured model before serving
        \\  --allow-unknown-models Permit artifacts whose compatibility cannot be proven; known incompatible models remain blocked
        \\
        \\Pull options:
        \\  --token <token>   HuggingFace API token (or set HF_TOKEN env var)
        \\  --name <name>     Local predictor name when pulling a tabular model URL
        \\  --tasks <list>    Comma-separated task hints for the pulled model
        \\  --capabilities <list> Comma-separated capability hints for the pulled model
        \\  --projector <value> Projector sidecar selection for GGUF pulls: auto, none, quant suffix, or filename
        \\  --max-artifact-bytes <n> Maximum bytes accepted for one model artifact (default: 68719476736)
        \\  --max-model-bytes <n> Maximum aggregate bytes accepted for one pull (default: 137438953472)
        \\  --models-dir <dir>    AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <dir>        Traditional ML directory for URL pulls (default: ~/.antfly/inference/ml)
        \\  variants          <model-ref>:gguf, <model-ref>:gguf:Q4_K, <model-ref>:onnx, <model-ref>:hybrid, <model-ref>:safetensors
        \\                    default :gguf now prefers smaller GGUF quants; use :gguf:Q... for larger files
        \\  CLIP/CLAP v0.2    {s} pull antflydb/clipclap:gguf:Q4_K
        \\
    , .{ usage_name, usage_name });
}

test "run config parses shared scraping fields and ignores api_url" {
    const raw =
        \\{
        \\  "api_url": "http://127.0.0.1:8082",
        \\  "models_dir": "/tmp/models",
        \\  "ml_dir": "/tmp/ml",
        \\  "content_security": {
        \\    "block_private_ips": true
        \\  },
        \\  "s3_credentials": {
        \\    "endpoint": "s3.amazonaws.com"
        \\  },
        \\  "preload": [
        \\    { "kind": "generator", "name": "antflydb/gemma-e2b", "backend": "metal", "format": "gguf", "quantization": "q4_k" }
        \\  ],
        \\  "max_loaded_models": 8,
        \\  "admission": { "inference": { "max_concurrent_requests": 12 } },
        \\  "pool_size": 4,
        \\  "generation_batching": {
        \\    "mode": "on",
        \\    "max_step_items": 8,
        \\    "max_step_query_tokens": 256,
        \\    "max_decode_wait_us": 750,
        \\    "max_idle_prefill_chunk_size": 1024
        \\  },
        \\  "kernel_jit": { "mode": "shadow", "cache_dir": "/tmp/jit", "qualified_profile_path": "/tmp/jit-profile.json", "max_cache_bytes_mb": 256, "preload_budget_ms": 120000 },
        \\  "prompt_cache": { "enabled": true, "mode": "block_hash", "max_bytes_mb": 64, "min_tokens": 32, "ttl_ms": 1000 }
        \\}
    ;
    const parsed = try parseRunConfig(std.testing.allocator, raw);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("/tmp/models", parsed.value.models_dir.?);
    try std.testing.expectEqual(@as(?u32, 12), parsed.value.admission.?.inference.?.max_concurrent_requests);
    try std.testing.expectEqualStrings("/tmp/ml", parsed.value.ml_dir.?);
    try std.testing.expectEqual(@as(?bool, true), parsed.value.content_security.?.block_private_ips);
    try std.testing.expectEqualStrings("s3.amazonaws.com", parsed.value.s3_credentials.?.endpoint.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.preload.len);
    try std.testing.expectEqualStrings("generator", parsed.value.preload[0].kind);
    try std.testing.expectEqualStrings("antflydb/gemma-e2b", parsed.value.preload[0].name);
    try std.testing.expectEqualStrings("metal", parsed.value.preload[0].backend.?);
    try std.testing.expectEqualStrings("gguf", parsed.value.preload[0].format.?);
    try std.testing.expectEqualStrings("q4_k", parsed.value.preload[0].quantization.?);
    try std.testing.expectEqual(@as(?usize, 8), parsed.value.max_loaded_models);
    try std.testing.expectEqual(@as(?usize, 4), parsed.value.pool_size);
    try std.testing.expectEqual(inference.server.GenerationBatchingMode.on, parsed.value.generation_batching.?.mode);
    try std.testing.expectEqual(@as(usize, 8), parsed.value.generation_batching.?.max_step_items);
    try std.testing.expectEqual(@as(usize, 256), parsed.value.generation_batching.?.max_step_query_tokens);
    try std.testing.expectEqual(@as(u32, 750), parsed.value.generation_batching.?.max_decode_wait_us);
    try std.testing.expectEqual(@as(usize, 1024), parsed.value.generation_batching.?.max_idle_prefill_chunk_size);
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.shadow, parsed.value.kernel_jit.?.mode);
    try std.testing.expectEqualStrings("/tmp/jit", parsed.value.kernel_jit.?.cache_dir.?);
    try std.testing.expectEqualStrings("/tmp/jit-profile.json", parsed.value.kernel_jit.?.qualified_profile_path.?);
    try std.testing.expectEqual(@as(usize, 256), parsed.value.kernel_jit.?.max_cache_bytes_mb);
    try std.testing.expectEqual(@as(u64, 120_000), parsed.value.kernel_jit.?.preload_budget_ms);
    try std.testing.expectEqual(true, parsed.value.prompt_cache.?.enabled);
    try std.testing.expectEqual(inference.runtime.kv.prompt_cache.Mode.block_hash, parsed.value.prompt_cache.?.mode);
    try std.testing.expectEqual(@as(usize, 64), parsed.value.prompt_cache.?.max_bytes_mb);
    try std.testing.expectEqual(@as(usize, 32), parsed.value.prompt_cache.?.min_tokens);
    try std.testing.expectEqual(@as(u64, 1000), parsed.value.prompt_cache.?.ttl_ms);
}

test "run config accepts canonical and legacy inference admission spellings" {
    const disabled = try parseRunConfig(std.testing.allocator,
        \\{"admission":{"inference":{"max_concurrent_requests":0}}}
    );
    defer disabled.deinit();
    try std.testing.expectEqual(@as(?u32, 0), disabled.value.admission.?.inference.?.max_concurrent_requests);

    const legacy = try parseRunConfig(std.testing.allocator, "{\"max_concurrent_requests\":1}");
    defer legacy.deinit();
    try std.testing.expectEqual(@as(?u32, 1), try resolvedMaxConcurrentRequests(legacy.value));

    const matching = try parseRunConfig(std.testing.allocator,
        \\{"max_concurrent_requests":1,"admission":{"inference":{"max_concurrent_requests":1}}}
    );
    defer matching.deinit();
    try std.testing.expectEqual(@as(?u32, 1), try resolvedMaxConcurrentRequests(matching.value));

    try std.testing.expectError(error.InvalidInferenceConfig, parseRunConfig(std.testing.allocator,
        \\{"max_concurrent_requests":1,"admission":{"inference":{"max_concurrent_requests":2}}}
    ));
    try std.testing.expectError(
        error.InvalidInferenceConfig,
        parseRunConfig(std.testing.allocator, "{\"admission\":{\"infer\":{\"max_concurrent_requests\":1}}}"),
    );
}

test "run config rejects unrepresentable prompt cache values" {
    const allocator = std.testing.allocator;
    const bytes_overflow = try std.fmt.allocPrint(
        allocator,
        "{{\"prompt_cache\":{{\"max_bytes_mb\":{d}}}}}",
        .{inference.runtime.kv.prompt_cache.max_config_bytes_mb + 1},
    );
    defer allocator.free(bytes_overflow);
    try std.testing.expectError(error.InvalidPromptCacheConfig, parseRunConfig(allocator, bytes_overflow));

    const ttl_overflow = try std.fmt.allocPrint(
        allocator,
        "{{\"prompt_cache\":{{\"ttl_ms\":{d}}}}}",
        .{inference.runtime.kv.prompt_cache.max_config_ttl_ms + 1},
    );
    defer allocator.free(ttl_overflow);
    try std.testing.expectError(error.InvalidPromptCacheConfig, parseRunConfig(allocator, ttl_overflow));
}

test "run config parses opt-in radix prompt cache mode" {
    const parsed = try parseRunConfig(std.testing.allocator, "{\"prompt_cache\":{\"enabled\":true,\"mode\":\"radix\"}}");
    defer parsed.deinit();
    try std.testing.expect(parsed.value.prompt_cache.?.enabled);
    try std.testing.expectEqual(inference.runtime.kv.prompt_cache.Mode.radix, parsed.value.prompt_cache.?.mode);
}

test "kernel JIT config defaults off and rejects invalid budgets" {
    const parsed = try parseRunConfig(std.testing.allocator, "{\"kernel_jit\":{}}");
    defer parsed.deinit();
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.off, parsed.value.kernel_jit.?.mode);
    try std.testing.expectEqual(@as(usize, 1024), parsed.value.kernel_jit.?.max_cache_bytes_mb);
    try std.testing.expectEqual(@as(u64, 300_000), parsed.value.kernel_jit.?.preload_budget_ms);

    try std.testing.expectError(
        error.InvalidKernelJitPreloadBudget,
        parseRunConfig(std.testing.allocator, "{\"kernel_jit\":{\"preload_budget_ms\":999}}"),
    );
    try std.testing.expectError(
        error.InvalidKernelJitCacheDir,
        parseRunConfig(std.testing.allocator, "{\"kernel_jit\":{\"cache_dir\":\"\"}}"),
    );
}

test "kernel JIT mode precedence is CLI then environment then config" {
    const configured = inference.graph.kernel_jit.Config{ .mode = .shadow, .max_cache_bytes_mb = 256 };
    const from_config = try resolveKernelJitConfig(configured, null, null);
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.shadow, from_config.mode);

    const from_env = try resolveKernelJitConfig(configured, "on", null);
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.on, from_env.mode);
    try std.testing.expectEqual(@as(usize, 256), from_env.max_cache_bytes_mb);

    const from_cli = try resolveKernelJitConfig(configured, "on", .required);
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.required, from_cli.mode);
    const from_cli_over_invalid_env = try resolveKernelJitConfig(configured, "invalid", .required);
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.required, from_cli_over_invalid_env.mode);

    const defaults = try resolveKernelJitConfig(null, null, null);
    try std.testing.expectEqual(inference.graph.kernel_jit.Mode.off, defaults.mode);
    try std.testing.expectError(error.InvalidArguments, resolveKernelJitConfig(null, "invalid", null));
}

test "process memory budget precedence ignores shadowed invalid sources" {
    try std.testing.expectEqual(
        ProcessMemoryBudgetSource.cli,
        (try resolveProcessMemoryBudgetMib(0, "invalid", "also-invalid", 512)).source,
    );
    try std.testing.expectEqual(
        @as(?usize, 0),
        (try resolveProcessMemoryBudgetMib(0, "invalid", "also-invalid", 512)).value_mib,
    );
    try std.testing.expectEqual(
        @as(?usize, 256),
        (try resolveProcessMemoryBudgetMib(null, "256", "invalid", 512)).value_mib,
    );
    try std.testing.expectEqual(
        @as(?usize, 384),
        (try resolveProcessMemoryBudgetMib(null, null, "384", 512)).value_mib,
    );
    try std.testing.expectEqual(
        @as(?usize, 512),
        (try resolveProcessMemoryBudgetMib(null, null, null, 512)).value_mib,
    );
    try std.testing.expectEqual(
        ProcessMemoryBudgetSource.config,
        (try resolveProcessMemoryBudgetMib(null, null, null, 512)).source,
    );
    try std.testing.expectError(
        error.InvalidArguments,
        resolveProcessMemoryBudgetMib(null, "invalid", null, 512),
    );
}

test "inference run rejects unknown flags instead of silently disabling policy" {
    try std.testing.expectError(
        error.InvalidArguments,
        runServer(std.heap.page_allocator, std.testing.io, &.{ "--kernel-jti-mode", "required" }),
    );
}

test "run max concurrent request parser accepts zero as unlimited" {
    try std.testing.expectEqual(@as(usize, 6), try parseAdmissionLimit("6"));
    try std.testing.expectEqual(@as(usize, 0), try parseAdmissionLimit("0"));
    try std.testing.expectError(error.InvalidCharacter, parseAdmissionLimit("six"));
    try std.testing.expectEqual(@as(u64, 137438953472), try parsePositiveU64("137438953472"));
    try std.testing.expectError(error.InvalidArguments, parsePositiveU64("0"));
}
