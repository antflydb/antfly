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

    models_dir: ?[]const u8 = null,
    ml_dir: ?[]const u8 = null,
    content_security: ?inference.scraping.ContentSecurityConfig = null,
    s3_credentials: ?inference.scraping.S3CredentialsConfig = null,
    preload: []const WarmModelConfig = &.{},
    keep_alive_ms: ?u64 = null,
    max_loaded_models: ?usize = null,
    max_concurrent_requests: ?usize = null,
    pool_size: ?usize = null,
    prompt_cache: ?PromptCacheConfig = null,
};

fn loadRunConfig(allocator: std.mem.Allocator, path: []const u8) !RunConfig {
    const raw = try inference.util.c_file.readFileMax(allocator, path, std.math.maxInt(usize));
    defer allocator.free(raw);
    const parsed = try std.json.parseFromSlice(RunConfig, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    return parsed.value;
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
        backend = parseBackendType(backend_name) orelse return error.InvalidArguments;
        model_name = model_name[backend_separator + 1 ..];
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

fn parsePositiveUsize(value: []const u8) !usize {
    const parsed = try std.fmt.parseInt(usize, value, 10);
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
    } else if (std.mem.eql(u8, command, "compile-artifact")) {
        try inference.native_compile.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "export")) {
        try inference.native_export.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "quantize")) {
        try inference.native_quantize.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "run-artifact")) {
        try inference.native_run_artifact.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "transcribe")) {
        try inference.native_transcribe.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "read")) {
        try inference.native_read.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "recognize")) {
        try inference.native_recognize.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "extract")) {
        try inference.native_extract.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "compare")) {
        try inference.compare_generate.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "finetune")) {
        try inference.finetune_cli.main(init, command_args);
    } else if (std.mem.eql(u8, command, "cuda-info")) {
        try inference.cuda_info.main(allocator, init.io, command_args);
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

fn runServer(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    structlog.init(.{ .formatter = .json, .level = .info });

    var host: []const u8 = "127.0.0.1";
    var port: u16 = 8090;
    var models_dir: []const u8 = defaultModelsDir(allocator);
    var ml_dir: []const u8 = defaultMlDir(allocator);
    var config_path: ?[]const u8 = null;
    var max_concurrent_requests_override: ?usize = null;
    var allow_experimental_models = false;
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
            max_concurrent_requests_override = try parsePositiveUsize(args[i + 1]);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--preload-model") and i + 1 < args.len) {
            try preload_models.append(allocator, try parsePreloadModelFlag(args[i + 1]));
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--allow-experimental-models")) {
            allow_experimental_models = true;
        }
    }

    const loaded_cfg = if (config_path) |path| try loadRunConfig(allocator, path) else null;
    if (loaded_cfg) |cfg| {
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

    var node_cfg = inference.server.NodeConfig{
        .models_dir = models_dir,
        .ml_dir = ml_dir,
        .preload = preload_models.items,
        .allow_experimental_models = allow_experimental_models,
    };
    if (loaded_cfg) |cfg| {
        node_cfg.content_security = cfg.content_security;
        node_cfg.s3_credentials = cfg.s3_credentials;
        if (preload_models.items.len == 0) {
            config_preload_models = try preloadModelsFromConfig(allocator, cfg.preload);
            node_cfg.preload = config_preload_models;
        }
        if (cfg.keep_alive_ms) |value| node_cfg.keep_alive_ms = value;
        if (cfg.max_loaded_models) |value| node_cfg.max_loaded_models = value;
        if (cfg.max_concurrent_requests) |value| node_cfg.max_concurrent_requests = value;
        if (cfg.pool_size) |value| node_cfg.pool_size = value;
        if (cfg.prompt_cache) |value| node_cfg.prompt_cache = .{
            .enabled = value.enabled,
            .mode = value.mode,
            .max_bytes_mb = value.max_bytes_mb,
            .min_tokens = value.min_tokens,
            .ttl_ms = value.ttl_ms,
        };
    }
    if (max_concurrent_requests_override) |value| node_cfg.max_concurrent_requests = value;

    var node = try inference.server.Node.init(allocator, node_cfg);
    defer node.deinit();
    node.attachIo(io);

    try node.warmConfiguredModels(allocator);
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
        print("usage: {s} pull <owner/name|hf:owner/name>[:gguf|:gguf:Q4_K_M|:mmproj] [--token <hf-token>] [--models-dir <dir>] [--tasks <task1,task2>] [--capabilities <cap1,cap2>] [--projector <auto|none|Q8_0|filename>]\n", .{usage_name});
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
        }
    }

    // Also check HF_TOKEN env var.
    if (token == null) {
        token = platform.env.getenv("HF_TOKEN");
    }

    print("pulling {s}...\n", .{ref});

    var reg = inference.registry.ModelRegistry.init(allocator, models_dir);
    defer reg.deinit();
    try reg.pull(io, ref, token, tasks_csv, capabilities_csv, projector_selection);

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
        \\  compile-artifact Compile one or more traced generation artifacts
        \\  export    Convert a model artifact to ONNX, GGUF, or safetensors
        \\  quantize  Create a quantized model variant
        \\  run-artifact Run or validate a compiled offline artifact
        \\  transcribe Run native audio transcription from the command line
        \\  read      Run image/document reading from the command line
        \\  recognize Run native entity recognition from the command line
        \\  extract   Run native structured extraction from the command line
        \\  compare   Compare inference backends or implementations
        \\  finetune  Run fine-tuning recipes, datasets, adapters, train/eval, and workflows
        \\  smoke     Run a native GGUF/SafeTensors smoke test
        \\  cuda-info Inspect CUDA Driver API availability and optionally run CUDA smoke checks
        \\  bench-cuda Benchmark CUDA Q4_K, GLiNER2, and CLIP/CLAP kernel shapes
        \\  list      List available models
        \\  pull      Download a HuggingFace model, or pull a hosted tabular_model.json predictor URL
        \\  convert   Convert a native ML model (XGBoost/LightGBM/ONNX) to the antfly tabular IR
        \\  version   Print version information
        \\
        \\Run options:
        \\  --host <addr>     Listen address (default: 127.0.0.1)
        \\  --port <port>     Listen port (default: 8090)
        \\  --models-dir <dir>    AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <dir>        Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --max-concurrent-requests <n> Bound weighted in-flight request capacity before returning 503
        \\  --preload-model <kind:name|kind:backend:name> Preload and warm a configured model before serving
        \\  --allow-experimental-models Permit unrecognized architectures; known unsafe models remain blocked
        \\
        \\Pull options:
        \\  --token <token>   HuggingFace API token (or set HF_TOKEN env var)
        \\  --name <name>     Local predictor name when pulling a tabular model URL
        \\  --tasks <list>    Comma-separated task hints for the pulled model
        \\  --capabilities <list> Comma-separated capability hints for the pulled model
        \\  --projector <value> Projector sidecar selection for GGUF pulls: auto, none, quant suffix, or filename
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
        \\  "pool_size": 4,
        \\  "prompt_cache": { "enabled": true, "mode": "block_hash", "max_bytes_mb": 64, "min_tokens": 32, "ttl_ms": 1000 }
        \\}
    ;
    const parsed = try std.json.parseFromSlice(RunConfig, std.testing.allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try std.testing.expectEqualStrings("/tmp/models", parsed.value.models_dir.?);
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
    try std.testing.expectEqual(true, parsed.value.prompt_cache.?.enabled);
    try std.testing.expectEqual(inference.runtime.kv.prompt_cache.Mode.block_hash, parsed.value.prompt_cache.?.mode);
    try std.testing.expectEqual(@as(usize, 64), parsed.value.prompt_cache.?.max_bytes_mb);
    try std.testing.expectEqual(@as(usize, 32), parsed.value.prompt_cache.?.min_tokens);
    try std.testing.expectEqual(@as(u64, 1000), parsed.value.prompt_cache.?.ttl_ms);
}

test "run max concurrent request parser rejects zero" {
    try std.testing.expectEqual(@as(usize, 6), try parsePositiveUsize("6"));
    try std.testing.expectError(error.InvalidArguments, parsePositiveUsize("0"));
    try std.testing.expectError(error.InvalidCharacter, parsePositiveUsize("six"));
}
