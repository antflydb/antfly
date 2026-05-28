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

const RunConfig = struct {
    models_dir: ?[]const u8 = null,
    content_security: ?inference.scraping.ContentSecurityConfig = null,
    s3_credentials: ?inference.scraping.S3CredentialsConfig = null,
    allow_downloads: ?bool = null,
    keep_alive_ms: ?u64 = null,
    max_loaded_models: ?usize = null,
    max_concurrent_requests: ?usize = null,
    pool_size: ?usize = null,
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
    } else if (std.mem.eql(u8, command, "smoke")) {
        try inference.native_smoke.main(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "list")) {
        try listModels(allocator, init.io, command_args);
    } else if (std.mem.eql(u8, command, "pull")) {
        try pullModel(allocator, init.io, usage_name, command_args);
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
    var config_path: ?[]const u8 = null;
    var models_overridden = false;

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
        } else if (std.mem.eql(u8, args[i], "--config") and i + 1 < args.len) {
            config_path = args[i + 1];
            i += 1;
        }
    }

    const loaded_cfg = if (config_path) |path| try loadRunConfig(allocator, path) else null;
    if (loaded_cfg) |cfg| {
        if (!models_overridden) {
            if (cfg.models_dir) |value| models_dir = value;
        }
    }

    print("antfly-inference v{s}\n", .{build_options.inference_version});
    print("backends: native={} onnx={} onnx_runtime={} metal={} mlx={}\n", .{
        build_options.enable_native,
        !build_options.enable_wasm,
        build_options.enable_onnx,
        build_options.enable_metal,
        build_options.enable_mlx,
    });
    print("models: {s}\n", .{models_dir});
    print("listening on {s}:{d}\n", .{ host, port });

    // Leave SIGINT/SIGTERM on the default OS behavior for now. The previous
    // signal-context stop path could close the listener while accept() was in
    // flight, which panicked under Zig's threaded IO backend.

    var node_cfg = inference.server.NodeConfig{
        .models_dir = models_dir,
    };
    if (loaded_cfg) |cfg| {
        node_cfg.content_security = cfg.content_security;
        node_cfg.s3_credentials = cfg.s3_credentials;
        if (cfg.allow_downloads) |value| node_cfg.allow_downloads = value;
        if (cfg.keep_alive_ms) |value| node_cfg.keep_alive_ms = value;
        if (cfg.max_loaded_models) |value| node_cfg.max_loaded_models = value;
        if (cfg.max_concurrent_requests) |value| node_cfg.max_concurrent_requests = value;
        if (cfg.pool_size) |value| node_cfg.pool_size = value;
    }

    var node = try inference.server.Node.init(allocator, node_cfg);
    defer node.deinit();

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
        print("usage: {s} pull <owner/name|hf:owner/name>[:gguf|:gguf:Q4_K_M|:mmproj] [--token <hf-token>] [--models-dir <dir>] [--tasks <task1,task2>] [--capabilities <cap1,cap2>]\n", .{usage_name});
        return;
    }
    const ref = args[0];

    // Parse optional --token flag
    var token: ?[]const u8 = null;
    var tasks_csv: ?[]const u8 = null;
    var capabilities_csv: ?[]const u8 = null;
    var models_dir: []const u8 = defaultModelsDir(allocator);
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--token") and i + 1 < args.len) {
            token = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--models-dir") and i + 1 < args.len) {
            models_dir = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--tasks") and i + 1 < args.len) {
            tasks_csv = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--capabilities") and i + 1 < args.len) {
            capabilities_csv = args[i + 1];
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
    try reg.pull(io, ref, token, tasks_csv, capabilities_csv);

    print("done.\n", .{});
}

pub fn printVersion() void {
    print("antfly-inference v{s}\n", .{build_options.inference_version});
    print("backends: native={} onnx={} onnx_runtime={} metal={} mlx={} cuda={}\n", .{
        build_options.enable_native,
        !build_options.enable_wasm,
        build_options.enable_onnx,
        build_options.enable_metal,
        build_options.enable_mlx,
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
        \\  list      List available models
        \\  pull      Download a model from HuggingFace Hub
        \\
        \\Run options:
        \\  --host <addr>     Listen address (default: 127.0.0.1)
        \\  --port <port>     Listen port (default: 8090)
        \\  --models-dir <dir>    Models directory (default: ~/.antfly/inference/models)
        \\
        \\Pull options:
        \\  --token <token>   HuggingFace API token (or set HF_TOKEN env var)
        \\  --tasks <list>    Comma-separated task hints for the pulled model
        \\  --capabilities <list> Comma-separated capability hints for the pulled model
        \\  --models-dir <dir>    Models directory (default: ~/.antfly/inference/models)
        \\  variants          <model-ref>:gguf, <model-ref>:gguf:Q4_K_M, <model-ref>:mmproj
        \\                    default :gguf now prefers smaller GGUF quants; use :gguf:Q... for larger files
        \\
    , .{usage_name});
}

test "run config parses shared scraping fields and ignores api_url" {
    const raw =
        \\{
        \\  "api_url": "http://127.0.0.1:8082",
        \\  "models_dir": "/tmp/models",
        \\  "content_security": {
        \\    "block_private_ips": true
        \\  },
        \\  "s3_credentials": {
        \\    "endpoint": "s3.amazonaws.com"
        \\  },
        \\  "max_loaded_models": 8,
        \\  "pool_size": 4
        \\}
    ;
    const parsed = try std.json.parseFromSlice(RunConfig, std.testing.allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try std.testing.expectEqualStrings("/tmp/models", parsed.value.models_dir.?);
    try std.testing.expectEqual(@as(?bool, true), parsed.value.content_security.?.block_private_ips);
    try std.testing.expectEqualStrings("s3.amazonaws.com", parsed.value.s3_credentials.?.endpoint.?);
    try std.testing.expectEqual(@as(?usize, 8), parsed.value.max_loaded_models);
    try std.testing.expectEqual(@as(?usize, 4), parsed.value.pool_size);
}
