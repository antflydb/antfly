// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const common_config = @import("../common/config.zig");
const inference = @import("inference_server");

pub const ServerBudgetOverrides = inference.server.BudgetOverrides;

/// Returns ~/.antfly/inference/models if $HOME is set, otherwise falls back to ./models.
pub fn defaultModelsDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./models";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "models" }) catch "./models";
}

pub const SpawnedServer = struct {
    base_uri: []u8,
    thread: std.Thread,
    node: *inference.server.Node,
    host: []u8,

    pub fn deinit(self: *SpawnedServer, alloc: std.mem.Allocator, _: std.Io) void {
        // The serve loop runs until the process exits; detach so we don't
        // block shutdown waiting for it.
        self.thread.detach();
        alloc.free(self.base_uri);
        // The embedded server thread owns the running node for the rest of the
        // process lifetime. Freeing it here would race the detached serve loop.
        self.* = undefined;
    }
};

const EmbeddedServerConfig = struct {
    api_url: []const u8,
    models_dir: ?[]const u8 = null,
    content_security: ?common_config.Config.ContentSecurityConfig = null,
    s3_credentials: ?common_config.Config.S3CredentialsConfig = null,
    generation_budget_overrides: ServerBudgetOverrides = .{},
};

const BudgetOverridesMb = struct {
    host_budget_mb: usize = 0,
    backend_budget_mb: usize = 0,
    combined_budget_mb: usize = 0,
    kv_budget_mb: usize = 0,
    scratch_budget_mb: usize = 0,
};

pub fn run(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly inference";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(
    init: std.process.Init,
    _: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    const alloc = init.gpa;
    const io = init.io;

    const command = args.next() orelse "run";

    if (std.mem.eql(u8, command, "run")) {
        return try runServer(alloc, io, args);
    } else if (std.mem.eql(u8, command, "embed")) {
        return try inference.native_embed.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "classify")) {
        return try inference.native_classify.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "generate")) {
        return try inference.native_generate.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "compile-artifact")) {
        return try inference.native_compile.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "export")) {
        return try inference.native_export.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "quantize")) {
        return try inference.native_quantize.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "run-artifact")) {
        return try inference.native_run_artifact.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "transcribe")) {
        return try inference.native_transcribe.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "read")) {
        return try inference.native_read.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "recognize")) {
        return try inference.native_recognize.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "extract")) {
        return try inference.native_extract.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "compare")) {
        return try inference.compare_generate.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "finetune")) {
        return try inference.finetune_cli.main(init, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "smoke")) {
        return try inference.native_smoke.main(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "list")) {
        return try listModels(alloc, io, args);
    } else if (std.mem.eql(u8, command, "pull")) {
        return try pullModel(alloc, io, args);
    } else if (std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h") or std.mem.eql(u8, command, "help")) {
        printUsage();
    } else {
        std.debug.print("unknown command: {s}\n", .{command});
        printUsage();
        return error.InvalidArguments;
    }
}

fn runServer(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    var host: []const u8 = "127.0.0.1";
    var port: u16 = 8090;
    var models_dir: []const u8 = defaultModelsDir(alloc);
    var budget_overrides_mb = BudgetOverridesMb{};

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--host")) {
            host = args.next() orelse host;
        } else if (std.mem.eql(u8, arg, "--port")) {
            if (args.next()) |p| port = std.fmt.parseInt(u16, p, 10) catch 8090;
        } else if (std.mem.eql(u8, arg, "--models-dir")) {
            models_dir = args.next() orelse models_dir;
        } else if (std.mem.eql(u8, arg, "--host-budget-mb")) {
            budget_overrides_mb.host_budget_mb = try parseBudgetMbArg(args);
        } else if (std.mem.eql(u8, arg, "--backend-budget-mb")) {
            budget_overrides_mb.backend_budget_mb = try parseBudgetMbArg(args);
        } else if (std.mem.eql(u8, arg, "--combined-budget-mb")) {
            budget_overrides_mb.combined_budget_mb = try parseBudgetMbArg(args);
        } else if (std.mem.eql(u8, arg, "--kv-budget-mb")) {
            budget_overrides_mb.kv_budget_mb = try parseBudgetMbArg(args);
        } else if (std.mem.eql(u8, arg, "--scratch-budget-mb")) {
            budget_overrides_mb.scratch_budget_mb = try parseBudgetMbArg(args);
        }
    }

    std.debug.print("antfly inference\n", .{});
    std.debug.print("models: {s}\n", .{models_dir});
    std.debug.print("listening on {s}:{d}\n", .{ host, port });

    var node = try inference.server.Node.init(alloc, .{
        .models_dir = models_dir,
        .generation_budget_overrides = budgetOverridesFromMb(budget_overrides_mb),
    });
    defer node.deinit();

    try node.serve(alloc, io, host, port);
}

pub fn spawnServerProcess(
    alloc: std.mem.Allocator,
    _: std.Io,
    _: []const u8,
    base_uri: []const u8,
    config: EmbeddedServerConfig,
) !SpawnedServer {
    const parsed = try parseHostPort(base_uri);

    var node_cfg = inference.server.NodeConfig{
        .models_dir = config.models_dir orelse defaultModelsDir(alloc),
        .generation_budget_overrides = config.generation_budget_overrides,
    };
    if (config.content_security) |sec| node_cfg.content_security = sec;
    if (config.s3_credentials) |creds| node_cfg.s3_credentials = creds;

    const node = try alloc.create(inference.server.Node);
    errdefer alloc.destroy(node);
    node.* = try inference.server.Node.init(alloc, node_cfg);
    errdefer node.deinit();

    const host_dup = try alloc.dupe(u8, parsed.host);
    errdefer alloc.free(host_dup);

    const thread = try std.Thread.spawn(.{}, serveThread, .{ node, alloc, host_dup, parsed.port });

    return .{
        .base_uri = try alloc.dupe(u8, base_uri),
        .thread = thread,
        .node = node,
        .host = host_dup,
    };
}

fn serveThread(node: *inference.server.Node, alloc: std.mem.Allocator, host: []const u8, port: u16) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    node.serve(alloc, io_impl.io(), host, port) catch |err| {
        std.debug.print("inference server error: {}\n", .{err});
    };
}

fn listModels(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    var models_dir: []const u8 = defaultModelsDir(alloc);
    if (args.next()) |arg| {
        if (!std.mem.startsWith(u8, arg, "--")) models_dir = arg;
    }

    var reg = inference.registry.ModelRegistry.init(alloc, models_dir);
    defer reg.deinit();

    const models = try reg.discover(io);
    defer alloc.free(models);

    for (models) |m| {
        std.debug.print("{s:<12} {s}\n", .{ @tagName(m.kind), m.name });
    }
}

fn pullModel(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const ref = args.next() orelse {
        std.debug.print("usage: antfly inference pull <model-ref> [--token <hf-token>] [--models-dir <dir>] [--tasks <csv>] [--capabilities <csv>]\n", .{});
        return;
    };

    var token: ?[]const u8 = null;
    var models_dir: []const u8 = defaultModelsDir(alloc);
    var tasks_csv: ?[]const u8 = null;
    var capabilities_csv: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--token")) {
            token = args.next();
        } else if (std.mem.eql(u8, arg, "--models-dir")) {
            models_dir = args.next() orelse models_dir;
        } else if (std.mem.eql(u8, arg, "--tasks")) {
            tasks_csv = args.next();
        } else if (std.mem.eql(u8, arg, "--capabilities")) {
            capabilities_csv = args.next();
        }
    }

    // Also check HF_TOKEN env var
    if (token == null) {
        token = platform.env.getenv("HF_TOKEN");
    }

    std.debug.print("pulling {s}...\n", .{ref});

    var reg = inference.registry.ModelRegistry.init(alloc, models_dir);
    defer reg.deinit();
    try reg.pull(io, ref, token, tasks_csv, capabilities_csv);

    std.debug.print("done.\n", .{});
}

fn collectArgs(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) ![]const []const u8 {
    var list = std.ArrayListUnmanaged([]const u8).empty;
    while (args.next()) |arg| try list.append(alloc, arg);
    return list.toOwnedSlice(alloc);
}

fn parseBudgetMbArg(args: *std.process.Args.Iterator) !usize {
    return std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
}

fn budgetOverridesFromMb(overrides: BudgetOverridesMb) ServerBudgetOverrides {
    return .{
        .host_limit_bytes = mbToBytes(overrides.host_budget_mb),
        .backend_limit_bytes = mbToBytes(overrides.backend_budget_mb),
        .combined_limit_bytes = mbToBytes(overrides.combined_budget_mb),
        .kv_limit_bytes = mbToBytes(overrides.kv_budget_mb),
        .scratch_limit_bytes = mbToBytes(overrides.scratch_budget_mb),
    };
}

fn mbToBytes(value: usize) usize {
    return value * 1024 * 1024;
}

fn parseHostPort(base_uri: []const u8) !struct { host: []const u8, port: u16 } {
    const scheme_pos = std.mem.indexOf(u8, base_uri, "://") orelse return error.InvalidArguments;
    const host_port = base_uri[scheme_pos + 3 ..];
    const path_pos = std.mem.indexOfScalar(u8, host_port, '/');
    const authority = if (path_pos) |pos| host_port[0..pos] else host_port;
    const colon_pos = std.mem.lastIndexOfScalar(u8, authority, ':') orelse return error.InvalidArguments;
    const host = authority[0..colon_pos];
    const port = try std.fmt.parseInt(u16, authority[colon_pos + 1 ..], 10);
    if (host.len == 0) return error.InvalidArguments;
    return .{ .host = host, .port = port };
}

fn printUsage() void {
    std.debug.print(
        \\usage: antfly inference <command> [options]
        \\
        \\Commands:
        \\  run         Start the inference server (default)
        \\  embed       Run text/image/audio embedding
        \\  classify    Run native text classification
        \\  generate    Run text generation
        \\  compile-artifact Compile traced generation artifacts
        \\  export      Export model data
        \\  quantize    Create a quantized model variant
        \\  run-artifact Run or validate compiled artifacts
        \\  transcribe  Run audio transcription
        \\  read        Run image/document reading
        \\  recognize   Run entity recognition
        \\  extract     Run structured extraction
        \\  compare     Compare generation outputs
        \\  finetune    Run LoRA finetuning
        \\  smoke       Run a model smoke test
        \\  list        List available models
        \\  pull        Download a model from HuggingFace Hub
        \\
        \\Run options:
        \\  --host <addr>    Listen address (default: 127.0.0.1)
        \\  --port <port>    Listen port (default: 8090)
        \\  --models-dir <dir> Models directory (default: ~/.antfly/inference/models)
        \\  --host-budget-mb <n>      Native generation host budget override
        \\  --backend-budget-mb <n>   Native generation backend budget override
        \\  --combined-budget-mb <n>  Native generation combined budget override
        \\  --kv-budget-mb <n>        Native generation KV cache budget override
        \\  --scratch-budget-mb <n>   Native generation scratch budget override
        \\
        \\Pull options:
        \\  --token <token>  HuggingFace API token (or set HF_TOKEN env var)
        \\  --models-dir <dir> Models directory (default: ~/.antfly/inference/models)
        \\
    , .{});
}

test "inference runtime module compiles" {
    _ = run;
    _ = runFromIterator;
    _ = spawnServerProcess;
}
