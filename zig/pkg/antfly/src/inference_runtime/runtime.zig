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

/// Returns ~/.antfly/inference/ml if $HOME is set, otherwise falls back to ./ml.
pub fn defaultMlDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_ML_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./ml";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "ml" }) catch "./ml";
}

pub fn defaultModelsDirForDataDir(allocator: std.mem.Allocator, data_dir: []const u8) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value| return value;
    return std.fs.path.join(allocator, &.{ data_dir, "inference", "models" }) catch defaultModelsDir(allocator);
}

pub fn defaultMlDirForDataDir(allocator: std.mem.Allocator, data_dir: []const u8) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_ML_DIR")) |value| return value;
    return std.fs.path.join(allocator, &.{ data_dir, "inference", "ml" }) catch defaultMlDir(allocator);
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
    allow_unknown_models: bool = false,
    ml_dir: ?[]const u8 = null,
    content_security: ?common_config.Config.ContentSecurityConfig = null,
    s3_credentials: ?common_config.Config.S3CredentialsConfig = null,
    generation_budget_overrides: ServerBudgetOverrides = .{},
    preload: []const inference.server.WarmModel = &.{},
};

const BudgetOverridesMb = struct {
    host_budget_mb: usize = 0,
    backend_budget_mb: usize = 0,
    combined_budget_mb: usize = 0,
    kv_budget_mb: usize = 0,
    scratch_budget_mb: usize = 0,
};

pub fn parseBackendType(value: []const u8) ?inference.backends.BackendType {
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "xla") or std.mem.eql(u8, value, "pjrt")) return .pjrt;
    if (std.mem.eql(u8, value, "wasm") or std.mem.eql(u8, value, "webgpu")) return .wasm;
    return null;
}

pub fn parseOptionalBackendType(value: ?[]const u8) !?inference.backends.BackendType {
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
        inference.native_generate.main(alloc, io, try collectArgs(alloc, args)) catch |err| switch (err) {
            error.WarmInferenceServerUnavailable => {
                std.debug.print(
                    "warm inference server unavailable; start one with `antfly inference run --preload-model generator:<model>` and pass --server\n",
                    .{},
                );
                std.process.exit(1);
            },
            error.UnsupportedServerGenerateOption => {
                std.debug.print("--require-server does not support one of the requested generate options\n", .{});
                std.process.exit(1);
            },
            else => return err,
        };
        return;
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
    } else if (std.mem.eql(u8, command, "convert")) {
        return try inference.tabular.cli.convertMain(alloc, io, try collectArgs(alloc, args));
    } else if (std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h") or std.mem.eql(u8, command, "help")) {
        printUsage();
    } else {
        std.debug.print("unknown command: {s}\n", .{command});
        printUsage();
        return error.InvalidArguments;
    }
}

fn runServer(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    // Help is side-effect free and wins over every run option, even if it
    // follows an otherwise invalid value. Probe a copy so normal parsing can
    // still consume the original iterator.
    var help_probe = args.*;
    var first_arg = true;
    while (help_probe.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h") or
            (first_arg and std.mem.eql(u8, arg, "help")))
        {
            printUsage();
            return;
        }
        first_arg = false;
    }

    var host: []const u8 = "127.0.0.1";
    var port: u16 = 8090;
    var models_dir: []const u8 = defaultModelsDir(alloc);
    var ml_dir: []const u8 = defaultMlDir(alloc);
    var max_loaded_models: usize = 10;
    var budget_overrides_mb = BudgetOverridesMb{};
    var allow_unknown_models = false;
    var preload_models = std.ArrayListUnmanaged(inference.server.WarmModel).empty;
    defer preload_models.deinit(alloc);

    while (args.next()) |arg| {
        if (isHelpArg(arg)) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, arg, "--host")) {
            host = args.next() orelse host;
        } else if (std.mem.eql(u8, arg, "--port")) {
            if (args.next()) |p| port = std.fmt.parseInt(u16, p, 10) catch 8090;
        } else if (std.mem.eql(u8, arg, "--models-dir")) {
            models_dir = args.next() orelse models_dir;
        } else if (std.mem.eql(u8, arg, "--ml-dir")) {
            ml_dir = args.next() orelse ml_dir;
        } else if (std.mem.eql(u8, arg, "--max-loaded-models")) {
            max_loaded_models = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.InvalidArguments,
                10,
            );
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
        } else if (std.mem.eql(u8, arg, "--preload-model")) {
            try preload_models.append(alloc, try parsePreloadModelFlag(args.next() orelse return error.InvalidArguments));
        } else if (std.mem.eql(u8, arg, "--allow-unknown-models")) {
            allow_unknown_models = true;
        }
    }

    std.debug.print("antfly inference\n", .{});
    std.debug.print("ai models: {s}\n", .{models_dir});
    std.debug.print("ml models: {s}\n", .{ml_dir});

    var node = try inference.server.Node.init(alloc, .{
        .models_dir = models_dir,
        .ml_dir = ml_dir,
        .max_loaded_models = max_loaded_models,
        .generation_budget_overrides = budgetOverridesFromMb(budget_overrides_mb),
        .preload = preload_models.items,
        .allow_unknown_models = allow_unknown_models,
    });
    defer node.deinit();

    // Bind the caller-owned runtime before warmup so model loading, tokenizer
    // work, and backend sessions all compose with the same executor.
    node.attachIo(io);
    try node.warmConfiguredGenerators(alloc);
    node.configureForcedRunAdmissionDenialsFromEnvironmentForTesting();
    std.debug.print("listening on {s}:{d}\n", .{ host, port });
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
        .ml_dir = config.ml_dir orelse defaultMlDir(alloc),
        .generation_budget_overrides = config.generation_budget_overrides,
        .preload = config.preload,
        .allow_unknown_models = config.allow_unknown_models,
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
    node.attachIo(io_impl.io());
    node.warmConfiguredGenerators(alloc) catch |err| {
        std.debug.print("inference warmup error: {}\n", .{err});
        return;
    };
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
    var argv = std.ArrayListUnmanaged([]const u8).empty;
    defer argv.deinit(alloc);
    while (args.next()) |arg| try argv.append(alloc, arg);
    if (argv.items.len == 0) {
        printPullUsage();
        return;
    }

    var refs = std.ArrayListUnmanaged([]const u8).empty;
    defer refs.deinit(alloc);
    var passthrough = std.ArrayListUnmanaged([]const u8).empty;
    defer passthrough.deinit(alloc);
    var variants_csv: ?[]const u8 = null;
    var token: ?[]const u8 = null;
    var models_dir: []const u8 = defaultModelsDir(alloc);
    var tasks_csv: ?[]const u8 = null;
    var capabilities_csv: ?[]const u8 = null;
    var projector_selection: inference.registry.download.ProjectorSelection = .auto;
    var max_artifact_bytes = inference.registry.download.default_max_artifact_bytes;
    var max_model_bytes = inference.registry.download.default_max_model_bytes;
    var predictor_pull = false;
    var first_ai_only_flag: ?[]const u8 = null;
    var first_predictor_only_flag: ?[]const u8 = null;

    var i: usize = 0;
    while (i < argv.items.len) : (i += 1) {
        const arg = argv.items[i];
        if (isHelpArg(arg)) {
            printPullUsage();
            return;
        }
        if (!std.mem.startsWith(u8, arg, "-")) {
            try refs.append(alloc, arg);
            continue;
        }
        if (std.mem.eql(u8, arg, "--optimize")) {
            if (first_predictor_only_flag == null) first_predictor_only_flag = arg;
            try passthrough.append(alloc, arg);
            continue;
        }
        if (!pullFlagTakesValue(arg)) {
            std.debug.print("unknown inference pull flag: {s}\n", .{arg});
            printPullUsage();
            return error.InvalidArguments;
        }
        i += 1;
        if (i >= argv.items.len) {
            std.debug.print("{s} requires a value\n", .{arg});
            printPullUsage();
            return error.InvalidArguments;
        }
        const value = argv.items[i];
        switch (pullFlagDomain(arg)) {
            .shared => {},
            .ai => if (first_ai_only_flag == null) {
                first_ai_only_flag = arg;
            },
            .predictor => if (first_predictor_only_flag == null) {
                first_predictor_only_flag = arg;
            },
        }
        if (std.mem.eql(u8, arg, "--variants")) {
            variants_csv = value;
        } else if (std.mem.eql(u8, arg, "--token")) {
            token = value;
            try passthrough.appendSlice(alloc, &.{ arg, value });
        } else if (std.mem.eql(u8, arg, "--models-dir")) {
            models_dir = value;
        } else if (std.mem.eql(u8, arg, "--tasks")) {
            tasks_csv = value;
        } else if (std.mem.eql(u8, arg, "--capabilities")) {
            capabilities_csv = value;
        } else if (std.mem.eql(u8, arg, "--projector")) {
            projector_selection = inference.registry.download.parseProjectorSelection(value) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-artifact-bytes")) {
            max_artifact_bytes = try parsePositiveDownloadBytes(value);
        } else if (std.mem.eql(u8, arg, "--max-model-bytes")) {
            max_model_bytes = try parsePositiveDownloadBytes(value);
        } else {
            if (std.mem.eql(u8, arg, "--type") and (std.mem.eql(u8, value, "predictor") or std.mem.eql(u8, value, "predictors"))) predictor_pull = true;
            try passthrough.appendSlice(alloc, &.{ arg, value });
        }
    }

    if (refs.items.len == 0) {
        std.debug.print("inference pull requires at least one model reference\n", .{});
        printPullUsage();
        return error.InvalidArguments;
    }

    const predictor_mode = inference.tabular.cli.isHttpUrl(refs.items[0]) or predictor_pull;
    validatePullFlagDomains(predictor_mode, first_ai_only_flag, first_predictor_only_flag) catch |err| {
        printPullUsage();
        return err;
    };

    if (predictor_mode) {
        if (refs.items.len != 1) return error.InvalidArguments;
        var normalized = std.ArrayListUnmanaged([]const u8).empty;
        defer normalized.deinit(alloc);
        try normalized.append(alloc, refs.items[0]);
        try normalized.appendSlice(alloc, passthrough.items);
        return try inference.tabular.cli.pullMain(alloc, io, normalized.items, defaultMlDir(alloc));
    }

    // Also check HF_TOKEN env var
    if (token == null) {
        token = platform.env.getenv("HF_TOKEN");
    }

    var reg = inference.registry.ModelRegistry.init(alloc, models_dir);
    defer reg.deinit();
    const hub_config = inference.registry.download.HubConfig{
        .token = token,
        .max_artifact_bytes = max_artifact_bytes,
        .max_model_bytes = max_model_bytes,
    };
    for (refs.items) |ref| {
        if (variants_csv) |raw_variants| {
            var variants = std.mem.splitScalar(u8, raw_variants, ',');
            var pulled_any = false;
            while (variants.next()) |raw_variant| {
                const variant = std.mem.trim(u8, raw_variant, " \t\r\n");
                if (variant.len == 0) continue;
                pulled_any = true;
                const qualified_ref = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ ref, variant });
                defer alloc.free(qualified_ref);
                try pullOneModel(&reg, io, qualified_ref, hub_config, tasks_csv, capabilities_csv, projector_selection);
            }
            if (!pulled_any) return error.InvalidArguments;
        } else {
            try pullOneModel(&reg, io, ref, hub_config, tasks_csv, capabilities_csv, projector_selection);
        }
    }
}

fn pullOneModel(
    registry: *inference.registry.ModelRegistry,
    io: std.Io,
    ref: []const u8,
    hub_config: inference.registry.download.HubConfig,
    tasks_csv: ?[]const u8,
    capabilities_csv: ?[]const u8,
    projector_selection: inference.registry.download.ProjectorSelection,
) !void {
    std.debug.print("pulling {s}...\n", .{ref});
    try registry.pull(io, ref, hub_config, tasks_csv, capabilities_csv, projector_selection);
    std.debug.print("done.\n", .{});
}

fn pullFlagTakesValue(arg: []const u8) bool {
    const flags = [_][]const u8{
        "--variants",  "--token",               "--models-dir",      "--ml-dir", "--tasks", "--capabilities",
        "--projector", "--max-artifact-bytes",  "--max-model-bytes", "--type",   "--name",  "--file",
        "--framework", "--dead-leaf-threshold",
    };
    for (flags) |flag| if (std.mem.eql(u8, arg, flag)) return true;
    return false;
}

const PullFlagDomain = enum { shared, ai, predictor };

fn pullFlagDomain(arg: []const u8) PullFlagDomain {
    if (std.mem.eql(u8, arg, "--token")) return .shared;
    const ai_flags = [_][]const u8{
        "--variants", "--models-dir", "--tasks", "--capabilities", "--projector", "--max-artifact-bytes", "--max-model-bytes",
    };
    for (ai_flags) |flag| if (std.mem.eql(u8, arg, flag)) return .ai;
    return .predictor;
}

fn validatePullFlagDomains(
    predictor_mode: bool,
    first_ai_only_flag: ?[]const u8,
    first_predictor_only_flag: ?[]const u8,
) !void {
    if (predictor_mode) {
        if (first_ai_only_flag) |flag| {
            std.debug.print("unexpected arg '{s}': only valid for AI model pulls; use --ml-dir for predictor storage\n", .{flag});
            return error.InvalidArguments;
        }
        return;
    }
    if (first_predictor_only_flag) |flag| {
        std.debug.print("unexpected arg '{s}': only valid for predictor pulls; use --type predictor or an HTTP URL\n", .{flag});
        return error.InvalidArguments;
    }
}

fn isHelpArg(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "help");
}

fn parsePositiveDownloadBytes(value: []const u8) !u64 {
    const parsed = try std.fmt.parseInt(u64, value, 10);
    if (parsed == 0) return error.InvalidArguments;
    return parsed;
}

fn printPullUsage() void {
    std.debug.print("usage: antfly inference pull [--variants <csv>] <model-ref>... [--token <hf-token>] [--models-dir <dir>] [--tasks <csv>] [--capabilities <csv>] [--projector <auto|none|Q8_0|filename>] [--max-artifact-bytes <n>] [--max-model-bytes <n>]\n", .{});
    std.debug.print("       antfly inference pull hf:<owner>/<repo> --type predictor [--name <predictor-name>] [--ml-dir <dir>] [--file <repo-path>] [--framework auto|onnx|xgboost|lightgbm]\n", .{});
    std.debug.print("       antfly inference pull <https-url-to-tabular-artifact> --name <predictor-name> [--ml-dir <dir>] [--token <bearer-token>]\n", .{});
    std.debug.print("variants: <model-ref>:gguf, <model-ref>:gguf:Q4_K, <model-ref>:onnx, <model-ref>:hybrid, <model-ref>:safetensors\n", .{});
    std.debug.print("CLIP/CLAP v0.2 example: antfly inference pull antflydb/clipclap:gguf:Q4_K\n", .{});
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
        \\  extract     Run entity, relation, or structured extraction
        \\  compare     Compare generation outputs
        \\  finetune    Run LoRA finetuning
        \\  smoke       Run a model smoke test
        \\  list        List available models
        \\  pull        Download a HuggingFace model, or pull a hosted tabular_model.json predictor URL
        \\  convert     Convert a native ML model (XGBoost/LightGBM/ONNX) to the antfly tabular IR
        \\
        \\Run options:
        \\  --host <addr>    Listen address (default: 127.0.0.1)
        \\  --port <port>    Listen port (default: 8090)
        \\  --models-dir <dir> AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <dir>     Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --max-loaded-models <n> Maximum resident models; 0 disables the count limit (default: 10)
        \\  --host-budget-mb <n>      Native generation host budget override
        \\  --backend-budget-mb <n>   Native generation backend budget override
        \\  --combined-budget-mb <n>  Native generation combined budget override
        \\  --kv-budget-mb <n>        Native generation KV cache budget override
        \\  --scratch-budget-mb <n>   Native generation scratch budget override
        \\  --preload-model <kind:name|kind:backend:name>  Preload and warm a configured model before serving
        \\  --allow-unknown-models  Permit artifacts whose compatibility cannot be proven; known incompatible models remain blocked
        \\
        \\Pull options:
        \\  --token <token>  HuggingFace API token (or set HF_TOKEN env var)
        \\  --name <name>    Local predictor name when pulling a tabular model URL
        \\  --tasks <list>   Comma-separated task hints for the pulled model
        \\  --capabilities <list> Comma-separated capability hints for the pulled model
        \\  --projector <value> Projector sidecar selection for GGUF pulls: auto, none, quant suffix, or filename
        \\  --max-artifact-bytes <n> Maximum bytes accepted for one model artifact (default: 68719476736)
        \\  --max-model-bytes <n> Maximum aggregate bytes accepted for one pull (default: 137438953472)
        \\  --models-dir <dir> AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <dir>     Traditional ML directory for URL pulls (default: ~/.antfly/inference/ml)
        \\
    , .{});
}

test "inference runtime module compiles" {
    _ = run;
    _ = runFromIterator;
    _ = spawnServerProcess;
}

test "inference pull recognizes help before model resolution" {
    try std.testing.expect(isHelpArg("--help"));
    try std.testing.expect(isHelpArg("-h"));
    try std.testing.expect(isHelpArg("help"));
    try std.testing.expect(!isHelpArg("antflydb/clipclap"));
}

test "inference run recognizes help before server startup" {
    try std.testing.expect(isHelpArg("--help"));
    try std.testing.expect(isHelpArg("-h"));
    try std.testing.expect(isHelpArg("help"));
}

test "inference pull classifies order independent value flags" {
    try std.testing.expect(pullFlagTakesValue("--variants"));
    try std.testing.expect(pullFlagTakesValue("--models-dir"));
    try std.testing.expect(pullFlagTakesValue("--max-model-bytes"));
    try std.testing.expect(pullFlagTakesValue("--framework"));
    try std.testing.expect(!pullFlagTakesValue("--optimize"));
    try std.testing.expect(!pullFlagTakesValue("--unknown"));
    try std.testing.expectEqual(PullFlagDomain.ai, pullFlagDomain("--models-dir"));
    try std.testing.expectEqual(PullFlagDomain.predictor, pullFlagDomain("--ml-dir"));
    try std.testing.expectEqual(PullFlagDomain.shared, pullFlagDomain("--token"));
}

test "inference pull rejects flags from the other model domain" {
    try std.testing.expectError(error.InvalidArguments, validatePullFlagDomains(true, "--models-dir", null));
    try std.testing.expectError(error.InvalidArguments, validatePullFlagDomains(false, null, "--ml-dir"));
    try validatePullFlagDomains(true, null, "--ml-dir");
    try validatePullFlagDomains(false, "--models-dir", null);
}

test "parseBackendType accepts warm generator backends" {
    try std.testing.expectEqual(inference.backends.BackendType.metal, parseBackendType("metal").?);
    try std.testing.expectEqual(inference.backends.BackendType.wasm, parseBackendType("webgpu").?);
    try std.testing.expectEqual(inference.backends.BackendType.pjrt, parseBackendType("xla").?);
    try std.testing.expect(try parseOptionalBackendType("auto") == null);
}
