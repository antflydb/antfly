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

//! `antfly inference convert` and `antfly inference predict` subcommands.
//!
//! Wired into pkg/inference's CLI dispatch (`main.zig` / `inference.zig`).
//! The functions here take pre-parsed argv slices and return a process exit
//! code, so they're driveable from tests too.

const std = @import("std");
const httpx = @import("httpx");
const tabular = @import("ml_tabular");
const hub_download = @import("../registry/download.zig");
const limits = @import("limits.zig");
const registry_mod = @import("registry.zig");

const print = std.debug.print;
var tmp_counter = std.atomic.Value(u64).init(0);

const PullInstallError = error{
    InvalidName,
    InvalidModel,
    IoError,
    OutOfMemory,
};

const HfPullError = error{
    InvalidModelRef,
    NoSupportedArtifact,
    AmbiguousArtifact,
    UnsupportedArtifact,
    ArtifactNotFound,
    ChecksumMismatch,
} || PullInstallError || anyerror;

const InstallSource = union(enum) {
    ir,
    framework: tabular.convert.Framework,
};

const OptimizeOptions = struct {
    enabled: bool = false,
    dead_leaf_threshold: f64 = 1e-3,
};

const PullOptions = struct {
    name: ?[]const u8 = null,
    token: ?[]const u8 = null,
    ml_dir: []const u8,
    type_name: ?[]const u8 = null,
    file: ?[]const u8 = null,
    framework: ?tabular.convert.Framework = null,
    optimize: OptimizeOptions = .{},
};

const HfRef = struct {
    owner: []const u8,
    name: []const u8,
};

const HfArtifact = struct {
    path: []const u8,
    source: InstallSource,
    sha256: ?[]const u8 = null,
};

pub fn isHttpUrl(value: []const u8) bool {
    return std.mem.startsWith(u8, value, "http://") or std.mem.startsWith(u8, value, "https://");
}

pub fn isHuggingFaceRef(value: []const u8) bool {
    return std.mem.startsWith(u8, value, "hf:");
}

pub fn pullMain(
    alloc: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
    default_ml_dir: []const u8,
) !void {
    if (args.len == 0 or std.mem.eql(u8, args[0], "--help") or std.mem.eql(u8, args[0], "-h")) {
        printPullUsage();
        return;
    }

    const ref = args[0];
    if (!isHttpUrl(ref) and !isHuggingFaceRef(ref)) {
        print("pull: tabular model source must be an http(s) URL or hf:<owner>/<repo>\n", .{});
        exitPullFailure();
    }

    var opts: PullOptions = .{ .ml_dir = default_ml_dir };

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--name") and i + 1 < args.len) {
            opts.name = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--token") and i + 1 < args.len) {
            opts.token = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--ml-dir") and i + 1 < args.len) {
            opts.ml_dir = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--type") and i + 1 < args.len) {
            opts.type_name = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--file") and i + 1 < args.len) {
            opts.file = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--framework") and i + 1 < args.len) {
            opts.framework = parseFramework(args[i + 1]) orelse {
                print("pull: unknown framework '{s}'\n", .{args[i + 1]});
                exitPullFailure();
            };
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--optimize")) {
            opts.optimize.enabled = true;
        } else if (std.mem.eql(u8, args[i], "--dead-leaf-threshold") and i + 1 < args.len) {
            opts.optimize.dead_leaf_threshold = std.fmt.parseFloat(f64, args[i + 1]) catch {
                print("pull: bad dead-leaf threshold\n", .{});
                exitPullFailure();
            };
            i += 1;
        } else {
            print("pull: unexpected arg '{s}'\n", .{args[i]});
            printPullUsage();
            exitPullFailure();
        }
    }

    if (opts.type_name) |type_name| {
        if (!std.mem.eql(u8, type_name, "predictor") and !std.mem.eql(u8, type_name, "predictors")) {
            print("pull: --type must be predictor for tabular pulls\n", .{});
            exitPullFailure();
        }
    }

    if (isHuggingFaceRef(ref)) {
        pullHuggingFace(alloc, io, ref, opts) catch |err| {
            print("pull: {s}\n", .{@errorName(err)});
            printHfPullAdvice();
            exitPullFailure();
        };
        return;
    }

    const model_name = opts.name orelse {
        print("pull: --name is required for tabular predictor URLs\n", .{});
        printPullUsage();
        exitPullFailure();
    };

    var auth_header: ?[]const u8 = null;
    defer if (auth_header) |h| alloc.free(h);
    var headers_buf: [1][2][]const u8 = undefined;
    var headers: ?[]const [2][]const u8 = null;
    if (opts.token) |t| {
        auth_header = try std.fmt.allocPrint(alloc, "Bearer {s}", .{t});
        headers_buf[0] = .{ "Authorization", auth_header.? };
        headers = headers_buf[0..1];
    }

    const source: InstallSource = if (opts.framework) |framework| .{ .framework = framework } else .ir;

    print("pulling {s}...\n", .{ref});
    var client = httpx.Client.initWithConfig(alloc, io, .{
        .keep_alive = false,
        .max_response_size = maxBytesForSource(source),
    });
    defer client.deinit();

    var resp = client.get(ref, .{
        .headers = headers,
        .follow_redirects = true,
        .timeout_ms = 300_000,
    }) catch |err| {
        print("pull: download failed: {s}\n", .{@errorName(err)});
        exitPullFailure();
    };
    defer resp.deinit();

    if (!resp.ok()) {
        print("pull: remote returned HTTP {d}\n", .{resp.status.code});
        exitPullFailure();
    }
    const body = resp.body orelse {
        print("pull: remote response had no body\n", .{});
        exitPullFailure();
    };

    installPulledModel(alloc, io, opts.ml_dir, model_name, body, source, opts.optimize) catch |err| {
        print("pull: {s}\n", .{@errorName(err)});
        exitPullFailure();
    };
}

pub fn convertMain(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var input_path: ?[]const u8 = null;
    var output_dir: ?[]const u8 = null;
    var framework: tabular.convert.Framework = .auto;
    var optimize_passes = false;
    var dead_leaf_threshold: f64 = 1e-3;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-o") or std.mem.eql(u8, a, "--output")) {
            i += 1;
            if (i >= args.len) {
                print("convert: missing value for -o\n", .{});
                return;
            }
            output_dir = args[i];
        } else if (std.mem.eql(u8, a, "--framework")) {
            i += 1;
            if (i >= args.len) return;
            framework = parseFramework(args[i]) orelse {
                print("convert: unknown framework '{s}'\n", .{args[i]});
                return;
            };
        } else if (std.mem.eql(u8, a, "--optimize")) {
            optimize_passes = true;
        } else if (std.mem.eql(u8, a, "--dead-leaf-threshold")) {
            i += 1;
            if (i >= args.len) return;
            dead_leaf_threshold = std.fmt.parseFloat(f64, args[i]) catch {
                print("convert: bad threshold\n", .{});
                return;
            };
        } else if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            printConvertUsage();
            return;
        } else if (input_path == null) {
            input_path = a;
        } else {
            print("convert: unexpected arg '{s}'\n", .{a});
            return;
        }
    }

    if (input_path == null or output_dir == null) {
        printConvertUsage();
        return;
    }

    const bytes = std.Io.Dir.cwd().readFileAlloc(io, input_path.?, alloc, .limited(limits.max_model_artifact_bytes)) catch {
        print("convert: cannot read {s}\n", .{input_path.?});
        return;
    };
    defer alloc.free(bytes);

    var result = tabular.convert.convert(alloc, bytes, framework) catch |err| {
        print("convert: {s}\n", .{@errorName(err)});
        return;
    };
    defer result.deinit();

    if (optimize_passes) optimizeTabularModel(alloc, &result.model, dead_leaf_threshold);

    std.Io.Dir.cwd().createDirPath(io, output_dir.?) catch {
        print("convert: cannot create {s}\n", .{output_dir.?});
        return;
    };
    const out_path = std.fs.path.join(alloc, &.{ output_dir.?, "tabular_model.json" }) catch return;
    defer alloc.free(out_path);

    const json_bytes = std.json.Stringify.valueAlloc(alloc, result.model, .{ .whitespace = .indent_2 }) catch {
        print("convert: stringify failed\n", .{});
        return;
    };
    defer alloc.free(json_bytes);

    std.Io.Dir.cwd().writeFile(io, .{ .sub_path = out_path, .data = json_bytes }) catch {
        print("convert: cannot write {s}\n", .{out_path});
        return;
    };

    print("Converted {s} -> {s} (framework: {s})\n", .{
        input_path.?,
        out_path,
        frameworkName(result.framework),
    });
}

fn installPulledModel(
    alloc: std.mem.Allocator,
    io: std.Io,
    ml_dir: []const u8,
    name: []const u8,
    body: []const u8,
    source: InstallSource,
    optimize: OptimizeOptions,
) PullInstallError!void {
    if (body.len > maxBytesForSource(source)) return PullInstallError.InvalidModel;
    if (!registry_mod.isSafeName(name)) return PullInstallError.InvalidName;

    const json_bytes = switch (source) {
        .ir => blk: {
            var loaded = tabular.loader.parseFromSlice(alloc, body) catch return PullInstallError.InvalidModel;
            defer loaded.deinit();
            loaded.model.metadata.name = name;
            if (optimize.enabled) optimizeTabularModel(alloc, &loaded.model, optimize.dead_leaf_threshold);
            break :blk std.json.Stringify.valueAlloc(alloc, loaded.model, .{ .whitespace = .indent_2 }) catch return PullInstallError.InvalidModel;
        },
        .framework => |framework| blk: {
            var result = tabular.convert.convert(alloc, body, framework) catch return PullInstallError.InvalidModel;
            defer result.deinit();
            result.model.metadata.name = name;
            if (optimize.enabled) optimizeTabularModel(alloc, &result.model, optimize.dead_leaf_threshold);
            break :blk std.json.Stringify.valueAlloc(alloc, result.model, .{ .whitespace = .indent_2 }) catch return PullInstallError.InvalidModel;
        },
    };
    defer alloc.free(json_bytes);

    const target_dir = try std.fs.path.join(alloc, &.{ ml_dir, name });
    defer alloc.free(target_dir);
    std.Io.Dir.cwd().createDirPath(io, target_dir) catch return PullInstallError.IoError;

    const tmp_path = try std.fmt.allocPrint(
        alloc,
        "{s}/tabular_model.json.{d}.tmp",
        .{ target_dir, tmp_counter.fetchAdd(1, .monotonic) },
    );
    defer alloc.free(tmp_path);
    const final_path = try std.fmt.allocPrint(alloc, "{s}/tabular_model.json", .{target_dir});
    defer alloc.free(final_path);

    std.Io.Dir.cwd().writeFile(io, .{ .sub_path = tmp_path, .data = json_bytes }) catch return PullInstallError.IoError;
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    std.Io.Dir.cwd().rename(tmp_path, std.Io.Dir.cwd(), final_path, io) catch return PullInstallError.IoError;

    print("Pulled {s} -> {s}\n", .{ name, final_path });
}

fn pullHuggingFace(alloc: std.mem.Allocator, io: std.Io, ref: []const u8, opts: PullOptions) HfPullError!void {
    const hf_ref = parseHuggingFaceRef(ref) orelse return HfPullError.InvalidModelRef;
    const model_name = opts.name orelse hf_ref.name;
    if (!registry_mod.isSafeName(model_name)) return HfPullError.InvalidName;

    var token_env: ?[]u8 = null;
    defer if (token_env) |value| alloc.free(value);
    var token = opts.token;
    if (token == null) {
        token_env = try getEnvVarOwned(alloc, "HF_TOKEN");
        token = token_env;
    }

    const base_url_env = try getEnvVarOwned(alloc, "ANTFLY_INFERENCE_HF_BASE_URL");
    defer if (base_url_env) |value| alloc.free(value);
    const base_url = base_url_env orelse "https://huggingface.co";
    const config: hub_download.HubConfig = .{
        .token = token,
        .base_url = base_url,
    };

    const files = try hub_download.listModelFiles(alloc, io, hf_ref.owner, hf_ref.name, config);
    defer {
        for (files) |f| {
            alloc.free(f.name);
            if (f.sha256) |sum| alloc.free(sum);
        }
        alloc.free(files);
    }

    const artifact = try selectHfArtifact(files, opts.file, opts.framework);
    print("pulling hf:{s}/{s}:{s}...\n", .{ hf_ref.owner, hf_ref.name, artifact.path });
    const body = try hub_download.readModelFileAlloc(alloc, io, hf_ref.owner, hf_ref.name, artifact.path, config, maxBytesForSource(artifact.source));
    defer alloc.free(body);
    if (artifact.sha256) |sum| try verifyBytesSha256(body, sum);

    try installPulledModel(alloc, io, opts.ml_dir, model_name, body, artifact.source, opts.optimize);
}

fn getEnvVarOwned(allocator: std.mem.Allocator, comptime name: [:0]const u8) !?[]u8 {
    const value = std.c.getenv(name) orelse return null;
    return try allocator.dupe(u8, std.mem.span(value));
}

fn parseHuggingFaceRef(ref: []const u8) ?HfRef {
    if (!isHuggingFaceRef(ref)) return null;
    const repo = ref["hf:".len..];
    if (repo.len == 0 or std.mem.indexOfScalar(u8, repo, ':') != null) return null;
    const slash = std.mem.indexOfScalar(u8, repo, '/') orelse return null;
    if (slash == 0 or slash + 1 >= repo.len) return null;
    return .{ .owner = repo[0..slash], .name = repo[slash + 1 ..] };
}

fn selectHfArtifact(files: []const hub_download.HubFile, explicit_file: ?[]const u8, framework_hint: ?tabular.convert.Framework) HfPullError!HfArtifact {
    if (explicit_file) |path| {
        for (files) |file| {
            if (std.mem.eql(u8, file.name, path)) {
                const source = classifyHfArtifact(path, framework_hint) orelse return HfPullError.UnsupportedArtifact;
                return .{ .path = file.name, .source = source, .sha256 = file.sha256 };
            }
        }
        return HfPullError.ArtifactNotFound;
    }

    var selected: ?HfArtifact = null;
    var unsupported_only = false;
    for (files) |file| {
        if (std.mem.indexOfScalar(u8, file.name, '/') != null) continue;
        if (std.ascii.eqlIgnoreCase(file.name, "tabular_model.json")) {
            return .{ .path = file.name, .source = .ir, .sha256 = file.sha256 };
        }
    }
    for (files) |file| {
        if (std.mem.indexOfScalar(u8, file.name, '/') != null) continue;
        if (isUnsupportedSerializedModel(file.name)) {
            unsupported_only = true;
            continue;
        }
        const source = classifyHfArtifact(file.name, framework_hint) orelse continue;
        if (selected != null) return HfPullError.AmbiguousArtifact;
        selected = .{ .path = file.name, .source = source, .sha256 = file.sha256 };
    }

    if (selected) |artifact| return artifact;
    if (unsupported_only) return HfPullError.UnsupportedArtifact;
    return HfPullError.NoSupportedArtifact;
}

fn classifyHfArtifact(path: []const u8, framework_hint: ?tabular.convert.Framework) ?InstallSource {
    if (framework_hint) |framework| return .{ .framework = framework };
    const base = basename(path);
    if (std.ascii.eqlIgnoreCase(base, "tabular_model.json")) return .ir;
    if (endsWithIgnoreCase(base, ".onnx")) return .{ .framework = .onnx_ml };
    if (isConventionalXgboostFile(base)) return .{ .framework = .xgboost };
    if (isConventionalLightgbmFile(base)) return .{ .framework = .lightgbm };
    return null;
}

fn isConventionalXgboostFile(base: []const u8) bool {
    return std.ascii.eqlIgnoreCase(base, "model.json") or
        std.ascii.eqlIgnoreCase(base, "xgboost.json") or
        std.ascii.eqlIgnoreCase(base, "xgb_model.json");
}

fn isConventionalLightgbmFile(base: []const u8) bool {
    return std.ascii.eqlIgnoreCase(base, "model.txt") or
        std.ascii.eqlIgnoreCase(base, "lightgbm.txt") or
        std.ascii.eqlIgnoreCase(base, "lgbm_model.txt");
}

fn isUnsupportedSerializedModel(path: []const u8) bool {
    const base = basename(path);
    return endsWithIgnoreCase(base, ".pkl") or
        endsWithIgnoreCase(base, ".pickle") or
        endsWithIgnoreCase(base, ".joblib") or
        endsWithIgnoreCase(base, ".skops");
}

fn basename(path: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, path, '/')) |slash| return path[slash + 1 ..];
    return path;
}

fn endsWithIgnoreCase(value: []const u8, suffix: []const u8) bool {
    if (value.len < suffix.len) return false;
    return std.ascii.eqlIgnoreCase(value[value.len - suffix.len ..], suffix);
}

fn verifyBytesSha256(bytes: []const u8, expected_hex: []const u8) HfPullError!void {
    if (expected_hex.len != 64) return HfPullError.ChecksumMismatch;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(bytes);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual_hex = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual_hex, expected_hex)) return HfPullError.ChecksumMismatch;
}

fn maxBytesForSource(source: InstallSource) usize {
    return switch (source) {
        .ir => limits.max_model_json_bytes,
        .framework => limits.max_model_artifact_bytes,
    };
}

fn optimizeTabularModel(alloc: std.mem.Allocator, model: *tabular.TabularModel, dead_leaf_threshold: f64) void {
    // Operate directly on the IR's TreeEnsemble pointer so --optimize mutates
    // the serialized model rather than a stack-local copy.
    for (model.pipeline) |s| {
        if (s.type == .tree_ensemble) {
            if (s.tree_ensemble) |te_const| {
                const te_mut: *tabular.ir.TreeEnsemble = @constCast(te_const);
                tabular.optimizer.optimizeEnsemble(alloc, te_mut, .{
                    .dead_leaf_threshold_fraction = dead_leaf_threshold,
                }) catch {};
            }
        }
    }
}

fn printHfPullAdvice() void {
    print(
        \\Hugging Face predictor pulls support Antfly tabular_model.json, ONNX-ML .onnx,
        \\XGBoost JSON, and LightGBM text artifacts. Pickle, joblib, cloudpickle,
        \\and skops artifacts are intentionally not loaded by the Zig runtime; export
        \\to ONNX-ML or a native tree format first, or pass --file for an explicit
        \\supported artifact in a nested repository.
        \\
    , .{});
}

fn exitPullFailure() noreturn {
    std.process.exit(1);
}

fn printConvertUsage() void {
    print(
        \\usage: antfly inference convert <model> -o <out_dir> [options]
        \\
        \\Convert a native ML model to the antfly tabular IR.
        \\
        \\Options:
        \\  --framework auto|xgboost|lightgbm|onnx  (default: auto)
        \\  --optimize                                               run dead-leaf + threshold-precision passes
        \\  --dead-leaf-threshold <fraction>                         leaf-value pruning cutoff (default: 0.001)
        \\
        \\Supported in this binary:
        \\  XGBoost JSON, LightGBM text, ONNX-ML.
        \\Models already exported as tabular_model.json can be served from
        \\<ml-dir>/<name>/, or pulled with:
        \\  antfly inference pull <url> --name <name> [--ml-dir <dir>]
        \\
    , .{});
}

fn printPullUsage() void {
    print(
        \\usage: antfly inference pull <url> --name <name> [options]
        \\       antfly inference pull hf:<owner>/<repo> --type predictor [options]
        \\
        \\Download a hosted tabular predictor artifact and install it as a local predictor.
        \\
        \\Options:
        \\  --name <name>                  Local predictor name. Defaults to repo name for hf: pulls.
        \\  --type predictor               Route hf:<owner>/<repo> pulls to Traditional ML storage.
        \\  --ml-dir <dir>                 Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --token <token>                Bearer token for the model URL or Hugging Face.
        \\  --file <path>                  Explicit Hugging Face repo file to download.
        \\  --framework auto|xgboost|lightgbm|onnx
        \\  --optimize                     run dead-leaf + threshold-precision passes after conversion
        \\  --dead-leaf-threshold <value>  leaf-value pruning cutoff (default: 0.001)
        \\
    , .{});
}

fn parseFramework(s: []const u8) ?tabular.convert.Framework {
    if (std.mem.eql(u8, s, "auto")) return .auto;
    if (std.mem.eql(u8, s, "xgboost")) return .xgboost;
    if (std.mem.eql(u8, s, "lightgbm")) return .lightgbm;
    if (std.mem.eql(u8, s, "onnx")) return .onnx_ml;
    return null;
}

fn frameworkName(f: tabular.convert.Framework) []const u8 {
    return switch (f) {
        .auto => "auto",
        .xgboost => "xgboost",
        .lightgbm => "lightgbm",
        .onnx_ml => "onnx_ml",
    };
}

test "tabular hf selection chooses root tabular IR" {
    const files = [_]hub_download.HubFile{
        .{ .name = "tabular_model.json" },
        .{ .name = "model.json" },
        .{ .name = "README.md" },
    };
    const selected = try selectHfArtifact(&files, null, null);
    try std.testing.expectEqualStrings("tabular_model.json", selected.path);
    try std.testing.expect(selected.source == .ir);
}

test "tabular hf selection rejects ambiguous root artifacts" {
    const files = [_]hub_download.HubFile{
        .{ .name = "model.json" },
        .{ .name = "model.onnx" },
    };
    try std.testing.expectError(HfPullError.AmbiguousArtifact, selectHfArtifact(&files, null, null));
}

test "tabular hf selection detects unsupported serialized artifacts" {
    const files = [_]hub_download.HubFile{
        .{ .name = "model.pkl" },
    };
    try std.testing.expectError(HfPullError.UnsupportedArtifact, selectHfArtifact(&files, null, null));
}

test "tabular hf selection supports explicit nested artifact" {
    const files = [_]hub_download.HubFile{
        .{ .name = "models/stump.txt" },
    };
    const selected = try selectHfArtifact(&files, "models/stump.txt", .lightgbm);
    try std.testing.expectEqualStrings("models/stump.txt", selected.path);
    try std.testing.expect(selected.source == .framework);
    try std.testing.expectEqual(tabular.convert.Framework.lightgbm, selected.source.framework);
}
