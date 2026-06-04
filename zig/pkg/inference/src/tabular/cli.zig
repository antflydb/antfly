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

pub fn isHttpUrl(value: []const u8) bool {
    return std.mem.startsWith(u8, value, "http://") or std.mem.startsWith(u8, value, "https://");
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

    const url = args[0];
    if (!isHttpUrl(url)) {
        print("pull: tabular model URL must start with http:// or https://\n", .{});
        return;
    }

    var name: ?[]const u8 = null;
    var token: ?[]const u8 = null;
    var ml_dir: []const u8 = default_ml_dir;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--name") and i + 1 < args.len) {
            name = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--token") and i + 1 < args.len) {
            token = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--ml-dir") and i + 1 < args.len) {
            ml_dir = args[i + 1];
            i += 1;
        } else {
            print("pull: unexpected arg '{s}'\n", .{args[i]});
            printPullUsage();
            return;
        }
    }

    const model_name = name orelse {
        print("pull: --name is required for tabular predictor URLs\n", .{});
        printPullUsage();
        return;
    };

    var auth_header: ?[]const u8 = null;
    defer if (auth_header) |h| alloc.free(h);
    var headers_buf: [1][2][]const u8 = undefined;
    var headers: ?[]const [2][]const u8 = null;
    if (token) |t| {
        auth_header = try std.fmt.allocPrint(alloc, "Bearer {s}", .{t});
        headers_buf[0] = .{ "Authorization", auth_header.? };
        headers = headers_buf[0..1];
    }

    print("pulling {s}...\n", .{url});
    var client = httpx.Client.initWithConfig(alloc, io, .{
        .keep_alive = false,
        .max_response_size = limits.max_model_json_bytes,
    });
    defer client.deinit();

    var resp = client.get(url, .{
        .headers = headers,
        .follow_redirects = true,
        .timeout_ms = 300_000,
    }) catch |err| {
        print("pull: download failed: {s}\n", .{@errorName(err)});
        return;
    };
    defer resp.deinit();

    if (!resp.ok()) {
        print("pull: remote returned HTTP {d}\n", .{resp.status.code});
        return;
    }
    const body = resp.body orelse {
        print("pull: remote response had no body\n", .{});
        return;
    };

    installPulledModel(alloc, io, ml_dir, model_name, body) catch |err| {
        print("pull: {s}\n", .{@errorName(err)});
        return;
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

    const bytes = std.Io.Dir.cwd().readFileAlloc(io, input_path.?, alloc, .limited(limits.max_model_json_bytes)) catch {
        print("convert: cannot read {s}\n", .{input_path.?});
        return;
    };
    defer alloc.free(bytes);

    var result = tabular.convert.convert(alloc, bytes, framework) catch |err| {
        print("convert: {s}\n", .{@errorName(err)});
        return;
    };
    defer result.deinit();

    if (optimize_passes) {
        // Operate directly on the IR's TreeEnsemble pointer — the previous
        // version copied to a stack-local `mut` and dropped the mutations
        // on the floor, making --optimize a silent no-op.
        for (result.model.pipeline) |s| {
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
) PullInstallError!void {
    if (body.len > limits.max_model_json_bytes) return PullInstallError.InvalidModel;
    if (!registry_mod.isSafeName(name)) return PullInstallError.InvalidName;

    var loaded = tabular.loader.parseFromSlice(alloc, body) catch return PullInstallError.InvalidModel;
    defer loaded.deinit();
    loaded.model.metadata.name = name;

    const target_dir = try std.fs.path.join(alloc, &.{ ml_dir, name });
    defer alloc.free(target_dir);
    std.Io.Dir.cwd().createDirPath(io, target_dir) catch return PullInstallError.IoError;

    const json_bytes = std.json.Stringify.valueAlloc(alloc, loaded.model, .{ .whitespace = .indent_2 }) catch return PullInstallError.InvalidModel;
    defer alloc.free(json_bytes);

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

    print("Pulled {s} -> {s} (task: {s}, features: {d}, outputs: {d})\n", .{
        name,
        final_path,
        tabular.ir.taskTypeToString(loaded.model.metadata.task),
        loaded.model.metadata.num_features,
        loaded.model.output.num_outputs,
    });
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
        \\
        \\Download a hosted tabular_model.json and install it as a local predictor.
        \\
        \\Options:
        \\  --name <name>        Local predictor name. Must match [A-Za-z0-9_-]+.
        \\  --ml-dir <dir>     Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --token <token>     Bearer token for the model URL
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
