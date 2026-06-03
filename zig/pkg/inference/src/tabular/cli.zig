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
const tabular = @import("ml_tabular");

const print = std.debug.print;

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

    const bytes = std.Io.Dir.cwd().readFileAlloc(io, input_path.?, alloc, .limited(256 * 1024 * 1024)) catch {
        print("convert: cannot read {s}\n", .{input_path.?});
        return;
    };
    defer alloc.free(bytes);

    var result = tabular.convert.convert(alloc, bytes, framework) catch |err| switch (err) {
        error.UnsupportedSource => {
            print(
                "convert: {s} models are produced by termite-convert. Install with `pip install termite-convert` and run `termite-convert {s} ...` (see docs/TABULAR.md).\n",
                .{ frameworkName(framework), frameworkName(framework) },
            );
            return;
        },
        else => {
            print("convert: {s}\n", .{@errorName(err)});
            return;
        },
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

fn printConvertUsage() void {
    print(
        \\usage: antfly inference convert <model> -o <out_dir> [options]
        \\
        \\Convert a native ML model to the antfly tabular IR.
        \\
        \\Options:
        \\  --framework auto|xgboost|lightgbm|onnx|sklearn|catboost  (default: auto)
        \\  --optimize                                               run dead-leaf + threshold-precision passes
        \\  --dead-leaf-threshold <fraction>                         leaf-value pruning cutoff (default: 0.001)
        \\
        \\Supported in this binary:
        \\  XGBoost JSON, LightGBM text, ONNX-ML (stub).
        \\For sklearn / CatBoost, use `termite-convert` to produce tabular_model.json,
        \\then `antfly inference run` will serve it from <models-dir>/predictors/<name>/.
        \\
    , .{});
}

fn parseFramework(s: []const u8) ?tabular.convert.Framework {
    if (std.mem.eql(u8, s, "auto")) return .auto;
    if (std.mem.eql(u8, s, "xgboost")) return .xgboost;
    if (std.mem.eql(u8, s, "lightgbm")) return .lightgbm;
    if (std.mem.eql(u8, s, "onnx")) return .onnx_ml;
    if (std.mem.eql(u8, s, "sklearn")) return .sklearn;
    if (std.mem.eql(u8, s, "catboost")) return .catboost;
    return null;
}

fn frameworkName(f: tabular.convert.Framework) []const u8 {
    return switch (f) {
        .auto => "auto",
        .xgboost => "xgboost",
        .lightgbm => "lightgbm",
        .onnx_ml => "onnx_ml",
        .sklearn => "sklearn",
        .catboost => "catboost",
    };
}
