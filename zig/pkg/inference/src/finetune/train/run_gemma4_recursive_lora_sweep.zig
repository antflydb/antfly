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
const inference = @import("inference_internal");
const platform = @import("antfly_platform");
const compat = inference.io.compat;
const c_file = inference.util.c_file;

const run_gemma4_lora_pilot_workflow = @import("run_gemma4_lora_pilot_workflow.zig");
const run_gemma4_recursive_lora_smoke_workflow = @import("run_gemma4_recursive_lora_smoke_workflow.zig");

const CommandMain = *const fn (std.process.Init) anyerror!void;

const Options = struct {
    base_model_dir: []const u8,
    output_root: []const u8,
    dataset_path: ?[]const u8 = null,
    count: []const u8 = "16",
    max_examples: ?[]const u8 = null,
    eval_max_examples: ?[]const u8 = null,
    max_seq_len: []const u8 = "256",
    epochs: []const u8 = "1",
    learning_rate: []const u8 = "0.0003",
    ranks_csv: []const u8 = "4,8",
    target_modules: []const u8 = "q_proj,k_proj,v_proj,o_proj",
    shared_block_sizes_csv: []const u8 = "5",
    teacher_temperatures_csv: []const u8 = "1.0,2.0",
    teacher_top_k: []const u8 = "8",
    recursive_init: []const u8 = "average_residual_svd",
    backend: []const u8 = "mlx",
    split: []const u8 = "train",
    dry_run: bool = false,
};

const Row = struct {
    kind: []const u8,
    path: []const u8,
    rank: ?usize = null,
    shared_block_size: ?usize = null,
    teacher_temperature: ?f64 = null,
    before_average_loss: f64 = 0,
    after_average_loss: f64 = 0,
    last_epoch_average_loss: f64 = 0,
    teacher_supervised_tokens: f64 = 0,
    trained_adapter_bytes: u64 = 0,
    compressed_base_checkpoint_bytes: ?u64 = null,
    compressed_base_ratio: ?f64 = null,
    train_supervised_tokens_per_second: ?f64 = null,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var opts = try parseOptions(&args);
    const invocation_cwd = invocationCwd();
    opts.base_model_dir = try resolveCliPath(allocator, invocation_cwd, opts.base_model_dir);
    defer allocator.free(opts.base_model_dir);
    opts.output_root = try resolveCliPath(allocator, invocation_cwd, opts.output_root);
    defer allocator.free(opts.output_root);
    var owned_dataset_path: ?[]const u8 = null;
    defer if (owned_dataset_path) |value| allocator.free(value);
    if (opts.dataset_path) |value| {
        owned_dataset_path = try resolveCliPath(allocator, invocation_cwd, value);
        opts.dataset_path = owned_dataset_path;
    }

    try runSweep(init, allocator, opts);
}

fn runSweep(init: std.process.Init, allocator: std.mem.Allocator, opts: Options) !void {
    const max_examples = opts.max_examples orelse opts.count;
    const eval_max_examples = opts.eval_max_examples orelse blk: {
        const parsed = try std.fmt.parseUnsigned(usize, max_examples, 10);
        break :blk if (parsed < 8) max_examples else "8";
    };
    const dataset_path = if (opts.dataset_path) |path|
        try allocator.dupe(u8, path)
    else
        try std.fs.path.join(allocator, &.{ opts.output_root, "text_pilot.jsonl" });
    defer allocator.free(dataset_path);
    const ranks = try parseCsv(allocator, opts.ranks_csv);
    defer allocator.free(ranks);
    const shared_block_sizes = try parseCsv(allocator, opts.shared_block_sizes_csv);
    defer allocator.free(shared_block_sizes);
    const teacher_temperatures = try parseCsv(allocator, opts.teacher_temperatures_csv);
    defer allocator.free(teacher_temperatures);

    try compat.cwd().createDirPath(compat.io(), opts.output_root);

    var baseline_dirs = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (baseline_dirs.items) |path| allocator.free(path);
        baseline_dirs.deinit(allocator);
    }
    var variant_dirs = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (variant_dirs.items) |path| allocator.free(path);
        variant_dirs.deinit(allocator);
    }

    for (ranks) |rank| {
        const baseline_dir = try std.fmt.allocPrint(allocator, "{s}/baseline_rank{s}", .{ opts.output_root, rank });
        try baselineDirsAppend(allocator, &baseline_dirs, baseline_dir);
        var baseline_cmd = std.ArrayListUnmanaged([]const u8).empty;
        defer baseline_cmd.deinit(allocator);
        const alpha = try alphaForRank(allocator, rank);
        defer allocator.free(alpha);
        try baseline_cmd.appendSlice(allocator, &.{
            "text",
            opts.base_model_dir,
            baseline_dir,
            "--dataset",
            dataset_path,
            "--count",
            opts.count,
            "--max-examples",
            max_examples,
            "--eval-max-examples",
            eval_max_examples,
            "--max-seq-len",
            opts.max_seq_len,
            "--epochs",
            opts.epochs,
            "--lr",
            opts.learning_rate,
            "--rank",
            rank,
            "--alpha",
            alpha,
            "--target-modules",
            opts.target_modules,
            "--backend",
            opts.backend,
            "--split",
            opts.split,
        });
        if (opts.dry_run) try baseline_cmd.append(allocator, "--dry-run");
        try runCommand(init, allocator, "run-gemma4-lora-pilot-workflow", run_gemma4_lora_pilot_workflow.main, baseline_cmd.items, false);

        for (shared_block_sizes) |shared_block_size| {
            for (teacher_temperatures) |teacher_temperature| {
                const variant_dir = try std.fmt.allocPrint(allocator, "{s}/recursive_rank{s}_share{s}_temp{s}", .{ opts.output_root, rank, shared_block_size, teacher_temperature });
                try baselineDirsAppend(allocator, &variant_dirs, variant_dir);
                var recursive_cmd = std.ArrayListUnmanaged([]const u8).empty;
                defer recursive_cmd.deinit(allocator);
                try recursive_cmd.appendSlice(allocator, &.{
                    opts.base_model_dir,
                    variant_dir,
                    "--dataset",
                    dataset_path,
                    "--count",
                    opts.count,
                    "--max-examples",
                    max_examples,
                    "--eval-max-examples",
                    eval_max_examples,
                    "--max-seq-len",
                    opts.max_seq_len,
                    "--epochs",
                    opts.epochs,
                    "--lr",
                    opts.learning_rate,
                    "--rank",
                    rank,
                    "--alpha",
                    alpha,
                    "--target-modules",
                    opts.target_modules,
                    "--recursive-shared-block-size",
                    shared_block_size,
                    "--recursive-init",
                    opts.recursive_init,
                    "--teacher-top-k",
                    opts.teacher_top_k,
                    "--teacher-temperature",
                    teacher_temperature,
                    "--backend",
                    opts.backend,
                    "--split",
                    opts.split,
                });
                if (opts.dry_run) try recursive_cmd.append(allocator, "--dry-run");
                try runCommand(init, allocator, "run-gemma4-recursive-lora-smoke-workflow", run_gemma4_recursive_lora_smoke_workflow.main, recursive_cmd.items, false);
            }
        }
    }

    const comparison_path = try std.fs.path.join(allocator, &.{ opts.output_root, "recursive_lora_sweep_comparison.json" });
    defer allocator.free(comparison_path);
    if (!opts.dry_run) try writeComparison(allocator, comparison_path, opts.backend, baseline_dirs.items, variant_dirs.items);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try writer.interface.print(
        \\recursive_sweep_complete: true
        \\base_model_dir: {s}
        \\output_root: {s}
        \\dataset: {s}
        \\comparison: {s}
        \\ranks: {s}
        \\shared_block_sizes: {s}
        \\teacher_temperatures: {s}
        \\
    , .{ opts.base_model_dir, opts.output_root, dataset_path, comparison_path, opts.ranks_csv, opts.shared_block_sizes_csv, opts.teacher_temperatures_csv });
    try writer.interface.flush();
}

fn writeComparison(
    allocator: std.mem.Allocator,
    comparison_path: []const u8,
    backend: []const u8,
    baseline_dirs: []const []const u8,
    variant_dirs: []const []const u8,
) !void {
    var rows = std.ArrayListUnmanaged(Row).empty;
    defer rows.deinit(allocator);
    for (baseline_dirs) |path| try rows.append(allocator, try baselineRow(allocator, backend, path));
    for (variant_dirs) |path| try rows.append(allocator, try recursiveRow(allocator, path));

    var best_after_loss: ?Row = null;
    var smallest_trained_adapter: ?Row = null;
    for (rows.items) |row| {
        if (best_after_loss == null or row.after_average_loss < best_after_loss.?.after_average_loss) best_after_loss = row;
        if (smallest_trained_adapter == null or row.trained_adapter_bytes < smallest_trained_adapter.?.trained_adapter_bytes) smallest_trained_adapter = row;
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try std.json.Stringify.value(.{
        .task = "gemma4_recursive_lora_sweep_comparison",
        .backend = backend,
        .rows = rows.items,
        .best_after_loss = best_after_loss,
        .smallest_trained_adapter = smallest_trained_adapter,
    }, .{ .whitespace = .indent_2 }, &out.writer);
    try out.writer.writeByte('\n');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = comparison_path, .data = out.written() });
}

fn baselineRow(allocator: std.mem.Allocator, backend: []const u8, path: []const u8) !Row {
    const train_out_name = try std.fmt.allocPrint(allocator, "train_out_{s}", .{backend});
    defer allocator.free(train_out_name);
    const report_path = try std.fs.path.join(allocator, &.{ path, train_out_name, "train_eval_report.json" });
    defer allocator.free(report_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ path, train_out_name, "adapter_model.safetensors" });
    defer allocator.free(adapter_path);
    var report = try loadJson(allocator, report_path);
    defer report.deinit();
    const epochs = getArray(getObjectField(&report.value, "epoch_history"));
    var teacher_tokens: f64 = 0;
    for (epochs) |epoch| teacher_tokens += numberField(epoch, "teacher_supervised_tokens_seen", 0);
    const last_epoch: ?std.json.Value = if (epochs.len > 0) epochs[epochs.len - 1] else null;
    return .{
        .kind = "baseline",
        .path = path,
        .rank = inferRank(path),
        .before_average_loss = numberFieldOpt(getObjectField(&report.value, "before"), "average_loss", 0),
        .after_average_loss = numberFieldOpt(getObjectField(&report.value, "after"), "average_loss", 0),
        .last_epoch_average_loss = if (last_epoch) |epoch| numberField(epoch, "average_loss", 0) else 0,
        .teacher_supervised_tokens = teacher_tokens,
        .trained_adapter_bytes = fileSize(allocator, adapter_path),
    };
}

fn recursiveRow(allocator: std.mem.Allocator, path: []const u8) !Row {
    const result_path = try std.fs.path.join(allocator, &.{ path, "recursive_smoke_results.json" });
    defer allocator.free(result_path);
    var result = try loadJson(allocator, result_path);
    defer result.deinit();
    const recursive = getObjectField(&result.value, "recursive_lora");
    const teacher = getObjectField(&result.value, "teacher");
    const loss = getObjectField(&result.value, "loss");
    const sizes = getObjectField(&result.value, "sizes");
    const throughput = getObjectField(&result.value, "throughput");
    return .{
        .kind = "recursive",
        .path = path,
        .rank = inferRank(path),
        .shared_block_size = numberFieldUsizeOpt(recursive, "shared_block_size"),
        .teacher_temperature = numberFieldF64Opt(teacher, "temperature"),
        .before_average_loss = numberFieldOpt(loss, "before_average_loss", 0),
        .after_average_loss = numberFieldOpt(loss, "after_average_loss", 0),
        .last_epoch_average_loss = numberFieldOpt(loss, "last_epoch_average_loss", 0),
        .teacher_supervised_tokens = numberFieldOpt(teacher, "supervised_tokens_seen", 0),
        .trained_adapter_bytes = numberFieldU64Opt(sizes, "trained_adapter_bytes") orelse 0,
        .compressed_base_checkpoint_bytes = numberFieldU64Opt(sizes, "compressed_base_checkpoint_bytes"),
        .compressed_base_ratio = numberFieldF64Opt(sizes, "compressed_base_ratio"),
        .train_supervised_tokens_per_second = numberFieldF64Opt(throughput, "train_supervised_tokens_per_second"),
    };
}

fn parseOptions(args: *std.process.Args.Iterator) !Options {
    var opts = Options{
        .base_model_dir = args.next() orelse return usageError(),
        .output_root = args.next() orelse return usageError(),
    };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dataset")) {
            opts.dataset_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--count")) {
            opts.count = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            opts.max_examples = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--eval-max-examples")) {
            opts.eval_max_examples = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-seq-len")) {
            opts.max_seq_len = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            opts.epochs = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--lr") or std.mem.eql(u8, arg, "--learning-rate")) {
            opts.learning_rate = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--ranks")) {
            opts.ranks_csv = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--target-modules")) {
            opts.target_modules = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--shared-block-sizes")) {
            opts.shared_block_sizes_csv = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--teacher-temperatures")) {
            opts.teacher_temperatures_csv = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--teacher-top-k")) {
            opts.teacher_top_k = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--recursive-init")) {
            opts.recursive_init = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--split")) {
            opts.split = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            opts.dry_run = true;
        } else {
            return usageError();
        }
    }
    return opts;
}

fn baselineDirsAppend(allocator: std.mem.Allocator, dirs: *std.ArrayListUnmanaged([]const u8), path: []const u8) !void {
    errdefer allocator.free(path);
    try dirs.append(allocator, path);
}

fn parseCsv(allocator: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    errdefer parts.deinit(allocator);
    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == 0) continue;
        try parts.append(allocator, trimmed);
    }
    return parts.toOwnedSlice(allocator);
}

fn alphaForRank(allocator: std.mem.Allocator, rank: []const u8) ![]const u8 {
    const value = try std.fmt.parseUnsigned(usize, rank, 10);
    return std.fmt.allocPrint(allocator, "{d}", .{value * 2});
}

fn runCommand(init: std.process.Init, allocator: std.mem.Allocator, argv0: []const u8, main_fn: CommandMain, args: []const []const u8, dry_run: bool) !void {
    printCommand(argv0, args);
    if (dry_run) return;

    var owned = try allocator.alloc([:0]u8, args.len + 1);
    defer {
        for (owned) |arg| allocator.free(arg);
        allocator.free(owned);
    }
    var vector = try allocator.alloc([*:0]const u8, args.len + 1);
    defer allocator.free(vector);

    owned[0] = try allocator.dupeSentinel(u8, argv0, 0);
    vector[0] = owned[0].ptr;
    for (args, 0..) |arg, idx| {
        owned[idx + 1] = try allocator.dupeSentinel(u8, arg, 0);
        vector[idx + 1] = owned[idx + 1].ptr;
    }

    var command_init = init;
    command_init.minimal.args = .{ .vector = vector };
    try main_fn(command_init);
}

fn printCommand(argv0: []const u8, args: []const []const u8) void {
    std.debug.print("+ {s}", .{argv0});
    for (args) |arg| std.debug.print(" {s}", .{arg});
    std.debug.print("\n", .{});
}

fn loadJson(allocator: std.mem.Allocator, path: []const u8) !std.json.Parsed(std.json.Value) {
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    return std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
}

fn getObjectField(value: *const std.json.Value, key: []const u8) ?std.json.Value {
    return switch (value.*) {
        .object => |object| object.get(key),
        else => null,
    };
}

fn getArray(value: ?std.json.Value) []const std.json.Value {
    const v = value orelse return &.{};
    return switch (v) {
        .array => |items| items.items,
        else => &.{},
    };
}

fn numberFieldOpt(value: ?std.json.Value, key: []const u8, default: f64) f64 {
    const root = value orelse return default;
    return numberField(root, key, default);
}

fn numberField(value: std.json.Value, key: []const u8, default: f64) f64 {
    const field = getObjectField(&value, key) orelse return default;
    return numberValue(field, default);
}

fn numberFieldF64Opt(value: ?std.json.Value, key: []const u8) ?f64 {
    const root = value orelse return null;
    const field = getObjectField(&root, key) orelse return null;
    return numberValue(field, 0);
}

fn numberFieldUsizeOpt(value: ?std.json.Value, key: []const u8) ?usize {
    const raw = numberFieldF64Opt(value, key) orelse return null;
    return @intFromFloat(raw);
}

fn numberFieldU64Opt(value: ?std.json.Value, key: []const u8) ?u64 {
    const raw = numberFieldF64Opt(value, key) orelse return null;
    return @intFromFloat(raw);
}

fn numberValue(value: std.json.Value, default: f64) f64 {
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| v,
        else => default,
    };
}

fn fileSize(allocator: std.mem.Allocator, path: []const u8) u64 {
    return c_file.fileSize(allocator, path) catch 0;
}

fn inferRank(path: []const u8) ?usize {
    const base = std.fs.path.basename(path);
    const marker = "rank";
    const marker_idx = std.mem.indexOf(u8, base, marker) orelse return null;
    const tail = base[marker_idx + marker.len ..];
    var end: usize = 0;
    while (end < tail.len and std.ascii.isDigit(tail[end])) : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseUnsigned(usize, tail[0..end], 10) catch null;
}

fn invocationCwd() ?[]const u8 {
    return platform.env.getenv("ANTFLY_WORKFLOW_CWD");
}

fn resolveCliPath(allocator: std.mem.Allocator, invocation_cwd: ?[]const u8, path: []const u8) ![]const u8 {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return allocator.dupe(u8, path);
    const cwd = invocation_cwd orelse return allocator.dupe(u8, path);
    return std.fs.path.join(allocator, &.{ cwd, path });
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: run-gemma4-recursive-lora-sweep <base_model_dir> <output_root> [options]
        \\
        \\Options:
        \\  --dataset PATH
        \\  --count N
        \\  --max-examples N
        \\  --eval-max-examples N
        \\  --max-seq-len N
        \\  --epochs N
        \\  --lr FLOAT
        \\  --ranks CSV
        \\  --target-modules CSV
        \\  --shared-block-sizes CSV
        \\  --teacher-temperatures CSV
        \\  --teacher-top-k N
        \\  --recursive-init NAME
        \\  --backend auto|mlx|native
        \\  --split NAME
        \\  --dry-run
        \\
    , .{});
    return error.InvalidArguments;
}
