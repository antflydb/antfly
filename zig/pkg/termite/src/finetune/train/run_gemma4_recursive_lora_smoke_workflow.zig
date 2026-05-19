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
const termite = @import("termite_internal");
const platform = @import("antfly_platform");
const compat = termite.io.compat;
const c_file = termite.util.c_file;

const materialize_gemma4_recursive_base = @import("../tools/materialize_gemma4_recursive_base.zig");
const run_gemma4_lora_pilot_workflow = @import("run_gemma4_lora_pilot_workflow.zig");

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
    rank: []const u8 = "8",
    alpha: []const u8 = "16",
    target_modules: []const u8 = "q_proj,k_proj,v_proj,o_proj",
    recursive_shared_block_size: []const u8 = "5",
    recursive_init: []const u8 = "average_residual_svd",
    teacher_top_k: []const u8 = "8",
    teacher_temperature: []const u8 = "2.0",
    backend: []const u8 = "mlx",
    split: []const u8 = "train",
    dry_run: bool = false,
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

    try runSmoke(init, allocator, opts);
}

fn runSmoke(init: std.process.Init, allocator: std.mem.Allocator, opts: Options) !void {
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

    try compat.cwd().createDirPath(compat.io(), opts.output_root);

    const started_at = std.Io.Timestamp.now(init.io, .awake);
    var pilot_cmd = std.ArrayListUnmanaged([]const u8).empty;
    defer pilot_cmd.deinit(allocator);
    try pilot_cmd.appendSlice(allocator, &.{
        "text",
        opts.base_model_dir,
        opts.output_root,
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
        opts.rank,
        "--alpha",
        opts.alpha,
        "--target-modules",
        opts.target_modules,
        "--recursive-shared-block-size",
        opts.recursive_shared_block_size,
        "--recursive-init",
        opts.recursive_init,
        "--teacher-top-k",
        opts.teacher_top_k,
        "--teacher-temperature",
        opts.teacher_temperature,
        "--backend",
        opts.backend,
        "--split",
        opts.split,
    });
    if (opts.dry_run) try pilot_cmd.append(allocator, "--dry-run");
    try runCommand(init, allocator, "run-gemma4-lora-pilot-workflow", run_gemma4_lora_pilot_workflow.main, pilot_cmd.items, false);
    const elapsed_seconds = elapsedSeconds(init.io, started_at);

    const adapter_config = try std.fs.path.join(allocator, &.{ opts.output_root, "adapter_seed", "adapter_config.json" });
    defer allocator.free(adapter_config);
    const teacher_prepared = try std.fs.path.join(allocator, &.{ opts.output_root, "prepared.teacher.json" });
    defer allocator.free(teacher_prepared);
    const train_out_name = try std.fmt.allocPrint(allocator, "train_out_{s}", .{opts.backend});
    defer allocator.free(train_out_name);
    const report_path = try std.fs.path.join(allocator, &.{ opts.output_root, train_out_name, "train_eval_report.json" });
    defer allocator.free(report_path);
    const trained_adapter_config = try std.fs.path.join(allocator, &.{ opts.output_root, train_out_name, "adapter_config.json" });
    defer allocator.free(trained_adapter_config);
    const trained_adapter_path = try std.fs.path.join(allocator, &.{ opts.output_root, train_out_name, "adapter_model.safetensors" });
    defer allocator.free(trained_adapter_path);
    const compressed_base_dir = try std.fs.path.join(allocator, &.{ opts.output_root, "compressed_base" });
    defer allocator.free(compressed_base_dir);
    const compressed_base_metadata = try std.fs.path.join(allocator, &.{ compressed_base_dir, "recursive_lora_base_config.json" });
    defer allocator.free(compressed_base_metadata);
    const results_path = try std.fs.path.join(allocator, &.{ opts.output_root, "recursive_smoke_results.json" });
    defer allocator.free(results_path);

    if (!opts.dry_run) {
        try requireFile(adapter_config);
        try requireFile(teacher_prepared);
        try requireFile(report_path);
        try requireFile(trained_adapter_config);
        try requireContains(allocator, adapter_config, "\"recursive_lora\"");
        try requireContains(allocator, adapter_config, opts.recursive_shared_block_size);
        try requireContains(allocator, teacher_prepared, "\"teacher_top_k\"");
        try requireContains(allocator, teacher_prepared, "\"teacher_top_k_probs\"");
        try requireContains(allocator, report_path, "\"teacher_examples_seen\"");
        try requireContains(allocator, report_path, "\"mean_teacher_temperature\"");
        try requireContains(allocator, trained_adapter_config, "\"recursive_lora\"");

        const train_out_dir = try std.fs.path.join(allocator, &.{ opts.output_root, train_out_name });
        defer allocator.free(train_out_dir);
        const materialize_cmd = [_][]const u8{ opts.base_model_dir, train_out_dir, compressed_base_dir };
        try runCommand(init, allocator, "materialize-gemma4-recursive-base", materialize_gemma4_recursive_base.main, &materialize_cmd, false);
        const compressed_checkpoint = try std.fs.path.join(allocator, &.{ compressed_base_dir, "model.safetensors" });
        defer allocator.free(compressed_checkpoint);
        try requireFile(compressed_checkpoint);
        try requireFile(compressed_base_metadata);
        try requireContains(allocator, compressed_base_metadata, "\"compression_ratio\"");

        try writeResults(
            allocator,
            results_path,
            opts,
            dataset_path,
            adapter_config,
            teacher_prepared,
            report_path,
            trained_adapter_config,
            trained_adapter_path,
            compressed_base_dir,
            compressed_base_metadata,
            elapsed_seconds,
        );
        try requireFile(results_path);
    }

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try writer.interface.print(
        \\recursive_smoke_complete: true
        \\base_model_dir: {s}
        \\output_root: {s}
        \\dataset: {s}
        \\adapter_config: {s}
        \\teacher_prepared: {s}
        \\report: {s}
        \\results: {s}
        \\trained_adapter_config: {s}
        \\compressed_base: {s}
        \\recursive_shared_block_size: {s}
        \\teacher_top_k: {s}
        \\teacher_temperature: {s}
        \\
    , .{
        opts.base_model_dir,
        opts.output_root,
        dataset_path,
        adapter_config,
        teacher_prepared,
        report_path,
        results_path,
        trained_adapter_config,
        compressed_base_dir,
        opts.recursive_shared_block_size,
        opts.teacher_top_k,
        opts.teacher_temperature,
    });
    try writer.interface.flush();
}

fn writeResults(
    allocator: std.mem.Allocator,
    results_path: []const u8,
    opts: Options,
    dataset_path: []const u8,
    adapter_config_path: []const u8,
    teacher_prepared_path: []const u8,
    report_path: []const u8,
    trained_adapter_config_path: []const u8,
    trained_adapter_path: []const u8,
    compressed_base_dir: []const u8,
    compressed_base_metadata_path: []const u8,
    elapsed_seconds: f64,
) !void {
    var report = try loadJson(allocator, report_path);
    defer report.deinit();
    var seed_config = try loadJson(allocator, adapter_config_path);
    defer seed_config.deinit();
    var trained_config = try loadJson(allocator, trained_adapter_config_path);
    defer trained_config.deinit();
    var compressed_metadata = try loadJson(allocator, compressed_base_metadata_path);
    defer compressed_metadata.deinit();

    const epochs = getArray(getObjectField(&report.value, "epoch_history"));
    var train_tokens: f64 = 0;
    var teacher_tokens: f64 = 0;
    var teacher_examples: f64 = 0;
    for (epochs) |epoch| {
        train_tokens += numberField(epoch, "supervised_tokens_seen", 0);
        teacher_tokens += numberField(epoch, "teacher_supervised_tokens_seen", 0);
        teacher_examples += numberField(epoch, "teacher_examples_seen", 0);
    }
    const last_epoch: ?std.json.Value = if (epochs.len > 0) epochs[epochs.len - 1] else null;
    const compressed_checkpoint = try std.fs.path.join(allocator, &.{ compressed_base_dir, "model.safetensors" });
    defer allocator.free(compressed_checkpoint);
    const seed_adapter_path = try std.fs.path.join(allocator, &.{ opts.output_root, "adapter_seed", "adapter_model.safetensors" });
    defer allocator.free(seed_adapter_path);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try std.json.Stringify.value(.{
        .task = "gemma4_recursive_lora_smoke_results",
        .base_model_dir = opts.base_model_dir,
        .output_root = opts.output_root,
        .dataset_path = dataset_path,
        .teacher_prepared_path = teacher_prepared_path,
        .train_eval_report_path = report_path,
        .elapsed_seconds = elapsed_seconds,
        .throughput = .{
            .train_supervised_tokens = train_tokens,
            .teacher_supervised_tokens = teacher_tokens,
            .train_supervised_tokens_per_second = train_tokens / @max(elapsed_seconds, 1.0),
        },
        .loss = .{
            .before_average_loss = numberFieldOpt(getObjectField(&report.value, "before"), "average_loss", 0),
            .after_average_loss = numberFieldOpt(getObjectField(&report.value, "after"), "average_loss", 0),
            .last_epoch_average_loss = if (last_epoch) |epoch| numberField(epoch, "average_loss", 0) else 0,
        },
        .teacher = .{
            .top_k = try std.fmt.parseUnsigned(usize, opts.teacher_top_k, 10),
            .temperature = try std.fmt.parseFloat(f64, opts.teacher_temperature),
            .examples_seen = teacher_examples,
            .supervised_tokens_seen = teacher_tokens,
            .mean_temperature = if (last_epoch) |epoch| numberField(epoch, "mean_teacher_temperature", 0) else 0,
        },
        .recursive_lora = .{
            .shared_block_size = try std.fmt.parseUnsigned(usize, opts.recursive_shared_block_size, 10),
            .seed_config = getObjectField(&seed_config.value, "recursive_lora"),
            .trained_config = getObjectField(&trained_config.value, "recursive_lora"),
        },
        .sizes = .{
            .base_model_dir_bytes = dirSize(allocator, opts.base_model_dir) catch 0,
            .compressed_base_dir_bytes = dirSize(allocator, compressed_base_dir) catch 0,
            .compressed_base_checkpoint_bytes = fileSize(allocator, compressed_checkpoint),
            .compressed_base_ratio = numberField(compressed_metadata.value, "compression_ratio", 0),
            .seed_adapter_bytes = fileSize(allocator, seed_adapter_path),
            .trained_adapter_bytes = fileSize(allocator, trained_adapter_path),
            .teacher_prepared_bytes = fileSize(allocator, teacher_prepared_path),
        },
    }, .{ .whitespace = .indent_2 }, &out.writer);
    try out.writer.writeByte('\n');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = results_path, .data = out.written() });
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
        } else if (std.mem.eql(u8, arg, "--rank")) {
            opts.rank = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--alpha")) {
            opts.alpha = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--target-modules")) {
            opts.target_modules = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--recursive-shared-block-size")) {
            opts.recursive_shared_block_size = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--recursive-init")) {
            opts.recursive_init = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--teacher-top-k")) {
            opts.teacher_top_k = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--teacher-temperature")) {
            opts.teacher_temperature = args.next() orelse return usageError();
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

    owned[0] = try allocator.dupeZ(u8, argv0);
    vector[0] = owned[0].ptr;
    for (args, 0..) |arg, idx| {
        owned[idx + 1] = try allocator.dupeZ(u8, arg);
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

fn dirSize(allocator: std.mem.Allocator, path: []const u8) !u64 {
    var dir = compat.cwd().openDir(compat.io(), path, .{ .iterate = true }) catch return 0;
    defer dir.close(compat.io());
    var total: u64 = 0;
    var it = dir.iterate();
    while (try it.next(compat.io())) |entry| {
        const child_path = try std.fs.path.join(allocator, &.{ path, entry.name });
        defer allocator.free(child_path);
        switch (entry.kind) {
            .file => total += fileSize(allocator, child_path),
            .directory => total += try dirSize(allocator, child_path),
            else => {},
        }
    }
    return total;
}

fn fileExists(path: []const u8) bool {
    compat.cwd().access(compat.io(), path, .{}) catch return false;
    return true;
}

fn requireFile(path: []const u8) !void {
    if (!fileExists(path)) {
        std.debug.print("missing expected file: {s}\n", .{path});
        return error.MissingExpectedFile;
    }
}

fn requireContains(allocator: std.mem.Allocator, path: []const u8, needle: []const u8) !void {
    if (!(try fileContains(allocator, path, needle))) {
        std.debug.print("expected file {s} to contain {s}\n", .{ path, needle });
        return error.ExpectedFileContentMissing;
    }
}

fn fileContains(allocator: std.mem.Allocator, path: []const u8, needle: []const u8) !bool {
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    return std.mem.indexOf(u8, bytes, needle) != null;
}

fn elapsedSeconds(io: std.Io, started_at: std.Io.Timestamp) f64 {
    const elapsed_ns = started_at.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds;
    if (elapsed_ns <= 0) return 1.0;
    return @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
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
        \\usage: run-gemma4-recursive-lora-smoke-workflow <base_model_dir> <output_root> [options]
        \\
        \\Options:
        \\  --dataset PATH
        \\  --count N
        \\  --max-examples N
        \\  --eval-max-examples N
        \\  --max-seq-len N
        \\  --epochs N
        \\  --lr FLOAT
        \\  --rank N
        \\  --alpha FLOAT
        \\  --target-modules CSV
        \\  --recursive-shared-block-size N
        \\  --recursive-init NAME
        \\  --teacher-top-k N
        \\  --teacher-temperature F
        \\  --backend auto|mlx|blas
        \\  --split NAME
        \\  --dry-run
        \\
    , .{});
    return error.InvalidArguments;
}
