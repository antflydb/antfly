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

const bootstrap_gemma4_lora = @import("../tools/bootstrap_gemma4_lora.zig");
const generate_gemma4_multimodal_pilot_dataset = @import("../tools/generate_gemma4_multimodal_pilot_dataset.zig");
const generate_gemma4_pilot_dataset = @import("../tools/generate_gemma4_pilot_dataset.zig");
const materialize_gemma4_teacher_targets = @import("../tools/materialize_gemma4_teacher_targets.zig");
const prepare_gemma4_lora_inputs = @import("../tools/prepare_gemma4_lora_inputs.zig");
const train_eval_gemma4_lora_bundle = @import("train_eval_gemma4_lora_bundle.zig");

const CommandMain = *const fn (std.process.Init) anyerror!void;

const Mode = enum {
    text,
    multimodal,
};

const Options = struct {
    mode: Mode,
    base_model_dir: []const u8,
    output_root: []const u8,
    adapter_dir: ?[]const u8 = null,
    dataset_path: ?[]const u8 = null,
    projector_path: ?[]const u8 = null,
    image_path: ?[]const u8 = null,
    count: ?usize = null,
    max_examples: ?usize = null,
    eval_max_examples: ?usize = null,
    max_seq_len: usize = 512,
    epochs: usize = 1,
    learning_rate: []const u8 = "0.0003",
    rank: []const u8 = "16",
    alpha: []const u8 = "32",
    target_modules: ?[]const u8 = null,
    recursive_shared_block_size: ?[]const u8 = null,
    recursive_init: []const u8 = "average_residual_svd",
    teacher_top_k: ?[]const u8 = null,
    teacher_temperature: []const u8 = "1.0",
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
    var owned_adapter_dir: ?[]const u8 = null;
    defer if (owned_adapter_dir) |value| allocator.free(value);
    if (opts.adapter_dir) |value| {
        owned_adapter_dir = try resolveCliPath(allocator, invocation_cwd, value);
        opts.adapter_dir = owned_adapter_dir;
    }
    var owned_dataset_path: ?[]const u8 = null;
    defer if (owned_dataset_path) |value| allocator.free(value);
    if (opts.dataset_path) |value| {
        owned_dataset_path = try resolveCliPath(allocator, invocation_cwd, value);
        opts.dataset_path = owned_dataset_path;
    }
    var owned_projector_path: ?[]const u8 = null;
    defer if (owned_projector_path) |value| allocator.free(value);
    if (opts.projector_path) |value| {
        owned_projector_path = try resolveCliPath(allocator, invocation_cwd, value);
        opts.projector_path = owned_projector_path;
    }
    var owned_image_path: ?[]const u8 = null;
    defer if (owned_image_path) |value| allocator.free(value);
    if (opts.image_path) |value| {
        owned_image_path = try resolveCliPath(allocator, invocation_cwd, value);
        opts.image_path = owned_image_path;
    }

    try runPilot(init, allocator, opts);
}

fn runPilot(init: std.process.Init, allocator: std.mem.Allocator, opts: Options) !void {
    const count = opts.count orelse switch (opts.mode) {
        .text => @as(usize, 1000),
        .multimodal => @as(usize, 100),
    };
    const max_examples = opts.max_examples orelse count;
    const eval_max_examples = opts.eval_max_examples orelse @min(max_examples, @as(usize, 32));

    const adapter_dir = if (opts.adapter_dir) |path|
        try allocator.dupe(u8, path)
    else
        try std.fs.path.join(allocator, &.{ opts.output_root, "adapter_seed" });
    defer allocator.free(adapter_dir);

    const dataset_path = if (opts.dataset_path) |path|
        try allocator.dupe(u8, path)
    else
        try std.fs.path.join(allocator, &.{ opts.output_root, if (opts.mode == .text) "text_pilot.jsonl" else "multimodal_pilot.jsonl" });
    defer allocator.free(dataset_path);

    const prepared_path = try std.fs.path.join(allocator, &.{ opts.output_root, "prepared.json" });
    defer allocator.free(prepared_path);
    const teacher_prepared_path = try std.fs.path.join(allocator, &.{ opts.output_root, "prepared.teacher.json" });
    defer allocator.free(teacher_prepared_path);
    const train_out_name = try std.fmt.allocPrint(allocator, "train_out_{s}", .{opts.backend});
    defer allocator.free(train_out_name);
    const train_out_dir = try std.fs.path.join(allocator, &.{ opts.output_root, train_out_name });
    defer allocator.free(train_out_dir);
    const report_path = try std.fs.path.join(allocator, &.{ train_out_dir, "train_eval_report.json" });
    defer allocator.free(report_path);
    const config_path = try std.fs.path.join(allocator, &.{ train_out_dir, "training_config.json" });
    defer allocator.free(config_path);

    if (opts.mode == .multimodal) {
        if (opts.projector_path == null) return error.MissingGgufProjector;
        if (!fileExists(dataset_path) and opts.image_path == null) return error.MissingPilotImagePath;
    }

    try compat.cwd().createDirPath(compat.io(), opts.output_root);

    if (!fileExists(dataset_path)) {
        const count_arg = try std.fmt.allocPrint(allocator, "{d}", .{count});
        defer allocator.free(count_arg);
        if (opts.mode == .text) {
            const cmd = [_][]const u8{ dataset_path, count_arg, opts.split };
            try runCommand(init, allocator, "generate-gemma4-pilot-dataset", generate_gemma4_pilot_dataset.main, &cmd, opts.dry_run);
        } else {
            const cmd = [_][]const u8{ dataset_path, count_arg, opts.image_path.?, opts.split };
            try runCommand(init, allocator, "generate-gemma4-multimodal-pilot-dataset", generate_gemma4_multimodal_pilot_dataset.main, &cmd, opts.dry_run);
        }
    } else {
        std.debug.print("using existing dataset: {s}\n", .{dataset_path});
    }

    const adapter_config_path = try std.fs.path.join(allocator, &.{ adapter_dir, "adapter_config.json" });
    defer allocator.free(adapter_config_path);
    const adapter_weights_path = try std.fs.path.join(allocator, &.{ adapter_dir, "adapter_model.safetensors" });
    defer allocator.free(adapter_weights_path);
    if (!fileExists(adapter_config_path) or !fileExists(adapter_weights_path)) {
        var cmd = std.ArrayListUnmanaged([]const u8).empty;
        defer cmd.deinit(allocator);
        try cmd.appendSlice(allocator, &.{ opts.base_model_dir, adapter_dir, opts.rank, opts.alpha, opts.base_model_dir });
        if (opts.target_modules) |target_modules| try cmd.appendSlice(allocator, &.{ "--target-modules", target_modules });
        if (opts.recursive_shared_block_size) |shared_block_size| try cmd.appendSlice(allocator, &.{ "--recursive-shared-block-size", shared_block_size, "--recursive-init", opts.recursive_init });
        try runCommand(init, allocator, "bootstrap-gemma4-lora", bootstrap_gemma4_lora.main, cmd.items, opts.dry_run);
    } else {
        std.debug.print("using existing adapter seed: {s}\n", .{adapter_dir});
    }

    const max_examples_arg = try std.fmt.allocPrint(allocator, "{d}", .{max_examples});
    defer allocator.free(max_examples_arg);
    const max_seq_len_arg = try std.fmt.allocPrint(allocator, "{d}", .{opts.max_seq_len});
    defer allocator.free(max_seq_len_arg);
    var prepare_cmd = std.ArrayListUnmanaged([]const u8).empty;
    defer prepare_cmd.deinit(allocator);
    try prepare_cmd.appendSlice(allocator, &.{ opts.base_model_dir, dataset_path, opts.split, prepared_path, "--max-examples", max_examples_arg, "--max-seq-len", max_seq_len_arg });
    if (opts.mode == .multimodal) try prepare_cmd.appendSlice(allocator, &.{ "--gguf-projector", opts.projector_path.? });
    try runCommand(init, allocator, "prepare-gemma4-lora-inputs", prepare_gemma4_lora_inputs.main, prepare_cmd.items, opts.dry_run);

    const train_prepared_path = if (opts.teacher_top_k) |teacher_top_k| blk: {
        var teacher_cmd = std.ArrayListUnmanaged([]const u8).empty;
        defer teacher_cmd.deinit(allocator);
        try teacher_cmd.appendSlice(allocator, &.{ opts.base_model_dir, prepared_path, teacher_prepared_path, "--top-k", teacher_top_k, "--temperature", opts.teacher_temperature, "--max-examples", max_examples_arg, "--backend", "native" });
        if (opts.mode == .multimodal) try teacher_cmd.appendSlice(allocator, &.{ "--gguf-projector", opts.projector_path.? });
        try runCommand(init, allocator, "materialize-gemma4-teacher-targets", materialize_gemma4_teacher_targets.main, teacher_cmd.items, opts.dry_run);
        break :blk teacher_prepared_path;
    } else prepared_path;

    const eval_max_examples_arg = try std.fmt.allocPrint(allocator, "{d}", .{eval_max_examples});
    defer allocator.free(eval_max_examples_arg);
    const epochs_arg = try std.fmt.allocPrint(allocator, "{d}", .{opts.epochs});
    defer allocator.free(epochs_arg);
    var train_cmd = std.ArrayListUnmanaged([]const u8).empty;
    defer train_cmd.deinit(allocator);
    try train_cmd.appendSlice(allocator, &.{
        opts.base_model_dir,
        adapter_dir,
        train_prepared_path,
        train_out_dir,
        "--trainer",
        "autodiff",
        "--backend",
        opts.backend,
        "--max-examples",
        max_examples_arg,
        "--eval-max-examples",
        eval_max_examples_arg,
        "--epochs",
        epochs_arg,
        "--lr",
        opts.learning_rate,
        "--max-grad-norm",
        "1.0",
        "--grad-accum",
        "1",
    });
    if (opts.mode == .multimodal) try train_cmd.appendSlice(allocator, &.{ "--gguf-projector", opts.projector_path.? });
    try runCommand(init, allocator, "train-eval-gemma4-lora-bundle", train_eval_gemma4_lora_bundle.main, train_cmd.items, opts.dry_run);

    if (!opts.dry_run) {
        try requireFile(prepared_path);
        if (opts.teacher_top_k != null) {
            try requireFile(teacher_prepared_path);
            try requireContains(allocator, teacher_prepared_path, "\"teacher_top_k\"");
        }
        try requireFile(report_path);
        try requireFile(config_path);
        const trained_adapter_path = try std.fs.path.join(allocator, &.{ train_out_dir, "adapter_model.safetensors" });
        defer allocator.free(trained_adapter_path);
        try requireFile(trained_adapter_path);
        try requireContains(allocator, config_path, "\"trainer\": \"autodiff");
        if (!(fileContains(allocator, config_path, opts.backend) catch false)) return error.TrainingBackendMismatch;
        try requireContains(allocator, report_path, "\"optimizer_steps\": 0");
        if (opts.mode == .multimodal) {
            try requireContains(allocator, prepared_path, "\"gguf_projector_sha256\"");
            try requireContains(allocator, report_path, "\"projected_media_cache_entries\"");
        }
    }

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try writer.interface.print(
        \\pilot_complete: true
        \\mode: {s}
        \\dataset: {s}
        \\prepared: {s}
        \\teacher_prepared: {s}
        \\adapter_seed: {s}
        \\train_out: {s}
        \\report: {s}
        \\config: {s}
        \\
    , .{ @tagName(opts.mode), dataset_path, prepared_path, train_prepared_path, adapter_dir, train_out_dir, report_path, config_path });
    try writer.interface.flush();
}

fn parseOptions(args: *std.process.Args.Iterator) !Options {
    const mode_arg = args.next() orelse return usageError();
    const mode: Mode = if (std.mem.eql(u8, mode_arg, "text"))
        .text
    else if (std.mem.eql(u8, mode_arg, "multimodal"))
        .multimodal
    else
        return usageError();

    var opts = Options{
        .mode = mode,
        .base_model_dir = args.next() orelse return usageError(),
        .output_root = args.next() orelse return usageError(),
    };

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--adapter-dir")) {
            opts.adapter_dir = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--dataset")) {
            opts.dataset_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--projector") or std.mem.eql(u8, arg, "--gguf-projector")) {
            opts.projector_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--image-path")) {
            opts.image_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--count")) {
            opts.count = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            opts.max_examples = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--eval-max-examples")) {
            opts.eval_max_examples = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--max-seq-len")) {
            opts.max_seq_len = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            opts.epochs = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
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
        \\usage: run-gemma4-lora-pilot-workflow <text|multimodal> <base_model_dir> <output_root> [options]
        \\
        \\Options:
        \\  --adapter-dir PATH       Existing or generated LoRA seed adapter dir.
        \\  --dataset PATH           Existing Gemma chat JSONL dataset.
        \\  --projector PATH         Gemma4 GGUF projector. Required for multimodal mode.
        \\  --image-path PATH        Image used when generating a multimodal pilot dataset.
        \\  --count N                Generated dataset size.
        \\  --max-examples N         Training examples per epoch.
        \\  --eval-max-examples N    Before/after eval examples.
        \\  --max-seq-len N          Prepared input sequence length.
        \\  --epochs N               Training epochs.
        \\  --lr FLOAT               Learning rate.
        \\  --rank N                 Bootstrap LoRA rank.
        \\  --alpha FLOAT            Bootstrap LoRA alpha.
        \\  --target-modules CSV     Bootstrap LoRA target modules.
        \\  --recursive-shared-block-size N
        \\  --recursive-init NAME
        \\  --teacher-top-k N
        \\  --teacher-temperature F
        \\  --backend auto|mlx|native
        \\  --split NAME
        \\  --dry-run
        \\
    , .{});
    return error.InvalidArguments;
}
