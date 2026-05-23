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
const validation = @import("termite_internal").finetune.gliner2_run_validation;
const gliner2_bundle = @import("termite_internal").finetune.gliner2;

const train_gliner2_autodiff = @import("train_gliner2_autodiff.zig");

const CommandMain = *const fn (std.process.Init) anyerror!void;

const Options = struct {
    model_dir: []const u8,
    train_data: []const u8,
    out_dir: []const u8,
    epochs: []const u8 = "1",
    batch_size: []const u8 = "1",
    max_examples: []const u8 = "1",
    seq_len: []const u8 = "64",
    num_classes: []const u8 = "4",
    learning_rate: []const u8 = "2e-5",
    objective: []const u8 = "token",
    max_span_width: []const u8 = "4",
    require_loss_decrease: bool = false,
    dry_run: bool = false,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const opts = try parseOptions(&args) orelse return;
    try runWorkflow(init, allocator, opts);
}

fn runWorkflow(init: std.process.Init, allocator: std.mem.Allocator, opts: Options) !void {
    const train_args = [_][]const u8{
        "--model-dir",
        opts.model_dir,
        "--train-data",
        opts.train_data,
        "--out-dir",
        opts.out_dir,
        "--epochs",
        opts.epochs,
        "--batch-size",
        opts.batch_size,
        "--max-examples",
        opts.max_examples,
        "--seq-len",
        opts.seq_len,
        "--num-classes",
        opts.num_classes,
        "--learning-rate",
        opts.learning_rate,
        "--objective",
        opts.objective,
        "--max-span-width",
        opts.max_span_width,
    };
    try runCommand(init, allocator, "train-gliner2-autodiff", train_gliner2_autodiff.main, &train_args, opts.dry_run);

    printValidateCommand(opts.out_dir, opts.require_loss_decrease);
    if (!opts.dry_run) {
        var validation_summary = try validation.validateRun(allocator, opts.out_dir, .{
            .require_loss_decrease = opts.require_loss_decrease,
        });
        defer validation.freeRunValidationSummary(allocator, &validation_summary);
    }

    printInspectCommand(opts.model_dir, opts.out_dir);
    if (!opts.dry_run) {
        var bundle_summary = try gliner2_bundle.inspectLoRABundle(allocator, opts.model_dir, opts.out_dir);
        defer gliner2_bundle.freeLoRABundleInspectionSummary(allocator, &bundle_summary);
        if (bundle_summary.resolved_tensor_count == 0) return error.NoPeftAdapterTensors;
    }

    const stdout = std.Io.File.stdout();
    var buf: [2048]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try writer.interface.print(
        \\gliner2_autodiff_smoke_complete: true
        \\model_dir: {s}
        \\train_data: {s}
        \\out_dir: {s}
        \\require_loss_decrease: {}
        \\
    , .{ opts.model_dir, opts.train_data, opts.out_dir, opts.require_loss_decrease });
    try writer.interface.flush();
}

fn parseOptions(args: *std.process.Args.Iterator) !?Options {
    const model_dir = args.next() orelse return usageError();
    if (std.mem.eql(u8, model_dir, "--help") or std.mem.eql(u8, model_dir, "-h")) {
        printUsage();
        return null;
    }

    var opts = Options{
        .model_dir = model_dir,
        .train_data = args.next() orelse return usageError(),
        .out_dir = args.next() orelse return usageError(),
    };

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--epochs")) {
            opts.epochs = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            opts.batch_size = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            opts.max_examples = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            opts.seq_len = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--num-classes")) {
            opts.num_classes = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--learning-rate") or std.mem.eql(u8, arg, "--lr")) {
            opts.learning_rate = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--objective")) {
            opts.objective = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-span-width")) {
            opts.max_span_width = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--require-loss-decrease")) {
            opts.require_loss_decrease = true;
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            opts.dry_run = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return null;
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

fn printValidateCommand(out_dir: []const u8, require_loss_decrease: bool) void {
    std.debug.print("+ validate-gliner2-autodiff-run {s}", .{out_dir});
    if (require_loss_decrease) std.debug.print(" --require-loss-decrease", .{});
    std.debug.print("\n", .{});
}

fn printInspectCommand(model_dir: []const u8, out_dir: []const u8) void {
    std.debug.print("+ inspect-gliner2-lora-bundle {s} {s}\n", .{ model_dir, out_dir });
}

fn usageError() error{InvalidArguments} {
    printUsage();
    return error.InvalidArguments;
}

fn printUsage() void {
    std.debug.print(
        \\usage: run-gliner2-autodiff-smoke-workflow <model_dir> <train_jsonl> <out_dir> [options]
        \\
        \\Options:
        \\  --epochs N                 Training epochs (default: 1)
        \\  --batch-size N             Batch size (default: 1)
        \\  --max-examples N           Training example cap (default: 1)
        \\  --seq-len N                Sequence length (default: 64)
        \\  --num-classes N            Entity classes including O (default: 4)
        \\  --learning-rate FLOAT      Learning rate (default: 2e-5)
        \\  --objective NAME           token or span-start (default: token)
        \\  --max-span-width N         Max span width for span-start objective (default: 4)
        \\  --require-loss-decrease    Fail validation unless final step loss is lower than first step loss
        \\  --dry-run                  Print child commands without running them
        \\
    , .{});
}
