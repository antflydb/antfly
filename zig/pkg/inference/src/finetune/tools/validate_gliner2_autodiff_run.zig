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
const validation = @import("inference_internal").finetune.gliner2_run_validation;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var out_dir: ?[]const u8 = null;
    var require_loss_decrease = false;
    var min_supervised_tokens_per_second: ?f64 = null;
    var max_avg_step_wall_ms: ?f64 = null;
    var max_total_execute_ms: ?f64 = null;
    var max_peak_resident_bytes: ?usize = null;
    var min_examples: ?usize = null;
    var min_steps: ?usize = null;
    var min_entity_labels: ?usize = null;
    var min_supervised_tokens: ?usize = null;
    var min_entity_tokens: ?usize = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, arg, "--require-loss-decrease")) {
            require_loss_decrease = true;
        } else if (std.mem.eql(u8, arg, "--min-supervised-tokens-per-second")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_supervised_tokens_per_second = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-avg-step-wall-ms")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_avg_step_wall_ms = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-total-execute-ms")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_total_execute_ms = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-peak-resident-bytes")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_peak_resident_bytes = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-examples")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_examples = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-steps")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_steps = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-entity-labels")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_entity_labels = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-supervised-tokens")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_supervised_tokens = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-entity-tokens")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_entity_tokens = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (out_dir == null) {
            out_dir = arg;
        } else {
            std.debug.print("error: unknown argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArguments;
        }
    }

    const dir = out_dir orelse {
        printUsage();
        return error.InvalidArguments;
    };

    var summary = try validation.validateRun(allocator, dir, .{
        .require_loss_decrease = require_loss_decrease,
        .min_supervised_tokens_per_second = min_supervised_tokens_per_second,
        .max_avg_step_wall_ms = max_avg_step_wall_ms,
        .max_total_execute_ms = max_total_execute_ms,
        .max_peak_resident_bytes = max_peak_resident_bytes,
        .min_examples = min_examples,
        .min_steps = min_steps,
        .min_entity_labels = min_entity_labels,
        .min_supervised_tokens = min_supervised_tokens,
        .min_entity_tokens = min_entity_tokens,
    });
    defer validation.freeRunValidationSummary(allocator, &summary);

    const io = init.io;
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn printUsage() void {
    std.debug.print(
        \\usage: validate-gliner2-autodiff-run <out_dir> [--require-loss-decrease] [--min-supervised-tokens-per-second <f64>] [--max-avg-step-wall-ms <f64>] [--max-total-execute-ms <f64>] [--max-peak-resident-bytes <n>] [--min-examples <n>] [--min-steps <n>] [--min-entity-labels <n>] [--min-supervised-tokens <n>] [--min-entity-tokens <n>]
        \\example: validate-gliner2-autodiff-run /tmp/gliner2-run --require-loss-decrease --min-supervised-tokens-per-second 10 --max-avg-step-wall-ms 1000 --max-total-execute-ms 50000 --max-peak-resident-bytes 2000000000 --min-examples 100 --min-steps 100 --min-entity-labels 2 --min-supervised-tokens 1000 --min-entity-tokens 100
        \\
        \\Validates a train-gliner2-autodiff output directory containing:
        \\  training_manifest.json
        \\  training_metrics.jsonl
        \\  one or more saved LoRA parameter .bin files
        \\  adapter_model.safetensors + adapter_config.json
        \\  task_head.safetensors
        \\
    , .{});
}
