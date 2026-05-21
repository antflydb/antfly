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
const reranker_data = @import("../reranker_data.zig");
const reranker_head = @import("../reranker_head.zig");

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 256;
    var epochs: usize = 1;
    var learning_rate: f32 = 0.001;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--backend")) {
            const value = args.next() orelse return error.MissingBackendValue;
            backend = parseBackendChoice(value) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const value = args.next() orelse return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            const value = args.next() orelse return error.MissingEpochs;
            epochs = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--learning-rate")) {
            const value = args.next() orelse return error.MissingLearningRate;
            learning_rate = try std.fmt.parseFloat(f32, value);
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 4) {
        printUsage();
        return error.InvalidArguments;
    }

    const model_dir = positional.items[0];
    const train_input = positional.items[1];
    const eval_input = positional.items[2];
    const out_dir = positional.items[3];
    const train_split = if (positional.items.len >= 5) positional.items[4] else null;
    const eval_split = if (positional.items.len >= 6) positional.items[5] else train_split;

    var train_loaded = try reranker_data.loadExamples(allocator, train_input, train_split);
    defer train_loaded.deinit();
    var eval_loaded = try reranker_data.loadExamples(allocator, eval_input, eval_split);
    defer eval_loaded.deinit();

    const summary = try reranker_head.trainEvalHead(
        allocator,
        model_dir,
        train_loaded.examples,
        eval_loaded.examples,
        out_dir,
        backend,
        epochs,
        .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
        },
    );
    defer allocator.free(summary.output_dir);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .model_dir = model_dir,
        .train_input = train_input,
        .eval_input = eval_input,
        .train_split = train_split,
        .eval_split = eval_split,
        .requested_backend = @tagName(backend),
        .epochs = epochs,
        .learning_rate = learning_rate,
        .max_examples = max_examples,
        .output_dir = summary.output_dir,
        .head_checkpoint = reranker_head.head_checkpoint_file_name,
        .head_config = reranker_head.head_config_file_name,
        .train = summary.train,
        .eval = summary.eval,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackendChoice(value: []const u8) ?reranker_head.BackendChoice {
    return reranker_head.parseBackendChoice(value);
}

fn printUsage() void {
    print(
        \\usage: train-eval-reranker-head <model-dir> <train-jsonl-or-dir> <eval-jsonl-or-dir> <out-dir> [train-split] [eval-split] [--backend auto|cuda|blas|mlx] [--max-examples N] [--epochs N] [--learning-rate LR]
        \\example: train-eval-reranker-head /tmp/bge-reranker /tmp/train /tmp/eval /tmp/out train eval --backend mlx --max-examples 128 --epochs 2 --learning-rate 0.0005
        \\
    , .{});
}
