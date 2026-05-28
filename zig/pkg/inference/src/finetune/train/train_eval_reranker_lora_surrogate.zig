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
const reranker_lora = @import("../reranker_lora.zig");

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 128;
    var epochs: usize = 1;
    var learning_rate: f32 = 0.001;
    var layer_name: ?[]const u8 = null;
    var max_grad_norm: f32 = 1.0;
    var use_schedule_free: bool = false;
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
        } else if (std.mem.eql(u8, arg, "--layer-name")) {
            layer_name = args.next() orelse return error.MissingLayerName;
        } else if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            const value = args.next() orelse return error.MissingMaxGradNorm;
            max_grad_norm = try std.fmt.parseFloat(f32, value);
        } else if (std.mem.eql(u8, arg, "--schedule-free")) {
            use_schedule_free = true;
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 6) return usage();
    const model_dir = positional.items[0];
    const adapter_dir = positional.items[1];
    const head_input = positional.items[2];
    const train_input = positional.items[3];
    const eval_input = positional.items[4];
    const out_dir = positional.items[5];
    const train_split = if (positional.items.len >= 7) positional.items[6] else null;
    const eval_split = if (positional.items.len >= 8) positional.items[7] else train_split;

    var train_loaded = try reranker_data.loadExamples(allocator, train_input, train_split);
    defer train_loaded.deinit();
    var eval_loaded = try reranker_data.loadExamples(allocator, eval_input, eval_split);
    defer eval_loaded.deinit();

    var summary = try reranker_lora.trainEvalSurrogate(
        allocator,
        model_dir,
        adapter_dir,
        head_input,
        train_loaded.examples,
        eval_loaded.examples,
        out_dir,
        backend,
        .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
            .epochs = epochs,
            .layer_name = layer_name,
            .max_grad_norm = max_grad_norm,
            .use_schedule_free = use_schedule_free,
        },
    );
    defer reranker_lora.freeSurrogateTrainEvalSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackendChoice(value: []const u8) ?reranker_head.BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    return null;
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: train-eval-reranker-lora-surrogate <model-dir> <adapter-dir> <head-dir-or-file> <train-jsonl-or-dir> <eval-jsonl-or-dir> <out-dir> [train-split] [eval-split] [--backend auto|native|mlx] [--max-examples N] [--epochs N] [--learning-rate LR] [--layer-name NAME] [--max-grad-norm F]
        \\example: train-eval-reranker-lora-surrogate /tmp/bge-reranker /tmp/adapter /tmp/head /tmp/train /tmp/eval /tmp/out train eval --epochs 2 --learning-rate 0.0005 --max-grad-norm 1.0
        \\
    , .{});
    return error.InvalidArguments;
}
