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
const reranker_head = @import("../reranker_head.zig");
const reranker_lora = @import("../reranker_lora.zig");

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var argv: std.ArrayListUnmanaged([]const u8) = .empty;
    defer argv.deinit(allocator);
    while (args.next()) |arg| try argv.append(allocator, arg);
    try runFromArgs(allocator, init.io, argv.items);
}

pub fn runFromArgs(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8) !void {
    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 128;
    var epochs: usize = 1;
    var learning_rate: f32 = 0.001;
    var layer_name: ?[]const u8 = null;
    var i: usize = 0;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= argv.len) return error.MissingBackendValue;
            const value = argv[i];
            backend = parseBackendChoice(value) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            i += 1;
            if (i >= argv.len) return error.MissingMaxExamples;
            const value = argv[i];
            max_examples = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            i += 1;
            if (i >= argv.len) return error.MissingEpochs;
            const value = argv[i];
            epochs = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--learning-rate")) {
            i += 1;
            if (i >= argv.len) return error.MissingLearningRate;
            const value = argv[i];
            learning_rate = try std.fmt.parseFloat(f32, value);
        } else if (std.mem.eql(u8, arg, "--layer-name")) {
            i += 1;
            if (i >= argv.len) return error.MissingLayerName;
            layer_name = argv[i];
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 6) return usage();
    const model_dir = positional.items[0];
    const adapter_dir = positional.items[1];
    const head_input = positional.items[2];
    const train_cache_path = positional.items[3];
    const eval_cache_path = positional.items[4];
    const out_dir = positional.items[5];

    var train_summary = try reranker_head.loadCachedTopLayerSummary(allocator, train_cache_path);
    defer reranker_head.freeCachedTopLayerSummary(allocator, &train_summary);
    var eval_summary = try reranker_head.loadCachedTopLayerSummary(allocator, eval_cache_path);
    defer reranker_head.freeCachedTopLayerSummary(allocator, &eval_summary);

    var summary = try reranker_lora.trainEvalTopLayerCachedSurrogate(
        allocator,
        model_dir,
        adapter_dir,
        head_input,
        &train_summary,
        &eval_summary,
        out_dir,
        backend,
        .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
            .epochs = epochs,
            .layer_name = layer_name,
        },
    );
    defer reranker_lora.freeSurrogateTrainEvalSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackendChoice(value: []const u8) ?reranker_head.BackendChoice {
    return reranker_head.parseBackendChoice(value);
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: train-eval-reranker-lora-top-layer-cached-surrogate <model-dir> <adapter-dir> <head-dir-or-file> <train-top-layer-cache-json> <eval-top-layer-cache-json> <out-dir> [--backend auto|cuda|blas|mlx] [--max-examples N] [--epochs N] [--learning-rate LR] [--layer-name NAME]
        \\note: for BERT/RoBERTa families, omitting --layer-name uses the exact cached full replayed top-layer block path when available; passing --layer-name keeps single-layer targeting.
        \\example: train-eval-reranker-lora-top-layer-cached-surrogate /tmp/bge-reranker /tmp/adapter /tmp/head /tmp/train_top_cache.json /tmp/eval_top_cache.json /tmp/out --epochs 2 --learning-rate 0.0005
        \\
    , .{});
    return error.InvalidArguments;
}
