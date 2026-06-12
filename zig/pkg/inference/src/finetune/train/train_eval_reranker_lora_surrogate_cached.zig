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

    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 128;
    var epochs: usize = 1;
    var learning_rate: f32 = 0.001;
    var layer_name: ?[]const u8 = null;
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

    var train_summary = try reranker_head.loadCachedPooledSummary(allocator, train_cache_path);
    defer reranker_head.freeCachedPooledSummary(allocator, &train_summary);
    var eval_summary = try reranker_head.loadCachedPooledSummary(allocator, eval_cache_path);
    defer reranker_head.freeCachedPooledSummary(allocator, &eval_summary);

    var summary = try reranker_lora.trainEvalSurrogateCachedSummaries(
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
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackendChoice(value: []const u8) ?reranker_head.BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    return null;
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: train-eval-reranker-lora-surrogate-cached <model-dir> <adapter-dir> <head-dir-or-file> <train-cache-json> <eval-cache-json> <out-dir> [--backend auto|native] [--max-examples N] [--epochs N] [--learning-rate LR] [--layer-name NAME]
        \\example: train-eval-reranker-lora-surrogate-cached /tmp/bge-reranker /tmp/adapter /tmp/head /tmp/train_cache.json /tmp/eval_cache.json /tmp/out --epochs 2 --learning-rate 0.0005
        \\
    , .{});
    return error.InvalidArguments;
}
