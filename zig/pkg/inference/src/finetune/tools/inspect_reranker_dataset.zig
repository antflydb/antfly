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

const reranker_data = inference.finetune.reranker_data;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const input_path = args.next() orelse return usageError();
    const split = args.next();
    const max_examples_arg = args.next() orelse "0";
    const max_examples = try std.fmt.parseUnsigned(usize, max_examples_arg, 10);

    var loaded = try reranker_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();

    const effective_examples = if (max_examples == 0 or max_examples >= loaded.examples.len)
        loaded.examples
    else
        loaded.examples[0..max_examples];
    const stats = reranker_data.computeStats(effective_examples);
    const pair_count = reranker_data.countPairwiseTrainingPairs(effective_examples);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .dataset_root = loaded.dataset_root,
        .input_path = input_path,
        .split = split,
        .max_examples = max_examples,
        .effective_examples = effective_examples.len,
        .stats = stats,
        .pairwise_training_pairs = pair_count,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: inspect-reranker-dataset <jsonl_or_dir> [split] [max_examples]
        \\example: inspect-reranker-dataset /tmp/rerank train 256
        \\
    , .{});
    return error.InvalidArguments;
}
