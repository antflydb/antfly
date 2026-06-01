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
const finetune = @import("../colqwen2.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_input = args.next() orelse return usageError();
    const dataset_root = args.next() orelse return usageError();
    const examples_jsonl = args.next() orelse return usageError();
    const out_path = args.next() orelse return usageError();
    const max_examples_arg = args.next() orelse "32";

    const max_examples = try std.fmt.parseUnsigned(usize, max_examples_arg, 10);
    const examples = try finetune.loadExamples(allocator, examples_jsonl);
    defer finetune.freeExamples(allocator, examples);

    var summary = try finetune.prepareInputsAgainstExamples(allocator, model_input, dataset_root, examples, max_examples);
    defer finetune.freePreparedInputsSummary(allocator, &summary);
    try finetune.savePreparedInputsSummary(allocator, out_path, summary);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: prepare-colqwen2-inputs <model_dir_or_adapter_dir> <dataset_root> <examples_jsonl> <out_summary_json> [max_examples]
        \\example: prepare-colqwen2-inputs /tmp/colqwen2_base /tmp/colqwen_dataset /tmp/examples.jsonl /tmp/colqwen2_inputs.json 32
        \\
    , .{});
    return error.InvalidArguments;
}
