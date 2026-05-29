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

const gemma_data = inference.finetune.gemma_data;
const compat = inference.io.compat;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const dataset_path = args.next() orelse return usageError();
    const split_arg = args.next() orelse return usageError();
    const out_csv_path = args.next() orelse return usageError();
    const out_summary_path = args.next() orelse return usageError();
    const max_examples = std.fmt.parseUnsigned(usize, args.next() orelse "0", 10) catch return error.InvalidArguments;
    const split = if (std.mem.eql(u8, split_arg, "-")) null else split_arg;

    var loaded = try gemma_data.loadExamples(allocator, dataset_path, split);
    defer loaded.deinit();
    const summary = try gemma_data.writeCsv(allocator, out_csv_path, loaded.examples, max_examples);

    var json_buf: std.Io.Writer.Allocating = .init(allocator);
    defer json_buf.deinit();
    try std.json.Stringify.value(.{
        .dataset_path = dataset_path,
        .split = split,
        .dataset_root = loaded.dataset_root,
        .mode = @tagName(summary.mode),
        .examples_written = summary.examples_written,
        .id_column = summary.id_column,
        .prompt_column = summary.prompt_column,
        .text_column = summary.text_column,
        .max_prompt_chars = summary.max_prompt_chars,
        .max_response_chars = summary.max_response_chars,
        .out_csv_path = summary.out_csv_path,
    }, .{ .whitespace = .indent_2 }, &json_buf.writer);
    try json_buf.writer.writeByte('\n');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = out_summary_path, .data = json_buf.written() });

    std.debug.print("mode: {s}\n", .{@tagName(summary.mode)});
    std.debug.print("examples_written: {d}\n", .{summary.examples_written});
    std.debug.print("saved_csv: {s}\n", .{out_csv_path});
    std.debug.print("saved_summary: {s}\n", .{out_summary_path});
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: prepare-gemma4-text-dataset <dataset-path> <split|-> <out_csv_path> <out_summary_path> [max_examples]
        \\example: prepare-gemma4-text-dataset /tmp/gemma_train.jsonl train /tmp/gemma/train.csv /tmp/gemma/train_summary.json 256
        \\
    , .{});
    return error.InvalidArguments;
}
