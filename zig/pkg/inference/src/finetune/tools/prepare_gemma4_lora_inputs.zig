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
const finetune = inference.finetune.gemma4;
const gemma_chat_data = inference.finetune.gemma_chat_data;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const dataset_path = args.next() orelse return usageError();
    const split_arg = args.next() orelse return usageError();
    const out_path = args.next() orelse return usageError();

    var max_examples: usize = 0;
    var max_seq_len: usize = 512;
    var gguf_projector_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--max-examples")) {
            const val = args.next() orelse return usageError();
            max_examples = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--max-seq-len")) {
            const val = args.next() orelse return usageError();
            max_seq_len = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--gguf-projector")) {
            gguf_projector_path = args.next() orelse return usageError();
        } else {
            return usageError();
        }
    }

    const split = if (std.mem.eql(u8, split_arg, "-")) null else split_arg;

    var loaded = try gemma_chat_data.loadExamples(allocator, dataset_path, split);
    defer loaded.deinit();

    const has_multimodal = messagesHaveMedia(loaded.examples);
    if (has_multimodal and gguf_projector_path == null) return error.MissingGgufProjector;
    var summary = if (has_multimodal)
        try finetune.prepareMultimodalInputsFromChatData(
            allocator,
            model_dir,
            gguf_projector_path.?,
            loaded.examples,
            max_examples,
            max_seq_len,
        )
    else
        try finetune.prepareInputsFromChatData(
            allocator,
            model_dir,
            loaded.examples,
            max_examples,
            max_seq_len,
        );
    defer finetune.freePreparedInputsSummary(allocator, &summary);
    try finetune.savePreparedInputsSummary(allocator, out_path, summary);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();

    std.debug.print("examples_written: {d}\n", .{summary.examples_seen});
    std.debug.print("saved_summary: {s}\n", .{out_path});
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: prepare-gemma4-lora-inputs <model_dir> <dataset_path> <split|-> <out_summary_json> [options]
        \\
        \\  <model_dir>         Path to Gemma4 model directory (must contain tokenizer.json)
        \\  <dataset_path>      JSONL file, directory, or manifest.json
        \\  <split|->           Dataset split to filter (e.g. "train") or "-" for all
        \\  <out_summary_json>  Output path for prepared inputs JSON
        \\
        \\Options:
        \\  --max-examples N    Maximum number of examples to prepare (default: 0 = all)
        \\  --max-seq-len N     Maximum sequence length in tokens (default: 512)
        \\  --gguf-projector P  Required when the dataset contains image/audio parts; path to Gemma4 projector GGUF
        \\
        \\example: prepare-gemma4-lora-inputs /tmp/gemma4-base /tmp/data.jsonl train /tmp/gemma4_inputs.json --max-examples 256 --max-seq-len 512
        \\
    , .{});
    return error.InvalidArguments;
}

fn messagesHaveMedia(examples: []const gemma_chat_data.Example) bool {
    for (examples) |example| {
        if (example.image_paths.len > 0 or example.audio_paths.len > 0) return true;
    }
    return false;
}
