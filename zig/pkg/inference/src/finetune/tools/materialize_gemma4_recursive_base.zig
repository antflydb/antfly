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
const termite = @import("termite_internal");

const finetune = termite.finetune.gemma4;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const base_model_dir = args.next() orelse return usageError();
    const adapter_model_dir = args.next() orelse return usageError();
    const out_dir = args.next() orelse return usageError();

    var options = finetune.RecursiveCompressedBaseOptions{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--metadata-file")) {
            options.metadata_file_name = args.next() orelse return usageError();
        } else {
            return usageError();
        }
    }

    var summary = try finetune.materializeRecursiveCompressedBase(
        allocator,
        base_model_dir,
        adapter_model_dir,
        out_dir,
        options,
    );
    defer finetune.freeRecursiveCompressedBaseSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: materialize-gemma4-recursive-base <base_model_dir> <recursive_adapter_dir> <out_dir> [options]
        \\
        \\Writes a compressed recursive Gemma4 base directory:
        \\  - model.safetensors contains non-layer tensors plus physical shared layers only
        \\  - config/tokenizer/projector support files are copied from the base directory
        \\  - recursive_lora_base_config.json records source depth and compression stats
        \\
        \\Options:
        \\  --metadata-file NAME  Metadata JSON file name (default: recursive_lora_base_config.json)
        \\
        \\example:
        \\  materialize-gemma4-recursive-base /models/gemma4 /tmp/recursive_adapter /tmp/gemma4-recursive-base
        \\
    , .{});
    return error.InvalidArguments;
}
