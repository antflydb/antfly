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
const finetune = @import("termite_internal").finetune.gemma4;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const base_model_input = args.next() orelse return usageError();
    const adapter_model_input = args.next() orelse return usageError();

    var summary = try finetune.inspectLoRABundle(allocator, base_model_input, adapter_model_input);
    defer finetune.freeLoRABundleInspectionSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: inspect-gemma4-lora-bundle <base_model_dir_or_checkpoint> <adapter_dir_or_checkpoint>
        \\example: inspect-gemma4-lora-bundle /tmp/gemma4-base /tmp/gemma4-lora
        \\
    , .{});
    return error.InvalidArguments;
}
