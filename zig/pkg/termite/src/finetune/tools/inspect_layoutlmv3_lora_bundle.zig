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
const compat = @import("../../io/compat.zig");
const finetune = @import("../layoutlmv3.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const base_model_dir = args.next() orelse return usageError();
    const adapter_model_dir = args.next() orelse return usageError();
    const report_path = args.next();

    var summary = try finetune.inspectLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer finetune.freeLoRABundleInspectionSummary(allocator, &summary);

    if (report_path) |path| {
        const io = compat.io();
        var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
        defer file.close(io);
        const rendered = try std.json.Stringify.valueAlloc(allocator, summary, .{ .whitespace = .indent_2 });
        defer allocator.free(rendered);
        try file.writeStreamingAll(io, rendered);
        try file.writeStreamingAll(io, "\n");
    }

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: inspect-layoutlmv3-lora-bundle <base_model_dir> <adapter_model_dir> [report_path]
        \\example: inspect-layoutlmv3-lora-bundle /tmp/layoutlmv3-base /tmp/layoutlmv3-lora /tmp/layoutlmv3_lora_inspect.json
        \\
    , .{});
    return error.InvalidArguments;
}
