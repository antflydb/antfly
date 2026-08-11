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
const finetune = @import("inference_internal").finetune.gliner2;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_input = args.next() orelse return usageError();
    var lora_input: ?[]const u8 = null;
    var out_path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--out")) {
            out_path = args.next() orelse return usageError();
        } else if (lora_input == null) {
            lora_input = arg;
        } else {
            return usageError();
        }
    }

    var summary = try finetune.inspectCheckpoint(allocator, model_input, lora_input);
    defer finetune.freeCheckpointInspection(allocator, &summary);

    const io = init.io;
    if (out_path) |path| {
        if (std.fs.path.dirname(path)) |parent| {
            if (parent.len > 0) try std.Io.Dir.cwd().createDirPath(io, parent);
        }
        const rendered = try std.json.Stringify.valueAlloc(allocator, summary, .{ .whitespace = .indent_2 });
        defer allocator.free(rendered);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = rendered });
        return;
    }
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: inspect-gliner2-checkpoint <model_dir_or_model.safetensors> [lora_dir_or_checkpoint] [--out <report.json>]
        \\example: inspect-gliner2-checkpoint /tmp/gliner2-base /tmp/gliner2-lora
        \\
    , .{});
    return error.InvalidArguments;
}
