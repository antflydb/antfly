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
const peft = inference.finetune.peft;

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var out_path: ?[]const u8 = null;
    var eval_program: ?[]const u8 = null;
    var inputs = std.ArrayListUnmanaged(peft.CompositionInput).empty;
    defer inputs.deinit(allocator);

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--out")) {
            out_path = args.next() orelse return usage();
        } else if (std.mem.eql(u8, arg, "--eval")) {
            eval_program = args.next() orelse return usage();
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return usage();
        } else {
            try inputs.append(allocator, parseInput(arg));
        }
    }

    const output = out_path orelse return usage();
    if (inputs.items.len == 0) return usage();

    const summary = try peft.composeAdapterSafetensors(allocator, init.io, inputs.items, output, eval_program);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseInput(arg: []const u8) peft.CompositionInput {
    if (std.mem.lastIndexOfScalar(u8, arg, ':')) |idx| {
        if (idx > 0 and idx + 1 < arg.len) {
            const weight = std.fmt.parseFloat(f32, arg[idx + 1 ..]) catch return .{ .path = arg };
            return .{ .path = arg[0..idx], .weight = weight };
        }
    }
    return .{ .path = arg };
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: compose-lora-adapters --out <adapter_model.safetensors> [--eval <program>] <adapter[:weight]>...
        \\example: compose-lora-adapters --out /tmp/composed.safetensors /tmp/a.safetensors:0.7 /tmp/b.safetensors:0.3
        \\
        \\The optional eval program is run before export; non-zero exit blocks writing.
        \\
    , .{});
    return error.InvalidArguments;
}
