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
const finetune = termite.finetune.layoutlmv3;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const out_dir = args.next() orelse return usageError();
    const rank_arg = args.next() orelse "16";
    const alpha_arg = args.next() orelse "32";
    const base_model_name_or_path = args.next();

    const rank = try std.fmt.parseUnsigned(usize, rank_arg, 10);
    const alpha = try std.fmt.parseFloat(f32, alpha_arg);

    var summary = try finetune.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_name_or_path,
    });
    defer finetune.freeBootstrapSummary(allocator, &summary);

    const io = init.io;
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: bootstrap-layoutlmv3-lora <model_dir> <out_dir> [rank] [alpha] [base_model_name_or_path]
        \\example: bootstrap-layoutlmv3-lora /tmp/layoutlmv3-base /tmp/layoutlmv3-lora 16 32 microsoft/layoutlmv3-base
        \\
    , .{});
    return error.InvalidArguments;
}
