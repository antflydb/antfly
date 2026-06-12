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
const reranker_lora = @import("../reranker_lora.zig");

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usage();
    const out_dir = args.next() orelse return usage();
    const rank = if (args.next()) |value| try std.fmt.parseUnsigned(usize, value, 10) else 16;
    const alpha = if (args.next()) |value| try std.fmt.parseFloat(f32, value) else 32.0;
    const top_layer_count = if (args.next()) |value| try std.fmt.parseUnsigned(usize, value, 10) else 1;

    var summary = try reranker_lora.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .top_layer_count = top_layer_count,
    });
    defer reranker_lora.freeBootstrapSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: bootstrap-reranker-lora <model-dir> <out-dir> [rank] [alpha] [top-layer-count]
        \\example: bootstrap-reranker-lora /tmp/bge-reranker /tmp/out 8 16 1
        \\
    , .{});
    return error.InvalidArguments;
}
