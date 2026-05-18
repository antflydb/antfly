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
const reranker_head = @import("../reranker_head.zig");

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse {
        printUsage();
        return error.InvalidArguments;
    };
    const head_input = args.next() orelse {
        printUsage();
        return error.InvalidArguments;
    };
    const out_dir = args.next() orelse {
        printUsage();
        return error.InvalidArguments;
    };
    if (args.next() != null) {
        printUsage();
        return error.InvalidArguments;
    }

    try reranker_head.materializeHeadFromDir(allocator, model_dir, head_input, out_dir);

    const stdout = std.Io.File.stdout();
    var buf: [1024]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .model_dir = model_dir,
        .head_input = head_input,
        .output_dir = out_dir,
        .checkpoint = reranker_head.merged_head_checkpoint_file_name,
        .materialized_head_contract = "classifier.out_proj.{weight,bias}",
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn printUsage() void {
    print(
        \\usage: materialize-reranker-head <model-dir> <head-dir-or-checkpoint> <out-dir>
        \\example: materialize-reranker-head /tmp/bge-reranker /tmp/out /tmp/materialized
        \\
    , .{});
}
