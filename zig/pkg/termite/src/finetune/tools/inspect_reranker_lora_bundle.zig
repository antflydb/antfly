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
    const adapter_dir = args.next() orelse return usage();
    var summary = try reranker_lora.inspectLoRABundle(allocator, model_dir, adapter_dir);
    defer reranker_lora.freeLoRABundleInspectionSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: inspect-reranker-lora-bundle <model-dir> <adapter-dir>
        \\example: inspect-reranker-lora-bundle /tmp/bge-reranker /tmp/out
        \\
    , .{});
    return error.InvalidArguments;
}
