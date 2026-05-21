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
const reranker_data = @import("../reranker_data.zig");
const reranker_head = @import("../reranker_head.zig");

const print = std.debug.print;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 128;
    var top_layer_count: usize = 1;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--backend")) {
            const value = args.next() orelse return error.MissingBackendValue;
            backend = parseBackendChoice(value) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const value = args.next() orelse return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--top-layer-count")) {
            const value = args.next() orelse return error.MissingTopLayerCount;
            top_layer_count = try std.fmt.parseUnsigned(usize, value, 10);
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 3) return usage();
    const model_dir = positional.items[0];
    const input_path = positional.items[1];
    const out_path = positional.items[2];
    const split = if (positional.items.len >= 4) positional.items[3] else null;

    var loaded = try reranker_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();

    var summary = try reranker_head.prepareCachedTopLayerSummary(
        allocator,
        model_dir,
        input_path,
        split,
        loaded.examples,
        backend,
        max_examples,
        top_layer_count,
    );
    defer reranker_head.freeCachedTopLayerSummary(allocator, &summary);
    try reranker_head.saveCachedTopLayerSummary(allocator, out_path, summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackendChoice(value: []const u8) ?reranker_head.BackendChoice {
    return reranker_head.parseBackendChoice(value);
}

fn usage() error{InvalidArguments}!void {
    print(
        \\usage: prepare-reranker-top-layer-cache <model-dir> <jsonl-or-dir> <out-summary-json> [split] [--backend auto|cuda|blas|mlx] [--max-examples N] [--top-layer-count N]
        \\example: prepare-reranker-top-layer-cache /tmp/bge-reranker /tmp/rerank /tmp/reranker_top_layer_cache.json train --backend mlx --max-examples 128 --top-layer-count 1
        \\
    , .{});
    return error.InvalidArguments;
}
