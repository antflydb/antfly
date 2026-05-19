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
const build_options = @import("build_options");
const reranker_data = @import("../reranker_data.zig");
const reranker = @import("../reranker.zig");
const reranker_head = @import("../reranker_head.zig");

const print = std.debug.print;

const Options = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8 = null,
    head_input: ?[]const u8 = null,
    backend: reranker.BackendChoice = .auto,
    max_examples: usize = 256,

    fn deinit(self: *Options) void {
        _ = self;
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var backend: reranker.BackendChoice = .auto;
    var max_examples: usize = 256;
    var head_input: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--backend")) {
            const value = args.next() orelse return error.MissingBackendValue;
            backend = parseBackendChoice(value) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--head-dir")) {
            head_input = args.next() orelse return error.MissingHeadDir;
        } else if (std.mem.eql(u8, arg, "--head")) {
            head_input = args.next() orelse return error.MissingHeadPath;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const value = args.next() orelse return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, value, 10);
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var split: ?[]const u8 = null;
    if (positional.items.len >= 3) split = positional.items[2];

    var opts = Options{
        .allocator = allocator,
        .model_dir = positional.items[0],
        .input_path = positional.items[1],
        .split = split,
        .head_input = head_input,
        .backend = backend,
        .max_examples = max_examples,
    };
    defer opts.deinit();

    var loaded = try reranker_data.loadExamples(allocator, opts.input_path, opts.split);
    defer loaded.deinit();

    const limited = loaded.examples[0..@min(loaded.examples.len, opts.max_examples)];
    const result = if (opts.head_input) |head_input_path| blk: {
        const hidden_size = try reranker_head.resolveModelHiddenSize(allocator, opts.model_dir);
        var head = (try reranker_head.loadHeadFromInput(allocator, head_input_path, hidden_size)) orelse return error.MissingRerankerHead;
        defer head.deinit();
        const summary = try reranker_head.evaluateHead(allocator, opts.model_dir, &head, loaded.examples, opts.backend, opts.max_examples);
        break :blk reranker.RuntimeEvalResult{
            .summary = summary,
            .backend_selected = switch (opts.backend) {
                .auto => .native,
                .native => .native,
                .mlx => .mlx,
            },
            .distributed = .{},
            .uses_distributed_mlx = false,
            .uses_tensor_parallel_mlx = false,
        };
    } else try reranker.evaluateExamplesRuntime(
        allocator,
        opts.model_dir,
        loaded.examples,
        opts.backend,
        opts.max_examples,
    );

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .model_dir = opts.model_dir,
        .input_path = opts.input_path,
        .split = opts.split,
        .build_enable_mlx = build_options.enable_mlx,
        .requested_backend = @tagName(opts.backend),
        .selected_backend = @tagName(result.backend_selected),
        .dataset_root = loaded.dataset_root,
        .head_input = opts.head_input,
        .dataset_stats = reranker_data.computeStats(limited),
        .pairwise_training_pairs = reranker_data.countPairwiseTrainingPairs(limited),
        .distributed = result.distributed,
        .uses_distributed_mlx = result.uses_distributed_mlx,
        .uses_tensor_parallel_mlx = result.uses_tensor_parallel_mlx,
        .summary = result.summary,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackendChoice(value: []const u8) ?reranker.BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "blas") or std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    return null;
}

fn printUsage() void {
    print(
        \\usage: eval-reranker-checkpoint <model-dir> <jsonl_or_dir> [split] [--head-dir DIR|--head FILE] [--backend auto|blas|mlx] [--max-examples N]
        \\example: eval-reranker-checkpoint /tmp/bge-reranker /tmp/rerank train --head-dir /tmp/out --backend mlx --max-examples 128
        \\
    , .{});
}
