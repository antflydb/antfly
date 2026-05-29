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
const cleanup_model = @import("inference_internal").finetune.entity_cleanup_model;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var epochs: usize = 3;
    var learning_rate: f32 = 0.05;
    var embedding_learning_rate: f32 = 0.01;
    var embedding_dim: usize = 32;
    var max_mentions: usize = 0;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--epochs")) {
            epochs = try std.fmt.parseUnsigned(usize, args.next() orelse return usage(), 10);
        } else if (std.mem.eql(u8, arg, "--learning-rate") or std.mem.eql(u8, arg, "--lr")) {
            learning_rate = try std.fmt.parseFloat(f32, args.next() orelse return usage());
        } else if (std.mem.eql(u8, arg, "--embedding-learning-rate")) {
            embedding_learning_rate = try std.fmt.parseFloat(f32, args.next() orelse return usage());
        } else if (std.mem.eql(u8, arg, "--embedding-dim")) {
            embedding_dim = try std.fmt.parseUnsigned(usize, args.next() orelse return usage(), 10);
        } else if (std.mem.eql(u8, arg, "--max-mentions")) {
            max_mentions = try std.fmt.parseUnsigned(usize, args.next() orelse return usage(), 10);
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 3) return usage();
    const train_cache = positional.items[0];
    const eval_cache = positional.items[1];
    const out_dir = positional.items[2];

    var train_summary = try cleanup_model.loadCachedSummary(allocator, train_cache);
    defer cleanup_model.freeCachedSummary(allocator, &train_summary);
    var eval_summary = try cleanup_model.loadCachedSummary(allocator, eval_cache);
    defer cleanup_model.freeCachedSummary(allocator, &eval_summary);

    const summary = try cleanup_model.trainEvalCached(allocator, &train_summary, &eval_summary, out_dir, .{
        .epochs = epochs,
        .learning_rate = learning_rate,
        .embedding_learning_rate = embedding_learning_rate,
        .embedding_dim = embedding_dim,
        .max_mentions = max_mentions,
    });

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usage() error{InvalidArguments}!void {
    std.debug.print(
        \\usage: train-eval-entity-cleanup-head <train-cache.json> <eval-cache.json> <out-dir> [--epochs N] [--learning-rate LR] [--embedding-learning-rate LR] [--embedding-dim N] [--max-mentions N]
        \\example: train-eval-entity-cleanup-head /tmp/train_cleanup.json /tmp/eval_cleanup.json /tmp/out --epochs 5 --learning-rate 0.05 --embedding-dim 32
        \\
    , .{});
    return error.InvalidArguments;
}
