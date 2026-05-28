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
const cleanup_data = @import("inference_internal").finetune.entity_cleanup_data;
const cleanup_model = @import("inference_internal").finetune.entity_cleanup_model;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const input_path = args.next() orelse return usage();
    const out_path = args.next() orelse return usage();
    const split = args.next();
    const feature_dim_arg = args.next() orelse "128";
    const context_window_arg = args.next() orelse "24";

    const feature_dim = try std.fmt.parseUnsigned(usize, feature_dim_arg, 10);
    const context_window = try std.fmt.parseUnsigned(usize, context_window_arg, 10);

    var loaded = try cleanup_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();

    var summary = try cleanup_model.prepareCachedSummary(allocator, input_path, split, loaded.examples, .{
        .feature_dim = feature_dim,
        .context_window = context_window,
    });
    defer cleanup_model.freeCachedSummary(allocator, &summary);
    try cleanup_model.saveCachedSummary(allocator, out_path, summary);
}

fn usage() error{InvalidArguments}!void {
    std.debug.print(
        \\usage: prepare-entity-cleanup-cache <jsonl_or_dir> <out_json> [split] [feature_dim] [context_window]
        \\example: prepare-entity-cleanup-cache /tmp/entity_cleanup.jsonl /tmp/entity_cleanup_cache.json train 128 24
        \\
    , .{});
    return error.InvalidArguments;
}
