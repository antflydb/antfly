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
const cleanup_data = @import("termite_internal").finetune.entity_cleanup_data;
const cleanup_gliner_cache = @import("termite_internal").finetune.entity_cleanup_gliner_cache;
const cleanup_model = @import("termite_internal").finetune.entity_cleanup_model;
const text_encoder_boundary = @import("termite_internal").finetune.text_encoder_boundary;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usage();
    const input_path = args.next() orelse return usage();
    const out_path = args.next() orelse return usage();
    const split = args.next();
    const backend_arg = args.next() orelse "native";
    const max_examples_arg = args.next() orelse "128";
    const max_length_arg = args.next() orelse "256";
    const max_span_width_arg = args.next() orelse "8";
    const top_layer_count_arg = args.next() orelse "1";

    const backend = try parseBackend(backend_arg);
    const max_examples = try std.fmt.parseUnsigned(usize, max_examples_arg, 10);
    const max_length = try std.fmt.parseUnsigned(usize, max_length_arg, 10);
    const max_span_width = try std.fmt.parseUnsigned(usize, max_span_width_arg, 10);
    const top_layer_count = try std.fmt.parseUnsigned(usize, top_layer_count_arg, 10);

    var loaded = try cleanup_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();

    var summary = try cleanup_gliner_cache.prepareCachedSummary(
        allocator,
        model_dir,
        input_path,
        split,
        loaded.examples,
        backend,
        max_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    defer cleanup_model.freeCachedSummary(allocator, &summary);
    try cleanup_model.saveCachedSummary(allocator, out_path, summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .output_path = out_path,
        .artifact_family_version = summary.artifact_family_version,
        .input_path = summary.input_path,
        .split = summary.split,
        .feature_dim = summary.feature_dim,
        .context_window = summary.context_window,
        .mention_count = summary.mentions.len,
        .stats = summary.stats,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) !text_encoder_boundary.BackendChoice {
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return error.InvalidBackend;
}

fn usage() error{InvalidArguments}!void {
    std.debug.print(
        \\usage: prepare-gliner2-entity-cleanup-cache <model_dir> <jsonl_or_dir> <out_json> [split] [backend] [max_examples] [max_length] [max_span_width] [top_layer_count]
        \\example: prepare-gliner2-entity-cleanup-cache /tmp/gliner2_base /tmp/entity_cleanup.jsonl /tmp/entity_cleanup_gliner_cache.json train native 128 256 8 1
        \\
    , .{});
    return error.InvalidArguments;
}
