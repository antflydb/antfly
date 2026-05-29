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
const gliner2_boundary = inference.finetune.gliner2_boundary;
const gliner2_data = inference.finetune.gliner2_data;
const reranker = inference.finetune.reranker;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const input_path = args.next() orelse return usageError();
    const entity_types_csv = args.next() orelse return usageError();
    const out_path = args.next() orelse return usageError();
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

    const entity_types = try parseCsv(allocator, entity_types_csv);
    defer {
        for (entity_types) |item| allocator.free(item);
        allocator.free(entity_types);
    }

    var loaded = try gliner2_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();

    const summary = try gliner2_boundary.prepareCachedBoundarySummary(
        allocator,
        model_dir,
        input_path,
        split,
        loaded.examples,
        entity_types,
        backend,
        max_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    defer {
        var owned = summary;
        gliner2_boundary.freeCachedBoundarySummary(allocator, &owned);
    }
    try gliner2_boundary.saveCachedBoundarySummary(allocator, out_path, summary);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .output_path = out_path,
        .artifact_family_version = summary.artifact_family_version,
        .model_dir = summary.model_dir,
        .input_path = summary.input_path,
        .split = summary.split,
        .requested_backend = summary.requested_backend,
        .top_layer_count = summary.top_layer_count,
        .hidden_size = summary.hidden_size,
        .max_length = summary.max_length,
        .max_span_width = summary.max_span_width,
        .entity_types = summary.entity_types,
        .dataset_stats = summary.dataset_stats,
        .example_count = summary.examples.len,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) !reranker.BackendChoice {
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return error.InvalidBackend;
}

fn parseCsv(allocator: std.mem.Allocator, value: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, item));
    }
    if (out.items.len == 0) return error.EmptyEntityTypes;
    return try out.toOwnedSlice(allocator);
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: prepare-gliner2-top-layer-boundary-cache <model_dir> <jsonl_or_dir> <entity_types_csv> <out_json> [split] [backend] [max_examples] [max_length] [max_span_width] [top_layer_count]
        \\example: prepare-gliner2-top-layer-boundary-cache /tmp/gliner2_base /tmp/train person,organization,location /tmp/gliner2_boundary.json train native 128 256 8 1
        \\
    , .{});
    return error.InvalidArguments;
}
