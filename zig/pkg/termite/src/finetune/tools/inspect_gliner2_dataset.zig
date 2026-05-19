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

const gliner2_data = termite.finetune.gliner2_data;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const input_path = args.next() orelse return usageError();
    const entity_types_csv = args.next() orelse return usageError();
    const split = args.next();
    const max_length_arg = args.next() orelse "256";
    const max_span_width_arg = args.next() orelse "8";
    const batch_size_arg = args.next() orelse "4";
    const drop_no_target_arg = args.next() orelse "false";

    const max_length = try std.fmt.parseUnsigned(usize, max_length_arg, 10);
    const max_span_width = try std.fmt.parseUnsigned(usize, max_span_width_arg, 10);
    const batch_size = try std.fmt.parseUnsigned(usize, batch_size_arg, 10);
    const drop_no_target = std.mem.eql(u8, drop_no_target_arg, "true");

    const entity_types = try parseCsv(allocator, entity_types_csv);
    defer {
        for (entity_types) |item| allocator.free(item);
        allocator.free(entity_types);
    }

    var loaded = try gliner2_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();
    const stats = try gliner2_data.computeStats(allocator, loaded.examples);

    const label_vocab = try gliner2_data.buildLabelVocab(allocator, loaded.examples, entity_types);
    defer {
        for (label_vocab) |label| allocator.free(label);
        allocator.free(label_vocab);
    }

    const coverage = gliner2_data.computeTargetCoverageStats(loaded.examples, entity_types);
    const filtered = try gliner2_data.filterExamplesForEntityTypes(allocator, loaded.examples, entity_types, drop_no_target);
    defer gliner2_data.freeExamples(allocator, filtered);
    const batch_summary = try gliner2_data.buildSimpleBatchShapeSummary(
        allocator,
        filtered,
        entity_types,
        max_length,
        max_span_width,
        batch_size,
    );

    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, model_dir);
    defer tokenizer.deinit(allocator);
    const preview_count = @min(batch_size, filtered.len);
    var preview_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        &tokenizer,
        filtered[0..preview_count],
        entity_types,
        max_length,
        max_span_width,
        batch_size,
    );
    defer preview_batch.deinit();

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .dataset_root = loaded.dataset_root,
        .input_path = input_path,
        .split = split,
        .entity_types = entity_types,
        .drop_no_target = drop_no_target,
        .stats = stats,
        .coverage = coverage,
        .filtered_examples = filtered.len,
        .label_vocab = label_vocab,
        .batch_shape = batch_summary,
        .preview = .{
            .batch_size = preview_batch.batch_size,
            .max_length = preview_batch.max_length,
            .max_words_per_sample = preview_batch.max_words_per_sample,
            .max_spans = preview_batch.max_spans,
            .num_entity_types = preview_batch.num_entity_types,
            .input_ids_len = preview_batch.input_ids.len,
            .span_labels_len = preview_batch.span_labels.len,
        },
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
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
        \\usage: inspect-gliner2-dataset <model_dir> <jsonl_or_dir> <entity_types_csv> [split] [max_length] [max_span_width] [batch_size] [drop_no_target]
        \\example: inspect-gliner2-dataset /tmp/gliner2_base /tmp/train person,organization,location train 256 8 4 true
        \\
    , .{});
    return error.InvalidArguments;
}
