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
    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);
    var readiness_options = gliner2_data.DatasetReadinessOptions{};
    var fail_on_readiness = false;
    while (args.next()) |arg| {
        if (!std.mem.startsWith(u8, arg, "--")) {
            try positional.append(allocator, arg);
        } else if (std.mem.eql(u8, arg, "--preset")) {
            const value = args.next() orelse return usageError();
            if (std.mem.eql(u8, value, "smoke")) {
                readiness_options = .{};
            } else if (std.mem.eql(u8, value, "non-toy")) {
                readiness_options = .{
                    .min_examples = 100,
                    .min_total_entities = 100,
                    .min_unique_labels = 2,
                    .min_target_entities = 100,
                    .min_target_coverage_ratio = 0.95,
                    .require_all_examples_with_target = true,
                    .min_positive_span_labels = 100,
                };
            } else {
                return usageError();
            }
        } else if (std.mem.eql(u8, arg, "--fail-on-readiness")) {
            fail_on_readiness = true;
        } else if (std.mem.eql(u8, arg, "--min-examples")) {
            readiness_options.min_examples = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--min-entities")) {
            readiness_options.min_total_entities = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--min-labels")) {
            readiness_options.min_unique_labels = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--min-target-entities")) {
            readiness_options.min_target_entities = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--min-target-coverage")) {
            readiness_options.min_target_coverage_ratio = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--require-all-examples-with-target")) {
            readiness_options.require_all_examples_with_target = true;
        } else if (std.mem.eql(u8, arg, "--min-positive-span-labels")) {
            readiness_options.min_positive_span_labels = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--min-positive-rate")) {
            readiness_options.min_positive_rate_per_label = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else {
            return usageError();
        }
    }
    if (positional.items.len > 5) return usageError();

    const split = if (positional.items.len >= 1 and !std.mem.eql(u8, positional.items[0], "-")) positional.items[0] else null;
    const max_length_arg = if (positional.items.len >= 2) positional.items[1] else "256";
    const max_span_width_arg = if (positional.items.len >= 3) positional.items[2] else "8";
    const batch_size_arg = if (positional.items.len >= 4) positional.items[3] else "4";
    const drop_no_target_arg = if (positional.items.len >= 5) positional.items[4] else "false";

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
    var readiness = try gliner2_data.evaluateDatasetReadiness(
        allocator,
        loaded.examples,
        entity_types,
        max_length,
        max_span_width,
        batch_size,
        readiness_options,
    );
    defer gliner2_data.freeDatasetReadinessSummary(allocator, &readiness);

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
        .readiness = readiness,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
    if (fail_on_readiness and !readiness.passed) return error.DatasetReadinessFailed;
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
        \\optional readiness flags:
        \\  --preset smoke|non-toy
        \\  --fail-on-readiness
        \\  --min-examples N
        \\  --min-entities N
        \\  --min-labels N
        \\  --min-target-entities N
        \\  --min-target-coverage FLOAT
        \\  --require-all-examples-with-target
        \\  --min-positive-span-labels N
        \\  --min-positive-rate FLOAT
        \\
    , .{});
    return error.InvalidArguments;
}
