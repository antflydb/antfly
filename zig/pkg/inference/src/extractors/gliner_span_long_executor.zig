// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded word windows for span encoders. Token admission precedes every
//! forward pass; document mergers own offset rebasing and overlap semantics.
const std = @import("std");
const executor = @import("gliner_span_v2_executor.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const document = @import("../pipelines/gliner_boundary_long_document.zig");
const long_relations = @import("../pipelines/gliner_boundary_long_relations.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const compute = @import("../ops/ops.zig");
const wire = @import("extraction_v2.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const A = std.mem.Allocator;

// Preserve generic caller limits; only the exact qualified 1B route supplies
// the separately published caps. Generic planner byte/word defaults are unchanged.
fn planningLimits(options: executor.Options) @import("../models/gliner_span_window_limits.zig").Limits {
    return options.long_document_limits orelse .{
        .max_windows = options.max_sequences_per_item,
        .max_request_tokens = options.max_request_tokens,
    };
}

test "span window limits remain model scoped" {
    const generic = planningLimits(.{ .max_sequences_per_item = 128, .max_request_tokens = 200000 });
    try std.testing.expectEqual(@as(usize, 128), generic.max_windows);
    try std.testing.expectEqual(@as(usize, 200000), generic.max_request_tokens);
    const qualified = planningLimits(.{ .max_sequences_per_item = 128, .max_request_tokens = 200000, .long_document_limits = @import("../models/gliner_span_window_limits.zig").qualified });
    try std.testing.expectEqual(@as(usize, 64), qualified.max_windows);
    try std.testing.expectEqual(@as(usize, 65536), qualified.max_request_tokens);
}

pub fn prepare(a: A, tokenizer: Tokenizer, item: *const wire.Item, preprocessing: processor.Options, options: executor.Options) !executor.PreparedItem {
    const selected = item.options.long_document;
    const limits = planningLimits(options);
    var body_words: usize = @min(selected.window_words, preprocessing.max_sequence_tokens);
    if (selected.overlap_words >= body_words) return error.InvalidExtractionOptions;
    while (true) {
        if (options.control) |control| try control.check();
        var planned = try document.plan(a, item.text, item.compiled.fingerprint, .{
            .mode = .windowed,
            .word_splitter = item.options.word_splitter,
            .max_window_body_words = body_words,
            .overlap_words = selected.overlap_words,
            .max_windows = @min(selected.max_windows, @min(options.max_sequences_per_item, limits.max_windows)),
            .max_document_bytes = limits.max_document_bytes,
            .max_document_words = limits.max_document_words,
            .control = options.control,
            .inference_fingerprint = @splat(0),
        });
        errdefer planned.deinit();
        const batches = try a.alloc(processor.PreparedBatch, planned.windows.len);
        errdefer a.free(batches);
        var built: usize = 0;
        errdefer for (batches[0..built]) |*batch| batch.deinit();
        var total: usize = 0;
        var too_long = false;
        for (planned.windows, batches) |window, *batch| {
            var process = preprocessing;
            process.max_text_words = body_words;
            batch.* = processor.prepare(a, tokenizer, &.{.{ .text = try planned.windowText(window.index), .schema = &item.compiled }}, process) catch |err| {
                if (err != error.BoundarySequenceLimitExceeded) return err;
                too_long = true;
                break;
            };
            built += 1;
            total = try std.math.add(usize, total, batch.input_ids.len);
            if (total > @min(options.max_request_tokens, limits.max_request_tokens)) return error.ExtractionRequestLimitExceeded;
        }
        if (too_long) {
            for (batches[0..built]) |*batch| batch.deinit();
            a.free(batches);
            planned.deinit();
            if (body_words <= selected.overlap_words + 2) return error.BoundarySequenceLimitExceeded;
            body_words = selected.overlap_words + @max((body_words - selected.overlap_words) / 2, 1);
            continue;
        }
        const ranges = try a.alloc(executor.TaskRange, batches.len);
        for (ranges) |*range| range.* = .{ .start = 0, .end = item.compiled.schema.classifications.len };
        return .{ .document = planned, .ranges = ranges, .batches = batches, .prompt_tokens = total };
    }
}

pub fn classificationLogits(cb: *const compute.ComputeBackend, a: A, config: executor.EncoderConfig, item: *const wire.Item, prepared: *const executor.PreparedItem) !executor.ItemLogits {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const counts = try scratch.alloc(usize, item.compiled.schema.classifications.len);
    for (counts, item.compiled.schema.classifications) |*count, task| count.* = task.task.labels.len;
    const windows = try scratch.alloc(document.WindowLogits, prepared.batches.len);
    const planned = prepared.document.?;
    for (prepared.batches, windows, 0..) |batch, *window, i| {
        var window_arena = std.heap.ArenaAllocator.init(a);
        defer window_arena.deinit();
        const rows = try executor.classificationLogits(cb, window_arena.allocator(), config, batch.samples[0], counts);
        const const_rows = try scratch.alloc([]const f64, rows.len);
        for (rows, const_rows) |row, *out| out.* = try scratch.dupe(f64, row);
        window.* = .{ .identity = try planned.identity(i), .logits = const_rows };
    }
    var merged = try document.aggregateClassificationLogits(a, planned, &item.compiled, windows, .{});
    defer merged.deinit();
    const rows = try a.alloc([]f64, merged.logits.len);
    var built: usize = 0;
    errdefer {
        for (rows[0..built]) |row| a.free(row);
        a.free(rows);
    }
    for (merged.logits, rows) |row, *out| {
        out.* = try a.dupe(f64, row);
        built += 1;
    }
    return .{ .rows = rows, .prompt_tokens = prepared.prompt_tokens, .sequences = prepared.batches.len };
}

pub fn extract(cb: *const compute.ComputeBackend, allocator: A, config: @import("../architectures/modern_bert.zig").Config, item: *const wire.Item, prepared: *const executor.PreparedItem, options: pipeline.Options) !pipeline.Result {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    const planned = prepared.document.?;
    const mentions = try a.alloc(document.WindowMentions, prepared.batches.len);
    const relations = try a.alloc(long_relations.Window, prepared.batches.len);
    for (prepared.batches, mentions, relations, 0..) |batch, *mention_window, *relation_window, i| {
        var local_options = options;
        local_options.overlap = .allow;
        var result = try @import("gliner_span_extraction.zig").run(cb, allocator, config, batch.samples[0], &item.compiled, local_options);
        defer result.deinit();
        var candidates: std.ArrayList(document.MentionCandidate) = .empty;
        for (result.samples[0].entities, 0..) |entity, kind| for (entity.values) |value| {
            try candidates.append(a, .{ .entity_type = kind, .source = value.source.?, .probability = value.confidence });
        };
        mention_window.* = .{ .identity = try planned.identity(i), .pre_overlap_candidates = candidates.items };
        relation_window.* = .{ .identity = try planned.identity(i), .relations = try copyRelations(a, result.samples[0].relations) };
    }
    var merged = try document.mergeMentions(allocator, planned, &item.compiled, mentions, options.overlap, options.offset_unit, .{ .control = options.control });
    defer merged.deinit();
    const groups = try a.alloc(pipeline.EntityGroup, item.compiled.schema.entities.len);
    for (groups, item.compiled.schema.entities, 0..) |*group, entity, kind| {
        var values: std.ArrayList(pipeline.Value) = .empty;
        for (merged.selected) |mention| if (mention.entity_type == kind) {
            const source = mention.source;
            const value: pipeline.Value = .{ .text = try a.dupe(u8, planned.text[source.byte_start..source.byte_end]), .confidence = mention.probability, .source = source, .token_span = null };
            if (entity.dtype == .str and values.items.len > 0) {
                if (value.confidence > values.items[0].confidence) values.items[0] = value;
            } else try values.append(a, value);
        };
        const Order = struct {
            fn less(_: void, left: pipeline.Value, right: pipeline.Value) bool {
                if (left.confidence != right.confidence) return left.confidence > right.confidence;
                if (left.source.?.byte_start != right.source.?.byte_start) return left.source.?.byte_start < right.source.?.byte_start;
                return left.source.?.byte_end < right.source.?.byte_end;
            }
        };
        std.mem.sort(pipeline.Value, values.items, {}, Order.less);
        group.* = .{ .name = entity.name, .dtype = entity.dtype, .values = values.items };
    }
    var edges = try long_relations.merge(allocator, planned, &item.compiled, relations, .{ .output_unit = options.offset_unit, .merge = .{ .control = options.control } });
    defer edges.deinit();
    const copied_edges = try copyRelations(a, edges.relations);
    var classes: []const pipeline.Classification = &.{};
    if (item.compiled.schema.classifications.len != 0) {
        var logits = try classificationLogits(cb, allocator, .{ .modern_bert = config }, item, prepared);
        defer logits.deinit(allocator);
        const rows = try a.alloc([]const f64, logits.rows.len);
        for (rows, logits.rows) |*out, row| out.* = row;
        var presented = try pipeline.presentClassifications(a, &item.compiled, rows, 1, options);
        defer presented.deinit();
        const copied = try a.dupe(pipeline.Classification, presented.classifications);
        for (copied) |*task| task.labels = try a.dupe(pipeline.Label, task.labels);
        classes = copied;
    }
    var output_values = copied_edges.len;
    for (groups) |group| output_values = try std.math.add(usize, output_values, group.values.len);
    for (classes) |task| output_values = try std.math.add(usize, output_values, task.labels.len);
    if (output_values > options.max_output_values) return error.ExtractionOutputLimitExceeded;
    const samples = try a.alloc(pipeline.Sample, 1);
    samples[0] = .{ .entities = groups, .relations = copied_edges, .classifications = classes };
    return .{ .arena = arena, .samples = samples };
}

// A merged result must own every string after both intermediate arenas close.
fn copyRelations(a: A, input: []const pipeline.Relation) ![]pipeline.Relation {
    const result = try a.dupe(pipeline.Relation, input);
    for (result) |*edge| {
        edge.name = try a.dupe(u8, edge.name);
        edge.head.text = try a.dupe(u8, edge.head.text);
        edge.tail.text = try a.dupe(u8, edge.tail.text);
    }
    return result;
}

test "windowed relation copies own names and endpoints" {
    var output = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer output.deinit();
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    const a = source.allocator();
    const input = [_]pipeline.Relation{.{
        .name = try a.dupe(u8, "works_for"),
        .head = .{ .text = try a.dupe(u8, "Alice"), .confidence = 0.9, .source = null, .token_span = null },
        .tail = .{ .text = try a.dupe(u8, "Antfly"), .confidence = 0.8, .source = null, .token_span = null },
        .confidence = 0.7,
    }};
    const copied = try copyRelations(output.allocator(), &input);
    try std.testing.expect(copied[0].name.ptr != input[0].name.ptr);
    try std.testing.expect(copied[0].head.text.ptr != input[0].head.text.ptr);
    try std.testing.expect(copied[0].tail.text.ptr != input[0].tail.text.ptr);
    source.deinit();
    try std.testing.expectEqualStrings("works_for", copied[0].name);
    try std.testing.expectEqualStrings("Alice", copied[0].head.text);
    try std.testing.expectEqualStrings("Antfly", copied[0].tail.text);
}
