// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! markerV0/count_lstm entity and relation inference. The processor is shared
//! with upstream's SchemaTransformer; instance slots remain learned GRU slots.
const std = @import("std");
const compute = @import("../ops/ops.zig");
const head = @import("../architectures/gliner_head.zig");
const modern = @import("../architectures/modern_bert.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const decode = @import("../pipelines/gliner_boundary_decode.zig");
const schema = @import("../pipelines/extraction_schema.zig");
const wire = @import("extraction_v2.zig");
const CT = compute.CT;

pub fn mlp(cb: *const compute.ComputeBackend, a: std.mem.Allocator, input: CT, H: usize, output: usize, prefix: []const u8) !CT {
    const w0 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.weight", .{prefix}));
    defer cb.free(w0);
    const b0 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.bias", .{prefix}));
    defer cb.free(b0);
    const linear = try cb.linear(input, w0, b0, 1, H, 2 * H);
    defer cb.free(linear);
    const relu = try cb.relu(linear);
    defer cb.free(relu);
    const w2 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.2.weight", .{prefix}));
    defer cb.free(w2);
    const b2 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.2.bias", .{prefix}));
    defer cb.free(b2);
    return cb.linear(relu, w2, b2, 1, 2 * H, output);
}

pub fn gather(cb: *const compute.ComputeBackend, a: std.mem.Allocator, hidden: CT, positions: []const usize, H: usize) !CT {
    const ids = try a.alloc(i64, positions.len);
    for (positions, ids) |position, *id| id.* = @intCast(position);
    return cb.embeddingLookup(hidden, ids, positions.len, H);
}

fn values(a: std.mem.Allocator, sample: processor.Sample, logits: []const f32, columns: usize, column: usize, threshold: f32, options: pipeline.Options) ![]pipeline.Value {
    const width = 8;
    var candidates: std.ArrayList(decode.Candidate) = .empty;
    for (sample.words, 0..) |first, start| {
        const begin = first.source orelse continue;
        for (0..@min(width, sample.words.len - start)) |span_width| {
            const last = sample.words[start + span_width].source orelse continue;
            const logit = logits[(start * width + span_width) * columns + column];
            if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
            const probability = 1 / (1 + @exp(-logit));
            if (probability < threshold) continue;
            if (candidates.items.len >= options.query_limits.max_candidates) return error.ExtractionCandidateLimitExceeded;
            try candidates.append(a, .{ .start = begin.start, .end = last.end, .probability = probability, .source_index = start * width + span_width });
        }
    }
    const selected = try decode.resolveOverlaps(a, candidates.items, options.overlap, .{ .max_candidates = options.query_limits.max_candidates, .control = options.control });
    var offsets = try decode.OffsetMap.init(a, sample.original_text, sample.original_text.len);
    defer offsets.deinit();
    const result = try a.alloc(pipeline.Value, selected.len);
    for (selected, result) |candidate, *value| {
        const coordinate = try offsets.convert(.{ .start = candidate.start, .end = candidate.end }, options.offset_unit);
        value.* = .{ .text = sample.original_text[candidate.start..candidate.end], .confidence = candidate.probability, .source = .{ .start = coordinate.start, .end = coordinate.end, .unit = options.offset_unit, .byte_start = candidate.start, .byte_end = candidate.end }, .token_span = null };
    }
    return result;
}

fn best(input: []const pipeline.Value) ?pipeline.Value {
    var result: ?pipeline.Value = null;
    for (input) |value| {
        if (result == null or value.confidence > result.?.confidence) result = value;
    }
    return result;
}

pub fn run(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: modern.Config, sample: processor.Sample, compiled: *const schema.CompiledSchema, options: pipeline.Options) !pipeline.Result {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    if (options.control) |control| try control.check();
    const H: usize = config.hidden_size;
    const mask = try a.alloc(i64, sample.input_ids.len);
    @memset(mask, 1);
    const hidden = try modern.forwardCT(cb, a, config, sample.input_ids, mask, 1, sample.input_ids.len);
    defer cb.free(hidden);
    const spans: ?CT = if (compiled.schema.entities.len != 0 or compiled.schema.relations.len != 0) try spanRepresentations(cb, a, hidden, sample, H) else null;
    defer if (spans) |value| cb.free(value);
    const span_count = sample.words.len * 8;
    var entities: std.ArrayList(pipeline.EntityGroup) = .empty;
    var relations: std.ArrayList(pipeline.Relation) = .empty;
    var classifications: std.ArrayList(pipeline.Classification) = .empty;
    var output_values: usize = 0;
    for (sample.groups) |group| {
        if (options.control) |control| try control.check();
        if (group.kind == .classification) continue;
        if (group.kind != .entities and group.kind != .relation) return error.UnsupportedGlinerSpanV2Task;
        const parent = try gather(cb, a, hidden, &.{group.parent_marker}, H);
        defer cb.free(parent);
        const counts_ct = try mlp(cb, a, parent, H, 20, "count_pred");
        defer cb.free(counts_ct);
        const counts = try cb.toFloat32(counts_ct, a);
        var count: usize = 0;
        for (counts, 0..) |value, i| {
            if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
            if (value > counts[count]) count = i;
        }
        const labels = try gather(cb, a, hidden, group.child_markers, H);
        defer cb.free(labels);
        const columns = group.child_markers.len;
        // Entities use only slot zero, but the learned zero-count gate still
        // applies. Relations independently decode each predicted instance.
        for (0..if (group.kind == .entities) @min(count, 1) else count) |instance| {
            const projected = try head.countLstmV1AtStep(cb, a, labels, columns, H, instance);
            defer cb.free(projected);
            const scores_ct = if (cb.kind() == .native)
                try cb.linearNoBias(spans.?, projected, span_count, H, columns)
            else
                try cb.primDotGeneral(spans.?, projected, &.{ @intCast(span_count), @intCast(H) }, &.{ @intCast(columns), @intCast(H) }, &.{1}, &.{1}, &.{}, &.{});
            defer cb.free(scores_ct);
            const scores = try cb.toFloat32(scores_ct, a);
            if (group.kind == .entities) {
                for (compiled.schema.entities, 0..) |entity, column| {
                    const selected = try values(a, sample, scores, columns, column, @floatCast(entity.threshold orelse options.threshold), options);
                    const retained = if (entity.dtype == .str and selected.len > 1) blk: {
                        const single = try a.alloc(pipeline.Value, 1);
                        single[0] = best(selected).?;
                        break :blk single;
                    } else selected;
                    output_values += retained.len;
                    try entities.append(a, .{ .name = entity.name, .dtype = entity.dtype, .values = retained });
                }
            } else {
                const relation = compiled.schema.relations[group.schema_index];
                const threshold: f32 = @floatCast(relation.threshold orelse options.threshold);
                const first = best(try values(a, sample, scores, columns, 0, threshold, options)) orelse continue;
                const second = best(try values(a, sample, scores, columns, 1, threshold, options)) orelse continue;
                try relations.append(a, .{ .name = relation.name, .head = first, .tail = second, .confidence = @min(first.confidence, second.confidence), .schema_index = group.schema_index });
                output_values += 1;
            }
            if (output_values > options.max_output_values) return error.ExtractionOutputLimitExceeded;
        }
        if (group.kind == .entities and count == 0) for (compiled.schema.entities) |entity| {
            try entities.append(a, .{ .name = entity.name, .dtype = entity.dtype, .values = &.{} });
        };
    }
    if (compiled.schema.classifications.len > 0) {
        const positions = try a.alloc(usize, sample.classification_labels.len);
        for (sample.classification_labels, positions) |label, *position| position.* = label.marker_index;
        const rows = try a.alloc([]f64, compiled.schema.classifications.len);
        for (compiled.schema.classifications, rows) |task, *row| row.* = try a.alloc(f64, task.task.labels.len);
        for (sample.classification_labels) |label| {
            const state = try gather(cb, a, hidden, &.{label.marker_index}, H);
            defer cb.free(state);
            const score = try mlp(cb, a, state, H, 1, "classifier");
            defer cb.free(score);
            const raw = try cb.toFloat32(score, a);
            rows[label.schema_index][label.label_index] = raw[0];
        }
        const const_rows = try a.alloc([]const f64, rows.len);
        for (rows, const_rows) |row, *out| out.* = row;
        var presented = try pipeline.presentClassifications(a, compiled, const_rows, 1, options);
        defer presented.deinit();
        // Copy into this result's arena before the presentation arena closes.
        for (presented.classifications) |task| {
            try classifications.append(a, .{ .name = try a.dupe(u8, task.name), .multi_label = task.multi_label, .labels = try a.dupe(pipeline.Label, task.labels) });
        }
    }
    const samples = try a.alloc(pipeline.Sample, 1);
    samples[0] = .{ .entities = entities.items, .relations = relations.items, .classifications = classifications.items };
    return .{ .arena = arena, .samples = samples };
}

/// Shared by production scoring and pinned intermediate-head parity tests.
pub fn spanRepresentations(cb: *const compute.ComputeBackend, a: std.mem.Allocator, hidden: CT, sample: processor.Sample, H: usize) !CT {
    const word_positions = try a.alloc(usize, sample.words.len);
    for (sample.words, word_positions) |word, *position| position.* = word.input_start;
    const words = try gather(cb, a, hidden, word_positions, H);
    defer cb.free(words);
    const span_count = sample.words.len * 8;
    const starts = try a.alloc(u32, span_count);
    const ends = try a.alloc(u32, span_count);
    for (starts, ends, 0..) |*start, *end, i| {
        start.* = @intCast(i / 8);
        end.* = @intCast(@min(i / 8 + i % 8, sample.words.len - 1));
    }
    if (cb.kind() == .native) return head.spanMarkerForwardFromWordCt(cb, a, words, starts, ends, 1, sample.words.len, span_count, H, null);
    // Qualify ModernBERT's wider head with generic device primitives.
    const start = try markerMlp(cb, a, words, sample.words.len, H, H, "span_rep.span_rep_layer.project_start");
    defer cb.free(start);
    const end = try markerMlp(cb, a, words, sample.words.len, H, H, "span_rep.span_rep_layer.project_end");
    defer cb.free(end);
    const first_positions = try a.alloc(usize, span_count);
    const last_positions = try a.alloc(usize, span_count);
    for (starts, ends, first_positions, last_positions) |first, last, *f, *l| {
        f.* = first;
        l.* = last;
    }
    const first = try gather(cb, a, start, first_positions, H);
    defer cb.free(first);
    const last = try gather(cb, a, end, last_positions, H);
    defer cb.free(last);
    const concatenated = try cb.concat(first, last, span_count, H, H);
    defer cb.free(concatenated);
    const activated = try cb.relu(concatenated);
    defer cb.free(activated);
    return markerMlp(cb, a, activated, span_count, H * 2, H, "span_rep.span_rep_layer.out_project");
}

pub fn markerMlp(cb: *const compute.ComputeBackend, a: std.mem.Allocator, input: CT, rows: usize, input_width: usize, H: usize, prefix: []const u8) !CT {
    const w0 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.weight", .{prefix}));
    defer cb.free(w0);
    const b0 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.bias", .{prefix}));
    defer cb.free(b0);
    const linear = try cb.linear(input, w0, b0, rows, input_width, 4 * H);
    defer cb.free(linear);
    const relu = try cb.relu(linear);
    defer cb.free(relu);
    const w3 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.3.weight", .{prefix}));
    defer cb.free(w3);
    const b3 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.3.bias", .{prefix}));
    defer cb.free(b3);
    return cb.linear(relu, w3, b3, rows, 4 * H, H);
}
