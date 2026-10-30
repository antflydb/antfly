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

pub const HeadOptions = struct {
    /// Exact markerV0 geometry. Specialty profiles set this from authenticated
    /// wrapper metadata; the first supported profile is width eight.
    max_width: usize = 8,
    /// Historical span checkpoints greedily suppress overlapping mentions in
    /// descending-confidence order when the caller omits an overlap option.
    legacy_greedy_overlap: bool = false,
};

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
    defer a.free(ids);
    for (positions, ids) |position, *id| id.* = @intCast(position);
    return cb.embeddingLookup(hidden, ids, positions.len, H);
}

fn greedyOverlaps(a: std.mem.Allocator, input: []const decode.Candidate, control: ?@import("../execution_control.zig").InferenceExecutionControl) ![]decode.Candidate {
    const ranked = try a.dupe(decode.Candidate, input);
    defer a.free(ranked);
    const Order = struct {
        fn less(_: void, left: decode.Candidate, right: decode.Candidate) bool {
            if (left.probability != right.probability) return left.probability > right.probability;
            return left.source_index < right.source_index;
        }
    };
    std.mem.sort(decode.Candidate, ranked, {}, Order.less);
    var selected: std.ArrayList(decode.Candidate) = .empty;
    errdefer selected.deinit(a);
    for (ranked) |candidate| {
        if (control) |value| try value.check();
        var overlaps = false;
        for (selected.items) |existing| {
            if (candidate.start < existing.end and existing.start < candidate.end) {
                overlaps = true;
                break;
            }
        }
        if (!overlaps) try selected.append(a, candidate);
    }
    return selected.toOwnedSlice(a);
}

fn values(result_a: std.mem.Allocator, scratch_a: std.mem.Allocator, sample: processor.Sample, logits: []const f32, columns: usize, column: usize, threshold: f32, options: pipeline.Options, head_options: HeadOptions) ![]pipeline.Value {
    const width = head_options.max_width;
    var candidates: std.ArrayList(decode.Candidate) = .empty;
    defer candidates.deinit(scratch_a);
    for (sample.words, 0..) |first, start| {
        const begin = first.source orelse continue;
        for (0..@min(width, sample.words.len - start)) |span_width| {
            const last = sample.words[start + span_width].source orelse continue;
            const logit = logits[(start * width + span_width) * columns + column];
            if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
            const probability = 1 / (1 + @exp(-logit));
            if (probability < threshold) continue;
            if (candidates.items.len >= options.query_limits.max_candidates) return error.ExtractionCandidateLimitExceeded;
            try candidates.append(scratch_a, .{ .start = begin.start, .end = last.end, .probability = probability, .source_index = start * width + span_width });
        }
    }
    const selected = if (head_options.legacy_greedy_overlap)
        try greedyOverlaps(scratch_a, candidates.items, options.control)
    else
        try decode.resolveOverlaps(scratch_a, candidates.items, options.overlap, .{ .max_candidates = options.query_limits.max_candidates, .control = options.control });
    defer scratch_a.free(selected);
    var offsets = try decode.OffsetMap.init(scratch_a, sample.original_text, sample.original_text.len);
    defer offsets.deinit();
    const result = try result_a.alloc(pipeline.Value, selected.len);
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
    if (options.control) |control| try control.check();
    const H: usize = config.hidden_size;
    const mask = try allocator.alloc(i64, sample.input_ids.len);
    defer allocator.free(mask);
    @memset(mask, 1);
    const hidden = try modern.forwardCT(cb, allocator, config, sample.input_ids, mask, 1, sample.input_ids.len);
    defer cb.free(hidden);
    return runFromHidden(cb, allocator, hidden, H, sample, compiled, options, .{});
}

pub fn runFromHidden(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, hidden: CT, H: usize, sample: processor.Sample, compiled: *const schema.CompiledSchema, options: pipeline.Options, head_options: HeadOptions) !pipeline.Result {
    if (head_options.max_width != 8) return error.UnsupportedGlinerSpanV2Task;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    if (options.control) |control| try control.check();
    const spans: ?CT = if (compiled.schema.entities.len != 0 or compiled.schema.relations.len != 0) try spanRepresentationsWidth(cb, a, hidden, sample, H, head_options.max_width) else null;
    defer if (spans) |value| cb.free(value);
    const span_count = try std.math.mul(usize, sample.words.len, head_options.max_width);
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
                    const selected = try values(a, allocator, sample, scores, columns, column, @floatCast(entity.threshold orelse options.threshold), options, head_options);
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
                const first = best(try values(a, allocator, sample, scores, columns, 0, threshold, options, head_options)) orelse continue;
                const second = best(try values(a, allocator, sample, scores, columns, 1, threshold, options, head_options)) orelse continue;
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
    return spanRepresentationsWidth(cb, a, hidden, sample, H, 8);
}

pub fn spanRepresentationsWidth(cb: *const compute.ComputeBackend, a: std.mem.Allocator, hidden: CT, sample: processor.Sample, H: usize, max_width: usize) !CT {
    const word_positions = try a.alloc(usize, sample.words.len);
    for (sample.words, word_positions) |word, *position| position.* = word.input_start;
    const span_count = try std.math.mul(usize, sample.words.len, max_width);
    const starts = try a.alloc(u32, span_count);
    const ends = try a.alloc(u32, span_count);
    const first_positions = try a.alloc(usize, span_count);
    const last_positions = try a.alloc(usize, span_count);
    for (starts, ends, 0..) |*start, *end, i| {
        start.* = @intCast(i / max_width);
        end.* = @intCast(@min(i / max_width + i % max_width, sample.words.len - 1));
    }
    for (starts, ends, first_positions, last_positions) |first, last, *f, *l| {
        f.* = first;
        l.* = last;
    }
    const words = try gather(cb, a, hidden, word_positions, H);
    if (cb.kind() == .native) {
        defer cb.free(words);
        return head.spanMarkerForwardFromWordCt(cb, a, words, starts, ends, 1, sample.words.len, span_count, H, null);
    }
    // Qualify ModernBERT's wider head with generic device primitives.
    const projections = blk: {
        defer cb.free(words);
        const start = try markerMlp(cb, a, words, sample.words.len, H, H, "span_rep.span_rep_layer.project_start");
        errdefer cb.free(start);
        const end = try markerMlp(cb, a, words, sample.words.len, H, H, "span_rep.span_rep_layer.project_end");
        break :blk .{ .start = start, .end = end };
    };
    const gathered = blk: {
        defer cb.free(projections.start);
        defer cb.free(projections.end);
        const first = try gather(cb, a, projections.start, first_positions, H);
        errdefer cb.free(first);
        const last = try gather(cb, a, projections.end, last_positions, H);
        break :blk .{ .first = first, .last = last };
    };
    const concatenated = blk: {
        defer cb.free(gathered.first);
        defer cb.free(gathered.last);
        break :blk try cb.concat(gathered.first, gathered.last, span_count, H, H);
    };
    const activated = blk: {
        defer cb.free(concatenated);
        break :blk try cb.relu(concatenated);
    };
    defer cb.free(activated);
    return markerMlp(cb, a, activated, span_count, H * 2, H, "span_rep.span_rep_layer.out_project");
}

fn markerMlp(cb: *const compute.ComputeBackend, a: std.mem.Allocator, input: CT, rows: usize, input_width: usize, H: usize, prefix: []const u8) !CT {
    const w0 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.weight", .{prefix}));
    defer cb.free(w0);
    const b0 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.bias", .{prefix}));
    defer cb.free(b0);
    const relu = blk: {
        const linear = try cb.linear(input, w0, b0, rows, input_width, 4 * H);
        defer cb.free(linear);
        break :blk try cb.relu(linear);
    };
    defer cb.free(relu);
    const w3 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.3.weight", .{prefix}));
    defer cb.free(w3);
    const b3 = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.3.bias", .{prefix}));
    defer cb.free(b3);
    return cb.linear(relu, w3, b3, rows, 4 * H, H);
}

test "legacy span overlap is confidence-first greedy rather than weighted flat" {
    var greedy_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer greedy_arena.deinit();
    const candidates = [_]decode.Candidate{
        .{ .start = 0, .end = 10, .probability = 0.9, .source_index = 0 },
        .{ .start = 0, .end = 5, .probability = 0.6, .source_index = 1 },
        .{ .start = 5, .end = 10, .probability = 0.6, .source_index = 2 },
    };
    const greedy = try greedyOverlaps(greedy_arena.allocator(), &candidates, null);
    try std.testing.expectEqual(@as(usize, 1), greedy.len);
    try std.testing.expectEqual(@as(usize, 0), greedy[0].source_index);

    var flat_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer flat_arena.deinit();
    const flat = try decode.resolveOverlaps(flat_arena.allocator(), &candidates, .flat, .{});
    try std.testing.expectEqual(@as(usize, 2), flat.len);
    try std.testing.expectEqual(@as(usize, 1), flat[0].source_index);
    try std.testing.expectEqual(@as(usize, 2), flat[1].source_index);
}

test "legacy span overlap ties retain source enumeration order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const candidates = [_]decode.Candidate{
        .{ .start = 1, .end = 4, .probability = 0.5, .source_index = 7 },
        .{ .start = 0, .end = 3, .probability = 0.5, .source_index = 2 },
    };
    const selected = try greedyOverlaps(arena.allocator(), &candidates, null);
    try std.testing.expectEqual(@as(usize, 1), selected.len);
    try std.testing.expectEqual(@as(usize, 2), selected[0].source_index);
}

const SpanV1CountFixture = struct {
    const native = @import("../ops/native_compute.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;

    fn weight(store: *native.WeightStore, name: []const u8, rows: usize, cols: usize, vector: bool) !void {
        const a = store.allocator;
        const data = try a.alloc(f32, rows * cols);
        defer a.free(data);
        @memset(data, 0);
        const key = try a.dupe(u8, name);
        errdefer a.free(key);
        const matrix_shape = [_]i64{ @intCast(rows), @intCast(cols) };
        const vector_shape = [_]i64{@intCast(rows)};
        var tensor = try Tensor.initFloat32(a, key, if (vector) &vector_shape else &matrix_shape, data);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, key, .{ .tensor = tensor });
    }

    fn countBias(store: *native.WeightStore, selected: usize) !void {
        const a = store.allocator;
        var data: [20]f32 = @splat(0);
        data[selected] = 1;
        const name = "count_pred.2.bias";
        const key = try a.dupe(u8, name);
        errdefer a.free(key);
        const shape = [_]i64{20};
        var tensor = try Tensor.initFloat32(a, key, &shape, &data);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, key, .{ .tensor = tensor });
    }

    fn init(a: std.mem.Allocator, selected_count: usize) !native.WeightStore {
        var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        errdefer store.deinitOwned();
        inline for (.{ "project_start", "project_end", "out_project" }) |component| {
            try weight(&store, "span_rep.span_rep_layer." ++ component ++ ".0.weight", 8, if (std.mem.eql(u8, component, "out_project")) 4 else 2, false);
            try weight(&store, "span_rep.span_rep_layer." ++ component ++ ".0.bias", 8, 1, true);
            try weight(&store, "span_rep.span_rep_layer." ++ component ++ ".3.weight", 2, 8, false);
            try weight(&store, "span_rep.span_rep_layer." ++ component ++ ".3.bias", 2, 1, true);
        }
        try weight(&store, "count_pred.0.weight", 4, 2, false);
        try weight(&store, "count_pred.0.bias", 4, 1, true);
        try weight(&store, "count_pred.2.weight", 20, 4, false);
        try countBias(&store, selected_count);
        try weight(&store, "count_embed.pos_embedding.weight", 20, 2, false);
        try weight(&store, "count_embed.gru.weight_ih_l0", 6, 2, false);
        try weight(&store, "count_embed.gru.weight_hh_l0", 6, 2, false);
        try weight(&store, "count_embed.gru.bias_ih_l0", 6, 1, true);
        try weight(&store, "count_embed.gru.bias_hh_l0", 6, 1, true);
        try weight(&store, "count_embed.projector.0.weight", 8, 4, false);
        try weight(&store, "count_embed.projector.0.bias", 8, 1, true);
        try weight(&store, "count_embed.projector.2.weight", 2, 8, false);
        try weight(&store, "count_embed.projector.2.bias", 2, 1, true);
        return store;
    }

    const WeightProbe = struct {
        threadlocal var original: ?*const fn (*anyopaque, []const u8) anyerror!CT = null;
        threadlocal var position_lookups: usize = 0;
        threadlocal var gru_lookups: usize = 0;

        fn reset(callback: *const fn (*anyopaque, []const u8) anyerror!CT) void {
            original = callback;
            position_lookups = 0;
            gru_lookups = 0;
        }

        fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
            if (std.mem.eql(u8, name, "count_embed.pos_embedding.weight")) position_lookups += 1;
            if (std.mem.startsWith(u8, name, "count_embed.gru.")) gru_lookups += 1;
            return original.?(ctx, name);
        }
    };
};

test "GLiNER specialty v1 real span run honors zero count and entity slot zero semantics" {
    const a = std.testing.allocator;
    var compiled = try schema.compile(a,
        \\{"entities":["thing"]}
    , .{});
    defer compiled.deinit();
    const words = [_]processor.Word{
        .{ .text = "aa", .source = .{ .start = 0, .end = 2 }, .input_start = 0, .input_end = 1 },
        .{ .text = "bb", .source = .{ .start = 3, .end = 5 }, .input_start = 1, .input_end = 2 },
        .{ .text = "cc", .source = .{ .start = 6, .end = 8 }, .input_start = 2, .input_end = 3 },
    };
    const groups = [_]processor.Group{.{
        .kind = .entities,
        .schema_index = 0,
        .name = "entities",
        .model_name = "entities",
        .fragments = &.{},
        .parent_marker = 3,
        .child_markers = &.{4},
    }};
    const sample = processor.Sample{
        .original_text = "aa bb cc",
        .schema_fingerprint = compiled.fingerprint,
        .input_ids = &.{ 0, 0, 0, 0, 0 },
        .words = &words,
        .groups = &groups,
        .queries = &.{},
        .classification_labels = &.{},
        .enum_choices = &.{},
        .prefix_word_count = 0,
        .body_word_count = words.len,
        .terminal_period_added = false,
        .is_joint_ie = false,
    };
    const hidden_values: [10]f32 = @splat(0);
    const hidden_shape = [_]i32{ 5, 2 };

    for ([_]struct { count: usize, threshold: f32, expected_values: usize, expected_gru: usize }{
        .{ .count = 0, .threshold = 0.5, .expected_values = 0, .expected_gru = 0 },
        .{ .count = 2, .threshold = 0.5, .expected_values = 3, .expected_gru = 4 },
        .{ .count = 2, .threshold = 0.5001, .expected_values = 0, .expected_gru = 4 },
    }) |case| {
        var store = try SpanV1CountFixture.init(a, case.count);
        defer store.deinitOwned();
        var native_compute = SpanV1CountFixture.native.NativeCompute.init(a, &store, null);
        defer native_compute.deinit();
        var cb = native_compute.computeBackend();
        var vtable = cb.vtable.*;
        SpanV1CountFixture.WeightProbe.reset(vtable.getWeight);
        vtable.getWeight = SpanV1CountFixture.WeightProbe.getWeight;
        cb.vtable = &vtable;
        const hidden = try cb.fromFloat32Shape(&hidden_values, &hidden_shape);
        defer cb.free(hidden);
        var result = try runFromHidden(&cb, a, hidden, 2, sample, &compiled, .{ .threshold = case.threshold }, .{ .legacy_greedy_overlap = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try std.testing.expectEqual(@as(usize, 1), result.samples[0].entities.len);
        try std.testing.expectEqual(case.expected_values, result.samples[0].entities[0].values.len);
        try std.testing.expectEqual(case.expected_gru, SpanV1CountFixture.WeightProbe.gru_lookups);
        try std.testing.expectEqual(@as(usize, @intFromBool(case.count != 0)), SpanV1CountFixture.WeightProbe.position_lookups);
        if (case.expected_values == 3) {
            try std.testing.expectEqualStrings("aa", result.samples[0].entities[0].values[0].text);
            try std.testing.expectEqualStrings("bb", result.samples[0].entities[0].values[1].text);
            try std.testing.expectEqualStrings("cc", result.samples[0].entities[0].values[2].text);
            for (result.samples[0].entities[0].values) |value| try std.testing.expectEqual(@as(f32, 0.5), value.confidence);
        }
    }
}
