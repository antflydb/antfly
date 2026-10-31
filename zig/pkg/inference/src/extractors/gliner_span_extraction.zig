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
    defer a.free(ids);
    for (positions, ids) |position, *id| id.* = @intCast(position);
    return cb.embeddingLookup(hidden, ids, positions.len, H);
}

pub const HeadOptions = struct {
    /// Legacy span artifacts suppress overlapping mentions independently per label.
    confidence_first_greedy: bool = false,
    require_device_gru: bool = false,
};

fn greedy(a: std.mem.Allocator, candidates: []const decode.Candidate, options: pipeline.Options) ![]decode.Candidate {
    const ranked = try a.dupe(decode.Candidate, candidates);
    const Rank = struct {
        fn less(_: void, x: decode.Candidate, y: decode.Candidate) bool {
            if (x.probability != y.probability) return x.probability > y.probability;
            return x.source_index < y.source_index;
        }
    };
    std.mem.sort(decode.Candidate, ranked, {}, Rank.less);
    var kept: usize = 0;
    for (ranked) |candidate| {
        if (options.control) |control| try control.check();
        var overlaps = false;
        for (ranked[0..kept]) |previous| {
            if (candidate.start < previous.end and previous.start < candidate.end) {
                overlaps = true;
                break;
            }
        }
        if (!overlaps) {
            ranked[kept] = candidate;
            kept += 1;
        }
    }
    return ranked[0..kept];
}

fn values(a: std.mem.Allocator, scratch_allocator: std.mem.Allocator, sample: processor.Sample, logits: []const f32, columns: usize, column: usize, threshold: f32, options: pipeline.Options, head_options: HeadOptions) ![]pipeline.Value {
    // Candidate lists and weighted-interval history are disposable per label.
    var scratch = std.heap.ArenaAllocator.init(scratch_allocator);
    defer scratch.deinit();
    const temporary = scratch.allocator();
    const width = 8;
    var candidates: std.ArrayList(decode.Candidate) = .empty;
    for (sample.words, 0..) |first, start| {
        if (options.control) |control| try control.check();
        const begin = first.source orelse continue;
        for (0..@min(width, sample.words.len - start)) |span_width| {
            const last = sample.words[start + span_width].source orelse continue;
            const logit = logits[(start * width + span_width) * columns + column];
            if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
            const probability = 1 / (1 + @exp(-logit));
            if (probability < threshold) continue;
            if (candidates.items.len >= options.query_limits.max_candidates) return error.ExtractionCandidateLimitExceeded;
            try candidates.append(temporary, .{ .start = begin.start, .end = last.end, .probability = probability, .source_index = start * width + span_width });
        }
    }
    const selected = if (head_options.confidence_first_greedy)
        try greedy(temporary, candidates.items, options)
    else
        try decode.resolveOverlaps(temporary, candidates.items, options.overlap, .{ .max_candidates = options.query_limits.max_candidates, .control = options.control });
    var offsets = try decode.OffsetMap.init(temporary, sample.original_text, sample.original_text.len);
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
    const mask = try allocator.alloc(i64, sample.input_ids.len);
    defer allocator.free(mask);
    @memset(mask, 1);
    const hidden = try modern.forwardCT(cb, allocator, config, sample.input_ids, mask, 1, sample.input_ids.len);
    defer cb.free(hidden);
    return runFromHidden(cb, allocator, hidden, config.hidden_size, sample, compiled, options, .{});
}

/// Encoder-independent span/count head. The caller retains ownership of hidden.
pub fn runFromHidden(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, hidden: CT, H: usize, sample: processor.Sample, compiled: *const schema.CompiledSchema, options: pipeline.Options, head_options: HeadOptions) !pipeline.Result {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    if (options.control) |control| try control.check();
    const spans: ?CT = if (compiled.schema.entities.len != 0 or compiled.schema.relations.len != 0) try spanRepresentations(cb, allocator, hidden, sample, H) else null;
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
            const projected = if (head_options.require_device_gru and cb.kind() != .native)
                (try head.countLstmV1SlotZeroResident(cb, labels, columns, H)) orelse return error.UnsupportedExtractionBackend
            else
                try head.countLstmV1AtStep(cb, a, labels, columns, H, instance);
            defer cb.free(projected);
            const scores_ct = if (cb.kind() == .native)
                try cb.linearNoBias(spans.?, projected, span_count, H, columns)
            else
                try cb.primDotGeneral(spans.?, projected, &.{ @intCast(span_count), @intCast(H) }, &.{ @intCast(columns), @intCast(H) }, &.{1}, &.{1}, &.{}, &.{});
            defer cb.free(scores_ct);
            const scores = try cb.toFloat32(scores_ct, allocator);
            defer allocator.free(scores);
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
pub fn spanRepresentations(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, hidden: CT, sample: processor.Sample, H: usize) !CT {
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
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

fn markerMlp(cb: *const compute.ComputeBackend, a: std.mem.Allocator, input: CT, rows: usize, input_width: usize, H: usize, prefix: []const u8) !CT {
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

test "gliner specialty PII confidence greedy differs from explicit flat and preserves ties" {
    const a = std.testing.allocator;
    const candidates = [_]decode.Candidate{
        .{ .start = 0, .end = 8, .probability = 0.9, .source_index = 0 },
        .{ .start = 0, .end = 3, .probability = 0.6, .source_index = 1 },
        .{ .start = 4, .end = 8, .probability = 0.6, .source_index = 2 },
        .{ .start = 9, .end = 10, .probability = 0.5, .source_index = 3 },
        .{ .start = 9, .end = 11, .probability = 0.5, .source_index = 4 },
    };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const g = try greedy(scratch.allocator(), &candidates, .{});
    const flat = try decode.resolveOverlaps(scratch.allocator(), &candidates, .flat, .{});
    try std.testing.expectEqual(@as(usize, 2), g.len);
    try std.testing.expectEqual(@as(usize, 0), g[0].source_index);
    try std.testing.expectEqual(@as(usize, 3), g[1].source_index);
    try std.testing.expectEqual(@as(usize, 3), flat.len);
    try std.testing.expectEqual(@as(usize, 1), flat[0].source_index);
}

test "gliner specialty PII threshold equality cross-label overlap and Unicode offsets" {
    const a = std.testing.allocator;
    const sample = processor.Sample{
        .original_text = "é é",
        .schema_fingerprint = @splat(0),
        .input_ids = &.{ 1, 2 },
        .words = &.{
            .{ .text = "é", .source = .{ .start = 0, .end = 2 }, .input_start = 0, .input_end = 1 },
            .{ .text = "é", .source = .{ .start = 3, .end = 5 }, .input_start = 1, .input_end = 2 },
        },
        .groups = &.{},
        .queries = &.{},
        .classification_labels = &.{},
        .enum_choices = &.{},
        .prefix_word_count = 0,
        .body_word_count = 2,
        .terminal_period_added = false,
        .is_joint_ie = false,
    };
    var logits: [2 * 8 * 2]f32 = @splat(-100);
    // Equal 0.5 probabilities are included at threshold equality. Repeated
    // surfaces retain distinct positions; independently decoded labels overlap.
    logits[0] = 0;
    logits[1] = 0;
    logits[8 * 2] = 0;
    const options = pipeline.Options{ .offset_unit = .unicode_codepoints };
    const first = try values(a, a, sample, &logits, 2, 0, 0.5, options, .{ .confidence_first_greedy = true });
    defer a.free(first);
    const second = try values(a, a, sample, &logits, 2, 1, 0.5, options, .{ .confidence_first_greedy = true });
    defer a.free(second);
    try std.testing.expectEqual(@as(usize, 2), first.len);
    try std.testing.expectEqual(@as(usize, 1), second.len);
    try std.testing.expectEqual(@as(usize, 0), first[0].source.?.start);
    try std.testing.expectEqual(@as(usize, 1), first[0].source.?.end);
    try std.testing.expectEqual(@as(usize, 2), first[1].source.?.start);
    try std.testing.expectEqual(@as(usize, 3), first[1].source.?.end);
    try std.testing.expectEqualStrings("é", first[1].text);
}

const CountGateFixture = struct {
    const native = @import("../ops/native_compute.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;

    fn weight(store: *native.WeightStore, name: []const u8, rows: usize, cols: usize, diagonal: ?usize, hot: ?usize) !void {
        const a = store.allocator;
        const data = try a.alloc(f32, rows * cols);
        defer a.free(data);
        @memset(data, 0);
        if (diagonal) |offset| for (0..@min(rows, cols - offset)) |i| {
            data[i * cols + i + offset] = 1;
        };
        if (hot) |index| data[index] = 10;
        const key = try a.dupe(u8, name);
        errdefer a.free(key);
        const matrix = [_]i64{ @intCast(rows), @intCast(cols) };
        const vector = [_]i64{@intCast(rows)};
        var tensor = try Tensor.initFloat32(a, key, if (cols == 1) &vector else &matrix, data);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, key, .{ .tensor = tensor });
    }

    fn linear(store: *native.WeightStore, prefix: []const u8, rows: usize, cols: usize, diagonal: ?usize, hot: ?usize) !void {
        var name: [192]u8 = undefined;
        try weight(store, try std.fmt.bufPrint(&name, "{s}.weight", .{prefix}), rows, cols, diagonal, null);
        try weight(store, try std.fmt.bufPrint(&name, "{s}.bias", .{prefix}), rows, 1, null, hot);
    }

    fn init(a: std.mem.Allocator, count: usize) !native.WeightStore {
        var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        errdefer store.deinitOwned();
        inline for (.{ "project_start", "project_end", "out_project" }) |component| {
            try linear(&store, "span_rep.span_rep_layer." ++ component ++ ".0", 8, if (std.mem.eql(u8, component, "out_project")) 4 else 2, if (std.mem.eql(u8, component, "project_end")) null else 0, null);
            try linear(&store, "span_rep.span_rep_layer." ++ component ++ ".3", 2, 8, 0, null);
        }
        try linear(&store, "count_pred.0", 4, 2, null, null);
        try linear(&store, "count_pred.2", 20, 4, null, count);
        try weight(&store, "count_embed.pos_embedding.weight", 20, 2, null, null);
        try weight(&store, "count_embed.gru.weight_ih_l0", 6, 2, null, null);
        try weight(&store, "count_embed.gru.weight_hh_l0", 6, 2, null, null);
        try weight(&store, "count_embed.gru.bias_ih_l0", 6, 1, null, null);
        try weight(&store, "count_embed.gru.bias_hh_l0", 6, 1, null, null);
        // Retain the raw label half of concat([GRU, label]). If the executor
        // accidentally uses count as a mention cap, two spans with count=1 fail.
        try linear(&store, "count_embed.projector.0", 8, 4, 2, null);
        try linear(&store, "count_embed.projector.2", 2, 8, 0, null);
        return store;
    }
};

test "gliner specialty PII zero count gates spans and positive count never caps mentions" {
    const a = std.testing.allocator;
    var compiled = try schema.compile(a, "{\"entities\":[\"person\"]}", .{ .schema_version = 2 });
    defer compiled.deinit();
    const sample = processor.Sample{
        .original_text = "a b",
        .schema_fingerprint = @splat(0),
        .input_ids = &.{ 0, 1, 2, 3 },
        .words = &.{
            .{ .text = "a", .source = .{ .start = 0, .end = 1 }, .input_start = 2, .input_end = 3 },
            .{ .text = "b", .source = .{ .start = 2, .end = 3 }, .input_start = 3, .input_end = 4 },
        },
        .groups = &.{.{ .kind = .entities, .schema_index = 0, .name = "entities", .model_name = "entities", .fragments = &.{}, .parent_marker = 0, .child_markers = &.{1} }},
        .queries = &.{},
        .classification_labels = &.{},
        .enum_choices = &.{},
        .prefix_word_count = 0,
        .body_word_count = 2,
        .terminal_period_added = false,
        .is_joint_ie = false,
    };
    for ([_]usize{ 0, 1, 2, 19 }) |count| {
        var store = try CountGateFixture.init(a, count);
        defer store.deinitOwned();
        var native = CountGateFixture.native.NativeCompute.init(a, &store, null);
        defer native.deinit();
        const cb = native.computeBackend();
        const hidden = try cb.fromFloat32Shape(&.{ 0, 0, 1, 0, 1, 0, 1, 0 }, &.{ 4, 2 });
        defer cb.free(hidden);
        var result = try runFromHidden(&cb, a, hidden, 2, sample, &compiled, .{}, .{ .confidence_first_greedy = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples[0].entities.len);
        const mentions = result.samples[0].entities[0].values;
        try std.testing.expectEqual(@as(usize, if (count == 0) 0 else 2), mentions.len);
        if (count != 0) {
            try std.testing.expectEqualStrings("a", mentions[0].text);
            try std.testing.expectEqualStrings("b", mentions[1].text);
        }
    }
}

fn oracleNumber(value: std.json.Value) f32 {
    return switch (value) {
        .float => |x| @floatCast(x),
        .integer => |x| @floatFromInt(x),
        else => std.math.nan(f32),
    };
}

test "gliner specialty frozen upstream decoder oracle covers greedy thresholds count and Unicode" {
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, "specialty/decoder_oracle.json");
    defer a.free(bytes);
    try std.testing.expectEqual(@as(usize, 8388), bytes.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings("9516086e768d91c4e010e5562ce39f40769bc95831ed3ca0fcb809eec6a98fe8", &hex);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const policies = [_]struct { key: []const u8, policy: ?decode.OverlapPolicy }{
        .{ .key = "omitted_overlap", .policy = null },
        .{ .key = "explicit_flat", .policy = .flat },
        .{ .key = "explicit_allow", .policy = .allow },
    };
    for (parsed.value.object.get("overlap_cases").?.array.items) |case| {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const raw = case.object.get("candidates").?.array.items;
        const candidates = try temporary.alloc(decode.Candidate, raw.len);
        for (raw, candidates, 0..) |row, *candidate, i| {
            const fields = row.array.items;
            candidate.* = .{ .start = @intCast(fields[2].integer), .end = @intCast(fields[3].integer), .probability = oracleNumber(fields[1]), .source_index = i };
        }
        for (policies) |policy| {
            const actual = if (policy.policy) |explicit|
                try decode.resolveOverlaps(temporary, candidates, explicit, .{})
            else
                try greedy(temporary, candidates, .{});
            const expected = case.object.get(policy.key).?.array.items;
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |want, got| {
                const fields = want.array.items;
                try std.testing.expectEqualStrings(fields[0].string, raw[got.source_index].array.items[0].string);
                try std.testing.expectEqual(@as(usize, @intCast(fields[2].integer)), got.start);
                try std.testing.expectEqual(@as(usize, @intCast(fields[3].integer)), got.end);
                try std.testing.expectEqual(oracleNumber(fields[1]), got.probability);
            }
        }
    }
    for (parsed.value.object.get("count_threshold_unicode_cases").?.array.items) |case| {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const text = case.object.get("text").?.string;
        const points = try temporary.alloc(usize, 1 + try std.unicode.utf8CountCodepoints(text));
        var position: usize = 0;
        for (points[0 .. points.len - 1]) |*point| {
            point.* = position;
            position += try std.unicode.utf8ByteSequenceLength(text[position]);
        }
        points[points.len - 1] = text.len;
        const starts = case.object.get("start_mapping").?.array.items;
        const ends = case.object.get("end_mapping").?.array.items;
        const words = try temporary.alloc(processor.Word, starts.len);
        for (starts, ends, words) |start, end, *word| {
            const first = points[@intCast(start.integer)];
            const last = points[@intCast(end.integer)];
            word.* = .{ .text = text[first..last], .source = .{ .start = first, .end = last }, .input_start = 0, .input_end = 0 };
        }
        var sample = std.mem.zeroes(processor.Sample);
        sample.original_text = text;
        sample.words = words;
        const raw_logits = case.object.get("span_logits").?.array.items;
        const logits = try temporary.alloc(f32, words.len * 8);
        for (raw_logits, 0..) |row, i| for (row.array.items, logits[i * 8 ..][0..8]) |raw, *logit| {
            logit.* = oracleNumber(raw);
        };
        const expected_entities = case.object.get("expected").?.object.get("entities").?.object;
        for ([_][]const u8{ "person", "alias" }) |label| {
            // The production gate itself is exercised with native learned
            // count-head fixtures above. This fixture freezes its decoder edge.
            const actual = if (case.object.get("predicted_count").?.integer == 0)
                try temporary.alloc(pipeline.Value, 0)
            else
                try values(temporary, a, sample, logits, 1, 0, oracleNumber(case.object.get("threshold").?), .{ .offset_unit = .unicode_codepoints }, .{ .confidence_first_greedy = true });
            const expected = if (expected_entities.get(label)) |rows| rows.array.items else &.{};
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |want, got| {
                try std.testing.expectEqualStrings(want.object.get("text").?.string, got.text);
                try std.testing.expectEqual(@as(usize, @intCast(want.object.get("start").?.integer)), got.source.?.start);
                try std.testing.expectEqual(@as(usize, @intCast(want.object.get("end").?.integer)), got.source.?.end);
                try std.testing.expectEqual(oracleNumber(want.object.get("confidence").?), got.confidence);
            }
        }
    }
}
