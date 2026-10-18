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

//! Immutable features from the frozen ModernBERT span model. Caches retain
//! source model/data identities; a training run never silently changes its
//! encoder or the non-selected head layers.
const std = @import("std");
const ops = @import("../../ops/ops.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const modern = @import("../../architectures/modern_bert.zig");
const core = @import("../../extractors/gliner_span_extraction.zig");
const head = @import("../../architectures/gliner_head.zig");
const schema = @import("../../pipelines/extraction_schema.zig");
const offsets = @import("../../pipelines/gliner_boundary_decode.zig");
const training = @import("head.zig");
const A = std.mem.Allocator;
const CT = ops.CT;
pub const Entity = struct { label: []const u8, start: usize, end: usize };
pub const Span = struct { start: usize, end: usize };
pub const Relation = struct { type: []const u8, head: Span, tail: Span };
pub const Classification = struct { task: []const u8, label: []const u8, probabilities: ?[]const f32 = null };
pub const Record = struct {
    id: []const u8,
    text: []const u8,
    schema_json: []const u8,
    offset_unit: offsets.OffsetUnit = .unicode_codepoints,
    entities: []const Entity = &.{},
    relations: []const Relation = &.{},
    classifications: []const Classification = &.{},
};
pub const Cached = struct {
    record_id: []const u8,
    prefix: []const u8,
    layout: training.Layout,
    example: training.Example,
    objective: training.Objective,
    gold_index: ?usize = null,
};

pub fn activated(cb: *const ops.ComputeBackend, a: A, value: CT, rows: usize, input: usize, output: usize, prefix: []const u8) !CT {
    const w = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.weight", .{prefix}));
    defer cb.free(w);
    const bias = try cb.getWeight(try std.fmt.allocPrint(a, "{s}.0.bias", .{prefix}));
    defer cb.free(bias);
    const z = try cb.linear(value, w, bias, rows, input, output);
    defer cb.free(z);
    return cb.relu(z);
}

fn spanFeatures(cb: *const ops.ComputeBackend, a: A, hidden: CT, sample: processor.Sample, H: usize) ![]f32 {
    const positions = try a.alloc(usize, sample.words.len);
    for (sample.words, positions) |word, *p| p.* = word.input_start;
    const words = try core.gather(cb, a, hidden, positions, H);
    defer cb.free(words);
    const first = try core.markerMlp(cb, a, words, sample.words.len, H, H, "span_rep.span_rep_layer.project_start");
    defer cb.free(first);
    const last = try core.markerMlp(cb, a, words, sample.words.len, H, H, "span_rep.span_rep_layer.project_end");
    defer cb.free(last);
    const n = sample.words.len * 8;
    const starts = try a.alloc(usize, n);
    const ends = try a.alloc(usize, n);
    for (starts, ends, 0..) |*start, *end, i| {
        start.* = i / 8;
        end.* = @min(i / 8 + i % 8, sample.words.len - 1);
    }
    const start = try core.gather(cb, a, first, starts, H);
    defer cb.free(start);
    const end = try core.gather(cb, a, last, ends, H);
    defer cb.free(end);
    const joined = try cb.concat(start, end, n, H, H);
    defer cb.free(joined);
    const positive = try cb.relu(joined);
    defer cb.free(positive);
    const features = try activated(cb, a, positive, n, 2 * H, 4 * H, "span_rep.span_rep_layer.out_project");
    defer cb.free(features);
    return cb.toFloat32(features, a);
}

fn mark(a: A, sample: processor.Sample, record: Record, target: []f32, columns: usize, column: usize, span: Span) !void {
    var map = try offsets.OffsetMap.init(a, record.text, record.text.len);
    defer map.deinit();
    const bytes = try map.toBytes(.{ .start = span.start, .end = span.end }, record.offset_unit);
    if (bytes.start >= bytes.end) return error.InvalidSpanTrainingAnnotation;
    for (sample.words, 0..) |word, i| {
        const start = word.source orelse continue;
        if (start.start != bytes.start) continue;
        for (0..@min(@as(usize, 8), sample.words.len - i)) |width| {
            const end = sample.words[i + width].source orelse continue;
            if (end.end == bytes.end) {
                target[(i * 8 + width) * columns + column] = 1;
                return;
            }
        }
    }
    return error.UnalignedSpanTrainingAnnotation;
}

fn validMask(a: A, sample: processor.Sample, columns: usize) ![]bool {
    const result = try a.alloc(bool, sample.words.len * 8 * columns);
    @memset(result, false);
    for (sample.words, 0..) |word, i| {
        if (word.source == null) continue;
        for (0..@min(@as(usize, 8), sample.words.len - i)) |width| {
            if (sample.words[i + width].source == null) continue;
            @memset(result[(i * 8 + width) * columns ..][0..columns], true);
        }
    }
    return result;
}

/// The caller's cache arena owns every result and compiled/prepared input.
pub fn prepare(a: A, cb: *const ops.ComputeBackend, config: modern.Config, sample: processor.Sample, compiled: *const schema.CompiledSchema, record: Record) ![]Cached {
    const H: usize = config.hidden_size;
    const mask = try a.alloc(i64, sample.input_ids.len);
    @memset(mask, 1);
    const hidden = try modern.forwardCT(cb, a, config, sample.input_ids, mask, 1, sample.input_ids.len);
    defer cb.free(hidden);
    var cached: std.ArrayList(Cached) = .empty;
    var spans: ?[]f32 = null;
    for (sample.groups) |group| {
        if (group.kind == .classification) {
            const task = compiled.schema.classifications[group.schema_index].task;
            var gold: ?[]const u8 = null;
            var probabilities: ?[]const f32 = null;
            for (record.classifications) |target| if (std.mem.eql(u8, target.task, task.name)) {
                gold = target.label;
                probabilities = target.probabilities;
            };
            const label = gold orelse continue;
            const target = try a.alloc(f32, task.labels.len);
            @memset(target, 0);
            var found = false;
            var gold_index: ?usize = null;
            for (task.labels, target, 0..) |name, *t, i| if (std.mem.eql(u8, label, name)) {
                t.* = 1;
                found = true;
                gold_index = i;
            };
            if (!found) return error.InvalidSpanTrainingAnnotation;
            if (probabilities) |values| {
                if (values.len != target.len) return error.InvalidSpanTrainingAnnotation;
                @memcpy(target, values);
            }
            const states = try core.gather(cb, a, hidden, group.child_markers, H);
            defer cb.free(states);
            const features = try activated(cb, a, states, task.labels.len, H, 2 * H, "classifier");
            defer cb.free(features);
            try cached.append(a, .{ .record_id = record.id, .prefix = "classifier.2", .gold_index = gold_index, .layout = .{ .rows = @intCast(task.labels.len), .features = @intCast(2 * H), .outputs = 1 }, .example = .{ .features = try cb.toFloat32(features, a), .targets = target }, .objective = if (probabilities != null) .binary else .exclusive });
            continue;
        }
        if (group.kind != .entities and group.kind != .relation) return error.UnsupportedGlinerSpanV2Task;
        var instances: std.ArrayList(Relation) = .empty;
        if (group.kind == .relation) {
            const name = compiled.schema.relations[group.schema_index].name;
            for (record.relations) |relation| if (std.mem.eql(u8, name, relation.type)) {
                try instances.append(a, relation);
            };
            if (instances.items.len > 19) return error.InvalidSpanTrainingAnnotation;
        }
        {
            const parent = try core.gather(cb, a, hidden, &.{group.parent_marker}, H);
            defer cb.free(parent);
            const features = try activated(cb, a, parent, 1, H, 2 * H, "count_pred");
            defer cb.free(features);
            const target = try a.alloc(f32, 20);
            @memset(target, 0);
            target[if (group.kind == .entities) @as(usize, @intFromBool(record.entities.len != 0)) else instances.items.len] = 1;
            try cached.append(a, .{ .record_id = record.id, .prefix = "count_pred.2", .layout = .{ .rows = 1, .features = @intCast(2 * H), .outputs = 20 }, .example = .{ .features = try cb.toFloat32(features, a), .targets = target }, .objective = .exclusive });
        }
        if (spans == null) spans = try spanFeatures(cb, a, hidden, sample, H);
        const labels = try core.gather(cb, a, hidden, group.child_markers, H);
        defer cb.free(labels);
        const columns = group.child_markers.len;
        const count = if (group.kind == .entities) 1 else instances.items.len;
        for (0..count) |instance| {
            const projected = try head.countLstmV1AtStep(cb, a, labels, columns, H, instance);
            defer cb.free(projected);
            const target = try a.alloc(f32, sample.words.len * 8 * columns);
            @memset(target, 0);
            if (group.kind == .entities) {
                for (record.entities) |entity| {
                    var found = false;
                    for (compiled.schema.entities, 0..) |kind, column| if (std.mem.eql(u8, entity.label, kind.name)) {
                        try mark(a, sample, record, target, columns, column, .{ .start = entity.start, .end = entity.end });
                        found = true;
                    };
                    if (!found) return error.InvalidSpanTrainingAnnotation;
                }
            } else {
                try mark(a, sample, record, target, columns, 0, instances.items[instance].head);
                try mark(a, sample, record, target, columns, 1, instances.items[instance].tail);
            }
            try cached.append(a, .{ .record_id = record.id, .prefix = "span_rep.span_rep_layer.out_project.3", .layout = .{ .rows = @intCast(sample.words.len * 8), .features = @intCast(4 * H), .outputs = @intCast(H), .columns = @intCast(columns) }, .example = .{ .features = spans.?, .projection = try cb.toFloat32(projected, a), .targets = target, .mask = try validMask(a, sample, columns) }, .objective = .binary });
        }
    }
    return cached.toOwnedSlice(a);
}
