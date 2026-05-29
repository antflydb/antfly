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

const cleanup_data = @import("entity_cleanup_data.zig");
const cleanup_model = @import("entity_cleanup_model.zig");
const gliner2_boundary = @import("gliner2_boundary.zig");
const gliner2_data = @import("gliner2_data.zig");
const text_encoder_boundary = @import("text_encoder_boundary.zig");

pub const cache_family_version = "entity_cleanup_gliner_cache/v1alpha1";

const WordSpan = struct {
    start: usize,
    end: usize,
};

const WordRange = struct {
    start: usize,
    end: usize,
};

pub fn prepareCachedSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8,
    examples: []const cleanup_data.Example,
    backend: text_encoder_boundary.BackendChoice,
    max_examples: usize,
    max_length: usize,
    max_span_width: usize,
    top_layer_count: usize,
) !cleanup_model.CachedSummary {
    const effective = examples[0..@min(examples.len, max_examples)];
    const entity_types = try collectUniqueLabels(allocator, effective);
    defer freeStringSlice(allocator, entity_types);

    const gliner_examples = try convertCleanupExamples(allocator, effective);
    defer freeGlinerExamples(allocator, gliner_examples);

    var boundary_summary = try gliner2_boundary.prepareCachedBoundarySummary(
        allocator,
        model_dir,
        input_path,
        split,
        gliner_examples,
        entity_types,
        backend,
        effective.len,
        max_length,
        max_span_width,
        top_layer_count,
    );
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &boundary_summary);

    return try prepareCachedSummaryFromBoundarySummary(allocator, input_path, split, effective, &boundary_summary);
}

pub fn prepareCachedSummaryFromBoundarySummary(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    split: ?[]const u8,
    examples: []const cleanup_data.Example,
    boundary_summary: *const gliner2_boundary.CachedBoundarySummary,
) !cleanup_model.CachedSummary {
    if (examples.len != boundary_summary.examples.len) return error.ShapeMismatch;

    var mentions = std.ArrayListUnmanaged(cleanup_model.CachedMentionSummary).empty;
    errdefer {
        for (mentions.items) |*mention| freeCachedMentionSummary(allocator, mention);
        mentions.deinit(allocator);
    }

    for (examples, boundary_summary.examples) |example, boundary_example| {
        const word_ranges = try buildWordRanges(allocator, example.text);
        defer allocator.free(word_ranges);

        for (example.mentions) |mention| {
            if (mention.end > example.text.len or mention.start >= mention.end) return error.InvalidCleanupMentionSpan;
            const word_span = findWordSpanForMention(word_ranges, mention.start, mention.end) orelse return error.CleanupMentionAlignmentFailed;
            const features = try poolMentionVector(allocator, &boundary_example, boundary_summary.hidden_size, word_span);
            try mentions.append(allocator, .{
                .text = try allocator.dupe(u8, example.text[mention.start..mention.end]),
                .label = try allocator.dupe(u8, mention.label),
                .start = mention.start,
                .end = mention.end,
                .detect_score = 1.0,
                .features = features,
                .keep = mention.keep,
                .preferred_surface = mention.preferred_surface,
                .group_id = if (mention.group_id) |value| try allocator.dupe(u8, value) else null,
            });
        }
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, cache_family_version),
        .input_path = try allocator.dupe(u8, input_path),
        .split = if (split) |value| try allocator.dupe(u8, value) else null,
        .feature_dim = boundary_summary.hidden_size,
        .context_window = 0,
        .stats = cleanup_data.computeStats(examples),
        .mentions = try mentions.toOwnedSlice(allocator),
    };
}

fn collectUniqueLabels(allocator: std.mem.Allocator, examples: []const cleanup_data.Example) ![][]const u8 {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(allocator);

    var labels = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (labels.items) |label| allocator.free(label);
        labels.deinit(allocator);
    }

    for (examples) |example| {
        for (example.mentions) |mention| {
            if (seen.contains(mention.label)) continue;
            const dupe = try allocator.dupe(u8, mention.label);
            try seen.put(allocator, mention.label, {});
            try labels.append(allocator, dupe);
        }
    }

    if (labels.items.len == 0) return error.NoLabels;
    std.mem.sort([]const u8, labels.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);
    return try labels.toOwnedSlice(allocator);
}

fn convertCleanupExamples(
    allocator: std.mem.Allocator,
    examples: []const cleanup_data.Example,
) ![]gliner2_data.Example {
    const out = try allocator.alloc(gliner2_data.Example, examples.len);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |example| allocator.free(example.entities);
        allocator.free(out);
    }

    for (examples, 0..) |example, idx| {
        const entities = try allocator.alloc(gliner2_data.Entity, example.mentions.len);
        for (example.mentions, 0..) |mention, mention_idx| {
            const end = @min(mention.end, example.text.len);
            const start = @min(mention.start, end);
            entities[mention_idx] = .{
                .text = example.text[start..end],
                .label = mention.label,
                .start = start,
                .end = end,
            };
        }
        out[idx] = .{
            .text = example.text,
            .entities = entities,
        };
        built += 1;
    }

    return out;
}

fn freeGlinerExamples(allocator: std.mem.Allocator, examples: []gliner2_data.Example) void {
    for (examples) |example| allocator.free(example.entities);
    allocator.free(examples);
}

fn buildWordRanges(allocator: std.mem.Allocator, text: []const u8) ![]WordRange {
    var ranges = std.ArrayListUnmanaged(WordRange).empty;
    errdefer ranges.deinit(allocator);

    var idx: usize = 0;
    while (idx < text.len) {
        while (idx < text.len and std.ascii.isWhitespace(text[idx])) : (idx += 1) {}
        if (idx >= text.len) break;
        const start = idx;
        while (idx < text.len and !std.ascii.isWhitespace(text[idx])) : (idx += 1) {}
        try ranges.append(allocator, .{ .start = start, .end = idx });
    }

    return try ranges.toOwnedSlice(allocator);
}

fn findWordSpanForMention(word_ranges: []const WordRange, start: usize, end: usize) ?WordSpan {
    var first: ?usize = null;
    var last: ?usize = null;
    for (word_ranges, 0..) |word, idx| {
        if (word.start >= end) break;
        if (word.end <= start) continue;
        if (first == null) first = idx;
        last = idx;
    }
    return if (first != null and last != null) .{ .start = first.?, .end = last.? } else null;
}

fn poolMentionVector(
    allocator: std.mem.Allocator,
    entry: *const gliner2_boundary.CachedBoundaryExampleSummary,
    hidden_size: usize,
    span: WordSpan,
) ![]f32 {
    if (entry.hidden_in.len < entry.seq_len * hidden_size) return error.InvalidHiddenStateShape;
    if (span.start >= entry.max_words_per_sample or span.end >= entry.max_words_per_sample) return error.WordIndexOutOfRange;

    const tok_start_raw = entry.first_token_positions[span.start];
    const tok_end: usize = blk: {
        if (span.end + 1 < entry.max_words_per_sample and entry.first_token_positions[span.end + 1] > 0) {
            break :blk @as(usize, @intCast(entry.first_token_positions[span.end + 1]));
        }
        const expected_mask: i32 = @intCast(span.end + 1);
        var t = @as(usize, @intCast(entry.first_token_positions[span.end]));
        while (t < entry.seq_len and t < entry.words_mask.len and entry.words_mask[t] == expected_mask) : (t += 1) {}
        break :blk t;
    };

    if (tok_start_raw < 0) return error.InvalidTokenPosition;
    const tok_start = @as(usize, @intCast(tok_start_raw));
    if (tok_start >= entry.seq_len or tok_end > entry.seq_len or tok_start >= tok_end) return error.InvalidTokenWindow;

    const features = try allocator.alloc(f32, hidden_size);
    @memset(features, 0);
    const span_len: f32 = @floatFromInt(tok_end - tok_start);
    for (tok_start..tok_end) |tok_idx| {
        const row = entry.hidden_in[tok_idx * hidden_size .. (tok_idx + 1) * hidden_size];
        for (row, 0..) |value, dim| features[dim] += value;
    }
    for (features) |*value| value.* /= span_len;
    return features;
}

fn freeStringSlice(allocator: std.mem.Allocator, values: [][]const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn freeCachedMentionSummary(allocator: std.mem.Allocator, mention: *cleanup_model.CachedMentionSummary) void {
    allocator.free(mention.text);
    allocator.free(mention.label);
    allocator.free(mention.features);
    if (mention.group_id) |value| allocator.free(value);
    mention.* = undefined;
}

test "prepare cleanup summary from boundary summary pools GLiNER features by mention span" {
    const allocator = std.testing.allocator;

    var mentions = [_]cleanup_data.Mention{
        .{ .start = 0, .end = 7, .label = "person", .keep = false },
        .{ .start = 12, .end = 20, .label = "person", .keep = true, .group_id = "g1", .preferred_surface = true },
    };
    var examples = [_]cleanup_data.Example{
        .{ .text = "TimKaye met Tim Kaye", .mentions = mentions[0..] },
    };

    var hidden_in = [_]f32{
        1, 0,
        0, 1,
        2, 2,
        3, 3,
    };
    var input_ids = [_]i32{ 1, 2, 3, 4 };
    var attention_mask = [_]i64{ 1, 1, 1, 1 };
    var words_mask = [_]i32{ 1, 2, 3, 4 };
    var first_token_positions = [_]i32{ 0, 1, 2, 3 };
    var span_indices = [_]i32{ 0, 0 };
    var span_mask = [_]f32{1};
    var span_labels = [_]f32{0};
    var e_token_positions = [_]i32{0};
    var e_token_end_positions = [_]i32{0};
    var entity_type_kind = [_]i32{0};

    var boundary_examples = [_]gliner2_boundary.CachedBoundaryExampleSummary{
        .{
            .text = "TimKaye met Tim Kaye",
            .hidden_in = hidden_in[0..],
            .input_ids = input_ids[0..],
            .attention_mask = attention_mask[0..],
            .words_mask = words_mask[0..],
            .first_token_positions = first_token_positions[0..],
            .span_indices = span_indices[0..],
            .span_mask = span_mask[0..],
            .span_labels = span_labels[0..],
            .e_token_positions = e_token_positions[0..],
            .e_token_end_positions = e_token_end_positions[0..],
            .entity_type_kind = entity_type_kind[0..],
            .seq_len = 4,
            .max_words_per_sample = 4,
            .max_spans = 1,
            .num_entity_types = 1,
        },
    };
    var boundary_summary = gliner2_boundary.CachedBoundarySummary{
        .artifact_family_version = "x",
        .model_dir = "/tmp/model",
        .input_path = "/tmp/in.jsonl",
        .split = "train",
        .requested_backend = "native",
        .top_layer_count = 1,
        .hidden_size = 2,
        .max_length = 8,
        .max_span_width = 4,
        .entity_types = &.{"person"},
        .dataset_stats = .{},
        .examples = boundary_examples[0..],
    };

    var summary = try prepareCachedSummaryFromBoundarySummary(allocator, "/tmp/in.jsonl", "train", examples[0..], &boundary_summary);
    defer cleanup_model.freeCachedSummary(allocator, &summary);

    try std.testing.expectEqualStrings(cache_family_version, summary.artifact_family_version);
    try std.testing.expectEqual(@as(usize, 2), summary.mentions.len);
    try std.testing.expectEqual(@as(usize, 2), summary.feature_dim);
    try std.testing.expectEqual(@as(f32, 1), summary.mentions[0].features[0]);
    try std.testing.expectEqual(@as(f32, 0), summary.mentions[0].features[1]);
    try std.testing.expectEqual(@as(f32, 2.5), summary.mentions[1].features[0]);
    try std.testing.expectEqual(@as(f32, 2.5), summary.mentions[1].features[1]);
}

test "prepare cleanup summary rejects unaligned mention spans" {
    const allocator = std.testing.allocator;

    var mentions = [_]cleanup_data.Mention{
        .{ .start = 3, .end = 4, .label = "person", .keep = true, .group_id = "g1", .preferred_surface = true },
    };
    var examples = [_]cleanup_data.Example{
        .{ .text = "Tim Kaye", .mentions = mentions[0..] },
    };

    var hidden_in = [_]f32{
        1, 0,
        0, 1,
    };
    var input_ids = [_]i32{ 1, 2 };
    var attention_mask = [_]i64{ 1, 1 };
    var words_mask = [_]i32{ 1, 2 };
    var first_token_positions = [_]i32{ 0, 1 };
    var span_indices = [_]i32{ 0, 0 };
    var span_mask = [_]f32{1};
    var span_labels = [_]f32{0};
    var e_token_positions = [_]i32{0};
    var e_token_end_positions = [_]i32{0};
    var entity_type_kind = [_]i32{0};

    var boundary_examples = [_]gliner2_boundary.CachedBoundaryExampleSummary{
        .{
            .text = "Tim Kaye",
            .hidden_in = hidden_in[0..],
            .input_ids = input_ids[0..],
            .attention_mask = attention_mask[0..],
            .words_mask = words_mask[0..],
            .first_token_positions = first_token_positions[0..],
            .span_indices = span_indices[0..],
            .span_mask = span_mask[0..],
            .span_labels = span_labels[0..],
            .e_token_positions = e_token_positions[0..],
            .e_token_end_positions = e_token_end_positions[0..],
            .entity_type_kind = entity_type_kind[0..],
            .seq_len = 2,
            .max_words_per_sample = 2,
            .max_spans = 1,
            .num_entity_types = 1,
        },
    };
    var boundary_summary = gliner2_boundary.CachedBoundarySummary{
        .artifact_family_version = "x",
        .model_dir = "/tmp/model",
        .input_path = "/tmp/in.jsonl",
        .split = "train",
        .requested_backend = "native",
        .top_layer_count = 1,
        .hidden_size = 2,
        .max_length = 8,
        .max_span_width = 4,
        .entity_types = &.{"person"},
        .dataset_stats = .{},
        .examples = boundary_examples[0..],
    };

    try std.testing.expectError(
        error.CleanupMentionAlignmentFailed,
        prepareCachedSummaryFromBoundarySummary(allocator, "/tmp/in.jsonl", "train", examples[0..], &boundary_summary),
    );
}
