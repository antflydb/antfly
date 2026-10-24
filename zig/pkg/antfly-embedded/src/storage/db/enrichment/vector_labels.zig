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

//! Zero-shot classification is a consumer of accepted vectors, independent of
//! source decoding and chunk planning. Scores are raw dot products, not
//! probabilities. Vocabulary/model changes invalidate labels alone.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const max_labels = 4096;
pub const max_top_k = 128;
pub const Label = struct {
    label: []const u8,
    prompt: ?[]const u8 = null,
};
pub const Config = struct {
    labels: []const Label,
    top_k: u32 = 3,
    min_score: ?f32 = null,

    pub fn validate(self: Config) !void {
        if (self.labels.len == 0 or self.labels.len > max_labels or self.top_k == 0 or self.top_k > max_top_k) return error.InvalidAssetProducerConfig;
        if (self.min_score) |minimum| if (!std.math.isFinite(minimum)) return error.InvalidAssetProducerConfig;
        // Bounded open-addressed set avoids quadratic validation for large
        // vocabularies, including validation at the publication receiver.
        var names: [max_labels * 2]u16 = @splat(0);
        for (self.labels, 0..) |label, i| {
            if (label.label.len == 0 or label.label.len > 1024 or !std.unicode.utf8ValidateSlice(label.label)) return error.InvalidAssetProducerConfig;
            if (label.prompt) |prompt| if (prompt.len == 0 or prompt.len > 65536 or !std.unicode.utf8ValidateSlice(prompt)) return error.InvalidAssetProducerConfig;
            var bucket: usize = @intCast(std.hash.Wyhash.hash(0, label.label) % names.len);
            while (names[bucket] != 0) : (bucket = (bucket + 1) % names.len) {
                if (std.mem.eql(u8, self.labels[names[bucket] - 1].label, label.label)) return error.InvalidAssetProducerConfig;
            }
            names[bucket] = @intCast(i + 1);
        }
    }
};

pub fn parse(alloc: Allocator, raw: []const u8) !std.json.Parsed(Config) {
    var parsed = try std.json.parseFromSlice(Config, alloc, raw, .{});
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub const Score = struct { label: []const u8, score: f64 };

/// Vocabulary validation and prompt packing happen once per label pass. Eight
/// labels are contiguous at each dimension; four chunk rows share each load.
pub const Scorer = struct {
    config: Config,
    dimensions: usize,
    padded_labels: usize,
    matrix: []f32,
    allocator: Allocator,

    pub fn init(a: Allocator, config: Config, prompts: []const []const f32) !Scorer {
        try config.validate();
        return initValidated(a, config, prompts);
    }
    /// The owner must have already validated this immutable vocabulary.
    pub fn initValidated(a: Allocator, config: Config, prompts: []const []const f32) !Scorer {
        if (prompts.len != config.labels.len or prompts.len == 0 or prompts[0].len == 0) return error.InvalidEmbeddingResponse;
        const dimensions = prompts[0].len;
        const padded = std.mem.alignForward(usize, prompts.len, 8);
        const matrix = try a.alloc(f32, try std.math.mul(usize, padded, dimensions));
        errdefer a.free(matrix);
        @memset(matrix, 0);
        for (prompts, 0..) |prompt, label| {
            if (prompt.len != dimensions) return error.InvalidEmbeddingDimensions;
            for (prompt, 0..) |weight, dimension| {
                if (!std.math.isFinite(weight)) return error.InvalidEmbeddingResponse;
                matrix[(label / 8 * dimensions + dimension) * 8 + label % 8] = weight;
            }
        }
        return .{ .config = config, .dimensions = dimensions, .padded_labels = padded, .matrix = matrix, .allocator = a };
    }
    pub fn deinit(self: *Scorer) void {
        self.allocator.free(self.matrix);
    }
    /// Only returned slices are owned; label strings borrow the vocabulary.
    pub fn scoreBatch(self: Scorer, a: Allocator, vectors: []const []const f32) ![][]Score {
        const rows = try a.alloc([]Score, vectors.len);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |row| a.free(row);
            a.free(rows);
        }
        for (vectors) |vector| {
            if (vector.len != self.dimensions) return error.InvalidEmbeddingDimensions;
            for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
        }
        var first: usize = 0;
        while (first < vectors.len) : (first += 4) {
            const count = @min(@as(usize, 4), vectors.len - first);
            const dots = try a.alloc(f64, count * self.config.labels.len);
            defer a.free(dots);
            var label: usize = 0;
            while (label < self.padded_labels) : (label += 8) {
                var sums: [4]@Vector(8, f64) = @splat(@splat(0));
                for (0..self.dimensions) |dimension| {
                    const weights_f32: @Vector(8, f32) = self.matrix[(label / 8 * self.dimensions + dimension) * 8 ..][0..8].*;
                    const weights: @Vector(8, f64) = @floatCast(weights_f32);
                    for (0..count) |row| sums[row] += weights * @as(@Vector(8, f64), @splat(vectors[first + row][dimension]));
                }
                for (0..count) |row| {
                    const values: [8]f64 = sums[row];
                    for (0..@min(@as(usize, 8), self.config.labels.len - label)) |lane| {
                        const dot = values[lane];
                        if (!std.math.isFinite(dot)) return error.InvalidEmbeddingResponse;
                        dots[row * self.config.labels.len + label + lane] = dot;
                    }
                }
            }
            for (0..count) |row| {
                rows[first + row] = try rank(a, self.config, dots[row * self.config.labels.len ..][0..self.config.labels.len]);
                initialized += 1;
            }
        }
        return rows;
    }
};

fn rank(a: Allocator, config: Config, dots: []const f64) ![]Score {
    const capacity = @min(@as(usize, config.top_k), config.labels.len);
    const best = try a.alloc(Score, capacity);
    errdefer a.free(best);
    var count: usize = 0;
    for (dots, config.labels) |dot, label| {
        if (config.min_score) |minimum| if (dot < minimum) continue;
        var at: usize = 0;
        while (at < count and best[at].score >= dot) : (at += 1) {}
        if (at >= capacity) continue;
        const end = @min(count, capacity - 1);
        var offset = end;
        while (offset > at) : (offset -= 1) best[offset] = best[offset - 1];
        best[at] = .{ .label = label.label, .score = dot };
        count = @min(count + 1, capacity);
    }
    const result = try a.dupe(Score, best[0..count]);
    a.free(best);
    return result;
}

pub fn score(a: Allocator, config: Config, prompts: []const []const f32, vector: []const f32) ![]Score {
    var scorer = try Scorer.init(a, config, prompts);
    defer scorer.deinit();
    const rows = try scorer.scoreBatch(a, &.{vector});
    defer a.free(rows);
    return rows[0];
}

test "model aware media vector labels score only stored vectors with deterministic ties and filtering" {
    const a = std.testing.allocator;
    const config = Config{ .labels = &.{ .{ .label = "speech" }, .{ .label = "music" }, .{ .label = "silence" } }, .top_k = 2, .min_score = 0.1 };
    const prompts = [_][]const f32{ &.{ 1, 0 }, &.{ 1, 0 }, &.{ 0, -1 } };
    const result = try score(a, config, &prompts, &.{ 0.6, 0.8 });
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqualStrings("speech", result[0].label);
    try std.testing.expectEqualStrings("music", result[1].label);
    try std.testing.expectApproxEqAbs(@as(f64, 0.6), result[0].score, 0.000001);
    try std.testing.expectError(error.InvalidEmbeddingDimensions, score(a, config, &prompts, &.{1}));
    try std.testing.expectError(error.InvalidEmbeddingResponse, score(a, config, &prompts, &.{ std.math.nan(f32), 0 }));
}

test "model aware media vector labels vocabulary admission rejects ambiguous and unbounded definitions" {
    try std.testing.expectError(error.InvalidAssetProducerConfig, (Config{ .labels = &.{} }).validate());
    try std.testing.expectError(error.InvalidAssetProducerConfig, (Config{ .labels = &.{ .{ .label = "same" }, .{ .label = "same" } } }).validate());
    try std.testing.expectError(error.InvalidAssetProducerConfig, (Config{ .labels = &.{.{ .label = "valid" }}, .top_k = 0 }).validate());
}

test "model aware media packed label batches match scalar raw dots across tails and ties" {
    const a = std.testing.allocator;
    const vocabulary = [_]Label{ .{ .label = "a" }, .{ .label = "b" }, .{ .label = "c" }, .{ .label = "d" }, .{ .label = "e" }, .{ .label = "f" }, .{ .label = "g" }, .{ .label = "h" }, .{ .label = "i" } };
    const prompts = [_][]const f32{ &.{ 1, 0, 0 }, &.{ 0, 1, 0 }, &.{ 0, 0, 1 }, &.{ -1, 2, 3 }, &.{ 1, 0, 0 }, &.{ 1, -2, 1 }, &.{ 0.5, 0.5, 0.5 }, &.{ -1, -2, -3 }, &.{ 3, 2, 1 } };
    const vectors = [_][]const f32{ &.{ 1, 0, 0 }, &.{ 0, 1, 0 }, &.{ 0, 0, 1 }, &.{ -1, -1, -1 }, &.{ 0.25, 0.5, -0.75 } };
    const config = Config{ .labels = &vocabulary, .top_k = 4, .min_score = -1 };
    var scorer = try Scorer.init(a, config, &prompts);
    defer scorer.deinit();
    const rows = try scorer.scoreBatch(a, &vectors);
    defer {
        for (rows) |row| a.free(row);
        a.free(rows);
    }
    for (vectors, rows) |vector, row| {
        var dots: [9]f64 = undefined;
        for (prompts, &dots) |prompt, *dot| {
            dot.* = 0;
            for (prompt, vector) |x, y| dot.* += @as(f64, x) * @as(f64, y);
        }
        const expected = try rank(a, config, &dots);
        defer a.free(expected);
        try std.testing.expectEqual(expected.len, row.len);
        for (expected, row) |left, right| {
            try std.testing.expectEqualStrings(left.label, right.label);
            try std.testing.expectApproxEqAbs(left.score, right.score, 1e-12);
        }
    }
}
