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
        for (self.labels, 0..) |label, i| {
            if (label.label.len == 0 or label.label.len > 1024 or !std.unicode.utf8ValidateSlice(label.label)) return error.InvalidAssetProducerConfig;
            if (label.prompt) |prompt| if (prompt.len == 0 or prompt.len > 65536 or !std.unicode.utf8ValidateSlice(prompt)) return error.InvalidAssetProducerConfig;
            for (self.labels[0..i]) |previous| if (std.mem.eql(u8, previous.label, label.label)) return error.InvalidAssetProducerConfig;
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

/// Returned labels borrow the immutable vocabulary; only the result slice is
/// owned. Stable vocabulary order breaks equal-score ties across restarts.
pub fn score(alloc: Allocator, config: Config, prompts: []const []const f32, vector: []const f32) ![]Score {
    try config.validate();
    if (prompts.len != config.labels.len or vector.len == 0) return error.InvalidEmbeddingResponse;
    for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
    const capacity = @min(@as(usize, config.top_k), config.labels.len);
    const best = try alloc.alloc(Score, capacity);
    errdefer alloc.free(best);
    var count: usize = 0;
    for (prompts, config.labels) |prompt, label| {
        if (prompt.len != vector.len) return error.InvalidEmbeddingDimensions;
        var dot: f64 = 0;
        for (prompt, vector) |weight, value| {
            if (!std.math.isFinite(weight)) return error.InvalidEmbeddingResponse;
            dot += @as(f64, weight) * @as(f64, value);
        }
        if (!std.math.isFinite(dot)) return error.InvalidEmbeddingResponse;
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
    const result = try alloc.dupe(Score, best[0..count]);
    alloc.free(best);
    return result;
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
