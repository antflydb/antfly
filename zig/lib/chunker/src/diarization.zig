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

//! Source-scoped, append-only speaker identities and bounded turn timelines.
const std = @import("std");
pub const default_model = "csukuangfj/speaker-embedding-models:3dspeaker_speech_campplus_sv_en_voxceleb_16k.onnx";
pub const algorithm = "antfly-speaker-timeline-v1";
pub const max_spans = 65536;
pub const Config = struct {
    model: ?[]const u8 = null,
    model_identity: ?[]const u8 = null,
    similarity_threshold: f32 = 0.45,
    ambiguity_margin: f32 = 0.03,
    min_rms: f32 = 0.01,
    max_speakers: u8 = 64,
    pub fn validate(self: Config) !void {
        for ([_]f32{ self.similarity_threshold, self.ambiguity_margin, self.min_rms }) |v| if (!std.math.isFinite(v) or v < 0 or v > 1) return error.InvalidDiarizationConfig;
        if (self.max_speakers == 0 or self.max_speakers > 64) return error.InvalidDiarizationConfig;
        if (self.model_identity) |identity| {
            var digest: [32]u8 = undefined;
            if (identity.len != 64) return error.InvalidDiarizationConfig;
            _ = std.fmt.hexToBytes(&digest, identity) catch return error.InvalidDiarizationConfig;
        }
        if (self.model) |model| if (model.len == 0 or model.len > 4096) return error.InvalidDiarizationConfig;
    }
};
pub const Span = struct { start_ns: i64, end_ns: i64, speaker_index: ?u32 = null };
/// Online centroids never merge or renumber previous speakers. A source replay
/// visits the same fixed grid in order, independent of parent/output windows.
pub const Registry = struct {
    sums: [64][512]f32 = @splat(@splat(0)),
    count: usize = 0,
    pub fn assign(self: *Registry, embedding: []const f32, config: Config) !?u32 {
        try config.validate();
        if (embedding.len != 512) return error.InvalidSpeakerEmbedding;
        var norm: f64 = 0;
        for (embedding) |v| {
            if (!std.math.isFinite(v)) return error.InvalidSpeakerEmbedding;
            norm += @as(f64, v) * v;
        }
        if (norm < 1e-12) return null;
        var best: ?usize = null;
        var score: f64 = -1;
        var second: f64 = -1;
        for (self.sums[0..self.count], 0..) |sum, i| {
            var dot: f64 = 0;
            var length: f64 = 0;
            for (sum, embedding) |s, v| {
                dot += @as(f64, s) * v;
                length += @as(f64, s) * s;
            }
            const similarity = dot / @sqrt(length * norm);
            if (similarity > score) {
                second = score;
                score = similarity;
                best = i;
            } else second = @max(second, similarity);
        }
        if (score >= config.similarity_threshold) {
            if (score - second < config.ambiguity_margin) return null;
            const index = best.?;
            // Bounded exponential centroid: no counter overflow on long sources.
            for (&self.sums[index], embedding) |*s, v| s.* = 0.9 * s.* + 0.1 * v;
            return @intCast(index);
        }
        if (self.count >= config.max_speakers) return null;
        const index = self.count;
        @memcpy(&self.sums[index], embedding);
        self.count += 1;
        return @intCast(index);
    }
};
pub const Timeline = struct {
    spans: std.ArrayList(Span) = .empty,
    pub fn deinit(self: *Timeline, a: std.mem.Allocator) void {
        self.spans.deinit(a);
    }
    pub fn append(self: *Timeline, a: std.mem.Allocator, span: Span) !void {
        if (span.start_ns < 0 or span.end_ns <= span.start_ns) return error.InvalidSpeakerTimeline;
        if (self.spans.items.len != 0) {
            const last = &self.spans.items[self.spans.items.len - 1];
            if (last.end_ns != span.start_ns) return error.InvalidSpeakerTimeline;
            if (last.speaker_index == span.speaker_index) {
                last.end_ns = span.end_ns;
                return;
            }
        } else if (span.start_ns != 0) return error.InvalidSpeakerTimeline;
        if (self.spans.items.len >= max_spans) return error.SpeakerTimelineTooLarge;
        try self.spans.append(a, span);
    }
    pub fn first(self: Timeline, start: i64) usize {
        var lo: usize = 0;
        var hi = self.spans.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.spans.items[mid].end_ns <= start) lo = mid + 1 else hi = mid;
        }
        return lo;
    }
    pub fn label(self: Timeline, start: i64, end: i64) ?u32 {
        const i = self.first(start);
        if (start >= end or i == self.spans.items.len) return null;
        const span = self.spans.items[i];
        return if (span.start_ns <= start and span.end_ns >= end) span.speaker_index else null;
    }
    pub fn turnEnd(self: Timeline, start: i64, end: i64) i64 {
        const i = self.first(start);
        return if (i < self.spans.items.len) @min(end, self.spans.items[i].end_ns) else end;
    }
    pub fn slice(self: Timeline, a: std.mem.Allocator, start: i64, end: i64) ![]const Span {
        var out: std.ArrayList(Span) = .empty;
        errdefer out.deinit(a);
        for (self.spans.items[self.first(start)..]) |span| {
            if (span.start_ns >= end) break;
            try out.append(a, .{ .start_ns = @max(start, span.start_ns), .end_ns = @min(end, span.end_ns), .speaker_index = span.speaker_index });
        }
        return out.toOwnedSlice(a);
    }
};
test "speaker timeline keeps returning voices source scoped and uncertain voices unassigned" {
    var registry = Registry{};
    var a: [512]f32 = @splat(0);
    a[0] = 1;
    var b: [512]f32 = @splat(0);
    b[1] = 1;
    try std.testing.expectEqual(@as(?u32, 0), try registry.assign(&a, .{}));
    try std.testing.expectEqual(@as(?u32, 1), try registry.assign(&b, .{}));
    try std.testing.expectEqual(@as(?u32, 0), try registry.assign(&a, .{}));
    var mixed: [512]f32 = @splat(0);
    mixed[0] = 0.707;
    mixed[1] = 0.707;
    try std.testing.expectEqual(@as(?u32, null), try registry.assign(&mixed, .{}));
    var timeline = Timeline{};
    defer timeline.deinit(std.testing.allocator);
    try timeline.append(std.testing.allocator, .{ .start_ns = 0, .end_ns = 10, .speaker_index = 0 });
    try timeline.append(std.testing.allocator, .{ .start_ns = 10, .end_ns = 20, .speaker_index = 0 });
    try timeline.append(std.testing.allocator, .{ .start_ns = 20, .end_ns = 30, .speaker_index = 1 });
    try std.testing.expectEqual(@as(usize, 2), timeline.spans.items.len);
    try std.testing.expectEqual(@as(i64, 20), timeline.turnEnd(5, 29));
    try std.testing.expectEqual(@as(?u32, null), timeline.label(5, 29));
    const page = try timeline.slice(std.testing.allocator, 15, 25);
    defer std.testing.allocator.free(page);
    try std.testing.expectEqual(@as(i64, 15), page[0].start_ns);
    try std.testing.expectEqual(@as(i64, 25), page[1].end_ns);
}

test "speaker timelines bound retained turns and leave excess speakers unassigned" {
    var registry = Registry{};
    var first: [512]f32 = @splat(0);
    first[0] = 1;
    var other: [512]f32 = @splat(0);
    other[1] = 1;
    try std.testing.expectEqual(@as(?u32, 0), try registry.assign(&first, .{ .max_speakers = 1 }));
    try std.testing.expectEqual(@as(?u32, null), try registry.assign(&other, .{ .max_speakers = 1 }));
    try std.testing.expectError(error.InvalidDiarizationConfig, (Config{ .similarity_threshold = std.math.nan(f32) }).validate());
    var timeline = Timeline{};
    defer timeline.deinit(std.testing.allocator);
    for (0..max_spans) |i| try timeline.append(std.testing.allocator, .{ .start_ns = @intCast(i), .end_ns = @intCast(i + 1), .speaker_index = @intCast(i % 2) });
    try std.testing.expectError(error.SpeakerTimelineTooLarge, timeline.append(std.testing.allocator, .{ .start_ns = max_spans, .end_ns = max_spans + 1, .speaker_index = 0 }));
}
