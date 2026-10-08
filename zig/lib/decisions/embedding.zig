// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Similarity routing has a different contract from a trained decision head.
//! Scores are cosine similarities, never probabilities or confidence values.
const std = @import("std");
pub fn validDimension(dimensions: usize) bool {
    return switch (dimensions) {
        128, 256, 512, 768 => true,
        else => false,
    };
}
pub const renderer_version = "instruction-category-v1";

pub const Options = struct {
    task_type: []const u8 = "CLUSTERING",
    dimensions: usize = 768,
    min_similarity: ?f64 = null,
    min_margin: ?f64 = null,
    calibration_id: ?[]const u8 = null,

    pub fn parse(value: std.json.Value) !Options {
        if (value != .object) return error.InvalidDecideRequest;
        var out = Options{};
        for (value.object.keys(), value.object.values()) |key, v| {
            if (std.mem.eql(u8, key, "task_type")) {
                if (v != .string or (!std.mem.eql(u8, v.string, "CLUSTERING") and !std.mem.eql(u8, v.string, "CLASSIFICATION"))) return error.InvalidDecideRequest;
                out.task_type = v.string;
            } else if (std.mem.eql(u8, key, "dimensions")) {
                if (v != .integer or v.integer < 0 or !validDimension(@intCast(v.integer))) return error.InvalidDecideRequest;
                out.dimensions = @intCast(v.integer);
            } else if (std.mem.eql(u8, key, "min_similarity")) {
                const n = try number(v);
                if (n < -1 or n > 1) return error.InvalidDecideRequest;
                out.min_similarity = n;
            } else if (std.mem.eql(u8, key, "min_margin")) {
                const n = try number(v);
                if (n < 0 or n > 2) return error.InvalidDecideRequest;
                out.min_margin = n;
            } else if (std.mem.eql(u8, key, "calibration_id")) {
                if (v != .string or v.string.len == 0 or v.string.len > 64) return error.InvalidDecideRequest;
                for (v.string) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return error.InvalidDecideRequest;
                out.calibration_id = v.string;
            } else return error.InvalidDecideRequest;
        }
        if (out.calibration_id != null and (out.min_similarity != null or out.min_margin != null)) return error.InvalidDecideRequest;
        return out;
    }
};

fn number(value: std.json.Value) !f64 {
    const n: f64 = switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        else => return error.InvalidDecideRequest,
    };
    if (!std.math.isFinite(n)) return error.InvalidDecideRequest;
    return n;
}

pub fn renderInput(a: std.mem.Allocator, instruction: []const u8, state: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "Instruction: {s}\nInput: {s}", .{ instruction, state });
}
pub fn renderCategory(a: std.mem.Allocator, instruction: []const u8, description: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "Instruction: {s}\nCategory: {s}", .{ instruction, description });
}

pub fn cosine(a: []const f32, b: []const f32, dimensions: usize) !f64 {
    if (!validDimension(dimensions) or a.len < dimensions or b.len < dimensions) return error.InvalidEmbeddingDimensions;
    var dot: f64 = 0;
    var aa: f64 = 0;
    var bb: f64 = 0;
    for (a[0..dimensions], b[0..dimensions]) |x, y| {
        if (!std.math.isFinite(x) or !std.math.isFinite(y)) return error.InvalidEmbeddingDecisionOutput;
        dot += @as(f64, x) * y;
        aa += @as(f64, x) * x;
        bb += @as(f64, y) * y;
    }
    if (aa <= 0 or bb <= 0) return error.InvalidEmbeddingDecisionOutput;
    return std.math.clamp(dot / @sqrt(aa * bb), -1, 1);
}

pub const Selection = struct {
    selected: ?usize,
    best_similarity: f64,
    margin: f64,
    reason: ?[]const u8,
};

pub fn select(scores: []const f64, options: Options) !Selection {
    if (scores.len < 2) return error.InvalidEmbeddingDecisionOutput;
    var best: usize = 0;
    var runner: f64 = -std.math.inf(f64);
    for (scores, 0..) |score, i| {
        if (!std.math.isFinite(score) or score < -1 or score > 1) return error.InvalidEmbeddingDecisionOutput;
        if (i == 0) continue;
        if (score > scores[best]) {
            runner = @max(runner, scores[best]);
            best = i;
        } else runner = @max(runner, score);
    }
    const margin = scores[best] - runner;
    const reason: ?[]const u8 = if (margin <= 1e-6) "tie" else if (options.min_similarity != null and scores[best] < options.min_similarity.?) "min_similarity" else if (options.min_margin != null and margin < options.min_margin.?) "min_margin" else null;
    return .{ .selected = if (reason == null) best else null, .best_similarity = scores[best], .margin = margin, .reason = reason };
}

pub const MultiSelection = struct { indices: []const usize, margin: f64, status: []const u8, reason: ?[]const u8 = null };

/// A valid empty set is distinct from a requested ambiguity abstention.
/// The set margin is the closest score's distance from its own threshold.
pub fn selectMulti(a: std.mem.Allocator, scores: []const f64, thresholds: []const f64, min_margin: ?f64) !MultiSelection {
    if (scores.len == 0 or scores.len != thresholds.len) return error.InvalidEmbeddingDecisionOutput;
    if (min_margin) |margin| if (!std.math.isFinite(margin) or margin < 0 or margin > 2) return error.InvalidEmbeddingOptions;
    var indices: std.ArrayListUnmanaged(usize) = .empty;
    defer indices.deinit(a);
    var margin: f64 = 2;
    for (scores, thresholds, 0..) |score, threshold, index| {
        if (!std.math.isFinite(score) or score < -1 or score > 1 or !std.math.isFinite(threshold) or threshold < -1 or threshold > 1) return error.InvalidEmbeddingDecisionOutput;
        margin = @min(margin, @abs(score - threshold));
        if (score >= threshold) try indices.append(a, index);
    }
    if (min_margin != null and margin < min_margin.?) return .{ .indices = &.{}, .margin = margin, .status = "abstained", .reason = "min_margin" };
    const status: []const u8 = if (indices.items.len == 0) "empty" else "selected";
    return .{ .indices = try indices.toOwnedSlice(a), .margin = margin, .status = status };
}

test "embeddinggemma2 multi choices distinguish selected empty and ambiguous sets" {
    const a = std.testing.allocator;
    const selected = try selectMulti(a, &.{ 0.8, 0.2, 0.6 }, &.{ 0.5, 0.4, 0.6 }, null);
    defer a.free(selected.indices);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2 }, selected.indices);
    try std.testing.expectEqualStrings("selected", selected.status);
    const empty = try selectMulti(a, &.{ 0.2, 0.1 }, &.{ 0.5, 0.5 }, null);
    defer a.free(empty.indices);
    try std.testing.expectEqual(@as(usize, 0), empty.indices.len);
    try std.testing.expectEqualStrings("empty", empty.status);
    const ambiguous = try selectMulti(a, &.{ 0.501, 0.2 }, &.{ 0.5, 0.5 }, 0.01);
    defer a.free(ambiguous.indices);
    try std.testing.expectEqualStrings("abstained", ambiguous.status);
    try std.testing.expectEqualStrings("min_margin", ambiguous.reason.?);
    try std.testing.expectError(error.InvalidEmbeddingDecisionOutput, selectMulti(a, &.{std.math.nan(f64)}, &.{0.5}, null));
}

/// Normalized centroid of normalized examples. Descriptions are used only
/// when a label has no examples; adding a description never biases its mean.
pub fn centroid(a: std.mem.Allocator, examples: []const []const f32, dimensions: usize) ![]f32 {
    if (examples.len == 0 or !validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
    const out = try a.alloc(f32, dimensions);
    errdefer a.free(out);
    @memset(out, 0);
    for (examples) |example| {
        if (example.len < dimensions) return error.InvalidEmbeddingDimensions;
        var sq: f64 = 0;
        for (example[0..dimensions]) |x| {
            if (!std.math.isFinite(x)) return error.InvalidEmbeddingDecisionOutput;
            sq += @as(f64, x) * x;
        }
        if (sq <= 0) return error.InvalidEmbeddingDecisionOutput;
        for (out, example[0..dimensions]) |*dest, x| dest.* += @floatCast(@as(f64, x) / @sqrt(sq));
    }
    var sq: f64 = 0;
    for (out) |x| sq += @as(f64, x) * x;
    if (sq <= 1e-20) return error.InvalidEmbeddingDecisionOutput;
    for (out) |*x| x.* = @floatCast(@as(f64, x.*) / @sqrt(sq));
    return out;
}

test "embeddinggemma2 decisions abstain on ties and threshold failures" {
    const tie = try select(&.{ 0.3, 0.3000005 }, .{});
    try std.testing.expect(tie.selected == null);
    try std.testing.expectEqualStrings("tie", tie.reason.?);
    const selected = try select(&.{ -0.4, 0.3, 0.2 }, .{});
    try std.testing.expectEqual(@as(?usize, 1), selected.selected);
    try std.testing.expectApproxEqAbs(@as(f64, 0.1), selected.margin, 1e-12);
    try std.testing.expect((try select(&.{ 0.3, 0.2 }, .{ .min_margin = 0.2 })).selected == null);
    try std.testing.expect((try select(&.{ 0.3, 0.2 }, .{ .min_similarity = 0.4 })).selected == null);
    try std.testing.expectError(error.InvalidEmbeddingDecisionOutput, select(&.{ std.math.nan(f64), 0 }, .{}));
}

test "embeddinggemma2 centroid normalizes each example before averaging" {
    var first: [128]f32 = @splat(0);
    first[0] = 100;
    var second: [128]f32 = @splat(0);
    second[1] = 1;
    const prototype = try centroid(std.testing.allocator, &.{ &first, &second }, 128);
    defer std.testing.allocator.free(prototype);
    try std.testing.expectApproxEqAbs(prototype[0], prototype[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f64, 1), try cosine(prototype, prototype, 128), 1e-12);
}
