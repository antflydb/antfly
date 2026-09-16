// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Detached, bounded proposal decisions between retained training stages.
//! Returned geometry is derived from current captured logits, original word
//! counts and explicit annotations. Neural candidate scores are never cached.
const std = @import("std");
const model = @import("../models/gliner_boundary.zig");
const selection = @import("gliner_boundary_selection.zig");
const loss = @import("gliner_boundary_losses.zig");
const Allocator = std.mem.Allocator;
pub const Input = struct {
    batch: usize,
    queries: usize,
    words: usize,
    dimension: usize,
    starts: []const f32, // [B,Q,W+1]
    ends: []const f32,
    inside: []const f32, // [B,Q,W]
    projected_starts: []const f32, // [B,W+1,D], live shared_pool_builder outputs
    projected_ends: []const f32,
    query_mask: []const bool,
    word_counts: []const usize,
};
pub const Pool = struct {
    arena: std.heap.ArenaAllocator,
    selection: selection.Result,
    starts: []const i32,
    ends: []const i32,
    valid: []const f32,
    lengths: []const f32,
    length_features: []const f32,
    inside_mean: []const f32,
    fingerprint: [32]u8,
    work: usize,
    pub fn deinit(self: *Pool) void {
        self.selection.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};
const Boundary = struct {
    score: f32,
    index: usize,
    valid: bool,
    fn before(_: void, a: Boundary, b: Boundary) bool {
        return a.score > b.score or (a.score == b.score and a.index < b.index);
    }
};
const Work = struct {
    limits: selection.Limits,
    steps: usize = 0,
    fn charge(self: *Work, count: usize) !void {
        if (count > self.limits.max_work -| self.steps) return error.BoundaryTrainingSelectionLimitExceeded;
        self.steps += count;
        if (self.limits.control) |control| try control.check();
    }
};
fn multiply(a: usize, b: usize, maximum: usize) !usize {
    const value = std.math.mul(usize, a, b) catch return error.BoundaryTrainingSelectionLimitExceeded;
    if (value > maximum) return error.BoundaryTrainingSelectionLimitExceeded;
    return value;
}
fn finite(values: []const f32) !void {
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryTraining;
}
fn appendInteger(hash: *std.crypto.hash.sha2.Sha256, integer: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, integer, .little);
    hash.update(&bytes);
}

pub fn selectPool(a: Allocator, head: model.HeadConfig, input: Input, gold: ?loss.Gold, options: selection.Options) !Pool {
    if (head.candidate_pool != .shared or input.batch == 0 or input.queries == 0 or input.dimension == 0 or input.dimension != head.boundary_dim)
        return error.InvalidBoundaryTrainingSelectionShape;
    var work = Work{ .limits = options.limits };
    try work.charge(0);
    const limit = options.limits.max_elements;
    if (input.batch > options.limits.max_batch or input.queries > options.limits.max_queries or input.words > options.limits.max_words)
        return error.BoundaryTrainingSelectionLimitExceeded;
    const n = std.math.add(usize, input.words, 1) catch return error.BoundaryTrainingSelectionLimitExceeded;
    const k = @min(@as(usize, head.pool_boundary_top_k), n);
    const proposals = try multiply(k, k, options.limits.max_proposals);
    const bn = try multiply(input.batch, n, limit);
    const bq = try multiply(input.batch, input.queries, limit);
    const bqn = try multiply(bq, n, limit);
    const bqw = try multiply(bq, input.words, limit);
    const bnd = try multiply(bn, input.dimension, limit);
    const bm = try multiply(input.batch, proposals, limit);
    const bqm = try multiply(bq, proposals, limit);
    if (input.starts.len != bqn or input.ends.len != bqn or input.inside.len != bqw or input.projected_starts.len != bnd or input.projected_ends.len != bnd or
        input.query_mask.len != bq or input.word_counts.len != input.batch) return error.InvalidBoundaryTrainingSelectionShape;
    for (input.word_counts) |words| if (words > input.words) return error.InvalidBoundaryTrainingSelectionShape;
    for ([_][]const f32{ input.starts, input.ends, input.inside, input.projected_starts, input.projected_ends }) |values| {
        try work.charge(values.len);
        try finite(values);
    }
    var scratch_arena = std.heap.ArenaAllocator.init(a);
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();
    const raw_spans = try scratch.alloc(loss.Span, bm);
    const raw_valid = try scratch.alloc(bool, bm);
    const global_scores = try scratch.alloc(f32, bm);
    const query_scores = try scratch.alloc(f32, bqm);
    const union_start = try scratch.alloc(f32, n);
    const union_end = try scratch.alloc(f32, n);
    const start_order = try scratch.alloc(Boundary, n);
    const end_order = try scratch.alloc(Boundary, n);
    for (0..input.batch) |b| {
        const any_query = std.mem.indexOfScalar(bool, input.query_mask[b * input.queries ..][0..input.queries], true) != null;
        for (0..n) |w| {
            try work.charge(input.queries);
            var s: f32 = -std.math.inf(f32);
            var e: f32 = -std.math.inf(f32);
            const active = w <= input.word_counts[b];
            for (0..input.queries) |q| {
                const row = b * input.queries + q;
                const keep = active and input.query_mask[row];
                s = @max(s, if (keep) input.starts[row * n + w] else -10000);
                e = @max(e, if (keep) input.ends[row * n + w] else -10000);
            }
            union_start[w] = s;
            union_end[w] = e;
            start_order[w] = .{ .score = s, .index = w, .valid = active and any_query };
            end_order[w] = .{ .score = e, .index = w, .valid = active and any_query };
        }
        try work.charge(try multiply(n, @max(@bitSizeOf(usize) - @clz(n), 1), options.limits.max_work));
        std.mem.sort(Boundary, start_order, {}, Boundary.before);
        std.mem.sort(Boundary, end_order, {}, Boundary.before);
        for (start_order[0..k], 0..) |start, s_index| for (end_order[0..k], 0..) |end, e_index| {
            try work.charge(input.dimension + input.queries);
            const s = if (start.valid) start.index else 0;
            const e = if (end.valid) end.index else 0;
            const local = s_index * k + e_index;
            const position = b * proposals + local;
            raw_spans[position] = .{ .start = @intCast(s), .end = @intCast(e) };
            raw_valid[position] = start.valid and end.valid and e > s;
            var compatibility: f32 = 0;
            for (0..input.dimension) |d| compatibility += input.projected_starts[(b * n + s) * input.dimension + d] * input.projected_ends[(b * n + e) * input.dimension + d];
            compatibility /= @sqrt(@as(f32, @floatFromInt(input.dimension)));
            global_scores[position] = (compatibility + union_start[s]) + union_end[e];
            for (0..input.queries) |q| {
                const row = b * input.queries + q;
                query_scores[row * proposals + local] = (input.starts[row * n + s] + input.ends[row * n + e]) + compatibility;
            }
        };
    }
    var remaining = options;
    remaining.limits.max_work -|= work.steps;
    var selected = try selection.assembleDocumentPool(a, .{ .batch = input.batch, .queries = input.queries, .proposals = proposals, .spans = raw_spans, .global_scores = global_scores, .valid = raw_valid, .query_scores = query_scores, .query_mask = input.query_mask, .word_counts = input.word_counts, .min_pool_per_query = head.min_pool_per_query }, gold, remaining);
    errdefer selected.deinit();
    try work.charge(selected.work);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const bc = try multiply(input.batch, selected.capacity, limit);
    const starts = try owned.alloc(i32, bc);
    const ends = try owned.alloc(i32, bc);
    const valid = try owned.alloc(f32, bc);
    const lengths = try owned.alloc(f32, bc);
    const features = try owned.alloc(f32, try multiply(bc, 3, limit));
    const means = try owned.alloc(f32, bq);
    @memset(means, 0);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.pool.v1");
    appendInteger(&hash, input.batch);
    appendInteger(&hash, input.queries);
    appendInteger(&hash, input.words);
    appendInteger(&hash, selected.capacity);
    for (selected.spans, selected.valid, selected.injected, 0..) |span, active, injected, i| {
        try work.charge(1);
        const b = i / selected.capacity;
        const s: usize = @intCast(span.start);
        const e: usize = @intCast(span.end);
        starts[i] = std.math.cast(i32, b * n + s) orelse return error.BoundaryTrainingSelectionLimitExceeded;
        ends[i] = std.math.cast(i32, b * n + e) orelse return error.BoundaryTrainingSelectionLimitExceeded;
        valid[i] = if (active) 1 else 0;
        const length: f32 = @floatFromInt(@max(e - s, 1));
        lengths[i] = length;
        features[i * 3] = std.math.log1p(length);
        features[i * 3 + 1] = length / @as(f32, @floatFromInt(@max(input.word_counts[b], 1)));
        features[i * 3 + 2] = 1 / @sqrt(length);
        appendInteger(&hash, s);
        appendInteger(&hash, e);
        hash.update(&.{ @intFromBool(active), @intFromBool(injected) });
    }
    for (0..input.batch) |b| for (0..input.queries) |q| {
        const row = b * input.queries + q;
        try work.charge(input.word_counts[b]);
        if (input.query_mask[row]) for (0..input.word_counts[b]) |w| {
            means[row] += input.inside[row * input.words + w];
        };
        means[row] /= @as(f32, @floatFromInt(@max(input.word_counts[b], 1)));
        if (!std.math.isFinite(means[row])) return error.NonFiniteBoundaryTraining;
        appendInteger(&hash, @as(u32, @bitCast(means[row])));
    };
    var fingerprint: [32]u8 = undefined;
    hash.final(&fingerprint);
    try work.charge(0);
    return .{ .arena = arena, .selection = selected, .starts = starts, .ends = ends, .valid = valid, .lengths = lengths, .length_features = features, .inside_mean = means, .fingerprint = fingerprint, .work = work.steps };
}

test "boundary training decisions select live endpoint union and explicit gold geometry" {
    const a = std.testing.allocator;
    var head = model.HeadConfig{};
    head.candidate_pool = .shared;
    head.boundary_dim = 2;
    head.pool_boundary_top_k = 2;
    head.min_pool_per_query = 0;
    const input = Input{ .batch = 1, .queries = 1, .words = 2, .dimension = 2, .starts = &.{ 3, 2, -1 }, .ends = &.{ -1, 1, 4 }, .inside = &.{ 2, 6 }, .projected_starts = &.{ 1, 0, 0, 1, 1, 1 }, .projected_ends = &.{ 0, 1, 1, 0, 1, 1 }, .query_mask = &.{true}, .word_counts = &.{2} };
    const gold = loss.Gold{ .batch = 1, .queries = 1, .capacity = 1, .spans = &.{.{ .start = 1, .end = 2 }}, .valid = &.{true} };
    var result = try selectPool(a, head, input, gold, .{ .phase = .training, .capacity = 2 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.selection.capacity);
    try std.testing.expectEqual(@as(f32, 4), result.inside_mean[0]);
    var found = false;
    for (result.selection.spans, result.selection.injected, result.starts, result.ends, result.lengths) |span, injected, s, e, length| {
        try std.testing.expectEqual(span.start, @as(i64, s));
        try std.testing.expectEqual(span.end, @as(i64, e));
        try std.testing.expectEqual(@as(f32, @floatFromInt(@max(span.end - span.start, 1))), length);
        if (injected and span.start == 1 and span.end == 2) found = true;
    }
    try std.testing.expect(found);
    var changed = input;
    changed.inside = &.{ 4, 8 };
    var second = try selectPool(a, head, changed, gold, .{ .phase = .training, .capacity = 2 });
    defer second.deinit();
    try std.testing.expect(!std.mem.eql(u8, &result.fingerprint, &second.fingerprint));
}
