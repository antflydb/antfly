// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Source-derived mixed boundary objectives and their live-logit cotangents.
//! Decisions use caller-owned random draws and are included in the returned
//! fingerprint. Each scalar is computed from the supplied current logits;
//! no scalar graph constant substitutes for the supervised objective.
const std = @import("std");
const model = @import("../../models/gliner_boundary.zig");
const primitive = @import("boundary_losses.zig");
const Allocator = std.mem.Allocator;

pub const Weights = struct { start: f32 = 1, end: f32 = 1, pair: f32 = 1, inside: f32 = 0.5 };
pub const Progress = struct {
    optimizer_step: u64,
    total_optimizer_steps: u64,
    gold_start: f32 = 1,
    gold_end: f32 = 0.25,
    gold_hold_fraction: f32 = 0.15,
};
pub const Scales = struct { gold_injection: f32, consistency: f64, soft_iou: f64 };

/// Pinned trainer schedules use the optimizer step, including accumulation.
pub fn scales(head: model.HeadConfig, progress: Progress) !Scales {
    for ([_]f32{ progress.gold_start, progress.gold_end, progress.gold_hold_fraction }) |x|
        if (!std.math.isFinite(x) or x < 0 or x > 1) return error.InvalidBoundaryTrainingSchedule;
    const step: f64 = @floatFromInt(progress.optimizer_step);
    const fraction = step / @as(f64, @floatFromInt(@max(progress.total_optimizer_steps, 1)));
    const gold = if (fraction <= progress.gold_hold_fraction) progress.gold_start else @as(f64, progress.gold_start) + (@as(f64, progress.gold_end) - progress.gold_start) *
        std.math.clamp((fraction - progress.gold_hold_fraction) / @max(1 - @as(f64, progress.gold_hold_fraction), 1e-12), 0, 1);
    return .{
        .gold_injection = @floatCast(gold),
        .consistency = if (head.consistency_warmup_steps == 0) 1 else @min(step / @as(f64, @floatFromInt(head.consistency_warmup_steps)), 1),
        .soft_iou = if (head.soft_iou_anneal_steps == 0) 0 else @max(1 - step / @as(f64, @floatFromInt(head.soft_iou_anneal_steps)), 0),
    };
}
fn scheduledWeight(weight: f64, scale: f64, tensor_profile: bool) f32 {
    return if (tensor_profile) @floatCast(weight * scale) else @as(f32, @floatCast(weight)) * @as(f32, @floatCast(scale));
}
pub const Terms = struct {
    start: f32 = 0,
    end: f32 = 0,
    pair: f32 = 0,
    inside: f32 = 0,
    soft_iou: f32 = 0,
    rerank_listwise: f32 = 0,
    proposal: f32 = 0,
    consistency: f32 = 0,
    abstention: f32 = 0,
    count: f32 = 0,
    classification: f32 = 0,
    record_object: f32 = 0,
    record_field: f32 = 0,
    relation: f32 = 0,
    total: f32 = 0,
};
pub const Input = struct {
    batch: usize,
    queries: usize,
    words: usize,
    capacity: usize,
    starts: []const f32, // [B,Q,W+1]
    ends: []const f32,
    inside: []const f32, // [B,Q,W]
    pairs: []const f32, // [B,Q,C]
    proposals: []const f32, // [B,C], the live shared-pool proposal scores
    nulls: ?[]const f32 = null,
    counts: ?[]const f32 = null,
    spans: []const primitive.Span, // [B,C]
    pool_mask: []const bool,
    query_mask: []const bool,
    text_mask: []const bool,
    boundary_mask: []const bool,
    gold: primitive.Gold,
};
pub const Options = struct {
    gradient_backend: ?primitive.elementwise_math.Backend = null,
    listwise_backend: ?primitive.listwise_math.Backend = null,
    consistency_backend: ?primitive.consistency_math.Backend = null,
    gradient_precision: primitive.GradientPrecision = .reference_f64,
    training: bool = true,
    weights: Weights = .{},
    scales: Scales = .{ .gold_injection = 1, .consistency = 1, .soft_iou = 1 },
    /// Pinned negative-query sampling consumes one uniform per padded query.
    /// Ties use ascending flattened query index, independent of allocations.
    negative_query_draws: ?[]const f32 = null,
    limits: primitive.Limits = .{},
};
pub const Gradients = struct {
    starts: []f32,
    ends: []f32,
    inside: []f32,
    pairs: []f32,
    proposals: []f32,
    nulls: ?[]f32,
    counts: ?[]f32,
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    terms: Terms,
    gradients: Gradients,
    pair_query_mask: []const bool,
    hard_negative_mask: []const bool,
    decision_fingerprint: [32]u8,
    work: usize,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
const Work = struct {
    limits: primitive.Limits,
    count: usize = 0,
    fn check(self: *const Work) !void {
        if (self.limits.control) |control| try control.check();
    }
    fn charge(self: *Work, n: usize) !void {
        if (n > self.limits.max_work -| self.count) return error.BoundaryTrainingLimitExceeded;
        self.count += n;
        try self.check();
    }
    fn remaining(self: *const Work) primitive.Limits {
        var out = self.limits;
        out.max_work -|= self.count;
        return out;
    }
};
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryTrainingLimitExceeded;
}
fn zero(a: Allocator, count: usize) ![]f32 {
    const out = try a.alloc(f32, count);
    @memset(out, 0);
    return out;
}
fn finite(values: []const f32) !void {
    for (values) |x| if (!std.math.isFinite(x)) return error.NonFiniteBoundaryTraining;
}
fn addGradient(destination: []f32, source: []const f32, weight: f32) !void {
    if (destination.len != source.len) return error.InvalidBoundaryTrainingShape;
    for (destination, source) |*out, dy| {
        out.* += dy * weight;
        if (!std.math.isFinite(out.*)) return error.NonFiniteBoundaryTraining;
    }
}
fn take(term: *f32, total: *f32, destination: ?[]f32, item: primitive.Loss, weight: f32, work: *Work) !void {
    try work.charge(item.work);
    term.* = @floatCast(item.value);
    total.* += term.* * weight;
    if (!std.math.isFinite(term.*) or !std.math.isFinite(total.*)) return error.NonFiniteBoundaryTraining;
    if (item.gradient_cotangent) |seed| {
        if (seed != weight) return error.InvalidBoundaryTrainingCotangent;
        if (destination) |out| try addGradient(out, item.gradient, 1);
    } else if (destination) |out| try addGradient(out, item.gradient, weight);
}

/// Returns owned cotangents in graph-output order. Query-major iteration is
/// an axis permutation of upstream's shared candidate-major loss tensors.
pub fn boundary(a: Allocator, head: model.HeadConfig, input: Input, options: Options) !Result {
    try head.validate();
    if (head.candidate_pool != .shared) return error.UnsupportedBoundaryTrainingPool;
    var work = Work{ .limits = options.limits };
    try work.check();
    for ([_]f64{ options.weights.start, options.weights.end, options.weights.pair, options.weights.inside, options.scales.consistency, options.scales.soft_iou }) |x|
        if (!std.math.isFinite(x) or x < 0) return error.InvalidBoundaryTrainingOptions;
    const bq = try mul(input.batch, input.queries);
    const bc = try mul(input.batch, input.capacity);
    const bqc = try mul(bq, input.capacity);
    const n = std.math.add(usize, input.words, 1) catch return error.BoundaryTrainingLimitExceeded;
    const bqn = try mul(bq, n);
    const bqw = try mul(bq, input.words);
    if (input.batch > options.limits.max_batch or input.queries > options.limits.max_queries or input.capacity > options.limits.max_candidates or n > options.limits.max_candidates or
        @max(@max(bqc, bqn), @max(bqw, bc)) > options.limits.max_elements) return error.BoundaryTrainingLimitExceeded;
    if (input.starts.len != bqn or input.ends.len != bqn or input.inside.len != bqw or input.pairs.len != bqc or input.proposals.len != bc or
        input.spans.len != bc or input.pool_mask.len != bc or input.query_mask.len != bq or input.text_mask.len != try mul(input.batch, input.words) or
        input.boundary_mask.len != try mul(input.batch, n) or input.gold.batch != input.batch or input.gold.queries != input.queries)
        return error.InvalidBoundaryTrainingShape;
    if ((input.nulls != null) != (head.enable_abstention and head.abstention_loss_weight > 0) or
        (input.counts != null) != (head.enable_count_head and head.count_loss_weight > 0)) return error.InvalidBoundaryTrainingShape;
    for ([_][]const f32{ input.starts, input.ends, input.inside, input.pairs, input.proposals }) |values| {
        try work.charge(values.len);
        try finite(values);
    }
    for ([_]?[]const f32{ input.nulls, input.counts }) |optional| if (optional) |values| {
        if (values.len != bq) return error.InvalidBoundaryTrainingShape;
        try work.charge(values.len);
        try finite(values);
    };
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const gradients = Gradients{ .starts = try zero(owned, bqn), .ends = try zero(owned, bqn), .inside = try zero(owned, bqw), .pairs = try zero(owned, bqc), .proposals = try zero(owned, bc), .nulls = if (input.nulls != null) try zero(owned, bq) else null, .counts = if (input.counts != null) try zero(owned, bq) else null };
    const keep = try owned.alloc(bool, bqn);
    const valid = try owned.alloc(bool, bqc);
    const proposal_valid = try owned.alloc(bool, bqc);
    const expanded_spans = try owned.alloc(primitive.Span, bqc);
    for (0..input.batch) |b| for (0..input.queries) |q| {
        const row = b * input.queries + q;
        try work.charge(n + input.capacity);
        for (0..n) |w| keep[row * n + w] = input.boundary_mask[b * n + w] and input.query_mask[row];
        for (0..input.capacity) |c| {
            valid[row * input.capacity + c] = input.pool_mask[b * input.capacity + c] and input.query_mask[row];
            // PooledCandidates.to_candidate_batch does not fold query_mask
            // into its proposal-valid mask; consistency uses this exact mask.
            proposal_valid[row * input.capacity + c] = input.pool_mask[b * input.capacity + c];
            expanded_spans[row * input.capacity + c] = input.spans[b * input.capacity + c];
        }
    };
    var dense = try primitive.denseTargets(a, input.gold, input.words, work.remaining());
    defer dense.deinit();
    try work.charge(dense.work);
    const margin_shape = primitive.Shape{ .batch = input.batch, .queries = input.queries, .candidates = n };
    const pair_shape = primitive.Shape{ .batch = input.batch, .queries = input.queries, .candidates = input.capacity };
    const reduction: primitive.Reduction = switch (head.loss_reduction) {
        .global => .global,
        .per_query => .per_query,
        .sum => .sum,
    };
    var terms = Terms{};
    inline for (.{ "start", "end" }) |which| {
        const logits = if (comptime std.mem.eql(u8, which, "start")) input.starts else input.ends;
        const targets = if (comptime std.mem.eql(u8, which, "start")) dense.starts else dense.ends;
        const destination = if (comptime std.mem.eql(u8, which, "start")) gradients.starts else gradients.ends;
        const settings = primitive.BinaryOptions{ .gradient_backend = options.gradient_backend, .cotangent = if (options.gradient_backend != null) @field(options.weights, which) else null, .gradient_precision = options.gradient_precision, .reduction = reduction, .negative_weight = head.boundary_negative_weight, .query_mask = input.query_mask, .limits = work.remaining() };
        var item = if (head.boundary_marginal_loss == .asymmetric_focal)
            try primitive.asymmetricFocal(a, .{ .shape = margin_shape, .values = logits }, targets, keep, .{ .binary = settings, .gamma_positive = head.boundary_focal_gamma_positive, .gamma_negative = head.boundary_focal_gamma_negative, .clip = head.boundary_focal_clip })
        else
            try primitive.balancedBce(a, .{ .shape = margin_shape, .values = logits }, targets, keep, settings);
        defer item.deinit();
        try take(&@field(terms, which), &terms.total, destination, item, @field(options.weights, which), &work);
    }
    var labels = try primitive.candidateLabels(a, .{ .shape = pair_shape, .pooled = true, .spans = input.spans, .valid = input.pool_mask }, input.gold, head.soft_iou_aux_weight > 0, work.remaining());
    defer labels.deinit();
    try work.charge(labels.work);
    var hard = try primitive.hardNegatives(a, .{ .shape = pair_shape, .values = input.pairs }, labels.values, valid, .{ .negatives_per_positive = head.hard_negatives_per_positive, .minimum_negatives = head.minimum_hard_negatives, .keep_all_when_no_positive = head.hard_negative_keep_all_when_absent, .limits = work.remaining() });
    defer hard.deinit();
    try work.charge(hard.work);
    const pair_mask = try owned.dupe(bool, input.query_mask);
    if (options.training and head.negative_query_ratio > 0) {
        const draws = options.negative_query_draws orelse return error.MissingBoundaryTrainingQueryDraws;
        if (draws.len != bq) return error.InvalidBoundaryTrainingShape;
        var positives: usize = 0;
        for (pair_mask, 0..) |*active, row| {
            if (!std.math.isFinite(draws[row]) or draws[row] < 0 or draws[row] >= 1) return error.InvalidBoundaryTrainingQueryDraw;
            active.* = input.query_mask[row] and std.mem.indexOfScalar(f32, labels.values[row * input.capacity ..][0..input.capacity], 1) != null;
            positives += @intFromBool(active.*);
        }
        const requested: f64 = @ceil(@as(f64, @floatFromInt(positives)) * head.negative_query_ratio);
        const retain: usize = @intFromFloat(@min(@max(requested, 1), @as(f64, @floatFromInt(head.max_negative_queries_per_batch))));
        for (0..@min(retain, bq)) |_| {
            try work.charge(bq);
            var best: ?usize = null;
            for (0..bq) |row| if (input.query_mask[row] and !pair_mask[row]) {
                if (best == null or draws[row] > draws[best.?]) best = row;
            };
            pair_mask[best orelse break] = true;
        }
    }
    // The pinned CUDA graph accumulates listwise, soft-IoU and BCE before
    // the deeper consistency branch. Preserve that FP32 association, including
    // after warmup activates consistency (where adding it first loses bits).
    const reverse_pair = options.gradient_backend != null and options.listwise_backend != null;
    var pair_parts: [3]?[]const f32 = @splat(null);
    const binary_options = primitive.BinaryOptions{ .gradient_backend = options.gradient_backend, .cotangent = if (options.gradient_backend != null) options.weights.pair else null, .gradient_precision = options.gradient_precision, .reduction = reduction, .query_mask = pair_mask, .hard_negative_mask = hard.values, .limits = work.remaining() };
    {
        var item = try primitive.candidatePairBce(a, .{ .shape = pair_shape, .values = input.pairs }, labels.values, valid, binary_options);
        defer item.deinit();
        try take(&terms.pair, &terms.total, if (reverse_pair) null else gradients.pairs, item, options.weights.pair, &work);
        if (reverse_pair) {
            try work.charge(item.gradient.len);
            pair_parts[0] = try owned.dupe(f32, item.gradient);
        }
    }
    if (labels.soft_iou) |soft| if (options.scales.soft_iou > 0) {
        const weight = scheduledWeight(head.soft_iou_aux_weight, options.scales.soft_iou, options.gradient_backend != null);
        const effective = try owned.alloc(bool, bqc);
        for (effective, valid, labels.values, hard.values) |*out, active, label, negative| out.* = active and (label > 0.5 or negative);
        var item = try primitive.candidatePairBce(a, .{ .shape = pair_shape, .values = input.pairs }, soft, effective, .{ .gradient_backend = options.gradient_backend, .cotangent = if (options.gradient_backend != null) weight else null, .gradient_precision = options.gradient_precision, .reduction = reduction, .query_mask = pair_mask, .limits = work.remaining() });
        defer item.deinit();
        try take(&terms.soft_iou, &terms.total, if (reverse_pair) null else gradients.pairs, item, weight, &work);
        if (reverse_pair) {
            try work.charge(item.gradient.len);
            pair_parts[1] = try owned.dupe(f32, item.gradient);
        }
    };
    const gold_labels = try owned.alloc(bool, bqc);
    for (gold_labels, labels.values) |*out, label| out.* = label > 0.5;
    if (head.rerank_listwise_weight > 0) {
        var item = try primitive.listwiseWithBackend(a, .{ .shape = pair_shape, .values = input.pairs }, gold_labels, valid, pair_mask, work.remaining(), options.gradient_precision, .{ .backend = options.listwise_backend, .cotangent = if (options.listwise_backend != null) head.rerank_listwise_weight else null });
        defer item.deinit();
        try take(&terms.rerank_listwise, &terms.total, if (reverse_pair) null else gradients.pairs, item, head.rerank_listwise_weight, &work);
        if (reverse_pair) {
            try work.charge(item.gradient.len);
            pair_parts[2] = try owned.dupe(f32, item.gradient);
        }
    }
    {
        var item = try primitive.insideConsistency(a, .{ .shape = .{ .batch = input.batch, .queries = input.queries, .candidates = input.words }, .values = input.inside }, dense.inside, input.text_mask, input.query_mask, .{ .gradient_backend = options.gradient_backend, .cotangent = if (options.gradient_backend != null) options.weights.inside else null, .gradient_precision = options.gradient_precision, .reduction = reduction, .negative_weight = head.boundary_negative_weight, .limits = work.remaining() });
        defer item.deinit();
        try take(&terms.inside, &terms.total, gradients.inside, item, options.weights.inside, &work);
    }
    if (head.proposal_loss_weight > 0) {
        const expanded = try owned.alloc(f32, bqc);
        for (0..input.batch) |b| for (0..input.queries) |q| @memcpy(expanded[(b * input.queries + q) * input.capacity ..][0..input.capacity], input.proposals[b * input.capacity ..][0..input.capacity]);
        var item = try primitive.listwiseWithBackend(a, .{ .shape = pair_shape, .values = expanded }, gold_labels, valid, input.query_mask, work.remaining(), options.gradient_precision, .{ .backend = options.listwise_backend, .cotangent = if (options.listwise_backend != null) head.proposal_loss_weight else null, .reduce_queries = options.listwise_backend != null });
        defer item.deinit();
        try work.charge(item.work);
        terms.proposal = @floatCast(item.value);
        terms.total += terms.proposal * head.proposal_loss_weight;
        if (item.gradient_cotangent) |seed| {
            if (seed != head.proposal_loss_weight) return error.InvalidBoundaryTrainingOptions;
            try addGradient(gradients.proposals, item.gradient, 1);
        } else {
            for (0..input.batch) |b| for (0..input.queries) |q| try addGradient(gradients.proposals[b * input.capacity ..][0..input.capacity], item.gradient[(b * input.queries + q) * input.capacity ..][0..input.capacity], head.proposal_loss_weight);
        }
    }
    if (reverse_pair) {
        var part = pair_parts.len;
        while (part > 0) {
            part -= 1;
            if (pair_parts[part]) |values| try addGradient(gradients.pairs, values, 1);
        }
    }
    if (head.consistency_loss_weight > 0) {
        // Python multiplies configuration/schedule doubles before the tensor
        // conversion. Preserve the legacy FP32 product outside this profile.
        const weight = scheduledWeight(head.consistency_loss_weight, options.scales.consistency, options.consistency_backend != null);
        var item = try primitive.marginalConsistencyWithBackend(a, .{ .shape = pair_shape, .values = input.pairs }, expanded_spans, proposal_valid, .{ .shape = margin_shape, .values = input.starts }, input.ends, keep, work.remaining(), .{ .backend = options.consistency_backend, .cotangent = if (options.consistency_backend != null) weight else null });
        defer item.deinit();
        try work.charge(item.work);
        terms.consistency = @floatCast(item.value);
        terms.total += terms.consistency * weight;
        const gradient_weight: f32 = if (item.gradient_cotangent != null) 1 else weight;
        try addGradient(gradients.pairs, item.pair_gradient, gradient_weight);
        try addGradient(gradients.starts, item.start_gradient, gradient_weight);
        try addGradient(gradients.ends, item.end_gradient, gradient_weight);
    }
    inline for (.{ "nulls", "counts" }) |name| if (@field(input, name)) |logits| {
        const is_null = comptime std.mem.eql(u8, name, "nulls");
        const weight = if (is_null) head.abstention_loss_weight else head.count_loss_weight;
        if (weight > 0) {
            var item = try primitive.queryLossWithBackend(a, logits, .{ .batch = input.batch, .queries = input.queries, .capacity = input.gold.capacity, .values = input.gold.valid }, input.query_mask, if (is_null) .abstention else .poisson_count, work.remaining(), options.gradient_precision, .{ .backend = options.gradient_backend, .cotangent = if (options.gradient_backend != null) weight else null });
            defer item.deinit();
            try take(if (is_null) &terms.abstention else &terms.count, &terms.total, @field(gradients, name).?, item, weight, &work);
        }
    };
    if (!std.math.isFinite(terms.total)) return error.NonFiniteBoundaryTraining;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.objectives.v1");
    for (pair_mask) |active| hash.update(&.{@intFromBool(active)});
    for (hard.values) |active| hash.update(&.{@intFromBool(active)});
    var fingerprint: [32]u8 = undefined;
    hash.final(&fingerprint);
    const hard_copy = try owned.dupe(bool, hard.values);
    try work.check();
    return .{ .arena = arena, .terms = terms, .gradients = gradients, .pair_query_mask = pair_mask, .hard_negative_mask = hard_copy, .decision_fingerprint = fingerprint, .work = work.count };
}

/// Classification and relation loss use one global supervised-label/pair
/// denominator; inactive rows remain exactly zero with no implicit fallback.
pub fn supervisedBce(a: Allocator, logits: []const f32, labels: []const f32, mask: []const bool, weight: f32, limits: primitive.Limits) !primitive.Loss {
    return supervisedBceWithPrecision(a, logits, labels, mask, weight, limits, .reference_f64);
}
pub fn supervisedBceWithPrecision(a: Allocator, logits: []const f32, labels: []const f32, mask: []const bool, weight: f32, limits: primitive.Limits, precision: primitive.GradientPrecision) !primitive.Loss {
    return supervisedBceWithBackend(a, logits, labels, mask, weight, limits, precision, null);
}
pub fn supervisedBceWithBackend(a: Allocator, logits: []const f32, labels: []const f32, mask: []const bool, weight: f32, limits: primitive.Limits, precision: primitive.GradientPrecision, backend: ?primitive.elementwise_math.Backend) !primitive.Loss {
    if (!std.math.isFinite(weight) or weight < 0) return error.InvalidBoundaryTrainingOptions;
    var item = try primitive.balancedBce(a, .{ .shape = .{ .batch = 1, .queries = 1, .candidates = logits.len }, .values = logits }, labels, mask, .{ .gradient_backend = backend, .cotangent = if (backend != null) weight else null, .gradient_precision = precision, .limits = limits });
    errdefer item.deinit();
    item.value *= weight;
    if (item.gradient_cotangent == null) for (item.gradient) |*gradient| {
        gradient.* *= weight;
    };
    item.gradient_cotangent = null;
    if (!std.math.isFinite(item.value)) return error.NonFiniteBoundaryTraining;
    try finite(item.gradient);
    return item;
}

test "binary backend applies supervised scalar weight before VJP exactly once" {
    const Probe = struct {
        fn apply(_: *anyopaque, request: *const primitive.elementwise_math.Request) !void {
            try request.validate();
            try std.testing.expectEqualSlices(f32, &.{ 1.5, 1.5, 0 }, request.cotangents);
            for (request.targets, request.cotangents) |target, *seed|
                seed.* = if (seed.* == 0) 0 else seed.* * (0.5 - target);
        }
    };
    var context: u8 = 0;
    var result = try supervisedBceWithBackend(std.testing.allocator, &.{ 0, 0, 0 }, &.{ 0, 1, 0 }, &.{ true, true, false }, 3, .{}, .tensor_f32, .{ .ptr = &context, .apply = Probe.apply });
    defer result.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 0.75, -0.75, 0 }, result.gradient);
    try std.testing.expect(result.gradient_cotangent == null);
    try std.testing.expectApproxEqAbs(@as(f64, 3 * @log(@as(f64, 2))), result.value, 1e-12);
}

test "CUDA boundary mixed objectives propagate FP32 precision to inside supervision" {
    const head = model.HeadConfig{
        .candidate_pool = .shared,
        .enable_abstention = false,
        .enable_count_head = false,
        .negative_query_ratio = 0,
        .soft_iou_aux_weight = 0,
        .rerank_listwise_weight = 0,
        .proposal_loss_weight = 0,
        .consistency_loss_weight = 0,
    };
    const input = Input{
        .batch = 1,
        .queries = 1,
        .words = 1,
        .capacity = 1,
        .starts = &.{ 0, 0 },
        .ends = &.{ 0, 0 },
        .inside = &.{17},
        .pairs = &.{0},
        .proposals = &.{0},
        .spans = &.{.{ .start = 0, .end = 1 }},
        .pool_mask = &.{true},
        .query_mask = &.{true},
        .text_mask = &.{true},
        .boundary_mask = &.{ true, true },
        .gold = .{ .batch = 1, .queries = 1, .capacity = 1, .spans = &.{.{ .start = 0, .end = 1 }}, .valid = &.{true} },
    };
    var cuda = try boundary(std.testing.allocator, head, input, .{ .gradient_precision = .tensor_f32 });
    defer cuda.deinit();
    try std.testing.expectEqual(@as(f32, 0), cuda.gradients.inside[0]);
    var reference = try boundary(std.testing.allocator, head, input, .{});
    defer reference.deinit();
    try std.testing.expect(reference.gradients.inside[0] < 0);
}

test "boundary training objectives schedules match pinned hold warmup and anneal endpoints" {
    const head = model.HeadConfig{};
    const initial = try scales(head, .{ .optimizer_step = 0, .total_optimizer_steps = 100 });
    try std.testing.expectEqual(Scales{ .gold_injection = 1, .consistency = 0, .soft_iou = 1 }, initial);
    try std.testing.expectEqual(@as(f32, 1), (try scales(head, .{ .optimizer_step = 15, .total_optimizer_steps = 100 })).gold_injection);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), (try scales(head, .{ .optimizer_step = 100, .total_optimizer_steps = 100 })).gold_injection, 1e-7);
    var disabled = head;
    disabled.consistency_warmup_steps = 0;
    disabled.soft_iou_anneal_steps = 0;
    const result = try scales(disabled, .{ .optimizer_step = 300, .total_optimizer_steps = 100 });
    try std.testing.expectEqual(@as(f64, 1), result.consistency);
    try std.testing.expectEqual(@as(f64, 0), result.soft_iou);
    try std.testing.expectError(error.InvalidBoundaryTrainingSchedule, scales(head, .{ .optimizer_step = 0, .total_optimizer_steps = 1, .gold_hold_fraction = 1.1 }));
}

test "CUDA boundary pair loss accumulation preserves the active consistency contribution" {
    const Probe = struct {
        fn binary(_: *anyopaque, request: *const primitive.elementwise_math.Request) !void {
            try request.validate();
            for (request.cotangents) |*seed| seed.* = if (seed.* == 0) 0 else 1e8;
        }
        fn listwise(_: *anyopaque, request: *const primitive.listwise_math.Request) !void {
            try request.validate();
            @memset(request.gradient, -1e8);
        }
    };
    const head = model.HeadConfig{
        .candidate_pool = .shared,
        .enable_abstention = false,
        .enable_count_head = false,
        .negative_query_ratio = 0,
        .soft_iou_aux_weight = 0,
        .rerank_listwise_weight = 1,
        .proposal_loss_weight = 0,
        .consistency_loss_weight = 0.1,
    };
    const input = Input{
        .batch = 1,
        .queries = 1,
        .words = 1,
        .capacity = 1,
        .starts = &.{ 2, 2 },
        .ends = &.{ 2, 2 },
        .inside = &.{0},
        .pairs = &.{0},
        .proposals = &.{0},
        .spans = &.{.{ .start = 0, .end = 1 }},
        .pool_mask = &.{true},
        .query_mask = &.{true},
        .text_mask = &.{true},
        .boundary_mask = &.{ true, true },
        .gold = .{ .batch = 1, .queries = 1, .capacity = 1, .spans = &.{.{ .start = 0, .end = 1 }}, .valid = &.{true} },
    };
    var context: u8 = 0;
    const options = Options{
        .gradient_precision = .tensor_f32,
        .gradient_backend = .{ .ptr = &context, .apply = Probe.binary },
        .listwise_backend = .{ .ptr = &context, .apply = Probe.listwise },
    };
    var result = try boundary(std.testing.allocator, head, input, options);
    defer result.deinit();
    var consistency = try primitive.marginalConsistency(std.testing.allocator, .{ .shape = .{ .batch = 1, .queries = 1, .candidates = 1 }, .values = input.pairs }, input.spans, input.pool_mask, .{ .shape = .{ .batch = 1, .queries = 1, .candidates = 2 }, .values = input.starts }, input.ends, input.boundary_mask, .{});
    defer consistency.deinit();
    const expected = consistency.pair_gradient[0] * @as(f32, @floatCast(head.consistency_loss_weight));
    try std.testing.expect(expected != 0);
    // The equal and opposite large branch seeds cancel before this small
    // contribution arrives; placing it first would silently round it away.
    try std.testing.expectEqual(expected, result.gradients.pairs[0]);
}

test "boundary consistency schedule preserves Python scalar multiplication" {
    const head = model.HeadConfig{};
    const scheduled = try scales(head, .{ .optimizer_step = 2, .total_optimizer_steps = 100 });
    const precise: f32 = @floatCast(head.consistency_loss_weight * scheduled.consistency);
    const legacy = @as(f32, @floatCast(head.consistency_loss_weight)) * @as(f32, @floatCast(scheduled.consistency));
    try std.testing.expectEqual(@as(f32, 0.0001), precise);
    try std.testing.expect(precise != legacy);
}

test "boundary soft IoU schedule retains the final scalar rounding boundary" {
    const head = model.HeadConfig{};
    const scheduled = try scales(head, .{ .optimizer_step = 3, .total_optimizer_steps = 100 });
    const precise = scheduledWeight(head.soft_iou_aux_weight, scheduled.soft_iou, true);
    const legacy = scheduledWeight(head.soft_iou_aux_weight, scheduled.soft_iou, false);
    try std.testing.expectEqual(@as(f32, 0.19997000694274902), precise);
    try std.testing.expectEqual(@as(f32, 0.19996999204158783), legacy);
}
