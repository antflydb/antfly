// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GLiNER2.5 boundary training losses and explicit logit gradients.
//! Formulas and reductions follow pinned upstream commit 3c913c7369301133d3b7699252074c4303ada50e.
//! Candidate ranking/labels are discrete, detached supervision. Floating-point
//! inputs are required: this module does not implement quantized training.
//! These kernels do not by themselves provide a differentiable encoder/head.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Layout = enum { query_candidate, candidate_query };
pub const Reduction = enum { global, per_query, sum };
pub const Shape = struct {
    batch: usize,
    queries: usize,
    candidates: usize,
    layout: Layout = .query_candidate,
    pub fn index(self: Shape, b: usize, q: usize, c: usize) usize {
        return if (self.layout == .query_candidate)
            (b * self.queries + q) * self.candidates + c
        else
            (b * self.candidates + c) * self.queries + q;
    }
    fn count(self: Shape, limits: Limits) !usize {
        if (self.batch > limits.max_batch or self.queries > limits.max_queries or self.candidates > limits.max_candidates or self.candidates > limits.max_elements)
            return error.BoundaryTrainingLimitExceeded;
        const rows = try multiply(self.batch, self.queries);
        if (rows > limits.max_elements) return error.BoundaryTrainingLimitExceeded;
        const n = try multiply(rows, self.candidates);
        if (n > limits.max_elements) return error.BoundaryTrainingLimitExceeded;
        return n;
    }
};
pub const Tensor = struct { shape: Shape, values: []const f32 };
pub const Span = struct { start: i64, end: i64 };
pub const Limits = struct {
    max_batch: usize = 128,
    max_queries: usize = 1024,
    max_candidates: usize = 65536,
    max_elements: usize = 16 * 1024 * 1024,
    max_work: usize = 100000000,
    control: ?Control = null,
};
const Work = struct {
    limits: Limits,
    steps: usize = 0,
    fn tick(self: *Work) !void {
        if (self.steps >= self.limits.max_work) return error.BoundaryTrainingLimitExceeded;
        self.steps += 1;
        if (self.steps % 128 == 1) try self.check();
    }
    fn check(self: *const Work) !void {
        if (self.limits.control) |control| try control.check();
    }
};
pub const Loss = struct {
    allocator: Allocator,
    value: f64,
    /// Derivative with respect to the input logits, in the input axis order.
    gradient: []f32,
    work: usize,
    pub fn deinit(self: *Loss) void {
        self.allocator.free(self.gradient);
        self.* = undefined;
    }
};
pub fn Owned(comptime T: type) type {
    return struct {
        allocator: Allocator,
        values: []T,
        work: usize,
        pub fn deinit(self: *@This()) void {
            self.allocator.free(self.values);
            self.* = undefined;
        }
    };
}
pub const BinaryOptions = struct {
    reduction: Reduction = .global,
    negative_weight: f64 = 1,
    /// Upstream global reduction does not apply this mask. Its caller must
    /// already include query validity in valid_mask; sum/per_query use it.
    query_mask: ?[]const bool = null,
    /// Positives are retained even when absent from this detached mask.
    hard_negative_mask: ?[]const bool = null,
    limits: Limits = .{},
};
pub const FocalOptions = struct {
    binary: BinaryOptions = .{},
    gamma_positive: f64 = 0,
    gamma_negative: f64 = 2,
    clip: f64 = 0.05,
};
fn multiply(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryTrainingLimitExceeded;
}
fn finite(value: f64) !void {
    if (!std.math.isFinite(value)) return error.NonFiniteBoundaryTraining;
}
fn probability(value: f64) !void {
    try finite(value);
    if (value < 0 or value > 1) return error.InvalidBoundaryTrainingTargets;
}
fn scalar32(value: f64) !f32 {
    try finite(value);
    const out: f32 = @floatCast(value);
    if (!std.math.isFinite(out)) return error.NonFiniteBoundaryTraining;
    return out;
}
fn sigmoid(x: f64) f64 {
    return if (x >= 0) 1 / (1 + @exp(-x)) else blk: {
        const e = @exp(x);
        break :blk e / (1 + e);
    };
}
fn bce(x: f64, y: f64) f64 {
    return @max(x, 0) - x * y + std.math.log1p(@exp(-@abs(x)));
}
fn validateTensor(tensor: Tensor, limits: Limits) !usize {
    const n = try tensor.shape.count(limits);
    if (tensor.values.len != n) return error.InvalidBoundaryTrainingShape;
    return n;
}
fn validateQueryMask(shape: Shape, mask: ?[]const bool) !void {
    if (mask) |values| if (values.len != try multiply(shape.batch, shape.queries)) return error.InvalidBoundaryTrainingShape;
}
fn activeQuery(mask: ?[]const bool, row: usize) bool {
    return if (mask) |values| values[row] else true;
}
fn keepAt(valid: []const bool, targets: []const f32, hard: ?[]const bool, index: usize) bool {
    return valid[index] and (if (hard) |mask| targets[index] > 0.5 or mask[index] else true);
}
const ReductionPlan = struct {
    allocator: Allocator,
    counts: []usize,
    active: usize,
    total: usize,
    fn deinit(self: *ReductionPlan) void {
        self.allocator.free(self.counts);
    }
    fn factor(self: ReductionPlan, options: BinaryOptions, row: usize) f64 {
        return switch (options.reduction) {
            .global => 1 / @as(f64, @floatFromInt(@max(self.total, 1))),
            .sum => if (activeQuery(options.query_mask, row)) 1 else 0,
            .per_query => if (activeQuery(options.query_mask, row) and self.counts[row] > 0)
                1 / (@as(f64, @floatFromInt(self.counts[row])) * @as(f64, @floatFromInt(@max(self.active, 1))))
            else
                0,
        };
    }
};
fn reductionPlan(a: Allocator, shape: Shape, targets: []const f32, valid: []const bool, options: BinaryOptions, work: *Work) !ReductionPlan {
    const counts = try a.alloc(usize, try multiply(shape.batch, shape.queries));
    errdefer a.free(counts);
    @memset(counts, 0);
    var total: usize = 0;
    var active: usize = 0;
    for (0..shape.batch) |b| for (0..shape.queries) |q| {
        try work.tick();
        const row = b * shape.queries + q;
        for (0..shape.candidates) |c| {
            try work.tick();
            if (keepAt(valid, targets, options.hard_negative_mask, shape.index(b, q, c))) {
                counts[row] += 1;
                total += 1;
            }
        }
        if (counts[row] > 0 and activeQuery(options.query_mask, row)) active += 1;
    };
    return .{ .allocator = a, .counts = counts, .active = active, .total = total };
}
fn binary(a: Allocator, logits: Tensor, targets: []const f32, valid: []const bool, options: BinaryOptions, focal: ?FocalOptions) !Loss {
    var work = Work{ .limits = options.limits };
    try work.check();
    const n = try validateTensor(logits, options.limits);
    if (targets.len != n or valid.len != n) return error.InvalidBoundaryTrainingShape;
    if (options.hard_negative_mask) |mask| if (mask.len != n) return error.InvalidBoundaryTrainingShape;
    try validateQueryMask(logits.shape, options.query_mask);
    try finite(options.negative_weight);
    if (options.negative_weight < 0) return error.InvalidBoundaryTrainingOptions;
    if (focal) |settings| {
        try finite(settings.gamma_positive);
        try finite(settings.gamma_negative);
        try finite(settings.clip);
        if (settings.gamma_positive < 0 or settings.gamma_negative < 0 or settings.clip < 0 or settings.clip > 1)
            return error.InvalidBoundaryTrainingOptions;
    }
    if (options.limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    var plan = try reductionPlan(a, logits.shape, targets, valid, options, &work);
    defer plan.deinit();
    const gradient = try a.alloc(f32, n);
    errdefer a.free(gradient);
    @memset(gradient, 0);
    var loss: f64 = 0;
    for (0..logits.shape.batch) |b| for (0..logits.shape.queries) |q| {
        try work.tick();
        const scale = plan.factor(options, b * logits.shape.queries + q);
        for (0..logits.shape.candidates) |c| {
            try work.tick();
            const i = logits.shape.index(b, q, c);
            if (!keepAt(valid, targets, options.hard_negative_mask, i)) continue;
            const x: f64 = logits.values[i];
            const y: f64 = targets[i];
            try finite(x);
            try probability(y);
            if (scale == 0) continue;
            const p = sigmoid(x);
            if (focal) |settings| {
                const gp = settings.gamma_positive;
                const gn = settings.gamma_negative;
                const negative = @min(1 - p + settings.clip, 1);
                const log_positive = @log(@max(p, 1e-8));
                const log_negative = @log(@max(negative, 1e-8));
                const pos_power = std.math.pow(f64, 1 - p, gp);
                const neg_power = std.math.pow(f64, p, gn);
                const positive_derivative = (if (p >= 1e-8) pos_power / p else 0) -
                    (if (gp == 0) 0 else log_positive * gp * std.math.pow(f64, 1 - p, gp - 1));
                const negative_derivative = (if (1 - p + settings.clip <= 1 and negative >= 1e-8) -neg_power / negative else 0) +
                    (if (gn == 0) 0 else log_negative * gn * std.math.pow(f64, p, gn - 1));
                loss -= scale * (y * log_positive * pos_power + (1 - y) * log_negative * neg_power * options.negative_weight);
                gradient[i] = try scalar32(-scale * (y * positive_derivative + (1 - y) * options.negative_weight * negative_derivative) * p * (1 - p));
            } else {
                const weight: f64 = if (y > 0.5) 1 else options.negative_weight;
                loss += scale * weight * bce(x, y);
                gradient[i] = try scalar32(scale * weight * (p - y));
            }
        }
    };
    try finite(loss);
    try work.check();
    return .{ .allocator = a, .value = loss, .gradient = gradient, .work = work.steps };
}
pub fn balancedBce(a: Allocator, logits: Tensor, targets: []const f32, valid: []const bool, options: BinaryOptions) !Loss {
    return binary(a, logits, targets, valid, options, null);
}
pub fn candidatePairBce(a: Allocator, logits: Tensor, labels: []const f32, valid: []const bool, options: BinaryOptions) !Loss {
    if (options.negative_weight != 1) return error.InvalidBoundaryTrainingOptions;
    return binary(a, logits, labels, valid, options, null);
}
pub fn asymmetricFocal(a: Allocator, logits: Tensor, targets: []const f32, valid: []const bool, options: FocalOptions) !Loss {
    if (options.binary.hard_negative_mask != null) return error.InvalidBoundaryTrainingOptions;
    return binary(a, logits, targets, valid, options.binary, options);
}

/// Inside supervision uses BCE even when marginal boundaries use focal loss.
pub fn insideConsistency(a: Allocator, logits: Tensor, targets: []const f32, text_mask: []const bool, query_mask: []const bool, options: BinaryOptions) !Loss {
    var work = Work{ .limits = options.limits };
    try work.check();
    const n = try validateTensor(logits, options.limits);
    try validateQueryMask(logits.shape, query_mask);
    if (targets.len != n or text_mask.len != try multiply(logits.shape.batch, logits.shape.candidates) or options.hard_negative_mask != null)
        return error.InvalidBoundaryTrainingShape;
    if (options.limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    const valid = try a.alloc(bool, n);
    defer a.free(valid);
    for (0..logits.shape.batch) |b| for (0..logits.shape.queries) |q| for (0..logits.shape.candidates) |c| {
        try work.tick();
        valid[logits.shape.index(b, q, c)] = text_mask[b * logits.shape.candidates + c] and query_mask[b * logits.shape.queries + q];
    };
    var adjusted = options;
    adjusted.query_mask = query_mask;
    adjusted.limits.max_work -|= work.steps;
    var result = try balancedBce(a, logits, targets, valid, adjusted);
    result.work += work.steps;
    return result;
}

/// Proposal and reranker gold-mass objective. Invalid candidates retain the
/// upstream finite -1e4 sentinel in logsumexp, and have exactly zero gradient.
pub fn listwise(a: Allocator, logits: Tensor, gold: []const bool, valid: []const bool, query_mask: []const bool, limits: Limits) !Loss {
    var work = Work{ .limits = limits };
    try work.check();
    const n = try validateTensor(logits, limits);
    if (gold.len != n or valid.len != n) return error.InvalidBoundaryTrainingShape;
    try validateQueryMask(logits.shape, query_mask);
    if (limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    var active: usize = 0;
    for (0..logits.shape.batch) |b| for (0..logits.shape.queries) |q| {
        try work.tick();
        var has_gold = false;
        for (0..logits.shape.candidates) |c| {
            try work.tick();
            const i = logits.shape.index(b, q, c);
            if (valid[i]) try finite(logits.values[i]);
            has_gold = has_gold or gold[i];
        }
        if (has_gold and query_mask[b * logits.shape.queries + q]) active += 1;
    };
    const gradient = try a.alloc(f32, n);
    errdefer a.free(gradient);
    @memset(gradient, 0);
    var value: f64 = 0;
    for (0..logits.shape.batch) |b| for (0..logits.shape.queries) |q| {
        try work.tick();
        if (!query_mask[b * logits.shape.queries + q]) continue;
        var has_gold = false;
        var max_all: f64 = -std.math.inf(f64);
        var max_gold: f64 = -std.math.inf(f64);
        for (0..logits.shape.candidates) |c| {
            try work.tick();
            const i = logits.shape.index(b, q, c);
            const x: f64 = if (valid[i]) logits.values[i] else -1e4;
            max_all = @max(max_all, x);
            max_gold = @max(max_gold, if (gold[i]) x else -1e4);
            has_gold = has_gold or gold[i];
        }
        if (!has_gold) continue;
        var sum_all: f64 = 0;
        var sum_gold: f64 = 0;
        for (0..logits.shape.candidates) |c| {
            try work.tick();
            const i = logits.shape.index(b, q, c);
            const x: f64 = if (valid[i]) logits.values[i] else -1e4;
            sum_all += @exp(x - max_all);
            sum_gold += @exp((if (gold[i]) x else -1e4) - max_gold);
        }
        const divisor: f64 = @floatFromInt(@max(active, 1));
        value += ((max_all + @log(sum_all)) - (max_gold + @log(sum_gold))) / divisor;
        for (0..logits.shape.candidates) |c| {
            try work.tick();
            const i = logits.shape.index(b, q, c);
            if (!valid[i]) continue;
            const x: f64 = logits.values[i];
            gradient[i] = try scalar32((@exp(x - max_all) / sum_all - (if (gold[i]) @exp(x - max_gold) / sum_gold else 0)) / divisor);
        }
    };
    try finite(value);
    try work.check();
    return .{ .allocator = a, .value = value, .gradient = gradient, .work = work.steps };
}

pub const MentionMask = struct { batch: usize, queries: usize, capacity: usize, values: []const bool };
pub const QueryObjective = enum { abstention, poisson_count };
/// Query-level objectives normalized over active queries. Count is Poisson
/// NLL with log_input=true/full=false; no factorial constant is added.
pub fn queryLoss(a: Allocator, logits: []const f32, mentions: MentionMask, query_mask: []const bool, objective: QueryObjective, limits: Limits) !Loss {
    var work = Work{ .limits = limits };
    try work.check();
    const shape = Shape{ .batch = mentions.batch, .queries = mentions.queries, .candidates = mentions.capacity };
    const n = try shape.count(limits);
    const rows = try multiply(mentions.batch, mentions.queries);
    if (mentions.values.len != n or logits.len != rows or query_mask.len != rows) return error.InvalidBoundaryTrainingShape;
    if (limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    var active: usize = 0;
    for (query_mask) |valid| {
        try work.tick();
        if (valid) active += 1;
    }
    const scale = 1 / @as(f64, @floatFromInt(@max(active, 1)));
    const gradient = try a.alloc(f32, rows);
    errdefer a.free(gradient);
    @memset(gradient, 0);
    var value: f64 = 0;
    for (query_mask, logits, 0..) |valid, x32, row| {
        try work.tick();
        if (!valid) continue;
        const x: f64 = x32;
        try finite(x);
        var count: usize = 0;
        for (mentions.values[row * mentions.capacity ..][0..mentions.capacity]) |present| {
            try work.tick();
            if (present) count += 1;
        }
        switch (objective) {
            .abstention => {
                const target: f64 = if (count == 0) 1 else 0;
                value += scale * bce(x, target);
                gradient[row] = try scalar32(scale * (sigmoid(x) - target));
            },
            .poisson_count => {
                const target: f64 = @floatFromInt(count);
                const rate = @exp(x);
                value += scale * (rate - target * x);
                gradient[row] = try scalar32(scale * (rate - target));
            },
        }
    }
    try finite(value);
    try work.check();
    return .{ .allocator = a, .value = value, .gradient = gradient, .work = work.steps };
}

pub const HardNegativeOptions = struct {
    negatives_per_positive: usize = 3,
    minimum_negatives: usize = 1,
    keep_all_when_no_positive: bool = false,
    limits: Limits = .{},
};
const Ranked = struct { score: f32, index: usize };
fn ranksBefore(a: Ranked, b: Ranked) bool {
    return a.score > b.score or (a.score == b.score and a.index < b.index);
}
fn heapInsert(heap: []Ranked, size: *usize, value: Ranked, work: *Work) !void {
    var index = size.*;
    size.* += 1;
    heap[index] = value;
    while (index > 0) {
        try work.tick();
        const parent = (index - 1) / 2;
        if (!ranksBefore(heap[index], heap[parent])) break;
        std.mem.swap(Ranked, &heap[index], &heap[parent]);
        index = parent;
    }
}
fn heapRemove(heap: []Ranked, size: *usize, work: *Work) !Ranked {
    const result = heap[0];
    size.* -= 1;
    if (size.* == 0) return result;
    heap[0] = heap[size.*];
    var index: usize = 0;
    while (index * 2 + 1 < size.*) {
        try work.tick();
        var child = index * 2 + 1;
        if (child + 1 < size.* and ranksBefore(heap[child + 1], heap[child])) child += 1;
        if (!ranksBefore(heap[child], heap[index])) break;
        std.mem.swap(Ranked, &heap[child], &heap[index]);
        index = child;
    }
    return result;
}
pub fn hardNegatives(a: Allocator, logits: Tensor, labels: []const f32, valid: []const bool, options: HardNegativeOptions) !Owned(bool) {
    var work = Work{ .limits = options.limits };
    try work.check();
    const n = try validateTensor(logits, options.limits);
    if (labels.len != n or valid.len != n) return error.InvalidBoundaryTrainingShape;
    if (options.limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    const output = try a.alloc(bool, n);
    errdefer a.free(output);
    @memset(output, false);
    const heap = try a.alloc(Ranked, logits.shape.candidates);
    defer a.free(heap);
    for (0..logits.shape.batch) |b| for (0..logits.shape.queries) |q| {
        try work.tick();
        var size: usize = 0;
        var positive: usize = 0;
        for (0..logits.shape.candidates) |c| {
            try work.tick();
            const index = logits.shape.index(b, q, c);
            if (!valid[index]) continue;
            try finite(logits.values[index]);
            try probability(labels[index]);
            if (labels[index] > 0.5) {
                output[index] = true;
                positive += 1;
            } else try heapInsert(heap, &size, .{ .score = logits.values[index], .index = c }, &work);
        }
        const cap = if (positive == 0 and options.keep_all_when_no_positive) size else @min(size, @max(options.minimum_negatives, std.math.mul(usize, options.negatives_per_positive, positive) catch std.math.maxInt(usize)));
        for (0..cap) |_| {
            try work.tick();
            const selected = try heapRemove(heap, &size, &work);
            output[logits.shape.index(b, q, selected.index)] = true;
        }
    };
    try work.check();
    return .{ .allocator = a, .values = output, .work = work.steps };
}

pub const Candidates = struct {
    shape: Shape,
    /// Pooled candidates use [B,C] spans/mask, with one label for every query.
    pooled: bool = false,
    spans: []const Span,
    valid: []const bool,
};
pub const Gold = struct { batch: usize, queries: usize, capacity: usize, spans: []const Span, valid: []const bool };
pub const Labels = struct {
    arena: std.heap.ArenaAllocator,
    values: []f32,
    soft_iou: ?[]f32,
    work: usize,
    pub fn deinit(self: *Labels) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
fn validateSpan(span: Span) !void {
    if (span.start < 0 or span.end <= span.start) return error.InvalidBoundaryTrainingTargets;
}
fn validateGold(gold: Gold, limits: Limits, work: *Work) !void {
    const count = try (Shape{ .batch = gold.batch, .queries = gold.queries, .candidates = gold.capacity }).count(limits);
    if (gold.spans.len != count or gold.valid.len != count) return error.InvalidBoundaryTrainingShape;
    for (gold.spans, gold.valid) |span, valid| {
        try work.tick();
        if (valid) try validateSpan(span);
    }
}
pub fn candidateLabels(a: Allocator, candidates: Candidates, gold: Gold, soft_iou: bool, limits: Limits) !Labels {
    var work = Work{ .limits = limits };
    try work.check();
    const n = try candidates.shape.count(limits);
    const input_count = if (candidates.pooled) try multiply(candidates.shape.batch, candidates.shape.candidates) else n;
    if (input_count > limits.max_elements) return error.BoundaryTrainingLimitExceeded;
    if (candidates.spans.len != input_count or candidates.valid.len != input_count or gold.batch != candidates.shape.batch or gold.queries != candidates.shape.queries)
        return error.InvalidBoundaryTrainingShape;
    if (limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    try validateGold(gold, limits, &work);
    for (candidates.spans, candidates.valid) |span, valid| {
        try work.tick();
        if (valid) try validateSpan(span);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const values = try arena.allocator().alloc(f32, n);
    const soft = if (soft_iou) try arena.allocator().alloc(f32, n) else null;
    @memset(values, 0);
    if (soft) |out| @memset(out, 0);
    for (0..candidates.shape.batch) |b| for (0..candidates.shape.queries) |q| for (0..candidates.shape.candidates) |c| {
        try work.tick();
        const index = candidates.shape.index(b, q, c);
        const source = if (candidates.pooled) b * candidates.shape.candidates + c else index;
        if (!candidates.valid[source]) continue;
        const candidate = candidates.spans[source];
        const row = (b * gold.queries + q) * gold.capacity;
        for (gold.spans[row..][0..gold.capacity], gold.valid[row..][0..gold.capacity]) |expected, valid| {
            try work.tick();
            if (!valid) continue;
            if (candidate.start == expected.start and candidate.end == expected.end) values[index] = 1;
            if (soft) |out| {
                const intersection = @max(0, @min(candidate.end, expected.end) - @max(candidate.start, expected.start));
                // Convert before addition so valid large coordinates cannot
                // overflow the signed integer used for an individual span.
                const union_size = @as(f64, @floatFromInt(candidate.end - candidate.start)) +
                    @as(f64, @floatFromInt(expected.end - expected.start)) - @as(f64, @floatFromInt(intersection));
                const iou: f32 = @floatCast(@as(f64, @floatFromInt(intersection)) / @max(union_size, 1));
                out[index] = @max(out[index], iou);
            }
        }
    };
    try work.check();
    return .{ .arena = arena, .values = values, .soft_iou = soft, .work = work.steps };
}

pub const ConsistencyLoss = struct {
    arena: std.heap.ArenaAllocator,
    value: f64,
    pair_gradient: []f32,
    start_gradient: []f32,
    end_gradient: []f32,
    work: usize,
    pub fn deinit(self: *ConsistencyLoss) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
/// Noisy-OR boundary consistency. Gradients flow through both pair and
/// marginal logits; the discrete candidate boundary indices are detached.
pub fn marginalConsistency(a: Allocator, pairs: Tensor, spans: []const Span, valid: []const bool, starts: Tensor, ends: []const f32, boundary_keep: []const bool, limits: Limits) !ConsistencyLoss {
    var work = Work{ .limits = limits };
    try work.check();
    const count = try validateTensor(pairs, limits);
    const boundary_count = try validateTensor(starts, limits);
    if (spans.len != count or valid.len != count or ends.len != boundary_count or boundary_keep.len != boundary_count or starts.shape.batch != pairs.shape.batch or starts.shape.queries != pairs.shape.queries or starts.shape.layout != .query_candidate)
        return error.InvalidBoundaryTrainingShape;
    if (starts.shape.candidates == 0 and count > 0) return error.InvalidBoundaryTrainingShape;
    if (limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const pair_gradient = try owned.alloc(f32, count);
    const start_gradient = try owned.alloc(f32, boundary_count);
    const end_gradient = try owned.alloc(f32, boundary_count);
    @memset(pair_gradient, 0);
    @memset(start_gradient, 0);
    @memset(end_gradient, 0);
    const survival_start = try a.alloc(f64, boundary_count);
    defer a.free(survival_start);
    const survival_end = try a.alloc(f64, boundary_count);
    defer a.free(survival_end);
    const reached_start = try a.alloc(bool, boundary_count);
    defer a.free(reached_start);
    const reached_end = try a.alloc(bool, boundary_count);
    defer a.free(reached_end);
    @memset(survival_start, 0);
    @memset(survival_end, 0);
    @memset(reached_start, false);
    @memset(reached_end, false);
    for (0..pairs.shape.batch) |b| for (0..pairs.shape.queries) |q| for (0..pairs.shape.candidates) |c| {
        try work.tick();
        const i = pairs.shape.index(b, q, c);
        if (!valid[i]) continue;
        try validateSpan(spans[i]);
        if (spans[i].end >= starts.shape.candidates) return error.InvalidBoundaryTrainingTargets;
        try finite(pairs.values[i]);
        const p = sigmoid(pairs.values[i]);
        const log_survival = std.math.log1p(-@min(p, 1 - 1e-6));
        const s = starts.shape.index(b, q, @intCast(spans[i].start));
        const e = starts.shape.index(b, q, @intCast(spans[i].end));
        survival_start[s] += log_survival;
        survival_end[e] += log_survival;
        reached_start[s] = true;
        reached_end[e] = true;
    };
    var start_count: usize = 0;
    var end_count: usize = 0;
    for (boundary_keep, reached_start, reached_end) |keep, reached_s, reached_e| {
        try work.tick();
        if (keep and reached_s) start_count += 1;
        if (keep and reached_e) end_count += 1;
    }
    const start_scale = 1 / @as(f64, @floatFromInt(@max(start_count, 1)));
    const end_scale = 1 / @as(f64, @floatFromInt(@max(end_count, 1)));
    var value: f64 = 0;
    // The scratch arrays become dL/d(noisy-OR probability), multiplied by
    // survival. That factor is reused by all candidates at this boundary.
    for (0..boundary_count) |i| {
        try work.tick();
        if (boundary_keep[i] and reached_start[i]) {
            try finite(starts.values[i]);
            const target = sigmoid(starts.values[i]);
            const survived = @exp(survival_start[i]);
            const difference = (1 - survived) - target;
            value += 0.5 * start_scale * difference * difference;
            start_gradient[i] = try scalar32(-start_scale * difference * target * (1 - target));
            survival_start[i] = start_scale * difference * survived;
        } else survival_start[i] = 0;
        if (boundary_keep[i] and reached_end[i]) {
            try finite(ends[i]);
            const target = sigmoid(ends[i]);
            const survived = @exp(survival_end[i]);
            const difference = (1 - survived) - target;
            value += 0.5 * end_scale * difference * difference;
            end_gradient[i] = try scalar32(-end_scale * difference * target * (1 - target));
            survival_end[i] = end_scale * difference * survived;
        } else survival_end[i] = 0;
    }
    for (0..pairs.shape.batch) |b| for (0..pairs.shape.queries) |q| for (0..pairs.shape.candidates) |c| {
        try work.tick();
        const i = pairs.shape.index(b, q, c);
        if (!valid[i]) continue;
        const p = sigmoid(pairs.values[i]);
        if (p > 1 - 1e-6) continue;
        const s = starts.shape.index(b, q, @intCast(spans[i].start));
        const e = starts.shape.index(b, q, @intCast(spans[i].end));
        pair_gradient[i] = try scalar32((survival_start[s] + survival_end[e]) * p);
    };
    try finite(value);
    try work.check();
    return .{ .arena = arena, .value = value, .pair_gradient = pair_gradient, .start_gradient = start_gradient, .end_gradient = end_gradient, .work = work.steps };
}

pub const DenseTargets = struct {
    arena: std.heap.ArenaAllocator,
    starts: []f32,
    ends: []f32,
    inside: []f32,
    work: usize,
    pub fn deinit(self: *DenseTargets) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
/// Construct exact multi-label boundaries and union-of-mentions token labels.
/// Active invalid spans are rejected, matching the full model's validation;
/// masked padding coordinates are ignored. No gold target is truncated.
pub fn denseTargets(a: Allocator, gold: Gold, text_length: usize, limits: Limits) !DenseTargets {
    var work = Work{ .limits = limits };
    try work.check();
    if (limits.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    try validateGold(gold, limits, &work);
    const boundary_width = std.math.add(usize, text_length, 1) catch return error.BoundaryTrainingLimitExceeded;
    const boundary_shape = Shape{ .batch = gold.batch, .queries = gold.queries, .candidates = boundary_width };
    const n = try boundary_shape.count(limits);
    const inside_shape = Shape{ .batch = gold.batch, .queries = gold.queries, .candidates = text_length };
    const inside_count = try inside_shape.count(limits);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const starts = try owned.alloc(f32, n);
    const ends = try owned.alloc(f32, n);
    const inside = try owned.alloc(f32, inside_count);
    const difference = try a.alloc(i64, boundary_width);
    defer a.free(difference);
    @memset(starts, 0);
    @memset(ends, 0);
    for (0..gold.batch) |b| for (0..gold.queries) |q| {
        try work.tick();
        @memset(difference, 0);
        const row = (b * gold.queries + q) * gold.capacity;
        for (gold.spans[row..][0..gold.capacity], gold.valid[row..][0..gold.capacity]) |span, valid| {
            try work.tick();
            if (!valid) continue;
            if (span.end > text_length) return error.InvalidBoundaryTrainingTargets;
            const s: usize = @intCast(span.start);
            const e: usize = @intCast(span.end);
            starts[boundary_shape.index(b, q, s)] = 1;
            ends[boundary_shape.index(b, q, e)] = 1;
            difference[s] += 1;
            difference[e] -= 1;
        }
        var active: i64 = 0;
        for (0..text_length) |t| {
            try work.tick();
            active += difference[t];
            inside[inside_shape.index(b, q, t)] = if (active > 0) 1 else 0;
        }
    };
    try work.check();
    return .{ .arena = arena, .starts = starts, .ends = ends, .inside = inside, .work = work.steps };
}

fn expectNear(expected: f64, actual: f64) !void {
    try std.testing.expectApproxEqAbs(expected, actual, 2e-6 * @max(1, @abs(expected)));
}
fn expectGradient(expected: []const f32, actual: []const f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |wanted, got| try expectNear(wanted, got);
}
test "boundary training losses match pinned Torch values gradients masks and targets" {
    const Fixture = struct {
        format_version: u32,
        provenance: std.json.Value,
        loss_cases: []const struct {
            kind: enum { bce, focal, pair, listwise, abstention, poisson_count },
            shape: Shape,
            logits: []const f32,
            targets: []const f32 = &.{},
            valid: []const bool = &.{},
            hard: ?[]const bool = null,
            gold: []const bool = &.{},
            query_mask: []const bool,
            reduction: Reduction = .global,
            negative_weight: f64 = 1,
            gamma_positive: f64 = 0,
            gamma_negative: f64 = 2,
            clip: f64 = 0.05,
            mentions: []const bool = &.{},
            mention_capacity: usize = 0,
            expected: f64,
            gradient: []const f32,
        },
        hard_cases: []const struct {
            shape: Shape,
            logits: []const f32,
            labels: []const f32,
            valid: []const bool,
            ratio: usize,
            minimum: usize,
            keep_absent: bool,
            expected: []const bool,
        },
        label_cases: []const struct {
            shape: Shape,
            pooled: bool,
            spans: []const Span,
            valid: []const bool,
            gold: []const Span,
            gold_valid: []const bool,
            gold_capacity: usize,
            expected: []const f32,
            soft: []const f32,
        },
        consistency_cases: []const struct {
            shape: Shape,
            logits: []const f32,
            spans: []const Span,
            valid: []const bool,
            starts: []const f32,
            ends: []const f32,
            boundary_width: usize,
            boundary_keep: []const bool,
            expected: f64,
            pair_gradient: []const f32,
            start_gradient: []const f32,
            end_gradient: []const f32,
        },
        dense_cases: []const struct {
            batch: usize,
            queries: usize,
            capacity: usize,
            text_length: usize,
            gold: []const Span,
            valid: []const bool,
            starts: []const f32,
            ends: []const f32,
            inside: []const f32,
        },
    };
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "training_losses.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(Fixture, a, bytes, .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u32, 1), fixture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", fixture.value.provenance.object.get("upstream_commit").?.string);
    for (fixture.value.loss_cases, 0..) |case, i| {
        const tensor = Tensor{ .shape = case.shape, .values = case.logits };
        const options = BinaryOptions{ .reduction = case.reduction, .query_mask = case.query_mask, .negative_weight = case.negative_weight, .hard_negative_mask = case.hard };
        var result = switch (case.kind) {
            .bce => try balancedBce(a, tensor, case.targets, case.valid, options),
            .pair => try candidatePairBce(a, tensor, case.targets, case.valid, options),
            .focal => try asymmetricFocal(a, tensor, case.targets, case.valid, .{ .binary = options, .gamma_positive = case.gamma_positive, .gamma_negative = case.gamma_negative, .clip = case.clip }),
            .listwise => try listwise(a, tensor, case.gold, case.valid, case.query_mask, .{}),
            .abstention, .poisson_count => try queryLoss(a, case.logits, .{ .batch = case.shape.batch, .queries = case.shape.queries, .capacity = case.mention_capacity, .values = case.mentions }, case.query_mask, if (case.kind == .abstention) .abstention else .poisson_count, .{}),
        };
        defer result.deinit();
        expectNear(case.expected, result.value) catch |err| {
            std.debug.print("loss fixture {d} kind={s} reduction={s} layout={s}\n", .{ i, @tagName(case.kind), @tagName(case.reduction), @tagName(case.shape.layout) });
            return err;
        };
        expectGradient(case.gradient, result.gradient) catch |err| {
            std.debug.print("gradient fixture {d} kind={s} reduction={s} layout={s}\n", .{ i, @tagName(case.kind), @tagName(case.reduction), @tagName(case.shape.layout) });
            return err;
        };
    }
    for (fixture.value.hard_cases) |case| {
        var result = try hardNegatives(a, .{ .shape = case.shape, .values = case.logits }, case.labels, case.valid, .{ .negatives_per_positive = case.ratio, .minimum_negatives = case.minimum, .keep_all_when_no_positive = case.keep_absent });
        defer result.deinit();
        try std.testing.expectEqualSlices(bool, case.expected, result.values);
    }
    for (fixture.value.label_cases) |case| {
        var result = try candidateLabels(a, .{ .shape = case.shape, .pooled = case.pooled, .spans = case.spans, .valid = case.valid }, .{ .batch = case.shape.batch, .queries = case.shape.queries, .capacity = case.gold_capacity, .spans = case.gold, .valid = case.gold_valid }, true, .{});
        defer result.deinit();
        try expectGradient(case.expected, result.values);
        try expectGradient(case.soft, result.soft_iou.?);
    }
    for (fixture.value.consistency_cases) |case| {
        var result = try marginalConsistency(a, .{ .shape = case.shape, .values = case.logits }, case.spans, case.valid, .{ .shape = .{ .batch = case.shape.batch, .queries = case.shape.queries, .candidates = case.boundary_width }, .values = case.starts }, case.ends, case.boundary_keep, .{});
        defer result.deinit();
        try expectNear(case.expected, result.value);
        try expectGradient(case.pair_gradient, result.pair_gradient);
        try expectGradient(case.start_gradient, result.start_gradient);
        try expectGradient(case.end_gradient, result.end_gradient);
    }
    for (fixture.value.dense_cases) |case| {
        var result = try denseTargets(a, .{ .batch = case.batch, .queries = case.queries, .capacity = case.capacity, .spans = case.gold, .valid = case.valid }, case.text_length, .{});
        defer result.deinit();
        try expectGradient(case.starts, result.starts);
        try expectGradient(case.ends, result.ends);
        try expectGradient(case.inside, result.inside);
    }
}

test "boundary training rejects invalid shapes targets and exhausted budgets before allocation" {
    const a = std.testing.allocator;
    const tensor = Tensor{ .shape = .{ .batch = 1, .queries = 1, .candidates = 2 }, .values = &.{ 0, 1 } };
    try std.testing.expectError(error.InvalidBoundaryTrainingShape, balancedBce(a, tensor, &.{0}, &.{ true, true }, .{}));
    try std.testing.expectError(error.InvalidBoundaryTrainingTargets, balancedBce(a, tensor, &.{ 2, 0 }, &.{ true, true }, .{}));
    try std.testing.expectError(error.InvalidBoundaryTrainingOptions, balancedBce(a, tensor, &.{ 1, 0 }, &.{ true, true }, .{ .negative_weight = -1 }));
    try std.testing.expectError(error.InvalidBoundaryTrainingOptions, candidatePairBce(a, tensor, &.{ 1, 0 }, &.{ true, true }, .{ .negative_weight = 0.5 }));
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, hardNegatives(a, tensor, &.{ 1, 0 }, &.{ true, true }, .{ .limits = .{ .max_work = 1 } }));
    const bad_gold = Gold{ .batch = 1, .queries = 1, .capacity = 1, .spans = &.{.{ .start = 1, .end = 1 }}, .valid = &.{true} };
    try std.testing.expectError(error.InvalidBoundaryTrainingTargets, denseTargets(a, bad_gold, 3, .{}));
    const beyond = Gold{ .batch = 1, .queries = 1, .capacity = 1, .spans = &.{.{ .start = 1, .end = 4 }}, .valid = &.{true} };
    try std.testing.expectError(error.InvalidBoundaryTrainingTargets, denseTargets(a, beyond, 3, .{}));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, balancedBce(failing.allocator(), tensor, &.{ 1, 0 }, &.{ true, true }, .{ .limits = .{ .max_work = 0 } }));
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, balancedBce(failing.allocator(), tensor, &.{ 1, 0 }, &.{ true, true }, .{ .limits = .{ .max_elements = 1 } }));
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, balancedBce(failing.allocator(), .{ .shape = .{ .batch = 1, .queries = 10, .candidates = 0 }, .values = &.{} }, &.{}, &.{}, .{ .limits = .{ .max_elements = 1 } }));
    try std.testing.expect(!failing.has_induced_failure);
}

test "boundary training masked nonfinite padding is ignored and active nonfinite loss fails closed" {
    const a = std.testing.allocator;
    const shape = Shape{ .batch = 1, .queries = 1, .candidates = 2 };
    const tensor = Tensor{ .shape = shape, .values = &.{ std.math.nan(f32), 1 } };
    var result = try balancedBce(a, tensor, &.{ std.math.nan(f32), 1 }, &.{ false, true }, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(f32, 0), result.gradient[0]);
    try expectNear(bce(1, 1), result.value);
    try std.testing.expectError(error.NonFiniteBoundaryTraining, balancedBce(a, tensor, &.{ 0, 1 }, &.{ true, true }, .{}));
    try std.testing.expectError(error.NonFiniteBoundaryTraining, queryLoss(a, &.{1000}, .{ .batch = 1, .queries = 1, .capacity = 0, .values = &.{} }, &.{true}, .poisson_count, .{}));
    const extreme = Tensor{ .shape = shape, .values = &.{ -10000, 10000 } };
    var stable = try balancedBce(a, extreme, &.{ 1, 0 }, &.{ true, true }, .{});
    defer stable.deinit();
    try std.testing.expectEqual(@as(f64, 10000), stable.value);
    try std.testing.expectEqualSlices(f32, &.{ -0.5, 0.5 }, stable.gradient);
}

test "boundary training cancellation interrupts selection matching and loss accumulation" {
    const Cancel = struct {
        fn check(raw: ?*anyopaque) !void {
            const steps: *usize = @ptrCast(@alignCast(raw.?));
            steps.* += 1;
            if (steps.* >= 3) return error.Cancelled;
        }
    };
    const a = std.testing.allocator;
    var checks: usize = 0;
    const limits = Limits{ .control = .{ .ptr = &checks, .check_fn = Cancel.check } };
    const values = [_]f32{0} ** 256;
    const valid = [_]bool{true} ** 256;
    const tensor = Tensor{ .shape = .{ .batch = 1, .queries = 1, .candidates = 256 }, .values = &values };
    try std.testing.expectError(error.Cancelled, balancedBce(a, tensor, &values, &valid, .{ .limits = limits }));
    checks = 0;
    try std.testing.expectError(error.Cancelled, hardNegatives(a, tensor, &values, &valid, .{ .limits = limits }));
    checks = 0;
    const spans = [_]Span{.{ .start = 0, .end = 1 }} ** 256;
    const gold = Gold{ .batch = 1, .queries = 1, .capacity = 256, .spans = &spans, .valid = &valid };
    try std.testing.expectError(error.Cancelled, candidateLabels(a, .{ .shape = tensor.shape, .spans = &spans, .valid = &valid }, gold, true, limits));
}

fn allocationLifecycle(a: Allocator) !void {
    const shape = Shape{ .batch = 1, .queries = 1, .candidates = 2 };
    const tensor = Tensor{ .shape = shape, .values = &.{ 0, 1 } };
    const targets: []const f32 = &.{ 1, 0 };
    const valid: []const bool = &.{ true, true };
    const spans: []const Span = &.{ .{ .start = 0, .end = 2 }, .{ .start = 1, .end = 2 } };
    const gold = Gold{ .batch = 1, .queries = 1, .capacity = 2, .spans = spans, .valid = valid };
    var binary_result = try balancedBce(a, tensor, targets, valid, .{});
    defer binary_result.deinit();
    var focal_result = try asymmetricFocal(a, tensor, targets, valid, .{});
    defer focal_result.deinit();
    var ranked = try listwise(a, tensor, &.{ true, false }, valid, &.{true}, .{});
    defer ranked.deinit();
    var selected = try hardNegatives(a, tensor, targets, valid, .{});
    defer selected.deinit();
    var labels = try candidateLabels(a, .{ .shape = shape, .spans = spans, .valid = valid }, gold, true, .{});
    defer labels.deinit();
    var dense = try denseTargets(a, gold, 2, .{});
    defer dense.deinit();
    var inside = try insideConsistency(a, tensor, targets, valid, &.{true}, .{});
    defer inside.deinit();
    var count = try queryLoss(a, &.{0}, .{ .batch = 1, .queries = 1, .capacity = 2, .values = valid }, &.{true}, .poisson_count, .{});
    defer count.deinit();
    var consistency = try marginalConsistency(a, tensor, spans, valid, .{ .shape = .{ .batch = 1, .queries = 1, .candidates = 3 }, .values = &.{ 0, 0, 0 } }, &.{ 0, 0, 0 }, &.{ true, true, true }, .{});
    defer consistency.deinit();
}
test "boundary training all kernels release allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{});
}
