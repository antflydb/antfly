// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Detached GLiNER2.5 proposal identities and gold-inclusive candidate pools.
//! Neural logits must be recomputed for the retained spans through the live
//! differentiable graph. This module never returns detached training logits.
const std = @import("std");
const Allocator = std.mem.Allocator;
const loss = @import("gliner_boundary_losses.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Span = loss.Span;
pub const Gold = loss.Gold;
pub const Phase = enum { training, evaluation };
pub const Limits = struct {
    max_batch: usize = 128,
    max_queries: usize = 1024,
    max_words: usize = 16384,
    max_proposals: usize = 65536,
    max_capacity: usize = 65536,
    max_gold_per_query: usize = 256,
    max_row_entries: usize = 1024 * 1024,
    max_elements: usize = 16 * 1024 * 1024,
    max_work: usize = 100 * 1024 * 1024,
    control: ?Control = null,
};
pub const Options = struct {
    phase: Phase,
    capacity: usize,
    gold_injection_probability: f32 = 1,
    /// Exact caller-owned [B,Q,G] uniform draws in [0,1), consumed only for
    /// training with 0 < probability < 1. RNG identity/counters belong to the
    /// training run and checkpoint, not an allocation-dependent global stream.
    injection_draws: ?[]const f32 = null,
    limits: Limits = .{},
};
pub const QueryInput = struct {
    batch: usize,
    queries: usize,
    proposals: usize,
    /// Detached proposal tensors use [B,Q,M] order.
    spans: []const Span,
    scores: []const f32,
    valid: []const bool,
    query_mask: []const bool,
    word_counts: []const usize,
};
pub const DocumentInput = struct {
    batch: usize,
    queries: usize,
    proposals: usize,
    /// One global Cartesian pair set [B,M], before quota/global selection.
    spans: []const Span,
    global_scores: []const f32,
    valid: []const bool,
    /// Query-conditioned start+end+compat scores [B,Q,M]. The caller computes
    /// these from the same detached boundary projections as global_scores.
    query_scores: []const f32,
    query_mask: []const bool,
    word_counts: []const usize,
    min_pool_per_query: usize,
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    batch: usize,
    queries: usize,
    capacity: usize,
    pooled: bool,
    /// Per-query [B,Q,C] or pooled [B,C]. Padding is exactly [0,0), false.
    spans: []const Span,
    valid: []const bool,
    /// Whether the selected highest-priority occurrence was an injected gold
    /// entry. This differs from exact gold membership for a predicted span.
    injected: []const bool,
    /// Exact gold membership: [B,Q,C] or pooled [B,C,Q], including gold not
    /// selected for injection. Padded queries and candidates remain false.
    gold_labels: []const bool,
    gold_total: usize,
    /// Pinned recall boundary: raw query proposals, or the bounded document
    /// pool before injection. Counts annotation entries, including duplicates.
    gold_hits_before_injection: usize,
    work: usize,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
const floor: f32 = -10000;
const gold_priority: f32 = 10000;
const Entry = struct {
    span: Span = .{ .start = 0, .end = 0 },
    priority: f32 = floor,
    key: usize,
    ordinal: usize,
    valid: bool = false,
    injected: bool = false,
    keep: bool = false,
    rank: usize = 0,
};
const Sort = enum { group, retained, quota };
const Work = struct {
    options: Options,
    count: usize = 0,
    fn tick(self: *Work) !void {
        if (self.count >= self.options.limits.max_work) return error.BoundaryTrainingSelectionLimitExceeded;
        self.count += 1;
        if (self.count % 128 == 1) try self.check();
    }
    fn check(self: *const Work) !void {
        if (self.options.limits.control) |control| try control.check();
    }
    fn validate(self: *Work, batch: usize, queries: usize, proposals: usize, word_counts: []const usize, query_mask: []const bool, gold: ?Gold) !usize {
        try self.check();
        const limits = self.options.limits;
        if (!std.math.isFinite(self.options.gold_injection_probability) or self.options.gold_injection_probability < 0 or self.options.gold_injection_probability > 1 or
            self.options.capacity == 0) return error.InvalidBoundaryTrainingSelectionOptions;
        if (batch == 0 or batch != word_counts.len) return error.InvalidBoundaryTrainingSelectionShape;
        if (batch > limits.max_batch or queries > limits.max_queries or proposals > limits.max_proposals or proposals > limits.max_elements or
            self.options.capacity > limits.max_capacity or limits.max_work == 0) return error.BoundaryTrainingSelectionLimitExceeded;
        const rows = try elements(batch, queries, limits.max_elements);
        if (query_mask.len != rows) return error.InvalidBoundaryTrainingSelectionShape;
        var max_words: usize = 0;
        for (word_counts) |words| {
            try self.tick();
            if (words > limits.max_words) return error.BoundaryTrainingSelectionLimitExceeded;
            max_words = @max(max_words, words);
        }
        if (gold) |g| {
            if (g.batch != batch or g.queries != queries) return error.InvalidBoundaryTrainingSelectionShape;
            if (g.capacity > limits.max_gold_per_query) return error.BoundaryTrainingSelectionLimitExceeded;
            const count = try elements(rows, g.capacity, limits.max_elements);
            if (g.spans.len != count or g.valid.len != count) return error.InvalidBoundaryTrainingSelectionShape;
            if (self.options.phase == .training and self.options.gold_injection_probability > 0 and self.options.gold_injection_probability < 1) {
                const draws = self.options.injection_draws orelse return error.MissingBoundaryTrainingInjectionDraws;
                if (draws.len != count) return error.InvalidBoundaryTrainingSelectionShape;
                for (draws) |draw| {
                    try self.tick();
                    if (!std.math.isFinite(draw) or draw < 0 or draw >= 1) return error.InvalidBoundaryTrainingInjectionDraw;
                }
            }
            for (0..batch) |b| for (0..queries) |q| {
                if (!query_mask[b * queries + q]) continue;
                for (0..g.capacity) |slot| {
                    try self.tick();
                    const index = (b * queries + q) * g.capacity + slot;
                    if (g.valid[index]) try validSpan(g.spans[index], word_counts[b]);
                }
            };
        }
        return std.math.add(usize, max_words, 1) catch error.BoundaryTrainingSelectionLimitExceeded;
    }
    fn inject(self: *const Work, index: usize) bool {
        if (self.options.phase != .training or self.options.gold_injection_probability <= 0) return false;
        return self.options.gold_injection_probability >= 1 or self.options.injection_draws.?[index] < self.options.gold_injection_probability;
    }
    fn sort(self: *Work, entries: []Entry, mode: Sort) !void {
        // Explicit bounded heap sort avoids an uninterruptible library sort.
        // Every comparator has a total deterministic order.
        var start = entries.len / 2;
        while (start > 0) {
            start -= 1;
            try self.sift(entries, start, mode);
        }
        var end = entries.len;
        while (end > 1) {
            end -= 1;
            std.mem.swap(Entry, &entries[0], &entries[end]);
            try self.sift(entries[0..end], 0, mode);
        }
    }
    fn sift(self: *Work, entries: []Entry, initial: usize, mode: Sort) !void {
        var root = initial;
        while (root < entries.len / 2) {
            try self.tick();
            var child = root * 2 + 1;
            if (child + 1 < entries.len and less(entries[child], entries[child + 1], mode)) child += 1;
            if (!less(entries[root], entries[child], mode)) break;
            std.mem.swap(Entry, &entries[root], &entries[child]);
            root = child;
        }
    }
    fn select(self: *Work, entries: []Entry, spans: []Span, valid: []bool, injected: []bool) !void {
        try self.sort(entries, .group);
        for (entries, 0..) |*entry, i| {
            try self.tick();
            entry.keep = entry.valid and (i == 0 or entry.key != entries[i - 1].key);
            entry.rank = i;
        }
        try self.sort(entries, .retained);
        @memset(spans, .{ .start = 0, .end = 0 });
        @memset(valid, false);
        @memset(injected, false);
        for (entries[0..@min(entries.len, spans.len)], 0..) |entry, i| {
            try self.tick();
            if (!entry.keep) continue;
            spans[i] = entry.span;
            valid[i] = true;
            injected[i] = entry.injected;
        }
    }
    fn contains(self: *Work, spans: []const Span, valid: []const bool, needle: Span) !bool {
        for (spans, valid) |span, keep| {
            try self.tick();
            if (keep and equal(span, needle)) return true;
        }
        return false;
    }
};
fn less(a: Entry, b: Entry, mode: Sort) bool {
    if (mode == .group) {
        if (a.key != b.key) return a.key < b.key;
        if (a.priority != b.priority) return a.priority > b.priority;
        return a.ordinal < b.ordinal;
    }
    const ascore = if (mode == .retained) if (a.keep) a.priority else floor else a.priority;
    const bscore = if (mode == .retained) if (b.keep) b.priority else floor else b.priority;
    if (ascore != bscore) return ascore > bscore;
    return if (mode == .retained) a.rank < b.rank else a.ordinal < b.ordinal;
}
fn elements(a: usize, b: usize, limit: usize) !usize {
    const count = std.math.mul(usize, a, b) catch return error.BoundaryTrainingSelectionLimitExceeded;
    if (count > limit) return error.BoundaryTrainingSelectionLimitExceeded;
    return count;
}
fn addEntries(a: usize, b: usize, limit: usize) !usize {
    const count = std.math.add(usize, a, b) catch return error.BoundaryTrainingSelectionLimitExceeded;
    if (count > limit) return error.BoundaryTrainingSelectionLimitExceeded;
    return count;
}
fn validSpan(span: Span, words: usize) !void {
    if (span.start < 0 or span.end <= span.start or span.end > words) return error.InvalidBoundaryTrainingCandidate;
}
fn equal(a: Span, b: Span) bool {
    return a.start == b.start and a.end == b.end;
}
fn key(span: Span, boundaries: usize) usize {
    return @as(usize, @intCast(span.start)) * boundaries + @as(usize, @intCast(span.end));
}
fn makeEntry(span: Span, priority: f32, valid: bool, injected: bool, ordinal: usize, boundaries: usize) Entry {
    return .{ .span = if (valid) span else .{ .start = 0, .end = 0 }, .priority = if (valid) priority else floor, .key = if (valid) key(span, boundaries) else boundaries * boundaries, .ordinal = ordinal, .valid = valid, .injected = injected };
}

/// Mirrors pinned proposal.assemble_candidates, with strict source/gold bounds
/// and an explicit error if selected injection gold would be lost to capacity.
pub fn assembleQueryCandidates(a: Allocator, input: QueryInput, gold: ?Gold, options: Options) !Result {
    var work = Work{ .options = options };
    const boundaries = try work.validate(input.batch, input.queries, input.proposals, input.word_counts, input.query_mask, gold);
    _ = try elements(boundaries, boundaries, std.math.maxInt(usize));
    const rows = try elements(input.batch, input.queries, options.limits.max_elements);
    const inputs = try elements(rows, input.proposals, options.limits.max_elements);
    const outputs = try elements(rows, options.capacity, options.limits.max_elements);
    if (input.spans.len != inputs or input.scores.len != inputs or input.valid.len != inputs) return error.InvalidBoundaryTrainingSelectionShape;
    const inject_capacity = if (options.phase == .training and gold != null) gold.?.capacity else 0;
    const scratch_count = try addEntries(input.proposals, inject_capacity, @min(options.limits.max_row_entries, options.limits.max_elements));
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const spans = try owned.alloc(Span, outputs);
    const valid = try owned.alloc(bool, outputs);
    const injected = try owned.alloc(bool, outputs);
    const labels = try owned.alloc(bool, outputs);
    @memset(labels, false);
    const entries = try a.alloc(Entry, scratch_count);
    defer a.free(entries);
    var total: usize = 0;
    var hits: usize = 0;
    for (0..input.batch) |b| for (0..input.queries) |q| {
        const row = b * input.queries + q;
        const active = input.query_mask[row];
        const offset = row * input.proposals;
        for (0..input.proposals) |p| {
            try work.tick();
            const index = offset + p;
            const keep = active and input.valid[index];
            if (keep) {
                try validSpan(input.spans[index], input.word_counts[b]);
                if (!std.math.isFinite(input.scores[index])) return error.InvalidBoundaryTrainingCandidate;
            }
            entries[p] = makeEntry(input.spans[index], input.scores[index], keep, false, p, boundaries);
        }
        if (gold) |g| for (0..g.capacity) |slot| {
            try work.tick();
            const index = row * g.capacity + slot;
            const keep = active and g.valid[index];
            if (keep) {
                total += 1;
                if (try work.contains(input.spans[offset..][0..input.proposals], input.valid[offset..][0..input.proposals], g.spans[index])) hits += 1;
            }
            if (inject_capacity > 0) entries[input.proposals + slot] = makeEntry(g.spans[index], gold_priority, keep and work.inject(index), true, input.proposals + slot, boundaries);
        };
        const start = row * options.capacity;
        const row_spans = spans[start..][0..options.capacity];
        const row_valid = valid[start..][0..options.capacity];
        try work.select(entries, row_spans, row_valid, injected[start..][0..options.capacity]);
        if (gold) |g| for (0..g.capacity) |slot| {
            const index = row * g.capacity + slot;
            if (!active or !g.valid[index]) continue;
            var retained = false;
            for (row_spans, row_valid, 0..) |span, keep, c| {
                try work.tick();
                if (keep and equal(span, g.spans[index])) {
                    labels[start + c] = true;
                    retained = true;
                }
            }
            if (work.inject(index) and !retained) return error.BoundaryTrainingCandidateCapacityExceeded;
        };
    };
    try work.check();
    return .{ .arena = arena, .batch = input.batch, .queries = input.queries, .capacity = options.capacity, .pooled = false, .spans = spans, .valid = valid, .injected = injected, .gold_labels = labels, .gold_total = total, .gold_hits_before_injection = hits, .work = work.count };
}

/// Mirrors DocumentCandidatePool's quota/global/gold selection. The caller
/// provides its detached Cartesian proposals and compatibility-aware scores;
/// retained selected-score recomputation stays in the differentiable graph.
pub fn assembleDocumentPool(a: Allocator, input: DocumentInput, gold: ?Gold, options: Options) !Result {
    var work = Work{ .options = options };
    const boundaries = try work.validate(input.batch, input.queries, input.proposals, input.word_counts, input.query_mask, gold);
    _ = try elements(boundaries, boundaries, std.math.maxInt(usize));
    const inputs = try elements(input.batch, input.proposals, options.limits.max_elements);
    const query_inputs = try elements(inputs, input.queries, options.limits.max_elements);
    if (input.spans.len != inputs or input.global_scores.len != inputs or input.valid.len != inputs or input.query_scores.len != query_inputs)
        return error.InvalidBoundaryTrainingSelectionShape;
    const outputs = try elements(input.batch, options.capacity, options.limits.max_elements);
    const labels_count = try elements(outputs, input.queries, options.limits.max_elements);
    const quota = @min(input.min_pool_per_query, input.proposals);
    const quota_count = try elements(input.queries, quota, options.limits.max_row_entries);
    const base_count = try addEntries(input.proposals, quota_count, options.limits.max_row_entries);
    const inject_count = if (options.phase == .training and gold != null) try elements(input.queries, gold.?.capacity, options.limits.max_row_entries) else 0;
    const entry_count = try addEntries(base_count, inject_count, @min(options.limits.max_row_entries, options.limits.max_elements));
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const spans = try owned.alloc(Span, outputs);
    const valid = try owned.alloc(bool, outputs);
    const injected = try owned.alloc(bool, outputs);
    const labels = try owned.alloc(bool, labels_count);
    @memset(labels, false);
    const entries = try a.alloc(Entry, entry_count);
    defer a.free(entries);
    const ranked = try a.alloc(Entry, input.proposals);
    defer a.free(ranked);
    var total: usize = 0;
    var hits: usize = 0;
    for (0..input.batch) |b| {
        const input_start = b * input.proposals;
        const active_queries = input.query_mask[b * input.queries ..][0..input.queries];
        const active = std.mem.indexOfScalar(bool, active_queries, true) != null;
        for (0..input.proposals) |p| {
            try work.tick();
            const index = input_start + p;
            const keep = active and input.valid[index];
            if (keep) {
                try validSpan(input.spans[index], input.word_counts[b]);
                if (!std.math.isFinite(input.global_scores[index])) return error.InvalidBoundaryTrainingCandidate;
            }
            entries[quota_count + p] = makeEntry(input.spans[index], input.global_scores[index], keep, false, quota_count + p, boundaries);
        }
        for (0..input.queries) |q| {
            for (0..input.proposals) |p| {
                try work.tick();
                const index = input_start + p;
                const keep = active_queries[q] and input.valid[index];
                const score = input.query_scores[(b * input.queries + q) * input.proposals + p];
                if (keep and !std.math.isFinite(score)) return error.InvalidBoundaryTrainingCandidate;
                ranked[p] = makeEntry(input.spans[index], score, keep, false, p, boundaries);
            }
            if (quota > 0) {
                try work.sort(ranked, .quota);
                for (ranked[0..quota], 0..) |entry, rank| {
                    try work.tick();
                    const index = q * quota + rank;
                    const priority: f32 = 5000 + @as(f32, @floatFromInt(quota - rank));
                    entries[index] = makeEntry(entry.span, priority, entry.valid, false, index, boundaries);
                }
            }
        }
        const output_start = b * options.capacity;
        const row_spans = spans[output_start..][0..options.capacity];
        const row_valid = valid[output_start..][0..options.capacity];
        const row_injected = injected[output_start..][0..options.capacity];
        // The diagnostic baseline is bounded by the actual pool capacity.
        try work.select(entries[0..base_count], row_spans, row_valid, row_injected);
        if (gold) |g| for (0..input.queries) |q| for (0..g.capacity) |slot| {
            try work.tick();
            const index = (b * input.queries + q) * g.capacity + slot;
            const keep = active_queries[q] and g.valid[index];
            if (keep) {
                total += 1;
                if (try work.contains(row_spans, row_valid, g.spans[index])) hits += 1;
            }
            if (inject_count > 0) {
                const entry = base_count + q * g.capacity + slot;
                entries[entry] = makeEntry(g.spans[index], gold_priority, keep and work.inject(index), true, entry, boundaries);
            }
        };
        if (inject_count > 0) try work.select(entries, row_spans, row_valid, row_injected);
        if (gold) |g| for (0..input.queries) |q| for (0..g.capacity) |slot| {
            const index = (b * input.queries + q) * g.capacity + slot;
            if (!active_queries[q] or !g.valid[index]) continue;
            var retained = false;
            for (row_spans, row_valid, 0..) |span, keep, c| {
                try work.tick();
                if (keep and equal(span, g.spans[index])) {
                    labels[(b * options.capacity + c) * input.queries + q] = true;
                    retained = true;
                }
            }
            if (work.inject(index) and !retained) return error.BoundaryTrainingCandidateCapacityExceeded;
        };
    }
    try work.check();
    return .{ .arena = arena, .batch = input.batch, .queries = input.queries, .capacity = options.capacity, .pooled = true, .spans = spans, .valid = valid, .injected = injected, .gold_labels = labels, .gold_total = total, .gold_hits_before_injection = hits, .work = work.count };
}

const test_spans = [_]Span{ .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 }, .{ .start = 0, .end = 1 }, .{ .start = -1, .end = -1 } };
const test_input = QueryInput{ .batch = 1, .queries = 1, .proposals = 4, .spans = &test_spans, .scores = &.{ 1, 2, 3, 0 }, .valid = &.{ true, true, true, false }, .query_mask = &.{true}, .word_counts = &.{3} };
const test_gold = Gold{ .batch = 1, .queries = 1, .capacity = 2, .spans = &.{ .{ .start = 2, .end = 3 }, .{ .start = 0, .end = 1 } }, .valid = &.{ true, true } };

test "boundary training selection retains injected gold and deterministic predicted identities" {
    const a = std.testing.allocator;
    var result = try assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 3 });
    defer result.deinit();
    try std.testing.expectEqualDeep(@as([]const Span, &.{ .{ .start = 0, .end = 1 }, .{ .start = 2, .end = 3 }, .{ .start = 1, .end = 2 } }), result.spans);
    try std.testing.expectEqualSlices(bool, &.{ true, true, true }, result.valid);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false }, result.injected);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false }, result.gold_labels);
    try std.testing.expectEqual(@as(usize, 2), result.gold_total);
    try std.testing.expectEqual(@as(usize, 1), result.gold_hits_before_injection);
    try std.testing.expectError(error.BoundaryTrainingCandidateCapacityExceeded, assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 1 }));
    var sampled = try assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 3, .gold_injection_probability = 0.5, .injection_draws = &.{ 0.2, 0.7 } });
    defer sampled.deinit();
    try std.testing.expectEqualDeep(@as([]const Span, &.{ .{ .start = 2, .end = 3 }, .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 } }), sampled.spans);
    try std.testing.expectEqualSlices(bool, &.{ true, false, false }, sampled.injected);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false }, sampled.gold_labels);
}

test "boundary training selection evaluation cannot consume gold injection or draws" {
    const a = std.testing.allocator;
    var baseline = try assembleQueryCandidates(a, test_input, null, .{ .phase = .evaluation, .capacity = 2 });
    defer baseline.deinit();
    var supervised = try assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .evaluation, .capacity = 2, .gold_injection_probability = 0.5, .injection_draws = &.{std.math.nan(f32)} });
    defer supervised.deinit();
    try std.testing.expectEqualDeep(baseline.spans, supervised.spans);
    try std.testing.expectEqualSlices(bool, baseline.valid, supervised.valid);
    try std.testing.expectEqualSlices(bool, &.{ false, false }, supervised.injected);
    try std.testing.expectEqualSlices(bool, &.{ true, false }, supervised.gold_labels);
    try std.testing.expectError(error.MissingBoundaryTrainingInjectionDraws, assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 2, .gold_injection_probability = 0.5 }));
    try std.testing.expectError(error.InvalidBoundaryTrainingInjectionDraw, assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 2, .gold_injection_probability = 0.5, .injection_draws = &.{ 0, 1 } }));
}

const test_document = DocumentInput{ .batch = 1, .queries = 2, .proposals = 4, .spans = &test_spans, .global_scores = &.{ 1, 2, 3, 0 }, .valid = &.{ true, true, true, false }, .query_scores = &.{ 9, 1, 9, 0, 1, 9, 1, 0 }, .query_mask = &.{ true, true }, .word_counts = &.{3}, .min_pool_per_query = 1 };
const test_document_gold = Gold{ .batch = 1, .queries = 2, .capacity = 1, .spans = &.{ .{ .start = 2, .end = 3 }, .{ .start = 0, .end = 1 } }, .valid = &.{ true, true } };
test "boundary training selection document quotas share candidates and preserve cross-query gold" {
    const a = std.testing.allocator;
    var result = try assembleDocumentPool(a, test_document, test_document_gold, .{ .phase = .training, .capacity = 3 });
    defer result.deinit();
    try std.testing.expect(result.pooled);
    try std.testing.expectEqualDeep(@as([]const Span, &.{ .{ .start = 0, .end = 1 }, .{ .start = 2, .end = 3 }, .{ .start = 1, .end = 2 } }), result.spans);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, false, false, false }, result.gold_labels);
    try std.testing.expectEqual(@as(usize, 1), result.gold_hits_before_injection);
    var without = try assembleDocumentPool(a, test_document, null, .{ .phase = .evaluation, .capacity = 2 });
    defer without.deinit();
    var evaluation = try assembleDocumentPool(a, test_document, test_document_gold, .{ .phase = .evaluation, .capacity = 2 });
    defer evaluation.deinit();
    try std.testing.expectEqualDeep(without.spans, evaluation.spans);
    try std.testing.expectEqualSlices(bool, &.{ true, true }, evaluation.valid);
    try std.testing.expectError(error.BoundaryTrainingCandidateCapacityExceeded, assembleDocumentPool(a, test_document, test_document_gold, .{ .phase = .training, .capacity = 1 }));
}

fn allocationLifecycle(a: Allocator) !void {
    var query = try assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 3 });
    defer query.deinit();
    var document_ = try assembleDocumentPool(a, test_document, test_document_gold, .{ .phase = .training, .capacity = 3 });
    defer document_.deinit();
}
test "boundary training selection releases allocations on all failure points" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{});
}

test "boundary training selection validates limits masks cancellation and empty shapes" {
    const a = std.testing.allocator;
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.Cancelled, assembleQueryCandidates(failing.allocator(), test_input, test_gold, .{ .phase = .training, .capacity = 3, .limits = .{ .control = .{ .check_fn = Cancel.check } } }));
    try std.testing.expectError(error.BoundaryTrainingSelectionLimitExceeded, assembleDocumentPool(failing.allocator(), test_document, test_document_gold, .{ .phase = .training, .capacity = 3, .limits = .{ .max_work = 0 } }));
    try std.testing.expectError(error.BoundaryTrainingSelectionLimitExceeded, assembleQueryCandidates(failing.allocator(), test_input, test_gold, .{ .phase = .training, .capacity = 3, .limits = .{ .max_elements = 3 } }));
    try std.testing.expect(!failing.has_induced_failure);
    var bad = test_input;
    bad.scores = &.{ 1, std.math.nan(f32), 3, 0 };
    try std.testing.expectError(error.InvalidBoundaryTrainingCandidate, assembleQueryCandidates(a, bad, null, .{ .phase = .evaluation, .capacity = 2 }));
    bad.query_mask = &.{false};
    var masked = try assembleQueryCandidates(a, bad, test_gold, .{ .phase = .training, .capacity = 2 });
    defer masked.deinit();
    try std.testing.expectEqualSlices(bool, &.{ false, false }, masked.valid);
    try std.testing.expectEqual(@as(usize, 0), masked.gold_total);
    try std.testing.expectError(error.BoundaryTrainingSelectionLimitExceeded, assembleQueryCandidates(a, test_input, test_gold, .{ .phase = .training, .capacity = 3, .limits = .{ .max_work = 4 } }));
    var empty = try assembleQueryCandidates(a, .{ .batch = 1, .queries = 0, .proposals = 0, .spans = &.{}, .scores = &.{}, .valid = &.{}, .query_mask = &.{}, .word_counts = &.{0} }, null, .{ .phase = .evaluation, .capacity = 1 });
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.spans.len);
}

test "boundary training selection matches pinned query and document pool oracle" {
    const Fixture = struct {
        format_version: u32,
        provenance: struct { upstream_commit: []const u8 },
        cases: []const struct {
            id: []const u8,
            query: ?QueryInput = null,
            document: ?DocumentInput = null,
            gold: Gold,
            options: struct { phase: Phase, capacity: usize, gold_injection_probability: f32, injection_draws: ?[]const f32 },
            native_capacity_error: bool,
            expected: struct {
                spans: []const Span,
                valid: []const bool,
                injected: ?[]const bool = null,
                gold_labels: []const bool,
                gold_total: usize,
                gold_hits_before_injection: usize,
            },
        },
    };
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "training_selection.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u32, 1), fixture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", fixture.value.provenance.upstream_commit);
    for (fixture.value.cases) |case| {
        errdefer std.debug.print("training selection fixture: {s}\n", .{case.id});
        const options = Options{ .phase = case.options.phase, .capacity = case.options.capacity, .gold_injection_probability = case.options.gold_injection_probability, .injection_draws = case.options.injection_draws };
        const result = if (case.query) |input| assembleQueryCandidates(a, input, case.gold, options) else assembleDocumentPool(a, case.document.?, case.gold, options);
        if (case.native_capacity_error) {
            try std.testing.expectError(error.BoundaryTrainingCandidateCapacityExceeded, result);
            continue;
        }
        var selected = try result;
        defer selected.deinit();
        try std.testing.expectEqualDeep(case.expected.spans, selected.spans);
        try std.testing.expectEqualSlices(bool, case.expected.valid, selected.valid);
        if (case.expected.injected) |flags| try std.testing.expectEqualSlices(bool, flags, selected.injected);
        try std.testing.expectEqualSlices(bool, case.expected.gold_labels, selected.gold_labels);
        try std.testing.expectEqual(case.expected.gold_total, selected.gold_total);
        try std.testing.expectEqual(case.expected.gold_hits_before_injection, selected.gold_hits_before_injection);
    }
}
