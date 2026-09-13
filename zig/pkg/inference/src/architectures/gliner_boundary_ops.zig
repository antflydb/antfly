// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded CPU primitives for GLiNER boundary extraction. Layouts are explicit:
//! marginals [B,Q,N], projected boundaries [B,N,D], shared candidates [B,C].
//! The ordering and finite mask contract follow Fastino GLiNER2 commit
//! 3c913c7369301133d3b7699252074c4303ada50e. These operations perform no encoding
//! and own no model weights; callers must admit their inputs and workspace.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const mask_logit: f32 = -10000.0;
pub const Span = struct { start: usize, end: usize };

pub const ContentShape = struct {
    batch: usize,
    words: usize,
    dim: usize,
    lengths: []const usize,
    control: ?Control = null,

    fn validate(self: ContentShape) !void {
        if (self.batch == 0 or self.dim == 0 or self.lengths.len != self.batch) return error.InvalidInputShape;
        for (self.lengths) |length| if (length > self.words) return error.InvalidInputShape;
        if (self.control) |control| try control.check();
    }

    pub fn prefixElements(self: ContentShape) !usize {
        return mul(try mul(self.batch, try std.math.add(usize, self.words, 1)), self.dim);
    }
};

/// FP32 prefix over projected token content, [B,W,D] -> [B,W+1,D].
/// Padded input values are never read. Each document has an independent zero.
pub fn tokenPrefix(shape: ContentShape, values: []const f32, prefix: []f32) !void {
    try shape.validate();
    if (values.len != try mul(try mul(shape.batch, shape.words), shape.dim) or prefix.len != try shape.prefixElements()) return error.InvalidInputShape;
    for (0..shape.batch) |b| {
        const output = prefix[b * (shape.words + 1) * shape.dim ..][0 .. (shape.words + 1) * shape.dim];
        @memset(output[0..shape.dim], 0);
        for (0..shape.words) |w| {
            if (shape.control) |control| try control.check();
            for (0..shape.dim) |d| output[(w + 1) * shape.dim + d] = try finite(output[w * shape.dim + d] +
                (if (w < shape.lengths[b]) try finite(values[(b * shape.words + w) * shape.dim + d]) else 0));
        }
    }
}

/// Shared span content means from a prefix, without a [B,C,W,D] tensor.
pub fn rangeMean(shape: ContentShape, prefix: []const f32, capacity: usize, spans: []const Span, valid: []const bool, output: []f32) !void {
    try shape.validate();
    const count = try mul(shape.batch, capacity);
    if (prefix.len != try shape.prefixElements() or spans.len != count or valid.len != count or output.len != try mul(count, shape.dim)) return error.InvalidInputShape;
    @memset(output, 0);
    for (0..shape.batch) |b| {
        if (shape.control) |control| try control.check();
        for (0..capacity) |c| {
            const i = b * capacity + c;
            if (!valid[i]) continue;
            const span = spans[i];
            if (span.end <= span.start or span.end > shape.lengths[b]) return error.InvalidBoundarySpan;
            const length: f32 = @floatFromInt(span.end - span.start);
            for (0..shape.dim) |d| output[i * shape.dim + d] = try finite((prefix[(b * (shape.words + 1) + span.end) * shape.dim + d] -
                prefix[(b * (shape.words + 1) + span.start) * shape.dim + d]) / length);
        }
    }
}

/// Reverse of tokenPrefix + rangeMean. Interval difference updates make work
/// O(B*(C+W)*D), even when many long spans overlap. Scratch has prefix shape.
pub fn rangeMeanBackward(shape: ContentShape, capacity: usize, spans: []const Span, valid: []const bool, grad_means: []const f32, grad_values: []f32, scratch: []f32) !void {
    try shape.validate();
    const count = try mul(shape.batch, capacity);
    if (spans.len != count or valid.len != count or grad_means.len != try mul(count, shape.dim) or
        grad_values.len != try mul(try mul(shape.batch, shape.words), shape.dim) or scratch.len != try shape.prefixElements()) return error.InvalidInputShape;
    @memset(scratch, 0);
    @memset(grad_values, 0);
    for (0..shape.batch) |b| {
        if (shape.control) |control| try control.check();
        for (0..capacity) |c| {
            const i = b * capacity + c;
            if (!valid[i]) continue;
            const span = spans[i];
            if (span.end <= span.start or span.end > shape.lengths[b]) return error.InvalidBoundarySpan;
            const length: f32 = @floatFromInt(span.end - span.start);
            for (0..shape.dim) |d| {
                const grad = try finite(grad_means[i * shape.dim + d] / length);
                scratch[(b * (shape.words + 1) + span.start) * shape.dim + d] += grad;
                scratch[(b * (shape.words + 1) + span.end) * shape.dim + d] -= grad;
            }
        }
        for (0..shape.lengths[b]) |w| {
            if (shape.control) |control| try control.check();
            for (0..shape.dim) |d| {
                const value = try finite(scratch[(b * (shape.words + 1) + w) * shape.dim + d] +
                    (if (w == 0) @as(f32, 0) else grad_values[(b * shape.words + w - 1) * shape.dim + d]));
                grad_values[(b * shape.words + w) * shape.dim + d] = value;
            }
        }
    }
}

pub const PoolConfig = struct {
    boundary_top_k: usize = 32,
    capacity: usize = 192,
    min_per_query: usize = 8,
    max_pair_elements: usize = 1024 * 1024,
};

test "gliner boundary content interval means and reverse preserve ragged masks" {
    const shape = ContentShape{ .batch = 2, .words = 3, .dim = 2, .lengths = &.{ 3, 1 } };
    const nan = std.math.nan(f32);
    const values = [_]f32{ 1, 2, 3, 4, 5, 6, 10, 20, nan, nan, nan, nan };
    const spans = [_]Span{ .{ .start = 0, .end = 2 }, .{ .start = 1, .end = 3 }, .{ .start = 0, .end = 0 }, .{ .start = 0, .end = 1 }, .{ .start = 9, .end = 99 }, .{ .start = 0, .end = 0 } };
    const valid = [_]bool{ true, true, false, true, false, false };
    var prefix: [16]f32 = undefined;
    var means: [12]f32 = undefined;
    try tokenPrefix(shape, &values, &prefix);
    try rangeMean(shape, &prefix, 3, &spans, &valid, &means);
    try std.testing.expectEqualSlices(f32, &.{ 2, 3, 4, 5, 0, 0, 10, 20, 0, 0, 0, 0 }, &means);
    var gradients: [12]f32 = undefined;
    var scratch: [16]f32 = undefined;
    const grad_means = [_]f32{ 2, 4, 6, 8, nan, nan, 1, 3, nan, nan, nan, nan };
    try rangeMeanBackward(shape, 3, &spans, &valid, &grad_means, &gradients, &scratch);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 4, 6, 3, 4, 1, 3, 0, 0, 0, 0 }, &gradients);
}

pub const PoolInput = struct {
    batch: usize,
    boundaries: usize,
    queries: usize,
    dim: usize,
    lengths: []const usize,
    query_mask: []const bool,
    start_logits: []const f32,
    end_logits: []const f32,
    projected_starts: []const f32,
    projected_ends: []const f32,
    control: ?Control = null,
};

pub const SharedPool = struct {
    allocator: std.mem.Allocator,
    batch: usize,
    capacity: usize,
    indices: []Span,
    valid: []bool,
    proposal_logits: []f32,
    compat_logits: []f32,

    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.indices);
        self.allocator.free(self.valid);
        self.allocator.free(self.proposal_logits);
        self.allocator.free(self.compat_logits);
        self.* = undefined;
    }
};

const Ranked = struct {
    score: f32,
    index: usize,
    valid: bool,

    fn less(_: void, a: @This(), b: @This()) bool {
        return if (a.score != b.score) a.score > b.score else a.index < b.index;
    }
};

const Entry = struct {
    key: usize,
    score: f32,
    valid: bool,
    order: usize,

    fn byKey(_: void, a: @This(), b: @This()) bool {
        if (a.key != b.key) return a.key < b.key;
        if (a.score != b.score) return a.score > b.score;
        return a.order < b.order;
    }

    fn byScore(_: void, a: @This(), b: @This()) bool {
        return if (a.score != b.score) a.score > b.score else a.order < b.order;
    }
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}

fn finite(value: f32) !f32 {
    if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
    return value;
}

fn compatibility(input: PoolInput, row: usize, start: usize, end: usize) !f32 {
    const s = input.projected_starts[(row * input.boundaries + start) * input.dim ..][0..input.dim];
    const e = input.projected_ends[(row * input.boundaries + end) * input.dim ..][0..input.dim];
    var value: f32 = 0;
    for (s, e) |a, b| value += a * b;
    return finite(value / @sqrt(@as(f32, @floatFromInt(input.dim))));
}

/// Shared document pool, including quota priority, stable ties, duplicate
/// suppression and padded invalid slots. Selection is nondifferentiable; the
/// returned compatibility/proposal scores are recomputed at selected indices.
pub fn buildSharedPool(allocator: std.mem.Allocator, input: PoolInput, config: PoolConfig) !SharedPool {
    if (input.control) |control| try control.check();
    if (input.batch == 0 or input.boundaries == 0 or input.queries == 0 or input.dim == 0 or
        config.boundary_top_k == 0 or config.capacity == 0) return error.InvalidInputShape;
    const bq = try mul(input.batch, input.queries);
    const logits_len = try mul(bq, input.boundaries);
    const state_len = try mul(try mul(input.batch, input.boundaries), input.dim);
    if (input.lengths.len != input.batch or input.query_mask.len != bq or
        input.start_logits.len != logits_len or input.end_logits.len != logits_len or
        input.projected_starts.len != state_len or input.projected_ends.len != state_len)
        return error.InvalidInputShape;
    for (input.lengths) |length| if (length >= input.boundaries) return error.InvalidInputShape;
    const k = @min(input.boundaries, config.boundary_top_k);
    const pairs = try mul(k, k);
    const quota = @min(config.min_per_query, pairs);
    const entries_len = try std.math.add(usize, try mul(input.queries, quota), pairs);
    if (pairs > config.max_pair_elements or entries_len > config.max_pair_elements)
        return error.ResourceLimitExceeded;
    const invalid_key = try mul(input.boundaries, input.boundaries);
    const output_len = try mul(input.batch, config.capacity);
    const indices = try allocator.alloc(Span, output_len);
    errdefer allocator.free(indices);
    const valid = try allocator.alloc(bool, output_len);
    errdefer allocator.free(valid);
    const proposals = try allocator.alloc(f32, output_len);
    errdefer allocator.free(proposals);
    const compats = try allocator.alloc(f32, output_len);
    errdefer allocator.free(compats);
    @memset(indices, .{ .start = 0, .end = 0 });
    @memset(valid, false);
    @memset(proposals, mask_logit);
    @memset(compats, 0);

    // Scratch is reused per document and per query; no [B,Q,K,K] allocation.
    const union_start = try allocator.alloc(f32, input.boundaries);
    defer allocator.free(union_start);
    const union_end = try allocator.alloc(f32, input.boundaries);
    defer allocator.free(union_end);
    const starts = try allocator.alloc(Ranked, input.boundaries);
    defer allocator.free(starts);
    const ends = try allocator.alloc(Ranked, input.boundaries);
    defer allocator.free(ends);
    const pair_entries = try allocator.alloc(Entry, pairs);
    defer allocator.free(pair_entries);
    const pair_compat = try allocator.alloc(f32, pairs);
    defer allocator.free(pair_compat);
    const ranked_pairs = try allocator.alloc(Ranked, pairs);
    defer allocator.free(ranked_pairs);
    const entries = try allocator.alloc(Entry, entries_len);
    defer allocator.free(entries);

    for (0..input.batch) |row| {
        if (input.control) |control| try control.check();
        var active = false;
        for (input.query_mask[row * input.queries ..][0..input.queries]) |enabled| active = active or enabled;
        for (0..input.boundaries) |position| {
            var s: f32 = -std.math.inf(f32);
            var e: f32 = -std.math.inf(f32);
            for (0..input.queries) |query| {
                const enabled = position <= input.lengths[row] and input.query_mask[row * input.queries + query];
                const offset = (row * input.queries + query) * input.boundaries + position;
                s = @max(s, if (enabled) try finite(input.start_logits[offset]) else mask_logit);
                e = @max(e, if (enabled) try finite(input.end_logits[offset]) else mask_logit);
            }
            union_start[position] = s;
            union_end[position] = e;
            const enabled = active and position <= input.lengths[row];
            starts[position] = .{ .score = if (enabled) s else mask_logit, .index = position, .valid = enabled };
            ends[position] = .{ .score = if (enabled) e else mask_logit, .index = position, .valid = enabled };
        }
        std.mem.sort(Ranked, starts, {}, Ranked.less);
        std.mem.sort(Ranked, ends, {}, Ranked.less);
        for (0..k) |si| {
            for (0..k) |ei| {
                const p = si * k + ei;
                const start = if (starts[si].valid) starts[si].index else 0;
                const end = if (ends[ei].valid) ends[ei].index else 0;
                const enabled = starts[si].valid and ends[ei].valid and end > start;
                const compat = try compatibility(input, row, start, end);
                pair_compat[p] = compat;
                pair_entries[p] = .{
                    .key = start * input.boundaries + end,
                    .score = try finite(compat + union_start[start] + union_end[end]),
                    .valid = enabled,
                    .order = p,
                };
            }
        }
        var written: usize = 0;
        for (0..input.queries) |query| {
            if (input.control) |control| try control.check();
            for (pair_entries, 0..) |pair, p| {
                const start = pair.key / input.boundaries;
                const end = pair.key % input.boundaries;
                const enabled = pair.valid and input.query_mask[row * input.queries + query];
                const offset = (row * input.queries + query) * input.boundaries;
                ranked_pairs[p] = .{
                    .score = if (enabled) try finite(input.start_logits[offset + start] + input.end_logits[offset + end] + pair_compat[p]) else mask_logit,
                    .index = p,
                    .valid = enabled,
                };
            }
            std.mem.sort(Ranked, ranked_pairs, {}, Ranked.less);
            for (ranked_pairs[0..quota], 0..) |ranked, rank| {
                entries[written] = .{
                    .key = if (ranked.valid) pair_entries[ranked.index].key else invalid_key,
                    .score = if (ranked.valid) -mask_logit * 0.5 + @as(f32, @floatFromInt(quota - rank)) else mask_logit,
                    .valid = ranked.valid,
                    .order = written,
                };
                written += 1;
            }
        }
        for (pair_entries) |pair| {
            entries[written] = .{
                .key = if (pair.valid) pair.key else invalid_key,
                .score = if (pair.valid) pair.score else mask_logit,
                .valid = pair.valid,
                .order = written,
            };
            written += 1;
        }
        std.mem.sort(Entry, entries, {}, Entry.byKey);
        var previous: ?usize = null;
        for (entries, 0..) |*entry, order| {
            const keep = entry.valid and (previous == null or previous.? != entry.key);
            previous = entry.key;
            entry.valid = keep;
            if (!keep) entry.score = mask_logit;
            entry.order = order;
        }
        std.mem.sort(Entry, entries, {}, Entry.byScore);
        for (entries[0..@min(config.capacity, entries.len)], 0..) |entry, index| {
            if (!entry.valid) continue;
            const start = entry.key / input.boundaries;
            const end = entry.key % input.boundaries;
            const output = row * config.capacity + index;
            indices[output] = .{ .start = start, .end = end };
            valid[output] = true;
            compats[output] = try compatibility(input, row, start, end);
            proposals[output] = try finite(compats[output] + union_start[start] + union_end[end]);
        }
    }
    if (input.control) |control| try control.check();
    return .{ .allocator = allocator, .batch = input.batch, .capacity = config.capacity, .indices = indices, .valid = valid, .proposal_logits = proposals, .compat_logits = compats };
}

/// Centered inside prefixes [rows,width+1] and detached means [rows]. The
/// caller restores mean*span_length before dividing the interval by sqrt(length).
pub fn centeredInsidePrefix(logits: []const f32, mask: []const bool, rows: usize, width: usize, prefix: []f32, means: []f32) !void {
    const stride = try std.math.add(usize, width, 1);
    const count = try mul(rows, width);
    if (logits.len != count or mask.len != count or prefix.len != try mul(rows, stride) or means.len != rows)
        return error.InvalidInputShape;
    for (0..rows) |row| {
        const offset = row * width;
        var total: f32 = 0;
        var valid_count: usize = 0;
        for (0..width) |i| if (mask[offset + i]) {
            total += try finite(logits[offset + i]);
            valid_count += 1;
        };
        const mean = try finite(total / @as(f32, @floatFromInt(@max(valid_count, 1))));
        means[row] = mean;
        prefix[row * stride] = 0;
        var running: f32 = 0;
        for (0..width) |i| {
            if (mask[offset + i]) running = try finite(running + (logits[offset + i] - mean));
            prefix[row * stride + i + 1] = running;
        }
    }
}

/// VJP of the centered prefix with its mean detached. Restoring the detached
/// mean in the scorer adds no gradient. Padding contributes no gradient.
pub fn centeredInsidePrefixBackward(grad_prefix: []const f32, mask: []const bool, rows: usize, width: usize, grad_logits: []f32) !void {
    const stride = try std.math.add(usize, width, 1);
    const count = try mul(rows, width);
    if (grad_prefix.len != try mul(rows, stride) or mask.len != count or grad_logits.len != count)
        return error.InvalidInputShape;
    for (0..rows) |row| {
        var running: f32 = 0;
        var i = width;
        while (i > 0) {
            running = try finite(running + grad_prefix[row * stride + i]);
            i -= 1;
            grad_logits[row * width + i] = if (mask[row * width + i]) running else 0;
        }
    }
}

pub const AttentionShape = struct {
    batch: usize,
    positions: usize,
    heads: usize,
    head_dim: usize,
    /// Symmetric radius, as in BoundaryAttentionBlock. Zero is full attention.
    window: usize,
    lengths: []const usize,
    control: ?Control = null,

    fn elements(self: @This()) !usize {
        if (self.batch == 0 or self.positions == 0 or self.heads == 0 or self.head_dim == 0 or self.lengths.len != self.batch)
            return error.InvalidInputShape;
        for (self.lengths) |length| if (length > self.positions) return error.InvalidInputShape;
        return mul(try mul(self.batch, self.positions), try mul(self.heads, self.head_dim));
    }

    fn offset(self: @This(), row: usize, position: usize, head: usize) usize {
        return ((row * self.positions + position) * self.heads + head) * self.head_dim;
    }

    fn keyRange(self: @This(), row: usize, position: usize) struct { start: usize, end: usize } {
        return .{
            .start = if (self.window == 0) 0 else position -| self.window,
            .end = if (self.window == 0) self.lengths[row] else @min(self.lengths[row], position +| self.window +| 1),
        };
    }
};

fn attentionScore(q: []const f32, k: []const f32, scale: f32) !f32 {
    var score: f32 = 0;
    for (q, k) |a, b| score += a * b;
    return finite(score * scale);
}

/// Attention on [B,N,heads,head_dim] without a dense attention matrix. Padding
/// queries produce zero, matching the upstream forced-diagonal then mask path.
/// Inputs/outputs must not alias. Dropout is handled outside this inference op.
pub fn bandedAttention(shape: AttentionShape, q: []const f32, k: []const f32, v: []const f32, out: []f32) !void {
    const count = try shape.elements();
    if (q.len != count or k.len != count or v.len != count or out.len != count) return error.InvalidInputShape;
    @memset(out, 0);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(shape.head_dim)));
    for (0..shape.batch) |row| {
        for (0..shape.lengths[row]) |position| {
            if (shape.control) |control| try control.check();
            const range = shape.keyRange(row, position);
            for (0..shape.heads) |head| {
                const qi = shape.offset(row, position, head);
                const query = q[qi..][0..shape.head_dim];
                const output = out[qi..][0..shape.head_dim];
                var maximum: f32 = -std.math.inf(f32);
                for (range.start..range.end) |key| {
                    const ki = shape.offset(row, key, head);
                    maximum = @max(maximum, try attentionScore(query, k[ki..][0..shape.head_dim], scale));
                }
                var denominator: f32 = 0;
                for (range.start..range.end) |key| {
                    const ki = shape.offset(row, key, head);
                    const weight = @exp((try attentionScore(query, k[ki..][0..shape.head_dim], scale)) - maximum);
                    denominator += weight;
                    for (output, v[ki..][0..shape.head_dim]) |*dst, value| dst.* += weight * value;
                }
                for (output) |*value| value.* = try finite(value.* / denominator);
            }
        }
    }
    if (shape.control) |control| try control.check();
}

/// VJP of bandedAttention, recomputing probabilities in bounded space. The
/// supplied output must come from the same forward inputs. All gradient buffers
/// are overwritten; no gradient is propagated through padded rows or keys.
pub fn bandedAttentionBackward(shape: AttentionShape, q: []const f32, k: []const f32, v: []const f32, out: []const f32, dy: []const f32, dq: []f32, dk: []f32, dv: []f32) !void {
    const count = try shape.elements();
    if (q.len != count or k.len != count or v.len != count or out.len != count or dy.len != count or
        dq.len != count or dk.len != count or dv.len != count) return error.InvalidInputShape;
    @memset(dq, 0);
    @memset(dk, 0);
    @memset(dv, 0);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(shape.head_dim)));
    for (0..shape.batch) |row| {
        for (0..shape.lengths[row]) |position| {
            if (shape.control) |control| try control.check();
            const range = shape.keyRange(row, position);
            for (0..shape.heads) |head| {
                const qi = shape.offset(row, position, head);
                const query = q[qi..][0..shape.head_dim];
                var maximum: f32 = -std.math.inf(f32);
                var mean_dp: f32 = 0;
                for (dy[qi..][0..shape.head_dim], out[qi..][0..shape.head_dim]) |grad, value| mean_dp += grad * value;
                for (range.start..range.end) |key| {
                    const ki = shape.offset(row, key, head);
                    maximum = @max(maximum, try attentionScore(query, k[ki..][0..shape.head_dim], scale));
                }
                var denominator: f32 = 0;
                for (range.start..range.end) |key| {
                    const ki = shape.offset(row, key, head);
                    denominator += @exp((try attentionScore(query, k[ki..][0..shape.head_dim], scale)) - maximum);
                }
                for (range.start..range.end) |key| {
                    const ki = shape.offset(row, key, head);
                    const probability = @exp((try attentionScore(query, k[ki..][0..shape.head_dim], scale)) - maximum) / denominator;
                    var dp: f32 = 0;
                    for (dy[qi..][0..shape.head_dim], v[ki..][0..shape.head_dim]) |grad, value| dp += grad * value;
                    const ds = try finite(probability * (dp - mean_dp) * scale);
                    for (0..shape.head_dim) |d| {
                        dq[qi + d] += ds * k[ki + d];
                        dk[ki + d] += ds * q[qi + d];
                        dv[ki + d] += probability * dy[qi + d];
                    }
                }
            }
        }
    }
    for (dq, dk, dv) |a, b, c| {
        _ = try finite(a);
        _ = try finite(b);
        _ = try finite(c);
    }
    if (shape.control) |control| try control.check();
}

test "gliner boundary banded attention uses local context and masks ragged rows" {
    const shape = AttentionShape{ .batch = 2, .positions = 4, .heads = 1, .head_dim = 1, .window = 1, .lengths = &.{ 4, 2 } };
    const q = [_]f32{0} ** 8;
    const values = [_]f32{ 1, 2, 6, 8, 10, 20, std.math.nan(f32), std.math.nan(f32) };
    var out: [8]f32 = undefined;
    try bandedAttention(shape, &q, &q, &values, &out);
    const expected = [_]f32{ 1.5, 3, 16.0 / 3.0, 7, 15, 15, 0, 0 };
    for (out, expected) |actual, value| try std.testing.expectApproxEqAbs(value, actual, 1e-6);
}

test "gliner boundary banded attention backward matches finite differences" {
    const shape = AttentionShape{ .batch = 1, .positions = 3, .heads = 1, .head_dim = 2, .window = 1, .lengths = &.{3} };
    var q = [_]f32{ 0.1, 0.3, -0.1, 0.2, 0.4, -0.5 };
    var k = [_]f32{ 0.2, -0.2, 0.3, 0.1, 0.5, -0.3 };
    var v = [_]f32{ 0.3, 0.5, -0.7, 0.2, 0.8, -0.4 };
    const dy = [_]f32{ 0.2, -0.1, 0.5, -0.7, 0.1, 0.8 };
    var out: [6]f32 = undefined;
    var dq: [6]f32 = undefined;
    var dk: [6]f32 = undefined;
    var dv: [6]f32 = undefined;
    try bandedAttention(shape, &q, &k, &v, &out);
    try bandedAttentionBackward(shape, &q, &k, &v, &out, &dy, &dq, &dk, &dv);
    const delta: f32 = 1e-3;
    for ([_][]f32{ &q, &k, &v }, [_][]const f32{ &dq, &dk, &dv }) |values, gradients| {
        for (values, gradients) |*value, expected| {
            const original = value.*;
            value.* = original + delta;
            try bandedAttention(shape, &q, &k, &v, &out);
            var upper: f32 = 0;
            for (out, dy) |a, b| upper += a * b;
            value.* = original - delta;
            try bandedAttention(shape, &q, &k, &v, &out);
            var lower: f32 = 0;
            for (out, dy) |a, b| lower += a * b;
            value.* = original;
            try std.testing.expectApproxEqAbs(expected, (upper - lower) / (2 * delta), 8e-5);
        }
    }
}

fn poolAllocationProbe(allocator: std.mem.Allocator) !void {
    const zeros = [_]f32{0} ** 8;
    var pool = try buildSharedPool(allocator, .{
        .batch = 1,
        .boundaries = 4,
        .queries = 2,
        .dim = 2,
        .lengths = &.{3},
        .query_mask = &.{ true, true },
        .start_logits = &zeros,
        .end_logits = &zeros,
        .projected_starts = &zeros,
        .projected_ends = &zeros,
    }, .{ .boundary_top_k = 4, .capacity = 8, .min_per_query = 2 });
    defer pool.deinit();
    const expected = [_]Span{
        .{ .start = 0, .end = 1 }, .{ .start = 0, .end = 2 }, .{ .start = 0, .end = 3 },
        .{ .start = 1, .end = 2 }, .{ .start = 1, .end = 3 }, .{ .start = 2, .end = 3 },
    };
    for (expected, 0..) |span, i| {
        try std.testing.expect(pool.valid[i]);
        try std.testing.expectEqualDeep(span, pool.indices[i]);
        try std.testing.expectEqual(@as(f32, 0), pool.proposal_logits[i]);
    }
    try std.testing.expect(!pool.valid[6] and !pool.valid[7]);
}

test "gliner boundary shared pool stable ties deduplicate quotas and unwind every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, poolAllocationProbe, .{});
}

test "gliner boundary shared pool masks padding and inactive queries per sample" {
    const zeros = [_]f32{0} ** 12;
    var pool = try buildSharedPool(std.testing.allocator, .{
        .batch = 2,
        .boundaries = 3,
        .queries = 2,
        .dim = 2,
        .lengths = &.{ 1, 2 },
        .query_mask = &.{ true, false, false, false },
        .start_logits = &zeros,
        .end_logits = &zeros,
        .projected_starts = &zeros,
        .projected_ends = &zeros,
    }, .{ .boundary_top_k = 3, .capacity = 4 });
    defer pool.deinit();
    try std.testing.expect(pool.valid[0]);
    try std.testing.expectEqualDeep(Span{ .start = 0, .end = 1 }, pool.indices[0]);
    for (pool.valid[1..]) |valid| try std.testing.expect(!valid);
    for (pool.proposal_logits[1..]) |score| try std.testing.expectEqual(mask_logit, score);
}

test "gliner boundary centered prefixes preserve intervals and masked gradients" {
    const logits = [_]f32{ 10001, 9998, 10004, std.math.nan(f32), 9, 8, 7, 6 };
    const mask = [_]bool{ true, true, true, false, false, false, false, false };
    var prefix: [10]f32 = undefined;
    var means: [2]f32 = undefined;
    try centeredInsidePrefix(&logits, &mask, 2, 4, &prefix, &means);
    try std.testing.expectEqual(@as(f32, 10001), means[0]);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, -3, 0, 0, 0, 0, 0, 0, 0 }, &prefix);
    try std.testing.expectEqual(@as(f32, 20002), prefix[3] - prefix[1] + means[0] * 2);
    var grad: [8]f32 = undefined;
    try centeredInsidePrefixBackward(&.{ 99, 0, 2, 3, 5, 99, 1, 2, 3, 4 }, &mask, 2, 4, &grad);
    try std.testing.expectEqualSlices(f32, &.{ 10, 10, 8, 0, 0, 0, 0, 0 }, &grad);
}
