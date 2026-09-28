// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! CPU reference for the ModernBERT training attention
//! (`ml.graph.ModernBertTrainingAttentionAttrs`). Storage is linear in the
//! sequence: the forward keeps only per-row softmax statistics, and the
//! backward replays them. Every loop runs in a fixed order, so results are
//! bit-reproducible. The Metal kernels implement the same arithmetic.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Attrs = @import("ml").graph.ModernBertTrainingAttentionAttrs;

/// The i32 control leaf: `tokens*6` range bounds, then `tokens` positions.
pub const ControlView = struct {
    ranges: []align(1) const i32,
    positions: []align(1) const i32,
};

/// Every range is half-open, ordered, disjoint from the query's other ranges
/// (device kernels walk them without deduplicating keys), and, when nonempty,
/// inside the query's own batch row, so a key's possible queries are exactly
/// its row's queries.
pub fn validateControl(attrs: Attrs, words: []align(1) const i32) !ControlView {
    const layout = try attrs.layout();
    const tokens: usize = @intCast(layout.tokens);
    if (words.len != tokens * 7) return error.InvalidModernBertTrainingAttentionControl;
    const ranges = words[0 .. tokens * 6];
    const s: i64 = attrs.seq_len;
    for (0..tokens) |q| {
        const row_start = @as(i64, @intCast(q / attrs.seq_len)) * s;
        const own = ranges[q * 6 ..][0..6];
        for (0..3) |r| {
            const lo: i64 = own[2 * r];
            const hi: i64 = own[2 * r + 1];
            if (lo < 0 or hi < lo) return error.InvalidModernBertTrainingAttentionControl;
            if (lo == hi) continue;
            if (lo < row_start or hi > row_start + s) return error.InvalidModernBertTrainingAttentionControl;
            for (0..r) |prior| {
                const prior_lo: i64 = own[2 * prior];
                const prior_hi: i64 = own[2 * prior + 1];
                if (prior_lo < prior_hi and lo < prior_hi and prior_lo < hi) return error.InvalidModernBertTrainingAttentionControl;
            }
        }
    }
    return .{ .ranges = ranges, .positions = words[tokens * 6 ..] };
}

pub fn visible(attrs: Attrs, view: ControlView, q: usize, k: usize) bool {
    const own = view.ranges[q * 6 ..][0..6];
    const key: i64 = @intCast(k);
    const in_range = (key >= own[0] and key < own[1]) or (key >= own[2] and key < own[3]) or (key >= own[4] and key < own[5]);
    if (!in_range) return false;
    if (attrs.window == std.math.maxInt(u32)) return true;
    return @abs(@as(i64, view.positions[q]) - view.positions[k]) <= attrs.window;
}

/// Linear device scratch for one call: per-row statistics (forward), plus
/// the replayed output and D = rowsum(dO * O) (backward).
pub fn scratchBytes(attrs: Attrs, is_backward: bool) !usize {
    const layout = try attrs.layout();
    const rows = try std.math.mul(usize, @intCast(layout.tokens), attrs.num_heads);
    if (!is_backward) return std.math.mul(usize, rows, 4);
    const replayed = try std.math.mul(usize, try std.math.mul(usize, @intCast(layout.tokens), @intCast(layout.hidden)), 4);
    return std.math.add(usize, replayed, try std.math.mul(usize, rows, 8));
}

/// Upper bound on multiply-adds: every query against its whole row.
pub fn workItems(attrs: Attrs, is_backward: bool) !u64 {
    const layout = try attrs.layout();
    const pairs = try std.math.mul(u64, try std.math.mul(u64, @intCast(layout.tokens), attrs.seq_len), attrs.num_heads);
    return std.math.mul(u64, pairs, @as(u64, attrs.head_dim) * (if (is_backward) @as(u64, 8) else 3));
}

fn dot(a: []const f32, b: []const f32) f32 {
    var total: f32 = 0;
    for (a, b) |x, y| total += x * y;
    return total;
}

const Operands = struct {
    q: []const f32,
    k: []const f32,
    v: []const f32,
    hidden: usize,
    head_dim: usize,

    fn init(attrs: Attrs, qkv: []const f32) !Operands {
        const layout = try attrs.layout();
        const tokens: usize = @intCast(layout.tokens);
        const hidden: usize = @intCast(layout.hidden);
        if (qkv.len != 3 * tokens * hidden) return error.InvalidModernBertTrainingAttentionShape;
        return .{ .q = qkv[0 .. tokens * hidden], .k = qkv[tokens * hidden .. 2 * tokens * hidden], .v = qkv[2 * tokens * hidden ..], .hidden = hidden, .head_dim = attrs.head_dim };
    }
    fn row(self: Operands, values: []const f32, token: usize, head: usize) []const f32 {
        return values[token * self.hidden + head * self.head_dim ..][0..self.head_dim];
    }
};

/// Softmax statistics of one (query, head) row and, optionally, its output.
/// `lse` is -inf for a row with no visible key, whose output is zero.
fn rowForward(attrs: Attrs, view: ControlView, x: Operands, scale: f32, q: usize, h: usize, out: ?[]f32) f32 {
    const base = (q / attrs.seq_len) * attrs.seq_len;
    const query = x.row(x.q, q, h);
    var best = -std.math.inf(f32);
    for (base..base + attrs.seq_len) |k| if (visible(attrs, view, q, k)) {
        best = @max(best, dot(query, x.row(x.k, k, h)) * scale);
    };
    if (out) |o| @memset(o, 0);
    if (best == -std.math.inf(f32)) return best;
    var sum: f32 = 0;
    for (base..base + attrs.seq_len) |k| if (visible(attrs, view, q, k)) {
        const w = @exp(dot(query, x.row(x.k, k, h)) * scale - best);
        sum += w;
        if (out) |o| for (o, x.row(x.v, k, h)) |*acc, value| {
            acc.* += w * value;
        };
    };
    if (out) |o| for (o) |*acc| {
        acc.* /= sum;
    };
    return best + @log(sum);
}

pub fn forward(a: Allocator, attrs: Attrs, qkv: []const f32, words: []align(1) const i32, control: ?Control) ![]f32 {
    const view = try validateControl(attrs, words);
    const x = try Operands.init(attrs, qkv);
    const tokens = x.q.len / x.hidden;
    const output = try a.alloc(f32, tokens * x.hidden);
    errdefer a.free(output);
    const scale = 1 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim)));
    for (0..attrs.num_heads) |h| {
        if (control) |c| try c.check();
        for (0..tokens) |q| _ = rowForward(attrs, view, x, scale, q, h, output[q * x.hidden + h * x.head_dim ..][0..x.head_dim]);
    }
    return output;
}

/// Packed [dQ; dK; dV], shaped like the forward's [Q; K; V].
pub fn backward(a: Allocator, attrs: Attrs, qkv: []const f32, words: []align(1) const i32, dout: []const f32, control: ?Control) ![]f32 {
    const view = try validateControl(attrs, words);
    const x = try Operands.init(attrs, qkv);
    const tokens = x.q.len / x.hidden;
    if (dout.len != tokens * x.hidden) return error.InvalidModernBertTrainingAttentionShape;
    const gradient = try a.alloc(f32, 3 * tokens * x.hidden);
    errdefer a.free(gradient);
    @memset(gradient, 0);
    const dq = gradient[0 .. tokens * x.hidden];
    const dk = gradient[tokens * x.hidden .. 2 * tokens * x.hidden];
    const dv = gradient[2 * tokens * x.hidden ..];
    const lse = try a.alloc(f32, tokens);
    defer a.free(lse);
    const delta = try a.alloc(f32, tokens);
    defer a.free(delta);
    const replayed = try a.alloc(f32, x.head_dim);
    defer a.free(replayed);
    const scale = 1 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim)));
    for (0..attrs.num_heads) |h| {
        if (control) |c| try c.check();
        // Per query: replay the statistics, D = dO . O, and dQ.
        for (0..tokens) |q| {
            lse[q] = rowForward(attrs, view, x, scale, q, h, replayed);
            const upstream = x.row(dout, q, h);
            delta[q] = dot(upstream, replayed);
            if (lse[q] == -std.math.inf(f32)) continue;
            const base = (q / attrs.seq_len) * attrs.seq_len;
            const query = x.row(x.q, q, h);
            const grad = dq[q * x.hidden + h * x.head_dim ..][0..x.head_dim];
            for (base..base + attrs.seq_len) |k| if (visible(attrs, view, q, k)) {
                const key = x.row(x.k, k, h);
                const p = @exp(dot(query, key) * scale - lse[q]);
                const ds = p * (dot(upstream, x.row(x.v, k, h)) - delta[q]) * scale;
                for (grad, key) |*g, value| g.* += ds * value;
            };
        }
        // Per key: its row's queries in order give dK and dV.
        for (0..tokens) |k| {
            const base = (k / attrs.seq_len) * attrs.seq_len;
            const key = x.row(x.k, k, h);
            const value = x.row(x.v, k, h);
            const grad_k = dk[k * x.hidden + h * x.head_dim ..][0..x.head_dim];
            const grad_v = dv[k * x.hidden + h * x.head_dim ..][0..x.head_dim];
            for (base..base + attrs.seq_len) |q| if (lse[q] != -std.math.inf(f32) and visible(attrs, view, q, k)) {
                const query = x.row(x.q, q, h);
                const upstream = x.row(dout, q, h);
                const p = @exp(dot(query, key) * scale - lse[q]);
                const ds = p * (dot(upstream, value) - delta[q]) * scale;
                for (grad_v, upstream) |*g, u| g.* += p * u;
                for (grad_k, query) |*g, u| g.* += ds * u;
            };
        }
    }
    return gradient;
}

// ---------------------------------------------------------------------------
// Tests: a dense reference with the materialized trunk's arithmetic.

pub const TestCase = struct {
    attrs: Attrs,
    lengths: []const usize, // valid tokens per row; padded queries see the valid keys
    segments: bool = false, // split each row's valid prefix into two trees under a trunk
};

/// Controls equivalent to the materialized masks: key padding, the local
/// window on physical positions, and optionally Laya-style trunk/branch trees.
pub fn testControl(a: Allocator, case: TestCase) ![]i32 {
    const s = case.attrs.seq_len;
    const tokens = case.attrs.batch * s;
    const words = try a.alloc(i32, tokens * 7);
    @memset(words, 0);
    for (0..case.attrs.batch) |b| {
        const len = case.lengths[b];
        const base: i32 = @intCast(b * s);
        for (0..s) |i| {
            const q = b * s + i;
            const ranges = words[q * 6 ..][0..6];
            words[tokens * 6 + q] = @intCast(i);
            const trunk = len / 2;
            if (case.segments and i < len and len >= 4) {
                // Trunk [0, trunk) sees itself; branch A [trunk, trunk+1) and
                // branch B [trunk+1, len) see the trunk and themselves.
                ranges[0] = base;
                ranges[1] = base + @as(i32, @intCast(trunk));
                if (i >= trunk) {
                    const own_lo: usize = if (i == trunk) trunk else trunk + 1;
                    const own_hi: usize = if (i == trunk) trunk + 1 else len;
                    ranges[2] = base + @as(i32, @intCast(own_lo));
                    ranges[3] = base + @as(i32, @intCast(own_hi));
                }
            } else {
                ranges[0] = base;
                ranges[1] = base + @as(i32, @intCast(len));
            }
        }
    }
    return words;
}

/// Dense scores + visibility mask + softmax + PV, the decomposed arithmetic.
fn denseForward(a: Allocator, attrs: Attrs, qkv: []const f32, words: []const i32) ![]f32 {
    const view = try validateControl(attrs, words);
    const x = try Operands.init(attrs, qkv);
    const tokens = x.q.len / x.hidden;
    const output = try a.alloc(f32, tokens * x.hidden);
    @memset(output, 0);
    const scores = try a.alloc(f32, tokens);
    defer a.free(scores);
    const scale = 1 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim)));
    for (0..attrs.num_heads) |h| for (0..tokens) |q| {
        var best = -std.math.inf(f32);
        for (0..tokens) |k| {
            scores[k] = if (visible(attrs, view, q, k)) dot(x.row(x.q, q, h), x.row(x.k, k, h)) * scale else -std.math.inf(f32);
            best = @max(best, scores[k]);
        }
        if (best == -std.math.inf(f32)) continue;
        var sum: f32 = 0;
        for (scores) |*score| {
            score.* = if (score.* == -std.math.inf(f32)) 0 else @exp(score.* - best);
            sum += score.*;
        }
        const out = output[q * x.hidden + h * x.head_dim ..][0..x.head_dim];
        for (scores, 0..) |w, k| for (out, x.row(x.v, k, h)) |*o, value| {
            o.* += w / sum * value;
        };
    };
    return output;
}

fn fill(values: []f32, seed: u64) void {
    var prng = std.Random.DefaultPrng.init(seed);
    for (values) |*value| value.* = prng.random().floatNorm(f32) * 0.7;
}

const cases = [_]TestCase{
    .{ .attrs = .{ .batch = 2, .seq_len = 7, .num_heads = 2, .head_dim = 4 }, .lengths = &.{ 7, 4 } },
    .{ .attrs = .{ .batch = 2, .seq_len = 9, .num_heads = 3, .head_dim = 2, .window = 2 }, .lengths = &.{ 9, 5 } },
    .{ .attrs = .{ .batch = 1, .seq_len = 10, .num_heads = 2, .head_dim = 4 }, .lengths = &.{8}, .segments = true },
    .{ .attrs = .{ .batch = 2, .seq_len = 8, .num_heads = 1, .head_dim = 6, .window = 1 }, .lengths = &.{ 8, 3 }, .segments = true },
};

test "modernbert training attention forward equals dense masked attention, with zero rows when nothing is visible" {
    const a = std.testing.allocator;
    for (cases) |case| {
        const layout = try case.attrs.layout();
        const qkv = try a.alloc(f32, @intCast(layout.qkv_rows * layout.hidden));
        defer a.free(qkv);
        fill(qkv, 11);
        const words = try testControl(a, case);
        defer a.free(words);
        const fused = try forward(a, case.attrs, qkv, words, null);
        defer a.free(fused);
        const dense = try denseForward(a, case.attrs, qkv, words);
        defer a.free(dense);
        for (dense, fused) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-6);
    }
    // Window 1 over physical positions leaves padded queries far from every
    // valid key with no visible key: their output is exactly zero.
    const case = cases[3];
    const layout = try case.attrs.layout();
    const qkv = try a.alloc(f32, @intCast(layout.qkv_rows * layout.hidden));
    defer a.free(qkv);
    fill(qkv, 5);
    const words = try testControl(a, case);
    defer a.free(words);
    const fused = try forward(a, case.attrs, qkv, words, null);
    defer a.free(fused);
    const padded_far = (8 + 7) * 6; // row 1, position 7, farther than 1 from its 3 valid keys
    for (fused[padded_far..][0..6]) |value| try std.testing.expectEqual(@as(f32, 0), value);
}

test "modernbert training attention backward matches central finite differences" {
    const a = std.testing.allocator;
    for (cases) |case| {
        const layout = try case.attrs.layout();
        const qkv = try a.alloc(f32, @intCast(layout.qkv_rows * layout.hidden));
        defer a.free(qkv);
        fill(qkv, 23);
        const words = try testControl(a, case);
        defer a.free(words);
        const dout = try a.alloc(f32, @intCast(layout.tokens * layout.hidden));
        defer a.free(dout);
        fill(dout, 29);
        const analytic = try backward(a, case.attrs, qkv, words, dout, null);
        defer a.free(analytic);
        // L = sum(dout * forward(qkv)); check every coordinate in f64-safe steps.
        const eps: f32 = 1e-2;
        for (qkv, 0..) |original, i| {
            qkv[i] = original + eps;
            const plus = try forward(a, case.attrs, qkv, words, null);
            defer a.free(plus);
            qkv[i] = original - eps;
            const minus = try forward(a, case.attrs, qkv, words, null);
            defer a.free(minus);
            qkv[i] = original;
            var numeric: f64 = 0;
            for (plus, minus, dout) |p, m, d| numeric += (@as(f64, p) - m) * d;
            numeric /= 2 * eps;
            try std.testing.expectApproxEqAbs(numeric, analytic[i], 2e-3 + 2e-3 * @abs(numeric));
        }
    }
}

test "modernbert training attention rejects ranges outside the query's row and is bit-reproducible" {
    const a = std.testing.allocator;
    const case = cases[0];
    const words = try testControl(a, case);
    defer a.free(words);
    words[7 * 6 + 1] = 8; // row 1's first query reaching into row 0 is fine only within row 1
    words[7 * 6 + 0] = 6;
    try std.testing.expectError(error.InvalidModernBertTrainingAttentionControl, validateControl(case.attrs, words));
    words[7 * 6 + 0] = 7;
    words[7 * 6 + 1] = 6; // hi < lo
    try std.testing.expectError(error.InvalidModernBertTrainingAttentionControl, validateControl(case.attrs, words));
    words[7 * 6 + 1] = 11;
    _ = try validateControl(case.attrs, words);
    const layout = try case.attrs.layout();
    const qkv = try a.alloc(f32, @intCast(layout.qkv_rows * layout.hidden));
    defer a.free(qkv);
    fill(qkv, 3);
    const dout = try a.alloc(f32, @intCast(layout.tokens * layout.hidden));
    defer a.free(dout);
    fill(dout, 4);
    const first = try backward(a, case.attrs, qkv, words, dout, null);
    defer a.free(first);
    const second = try backward(a, case.attrs, qkv, words, dout, null);
    defer a.free(second);
    try std.testing.expectEqualSlices(u32, std.mem.bytesAsSlice(u32, std.mem.sliceAsBytes(first)), std.mem.bytesAsSlice(u32, std.mem.sliceAsBytes(second)));
}
