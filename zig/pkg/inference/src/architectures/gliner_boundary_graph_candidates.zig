// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Candidate-dependent differentiable GLiNER2.5 heads. Indices and geometry
//! are immutable external decisions; all compatibility and learned content
//! scores are recomputed from live Stage1 graph nodes.
const std = @import("std");
const ml = @import("ml").graph;
const core = @import("gliner_boundary_graph.zig");
const G = core.GraphBuilder;
const Id = ml.NodeId;
const Shape = ml.Shape;

pub const PoolInput = struct {
    capacity: u32,
    /// Absolute rows in [B*(W+1),D], including the sample's batch offset.
    starts: Id, // [B*C] i32
    ends: Id,
    valid: Id, // [B,C] f32 binary
    lengths: Id, // [B*C,1] f32 max(end-start,1)
    length_features: Id, // [B*C,3] log1p(length), length/max(text_length,1), rsqrt(length)
    /// Pinned inside-prefix centering is explicitly detached. Bind this to
    /// the mean calculated from the SAME captured Stage1 inside logits.
    inside_mean: ?Id = null, // [B,Q,1]
};
pub const PoolOutput = struct {
    pair_logits: Id, // [B,Q,C]
    shared_logits: Id, // [B,C,Q], same values in loss-native pool layout
    proposal_logits: Id, // [B,C], live query-union marginal + compatibility
    proposal_compat: Id, // [B,C], live query-independent compatibility
    candidate_features: Id, // [B*C,P]
    candidate_states: ?Id, // [B*C,H], endpoint representation used by records
};

fn mul(a: u32, b: u32) !u32 {
    return std.math.mul(u32, a, b) catch error.BoundaryTrainingGraphLimitExceeded;
}
fn admit(g: *G, dims: []const i64) !void {
    try g.check();
    const count = Shape.init(.f32, dims).numElements() orelse return error.InvalidBoundaryTrainingGraphShape;
    if (count < 0 or count > g.limits.max_tensor_elements) return error.BoundaryTrainingGraphLimitExceeded;
}
fn staticBytes(g: *G, count: usize) !void {
    try g.check();
    const total = std.math.add(usize, g.builder.graph.constant_pool.items.len, count) catch return error.BoundaryTrainingGraphLimitExceeded;
    if (total > g.limits.max_constant_bytes) return error.BoundaryTrainingGraphLimitExceeded;
}

/// Creates bounded descriptor leaves only. A training packet binder must prove
/// spans/masks/lengths describe the retained candidates and original samples.
pub fn poolInputs(g: *G, capacity: u32) !PoolInput {
    if (capacity == 0) return error.InvalidBoundaryTrainingGraphLayout;
    const b = g.layout.batch;
    const rows = try mul(b, capacity);
    const bound = try mul(b, g.layout.words + 1);
    return .{
        .capacity = capacity,
        .starts = try g.input("__gliner25.pool.starts", Shape.init(.i32, &.{rows}), .indices, .candidates, bound),
        .ends = try g.input("__gliner25.pool.ends", Shape.init(.i32, &.{rows}), .indices, .candidates, bound),
        .valid = try g.input("__gliner25.pool.valid", Shape.init(.f32, &.{ b, capacity }), .binary_mask, .candidates, 0),
        .lengths = try g.input("__gliner25.pool.lengths", Shape.init(.f32, &.{ rows, 1 }), .values, .candidates, 0),
        .length_features = try g.input("__gliner25.pool.length_features", Shape.init(.f32, &.{ rows, 3 }), .values, .candidates, 0),
        .inside_mean = if (g.config.head.use_inside_evidence) try g.input("__gliner25.pool.inside_mean", Shape.init(.f32, &.{ b, g.layout.queries, 1 }), .values, .candidates, 0) else null,
    };
}

fn checkedPool(g: *G, input: PoolInput) !void {
    const b = g.layout.batch;
    const c = input.capacity;
    if (c == 0 or g.layout.queries == 0) return error.InvalidBoundaryTrainingGraphLayout;
    const rows = try mul(b, c);
    try g.require(input.starts, Shape.init(.i32, &.{rows}));
    try g.require(input.ends, Shape.init(.i32, &.{rows}));
    try g.require(input.valid, Shape.init(.f32, &.{ b, c }));
    try g.require(input.lengths, Shape.init(.f32, &.{ rows, 1 }));
    try g.require(input.length_features, Shape.init(.f32, &.{ rows, 3 }));
    if (g.config.head.use_inside_evidence) try g.require(input.inside_mean orelse return error.MissingBoundaryTrainingInsideMean, Shape.init(.f32, &.{ b, g.layout.queries, 1 }));
}

/// Prefix table [B*(W+1),D] and absolute boundary indices. No source or
/// projection activation is detached; gather VJPs accumulate reused spans.
pub fn meanPool(g: *G, prefix: Id, starts: Id, ends: Id, lengths: Id, rows: u32, dim: u32) !Id {
    try g.require(lengths, Shape.init(.f32, &.{ rows, 1 }));
    const start = try g.gather(prefix, starts, rows, dim);
    const end = try g.gather(prefix, ends, rows, dim);
    const width = try g.expand(lengths, &.{ rows, dim }, &.{ 0, 1 });
    return g.builder.div(try g.builder.sub(end, start), width);
}

fn contentPool(g: *G, stage1: core.Proposals, starts: Id, ends: Id, lengths: Id, rows: u32, prefix: []const u8) !Id {
    if (g.config.head.content_soft_max_pool) return error.UnsupportedBoundaryTrainingGraphOption;
    const b = g.layout.batch;
    const w = g.layout.words;
    const h = g.config.encoder.hidden_size;
    const dim = g.config.head.content_dim;
    var name: [384]u8 = undefined;
    var values = try g.linear(stage1.input.text, h, dim, try std.fmt.bufPrint(&name, "{s}.value_projection", .{prefix}));
    values = try g.rowMask(values, stage1.input.text_mask, try mul(b, w), dim);
    const table = try g.prefixSum(values, b, w, dim);
    const pooled = try meanPool(g, table, starts, ends, lengths, rows, dim);
    const normed = try g.norm(pooled, dim, try std.fmt.bufPrint(&name, "{s}.layer_norm", .{prefix}));
    return g.dropout(normed, prefix, .candidates);
}

fn transposedMarginals(g: *G, value: Id) !Id {
    const b = g.layout.batch;
    const n = g.layout.words + 1;
    const q = g.layout.queries;
    try g.require(value, Shape.init(.f32, &.{ b, q, n }));
    return g.reshape(try g.builder.transpose(value, &.{ 0, 2, 1 }), &.{ try mul(b, n), q });
}

fn insidePrefix(g: *G, stage1: core.Proposals, detached_mean: Id) !Id {
    const b = g.layout.batch;
    const q = g.layout.queries;
    const w = g.layout.words;
    try g.require(detached_mean, Shape.init(.f32, &.{ b, q, 1 }));
    const dims = [_]i64{ b, q, w };
    const keep = try g.builder.mul(try g.expand(stage1.input.text_mask, &dims, &.{ 0, 2 }), try g.expand(stage1.input.query_mask, &dims, &.{ 0, 1 }));
    const centered = try g.builder.sub(stage1.inside_logits, try g.expand(detached_mean, &dims, &.{ 0, 1, 2 }));
    const valid = try g.maskFill(centered, keep, 0);
    const prefix = try g.prefixSum(try g.reshape(valid, &.{ try mul(try mul(b, q), w), 1 }), try mul(b, q), w, 1);
    return g.reshape(prefix, &.{ b, q, w + 1 });
}

pub fn buildSharedPool(g: *G, stage1: core.Proposals, input: PoolInput) !PoolOutput {
    // Published base/small/multi profiles disable these optional modules. A
    // future artifact must provide a qualified graph path before using them.
    if (g.config.head.candidate_attention_layers != 0 or g.config.head.query_attention_layers != 0 or g.config.head.content_soft_max_pool) return error.UnsupportedBoundaryTrainingGraphOption;
    try checkedPool(g, input);
    const b = g.layout.batch;
    const q = g.layout.queries;
    const c = input.capacity;
    const n = g.layout.words + 1;
    const d = g.config.head.boundary_dim;
    const h = g.config.encoder.hidden_size;
    const p = g.config.head.pair_dim;
    const rows = try mul(b, c);
    const score_rows = try mul(rows, q);
    try admit(g, &.{ score_rows, @max(p, 64) });
    const gs = try g.gather(stage1.pool_start, input.starts, rows, d);
    const ge = try g.gather(stage1.pool_end, input.ends, rows, d);
    var compatibility = try g.scale(try g.builder.reduceSum(try g.builder.mul(gs, ge), &.{1}), 1 / @sqrt(@as(f32, @floatFromInt(d))));
    compatibility = try g.maskFill(compatibility, try g.reshape(input.valid, &.{ rows, 1 }), 0);
    const union_start = try g.reshape(try g.builder.reduceMax(stage1.start_logits, &.{1}), &.{ try mul(b, n), 1 });
    const union_end = try g.reshape(try g.builder.reduceMax(stage1.end_logits, &.{1}), &.{ try mul(b, n), 1 });
    const union_s = try g.gather(union_start, input.starts, rows, 1);
    const union_e = try g.gather(union_end, input.ends, rows, 1);
    const proposal_logits = try g.maskFill(try g.builder.add(compatibility, try g.builder.add(union_s, union_e)), try g.reshape(input.valid, &.{ rows, 1 }), -10000);
    const start_all = try g.linear(stage1.boundary_states, d, p, "boundary_head.shared_pool_scorer.start_projection");
    const end_all = try g.linear(stage1.boundary_states, d, p, "boundary_head.shared_pool_scorer.end_projection");
    var candidates = try g.builder.add(try g.gather(start_all, input.starts, rows, p), try g.gather(end_all, input.ends, rows, p));
    candidates = try g.builder.add(candidates, try g.linear(input.length_features, 3, p, "boundary_head.shared_pool_scorer.length_projection"));
    candidates = try g.builder.add(candidates, try g.linear(compatibility, 1, p, "boundary_head.shared_pool_scorer.prior_projection"));
    if (g.config.head.enable_span_content) {
        const content = try contentPool(g, stage1, input.starts, input.ends, input.lengths, rows, "boundary_head.shared_pool_scorer.content_pooler");
        candidates = try g.builder.add(candidates, try g.linear(content, g.config.head.content_dim, p, "boundary_head.shared_pool_scorer.content_projection"));
    }
    candidates = try g.rowMask(try g.norm(candidates, p, "boundary_head.shared_pool_scorer.candidate_norm"), input.valid, rows, p);
    const queries = try g.linear(stage1.input.queries, h, p, "boundary_head.shared_pool_scorer.query_projection");
    const candidate3 = try g.reshape(candidates, &.{ b, c, p });
    const query3 = try g.reshape(queries, &.{ b, q, p });
    var scores = try g.scale(try g.builder.matmul3D(candidate3, try g.builder.transpose(query3, &.{ 0, 2, 1 })), 1 / @sqrt(@as(f32, @floatFromInt(p))));
    const film = try g.linear(queries, p, try mul(2, p), "boundary_head.shared_pool_scorer.film");
    const gamma = try g.reshape(try g.builder.sliceLastDim(film, 0, p), &.{ b, q, p });
    const beta = try g.reshape(try g.builder.sliceLastDim(film, p, 2 * p), &.{ b, q, p });
    const dims4 = [_]i64{ b, c, q, p };
    const conditioned = try g.builder.add(try g.builder.mul(try g.expand(candidate3, &dims4, &.{ 0, 1, 3 }), try g.builder.add(try g.expand(gamma, &dims4, &.{ 0, 2, 3 }), try g.builder.scalarConst(.f32, 1))), try g.expand(beta, &dims4, &.{ 0, 2, 3 }));
    var hidden = try g.linear(try g.reshape(conditioned, &.{ score_rows, p }), p, 64, "boundary_head.shared_pool_scorer.film_output.0");
    hidden = try g.dropout(try g.builder.geluExact(hidden), "shared_pool.film_hidden", .candidates);
    const film_score = try g.reshape(try g.linear(hidden, 64, 1, "boundary_head.shared_pool_scorer.film_output.3"), &.{ b, c, q });
    scores = try g.builder.add(scores, film_score);
    const start_logits = try g.gather(try transposedMarginals(g, stage1.start_logits), input.starts, rows, q);
    const end_logits = try g.gather(try transposedMarginals(g, stage1.end_logits), input.ends, rows, q);
    scores = try g.builder.add(scores, try g.reshape(try g.builder.add(start_logits, end_logits), &.{ b, c, q }));
    if (g.config.head.use_inside_evidence) {
        const mean = input.inside_mean.?;
        const prefix = try transposedMarginals(g, try insidePrefix(g, stage1, mean));
        const interval = try g.builder.sub(try g.gather(prefix, input.ends, rows, q), try g.gather(prefix, input.starts, rows, q));
        const mean3 = try g.expand(try g.reshape(mean, &.{ b, q }), &.{ b, c, q }, &.{ 0, 2 });
        const widths = try g.expand(input.lengths, &.{ rows, q }, &.{ 0, 1 });
        const restored = try g.builder.add(interval, try g.builder.mul(try g.reshape(mean3, &.{ rows, q }), widths));
        const inside = try g.builder.div(restored, try g.builder.sqrt(widths));
        scores = try g.builder.add(scores, try g.reshape(inside, &.{ b, c, q }));
    }
    const keep = try g.builder.mul(try g.expand(input.valid, &.{ b, c, q }, &.{ 0, 1 }), try g.expand(stage1.input.query_mask, &.{ b, c, q }, &.{ 0, 2 }));
    scores = try g.maskFill(scores, keep, -10000);
    const candidate_states: ?Id = if (g.config.head.enable_records) blk: {
        const endpoints = try g.builder.concat(try g.gather(stage1.boundary_states, input.starts, rows, d), try g.gather(stage1.boundary_states, input.ends, rows, d), 1);
        break :blk try g.rowMask(try g.linear(endpoints, try mul(2, d), h, "boundary_head.candidate_encoder"), input.valid, rows, h);
    } else null;
    const result = PoolOutput{ .pair_logits = try g.builder.transpose(scores, &.{ 0, 2, 1 }), .shared_logits = scores, .proposal_logits = try g.reshape(proposal_logits, &.{ b, c }), .proposal_compat = try g.reshape(compatibility, &.{ b, c }), .candidate_features = candidates, .candidate_states = candidate_states };
    try g.check();
    return result;
}

pub const ExplicitInput = struct {
    capacity: u32,
    starts: Id, // [B*Q*C], absolute boundary/prefix row B*(W+1)
    ends: Id,
    marginal_starts: Id, // [B*Q*C], absolute row in flattened [B,Q,W+1]
    marginal_ends: Id,
    valid: Id, // [B,Q,C], binary and already aligned to real word spans
    lengths: Id, // [B*Q*C,1], max(end-start,1)
    length_features: Id, // [B*Q*C,3]
    inside_mean: ?Id = null, // [B,Q,1], detached from the same Stage1
};
pub const ExplicitOutput = struct { logits: Id, proposal_compat: Id };

fn repeatedQueryIndices(g: *G, capacity: u32) !Id {
    const rows = try mul(try mul(g.layout.batch, g.layout.queries), capacity);
    try staticBytes(g, try std.math.mul(usize, rows, 4));
    const indices = try g.allocator.alloc(i32, rows);
    defer g.allocator.free(indices);
    for (indices, 0..) |*index, row| {
        if (row % 4096 == 0) try g.check();
        index.* = @intCast(row / capacity);
    }
    return g.builder.tensorConstBytes(std.mem.sliceAsBytes(indices), Shape.init(.i32, &.{rows}));
}
fn rotate(g: *G, values: Id, dim: u32) !Id {
    if (!g.config.head.enable_rotary_endpoints) return values;
    if (dim == 0 or dim % 2 != 0) return error.InvalidBoundaryTrainingGraphShape;
    const n = g.layout.words + 1;
    const rows = try mul(g.layout.batch, n);
    const half = dim / 2;
    const count = try mul(rows, half);
    try staticBytes(g, try std.math.mul(usize, count, 8));
    const cosine = try g.allocator.alloc(f32, count);
    defer g.allocator.free(cosine);
    const sine = try g.allocator.alloc(f32, count);
    defer g.allocator.free(sine);
    for (cosine, sine, 0..) |*cs, *sn, i| {
        if (i % 4096 == 0) try g.check();
        const position: f32 = @floatFromInt((i / half) % n);
        const exponent = @as(f32, @floatFromInt(2 * (i % half))) / @as(f32, @floatFromInt(dim));
        const inverse = 1 / std.math.pow(f32, g.config.head.rotary_base, exponent);
        cs.* = @cos(position * inverse);
        sn.* = @sin(position * inverse);
    }
    const cs = try g.builder.tensorConst(cosine, Shape.init(.f32, &.{ count, 1 }));
    const sn = try g.builder.tensorConst(sine, Shape.init(.f32, &.{ count, 1 }));
    const paired = try g.reshape(values, &.{ count, 2 });
    const even = try g.builder.sliceLastDim(paired, 0, 1);
    const odd = try g.builder.sliceLastDim(paired, 1, 2);
    const first = try g.builder.sub(try g.builder.mul(even, cs), try g.builder.mul(odd, sn));
    const second = try g.builder.add(try g.builder.mul(even, sn), try g.builder.mul(odd, cs));
    return g.reshape(try g.builder.concat(first, second, 1), &.{ rows, dim });
}
fn queryGate(g: *G, queries: Id, dim: u32, prefix: []const u8) !Id {
    const qrows = try mul(g.layout.batch, g.layout.queries);
    const width = if (g.config.head.enable_rotary_endpoints) dim / 2 else dim;
    const gate = try g.builder.sigmoid(try g.linear(queries, g.config.encoder.hidden_size, width, prefix));
    if (!g.config.head.enable_rotary_endpoints) return gate;
    return g.reshape(try g.expand(gate, &.{ qrows, width, 2 }, &.{ 0, 1 }), &.{ qrows, dim });
}

/// Reuses live boundary states but independently rescales selected spans for
/// every query, including hidden attribute/enum queries. No sparse shortlist
/// or detached cached score substitutes for this path.
pub fn buildExplicitSpans(g: *G, stage1: core.Proposals, input: ExplicitInput) !ExplicitOutput {
    if (input.capacity == 0 or g.layout.queries == 0 or g.config.head.content_soft_max_pool) return error.UnsupportedBoundaryTrainingGraphOption;
    const b = g.layout.batch;
    const q = g.layout.queries;
    const c = input.capacity;
    const qrows = try mul(b, q);
    const rows = try mul(qrows, c);
    const n = g.layout.words + 1;
    const h = g.config.encoder.hidden_size;
    const d = g.config.head.boundary_dim;
    const p = g.config.head.pair_dim;
    for ([_]Id{ input.starts, input.ends, input.marginal_starts, input.marginal_ends }) |index| try g.require(index, Shape.init(.i32, &.{rows}));
    try g.require(input.valid, Shape.init(.f32, &.{ b, q, c }));
    try g.require(input.lengths, Shape.init(.f32, &.{ rows, 1 }));
    try g.require(input.length_features, Shape.init(.f32, &.{ rows, 3 }));
    try admit(g, &.{ rows, @max(p, d) });
    const query_indices = try repeatedQueryIndices(g, c);
    const query_keep = try g.gather(try g.reshape(stage1.input.query_mask, &.{ qrows, 1 }), query_indices, rows, 1);
    const keep = try g.builder.mul(try g.reshape(input.valid, &.{ rows, 1 }), query_keep);
    const projected_start = try rotate(g, try g.linear(stage1.boundary_states, d, d, "boundary_head.boundary_proposer.start_pair_projection"), d);
    const projected_end = try rotate(g, try g.linear(stage1.boundary_states, d, d, "boundary_head.boundary_proposer.end_key_projection"), d);
    const gate = try g.gather(try queryGate(g, stage1.input.queries, d, "boundary_head.boundary_proposer.start_query_projection"), query_indices, rows, d);
    const start = try g.gather(projected_start, input.starts, rows, d);
    const end = try g.gather(projected_end, input.ends, rows, d);
    const prior_raw = try g.scale(try g.builder.reduceSum(try g.builder.mul(try g.builder.mul(start, gate), end), &.{1}), 1 / @sqrt(@as(f32, @floatFromInt(d))));
    const prior = try g.maskFill(prior_raw, keep, 0);
    const scorer_start_all = try rotate(g, try g.linear(stage1.boundary_states, d, p, "boundary_head.pair_scorer.start_endpoint_projection"), p);
    const scorer_end_all = try rotate(g, try g.linear(stage1.boundary_states, d, p, "boundary_head.pair_scorer.end_endpoint_projection"), p);
    const scorer_start = try g.dropout(try g.gather(scorer_start_all, input.starts, rows, p), "explicit.start", .candidates);
    const scorer_end = try g.dropout(try g.gather(scorer_end_all, input.ends, rows, p), "explicit.end", .candidates);
    const scorer_gate = try g.gather(try queryGate(g, stage1.input.queries, p, "boundary_head.pair_scorer.query_gate"), query_indices, rows, p);
    var compatibility = if (g.config.head.reranker_endpoint_compat) blk: {
        const heads = g.config.head.multihead_pair_compat_heads;
        const product = try g.builder.mul(try g.builder.mul(scorer_start, scorer_gate), scorer_end);
        const by_head = try g.reshape(product, &.{ rows, heads, p / heads });
        const per_head = try g.reshape(try g.builder.reduceSum(by_head, &.{2}), &.{ rows, heads });
        break :blk try g.scale(try g.linear(per_head, heads, 1, "boundary_head.pair_scorer.compat_mix"), 1 / @sqrt(@as(f32, @floatFromInt(p))));
    } else try g.fill(&.{ rows, 1 }, 0);
    if (g.config.head.endpoint_difference_features) {
        const difference = try g.builder.sub(scorer_start, scorer_end);
        const joined = try g.builder.concat(difference, try g.builder.absOp(difference), 1);
        compatibility = try g.builder.add(compatibility, try g.linear(joined, try mul(2, p), 1, "boundary_head.pair_scorer.endpoint_difference_projection"));
    }
    const a = try g.gather(try g.reshape(stage1.start_logits, &.{ try mul(qrows, n), 1 }), input.marginal_starts, rows, 1);
    const e = try g.gather(try g.reshape(stage1.end_logits, &.{ try mul(qrows, n), 1 }), input.marginal_ends, rows, 1);
    var scores = try g.builder.add(try g.builder.add(try g.builder.add(compatibility, a), e), prior);
    if (g.config.head.enable_span_content) {
        const content = try contentPool(g, stage1, input.starts, input.ends, input.lengths, rows, "boundary_head.pair_scorer.content_pooler");
        const cd = g.config.head.content_dim;
        const coefficients = try g.gather(try g.linear(stage1.input.queries, h, cd, "boundary_head.pair_scorer.content_query_projection"), query_indices, rows, cd);
        const term = try g.scale(try g.builder.reduceSum(try g.builder.mul(content, coefficients), &.{1}), 1 / @sqrt(@as(f32, @floatFromInt(cd))));
        scores = try g.builder.add(scores, try g.builder.add(term, try g.linear(content, cd, 1, "boundary_head.pair_scorer.content_bias")));
    }
    if (g.config.head.use_inside_evidence) {
        const mean = input.inside_mean orelse return error.MissingBoundaryTrainingInsideMean;
        const prefix = try g.reshape(try insidePrefix(g, stage1, mean), &.{ try mul(qrows, n), 1 });
        const interval = try g.builder.sub(try g.gather(prefix, input.marginal_ends, rows, 1), try g.gather(prefix, input.marginal_starts, rows, 1));
        const means = try g.gather(try g.reshape(mean, &.{ qrows, 1 }), query_indices, rows, 1);
        const restored = try g.builder.add(interval, try g.builder.mul(means, input.lengths));
        const weight = if (g.config.head.query_conditioned_inside_weight) try g.gather(try g.linear(stage1.input.queries, h, 1, "boundary_head.pair_scorer.inside_weight"), query_indices, rows, 1) else try g.weight("boundary_head.pair_scorer.inside_weight", &.{});
        scores = try g.builder.add(scores, try g.builder.mul(weight, try g.builder.div(restored, try g.builder.sqrt(input.lengths))));
    }
    const length_coeff = try g.gather(try g.linear(stage1.input.queries, h, 3, "boundary_head.pair_scorer.length_query_projection"), query_indices, rows, 3);
    scores = try g.builder.add(scores, try g.builder.reduceSum(try g.builder.mul(length_coeff, input.length_features), &.{1}));
    const result = ExplicitOutput{ .logits = try g.reshape(try g.maskFill(scores, keep, -10000), &.{ b, q, c }), .proposal_compat = try g.reshape(prior, &.{ b, q, c }) };
    try g.check();
    return result;
}
