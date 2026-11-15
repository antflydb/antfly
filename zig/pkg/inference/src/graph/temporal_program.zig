// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Temporal layouts and bounded local attention expressed in canonical IR.
//! Window storage is O(rows * context * hidden), independent of clip length².
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const Id = ml.graph.NodeId;
const none = ml.graph.null_node;

fn broadcast(g: *Graph, value: Id, axes: []const u8, shape: Shape) !Id {
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = shape, .num_axes = @intCast(axes.len) };
    @memcpy(attrs.broadcast_axes[0..axes.len], axes);
    return g.addNode(.{ .op = .{ .broadcast_in_dim = attrs }, .output_shape = shape, .inputs = .{ value, none, none, none }, .num_inputs = 1 });
}
fn select(g: *Graph, predicate: Id, yes: Id, no: Id, shape: Shape) !Id {
    return g.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape, .inputs = .{ predicate, yes, no, none }, .num_inputs = 3 });
}
fn positive(g: *Graph, value: Id) !Id {
    var b = Builder.init(g);
    const shape = g.node(value).output_shape;
    const zero = try broadcast(g, try b.scalarConst(.f32, 0), &.{}, shape);
    return g.addNode(.{ .op = .{ .less_than = {} }, .output_shape = Shape.init(.bool_, shape.dims[0..shape.rank()]), .inputs = .{ zero, value, none, none }, .num_inputs = 2 });
}
pub fn channelNormRelu(g: *Graph, input: Id, weight: Id, channels: usize, positions: usize, eps: f32) !Id {
    if (channels == 0 or positions == 0) return error.InvalidInputShape;
    const original = g.node(input).output_shape;
    var b = Builder.init(g);
    const transposed = try b.transpose(try b.reshape(input, Shape.init(.f32, &.{ @intCast(channels), @intCast(positions) })), &.{ 1, 0 });
    const zero = try broadcast(g, try b.scalarConst(.f32, 0), &.{}, Shape.init(.f32, &.{@intCast(channels)}));
    const normalized = try b.layerNorm(transposed, weight, zero, @intCast(channels), eps);
    return b.reshape(try b.transpose(try b.relu(normalized), &.{ 1, 0 }), original);
}
pub fn flattenChannels(g: *Graph, input: Id, time: usize, freq: usize, channels: usize) !Id {
    var b = Builder.init(g);
    const x = try b.reshape(input, Shape.init(.f32, &.{ @intCast(channels), @intCast(time), @intCast(freq) }));
    return b.reshape(try b.transpose(x, &.{ 1, 2, 0 }), Shape.init(.f32, &.{ @intCast(time), @intCast(freq * channels) }));
}
pub fn glu(g: *Graph, input: Id, rows: usize, dim: usize) !Id {
    var b = Builder.init(g);
    const x = try b.reshape(input, Shape.init(.f32, &.{ @intCast(rows), @intCast(2 * dim) }));
    const left = try b.sliceLastDim(x, 0, @intCast(dim));
    const right = try b.sliceLastDim(x, @intCast(dim), @intCast(2 * dim));
    return b.mul(left, try b.sigmoid(right));
}
pub fn depthwiseCausal(g: *Graph, input: Id, weight: Id, rows: usize, dim: usize, kernel: usize) !Id {
    if (rows == 0 or dim == 0 or kernel == 0) return error.InvalidInputShape;
    var b = Builder.init(g);
    const x = try b.reshape(try b.transpose(try b.reshape(input, Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) })), &.{ 1, 0 }), Shape.init(.f32, &.{ 1, @intCast(dim), @intCast(rows) }));
    const w = try b.reshape(try b.transpose(try b.reshape(weight, Shape.init(.f32, &.{ @intCast(kernel), @intCast(dim) })), &.{ 1, 0 }), Shape.init(.f32, &.{ @intCast(dim), 1, @intCast(kernel) }));
    var attrs = ml.graph.node.ConvAttrs{ .num_spatial = 1, .groups = @intCast(dim) };
    attrs.padding[0] = .{ @intCast(kernel - 1), 0 };
    const conv = try g.addNode(.{ .op = .{ .conv_general = attrs }, .output_shape = Shape.init(.f32, &.{ 1, @intCast(dim), @intCast(rows) }), .inputs = .{ x, w, none, none }, .num_inputs = 2 });
    return b.transpose(try b.reshape(conv, Shape.init(.f32, &.{ @intCast(dim), @intCast(rows) })), &.{ 1, 0 });
}
pub fn localRelative(g: *Graph, q: Id, k: Id, v: Id, relative: Id, scales: Id, valid: Id, p: ops.Gemma4AudioLocalAttentionParams) !Id {
    if (p.rows == 0 or p.heads == 0 or p.head_dim == 0 or p.hidden != p.heads * p.head_dim or p.context_left == 0 or p.chunk == 0 or p.context < p.context_left or p.context - p.context_left < p.chunk - 1 or !std.math.isFinite(p.k_scale) or !std.math.isFinite(p.query_scale) or std.math.isNan(p.invalid_value) or p.segmented or p.packed_qkv or !std.math.isFinite(p.logit_cap) or p.logit_cap <= 0 or p.invalid_value > -1e4) return error.UnsupportedAttentionShape;
    const rows: i64 = @intCast(p.rows);
    const heads: i64 = @intCast(p.heads);
    const dim: i64 = @intCast(p.head_dim);
    const width: i64 = @intCast(p.hidden);
    const window: i64 = @intCast(p.context_left);
    const count = try std.math.mul(usize, p.rows, p.context_left);
    const indices = try g.allocator.alloc(i64, count);
    defer g.allocator.free(indices);
    const legal = try g.allocator.alloc(f32, count);
    defer g.allocator.free(legal);
    const relative_ids = try g.allocator.alloc(i64, p.context_left);
    defer g.allocator.free(relative_ids);
    for (relative_ids, 0..) |*index, distance| index.* = @intCast(p.context_left - 1 - distance);
    for (0..p.rows) |row| for (0..p.context_left) |distance| {
        const index = row * p.context_left + distance;
        indices[index] = @intCast(row -| distance);
        legal[index] = @floatFromInt(@intFromBool(distance <= row and (!p.exclude_farthest_key or distance + 1 < p.context_left)));
    };
    var b = Builder.init(g);
    const ids = try b.tensorConstBytes(std.mem.sliceAsBytes(indices), Shape.init(.i64, &.{@intCast(count)}));
    const ri = try b.tensorConstBytes(std.mem.sliceAsBytes(relative_ids), Shape.init(.i64, &.{window}));
    const flat = Shape.init(.f32, &.{ rows * window, width });
    const gathered_k = try b.transpose(try b.reshape(try b.gather(k, ids, flat), Shape.init(.f32, &.{ rows, window, heads, dim })), &.{ 0, 2, 1, 3 });
    const gathered_v = try b.transpose(try b.reshape(try b.gather(v, ids, flat), Shape.init(.f32, &.{ rows, window, heads, dim })), &.{ 0, 2, 1, 3 });
    const rel = try b.transpose(try b.reshape(try b.gather(relative, ri, Shape.init(.f32, &.{ window, width })), Shape.init(.f32, &.{ 1, window, heads, dim })), &.{ 0, 2, 1, 3 });
    var query = try b.reshape(q, Shape.init(.f32, &.{ rows, heads, 1, dim }));
    if (p.arithmetic == .independent_f32) query = try b.mul(query, try b.scalarConst(.f32, p.query_scale));
    query = try b.mul(query, scales);
    const full = Shape.init(.f32, &.{ rows, heads, window, dim });
    query = try broadcast(g, query, &.{ 0, 1, 2, 3 }, full);
    const content = try b.mul(query, try b.mul(gathered_k, try b.scalarConst(.f32, p.k_scale)));
    const position = try b.mul(query, try broadcast(g, rel, &.{ 0, 1, 2, 3 }, full));
    var score = if (p.arithmetic == .independent_f32) try b.add(try b.reduceSum(content, &.{3}), try b.reduceSum(position, &.{3})) else try b.reduceSum(try b.add(content, position), &.{3});
    const score_shape = Shape.init(.f32, &.{ rows, heads, window });
    score = try b.reshape(try b.mul(try b.tanhOp(try b.div(score, try b.scalarConst(.f32, p.logit_cap))), try b.scalarConst(.f32, p.logit_cap)), score_shape);
    const mask_flat = try b.reshape(valid, Shape.init(.f32, &.{rows}));
    const key_mask = try b.reshape(try b.gather(mask_flat, ids, Shape.init(.f32, &.{rows * window})), Shape.init(.f32, &.{ rows, window }));
    const legal_node = try b.tensorConst(legal, Shape.init(.f32, &.{ rows, window }));
    const visibility = try b.mul(legal_node, key_mask);
    const visible = try positive(g, visibility);
    const negative = try broadcast(g, try b.scalarConst(.f32, p.invalid_value), &.{}, score_shape);
    score = try select(g, try broadcast(g, visible, &.{ 0, 2 }, Shape.init(.bool_, &.{ rows, heads, window })), score, negative, score_shape);
    const probability = try b.softmax(score);
    const safe_v = try select(g, try broadcast(g, visible, &.{ 0, 2 }, Shape.init(.bool_, &.{ rows, heads, window, dim })), gathered_v, try broadcast(g, try b.scalarConst(.f32, 0), &.{}, full), full);
    const weighted = try b.mul(try b.reshape(probability, Shape.init(.f32, &.{ rows, heads, window, 1 })), safe_v);
    const result_shape = Shape.init(.f32, &.{ rows, width });
    var result = try b.reshape(try b.reduceSum(weighted, &.{2}), result_shape);
    if (p.publication == .bf16_values_f32) result = try b.convertDtype(try b.convertDtype(result, .bf16), .f32);
    const query_valid = try b.mul(mask_flat, try b.reshape(try b.reduceSum(visibility, &.{1}), Shape.init(.f32, &.{rows})));
    return select(g, try broadcast(g, try positive(g, query_valid), &.{0}, Shape.init(.bool_, &.{ rows, width })), result, try broadcast(g, try b.scalarConst(.f32, 0), &.{}, result_shape), result_shape);
}
