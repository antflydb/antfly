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

//! F32 causal depthwise convolution, gated recurrence and gated RMS epilogue.
//! Projection inputs retain their declared rounding; no internal BF16 stores
//! are introduced into the recurrent matrix or the normalization reductions.
const ml = @import("ml");
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const Id = ml.graph.NodeId;
const Attrs = ml.graph.node.GatedDeltaAttrs;
const none = ml.graph.null_node;

fn full(b: *Builder, scalar: f32, shape: Shape) !Id {
    return broadcast(b.graph, try b.scalarConst(.f32, scalar), shape, &.{});
}

fn broadcast(g: *Graph, input: Id, shape: Shape, axes: []const u8) !Id {
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = shape, .num_axes = @intCast(axes.len) };
    @memcpy(attrs.broadcast_axes[0..axes.len], axes);
    return g.addNode(.{ .op = .{ .broadcast_in_dim = attrs }, .output_shape = shape, .inputs = .{ input, none, none, none }, .num_inputs = 1 });
}
fn less(g: *Graph, lhs: Id, rhs: Id) !Id {
    var shape = g.node(lhs).output_shape;
    shape.dtype = .bool_;
    return g.addNode(.{ .op = .{ .less_than = {} }, .output_shape = shape, .inputs = .{ lhs, rhs, none, none }, .num_inputs = 2 });
}
fn choose(g: *Graph, predicate: Id, yes: Id, no: Id) !Id {
    return g.addNode(.{ .op = .{ .where_select = {} }, .output_shape = g.node(yes).output_shape, .inputs = .{ predicate, yes, no, none }, .num_inputs = 3 });
}

pub fn emit(g: *Graph, sources: [8]Id, p: Attrs, kernel: u32, eps: f32) !Id {
    if (p.rows == 0 or p.key_heads == 0 or p.value_heads == 0 or p.value_heads % p.key_heads != 0 or p.key_dim == 0 or p.value_dim == 0 or kernel == 0) return error.InvalidInputShape;
    var b = Builder.init(g);
    var x: [8]Id = undefined;
    for (sources, &x) |source, *node| node.* = if (g.node(source).output_shape.dtype == .f32) source else try b.convertDtype(source, .f32);
    const rows: i64 = p.rows;
    const heads: i64 = p.value_heads;
    const value: i64 = p.value_dim;
    const channels = 2 * @as(i64, p.key_heads) * p.key_dim + heads * value;
    // The temporal stencil owner consumes [kernel,channel] taps. Checkpoint
    // adapters supply [channel,kernel], as used by the native recurrent core.
    const taps = try b.transpose(try b.reshape(x[4], Shape.init(.f32, &.{ channels, kernel })), &.{ 1, 0 });
    const convolved = try @import("temporal_program.zig").depthwiseCausal(g, x[0], taps, p.rows, @intCast(channels), kernel);
    const mixed = try b.silu(convolved);
    const gate_shape = Shape.init(.f32, &.{ rows, heads });
    const a = try b.add(try b.reshape(x[3], gate_shape), try broadcast(g, try b.reshape(x[6], Shape.init(.f32, &.{heads})), gate_shape, &.{1}));
    const exponent = try b.expOp(a);
    const soft = try b.logOp(try b.add(try full(&b, 1, gate_shape), exponent));
    const high = try less(g, try full(&b, 20, gate_shape), a);
    const low = try less(g, a, try full(&b, -20, gate_shape));
    const softplus = try choose(g, high, a, try choose(g, low, exponent, soft));
    const alog = try broadcast(g, try b.expOp(try b.reshape(x[5], Shape.init(.f32, &.{heads}))), gate_shape, &.{1});
    const decay = try b.expOp(try b.neg(try b.mul(alog, softplus)));
    const beta = try b.sigmoid(try b.reshape(x[2], gate_shape));
    const core = try g.addNode(.{ .op = .{ .fused_gated_delta = p }, .output_shape = Shape.init(.f32, &.{ rows, heads * value }), .inputs = .{ mixed, beta, decay, none }, .num_inputs = 3 });
    const norm_input = try b.reshape(core, Shape.init(.f32, &.{ rows * heads, value }));
    const normalized = try b.rmsNorm(norm_input, try b.reshape(x[7], Shape.init(.f32, &.{value})), @intCast(value), eps);
    return b.mul(try b.reshape(normalized, Shape.init(.f32, &.{ rows, heads * value })), try b.silu(try b.reshape(x[1], Shape.init(.f32, &.{ rows, heads * value }))));
}
