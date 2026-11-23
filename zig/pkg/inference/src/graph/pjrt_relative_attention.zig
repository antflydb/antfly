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

//! Content/content and two content/relative terms share batched dot and indexed
//! selection. Relative index grids are computed from linear position vectors;
//! neither checkpoint-specific kernels nor host quadratic tables are needed.
const std = @import("std");
const hlo = @import("pjrt").hlo;
const Attrs = @import("ml").graph.node.AttentionAttrs;

fn heads(b: *hlo.Builder, value: hlo.Id, batch: i64, rows: i64, count: i64, dim: i64) !hlo.Id {
    const shaped = try b.reshape(value, hlo.Shape.init(.f32, &.{ batch, rows, count, dim }));
    return b.transpose(shaped, &.{ 0, 2, 1, 3 }, hlo.Shape.init(.f32, &.{ batch, count, rows, dim }));
}
fn projectedRelative(b: *hlo.Builder, value: hlo.Id, batch: i64, rows: i64, count: i64, dim: i64) !hlo.Id {
    const shaped = try b.reshape(value, hlo.Shape.init(.f32, &.{ rows, count, dim }));
    const transposed = try b.transpose(shaped, &.{ 1, 2, 0 }, hlo.Shape.init(.f32, &.{ count, dim, rows }));
    return b.broadcast(transposed, &.{ 1, 2, 3 }, hlo.Shape.init(.f32, &.{ batch, count, dim, rows }));
}
fn dot(b: *hlo.Builder, lhs: hlo.Id, rhs: hlo.Id, shape: hlo.Shape) !hlo.Id {
    return b.dot(lhs, rhs, shape, .{ .lhs_contracting_dimensions = &.{3}, .rhs_contracting_dimensions = &.{2}, .lhs_batch_dimensions = &.{ 0, 1 }, .rhs_batch_dimensions = &.{ 0, 1 } });
}
fn indexed(b: *hlo.Builder, value: hlo.Id, position: hlo.Id, relative: hlo.Id, batch: i64, count: i64, seq: i64) !hlo.Id {
    const index_shape = hlo.Shape.init(.s32, &.{ seq, seq, 1 });
    const indices = try b.concatenate(&.{ try b.reshape(position, index_shape), try b.reshape(relative, index_shape) }, 2, hlo.Shape.init(.s32, &.{ seq, seq, 2 }));
    return b.gatherWithDimensions(value, indices, hlo.Shape.init(.f32, &.{ batch, count, seq, seq }), .{ .offset_dims = &.{ 0, 1 }, .collapsed_slice_dims = &.{ 2, 3 }, .start_index_map = &.{ 2, 3 }, .index_vector_dim = 2 }, &.{ batch, count, 1, 1 });
}

/// The packed ABI is shared with the ordinary interpreter and tuned backends:
/// Q/K/V [3*B*S,H], relative Q/K [2*(2*S-1),H], padding bias [B*heads,S,S].
pub fn emit(allocator: std.mem.Allocator, b: *hlo.Builder, qkv_packed: hlo.Id, relative_packed: hlo.Id, bias: hlo.Id, attrs: Attrs, add_reducer: hlo.Id, max_reducer: hlo.Id) !hlo.Id {
    if (attrs.batch == 0 or attrs.seq_len == 0 or attrs.num_heads == 0 or attrs.head_dim == 0) return error.UnsupportedShape;
    const batch: i64 = attrs.batch;
    const seq: i64 = attrs.seq_len;
    const count: i64 = attrs.num_heads;
    const dim: i64 = attrs.head_dim;
    const hidden = count * dim;
    const rows = batch * seq;
    const relatives = 2 * seq - 1;
    if (!std.mem.eql(i64, b.getInst(qkv_packed).shape.dimensions, &.{ 3 * rows, hidden }) or !std.mem.eql(i64, b.getInst(relative_packed).shape.dimensions, &.{ 2 * relatives, hidden }) or !std.mem.eql(i64, b.getInst(bias).shape.dimensions, &.{ batch * count, seq, seq })) return error.UnsupportedShape;
    var qkv: [3]hlo.Id = undefined;
    for (&qkv, 0..) |*part, index| {
        const offset: i64 = @intCast(index);
        const sliced = try b.slice(qkv_packed, &.{ offset * rows, 0 }, &.{ (offset + 1) * rows, hidden }, &.{ 1, 1 }, hlo.Shape.init(.f32, &.{ rows, hidden }));
        part.* = try heads(b, sliced, batch, seq, count, dim);
    }
    var relative: [2]hlo.Id = undefined;
    for (&relative, 0..) |*part, index| {
        const offset: i64 = @intCast(index);
        const sliced = try b.slice(relative_packed, &.{ offset * relatives, 0 }, &.{ (offset + 1) * relatives, hidden }, &.{ 1, 1 }, hlo.Shape.init(.f32, &.{ relatives, hidden }));
        part.* = try projectedRelative(b, sliced, batch, relatives, count, dim);
    }
    const positions = try allocator.alloc(i32, @intCast(seq + 1));
    defer allocator.free(positions);
    for (positions[0..@intCast(seq)], 0..) |*value, index| value.* = std.math.cast(i32, index) orelse return error.UnsupportedShape;
    positions[@intCast(seq)] = std.math.cast(i32, seq - 1) orelse return error.UnsupportedShape;
    const index = try b.constantS32(hlo.Shape.init(.s32, &.{seq}), positions[0..@intCast(seq)]);
    const grid_shape = hlo.Shape.init(.s32, &.{ seq, seq });
    const qi = try b.broadcast(index, &.{0}, grid_shape);
    const ki = try b.broadcast(index, &.{1}, grid_shape);
    const center = try b.constantS32(hlo.Shape.scalar(.s32), positions[@intCast(seq)..]);
    const rel_index = try b.add(try b.subtract(qi, ki), center);
    const score_shape = hlo.Shape.init(.f32, &.{ batch, count, seq, seq });
    const relative_shape = hlo.Shape.init(.f32, &.{ batch, count, seq, relatives });
    const key_t = try b.transpose(qkv[1], &.{ 0, 1, 3, 2 }, hlo.Shape.init(.f32, &.{ batch, count, dim, seq }));
    const content = try dot(b, qkv[0], key_t, score_shape);
    const c2p = try indexed(b, try dot(b, qkv[0], relative[1], relative_shape), qi, rel_index, batch, count, seq);
    const p2c = try indexed(b, try dot(b, qkv[1], relative[0], relative_shape), ki, rel_index, batch, count, seq);
    var scores = try b.multiply(try b.add(try b.add(content, c2p), p2c), try b.constantScalarF32(1 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim)) * 3)));
    // The canonical packed contract derives visibility from the first query of
    // each batch's first head; arbitrary additive biases are a different ABI.
    const key_bias = try b.slice(bias, &.{ 0, 0, 0 }, &.{ batch * count, 1, seq }, &.{ count, 1, 1 }, hlo.Shape.init(.f32, &.{ batch, 1, seq }));
    const visibility = try b.compare(try b.reshape(key_bias, hlo.Shape.init(.f32, &.{ batch, seq })), try b.broadcast(try b.constantScalarF32(-1.0e8), &.{}, hlo.Shape.init(.f32, &.{ batch, seq })), "GE");
    const zero = try b.constantScalarF32(0);
    const negative = try b.constantScalarF32(-std.math.inf(f32));
    scores = try b.select(try b.broadcast(visibility, &.{ 0, 3 }, hlo.Shape.init(.pred, score_shape.dimensions)), scores, try b.broadcast(negative, &.{}, score_shape));
    const row_shape = hlo.Shape.init(.f32, &.{ batch, count, seq });
    const maximum = try b.reduce(scores, negative, &.{3}, row_shape, max_reducer);
    const safe_maximum = try b.maximum(maximum, try b.constantScalarF32(-std.math.floatMax(f32)));
    const exp = try b.exponential(try b.subtract(scores, try b.broadcast(safe_maximum, &.{ 0, 1, 2 }, score_shape)));
    const sums = try b.reduce(exp, zero, &.{3}, row_shape, add_reducer);
    const probabilities = try b.divide(exp, try b.broadcast(try b.maximum(sums, try b.constantScalarF32(1)), &.{ 0, 1, 2 }, score_shape));
    const value_shape = hlo.Shape.init(.f32, &.{ batch, count, seq, dim });
    const value = try b.select(try b.broadcast(visibility, &.{ 0, 2 }, hlo.Shape.init(.pred, value_shape.dimensions)), qkv[2], try b.broadcast(zero, &.{}, value_shape));
    const attended = try dot(b, probabilities, value, value_shape);
    const transposed = try b.transpose(attended, &.{ 0, 2, 1, 3 }, hlo.Shape.init(.f32, &.{ batch, seq, count, dim }));
    return b.reshape(transposed, hlo.Shape.init(.f32, &.{ rows, hidden }));
}
