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

//! Grouped attention with explicit causal/block visibility. Host controls
//! specialize the layout; every projection, score and reduction stays in IR.
const std = @import("std");
const ml = @import("ml");
const contracts = @import("backend_contracts.zig");
const Graph = ml.graph.Graph;
const Shape = ml.graph.Shape;
const Id = ml.graph.NodeId;

pub fn emit(g: *Graph, q: Id, k: Id, v: Id, bias: ?Id, attention: contracts.AttentionContext, batch: usize, heads: usize, kv_heads: usize, dim: usize) !Id {
    const queries = attention.query_sequence_len;
    const keys = attention.kv_sequence_len;
    const total = attention.total_sequence_len;
    const mask = attention.attn_or_mask orelse return error.MissingAttentionMask;
    if (attention.attention_sink.hasMetadata() or attention.kv_cache != null or attention.kv_manager != null or attention.kv_storage != null or attention.kv_batch != null) return error.UnsupportedAttentionShape;
    if (batch != 1 or heads == 0 or kv_heads == 0 or heads % kv_heads != 0 or dim == 0 or queries == 0 or keys == 0 or queries > total or attention.kv_position_offset > total or keys > total - attention.kv_position_offset or mask.len != try std.math.mul(usize, total, total) or attention.mode != .dense_causal) return error.UnsupportedAttentionShape;
    var b = ml.graph.Builder.init(g);
    const qh = try headRows(&b, q, queries, heads, dim);
    var kh = try headRows(&b, k, keys, kv_heads, dim);
    var vh = try headRows(&b, v, keys, kv_heads, dim);
    if (heads != kv_heads) {
        const groups = try g.allocator.alloc(i32, heads);
        defer g.allocator.free(groups);
        for (groups, 0..) |*group, index| group.* = @intCast(index / (heads / kv_heads));
        const indices = try b.tensorConstBytes(std.mem.sliceAsBytes(groups), Shape.init(.i32, &.{@intCast(heads)}));
        const repeated = Shape.init(.f32, &.{ @intCast(heads), @intCast(keys), @intCast(dim) });
        kh = try b.gather(kh, indices, repeated);
        vh = try b.gather(vh, indices, repeated);
    }
    var scores = try b.mul(try b.matmul3DTransB(qh, kh), try b.scalarConst(.f32, 1 / @sqrt(@as(f32, @floatFromInt(dim)))));
    const visibility = try g.allocator.alloc(f32, try std.math.mul(usize, queries, keys));
    defer g.allocator.free(visibility);
    for (0..queries) |row| for (0..keys) |col| {
        const query = total - queries + row;
        const key = attention.kv_position_offset + col;
        const causal_or_block = key <= query or mask[query * total + key] != 0;
        // Transformers' sliding-window overlay clips old keys after applying
        // the blockwise OR; future keys in the same block pass that overlay.
        const window = key > query or attention.sliding_window == 0 or query - key < attention.sliding_window;
        visibility[row * keys + col] = if (causal_or_block and window) 0 else -1e9;
    };
    const layout = try b.tensorConst(visibility, Shape.init(.f32, &.{ 1, @intCast(queries), @intCast(keys) }));
    scores = try b.add(scores, layout);
    if (bias) |value| scores = try b.add(scores, if (g.node(value).output_shape.dtype == .f32) value else try b.convertDtype(value, .f32));
    const context = try b.matmul3D(try b.softmax(scores), vh);
    const ordered = try b.transpose(context, &.{ 1, 0, 2 });
    return b.reshape(ordered, Shape.init(.f32, &.{ @intCast(queries), @intCast(heads * dim) }));
}

fn headRows(b: *ml.graph.Builder, input: Id, rows: usize, heads: usize, dim: usize) !Id {
    const source = if (b.graph.node(input).output_shape.dtype == .f32) input else try b.convertDtype(input, .f32);
    const shaped = try b.reshape(source, Shape.init(.f32, &.{ @intCast(rows), @intCast(heads), @intCast(dim) }));
    return b.transpose(shaped, &.{ 1, 0, 2 });
}
