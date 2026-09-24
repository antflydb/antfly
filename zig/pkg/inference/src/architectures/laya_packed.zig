// Copyright 2026 Antfly, Inc.
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

//! Session contract for one tree-packed Laya row (models/laya/LAYA.md).
//!
//! Inputs, all i64: `input_ids`, `position_ids`, `token_segment`,
//! `token_qtype` as `[1, L]`; `segment_parent` as `[1, S]`; `marker_pos` as
//! `[Q, W]`; `anchor_pos` as `[Q, 1]`. Outputs: `logits` `[Q, W]` and
//! `action_logits` `[Q, n_act]`.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const modern = @import("modern_bert.zig");
const head = @import("laya_head.zig");
const tree = @import("../pipelines/laya_tree.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;

pub const input_count = 7;
pub const inputs_info = [_]TensorInfo{
    .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ -1, -1 } },
    .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ -1, 1 } },
};

/// Copy an i64 matrix; request tensors carry no alignment guarantee.
fn matrix(a: std.mem.Allocator, tensor: Tensor, rows: ?usize, columns: ?usize) ![]const i64 {
    if (tensor.dtype != .i64 or tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0) return error.InvalidLayaInputs;
    const r: usize = @intCast(tensor.shape[0]);
    const c: usize = @intCast(tensor.shape[1]);
    if ((rows != null and rows.? != r) or (columns != null and columns.? != c)) return error.InvalidLayaInputs;
    const count = std.math.mul(usize, r, c) catch return error.InvalidLayaInputs;
    if (tensor.data.len != count * @sizeOf(i64)) return error.InvalidLayaInputs;
    const values = try a.alloc(i64, count);
    @memcpy(std.mem.sliceAsBytes(values), tensor.data);
    return values;
}

/// A validated row owned by `a`; free it with `Row.deinit`.
pub fn view(a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor) !tree.Row {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    if (!laya.packing.enabled() or inputs.len != input_count) return error.InvalidLayaInputs;
    const markers_tensor = inputs[5];
    if (markers_tensor.shape.len != 2 or markers_tensor.shape[0] <= 0 or markers_tensor.shape[1] <= 0) return error.InvalidLayaInputs;
    const questions: usize = @intCast(markers_tensor.shape[0]);
    const width: usize = @intCast(markers_tensor.shape[1]);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const ids = try matrix(s, inputs[0], 1, null);
    const index = try s.alloc(usize, questions);
    for (index, 0..) |*value, i| value.* = i;
    const row = tree.Row{
        .ids = ids,
        .positions = try matrix(s, inputs[1], 1, ids.len),
        .segments = try matrix(s, inputs[2], 1, ids.len),
        .parents = try matrix(s, inputs[3], 1, null),
        .kinds = try matrix(s, inputs[4], 1, ids.len),
        .markers = try matrix(s, markers_tensor, questions, width),
        .anchors = try matrix(s, inputs[6], questions, 1),
        .question_index = index,
        .width = width,
    };
    try tree.validate(row, laya.max_len, laya.packing.max_packed_len, laya.maxOptions());
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidLayaInputs;
    return tree.own(a, row);
}

pub fn run(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor) ![]Tensor {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    const row = try view(a, cfg, inputs);
    defer row.deinit(a);
    return forwardRow(cb, a, cfg, laya, row);
}

/// Encoder and decision head for one validated row.
pub fn forwardRow(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: @import("../models/laya.zig").Config, row: tree.Row) ![]Tensor {
    const n = row.ids.len;
    const shape = [_]i32{ @intCast(n), @intCast(n) };
    const global_values = try tree.bias(a, row, null);
    defer a.free(global_values);
    const global = try cb.fromFloat32Shape(global_values, &shape);
    defer cb.free(global);
    const local_values = try tree.bias(a, row, cfg.local_attention_window / 2);
    defer a.free(local_values);
    const local = try cb.fromFloat32Shape(local_values, &shape);
    defer cb.free(local);
    const encoded = try modern.forwardPackedCT(cb, a, cfg, row.ids, .{ .positions = row.positions, .global_bias = global, .local_bias = local });
    defer cb.free(encoded);
    return head.forwardPacked(cb, a, laya, encoded, global, row.kinds, row.markers, row.anchors, row.width, cfg.hidden_size);
}

/// Peak transient bytes for one row: two encoder masks, attention scores for
/// every head, and FFN activations. Packed rows are always batch 1.
pub fn workspaceBytes(cfg: modern.Config, sequence: usize) !usize {
    const mul = std.math.mul;
    const add = std.math.add;
    const square = try mul(usize, sequence, sequence);
    const masks = try mul(usize, square, 2);
    const scores = try mul(usize, square, @max(cfg.num_attention_heads, cfg.hidden_size / 64));
    const activations = try mul(usize, sequence, try add(usize, try mul(usize, cfg.hidden_size, 8), try mul(usize, cfg.intermediate_size, 4)));
    return mul(usize, try add(usize, try add(usize, masks, scores), activations), @sizeOf(f32));
}
