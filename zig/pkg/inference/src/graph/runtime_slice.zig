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

const std = @import("std");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");

const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const NodeId = ml.graph.NodeId;

fn runtimeBoundValue(value: f32) !i64 {
    if (std.math.isNan(value)) return error.InvalidTensorShape;
    const max_safe: f32 = @floatFromInt(@as(i64, std.math.maxInt(i64)));
    const min_safe: f32 = @floatFromInt(@as(i64, std.math.minInt(i64)));
    if (value >= max_safe) return std.math.maxInt(i64);
    if (value <= min_safe) return std.math.minInt(i64);
    return @intFromFloat(@round(value));
}

fn applyRuntimeBounds(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    values: []const ?CT,
    value_id: NodeId,
    axes: []const u8,
    rank: usize,
    out: []i64,
) !void {
    const index: usize = @intCast(value_id);
    if (index >= values.len) return error.MissingRuntimeInput;
    const value = values[index] orelse return error.MissingRuntimeInput;
    const raw = try cb.toFloat32(value, allocator);
    defer allocator.free(raw);
    if (raw.len != axes.len) return error.InvalidTensorShape;
    for (axes, raw) |axis, bound| {
        if (axis >= rank) return error.InvalidTensorShape;
        out[axis] = try runtimeBoundValue(bound);
    }
}

/// Resolve a SliceAttrs fallback plus optional ONNX runtime starts/limits.
/// Static graph-native slices have one input and never perform host reads.
pub fn resolve(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    values: []const ?CT,
    inputs: []const NodeId,
    attrs: ml.graph.node.SliceAttrs,
    starts: []i64,
    limits: []i64,
    strides: []i64,
) !void {
    const rank: usize = attrs.num_axes;
    if (rank > starts.len or rank > limits.len or rank > strides.len) return error.UnsupportedShape;
    for (0..rank) |axis| {
        starts[axis] = attrs.starts[axis];
        limits[axis] = attrs.limits[axis];
        strides[axis] = attrs.strides[axis];
    }

    if (!attrs.runtime_starts and !attrs.runtime_limits) return;
    if (inputs.len < 3 or attrs.num_bound_axes == 0) return error.MissingRuntimeInput;
    const axes = attrs.bound_axes[0..attrs.num_bound_axes];
    if (attrs.runtime_starts) {
        try applyRuntimeBounds(allocator, cb, values, inputs[1], axes, rank, starts);
    }
    if (attrs.runtime_limits) {
        try applyRuntimeBounds(allocator, cb, values, inputs[2], axes, rank, limits);
    }
}
