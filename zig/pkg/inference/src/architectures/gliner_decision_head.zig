// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Device-resident classification head for GLiNER2.5-Decide.
//!
//! The caller supplies the contextual encoder states and explicit `[L]` marker
//! positions. Only the final scalar logits leave the compute backend.

const std = @import("std");
const ops = @import("../ops/ops.zig");
const tensor = @import("../backends/tensor.zig");

const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

pub const Result = struct {
    logits: CT, // [batch * labels, 1]
    batch: usize,
    labels: usize,
};

fn checkedMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidInputShape;
}

fn requireWeight(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
) !CT {
    const weight = try cb.getWeight(name);
    errdefer cb.free(weight);
    const actual = try cb.tensorShape(weight, allocator);
    defer allocator.free(actual);
    if (!std.mem.eql(i64, actual, shape)) return error.InvalidGlinerDecisionWeightShape;
    switch (try cb.tensorDType(weight)) {
        .f32, .f16, .bf16 => {},
        else => return error.UnsupportedGlinerDecisionWeightType,
    }
    return weight;
}

/// Gather contextual label-marker states, then run the published scalar MLP:
/// `classifier.0 -> ReLU -> classifier.2`.
pub fn forwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    marker_positions: []const i64,
    marker_mask: []const i64,
    batch: usize,
    seq_len: usize,
    labels: usize,
    hidden_size: usize,
) !Result {
    if (batch == 0 or seq_len == 0 or labels == 0 or hidden_size == 0)
        return error.InvalidInputShape;
    const rows = try checkedMul(batch, labels);
    if (marker_positions.len != rows or marker_mask.len != rows)
        return error.InvalidInputShape;

    const row_ids = try allocator.alloc(u32, rows);
    defer allocator.free(row_ids);
    const fallback_ids = try allocator.alloc(i64, rows);
    defer allocator.free(fallback_ids);
    for (marker_positions, marker_mask, row_ids, fallback_ids, 0..) |position, mask, *row_id, *fallback, index| {
        if (mask != 0 and mask != 1) return error.InvalidGlinerDecisionMarkerMask;
        if (mask == 1 and (position < 0 or position >= seq_len))
            return error.InvalidGlinerDecisionMarkerPosition;
        const local: usize = if (mask == 1) @intCast(position) else 0;
        const global = try std.math.add(usize, try checkedMul(index / labels, seq_len), local);
        row_id.* = std.math.cast(u32, global) orelse return error.InvalidInputShape;
        fallback.* = @intCast(global);
    }

    const gathered = if (try cb.takeRows(hidden, row_ids, rows, hidden_size)) |value|
        value
    else
        try cb.embeddingLookup(hidden, fallback_ids, rows, hidden_size);
    defer cb.free(gathered);

    const hidden2 = try checkedMul(hidden_size, 2);
    const w1 = try requireWeight(cb, allocator, "classifier.0.weight", &.{ @intCast(hidden2), @intCast(hidden_size) });
    defer cb.free(w1);
    const b1 = try requireWeight(cb, allocator, "classifier.0.bias", &.{@intCast(hidden2)});
    defer cb.free(b1);
    const w2 = try requireWeight(cb, allocator, "classifier.2.weight", &.{ 1, @intCast(hidden2) });
    defer cb.free(w2);
    const b2 = try requireWeight(cb, allocator, "classifier.2.bias", &.{1});
    defer cb.free(b2);

    const activated = if (try cb.linearRelu(gathered, w1, b1, rows, hidden_size, hidden2)) |value|
        value
    else blk: {
        const projected = try cb.linear(gathered, w1, b1, rows, hidden_size, hidden2);
        defer cb.free(projected);
        break :blk try cb.relu(projected);
    };
    defer cb.free(activated);
    return .{
        .logits = try cb.linear(activated, w2, b2, rows, hidden2, 1),
        .batch = batch,
        .labels = labels,
    };
}

const Fixture = struct {
    const native = @import("../ops/native_compute.zig");
    const Tensor = tensor.Tensor;

    fn put(store: *native.WeightStore, name: []const u8, shape: []const i64, values: []const f32) !void {
        const a = store.allocator;
        const key = try a.dupe(u8, name);
        errdefer a.free(key);
        var value = try Tensor.initFloat32(a, key, shape, values);
        errdefer value.deinit();
        try store.resident_weights.put(a, key, .{ .tensor = value });
    }

    fn init(a: std.mem.Allocator) !native.WeightStore {
        var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        errdefer store.deinitOwned();
        // H=2, hidden=4. First layer copies positive inputs twice; second sums.
        try put(&store, "classifier.0.weight", &.{ 4, 2 }, &.{ 1, 0, 0, 1, 1, 0, 0, 1 });
        try put(&store, "classifier.0.bias", &.{4}, &.{ 0, 0, 0, 0 });
        try put(&store, "classifier.2.weight", &.{ 1, 4 }, &.{ 1, 1, 1, 1 });
        try put(&store, "classifier.2.bias", &.{1}, &.{1});
        return store;
    }
};

test "GLiNER decision head gathers batch-local markers and executes classifier.2" {
    const a = std.testing.allocator;
    var store = try Fixture.init(a);
    defer store.deinitOwned();
    var compute = Fixture.native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const hidden_values = [_]f32{
        1, 2, 3, 4,  5,  6,
        7, 8, 9, 10, 11, 12,
    };
    const hidden = try cb.fromFloat32Shape(&hidden_values, &.{ 6, 2 });
    defer cb.free(hidden);
    const result = try forwardCt(&cb, a, hidden, &.{ 2, 0, 1, 0 }, &.{ 1, 0, 1, 1 }, 2, 3, 2, 2);
    defer cb.free(result.logits);
    const logits = try cb.toFloat32(result.logits, a);
    defer a.free(logits);
    try std.testing.expectEqualSlices(f32, &.{ 23, 7, 39, 31 }, logits);
}

test "GLiNER decision head rejects active out-of-range markers" {
    const a = std.testing.allocator;
    var store = try Fixture.init(a);
    defer store.deinitOwned();
    var compute = Fixture.native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const hidden = try cb.fromFloat32Shape(&.{ 1, 2 }, &.{ 1, 2 });
    defer cb.free(hidden);
    try std.testing.expectError(error.InvalidGlinerDecisionMarkerPosition, forwardCt(&cb, a, hidden, &.{1}, &.{1}, 1, 1, 1, 2));
}

test "GLiNER decision head validates published classifier shapes" {
    const a = std.testing.allocator;
    var store = try Fixture.init(a);
    defer store.deinitOwned();
    const entry = store.resident_weights.getPtr("classifier.2.weight").?;
    entry.tensor.deinit();
    entry.tensor = try Fixture.Tensor.initFloat32(a, "classifier.2.weight", &.{ 2, 2 }, &.{ 1, 1, 1, 1 });
    var compute = Fixture.native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const hidden = try cb.fromFloat32Shape(&.{ 1, 2 }, &.{ 1, 2 });
    defer cb.free(hidden);
    try std.testing.expectError(error.InvalidGlinerDecisionWeightShape, forwardCt(&cb, a, hidden, &.{0}, &.{1}, 1, 1, 1, 2));
}
