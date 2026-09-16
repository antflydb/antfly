// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded synchronization of a resident optimizer epoch into already owned
//! host mirrors. Only checkpoint/export callers use this path. It never runs
//! in the training step, and a partial readback does not certify a mirror.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Limits = struct {
    chunk_bytes: usize = 1024 * 1024,
    max_scratch_bytes: usize = 2 * 1024 * 1024,
    primitive: ops.resident_training.Limits = .{},
};
pub const Receipt = struct { download_bytes: usize = 0, chunks: usize = 0, scratch_upper_bound_bytes: usize = 0 };

const CombinedControl = struct {
    original: ?Control,
    request: ?Control,
    fn check(raw: ?*anyopaque) !void {
        const self: *CombinedControl = @ptrCast(@alignCast(raw.?));
        if (self.original) |active| try active.check();
        if (self.request) |active| try active.check();
    }
};

pub fn admission(elements: usize, limits: Limits) !Receipt {
    if (elements == 0 or elements > std.math.maxInt(i32) or limits.chunk_bytes < 4 or limits.chunk_bytes > 1024 * 1024 or limits.chunk_bytes % 4 != 0)
        return error.InvalidTrainingSnapshotLimits;
    const bytes = std.math.mul(usize, elements, 4) catch return error.TrainingOptimizerLimitExceeded;
    if (bytes > limits.primitive.max_tensor_bytes) return error.TrainingOptimizerLimitExceeded;
    const scratch = std.math.mul(usize, @min(bytes, limits.chunk_bytes), 2) catch return error.TrainingOptimizerLimitExceeded;
    // One private slice and its synchronous shared-memory download staging.
    // The caller already owns the destination host mirror and source tensor.
    if (scratch > limits.max_scratch_bytes) return error.TrainingOptimizerLimitExceeded;
    return .{ .download_bytes = bytes, .chunks = (bytes - 1) / limits.chunk_bytes + 1, .scratch_upper_bound_bytes = scratch };
}

/// Caller keeps the immutable resident epoch and the destination alive. On
/// failure destination may contain a prefix of the new epoch; its owner must
/// leave the mirror uncertified and retry before any host consumer can run.
pub fn readInto(backend: *const ops.ComputeBackend, tensor: ops.CT, destination: []f32, limits: Limits, control: ?Control) !Receipt {
    const result = try admission(destination.len, limits);
    if (backend.kind() != .metal) return error.UnsupportedSeededTrainingBackend;
    var combined = CombinedControl{ .original = backend.execution_control, .request = control };
    const active = Control{ .ptr = &combined, .check_fn = CombinedControl.check };
    var cb = backend.*;
    cb.execution_control = active;
    try active.check();
    if (try cb.tensorDType(tensor) != .f32) return error.TrainingBindingDTypeMismatch;
    const count: i32 = @intCast(destination.len);
    const flat = try cb.residentTrainingPrimitive(&.{ .reshape = .{ .input = tensor, .shape = &.{count} } }, limits.primitive);
    defer cb.free(flat);
    const input_shape = ml.Shape.init(.f32, &.{count});
    var offset: usize = 0;
    while (offset < destination.len) {
        try active.check();
        const end = offset + @min(destination.len - offset, limits.chunk_bytes / 4);
        var attrs = ml.node.SliceAttrs{ .num_axes = 1 };
        attrs.starts[0] = @intCast(offset);
        attrs.limits[0] = @intCast(end);
        const instruction = ops.resident_program.Instruction{ .op = .{ .slice = attrs }, .output = ml.Shape.init(.f32, &.{@intCast(end - offset)}), .inputs = .{ input_shape, .{}, .{}, .{} }, .num_inputs = 1 };
        const chunk = try cb.residentTrainingInstruction(&instruction, &.{flat}, .{ .primitive = limits.primitive, .max_scratch_bytes = limits.max_scratch_bytes });
        defer cb.free(chunk);
        try cb.glinerBoundaryDownload(chunk, destination[offset..end]);
        try active.check();
        for (destination[offset..end]) |value| if (!std.math.isFinite(value)) return error.NonFiniteTrainingUpdate;
        offset = end;
    }
    return result;
}

test "seeded device snapshot admits bounded slices and rejects invalid resource arithmetic" {
    try std.testing.expectEqual(Receipt{ .download_bytes = 36, .chunks = 3, .scratch_upper_bound_bytes = 32 }, try admission(9, .{ .chunk_bytes = 16, .max_scratch_bytes = 32 }));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, admission(9, .{ .chunk_bytes = 16, .max_scratch_bytes = 31 }));
    try std.testing.expectError(error.InvalidTrainingSnapshotLimits, admission(9, .{ .chunk_bytes = 3 }));
    try std.testing.expectError(error.InvalidTrainingSnapshotLimits, admission(9, .{ .chunk_bytes = 1024 * 1024 + 4 }));
    try std.testing.expectError(error.InvalidTrainingSnapshotLimits, admission(0, .{}));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, admission(100, .{ .primitive = .{ .max_tensor_bytes = 399 } }));
}

test "seeded device snapshot Metal copies bounded chunks and rejects cancellation before mirror publication" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try @import("../graph/resident_training_fixture.zig").Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const values = [_]f32{ 1, -2, 0, 4, 5, -6, 7, 8, 9 };
    const tensor = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &values, .shape = &.{ 3, 3 } } }, .{});
    defer cb.free(tensor);
    var output: [9]f32 = @splat(-999);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, readInto(&cb, tensor, &output, .{ .chunk_bytes = 16, .max_scratch_bytes = 32 }, .{ .check_fn = Cancel.check }));
    for (output) |value| try std.testing.expectEqual(@as(f32, -999), value);
    const receipt = try readInto(&cb, tensor, &output, .{ .chunk_bytes = 16, .max_scratch_bytes = 32 }, null);
    try std.testing.expectEqualSlices(f32, &values, &output);
    try std.testing.expectEqual(@as(usize, 3), receipt.chunks);
    try std.testing.expectEqual(@as(usize, 36), receipt.download_bytes);
}
