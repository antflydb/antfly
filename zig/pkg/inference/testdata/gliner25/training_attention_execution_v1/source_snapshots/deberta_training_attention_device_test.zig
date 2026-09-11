// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ownership and rejection checks for the strict resident attention path.
//! Numerical source-oracle comparisons live in the independent source test.
const std = @import("std");
const build_options = @import("build_options");
const ops = @import("ops.zig");
const metal = @import("metal_compute.zig");
const tensor = @import("../backends/metal_tensor.zig");
const runtime = @import("../backends/metal_runtime.zig");
const Device = @import("../graph/resident_training_fixture.zig").Device;
const attention = @import("deberta_training_attention.zig");
const device_plan = @import("deberta_training_attention_device.zig");

const attrs = attention.Attrs{ .batch = 2, .seq_len = 7, .num_heads = 2, .head_dim = 4, .relative_rows = 5, .dropout_probability = 0.125, .dropout_stream_id = (@as(u64, 7) << 32) | 3 };
const Host = struct {
    qkv: [336]f32,
    relative: [80]f32,
    control: [33]i32,
    dout: [112]f32,

    fn init() Host {
        var result: Host = undefined;
        for (&result.qkv, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(i % 13)) - 6) / 20;
        for (&result.relative, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(i % 11)) - 5) / 21;
        for (&result.dout, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(i % 7)) - 3) / 9;
        result.control[0..6].* = .{ @bitCast(@as(u32, 0x76543210)), @bitCast(@as(u32, 0xfedcba98)), 2, 1, 3, @bitCast(@as(u32, 0x80000000)) };
        for (result.control[6..20], 0..) |*value, i| value.* = @intFromBool(i < 7 and i != 3);
        for (result.control[20..], 0..) |*value, i| value.* = @intCast(i * 3 % 5);
        return result;
    }
};
const Inputs = struct {
    qkv: ops.CT,
    relative: ops.CT,
    control: ops.CT,
    dout: ops.CT,

    fn init(cb: *const ops.ComputeBackend, host: *const Host) !Inputs {
        const qkv = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &host.qkv, .shape = &.{ 42, 8 } } }, .{});
        errdefer cb.free(qkv);
        const relative = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &host.relative, .shape = &.{ 10, 8 } } }, .{});
        errdefer cb.free(relative);
        const control = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &host.control, .shape = &.{33} } }, .{});
        errdefer cb.free(control);
        const dout = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &host.dout, .shape = &.{ 14, 8 } } }, .{});
        return .{ .qkv = qkv, .relative = relative, .control = control, .dout = dout };
    }

    fn deinit(self: Inputs, cb: *const ops.ComputeBackend) void {
        cb.free(self.dout);
        cb.free(self.control);
        cb.free(self.relative);
        cb.free(self.qkv);
    }

    fn backward(self: Inputs, cb: *const ops.ComputeBackend) !ops.CT {
        return cb.debertaTrainingAttentionBackwardV1(self.qkv, self.relative, self.control, self.dout, attrs);
    }
};

fn sameLive(before: tensor.MemoryStats) !void {
    const after = tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_live_bytes, after.device_owned_live_bytes);
    try std.testing.expectEqual(before.host_mirror_live_bytes, after.host_mirror_live_bytes);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(after.device_owned_bytes_created - before.device_owned_bytes_created, after.device_owned_bytes_released - before.device_owned_bytes_released);
}

test "deberta training Metal checked metadata fails before any device call" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const before = tensor.memoryStatsSnapshot();
    // This deliberately invalid runtime pointer must never be dereferenced:
    // the ownership-record allocation fails before the first C/Metal call.
    try std.testing.expectError(error.OutOfMemory, tensor.MetalTensor.deviceAllocateFreshWithAllocator(
        failing.allocator(),
        @ptrFromInt(1),
        4,
        .private,
        &.{1},
    ));
    try sameLive(before);
}

fn checkedView(a: std.mem.Allocator, raw_runtime: *runtime.RawMetalDecodeRuntime) !void {
    var original = try tensor.MetalTensor.deviceAllocateFreshWithAllocator(a, @ptrCast(raw_runtime), 16, .private, &.{4});
    var original_alive = true;
    defer if (original_alive) original.deinit();
    var view = try original.retainedView(4, 8, &.{2});
    defer view.deinit();
    // Reverse the usual lifetime: the final retained view must destroy the
    // ownership record through its allocating owner, after the original dies.
    original.deinit();
    original_alive = false;
}

fn allocationCheck(a: std.mem.Allocator, backend: *metal.MetalCompute, inputs: Inputs) !void {
    const saved = backend.allocator;
    backend.allocator = a;
    defer backend.allocator = saved;
    const cb = backend.computeBackend();
    const output = try inputs.backward(&cb);
    defer cb.free(output);
}

test "deberta training Metal owned metadata views and attention release all allocation failures" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const host = Host.init();
    const inputs = try Inputs.init(&cb, &host);
    defer inputs.deinit(&cb);
    const before = tensor.memoryStatsSnapshot();
    try std.testing.checkAllAllocationFailures(a, checkedView, .{device.backend.provider_impl.raw_decode_runtime.?});
    try sameLive(before);
    try std.testing.checkAllAllocationFailures(a, allocationCheck, .{ device.backend, inputs });
    try sameLive(before);
    const result = try inputs.backward(&cb);
    cb.free(result);
    try sameLive(before);
}

const Checkpoint = struct {
    count: usize = 0,
    cancel_at: usize = std.math.maxInt(usize),
    fn check(raw: ?*anyopaque) !void {
        const self: *Checkpoint = @ptrCast(@alignCast(raw.?));
        self.count += 1;
        if (self.count == self.cancel_at) return error.Cancelled;
    }
};

test "deberta training Metal control finite limits cancellation and frame rejections recover" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var host = Host.init();
    const inputs = try Inputs.init(&cb, &host);
    defer inputs.deinit(&cb);
    const before = tensor.memoryStatsSnapshot();

    var checkpoint = Checkpoint{};
    var controlled = cb;
    controlled.execution_control = .{ .ptr = &checkpoint, .check_fn = Checkpoint.check };
    const completed = try inputs.backward(&controlled);
    controlled.free(completed);
    const checkpoints = checkpoint.count;
    try std.testing.expect(checkpoints > 6);
    for ([_]usize{ 1, checkpoints / 2, checkpoints }) |stop| {
        checkpoint = .{ .cancel_at = stop };
        try std.testing.expectError(error.Cancelled, inputs.backward(&controlled));
        try sameLive(before);
    }

    const layout = try attrs.layout();
    const instruction = ops.resident_program.Instruction{
        .op = .{ .fused_deberta_training_attention_backward_v1 = attrs },
        .output = layout.gradientShape(),
        .inputs = .{ layout.qkvShape(), layout.relativeShape(), layout.controlShape(), layout.outputShape() },
        .num_inputs = 4,
    };
    const planned = try device_plan.plan(attrs, true, .{});
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingInstruction(&instruction, &.{ inputs.qkv, inputs.relative, inputs.control, inputs.dout }, .{ .max_scratch_bytes = planned.scratch_bytes - 1 }));
    try sameLive(before);
    {
        try runtime.beginFrame(device.backend.provider_impl.raw_decode_runtime);
        defer runtime.cancelFrame(device.backend.provider_impl.raw_decode_runtime) catch {};
        try std.testing.expectError(error.ResidentTrainingExternalFrame, inputs.backward(&cb));
    }
    try sameLive(before);

    for ([_]usize{ 6, 20 }) |bad_index| {
        const saved = host.control[bad_index];
        host.control[bad_index] = if (bad_index == 6) 2 else @intCast(attrs.relative_rows);
        const invalid = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &host.control, .shape = &.{33} } }, .{});
        host.control[bad_index] = saved;
        defer cb.free(invalid);
        const invalid_before = tensor.memoryStatsSnapshot();
        try std.testing.expectError(error.InvalidDebertaTrainingAttentionControl, cb.debertaTrainingAttentionBackwardV1(inputs.qkv, inputs.relative, invalid, inputs.dout, attrs));
        try sameLive(invalid_before);
    }
    host.qkv[0] = std.math.inf(f32);
    const nonfinite = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &host.qkv, .shape = &.{ 42, 8 } } }, .{});
    defer cb.free(nonfinite);
    const nonfinite_before = tensor.memoryStatsSnapshot();
    try std.testing.expectError(error.NonFiniteDebertaTrainingAttention, cb.debertaTrainingAttentionBackwardV1(nonfinite, inputs.relative, inputs.control, inputs.dout, attrs));
    try sameLive(nonfinite_before);
    const retry = try inputs.backward(&cb);
    cb.free(retry);
    try sameLive(nonfinite_before);
}
