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

const WidthMask = enum { valid, ragged, fully_masked };

fn widthClose(expected: []const f32, actual: []const f32, config: attention.Attrs, mask: WidthMask, family: []const u8) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |want, got, index| {
        if (!std.math.isFinite(got) or @abs(want - got) > 2e-5 + 3e-5 * @abs(want)) {
            std.debug.print("attention CPU/Metal width={d} sequence={d} p={d} mask={s} {s}[{d}]: expected={d} actual={d}\n", .{
                config.head_dim, config.seq_len, config.dropout_probability, @tagName(mask), family, index, want, got,
            });
            return error.TestExpectedApproxEqAbs;
        }
    }
}

fn widthCase(a: std.mem.Allocator, cb: *const ops.ComputeBackend, dimension: u32, sequence: u32, probability: f32, mask: WidthMask) !void {
    const config = attention.Attrs{
        .batch = 1,
        .seq_len = sequence,
        .num_heads = 2,
        .head_dim = dimension,
        .relative_rows = 512,
        .dropout_probability = probability,
        .dropout_stream_id = (@as(u64, 7) << 32) | 3,
    };
    // Deliberately use CPU tiles different from Metal's fixed key tile. The
    // CPU primitive has separate scalar, finite-difference and source proof;
    // these additional widths are CPU/Metal parity, not new source captures.
    const options = attention.Options{ .limits = .{ .query_tile = 17, .key_tile = 31, .max_scratch_bytes = 4 * 1024 * 1024 } };
    const cpu_plan = try attention.plan(config, options.limits);
    const gpu_plan = try device_plan.plan(config, true, .{
        .max_tensor_bytes = 4 * 1024 * 1024,
        .max_scratch_bytes = 4 * 1024 * 1024,
        .max_host_metadata_bytes = 4 * 1024 * 1024,
        .max_work_items = 256 * 1024 * 1024,
    });
    // Both results coexist for the final diagnostics, alongside four inputs.
    const device_bound = cpu_plan.input_bytes + 2 * cpu_plan.output_bytes + gpu_plan.output_bytes + gpu_plan.scratch_bytes;
    try std.testing.expect(device_bound < 16 * 1024 * 1024);
    const qkv_values = try a.alloc(f32, cpu_plan.qkv_elements);
    defer a.free(qkv_values);
    const relative_values = try a.alloc(f32, cpu_plan.relative_elements);
    defer a.free(relative_values);
    const control_values = try a.alloc(i32, cpu_plan.control_elements);
    defer a.free(control_values);
    const dout_values = try a.alloc(f32, cpu_plan.output_elements);
    defer a.free(dout_values);
    for (qkv_values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i % 997)) * 0.23 + 0.1) * 0.31;
    for (relative_values, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i % 991)) * 0.37 + 0.2) * 0.21;
    for (dout_values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i % 983)) * 0.41 + 0.3) * 0.39;
    control_values[0..6].* = .{ @bitCast(@as(u32, 0x76543210)), @bitCast(@as(u32, 0xfedcba98)), 2, 1, 3, @bitCast(@as(u32, 0x80000000)) };
    for (control_values[6..][0..cpu_plan.batch_tokens], 0..) |*value, i| value.* = @intFromBool(switch (mask) {
        .valid => true,
        .ragged => i + 2 < sequence and i % 5 != 2,
        .fully_masked => false,
    });
    for (control_values[6 + cpu_plan.batch_tokens ..], 0..) |*value, i|
        value.* = @intCast(@import("../models/deberta.zig").relativePositionBucket(@as(i64, @intCast(i)) - (@as(i64, sequence) - 1), 256, 512));
    const expected_output = try attention.forward(a, config, qkv_values, relative_values, control_values, options);
    defer a.free(expected_output);
    const expected_gradient = try attention.backward(a, config, qkv_values, relative_values, control_values, dout_values, options);
    defer a.free(expected_gradient);
    const h: i32 = @intCast(cpu_plan.hidden);
    const s: i32 = @intCast(sequence);
    const qkv = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = qkv_values, .shape = &.{ 3 * s, h } } }, .{});
    defer cb.free(qkv);
    const projected = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = relative_values, .shape = &.{ 1024, h } } }, .{});
    defer cb.free(projected);
    const controls = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = control_values, .shape = &.{@intCast(control_values.len)} } }, .{});
    defer cb.free(controls);
    const dout = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = dout_values, .shape = &.{ s, h } } }, .{});
    defer cb.free(dout);
    const before = tensor.memoryStatsSnapshot();
    const output = try cb.debertaTrainingAttentionV1(qkv, projected, controls, config);
    defer cb.free(output);
    const gradient = try cb.debertaTrainingAttentionBackwardV1(qkv, projected, controls, dout, config);
    defer cb.free(gradient);
    const after = tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expect(after.device_owned_live_bytes <= before.device_owned_live_bytes + cpu_plan.output_bytes + gpu_plan.output_bytes);
    const actual_output = try a.alloc(f32, expected_output.len);
    defer a.free(actual_output);
    const actual_gradient = try a.alloc(f32, expected_gradient.len);
    defer a.free(actual_gradient);
    try cb.glinerBoundaryDownload(output, actual_output);
    try cb.glinerBoundaryDownload(gradient, actual_gradient);
    try widthClose(expected_output, actual_output, config, mask, "context");
    var offset: usize = 0;
    var masked_dv_nonzero = false;
    for ([_][]const u8{ "q", "k", "v", "qr", "kr" }, 0..) |family, component| {
        const count = if (component < 3) cpu_plan.output_elements else cpu_plan.relative_elements / 2;
        const actual = actual_gradient[offset..][0..count];
        try widthClose(expected_gradient[offset..][0..count], actual, config, mask, family);
        if (mask == .fully_masked) {
            if (component == 2) {
                for (actual) |value| masked_dv_nonzero = masked_dv_nonzero or value != 0;
            } else for (actual) |value| try std.testing.expectEqual(@as(f32, 0), value);
        }
        offset += count;
    }
    if (mask == .fully_masked and sequence > 1) try std.testing.expect(masked_dv_nonzero);
    try std.testing.expectEqual(actual_gradient.len, offset);
}

test "deberta training Metal published width and wider tails match CPU five VJPs" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var owner = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.testing.allocator, .limit = 64 * 1024 * 1024 };
    const a = owner.allocator();
    const initial = tensor.memoryStatsSnapshot();
    {
        var device = try Device.init(a);
        defer device.deinit();
        const cb = device.backend.computeBackend();
        try std.testing.expect(cb.kind() == .metal);
        for ([_]u32{ 64, 128, 256 }) |dimension| {
            for ([_]u32{ 1, 17, 65 }) |sequence| {
                for ([_]f32{ 0, 0.1 }) |probability| {
                    const mask: WidthMask = if (probability != 0 and sequence <= 17) .fully_masked else if (sequence == 1) .valid else .ragged;
                    const before = tensor.memoryStatsSnapshot();
                    try widthCase(a, &cb, dimension, sequence, probability, mask);
                    try sameLive(before);
                }
            }
        }
    }
    try sameLive(initial);
    try std.testing.expectEqual(@as(usize, 0), owner.live);
    try std.testing.expect(owner.peak <= owner.limit and !owner.denied);
}
