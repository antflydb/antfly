// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause

//! Static FP32 reduction geometry matching PyTorch 2.9.1 Reduce.cuh.
//! Pure host planning: admission and CUDA dispatch use the same checked result.
//! Inputs are dense row-major tensors; no implicit strided-view policy exists.
const std = @import("std");

pub const Device = struct {
    multiprocessors: u32,
    max_threads_per_multiprocessor: u32,

    pub fn validate(self: Device) !void {
        if (self.multiprocessors == 0 or self.multiprocessors > 1024 or
            self.max_threads_per_multiprocessor < 512 or self.max_threads_per_multiprocessor > 4096)
            return error.InvalidCudaReductionDevice;
    }
};

/// By-value kernel ABI. Keep in sync with Gliner25ReduceConfig in the artifact.
pub const Config = extern struct {
    rank: u32 = 0,
    mask: u32 = 0,
    dims: [8]u32 = @splat(1),
    inputs: u32 = 1,
    outputs: u32 = 1,
    step_input: u32 = 1,
    step_output: u32 = 1,
    input_x: u32 = 0,
    input_y: u32 = 0,
    input_cta: u32 = 0,
    output_x: u32 = 0,
    output_y: u32 = 0,
    vector_input: u32 = 0,
    vector_output: u32 = 1,
    ctas: u32 = 1,
    red_contiguous: u32 = 0,
    red_stride: u32 = 1,
    factor: f32 = 1,
};

pub const Plan = struct {
    config: Config,
    block: [2]u32,
    grid: u32,
    scratch_bytes: usize,

    pub fn init(shape: []const i64, axes: []const u8, mean: bool, device: Device) !Plan {
        try device.validate();
        if (shape.len == 0 or shape.len > 8 or axes.len == 0 or axes.len > shape.len)
            return error.InvalidResidentProgramShape;
        var c = Config{ .rank = @intCast(shape.len) };
        for (axes) |axis| {
            if (axis >= shape.len) return error.InvalidResidentProgramShape;
            const bit = @as(u32, 1) << @intCast(axis);
            if (c.mask & bit != 0) return error.InvalidResidentProgramShape;
            c.mask |= bit;
        }
        var total: u64 = 1;
        var effective: [8]u8 = undefined;
        var n: usize = 0;
        for (shape, 0..) |dim, i| {
            if (dim <= 0 or dim > std.math.maxInt(i32)) return error.InvalidResidentProgramShape;
            total = try std.math.mul(u64, total, @intCast(dim));
            if (total > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
            c.dims[i] = @intCast(dim);
            if (reduced(c, i)) c.inputs *= @intCast(dim) else c.outputs *= @intCast(dim);
            if (dim > 1) {
                effective[n] = @intCast(i);
                n += 1;
            }
        }
        // TensorIterator coalesces adjacent dimensions of the same kind and
        // removes size-one dimensions before choosing input/output vectorization.
        var groups: u32 = 0;
        var prior_reduced = false;
        var last_reduced: usize = shape.len;
        for (effective[0..n]) |axis| {
            const is_reduced = reduced(c, axis);
            if (is_reduced) {
                if (!prior_reduced) groups += 1;
                last_reduced = axis;
            }
            prior_reduced = is_reduced;
        }
        c.red_contiguous = @intFromBool(groups <= 1);
        if (last_reduced < shape.len) for (c.dims[last_reduced + 1 .. shape.len]) |dim| {
            c.red_stride *= dim;
        };
        const fast = n == 0 or reduced(c, effective[n - 1]);
        var dim0 = if (fast) c.inputs else c.outputs;
        const dim1 = if (fast) c.outputs else c.inputs;
        if (fast and c.inputs > 128 and groups <= 1) {
            c.vector_input = 1;
            dim0 /= 4;
        } else if (!fast) {
            var run: u32 = 1;
            var i = n;
            while (i > 0) {
                i -= 1;
                const axis = effective[i];
                if (reduced(c, axis)) break;
                run *= c.dims[axis];
            }
            c.vector_output = 4;
            while (run % c.vector_output != 0) c.vector_output /= 2;
            dim0 /= c.vector_output;
        }
        const max_threads = 512 / c.vector_output;
        const pow0 = floorPower(@min(dim0, max_threads));
        const pow1 = floorPower(@min(dim1, max_threads));
        var bx: u32 = @min(pow0, 32);
        const by = @min(pow1, max_threads / bx);
        bx = @min(pow0, max_threads / by);
        if (fast) {
            c.input_x = c.step_input;
            c.step_input *= bx;
        } else {
            c.output_x = c.step_output;
            c.step_output *= bx;
        }
        if (ceilDiv(c.inputs, c.step_input) >= @min(by * 16, 256)) {
            c.input_y = c.step_input;
            c.step_input *= by;
        } else {
            c.output_y = c.step_output;
            c.step_output *= by;
        }
        const grid = ceilDiv(c.outputs / c.vector_output, c.step_output);
        const target = device.multiprocessors * (device.max_threads_per_multiprocessor / (bx * by));
        const vpt = ceilDiv(c.inputs, c.step_input);
        if (c.input_y != 0 and vpt >= 256 and grid <= target) {
            c.ctas = @max(@min(ceilDiv(target, grid), ceilDiv(vpt, 16)), ceilDiv(vpt, 256));
            if (c.ctas > 1) {
                c.input_cta = c.step_input;
                c.step_input = try std.math.mul(u32, c.step_input, c.ctas);
            }
        }
        // A separate finishing launch preserves the reference's fixed CTA
        // index order without semaphores, atomics or persistent module scratch.
        const staging = if (c.ctas == 1) 0 else try std.math.mul(usize, try std.math.mul(usize, grid, c.ctas), (if (c.input_x != 0) @as(usize, 1) else bx) * c.vector_output * 4);
        if (mean) c.factor = @as(f32, @floatFromInt(c.outputs)) / @as(f32, @floatFromInt(total));
        return .{ .config = c, .block = .{ bx, by }, .grid = grid, .scratch_bytes = staging };
    }
};

fn reduced(c: Config, axis: usize) bool {
    return c.mask & (@as(u32, 1) << @intCast(axis)) != 0;
}
fn floorPower(n: u32) u32 {
    return @as(u32, 1) << @intCast(std.math.log2_int(u32, n));
}
fn ceilDiv(n: u32, d: u32) u32 {
    return n / d + @intFromBool(n % d != 0);
}

test "CUDA reference reduction plans cover vectorization global staging and invalid geometry" {
    const device = Device{ .multiprocessors = 58, .max_threads_per_multiprocessor = 1536 };
    const inner = try Plan.init(&.{ 384, 128 }, &.{1}, false, device);
    try std.testing.expectEqualSlices(u32, &.{ 32, 16 }, &inner.block);
    try std.testing.expectEqual(@as(usize, 0), inner.scratch_bytes);
    const outer = try Plan.init(&.{ 118, 768 }, &.{0}, false, device);
    try std.testing.expectEqual(@as(u32, 4), outer.config.vector_output);
    const global = try Plan.init(&.{ 131072, 8 }, &.{0}, true, device);
    try std.testing.expect(global.config.ctas > 1);
    try std.testing.expect(global.scratch_bytes > 0);
    try std.testing.expectEqual(@as(u32, 0), global.config.input_x);
    try std.testing.expectEqual(@as(f32, 1.0 / 131072.0), global.config.factor);
    const disjoint = try Plan.init(&.{ 2, 3, 5, 7 }, &.{ 0, 2 }, false, device);
    try std.testing.expectEqual(@as(u32, 0), disjoint.config.red_contiguous);
    const scalar = try Plan.init(&.{ 1, 3, 1, 7, 1 }, &.{ 1, 3 }, false, device);
    try std.testing.expectEqual(@as(u32, 1), scalar.config.red_contiguous);
    try std.testing.expectError(error.InvalidResidentProgramShape, Plan.init(&.{ 2, 3 }, &.{ 0, 0 }, false, device));
    try std.testing.expectError(error.InvalidResidentProgramShape, Plan.init(&.{ 2, 0 }, &.{0}, false, device));
    try std.testing.expectError(error.ResourceLimitExceeded, Plan.init(&.{ 65536, 65536 }, &.{0}, false, device));
    try std.testing.expectError(error.InvalidCudaReductionDevice, Plan.init(&.{2}, &.{0}, false, .{ .multiprocessors = 0, .max_threads_per_multiprocessor = 1536 }));
}
