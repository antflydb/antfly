// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Direct BF16 attention projections, with one shared input conversion.
const std = @import("std");
const Buffer = @import("buffer.zig").DeviceBuffer;
const Context = @import("context.zig").CudaContext;
const Lt = @import("cublaslt.zig").CublasLt;
const Kernels = @import("kernels.zig").KernelModule;
pub const Plan = struct { input_bytes: usize, bias_stride: usize, temporary_bytes: usize, output_bytes: usize };
pub fn plan(rows: usize, hidden: usize, projections: usize) ?Plan {
    if (rows == 0 or rows > 4096 or (hidden != 768 and hidden != 1024 and hidden != 64) or (projections != 2 and projections != 3)) return null;
    const input = rows * hidden * 2;
    const bias = std.mem.alignForward(usize, hidden * 2, 256);
    return .{ .input_bytes = input, .bias_stride = bias, .temporary_bytes = input + projections * bias, .output_bytes = input };
}
fn check(b: Buffer, bytes: usize) !void {
    if (b.ptr == 0 or b.len < bytes) return error.InvalidCudaState;
}
pub fn needsTemporary(native_input: Buffer, cached_biases: [3]Buffer, projections: usize) bool {
    if (native_input.ptr == 0 or projections < 2 or projections > 3) return true;
    for (cached_biases[0..projections]) |bias| if (bias.ptr == 0) return true;
    return false;
}
// Serving plans use heuristics on first use; exact row counts must not
// trigger synchronous GPU autotuning while the model execution lane is held.
pub fn run(ctx: *Context, lt: *Lt, kernels: *Kernels, workspace: Buffer, outputs: [3]Buffer, temporary: Buffer, input: Buffer, weights: [3]Buffer, biases: [3]Buffer, cached_biases: [3]Buffer, native_input: Buffer, rows: usize, hidden: usize, projections: usize) !void {
    const p = plan(rows, hidden, projections) orelse return error.InvalidCudaState;
    try check(input, p.input_bytes * 2);
    if (needsTemporary(native_input, cached_biases, projections)) try check(temporary, p.temporary_bytes);
    const staged = if (native_input.ptr != 0) native_input else Buffer{ .ptr = temporary.ptr, .len = p.input_bytes };
    try check(staged, p.input_bytes);
    if (native_input.ptr == 0) try kernels.launchF32ToBf16(ctx, staged, input, rows * hidden);
    for (0..projections) |i| {
        try check(weights[i], hidden * hidden * 2);
        try check(biases[i], hidden * 4);
        try check(outputs[i], p.output_bytes);
        const bias = if (cached_biases[i].ptr != 0) cached_biases[i] else Buffer{ .ptr = temporary.ptr + p.input_bytes + i * p.bias_stride, .len = hidden * 2 };
        try check(bias, hidden * 2);
        if (cached_biases[i].ptr == 0) try kernels.launchF32ToBf16(ctx, bias, biases[i], hidden);
        _ = try lt.matmulBf16WeightBiasBf16OutWithTuning(ctx, outputs[i], staged, weights[i], bias, workspace, rows, hidden, hidden, .{});
    }
}
fn round(x: f32) f32 {
    const bits: u32 = @bitCast(x);
    return @bitCast((bits + 0x7fff + ((bits >> 16) & 1)) & 0xffff0000);
}
test "GLiNER BF16 projections share input and match rounded CPU oracle" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var kernels = try Kernels.load(&ctx);
    defer kernels.unload(&ctx);
    var lt = try Lt.openWithAllocator(a, &ctx);
    defer lt.deinit();
    // Force misses after cache saturation as well as new ragged row counts.
    try lt.setPlanCacheCapacity(2);
    const rows = 128;
    const hidden = 64;
    const host = try a.alloc(f32, rows * hidden);
    defer a.free(host);
    for (host, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i % 133)) * 0.11);
    var input = try Buffer.alloc(&ctx, host.len * 4);
    defer input.free(&ctx);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(host));
    var weights: [3]Buffer = @splat(.{});
    var biases: [3]Buffer = @splat(.{});
    var outputs: [3]Buffer = @splat(.{});
    defer for (&weights) |*v| v.free(&ctx);
    defer for (&biases) |*v| v.free(&ctx);
    defer for (&outputs) |*v| v.free(&ctx);
    for (0..3) |i| {
        var matrix: [hidden * hidden]u16 = @splat(0);
        for (0..hidden) |j| matrix[j * hidden + (j + i) % hidden] = @intCast(@as(u32, @bitCast(@as(f32, 0.25))) >> 16);
        const bias: [hidden]f32 = @splat(@as(f32, @floatFromInt(i + 1)) * 0.013);
        weights[i] = try Buffer.alloc(&ctx, matrix.len * 2);
        biases[i] = try Buffer.alloc(&ctx, hidden * 4);
        outputs[i] = try Buffer.alloc(&ctx, host.len * 2);
        try weights[i].copyFromHost(&ctx, std.mem.sliceAsBytes(&matrix));
        try biases[i].copyFromHost(&ctx, std.mem.sliceAsBytes(&bias));
    }
    var temporary = try Buffer.alloc(&ctx, plan(rows, hidden, 3).?.temporary_bytes);
    defer temporary.free(&ctx);
    var workspace = try Buffer.alloc(&ctx, @import("gliner25_bf16_attention.zig").workspace_limit);
    defer workspace.free(&ctx);
    const result = try a.alloc(u16, host.len);
    defer a.free(result);
    for ([_]usize{ 1, 53, 127, 128 }) |n| for ([_]usize{ 2, 3 }) |count| {
        try run(&ctx, &lt, &kernels, workspace, outputs, temporary, input, weights, biases, @splat(.{}), .{}, n, hidden, count);
        for (0..count) |i| {
            try outputs[i].copyToHost(&ctx, std.mem.sliceAsBytes(result[0 .. n * hidden]));
            try ctx.synchronize();
            for (result[0 .. n * hidden], 0..) |bits, j| {
                const actual: f32 = @bitCast(@as(u32, bits) << 16);
                const expected = round(round(host[(j / hidden) * hidden + (j + i) % hidden]) * 0.25 + round(@as(f32, @floatFromInt(i + 1)) * 0.013));
                try std.testing.expectEqual(expected, actual);
            }
        }
        // A published norm mirror and cached biases need no temporary at
        // all. Exercise every projection count and ragged row count above.
        const p = plan(n, hidden, count).?;
        var cached: [3]Buffer = @splat(.{});
        for (0..count) |i| cached[i] = .{ .ptr = temporary.ptr + p.input_bytes + i * p.bias_stride, .len = hidden * 2 };
        const staged_input = Buffer{ .ptr = temporary.ptr, .len = p.input_bytes };
        try std.testing.expect(!needsTemporary(staged_input, cached, count));
        const baseline = try a.alloc(u16, n * hidden * count);
        defer a.free(baseline);
        for (0..count) |i| try outputs[i].copyToHost(&ctx, std.mem.sliceAsBytes(baseline[i * n * hidden ..][0 .. n * hidden]));
        try ctx.synchronize();
        try run(&ctx, &lt, &kernels, workspace, outputs, .{}, input, weights, biases, cached, staged_input, n, hidden, count);
        for (0..count) |i| {
            try outputs[i].copyToHost(&ctx, std.mem.sliceAsBytes(result[0 .. n * hidden]));
            try ctx.synchronize();
            try std.testing.expectEqualSlices(u16, baseline[i * n * hidden ..][0 .. n * hidden], result[0 .. n * hidden]);
        }
    };
    // Classifier and scalar boundary heads use a narrow F32 output, with
    // non-tile-sized row counts and the original F32 bias epilogue.
    var scalar_output = try Buffer.alloc(&ctx, rows * 4);
    defer scalar_output.free(&ctx);
    const staged = Buffer{ .ptr = temporary.ptr, .len = rows * hidden * 2 };
    const scalar_values = try a.alloc(f32, rows);
    defer a.free(scalar_values);
    for ([_]usize{ 1, 14, 112 }) |n| {
        try lt.matmulBf16WeightBiasF32Out(&ctx, scalar_output, staged, weights[0], biases[0], workspace, n, hidden, 1);
        try scalar_output.copyToHost(&ctx, std.mem.sliceAsBytes(scalar_values[0..n]));
        try ctx.synchronize();
        for (scalar_values[0..n], 0..) |value, j| try std.testing.expectApproxEqAbs(round(host[j * hidden]) * 0.25 + 0.013, value, 1e-6);
    }
    try std.testing.expect(plan(129, hidden, 3) != null);
    try std.testing.expect(plan(4097, hidden, 3) == null);
    try std.testing.expect(plan(rows, hidden, 1) == null);
    try std.testing.expect(lt.plan_cache_misses > 2);
    try std.testing.expect(lt.uncached_plans > 0);
    try std.testing.expectEqual(@as(usize, 0), lt.matmul_tuning_searches);
}
