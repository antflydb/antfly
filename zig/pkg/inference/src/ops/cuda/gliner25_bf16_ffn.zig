// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! BF16 FFN intermediates, with F32 bias, residual and LayerNorm boundaries.
const std = @import("std");
const Buffer = @import("buffer.zig").DeviceBuffer;
const Context = @import("context.zig").CudaContext;
const Lt = @import("cublaslt.zig").CublasLt;
const Kernels = @import("kernels.zig").KernelModule;
pub const Plan = struct { staged: usize, wide: usize, narrow: usize, temporary_bytes: usize, gemm_output_bytes: usize };
pub fn plan(rows: usize, hidden: usize, intermediate: usize) ?Plan {
    return planWithFloatDot(rows, hidden, intermediate, false);
}
pub fn planWithFloatDot(rows: usize, hidden: usize, intermediate: usize, float_dot: bool) ?Plan {
    if (rows == 0 or rows > 4096 or hidden < 64 or hidden > 1024 or hidden % 64 != 0 or intermediate != 4 * hidden) return null;
    const staged = rows * hidden * 2;
    const wide = rows * intermediate * 2;
    const narrow = rows * hidden * 4;
    const dot = wide * @as(usize, if (float_dot) 2 else 1);
    return .{ .staged = staged, .wide = wide, .narrow = narrow, .temporary_bytes = staged + dot + wide + narrow, .gemm_output_bytes = dot };
}
fn check(buffer: Buffer, bytes: usize) !void {
    if (buffer.ptr == 0 or buffer.len < bytes) return error.InvalidCudaState;
}
// Serving plans use heuristics on first use; exact row counts must not
// trigger synchronous GPU autotuning while the model execution lane is held.
pub fn run(ctx: *Context, lt: *Lt, kernels: *Kernels, workspace: Buffer, output: Buffer, temporary: [4]Buffer, inputs: [8]Buffer, input_native: bool, output_mirror: Buffer, rows: usize, hidden: usize, intermediate: usize, eps: f32, exact: bool) !void {
    _ = try runWithReference(ctx, lt, kernels, workspace, output, temporary, inputs, input_native, output_mirror, rows, hidden, intermediate, eps, exact, null, .{}, null);
}
pub fn runWithReference(ctx: *Context, lt: *Lt, kernels: *Kernels, workspace: Buffer, output: Buffer, temporary: [4]Buffer, inputs: [8]Buffer, input_native: bool, output_mirror: Buffer, rows: usize, hidden: usize, intermediate: usize, eps: f32, exact: bool, reference: ?*@import("gliner25_bf16_full.zig").Module, first_bias_bf16: Buffer, classic: ?*@import("libraries.zig").CublasF32) !bool {
    const biased_dot = if (reference) |ref| ref.flags & 2097152 != 0 else false;
    const float_dot = if (reference) |ref| ref.flags & 131072 != 0 and !biased_dot else false;
    const p = planWithFloatDot(rows, hidden, intermediate, float_dot) orelse return error.InvalidCudaState;
    if (!std.math.isFinite(eps) or eps <= 0 or workspace.len > @import("gliner25_bf16_attention.zig").workspace_limit) return error.InvalidCudaState;
    for ([_]usize{ 0, 1 }) |i| try check(inputs[i], p.narrow);
    try check(inputs[2], hidden * intermediate * 2);
    try check(inputs[3], intermediate * 4);
    try check(inputs[4], hidden * intermediate * 2);
    for ([_]usize{ 5, 6, 7 }) |i| try check(inputs[i], hidden * 4);
    for (temporary, [_]usize{ p.staged, p.gemm_output_bytes, p.wide, p.narrow }) |buffer, bytes| try check(buffer, bytes);
    try check(output, p.narrow);
    if (biased_dot) try check(first_bias_bf16, intermediate * 2);
    if (!input_native) try kernels.launchF32ToBf16(ctx, temporary[0], inputs[0], rows * hidden);
    if (biased_dot) {
        _ = try lt.matmulBf16WeightBiasBf16OutWithTuning(ctx, temporary[1], temporary[0], inputs[2], first_bias_bf16, workspace, rows, hidden, intermediate, .{});
    } else if (float_dot) {
        _ = try lt.matmulBf16WeightF32OutWithTuning(ctx, temporary[1], temporary[0], inputs[2], workspace, rows, hidden, intermediate, .{});
    } else _ = try lt.matmulBf16WeightBf16OutWithTuning(ctx, temporary[1], temporary[0], inputs[2], workspace, rows, hidden, intermediate, .{});
    if (reference) |ref| try ref.gelu(ctx, temporary[2], temporary[1], inputs[3], rows * intermediate, intermediate, exact) else try kernels.launchGliner25FfnGeluBiasBf16(ctx, temporary[2], temporary[1], inputs[3], rows, intermediate, exact);
    const separate_bias = if (reference) |ref| ref.flags & @import("gliner25_bf16_full.zig").separate_output_bias_flag != 0 else false;
    if (separate_bias) {
        const blas = classic orelse return error.UnsupportedCublasBf16;
        try blas.bindWorkspace(ctx.stream, @ptrFromInt(workspace.ptr), workspace.len);
        try blas.linearBf16(ctx.stream, @ptrFromInt(temporary[3].ptr), @ptrFromInt(temporary[2].ptr), @ptrFromInt(inputs[4].ptr), rows, intermediate, hidden);
    } else _ = try lt.matmulBf16WeightBiasF32OutWithTuning(ctx, temporary[3], temporary[2], inputs[4], inputs[5], workspace, rows, intermediate, hidden, .{});
    if (reference) |ref| {
        if (separate_bias) try ref.normDotBias(ctx, output, output_mirror, .{ temporary[3], inputs[5], inputs[1], inputs[6], inputs[7] }, rows, hidden, eps) else try ref.norm(ctx, output, output_mirror, .{ temporary[3], inputs[1], inputs[6], inputs[7] }, rows, hidden, eps);
    } else if (output_mirror.ptr != 0) {
        try kernels.launchGliner25NormBf16Mirror(ctx, output, output_mirror, .{ temporary[3], inputs[1], inputs[6], inputs[7] }, rows, hidden, eps, 2);
    } else try kernels.launchAddLayerNormF32(ctx, output, temporary[3], inputs[1], inputs[6], inputs[7], rows, hidden, eps);
    return separate_bias;
}
fn rounded(x: f32) f32 {
    const bits: u32 = @bitCast(x);
    return @bitCast((bits + 0x7fff + ((bits >> 16) & 1)) & 0xffff0000);
}
fn bits16(x: f32) u16 {
    return @intCast(@as(u32, @bitCast(rounded(x))) >> 16);
}

test "GLiNER BF16 FFN bounds temporary allocations" {
    const p = plan(4096, 1024, 4096).?;
    try std.testing.expectEqual(@as(usize, 88 * 1024 * 1024), p.temporary_bytes);
    try std.testing.expect(plan(4097, 1024, 4096) == null);
    try std.testing.expect(plan(128, 768, 2048) == null);
    const float_dot = planWithFloatDot(4096, 1024, 4096, true).?;
    // The fused bias/BF16 output removes one F32-wide dot temporary.
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), float_dot.temporary_bytes - p.temporary_bytes);
}

test "GLiNER BF16 FFN CPU oracle includes bias GELU residual and norm" {
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
    var classic = try @import("libraries.zig").CublasF32.init(ctx.stream);
    defer classic.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const h = 64;
    const intermediate = 256;
    const rows = 128;
    const p = plan(rows, h, intermediate).?;
    var values: [8][]f32 = undefined;
    const counts = [_]usize{ rows * h, rows * h, h * intermediate, intermediate, h * intermediate, h, h, h };
    for (&values, counts, 0..) |*v, n, j| {
        v.* = try temp.alloc(f32, n);
        @memset(v.*, 0);
        for (v.*, 0..) |*x, i| x.* = switch (j) {
            0 => @sin(@as(f32, @floatFromInt(i % 101)) * 0.03),
            1 => @cos(@as(f32, @floatFromInt(i % 97)) * 0.05),
            3 => 0.03125,
            5 => 0.015625,
            6 => 0.75,
            7 => -0.03125,
            else => 0,
        };
    }
    for (0..intermediate) |i| values[2][i * h + i % h] = 0.25;
    for (0..h) |i| for (0..4) |j| {
        values[4][i * intermediate + i + j * h] = @as(f32, @floatFromInt(j + 1)) * 0.125;
    };
    var inputs: [8]Buffer = @splat(.{});
    defer for (&inputs) |*v| v.free(&ctx);
    for (&inputs, values, 0..) |*v, host, j| {
        if (j == 2 or j == 4) {
            const quant = try temp.alloc(u16, host.len);
            for (quant, host) |*x, y| x.* = bits16(y);
            v.* = try Buffer.alloc(&ctx, quant.len * 2);
            try v.copyFromHost(&ctx, std.mem.sliceAsBytes(quant));
        } else {
            v.* = try Buffer.alloc(&ctx, host.len * 4);
            try v.copyFromHost(&ctx, std.mem.sliceAsBytes(host));
        }
    }
    var temporary: [4]Buffer = @splat(.{});
    defer for (&temporary) |*v| v.free(&ctx);
    for (&temporary, [_]usize{ p.staged, p.wide * 2, p.wide, p.narrow }) |*v, n| v.* = try Buffer.alloc(&ctx, n);
    var output = try Buffer.alloc(&ctx, p.narrow);
    defer output.free(&ctx);
    var workspace = try Buffer.alloc(&ctx, @import("gliner25_bf16_attention.zig").workspace_limit);
    defer workspace.free(&ctx);
    const actual = try temp.alloc(f32, rows * h);
    const wide = try temp.alloc(f32, intermediate);
    const narrow = try temp.alloc(f64, h);
    for ([_]bool{ false, true }) |exact| {
        try run(&ctx, &lt, &kernels, workspace, output, temporary, inputs, false, .{}, rows, h, intermediate, 1e-5, exact);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        if (ctx.info.compute_major == 8 and ctx.info.compute_minor == 9) {
            const baseline = try temp.dupe(f32, actual);
            // The first call already staged the exact BF16 input in temporary[0].
            try run(&ctx, &lt, &kernels, workspace, output, temporary, inputs, true, temporary[0], rows, h, intermediate, 1e-5, exact);
            try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
            const mirror = try temp.alloc(u16, rows * h);
            try temporary[0].copyToHost(&ctx, std.mem.sliceAsBytes(mirror));
            try ctx.synchronize();
            for (actual, baseline, mirror) |got, original, quantized| {
                try std.testing.expectEqual(@as(u32, @bitCast(original)), @as(u32, @bitCast(got)));
                try std.testing.expectEqual(bits16(original), quantized);
            }
        }
        for (0..rows) |row| {
            for (wide, 0..) |*x, j| {
                const up = rounded(rounded(values[0][row * h + j % h]) * 0.25);
                const biased = up + values[3][j];
                const y = if (exact) 0.5 * biased * (1 + @import("../../backends/activations.zig").erfApproxF32(biased * 0.7071067811865476)) else 0.5 * biased * (1 + std.math.tanh(@as(f32, 0.7978845608028654) * (biased + 0.044715 * biased * biased * biased)));
                x.* = rounded(y);
            }
            var mean: f64 = 0;
            for (narrow, 0..) |*x, j| {
                var down: f32 = 0;
                for (0..4) |k| down += wide[j + k * h] * values[4][j * intermediate + j + k * h];
                x.* = (down + values[5][j]) + values[1][row * h + j];
                mean += x.*;
            }
            mean /= h;
            var variance: f64 = 0;
            for (narrow) |x| variance += (x - mean) * (x - mean);
            variance /= h;
            for (narrow, 0..) |x, j| {
                const expected = (x - mean) / @sqrt(variance + 1e-5) * values[6][j] + values[7][j];
                try std.testing.expect(std.math.isFinite(actual[row * h + j]));
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), actual[row * h + j], 0.003);
            }
        }
    }
    if (ctx.info.compute_major == 8 and ctx.info.compute_minor == 9) {
        var reference = @import("gliner25_bf16_full.zig").Module{ .flags = 1212440 };
        try reference.init(&ctx);
        defer reference.deinit(&ctx);
        var bias = try Buffer.alloc(&ctx, intermediate * 2);
        defer bias.free(&ctx);
        try kernels.launchF32ToBf16(&ctx, bias, inputs[3], intermediate);
        const float_result = try temp.alloc(f32, rows * h);
        for ([_]bool{ false, true }) |exact| {
            // Float-dot is also the admission-denied bias-cache fallback.
            for ([_]u32{ 0, 2097152, 2097152 | @import("gliner25_bf16_full.zig").separate_output_bias_flag }) |extra_flags| {
                const fused_bias = extra_flags & 2097152 != 0;
                const separate_bias = extra_flags & @import("gliner25_bf16_full.zig").separate_output_bias_flag != 0;
                reference.flags = 1212440 | extra_flags;
                _ = try runWithReference(&ctx, &lt, &kernels, workspace, output, temporary, inputs, false, .{}, rows, h, intermediate, 1e-5, exact, &reference, if (fused_bias) bias else .{}, &classic);
                try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
                try ctx.synchronize();
                if (!fused_bias) @memcpy(float_result, actual);
                for (actual, float_result) |got, want| try std.testing.expectApproxEqAbs(want, got, 0.016);
                for (0..rows) |row| {
                    for (wide, 0..) |*x, j| {
                        const up = rounded(rounded(values[0][row * h + j % h]) * 0.25 + values[3][j]);
                        const y = if (exact) 0.5 * up * (1 + @import("../../backends/activations.zig").erfApproxF32(up * 0.7071067811865476)) else 0.5 * up * (1 + std.math.tanh(@as(f32, 0.7978845608028654) * (up + 0.044715 * up * up * up)));
                        x.* = rounded(y);
                    }
                    var mean: f64 = 0;
                    for (narrow, 0..) |*x, j| {
                        var down: f32 = 0;
                        for (0..4) |k| down += wide[j + k * h] * values[4][j * intermediate + j + k * h];
                        x.* = rounded(rounded((if (separate_bias) rounded(down) else down) + values[5][j]) + rounded(values[1][row * h + j]));
                        mean += x.*;
                    }
                    mean /= h;
                    var variance: f64 = 0;
                    for (narrow) |x| variance += (x - mean) * (x - mean);
                    variance /= h;
                    for (narrow, 0..) |x, j| {
                        const expected = rounded(@floatCast((x - mean) / @sqrt(variance + 1e-5) * values[6][j] + values[7][j]));
                        try std.testing.expectApproxEqAbs(expected, actual[row * h + j], 0.016);
                        try std.testing.expectEqual(rounded(actual[row * h + j]), actual[row * h + j]);
                    }
                }
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), lt.matmul_tuning_searches);
}
