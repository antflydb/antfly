// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded strict-FP32 GEMM/relative-gather/softmax/GEMM attention.
const std = @import("std");
const Buffer = @import("buffer.zig").DeviceBuffer;
const Context = @import("context.zig").CudaContext;
const Blas = @import("libraries.zig").CublasF32;
const Kernels = @import("kernels.zig").KernelModule;
pub const workspace_limit = 32 * 1024 * 1024;
const vendor_workspace_bytes = 4 * 1024 * 1024;
const pointer_workspace_bytes = 16 * 1024;

pub const Plan = struct {
    heads_per_group: usize,
    score_elements: usize,
    relative_elements: usize,
    workspace_bytes: usize,
};

pub fn plan(batch: usize, sequence: usize, heads: usize, dimension: usize, relative: usize) ?Plan {
    if (batch == 0 or batch > 8 or sequence == 0 or sequence > 512 or heads == 0 or heads > 16 or dimension != 64 or relative == 0 or relative > 1023) return null;
    const scores = sequence * sequence;
    const positions = sequence * relative;
    const per_head = (scores + 2 * positions) * 4;
    const group = @min(batch * heads, (workspace_limit - vendor_workspace_bytes - pointer_workspace_bytes) / per_head);
    if (group == 0) return null;
    return .{ .heads_per_group = group, .score_elements = scores, .relative_elements = positions, .workspace_bytes = std.mem.alignForward(usize, group * per_head, 256) + vendor_workspace_bytes + pointer_workspace_bytes };
}

fn region(buffer: Buffer, offset: usize, bytes: usize) !Buffer {
    if (buffer.ptr == 0 or offset > buffer.len or bytes > buffer.len - offset) return error.InvalidCudaState;
    return .{ .ptr = try std.math.add(u64, buffer.ptr, offset), .len = bytes };
}
fn ptr(buffer: Buffer) *anyopaque {
    return @ptrFromInt(buffer.ptr);
}

pub fn run(ctx: *Context, blas: *Blas, kernels: *Kernels, workspace: Buffer, output: Buffer, inputs: [7]Buffer, batch: usize, sequence: usize, heads: usize, relative: usize, boundary: bool) !bool {
    return runMode(ctx, blas, kernels, workspace, output, inputs, batch, sequence, heads, relative, boundary, !@import("antfly_platform").env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_GLINER_POINTER_BATCHES"));
}

fn runMode(ctx: *Context, blas: *Blas, kernels: *Kernels, workspace: Buffer, output: Buffer, inputs: [7]Buffer, batch: usize, sequence: usize, heads: usize, relative: usize, boundary: bool, pointer_batches: bool) !bool {
    const p = plan(batch, sequence, heads, 64, relative) orelse return error.InvalidCudaState;
    const hidden = heads * 64;
    const document_elements = sequence * hidden;
    const input_bytes = batch * document_elements * 4;
    for (inputs[0..3]) |input| _ = try region(input, 0, input_bytes);
    for (inputs[3..5]) |input| _ = try region(input, 0, relative * hidden * 4);
    _ = try region(inputs[6], 0, batch * sequence * @as(usize, if (boundary) 4 else 8));
    if (boundary or inputs[5].ptr != 0) _ = try region(inputs[5], 0, (2 * sequence - 1) * 4);
    _ = try region(output, 0, input_bytes);
    _ = try region(workspace, 0, p.workspace_bytes);
    const cc = try region(workspace, 0, p.heads_per_group * p.score_elements * 4);
    const cp = try region(workspace, cc.len, p.heads_per_group * p.relative_elements * 4);
    const pc = try region(workspace, cc.len + cp.len, cp.len);
    const vendor = try region(workspace, p.workspace_bytes - vendor_workspace_bytes - pointer_workspace_bytes, vendor_workspace_bytes);
    const arrays = try region(workspace, p.workspace_bytes - pointer_workspace_bytes, pointer_workspace_bytes);
    try blas.bindWorkspace(ctx.stream, ptr(vendor), vendor.len);
    if (pointer_batches and batch > 1 and blas.supportsPointerBatches() and kernels.gliner25_attention_pointer_views_f32 != null) {
        var first: usize = 0;
        while (first < batch * heads) : (first += p.heads_per_group) {
            const group = @min(p.heads_per_group, batch * heads - first);
            try kernels.launchGliner25AttentionPointerViewsF32(ctx, arrays, inputs[0..5].*, output, cc, cp, pc, sequence, heads, relative, first, group);
            var views: [9]Buffer = undefined;
            for (&views, 0..) |*view, i| view.* = try region(arrays, i * group * 8, group * 8);
            try blas.pointerBatched(ctx.stream, ptr(views[5]), ptr(views[0]), ptr(views[1]), group, sequence, 64, sequence, hidden, hidden, sequence, true);
            try blas.pointerBatched(ctx.stream, ptr(views[6]), ptr(views[0]), ptr(views[4]), group, sequence, 64, relative, hidden, hidden, relative, true);
            try blas.pointerBatched(ctx.stream, ptr(views[7]), ptr(views[1]), ptr(views[3]), group, sequence, 64, relative, hidden, hidden, relative, true);
            try kernels.launchGliner25AttentionSoftmaxF32(ctx, cc, cp, pc, inputs[6], inputs[5], sequence, relative, group, first, boundary, heads);
            try blas.pointerBatched(ctx.stream, ptr(views[8]), ptr(views[5]), ptr(views[2]), group, sequence, sequence, 64, sequence, hidden, hidden, false);
        }
        return true;
    }
    for (0..batch) |b| {
        var h: usize = 0;
        while (h < heads) : (h += p.heads_per_group) {
            const group = @min(p.heads_per_group, heads - h);
            const offset = (b * document_elements + h * 64) * 4;
            const q = try region(inputs[0], offset, input_bytes - offset);
            const k = try region(inputs[1], offset, input_bytes - offset);
            const v = try region(inputs[2], offset, input_bytes - offset);
            const qr = try region(inputs[3], h * 64 * 4, inputs[3].len - h * 64 * 4);
            const kr = try region(inputs[4], h * 64 * 4, inputs[4].len - h * 64 * 4);
            const out = try region(output, offset, input_bytes - offset);
            try blas.strided(ctx.stream, ptr(cc), ptr(q), ptr(k), group, sequence, 64, sequence, hidden, hidden, sequence, 64, 64, p.score_elements, true);
            try blas.strided(ctx.stream, ptr(cp), ptr(q), ptr(kr), group, sequence, 64, relative, hidden, hidden, relative, 64, 64, p.relative_elements, true);
            try blas.strided(ctx.stream, ptr(pc), ptr(k), ptr(qr), group, sequence, 64, relative, hidden, hidden, relative, 64, 64, p.relative_elements, true);
            try kernels.launchGliner25AttentionSoftmaxF32(ctx, cc, cp, pc, inputs[6], inputs[5], sequence, relative, group, b * heads + h, boundary, heads);
            try blas.strided(ctx.stream, ptr(out), ptr(cc), ptr(v), group, sequence, sequence, 64, sequence, hidden, hidden, p.score_elements, 64, 64, false);
        }
    }
    return false;
}

test "GLiNER FP32 attention groups bound scratch independently of batch" {
    for ([_]usize{ 1, 8 }) |batch| for ([_]usize{ 1, 31, 128, 129, 511, 512 }) |sequence| for ([_]usize{ 2, 12, 16 }) |heads| {
        const p = plan(batch, sequence, heads, 64, 1023).?;
        try std.testing.expect(p.workspace_bytes <= workspace_limit);
        try std.testing.expect(p.heads_per_group > 0 and p.heads_per_group <= batch * heads);
        const scores_bytes = p.heads_per_group * (p.score_elements + 2 * p.relative_elements) * 4;
        try std.testing.expect(p.workspace_bytes >= scores_bytes + vendor_workspace_bytes + pointer_workspace_bytes);
        try std.testing.expectEqual(@as(usize, 0), (p.workspace_bytes - vendor_workspace_bytes - pointer_workspace_bytes) % 256);
    };
    for ([_][5]usize{ .{ 0, 128, 12, 64, 512 }, .{ 9, 128, 12, 64, 512 }, .{ 1, 513, 12, 64, 512 }, .{ 1, 128, 17, 64, 512 }, .{ 1, 128, 12, 128, 512 }, .{ 1, 128, 12, 64, 1024 } }) |shape|
        try std.testing.expect(plan(shape[0], shape[1], shape[2], shape[3], shape[4]) == null);
}

test "GLiNER FP32 attention CUDA compact and legacy masks match CPU" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    // This dedicated test requires a live GPU; unavailable hardware is failure.
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var kernels = try Kernels.load(&ctx);
    defer kernels.unload(&ctx);
    try std.testing.expect(kernels.gliner25_attention_softmax_f32 != null);
    var blas = try Blas.init(ctx.stream);
    defer blas.deinit();
    for ([_][5]usize{ .{ 31, 2, 17, 1, 2 }, .{ 129, 12, 17, 1, 2 }, .{ 512, 16, 1023, 0, 2 }, .{ 128, 16, 255, 0, 8 }, .{ 512, 16, 37, 2, 8 }, .{ 128, 12, 512, 1, 8 } }) |geometry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const s = geometry[0];
        const h = geometry[1];
        const r = geometry[2];
        const boundary = geometry[3] == 1;
        const compact = boundary or geometry[3] == 2;
        const batch = geometry[4];
        const hidden = h * 64;
        const count = batch * s * hidden;
        var host: [5][]f32 = undefined;
        for (&host, 0..) |*values, operand| {
            values.* = try temp.alloc(f32, if (operand < 3) count else r * hidden);
            for (values.*, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i % 197 + operand * 13)) * 0.13) * 0.3;
        }
        const masks32 = try temp.alloc(i32, batch * s);
        const masks64 = try temp.alloc(i64, batch * s);
        for (masks32, masks64, 0..) |*v32, *v64, i| {
            const document = i / s;
            const token = i % s;
            v32.* = @intFromBool(document + 1 < batch and token % 7 != (document + 3) % 7 and (s < 129 or token >= 64));
            v64.* = v32.*;
        }
        const buckets = try temp.alloc(i32, 2 * s - 1);
        for (buckets, 0..) |*value, i| value.* = @intCast((i / 3) % r);
        var inputs: [7]Buffer = @splat(.{});
        defer for (&inputs) |*input| input.free(&ctx);
        for (&inputs, 0..) |*input, i| {
            if (i == 5 and !compact) continue;
            const bytes = if (i < 5) std.mem.sliceAsBytes(host[i]) else if (i == 5) std.mem.sliceAsBytes(buckets) else if (boundary) std.mem.sliceAsBytes(masks32) else std.mem.sliceAsBytes(masks64);
            input.* = try Buffer.alloc(&ctx, bytes.len);
            try input.copyFromHost(&ctx, bytes);
        }
        var output = try Buffer.alloc(&ctx, count * 4);
        defer output.free(&ctx);
        var workspace = try Buffer.alloc(&ctx, plan(batch, s, h, 64, r).?.workspace_bytes);
        defer workspace.free(&ctx);
        _ = try runMode(&ctx, &blas, &kernels, workspace, output, inputs, batch, s, h, r, boundary, false);
        const reference = try temp.alloc(f32, count);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(reference));
        try ctx.synchronize();
        try std.testing.expect(try runMode(&ctx, &blas, &kernels, workspace, output, inputs, batch, s, h, r, boundary, true));
        const actual = try temp.alloc(f32, count);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (actual, reference, 0..) |value, baseline, i| {
            try std.testing.expect(std.math.isFinite(value));
            try std.testing.expectApproxEqAbs(baseline, value, 3e-5);
            if (i >= (batch - 1) * s * hidden or (boundary and masks32[i / hidden] == 0)) try std.testing.expectEqual(@as(f32, 0), value);
        }
        const scores = try temp.alloc(f64, s);
        // Selected query rows cover tails and mask transitions; every output
        // is checked above for finiteness and fully masked document semantics.
        for (0..batch - 1) |document| for ([_]usize{ 0, @min(s - 1, 63), @min(s - 1, 64), s / 2, s - 1 }) |qi| for (0..h) |head| {
            var maximum: f64 = -std.math.inf(f64);
            for (scores, 0..) |*score, ki| {
                var sum: f64 = 0;
                const bucket: usize = if (compact) @intCast(buckets[qi + s - 1 - ki]) else qi + s - 1 - ki;
                for (0..64) |d| {
                    const qv: f64 = host[0][(document * s + qi) * hidden + head * 64 + d];
                    const kv: f64 = host[1][(document * s + ki) * hidden + head * 64 + d];
                    sum += qv * kv + qv * host[4][bucket * hidden + head * 64 + d] + kv * host[3][bucket * hidden + head * 64 + d];
                }
                score.* = if (masks32[document * s + ki] == 0) -std.math.inf(f64) else sum / @sqrt(@as(f64, 192));
                maximum = @max(maximum, score.*);
            }
            var denominator: f64 = 0;
            for (scores) |*score| {
                score.* = @exp(score.* - maximum);
                denominator += score.*;
            }
            for (0..64) |d| {
                var value: f64 = 0;
                if (!boundary or masks32[document * s + qi] != 0) for (scores, 0..) |score, ki| {
                    value += score / denominator * host[2][(document * s + ki) * hidden + head * 64 + d];
                };
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(value)), actual[(document * s + qi) * hidden + head * 64 + d], 3e-5);
            }
        };
        if (batch == 8 and s == 128 and h == 12) {
            var times: [2]u64 = @splat(0);
            for (0..4) |round| for (0..2) |index| {
                const pointer_batches = if (round % 2 == 0) index else 1 - index;
                const event = try ctx.beginProfileEventPair();
                for (0..8) |_| _ = try runMode(&ctx, &blas, &kernels, workspace, output, inputs, batch, s, h, r, boundary, pointer_batches == 1);
                times[pointer_batches] += try ctx.endProfileEventPairUs(event);
            };
            std.debug.print("gliner_fp32_attention b8_s128_h12 strided_us={} pointer_us={} iterations=32 alternating_blocks=4\n", .{ times[0], times[1] });
        }
    }
}

test "GLiNER FP32 attention projection workspace preserves FP32 and reuses plans" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var lt = try @import("cublaslt.zig").CublasLt.openWithAllocator(a, &ctx);
    defer lt.deinit();
    var workspace = try Buffer.alloc(&ctx, workspace_limit);
    defer workspace.free(&ctx);
    for ([_][3]usize{ .{ 128, 768, 3072 }, .{ 1024, 768, 768 }, .{ 1024, 768, 3072 }, .{ 512, 3072, 768 }, .{ 4096, 768, 3072 }, .{ 512, 1024, 4096 }, .{ 512, 4096, 1024 }, .{ 4096, 1024, 1024 }, .{ 4096, 1024, 4096 } }) |shape| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const rows = shape[0];
        const width = shape[1];
        const columns = shape[2];
        const x = try temp.alloc(f32, rows * width);
        const w = try temp.alloc(f32, columns * width);
        for (x, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i % 199)) * 0.13) * 0.03;
        for (w, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i % 197)) * 0.17) * 0.03;
        var input = try Buffer.alloc(&ctx, x.len * 4);
        defer input.free(&ctx);
        var weight = try Buffer.alloc(&ctx, w.len * 4);
        defer weight.free(&ctx);
        var output = try Buffer.alloc(&ctx, rows * columns * 4);
        defer output.free(&ctx);
        try input.copyFromHost(&ctx, std.mem.sliceAsBytes(x));
        try weight.copyFromHost(&ctx, std.mem.sliceAsBytes(w));
        try lt.matmulF32WeightF32OutWithWorkspace(&ctx, output, input, weight, .{}, rows, width, columns);
        const reference = try temp.alloc(f32, rows * columns);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(reference));
        try ctx.synchronize();
        try lt.matmulF32WeightF32OutWithWorkspace(&ctx, output, input, weight, workspace, rows, width, columns);
        const hits = lt.plan_cache_hits;
        var times: [2]u64 = @splat(0);
        for (0..4) |round| for (0..2) |index| {
            const with_workspace = if (round % 2 == 0) index else 1 - index;
            const event = try ctx.beginProfileEventPair();
            for (0..16) |_| {
                if (with_workspace == 1) {
                    try lt.matmulF32WeightF32OutWithWorkspace(&ctx, output, input, weight, workspace, rows, width, columns);
                } else try lt.matmulF32WeightF32OutWithWorkspace(&ctx, output, input, weight, .{}, rows, width, columns);
            }
            times[with_workspace] += try ctx.endProfileEventPairUs(event);
        };
        try std.testing.expect(lt.plan_cache_hits >= hits + 32);
        const actual = try temp.alloc(f32, rows * columns);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (actual, reference) |got, expected| try std.testing.expectApproxEqAbs(expected, got, 3e-5);
        for ([_]usize{ 0, rows / 2, rows - 1 }) |row| for ([_]usize{ 0, columns / 2, columns - 1 }) |column| {
            var expected: f64 = 0;
            for (0..width) |k| expected += @as(f64, x[row * width + k]) * w[column * width + k];
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), actual[row * columns + column], 3e-5);
        };
        const bias_values = try temp.alloc(f32, columns);
        for (bias_values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.05) * 0.03;
        var bias = try Buffer.alloc(&ctx, columns * 4);
        defer bias.free(&ctx);
        try bias.copyFromHost(&ctx, std.mem.sliceAsBytes(bias_values));
        try lt.matmulF32BiasF32Out(&ctx, output, input, weight, bias, workspace, rows, width, columns);
        const bias_hits = lt.plan_cache_hits;
        try lt.matmulF32BiasF32Out(&ctx, output, input, weight, bias, workspace, rows, width, columns);
        try std.testing.expect(lt.plan_cache_hits > bias_hits);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (actual, reference, 0..) |got, expected, i| try std.testing.expectApproxEqAbs(expected + bias_values[i % columns], got, 3e-5);
        std.debug.print("gliner_fp32_projection rows={} width={} columns={} zero_workspace_us={} workspace32_us={} iterations=64 alternating_blocks=4\n", .{ rows, width, columns, times[0], times[1] });
    }
}
