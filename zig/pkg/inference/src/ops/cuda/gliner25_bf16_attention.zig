// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Experimental BF16 tensor-core attention with F32 reductions and accumulation.
const std = @import("std");
const Buffer = @import("buffer.zig").DeviceBuffer;
const Context = @import("context.zig").CudaContext;
const Blas = @import("libraries.zig").CublasF32;
const Kernels = @import("kernels.zig").KernelModule;
pub const workspace_limit = 64 * 1024 * 1024;
const vendor_bytes = 4 * 1024 * 1024;
const pointer_bytes = 16 * 1024;
pub const Plan = struct {
    group: usize,
    tokens: usize,
    positions: usize,
    staging_bytes: usize,
    scaled_key_bytes: usize = 0,
    relative_pitch: usize,
    score_pitch: usize,
    workspace_bytes: usize,
};
pub fn plan(batch: usize, sequence: usize, heads: usize, dimension: usize, relative: usize) ?Plan {
    return planWithNativeInputs(batch, sequence, heads, dimension, relative, false);
}
pub fn planWithNativeInputs(batch: usize, sequence: usize, heads: usize, dimension: usize, relative: usize, native: bool) ?Plan {
    return planWithReference(batch, sequence, heads, dimension, relative, native, false);
}
pub fn planWithReference(batch: usize, sequence: usize, heads: usize, dimension: usize, relative: usize, native: bool, full: bool) ?Plan {
    if (batch == 0 or batch > 8 or sequence == 0 or sequence > 512 or (!full and sequence % 8 != 0) or heads == 0 or heads > 16 or dimension != 64 or relative == 0 or relative > 1023) return null;
    const tokens = batch * sequence * heads * 64;
    const positions = relative * heads * 64;
    const scaled = if (full) std.mem.alignForward(usize, tokens * 2, 256) else 0;
    const staged = (if (native) @as(usize, 0) else std.mem.alignForward(usize, (3 * tokens + 2 * positions) * 2, 256)) + scaled;
    const pitch = std.mem.alignForward(usize, relative, 8);
    // Every pointer-batched BF16 matrix must start on a 16-byte boundary.
    // Odd sequence lengths otherwise misalign later heads and workspace views.
    const score_pitch = if (full) std.mem.alignForward(usize, sequence, 8) else sequence;
    const per_head = 4 * sequence * score_pitch + 2 * pitch * (sequence + score_pitch);
    if (staged + vendor_bytes + pointer_bytes >= workspace_limit) return null;
    const group = @min(@min(batch * heads, if (native and !full) @as(usize, 16) else batch * heads), (workspace_limit - staged - vendor_bytes - pointer_bytes - 255) / per_head);
    if (group == 0) return null;
    return .{ .group = group, .tokens = tokens, .positions = positions, .staging_bytes = staged, .scaled_key_bytes = scaled, .relative_pitch = pitch, .score_pitch = score_pitch, .workspace_bytes = std.mem.alignForward(usize, staged + group * per_head, 256) + vendor_bytes + pointer_bytes };
}
fn region(buffer: Buffer, offset: usize, bytes: usize) !Buffer {
    if (buffer.ptr == 0 or offset > buffer.len or bytes > buffer.len - offset) return error.InvalidCudaState;
    return .{ .ptr = try std.math.add(u64, buffer.ptr, offset), .len = bytes };
}
fn ptr(buffer: Buffer) *anyopaque {
    return @ptrFromInt(buffer.ptr);
}
pub fn run(ctx: *Context, blas: *Blas, kernels: *Kernels, workspace: Buffer, output: Buffer, inputs: [7]Buffer, batch: usize, sequence: usize, heads: usize, relative: usize, boundary: bool) !void {
    return runWithNativeInputs(ctx, blas, kernels, workspace, output, inputs, batch, sequence, heads, relative, boundary, false);
}
pub fn runWithNativeInputs(ctx: *Context, blas: *Blas, kernels: *Kernels, workspace: Buffer, output: Buffer, inputs: [7]Buffer, batch: usize, sequence: usize, heads: usize, relative: usize, boundary: bool, native: bool) !void {
    return runWithReference(ctx, blas, kernels, workspace, output, inputs, batch, sequence, heads, relative, boundary, native, null);
}
pub fn runWithReference(ctx: *Context, blas: *Blas, kernels: *Kernels, workspace: Buffer, output: Buffer, inputs: [7]Buffer, batch: usize, sequence: usize, heads: usize, relative: usize, boundary: bool, native: bool, reference: ?*@import("gliner25_bf16_full.zig").Module) !void {
    const p = planWithReference(batch, sequence, heads, 64, relative, native, reference != null) orelse return error.InvalidCudaState;
    for (inputs[0..3]) |input| _ = try region(input, 0, p.tokens * @as(usize, if (native) 2 else 4));
    for (inputs[3..5]) |input| _ = try region(input, 0, p.positions * @as(usize, if (native) 2 else 4));
    _ = try region(inputs[6], 0, batch * sequence * @as(usize, if (boundary) 4 else 8));
    if (boundary or inputs[5].ptr != 0) _ = try region(inputs[5], 0, (2 * sequence - 1) * 4);
    _ = try region(output, 0, p.tokens * 4);
    _ = try region(workspace, 0, p.workspace_bytes);
    var staged: [5]Buffer = undefined;
    var offset: usize = 0;
    for (&staged, 0..) |*buffer, i| {
        if (native) {
            buffer.* = inputs[i];
            continue;
        }
        const bytes = (if (i < 3) p.tokens else p.positions) * 2;
        buffer.* = try region(workspace, offset, bytes);
        offset += bytes;
    }
    if (!native) try kernels.launchGliner25AttentionStageBf16(ctx, staged, inputs[0..5].*, p.tokens, p.positions);
    const scaled = if (reference != null) try region(workspace, p.staging_bytes - p.scaled_key_bytes, p.scaled_key_bytes) else Buffer{};
    const fused_views = reference != null and reference.?.fuse_views and batch == 1 and p.group >= heads;
    if (reference) |ref| if (!fused_views) try ref.scaleKey(ctx, scaled, staged[1], p.tokens);
    const score_elements = p.group * sequence * p.score_pitch;
    const relative_elements = p.group * sequence * p.relative_pitch;
    const cc = try region(workspace, p.staging_bytes, score_elements * 2);
    const cp = try region(workspace, p.staging_bytes + cc.len, relative_elements * 2);
    const pc = try region(workspace, p.staging_bytes + cc.len + cp.len, p.group * p.relative_pitch * p.score_pitch * 2);
    const probs = try region(workspace, p.staging_bytes + cc.len + cp.len + pc.len, score_elements * 2);
    const vendor = try region(workspace, p.workspace_bytes - vendor_bytes - pointer_bytes, vendor_bytes);
    const arrays = try region(workspace, p.workspace_bytes - pointer_bytes, pointer_bytes);
    try blas.bindWorkspace(ctx.stream, ptr(vendor), vendor.len);
    const hidden = heads * 64;
    var first: usize = 0;
    while (first < batch * heads) : (first += p.group) {
        const group = @min(p.group, batch * heads - first);
        if (reference) |ref| {
            const buffers = [12]Buffer{ arrays, staged[0], staged[1], staged[2], staged[3], staged[4], output, cc, cp, pc, probs, scaled };
            if (fused_views) try ref.scaleViews(ctx, buffers, sequence, heads, p.relative_pitch, group, p.tokens) else try ref.views(ctx, buffers, sequence, heads, p.relative_pitch, first, group);
        } else try kernels.launchGliner25AttentionPointerViewsBf16(ctx, arrays, staged, output, cc, cp, pc, probs, sequence, heads, relative, p.relative_pitch, first, group);
        var views: [11]Buffer = undefined;
        for (&views, 0..) |*view, i| view.* = try region(arrays, i * group * 8, group * 8);
        try blas.pointerBatchedBf16(ctx.stream, ptr(views[5]), ptr(views[0]), ptr(views[if (reference != null) @as(usize, 10) else 1]), group, sequence, 64, sequence, hidden, hidden, p.score_pitch, true, true);
        try blas.pointerBatchedBf16(ctx.stream, ptr(views[6]), ptr(views[0]), ptr(views[4]), group, sequence, 64, relative, hidden, hidden, p.relative_pitch, true, true);
        try blas.pointerBatchedBf16(ctx.stream, ptr(views[7]), ptr(views[3]), ptr(views[1]), group, relative, 64, sequence, hidden, hidden, p.score_pitch, true, true);
        if (reference) |ref| {
            try ref.softmax(ctx, .{ probs, cc, cp, pc, inputs[6], inputs[5] }, sequence, relative, group, first, boundary, heads, p.relative_pitch);
        } else try kernels.launchGliner25AttentionSoftmaxBf16(ctx, probs, cc, cp, pc, inputs[6], inputs[5], sequence, relative, p.relative_pitch, group, first, boundary, heads);
        try blas.pointerBatchedBf16(ctx.stream, ptr(views[8]), ptr(views[9]), ptr(views[2]), group, sequence, sequence, 64, p.score_pitch, hidden, hidden, false, false);
    }
    if (reference) |ref| if (ref.flags & 32768 == 0) try ref.round(ctx, output, p.tokens);
}

test "GLiNER BF16 attention bounds staging and vendor scratch" {
    for ([_]usize{ 1, 7, 53, 127, 511 }) |sequence| {
        const full = planWithReference(8, sequence, 16, 64, 2 * sequence - 1, true, true).?;
        try std.testing.expect(full.workspace_bytes <= workspace_limit);
        try std.testing.expect(full.group * 11 * @sizeOf(u64) <= pointer_bytes);
    }
    for ([_]usize{ 1, 8 }) |b| for ([_]usize{ 8, 32, 128, 512 }) |s| for ([_]usize{ 2, 12, 16 }) |h| for ([_]usize{ 17, 255, 512, 1023 }) |r| {
        const p = plan(b, s, h, 64, r).?;
        try std.testing.expect(p.workspace_bytes <= workspace_limit);
        try std.testing.expect(p.group > 0 and p.group <= b * h);
        try std.testing.expect(p.staging_bytes >= 2 * (3 * p.tokens + 2 * p.positions));
        const native = planWithNativeInputs(b, s, h, 64, r, true).?;
        try std.testing.expectEqual(@as(usize, 0), native.staging_bytes);
        try std.testing.expect(native.group <= 16 and native.workspace_bytes <= workspace_limit);
        const full = planWithReference(b, s, h, 64, r, true, true).?;
        try std.testing.expect(full.workspace_bytes <= workspace_limit);
        try std.testing.expect(full.group > 0 and full.group <= b * h);
        try std.testing.expect(full.group * 11 * @sizeOf(u64) <= pointer_bytes);
        try std.testing.expect(full.scaled_key_bytes >= 2 * full.tokens);
    };
    for ([_][5]usize{ .{ 0, 128, 12, 64, 512 }, .{ 9, 128, 12, 64, 512 }, .{ 1, 129, 12, 64, 512 }, .{ 1, 128, 12, 128, 512 } }) |g| try std.testing.expect(plan(g[0], g[1], g[2], g[3], g[4]) == null);
}
fn rounded(x: f32) f32 {
    const bits: u32 = @bitCast(x);
    return @bitCast((bits + 0x7fff + ((bits >> 16) & 1)) & 0xffff0000);
}
test "GLiNER BF16 attention matches rounded CPU oracle with document masks" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var kernels = try Kernels.load(&ctx);
    defer kernels.unload(&ctx);
    var blas = try Blas.init(ctx.stream);
    defer blas.deinit();
    try std.testing.expect(blas.supportsBf16PointerBatches());
    for ([_][5]usize{ .{ 32, 2, 17, 1, 2 }, .{ 128, 12, 256, 1, 8 }, .{ 128, 16, 255, 0, 8 }, .{ 512, 16, 511, 0, 8 } }) |g| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const s = g[0];
        const h = g[1];
        const r = g[2];
        const boundary = g[3] == 1;
        const b = g[4];
        const hidden = h * 64;
        var host: [5][]f32 = undefined;
        for (&host, 0..) |*v, j| {
            v.* = try temp.alloc(f32, (if (j < 3) b * s else r) * hidden);
            for (v.*, 0..) |*x, i| x.* = @sin(@as(f32, @floatFromInt(i % 197 + j * 13)) * 0.13) * 0.3;
        }
        const mask32 = try temp.alloc(i32, b * s);
        const mask64 = try temp.alloc(i64, b * s);
        for (mask32, mask64, 0..) |*m, *n, i| {
            m.* = @intFromBool(i / s + 1 < b and i % s % 7 != (i / s + 3) % 7);
            n.* = m.*;
        }
        const buckets = try temp.alloc(i32, 2 * s - 1);
        for (buckets, 0..) |*x, i| x.* = @intCast((i / 3) % r);
        var inputs: [7]Buffer = @splat(.{});
        defer for (&inputs) |*v| v.free(&ctx);
        for (&inputs, 0..) |*v, i| {
            const bytes = if (i < 5) std.mem.sliceAsBytes(host[i]) else if (i == 5) std.mem.sliceAsBytes(buckets) else if (boundary) std.mem.sliceAsBytes(mask32) else std.mem.sliceAsBytes(mask64);
            v.* = try Buffer.alloc(&ctx, bytes.len);
            try v.copyFromHost(&ctx, bytes);
        }
        var workspace = try Buffer.alloc(&ctx, plan(b, s, h, 64, r).?.workspace_bytes);
        defer workspace.free(&ctx);
        var output = try Buffer.alloc(&ctx, b * s * hidden * 4);
        defer output.free(&ctx);
        try run(&ctx, &blas, &kernels, workspace, output, inputs, b, s, h, r, boundary);
        const actual = try temp.alloc(f32, b * s * hidden);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (actual, 0..) |v, i| {
            try std.testing.expect(std.math.isFinite(v));
            if (i >= (b - 1) * s * hidden or (boundary and mask32[i / hidden] == 0)) try std.testing.expectEqual(@as(f32, 0), v);
        }
        // Native projection outputs bypass conversion staging entirely.
        // Test that route against the independently staged F32 inputs first.
        var native_inputs = inputs;
        native_inputs[0..5].* = @splat(.{});
        defer for (native_inputs[0..5]) |value| {
            var owned = value;
            owned.free(&ctx);
        };
        for (0..5) |j| {
            const quantized = try temp.alloc(u16, host[j].len);
            for (quantized, host[j]) |*x, y| x.* = @intCast(@as(u32, @bitCast(rounded(y))) >> 16);
            native_inputs[j] = try Buffer.alloc(&ctx, quantized.len * 2);
            try native_inputs[j].copyFromHost(&ctx, std.mem.sliceAsBytes(quantized));
        }
        try runWithNativeInputs(&ctx, &blas, &kernels, workspace, output, native_inputs, b, s, h, r, boundary, true);
        const direct = try temp.alloc(f32, actual.len);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(direct));
        try ctx.synchronize();
        for (actual, direct) |x, y| try std.testing.expectApproxEqAbs(x, y, 3e-4);
        const scores = try temp.alloc(f64, s);
        for (0..b - 1) |bi| for ([_]usize{ 0, s / 2, s - 1 }) |qi| for (0..h) |head| {
            var maximum: f64 = -std.math.inf(f64);
            for (scores, 0..) |*score, ki| {
                var cc_sum: f64 = 0;
                var cp_sum: f64 = 0;
                var pc_sum: f64 = 0;
                const bucket: @TypeOf(s) = @intCast(buckets[qi + s - 1 - ki]);
                for (0..64) |d| {
                    const qv: f64 = rounded(host[0][(bi * s + qi) * hidden + head * 64 + d]);
                    const kv: f64 = rounded(host[1][(bi * s + ki) * hidden + head * 64 + d]);
                    cc_sum += qv * kv;
                    cp_sum += qv * rounded(host[4][bucket * hidden + head * 64 + d]);
                    pc_sum += kv * rounded(host[3][bucket * hidden + head * 64 + d]);
                }
                const sum = @as(f64, rounded(@floatCast(cc_sum))) + rounded(@floatCast(cp_sum)) + rounded(@floatCast(pc_sum));
                score.* = if (mask32[bi * s + ki] == 0) -std.math.inf(f64) else sum / @sqrt(@as(f64, 192));
                maximum = @max(maximum, score.*);
            }
            var denominator: f64 = 0;
            for (scores) |*score| {
                score.* = @exp(score.* - maximum);
                denominator += score.*;
            }
            for (0..64) |d| {
                var expected: f64 = 0;
                if (!boundary or mask32[bi * s + qi] != 0) for (scores, 0..) |score, ki| {
                    expected += @as(f64, rounded(@floatCast(score / denominator))) * rounded(host[2][(bi * s + ki) * hidden + head * 64 + d]);
                };
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), actual[(bi * s + qi) * hidden + head * 64 + d], 3e-4);
            }
        };
    }
}

test "GLiNER BF16 attention ragged full profile aligns every grouped matrix" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    if (ctx.info.compute_major != 8 or ctx.info.compute_minor != 9) return error.SkipZigTest;
    var kernels = try Kernels.load(&ctx);
    defer kernels.unload(&ctx);
    var blas = try Blas.init(ctx.stream);
    defer blas.deinit();
    var reference: @import("gliner25_bf16_full.zig").Module = .{};
    try reference.init(&ctx);
    defer reference.deinit(&ctx);
    const heads = 12;
    const hidden = heads * 64;
    // These lengths expose both odd matrix strides and multiple head groups.
    for ([_]usize{ 1, 8 }) |batch| for ([_]usize{ 59, 239, 263, 511 }) |sequence| {
        const relative = 2 * sequence - 1;
        const p = planWithReference(batch, sequence, heads, 64, relative, false, true).?;
        try std.testing.expectEqual(@as(usize, 0), p.score_pitch % 8);
        try std.testing.expect(p.workspace_bytes <= workspace_limit);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const tokens = batch * sequence * hidden;
        var inputs: [7]Buffer = @splat(.{});
        defer for (&inputs) |*input| input.free(&ctx);
        for (0..5) |i| {
            const host = try temp.alloc(f32, if (i < 3) tokens else relative * hidden);
            @memset(host, 0);
            if (i == 2) for (host, 0..) |*value, index| {
                value.* = @as(f32, @floatFromInt(index / (sequence * hidden) * heads + index / 64 % heads)) / 8;
            };
            inputs[i] = try Buffer.alloc(&ctx, host.len * 4);
            try inputs[i].copyFromHost(&ctx, std.mem.sliceAsBytes(host));
        }
        const buckets = try temp.alloc(i32, relative);
        for (buckets, 0..) |*bucket, i| bucket.* = @intCast(i);
        inputs[5] = try Buffer.alloc(&ctx, relative * 4);
        try inputs[5].copyFromHost(&ctx, std.mem.sliceAsBytes(buckets));
        const mask = try temp.alloc(i32, batch * sequence);
        @memset(mask, 1);
        inputs[6] = try Buffer.alloc(&ctx, mask.len * 4);
        try inputs[6].copyFromHost(&ctx, std.mem.sliceAsBytes(mask));
        var workspace = try Buffer.alloc(&ctx, p.workspace_bytes);
        defer workspace.free(&ctx);
        var output = try Buffer.alloc(&ctx, tokens * 4);
        defer output.free(&ctx);
        try runWithReference(&ctx, &blas, &kernels, workspace, output, inputs, batch, sequence, heads, relative, true, false, &reference);
        const actual = try temp.alloc(f32, tokens);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        const probability = rounded(1 / @as(f32, @floatFromInt(sequence)));
        for (actual, 0..) |got, index| {
            const value = @as(f32, @floatFromInt(index / (sequence * hidden) * heads + index / 64 % heads)) / 8;
            const expected = value * probability * @as(f32, @floatFromInt(sequence));
            try std.testing.expectApproxEqAbs(expected, got, 0.0002);
        }
        if (batch == 1) {
            // Random nonzero Q/K and relative terms exercise the fused key
            // scaling, beyond the uniform-attention mapping oracle above.
            for (0..5) |operand| {
                const host = try temp.alloc(f32, if (operand < 3) tokens else relative * hidden);
                for (host, 0..) |*value, index| value.* = @sin(@as(f32, @floatFromInt(index % 211 + operand * 19)) * 0.03);
                try inputs[operand].copyFromHost(&ctx, std.mem.sliceAsBytes(host));
            }
            reference.fuse_views = false;
            try runWithReference(&ctx, &blas, &kernels, workspace, output, inputs, batch, sequence, heads, relative, true, false, &reference);
            const control = try temp.alloc(f32, tokens);
            try output.copyToHost(&ctx, std.mem.sliceAsBytes(control));
            try ctx.synchronize();
            reference.fuse_views = true;
            try runWithReference(&ctx, &blas, &kernels, workspace, output, inputs, batch, sequence, heads, relative, true, false, &reference);
            try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
            try ctx.synchronize();
            for (actual, control) |got, want| try std.testing.expectEqual(@as(u32, @bitCast(want)), @as(u32, @bitCast(got)));
        }
    };
}
