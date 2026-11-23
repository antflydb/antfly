// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const Kernels = @import("kernels.zig").KernelModule;
const Boundary = @import("../gliner_boundary_device_ops.zig");
test "GLiNER BF16 norm cache denial preserves full BF16 math" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const Cuda = @import("cuda_compute.zig");
    const Memory = @import("../../runtime/tier/memory.zig");
    var compute: Cuda.CudaCompute = undefined;
    compute.ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer compute.ctx.deinit();
    if (compute.ctx.info.compute_major != 8 or compute.ctx.info.compute_minor != 9) return error.SkipZigTest;
    compute.kernels = try Kernels.load(&compute.ctx);
    defer compute.kernels.unload(&compute.ctx);
    compute.gliner_deberta_bf16 = true;
    compute.gliner_bf16_reference = true;
    compute.gliner_bf16_full = .{ .flags = 0 };
    try compute.gliner_bf16_full.init(&compute.ctx);
    defer compute.gliner_bf16_full.deinit(&compute.ctx);
    compute.gliner_relative_cache = .{};
    defer compute.gliner_relative_cache.deinit(std.testing.allocator, &compute.ctx);
    compute.stats = .{};
    var controller = Memory.AdmissionController{};
    compute.ctx.video_cache_budget.admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1 }, .check_live_memory = false };
    var host: [64]f32 = undefined;
    var inputs: [4]Buffer = @splat(.{});
    defer for (&inputs) |*input| input.free(&compute.ctx);
    for (&inputs, 0..) |*input, j| {
        for (&host, 0..) |*value, i| value.* = if (j == 2) 1 else @sin(@as(f32, @floatFromInt(i + j * 5)) * 0.13);
        input.* = try Buffer.alloc(&compute.ctx, 64 * 4);
        try input.copyFromHost(&compute.ctx, std.mem.sliceAsBytes(&host));
    }
    var output = try Buffer.alloc(&compute.ctx, 64 * 4);
    defer output.free(&compute.ctx);
    var reference = try Buffer.alloc(&compute.ctx, 64 * 4);
    defer reference.free(&compute.ctx);
    for (0..4) |flavor| {
        const result = (try Cuda.gliner25NormMirror(&compute, output, inputs, 1, 64, 1e-5, @intCast(flavor))).?;
        try std.testing.expect(result.generation == null);
        try compute.gliner_bf16_full.norm(&compute.ctx, reference, .{}, .{ inputs[0], if (flavor == 1 or flavor == 2) inputs[1] else .{}, inputs[2], inputs[3] }, 1, 64, 1e-5);
        var expected: [64]f32 = undefined;
        try output.copyToHost(&compute.ctx, std.mem.sliceAsBytes(&host));
        try reference.copyToHost(&compute.ctx, std.mem.sliceAsBytes(&expected));
        try compute.ctx.synchronize();
        try std.testing.expectEqualSlices(f32, &expected, &host);
    }
    try std.testing.expectEqual(@as(u64, 0), controller.snapshot().backend_weight_bytes);
    compute.gliner_bf16_reference = false;
    try std.testing.expect((try Cuda.gliner25NormMirror(&compute, output, inputs, 1, 64, 1e-5, 0)) == null);
}

test "GLiNER BF16 norm mirror preserves F32 results and RNE bits for every reduction" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    if (ctx.info.compute_major != 8 or ctx.info.compute_minor != 9) return error.SkipZigTest;
    var kernels = try Kernels.load(&ctx);
    defer kernels.unload(&ctx);
    const a = std.testing.allocator;
    for ([_][2]usize{ .{ 1, 64 }, .{ 7, 128 }, .{ 128, 768 }, .{ 128, 1024 } }) |shape| {
        const rows = shape[0];
        const width = shape[1];
        const count = rows * width;
        const host = try a.alloc(f32, count);
        defer a.free(host);
        var inputs: [4]Buffer = @splat(.{});
        defer for (&inputs) |*value| value.free(&ctx);
        for (&inputs, 0..) |*value, j| {
            const n = if (j < 2) count else width;
            for (host[0..n], 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i % 197 + j * 11)) * 0.13) * 0.75 + (if (j == 2) @as(f32, 1) else 0.0625);
            value.* = try Buffer.alloc(&ctx, n * 4);
            try value.copyFromHost(&ctx, std.mem.sliceAsBytes(host[0..n]));
        }
        var reference = try Buffer.alloc(&ctx, count * 4);
        defer reference.free(&ctx);
        var result = try Buffer.alloc(&ctx, count * 4);
        defer result.free(&ctx);
        var mirror = try Buffer.alloc(&ctx, count * 2);
        defer mirror.free(&ctx);
        const expected = try a.alloc(f32, count);
        defer a.free(expected);
        const actual = try a.alloc(f32, count);
        defer a.free(actual);
        const bits = try a.alloc(u16, count);
        defer a.free(bits);
        for (0..4) |flavor| {
            if (flavor < 2) {
                var refs: [10]Buffer = @splat(.{});
                refs[0] = inputs[0];
                if (flavor == 0) {
                    refs[1] = inputs[2];
                    refs[2] = inputs[3];
                } else {
                    var add_refs: [10]Buffer = @splat(.{});
                    add_refs[0] = inputs[0];
                    add_refs[1] = inputs[1];
                    const add_desc = Boundary.Params{ .kind = @backingInt(Boundary.Kind.add), .dims = .{ @intCast(count), 0, 0, 0, 0, 0, 0, 0 }, .scalars = @splat(0) };
                    _ = try kernels.launchGliner25Boundary(&ctx, add_refs, reference, add_desc, count, false, false, false);
                    refs[0] = reference;
                    refs[1] = inputs[2];
                    refs[2] = inputs[3];
                }
                const descriptor = Boundary.Params{ .kind = @backingInt(Boundary.Kind.norm), .dims = .{ @intCast(rows), @intCast(width), 0, 0, 0, 0, 0, 0 }, .scalars = .{ 1e-5, 0, 0, 0 } };
                _ = try kernels.launchGliner25Boundary(&ctx, refs, reference, descriptor, rows * 32, false, false, false);
            } else if (flavor == 2) {
                try kernels.launchAddLayerNormF32(&ctx, reference, inputs[0], inputs[1], inputs[2], inputs[3], rows, width, 1e-5);
            } else try kernels.launchLayerNormF32(&ctx, reference, inputs[0], inputs[2], inputs[3], rows, width, 1e-5);
            if (flavor == 1) {
                var fallback_refs: [10]Buffer = @splat(.{});
                @memcpy(fallback_refs[0..4], &inputs);
                const fallback = Boundary.Params{ .kind = @backingInt(Boundary.Kind.add_norm_centered), .dims = .{ @intCast(rows), @intCast(width), 0, 0, 0, 0, 0, 0 }, .scalars = .{ 1e-5, 0, 0, 0 } };
                _ = try kernels.launchGliner25Boundary(&ctx, fallback_refs, result, fallback, rows * 32, false, false, false);
                try reference.copyToHost(&ctx, std.mem.sliceAsBytes(expected));
                try result.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
                try ctx.synchronize();
                for (actual, expected) |got, want| try std.testing.expectEqual(@as(u32, @bitCast(want)), @as(u32, @bitCast(got)));
            }
            try kernels.launchGliner25NormBf16Mirror(&ctx, result, mirror, inputs, rows, width, 1e-5, @intCast(flavor));
            try reference.copyToHost(&ctx, std.mem.sliceAsBytes(expected));
            try result.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
            try mirror.copyToHost(&ctx, std.mem.sliceAsBytes(bits));
            try ctx.synchronize();
            for (actual, expected, bits) |got, want, rounded| {
                if (@as(u32, @bitCast(want)) != @as(u32, @bitCast(got))) std.debug.print("norm mirror mismatch rows={d} width={d} flavor={d}\n", .{ rows, width, flavor });
                try std.testing.expectEqual(@as(u32, @bitCast(want)), @as(u32, @bitCast(got)));
                const raw: u32 = @bitCast(want);
                try std.testing.expectEqual(@as(u16, @intCast((raw + 0x7fff + ((raw >> 16) & 1)) >> 16)), rounded);
            }
        }
    }
}

test "GLiNER BF16 normalization and dot+bias agree with the reference" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    if (ctx.info.compute_major != 8 or ctx.info.compute_minor != 9) return error.SkipZigTest;
    var module = @import("gliner25_bf16_full.zig").Module{ .flags = @import("../../models/gliner25_bf16_policy.zig").qualified_flags };
    try module.init(&ctx);
    defer module.deinit(&ctx);

    const rows = 3;
    const width = 1024;
    const count = rows * width;
    var input: [count]f32 = undefined;
    var dot: [count]u16 = undefined;
    for (&input, &dot, 0..) |*x, *d, index| {
        x.* = @as(f32, @floatFromInt(index / width)) + (if (index % 256 < 128) @as(f32, 2) else 6);
        d.* = @truncate(@as(u32, @bitCast(x.* - 0.25)) >> 16);
    }
    const residual: [count]f32 = @splat(0.5);
    const bias: [width]f32 = @splat(0.25);
    const gamma: [width]f32 = @splat(1);
    const beta: [width]f32 = @splat(0);
    const host = [_][]const u8{ std.mem.asBytes(&input), std.mem.asBytes(&dot), std.mem.asBytes(&residual), std.mem.asBytes(&bias), std.mem.asBytes(&gamma), std.mem.asBytes(&beta) };
    var buffers: [host.len]Buffer = @splat(.{});
    defer for (&buffers) |*buffer| buffer.free(&ctx);
    for (&buffers, host) |*buffer, values| {
        buffer.* = try Buffer.alloc(&ctx, values.len);
        try buffer.copyFromHost(&ctx, values);
    }

    var output = try Buffer.alloc(&ctx, count * @sizeOf(f32));
    defer output.free(&ctx);
    var mirror = try Buffer.alloc(&ctx, count * @sizeOf(u16));
    defer mirror.free(&ctx);
    for ([_]bool{ false, true }) |fused_bias| {
        if (fused_bias) {
            try module.normDotBias(&ctx, output, mirror, .{ buffers[1], buffers[3], buffers[2], buffers[4], buffers[5] }, rows, width, 1e-5);
        } else {
            try module.norm(&ctx, output, mirror, .{ buffers[0], buffers[2], buffers[4], buffers[5] }, rows, width, 1e-5);
        }
        var actual: [count]f32 = undefined;
        var bits: [count]u16 = undefined;
        try output.copyToHost(&ctx, std.mem.asBytes(&actual));
        try mirror.copyToHost(&ctx, std.mem.asBytes(&bits));
        try ctx.synchronize();
        for (actual, bits, 0..) |value, word, index| {
            // Each row has two equally sized populations separated by four.
            // Normalizing with epsilon and rounding to BF16 gives +/-1.
            const expected: f32 = if (index % 256 < 128) -1 else 1;
            try std.testing.expectEqual(expected, value);
            try std.testing.expectEqual(@as(u16, @truncate(@as(u32, @bitCast(expected)) >> 16)), word);
        }
    }
}
