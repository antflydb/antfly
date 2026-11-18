// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const Kernels = @import("kernels.zig").KernelModule;
const Boundary = @import("../gliner_boundary_device_ops.zig");
test "GLiNER experimental banded head matches serial FP32 including padding and band edges" {
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
    for ([_][3]u32{ .{ 1, 3, 0 }, .{ 31, 16, 2 }, .{ 128, 32, 128 }, .{ 512, 32, 128 } }) |shape| {
        const n = shape[0];
        const d = shape[1];
        const dims = [8]u32{ 2, n, 4, d, shape[2], 0, 0, 0 };
        const layout = try Boundary.layoutFor(.banded_attention, dims, @splat(0));
        const host = try a.alloc(f32, layout.input_elements[0]);
        defer a.free(host);
        for (host, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i % 197)) * 0.17) * 0.7;
        var qkv = try Buffer.alloc(&ctx, host.len * 4);
        defer qkv.free(&ctx);
        try qkv.copyFromHost(&ctx, std.mem.sliceAsBytes(host));
        const lengths = [_]i32{ @intCast(if (n == 1) 1 else n - 1), 0 };
        var mask = try Buffer.alloc(&ctx, 8);
        defer mask.free(&ctx);
        try mask.copyFromHost(&ctx, std.mem.sliceAsBytes(&lengths));
        var expected = try Buffer.alloc(&ctx, layout.output_elements * 4);
        defer expected.free(&ctx);
        var actual = try Buffer.alloc(&ctx, layout.output_elements * 4);
        defer actual.free(&ctx);
        var inputs: [10]Buffer = @splat(.{});
        inputs[0] = qkv;
        inputs[1] = mask;
        const descriptor = Boundary.Params{ .kind = @backingInt(Boundary.Kind.banded_attention), .dims = dims, .scalars = @splat(0) };
        try std.testing.expect(!try kernels.launchGliner25Boundary(&ctx, inputs, expected, descriptor, layout.work_items, true, false, false));
        try std.testing.expect(try kernels.launchGliner25Boundary(&ctx, inputs, actual, descriptor, layout.work_items, true, false, true));
        const ref = try a.alloc(f32, layout.output_elements);
        defer a.free(ref);
        const got = try a.alloc(f32, layout.output_elements);
        defer a.free(got);
        try expected.copyToHost(&ctx, std.mem.sliceAsBytes(ref));
        try actual.copyToHost(&ctx, std.mem.sliceAsBytes(got));
        try ctx.synchronize();
        for (got, ref, 0..) |x, y, i| {
            try std.testing.expectApproxEqAbs(y, x, 3e-5);
            const row = i / (4 * d);
            if (row >= n or row == @as(usize, @intCast(lengths[0]))) try std.testing.expectEqual(@as(f32, 0), x);
        }
    }
}

test "GLiNER experimental parallel head reductions match serial and F64 ragged oracles" {
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
    for ([_]u32{ 256, 511, 512 }) |n| for ([_]u32{ 8, 9, 10 }) |kind| {
        const c: u32 = if (kind == 10) 64 else 3;
        const d: u32 = 128;
        const lengths = [_]i32{ @intCast(n - 17), 0 };
        const mask = [_]i32{ 1, 0, 1, 1, 1, 0 };
        const count = if (kind == 8) 2 * c * n else if (kind == 9) 2 * c * (n + 2) else 2 * c * (n + 1);
        const values = try a.alloc(f32, if (kind == 8) 2 * n * d else if (kind == 9) 2 * c * n else 2 * n * c);
        defer a.free(values);
        const queries = try a.alloc(f32, 2 * c * d);
        defer a.free(queries);
        for (values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i % 197)) * 0.13) * 0.03;
        for (queries, 0..) |*v, i| v.* = @cos(@as(f32, @floatFromInt(i % 173)) * 0.17) * 0.03;
        if (kind != 8) for (values, 0..) |*v, i| {
            const b = if (kind == 9) i / (c * n) else i / (n * c);
            const position = if (kind == 9) i % n else (i / c) % n;
            const enabled = kind != 9 or mask[i / n] != 0;
            if (!enabled or position >= @as(usize, @intCast(lengths[b]))) v.* = std.math.nan(f32);
        };
        const bytes = if (kind == 8)
            [_][]const u8{ std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(queries), std.mem.sliceAsBytes(&lengths), std.mem.sliceAsBytes(&mask) }
        else if (kind == 9)
            [_][]const u8{ std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(&lengths), std.mem.sliceAsBytes(&mask), &.{} }
        else
            [_][]const u8{ std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(&lengths), &.{}, &.{} };
        var inputs: [10]Buffer = @splat(.{});
        defer for (&inputs) |*buf| if (buf.ptr != 0) buf.free(&ctx);
        for (bytes, 0..) |data, i| if (data.len > 0) {
            inputs[i] = try Buffer.alloc(&ctx, data.len);
            try inputs[i].copyFromHost(&ctx, data);
        };
        var expected = try Buffer.alloc(&ctx, count * 4);
        defer expected.free(&ctx);
        var actual = try Buffer.alloc(&ctx, count * 4);
        defer actual.free(&ctx);
        const serial = try a.alloc(f32, count);
        defer a.free(serial);
        const got = try a.alloc(f32, count);
        defer a.free(got);
        for (0..if (kind == 8) @as(usize, 2) else 1) |include| {
            const descriptor = Boundary.Params{ .kind = kind, .dims = .{ 2, n, c, d, @intCast(include), 0, 0, 0 }, .scalars = @splat(0) };
            const work = if (kind == 8) count else 2 * c;
            try std.testing.expect(!try kernels.launchGliner25Boundary(&ctx, inputs, expected, descriptor, work, true, false, false));
            try std.testing.expect(try kernels.launchGliner25Boundary(&ctx, inputs, actual, descriptor, work, true, false, true));
            try expected.copyToHost(&ctx, std.mem.sliceAsBytes(serial));
            try actual.copyToHost(&ctx, std.mem.sliceAsBytes(got));
            try ctx.synchronize();
            for (got, serial) |x, y| {
                try std.testing.expect(std.math.isFinite(x));
                try std.testing.expectApproxEqAbs(y, x, 3e-5);
            }
            for (0..2 * c) |row| {
                const b = row / c;
                const channel = row % c;
                const length: usize = @intCast(if (kind == 9 and mask[row] == 0) 0 else lengths[b]);
                var mean: f64 = 0;
                if (kind == 9) {
                    for (0..length) |position| mean += values[row * n + position];
                    mean /= @floatFromInt(@max(length, 1));
                    try std.testing.expectApproxEqAbs(@as(f32, @floatCast(mean)), got[row * (n + 2) + n + 1], 3e-5);
                }
                var running: f64 = 0;
                for (0..n) |position| {
                    const index = if (kind == 8) row * n + position else if (kind == 9) row * (n + 2) + position + 1 else (b * (n + 1) + position + 1) * c + channel;
                    var oracle: f64 = 0;
                    if (kind == 8) {
                        if (mask[row] == 0 or position >= length + include) {
                            oracle = -10000;
                        } else {
                            for (0..d) |column| oracle += @as(f64, values[(b * n + position) * d + column]) * queries[row * d + column];
                            oracle /= @sqrt(@as(f64, @floatFromInt(d)));
                        }
                    } else {
                        if (position < length) running += if (kind == 9) @as(f64, values[row * n + position]) - mean else values[(b * n + position) * c + channel];
                        oracle = running;
                    }
                    try std.testing.expectApproxEqAbs(@as(f32, @floatCast(oracle)), got[index], 3e-5);
                }
                if (kind != 8) try std.testing.expectEqual(@as(f32, 0), got[if (kind == 9) row * (n + 2) else b * (n + 1) * c + channel]);
            }
        }
    };
}
