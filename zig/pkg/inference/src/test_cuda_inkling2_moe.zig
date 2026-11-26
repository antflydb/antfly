// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const checkpoint = @import("architectures/inkling2_checkpoint.zig");
const moe = @import("architectures/inkling2_moe_cuda.zig");
const Context = @import("ops/cuda/context.zig").CudaContext;
const Blas = @import("ops/cuda/cublaslt.zig").CublasLt;
const buffer = @import("ops/cuda/buffer.zig");
const Staging = @import("ops/cuda/pinned_input_arena.zig").Arena;

fn readBf16(bytes: []const u8, i: usize) f32 {
    return @bitCast(@as(u32, std.mem.readInt(u16, bytes[i * 2 ..][0..2], .little)) << 16);
}

test "CUDA Inkling 2 loads and executes both resident MoE formats" {
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_MODEL_DIR") orelse return error.Inkling2FixtureRequired;
    var source = try checkpoint.Checkpoint.open(std.testing.allocator, dir);
    defer source.deinit();
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var blas = try Blas.open(&ctx);
    defer blas.deinit();
    var staging: Staging = .{};
    defer staging.deinit(&ctx);
    var forward: moe.Forward = .{};
    defer forward.deinit(&ctx);
    const config = source.config.value.text_config;
    const row_bytes = @as(usize, config.hidden_size) * 2;
    var host = try buffer.HostBuffer.alloc(&ctx, row_bytes * 2);
    defer host.free(&ctx);
    var input = try buffer.DeviceBuffer.alloc(&ctx, row_bytes * 2);
    defer input.free(&ctx);
    var output = try buffer.DeviceBuffer.alloc(&ctx, input.len);
    defer output.free(&ctx);
    const embed = try source.reader.rawTensorBytes("model.llm.embed.weight");
    @memcpy(host.bytes()[0..row_bytes], embed[(42 * row_bytes)..][0..row_bytes]);
    @memset(host.bytes()[row_bytes..], 0);
    try input.copyFromHost(&ctx, host.constBytes());
    try ctx.synchronize();

    var blas_space = try buffer.DeviceBuffer.alloc(&ctx, 8 * 1024 * 1024);
    defer blas_space.free(&ctx);
    var router_output = try buffer.HostBuffer.alloc(&ctx, 2 * @as(usize, config.n_routed_experts) * 4);
    defer router_output.free(&ctx);
    for ([_]usize{ 0, 20 }) |number| {
        var layer = try moe.Layer.load(&ctx, &source, &staging, number);
        defer layer.deinit(&ctx);
        const sizes = try moe.scratchBytes(layer.format, config, 2);
        var logits = try buffer.DeviceBuffer.alloc(&ctx, sizes.logits);
        defer logits.free(&ctx);
        var scratch = try buffer.DeviceBuffer.alloc(&ctx, sizes.routed);
        defer scratch.free(&ctx);
        try forward.run(&ctx, &blas, config, &layer, 2, input, output, .{ .logits = logits, .blas = blas_space, .routed = scratch });
        try logits.copyToHost(&ctx, router_output.bytes());
        try output.copyToHost(&ctx, host.bytes());
        try ctx.synchronize();
        const router = try source.router(number);
        const embed_row = embed[(42 * row_bytes)..][0..row_bytes];
        for (0..config.n_routed_experts) |expert| {
            var expected: f64 = 0;
            for (0..config.hidden_size) |i|
                expected += @as(f64, readBf16(embed_row, i)) * readBf16(router.weight, expert * @as(usize, config.hidden_size) + i);
            const bits = std.mem.readInt(u32, router_output.constBytes()[expert * 4 ..][0..4], .little);
            const actual: f32 = @bitCast(bits);
            try std.testing.expectApproxEqAbs(expected, @as(f64, actual), 0.0001);
            const zero = std.mem.readInt(u32, router_output.constBytes()[(@as(usize, config.n_routed_experts) + expert) * 4 ..][0..4], .little);
            try std.testing.expectEqual(@as(u32, 0), zero & 0x7fff_ffff);
        }
        var nonzero: usize = 0;
        for (0..config.hidden_size) |i| {
            const bits = std.mem.readInt(u16, host.constBytes()[i * 2 ..][0..2], .little);
            try std.testing.expect(bits & 0x7f80 != 0x7f80);
            nonzero += @intFromBool(bits & 0x7fff != 0);
            const zero = std.mem.readInt(u16, host.constBytes()[row_bytes + i * 2 ..][0..2], .little);
            try std.testing.expectEqual(@as(u16, 0), zero & 0x7fff);
        }
        try std.testing.expect(nonzero > config.hidden_size / 2);
        std.debug.print("Inkling resident MoE layer={d} format={s} device={s} nonzero={d}/{d}; zero row verified\n", .{
            number, @tagName(layer.format), ctx.info.nameSlice(), nonzero, config.hidden_size,
        });
    }
}

test "CUDA Inkling 2 loads and executes same-expert groups with identical BF16 output" {
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_MODEL_DIR") orelse return error.Inkling2FixtureRequired;
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    if (ctx.info.compute_major != 12 or ctx.info.compute_minor != 1) return error.SkipZigTest;
    var source = try checkpoint.Checkpoint.open(std.testing.allocator, dir);
    defer source.deinit();
    var blas = try Blas.open(&ctx);
    defer blas.deinit();
    var staging: Staging = .{};
    defer staging.deinit(&ctx);
    var forward: moe.Forward = .{};
    defer forward.deinit(&ctx);
    const config = source.config.value.text_config;
    const row_bytes = @as(usize, config.hidden_size) * 2;
    const embed = try source.reader.rawTensorBytes("model.llm.embed.weight");
    var blas_space = try buffer.DeviceBuffer.alloc(&ctx, 8 * 1024 * 1024);
    defer blas_space.free(&ctx);
    const pair = try ctx.createProfileEventPair();
    defer ctx.destroyProfileEventPair(pair);

    for ([_]usize{ 0, 20 }) |number| {
        var layer = try moe.Layer.load(&ctx, &source, &staging, number);
        defer layer.deinit(&ctx);
        try forward.prepareResident(&ctx, config, &layer);
        for ([_]u32{ 16, 32, 64, 128, 256, 512, 1024, 2048 }) |rows| {
            const bytes = rows * row_bytes;
            var host = try buffer.HostBuffer.alloc(&ctx, bytes * 2);
            defer host.free(&ctx);
            var input = try buffer.DeviceBuffer.alloc(&ctx, bytes);
            defer input.free(&ctx);
            var output = try buffer.DeviceBuffer.alloc(&ctx, bytes);
            defer output.free(&ctx);
            const sizes = try moe.scratchBytes(layer.format, config, rows);
            var logits = try buffer.DeviceBuffer.alloc(&ctx, sizes.logits);
            defer logits.free(&ctx);
            var routed = try buffer.DeviceBuffer.alloc(&ctx, sizes.routed);
            defer routed.free(&ctx);
            for ([_]moe.ActivationPrecision{ .bf16, .block_scaled }) |precision| {
                if (precision == .block_scaled and rows != 64 and rows != 512 and rows != 2048) continue;
                const scratch: moe.Scratch = .{ .activation_precision = precision, .logits = logits, .blas = blas_space, .routed = routed };
                for ([_]bool{ false, true }) |repeat_tokens| {
                    // Repeated rows force an expert to use multiple groups,
                    // including a partially filled group alongside the unique rows.
                    for (0..rows) |row| {
                        const token = if (repeat_tokens and row < rows - 5) 42 else 100 + row * 29;
                        @memcpy(host.bytes()[row * row_bytes ..][0..row_bytes], embed[token * row_bytes ..][0..row_bytes]);
                    }
                    try input.copyFromHost(&ctx, host.constBytes()[0..bytes]);
                    try ctx.synchronize();
                    var elapsed: [2]u64 = undefined;
                    for ([_]bool{ true, false }, &elapsed, 0..) |single_route, *us, index| {
                        forward.projections.use_single_route = single_route;
                        try forward.run(&ctx, &blas, config, &layer, rows, input, output, scratch);
                        try ctx.recordProfileStart(pair);
                        const samples: u64 = if (rows >= 1024) 3 else 12;
                        for (0..samples) |_| try forward.run(&ctx, &blas, config, &layer, rows, input, output, scratch);
                        try ctx.recordProfileEnd(pair);
                        try output.copyToHost(&ctx, host.bytes()[index * bytes ..][0..bytes]);
                        try ctx.synchronize();
                        us.* = try ctx.readProfileEventPairUs(pair) / samples;
                    }
                    try std.testing.expectEqualSlices(u8, host.constBytes()[0..bytes], host.constBytes()[bytes..]);
                    std.debug.print("{d}-row {s} {s} FFN ({s} tokens): independent {d} us, grouped {d} us, bit-identical BF16 output\n", .{
                        rows, @tagName(layer.format), @tagName(precision), if (repeat_tokens) "mixed repeated and distinct" else "distinct", elapsed[0], elapsed[1],
                    });
                }
            }
        }
    }
}
