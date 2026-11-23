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
const buffer = @import("ops/cuda/buffer.zig");
const Context = @import("ops/cuda/context.zig").CudaContext;
const kv = @import("ops/cuda/hybrid_paged_kv.zig");
const attention = @import("ops/cuda/paged_relative_attention.zig");
const convolution = @import("ops/cuda/causal_depthwise4_bf16.zig");
const normalization = @import("ops/cuda/head_norm_bf16.zig");

const q_heads = 24;
const kv_heads = 8;
const head_dim = 128;
const rows = 2;
const relative_extent = 17;
const sliding_window = 133;
const model_scale = 1.0 / @sqrt(@as(f32, 128.0));

fn bf16(value: f32) u16 {
    const bits: u32 = @bitCast(value);
    return @truncate((bits +% 0x7fff +% ((bits >> 16) & 1)) >> 16);
}

fn fromBits(bits: u16) f32 {
    return @bitCast(@as(u32, bits) << 16);
}

fn sample(a: usize, b: usize, c: usize, salt: usize) f32 {
    const x: i32 = @as(i32, @intCast((a * 17 + b * 13 + c * 7 + a * c * 3 + salt) % 29)) - 14;
    return @as(f32, @floatFromInt(x)) / 32;
}

fn upload(ctx: *Context, bytes: []const u8) !buffer.DeviceBuffer {
    var host = try buffer.HostBuffer.alloc(ctx, bytes.len);
    defer host.free(ctx);
    @memcpy(host.bytes(), bytes);
    var result = try buffer.DeviceBuffer.alloc(ctx, bytes.len);
    errdefer result.free(ctx);
    try result.copyFromHost(ctx, host.constBytes());
    try ctx.synchronize();
    return result;
}

test "CUDA four-tap convolution and grouped BF16 normalization match separate launches" {
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var stencil: convolution.Module = .{};
    defer stencil.deinit(&ctx);
    var norm: normalization.Module = .{};
    defer norm.deinit(&ctx);

    const heads = 2;
    const channels = heads * head_dim;
    var input: [rows * channels]u16 = undefined;
    var weights: [4 * channels]u16 = undefined;
    var history: [3 * channels]u16 = undefined;
    var dynamic: [rows * heads * 4]u16 = undefined;
    for (&input, 0..) |*v, i| v.* = bf16(sample(i / channels, i / head_dim % heads, i % head_dim, 0));
    for (&weights, 0..) |*v, i| v.* = bf16(sample(i % 4, i / 4 / head_dim % heads, i / 4 % head_dim, 2));
    for (&history, 0..) |*v, i| v.* = bf16(sample(i / channels, i / head_dim % heads, i % head_dim, 4));
    for (&dynamic, 0..) |*v, i| v.* = bf16(sample(i / (heads * 4), i / 4 % heads, i % 4, 6));
    var sources: [4]buffer.DeviceBuffer = @splat(.{});
    defer for (&sources) |*device| device.free(&ctx);
    inline for (&sources, .{ &input, &weights, &history, &dynamic }) |*device, source| {
        device.* = try upload(&ctx, std.mem.sliceAsBytes(source));
    }
    const io_bytes = @sizeOf(@TypeOf(input));
    const history_bytes = @sizeOf(@TypeOf(history));
    var output: [5]buffer.DeviceBuffer = @splat(.{});
    defer for (&output) |*device| device.free(&ctx);
    for (&output, [_]usize{ io_bytes, io_bytes, history_bytes, io_bytes, history_bytes }) |*device, len| {
        device.* = try buffer.DeviceBuffer.alloc(&ctx, len);
    }
    var readback = try buffer.HostBuffer.alloc(&ctx, 2 * (io_bytes + history_bytes));
    defer readback.free(&ctx);
    for ([_]buffer.DeviceBuffer{ .{}, sources[3] }) |taps| {
        for ([_]f32{ 0, 1.25 }) |clip| {
            for ([_]u32{ 0, 1, 3 }) |valid| {
                var request: convolution.Request = .{
                    .input = sources[0],
                    .weight = sources[1],
                    .history = if (valid == 0) .{} else sources[2],
                    .dynamic_taps = taps,
                    .output = output[0],
                    .next_history = output[2],
                    .rows = rows,
                    .channels = channels,
                    .head_size = head_dim,
                    .valid_history = valid,
                    .output_scale = 1.0 / @sqrt(@as(f32, 5)),
                };
                try stencil.run(&ctx, request);
                try norm.run(&ctx, .{
                    .input = output[0],
                    .output = output[1],
                    .rows = rows,
                    .heads = heads,
                    .dim = head_dim,
                    .position = valid,
                    .clip = clip,
                    .eps = 1e-6,
                });
                request.output = output[3];
                request.next_history = output[4];
                try stencil.runHeadNorm(&ctx, request, clip, 1e-6);
                const stride = io_bytes + history_bytes;
                for (0..2) |side| {
                    try output[1 + 2 * side].copyToHost(&ctx, readback.bytes()[side * stride ..][0..io_bytes]);
                    try output[2 + 2 * side].copyToHost(&ctx, readback.bytes()[side * stride + io_bytes ..][0..history_bytes]);
                }
                try ctx.synchronize();
                try std.testing.expectEqualSlices(u8, readback.constBytes()[0..stride], readback.constBytes()[stride..]);
            }
        }
    }

    const relative_dim = 16;
    const relative_bytes = rows * heads * relative_dim * 2;
    var query: normalization.Request = .{
        .input = sources[0],
        .output = output[0],
        .gamma = sources[1],
        .rows = rows,
        .heads = heads,
        .dim = head_dim,
        .position = 8,
        .log_floor = 4,
        .log_alpha = 0.5,
        .clip = 1.25,
        .eps = 1e-6,
    };
    var relative = query;
    relative.output = output[1];
    relative.dim = relative_dim;
    relative.clip = 0.75;
    try norm.run(&ctx, query);
    try norm.run(&ctx, relative);
    query.output = output[2];
    relative.output = output[3];
    try norm.runPair(&ctx, query, relative);
    const stride = io_bytes + relative_bytes;
    for (0..2) |side| {
        try output[2 * side].copyToHost(&ctx, readback.bytes()[side * stride ..][0..io_bytes]);
        try output[2 * side + 1].copyToHost(&ctx, readback.bytes()[side * stride + io_bytes ..][0..relative_bytes]);
    }
    try ctx.synchronize();
    try std.testing.expectEqualSlices(u8, readback.constBytes()[0..stride], readback.constBytes()[stride .. 2 * stride]);
}

fn stage(cache: *kv.Cache, ctx: *Context, count: usize) !buffer.DeviceBuffer {
    const allocator = std.testing.allocator;
    const elements = count * kv_heads * head_dim;
    const data = try allocator.alloc(u16, 2 * elements);
    defer allocator.free(data);
    const start = cache.committedTokens();
    for (0..count) |row| for (0..kv_heads) |head| for (0..head_dim) |d| {
        const index = (row * kv_heads + head) * head_dim + d;
        data[index] = bf16(sample(start + row, head, d, 0));
        data[elements + index] = bf16(sample(start + row, head, d, 9));
    };
    var device = try upload(ctx, std.mem.sliceAsBytes(data));
    errdefer {
        ctx.synchronize() catch {};
        device.free(ctx);
    }
    const plane_bytes = elements * @sizeOf(u16);
    const planes: kv.Planes = .{
        .key = .{ .ptr = device.ptr, .len = plane_bytes, .borrowed = true },
        .value = .{ .ptr = device.ptr + plane_bytes, .len = plane_bytes, .borrowed = true },
    };
    try cache.prepareAppend(count);
    try cache.queueWrite(.{ .group = .full, .slot = 0 }, planes);
    try cache.queueWrite(.{ .group = .sliding, .slot = 0 }, planes);
    return device;
}

test "CUDA paged relative attention reads staged BF16 cache with GQA sink and gate" {
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var cache: kv.Cache = undefined;
    try cache.init(std.testing.allocator, &ctx, .{
        .layout = .{ .page_tokens = 16, .window_tokens = sliding_window, .max_tokens = 512, .max_append_tokens = 128 },
        .full_slots = 1,
        .sliding_slots = 1,
        .kv_heads = kv_heads,
        .head_dim = head_dim,
    });
    defer cache.deinit() catch @panic("unable to drain paged relative attention fixture");
    for ([_]usize{ 1, 128, 128, 16 }) |count| {
        var source = try stage(&cache, &ctx, count);
        defer source.free(&ctx);
        try cache.commit();
    }
    const start = cache.committedTokens();
    var source = try stage(&cache, &ctx, rows);
    defer source.free(&ctx);
    errdefer ctx.synchronize() catch {};

    var q: [rows * q_heads * head_dim]u16 = undefined;
    var gate: [q.len]u16 = undefined;
    var relative: [rows * q_heads * relative_extent]u16 = undefined;
    var sinks: [rows * q_heads]u16 = undefined;
    for (0..rows) |row| for (0..q_heads) |head| {
        const query_head = row * q_heads + head;
        for (0..head_dim) |d| {
            q[query_head * head_dim + d] = bf16(sample(start + row, head, d, 1));
            gate[query_head * head_dim + d] = bf16(sample(start + row, head, d, 11));
        }
        for (0..relative_extent) |distance|
            relative[query_head * relative_extent + distance] = bf16(sample(row, head, distance, 7) * 4);
        sinks[query_head] = bf16(sample(row, head, 0, 3) * 16);
    };
    var q_device = try upload(&ctx, std.mem.sliceAsBytes(&q));
    defer q_device.free(&ctx);
    var gate_device = try upload(&ctx, std.mem.sliceAsBytes(&gate));
    defer gate_device.free(&ctx);
    var relative_device = try upload(&ctx, std.mem.sliceAsBytes(&relative));
    defer relative_device.free(&ctx);
    var sink_device = try upload(&ctx, std.mem.sliceAsBytes(&sinks));
    defer sink_device.free(&ctx);
    var host = try buffer.HostBuffer.alloc(&ctx, q.len * 2);
    defer host.free(&ctx);
    var result = try buffer.DeviceBuffer.alloc(&ctx, host.len);
    defer result.free(&ctx);
    var module: attention.Module = .{};
    defer module.deinit(&ctx);

    for ([_]kv.Group{ .full, .sliding }) |group| {
        var ids: [32]u32 = undefined;
        const binding = try cache.stagedView(.{ .group = group, .slot = 0 }, &ids);
        if (group == .sliding) try std.testing.expect(binding.first_page > 0);
        var pages = try upload(&ctx, std.mem.sliceAsBytes(ids[0..binding.page_count]));
        defer pages.free(&ctx);
        const geometry: attention.Query = .{
            .rows = rows,
            .start = @intCast(start),
            .q_heads = q_heads,
            .kv_heads = kv_heads,
            .window = if (group == .sliding) sliding_window else 0,
            .relative_extent = relative_extent,
            .score_scale = model_scale,
        };
        var scratch = try buffer.DeviceBuffer.alloc(&ctx, try attention.scratchBytes(geometry, binding.visible_start, binding.visible_end));
        defer scratch.free(&ctx);
        var request: attention.Request = .{
            .geometry = geometry,
            .kv = binding,
            .device_page_ids = pages,
            .query = q_device,
            .relative = relative_device,
            .sinks = sink_device,
            .gate = gate_device,
            .scratch = scratch,
            .output = result,
        };
        try module.run(&ctx, request);
        try result.copyToHost(&ctx, host.bytes());
        try ctx.synchronize();

        for (0..rows) |row| for (0..q_heads) |head| {
            const query_head = row * q_heads + head;
            const kv_head = head / (q_heads / kv_heads);
            const position = start + row;
            const first = if (group == .sliding) position + 1 - sliding_window else 0;
            var denominator = @exp(@as(f64, fromBits(sinks[query_head])));
            var numerators: [head_dim]f64 = @splat(0);
            for (first..position + 1) |key_pos| {
                var dot: f64 = 0;
                for (0..head_dim) |d|
                    dot += @as(f64, fromBits(q[query_head * head_dim + d])) * sample(key_pos, kv_head, d, 0);
                const distance = position - key_pos;
                const bias: f64 = if (distance < relative_extent) fromBits(relative[query_head * relative_extent + distance]) else 0;
                const weight = @exp(dot * model_scale + bias);
                denominator += weight;
                for (&numerators, 0..) |*num, d| num.* += weight * sample(key_pos, kv_head, d, 9);
            }
            for (numerators, 0..) |num, d| {
                const index = query_head * head_dim + d;
                const expected = num / denominator / (1 + @exp(-@as(f64, fromBits(gate[index]))));
                const actual = fromBits(std.mem.readInt(u16, host.constBytes()[index * 2 ..][0..2], .little));
                try std.testing.expectApproxEqAbs(expected, @as(f64, actual), 0.0005);
            }
        };

        if (ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1) {
            // The final row uses the same split boundaries when run on its own.
            const row_bytes = q_heads * head_dim * 2;
            const relative_bytes = q_heads * relative_extent * 2;
            const sink_bytes = q_heads * 2;
            var expected: [row_bytes]u8 = undefined;
            @memcpy(&expected, host.constBytes()[row_bytes..]);
            request.geometry.rows = 1;
            request.geometry.start += 1;
            request.query = deviceSlice(q_device, row_bytes, row_bytes);
            request.gate = deviceSlice(gate_device, row_bytes, row_bytes);
            request.relative = deviceSlice(relative_device, relative_bytes, relative_bytes);
            request.sinks = deviceSlice(sink_device, sink_bytes, sink_bytes);
            request.output = deviceSlice(result, row_bytes, row_bytes);
            try module.run(&ctx, request);
            try request.output.copyToHost(&ctx, host.bytes()[row_bytes..]);
            try ctx.synchronize();
            try std.testing.expectEqualSlices(u8, &expected, host.constBytes()[row_bytes..]);
        }
    }
    try cache.commit();
    try std.testing.expectEqual(start + rows, cache.committedTokens());
}

fn deviceSlice(parent: buffer.DeviceBuffer, offset: usize, len: usize) buffer.DeviceBuffer {
    return .{ .ptr = parent.ptr + offset, .len = len, .borrowed = true };
}

test "CUDA four-tap convolution preserves its committed BF16 history on abort" {
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var module: convolution.Module = .{};
    defer module.deinit(&ctx);
    const tokens = 7;
    const channels = 16;
    const head_size = 4;
    const heads = channels / head_size;
    const row_bytes = channels * 2;
    var x: [tokens * channels]u16 = undefined;
    var taps: [channels * 4]u16 = undefined;
    var dynamic: [tokens * heads * 4]u16 = undefined;
    for (&x, 0..) |*value, i| value.* = bf16(sample(i / channels, 0, i % channels, 2));
    for (&taps, 0..) |*value, i| value.* = bf16(sample(i / 4, 1, i % 4, 9));
    for (&dynamic, 0..) |*value, i| value.* = bf16(sample(i / (heads * 4), i / 4 % heads, i % 4, 5));
    var x_device = try upload(&ctx, std.mem.sliceAsBytes(&x));
    defer x_device.free(&ctx);
    var taps_device = try upload(&ctx, std.mem.sliceAsBytes(&taps));
    defer taps_device.free(&ctx);
    var dynamic_device = try upload(&ctx, std.mem.sliceAsBytes(&dynamic));
    defer dynamic_device.free(&ctx);
    const zeroes: [3 * row_bytes]u8 = @splat(0);
    var poison = try upload(&ctx, &zeroes);
    defer poison.free(&ctx);
    var output = try buffer.DeviceBuffer.alloc(&ctx, tokens * row_bytes);
    defer output.free(&ctx);
    var history = try buffer.DeviceBuffer.alloc(&ctx, 3 * row_bytes);
    defer history.free(&ctx);
    var candidate = try buffer.DeviceBuffer.alloc(&ctx, 3 * row_bytes);
    defer candidate.free(&ctx);
    var host = try buffer.HostBuffer.alloc(&ctx, output.len);
    defer host.free(&ctx);

    for ([_]bool{ false, true }) |use_dynamic| {
        const scale: f32 = if (use_dynamic) 1.0 / 3.0 else 1.0 / @sqrt(@as(f32, 5.0));
        var offset: usize = 0;
        for ([_]usize{ 1, 2, 1, 3 }) |count| {
            var request: convolution.Request = .{
                .input = deviceSlice(x_device, offset * row_bytes, count * row_bytes),
                .weight = taps_device,
                .history = history,
                .dynamic_taps = if (use_dynamic) deviceSlice(dynamic_device, offset * heads * 8, count * heads * 8) else .{},
                .output = deviceSlice(output, offset * row_bytes, count * row_bytes),
                .next_history = candidate,
                .rows = @intCast(count),
                .channels = channels,
                .head_size = head_size,
                .valid_history = @intCast(@min(3, offset)),
                .output_scale = scale,
            };
            if (offset == 3) {
                const original = request.input;
                request.input = poison;
                try module.run(&ctx, request);
                // On abort, retain the original history rather than swapping.
                request.input = original;
            }
            try module.run(&ctx, request);
            std.mem.swap(buffer.DeviceBuffer, &history, &candidate);
            offset += count;
        }
        try output.copyToHost(&ctx, host.bytes());
        try ctx.synchronize();
        for (0..tokens) |row| for (0..channels) |c| {
            var sum: f32 = 0;
            for (0..4) |tap| {
                const lag = if (use_dynamic) tap else 3 - tap;
                if (lag > row) continue;
                var weight = fromBits(taps[if (use_dynamic) tap * channels + c else c * 4 + tap]);
                if (use_dynamic) weight += fromBits(dynamic[(row * heads + c / head_size) * 4 + tap]);
                sum = @mulAdd(f32, fromBits(x[(row - lag) * channels + c]), weight, sum);
            }
            const index = row * channels + c;
            const expected = bf16((sum + fromBits(x[index])) * scale);
            const actual = std.mem.readInt(u16, host.constBytes()[index * 2 ..][0..2], .little);
            try std.testing.expectEqual(expected, actual);
        };
    }
}
