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
const buffer = @import("ops/cuda/buffer.zig");
const Context = @import("ops/cuda/context.zig").CudaContext;
const projection = @import("ops/cuda/routed_block_scaled_projection.zig");

const width = 16;
const ids = [_]u32{ 1, 0, 1, 2 };
const row_count = ids.len;

fn e2m1(bits: u8) f64 {
    const values = [_]f64{ 0, 0.5, 1, 1.5, 2, 3, 4, 6 };
    return if (bits & 8 == 0) values[bits & 7] else -values[bits & 7];
}

fn e4m3(bits: u8) f64 {
    const exponent = (bits & 127) / 8;
    const mantissa: f64 = @floatFromInt(bits & 7);
    const value = if (exponent == 0) mantissa / 512 else @exp2(@as(f64, @floatFromInt(exponent)) - 7) * (1 + mantissa / 8);
    return if (bits & 128 == 0) value else -value;
}

fn bf16(value: f64) u16 {
    const bits: u32 = @bitCast(@as(f32, @floatCast(value)));
    return @truncate((bits +% 0x7fff +% ((bits >> 16) & 1)) >> 16);
}

fn widen(bits: u16) f32 {
    return @bitCast(@as(u32, bits) << 16);
}

fn mxfp8RoundTripUniform(value: f64) f64 {
    if (value == 0) return 0;
    const scale = @exp2(@ceil(@log2(@abs(value) / 448)));
    const normalized = @abs(value) / scale;
    var min_error: f64 = std.math.inf(f64);
    var selected: u8 = 0;
    for (0..127) |code| {
        const distance = @abs(normalized - e4m3(@intCast(code)));
        if (distance < min_error or (distance == min_error and code % 2 == 0)) {
            selected = @intCast(code);
            min_error = distance;
        }
    }
    return widen(bf16(e4m3(selected) * scale * @as(f64, if (value < 0) -1 else 1)));
}

fn expand(format: projection.Format, data: []const u8, scales: []const u8, i: usize) f64 {
    return switch (format) {
        .nvfp4 => e2m1(if (i % 2 == 0) data[i / 2] else data[i / 2] >> 4) * e4m3(scales[i / 16]),
        .mxfp8 => blk: {
            const scale = @exp2(@as(f64, @floatFromInt(scales[i / 32])) - 127);
            break :blk widen(bf16(e4m3(data[i]) * scale));
        },
    };
}

fn upload(ctx: *Context, bytes: []const u8) !buffer.DeviceBuffer {
    var staging = try buffer.HostBuffer.alloc(ctx, bytes.len);
    defer staging.free(ctx);
    @memcpy(staging.bytes(), bytes);
    var device = try buffer.DeviceBuffer.alloc(ctx, bytes.len);
    errdefer device.free(ctx);
    try device.copyFromHost(ctx, staging.constBytes());
    try ctx.synchronize();
    return device;
}

fn sampledRows(allocator: std.mem.Allocator, tensor: []const u8, columns: usize, row_bytes: usize) ![]u8 {
    const expert_stride = columns * row_bytes;
    const bytes = try allocator.alloc(u8, 2 * width * row_bytes);
    const selected_bytes = width * row_bytes;
    @memcpy(bytes[0..selected_bytes], tensor[0..selected_bytes]);
    @memcpy(bytes[selected_bytes..], tensor[expert_stride..][0..selected_bytes]);
    return bytes;
}

fn check(ctx: *Context, module: *projection.Module, source: *const checkpoint.Checkpoint, layer: usize, name: checkpoint.Projection) !void {
    const allocator = std.testing.allocator;
    const tensor = try source.experts(layer, name);
    const fp4 = tensor.format == .nvfp4;
    const format: projection.Format = if (fp4) .nvfp4 else .mxfp8;
    const config = source.config.value.text_config;
    const k = if (name == .w13) config.hidden_size else config.intermediate_size;
    const columns = if (name == .w13) 2 * config.intermediate_size else config.hidden_size;
    const packed_width: usize = if (fp4) k / 2 else k;
    const groups: usize = k / @as(u16, if (fp4) 16 else 32);
    const weights = try sampledRows(allocator, tensor.weights, columns, packed_width);
    defer allocator.free(weights);
    const scales = try sampledRows(allocator, tensor.block_scales, columns, groups);
    defer allocator.free(scales);

    const activation = try allocator.alloc(u8, row_count * packed_width);
    defer allocator.free(activation);
    const activation_scales = try allocator.alloc(u8, row_count * groups);
    defer allocator.free(activation_scales);
    for (activation, 0..) |*bits, i|
        bits.* = if (fp4) @truncate(i * 131 + i / 8) else @intCast(40 + (i * 13 + i / 17) % 24 + (i % 3) / 2 * 128);
    for (activation_scales, 0..) |*bits, i|
        bits.* = if (fp4) @intCast(48 + i % 12) else @intCast(125 + i % 5);
    const outer = [_]f32{ 0.75, 1.25, -0.625, 0 };
    const bytes = [_][]const u8{ activation, activation_scales, weights, scales, std.mem.sliceAsBytes(&ids), std.mem.sliceAsBytes(&outer) };
    var device: [bytes.len]buffer.DeviceBuffer = @splat(.{});
    defer for (&device) |*d| d.free(ctx);
    for (&device, bytes) |*d, src| d.* = try upload(ctx, src);
    var output = try buffer.DeviceBuffer.alloc(ctx, row_count * width * 2);
    defer output.free(ctx);
    var readback = try buffer.HostBuffer.alloc(ctx, output.len);
    defer readback.free(ctx);
    var resident: projection.PackedWeights = .{ .values = device[2], .scales = device[3] };
    device[2] = .{};
    device[3] = .{};
    defer resident.values.free(ctx);
    defer resident.scales.free(ctx);
    try module.interleaveWeights(ctx, &resident, format, k, width, 2);
    try module.run(ctx, .{
        .format = format,
        .activation = device[0],
        .activation_block_scale = device[1],
        .weight = resident.values,
        .weight_block_scale = resident.scales,
        .interleaved = resident.interleaved,
        .expert_ids = device[4],
        .row_outer_scale = device[5],
        .output = output,
        .rows = row_count,
        .in_dim = k,
        .out_dim = width,
        .expert_count = 2,
    });
    try output.copyToHost(ctx, readback.bytes());
    try ctx.synchronize();
    var exact: usize = 0;
    for (ids, 0..) |expert, row| for (0..width) |col| {
        const bits = std.mem.readInt(u16, readback.constBytes()[(row * width + col) * 2 ..][0..2], .little);
        if (expert >= 2) {
            try std.testing.expectEqual(@as(u16, 0), bits);
            continue;
        }
        const weight_row = expert * width + col;
        var sum: f64 = 0;
        for (0..k) |i| {
            const av = expand(format, activation[row * packed_width ..], activation_scales[row * groups ..], i);
            const wv = expand(format, weights[weight_row * packed_width ..], scales[weight_row * groups ..], i);
            sum += av * wv;
        }
        const expected = bf16(if (fp4) sum * outer[row] else sum);
        exact += @intFromBool(expected == bits);
        const difference: f32 = @abs(widen(expected) - widen(bits));
        try std.testing.expect(difference <= @abs(widen(expected)) * 0.008 + 1e-6);
    };
    std.debug.print("routed projection {s} {s} k={d} n={d} interleaved={} exact={d}/{d} plus masked row\n", .{ @tagName(format), @tagName(name), k, width, resident.interleaved, exact, (row_count - 1) * width });

    if (!resident.interleaved) return;
    const repeat = 2;
    const input_rows = row_count / repeat;
    const input = try allocator.alloc(u16, @as(usize, input_rows) * k);
    defer allocator.free(input);
    for (input, 0..) |*bits, i| {
        const value: f64 = @floatFromInt(@as(i32, @intCast((i * 73 + i / 17) % 129)) - 64);
        bits.* = bf16(value / 64);
    }
    var input_device = try upload(ctx, std.mem.sliceAsBytes(input));
    defer input_device.free(ctx);
    if (tensor.outer_amax) |amax| resident.amax = try upload(ctx, amax[0..8]);
    defer resident.amax.free(ctx);
    if (tensor.outer_denominator) |denominator| resident.denominator = try upload(ctx, denominator);
    defer resident.denominator.free(ctx);
    try module.runBf16(ctx, .{
        .format = format,
        .input = input_device,
        .weights = resident,
        .expert_ids = device[4],
        .output = output,
        .input_rows = input_rows,
        .repeat = repeat,
        .in_dim = k,
        .out_dim = width,
        .expert_count = 2,
    });
    try output.copyToHost(ctx, readback.bytes());
    try ctx.synchronize();
    exact = 0;
    for (ids, 0..) |expert, row| {
        const factor: f32 = if (fp4 and expert < 2) blk: {
            const amax: f32 = @bitCast(std.mem.readInt(u32, tensor.outer_amax.?[expert * 4 ..][0..4], .little));
            const denominator: f32 = @bitCast(std.mem.readInt(u32, tensor.outer_denominator.?[0..4], .little));
            break :blk amax / denominator;
        } else 1;
        for (0..width) |col| {
            const bits = std.mem.readInt(u16, readback.constBytes()[(row * width + col) * 2 ..][0..2], .little);
            if (expert >= 2) {
                try std.testing.expectEqual(@as(u16, 0), bits);
                continue;
            }
            const weight_row = expert * width + col;
            var sum: f64 = 0;
            for (0..k) |i| {
                const weight = expand(format, weights[weight_row * packed_width ..], scales[weight_row * groups ..], i);
                sum += @as(f64, widen(input[row / repeat * k + i])) * widen(bf16(weight * factor));
            }
            const expected = bf16(sum);
            exact += @intFromBool(expected == bits);
            const difference: f32 = @abs(widen(expected) - widen(bits));
            try std.testing.expect(difference <= @abs(widen(expected)) * 0.008 + 1e-6);
        }
    }
    std.debug.print("routed BF16-input projection {s} {s} exact={d}/{d} plus masked row\n", .{ @tagName(format), @tagName(name), exact, (row_count - 1) * width });
}

fn checkQuantize(ctx: *Context, module: *projection.Module, format: projection.Format) !void {
    const fp4 = format == .nvfp4;
    const k = 32;
    var input: [2 * k]u16 = @splat(0);
    for (input[0..k], 0..) |*bits, i| bits.* = bf16(if (i % 2 == 0) 1.0 else -1.0);
    const amax = [_]f32{ 2, 4 };
    const denominator = [_]f32{2};
    const inputs = [_][]const u8{ std.mem.sliceAsBytes(&input), std.mem.sliceAsBytes(&ids), std.mem.sliceAsBytes(&amax), std.mem.sliceAsBytes(&denominator) };
    var inputs_device: [inputs.len]buffer.DeviceBuffer = @splat(.{});
    defer for (&inputs_device) |*d| d.free(ctx);
    for (&inputs_device, inputs) |*d, bytes| d.* = try upload(ctx, bytes);
    const row_bytes: usize = if (fp4) k / 2 else k;
    const groups: usize = if (fp4) 2 else 1;
    const values_size = row_count * row_bytes;
    const scale_size = row_count * groups;
    var values = try buffer.DeviceBuffer.alloc(ctx, values_size);
    defer values.free(ctx);
    var scales = try buffer.DeviceBuffer.alloc(ctx, scale_size);
    defer scales.free(ctx);
    var outer = try buffer.DeviceBuffer.alloc(ctx, if (fp4) row_count * 4 else 0);
    defer outer.free(ctx);
    var download = try buffer.HostBuffer.alloc(ctx, values_size + scale_size + outer.len);
    defer download.free(ctx);
    try module.quantize(ctx, .{
        .format = format,
        .input = inputs_device[0],
        .expert_ids = inputs_device[1],
        .weight_amax = inputs_device[2],
        .weight_denominator = inputs_device[3],
        .activation = values,
        .activation_block_scale = scales,
        .row_outer_scale = outer,
        .input_rows = 2,
        .repeat = 2,
        .width = k,
        .expert_count = 2,
    });
    try values.copyToHost(ctx, download.bytes()[0..values_size]);
    try scales.copyToHost(ctx, download.bytes()[values_size..][0..scale_size]);
    try outer.copyToHost(ctx, download.bytes()[values_size + scale_size ..]);
    try ctx.synchronize();
    for (download.constBytes()[0..values_size], 0..) |byte, i| {
        const expected: u8 = if (i >= 2 * row_bytes) 0 else if (fp4) 0xf7 else if (i % 2 == 0) 120 else 248;
        try std.testing.expectEqual(expected, byte);
    }
    for (download.constBytes()[values_size..][0..scale_size], 0..) |byte, i| {
        const expected: u8 = if (i >= 2 * groups) 0 else if (fp4) 120 else 119;
        try std.testing.expectEqual(expected, byte);
    }
    if (fp4) {
        for ([_]f32{ 4.0 / 3072.0, 2.0 / 3072.0, 0, 0 }, 0..) |expected, i| {
            const bits = std.mem.readInt(u32, download.constBytes()[values_size + scale_size + 4 * i ..][0..4], .little);
            try std.testing.expectEqual(expected, @as(f32, @bitCast(bits)));
        }
    }
}

fn checkFfn(ctx: *Context, module: *projection.Module, format: projection.Format) !void {
    const w = 32;
    const fp4 = format == .nvfp4;
    const row_bytes = if (fp4) @as(usize, w / 2) else w;
    const group_count = if (fp4) @as(usize, 2) else 1;
    var gate_up: [2 * 2 * w * w]u8 = undefined;
    var down: [2 * w * w]u8 = undefined;
    var gate_up_scale: [2 * 2 * w * 2]u8 = undefined;
    var down_scale: [2 * w * 2]u8 = undefined;
    for (0..2) |expert| {
        for (0..2 * w) |column| {
            const one = (column % 2 == 1) != (expert == 1);
            const val: u8 = if (fp4) (if (one) 0x22 else 0x11) else (if (one) 56 else 48);
            @memset(gate_up[(expert * 2 * w + column) * row_bytes ..][0..row_bytes], val);
        }
        const val: u8 = if (fp4) (if (expert == 1) 0x22 else 0x11) else (if (expert == 1) 56 else 48);
        @memset(down[expert * w * row_bytes ..][0 .. w * row_bytes], val);
    }
    @memset(&gate_up_scale, if (fp4) 56 else 127);
    @memset(&down_scale, if (fp4) 56 else 127);
    var input: [2 * w]u16 = @splat(0);
    input[0] = bf16(1);
    input[w] = bf16(-1);
    const logits = [_]f32{ 0, 0, 0, 4 };
    const bias = [_]f32{ 0.2, -0.2 };
    const amax = [_]f32{ 1, 1 };
    const denom = [_]f32{1};
    const input_bytes = [_][]const u8{
        std.mem.sliceAsBytes(&input),         std.mem.sliceAsBytes(&logits),               std.mem.sliceAsBytes(&bias),
        gate_up[0 .. 2 * 2 * w * row_bytes],  gate_up_scale[0 .. 2 * 2 * w * group_count], down[0 .. 2 * w * row_bytes],
        down_scale[0 .. 2 * w * group_count], std.mem.sliceAsBytes(&amax),                 std.mem.sliceAsBytes(&denom),
    };
    var device: [input_bytes.len]buffer.DeviceBuffer = @splat(.{});
    defer for (&device) |*d| d.free(ctx);
    for (&device, input_bytes) |*dst, bytes| dst.* = try upload(ctx, bytes);
    var readback = try buffer.HostBuffer.alloc(ctx, 2 * w * 2);
    defer readback.free(ctx);
    var output = try buffer.DeviceBuffer.alloc(ctx, readback.len);
    defer output.free(ctx);
    var request: projection.FfnRequest = .{
        .format = format,
        .input = device[0],
        .logits = device[1],
        .router_bias = device[2],
        .gate_up = .{ .values = device[3], .scales = device[4], .amax = device[7], .denominator = device[8] },
        .down = .{ .values = device[5], .scales = device[6], .amax = device[7], .denominator = device[8] },
        .output = output,
        .scratch = .{},
        .rows = 2,
        .hidden = w,
        .intermediate = w,
        .experts = 2,
        .topk = 2,
        .route_scale = 8,
        .gate_cap = 4,
        .up_cap = 25,
    };
    var scratch = try buffer.DeviceBuffer.alloc(ctx, try projection.ffnScratchBytes(request));
    defer scratch.free(ctx);
    request.scratch = scratch;
    for ([_]u32{ 2, 1 }) |topk| {
        request.topk = topk;
        try module.runFfn(ctx, request);
        try output.copyToHost(ctx, readback.bytes());
        try ctx.synchronize();
        for (0..2) |row| {
            const sign: f64 = if (row == 0) 1 else -1;
            const p1 = 1 / (1 + @exp(-@as(f64, logits[row * 2 + 1])));
            var expected: f64 = 0;
            for (0..2) |expert| {
                if (topk == 1 and expert != row) continue;
                const probability = if (topk == 1) 1 else (if (expert == 0) 0.5 else p1) / (0.5 + p1);
                const gate = sign * @as(f64, if (expert == 0) 0.5 else 1);
                const up = sign * @as(f64, if (expert == 0) 1 else 0.5);
                const down_weight: f64 = if (expert == 0) 0.5 else 1;
                const a = 4 * std.math.tanh(gate / 4) / (1 + @exp(-gate));
                const b = 25 * std.math.tanh(up / 25);
                const activated = widen(bf16(a * b * probability * 8));
                const down_input = if (fp4) activated else mxfp8RoundTripUniform(activated);
                expected += widen(bf16(w * down_weight * down_input));
            }
            expected = widen(bf16(expected));
            for (0..w) |i| {
                const bits = std.mem.readInt(u16, readback.constBytes()[(row * w + i) * 2 ..][0..2], .little);
                try std.testing.expectApproxEqRel(expected, @as(f64, widen(bits)), 0.01);
            }
        }
    }
}

test "CUDA routed block scaled projection against released Inkling tensors" {
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_MODEL_DIR") orelse return error.Inkling2FixtureRequired;
    var source = try checkpoint.Checkpoint.open(std.testing.allocator, dir);
    defer source.deinit();
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var module: projection.Module = .{};
    defer module.deinit(&ctx);
    for ([_]projection.Format{ .nvfp4, .mxfp8 }, [_]usize{ 0, 20 }) |format, layer| {
        try checkQuantize(&ctx, &module, format);
        try checkFfn(&ctx, &module, format);
        try check(&ctx, &module, &source, layer, .w13);
        try check(&ctx, &module, &source, layer, .w2);
    }
}
