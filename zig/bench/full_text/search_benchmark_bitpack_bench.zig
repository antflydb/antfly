// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License 2.0 (the "License");
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
const antfly = @import("antfly-zig");

const bp128 = antfly.simd_bitpack;
const platform_time = antfly.platform_time;

fn packedLen(count: usize, bits: u8) usize {
    return (count * @as(usize, bits) + 7) / 8;
}

fn encodeHorizontal(dst: []u8, values: []const u32, bits: u8) void {
    @memset(dst[0..packedLen(values.len, bits)], 0);
    var bit_position: usize = 0;
    for (values) |value| {
        var shifted = value;
        var remaining = bits;
        while (remaining > 0) {
            const byte_index = bit_position / 8;
            const bit_in_byte: u3 = @intCast(bit_position % 8);
            const take: u8 = @min(remaining, 8 - @as(u8, bit_in_byte));
            const mask = (@as(u32, 1) << @intCast(take)) - 1;
            dst[byte_index] |= @as(u8, @truncate(shifted & mask)) << bit_in_byte;
            shifted >>= @intCast(take);
            remaining -= take;
            bit_position += take;
        }
    }
}

fn decodeHorizontal(src: []const u8, values: *[bp128.block_values]u32, bits: u8) void {
    var byte_index: usize = 0;
    var reservoir: u64 = 0;
    var reservoir_bits: u8 = 0;
    const mask: u64 = if (bits == 32) std.math.maxInt(u32) else (@as(u64, 1) << @intCast(bits)) - 1;
    for (values) |*value| {
        while (reservoir_bits < bits) {
            reservoir |= @as(u64, src[byte_index]) << @intCast(reservoir_bits);
            reservoir_bits += 8;
            byte_index += 1;
        }
        value.* = @intCast(reservoir & mask);
        reservoir >>= @intCast(bits);
        reservoir_bits -= bits;
    }
}

fn checksum(values: []const u32) u64 {
    var sum: u64 = 0;
    for (values) |value| sum = sum *% 1_099_511_628_211 +% value;
    return sum;
}

pub fn main(init: std.process.Init) !void {
    var iterations: usize = 250_000;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--iters")) {
            iterations = try std.fmt.parseInt(usize, args.next() orelse return error.MissingArgument, 10);
        } else {
            return error.UnknownArgument;
        }
    }
    if (iterations == 0) return error.InvalidArgument;

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buffer);
    var random = std.Random.DefaultPrng.init(0x4250_3132_385f_4245);
    const rng = random.random();
    const widths = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32 };

    var input: [bp128.block_values]u32 = undefined;
    var horizontal: [512]u8 = undefined;
    var vertical: [512]u8 = undefined;
    var decoded: [bp128.block_values]u32 = undefined;

    for (widths) |bits| {
        const mask: u32 = if (bits == 32) std.math.maxInt(u32) else (@as(u32, 1) << @intCast(bits)) - 1;
        for (&input) |*value| value.* = rng.int(u32) & mask;
        const bytes = packedLen(input.len, bits);
        encodeHorizontal(&horizontal, &input, bits);
        _ = try bp128.encodeBlock(&vertical, &input, bits);

        decodeHorizontal(horizontal[0..bytes], &decoded, bits);
        if (!std.mem.eql(u32, &input, &decoded)) return error.HorizontalMismatch;
        _ = try bp128.decodeBlock(vertical[0..bytes], &decoded, bits);
        if (!std.mem.eql(u32, &input, &decoded)) return error.VectorMismatch;

        const horizontal_start = platform_time.monotonicNs();
        for (0..iterations) |_| {
            decodeHorizontal(horizontal[0..bytes], &decoded, bits);
            std.mem.doNotOptimizeAway(&decoded);
        }
        const horizontal_elapsed = platform_time.monotonicNs() - horizontal_start;

        const vector_start = platform_time.monotonicNs();
        for (0..iterations) |_| {
            _ = try bp128.decodeBlock(vertical[0..bytes], &decoded, bits);
            std.mem.doNotOptimizeAway(&decoded);
        }
        const vector_elapsed = platform_time.monotonicNs() - vector_start;

        const scalar_start = platform_time.monotonicNs();
        for (0..iterations) |_| {
            _ = try bp128.decodeBlockScalar(vertical[0..bytes], &decoded, bits);
            std.mem.doNotOptimizeAway(&decoded);
        }
        const scalar_elapsed = platform_time.monotonicNs() - scalar_start;

        try stdout.interface.print(
            "{{\"bench\":\"bp128\",\"bits\":{d},\"values\":{d},\"bytes\":{d},\"iterations\":{d},\"horizontal_ns_per_block\":{d:.3},\"vertical_vector_ns_per_block\":{d:.3},\"vertical_scalar_ns_per_block\":{d:.3},\"checksum\":{d}}}\n",
            .{
                bits,
                input.len,
                bytes,
                iterations,
                @as(f64, @floatFromInt(horizontal_elapsed)) / @as(f64, @floatFromInt(iterations)),
                @as(f64, @floatFromInt(vector_elapsed)) / @as(f64, @floatFromInt(iterations)),
                @as(f64, @floatFromInt(scalar_elapsed)) / @as(f64, @floatFromInt(iterations)),
                checksum(&decoded),
            },
        );
    }
    try stdout.interface.flush();
}
