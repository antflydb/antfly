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

//! Frozen pre-migration scalar decoder from c34f6c5084. Test/benchmark oracle only;
//! production kernels must use shared recipes. Keep this implementation independent.
const std = @import("std");
const tensor_types = @import("../tensor_types.zig");
fn decodeFp16Le(lo: u8, hi: u8) f32 {
    return @floatCast(@as(f16, @bitCast(@as(u16, lo) | (@as(u16, hi) << 8))));
}
fn decodeF32Le(a: u8, b: u8, c: u8, d: u8) f32 {
    return @bitCast(@as(u32, a) | (@as(u32, b) << 8) | (@as(u32, c) << 16) | (@as(u32, d) << 24));
}
pub fn decode(comptime format: @import("../../graph/quant_matmul.zig").Format, data: []const u8, output: []f32) !void {
    return switch (format) {
        .q4_0 => dequantizeQ4_0(data, output),
        .q4_1 => dequantizeQ4_1(data, output),
        .q5_0 => dequantizeQ5_0(data, output),
        .q5_1 => dequantizeQ5_1(data, output),
        .q8_0 => dequantizeQ8_0(data, output),
        .q8_1 => dequantizeQ8_1(data, output),
        .q8_k => dequantizeQ8K(data, output),
        .q2_k => dequantizeQ2K(data, output),
        .q3_k => dequantizeQ3K(data, output),
        .q4_k => dequantizeQ4K(data, output),
        .q5_k => dequantizeQ5K(data, output),
        .q6_k => dequantizeQ6K(data, output),
        else => unreachable,
    };
}

fn dequantizeQ4_0(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q4_0 }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q4_0 }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        const qs = block[2..18];

        for (qs) |packed_byte| {
            if (out_off >= output.len) return;
            output[out_off] = d * @as(f32, @floatFromInt(@as(i32, packed_byte & 0x0F) - 8));
            out_off += 1;
        }
        for (qs) |packed_byte| {
            if (out_off >= output.len) return;
            output[out_off] = d * @as(f32, @floatFromInt(@as(i32, packed_byte >> 4) - 8));
            out_off += 1;
        }
    }
}

fn dequantizeQ4_1(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q4_1 }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q4_1 }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        const m = decodeFp16Le(block[2], block[3]);
        const qs = block[4..20];

        for (qs) |packed_byte| {
            if (out_off >= output.len) return;
            output[out_off] = d * @as(f32, @floatFromInt(packed_byte & 0x0F)) + m;
            out_off += 1;
        }
        for (qs) |packed_byte| {
            if (out_off >= output.len) return;
            output[out_off] = d * @as(f32, @floatFromInt(packed_byte >> 4)) + m;
            out_off += 1;
        }
    }
}

fn dequantizeQ5_0(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q5_0 }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q5_0 }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        const qh = std.mem.readInt(u32, block[2..6], .little);
        const qs = block[6..22];

        for (0..16) |i| {
            if (out_off >= output.len) return;
            const lo4: i32 = qs[i] & 0x0F;
            const hi1: i32 = @intCast((qh >> @intCast(i)) & 1);
            output[out_off] = d * @as(f32, @floatFromInt((lo4 | (hi1 << 4)) - 16));
            out_off += 1;
        }
        for (0..16) |i| {
            if (out_off >= output.len) return;
            const lo4: i32 = qs[i] >> 4;
            const hi1: i32 = @intCast((qh >> @intCast(i + 16)) & 1);
            output[out_off] = d * @as(f32, @floatFromInt((lo4 | (hi1 << 4)) - 16));
            out_off += 1;
        }
    }
}

fn dequantizeQ5_1(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q5_1 }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q5_1 }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        const m = decodeFp16Le(block[2], block[3]);
        const qh = std.mem.readInt(u32, block[4..8], .little);
        const qs = block[8..24];

        for (0..16) |i| {
            if (out_off >= output.len) return;
            const lo4: i32 = qs[i] & 0x0F;
            const hi1: i32 = @intCast((qh >> @intCast(i)) & 1);
            output[out_off] = d * @as(f32, @floatFromInt(lo4 | (hi1 << 4))) + m;
            out_off += 1;
        }
        for (0..16) |i| {
            if (out_off >= output.len) return;
            const lo4: i32 = qs[i] >> 4;
            const hi1: i32 = @intCast((qh >> @intCast(i + 16)) & 1);
            output[out_off] = d * @as(f32, @floatFromInt(lo4 | (hi1 << 4))) + m;
            out_off += 1;
        }
    }
}

fn dequantizeQ8_0(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q8_0 }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q8_0 }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        for (0..32) |i| {
            if (out_off >= output.len) return;
            const value: i8 = @bitCast(block[2 + i]);
            output[out_off] = d * @as(f32, @floatFromInt(value));
            out_off += 1;
        }
    }
}

fn dequantizeQ8_1(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q8_1 }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q8_1 }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        // block[2..4] stores d * sum(qs) for optimized dot products.
        for (0..32) |i| {
            if (out_off >= output.len) return;
            const value: i8 = @bitCast(block[4 + i]);
            output[out_off] = d * @as(f32, @floatFromInt(value));
            out_off += 1;
        }
    }
}

fn dequantizeQ8K(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q8_K }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q8_K }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeF32Le(block[0], block[1], block[2], block[3]);
        const qs = block[4 .. 4 + 256];
        for (0..256) |i| {
            if (out_off >= output.len) return;
            const value: i8 = @bitCast(qs[i]);
            output[out_off] = d * @as(f32, @floatFromInt(value));
            out_off += 1;
        }
    }
}

fn dequantizeQ2K(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q2_K }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q2_K }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const scales_raw = block[0..16];
        const d = decodeFp16Le(block[16], block[17]);
        const dmin = decodeFp16Le(block[18], block[19]);
        const qs = block[20..84];

        for (0..16) |sub| {
            const sc = @as(f32, @floatFromInt(scales_raw[sub] & 0x0F));
            const m = @as(f32, @floatFromInt(scales_raw[sub] >> 4));
            const dsc = d * sc;
            const dmn = dmin * m;
            const chunk = sub / 8;
            const group = (sub % 8) / 2;
            const l_base = (sub % 2) * 16;
            const q_base = chunk * 32;
            const shift: u3 = @intCast(group * 2);

            for (0..16) |i| {
                if (out_off >= output.len) return;
                const q = (qs[q_base + l_base + i] >> shift) & 0x03;
                output[out_off] = dsc * @as(f32, @floatFromInt(q)) - dmn;
                out_off += 1;
            }
        }
    }
}

fn dequantizeQ3K(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q3_K }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q3_K }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const hmask = block[0..32];
        const qs = block[32..96];
        const scale_data = block[96..108];
        const d = decodeFp16Le(block[108], block[109]);

        var raw_scales: [16]i32 = undefined;
        for (0..4) |i| {
            raw_scales[i] = @as(i32, scale_data[i] & 0x0F) | (@as(i32, scale_data[8 + i] & 0x03) << 4);
            raw_scales[i + 4] = @as(i32, scale_data[4 + i] & 0x0F) | (@as(i32, (scale_data[8 + i] >> 2) & 0x03) << 4);
            raw_scales[i + 8] = @as(i32, (scale_data[i] >> 4) & 0x0F) | (@as(i32, (scale_data[8 + i] >> 4) & 0x03) << 4);
            raw_scales[i + 12] = @as(i32, (scale_data[4 + i] >> 4) & 0x0F) | (@as(i32, (scale_data[8 + i] >> 6) & 0x03) << 4);
        }

        for (0..16) |sub| {
            const scale = d * @as(f32, @floatFromInt(raw_scales[sub] - 32));
            const chunk = sub / 8;
            const group = (sub % 8) / 2;
            const l_base = (sub % 2) * 16;
            const q_base = chunk * 32;
            const shift: u3 = @intCast(group * 2);
            const hm_bit: u3 = @intCast(chunk * 4 + group);

            for (0..16) |i| {
                if (out_off >= output.len) return;
                const l = l_base + i;
                const low2 = @as(i32, (qs[q_base + l] >> shift) & 0x03);
                const high1 = @as(i32, (hmask[l] >> hm_bit) & 0x01);
                output[out_off] = scale * @as(f32, @floatFromInt(low2 + high1 * 4 - 4));
                out_off += 1;
            }
        }
    }
}

fn dequantizeQ4K(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q4_K }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q4_K }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        const dmin = decodeFp16Le(block[2], block[3]);
        const scales = block[4..16];
        const qs = block[16..144];

        var scs: [8]f32 = undefined;
        var mins: [8]f32 = undefined;
        unpackQ4KScaleMins(scales, &scs, &mins);

        var q_off: usize = 0;
        for (0..4) |chunk| {
            const sub = chunk * 2;
            const dsc0 = d * scs[sub];
            const dmn0 = dmin * mins[sub];
            const dsc1 = d * scs[sub + 1];
            const dmn1 = dmin * mins[sub + 1];

            for (0..32) |i| {
                if (out_off >= output.len) return;
                output[out_off] = dsc0 * @as(f32, @floatFromInt(qs[q_off + i] & 0x0F)) - dmn0;
                out_off += 1;
            }
            for (0..32) |i| {
                if (out_off >= output.len) return;
                output[out_off] = dsc1 * @as(f32, @floatFromInt(qs[q_off + i] >> 4)) - dmn1;
                out_off += 1;
            }
            q_off += 32;
        }
    }
}

fn dequantizeQ5K(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q5_K }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q5_K }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const d = decodeFp16Le(block[0], block[1]);
        const dmin = decodeFp16Le(block[2], block[3]);
        const scales = block[4..16];
        const qh = block[16..48];
        const ql = block[48..176];

        var scs: [8]f32 = undefined;
        var mins: [8]f32 = undefined;
        unpackQ4KScaleMins(scales, &scs, &mins);

        for (0..8) |sub| {
            const dsc = d * scs[sub];
            const dmn = dmin * mins[sub];
            const chunk = sub / 2;
            const is_high = sub % 2 == 1;
            const ql_off = chunk * 32;
            const hb_shift: u3 = @intCast(sub);

            for (0..32) |i| {
                if (out_off >= output.len) return;
                const low = if (is_high) ql[ql_off + i] >> 4 else ql[ql_off + i] & 0x0F;
                const q = @as(i32, low) + @as(i32, (qh[i] >> hb_shift) & 1) * 16;
                output[out_off] = dsc * @as(f32, @floatFromInt(q)) - dmn;
                out_off += 1;
            }
        }
    }
}

fn dequantizeQ6K(data: []const u8, output: []f32) !void {
    const block_size = tensor_types.bytesPerBlock(.{ .known = .Q6_K }).?;
    const values_per_block = tensor_types.valuesPerBlock(.{ .known = .Q6_K }).?;
    if (data.len % block_size != 0) return error.InvalidQuantizedDataSize;
    if (output.len > (data.len / block_size) * values_per_block) return error.InvalidQuantizedOutputSize;

    var out_off: usize = 0;
    var block_index: usize = 0;
    while (block_index * block_size < data.len and out_off < output.len) : (block_index += 1) {
        const block = data[block_index * block_size .. (block_index + 1) * block_size];
        const ql = block[0..128];
        const qh = block[128..192];
        const scales = block[192..208];
        const d = decodeFp16Le(block[208], block[209]);

        for (0..16) |sub| {
            const scale = d * @as(f32, @floatFromInt(@as(i8, @bitCast(scales[sub]))));
            const half = sub / 8;
            const group = (sub % 8) / 2;
            const l_base = (sub % 2) * 16;
            const ql_off = half * 64 + (group & 1) * 32;
            const qh_off = half * 32;
            const qh_shift: u3 = @intCast(group * 2);
            const nibble_shift: u3 = @intCast((group / 2) * 4);

            for (0..16) |i| {
                if (out_off >= output.len) return;
                const l = l_base + i;
                const low4 = @as(i32, (ql[ql_off + l] >> nibble_shift) & 0x0F);
                const high2 = @as(i32, (qh[qh_off + l] >> qh_shift) & 0x03);
                output[out_off] = scale * @as(f32, @floatFromInt((low4 | (high2 << 4)) - 32));
                out_off += 1;
            }
        }
    }
}

fn unpackQ4KScaleMins(scales: []const u8, scs: *[8]f32, mins: *[8]f32) void {
    for (0..4) |j| {
        scs[j] = @floatFromInt(scales[j] & 63);
        mins[j] = @floatFromInt(scales[j + 4] & 63);
    }
    for (4..8) |j| {
        scs[j] = @floatFromInt((scales[j + 4] & 0x0F) | ((scales[j - 4] >> 6) << 4));
        mins[j] = @floatFromInt((scales[j + 4] >> 4) | ((scales[j] >> 6) << 4));
    }
}
