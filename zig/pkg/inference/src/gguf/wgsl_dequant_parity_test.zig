// Copyright 2026 Antfly, Inc.
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

//! Parity tests for the WGSL matmul-transB dequant kernels in web/shaders/.
//!
//! Each WGSL shader implements the same per-element dequant as quant_codec.zig
//! but addressed by `(b_row, k_abs)` rather than the reference's forward-iteration
//! out_off. Address-arithmetic mistakes are easy to make and don't show up in
//! shader compilation. This file ports each WGSL dequant function back to Zig
//! and checks that for every output index in a randomized block it produces the
//! same f32 value as the reference dequantizeToFloat32 path.
//!
//! When you regenerate the WGSL shaders via scripts/gen_iq_shaders.py, keep
//! these ports in sync.

const std = @import("std");
const tensor_types = @import("tensor_types.zig");
const codec = @import("quant_codec.zig");

const RowBytes = struct {
    bytes: []const u8,
};

fn readByte(row: RowBytes, byte_offset: u32) u32 {
    return row.bytes[byte_offset];
}

fn readU16(row: RowBytes, byte_offset: u32) u32 {
    return @as(u32, row.bytes[byte_offset]) | (@as(u32, row.bytes[byte_offset + 1]) << 8);
}

fn readF16(row: RowBytes, byte_offset: u32) f32 {
    return codec.decodeFp16Le(row.bytes[byte_offset], row.bytes[byte_offset + 1]);
}

// ---------- WGSL ports ----------

fn wgslQ1_0(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 128;
    const in_block = k_abs % 128;
    const block_byte: u32 = block_idx * 18;
    const d = readF16(row, block_byte);
    const byte_val = readByte(row, block_byte + 2 + in_block / 8);
    const bit = (byte_val >> @as(u5, @intCast(in_block % 8))) & 1;
    return if (bit != 0) d else -d;
}

fn wgslI8_S(row: RowBytes, k_abs: u32) f32 {
    const raw = row.bytes[k_abs];
    const signed: i32 = if (raw >= 128) @as(i32, raw) - 256 else @as(i32, raw);
    return @floatFromInt(signed);
}

fn wgslTQ2_0(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 66;
    const d = readF16(row, block_byte + 64);
    const j_byte_base: u32 = (in_block / 128) * 32;
    const l = (in_block % 128) / 32;
    const m = in_block % 32;
    const shift: u5 = @intCast(l * 2);
    const raw = (readByte(row, block_byte + j_byte_base + m) >> shift) & 0x03;
    return @as(f32, @floatFromInt(@as(i32, @intCast(raw)) - 1)) * d;
}

fn wgslTQ1_0(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 54;
    const d = readF16(row, block_byte + 52);

    var n: u32 = undefined;
    var src_off: u32 = undefined;
    if (in_block < 160) {
        n = in_block / 32;
        src_off = block_byte + (in_block % 32);
    } else if (in_block < 240) {
        const rel = in_block - 160;
        n = rel / 16;
        src_off = block_byte + 32 + (rel % 16);
    } else {
        const rel = in_block - 240;
        n = rel / 4;
        src_off = block_byte + 48 + (rel % 4);
    }
    const pow3 = [_]u32{ 1, 3, 9, 27, 81 };
    const src = readByte(row, src_off);
    const q = (src *% pow3[n]) & 0xFF;
    const xi = (q * 3) >> 8;
    return @as(f32, @floatFromInt(@as(i32, @intCast(xi)) - 1)) * d;
}

fn wgslMXFP4(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 32;
    const in_block = k_abs % 32;
    const block_byte: u32 = block_idx * 17;
    const d = codec.decodeE8M0Half(@truncate(readByte(row, block_byte)));
    const packed_byte = readByte(row, block_byte + 1 + (in_block % 16));
    const nibble = if (in_block < 16) packed_byte & 0x0F else (packed_byte >> 4) & 0x0F;
    return d * @as(f32, @floatFromInt(codec.mxfp4_values[nibble]));
}

fn wgslNVFP4(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 64;
    const in_block = k_abs % 64;
    const block_byte: u32 = block_idx * 36;
    const sub = in_block / 16;
    const elem_in_sub = in_block % 16;
    const d = codec.decodeUE4M3Half(@truncate(readByte(row, block_byte + sub)));
    const packed_byte = readByte(row, block_byte + 4 + sub * 8 + (elem_in_sub % 8));
    const nibble = if (elem_in_sub < 8) packed_byte & 0x0F else (packed_byte >> 4) & 0x0F;
    return d * @as(f32, @floatFromInt(codec.mxfp4_values[nibble]));
}

fn wgslIQ2_XXS(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 66;
    const d = readF16(row, block_byte);
    const qs_off = block_byte + 2;

    const ib32 = in_block / 32;
    const elem32 = in_block % 32;
    const lane = elem32 / 8;
    const j: u5 = @intCast(elem32 % 8);
    const group_off = qs_off + ib32 * 8;
    const packed_grids = std.mem.readInt(u32, row.bytes[group_off..][0..4], .little);
    const packed_signs_scales = std.mem.readInt(u32, row.bytes[group_off + 4 ..][0..4], .little);
    const db = d * (0.5 + @as(f32, @floatFromInt(packed_signs_scales >> 28))) * 0.25;
    const grid_index: u8 = @truncate(packed_grids >> @as(u5, @intCast(8 * lane)));
    const sign_idx: u7 = @truncate((packed_signs_scales >> @as(u5, @intCast(7 * lane))) & 0x7F);
    const signs = codec.iq2SignMask(sign_idx);
    const grid = codec.iq2_xxs_grid[grid_index];
    const raw = 2 * @as(i32, @intCast((grid >> @as(u4, @intCast(2 * j))) & 0x03)) + 1;
    const sign: f32 = if ((signs & (@as(u8, 1) << @as(u3, @intCast(j)))) != 0) -1.0 else 1.0;
    return db * @as(f32, @floatFromInt(raw)) * sign;
}

fn wgslIQ2_XS(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 74;
    const d = readF16(row, block_byte);
    const qs_off = block_byte + 2;
    const scales_off = block_byte + 66;

    const ib32 = in_block / 32;
    const elem32 = in_block % 32;
    const lane = elem32 / 8;
    const j: u5 = @intCast(elem32 % 8);
    const half = lane / 2;

    const scale_byte = readByte(row, scales_off + ib32);
    const scale_nibble: u32 = if (half == 0) scale_byte & 0x0F else scale_byte >> 4;
    const dl = d * (0.5 + @as(f32, @floatFromInt(scale_nibble))) * 0.25;

    const pair_off = qs_off + (4 * ib32 + lane) * 2;
    const packed_word = readU16(row, pair_off);
    const grid_index = packed_word & 0x01FF;
    const signs = codec.iq2SignMask(@truncate(packed_word >> 9));
    const grid = codec.iq2_xs_grid[grid_index];
    const raw = 2 * @as(i32, @intCast((grid >> @as(u4, @intCast(2 * j))) & 0x03)) + 1;
    const sign: f32 = if ((signs & (@as(u8, 1) << @as(u3, @intCast(j)))) != 0) -1.0 else 1.0;
    return dl * @as(f32, @floatFromInt(raw)) * sign;
}

fn wgslIQ2_S(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 82;
    const d = readF16(row, block_byte);
    const qs_off = block_byte + 2;
    const qh_off = block_byte + 66;
    const scales_off = block_byte + 74;

    const ib32 = in_block / 32;
    const elem32 = in_block % 32;
    const lane = elem32 / 8;
    const j: u5 = @intCast(elem32 % 8);
    const half = lane / 2;

    const scale_byte = readByte(row, scales_off + ib32);
    const scale_nibble: u32 = if (half == 0) scale_byte & 0x0F else scale_byte >> 4;
    const dl = d * (0.5 + @as(f32, @floatFromInt(scale_nibble))) * 0.25;

    const qh_byte = readByte(row, qh_off + ib32);
    const high = (qh_byte >> @as(u5, @intCast(2 * lane))) & 0x03;
    const low = readByte(row, qs_off + ib32 * 4 + lane);
    const grid_index = low | (high << 8);
    const signs = readByte(row, qs_off + 32 + ib32 * 4 + lane);
    const grid = codec.iq2_s_grid[grid_index];
    const raw = 2 * @as(i32, @intCast((grid >> @as(u4, @intCast(2 * j))) & 0x03)) + 1;
    const sign: f32 = if ((signs & (@as(u8, 1) << @as(u3, @intCast(j)))) != 0) -1.0 else 1.0;
    return dl * @as(f32, @floatFromInt(raw)) * sign;
}

fn wgslIQ3_XXS(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 98;
    const d = readF16(row, block_byte);
    const grid_indexes_off = block_byte + 2;
    const scales_signs_off = block_byte + 2 + 64;

    const ib32 = in_block / 32;
    const elem32 = in_block % 32;
    const lane = elem32 / 8;
    const half = (elem32 % 8) / 4;
    const j: u5 = @intCast(elem32 % 4);

    const packed_word = std.mem.readInt(u32, row.bytes[scales_signs_off + 4 * ib32 ..][0..4], .little);
    const db = d * (0.5 + @as(f32, @floatFromInt(packed_word >> 28))) * 0.5;
    const signs = codec.iq2SignMask(@truncate((packed_word >> @as(u5, @intCast(7 * lane))) & 0x7F));
    const grid_index = readByte(row, grid_indexes_off + ib32 * 8 + 2 * lane + half);
    const grid = codec.iq3_xxs_grid[grid_index];
    const raw = (grid >> @as(u5, @intCast(8 * j))) & 0xFF;
    const sign_bit_pos: u3 = @intCast(j + half * 4);
    const sign: f32 = if ((signs & (@as(u8, 1) << sign_bit_pos)) != 0) -1.0 else 1.0;
    return db * @as(f32, @floatFromInt(raw)) * sign;
}

fn wgslIQ3_S(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 110;
    const d = readF16(row, block_byte);
    const qs_off = block_byte + 2;
    const qh_off = block_byte + 66;
    const signs_off = block_byte + 74;
    const scales_off = block_byte + 106;

    const pair = (in_block / 32) / 2;
    const half = (in_block / 32) % 2;
    const elem32 = in_block % 32;
    const lane = elem32 / 8;
    const grid_half = (elem32 % 8) / 4;
    const j: u5 = @intCast(elem32 % 4);

    const scale_byte = readByte(row, scales_off + pair);
    const scale_nibble: u32 = if (half == 0) scale_byte & 0x0F else scale_byte >> 4;
    const dl = d * @as(f32, @floatFromInt(1 + 2 * scale_nibble));

    const q_base = pair * 16 + half * 8;
    const qh_byte = readByte(row, qh_off + pair * 2 + half);
    const q_idx = q_base + 2 * lane + grid_half;
    const high = (qh_byte >> @as(u5, @intCast(2 * lane + grid_half))) & 0x01;
    const grid_index = readByte(row, qs_off + q_idx) | (high << 8);
    const grid = codec.iq3_s_grid[grid_index];
    const sign_bits = readByte(row, signs_off + (pair * 8 + half * 4) + lane);
    const raw = (grid >> @as(u5, @intCast(8 * j))) & 0xFF;
    const sign_bit: u3 = @intCast(j + grid_half * 4);
    const sign: f32 = if ((sign_bits & (@as(u8, 1) << sign_bit)) != 0) -1.0 else 1.0;
    return dl * @as(f32, @floatFromInt(raw)) * sign;
}

fn wgslIQ1_S(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 50;
    const d = readF16(row, block_byte);
    const qs_off = block_byte + 2;
    const qh_off = block_byte + 34;

    const ib = in_block / 32;
    const elem = in_block % 32;
    const lane = elem / 8;
    const j: u5 = @intCast(elem % 8);

    const qh_word = readU16(row, qh_off + 2 * ib);
    const dl = d * @as(f32, @floatFromInt(2 * ((qh_word >> 12) & 0x7) + 1));
    const delta: f32 = if ((qh_word & 0x8000) != 0) -0.125 else 0.125;
    const low = readByte(row, qs_off + ib * 4 + lane);
    const high = (qh_word >> @as(u5, @intCast(3 * lane))) & 0x7;
    const grid_index = low | (high << 8);
    const grid = codec.iq1_s_grid[grid_index];
    const trit = (grid >> @as(u4, @intCast(2 * j))) & 0x3;
    const raw = @as(i32, @intCast(trit)) - 1;
    return dl * (@as(f32, @floatFromInt(raw)) + delta);
}

fn wgslIQ1_M(row: RowBytes, k_abs: u32) f32 {
    const block_idx = k_abs / 256;
    const in_block = k_abs % 256;
    const block_byte: u32 = block_idx * 56;
    const qs_off = block_byte;
    const qh_off = block_byte + 32;
    const scales_off = block_byte + 48;

    const s0 = readU16(row, scales_off + 0);
    const s1 = readU16(row, scales_off + 2);
    const s2 = readU16(row, scales_off + 4);
    const s3 = readU16(row, scales_off + 6);
    const scale_bits = (s0 >> 12) | ((s1 >> 8) & 0x00F0) | ((s2 >> 4) & 0x0F00) | (s3 & 0xF000);
    const d = codec.decodeFp16Le(@truncate(scale_bits & 0xFF), @truncate((scale_bits >> 8) & 0xFF));

    const ib = in_block / 32;
    const elem = in_block % 32;
    const lane = elem / 8;
    const j: u5 = @intCast(elem % 8);

    const sc_word: u32 = switch (ib / 2) {
        0 => s0,
        1 => s1,
        2 => s2,
        else => s3,
    };
    const sc_pair = ib % 2;
    const sc_shift: u5 = @intCast(6 * sc_pair);
    const dl_idx = lane / 2;
    const dl = d * @as(f32, @floatFromInt(2 * ((sc_word >> @as(u5, @intCast(sc_shift + 3 * dl_idx))) & 0x7) + 1));

    const q_base = ib * 4;
    const h_base = ib * 2;
    const qh_byte = readByte(row, qh_off + h_base + (lane / 2));
    const qs_byte = readByte(row, qs_off + q_base + lane);
    const high_shift: u5 = if ((lane % 2) == 1) 4 else 8;
    const grid_index = qs_byte | ((qh_byte << high_shift) & 0x700);

    const delta_bit: u32 = if ((lane % 2) == 1) 0x80 else 0x08;
    const delta: f32 = if ((qh_byte & delta_bit) != 0) -0.125 else 0.125;

    const grid = codec.iq1_s_grid[grid_index];
    const trit = (grid >> @as(u4, @intCast(2 * j))) & 0x3;
    const raw = @as(i32, @intCast(trit)) - 1;
    return dl * (@as(f32, @floatFromInt(raw)) + delta);
}

// ---------- Test driver ----------

const PortFn = fn (RowBytes, u32) f32;

fn runParity(
    comptime fmt: tensor_types.KnownTensorType,
    comptime port: PortFn,
    iters: usize,
) !void {
    const allocator = std.testing.allocator;
    const ttype = tensor_types.TensorType{ .known = fmt };
    const block_values = tensor_types.valuesPerBlock(ttype).?;
    const block_bytes = tensor_types.bytesPerBlock(ttype).?;

    var prng = std.Random.DefaultPrng.init(0xc0ffee01);
    const rng = prng.random();

    const raw = try allocator.alloc(u8, block_bytes);
    defer allocator.free(raw);
    const expected = try allocator.alloc(f32, block_values);
    defer allocator.free(expected);

    var iter: usize = 0;
    while (iter < iters) : (iter += 1) {
        rng.bytes(raw);

        try codec.dequantizeToFloat32(ttype, raw, expected);

        const row = RowBytes{ .bytes = raw };
        for (0..block_values) |k_abs| {
            const got = port(row, @intCast(k_abs));
            const want = expected[k_abs];
            const diff = @abs(got - want);
            const tol: f32 = 1e-4 * (@abs(want) + 1.0);
            if (!(diff <= tol or (std.math.isNan(got) and std.math.isNan(want)))) {
                std.debug.print(
                    "wgsl parity diverged for {s} at index {d}: got {d}, want {d}, raw[0..8] = {x}\n",
                    .{ @tagName(fmt), k_abs, got, want, raw[0..@min(raw.len, 8)] },
                );
                return error.WgslParityDiverged;
            }
        }
    }
}

test "wasm_compute: wgsl parity Q1_0" {
    try runParity(.Q1_0, wgslQ1_0, 8);
}

test "wasm_compute: wgsl parity I8_S" {
    try runParity(.I8_S, wgslI8_S, 8);
}

test "wasm_compute: wgsl parity TQ2_0" {
    try runParity(.TQ2_0, wgslTQ2_0, 8);
}

test "wasm_compute: wgsl parity TQ1_0" {
    try runParity(.TQ1_0, wgslTQ1_0, 8);
}

test "wasm_compute: wgsl parity MXFP4" {
    try runParity(.MXFP4, wgslMXFP4, 8);
}

test "wasm_compute: wgsl parity NVFP4" {
    try runParity(.NVFP4, wgslNVFP4, 8);
}

test "wasm_compute: wgsl parity IQ2_XXS" {
    try runParity(.IQ2_XXS, wgslIQ2_XXS, 8);
}

test "wasm_compute: wgsl parity IQ2_XS" {
    try runParity(.IQ2_XS, wgslIQ2_XS, 8);
}

test "wasm_compute: wgsl parity IQ2_S" {
    try runParity(.IQ2_S, wgslIQ2_S, 8);
}

test "wasm_compute: wgsl parity IQ3_XXS" {
    try runParity(.IQ3_XXS, wgslIQ3_XXS, 8);
}

test "wasm_compute: wgsl parity IQ3_S" {
    try runParity(.IQ3_S, wgslIQ3_S, 8);
}

test "wasm_compute: wgsl parity IQ1_S" {
    try runParity(.IQ1_S, wgslIQ1_S, 8);
}

test "wasm_compute: wgsl parity IQ1_M" {
    try runParity(.IQ1_M, wgslIQ1_M, 8);
}
