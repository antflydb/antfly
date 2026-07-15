// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Portable vertical BP128 codec.
//!
//! The block is viewed as 32 consecutive vectors of four values. Each vector
//! lane owns an independent 32-value bitstream; words from the four streams are
//! stored together as one little-endian `@Vector(4, u32)`. The representation
//! therefore consumes exactly `128 * bits / 8` bytes while decoding four
//! consecutive values with portable Zig vector shifts and masks.

const std = @import("std");

pub const block_values: usize = 128;
pub const lanes: usize = 4;
pub const groups: usize = block_values / lanes;
const U32x4 = @Vector(lanes, u32);

pub fn encodedLen(bits: u8) !usize {
    if (bits > 32) return error.InvalidBitWidth;
    return @as(usize, bits) * 16;
}

fn valueMask(bits: u8) u32 {
    return if (bits == 32) std.math.maxInt(u32) else (@as(u32, 1) << @intCast(bits)) - 1;
}

fn storeWordVector(dst: []u8, word_index: usize, words: U32x4) void {
    const offset = word_index * 16;
    if (comptime @import("builtin").target.cpu.arch.endian() == .little) {
        const bytes: [16]u8 = @bitCast(words);
        @memcpy(dst[offset..][0..16], &bytes);
    } else {
        const scalar: [lanes]u32 = words;
        inline for (0..lanes) |lane| {
            std.mem.writeInt(u32, dst[offset + lane * 4 ..][0..4], scalar[lane], .little);
        }
    }
}

fn loadWordVector(src: []const u8, word_index: usize) U32x4 {
    const offset = word_index * 16;
    if (comptime @import("builtin").target.cpu.arch.endian() == .little) {
        const bytes: [16]u8 = src[offset..][0..16].*;
        return @bitCast(bytes);
    }
    var scalar: [lanes]u32 = undefined;
    inline for (0..lanes) |lane| {
        scalar[lane] = std.mem.readInt(u32, src[offset + lane * 4 ..][0..4], .little);
    }
    return scalar;
}

/// Encode exactly 128 integers. Callers choose `bits` from the block maximum.
pub fn encodeBlock(dst: []u8, values: *const [block_values]u32, bits: u8) !usize {
    const needed = try encodedLen(bits);
    if (dst.len < needed) return error.BufferTooSmall;
    if (bits == 0) return 0;

    const mask = valueMask(bits);
    var words: [32]U32x4 = @splat(@as(U32x4, @splat(0)));
    for (0..groups) |group| {
        const value: U32x4 = values[group * lanes ..][0..lanes].*;
        if (@reduce(.Or, value & @as(U32x4, @splat(~mask))) != 0) return error.ValueOutOfRange;

        const bit_position = group * @as(usize, bits);
        const word_index = bit_position / 32;
        const shift: u5 = @intCast(bit_position % 32);
        words[word_index] |= value << @as(@Vector(lanes, u5), @splat(shift));
        if (@as(u8, shift) + bits > 32) {
            const right_shift: u5 = @intCast(32 - @as(u8, shift));
            words[word_index + 1] |= value >> @as(@Vector(lanes, u5), @splat(right_shift));
        }
    }

    for (0..bits) |word_index| storeWordVector(dst, word_index, words[word_index]);
    return needed;
}

/// Decode exactly 128 integers using portable Zig vector operations.
pub fn decodeBlock(src: []const u8, values: *[block_values]u32, bits: u8) !usize {
    const needed = try encodedLen(bits);
    if (src.len < needed) return error.Truncated;
    if (bits == 0) {
        @memset(values, 0);
        return 0;
    }

    const mask: U32x4 = @splat(valueMask(bits));
    for (0..groups) |group| {
        const bit_position = group * @as(usize, bits);
        const word_index = bit_position / 32;
        const shift: u5 = @intCast(bit_position % 32);
        var value = loadWordVector(src, word_index) >> @as(@Vector(lanes, u5), @splat(shift));
        if (@as(u8, shift) + bits > 32) {
            const left_shift: u5 = @intCast(32 - @as(u8, shift));
            value |= loadWordVector(src, word_index + 1) << @as(@Vector(lanes, u5), @splat(left_shift));
        }
        values[group * lanes ..][0..lanes].* = value & mask;
    }
    return needed;
}

/// Scalar reference decoder for the same vertical representation.
pub fn decodeBlockScalar(src: []const u8, values: *[block_values]u32, bits: u8) !usize {
    const needed = try encodedLen(bits);
    if (src.len < needed) return error.Truncated;
    if (bits == 0) {
        @memset(values, 0);
        return 0;
    }

    const mask = valueMask(bits);
    for (0..groups) |group| {
        const bit_position = group * @as(usize, bits);
        const word_index = bit_position / 32;
        const shift: u5 = @intCast(bit_position % 32);
        for (0..lanes) |lane| {
            const word_offset = word_index * 16 + lane * 4;
            var value = std.mem.readInt(u32, src[word_offset..][0..4], .little) >> shift;
            if (@as(u8, shift) + bits > 32) {
                const next_offset = (word_index + 1) * 16 + lane * 4;
                value |= std.mem.readInt(u32, src[next_offset..][0..4], .little) << @intCast(32 - @as(u8, shift));
            }
            values[group * lanes + lane] = value & mask;
        }
    }
    return needed;
}

test "portable vertical BP128 round-trips every bit width" {
    var random = std.Random.DefaultPrng.init(0x4250_3132_385f_5349);
    const rng = random.random();
    var encoded: [32 * 16]u8 = undefined;
    var input: [block_values]u32 = undefined;
    var vector_output: [block_values]u32 = undefined;
    var scalar_output: [block_values]u32 = undefined;

    for (0..33) |bits_usize| {
        const bits: u8 = @intCast(bits_usize);
        const mask = valueMask(bits);
        for (&input) |*value| value.* = rng.int(u32) & mask;

        const encoded_len = try encodeBlock(&encoded, &input, bits);
        try std.testing.expectEqual(try encodedLen(bits), encoded_len);
        try std.testing.expectEqual(encoded_len, try decodeBlock(encoded[0..encoded_len], &vector_output, bits));
        try std.testing.expectEqual(encoded_len, try decodeBlockScalar(encoded[0..encoded_len], &scalar_output, bits));
        try std.testing.expectEqualSlices(u32, &input, &vector_output);
        try std.testing.expectEqualSlices(u32, &input, &scalar_output);
    }
}

test "portable vertical BP128 validates buffers and values" {
    var input: [block_values]u32 = @splat(3);
    var encoded: [16]u8 = undefined;
    try std.testing.expectError(error.ValueOutOfRange, encodeBlock(&encoded, &input, 1));
    input = @splat(1);
    _ = try encodeBlock(&encoded, &input, 1);
    var output: [block_values]u32 = undefined;
    try std.testing.expectError(error.Truncated, decodeBlock(encoded[0..15], &output, 1));
    try std.testing.expectError(error.InvalidBitWidth, encodedLen(33));
}
