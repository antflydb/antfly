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

/// Decode a block of document deltas and turn it into absolute document IDs.
/// `previous` is the first absolute document ID; the encoded block's first
/// lane is the format-mandated zero placeholder. The inclusive scan uses only
/// portable Zig vectors so LLVM can select the target's normal SIMD lowering.
pub fn decodeBlockPrefixSum(src: []const u8, values: *[block_values]u32, bits: u8, previous: u32) !usize {
    const needed = try encodedLen(bits);
    if (src.len < needed) return error.Truncated;

    const zero: U32x4 = @splat(0);
    const shift_one: @Vector(lanes, i32) = .{ -1, 0, 1, 2 };
    const shift_two: @Vector(lanes, i32) = .{ -1, -2, 0, 1 };
    const mask: U32x4 = @splat(valueMask(bits));
    var carry = previous;
    for (0..groups) |group| {
        const bit_position = group * @as(usize, bits);
        const word_index = bit_position / 32;
        const shift: u5 = @intCast(bit_position % 32);
        var value: U32x4 = if (bits == 0)
            zero
        else
            loadWordVector(src, word_index) >> @as(@Vector(lanes, u5), @splat(shift));
        if (bits != 0 and @as(u8, shift) + bits > 32) {
            const left_shift: u5 = @intCast(32 - @as(u8, shift));
            value |= loadWordVector(src, word_index + 1) << @as(@Vector(lanes, u5), @splat(left_shift));
        }
        value &= mask;
        // The first encoded lane is padding because the first absolute doc ID
        // is stored separately. Preserve the old decoder's behavior of
        // ignoring it even if malformed input sets those bits.
        if (group == 0) value[0] = 0;
        value +%= @shuffle(u32, value, zero, shift_one);
        value +%= @shuffle(u32, value, zero, shift_two);
        value +%= @as(U32x4, @splat(carry));
        values[group * lanes ..][0..lanes].* = value;
        carry = value[lanes - 1];
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

test "portable vertical BP128 fuses document delta prefix sums" {
    var random = std.Random.DefaultPrng.init(0x5052_4546_4958_5355);
    const rng = random.random();
    var encoded: [32 * 16]u8 = undefined;
    var deltas: [block_values]u32 = undefined;
    var decoded: [block_values]u32 = undefined;

    for (0..33) |bits_usize| {
        const bits: u8 = @intCast(bits_usize);
        const mask = valueMask(bits);
        deltas[0] = 0;
        for (deltas[1..]) |*delta| delta.* = rng.int(u32) & mask;
        const encoded_len = try encodeBlock(&encoded, &deltas, bits);
        const first = rng.int(u32);
        _ = try decodeBlockPrefixSum(encoded[0..encoded_len], &decoded, bits, first);

        var expected = first;
        for (deltas, decoded) |delta, actual| {
            expected +%= delta;
            try std.testing.expectEqual(expected, actual);
        }

        if (bits > 0) {
            encoded[0] |= 1;
            _ = try decodeBlockPrefixSum(encoded[0..encoded_len], &decoded, bits, first);
            try std.testing.expectEqual(first, decoded[0]);
        }
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
