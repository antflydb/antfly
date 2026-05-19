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

const std = @import("std");

pub const supported_head_dims = [_]u32{ 64, 80, 128, 256 };
pub const turbo3_residual_bits_per_head: u32 = 32;
pub const turbo3_residual_default_scale: f32 = 0.125;
const dot_lanes = 8;
const F32xN = @Vector(dot_lanes, f32);

pub fn isSupportedHeadDim(head_dim: u32) bool {
    inline for (supported_head_dims) |supported| {
        if (head_dim == supported) return true;
    }
    return false;
}

pub fn polar4KeyBytes(num_kv_heads: u32, head_dim: u32) usize {
    if (!isSupportedHeadDim(head_dim)) return 0;
    const values: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
    return (values + 1) / 2;
}

pub fn turbo3KeyBytes(num_kv_heads: u32, head_dim: u32) usize {
    if (!isSupportedHeadDim(head_dim)) return 0;
    const values: usize = @as(usize, num_kv_heads) * @as(usize, head_dim);
    return (values * 3 + 7) / 8;
}

pub fn turbo3ResidualBytes(num_kv_heads: u32, head_dim: u32) usize {
    if (!isSupportedHeadDim(head_dim)) return 0;
    return (@as(usize, num_kv_heads) * turbo3_residual_bits_per_head + 7) / 8;
}

pub fn encodePolar4Key(src: []const f32, dst: []u8, num_kv_heads: u32, head_dim: u32) !void {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const expected = polar4KeyBytes(num_kv_heads, head_dim);
    if (dst.len != expected or src.len != @as(usize, num_kv_heads) * @as(usize, head_dim)) {
        return error.InvalidKvRowWidth;
    }

    var i: usize = 0;
    var out: usize = 0;
    while (i < src.len) : (i += 2) {
        const lo = encodePolar4Scalar(src[i]);
        const hi = if (i + 1 < src.len) encodePolar4Scalar(src[i + 1]) else 0;
        dst[out] = lo | (hi << 4);
        out += 1;
    }
}

pub fn decodePolar4Key(src: []const u8, dst: []f32, num_kv_heads: u32, head_dim: u32) !void {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const expected = polar4KeyBytes(num_kv_heads, head_dim);
    if (src.len != expected or dst.len != @as(usize, num_kv_heads) * @as(usize, head_dim)) {
        return error.InvalidKvRowWidth;
    }

    var i: usize = 0;
    for (src) |packed_byte| {
        if (i < dst.len) {
            dst[i] = decodePolar4Scalar(packed_byte & 0x0f);
            i += 1;
        }
        if (i < dst.len) {
            dst[i] = decodePolar4Scalar((packed_byte >> 4) & 0x0f);
            i += 1;
        }
    }
}

pub fn encodeTurbo3Key(src: []const f32, dst: []u8, num_kv_heads: u32, head_dim: u32) !void {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const expected = turbo3KeyBytes(num_kv_heads, head_dim);
    if (dst.len != expected or src.len != @as(usize, num_kv_heads) * @as(usize, head_dim)) {
        return error.InvalidKvRowWidth;
    }

    @memset(dst, 0);
    for (src, 0..) |value, i| {
        setPacked3(dst, i, encodeTurbo3Scalar(value));
    }
}

pub fn decodeTurbo3Key(src: []const u8, dst: []f32, num_kv_heads: u32, head_dim: u32) !void {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const expected = turbo3KeyBytes(num_kv_heads, head_dim);
    if (src.len != expected or dst.len != @as(usize, num_kv_heads) * @as(usize, head_dim)) {
        return error.InvalidKvRowWidth;
    }

    for (dst, 0..) |*value, i| {
        value.* = decodeTurbo3Scalar(getPacked3(src, i));
    }
}

pub fn encodeTurbo3ResidualSketch(src: []const f32, base_key: []const u8, dst: []u8, num_kv_heads: u32, head_dim: u32) !void {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const hd: usize = head_dim;
    if (src.len != @as(usize, num_kv_heads) * hd) return error.InvalidKvRowWidth;
    if (base_key.len != turbo3KeyBytes(num_kv_heads, head_dim)) return error.InvalidKvRowWidth;
    if (dst.len != turbo3ResidualBytes(num_kv_heads, head_dim)) return error.InvalidKvRowWidth;

    @memset(dst, 0);
    for (0..@as(usize, num_kv_heads)) |head| {
        const value_start = head * hd;
        for (0..turbo3_residual_bits_per_head) |projection| {
            var projected_residual: f32 = 0.0;
            for (0..hd) |d| {
                const decoded = decodeTurbo3Scalar(getPacked3(base_key, value_start + d));
                const residual = src[value_start + d] - decoded;
                projected_residual += randomSign(head, projection, d) * residual;
            }
            setBit(dst, head * turbo3_residual_bits_per_head + projection, projected_residual >= 0.0);
        }
    }
}

pub fn dotPolar4Key(query: []const f32, encoded_key: []const u8, num_kv_heads: u32, head_dim: u32, kv_head: usize) !f32 {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const hd: usize = head_dim;
    if (query.len != hd) return error.InvalidKvRowWidth;
    if (encoded_key.len != polar4KeyBytes(num_kv_heads, head_dim)) return error.InvalidKvRowWidth;
    if (kv_head >= num_kv_heads) return error.InvalidKvHead;

    const value_start = kv_head * hd;
    var sum: f32 = 0.0;
    for (0..hd) |d| {
        sum += query[d] * decodePolar4At(encoded_key, value_start + d);
    }
    return sum;
}

pub fn dotPolar4KeyFast(query: []const f32, encoded_key: []const u8, num_kv_heads: u32, head_dim: u32, kv_head: usize) !f32 {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const hd: usize = head_dim;
    if (query.len != hd) return error.InvalidKvRowWidth;
    if (encoded_key.len != polar4KeyBytes(num_kv_heads, head_dim)) return error.InvalidKvRowWidth;
    if (kv_head >= num_kv_heads) return error.InvalidKvHead;

    return switch (head_dim) {
        64 => dotPolar4KeyPackedVector(query, encoded_key, kv_head, 64),
        80 => dotPolar4Key(query, encoded_key, num_kv_heads, head_dim, kv_head),
        128 => dotPolar4KeyPackedVector(query, encoded_key, kv_head, 128),
        256 => dotPolar4Key(query, encoded_key, num_kv_heads, head_dim, kv_head),
        else => unreachable,
    };
}

pub fn dotTurbo3Key(query: []const f32, encoded_key: []const u8, num_kv_heads: u32, head_dim: u32, kv_head: usize) !f32 {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const hd: usize = head_dim;
    if (query.len != hd) return error.InvalidKvRowWidth;
    if (encoded_key.len != turbo3KeyBytes(num_kv_heads, head_dim)) return error.InvalidKvRowWidth;
    if (kv_head >= num_kv_heads) return error.InvalidKvHead;

    const value_start = kv_head * hd;
    var sum: f32 = 0.0;
    for (0..hd) |d| {
        sum += query[d] * decodeTurbo3Scalar(getPacked3(encoded_key, value_start + d));
    }
    return sum;
}

pub fn dotTurbo3KeyFast(query: []const f32, encoded_key: []const u8, num_kv_heads: u32, head_dim: u32, kv_head: usize) !f32 {
    return dotTurbo3Key(query, encoded_key, num_kv_heads, head_dim, kv_head);
}

pub fn dotTurbo3ResidualSketch(query: []const f32, residual_sketch: []const u8, num_kv_heads: u32, head_dim: u32, kv_head: usize) !f32 {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const hd: usize = head_dim;
    if (query.len != hd) return error.InvalidKvRowWidth;

    var projected_query: [turbo3_residual_bits_per_head]f32 = undefined;
    try projectTurbo3ResidualQuery(query, &projected_query, head_dim, kv_head);
    return dotTurbo3ProjectedResidualSketch(&projected_query, residual_sketch, num_kv_heads, head_dim, kv_head);
}

pub fn projectTurbo3ResidualQuery(query: []const f32, dst: []f32, head_dim: u32, kv_head: usize) !void {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    const hd: usize = head_dim;
    if (query.len != hd or dst.len != turbo3_residual_bits_per_head) return error.InvalidKvRowWidth;
    for (0..turbo3_residual_bits_per_head) |projection| {
        var projected_query: f32 = 0.0;
        for (0..hd) |d| {
            projected_query += randomSign(kv_head, projection, d) * query[d];
        }
        dst[projection] = projected_query;
    }
}

pub fn dotTurbo3ProjectedResidualSketch(projected_query: []const f32, residual_sketch: []const u8, num_kv_heads: u32, head_dim: u32, kv_head: usize) !f32 {
    if (!isSupportedHeadDim(head_dim)) return error.UnsupportedKvHeadDim;
    if (projected_query.len != turbo3_residual_bits_per_head) return error.InvalidKvRowWidth;
    if (residual_sketch.len != turbo3ResidualBytes(num_kv_heads, head_dim)) return error.InvalidKvRowWidth;
    if (kv_head >= num_kv_heads) return error.InvalidKvHead;

    var acc: f32 = 0.0;
    for (0..turbo3_residual_bits_per_head) |projection| {
        const residual_sign: f32 = if (getBit(residual_sketch, kv_head * turbo3_residual_bits_per_head + projection)) 1.0 else -1.0;
        acc += residual_sign * projected_query[projection];
    }
    return acc / @as(f32, @floatFromInt(turbo3_residual_bits_per_head));
}

fn dotPolar4KeyPackedVector(query: []const f32, encoded_key: []const u8, kv_head: usize, comptime head_dim: usize) f32 {
    const byte_start = kv_head * (head_dim / 2);
    const head_bytes = encoded_key[byte_start..][0 .. head_dim / 2];
    var acc_lo: F32xN = @splat(0.0);
    var acc_hi: F32xN = @splat(0.0);

    var byte_index: usize = 0;
    while (byte_index < head_bytes.len) : (byte_index += dot_lanes) {
        var q_lo: F32xN = undefined;
        var q_hi: F32xN = undefined;
        var k_lo: F32xN = undefined;
        var k_hi: F32xN = undefined;
        inline for (0..dot_lanes) |lane| {
            const packed_byte = head_bytes[byte_index + lane];
            q_lo[lane] = query[(byte_index + lane) * 2];
            q_hi[lane] = query[(byte_index + lane) * 2 + 1];
            k_lo[lane] = decodePolar4Scalar(packed_byte & 0x0f);
            k_hi[lane] = decodePolar4Scalar((packed_byte >> 4) & 0x0f);
        }
        acc_lo += q_lo * k_lo;
        acc_hi += q_hi * k_hi;
    }

    return @reduce(.Add, acc_lo + acc_hi);
}

fn encodePolar4Scalar(value: f32) u8 {
    const clipped = std.math.clamp(value, -1.0, 1.0);
    const scaled = @round((clipped + 1.0) * 7.5);
    return @as(u8, @intFromFloat(std.math.clamp(scaled, 0.0, 15.0)));
}

fn decodePolar4Scalar(code: u8) f32 {
    return (@as(f32, @floatFromInt(code & 0x0f)) / 7.5) - 1.0;
}

fn decodePolar4At(encoded_key: []const u8, value_index: usize) f32 {
    const packed_byte = encoded_key[value_index / 2];
    const code = if (value_index % 2 == 0) packed_byte & 0x0f else (packed_byte >> 4) & 0x0f;
    return decodePolar4Scalar(code);
}

fn encodeTurbo3Scalar(value: f32) u8 {
    const clipped = std.math.clamp(value, -1.0, 1.0);
    const scaled = @round((clipped + 1.0) * 3.5);
    return @as(u8, @intFromFloat(std.math.clamp(scaled, 0.0, 7.0)));
}

fn decodeTurbo3Scalar(code: u8) f32 {
    return (@as(f32, @floatFromInt(code & 0x07)) / 3.5) - 1.0;
}

fn setPacked3(dst: []u8, value_index: usize, code: u8) void {
    const bit_offset = value_index * 3;
    const byte_index = bit_offset / 8;
    const shift: u4 = @intCast(bit_offset % 8);
    const bits: u16 = @as(u16, code & 0x07) << shift;
    dst[byte_index] |= @intCast(bits & 0xff);
    if (shift > 5) {
        dst[byte_index + 1] |= @intCast(bits >> 8);
    }
}

fn getPacked3(src: []const u8, value_index: usize) u8 {
    const bit_offset = value_index * 3;
    const byte_index = bit_offset / 8;
    const shift: u4 = @intCast(bit_offset % 8);
    var bits: u16 = @as(u16, src[byte_index]) >> shift;
    if (shift > 5) {
        bits |= @as(u16, src[byte_index + 1]) << (8 - shift);
    }
    return @intCast(bits & 0x07);
}

fn setBit(dst: []u8, bit_index: usize, value: bool) void {
    const byte_index = bit_index / 8;
    const mask: u8 = @as(u8, 1) << @intCast(bit_index % 8);
    if (value) dst[byte_index] |= mask else dst[byte_index] &= ~mask;
}

fn getBit(src: []const u8, bit_index: usize) bool {
    const byte_index = bit_index / 8;
    const mask: u8 = @as(u8, 1) << @intCast(bit_index % 8);
    return (src[byte_index] & mask) != 0;
}

fn randomSign(head: usize, projection: usize, dim: usize) f32 {
    var x = @as(u64, head + 1) *% 0x9e3779b97f4a7c15;
    x ^= @as(u64, projection + 1) *% 0xbf58476d1ce4e5b9;
    x ^= @as(u64, dim + 1) *% 0x94d049bb133111eb;
    x ^= x >> 30;
    x *%= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *%= 0x94d049bb133111eb;
    x ^= x >> 31;
    return if ((x & 1) == 0) 1.0 else -1.0;
}

test "polar4 supports current metal head dimensions" {
    try std.testing.expect(isSupportedHeadDim(64));
    try std.testing.expect(isSupportedHeadDim(80));
    try std.testing.expect(isSupportedHeadDim(128));
    try std.testing.expect(!isSupportedHeadDim(96));
}

test "polar4 key bytes packs two values per byte" {
    try std.testing.expectEqual(@as(usize, 256), polar4KeyBytes(8, 64));
    try std.testing.expectEqual(@as(usize, 320), polar4KeyBytes(8, 80));
    try std.testing.expectEqual(@as(usize, 512), polar4KeyBytes(8, 128));
    try std.testing.expectEqual(@as(usize, 1024), polar4KeyBytes(8, 256));
    try std.testing.expectEqual(@as(usize, 0), polar4KeyBytes(8, 96));
}

test "turbo3 key bytes packs three bits per value" {
    try std.testing.expectEqual(@as(usize, 192), turbo3KeyBytes(8, 64));
    try std.testing.expectEqual(@as(usize, 240), turbo3KeyBytes(8, 80));
    try std.testing.expectEqual(@as(usize, 384), turbo3KeyBytes(8, 128));
    try std.testing.expectEqual(@as(usize, 768), turbo3KeyBytes(8, 256));
    try std.testing.expectEqual(@as(usize, 0), turbo3KeyBytes(8, 96));
}

test "turbo3 residual bytes stores fixed one-bit sketch per head" {
    try std.testing.expectEqual(@as(usize, 32), turbo3ResidualBytes(8, 64));
    try std.testing.expectEqual(@as(usize, 32), turbo3ResidualBytes(8, 80));
    try std.testing.expectEqual(@as(usize, 32), turbo3ResidualBytes(8, 128));
    try std.testing.expectEqual(@as(usize, 32), turbo3ResidualBytes(8, 256));
    try std.testing.expectEqual(@as(usize, 0), turbo3ResidualBytes(8, 96));
}

test "polar4 key round trip" {
    var src: [64]f32 = undefined;
    for (&src, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) / 8.0;
    }

    var encoded: [32]u8 = undefined;
    try encodePolar4Key(&src, &encoded, 1, 64);

    var decoded: [64]f32 = undefined;
    try decodePolar4Key(&encoded, &decoded, 1, 64);

    for (src, decoded) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.08);
    }
}

test "turbo3 key round trip" {
    var src: [64]f32 = undefined;
    for (&src, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 17)) - 8)) / 8.0;
    }

    var encoded: [24]u8 = undefined;
    try encodeTurbo3Key(&src, &encoded, 1, 64);

    var decoded: [64]f32 = undefined;
    try decodeTurbo3Key(&encoded, &decoded, 1, 64);

    for (src, decoded) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.15);
    }
}

test "polar4 direct dot matches decoded dot" {
    var key: [64]f32 = undefined;
    var query: [64]f32 = undefined;
    for (0..64) |i| {
        key[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i % 9)) - 4)) / 4.0;
        query[i] = @as(f32, @floatFromInt(@as(i32, @intCast((i * 3) % 11)) - 5)) / 5.0;
    }

    var encoded: [32]u8 = undefined;
    try encodePolar4Key(&key, &encoded, 1, 64);
    var decoded: [64]f32 = undefined;
    try decodePolar4Key(&encoded, &decoded, 1, 64);

    var decoded_dot: f32 = 0.0;
    for (query, decoded) |q, k| decoded_dot += q * k;
    const direct_dot = try dotPolar4Key(&query, &encoded, 1, 64, 0);
    try std.testing.expectApproxEqAbs(decoded_dot, direct_dot, 1e-5);
}

test "turbo3 direct dot matches decoded dot" {
    var key: [64]f32 = undefined;
    var query: [64]f32 = undefined;
    for (0..64) |i| {
        key[i] = @as(f32, @floatFromInt(@as(i32, @intCast(i % 9)) - 4)) / 4.0;
        query[i] = @as(f32, @floatFromInt(@as(i32, @intCast((i * 3) % 11)) - 5)) / 5.0;
    }

    var encoded: [24]u8 = undefined;
    try encodeTurbo3Key(&key, &encoded, 1, 64);
    var decoded: [64]f32 = undefined;
    try decodeTurbo3Key(&encoded, &decoded, 1, 64);

    var decoded_dot: f32 = 0.0;
    for (query, decoded) |q, k| decoded_dot += q * k;
    const direct_dot = try dotTurbo3Key(&query, &encoded, 1, 64, 0);
    try std.testing.expectApproxEqAbs(decoded_dot, direct_dot, 1e-5);
}

test "turbo3 residual sketch is deterministic and query-dependent" {
    var key: [128]f32 = undefined;
    var query_a: [64]f32 = undefined;
    var query_b: [64]f32 = undefined;
    for (&key, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 31)) - 15)) / 15.0;
    }
    for (&query_a, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 29)) - 14)) / 14.0;
    }
    for (&query_b, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 11) % 37)) - 18)) / 18.0;
    }

    var encoded: [turbo3KeyBytes(2, 64)]u8 = undefined;
    try encodeTurbo3Key(&key, &encoded, 2, 64);

    var sketch_a: [turbo3ResidualBytes(2, 64)]u8 = undefined;
    var sketch_b: [turbo3ResidualBytes(2, 64)]u8 = undefined;
    try encodeTurbo3ResidualSketch(&key, &encoded, &sketch_a, 2, 64);
    try encodeTurbo3ResidualSketch(&key, &encoded, &sketch_b, 2, 64);
    try std.testing.expectEqualSlices(u8, &sketch_a, &sketch_b);

    const score_a = try dotTurbo3ResidualSketch(&query_a, &sketch_a, 2, 64, 1);
    const score_b = try dotTurbo3ResidualSketch(&query_b, &sketch_a, 2, 64, 1);
    try std.testing.expect(@abs(score_a - score_b) > 1e-4);
}

test "polar4 fast dot matches scalar dot for supported head dimensions" {
    inline for (supported_head_dims) |head_dim| {
        var key: [head_dim * 2]f32 = undefined;
        var query: [head_dim]f32 = undefined;
        for (&key, 0..) |*value, i| {
            value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 31)) - 15)) / 15.0;
        }
        for (&query, 0..) |*value, i| {
            value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5) % 29)) - 14)) / 14.0;
        }

        var encoded: [polar4KeyBytes(2, head_dim)]u8 = undefined;
        try encodePolar4Key(&key, &encoded, 2, head_dim);

        const scalar_dot = try dotPolar4Key(&query, &encoded, 2, head_dim, 1);
        const fast_dot = try dotPolar4KeyFast(&query, &encoded, 2, head_dim, 1);
        try std.testing.expectApproxEqAbs(scalar_dot, fast_dot, 1e-5);
    }
}
