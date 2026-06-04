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

//! StreamVByte: SIMD-accelerated variable-length uint32 encoding.
//!
//! Port of go-highway/hwy/contrib/varint StreamVByte implementation.
//! Wire-compatible with the Go version used in zapx.
//!
//! Format: each group of 4 values has a 1-byte control header where each
//! 2-bit field encodes (byte_length - 1) for that value. Data bytes follow
//! in little-endian order, packed tightly.
//!
//! Uses Zig @shuffle on @Vector(16, u8) which compiles to:
//!   - vpshufb on x86 (AVX2/SSE)
//!   - tbl on ARM64 (NEON)

const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================================
// Lookup tables (computed at comptime)
// ============================================================================

/// Maps control byte -> total data bytes for 4 values.
const data_len_table: [256]u8 = blk: {
    @setEvalBranchQuota(10_000);
    var table: [256]u8 = undefined;
    for (0..256) |ctrl| {
        const len0 = ((ctrl >> 0) & 0x3) + 1;
        const len1 = ((ctrl >> 2) & 0x3) + 1;
        const len2 = ((ctrl >> 4) & 0x3) + 1;
        const len3 = ((ctrl >> 6) & 0x3) + 1;
        table[ctrl] = @intCast(len0 + len1 + len2 + len3);
    }
    break :blk table;
};

/// Decode shuffle masks: for each control byte, indices to scatter data bytes
/// into 4 little-endian uint32 slots. Index 0x80+ means output zero (padding).
const decode_shuffle_masks: [256][16]u8 = blk: {
    @setEvalBranchQuota(100_000);
    var masks: [256][16]u8 = undefined;
    for (0..256) |ctrl| {
        const len0 = ((ctrl >> 0) & 0x3) + 1;
        const len1 = ((ctrl >> 2) & 0x3) + 1;
        const len2 = ((ctrl >> 4) & 0x3) + 1;
        const len3 = ((ctrl >> 6) & 0x3) + 1;

        const off0 = 0;
        const off1 = len0;
        const off2 = len0 + len1;
        const off3 = len0 + len1 + len2;

        var mask: [16]u8 = undefined;
        // Value 0 at positions 0-3
        for (0..4) |i| {
            mask[i] = if (i < len0) @intCast(off0 + i) else 0x80;
        }
        // Value 1 at positions 4-7
        for (0..4) |i| {
            mask[4 + i] = if (i < len1) @intCast(off1 + i) else 0x80;
        }
        // Value 2 at positions 8-11
        for (0..4) |i| {
            mask[8 + i] = if (i < len2) @intCast(off2 + i) else 0x80;
        }
        // Value 3 at positions 12-15
        for (0..4) |i| {
            mask[12 + i] = if (i < len3) @intCast(off3 + i) else 0x80;
        }
        masks[ctrl] = mask;
    }
    break :blk masks;
};

/// Encode shuffle masks: for each control byte, indices to gather bytes from
/// 4 little-endian uint32s (16 bytes) into packed output.
const encode_shuffle_masks: [256][16]u8 = blk: {
    @setEvalBranchQuota(100_000);
    var masks: [256][16]u8 = undefined;
    for (0..256) |ctrl| {
        const len0 = ((ctrl >> 0) & 0x3) + 1;
        const len1 = ((ctrl >> 2) & 0x3) + 1;
        const len2 = ((ctrl >> 4) & 0x3) + 1;
        const len3 = ((ctrl >> 6) & 0x3) + 1;

        var mask: [16]u8 = undefined;
        var out_pos: usize = 0;

        // Value 0: bytes at input positions 0-3
        for (0..len0) |i| {
            mask[out_pos] = @intCast(i);
            out_pos += 1;
        }
        // Value 1: bytes at input positions 4-7
        for (0..len1) |i| {
            mask[out_pos] = @intCast(4 + i);
            out_pos += 1;
        }
        // Value 2: bytes at input positions 8-11
        for (0..len2) |i| {
            mask[out_pos] = @intCast(8 + i);
            out_pos += 1;
        }
        // Value 3: bytes at input positions 12-15
        for (0..len3) |i| {
            mask[out_pos] = @intCast(12 + i);
            out_pos += 1;
        }
        // Fill remaining with 0x80 (unused)
        while (out_pos < 16) : (out_pos += 1) {
            mask[out_pos] = 0x80;
        }
        masks[ctrl] = mask;
    }
    break :blk masks;
};

// ============================================================================
// Core SIMD operations
// ============================================================================

/// Number of bytes needed to encode a uint32 value (1-4).
fn encodedLength(v: u32) u3 {
    if (v == 0) return 1;
    // (bit_length - 1) / 8 + 1
    const bits = 32 - @clz(v);
    return @intCast((bits - 1) / 8 + 1);
}

fn decodeShuffleMask(comptime ctrl: u8) @Vector(16, i32) {
    const raw = decode_shuffle_masks[ctrl];
    var mask: [16]i32 = undefined;
    for (0..16) |i| {
        mask[i] = if (raw[i] >= 0x80) -1 else raw[i];
    }
    return mask;
}

fn encodeShuffleMask(comptime ctrl: u8) @Vector(16, i32) {
    const raw = encode_shuffle_masks[ctrl];
    var mask: [16]i32 = undefined;
    for (0..16) |i| {
        mask[i] = if (raw[i] >= 0x80) -1 else raw[i];
    }
    return mask;
}

/// Decode one full group of 4 uint32 values using a real vector shuffle.
/// The control byte is comptime so Zig can lower @shuffle to pshufb/tbl-style
/// instructions on x86/ARM64 instead of scalar lane-by-lane table lookups.
fn decodeGroupShuffle(comptime ctrl: u8, data: []const u8, dst: *[4]u32) usize {
    const data_bytes = data_len_table[ctrl];

    const data_vec: @Vector(16, u8) = data[0..16].*;
    const zero_vec: @Vector(16, u8) = @splat(0);
    const shuffled = @shuffle(u8, data_vec, zero_vec, decodeShuffleMask(ctrl));

    if (comptime @import("builtin").target.cpu.arch.endian() == .little) {
        const words: @Vector(4, u32) = @bitCast(shuffled);
        dst.* = words;
    } else {
        const result_bytes: [16]u8 = shuffled;
        dst[0] = std.mem.readInt(u32, result_bytes[0..4], .little);
        dst[1] = std.mem.readInt(u32, result_bytes[4..8], .little);
        dst[2] = std.mem.readInt(u32, result_bytes[8..12], .little);
        dst[3] = std.mem.readInt(u32, result_bytes[12..16], .little);
    }

    return data_bytes;
}

/// Decode one group of 4 uint32 values using SIMD shuffle.
/// Returns number of data bytes consumed.
fn decodeGroupSimd(ctrl: u8, data: []const u8, dst: *[4]u32) usize {
    return switch (ctrl) {
        inline 0...255 => |comptime_ctrl| decodeGroupShuffle(@intCast(comptime_ctrl), data, dst),
    };
}

fn encodeGroupShuffle(comptime ctrl: u8, values: [4]u32, dst: *[16]u8) usize {
    const n = data_len_table[ctrl];

    const input_bytes: [16]u8 = @bitCast([4]u32{
        std.mem.nativeToLittle(u32, values[0]),
        std.mem.nativeToLittle(u32, values[1]),
        std.mem.nativeToLittle(u32, values[2]),
        std.mem.nativeToLittle(u32, values[3]),
    });
    const input_vec: @Vector(16, u8) = input_bytes;
    const zero_vec: @Vector(16, u8) = @splat(0);
    dst.* = @shuffle(u8, input_vec, zero_vec, encodeShuffleMask(ctrl));

    return n;
}

/// Encode one group of 4 uint32 values using SIMD shuffle.
/// Returns control byte and number of data bytes written.
fn encodeGroupSimd(values: [4]u32, dst: *[16]u8) struct { ctrl: u8, n: usize } {
    // Fast path: all values fit in 1 byte
    const combined = values[0] | values[1] | values[2] | values[3];
    if (combined <= 0xFF) {
        dst[0] = @truncate(values[0]);
        dst[1] = @truncate(values[1]);
        dst[2] = @truncate(values[2]);
        dst[3] = @truncate(values[3]);
        return .{ .ctrl = 0, .n = 4 };
    }

    // Compute control byte
    var ctrl: u8 = 0;
    ctrl |= @as(u8, encodedLength(values[0]) - 1);
    ctrl |= @as(u8, encodedLength(values[1]) - 1) << 2;
    ctrl |= @as(u8, encodedLength(values[2]) - 1) << 4;
    ctrl |= @as(u8, encodedLength(values[3]) - 1) << 6;

    const n = switch (ctrl) {
        inline 0...255 => |comptime_ctrl| encodeGroupShuffle(@intCast(comptime_ctrl), values, dst),
    };

    return .{ .ctrl = ctrl, .n = n };
}

/// Scalar fallback for encoding a partial group (< 4 values).
fn encodeGroupScalar(values: []const u32, dst: []u8) struct { ctrl: u8, n: usize } {
    var ctrl: u8 = 0;
    var pos: usize = 0;

    for (0..4) |i| {
        const v: u32 = if (i < values.len) values[i] else 0;
        const length: usize = encodedLength(v);
        ctrl |= @as(u8, @intCast(length - 1)) << @intCast(i * 2);

        const le = std.mem.nativeToLittle(u32, v);
        const le_bytes: [4]u8 = @bitCast(le);
        for (0..length) |b| {
            dst[pos + b] = le_bytes[b];
        }
        pos += length;
    }

    return .{ .ctrl = ctrl, .n = pos };
}

// ============================================================================
// Public API
// ============================================================================

pub fn encodedControlLen(value_count: usize) usize {
    return (value_count + 3) / 4;
}

pub fn encodedDataCapacity(value_count: usize) usize {
    return encodedControlLen(value_count) * 16;
}

/// Encode uint32 values into caller-owned StreamVByte buffers.
/// Returns the used control/data lengths.
pub fn encodeInto(control: []u8, data: []u8, values: []const u32) !struct { control_len: usize, data_len: usize } {
    if (values.len == 0) return .{ .control_len = 0, .data_len = 0 };

    const num_groups = encodedControlLen(values.len);
    if (control.len < num_groups or data.len < encodedDataCapacity(values.len)) return error.BufferTooSmall;

    var data_pos: usize = 0;
    var val_pos: usize = 0;

    for (0..num_groups) |g| {
        const remaining = values.len - val_pos;
        if (remaining >= 4) {
            var dst: [16]u8 = undefined;
            const result = encodeGroupSimd(
                .{ values[val_pos], values[val_pos + 1], values[val_pos + 2], values[val_pos + 3] },
                &dst,
            );
            @memcpy(data[data_pos..][0..result.n], dst[0..result.n]);
            control[g] = result.ctrl;
            data_pos += result.n;
            val_pos += 4;
        } else {
            const result = encodeGroupScalar(values[val_pos..], data[data_pos..]);
            control[g] = result.ctrl;
            data_pos += result.n;
            val_pos += remaining;
        }
    }

    return .{ .control_len = num_groups, .data_len = data_pos };
}

/// Encode uint32 values into StreamVByte format.
/// Returns owned control and data slices. Caller must free both with `alloc`.
pub fn encode(alloc: Allocator, values: []const u32) !struct { control: []u8, data: []u8 } {
    if (values.len == 0) {
        return .{ .control = &.{}, .data = &.{} };
    }

    // Allocate output buffers
    const num_groups = encodedControlLen(values.len);
    const control = try alloc.alloc(u8, num_groups);
    errdefer alloc.free(control);
    // Worst case: 4 bytes per value, rounded up to full groups of 4
    const data = try alloc.alloc(u8, encodedDataCapacity(values.len));
    errdefer alloc.free(data);

    const encoded = try encodeInto(control, data, values);

    // Shrink data to actual size
    const final_data = try alloc.realloc(data, encoded.data_len);
    return .{ .control = control, .data = final_data };
}

/// Decode StreamVByte format back to uint32 values.
/// `n` is the number of values to decode.
/// Returns owned slice. Caller must free with `alloc`.
pub fn decode(alloc: Allocator, control: []const u8, data: []const u8, n: usize) ![]u32 {
    if (n == 0) return &.{};

    const result = try alloc.alloc(u32, n);
    errdefer alloc.free(result);

    const decoded = decodeInto(control, data, result);
    _ = decoded;

    return result;
}

/// Decode into a pre-allocated destination buffer.
/// Returns number of values decoded and data bytes consumed.
pub fn decodeInto(control: []const u8, data: []const u8, dst: []u32) struct { decoded: usize, data_consumed: usize } {
    if (dst.len == 0 or control.len == 0) {
        return .{ .decoded = 0, .data_consumed = 0 };
    }

    var data_pos: usize = 0;
    var dst_pos: usize = 0;

    for (control) |ctrl| {
        if (dst_pos >= dst.len) break;

        // Full group with enough buffer space for SIMD
        if (dst_pos + 4 <= dst.len and data_pos + 16 <= data.len) {
            var group: [4]u32 = undefined;
            const consumed = decodeGroupSimd(ctrl, data[data_pos..], &group);
            dst[dst_pos] = group[0];
            dst[dst_pos + 1] = group[1];
            dst[dst_pos + 2] = group[2];
            dst[dst_pos + 3] = group[3];
            data_pos += consumed;
            dst_pos += 4;
            continue;
        }

        // Scalar fallback for partial groups or near end of buffer
        for (0..4) |i| {
            if (dst_pos >= dst.len) break;
            const length: usize = @as(usize, ((ctrl >> @intCast(i * 2)) & 0x3)) + 1;
            if (data_pos + length > data.len) {
                return .{ .decoded = dst_pos, .data_consumed = data_pos };
            }

            var v: u32 = 0;
            for (0..length) |b| {
                v |= @as(u32, data[data_pos + b]) << @intCast(b * 8);
            }
            dst[dst_pos] = v;
            dst_pos += 1;
            data_pos += length;
        }
    }

    return .{ .decoded = dst_pos, .data_consumed = data_pos };
}

/// Calculate total data length from control bytes.
pub fn dataLength(control: []const u8) usize {
    var total: usize = 0;
    for (control) |ctrl| {
        total += data_len_table[ctrl];
    }
    return total;
}

// ============================================================================
// Delta encoding helpers
// ============================================================================

/// Delta encode in-place: [a, b, c] -> [a, b-a, c-b]
pub fn deltaEncode(values: []u32) void {
    if (values.len <= 1) return;
    var i = values.len - 1;
    while (i > 0) : (i -= 1) {
        values[i] = values[i] -% values[i - 1];
    }
}

/// Delta decode in-place (prefix sum): [a, d1, d2] -> [a, a+d1, a+d1+d2]
pub fn deltaDecode(values: []u32) void {
    if (values.len <= 1) return;
    for (1..values.len) |i| {
        values[i] = values[i] +% values[i - 1];
    }
}

// ============================================================================
// Tests
// ============================================================================

test "encodedLength" {
    try std.testing.expectEqual(@as(u3, 1), encodedLength(0));
    try std.testing.expectEqual(@as(u3, 1), encodedLength(1));
    try std.testing.expectEqual(@as(u3, 1), encodedLength(255));
    try std.testing.expectEqual(@as(u3, 2), encodedLength(256));
    try std.testing.expectEqual(@as(u3, 2), encodedLength(65535));
    try std.testing.expectEqual(@as(u3, 3), encodedLength(65536));
    try std.testing.expectEqual(@as(u3, 3), encodedLength(0xFFFFFF));
    try std.testing.expectEqual(@as(u3, 4), encodedLength(0x1000000));
    try std.testing.expectEqual(@as(u3, 4), encodedLength(0xFFFFFFFF));
}

test "encode and decode roundtrip - small values" {
    const alloc = std.testing.allocator;
    const values = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8 };

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    // All small values -> control bytes should be 0x00
    try std.testing.expectEqual(@as(u8, 0x00), encoded.control[0]);
    try std.testing.expectEqual(@as(u8, 0x00), encoded.control[1]);
    // 8 values * 1 byte each = 8 data bytes
    try std.testing.expectEqual(@as(usize, 8), encoded.data.len);

    const decoded = try decode(alloc, encoded.control, encoded.data, values.len);
    defer alloc.free(decoded);

    try std.testing.expectEqualSlices(u32, &values, decoded);
}

test "encode and decode roundtrip - mixed sizes" {
    const alloc = std.testing.allocator;
    const values = [_]u32{ 300, 5, 1000, 2, 70000, 0, 0xFFFFFFFF, 42 };

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    const decoded = try decode(alloc, encoded.control, encoded.data, values.len);
    defer alloc.free(decoded);

    try std.testing.expectEqualSlices(u32, &values, decoded);
}

test "encode and decode roundtrip - partial group" {
    const alloc = std.testing.allocator;
    // 5 values = 1 full group + 1 partial
    const values = [_]u32{ 100, 200, 300, 400, 500 };

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    try std.testing.expectEqual(@as(usize, 2), encoded.control.len);

    const decoded = try decode(alloc, encoded.control, encoded.data, values.len);
    defer alloc.free(decoded);

    try std.testing.expectEqualSlices(u32, &values, decoded);
}

test "encode and decode roundtrip - single value" {
    const alloc = std.testing.allocator;
    const values = [_]u32{12345};

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    const decoded = try decode(alloc, encoded.control, encoded.data, 1);
    defer alloc.free(decoded);

    try std.testing.expectEqual(@as(u32, 12345), decoded[0]);
}

test "encode and decode roundtrip - all zeros" {
    const alloc = std.testing.allocator;
    const values = @as([12]u32, @splat(0));

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    // All zeros are 1 byte each, ctrl=0x00
    for (encoded.control) |ctrl| {
        try std.testing.expectEqual(@as(u8, 0x00), ctrl);
    }

    const decoded = try decode(alloc, encoded.control, encoded.data, values.len);
    defer alloc.free(decoded);

    try std.testing.expectEqualSlices(u32, &values, decoded);
}

test "encode and decode roundtrip - large values" {
    const alloc = std.testing.allocator;
    const values = [_]u32{ 0xFFFFFFFF, 0xDEADBEEF, 0xCAFEBABE, 0x12345678 };

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    // All 4-byte values -> ctrl = 0xFF
    try std.testing.expectEqual(@as(u8, 0xFF), encoded.control[0]);
    try std.testing.expectEqual(@as(usize, 16), encoded.data.len);

    const decoded = try decode(alloc, encoded.control, encoded.data, values.len);
    defer alloc.free(decoded);

    try std.testing.expectEqualSlices(u32, &values, decoded);
}

test "decodeInto" {
    const alloc = std.testing.allocator;
    const values = [_]u32{ 10, 20, 30, 40, 50, 60 };

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    var dst: [6]u32 = undefined;
    const result = decodeInto(encoded.control, encoded.data, &dst);
    try std.testing.expectEqual(@as(usize, 6), result.decoded);
    try std.testing.expectEqualSlices(u32, &values, &dst);
}

test "SIMD group decoder handles every control byte" {
    const alloc = std.testing.allocator;

    for (0..256) |ctrl_usize| {
        const ctrl: u8 = @intCast(ctrl_usize);
        var values: [4]u32 = undefined;
        inline for (0..4) |i| {
            const len = ((ctrl >> @intCast(i * 2)) & 0x3) + 1;
            values[i] = switch (len) {
                1 => 0x7f,
                2 => 0x1234,
                3 => 0x123456,
                4 => 0x12345678,
                else => unreachable,
            };
        }

        const encoded = try encode(alloc, &values);
        defer alloc.free(encoded.control);
        defer alloc.free(encoded.data);

        try std.testing.expectEqual(ctrl, encoded.control[0]);
        try std.testing.expectEqual(@as(usize, data_len_table[ctrl]), encoded.data.len);

        var padded: [16]u8 = @splat(0);
        @memcpy(padded[0..encoded.data.len], encoded.data);

        var decoded: [4]u32 = undefined;
        const consumed = decodeGroupSimd(ctrl, &padded, &decoded);

        try std.testing.expectEqual(encoded.data.len, consumed);
        try std.testing.expectEqualSlices(u32, &values, &decoded);
    }
}

test "SIMD group encoder emits scalar StreamVByte bytes for every control byte" {
    const alloc = std.testing.allocator;

    for (0..256) |ctrl_usize| {
        const ctrl: u8 = @intCast(ctrl_usize);
        var values: [4]u32 = undefined;
        var expected: [16]u8 = undefined;
        var expected_len: usize = 0;

        inline for (0..4) |i| {
            const len = ((ctrl >> @intCast(i * 2)) & 0x3) + 1;
            values[i] = switch (len) {
                1 => 0x7f,
                2 => 0x1234,
                3 => 0x123456,
                4 => 0x12345678,
                else => unreachable,
            };

            const le = std.mem.nativeToLittle(u32, values[i]);
            const bytes: [4]u8 = @bitCast(le);
            @memcpy(expected[expected_len..][0..len], bytes[0..len]);
            expected_len += len;
        }

        const encoded = try encode(alloc, &values);
        defer alloc.free(encoded.control);
        defer alloc.free(encoded.data);

        try std.testing.expectEqual(ctrl, encoded.control[0]);
        try std.testing.expectEqual(expected_len, encoded.data.len);
        try std.testing.expectEqualSlices(u8, expected[0..expected_len], encoded.data);
    }
}

test "dataLength" {
    const alloc = std.testing.allocator;
    const values = [_]u32{ 300, 5, 1000, 2 };

    const encoded = try encode(alloc, &values);
    defer alloc.free(encoded.control);
    defer alloc.free(encoded.data);

    try std.testing.expectEqual(encoded.data.len, dataLength(encoded.control));
}

test "delta encode and decode roundtrip" {
    var values = [_]u32{ 100, 150, 200, 350, 400 };
    const original = [_]u32{ 100, 150, 200, 350, 400 };

    deltaEncode(&values);
    // First value unchanged, rest are deltas
    try std.testing.expectEqual(@as(u32, 100), values[0]);
    try std.testing.expectEqual(@as(u32, 50), values[1]);
    try std.testing.expectEqual(@as(u32, 50), values[2]);
    try std.testing.expectEqual(@as(u32, 150), values[3]);
    try std.testing.expectEqual(@as(u32, 50), values[4]);

    deltaDecode(&values);
    try std.testing.expectEqualSlices(u32, &original, &values);
}

test "delta encode with zeros" {
    var values = [_]u32{ 0, 0, 0, 5, 5 };
    const original = [_]u32{ 0, 0, 0, 5, 5 };

    deltaEncode(&values);
    deltaDecode(&values);
    try std.testing.expectEqualSlices(u32, &original, &values);
}

test "wire compatibility - control byte format" {
    // Verify our control byte encoding matches Go's format:
    // 2 bits per value, value = (byte_length - 1)
    // Bits [1:0] = value 0 length-1
    // Bits [3:2] = value 1 length-1
    // Bits [5:4] = value 2 length-1
    // Bits [7:6] = value 3 length-1

    const alloc = std.testing.allocator;

    // All 1-byte values -> ctrl = 0b00_00_00_00 = 0x00
    {
        const vals = [_]u32{ 1, 2, 3, 4 };
        const enc = try encode(alloc, &vals);
        defer alloc.free(enc.control);
        defer alloc.free(enc.data);
        try std.testing.expectEqual(@as(u8, 0x00), enc.control[0]);
    }

    // All 4-byte values -> ctrl = 0b11_11_11_11 = 0xFF
    {
        const vals = [_]u32{ 0x1000000, 0x1000000, 0x1000000, 0x1000000 };
        const enc = try encode(alloc, &vals);
        defer alloc.free(enc.control);
        defer alloc.free(enc.data);
        try std.testing.expectEqual(@as(u8, 0xFF), enc.control[0]);
    }

    // Mixed: 1-byte, 2-byte, 3-byte, 4-byte
    // ctrl = 0b11_10_01_00 = 0xE4
    {
        const vals = [_]u32{ 5, 300, 70000, 0x1000000 };
        const enc = try encode(alloc, &vals);
        defer alloc.free(enc.control);
        defer alloc.free(enc.data);
        try std.testing.expectEqual(@as(u8, 0xE4), enc.control[0]);
    }
}
