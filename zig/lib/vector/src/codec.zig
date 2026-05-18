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
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;
const native_endian = builtin.target.cpu.arch.endian();
const swap_simd_width = 8;
const SimdU32 = @Vector(swap_simd_width, u32);

pub const PackedSparse = struct {
    indices: []u32,
    values: []f32,

    pub fn deinit(self: *PackedSparse, alloc: Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub fn decodePackedF32Base64Alloc(alloc: Allocator, encoded: []const u8) ![]f32 {
    const bytes = try decodeBase64Alloc(alloc, encoded);
    defer alloc.free(bytes);
    return try decodePackedF32BytesAlloc(alloc, bytes);
}

pub fn decodePackedU32Base64Alloc(alloc: Allocator, encoded: []const u8) ![]u32 {
    const bytes = try decodeBase64Alloc(alloc, encoded);
    defer alloc.free(bytes);
    return try decodePackedU32BytesAlloc(alloc, bytes);
}

pub fn decodePackedSparseBase64Alloc(
    alloc: Allocator,
    packed_indices: []const u8,
    packed_values: []const u8,
) !PackedSparse {
    const indices = try decodePackedU32Base64Alloc(alloc, packed_indices);
    errdefer alloc.free(indices);
    const values = try decodePackedF32Base64Alloc(alloc, packed_values);
    errdefer alloc.free(values);
    if (indices.len != values.len) return error.InvalidPackedVector;
    return .{
        .indices = indices,
        .values = values,
    };
}

pub fn decodePackedF32BytesAlloc(alloc: Allocator, bytes: []const u8) ![]f32 {
    if (bytes.len % @sizeOf(f32) != 0) return error.InvalidPackedVector;

    const vector = try alloc.alloc(f32, bytes.len / @sizeOf(f32));
    errdefer alloc.free(vector);
    try decodePackedF32BytesInto(vector, bytes);
    return vector;
}

pub fn decodePackedU32BytesAlloc(alloc: Allocator, bytes: []const u8) ![]u32 {
    if (bytes.len % @sizeOf(u32) != 0) return error.InvalidPackedVector;

    const vector = try alloc.alloc(u32, bytes.len / @sizeOf(u32));
    errdefer alloc.free(vector);
    try decodePackedU32BytesInto(vector, bytes);
    return vector;
}

pub fn decodePackedF32BytesInto(out: []f32, bytes: []const u8) !void {
    if (bytes.len != out.len * @sizeOf(f32)) return error.InvalidPackedVector;

    @memcpy(std.mem.sliceAsBytes(out), bytes);
    if (native_endian != .little) {
        byteSwapU32Slice(std.mem.bytesAsSlice(u32, std.mem.sliceAsBytes(out)));
    }
}

pub fn decodePackedU32BytesInto(out: []u32, bytes: []const u8) !void {
    if (bytes.len != out.len * @sizeOf(u32)) return error.InvalidPackedVector;

    @memcpy(std.mem.sliceAsBytes(out), bytes);
    if (native_endian != .little) {
        byteSwapU32Slice(out);
    }
}

pub fn encodePackedF32Base64Alloc(alloc: Allocator, values: []const f32) ![]u8 {
    const bytes = try encodePackedF32BytesAlloc(alloc, values);
    defer alloc.free(bytes);
    return try encodeBase64Alloc(alloc, bytes);
}

pub fn encodePackedU32Base64Alloc(alloc: Allocator, values: []const u32) ![]u8 {
    const bytes = try encodePackedU32BytesAlloc(alloc, values);
    defer alloc.free(bytes);
    return try encodeBase64Alloc(alloc, bytes);
}

pub fn encodePackedSparseBase64Alloc(
    alloc: Allocator,
    indices: []const u32,
    values: []const f32,
) !struct {
    packed_indices: []u8,
    packed_values: []u8,

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        allocator.free(self.packed_indices);
        allocator.free(self.packed_values);
        self.* = undefined;
    }
} {
    if (indices.len != values.len) return error.InvalidPackedVector;

    const packed_indices = try encodePackedU32Base64Alloc(alloc, indices);
    errdefer alloc.free(packed_indices);
    const packed_values = try encodePackedF32Base64Alloc(alloc, values);
    errdefer alloc.free(packed_values);
    return .{
        .packed_indices = packed_indices,
        .packed_values = packed_values,
    };
}

pub fn encodePackedF32BytesAlloc(alloc: Allocator, values: []const f32) ![]u8 {
    const out = try alloc.alloc(u8, values.len * @sizeOf(f32));
    errdefer alloc.free(out);
    try encodePackedF32BytesInto(out, values);
    return out;
}

pub fn encodePackedU32BytesAlloc(alloc: Allocator, values: []const u32) ![]u8 {
    const out = try alloc.alloc(u8, values.len * @sizeOf(u32));
    errdefer alloc.free(out);
    try encodePackedU32BytesInto(out, values);
    return out;
}

pub fn encodePackedF32BytesInto(out: []u8, values: []const f32) !void {
    if (out.len != values.len * @sizeOf(f32)) return error.InvalidPackedVector;

    if (native_endian == .little) {
        @memcpy(out, std.mem.sliceAsBytes(values));
        return;
    }

    for (values, 0..) |value, i| {
        const start = i * @sizeOf(f32);
        std.mem.writeInt(u32, out[start..][0..4], @bitCast(value), .little);
    }
}

pub fn encodePackedU32BytesInto(out: []u8, values: []const u32) !void {
    if (out.len != values.len * @sizeOf(u32)) return error.InvalidPackedVector;

    if (native_endian == .little) {
        @memcpy(out, std.mem.sliceAsBytes(values));
        return;
    }

    for (values, 0..) |value, i| {
        const start = i * @sizeOf(u32);
        std.mem.writeInt(u32, out[start..][0..4], value, .little);
    }
}

fn decodeBase64Alloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const decoder = std.base64.standard.Decoder;
    const size = try decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try decoder.decode(out, encoded);
    return out;
}

fn encodeBase64Alloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const encoder = std.base64.standard.Encoder;
    const out = try alloc.alloc(u8, encoder.calcSize(bytes.len));
    errdefer alloc.free(out);
    _ = encoder.encode(out, bytes);
    return out;
}

fn byteSwapU32Slice(words: []u32) void {
    var i: usize = 0;
    while (i + swap_simd_width <= words.len) : (i += swap_simd_width) {
        const lanes: SimdU32 = words[i..][0..swap_simd_width].*;
        words[i..][0..swap_simd_width].* = @byteSwap(lanes);
    }
    while (i < words.len) : (i += 1) {
        words[i] = @byteSwap(words[i]);
    }
}

test "packed f32 base64 round trips" {
    const alloc = std.testing.allocator;

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.5 };
    const encoded = try encodePackedF32Base64Alloc(alloc, &input);
    defer alloc.free(encoded);

    const decoded = try decodePackedF32Base64Alloc(alloc, encoded);
    defer alloc.free(decoded);

    try std.testing.expectEqual(@as(usize, input.len), decoded.len);
    for (input, 0..) |value, i| {
        try std.testing.expectApproxEqAbs(value, decoded[i], 0.0001);
    }
}

test "packed u32 base64 round trips" {
    const alloc = std.testing.allocator;

    const input = [_]u32{ 1, 5, 42, 999 };
    const encoded = try encodePackedU32Base64Alloc(alloc, &input);
    defer alloc.free(encoded);

    const decoded = try decodePackedU32Base64Alloc(alloc, encoded);
    defer alloc.free(decoded);

    try std.testing.expectEqualSlices(u32, &input, decoded);
}

test "packed sparse base64 round trips" {
    const alloc = std.testing.allocator;

    const indices = [_]u32{ 7, 42 };
    const values = [_]f32{ 1.5, 0.5 };

    var encoded = try encodePackedSparseBase64Alloc(alloc, &indices, &values);
    defer encoded.deinit(alloc);

    var decoded = try decodePackedSparseBase64Alloc(alloc, encoded.packed_indices, encoded.packed_values);
    defer decoded.deinit(alloc);

    try std.testing.expectEqualSlices(u32, &indices, decoded.indices);
    try std.testing.expectEqual(@as(usize, values.len), decoded.values.len);
    for (values, 0..) |value, i| {
        try std.testing.expectApproxEqAbs(value, decoded.values[i], 0.0001);
    }
}

test "packed decode rejects invalid element width" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidPackedVector, decodePackedF32BytesAlloc(alloc, &.{ 1, 2, 3 }));
    try std.testing.expectError(error.InvalidPackedVector, decodePackedU32BytesAlloc(alloc, &.{ 1, 2, 3 }));
}
