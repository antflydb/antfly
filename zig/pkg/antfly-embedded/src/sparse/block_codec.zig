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

//! Lossless native posting transport. Packed gaps reduce disk/cache traffic;
//! canonical V1 weight bytes and their f32 decoding order stay unchanged.
const std = @import("std");
const A = std.mem.Allocator;
const magic = "ASP2";
const header_bytes = 30;
pub const max_decoded_bytes = 1024 * 1024;
fn shape(block: []const u8) !struct { count: usize, range: usize } {
    if (block.len < 26 or block[8] != 1) return error.InvalidChunk;
    const count = std.mem.readInt(u32, block[9..13], .little);
    const chunk = std.mem.readInt(u32, block[0..4], .little);
    const range = std.mem.readInt(u32, block[4..8], .little);
    if (count == 0 or 13 + @as(u64, count) * 5 != chunk or 8 + @as(u64, chunk) + range != block.len or block.len > max_decoded_bytes) return error.InvalidChunk;
    return .{ .count = count, .range = range };
}
/// Null keeps the existing representation when packing cannot save bytes.
pub fn encode(a: A, block: []const u8) !?[]u8 {
    const info = try shape(block);
    var maximum: u32 = 0;
    for (1..info.count) |i| {
        const gap = std.mem.readInt(u32, block[21 + i * 4 ..][0..4], .little);
        if (gap == 0) return error.InvalidChunk;
        maximum = @max(maximum, gap);
    }
    const width: u8 = @intCast(32 - @clz(maximum));
    const packed_bytes = ((info.count - 1) * width + 7) / 8;
    const size = header_bytes + packed_bytes + info.count + info.range;
    if (size >= block.len) return null;
    const bytes = try a.alloc(u8, size);
    @memcpy(bytes[0..4], magic);
    bytes[4] = width;
    @memcpy(bytes[5..9], block[21..25]);
    @memcpy(bytes[9..30], block[0..21]);
    @memset(bytes[header_bytes..][0..packed_bytes], 0);
    for (1..info.count) |i| {
        const gap = std.mem.readInt(u32, block[21 + i * 4 ..][0..4], .little);
        const bit = (i - 1) * width;
        const shift: u6 = @intCast(bit % 8);
        var value = @as(u64, gap) << shift;
        var offset = bit / 8;
        const end = offset + (@as(usize, width) + shift + 7) / 8;
        while (offset < end) : (offset += 1) {
            bytes[header_bytes + offset] |= @truncate(value);
            value >>= 8;
        }
    }
    @memcpy(bytes[header_bytes + packed_bytes ..], block[21 + info.count * 4 ..]);
    return bytes;
}
pub fn isPacked(bytes: []const u8) bool {
    return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], magic);
}
pub fn decodedSize(bytes: []const u8) !usize {
    if (!isPacked(bytes)) {
        _ = try shape(bytes);
        return bytes.len;
    }
    if (bytes.len < header_bytes or bytes[4] > 32 or bytes[17] != 1) return error.InvalidChunk;
    const count = std.mem.readInt(u32, bytes[18..22], .little);
    const chunk = std.mem.readInt(u32, bytes[9..13], .little);
    const range = std.mem.readInt(u32, bytes[13..17], .little);
    if (count == 0 or (count > 1 and bytes[4] == 0) or 13 + @as(u64, count) * 5 != chunk) return error.InvalidChunk;
    const gaps = ((@as(u64, count) - 1) * bytes[4] + 7) / 8;
    const size = 8 + @as(u64, chunk) + range;
    if (header_bytes + gaps + count + range != bytes.len or size > max_decoded_bytes) return error.InvalidChunk;
    return @intCast(size);
}
pub fn decode(a: A, bytes: []const u8) ![]u8 {
    const size = try decodedSize(bytes);
    if (!isPacked(bytes)) return a.dupe(u8, bytes);
    const out = try a.alloc(u8, size);
    errdefer a.free(out);
    @memcpy(out[0..21], bytes[9..30]);
    @memcpy(out[21..25], bytes[5..9]);
    const count = std.mem.readInt(u32, out[9..13], .little);
    const width = bytes[4];
    const mask: u64 = (@as(u64, 1) << @as(u6, @intCast(width))) - 1;
    const packed_bytes = ((@as(usize, count) - 1) * width + 7) / 8;
    var i: usize = 1;
    while (i < count) : (i += 8) {
        var windows: [8]u64 = @splat(0);
        var shifts: [8]u6 = @splat(0);
        const lanes = @min(8, count - i);
        for (0..lanes) |lane| {
            const bit = (i + lane - 1) * width;
            shifts[lane] = @intCast(bit % 8);
            const begin = bit / 8;
            const length = (@as(usize, width) + shifts[lane] + 7) / 8;
            for (0..length) |j| windows[lane] |= @as(u64, bytes[header_bytes + begin + j]) << @as(u6, @intCast(j * 8));
        }
        const values: @Vector(8, u64) = (@as(@Vector(8, u64), windows) >> @as(@Vector(8, u6), shifts)) & @as(@Vector(8, u64), @splat(mask));
        const decoded: [8]u64 = values;
        for (0..lanes) |lane| {
            if (decoded[lane] == 0) return error.InvalidChunk;
            std.mem.writeInt(u32, out[21 + (i + lane) * 4 ..][0..4], @intCast(decoded[lane]), .little);
        }
    }
    @memcpy(out[21 + @as(usize, count) * 4 ..], bytes[header_bytes + packed_bytes ..]);
    return out;
}

test "sparse packed block transport preserves all gap widths weights and range bytes" {
    const a = std.testing.allocator;
    for (1..33) |width| {
        const count = 257;
        var raw: [8 + 13 + count * 5 + 12]u8 = @splat(0);
        std.mem.writeInt(u32, raw[0..4], 13 + count * 5, .little);
        std.mem.writeInt(u32, raw[4..8], 12, .little);
        raw[8] = 1;
        std.mem.writeInt(u32, raw[9..13], count, .little);
        std.mem.writeInt(u32, raw[21..25], 0xfffffffe, .little);
        for (1..count) |i| {
            const gap: u32 = if (i % 3 == 0) 1 else @as(u32, 1) << @as(u5, @intCast(width - 1));
            std.mem.writeInt(u32, raw[21 + i * 4 ..][0..4], gap, .little);
        }
        for (raw[21 + count * 4 ..], 0..) |*value, i| value.* = @truncate(i *% 91);
        const encoded = try encode(a, &raw);
        defer if (encoded) |bytes| a.free(bytes);
        if (width < 32) try std.testing.expect(encoded != null);
        if (width == 1) try std.testing.expect(encoded.?.len * 3 < raw.len);
        const restored = try decode(a, encoded orelse &raw);
        defer a.free(restored);
        try std.testing.expectEqualSlices(u8, &raw, restored);
        if (encoded) |bytes| try std.testing.expectError(error.InvalidChunk, decode(a, bytes[0 .. bytes.len - 1]));
    }
}

test "sparse packed transport rejects malformed lengths gaps and widths and cleans up OOM" {
    const Probe = struct {
        fn run(a: A) !void {
            const count = 64;
            var raw: [21 + count * 5]u8 = @splat(0);
            std.mem.writeInt(u32, raw[0..4], 13 + count * 5, .little);
            raw[8] = 1;
            std.mem.writeInt(u32, raw[9..13], count, .little);
            for (1..count) |i| std.mem.writeInt(u32, raw[21 + i * 4 ..][0..4], 1, .little);
            const encoded = (try encode(a, &raw)).?;
            defer a.free(encoded);
            const restored = try decode(a, encoded);
            defer a.free(restored);
            try std.testing.expectEqualSlices(u8, &raw, restored);
            encoded[header_bytes] = 0;
            if (decode(a, encoded)) |unexpected| {
                a.free(unexpected);
                return error.ExpectedInvalidChunk;
            } else |err| if (err != error.InvalidChunk) return err;
            encoded[4] = 33;
            try std.testing.expectError(error.InvalidChunk, decodedSize(encoded));
            encoded[4] = 1;
            std.mem.writeInt(u32, encoded[18..22], std.math.maxInt(u32), .little);
            try std.testing.expectError(error.InvalidChunk, decodedSize(encoded));
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
