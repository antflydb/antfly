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

pub fn encodeRgba(alloc: std.mem.Allocator, width: u32, height: u32, rgba: []const u8) ![]u8 {
    const expected = @as(usize, width) * @as(usize, height) * 4;
    if (rgba.len != expected) return error.InvalidRgbaSize;

    const scanline_len = @as(usize, width) * 4 + 1;
    const raw_len = scanline_len * @as(usize, height);
    const raw = try alloc.alloc(u8, raw_len);
    defer alloc.free(raw);

    for (0..@as(usize, height)) |row| {
        const raw_start = row * scanline_len;
        raw[raw_start] = 0;
        const src_start = row * @as(usize, width) * 4;
        @memcpy(raw[raw_start + 1 .. raw_start + scanline_len], rgba[src_start .. src_start + @as(usize, width) * 4]);
    }

    const zlib_data = try encodeZlibNoCompression(alloc, raw);
    defer alloc.free(zlib_data);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' });

    var ihdr: [13]u8 = undefined;
    std.mem.writeInt(u32, ihdr[0..4], width, .big);
    std.mem.writeInt(u32, ihdr[4..8], height, .big);
    ihdr[8] = 8;
    ihdr[9] = 6;
    ihdr[10] = 0;
    ihdr[11] = 0;
    ihdr[12] = 0;
    try appendChunk(alloc, &out, "IHDR", &ihdr);
    try appendChunk(alloc, &out, "IDAT", zlib_data);
    try appendChunk(alloc, &out, "IEND", &.{});

    return try out.toOwnedSlice(alloc);
}

fn encodeZlibNoCompression(alloc: std.mem.Allocator, payload: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, &.{ 0x78, 0x01 });

    var offset: usize = 0;
    while (offset < payload.len) {
        const remaining = payload.len - offset;
        const block_len: u16 = @intCast(@min(remaining, 65_535));
        const final_block = offset + block_len >= payload.len;
        try out.append(alloc, if (final_block) 0x01 else 0x00);
        try out.append(alloc, @truncate(block_len));
        try out.append(alloc, @truncate(block_len >> 8));
        const nlen: u16 = ~block_len;
        try out.append(alloc, @truncate(nlen));
        try out.append(alloc, @truncate(nlen >> 8));
        try out.appendSlice(alloc, payload[offset .. offset + block_len]);
        offset += block_len;
    }

    var checksum: [4]u8 = undefined;
    std.mem.writeInt(u32, &checksum, std.hash.Adler32.hash(payload), .big);
    try out.appendSlice(alloc, &checksum);

    return try out.toOwnedSlice(alloc);
}

fn appendChunk(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, data: []const u8) !void {
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, @intCast(data.len), .big);
    try out.appendSlice(alloc, &length);
    try out.appendSlice(alloc, name);
    try out.appendSlice(alloc, data);

    var crc_buf = try alloc.alloc(u8, name.len + data.len);
    defer alloc.free(crc_buf);
    @memcpy(crc_buf[0..name.len], name);
    @memcpy(crc_buf[name.len..], data);

    var crc: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc, std.hash.Crc32.hash(crc_buf), .big);
    try out.appendSlice(alloc, &crc);
}

test "encode rgba png writes png signature" {
    const alloc = std.testing.allocator;
    const png = try encodeRgba(alloc, 1, 1, &.{ 255, 0, 0, 255 });
    defer alloc.free(png);

    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}
