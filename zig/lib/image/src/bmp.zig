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
const test_support = @import("test_support.zig");

const Allocator = std.mem.Allocator;

const file_header_len = 14;
const dib_bitmap_info_header_len = 40;
const compression_rgb = 0;

pub const DecodedImage = struct {
    rgba: []u8,
    width: u32,
    height: u32,
};

pub fn hasSignature(bytes: []const u8) bool {
    return bytes.len >= 2 and bytes[0] == 'B' and bytes[1] == 'M';
}

pub fn decodeRgba(alloc: Allocator, bmp_bytes: []const u8) !DecodedImage {
    if (!hasSignature(bmp_bytes)) return error.BmpDecodeFailed;
    if (bmp_bytes.len < file_header_len + dib_bitmap_info_header_len) return error.BmpDecodeFailed;

    const pixel_offset = try readU32Le(bmp_bytes, 10);
    const dib_header_len = try readU32Le(bmp_bytes, 14);
    if (dib_header_len < dib_bitmap_info_header_len) return error.UnsupportedBmpFormat;
    const dib_header_len_usize: usize = @intCast(dib_header_len);
    const dib_end = try std.math.add(usize, file_header_len, dib_header_len_usize);
    if (dib_end > bmp_bytes.len) return error.BmpDecodeFailed;

    const dib_start: usize = file_header_len;
    const width_i = try readI32Le(bmp_bytes, dib_start + 4);
    const height_i = try readI32Le(bmp_bytes, dib_start + 8);
    if (width_i <= 0 or height_i == 0 or height_i == std.math.minInt(i32)) return error.BmpDecodeFailed;

    const planes = try readU16Le(bmp_bytes, dib_start + 12);
    const bits_per_pixel = try readU16Le(bmp_bytes, dib_start + 14);
    const compression = try readU32Le(bmp_bytes, dib_start + 16);
    const colors_used = try readU32Le(bmp_bytes, dib_start + 32);
    if (planes != 1) return error.BmpDecodeFailed;
    if (compression != compression_rgb) return error.UnsupportedBmpFormat;
    if (bits_per_pixel != 1 and bits_per_pixel != 4 and bits_per_pixel != 8 and bits_per_pixel != 24 and bits_per_pixel != 32) {
        return error.UnsupportedBmpFormat;
    }

    const width: u32 = @intCast(width_i);
    const height_abs_i = if (height_i < 0) -height_i else height_i;
    const height: u32 = @intCast(height_abs_i);
    const top_down = height_i < 0;

    const row_bits = try std.math.mul(usize, @as(usize, width), bits_per_pixel);
    const src_row_stride = ((row_bits + 31) / 32) * 4;
    const pixel_count = try std.math.mul(usize, @as(usize, width), @as(usize, height));
    const rgba_len = try std.math.mul(usize, pixel_count, 4);
    const pixel_start: usize = @intCast(pixel_offset);
    const pixel_bytes_len = try std.math.mul(usize, src_row_stride, @as(usize, height));
    if (pixel_start < dib_end) return error.BmpDecodeFailed;
    if (pixel_start > bmp_bytes.len or pixel_bytes_len > bmp_bytes.len - pixel_start) return error.BmpDecodeFailed;

    var palette: []const u8 = &.{};
    if (bits_per_pixel <= 8) {
        const color_count_u32 = if (colors_used != 0) colors_used else (@as(u32, 1) << @intCast(bits_per_pixel));
        if (color_count_u32 == 0 or color_count_u32 > 256) return error.BmpDecodeFailed;
        const color_count: usize = @intCast(color_count_u32);
        const palette_start = dib_end;
        const palette_len = try std.math.mul(usize, color_count, 4);
        if (palette_start > bmp_bytes.len or palette_len > bmp_bytes.len - palette_start) return error.BmpDecodeFailed;
        if (palette_start + palette_len > pixel_start) return error.BmpDecodeFailed;
        palette = bmp_bytes[palette_start .. palette_start + palette_len];
    }

    const rgba = try alloc.alloc(u8, rgba_len);
    errdefer alloc.free(rgba);

    const use_alpha = bits_per_pixel == 32 and hasNonZeroAlphaPlane(bmp_bytes[pixel_start .. pixel_start + pixel_bytes_len], width, height, src_row_stride);

    var y: usize = 0;
    while (y < height) : (y += 1) {
        const src_y = if (top_down) y else @as(usize, height) - 1 - y;
        const src_row = bmp_bytes[pixel_start + src_y * src_row_stride ..][0..src_row_stride];
        const dst_row = rgba[y * @as(usize, width) * 4 ..][0 .. @as(usize, width) * 4];
        switch (bits_per_pixel) {
            1 => try decodeIndexed1Row(dst_row, src_row, palette, width),
            4 => try decodeIndexed4Row(dst_row, src_row, palette, width),
            8 => try decodeIndexed8Row(dst_row, src_row, palette, width),
            24 => decodeBgr24Row(dst_row, src_row, width),
            32 => decodeBgr32Row(dst_row, src_row, width, use_alpha),
            else => unreachable,
        }
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn decodeIndexed1Row(dst: []u8, src: []const u8, palette: []const u8, width: u32) !void {
    for (0..@as(usize, width)) |x| {
        const byte = src[x / 8];
        const shift: u3 = @intCast(7 - (x % 8));
        try writePalettePixel(dst[x * 4 ..][0..4], palette, (byte >> shift) & 0x1);
    }
}

fn decodeIndexed4Row(dst: []u8, src: []const u8, palette: []const u8, width: u32) !void {
    for (0..@as(usize, width)) |x| {
        const byte = src[x / 2];
        const index = if ((x % 2) == 0) byte >> 4 else byte & 0x0f;
        try writePalettePixel(dst[x * 4 ..][0..4], palette, index);
    }
}

fn decodeIndexed8Row(dst: []u8, src: []const u8, palette: []const u8, width: u32) !void {
    for (0..@as(usize, width)) |x| {
        try writePalettePixel(dst[x * 4 ..][0..4], palette, src[x]);
    }
}

fn writePalettePixel(dst: []u8, palette: []const u8, index: u8) !void {
    const base = @as(usize, index) * 4;
    if (base + 4 > palette.len) return error.BmpDecodeFailed;
    dst[0] = palette[base + 2];
    dst[1] = palette[base + 1];
    dst[2] = palette[base + 0];
    dst[3] = 255;
}

fn decodeBgr24Row(dst: []u8, src: []const u8, width: u32) void {
    for (0..@as(usize, width)) |x| {
        const src_base = x * 3;
        const dst_base = x * 4;
        dst[dst_base + 0] = src[src_base + 2];
        dst[dst_base + 1] = src[src_base + 1];
        dst[dst_base + 2] = src[src_base + 0];
        dst[dst_base + 3] = 255;
    }
}

fn decodeBgr32Row(dst: []u8, src: []const u8, width: u32, use_alpha: bool) void {
    for (0..@as(usize, width)) |x| {
        const src_base = x * 4;
        const dst_base = x * 4;
        dst[dst_base + 0] = src[src_base + 2];
        dst[dst_base + 1] = src[src_base + 1];
        dst[dst_base + 2] = src[src_base + 0];
        dst[dst_base + 3] = if (use_alpha) src[src_base + 3] else 255;
    }
}

fn hasNonZeroAlphaPlane(pixel_bytes: []const u8, width: u32, height: u32, stride: usize) bool {
    var y: usize = 0;
    while (y < height) : (y += 1) {
        const row = pixel_bytes[y * stride ..][0..stride];
        for (0..@as(usize, width)) |x| {
            if (row[x * 4 + 3] != 0) return true;
        }
    }
    return false;
}

fn readU16Le(bytes: []const u8, offset: usize) !u16 {
    if (offset + 2 > bytes.len) return error.BmpDecodeFailed;
    return @as(u16, bytes[offset]) | (@as(u16, bytes[offset + 1]) << 8);
}

fn readU32Le(bytes: []const u8, offset: usize) !u32 {
    if (offset + 4 > bytes.len) return error.BmpDecodeFailed;
    return @as(u32, bytes[offset]) |
        (@as(u32, bytes[offset + 1]) << 8) |
        (@as(u32, bytes[offset + 2]) << 16) |
        (@as(u32, bytes[offset + 3]) << 24);
}

fn readI32Le(bytes: []const u8, offset: usize) !i32 {
    return @bitCast(try readU32Le(bytes, offset));
}

const bmp_24_2x2 = [_]u8{
    'B',  'M',  70,   0,    0,    0,    0,    0,    0,    0,    54,   0,    0,    0,
    40,   0,    0,    0,    2,    0,    0,    0,    2,    0,    0,    0,    1,    0,
    24,   0,    0,    0,    0,    0,    16,   0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0xff, 0x00,
    0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0xff, 0x00, 0xff, 0x00, 0x00, 0x00,
};

const bmp_32_top_down_2x1 = [_]u8{
    'B',  'M',  62,   0,    0,    0,    0, 0, 0,    0,    54,   0,    0,    0,
    40,   0,    0,    0,    2,    0,    0, 0, 0xff, 0xff, 0xff, 0xff, 1,    0,
    32,   0,    0,    0,    0,    0,    8, 0, 0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,    0,    0, 0, 0,    0,    0,    0,    0x00, 0x00,
    0xff, 0x80, 0x00, 0xff, 0x00, 0x40,
};

const bmp_8_palette_2x1 = [_]u8{
    'B',  'M',  66,   0,    0,    0,    0,    0,    0,    0,    62, 0, 0,    0,
    40,   0,    0,    0,    2,    0,    0,    0,    1,    0,    0,  0, 1,    0,
    8,    0,    0,    0,    0,    0,    4,    0,    0,    0,    0,  0, 0,    0,
    0,    0,    0,    0,    2,    0,    0,    0,    0,    0,    0,  0, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xff, 0x00, 0x01, 0x00, 0x00, 0x00,
};

test "decode 24-bit bottom-up bmp to rgba" {
    const alloc = std.testing.allocator;
    const decoded = try decodeRgba(alloc, &bmp_24_2x2);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 2), decoded.height);
    try std.testing.expectEqualSlices(u8, &.{
        0xff, 0x00, 0x00, 0xff,
        0x00, 0xff, 0x00, 0xff,
        0x00, 0x00, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff,
    }, decoded.rgba);
}

test "decode 32-bit top-down bmp preserves non-zero alpha" {
    const alloc = std.testing.allocator;
    const decoded = try decodeRgba(alloc, &bmp_32_top_down_2x1);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &.{
        0xff, 0x00, 0x00, 0x80,
        0x00, 0xff, 0x00, 0x40,
    }, decoded.rgba);
}

test "decode 8-bit indexed bmp palette" {
    const alloc = std.testing.allocator;
    const decoded = try decodeRgba(alloc, &bmp_8_palette_2x1);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &.{
        0xff, 0x00, 0x00, 0xff,
        0x00, 0x00, 0x00, 0xff,
    }, decoded.rgba);
}

test "decode rejects compressed bmp" {
    var bytes = bmp_24_2x2;
    bytes[30] = 3;
    try std.testing.expectError(error.UnsupportedBmpFormat, decodeRgba(std.testing.allocator, &bytes));
}

test "decode rejects truecolor pixel data before dib header end" {
    var bytes = bmp_24_2x2;
    bytes[10] = 14;
    bytes[11] = 0;
    bytes[12] = 0;
    bytes[13] = 0;
    try std.testing.expectError(error.BmpDecodeFailed, decodeRgba(std.testing.allocator, &bytes));
}

test "decode manifest-backed bmp success fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "bmp/orientation/bottom-up-24bpp-2x2.bmp",
        "bmp/orientation/top-down-32bpp-alpha-2x1.bmp",
        "bmp/indexed/palette-1bpp-8x1.bmp",
        "bmp/indexed/palette-4bpp-4x1.bmp",
        "bmp/indexed/palette-8bpp-2x1.bmp",
    };

    for (&paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
        try std.testing.expectEqualStrings("bmp", fixture.format);
        try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);

        const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
    }
}

test "decode manifest-backed bmp unsupported fixtures return typed unsupported error" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "bmp/unsupported/bitfields-16bpp-2x1.bmp",
        "bmp/unsupported/rle8-2x2.bmp",
    };

    for (&paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.known_unsupported, fixture.result);
        try std.testing.expectEqualStrings("bmp", fixture.format);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        try std.testing.expectError(error.UnsupportedBmpFormat, decodeRgba(alloc, fixture_bytes));
    }
}

test "decode manifest-backed bmp invalid fixtures return typed decode error" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "bmp/invalid/truncated-24bpp-2x2.bmp") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("bmp", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.BmpDecodeFailed, decodeRgba(alloc, fixture_bytes));
}
