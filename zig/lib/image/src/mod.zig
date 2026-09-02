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
const conformance_impl = @import("conformance.zig");
const limits = @import("limits.zig");

pub const png = @import("png.zig");
pub const jpeg = @import("jpeg.zig");
pub const jpeg2000 = @import("jpeg2000/mod.zig");
pub const gif = @import("gif.zig");
pub const bmp = @import("bmp.zig");
pub const webp = @import("webp.zig");
pub const ccitt = @import("ccitt.zig");
pub const processing = @import("processing.zig");
pub const conformance = conformance_impl;

pub const Format = enum { png, jpeg, jpeg2000_jp2, jpeg2000_j2k, gif, bmp, webp, unknown };
pub const DecodeLimits = limits.DecodeLimits;

pub const EncodedImageInfo = struct {
    width: u32,
    height: u32,

    pub fn pixels(self: EncodedImageInfo) !u64 {
        return std.math.mul(u64, self.width, self.height) catch error.ImageDimensionsOverflow;
    }
};

pub fn detectFormat(bytes: []const u8) Format {
    if (png.hasSignature(bytes)) return .png;
    if (jpeg2000.box.hasSignature(bytes)) return .jpeg2000_jp2;
    if (jpeg2000.codestream.hasSoc(bytes)) return .jpeg2000_j2k;
    if (jpeg.hasSignature(bytes)) return .jpeg;
    if (gif.hasSignature(bytes)) return .gif;
    if (bmp.hasSignature(bytes)) return .bmp;
    if (webp.hasSignature(bytes)) return .webp;
    return .unknown;
}

/// Inspect untrusted image headers without allocating decoded pixels. This is
/// the shared admission primitive used by planners, local executors, and the
/// distributed inference boundary so aggregate decoded-pixel limits have one
/// meaning everywhere.
pub fn inspectEncoded(bytes: []const u8) !EncodedImageInfo {
    switch (detectFormat(bytes)) {
        .png => {
            if (bytes.len < 24) return error.InvalidImageEncoding;
            const width = std.mem.readInt(u32, bytes[16..20], .big);
            const height = std.mem.readInt(u32, bytes[20..24], .big);
            if (width == 0 or height == 0) return error.InvalidImageEncoding;
            return .{ .width = width, .height = height };
        },
        .jpeg => {
            const info = jpeg.probe(bytes) catch return error.InvalidImageEncoding;
            return .{ .width = info.width, .height = info.height };
        },
        .gif => {
            if (bytes.len < 10) return error.InvalidImageEncoding;
            const width = std.mem.readInt(u16, bytes[6..8], .little);
            const height = std.mem.readInt(u16, bytes[8..10], .little);
            if (width == 0 or height == 0) return error.InvalidImageEncoding;
            return .{ .width = width, .height = height };
        },
        .bmp => {
            const info = bmp.probe(bytes) catch return error.InvalidImageEncoding;
            return .{ .width = info.width, .height = info.height };
        },
        .webp => {
            const info = webp.probe(bytes) catch return error.InvalidImageEncoding;
            return .{
                .width = info.width orelse return error.InvalidImageEncoding,
                .height = info.height orelse return error.InvalidImageEncoding,
            };
        },
        else => return error.UnsupportedImageEncoding,
    }
}

test {
    _ = png;
    _ = jpeg;
    _ = jpeg2000;
    _ = gif;
    _ = bmp;
    _ = webp;
    _ = ccitt;
    _ = processing;
    _ = conformance;
    _ = limits;
    _ = @import("test_support.zig");
}

test "detectFormat signatures" {
    try std.testing.expectEqual(Format.png, detectFormat(&.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }));
    try std.testing.expectEqual(Format.jpeg, detectFormat(&.{ 0xFF, 0xD8, 0xFF, 0xE0 }));
    try std.testing.expectEqual(Format.gif, detectFormat("GIF89a\x00"));
    try std.testing.expectEqual(Format.bmp, detectFormat("BM\x00\x00"));
    try std.testing.expectEqual(Format.webp, detectFormat("RIFF\x00\x00\x00\x00WEBP"));
    try std.testing.expectEqual(Format.jpeg2000_j2k, detectFormat(&.{ 0xFF, 0x4F, 0xFF, 0x51 }));
    try std.testing.expectEqual(Format.jpeg2000_jp2, detectFormat(&.{ 0x00, 0x00, 0x00, 0x0C, 'j', 'P', ' ', ' ', 0x0D, 0x0A, 0x87, 0x0A }));
    try std.testing.expectEqual(Format.unknown, detectFormat("hello"));
}

test "inspectEncoded reports dimensions without decoding pixels" {
    var header = [_]u8{0} ** 24;
    @memcpy(header[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, header[16..20], 320, .big);
    std.mem.writeInt(u32, header[20..24], 200, .big);
    const info = try inspectEncoded(&header);
    try std.testing.expectEqual(@as(u32, 320), info.width);
    try std.testing.expectEqual(@as(u32, 200), info.height);
    try std.testing.expectEqual(@as(u64, 64_000), try info.pixels());
    try std.testing.expectError(error.UnsupportedImageEncoding, inspectEncoded("not an image"));
}
