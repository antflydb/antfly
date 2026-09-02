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

// Image preprocessing for vision models (CLIP, Florence2).
//
// Decodes JPEG/PNG/GIF/BMP/WebP through the shared antfly image layer, then routes
// decoded pixels through the shared resize/normalize/CHW preprocessing path.

const std = @import("std");
const antfly_image = @import("antfly_image");
const shared = antfly_image.processing;

/// Standard ImageNet normalization (used by CLIP, Florence2, most vision models).
pub const IMAGENET_MEAN = [3]f32{ 0.48145466, 0.4578275, 0.40821073 };
pub const IMAGENET_STD = [3]f32{ 0.26862954, 0.26130258, 0.27577711 };

pub const Resample = shared.Resample;
pub const PixelFormat = shared.PixelFormat;
pub const ImageU8 = shared.ImageU8;
pub const DecodeLimits = antfly_image.DecodeLimits;

pub const EncodedImageInfo = antfly_image.EncodedImageInfo;

pub const supportsMimeEssence = antfly_image.supportsInferenceMimeEssence;

pub fn mimeEssenceForEncoded(bytes: []const u8) ?[]const u8 {
    return antfly_image.inferenceMimeEssenceForFormat(antfly_image.detectFormat(bytes));
}

/// Decoded image in HWC u8 format.
pub const Image = struct {
    data: []u8,
    width: u32,
    height: u32,
    channels: u32,

    pub fn deinit(self: Image, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

pub const Pix2StructPatches = struct {
    flattened_patches: []f32,
    attention_mask: []i64,
    rows: usize,
    cols: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Pix2StructPatches) void {
        self.allocator.free(self.flattened_patches);
        self.allocator.free(self.attention_mask);
    }
};

/// Decode a JPEG/PNG/GIF/BMP/WebP from raw bytes. Always returns 3-channel RGB.
pub fn decode(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    try validateEncodedImageDimensions(image_bytes);
    switch (antfly_image.detectFormat(image_bytes)) {
        .png => return decodePng(allocator, image_bytes),
        .jpeg => return decodeJpeg(allocator, image_bytes),
        .gif => return decodeGif(allocator, image_bytes),
        .bmp => return decodeBmp(allocator, image_bytes),
        .webp => return decodeWebp(allocator, image_bytes),
        else => return error.ImageDecodeFailed,
    }
}

fn validateImageDimensions(width: u32, height: u32) !void {
    return antfly_image.DecodeLimits.inference_default.validate(width, height) catch |err| switch (err) {
        error.ImageTooLarge => error.ImageDecodeFailed,
    };
}

/// Inspect untrusted image headers without allocating decoded pixels. The
/// server uses this before model loading/inference to account aggregate pixel
/// pressure and to distinguish malformed input from configured size limits.
pub fn inspectEncodedForInference(image_bytes: []const u8, max_dimension: ?u32) !EncodedImageInfo {
    const info = antfly_image.inspectEncoded(image_bytes) catch return error.ImageDecodeFailed;
    try antfly_image.DecodeLimits.inference_default.validate(info.width, info.height);
    if (max_dimension) |limit| {
        if (info.width > limit or info.height > limit) return error.ImageTooLarge;
    }
    return info;
}

fn validateEncodedImageDimensions(image_bytes: []const u8) !void {
    _ = inspectEncodedForInference(image_bytes, null) catch |err| switch (err) {
        error.ImageTooLarge => return error.ImageDecodeFailed,
        else => return err,
    };
}

fn decodePng(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    const decoded = antfly_image.png.decodeRgba(allocator, image_bytes) catch |err| switch (err) {
        error.PngDecodeFailed, error.UnsupportedPngFormat => return error.ImageDecodeFailed,
        else => return err,
    };
    errdefer allocator.free(decoded.rgba);
    return try imageFromRgba(allocator, decoded.rgba, decoded.width, decoded.height);
}

fn decodeJpeg(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    const decoded = antfly_image.jpeg.decodeRgba(allocator, image_bytes) catch |err| switch (err) {
        error.JpegDecodeFailed => return error.ImageDecodeFailed,
        else => return err,
    };
    errdefer allocator.free(decoded.rgba);
    return try imageFromRgba(allocator, decoded.rgba, decoded.width, decoded.height);
}

fn decodeGif(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    const frame = antfly_image.gif.decodeFirstFrameAlloc(allocator, image_bytes) catch |err| switch (err) {
        error.GifDecodeFailed, error.UnsupportedGifFormat => return error.ImageDecodeFailed,
        else => return err,
    };
    defer allocator.free(frame.rgba);
    return try imageFromRgbaNoFree(allocator, frame.rgba, frame.width, frame.height);
}

fn decodeBmp(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    const decoded = antfly_image.bmp.decodeRgbaLimited(allocator, image_bytes, antfly_image.DecodeLimits.inference_default) catch |err| switch (err) {
        error.BmpDecodeFailed, error.UnsupportedBmpFormat, error.ImageTooLarge => return error.ImageDecodeFailed,
        else => return err,
    };
    errdefer allocator.free(decoded.rgba);
    return try imageFromRgba(allocator, decoded.rgba, decoded.width, decoded.height);
}

fn decodeWebp(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    const decoded = antfly_image.webp.decodeRgbaLimited(allocator, image_bytes, antfly_image.DecodeLimits.inference_default) catch |err| switch (err) {
        error.WebpDecodeFailed, error.UnsupportedWebpFormat, error.AnimatedWebpUnsupported, error.ImageTooLarge => return error.ImageDecodeFailed,
        else => return err,
    };
    errdefer allocator.free(decoded.rgba);
    return try imageFromRgba(allocator, decoded.rgba, decoded.width, decoded.height);
}

fn imageFromRgba(allocator: std.mem.Allocator, rgba: []u8, width: u32, height: u32) !Image {
    try validateImageDimensions(width, height);
    const rgb = try rgbaToRgbAlloc(allocator, rgba);
    allocator.free(rgba);
    return .{
        .data = rgb,
        .width = width,
        .height = height,
        .channels = 3,
    };
}

fn imageFromRgbaNoFree(allocator: std.mem.Allocator, rgba: []const u8, width: u32, height: u32) !Image {
    try validateImageDimensions(width, height);
    return .{
        .data = try rgbaToRgbAlloc(allocator, rgba),
        .width = width,
        .height = height,
        .channels = 3,
    };
}

fn decodeRgba(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    try validateEncodedImageDimensions(image_bytes);
    switch (antfly_image.detectFormat(image_bytes)) {
        .png => {
            const decoded = antfly_image.png.decodeRgba(allocator, image_bytes) catch |err| switch (err) {
                error.PngDecodeFailed, error.UnsupportedPngFormat => return error.ImageDecodeFailed,
                else => return err,
            };
            return try imageFromOwnedRgba(allocator, decoded.rgba, decoded.width, decoded.height);
        },
        .jpeg => {
            const decoded = antfly_image.jpeg.decodeRgba(allocator, image_bytes) catch |err| switch (err) {
                error.JpegDecodeFailed => return error.ImageDecodeFailed,
                else => return err,
            };
            return try imageFromOwnedRgba(allocator, decoded.rgba, decoded.width, decoded.height);
        },
        .gif => {
            const frame = antfly_image.gif.decodeFirstFrameAlloc(allocator, image_bytes) catch |err| switch (err) {
                error.GifDecodeFailed, error.UnsupportedGifFormat => return error.ImageDecodeFailed,
                else => return err,
            };
            return try imageFromOwnedRgba(allocator, frame.rgba, frame.width, frame.height);
        },
        .bmp => {
            const decoded = antfly_image.bmp.decodeRgbaLimited(allocator, image_bytes, antfly_image.DecodeLimits.inference_default) catch |err| switch (err) {
                error.BmpDecodeFailed, error.UnsupportedBmpFormat, error.ImageTooLarge => return error.ImageDecodeFailed,
                else => return err,
            };
            return try imageFromOwnedRgba(allocator, decoded.rgba, decoded.width, decoded.height);
        },
        .webp => {
            const decoded = antfly_image.webp.decodeRgbaLimited(allocator, image_bytes, antfly_image.DecodeLimits.inference_default) catch |err| switch (err) {
                error.WebpDecodeFailed, error.UnsupportedWebpFormat, error.AnimatedWebpUnsupported, error.ImageTooLarge => return error.ImageDecodeFailed,
                else => return err,
            };
            return try imageFromOwnedRgba(allocator, decoded.rgba, decoded.width, decoded.height);
        },
        else => return error.ImageDecodeFailed,
    }
}

fn imageFromOwnedRgba(allocator: std.mem.Allocator, rgba: []u8, width: u32, height: u32) !Image {
    errdefer allocator.free(rgba);
    try validateImageDimensions(width, height);
    return .{
        .data = rgba,
        .width = width,
        .height = height,
        .channels = 4,
    };
}

fn rgbaToRgbAlloc(allocator: std.mem.Allocator, rgba: []const u8) ![]u8 {
    if (rgba.len % 4 != 0) return error.ImageDecodeFailed;
    const pixel_count = rgba.len / 4;
    const rgb = try allocator.alloc(u8, pixel_count * 3);
    errdefer allocator.free(rgb);
    for (0..pixel_count) |i| {
        rgb[i * 3 + 0] = rgba[i * 4 + 0];
        rgb[i * 3 + 1] = rgba[i * 4 + 1];
        rgb[i * 3 + 2] = rgba[i * 4 + 2];
    }
    return rgb;
}

const red_png_2x2 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02,
    0x08, 0x02, 0x00, 0x00, 0x00, 0xfd, 0xd4, 0x9a, 0x73, 0x00, 0x00, 0x00,
    0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x4f, 0x25, 0xc4, 0xd6, 0x00, 0x00, 0x00, 0x10, 0x49, 0x44,
    0x41, 0x54, 0x78, 0x9c, 0x63, 0xfc, 0xc3, 0x00, 0x02, 0x2c, 0x60, 0x92,
    0x01, 0x00, 0x0d, 0x04, 0x01, 0x02, 0xbf, 0x50, 0x15, 0xb3, 0x00, 0x00,
    0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
};

// A deliberately non-square, high-contrast RGB fixture. The asymmetric edge
// bands make resize/crop regressions visible, while the center gradient catches
// channel ordering and interpolation changes through the encoded-image path.
const clip_contract_png_16x8 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x08,
    0x08, 0x02, 0x00, 0x00, 0x00, 0x7f, 0x14, 0xe8, 0xc0, 0x00, 0x00, 0x00,
    0xfc, 0x49, 0x44, 0x41, 0x54, 0x78, 0xda, 0x25, 0xcb, 0xb1, 0x69, 0x68,
    0x21, 0x14, 0x00, 0xd0, 0x3b, 0x80, 0x65, 0xc0, 0xd2, 0x36, 0x95, 0x64,
    0x80, 0x60, 0x27, 0xb6, 0x19, 0x40, 0xad, 0xfe, 0x06, 0x22, 0xa4, 0xb0,
    0x0f, 0x88, 0x1b, 0x58, 0x89, 0x03, 0xa4, 0x95, 0xdb, 0x66, 0x00, 0xb1,
    0xfa, 0xb8, 0x81, 0x90, 0xc6, 0x0d, 0xcc, 0x03, 0xe1, 0xb4, 0x07, 0x36,
    0x90, 0x0b, 0xe0, 0x50, 0x58, 0x1c, 0x86, 0x04, 0xd4, 0x50, 0x1d, 0xa4,
    0x08, 0xbe, 0x80, 0x6d, 0xa0, 0x3a, 0xbc, 0xbd, 0xec, 0x73, 0xc1, 0x7e,
    0x21, 0x17, 0xb0, 0x43, 0xd9, 0xe2, 0x6c, 0x48, 0x86, 0x9a, 0x55, 0xc7,
    0x52, 0x64, 0xbe, 0x30, 0xdb, 0x98, 0xea, 0xec, 0x09, 0xbf, 0x17, 0xec,
    0x57, 0x72, 0x81, 0x38, 0x54, 0x2c, 0x2e, 0x86, 0x14, 0xa8, 0x45, 0x75,
    0x22, 0x45, 0xe1, 0x8b, 0xb0, 0x4d, 0xa8, 0x2e, 0x9e, 0xf0, 0xff, 0x82,
    0xfd, 0x4e, 0x2e, 0x30, 0x87, 0x9a, 0xc5, 0xcd, 0x90, 0x06, 0xb5, 0xa9,
    0xce, 0xa4, 0x68, 0x7c, 0x31, 0xb6, 0x19, 0xd5, 0xcd, 0x13, 0x7e, 0x2e,
    0xd8, 0x1f, 0xe4, 0x82, 0x70, 0x68, 0x58, 0x3c, 0x0c, 0x19, 0x50, 0x87,
    0xea, 0x42, 0x8a, 0xc1, 0x97, 0x60, 0x5b, 0x50, 0x3d, 0x3c, 0xe1, 0xfb,
    0x82, 0xfd, 0x8f, 0x5c, 0x90, 0x0f, 0xcd, 0x8b, 0xe7, 0x21, 0x33, 0xea,
    0x5c, 0x5d, 0x4e, 0x31, 0xfb, 0x92, 0x6d, 0xcb, 0xaa, 0xe7, 0x27, 0xe4,
    0x0b, 0xf6, 0x27, 0xb9, 0x00, 0x0f, 0xc5, 0xc5, 0x71, 0x48, 0x44, 0x8d,
    0xd5, 0x61, 0x8a, 0xe8, 0x0b, 0xda, 0x86, 0xaa, 0xe3, 0x13, 0xbe, 0x2e,
    0xd8, 0x89, 0x5c, 0x30, 0x0f, 0x9d, 0x8b, 0xcf, 0x21, 0x27, 0xea, 0x59,
    0xdd, 0x4c, 0x71, 0xfa, 0x32, 0x6d, 0x9b, 0xaa, 0xcf, 0x27, 0xb8, 0xeb,
    0x0f, 0x69, 0x25, 0xb9, 0x81, 0x25, 0x4d, 0x68, 0xb9, 0x00, 0x00, 0x00,
    0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
};

const bmp_24_2x2 = [_]u8{
    'B',  'M',  70,   0,    0,    0,    0,    0,    0,    0,    54,   0,    0,    0,
    40,   0,    0,    0,    2,    0,    0,    0,    2,    0,    0,    0,    1,    0,
    24,   0,    0,    0,    0,    0,    16,   0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0xff, 0x00,
    0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0xff, 0x00, 0xff, 0x00, 0x00, 0x00,
};

const animated_gif_1x1 = [_]u8{
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x21, 0xff, 0x0b, 0x4e, 0x45, 0x54, 0x53, 0x43, 0x41, 0x50, 0x45,
    0x32, 0x2e, 0x30, 0x03, 0x01, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x01,
    0x05, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
    0x00, 0x81, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00,
    0x00, 0x00, 0x02, 0x02, 0x4c, 0x01, 0x00, 0x21, 0xf9, 0x04, 0x01, 0x07,
    0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
    0x81, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00,
    0x00, 0x02, 0x02, 0x54, 0x01, 0x00, 0x3b,
};

test "decode png fixture returns rgb image" {
    const alloc = std.testing.allocator;
    const img = try decode(alloc, &red_png_2x2);
    defer img.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), img.width);
    try std.testing.expectEqual(@as(u32, 2), img.height);
    try std.testing.expectEqual(@as(u32, 3), img.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0xfc, 0x00, 0x00 }, img.data[0..3]);
}

test "decode animated gif returns first frame rgb image" {
    const alloc = std.testing.allocator;
    const img = try decode(alloc, &animated_gif_1x1);
    defer img.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), img.width);
    try std.testing.expectEqual(@as(u32, 1), img.height);
    try std.testing.expectEqual(@as(u32, 3), img.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0x00, 0x00 }, img.data[0..3]);
}

test "decode bmp fixture returns rgb image" {
    const alloc = std.testing.allocator;
    const img = try decode(alloc, &bmp_24_2x2);
    defer img.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), img.width);
    try std.testing.expectEqual(@as(u32, 2), img.height);
    try std.testing.expectEqual(@as(u32, 3), img.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0x00, 0x00 }, img.data[0..3]);
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0xff, 0x00 }, img.data[3..6]);
}

test "decode rejects oversized image dimensions before full decode" {
    var png = red_png_2x2;
    std.mem.writeInt(u32, png[16..20], 100_000, .big);
    std.mem.writeInt(u32, png[20..24], 100_000, .big);
    try std.testing.expectError(error.ImageDecodeFailed, decode(std.testing.allocator, &png));

    var bmp = bmp_24_2x2;
    std.mem.writeInt(i32, bmp[18..22], 100_000, .little);
    std.mem.writeInt(i32, bmp[22..26], 100_000, .little);
    try std.testing.expectError(error.ImageDecodeFailed, decode(std.testing.allocator, &bmp));
}

test "encoded image inspection distinguishes malformed and configured limits" {
    try std.testing.expectError(error.ImageDecodeFailed, inspectEncodedForInference("not-an-image", null));
    try std.testing.expectError(error.ImageDecodeFailed, inspectEncodedForInference(red_png_2x2[0..20], null));

    var png = red_png_2x2;
    std.mem.writeInt(u32, png[16..20], 2049, .big);
    try std.testing.expectError(error.ImageTooLarge, inspectEncodedForInference(&png, 2048));

    std.mem.writeInt(u32, png[16..20], 0, .big);
    try std.testing.expectError(error.ImageDecodeFailed, inspectEncodedForInference(&png, null));
    std.mem.writeInt(u32, png[16..20], std.math.maxInt(u32), .big);
    std.mem.writeInt(u32, png[20..24], std.math.maxInt(u32), .big);
    try std.testing.expectError(error.ImageTooLarge, inspectEncodedForInference(&png, null));

    const info = try inspectEncodedForInference(&red_png_2x2, 2);
    try std.testing.expectEqual(@as(u32, 2), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
}

test "decode webp fixture returns rgb image" {
    const alloc = std.testing.allocator;
    const bytes = std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "testdata/image/webp/lossless/literal-rgba-2x3.webp",
        alloc,
        .limited(64 * 1024),
    ) catch |err| switch (err) {
        error.FileNotFound => try std.Io.Dir.cwd().readFileAlloc(
            std.testing.io,
            "../../testdata/image/webp/lossless/literal-rgba-2x3.webp",
            alloc,
            .limited(64 * 1024),
        ),
        else => return err,
    };
    defer alloc.free(bytes);

    const img = try decode(alloc, bytes);
    defer img.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), img.width);
    try std.testing.expectEqual(@as(u32, 3), img.height);
    try std.testing.expectEqual(@as(u32, 3), img.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0x44, 0x22, 0x11 }, img.data[0..3]);
}

test "decode rejects unsupported image format" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.ImageDecodeFailed, decode(alloc, "not-an-image"));
}

test "clip preprocessing resizes short edge before center crop" {
    var rgb = [_]u8{
        0, 0, 0, 10, 0, 0, 20, 0, 0, 30, 0, 0, 40, 0, 0, 50, 0, 0, 60, 0, 0, 70, 0, 0,
        0, 0, 0, 10, 0, 0, 20, 0, 0, 30, 0, 0, 40, 0, 0, 50, 0, 0, 60, 0, 0, 70, 0, 0,
        0, 0, 0, 10, 0, 0, 20, 0, 0, 30, 0, 0, 40, 0, 0, 50, 0, 0, 60, 0, 0, 70, 0, 0,
        0, 0, 0, 10, 0, 0, 20, 0, 0, 30, 0, 0, 40, 0, 0, 50, 0, 0, 60, 0, 0, 70, 0, 0,
    };
    const img = Image{
        .data = rgb[0..],
        .width = 8,
        .height = 4,
        .channels = 3,
    };
    var out: [12]f32 = undefined;

    preprocessDecodedClip(
        img,
        &out,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );

    try std.testing.expectApproxEqAbs(@as(f32, 20.0 / 255.0), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 40.0 / 255.0), out[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 20.0 / 255.0), out[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 40.0 / 255.0), out[3], 1e-6);
}

test "clip preprocessing encoded production path matches full tensor contract" {
    const alloc = std.testing.allocator;
    const target_size: usize = 224;
    const output = try preprocessClipBatch(
        alloc,
        &.{&clip_contract_png_16x8},
        target_size,
        IMAGENET_MEAN,
        IMAGENET_STD,
    );
    defer alloc.free(output);

    try std.testing.expectEqual(@as(usize, 3 * target_size * target_size), output.len);
    try std.testing.expectEqual(@as(u64, 13_952_212_876_790_254_949), clipTensorDigest(output));
}

fn clipTensorDigest(values: []const f32) u64 {
    var hash: u64 = 14_695_981_039_346_656_037;
    for (values) |value| {
        const quantized: i32 = @intFromFloat(@round(value * 10_000.0));
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, quantized, .little);
        for (bytes) |byte| hash = (hash ^ byte) *% 1_099_511_628_211;
    }
    return hash;
}

test "clip preprocessing uses resize source coordinates" {
    var rgb = [_]u8{
        0,   0, 0,
        10,  0, 0,
        100, 0, 0,
        110, 0, 0,
    };
    const img = Image{
        .data = rgb[0..],
        .width = 2,
        .height = 2,
        .channels = 3,
    };
    var out: [48]f32 = undefined;

    preprocessDecodedClip(
        img,
        &out,
        4,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );

    const expected_red = [_]f32{
        0.0,   5.0,   10.0,  10.0,
        50.0,  55.0,  60.0,  60.0,
        100.0, 105.0, 110.0, 110.0,
        100.0, 105.0, 110.0, 110.0,
    };
    for (expected_red, 0..) |expected, i| {
        try std.testing.expectApproxEqAbs(expected / 255.0, out[i], 1e-6);
    }
}

test "clip preprocessing accepts decoded rgba without changing rgb result" {
    var rgb = [_]u8{
        10,  20,  30,
        40,  50,  60,
        70,  80,  90,
        100, 110, 120,
    };
    var rgba = [_]u8{
        10,  20,  30,  255,
        40,  50,  60,  128,
        70,  80,  90,  64,
        100, 110, 120, 0,
    };
    const rgb_img = Image{
        .data = rgb[0..],
        .width = 2,
        .height = 2,
        .channels = 3,
    };
    const rgba_img = Image{
        .data = rgba[0..],
        .width = 2,
        .height = 2,
        .channels = 4,
    };
    var rgb_out: [12]f32 = undefined;
    var rgba_out: [12]f32 = undefined;

    preprocessDecodedClip(
        rgb_img,
        &rgb_out,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );
    preprocessDecodedClip(
        rgba_img,
        &rgba_out,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );

    try std.testing.expectEqualSlices(f32, &rgb_out, &rgba_out);
}

test "clip batch preprocessing matches single image preprocessing" {
    const alloc = std.testing.allocator;
    const images = [_][]const u8{ red_png_2x2[0..], red_png_2x2[0..] };

    const batch = try preprocessClipBatch(
        alloc,
        &images,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );
    defer alloc.free(batch);
    const single = try preprocessClipBatch(
        alloc,
        images[0..1],
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );
    defer alloc.free(single);

    try std.testing.expectEqualSlices(f32, single, batch[0..single.len]);
    try std.testing.expectEqualSlices(f32, single, batch[single.len .. single.len * 2]);
}

test "decode png fixture dimensions are stable" {
    const alloc = std.testing.allocator;
    const img = try decode(alloc, &red_png_2x2);
    defer img.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), img.width);
    try std.testing.expectEqual(@as(u32, 2), img.height);
    try std.testing.expectEqual(@as(u32, 3), img.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0xfc, 0x00, 0x00 }, img.data[0..3]);
}

/// Preprocess image for a vision model: decode → resize → normalize → CHW f32.
/// Returns [3, target_size, target_size] as f32 in channel-first layout.
pub fn preprocess(
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    return preprocessWithResample(allocator, image_bytes, target_size, mean, std_dev, .bilinear);
}

/// Preprocess image with explicit resample mode.
pub fn preprocessWithResample(
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) ![]f32 {
    const img = try decode(allocator, image_bytes);
    defer img.deinit(allocator);
    return preprocessDecodedWithResample(allocator, img, target_size, mean, std_dev, resample);
}

/// Preprocess an already-decoded image.
pub fn preprocessToSize(
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    const img = try decode(image_bytes);
    defer img.deinit();
    return preprocessDecodedToSize(allocator, img, target_width, target_height, mean, std_dev);
}

pub fn preprocessDecodedToSize(
    allocator: std.mem.Allocator,
    img: Image,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    const tw: usize = target_width;
    const th: usize = target_height;
    const result = try allocator.alloc(f32, 3 * th * tw);
    errdefer allocator.free(result);

    const src_w: f32 = @floatFromInt(img.width);
    const src_h: f32 = @floatFromInt(img.height);
    const scale_x = src_w / @as(f32, @floatFromInt(tw));
    const scale_y = src_h / @as(f32, @floatFromInt(th));

    for (0..th) |y| {
        const src_y = @as(f32, @floatFromInt(y)) * scale_y;
        const y0: u32 = @intFromFloat(@floor(src_y));
        const y1: u32 = @min(y0 + 1, img.height - 1);
        const fy = src_y - @as(f32, @floatFromInt(y0));

        for (0..tw) |x| {
            const src_x = @as(f32, @floatFromInt(x)) * scale_x;
            const x0: u32 = @intFromFloat(@floor(src_x));
            const x1: u32 = @min(x0 + 1, img.width - 1);
            const fx = src_x - @as(f32, @floatFromInt(x0));

            for (0..3) |ch| {
                const p00 = pixelAt(img, x0, y0, ch);
                const p10 = pixelAt(img, x1, y0, ch);
                const p01 = pixelAt(img, x0, y1, ch);
                const p11 = pixelAt(img, x1, y1, ch);

                const top = p00 * (1.0 - fx) + p10 * fx;
                const bot = p01 * (1.0 - fx) + p11 * fx;
                const val = top * (1.0 - fy) + bot * fy;

                result[ch * th * tw + y * tw + x] = (val / 255.0 - mean[ch]) / std_dev[ch];
            }
        }
    }

    return result;
}

fn pixelAt(img: Image, x: u32, y: u32, ch: usize) f32 {
    const channels: usize = @intCast(img.channels);
    const width: usize = @intCast(img.width);
    const xi: usize = @intCast(@min(x, img.width - 1));
    const yi: usize = @intCast(@min(y, img.height - 1));
    const ci = @min(ch, channels - 1);
    const idx = (yi * width + xi) * channels + ci;
    return @floatFromInt(img.data[idx]);
}

pub fn preprocessDecoded(
    allocator: std.mem.Allocator,
    img: Image,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    return preprocessDecodedWithResample(allocator, img, target_size, mean, std_dev, .bilinear);
}

pub fn preprocessDecodedWithResample(
    allocator: std.mem.Allocator,
    img: Image,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) ![]f32 {
    return shared.preprocessDecodedWithResample(allocator, toSharedImage(img), target_size, mean, std_dev, resample);
}

/// Preprocess an already-decoded image to an explicit width/height target.
pub fn preprocessDecodedRect(
    allocator: std.mem.Allocator,
    img: Image,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    return preprocessDecodedRectWithResample(allocator, img, target_width, target_height, mean, std_dev, .bilinear);
}

pub fn preprocessDecodedRectWithResample(
    allocator: std.mem.Allocator,
    img: Image,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) ![]f32 {
    return shared.preprocessDecodedRectWithResample(allocator, toSharedImage(img), target_width, target_height, mean, std_dev, resample);
}

pub fn preprocessDecodedRectScaledWithResample(
    allocator: std.mem.Allocator,
    img: Image,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    resample: Resample,
) ![]f32 {
    var values = try shared.preprocessDecodedRectWithResample(
        allocator,
        toSharedImage(img),
        target_width,
        target_height,
        mean,
        std_dev,
        resample,
    );
    errdefer allocator.free(values);
    const scale = rescale_factor * 255.0;
    if (!std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) {
        const plane_stride = @as(usize, @intCast(target_width)) * @as(usize, @intCast(target_height));
        for (0..3) |ch| {
            const adjust = (mean[ch] * (scale - 1.0)) / std_dev[ch];
            const plane = values[ch * plane_stride ..][0..plane_stride];
            for (plane) |*value| value.* = value.* * scale + adjust;
        }
    }
    return values;
}

pub fn preprocessDecodedPix2Struct(
    allocator: std.mem.Allocator,
    img: Image,
    patch_height: usize,
    patch_width: usize,
    max_patches: usize,
    do_normalize: bool,
    resample: Resample,
) !Pix2StructPatches {
    _ = resample;

    if (img.channels < 3 or patch_height == 0 or patch_width == 0 or max_patches == 0) {
        return error.InvalidImageBuffer;
    }

    const feature_depth = 2 + patch_height * patch_width * 3;
    const flattened_patches = try allocator.alloc(f32, max_patches * feature_depth);
    errdefer allocator.free(flattened_patches);
    @memset(flattened_patches, 0);

    const attention_mask = try allocator.alloc(i64, max_patches);
    errdefer allocator.free(attention_mask);
    @memset(attention_mask, 0);

    const image_height = @as(f64, @floatFromInt(img.height));
    const image_width = @as(f64, @floatFromInt(img.width));
    const patch_h_f = @as(f64, @floatFromInt(patch_height));
    const patch_w_f = @as(f64, @floatFromInt(patch_width));
    const max_patches_f = @as(f64, @floatFromInt(max_patches));

    const scale = @sqrt(max_patches_f * (patch_h_f / image_height) * (patch_w_f / image_width));
    var rows = @max(@as(usize, @intFromFloat(@floor(scale * image_height / patch_h_f))), 1);
    var cols = @max(@as(usize, @intFromFloat(@floor(scale * image_width / patch_w_f))), 1);
    rows = @min(rows, max_patches);
    cols = @min(cols, max_patches);
    while (rows * cols > max_patches) {
        if (cols >= rows and cols > 1) {
            cols -= 1;
        } else if (rows > 1) {
            rows -= 1;
        } else {
            break;
        }
    }

    const resized_height = rows * patch_height;
    const resized_width = cols * patch_width;
    const scale_x = @as(f32, @floatCast(image_width / @as(f64, @floatFromInt(resized_width))));
    const scale_y = @as(f32, @floatCast(image_height / @as(f64, @floatFromInt(resized_height))));

    var mean: f32 = 0;
    var adjusted_stddev: f32 = 1;
    if (do_normalize) {
        var sum: f64 = 0;
        for (img.data) |value| sum += @as(f64, @floatFromInt(value));
        const count = @as(f64, @floatFromInt(img.data.len));
        mean = @floatCast(sum / count);

        var variance_sum: f64 = 0;
        for (img.data) |value| {
            const centered = @as(f64, @floatFromInt(value)) - mean;
            variance_sum += centered * centered;
        }
        const stddev = @sqrt(variance_sum / count);
        const min_stddev = 1.0 / @sqrt(count);
        adjusted_stddev = @floatCast(@max(stddev, min_stddev));
    }

    for (0..rows) |row| {
        for (0..cols) |col| {
            const patch_idx = row * cols + col;
            attention_mask[patch_idx] = 1;

            const base = patch_idx * feature_depth;
            flattened_patches[base] = @floatFromInt(row + 1);
            flattened_patches[base + 1] = @floatFromInt(col + 1);

            var out_idx = base + 2;
            for (0..patch_height) |patch_y| {
                const dst_y = row * patch_height + patch_y;
                for (0..patch_width) |patch_x| {
                    const dst_x = col * patch_width + patch_x;
                    for (0..3) |ch| {
                        const raw = sampleImageResized(img, dst_x, dst_y, ch, scale_x, scale_y);
                        flattened_patches[out_idx] = if (do_normalize)
                            (raw - mean) / adjusted_stddev
                        else
                            raw;
                        out_idx += 1;
                    }
                }
            }
        }
    }

    return .{
        .flattened_patches = flattened_patches,
        .attention_mask = attention_mask,
        .rows = rows,
        .cols = cols,
        .allocator = allocator,
    };
}

fn sampleImageResized(
    img: Image,
    dst_x: usize,
    dst_y: usize,
    ch: usize,
    scale_x: f32,
    scale_y: f32,
) f32 {
    const src_x = (@as(f32, @floatFromInt(dst_x)) + 0.5) * scale_x - 0.5;
    const src_y = (@as(f32, @floatFromInt(dst_y)) + 0.5) * scale_y - 0.5;
    const x0 = clampImageIndex(@intFromFloat(@floor(src_x)), img.width);
    const y0 = clampImageIndex(@intFromFloat(@floor(src_y)), img.height);
    const x1 = clampImageIndex(@as(i32, @intCast(x0)) + 1, img.width);
    const y1 = clampImageIndex(@as(i32, @intCast(y0)) + 1, img.height);
    const fx = src_x - @as(f32, @floatFromInt(x0));
    const fy = src_y - @as(f32, @floatFromInt(y0));

    const p00 = imagePixelAt(img, x0, y0, ch);
    const p10 = imagePixelAt(img, x1, y0, ch);
    const p01 = imagePixelAt(img, x0, y1, ch);
    const p11 = imagePixelAt(img, x1, y1, ch);

    const top = p00 * (1.0 - fx) + p10 * fx;
    const bottom = p01 * (1.0 - fx) + p11 * fx;
    return top * (1.0 - fy) + bottom * fy;
}

fn imagePixelAt(img: Image, x: u32, y: u32, ch: usize) f32 {
    const idx = (@as(usize, y) * @as(usize, img.width) + @as(usize, x)) * @as(usize, img.channels) + ch;
    return @floatFromInt(img.data[idx]);
}

fn clampImageIndex(idx: i32, dim: u32) u32 {
    if (idx <= 0) return 0;
    const max_idx: i32 = @intCast(dim - 1);
    if (idx >= max_idx) return @intCast(max_idx);
    return @intCast(idx);
}

pub fn computeAspectFitWidth(src_width: u32, src_height: u32, target_height: u32, max_width: u32) u32 {
    if (src_width == 0 or src_height == 0 or target_height == 0 or max_width == 0) return 0;
    const scaled = (@as(u64, src_width) * @as(u64, target_height) + @as(u64, src_height / 2)) / @as(u64, src_height);
    const clamped = @min(@as(u64, max_width), @max(@as(u64, 1), scaled));
    return @intCast(clamped);
}

pub fn preprocessDecodedRectKeepAspectPadRightWithResample(
    allocator: std.mem.Allocator,
    img: Image,
    max_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
    pad_rgb: [3]u8,
) ![]f32 {
    return preprocessDecodedRectKeepAspectPadRightScaledWithResample(
        allocator,
        img,
        max_width,
        target_height,
        mean,
        std_dev,
        1.0 / 255.0,
        resample,
        pad_rgb,
    );
}

pub fn preprocessDecodedRectKeepAspectPadRightScaledWithResample(
    allocator: std.mem.Allocator,
    img: Image,
    max_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    resample: Resample,
    pad_rgb: [3]u8,
) ![]f32 {
    const resized_width = computeAspectFitWidth(img.width, img.height, target_height, max_width);
    if (resized_width == 0) return error.InvalidImageBuffer;

    var resized = try preprocessDecodedRectScaledWithResample(
        allocator,
        img,
        resized_width,
        target_height,
        mean,
        std_dev,
        rescale_factor,
        resample,
    );
    defer allocator.free(resized);

    const output_plane_stride = @as(usize, max_width) * @as(usize, target_height);
    const resized_plane_stride = @as(usize, resized_width) * @as(usize, target_height);
    const output = try allocator.alloc(f32, 3 * output_plane_stride);
    errdefer allocator.free(output);

    for (0..3) |ch| {
        const pad_value = ((@as(f32, @floatFromInt(pad_rgb[ch])) * rescale_factor) - mean[ch]) / std_dev[ch];
        @memset(output[ch * output_plane_stride ..][0..output_plane_stride], pad_value);

        const src_plane = resized[ch * resized_plane_stride ..][0..resized_plane_stride];
        const dst_plane = output[ch * output_plane_stride ..][0..output_plane_stride];
        for (0..target_height) |row| {
            const dst_row = row * @as(usize, max_width);
            const src_row = row * @as(usize, resized_width);
            @memcpy(dst_plane[dst_row .. dst_row + @as(usize, resized_width)], src_plane[src_row .. src_row + @as(usize, resized_width)]);
        }
    }

    return output;
}

/// Preprocess a batch of images. Returns [batch, 3, target_size, target_size] as f32.
pub fn preprocessBatch(
    allocator: std.mem.Allocator,
    image_list: []const []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    const ts: usize = target_size;
    const per_image = 3 * ts * ts;
    const result = try allocator.alloc(f32, image_list.len * per_image);
    errdefer allocator.free(result);

    for (image_list, 0..) |image_bytes, i| {
        const single = try preprocess(allocator, image_bytes, target_size, mean, std_dev);
        defer allocator.free(single);
        @memcpy(result[i * per_image ..][0..per_image], single);
    }

    return result;
}

/// Preprocess CLIP embedding images: resize the shortest edge to target_size,
/// center crop target_size x target_size, and normalize to CHW f32.
pub fn preprocessClipBatch(
    allocator: std.mem.Allocator,
    image_list: []const []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    const ts: usize = target_size;
    const per_image = 3 * ts * ts;
    const result = try allocator.alloc(f32, image_list.len * per_image);
    errdefer allocator.free(result);

    if (image_list.len > 1) {
        try preprocessClipBatchParallel(image_list, result, per_image, target_size, mean, std_dev);
        return result;
    }

    for (image_list, 0..) |image_bytes, i| {
        try preprocessClipImage(
            allocator,
            image_bytes,
            result[i * per_image ..][0..per_image],
            target_size,
            mean,
            std_dev,
        );
    }

    return result;
}

const max_clip_preprocess_threads: usize = 8;

const ClipPreprocessBatch = struct {
    image_list: []const []const u8,
    result: []f32,
    per_image: usize,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    next: std.atomic.Value(usize) = .{ .raw = 0 },
};

const ClipPreprocessWorker = struct {
    batch: *ClipPreprocessBatch,
    err: ?anyerror = null,

    fn run(self: *ClipPreprocessWorker) void {
        const allocator = std.heap.page_allocator;

        while (true) {
            const i = self.batch.next.fetchAdd(1, .monotonic);
            if (i >= self.batch.image_list.len) return;

            preprocessClipImage(
                allocator,
                self.batch.image_list[i],
                self.batch.result[i * self.batch.per_image ..][0..self.batch.per_image],
                self.batch.target_size,
                self.batch.mean,
                self.batch.std_dev,
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

fn preprocessClipBatchParallel(
    image_list: []const []const u8,
    result: []f32,
    per_image: usize,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) !void {
    const cpu_count = std.Thread.getCpuCount() catch 1;
    const thread_count = @min(image_list.len, @min(cpu_count, max_clip_preprocess_threads));
    if (thread_count <= 1) {
        const allocator = std.heap.page_allocator;
        for (image_list, 0..) |image_bytes, i| {
            try preprocessClipImage(
                allocator,
                image_bytes,
                result[i * per_image ..][0..per_image],
                target_size,
                mean,
                std_dev,
            );
        }
        return;
    }

    var batch = ClipPreprocessBatch{
        .image_list = image_list,
        .result = result,
        .per_image = per_image,
        .target_size = target_size,
        .mean = mean,
        .std_dev = std_dev,
    };
    var workers: [max_clip_preprocess_threads]ClipPreprocessWorker = undefined;
    var threads: [max_clip_preprocess_threads]std.Thread = undefined;
    var spawned: usize = 0;

    while (spawned < thread_count) : (spawned += 1) {
        workers[spawned] = .{ .batch = &batch };
        threads[spawned] = std.Thread.spawn(.{}, ClipPreprocessWorker.run, .{&workers[spawned]}) catch |err| {
            for (threads[0..spawned]) |thread| thread.join();
            return err;
        };
    }
    for (threads[0..spawned]) |thread| thread.join();
    for (workers[0..spawned]) |worker| {
        if (worker.err) |err| return err;
    }
}

fn preprocessClipImage(
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    result: []f32,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) !void {
    if (antfly_image.detectFormat(image_bytes) == .jpeg) {
        const fast = preprocessClipJpegChw(allocator, image_bytes, target_size, mean, std_dev) catch |err| switch (err) {
            error.UnsupportedJpegFormat => null,
            error.JpegDecodeFailed => return error.ImageDecodeFailed,
            else => return err,
        };
        if (fast) |chw| {
            defer allocator.free(chw);
            @memcpy(result, chw);
            return;
        }
    }

    const img = try decodeRgba(allocator, image_bytes);
    defer img.deinit(allocator);
    preprocessDecodedClip(img, result, target_size, mean, std_dev);
}

fn preprocessClipJpegChw(
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    if (clipJpegDctScaleEnabled()) {
        // Opt-in only: DCT scaling is faster but changes the resulting CLIP tensor.
        return antfly_image.jpeg.preprocessClipChwDctScaled(allocator, image_bytes, target_size, mean, std_dev);
    }
    return antfly_image.jpeg.preprocessClipChw(allocator, image_bytes, target_size, mean, std_dev);
}

fn clipJpegDctScaleEnabled() bool {
    const raw = std.c.getenv("TERMITE_CLIP_JPEG_DCT_SCALE") orelse return false;
    const value = std.mem.span(raw);
    return std.mem.eql(u8, value, "1") or std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "TRUE");
}

fn preprocessDecodedClip(
    img: Image,
    result: []f32,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) void {
    std.debug.assert(target_size > 0);
    std.debug.assert(img.width > 0 and img.height > 0);

    const ts: usize = target_size;
    const resized = clipResizeDims(img.width, img.height, target_size);
    const resized_w = resized.width;
    const resized_h = resized.height;
    const crop_left = (resized_w - target_size) / 2;
    const crop_top = (resized_h - target_size) / 2;
    const scale_x = @as(f32, @floatFromInt(img.width)) / @as(f32, @floatFromInt(resized_w));
    const scale_y = @as(f32, @floatFromInt(img.height)) / @as(f32, @floatFromInt(resized_h));

    if (img.width == target_size and img.height == target_size) {
        preprocessClipCenterCropNoResize(img, result, ts, 0, 0, mean, std_dev);
        return;
    }

    for (0..ts) |y| {
        const src_y = @as(f32, @floatFromInt(crop_top + @as(u32, @intCast(y)))) * scale_y;
        const y0: u32 = @intFromFloat(@floor(src_y));
        const y1: u32 = @min(y0 + 1, img.height - 1);
        const fy = src_y - @as(f32, @floatFromInt(y0));

        for (0..ts) |x| {
            const src_x = @as(f32, @floatFromInt(crop_left + @as(u32, @intCast(x)))) * scale_x;
            const x0: u32 = @intFromFloat(@floor(src_x));
            const x1: u32 = @min(x0 + 1, img.width - 1);
            const fx = src_x - @as(f32, @floatFromInt(x0));

            for (0..3) |ch| {
                const p00 = pixelAt(img, x0, y0, ch);
                const p10 = pixelAt(img, x1, y0, ch);
                const p01 = pixelAt(img, x0, y1, ch);
                const p11 = pixelAt(img, x1, y1, ch);
                const top = p00 * (1.0 - fx) + p10 * fx;
                const bottom = p01 * (1.0 - fx) + p11 * fx;
                const value = top * (1.0 - fy) + bottom * fy;
                result[ch * ts * ts + y * ts + x] = (value / 255.0 - mean[ch]) / std_dev[ch];
            }
        }
    }
}

const ClipResizeDims = struct {
    width: u32,
    height: u32,
};

fn clipResizeDims(width: u32, height: u32, target_size: u32) ClipResizeDims {
    if (width <= height) {
        return .{ .width = target_size, .height = scaledLongEdge(height, width, target_size) };
    }
    return .{ .width = scaledLongEdge(width, height, target_size), .height = target_size };
}

fn scaledLongEdge(long_edge: u32, short_edge: u32, target_size: u32) u32 {
    const scaled = @divFloor(@as(u64, long_edge) * @as(u64, target_size), @as(u64, short_edge));
    return @max(target_size, @as(u32, @intCast(scaled)));
}

fn preprocessClipCenterCropNoResize(
    img: Image,
    result: []f32,
    target_size: usize,
    crop_left: u32,
    crop_top: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) void {
    const width: usize = @intCast(img.width);
    const channels: usize = @intCast(img.channels);
    const left: usize = @intCast(crop_left);
    const top: usize = @intCast(crop_top);
    const plane_stride = target_size * target_size;

    const inv_std = [3]f32{
        1.0 / std_dev[0],
        1.0 / std_dev[1],
        1.0 / std_dev[2],
    };
    const inv_255 = 1.0 / 255.0;

    for (0..target_size) |y| {
        const row_base = ((top + y) * width + left) * channels;
        const dst_row = y * target_size;
        for (0..target_size) |x| {
            const src = row_base + x * channels;
            const dst = dst_row + x;
            inline for (0..3) |ch| {
                const value = @as(f32, @floatFromInt(img.data[src + ch])) * inv_255;
                result[ch * plane_stride + dst] = (value - mean[ch]) * inv_std[ch];
            }
        }
    }
}

fn toSharedImage(img: Image) ImageU8 {
    return .{
        .data = img.data[0 .. @as(usize, img.width) * @as(usize, img.height) * @as(usize, img.channels)],
        .width = img.width,
        .height = img.height,
        .format = switch (img.channels) {
            3 => .rgb8,
            4 => .rgba8,
            else => .rgb8,
        },
    };
}
