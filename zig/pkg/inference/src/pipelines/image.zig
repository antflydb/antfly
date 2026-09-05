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
const linalg = @import("inference_linalg");
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

test "bounded batch preprocessing preserves input-indexed tensor order" {
    const images = [_][]const u8{ red_png_2x2[0..], clip_contract_png_16x8[0..] };
    var serial: [24]f32 = undefined;
    var parallel: [24]f32 = undefined;

    try preprocessBatchIntoBounded(
        &serial,
        &images,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        .bilinear,
        .{ .max_workers = 1, .max_inflight_decoded_bytes = 16 * 1024 },
    );
    try preprocessBatchIntoBounded(
        &parallel,
        &images,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        .bilinear,
        .{ .max_workers = 2, .max_inflight_decoded_bytes = 16 * 1024 },
    );

    try std.testing.expectEqualSlices(f32, &serial, &parallel);
    try std.testing.expect(!std.mem.eql(u8, std.mem.sliceAsBytes(serial[0..12]), std.mem.sliceAsBytes(serial[12..24])));
}

test "borrowed raster batch preprocesses strided RGBA without decode copy or retention" {
    var padded = [_]u8{
        255, 0, 0,   255, 0, 255, 0, 255, 91, 92, 93, 94,
        0,   0, 255, 255, 7, 8,   9, 255, 81, 82, 83, 84,
    };
    const raster = antfly_image.BorrowedRasterAttachment{
        .bytes = &padded,
        .width = 2,
        .height = 2,
        .stride_bytes = 12,
        .item_id = "page-7",
        .page_number = 7,
    };
    const view = try raster.imageView();
    try std.testing.expectEqual(@intFromPtr(padded[0..].ptr), @intFromPtr(view.data.ptr));

    var actual: [12]f32 = undefined;
    try preprocessBorrowedRasterBatchInto(
        &actual,
        &.{raster},
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        .nearest,
    );
    const snapshot = actual;
    @memset(&padded, 0);
    try std.testing.expectEqualSlices(f32, &snapshot, &actual);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), actual[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), actual[5], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), actual[10], 1e-6);
}

test "borrowed raster CLIP preprocessing preserves strided center crop semantics" {
    var padded = [_]u8{
        0, 0, 0, 255, 10, 0, 0, 255, 20, 0, 0, 255, 30, 0, 0, 255, 91, 92, 93, 94,
        0, 0, 0, 255, 10, 0, 0, 255, 20, 0, 0, 255, 30, 0, 0, 255, 81, 82, 83, 84,
    };
    const raster = antfly_image.BorrowedRasterAttachment{
        .bytes = &padded,
        .width = 4,
        .height = 2,
        .stride_bytes = 20,
    };
    var actual: [12]f32 = undefined;
    try preprocessClipBorrowedRasterBatchInto(
        &actual,
        &.{raster},
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
    );
    try std.testing.expectApproxEqAbs(@as(f32, 10.0 / 255.0), actual[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 20.0 / 255.0), actual[1], 1e-6);
    for (actual[4..]) |value| try std.testing.expectApproxEqAbs(@as(f32, 0), value, 1e-6);
}

test "bounded batch preprocessing uses a caller-owned executor without changing output" {
    const images = [_][]const u8{ red_png_2x2[0..], clip_contract_png_16x8[0..] };
    var serial: [24]f32 = undefined;
    var runtime_owned: [24]f32 = undefined;

    try preprocessBatchIntoBounded(
        &serial,
        &images,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        .bilinear,
        .{ .max_workers = 1, .max_inflight_decoded_bytes = 16 * 1024 },
    );
    try preprocessBatchIntoBounded(
        &runtime_owned,
        &images,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        .bilinear,
        .{
            .max_workers = 2,
            .max_inflight_decoded_bytes = 16 * 1024,
            .io = std.testing.io,
        },
    );

    try std.testing.expectEqualSlices(f32, &serial, &runtime_owned);
}

test "bounded batch preprocessing rejects a decoded image above its wave budget" {
    var output: [12]f32 = undefined;
    try std.testing.expectError(
        error.ImagePreprocessDecodedBytesExceeded,
        preprocessBatchIntoBounded(
            &output,
            &.{red_png_2x2[0..]},
            2,
            .{ 0.0, 0.0, 0.0 },
            .{ 1.0, 1.0, 1.0 },
            .bilinear,
            .{ .max_workers = 2, .max_inflight_decoded_bytes = 15 },
        ),
    );
}

fn readPipelineImageFixture(allocator: std.mem.Allocator, relative_path: []const u8) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        relative_path,
        allocator,
        .limited(4 * 1024 * 1024),
    ) catch |err| switch (err) {
        error.FileNotFound => {
            const prefixed = try std.fmt.allocPrint(allocator, "../../{s}", .{relative_path});
            defer allocator.free(prefixed);
            return std.Io.Dir.cwd().readFileAlloc(
                std.testing.io,
                prefixed,
                allocator,
                .limited(4 * 1024 * 1024),
            );
        },
        else => return err,
    };
}

test "bounded preprocessing charges high-bit-depth PNG codec scratch" {
    const alloc = std.testing.allocator;
    const fixture = try readPipelineImageFixture(alloc, "testdata/image/png/highbit/rgba16-2x2.png");
    defer alloc.free(fixture);

    var output: [12]f32 = undefined;
    // The old pixels*4 estimate was exactly 16 bytes and admitted this image,
    // despite the decoder retaining a 16-bit raw scan buffer at the same time.
    try std.testing.expectError(
        error.ImagePreprocessDecodedBytesExceeded,
        preprocessBatchIntoBounded(
            &output,
            &.{fixture},
            2,
            .{ 0.0, 0.0, 0.0 },
            .{ 1.0, 1.0, 1.0 },
            .bilinear,
            .{ .max_workers = 1, .max_inflight_decoded_bytes = 16 },
        ),
    );
}

test "bounded preprocessing charges progressive JPEG coefficient state" {
    const alloc = std.testing.allocator;
    const fixture = try readPipelineImageFixture(alloc, "testdata/image/jpeg/progressive/red-3x2.jpg");
    defer alloc.free(fixture);

    var output: [12]f32 = undefined;
    // The final 3x2 RGBA image is 24 bytes. Progressive coefficient storage is
    // now charged to the same hard boundary instead of escaping the wave cap.
    try std.testing.expectError(
        error.ImagePreprocessDecodedBytesExceeded,
        preprocessBatchIntoBounded(
            &output,
            &.{fixture},
            2,
            .{ 0.0, 0.0, 0.0 },
            .{ 1.0, 1.0, 1.0 },
            .bilinear,
            .{ .max_workers = 1, .max_inflight_decoded_bytes = 24 },
        ),
    );
}

test "preprocessing slab physically bounds many small codec allocations" {
    var budget = try SharedPreprocessBudget.initWithBacking(std.testing.allocator, 4096);
    defer budget.deinit();
    const allocator = budget.allocator();
    var allocations: [512][]u8 = undefined;
    var allocation_count: usize = 0;
    while (allocation_count < allocations.len) {
        const memory = allocator.alloc(u8, 1) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            break;
        };
        const address = @intFromPtr(memory.ptr);
        try std.testing.expect(address >= @intFromPtr(budget.slab.ptr));
        try std.testing.expect(address + memory.len <= @intFromPtr(budget.slab.ptr) + budget.slab.len);
        allocations[allocation_count] = memory;
        allocation_count += 1;
    }
    try std.testing.expect(allocation_count > 1);
    try std.testing.expect(allocation_count < allocations.len);
    try std.testing.expect(budget.limit_exceeded.load(.acquire));
    try std.testing.expect(budget.peak_live_bytes.load(.acquire) <= budget.slab.len);
    for (allocations[0..allocation_count]) |memory| allocator.free(memory);
    try std.testing.expectEqual(@as(usize, 0), budget.live_bytes.load(.acquire));
}

test "preprocessing slab preserves backing allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        SharedPreprocessBudget.initWithBacking(failing.allocator(), 4096),
    );
}

test "preprocessing slab supports concurrent codec allocation lifetimes" {
    var budget = try SharedPreprocessBudget.initWithBacking(std.testing.allocator, 1024 * 1024);
    defer budget.deinit();
    var failed = std.atomic.Value(bool).init(false);
    const Worker = struct {
        fn run(shared_budget: *SharedPreprocessBudget, worker_failed: *std.atomic.Value(bool)) void {
            const allocator = shared_budget.allocator();
            var blocks: [64][]u8 = undefined;
            var count: usize = 0;
            while (count < blocks.len) : (count += 1) {
                blocks[count] = allocator.alloc(u8, 1 + (count % 31)) catch {
                    worker_failed.store(true, .release);
                    break;
                };
            }
            while (count > 0) {
                count -= 1;
                allocator.free(blocks[count]);
            }
        }
    };

    var threads: [8]std.Thread = undefined;
    var started: usize = 0;
    errdefer for (threads[0..started]) |thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{ &budget, &failed });
        started += 1;
    }
    for (threads) |thread| thread.join();
    started = 0;

    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), budget.live_bytes.load(.acquire));
    try std.testing.expect(budget.peak_live_bytes.load(.acquire) <= budget.slab.len);
}

test "preprocessing slab sizing follows bounded wave demand instead of request ceiling" {
    const ceiling = 128 * 1024 * 1024;
    const tiny = preprocessSlabBytesForWave(16, 1, ceiling);
    try std.testing.expect(tiny >= 16);
    try std.testing.expect(tiny < 1024 * 1024);
    try std.testing.expectEqual(ceiling, preprocessSlabBytesForWave(ceiling, maximum_preprocess_workers, ceiling));
    try std.testing.expectEqual(ceiling, preprocessSlabBytesForWave(std.math.maxInt(usize), maximum_preprocess_workers, ceiling));
}

test "preprocessing slab growth remains physically capped" {
    const TrackingBacking = struct {
        child: std.mem.Allocator,
        live_bytes: usize = 0,
        peak_live_bytes: usize = 0,

        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            } };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const memory = self.child.rawAlloc(len, alignment, ret_addr) orelse return null;
            self.live_bytes += len;
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
            return memory;
        }

        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }

        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }

        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.assert(self.live_bytes >= memory.len);
            self.live_bytes -= memory.len;
            self.child.rawFree(memory, alignment, ret_addr);
        }
    };
    var tracking = TrackingBacking{ .child = std.testing.allocator };
    var budget = try SharedPreprocessBudget.initResizableWithBacking(tracking.allocator(), 4096, 16 * 1024);
    defer budget.deinit();
    const allocator = budget.allocator();

    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 6000));
    try std.testing.expect(budget.limit_exceeded.load(.acquire));
    try std.testing.expect(try budget.growAfterExhaustion());
    try std.testing.expectEqual(@as(usize, 8192), budget.slab.len);
    const memory = try allocator.alloc(u8, 6000);
    allocator.free(memory);

    try std.testing.expect(try budget.growAfterExhaustion());
    try std.testing.expectEqual(@as(usize, 16 * 1024), budget.slab.len);
    try std.testing.expect(!(try budget.growAfterExhaustion()));
    try std.testing.expect(budget.slab.len <= budget.max_bytes);
    try std.testing.expect(tracking.peak_live_bytes <= budget.max_bytes);
}

test "preprocessing slab growth failure leaves no retired backing allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var budget = try SharedPreprocessBudget.initResizableWithBacking(failing.allocator(), 4096, 8192);
    defer budget.deinit();

    try std.testing.expectError(error.OutOfMemory, budget.ensureCapacity(8192));
    try std.testing.expectEqual(@as(usize, 0), budget.slab.len);
    try std.testing.expect(budget.free_head == null);
}

test "adaptive preprocessing recovers concurrency for a cheap tail" {
    const ceiling: usize = 8;
    var workers = reduceAdaptivePreprocessWorkers(ceiling, ceiling);
    try std.testing.expectEqual(@as(usize, 4), workers);
    workers = reduceAdaptivePreprocessWorkers(workers, workers);
    try std.testing.expectEqual(@as(usize, 2), workers);

    // Once an expensive prefix has completed at the reduced width, successful
    // cheap waves restore parallelism without exceeding the configured limit.
    workers = recoverAdaptivePreprocessWorkers(workers, ceiling);
    try std.testing.expectEqual(@as(usize, 4), workers);
    workers = recoverAdaptivePreprocessWorkers(workers, ceiling);
    try std.testing.expectEqual(ceiling, workers);
    try std.testing.expectEqual(ceiling, recoverAdaptivePreprocessWorkers(workers, ceiling));
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

pub fn preprocessDecodedWithResampleInto(
    img: Image,
    output: []f32,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) !void {
    return shared.preprocessDecodedWithResampleInto(toSharedImage(img), output, target_size, mean, std_dev, resample);
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
    return preprocessBatchWithOptions(allocator, image_list, target_size, mean, std_dev, .{});
}

pub fn preprocessBatchWithOptions(
    allocator: std.mem.Allocator,
    image_list: []const []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    options: BatchPreprocessOptions,
) ![]f32 {
    const ts: usize = target_size;
    const per_image_side = std.math.mul(usize, ts, ts) catch return error.InvalidInputShape;
    const per_image = std.math.mul(usize, 3, per_image_side) catch return error.InvalidInputShape;
    const output_len = std.math.mul(usize, image_list.len, per_image) catch return error.InvalidInputShape;
    const result = try allocator.alloc(f32, output_len);
    errdefer allocator.free(result);
    try preprocessBatchIntoBounded(result, image_list, target_size, mean, std_dev, .bilinear, options);
    return result;
}

/// Controls CPU preprocessing independently from model batch size. The byte
/// ceiling is a hard aggregate limit for every allocation made by the image
/// codecs in an active wave. The caller-owned output tensor is admitted by the
/// model runtime separately and is intentionally not charged here.
pub const BatchPreprocessOptions = struct {
    max_workers: usize = 8,
    max_inflight_decoded_bytes: usize = 128 * 1024 * 1024,
    /// Production runtimes pass their leased inference executor here. Offline
    /// and test callers may omit it and use the synchronous linalg fallback.
    io: ?std.Io = null,
};

/// Decode and preprocess directly into a caller-owned batch tensor. Output
/// slices are indexed before workers start, so completion order cannot reorder
/// model inputs. Temporary decode state uses one thread-safe allocator per
/// task and is released before the next admitted wave.
pub fn preprocessBatchIntoBounded(
    result: []f32,
    image_list: []const []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
    options: BatchPreprocessOptions,
) !void {
    const ts: usize = target_size;
    const per_image_side = std.math.mul(usize, ts, ts) catch return error.InvalidInputShape;
    const per_image = std.math.mul(usize, 3, per_image_side) catch return error.InvalidInputShape;
    const expected_len = std.math.mul(usize, image_list.len, per_image) catch return error.InvalidInputShape;
    if (result.len != expected_len) return error.InvalidInputShape;
    try runBoundedPreprocessBatch(image_list, result, per_image, .{ .square = .{
        .target_size = target_size,
        .mean = mean,
        .std_dev = std_dev,
        .resample = resample,
    } }, options);
}

/// Preprocess renderer-owned rasters directly into caller-owned model storage.
/// This synchronous path has no allocator and therefore cannot decode, copy,
/// or retain the borrowed source buffers. Arbitrary validated row stride is
/// preserved all the way into the sampler.
pub fn preprocessBorrowedRasterBatchInto(
    result: []f32,
    rasters: []const antfly_image.BorrowedRasterAttachment,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) !void {
    const ts: usize = target_size;
    const per_image_side = std.math.mul(usize, ts, ts) catch return error.InvalidInputShape;
    const per_image = std.math.mul(usize, 3, per_image_side) catch return error.InvalidInputShape;
    const expected_len = std.math.mul(usize, rasters.len, per_image) catch return error.InvalidInputShape;
    if (result.len != expected_len) return error.InvalidInputShape;

    for (rasters, 0..) |raster, index| {
        const view = try raster.imageView();
        try shared.preprocessDecodedWithResampleInto(
            view,
            result[index * per_image ..][0..per_image],
            target_size,
            mean,
            std_dev,
            resample,
        );
    }
}

/// CLIP/SigLIP variant of the borrowed-raster fast path. It preserves the
/// encoded implementation's short-edge resize and center-crop semantics while
/// honoring renderer row stride and retaining no source memory.
pub fn preprocessClipBorrowedRasterBatchInto(
    result: []f32,
    rasters: []const antfly_image.BorrowedRasterAttachment,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) !void {
    const ts: usize = target_size;
    const per_image_side = std.math.mul(usize, ts, ts) catch return error.InvalidInputShape;
    const per_image = std.math.mul(usize, 3, per_image_side) catch return error.InvalidInputShape;
    const expected_len = std.math.mul(usize, rasters.len, per_image) catch return error.InvalidInputShape;
    if (result.len != expected_len) return error.InvalidInputShape;

    for (rasters, 0..) |raster, index| {
        const view = try raster.imageView();
        try view.validate();
        preprocessImageViewClip(
            view,
            result[index * per_image ..][0..per_image],
            target_size,
            mean,
            std_dev,
        );
    }
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
    return preprocessClipBatchWithOptions(allocator, image_list, target_size, mean, std_dev, .{});
}

pub fn preprocessClipBatchWithOptions(
    allocator: std.mem.Allocator,
    image_list: []const []const u8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    options: BatchPreprocessOptions,
) ![]f32 {
    const ts: usize = target_size;
    const per_image_side = std.math.mul(usize, ts, ts) catch return error.InvalidInputShape;
    const per_image = std.math.mul(usize, 3, per_image_side) catch return error.InvalidInputShape;
    const output_len = std.math.mul(usize, image_list.len, per_image) catch return error.InvalidInputShape;
    const result = try allocator.alloc(f32, output_len);
    errdefer allocator.free(result);

    try runBoundedPreprocessBatch(image_list, result, per_image, .{ .clip = .{
        .target_size = target_size,
        .mean = mean,
        .std_dev = std_dev,
    } }, options);
    return result;
}

const maximum_preprocess_workers: usize = 8;
const preprocess_slab_worker_headroom_bytes: usize = 64 * 1024;

const BatchPreprocessOperation = union(enum) {
    square: struct {
        target_size: u32,
        mean: [3]f32,
        std_dev: [3]f32,
        resample: Resample,
    },
    clip: struct {
        target_size: u32,
        mean: [3]f32,
        std_dev: [3]f32,
    },
};

fn preprocessSlabBytesForWave(decoded_bytes: usize, worker_count: usize, max_bytes: usize) usize {
    // RGBA is the format-independent lower bound. Double it for codec working
    // state and add bounded per-worker metadata/entropy scratch. This is only
    // an eager sizing hint: the allocator grows on a real exhaustion signal,
    // and max_bytes remains the authoritative physical ceiling.
    const decoded_and_scratch = std.math.mul(usize, decoded_bytes, 2) catch std.math.maxInt(usize);
    const worker_headroom = std.math.mul(usize, worker_count, preprocess_slab_worker_headroom_bytes) catch std.math.maxInt(usize);
    const desired = std.math.add(usize, decoded_and_scratch, worker_headroom) catch std.math.maxInt(usize);
    return @min(max_bytes, @max(@min(max_bytes, @sizeOf(SharedPreprocessBudget.FreeBlock)), desired));
}

fn reduceAdaptivePreprocessWorkers(current: usize, failed_wave_len: usize) usize {
    const observed = @min(current, failed_wave_len);
    return if (observed <= 2) 1 else observed / 2;
}

fn recoverAdaptivePreprocessWorkers(current: usize, configured_limit: usize) usize {
    return @min(configured_limit, std.math.mul(usize, current, 2) catch configured_limit);
}

/// A wave-scoped, thread-safe allocator that accounts decoder output and all
/// codec scratch against one aggregate ceiling. Using the allocator as the
/// enforcement boundary avoids format-specific peak estimates becoming stale
/// when a codec adds a new intermediate buffer.
const SharedPreprocessBudget = struct {
    const FreeBlock = extern struct {
        len: usize,
        next: ?*FreeBlock,
    };
    const AllocationHeader = extern struct {
        block_start: [*]u8,
        block_len: usize,
    };

    backing: std.mem.Allocator,
    slab: []align(@alignOf(FreeBlock)) u8,
    max_bytes: usize,
    free_head: ?*FreeBlock,
    locked: std.atomic.Value(bool) = .init(false),
    live_bytes: std.atomic.Value(usize) = .init(0),
    peak_live_bytes: std.atomic.Value(usize) = .init(0),
    limit_exceeded: std.atomic.Value(bool) = .init(false),

    fn init(max_live_bytes: usize) !@This() {
        return initResizableWithBacking(std.heap.page_allocator, max_live_bytes, max_live_bytes);
    }

    fn initWithBacking(backing: std.mem.Allocator, max_live_bytes: usize) !@This() {
        return initResizableWithBacking(backing, max_live_bytes, max_live_bytes);
    }

    fn initResizableWithBacking(backing: std.mem.Allocator, initial_bytes: usize, max_bytes: usize) !@This() {
        if (initial_bytes == 0 or initial_bytes > max_bytes) return error.InvalidBatchPreprocessOptions;
        const slab = try backing.alignedAlloc(u8, .of(FreeBlock), initial_bytes);
        var self = @This(){
            .backing = backing,
            .slab = slab,
            .max_bytes = max_bytes,
            .free_head = null,
        };
        self.resetFreeList();
        return self;
    }

    fn resetFreeList(self: *@This()) void {
        self.free_head = null;
        if (self.slab.len < @sizeOf(FreeBlock)) return;
        const first: *FreeBlock = @ptrCast(@alignCast(self.slab.ptr));
        first.* = .{ .len = self.slab.len, .next = null };
        self.free_head = first;
    }

    /// Grow only between waves, when no codec retains an allocation. Retire the
    /// old slab before allocating its replacement so physical backing never
    /// exceeds max_bytes, even transiently. On backing failure the request is
    /// terminal, but the empty budget remains valid for deferred destruction.
    fn ensureCapacity(self: *@This(), requested_bytes: usize) !void {
        std.debug.assert(self.live_bytes.load(.acquire) == 0);
        const target = @min(requested_bytes, self.max_bytes);
        if (target <= self.slab.len) return;

        const retired = self.slab;
        self.slab = retired[0..0];
        self.free_head = null;
        self.backing.free(retired);
        const replacement = try self.backing.alignedAlloc(u8, .of(FreeBlock), target);
        self.slab = replacement;
        self.resetFreeList();
    }

    fn growAfterExhaustion(self: *@This()) !bool {
        std.debug.assert(self.live_bytes.load(.acquire) == 0);
        if (self.slab.len >= self.max_bytes) return false;
        const doubled = std.math.mul(usize, self.slab.len, 2) catch self.max_bytes;
        try self.ensureCapacity(@min(self.max_bytes, @max(self.slab.len + 1, doubled)));
        return true;
    }

    fn deinit(self: *@This()) void {
        std.debug.assert(self.live_bytes.load(.acquire) == 0);
        if (self.slab.len > 0) self.backing.free(self.slab);
        self.* = undefined;
    }

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn lock(self: *@This()) void {
        while (true) {
            if (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) == null) return;
            std.Thread.yield() catch {};
        }
    }

    fn unlock(self: *@This()) void {
        self.locked.store(false, .release);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        _ = ret_addr;
        self.lock();
        defer self.unlock();

        const requested_alignment = alignment.toByteUnits();
        const allocation_alignment = @max(requested_alignment, @alignOf(AllocationHeader));
        var previous: ?*FreeBlock = null;
        var current = self.free_head;
        while (current) |block| {
            const block_start = @intFromPtr(block);
            const header_end = std.math.add(usize, block_start, @sizeOf(AllocationHeader)) catch break;
            const user_start = std.mem.alignForward(usize, header_end, allocation_alignment);
            const user_end = std.math.add(usize, user_start, len) catch break;
            var suffix_start = std.mem.alignForward(usize, user_end, @alignOf(FreeBlock));
            const block_end = std.math.add(usize, block_start, block.len) catch break;
            if (suffix_start > block_end or user_end > block_end) {
                previous = block;
                current = block.next;
                continue;
            }

            // Avoid manufacturing a free fragment too small to hold its own
            // metadata. Such tail padding remains charged to this allocation.
            if (block_end - suffix_start < @sizeOf(FreeBlock)) suffix_start = block_end;
            const consumed = suffix_start - block_start;
            const next = block.next;
            if (suffix_start < block_end) {
                const suffix: *FreeBlock = @ptrFromInt(suffix_start);
                suffix.* = .{ .len = block_end - suffix_start, .next = next };
                if (previous) |prev| prev.next = suffix else self.free_head = suffix;
            } else if (previous) |prev| {
                prev.next = next;
            } else {
                self.free_head = next;
            }

            const header: *AllocationHeader = @ptrFromInt(user_start - @sizeOf(AllocationHeader));
            header.* = .{ .block_start = @ptrFromInt(block_start), .block_len = consumed };
            const live = self.live_bytes.load(.monotonic) + consumed;
            self.live_bytes.store(live, .release);
            self.peak_live_bytes.store(@max(self.peak_live_bytes.load(.monotonic), live), .release);
            return @ptrFromInt(user_start);
        }
        self.limit_exceeded.store(true, .release);
        return null;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        _ = ctx;
        _ = memory;
        _ = alignment;
        _ = new_len;
        _ = ret_addr;
        // Returning false asks Allocator to allocate-copy-free. This keeps the
        // free-list metadata simple and charges the true simultaneous peak.
        return false;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        _ = ctx;
        _ = memory;
        _ = alignment;
        _ = new_len;
        _ = ret_addr;
        return null;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        _ = alignment;
        _ = ret_addr;
        self.lock();
        defer self.unlock();

        const header_addr = @intFromPtr(memory.ptr) - @sizeOf(AllocationHeader);
        const header: *const AllocationHeader = @ptrFromInt(header_addr);
        const block_start = header.block_start;
        const block_len = header.block_len;
        const released: *FreeBlock = @ptrCast(@alignCast(block_start));
        released.* = .{ .len = block_len, .next = null };

        var previous: ?*FreeBlock = null;
        var current = self.free_head;
        while (current) |block| {
            if (@intFromPtr(block) > @intFromPtr(released)) break;
            previous = block;
            current = block.next;
        }
        released.next = current;
        if (previous) |prev| prev.next = released else self.free_head = released;

        if (current) |next| {
            if (@intFromPtr(released) + released.len == @intFromPtr(next)) {
                released.len += next.len;
                released.next = next.next;
            }
        }
        if (previous) |prev| {
            if (@intFromPtr(prev) + prev.len == @intFromPtr(released)) {
                prev.len += released.len;
                prev.next = released.next;
            }
        }

        const old_live = self.live_bytes.load(.monotonic);
        std.debug.assert(old_live >= block_len);
        self.live_bytes.store(old_live - block_len, .release);
    }
};

const BatchPreprocessTask = struct {
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    output: []f32,
    operation: BatchPreprocessOperation,
    err: ?anyerror = null,

    fn run(self: *BatchPreprocessTask) void {
        switch (self.operation) {
            .square => |square| {
                const decoded = decodeRgba(self.allocator, self.image_bytes) catch |err| {
                    self.err = err;
                    return;
                };
                defer decoded.deinit(self.allocator);
                preprocessDecodedWithResampleInto(
                    decoded,
                    self.output,
                    square.target_size,
                    square.mean,
                    square.std_dev,
                    square.resample,
                ) catch |err| {
                    self.err = err;
                };
            },
            .clip => |clip| preprocessClipImage(
                self.allocator,
                self.image_bytes,
                self.output,
                clip.target_size,
                clip.mean,
                clip.std_dev,
            ) catch |err| {
                self.err = err;
            },
        }
    }

    fn runOpaque(ptr: *anyopaque) void {
        const self: *BatchPreprocessTask = @ptrCast(@alignCast(ptr));
        self.run();
    }
};

fn runBoundedPreprocessBatch(
    image_list: []const []const u8,
    result: []f32,
    per_image: usize,
    operation: BatchPreprocessOperation,
    options: BatchPreprocessOptions,
) !void {
    if (options.max_workers == 0 or options.max_inflight_decoded_bytes == 0)
        return error.InvalidBatchPreprocessOptions;
    if (image_list.len == 0) return;

    const decoded_bytes = try std.heap.page_allocator.alloc(usize, image_list.len);
    defer std.heap.page_allocator.free(decoded_bytes);
    for (image_list, decoded_bytes) |image_bytes, *bytes| {
        const info = inspectEncodedForInference(image_bytes, null) catch |err| switch (err) {
            // Preserve the public preprocess/decode error contract while the
            // new batch-only aggregate ceiling remains separately observable.
            error.ImageTooLarge => return error.ImageDecodeFailed,
            else => return err,
        };
        const pixels = try info.pixels();
        const rgba_bytes_u64 = std.math.mul(u64, pixels, 4) catch return error.ImagePreprocessDecodedBytesExceeded;
        bytes.* = std.math.cast(usize, rgba_bytes_u64) orelse return error.ImagePreprocessDecodedBytesExceeded;
        if (bytes.* > options.max_inflight_decoded_bytes) return error.ImagePreprocessDecodedBytesExceeded;
    }

    const cpu_count = linalg.pool.cachedCpuCount();
    const worker_limit = @max(@as(usize, 1), @min(image_list.len, @min(cpu_count, @min(options.max_workers, maximum_preprocess_workers))));
    var tasks: [maximum_preprocess_workers]BatchPreprocessTask = undefined;
    var jobs: [maximum_preprocess_workers]linalg.pool.Job = undefined;

    var first: usize = 0;
    var adaptive_worker_limit: usize = worker_limit;
    // Start from the first wave's bounded demand rather than reserving the
    // entire (normally 128 MiB) request ceiling. The slab grows only between
    // waves and never beyond the ceiling, then remains reusable for the rest
    // of this request without per-wave mmap/munmap churn.
    const first_wave_len = planPreprocessWaveLength(decoded_bytes, first, adaptive_worker_limit, options.max_inflight_decoded_bytes);
    const first_wave_bytes = preprocessWaveDecodedBytes(decoded_bytes[first..][0..first_wave_len]);
    var wave_budget = try SharedPreprocessBudget.initResizableWithBacking(
        std.heap.page_allocator,
        preprocessSlabBytesForWave(first_wave_bytes, first_wave_len, options.max_inflight_decoded_bytes),
        options.max_inflight_decoded_bytes,
    );
    defer wave_budget.deinit();
    const wave_allocator = wave_budget.allocator();
    while (first < image_list.len) {
        const wave_len = planPreprocessWaveLength(decoded_bytes, first, adaptive_worker_limit, options.max_inflight_decoded_bytes);
        const wave_decoded_bytes = preprocessWaveDecodedBytes(decoded_bytes[first..][0..wave_len]);
        std.debug.assert(wave_len > 0);
        std.debug.assert(wave_budget.live_bytes.load(.acquire) == 0);
        try wave_budget.ensureCapacity(preprocessSlabBytesForWave(
            wave_decoded_bytes,
            wave_len,
            options.max_inflight_decoded_bytes,
        ));
        wave_budget.limit_exceeded.store(false, .release);

        for (tasks[0..wave_len], 0..) |*task, offset| {
            const index = first + offset;
            task.* = .{
                .allocator = wave_allocator,
                .image_bytes = image_list[index],
                .output = result[index * per_image ..][0..per_image],
                .operation = operation,
            };
        }

        for (jobs[0..wave_len], tasks[0..wave_len]) |*job, *task| {
            job.* = .{ .fn_ptr = BatchPreprocessTask.runOpaque, .ctx = task };
        }
        // Production calls schedule through the BackendRuntime-owned
        // inference executor, which supplies the aggregate CPU/thread bound
        // and lifecycle. Offline callers retain the synchronous fallback.
        if (options.io) |io|
            try linalg.pool.dispatchJobsIo(io, jobs[0..wave_len])
        else for (jobs[0..wave_len]) |job|
            job.fn_ptr(job.ctx);
        // Error selection is input-index deterministic rather than completion
        // order dependent.
        var first_error: ?anyerror = null;
        for (tasks[0..wave_len]) |task| {
            if (task.err) |err| {
                first_error = err;
                break;
            }
        }
        std.debug.assert(wave_budget.live_bytes.load(.acquire) == 0);
        if (first_error) |err| {
            if (err == error.OutOfMemory and wave_budget.limit_exceeded.load(.acquire)) {
                // An intentionally small initial reservation is not evidence
                // that concurrency is unsafe. Grow within the physical cap and
                // retry this same wave before reducing worker width.
                if (try wave_budget.growAfterExhaustion()) continue;
                // A format may need substantially more scratch than its final
                // RGBA buffer. Retry with less concurrency so a valid image is
                // not rejected merely because peers occupied the shared cap.
                if (wave_len > 1) {
                    adaptive_worker_limit = reduceAdaptivePreprocessWorkers(adaptive_worker_limit, wave_len);
                    continue;
                }
                return error.ImagePreprocessDecodedBytesExceeded;
            }
            return err;
        }
        first += wave_len;
        adaptive_worker_limit = recoverAdaptivePreprocessWorkers(adaptive_worker_limit, worker_limit);
    }
}

fn planPreprocessWaveLength(decoded_bytes: []const usize, first: usize, worker_limit: usize, max_bytes: usize) usize {
    var wave_len: usize = 0;
    var wave_decoded_bytes: usize = 0;
    while (first + wave_len < decoded_bytes.len and wave_len < worker_limit) {
        const bytes = decoded_bytes[first + wave_len];
        const next = std.math.add(usize, wave_decoded_bytes, bytes) catch break;
        if (wave_len > 0 and next > max_bytes) break;
        wave_decoded_bytes = next;
        wave_len += 1;
    }
    return wave_len;
}

fn preprocessWaveDecodedBytes(decoded_bytes: []const usize) usize {
    var total: usize = 0;
    for (decoded_bytes) |bytes| total += bytes;
    return total;
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
        const fast = preprocessClipJpegChwInto(allocator, image_bytes, result, target_size, mean, std_dev) catch |err| switch (err) {
            error.UnsupportedJpegFormat => false,
            error.JpegDecodeFailed => return error.ImageDecodeFailed,
            else => return err,
        };
        if (fast) return;
    }

    const img = try decodeRgba(allocator, image_bytes);
    defer img.deinit(allocator);
    preprocessDecodedClip(img, result, target_size, mean, std_dev);
}

fn preprocessClipJpegChwInto(
    allocator: std.mem.Allocator,
    image_bytes: []const u8,
    result: []f32,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) !bool {
    if (clipJpegDctScaleEnabled()) {
        // Opt-in only: DCT scaling is faster but changes the resulting CLIP tensor.
        try antfly_image.jpeg.preprocessClipChwDctScaledInto(allocator, image_bytes, result, target_size, mean, std_dev);
    } else {
        try antfly_image.jpeg.preprocessClipChwInto(allocator, image_bytes, result, target_size, mean, std_dev);
    }
    return true;
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
    preprocessImageViewClip(toSharedImage(img), result, target_size, mean, std_dev);
}

fn preprocessImageViewClip(
    img: ImageU8,
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
        preprocessClipCenterCropNoResizeView(img, result, ts, 0, 0, mean, std_dev);
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
                const p00 = pixelAtView(img, x0, y0, ch);
                const p10 = pixelAtView(img, x1, y0, ch);
                const p01 = pixelAtView(img, x0, y1, ch);
                const p11 = pixelAtView(img, x1, y1, ch);
                const top = p00 * (1.0 - fx) + p10 * fx;
                const bottom = p01 * (1.0 - fx) + p11 * fx;
                const value = top * (1.0 - fy) + bottom * fy;
                result[ch * ts * ts + y * ts + x] = (value / 255.0 - mean[ch]) / std_dev[ch];
            }
        }
    }
}

fn pixelAtView(img: ImageU8, x: u32, y: u32, ch: usize) f32 {
    const channels = img.channels();
    const stride = img.rowStride() catch unreachable;
    const xi: usize = @intCast(@min(x, img.width - 1));
    const yi: usize = @intCast(@min(y, img.height - 1));
    const ci = @min(ch, channels - 1);
    return @floatFromInt(img.data[yi * stride + xi * channels + ci]);
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
    preprocessClipCenterCropNoResizeView(toSharedImage(img), result, target_size, crop_left, crop_top, mean, std_dev);
}

fn preprocessClipCenterCropNoResizeView(
    img: ImageU8,
    result: []f32,
    target_size: usize,
    crop_left: u32,
    crop_top: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) void {
    const channels = img.channels();
    const stride = img.rowStride() catch unreachable;
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
        const row_base = (top + y) * stride + left * channels;
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
