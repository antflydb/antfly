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
// Decodes JPEG/PNG/GIF through the shared antfly image layer, then routes
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

/// Decode a JPEG/PNG/GIF from raw bytes. Always returns 3-channel RGB.
pub fn decode(allocator: std.mem.Allocator, image_bytes: []const u8) !Image {
    if (isPng(image_bytes)) {
        const decoded = antfly_image.png.decodeRgba(allocator, image_bytes) catch |err| switch (err) {
            error.PngDecodeFailed, error.UnsupportedPngFormat => return error.ImageDecodeFailed,
            else => return err,
        };
        errdefer allocator.free(decoded.rgba);
        const rgb = try rgbaToRgbAlloc(allocator, decoded.rgba);
        allocator.free(decoded.rgba);
        return .{
            .data = rgb,
            .width = decoded.width,
            .height = decoded.height,
            .channels = 3,
        };
    }

    if (isJpeg(image_bytes)) {
        const decoded = antfly_image.jpeg.decodeRgba(allocator, image_bytes) catch |err| switch (err) {
            error.JpegDecodeFailed => return error.ImageDecodeFailed,
            else => return err,
        };
        errdefer allocator.free(decoded.rgba);
        const rgb = try rgbaToRgbAlloc(allocator, decoded.rgba);
        allocator.free(decoded.rgba);
        return .{
            .data = rgb,
            .width = decoded.width,
            .height = decoded.height,
            .channels = 3,
        };
    }

    if (isGif(image_bytes)) {
        const frames = antfly_image.gif.decodeFramesAlloc(allocator, image_bytes) catch |err| switch (err) {
            error.GifDecodeFailed, error.UnsupportedGifFormat => return error.ImageDecodeFailed,
            else => return err,
        };
        defer {
            for (frames) |frame| allocator.free(frame.rgba);
            allocator.free(frames);
        }
        if (frames.len == 0) return error.ImageDecodeFailed;
        const rgb = try rgbaToRgbAlloc(allocator, frames[0].rgba);
        return .{
            .data = rgb,
            .width = frames[0].width,
            .height = frames[0].height,
            .channels = 3,
        };
    }

    return error.ImageDecodeFailed;
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

fn isPng(bytes: []const u8) bool {
    return bytes.len >= 8 and std.mem.eql(u8, bytes[0..8], &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' });
}

fn isJpeg(bytes: []const u8) bool {
    return bytes.len >= 3 and bytes[0] == 0xff and bytes[1] == 0xd8 and bytes[2] == 0xff;
}

fn isGif(bytes: []const u8) bool {
    return bytes.len >= 6 and (std.mem.eql(u8, bytes[0..6], "GIF87a") or std.mem.eql(u8, bytes[0..6], "GIF89a"));
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

test "decode rejects unsupported image format" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.ImageDecodeFailed, decode(alloc, "not-an-image"));
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
