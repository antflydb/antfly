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
const image_mod = @import("image.zig");
const multistage_ocr = @import("multistage_ocr.zig");

pub const Orientation = enum { preserve, rotate_tall_ccw };

pub fn cropBBox(allocator: std.mem.Allocator, img: image_mod.Image, bbox: [4]f64, orientation: Orientation) !image_mod.Image {
    const x = @as(i64, @intFromFloat(bbox[0]));
    const y = @as(i64, @intFromFloat(bbox[1]));
    const width = @as(i64, @intFromFloat(bbox[2] - bbox[0])) + 1;
    const height = @as(i64, @intFromFloat(bbox[3] - bbox[1])) + 1;

    if (width <= 0 or height <= 0) return zeroImage(allocator, 1, 1, img.channels);

    const crop_width: u32 = @intCast(width);
    const crop_height: u32 = @intCast(height);
    // Paddle's recognition crop uses np.rot90 when height / width >= 1.5.
    const rotate = orientation == .rotate_tall_ccw and
        @as(u64, crop_height) * 2 >= @as(u64, crop_width) * 3;
    const out_width = if (rotate) crop_height else crop_width;
    const out_height = if (rotate) crop_width else crop_height;
    const channels: u32 = if (img.channels == 0) 3 else img.channels;
    const pixel_count = @as(usize, out_width) * @as(usize, out_height) * @as(usize, channels);
    const data = try allocator.alloc(u8, pixel_count);
    errdefer allocator.free(data);
    @memset(data, 0);

    const src_width = @as(i64, img.width);
    const src_height = @as(i64, img.height);
    const channel_count = @as(usize, channels);

    for (0..@as(usize, crop_height)) |dy| {
        for (0..@as(usize, crop_width)) |dx| {
            const src_x = x + @as(i64, @intCast(dx));
            const src_y = y + @as(i64, @intCast(dy));
            if (src_x < 0 or src_y < 0 or src_x >= src_width or src_y >= src_height) continue;

            const src_offset = (@as(usize, @intCast(src_y)) * @as(usize, img.width) + @as(usize, @intCast(src_x))) * channel_count;
            const dst_offset = (if (rotate)
                (@as(usize, crop_width) - 1 - dx) * @as(usize, out_width) + dy
            else
                dy * @as(usize, out_width) + dx) * channel_count;
            @memcpy(data[dst_offset .. dst_offset + channel_count], img.data[src_offset .. src_offset + channel_count]);
        }
    }

    return .{
        .data = data,
        .width = out_width,
        .height = out_height,
        .channels = channels,
    };
}

pub fn cropRegion(allocator: std.mem.Allocator, img: image_mod.Image, region: multistage_ocr.TextRegion) !image_mod.Image {
    return cropBBox(allocator, img, region.bbox, .preserve);
}

pub fn sortTextRegionsByReadingOrder(regions: []multistage_ocr.TextRegion) void {
    multistage_ocr.sortRegionsByReadingOrder(multistage_ocr.TextRegion, regions);
}

pub fn sortRecognizedRegionsByReadingOrder(regions: []multistage_ocr.RecognizedRegion) void {
    multistage_ocr.sortRegionsByReadingOrder(multistage_ocr.RecognizedRegion, regions);
}

fn zeroImage(allocator: std.mem.Allocator, width: u32, height: u32, channels_in: u32) !image_mod.Image {
    const channels: u32 = if (channels_in == 0) 3 else channels_in;
    const data = try allocator.alloc(u8, @as(usize, width) * @as(usize, height) * @as(usize, channels));
    @memset(data, 0);
    return .{
        .data = data,
        .width = width,
        .height = height,
        .channels = channels,
    };
}

test "cropBBox returns inclusive rectangular crop" {
    const allocator = std.testing.allocator;
    var src_data = [_]u8{
        1,  2,  3,  4,  5,  6,  7,  8,  9,
        10, 11, 12, 13, 14, 15, 16, 17, 18,
    };
    const src = image_mod.Image{
        .data = src_data[0..],
        .width = 3,
        .height = 2,
        .channels = 3,
    };

    const cropped = try cropBBox(allocator, src, .{ 1, 0, 2, 0 }, .preserve);
    defer cropped.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 2), cropped.width);
    try std.testing.expectEqual(@as(u32, 1), cropped.height);
    try std.testing.expectEqualSlices(u8, &.{ 4, 5, 6, 7, 8, 9 }, cropped.data);
}

test "cropBBox pads out of bounds with zeros" {
    const allocator = std.testing.allocator;
    var src_data = [_]u8{
        1, 2, 3, 4, 5, 6,
    };
    const src = image_mod.Image{
        .data = src_data[0..],
        .width = 2,
        .height = 1,
        .channels = 3,
    };

    const cropped = try cropBBox(allocator, src, .{ -1, 0, 1, 0 }, .preserve);
    defer cropped.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 3), cropped.width);
    try std.testing.expectEqualSlices(u8, &.{
        0, 0, 0,
        1, 2, 3,
        4, 5, 6,
    }, cropped.data);
}

test "recognition crop rotates tall RGB pixels counterclockwise and preserves source coordinates" {
    const allocator = std.testing.allocator;
    var pixels = [_]u8{
        1,  2,  3,  4,  5,  6,
        7,  8,  9,  10, 11, 12,
        13, 14, 15, 16, 17, 18,
    };
    const src = image_mod.Image{ .data = &pixels, .width = 2, .height = 3, .channels = 3 };
    const rotated = try cropBBox(allocator, src, .{ 0, 0, 1, 2 }, .rotate_tall_ccw);
    defer rotated.deinit(allocator);
    try std.testing.expectEqual(@as(u32, 3), rotated.width);
    try std.testing.expectEqual(@as(u32, 2), rotated.height);
    try std.testing.expectEqualSlices(u8, &.{
        4, 5, 6, 10, 11, 12, 16, 17, 18,
        1, 2, 3, 7,  8,  9,  13, 14, 15,
    }, rotated.data);

    const preserved = try cropBBox(allocator, src, .{ 0, 0, 1, 2 }, .preserve);
    defer preserved.deinit(allocator);
    try std.testing.expectEqual(@as(u32, 2), preserved.width);
    try std.testing.expectEqual(@as(u32, 3), preserved.height);
    try std.testing.expectEqualSlices(u8, &pixels, preserved.data);

    const square = try cropBBox(allocator, src, .{ 0, 0, 1, 1 }, .rotate_tall_ccw);
    defer square.deinit(allocator);
    try std.testing.expectEqualSlices(u8, pixels[0..12], square.data);

    const padded = try cropBBox(allocator, src, .{ -1, 0, 0, 2 }, .rotate_tall_ccw);
    defer padded.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &.{
        1, 2, 3, 7, 8, 9, 13, 14, 15,
        0, 0, 0, 0, 0, 0, 0,  0,  0,
    }, padded.data);
}
