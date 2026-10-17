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

//! Explicit coded-pixel versus presentation display geometry.
const std = @import("std");
const media = @import("antfly_media");
const preparation = @import("preparation.zig");
pub const Policy = enum { coded_pixels, presentation };
pub const Layout = struct {
    rotation: preparation.Rotation,
    /// Square-pixel display dimensions before model-specific resizing.
    width: u32,
    height: u32,
};

/// Recognizes orthogonal MP4 display transforms, including translation. Rejects
/// perspective, shear, reflection and arbitrary-angle transforms rather than
/// approximating them. SAR is applied along the coded horizontal axis before
/// rotation. Caller chooses whether to honor metadata; no implicit HDR mapping.
pub fn layout(track: media.mp4.Track, width: u32, height: u32, policy: Policy) !Layout {
    if (width == 0 or height == 0 or width > 16384 or height > 16384) return error.InvalidVideoGeometry;
    if (policy == .coded_pixels) return .{ .rotation = .none, .width = width, .height = height };
    const matrix = track.display_matrix;
    if (matrix[2] != 0 or matrix[5] != 0 or matrix[8] != 1073741824) return error.UnsupportedVideoDisplay;
    const basis = [4]i32{ matrix[0], matrix[1], matrix[3], matrix[4] };
    const rotation: preparation.Rotation = if (std.mem.eql(i32, &basis, &.{ 65536, 0, 0, 65536 })) .none else if (std.mem.eql(i32, &basis, &.{ 0, 65536, -65536, 0 })) .clockwise90 else if (std.mem.eql(i32, &basis, &.{ -65536, 0, 0, -65536 })) .half_turn else if (std.mem.eql(i32, &basis, &.{ 0, -65536, 65536, 0 })) .clockwise270 else return error.UnsupportedVideoDisplay;
    const sar = track.pixel_aspect;
    if (sar.horizontal == 0 or sar.vertical == 0) return error.UnsupportedVideoDisplay;
    const numerator = @as(u64, width) * sar.horizontal;
    const display_width = (numerator + sar.vertical / 2) / sar.vertical;
    if (display_width == 0 or display_width > 16384) return error.ResourceLimitExceeded;
    return if (rotation == .clockwise90 or rotation == .clockwise270)
        .{ .rotation = rotation, .width = height, .height = @intCast(display_width) }
    else
        .{ .rotation = rotation, .width = @intCast(display_width), .height = height };
}

pub const WebmLayout = struct { display: Layout, crop: media.webm.Crop };
/// WebM display dimensions describe the cropped raster. Physical units and
/// aspect-ratio units provide a ratio, never an inferred physical screen size.
pub fn webmLayout(track: media.webm.Track, width: u32, height: u32, policy: Policy) !WebmLayout {
    if (width == 0 or height == 0 or width > 16384 or height > 16384) return error.InvalidVideoGeometry;
    if (policy == .coded_pixels) return .{ .display = .{ .rotation = .none, .width = width, .height = height }, .crop = .{} };
    const p = track.presentation;
    const cropped = try preparation.croppedSize(width, height, p.crop);
    if (p.stereo_mode != 0 or p.projection_type != 0 or p.projection_private.len != 0 or p.yaw != 0 or p.pitch != 0) return error.UnsupportedVideoDisplay;
    const rotation: preparation.Rotation = if (p.roll == 0) .none else if (p.roll == 90) .clockwise270 else if (p.roll == -90) .clockwise90 else if (@abs(p.roll) == 180) .half_turn else return error.UnsupportedVideoDisplay;
    if (p.display_unit > 3) return error.UnsupportedVideoDisplay;
    if (p.display_unit != 0 and (p.display_width == null or p.display_height == null)) return error.UnsupportedVideoDisplay;
    // Schema defaults refer to Track PixelWidth/Height, not the dimensions of
    // a later codec picture. Preserve container aspect across resolution changes.
    const declared = try preparation.croppedSize(track.width, track.height, p.crop);
    const dw = p.display_width orelse declared[0];
    const dh = p.display_height orelse declared[1];
    if (dw == 0 or dh == 0) return error.InvalidVideoGeometry;
    // Preserve the cropped height and apply the display aspect once, directly
    // in the model target geometry; no intermediate stretched image is needed.
    const numerator = @as(u64, cropped[1]) * dw;
    const display_width = (numerator + dh / 2) / dh;
    if (display_width == 0 or display_width > 16384) return error.ResourceLimitExceeded;
    const display: Layout = if (rotation == .clockwise90 or rotation == .clockwise270)
        .{ .rotation = rotation, .width = cropped[1], .height = @intCast(display_width) }
    else
        .{ .rotation = rotation, .width = @intCast(display_width), .height = cropped[1] };
    return .{ .display = display, .crop = p.crop };
}

test "display policy distinguishes coded pixels SAR quarter turns and unsupported transforms" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/mjpeg.mov") }, .identity = "display-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var track = reader.track;
    track.pixel_aspect = .{ .horizontal = 4, .vertical = 3 };
    track.display_matrix = .{ 0, 65536, 0, -65536, 0, 0, 123, -456, 1073741824 };
    try std.testing.expectEqual(Layout{ .rotation = .clockwise90, .width = 480, .height = 960 }, try layout(track, 720, 480, .presentation));
    try std.testing.expectEqual(Layout{ .rotation = .none, .width = 720, .height = 480 }, try layout(track, 720, 480, .coded_pixels));
    track.display_matrix[1] = -65536;
    track.display_matrix[3] = 65536;
    try std.testing.expectEqual(preparation.Rotation.clockwise270, (try layout(track, 720, 480, .presentation)).rotation);
    track.display_matrix[0] = 32768;
    try std.testing.expectError(error.UnsupportedVideoDisplay, layout(track, 720, 480, .presentation));
    track.display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 };
    track.pixel_aspect.vertical = 0;
    try std.testing.expectError(error.UnsupportedVideoDisplay, layout(track, 720, 480, .presentation));
}

test "WebM presentation applies crop then aspect ratio then counterclockwise roll" {
    var track = media.webm.Track{ .number = 1, .codec = .vp9, .width = 720, .height = 480, .presentation = .{
        .crop = .{ .left = 8, .right = 8, .top = 4, .bottom = 4 },
        .display_width = 16,
        .display_height = 9,
        .display_unit = 3,
        .roll = 90,
    } };
    const actual = try webmLayout(track, 720, 480, .presentation);
    try std.testing.expectEqual(Layout{ .rotation = .clockwise270, .width = 472, .height = 839 }, actual.display);
    try std.testing.expectEqual(track.presentation.crop, actual.crop);
    try std.testing.expectEqual(Layout{ .rotation = .none, .width = 720, .height = 480 }, (try webmLayout(track, 720, 480, .coded_pixels)).display);
    track.presentation.roll = -90;
    try std.testing.expectEqual(preparation.Rotation.clockwise90, (try webmLayout(track, 720, 480, .presentation)).display.rotation);
    track.presentation.display_unit = 0;
    track.presentation.display_width = null;
    track.presentation.display_height = null;
    try std.testing.expectEqual(Layout{ .rotation = .clockwise90, .width = 472, .height = 704 }, (try webmLayout(track, 720, 480, .presentation)).display);
    track.presentation.roll = 45;
    try std.testing.expectError(error.UnsupportedVideoDisplay, webmLayout(track, 720, 480, .presentation));
    track.presentation.roll = 0;
    track.presentation.yaw = 90;
    try std.testing.expectError(error.UnsupportedVideoDisplay, webmLayout(track, 720, 480, .presentation));
    track.presentation.yaw = 0;
    track.presentation.display_unit = 4;
    try std.testing.expectError(error.UnsupportedVideoDisplay, webmLayout(track, 720, 480, .presentation));
    track.presentation.display_unit = 3;
    try std.testing.expectError(error.UnsupportedVideoDisplay, webmLayout(track, 720, 480, .presentation));
}

test "WebM implicit display aspect follows track dimensions across codec geometry changes" {
    const track = media.webm.Track{ .number = 1, .codec = .av1, .width = 720, .height = 480 };
    try std.testing.expectEqual(Layout{ .rotation = .none, .width = 300, .height = 200 }, (try webmLayout(track, 360, 200, .presentation)).display);
    try std.testing.expectEqual(Layout{ .rotation = .none, .width = 360, .height = 200 }, (try webmLayout(track, 360, 200, .coded_pixels)).display);
}
