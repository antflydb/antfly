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
