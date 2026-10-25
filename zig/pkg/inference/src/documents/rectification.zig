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

//! Bounded photo rectification. Pixel edges use original image coordinates;
//! the returned projective mapping is also used by every OCR/evidence consumer.
const std = @import("std");
const c = @import("contract.zig");
const image = @import("antfly_image");
const Quad = [4][2]f64;
pub const Plan = struct { width: u32, height: u32, raster_to_source: c.Transform };

fn distance(a: [2]f64, b: [2]f64) f64 {
    return @sqrt((a[0] - b[0]) * (a[0] - b[0]) + (a[1] - b[1]) * (a[1] - b[1]));
}

/// Corners are top-left, top-right, bottom-right, bottom-left in image-edge
/// coordinates. Require a strictly convex clockwise polygon, contained in the
/// source raster. Reject folds and degenerate geometry before any allocation.
pub fn plan(quad: Quad, width: u32, height: u32, limits: c.Limits) !Plan {
    if (width == 0 or height == 0) return error.InvalidDocumentQuadrilateral;
    for (quad) |p| {
        if (!std.math.isFinite(p[0]) or !std.math.isFinite(p[1]) or p[0] < 0 or p[1] < 0 or p[0] > @as(f64, @floatFromInt(width)) or p[1] > @as(f64, @floatFromInt(height))) return error.InvalidDocumentQuadrilateral;
    }
    for (0..4) |i| {
        const a = quad[i];
        const b = quad[(i + 1) % 4];
        const d = quad[(i + 2) % 4];
        if ((b[0] - a[0]) * (d[1] - b[1]) - (b[1] - a[1]) * (d[0] - b[0]) <= 1e-9) return error.InvalidDocumentQuadrilateral;
    }
    const w = @ceil(@max(distance(quad[0], quad[1]), distance(quad[3], quad[2])));
    const h = @ceil(@max(distance(quad[0], quad[3]), distance(quad[1], quad[2])));
    if (w < 1 or h < 1 or w > @as(f64, @floatFromInt(limits.max_dimension)) or h > @as(f64, @floatFromInt(limits.max_dimension))) return error.DocumentPixelLimitExceeded;
    const out_width: u32 = @intFromFloat(w);
    const out_height: u32 = @intFromFloat(h);
    if (@as(u64, out_width) * out_height > limits.max_pixels) return error.DocumentPixelLimitExceeded;
    const dx1 = quad[1][0] - quad[2][0];
    const dx2 = quad[3][0] - quad[2][0];
    const dy1 = quad[1][1] - quad[2][1];
    const dy2 = quad[3][1] - quad[2][1];
    const dx3 = quad[0][0] - quad[1][0] + quad[2][0] - quad[3][0];
    const dy3 = quad[0][1] - quad[1][1] + quad[2][1] - quad[3][1];
    const det = dx1 * dy2 - dx2 * dy1;
    if (@abs(det) < 1e-12) return error.InvalidDocumentQuadrilateral;
    const g = (dx3 * dy2 - dx2 * dy3) / det;
    const j = (dx1 * dy3 - dx3 * dy1) / det;
    const transform = c.Transform{ .matrix = .{
        (quad[1][0] - quad[0][0] + g * quad[1][0]) / w, (quad[3][0] - quad[0][0] + j * quad[3][0]) / h, quad[0][0],
        (quad[1][1] - quad[0][1] + g * quad[1][1]) / w, (quad[3][1] - quad[0][1] + j * quad[3][1]) / h, quad[0][1],
        g / w,                                          j / h,                                          1,
    } };
    _ = try transform.inverse();
    return .{ .width = out_width, .height = out_height, .raster_to_source = transform };
}

/// Bilinear interpolation in premultiplied alpha avoids dark or colored fringes
/// around transparent source pixels. Storage and work are bounded by the plan.
pub fn warp(a: std.mem.Allocator, source: image.processing.ImageU8, target: Plan, control: c.Control) ![]u8 {
    try control.check();
    try source.validate();
    if (source.format != .rgba8 or target.width == 0 or target.height == 0) return error.InvalidDocumentQuadrilateral;
    const pixels = std.math.mul(usize, target.width, target.height) catch return error.DocumentPixelLimitExceeded;
    const length = std.math.mul(usize, pixels, 4) catch return error.DocumentPixelLimitExceeded;
    const result = try a.alloc(u8, length);
    errdefer a.free(result);
    const stride = try source.rowStride();
    for (0..target.height) |y| {
        try control.check();
        for (0..target.width) |x| {
            const p = try target.raster_to_source.point(.{ @as(f64, @floatFromInt(x)) + 0.5, @as(f64, @floatFromInt(y)) + 0.5 });
            const sx = @max(0, @min(@as(f64, @floatFromInt(source.width - 1)), p[0] - 0.5));
            const sy = @max(0, @min(@as(f64, @floatFromInt(source.height - 1)), p[1] - 0.5));
            const left: usize = @intFromFloat(@floor(sx));
            const top: usize = @intFromFloat(@floor(sy));
            const right = @min(left + 1, source.width - 1);
            const bottom = @min(top + 1, source.height - 1);
            const fx = sx - @floor(sx);
            const fy = sy - @floor(sy);
            const weights = [4]f64{ (1 - fx) * (1 - fy), fx * (1 - fy), (1 - fx) * fy, fx * fy };
            const offsets = [4]usize{ top * stride + left * 4, top * stride + right * 4, bottom * stride + left * 4, bottom * stride + right * 4 };
            var alpha: f64 = 0;
            var colors: [3]f64 = @splat(0);
            for (offsets, weights) |offset, weight| {
                const contribution = weight * @as(f64, @floatFromInt(source.data[offset + 3]));
                alpha += contribution;
                for (&colors, 0..) |*color, channel| color.* += contribution * @as(f64, @floatFromInt(source.data[offset + channel]));
            }
            const offset = (y * target.width + x) * 4;
            result[offset + 3] = @intFromFloat(@round(@max(0, @min(255, alpha))));
            for (colors, 0..) |color, channel| result[offset + channel] = if (alpha > 0) @intFromFloat(@round(@max(0, @min(255, color / alpha)))) else 0;
        }
    }
    return result;
}

test "document rectification maps all corners and round trips source evidence" {
    const quad: Quad = .{ .{ 1, 1 }, .{ 8, 2 }, .{ 9, 9 }, .{ 0, 8 } };
    const target = try plan(quad, 10, 10, .{});
    const inverse = try target.raster_to_source.inverse();
    const corners = [4][2]f64{ .{ 0, 0 }, .{ @floatFromInt(target.width), 0 }, .{ @floatFromInt(target.width), @floatFromInt(target.height) }, .{ 0, @floatFromInt(target.height) } };
    for (corners, quad) |corner, expected| {
        const mapped = try target.raster_to_source.point(corner);
        const back = try inverse.point(mapped);
        for (mapped, expected) |actual, wanted| try std.testing.expectApproxEqAbs(wanted, actual, 1e-9);
        for (back, corner) |actual, wanted| try std.testing.expectApproxEqAbs(wanted, actual, 1e-9);
    }
    for ([_]Quad{
        .{ .{ 0, 0 }, .{ 10, 0 }, .{ 0, 10 }, .{ 10, 10 } },
        .{ .{ 0, 0 }, .{ 10, 0 }, .{ 10, 0 }, .{ 0, 10 } },
        .{ .{ -1, 0 }, .{ 10, 0 }, .{ 10, 10 }, .{ 0, 10 } },
        .{ .{ std.math.nan(f64), 0 }, .{ 10, 0 }, .{ 10, 10 }, .{ 0, 10 } },
    }) |invalid| try std.testing.expectError(error.InvalidDocumentQuadrilateral, plan(invalid, 10, 10, .{}));
    try std.testing.expectError(error.DocumentPixelLimitExceeded, plan(.{ .{ 0, 0 }, .{ 10, 0 }, .{ 10, 10 }, .{ 0, 10 } }, 10, 10, .{ .max_pixels = 99 }));
}

test "document rectification preserves identity pixels and premultiplied alpha" {
    const a = std.testing.allocator;
    const quad: Quad = .{ .{ 0, 0 }, .{ 2, 0 }, .{ 2, 2 }, .{ 0, 2 } };
    const pixels = [_]u8{ 255, 0, 0, 255, 0, 255, 0, 128, 0, 0, 255, 64, 255, 255, 255, 255 };
    const source = image.processing.ImageU8{ .data = &pixels, .width = 2, .height = 2, .format = .rgba8 };
    const result = try warp(a, source, try plan(quad, 2, 2, .{}), .{});
    defer a.free(result);
    try std.testing.expectEqualSlices(u8, &pixels, result);
    var shifted = try plan(quad, 2, 2, .{});
    shifted.raster_to_source.matrix[2] = 0.5;
    const blended = try warp(a, source, shifted, .{});
    defer a.free(blended);
    try std.testing.expectEqual(@as(u8, 192), blended[3]);
    try std.testing.expectEqual(@as(u8, 170), blended[0]);
    try std.testing.expectEqual(@as(u8, 85), blended[1]);
    const Stop = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, warp(std.testing.failing_allocator, source, shifted, .{ .check_fn = Stop.check }));
}
